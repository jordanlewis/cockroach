// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package logictest provides a datadriven test framework for the TDS
// (Sybase/SQL Server) wire-protocol frontend. Test files contain T-SQL
// statements and queries with expected results, exercising the full
// TDS stack against a real CockroachDB server.
//
// Supported directives:
//
//	exec
//	<T-SQL statement>
//	----
//
// Executes a T-SQL statement (DDL/DML) and expects success.
//
//	exec error=(substring)
//	<T-SQL statement>
//	----
//
// Executes a T-SQL statement and asserts the error message contains
// the given substring.
//
//	query [colnames]
//	<T-SQL query>
//	----
//	<expected rows, two-space separated columns>
//
// Executes a T-SQL query and compares formatted output against expected.
// With the "colnames" argument, column names are printed as the first row.
//
//	query error=(substring)
//	<T-SQL query>
//	----
//
// Executes a T-SQL query and asserts that it produces an error whose
// message contains the given substring.
package logictest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// Runner executes datadriven TDS logic tests against a running TDS
// server. Create one via NewRunner, which performs the PRELOGIN and
// LOGIN7 handshake. Use Run to process a single test file.
type Runner struct {
	t    *testing.T
	conn net.Conn
	pr   *tdswire.PacketReader
	pw   *tdswire.PacketWriter
}

// NewRunner dials the TDS server at addr, performs the PRELOGIN and
// LOGIN7 handshake, and returns a ready-to-use Runner. The caller
// must call Close when done.
func NewRunner(t *testing.T, addr string) *Runner {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err, "dialing TDS server at %s", addr)

	r := &Runner{
		t:    t,
		conn: conn,
		pr:   tdswire.NewPacketReader(conn),
		pw:   tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize),
	}

	r.prelogin(t)
	r.login(t)
	return r
}

// Close closes the underlying TCP connection.
func (r *Runner) Close() {
	r.conn.Close()
}

// Run processes a single datadriven test file.
func (r *Runner) Run(t *testing.T, path string) {
	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "exec":
			return r.runExec(t, td)
		case "query":
			return r.runQuery(t, td)
		default:
			t.Fatalf("unknown directive %q at %s", td.Cmd, td.Pos)
			return ""
		}
	})
}

// runExec handles the "exec" directive.
func (r *Runner) runExec(t *testing.T, td *datadriven.TestData) string {
	t.Helper()
	expectErr := getErrorArg(td)
	result := r.sendQuery(t, td.Input)
	checkError(t, td, result, expectErr)
	return ""
}

// runQuery handles the "query" directive.
func (r *Runner) runQuery(t *testing.T, td *datadriven.TestData) string {
	t.Helper()
	expectErr := getErrorArg(td)
	showColNames := hasArg(td, "colnames")
	result := r.sendQuery(t, td.Input)
	if expectErr != "" {
		checkError(t, td, result, expectErr)
		return ""
	}
	if result.Error != nil {
		t.Fatalf("%s: unexpected error: %s", td.Pos, result.Error.Message)
	}
	return formatResult(result, showColNames)
}

// getErrorArg returns the error= argument value, or empty string if
// no error argument is present.
func getErrorArg(td *datadriven.TestData) string {
	for _, arg := range td.CmdArgs {
		if arg.Key == "error" && len(arg.Vals) > 0 {
			return arg.Vals[0]
		}
	}
	return ""
}

// checkError verifies that the result matches the expected error state.
func checkError(t *testing.T, td *datadriven.TestData, result parsedResult, expectErr string) {
	t.Helper()
	if expectErr != "" {
		if result.Error == nil {
			t.Fatalf("%s: expected error containing %q but got success", td.Pos, expectErr)
		}
		if !strings.Contains(result.Error.Message, expectErr) {
			t.Fatalf("%s: expected error containing %q, got %q",
				td.Pos, expectErr, result.Error.Message)
		}
		return
	}
	if result.Error != nil {
		t.Fatalf("%s: unexpected error: %s", td.Pos, result.Error.Message)
	}
}

// parsedResult holds the parsed tokens from a TDS response.
type parsedResult struct {
	ColMeta *tdswire.ColMetaData
	Rows    []tdswire.Row
	Done    *tdswire.DoneToken
	Error   *tdswire.ErrorToken
}

// sendQuery sends a SQL_BATCH and parses the full token stream response.
func (r *Runner) sendQuery(t *testing.T, sql string) parsedResult {
	t.Helper()

	sqlBytes := encodeUTF16LE(sql)

	// ALL_HEADERS: total_len(4) + header_len(4) + type(2) + txn_desc(8).
	allHeadersLen := uint32(4 + 4 + 2 + 8)
	headerBuf := make([]byte, allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[0:4], allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[4:8], 4+2+8)
	binary.LittleEndian.PutUint16(headerBuf[8:10], 2)
	binary.LittleEndian.PutUint32(headerBuf[14:18], 1)

	payload := append(headerBuf, sqlBytes...)
	require.NoError(t,
		r.pw.WriteMessage(tdswire.PacketTypeSQLBatch, payload),
		"writing SQL_BATCH")

	pktType, resp, err := r.pr.ReadMessage()
	require.NoError(t, err, "reading SQL_BATCH response")
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType)

	return parseTokens(t, resp)
}

// parseTokens reads through a TDS token stream and returns the parsed
// result containing column metadata, rows, and any error.
func parseTokens(t *testing.T, data []byte) parsedResult {
	t.Helper()
	var result parsedResult
	rd := bytes.NewReader(data)
	tr := tdswire.NewTokenReader(rd)

	for {
		tok, err := tr.PeekToken()
		if err != nil {
			break
		}
		switch tok {
		case tdswire.TokenColMetaData:
			md, err := tr.ReadColMetaData()
			require.NoError(t, err, "reading ColMetaData")
			result.ColMeta = &md
			// Consume subsequent ROW tokens.
			for {
				next, err := tr.PeekToken()
				if err != nil || next != tdswire.TokenRow {
					break
				}
				row, err := tr.ReadRow(md)
				require.NoError(t, err, "reading Row")
				result.Rows = append(result.Rows, row)
			}
		case tdswire.TokenDone, tdswire.TokenDoneProc, tdswire.TokenDoneInProc:
			d, err := tr.ReadDone(tok)
			require.NoError(t, err, "reading Done")
			result.Done = &d
		case tdswire.TokenError:
			e, err := tr.ReadError(tok)
			require.NoError(t, err, "reading Error")
			result.Error = &e
		case tdswire.TokenInfo:
			_, err := tr.ReadError(tok)
			require.NoError(t, err, "reading Info")
		case tdswire.TokenEnvChange:
			_, err := tr.ReadEnvChange()
			require.NoError(t, err, "reading EnvChange")
		case tdswire.TokenLoginAck:
			_, err := tr.ReadLoginAck()
			require.NoError(t, err, "reading LoginAck")
		default:
			return result
		}
	}
	return result
}

// formatResult produces human-readable output from a TDS query result.
// Columns are separated by two spaces. If showColNames is true, the
// first line contains column names.
func formatResult(result parsedResult, showColNames bool) string {
	if result.ColMeta == nil {
		return ""
	}

	var lines []string
	if showColNames {
		names := make([]string, len(result.ColMeta.Columns))
		for i, col := range result.ColMeta.Columns {
			names[i] = col.ColName
		}
		lines = append(lines, strings.Join(names, "  "))
	}

	for _, row := range result.Rows {
		vals := make([]string, len(row.Values))
		for i, v := range row.Values {
			if v == nil {
				vals[i] = "NULL"
			} else {
				vals[i] = decodeTypedValue(v, result.ColMeta.Columns[i].TypeInfo)
			}
		}
		lines = append(lines, strings.Join(vals, "  "))
	}

	return strings.Join(lines, "\n")
}

// hasArg returns true if the given key appears in td.CmdArgs.
func hasArg(td *datadriven.TestData, key string) bool {
	for _, arg := range td.CmdArgs {
		if arg.Key == key {
			return true
		}
	}
	return false
}

// prelogin sends the TDS PRELOGIN handshake.
func (r *Runner) prelogin(t *testing.T) {
	t.Helper()
	msg := &tdswire.PreLoginMsg{
		Options: []tdswire.PreLoginOption{
			{
				Token: tdswire.PreLoginVersion,
				Data: tdswire.EncodeVersionData(tdswire.PreLoginVersionData{
					Major: 16, Minor: 0, Build: 0, SubBuild: 0,
				}),
			},
			{
				Token: tdswire.PreLoginEncryption,
				Data:  []byte{byte(tdswire.EncryptNotSup)},
			},
		},
	}
	require.NoError(t,
		r.pw.WriteMessage(tdswire.PacketTypePreLogin, tdswire.EncodePreLogin(msg)))
	pktType, _, err := r.pr.ReadMessage()
	require.NoError(t, err, "reading PRELOGIN response")
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType)
}

// login sends the TDS LOGIN7 handshake.
func (r *Runner) login(t *testing.T) {
	t.Helper()
	require.NoError(t,
		r.pw.WriteMessage(tdswire.PacketTypeLogin7,
			buildLogin7("", "", "defaultdb")))
	pktType, payload, err := r.pr.ReadMessage()
	require.NoError(t, err, "reading LOGIN7 response")
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType)
	result := parseTokens(t, payload)
	require.Nil(t, result.Error, "LOGIN7 failed")
}

// buildLogin7 constructs a minimal LOGIN7 packet.
func buildLogin7(username, password, database string) []byte {
	type field struct {
		value string
		pos   int
	}
	fields := []field{
		{value: "testhost", pos: 36},
		{value: username, pos: 40},
		{value: password, pos: 44},
		{value: "logictest", pos: 48},
		{value: "localhost", pos: 52},
		{value: "gotest", pos: 60},
		{value: "", pos: 64},
		{value: database, pos: 68},
	}

	fixedLen := 94
	buf := make([]byte, fixedLen)
	binary.LittleEndian.PutUint32(buf[4:8], 0x74000004) // TDS 7.4
	binary.LittleEndian.PutUint32(buf[8:12], 4096)      // packet size

	offset := fixedLen
	var varData []byte
	for _, f := range fields {
		encoded := encodeUTF16LE(f.value)
		if f.pos == 44 {
			tdswire.ObfuscatePassword(encoded)
		}
		charLen := len(encoded) / 2
		binary.LittleEndian.PutUint16(buf[f.pos:f.pos+2], uint16(offset))
		binary.LittleEndian.PutUint16(buf[f.pos+2:f.pos+4], uint16(charLen))
		varData = append(varData, encoded...)
		offset += len(encoded)
	}
	// Unused extension field at position 56.
	binary.LittleEndian.PutUint16(buf[56:58], uint16(offset))
	binary.LittleEndian.PutUint16(buf[58:60], 0)

	result := append(buf, varData...)
	binary.LittleEndian.PutUint32(result[0:4], uint32(len(result)))
	return result
}

// encodeUTF16LE encodes a Go string to little-endian UTF-16 bytes.
func encodeUTF16LE(s string) []byte {
	u16 := utf16.Encode([]rune(s))
	b := make([]byte, len(u16)*2)
	for i, v := range u16 {
		binary.LittleEndian.PutUint16(b[i*2:i*2+2], v)
	}
	return b
}

// decodeTypedValue converts raw TDS row value bytes to a display
// string using the column's type information for correct decoding.
func decodeTypedValue(b []byte, ti tdswire.TypeInfo) string {
	if len(b) == 0 {
		return ""
	}
	switch ti.TypeID {
	// Fixed-length integer types.
	case tdswire.TypeInt1:
		return fmt.Sprintf("%d", b[0])
	case tdswire.TypeInt2:
		return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(b)))
	case tdswire.TypeInt4:
		return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(b)))
	case tdswire.TypeInt8:
		return fmt.Sprintf("%d", int64(binary.LittleEndian.Uint64(b)))

	// Nullable integer (IntN) — length determines width.
	case tdswire.TypeIntN:
		switch len(b) {
		case 1:
			return fmt.Sprintf("%d", b[0])
		case 2:
			return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(b)))
		case 4:
			return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(b)))
		case 8:
			return fmt.Sprintf("%d", int64(binary.LittleEndian.Uint64(b)))
		}

	// Bit types.
	case tdswire.TypeBit, tdswire.TypeBitN:
		if b[0] != 0 {
			return "true"
		}
		return "false"

	// Fixed-length float types.
	case tdswire.TypeFloat4:
		bits := binary.LittleEndian.Uint32(b)
		return fmt.Sprintf("%g", math.Float32frombits(bits))
	case tdswire.TypeFloat8:
		bits := binary.LittleEndian.Uint64(b)
		return fmt.Sprintf("%g", math.Float64frombits(bits))
	case tdswire.TypeFloatN:
		if len(b) == 4 {
			bits := binary.LittleEndian.Uint32(b)
			return fmt.Sprintf("%g", math.Float32frombits(bits))
		}
		bits := binary.LittleEndian.Uint64(b)
		return fmt.Sprintf("%g", math.Float64frombits(bits))

	// NVARCHAR/NCHAR — UTF-16LE encoded.
	case tdswire.TypeNVarChar, tdswire.TypeNChar:
		return decodeUTF16LE(b)

	// VARCHAR/CHAR — plain bytes.
	case tdswire.TypeBigVarChar, tdswire.TypeBigChar:
		return string(b)
	}

	// Fallback: try UTF-16LE heuristic, then raw bytes.
	return decodeUTF16LEOrRaw(b)
}

// decodeUTF16LE decodes a UTF-16LE byte slice to a Go string.
func decodeUTF16LE(b []byte) string {
	if len(b) < 2 || len(b)%2 != 0 {
		return string(b)
	}
	u16 := make([]uint16, len(b)/2)
	for i := range u16 {
		u16[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}
	return string(utf16.Decode(u16))
}

// decodeUTF16LEOrRaw tries UTF-16LE decoding if the byte slice looks
// like it, otherwise returns the raw bytes as a string.
func decodeUTF16LEOrRaw(b []byte) string {
	if len(b)%2 == 0 && len(b) >= 2 && b[1] == 0x00 {
		return decodeUTF16LE(b)
	}
	return string(b)
}
