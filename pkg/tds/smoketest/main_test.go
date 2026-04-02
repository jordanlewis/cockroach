// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package smoketest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/tds"
	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	m.Run()
}

// TestTDSSmokeEndToEnd starts a single-node CockroachDB cluster, creates
// a TDS server pointed at the cluster's internal SQL executor, and runs
// a series of operations through the TDS protocol to verify end-to-end
// correctness: USE, CREATE TABLE, INSERT, SELECT, SET commands, and
// @@VERSION.
func TestTDSSmokeEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Start a single-node CockroachDB test server.
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	// Get the internal DB from the test server.
	internalDB := srv.ApplicationLayer().InternalDB().(isql.DB)

	// Create a TDS server on a random port, backed by the test server's
	// internal executor.
	tdsServer := tds.NewServer(tds.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		DefaultDatabase: "defaultdb",
		DB:              internalDB,
	})
	if err := tdsServer.Start(ctx); err != nil {
		t.Fatalf("starting TDS server: %v", err)
	}
	defer tdsServer.Stop()

	tdsAddr := tdsServer.Addr().String()
	t.Logf("TDS server listening on %s", tdsAddr)

	// Connect to the TDS server.
	conn, err := net.DialTimeout("tcp", tdsAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dialing TDS server: %v", err)
	}
	defer conn.Close()

	// -- PRELOGIN handshake --
	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	preLogin := &tdswire.PreLoginMsg{
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
	if err := pw.WriteMessage(tdswire.PacketTypePreLogin, tdswire.EncodePreLogin(preLogin)); err != nil {
		t.Fatalf("writing PRELOGIN: %v", err)
	}

	pktType, payload, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading PRELOGIN response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT for PRELOGIN, got %s", pktType)
	}
	preLoginResp, err := tdswire.DecodePreLogin(payload)
	if err != nil {
		t.Fatalf("decoding PRELOGIN response: %v", err)
	}
	t.Logf("PRELOGIN: server responded with %d options", len(preLoginResp.Options))

	// -- LOGIN7 handshake --
	login7Payload := buildLogin7("", "", "defaultdb")
	if err := pw.WriteMessage(tdswire.PacketTypeLogin7, login7Payload); err != nil {
		t.Fatalf("writing LOGIN7: %v", err)
	}

	pktType, payload, err = pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading LOGIN7 response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT for LOGIN7, got %s", pktType)
	}

	loginResult := parseTokenStream(t, payload)
	if loginResult.LoginAck == nil {
		if loginResult.Error != nil {
			t.Fatalf("LOGIN7 failed: %s", loginResult.Error.Message)
		}
		t.Fatal("LOGIN7 response missing LOGINACK token")
	}
	t.Logf("LOGIN7: authenticated, program=%s", loginResult.LoginAck.ProgName)

	// From here on, we reuse the pr/pw for SQL batches.

	// -- Test 1: USE defaultdb --
	t.Run("UseDatabase", func(t *testing.T) {
		resp := sendBatch(t, pr, pw, "USE defaultdb")
		result := parseTokenStream(t, resp)
		if len(result.EnvChanges) == 0 {
			// USE defaultdb might not produce an ENVCHANGE if already in
			// defaultdb, but should not produce an error.
			if result.Error != nil {
				t.Fatalf("USE defaultdb failed: %s", result.Error.Message)
			}
		}
		if result.Done == nil {
			t.Fatal("USE defaultdb: missing DONE token")
		}
		t.Log("USE defaultdb: OK")
	})

	// -- Test 2: CREATE TABLE --
	t.Run("CreateTable", func(t *testing.T) {
		resp := sendBatch(t, pr, pw,
			"CREATE TABLE test_users (id INT NOT NULL, name VARCHAR(100), age INT)")
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			t.Fatalf("CREATE TABLE failed: %s", result.Error.Message)
		}
		if result.Done == nil {
			t.Fatal("CREATE TABLE: missing DONE token")
		}
		t.Log("CREATE TABLE test_users: OK")
	})

	// -- Test 3: INSERT --
	t.Run("Insert", func(t *testing.T) {
		resp := sendBatch(t, pr, pw,
			"INSERT INTO test_users VALUES (1, 'Alice', 30)")
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			t.Fatalf("INSERT failed: %s", result.Error.Message)
		}
		if result.Done == nil {
			t.Fatal("INSERT: missing DONE token")
		}
		t.Logf("INSERT: OK, rows affected status=0x%04X", result.Done.Status)
	})

	// -- Test 4: SELECT with verification --
	t.Run("Select", func(t *testing.T) {
		resp := sendBatch(t, pr, pw,
			"SELECT TOP 1 id, name, age FROM test_users WHERE id = 1")
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			t.Fatalf("SELECT failed: %s", result.Error.Message)
		}
		if result.ColMeta == nil {
			t.Fatal("SELECT: missing COLMETADATA token")
		}
		if len(result.ColMeta.Columns) != 3 {
			t.Fatalf("SELECT: expected 3 columns, got %d", len(result.ColMeta.Columns))
		}

		// Verify column names.
		expectedCols := []string{"id", "name", "age"}
		for i, col := range result.ColMeta.Columns {
			if !strings.EqualFold(col.ColName, expectedCols[i]) {
				t.Errorf("column %d: expected name %q, got %q",
					i, expectedCols[i], col.ColName)
			}
		}

		// Verify we got at least 1 row.
		if len(result.Rows) == 0 {
			t.Fatal("SELECT: expected at least 1 row, got 0")
		}

		t.Logf("SELECT: returned %d columns, %d rows",
			len(result.ColMeta.Columns), len(result.Rows))

		if result.Done == nil {
			t.Fatal("SELECT: missing DONE token")
		}
	})

	// -- Test 5: SET commands (common Sybase driver init) --
	t.Run("SetCommands", func(t *testing.T) {
		setCmds := []string{
			"SET ANSI_NULLS ON",
			"SET QUOTED_IDENTIFIER ON",
			"SET ANSI_PADDING ON",
			"SET CONCAT_NULL_YIELDS_NULL ON",
			"SET TEXTSIZE 2147483647",
		}
		for _, cmd := range setCmds {
			resp := sendBatch(t, pr, pw, cmd)
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Errorf("SET command %q failed: %s", cmd, result.Error.Message)
			}
			if result.Done == nil {
				t.Errorf("SET command %q: missing DONE token", cmd)
			}
		}
		t.Log("SET commands: all OK")
	})

	// -- Test 6: @@VERSION --
	t.Run("Version", func(t *testing.T) {
		resp := sendBatch(t, pr, pw, "SELECT @@VERSION")
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			t.Fatalf("SELECT @@VERSION failed: %s", result.Error.Message)
		}
		if result.ColMeta == nil {
			t.Fatal("SELECT @@VERSION: missing COLMETADATA")
		}
		if len(result.Rows) == 0 {
			t.Fatal("SELECT @@VERSION: expected at least 1 row")
		}
		// The version string may be returned as NVarChar (UTF-16LE) on the
		// wire. Try to decode it if it looks like UTF-16LE.
		rawVal := result.Rows[0].Values[0]
		versionStr := decodeRowString(rawVal)
		t.Logf("@@VERSION: %s", versionStr)
		if !strings.Contains(versionStr, "CockroachDB") {
			t.Errorf("expected version to contain 'CockroachDB', got %q", versionStr)
		}
	})
}

// -------------------------------------------------------
// Helpers
// -------------------------------------------------------

// buildLogin7 constructs a LOGIN7 packet payload.
func buildLogin7(username, password, database string) []byte {
	type field struct {
		value string
		pos   int
	}
	fields := []field{
		{value: "testhost", pos: 36},
		{value: username, pos: 40},
		{value: password, pos: 44},
		{value: "smoketest", pos: 48},
		{value: "localhost", pos: 52},
		// pos 56 unused/extension
		{value: "gotest", pos: 60},
		{value: "", pos: 64},
		{value: database, pos: 68},
	}

	fixedLen := 94
	buf := make([]byte, fixedLen)

	// TDS version 7.4.
	binary.LittleEndian.PutUint32(buf[4:8], 0x74000004)
	// Packet size.
	binary.LittleEndian.PutUint32(buf[8:12], 4096)

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

// decodeRowString decodes a raw TDS row value into a Go string. If the
// value looks like UTF-16LE (even length and contains null bytes
// interleaved with ASCII), it is decoded as such. Otherwise, it is
// returned as a plain string.
func decodeRowString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// Heuristic: if the length is even and the second byte is 0x00
	// (typical for ASCII encoded as UTF-16LE), decode as UTF-16LE.
	if len(b)%2 == 0 && len(b) >= 2 && b[1] == 0x00 {
		u16 := make([]uint16, len(b)/2)
		for i := range u16 {
			u16[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
		}
		return string(utf16.Decode(u16))
	}
	return string(b)
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

// sendBatch sends a SQL_BATCH TDS packet and reads the response.
func sendBatch(
	t *testing.T, pr *tdswire.PacketReader, pw *tdswire.PacketWriter, sql string,
) []byte {
	t.Helper()

	sqlBytes := encodeUTF16LE(sql)

	// Minimal ALL_HEADERS: total_len(4) + header_len(4) + type(2) + txn_desc(8).
	allHeadersLen := uint32(4 + 4 + 2 + 8)
	headerBuf := make([]byte, allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[0:4], allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[4:8], 4+2+8)
	binary.LittleEndian.PutUint16(headerBuf[8:10], 2)
	binary.LittleEndian.PutUint32(headerBuf[14:18], 1)

	payload := append(headerBuf, sqlBytes...)

	if err := pw.WriteMessage(tdswire.PacketTypeSQLBatch, payload); err != nil {
		t.Fatalf("writing SQL_BATCH (%s): %v", sql, err)
	}

	pktType, resp, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading SQL_BATCH response (%s): %v", sql, err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT for SQL_BATCH (%s), got %s", sql, pktType)
	}
	return resp
}

// parsedResult holds the parsed tokens from a TDS response.
type parsedResult struct {
	Tokens     []byte
	ColMeta    *tdswire.ColMetaData
	Rows       []tdswire.Row
	Done       *tdswire.DoneToken
	Error      *tdswire.ErrorToken
	EnvChanges []tdswire.EnvChangeToken
	LoginAck   *tdswire.LoginAckToken
}

// parseTokenStream parses a TDS token stream into structured result.
func parseTokenStream(t *testing.T, data []byte) parsedResult {
	t.Helper()
	var result parsedResult
	r := bytes.NewReader(data)
	tr := tdswire.NewTokenReader(r)

	for {
		tok, err := tr.PeekToken()
		if err != nil {
			break
		}
		result.Tokens = append(result.Tokens, tok)

		switch tok {
		case tdswire.TokenLoginAck:
			la, err := tr.ReadLoginAck()
			if err != nil {
				t.Fatalf("reading login ack: %v", err)
			}
			result.LoginAck = &la
		case tdswire.TokenEnvChange:
			ec, err := tr.ReadEnvChange()
			if err != nil {
				t.Fatalf("reading envchange: %v", err)
			}
			result.EnvChanges = append(result.EnvChanges, ec)
		case tdswire.TokenDone, tdswire.TokenDoneProc, tdswire.TokenDoneInProc:
			d, err := tr.ReadDone(tok)
			if err != nil {
				t.Fatalf("reading done: %v", err)
			}
			result.Done = &d
		case tdswire.TokenError, tdswire.TokenInfo:
			e, err := tr.ReadError(tok)
			if err != nil {
				t.Fatalf("reading error/info: %v", err)
			}
			if tok == tdswire.TokenError {
				result.Error = &e
			}
		case tdswire.TokenColMetaData:
			md, err := tr.ReadColMetaData()
			if err != nil {
				t.Fatalf("reading colmetadata: %v", err)
			}
			result.ColMeta = &md

			// Read subsequent ROW tokens.
			for {
				nextTok, err := tr.PeekToken()
				if err != nil {
					return result
				}
				if nextTok != tdswire.TokenRow {
					result.Tokens = append(result.Tokens, nextTok)
					switch nextTok {
					case tdswire.TokenDone, tdswire.TokenDoneProc, tdswire.TokenDoneInProc:
						d, err := tr.ReadDone(nextTok)
						if err != nil {
							t.Fatalf("reading done after rows: %v", err)
						}
						result.Done = &d
					default:
						return result
					}
					break
				}
				result.Tokens = append(result.Tokens, nextTok)
				row, err := tr.ReadRow(md)
				if err != nil {
					t.Fatalf("reading row: %v", err)
				}
				result.Rows = append(result.Rows, row)
			}
		default:
			return result
		}
	}
	return result
}

// TestTDSBuiltinStartup verifies that the TDS server is wired into
// CockroachDB's real server startup path (maybeStartTDS in server_sql.go),
// not just manually instantiated in a test harness. This test:
//  1. Overrides the server.tds.enabled and server.tds.port cluster settings
//  2. Starts a CockroachDB test server with those settings
//  3. Connects to the TDS port that the server started automatically
//  4. Runs a full PRELOGIN → LOGIN7 → SQL_BATCH flow
func TestTDSBuiltinStartup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Find a free port for the TDS server to use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tdsPort := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	// Create cluster settings with TDS enabled on the chosen port.
	st := cluster.MakeTestingClusterSettings()

	enabledSetting, ok, _ := settings.LookupForLocalAccess(
		"server.tds.enabled", true, /* forSystemTenant */
	)
	require.True(t, ok, "server.tds.enabled setting not found")
	enabledSetting.(*settings.BoolSetting).Override(ctx, &st.SV, true)

	portSetting, ok, _ := settings.LookupForLocalAccess(
		"server.tds.port", true, /* forSystemTenant */
	)
	require.True(t, ok, "server.tds.port setting not found")
	portSetting.(*settings.IntSetting).Override(ctx, &st.SV, int64(tdsPort))

	// Start a CockroachDB server — this exercises the real maybeStartTDS
	// code path in server_sql.go, not a test-only TDS server.
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: st,
	})
	defer srv.Stopper().Stop(ctx)

	tdsAddr := fmt.Sprintf("127.0.0.1:%d", tdsPort)
	t.Logf("expecting TDS server at %s (started by CRDB, not test harness)", tdsAddr)

	// Connect to the TDS server that CRDB started.
	conn, err := net.DialTimeout("tcp", tdsAddr, 5*time.Second)
	require.NoError(t, err, "TDS server should be listening on port %d", tdsPort)
	defer conn.Close()

	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	// -- PRELOGIN --
	preLogin := &tdswire.PreLoginMsg{
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
		pw.WriteMessage(tdswire.PacketTypePreLogin, tdswire.EncodePreLogin(preLogin)))

	pktType, payload, err := pr.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType,
		"expected TABULAR_RESULT for PRELOGIN response")
	preLoginResp, err := tdswire.DecodePreLogin(payload)
	require.NoError(t, err)
	require.NotEmpty(t, preLoginResp.Options, "PRELOGIN response should have options")

	// -- LOGIN7 --
	login7Payload := buildLogin7("", "", "defaultdb")
	require.NoError(t, pw.WriteMessage(tdswire.PacketTypeLogin7, login7Payload))

	pktType, payload, err = pr.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType)

	loginResult := parseTokenStream(t, payload)
	require.NotNil(t, loginResult.LoginAck, "LOGIN7 should produce a LOGINACK token")
	require.Equal(t, "CockroachDB", loginResult.LoginAck.ProgName)

	// -- SQL_BATCH: SELECT 1 --
	resp := sendBatch(t, pr, pw, "SELECT 1 AS val")
	result := parseTokenStream(t, resp)
	require.Nil(t, result.Error, "SELECT 1 should succeed")
	require.NotNil(t, result.ColMeta, "expected COLMETADATA")
	require.Len(t, result.ColMeta.Columns, 1)
	require.Len(t, result.Rows, 1)

	// -- SQL_BATCH: @@VERSION --
	resp = sendBatch(t, pr, pw, "SELECT @@VERSION")
	result = parseTokenStream(t, resp)
	require.Nil(t, result.Error)
	require.NotEmpty(t, result.Rows)
	versionStr := decodeRowString(result.Rows[0].Values[0])
	require.Contains(t, versionStr, "CockroachDB",
		"@@VERSION from builtin TDS server should contain CockroachDB")
	t.Logf("builtin TDS startup verified: @@VERSION = %s", versionStr)
}
