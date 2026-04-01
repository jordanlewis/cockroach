// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tds

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
)

// testServer creates a TDS server listening on a random port and returns
// the server and its address. The caller must call Stop.
func testServer(t *testing.T, cfg ServerConfig) *Server {
	t.Helper()
	cfg.ListenAddr = "127.0.0.1:0"
	s := NewServer(cfg)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

// dialServer dials the test server and returns the connection.
func dialServer(t *testing.T, s *Server) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", s.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	return conn
}

// doPreLogin performs the PRELOGIN handshake on the given connection
// and returns the server's decoded PRELOGIN response.
func doPreLogin(t *testing.T, conn net.Conn) *tdswire.PreLoginMsg {
	t.Helper()
	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	// Send client PRELOGIN.
	clientPreLogin := &tdswire.PreLoginMsg{
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
	payload := tdswire.EncodePreLogin(clientPreLogin)
	if err := pw.WriteMessage(tdswire.PacketTypePreLogin, payload); err != nil {
		t.Fatalf("writing client PRELOGIN: %v", err)
	}

	// Read server PRELOGIN response.
	pktType, respPayload, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading server PRELOGIN: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT, got %s", pktType)
	}

	resp, err := tdswire.DecodePreLogin(respPayload)
	if err != nil {
		t.Fatalf("decoding server PRELOGIN: %v", err)
	}
	return resp
}

// buildLogin7Payload constructs a LOGIN7 message payload with the given
// username, password, and database.
func buildLogin7Payload(username, password, database string) []byte {
	type field struct {
		value string
		pos   int
	}
	fields := []field{
		{value: "testhost", pos: 36},   // hostname
		{value: username, pos: 40},     // username
		{value: password, pos: 44},     // password
		{value: "testapp", pos: 48},    // appname
		{value: "localhost", pos: 52},  // servername
		// pos 56 is unused/extension
		{value: "gotest", pos: 60},     // libraryname
		{value: "", pos: 64},           // language
		{value: database, pos: 68},     // database
	}

	// Fixed header is 94 bytes.
	fixedLen := 94
	buf := make([]byte, fixedLen)

	// TDS version (7.4).
	binary.LittleEndian.PutUint32(buf[4:8], 0x74000004)
	// Packet size.
	binary.LittleEndian.PutUint32(buf[8:12], 4096)

	// Calculate variable-length data offsets.
	offset := fixedLen
	var varData []byte
	for _, f := range fields {
		encoded := encodeUTF16LETest(f.value)
		if f.pos == 44 {
			// Password needs obfuscation.
			tdswire.ObfuscatePassword(encoded)
		}
		charLen := len(encoded) / 2
		binary.LittleEndian.PutUint16(buf[f.pos:f.pos+2], uint16(offset))
		binary.LittleEndian.PutUint16(buf[f.pos+2:f.pos+4], uint16(charLen))
		varData = append(varData, encoded...)
		offset += len(encoded)
	}

	// Fill unused extension offset/length at position 56.
	binary.LittleEndian.PutUint16(buf[56:58], uint16(offset))
	binary.LittleEndian.PutUint16(buf[58:60], 0)

	result := append(buf, varData...)
	// Set total length.
	binary.LittleEndian.PutUint32(result[0:4], uint32(len(result)))
	return result
}

// encodeUTF16LETest encodes a Go string to UTF-16LE bytes (test helper).
func encodeUTF16LETest(s string) []byte {
	u16 := utf16.Encode([]rune(s))
	b := make([]byte, len(u16)*2)
	for i, v := range u16 {
		binary.LittleEndian.PutUint16(b[i*2:i*2+2], v)
	}
	return b
}

// doLogin7 sends a LOGIN7 packet and reads the response tokens.
func doLogin7(t *testing.T, conn net.Conn, username, password, database string) []byte {
	t.Helper()
	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	payload := buildLogin7Payload(username, password, database)
	if err := pw.WriteMessage(tdswire.PacketTypeLogin7, payload); err != nil {
		t.Fatalf("writing LOGIN7: %v", err)
	}

	pktType, resp, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading LOGIN7 response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT, got %s", pktType)
	}
	return resp
}

// sendSQLBatch sends a SQL_BATCH packet with the given SQL text and
// reads the response.
func sendSQLBatch(t *testing.T, conn net.Conn, sql string) []byte {
	t.Helper()
	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	// Build SQL_BATCH payload: ALL_HEADERS (minimal) + UTF-16LE SQL.
	sqlBytes := encodeUTF16LETest(sql)

	// Minimal ALL_HEADERS: just the total length (4 bytes) indicating
	// no additional headers beyond the length field itself.
	allHeadersLen := uint32(4 + 4 + 2 + 8) // total_len(4) + header_len(4) + header_type(2) + txn_descriptor(8)
	headerBuf := make([]byte, allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[0:4], allHeadersLen)
	// Individual header: length = 4+2+8 = 14
	binary.LittleEndian.PutUint32(headerBuf[4:8], 4+2+8)
	// Header type: Transaction descriptor = 2
	binary.LittleEndian.PutUint16(headerBuf[8:10], 2)
	// Transaction descriptor: 0 (no active transaction)
	// Outstanding request count: 1
	binary.LittleEndian.PutUint32(headerBuf[14:18], 1)

	payload := append(headerBuf, sqlBytes...)

	if err := pw.WriteMessage(tdswire.PacketTypeSQLBatch, payload); err != nil {
		t.Fatalf("writing SQL_BATCH: %v", err)
	}

	pktType, resp, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading SQL_BATCH response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT, got %s", pktType)
	}
	return resp
}

// parseTokenTypes extracts the token type bytes from a TDS token stream.
// This is a simplified parser that understands enough of the token format
// to identify token types.
func parseTokenTypes(data []byte) []byte {
	var tokens []byte
	r := bytes.NewReader(data)
	tr := tdswire.NewTokenReader(r)

	for {
		tok, err := tr.PeekToken()
		if err != nil {
			break
		}
		tokens = append(tokens, tok)

		// Consume the rest of the token based on its type.
		switch tok {
		case tdswire.TokenLoginAck:
			if _, err := tr.ReadLoginAck(); err != nil {
				return tokens
			}
		case tdswire.TokenEnvChange:
			if _, err := tr.ReadEnvChange(); err != nil {
				return tokens
			}
		case tdswire.TokenDone, tdswire.TokenDoneProc, tdswire.TokenDoneInProc:
			if _, err := tr.ReadDone(tok); err != nil {
				return tokens
			}
		case tdswire.TokenError, tdswire.TokenInfo:
			if _, err := tr.ReadError(tok); err != nil {
				return tokens
			}
		case tdswire.TokenColMetaData:
			if _, err := tr.ReadColMetaData(); err != nil {
				return tokens
			}
		case tdswire.TokenRow:
			// We need column metadata to read rows properly; for token type
			// scanning, skip by reading raw bytes. In practice this won't
			// be reached without preceding COLMETADATA.
			return tokens
		default:
			return tokens
		}
	}
	return tokens
}

func TestPreLoginHandshake(t *testing.T) {
	s := testServer(t, ServerConfig{})
	conn := dialServer(t, s)
	defer conn.Close()

	resp := doPreLogin(t, conn)

	// Check that the server responds with VERSION and ENCRYPTION options.
	var hasVersion, hasEncryption bool
	for _, opt := range resp.Options {
		switch opt.Token {
		case tdswire.PreLoginVersion:
			hasVersion = true
			v, err := tdswire.DecodeVersionData(opt.Data)
			if err != nil {
				t.Fatalf("decoding version: %v", err)
			}
			if v.Major != 16 {
				t.Errorf("expected major version 16, got %d", v.Major)
			}
		case tdswire.PreLoginEncryption:
			hasEncryption = true
			if len(opt.Data) != 1 || tdswire.EncryptionLevel(opt.Data[0]) != tdswire.EncryptNotSup {
				t.Errorf("expected EncryptNotSup, got %v", opt.Data)
			}
		}
	}
	if !hasVersion {
		t.Error("server PRELOGIN response missing VERSION option")
	}
	if !hasEncryption {
		t.Error("server PRELOGIN response missing ENCRYPTION option")
	}
}

func TestLogin7Success(t *testing.T) {
	s := testServer(t, ServerConfig{
		Username: "sa",
		Password: "secret",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	resp := doLogin7(t, conn, "sa", "secret", "testdb")

	tokens := parseTokenTypes(resp)
	// Expect LOGINACK, ENVCHANGE, DONE.
	expected := []byte{tdswire.TokenLoginAck, tdswire.TokenEnvChange, tdswire.TokenDone}
	if !bytes.Equal(tokens, expected) {
		t.Errorf("expected tokens %v, got %v", expected, tokens)
	}
}

func TestLogin7Failure(t *testing.T) {
	s := testServer(t, ServerConfig{
		Username: "sa",
		Password: "secret",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	resp := doLogin7(t, conn, "sa", "wrong", "testdb")

	tokens := parseTokenTypes(resp)
	// Expect ERROR, DONE.
	expected := []byte{tdswire.TokenError, tdswire.TokenDone}
	if !bytes.Equal(tokens, expected) {
		t.Errorf("expected tokens %v, got %v", expected, tokens)
	}

	// Verify error message contains login failure.
	r := bytes.NewReader(resp)
	tr := tdswire.NewTokenReader(r)
	tok, _ := tr.PeekToken()
	if tok != tdswire.TokenError {
		t.Fatalf("expected TokenError, got 0x%02X", tok)
	}
	errTok, err := tr.ReadError(tok)
	if err != nil {
		t.Fatalf("reading error token: %v", err)
	}
	if errTok.Number != 18456 {
		t.Errorf("expected error 18456, got %d", errTok.Number)
	}
}

func TestSQLBatchWithQueryHandler(t *testing.T) {
	handler := func(ctx context.Context, query string, database string) ([]ResultColumn, [][]interface{}, error) {
		if query == "SELECT 1 AS val" {
			return []ResultColumn{
					{Name: "val", TypeID: tdswire.TypeInt4},
				}, [][]interface{}{
					{int32(1)},
				}, nil
		}
		return nil, nil, fmt.Errorf("unknown query: %s", query)
	}

	s := testServer(t, ServerConfig{QueryHandler: handler})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	doLogin7(t, conn, "", "", "")

	resp := sendSQLBatch(t, conn, "SELECT 1 AS val")

	tokens := parseTokenTypes(resp)
	// Expect COLMETADATA, ... (ROW skipped by parseTokenTypes), then more.
	// At minimum, COLMETADATA should be the first token.
	if len(tokens) == 0 {
		t.Fatal("no tokens in response")
	}
	if tokens[0] != tdswire.TokenColMetaData {
		t.Errorf("expected first token to be COLMETADATA (0x%02X), got 0x%02X",
			tdswire.TokenColMetaData, tokens[0])
	}

	// Verify we can parse the full result manually.
	r := bytes.NewReader(resp)
	tr := tdswire.NewTokenReader(r)

	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatalf("peek token: %v", err)
	}
	if tok != tdswire.TokenColMetaData {
		t.Fatalf("expected COLMETADATA, got 0x%02X", tok)
	}
	md, err := tr.ReadColMetaData()
	if err != nil {
		t.Fatalf("reading colmetadata: %v", err)
	}
	if len(md.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(md.Columns))
	}
	if md.Columns[0].ColName != "val" {
		t.Errorf("expected column name 'val', got %q", md.Columns[0].ColName)
	}

	// Read ROW.
	tok, err = tr.PeekToken()
	if err != nil {
		t.Fatalf("peek row token: %v", err)
	}
	if tok != tdswire.TokenRow {
		t.Fatalf("expected ROW, got 0x%02X", tok)
	}
	row, err := tr.ReadRow(md)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	if len(row.Values) != 1 || len(row.Values[0]) != 4 {
		t.Fatalf("unexpected row values: %v", row.Values)
	}
	gotVal := int32(binary.LittleEndian.Uint32(row.Values[0]))
	if gotVal != 1 {
		t.Errorf("expected row value 1, got %d", gotVal)
	}

	// Read DONE.
	tok, err = tr.PeekToken()
	if err != nil {
		t.Fatalf("peek done token: %v", err)
	}
	if tok != tdswire.TokenDone {
		t.Fatalf("expected DONE, got 0x%02X", tok)
	}
	done, err := tr.ReadDone(tok)
	if err != nil {
		t.Fatalf("reading done: %v", err)
	}
	if done.RowCount != 1 {
		t.Errorf("expected row count 1, got %d", done.RowCount)
	}
}

func TestConnectionDrain(t *testing.T) {
	s := testServer(t, ServerConfig{})

	// Establish a connection.
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	doLogin7(t, conn, "", "", "")

	// Drain the server.
	s.Drain()

	// The existing connection should be closed by drain.
	// Give a short time for the drain to take effect.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		t.Error("expected connection to be closed after drain")
	}

	// New connections should be rejected.
	newConn, err := net.DialTimeout("tcp", s.Addr().String(), 1*time.Second)
	if err != nil {
		// Connection refused or timeout is acceptable.
		return
	}
	defer newConn.Close()

	// If the connection was accepted, the server should close it immediately.
	newConn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err = newConn.Read(buf)
	if err == nil {
		t.Error("expected new connection to be rejected during drain")
	}
}

func TestMultipleConcurrentConnections(t *testing.T) {
	handler := func(ctx context.Context, query string, database string) ([]ResultColumn, [][]interface{}, error) {
		return []ResultColumn{
			{Name: "db", TypeID: tdswire.TypeBigVarChar, MaxLen: 128},
		}, [][]interface{}{
			{database},
		}, nil
	}

	s := testServer(t, ServerConfig{QueryHandler: handler})

	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn := dialServer(t, s)
			defer conn.Close()

			doPreLogin(t, conn)
			db := fmt.Sprintf("db%d", id)
			doLogin7(t, conn, "", "", db)

			resp := sendSQLBatch(t, conn, "SELECT DB_NAME()")

			// Verify we got a response with COLMETADATA.
			if len(resp) == 0 {
				errors <- fmt.Errorf("conn %d: empty response", id)
				return
			}
			if resp[0] != tdswire.TokenColMetaData {
				errors <- fmt.Errorf("conn %d: expected COLMETADATA, got 0x%02X", id, resp[0])
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify metrics.
	metrics := s.Metrics()
	if metrics.NewConns != numConns {
		t.Errorf("expected %d new connections, got %d", numConns, metrics.NewConns)
	}
}

func TestUseDatabase(t *testing.T) {
	s := testServer(t, ServerConfig{
		DefaultDatabase: "master",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	doLogin7(t, conn, "", "", "")

	resp := sendSQLBatch(t, conn, "USE mydb")

	// Parse the response: expect ENVCHANGE(database) + DONE.
	tokens := parseTokenTypes(resp)
	expected := []byte{tdswire.TokenEnvChange, tdswire.TokenDone}
	if !bytes.Equal(tokens, expected) {
		t.Errorf("expected tokens %v, got %v", expected, tokens)
	}

	// Verify ENVCHANGE details.
	r := bytes.NewReader(resp)
	tr := tdswire.NewTokenReader(r)
	tok, _ := tr.PeekToken()
	if tok != tdswire.TokenEnvChange {
		t.Fatalf("expected ENVCHANGE, got 0x%02X", tok)
	}
	ec, err := tr.ReadEnvChange()
	if err != nil {
		t.Fatalf("reading envchange: %v", err)
	}
	if ec.Type != tdswire.EnvDatabase {
		t.Errorf("expected EnvDatabase type, got %d", ec.Type)
	}
	if ec.NewValue != "mydb" {
		t.Errorf("expected new database 'mydb', got %q", ec.NewValue)
	}
	if ec.OldValue != "master" {
		t.Errorf("expected old database 'master', got %q", ec.OldValue)
	}
}

func TestAttentionPacket(t *testing.T) {
	s := testServer(t, ServerConfig{})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	doLogin7(t, conn, "", "", "")

	// Send an ATTENTION packet.
	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	if err := pw.WriteMessage(tdswire.PacketTypeAttention, nil); err != nil {
		t.Fatalf("writing ATTENTION: %v", err)
	}

	// Read response.
	pktType, resp, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading ATTENTION response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT, got %s", pktType)
	}

	tokens := parseTokenTypes(resp)
	if len(tokens) == 0 || tokens[0] != tdswire.TokenDone {
		t.Errorf("expected DONE token in ATTENTION response, got %v", tokens)
	}
}
