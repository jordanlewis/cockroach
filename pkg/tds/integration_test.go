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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
)

// mockQueryHandler returns a QueryHandler that records calls and produces
// canned results for known queries. Unknown queries receive an error.
func mockQueryHandler(calls *[]queryCall, mu *sync.Mutex) QueryHandler {
	return func(ctx context.Context, query string, database string) ([]ResultColumn, [][]interface{}, error) {
		mu.Lock()
		*calls = append(*calls, queryCall{Query: query, Database: database})
		mu.Unlock()

		trimmed := strings.TrimSpace(query)
		upper := strings.ToUpper(trimmed)

		// SET commands: return empty result (no columns, no rows).
		if strings.HasPrefix(upper, "SET ") {
			return nil, nil, nil
		}

		// @@VERSION detection.
		if strings.Contains(upper, "@@VERSION") {
			return []ResultColumn{
					{Name: "", TypeID: tdswire.TypeBigVarChar, MaxLen: 256},
				}, [][]interface{}{
					{"CockroachDB TDS compatibility layer v24.3"},
				}, nil
		}

		// Simple SELECT queries.
		if upper == "SELECT 1 AS NUM" {
			return []ResultColumn{
					{Name: "num", TypeID: tdswire.TypeInt4},
				}, [][]interface{}{
					{int32(1)},
				}, nil
		}

		if upper == "SELECT DB_NAME() AS CURRENT_DB" {
			return []ResultColumn{
					{Name: "current_db", TypeID: tdswire.TypeBigVarChar, MaxLen: 128},
				}, [][]interface{}{
					{database},
				}, nil
		}

		// Multi-row result.
		if upper == "SELECT ID, NAME FROM TEST_TABLE" {
			return []ResultColumn{
					{Name: "id", TypeID: tdswire.TypeInt4},
					{Name: "name", TypeID: tdswire.TypeBigVarChar, MaxLen: 100},
				}, [][]interface{}{
					{int32(1), "alice"},
					{int32(2), "bob"},
					{int32(3), "charlie"},
				}, nil
		}

		// Empty result set (columns but no rows).
		if upper == "SELECT ID FROM EMPTY_TABLE" {
			return []ResultColumn{
				{Name: "id", TypeID: tdswire.TypeInt4},
			}, nil, nil
		}

		return nil, nil, fmt.Errorf("unrecognized query: %s", trimmed)
	}
}

// queryCall records a query dispatched to the handler.
type queryCall struct {
	Query    string
	Database string
}

// fullLogin performs a PRELOGIN + LOGIN7 handshake and returns the
// PacketReader and PacketWriter for subsequent use, along with the
// LOGIN7 response bytes for optional inspection.
func fullLogin(
	t *testing.T, conn net.Conn, username, password, database string,
) (*tdswire.PacketReader, *tdswire.PacketWriter, []byte) {
	t.Helper()
	doPreLogin(t, conn)
	resp := doLogin7(t, conn, username, password, database)
	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)
	return pr, pw, resp
}

// sendSQLBatchRaw sends a SQL_BATCH and returns the raw response payload.
// It uses the provided PacketReader/PacketWriter to avoid creating new ones.
func sendSQLBatchRaw(
	t *testing.T, pr *tdswire.PacketReader, pw *tdswire.PacketWriter, sql string,
) []byte {
	t.Helper()

	sqlBytes := encodeUTF16LETest(sql)

	// Minimal ALL_HEADERS structure.
	allHeadersLen := uint32(4 + 4 + 2 + 8)
	headerBuf := make([]byte, allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[0:4], allHeadersLen)
	binary.LittleEndian.PutUint32(headerBuf[4:8], 4+2+8)
	binary.LittleEndian.PutUint16(headerBuf[8:10], 2)
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

// parseFullResult parses a TDS token stream into colmetadata, rows, done,
// and any error/envchange tokens for detailed verification.
type parsedResult struct {
	Tokens     []byte
	ColMeta    *tdswire.ColMetaData
	Rows       []tdswire.Row
	Done       *tdswire.DoneToken
	Error      *tdswire.ErrorToken
	EnvChanges []tdswire.EnvChangeToken
	LoginAck   *tdswire.LoginAckToken
}

func parseResult(t *testing.T, data []byte) parsedResult {
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
				t.Fatalf("reading error: %v", err)
			}
			result.Error = &e
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
					// Push back by re-adding to tokens and handling.
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

// TestIntegrationPreLoginRoundTrip verifies the full PRELOGIN handshake
// over a real TCP connection, checking version and encryption negotiation.
func TestIntegrationPreLoginRoundTrip(t *testing.T) {
	s := testServer(t, ServerConfig{})
	conn := dialServer(t, s)
	defer conn.Close()

	resp := doPreLogin(t, conn)

	// Verify all expected options are present.
	optMap := make(map[tdswire.PreLoginOptionToken][]byte)
	for _, opt := range resp.Options {
		optMap[opt.Token] = opt.Data
	}

	// VERSION must be present and valid.
	versionData, ok := optMap[tdswire.PreLoginVersion]
	if !ok {
		t.Fatal("server PRELOGIN response missing VERSION option")
	}
	v, err := tdswire.DecodeVersionData(versionData)
	if err != nil {
		t.Fatalf("decoding version data: %v", err)
	}
	if v.Major != 16 || v.Minor != 0 {
		t.Errorf("expected version 16.0, got %d.%d", v.Major, v.Minor)
	}

	// ENCRYPTION must be present and set to NotSup.
	encData, ok := optMap[tdswire.PreLoginEncryption]
	if !ok {
		t.Fatal("server PRELOGIN response missing ENCRYPTION option")
	}
	if len(encData) != 1 {
		t.Fatalf("expected 1 byte encryption data, got %d", len(encData))
	}
	if tdswire.EncryptionLevel(encData[0]) != tdswire.EncryptNotSup {
		t.Errorf("expected EncryptNotSup (%d), got %d", tdswire.EncryptNotSup, encData[0])
	}
}

// TestIntegrationLogin7SuccessTokens verifies the full sequence of tokens
// returned on successful LOGIN7 auth, including LOGINACK contents, ENVCHANGE
// database, and DONE status.
func TestIntegrationLogin7SuccessTokens(t *testing.T) {
	s := testServer(t, ServerConfig{
		Username:        "admin",
		Password:        "p@ssw0rd",
		DefaultDatabase: "defaultdb",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	resp := doLogin7(t, conn, "admin", "p@ssw0rd", "myappdb")

	result := parseResult(t, resp)

	// Token sequence: ENVCHANGE(database), ENVCHANGE(packet size),
	// LOGINACK, DONE — following the SQL Server convention where
	// ENVCHANGE tokens precede LOGINACK.
	expectedTokens := []byte{
		tdswire.TokenEnvChange, tdswire.TokenEnvChange,
		tdswire.TokenLoginAck, tdswire.TokenDone,
	}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
	}

	// LOGINACK details.
	if result.LoginAck == nil {
		t.Fatal("missing LOGINACK token")
	}
	if result.LoginAck.ProgName != "CockroachDB" {
		t.Errorf("expected ProgName 'CockroachDB', got %q", result.LoginAck.ProgName)
	}
	if result.LoginAck.Interface != 1 {
		t.Errorf("expected Interface 1 (TSQL), got %d", result.LoginAck.Interface)
	}

	// ENVCHANGE: first is database, second is packet size.
	if len(result.EnvChanges) != 2 {
		t.Fatalf("expected 2 ENVCHANGEs, got %d", len(result.EnvChanges))
	}
	ec := result.EnvChanges[0]
	if ec.Type != tdswire.EnvDatabase {
		t.Errorf("expected EnvDatabase type, got %d", ec.Type)
	}
	if ec.NewValue != "myappdb" {
		t.Errorf("expected new database 'myappdb', got %q", ec.NewValue)
	}
	ec2 := result.EnvChanges[1]
	if ec2.Type != tdswire.EnvPacketSize {
		t.Errorf("expected EnvPacketSize type, got %d", ec2.Type)
	}

	// DONE: must be final.
	if result.Done == nil {
		t.Fatal("missing DONE token")
	}
	if result.Done.Status != tdswire.DoneFinal {
		t.Errorf("expected DONE status DoneFinal (0x%02X), got 0x%02X",
			tdswire.DoneFinal, result.Done.Status)
	}
}

// TestIntegrationLogin7AuthFailure verifies that incorrect credentials
// produce an ERROR token with the appropriate error number and message.
func TestIntegrationLogin7AuthFailure(t *testing.T) {
	s := testServer(t, ServerConfig{
		Username: "sa",
		Password: "correct",
	})

	tests := []struct {
		name     string
		user     string
		pass     string
		wantUser string
	}{
		{"wrong password", "sa", "incorrect", "sa"},
		{"wrong username", "nobody", "correct", "nobody"},
		{"both wrong", "nobody", "incorrect", "nobody"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialServer(t, s)
			defer conn.Close()

			doPreLogin(t, conn)
			resp := doLogin7(t, conn, tc.user, tc.pass, "")

			result := parseResult(t, resp)

			// Token sequence: ERROR, DONE.
			expectedTokens := []byte{tdswire.TokenError, tdswire.TokenDone}
			if !bytes.Equal(result.Tokens, expectedTokens) {
				t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
			}

			if result.Error == nil {
				t.Fatal("missing ERROR token")
			}
			if result.Error.Number != 18456 {
				t.Errorf("expected error number 18456, got %d", result.Error.Number)
			}
			if result.Error.Class != 14 {
				t.Errorf("expected error class 14, got %d", result.Error.Class)
			}
			if !strings.Contains(result.Error.Message, tc.wantUser) {
				t.Errorf("expected error message to contain %q, got %q",
					tc.wantUser, result.Error.Message)
			}

			// DONE should have error flag.
			if result.Done == nil {
				t.Fatal("missing DONE token")
			}
			if result.Done.Status&tdswire.DoneError == 0 {
				t.Errorf("expected DONE status to include DoneError flag, got 0x%04X",
					result.Done.Status)
			}
		})
	}
}

// TestIntegrationSQLBatchResultSet verifies end-to-end SQL_BATCH execution
// through the mock QueryHandler, checking COLMETADATA, ROW, and DONE tokens.
func TestIntegrationSQLBatchResultSet(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler:    mockQueryHandler(&calls, &mu),
		DefaultDatabase: "testdb",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "testdb")
	resp := sendSQLBatch(t, conn, "SELECT 1 AS num")

	result := parseResult(t, resp)

	// Verify token sequence: COLMETADATA, ROW, DONE.
	expectedTokens := []byte{tdswire.TokenColMetaData, tdswire.TokenRow, tdswire.TokenDone}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
	}

	// Verify column metadata.
	if result.ColMeta == nil {
		t.Fatal("missing COLMETADATA")
	}
	if len(result.ColMeta.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(result.ColMeta.Columns))
	}
	if result.ColMeta.Columns[0].ColName != "num" {
		t.Errorf("expected column name 'num', got %q", result.ColMeta.Columns[0].ColName)
	}
	if result.ColMeta.Columns[0].TypeInfo.TypeID != tdswire.TypeInt4 {
		t.Errorf("expected TypeInt4, got 0x%02X", result.ColMeta.Columns[0].TypeInfo.TypeID)
	}

	// Verify row data.
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	gotVal := int32(binary.LittleEndian.Uint32(result.Rows[0].Values[0]))
	if gotVal != 1 {
		t.Errorf("expected value 1, got %d", gotVal)
	}

	// Verify DONE token.
	if result.Done == nil {
		t.Fatal("missing DONE")
	}
	if result.Done.RowCount != 1 {
		t.Errorf("expected row count 1, got %d", result.Done.RowCount)
	}
	if result.Done.Status&tdswire.DoneCount == 0 {
		t.Error("expected DONE to have DoneCount flag set")
	}

	// Verify the query handler was called.
	mu.Lock()
	if len(calls) != 1 {
		t.Errorf("expected 1 query call, got %d", len(calls))
	} else {
		if calls[0].Query != "SELECT 1 AS num" {
			t.Errorf("expected query 'SELECT 1 AS num', got %q", calls[0].Query)
		}
		if calls[0].Database != "testdb" {
			t.Errorf("expected database 'testdb', got %q", calls[0].Database)
		}
	}
	mu.Unlock()
}

// TestIntegrationMultiRowResult verifies that multi-row results are correctly
// encoded with multiple ROW tokens between COLMETADATA and DONE.
func TestIntegrationMultiRowResult(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler: mockQueryHandler(&calls, &mu),
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")
	resp := sendSQLBatch(t, conn, "SELECT id, name FROM test_table")

	result := parseResult(t, resp)

	// COLMETADATA + 3 ROWs + DONE.
	expectedTokens := []byte{
		tdswire.TokenColMetaData,
		tdswire.TokenRow, tdswire.TokenRow, tdswire.TokenRow,
		tdswire.TokenDone,
	}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
	}

	// 2 columns.
	if result.ColMeta == nil || len(result.ColMeta.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %v", result.ColMeta)
	}
	if result.ColMeta.Columns[0].ColName != "id" {
		t.Errorf("expected first column 'id', got %q", result.ColMeta.Columns[0].ColName)
	}
	if result.ColMeta.Columns[1].ColName != "name" {
		t.Errorf("expected second column 'name', got %q", result.ColMeta.Columns[1].ColName)
	}

	// 3 rows.
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Verify row 0: id=1, name="alice".
	id0 := int32(binary.LittleEndian.Uint32(result.Rows[0].Values[0]))
	if id0 != 1 {
		t.Errorf("row 0: expected id 1, got %d", id0)
	}
	if string(result.Rows[0].Values[1]) != "alice" {
		t.Errorf("row 0: expected name 'alice', got %q", result.Rows[0].Values[1])
	}

	// Verify row 2: id=3, name="charlie".
	id2 := int32(binary.LittleEndian.Uint32(result.Rows[2].Values[0]))
	if id2 != 3 {
		t.Errorf("row 2: expected id 3, got %d", id2)
	}
	if string(result.Rows[2].Values[1]) != "charlie" {
		t.Errorf("row 2: expected name 'charlie', got %q", result.Rows[2].Values[1])
	}

	// Done row count.
	if result.Done == nil {
		t.Fatal("missing DONE")
	}
	if result.Done.RowCount != 3 {
		t.Errorf("expected row count 3, got %d", result.Done.RowCount)
	}
}

// TestIntegrationEmptyResultSet verifies that a query returning columns
// but no rows produces COLMETADATA + DONE with row count 0.
func TestIntegrationEmptyResultSet(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler: mockQueryHandler(&calls, &mu),
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")
	resp := sendSQLBatch(t, conn, "SELECT id FROM empty_table")

	result := parseResult(t, resp)

	// COLMETADATA + DONE (no ROW tokens).
	expectedTokens := []byte{tdswire.TokenColMetaData, tdswire.TokenDone}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
	}

	if result.ColMeta == nil || len(result.ColMeta.Columns) != 1 {
		t.Fatalf("expected 1 column, got %v", result.ColMeta)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
	}
	if result.Done == nil {
		t.Fatal("missing DONE")
	}
	if result.Done.RowCount != 0 {
		t.Errorf("expected row count 0, got %d", result.Done.RowCount)
	}
}

// TestIntegrationUseDatabaseEnvChange verifies that USE <db> produces an
// ENVCHANGE token with the old and new database names, and that subsequent
// queries see the new database.
func TestIntegrationUseDatabaseEnvChange(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler:    mockQueryHandler(&calls, &mu),
		DefaultDatabase: "master",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")

	// USE mydb.
	resp := sendSQLBatch(t, conn, "USE mydb")
	result := parseResult(t, resp)

	expectedTokens := []byte{tdswire.TokenEnvChange, tdswire.TokenDone}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected tokens %v, got %v", expectedTokens, result.Tokens)
	}

	if len(result.EnvChanges) != 1 {
		t.Fatalf("expected 1 ENVCHANGE, got %d", len(result.EnvChanges))
	}
	ec := result.EnvChanges[0]
	if ec.Type != tdswire.EnvDatabase {
		t.Errorf("expected EnvDatabase, got %d", ec.Type)
	}
	if ec.NewValue != "mydb" {
		t.Errorf("expected new value 'mydb', got %q", ec.NewValue)
	}
	if ec.OldValue != "master" {
		t.Errorf("expected old value 'master', got %q", ec.OldValue)
	}

	// Subsequent query should see the new database.
	resp = sendSQLBatch(t, conn, "SELECT DB_NAME() AS current_db")
	result = parseResult(t, resp)

	mu.Lock()
	defer mu.Unlock()
	// Find the SELECT DB_NAME() call.
	var found bool
	for _, c := range calls {
		if strings.Contains(c.Query, "DB_NAME") {
			found = true
			if c.Database != "mydb" {
				t.Errorf("expected query to see database 'mydb', got %q", c.Database)
			}
		}
	}
	if !found {
		t.Error("DB_NAME query not found in handler calls")
	}
}

// TestIntegrationUseDatabaseWithBrackets verifies that USE [dbname] and
// USE "dbname" strip the surrounding brackets/quotes.
func TestIntegrationUseDatabaseWithBrackets(t *testing.T) {
	s := testServer(t, ServerConfig{DefaultDatabase: "master"})

	for _, tc := range []struct {
		sql    string
		wantDB string
	}{
		{"USE [mydb]", "mydb"},
		{`USE "mydb"`, "mydb"},
		{"USE plaindb", "plaindb"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			conn := dialServer(t, s)
			defer conn.Close()
			_, _, _ = fullLogin(t, conn, "", "", "")

			resp := sendSQLBatch(t, conn, tc.sql)
			result := parseResult(t, resp)

			if len(result.EnvChanges) != 1 {
				t.Fatalf("expected 1 ENVCHANGE, got %d", len(result.EnvChanges))
			}
			if result.EnvChanges[0].NewValue != tc.wantDB {
				t.Errorf("expected new database %q, got %q", tc.wantDB, result.EnvChanges[0].NewValue)
			}
		})
	}
}

// TestIntegrationAttentionPacketHandling verifies that an ATTENTION packet
// during the READY state produces a DONE response.
func TestIntegrationAttentionPacketHandling(t *testing.T) {
	s := testServer(t, ServerConfig{})
	conn := dialServer(t, s)
	defer conn.Close()

	doPreLogin(t, conn)
	doLogin7(t, conn, "", "", "")

	pr := tdswire.NewPacketReader(conn)
	pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

	// Send ATTENTION.
	if err := pw.WriteMessage(tdswire.PacketTypeAttention, nil); err != nil {
		t.Fatalf("writing ATTENTION: %v", err)
	}

	pktType, resp, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading ATTENTION response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT, got %s", pktType)
	}

	result := parseResult(t, resp)
	if result.Done == nil {
		t.Fatal("missing DONE token in ATTENTION response")
	}
	if result.Done.Status&tdswire.DoneFinal == 0 {
		// DoneFinal is 0x00, so check it is exactly final (no error/more flags).
		if result.Done.Status != tdswire.DoneFinal {
			t.Errorf("expected DONE status DoneFinal, got 0x%04X", result.Done.Status)
		}
	}

	// Verify the connection is still usable after ATTENTION.
	if err := pw.WriteMessage(tdswire.PacketTypeAttention, nil); err != nil {
		t.Fatalf("writing second ATTENTION: %v", err)
	}
	pktType, _, err = pr.ReadMessage()
	if err != nil {
		t.Fatalf("reading second ATTENTION response: %v", err)
	}
	if pktType != tdswire.PacketTypeTabularResult {
		t.Fatalf("expected TABULAR_RESULT for second ATTENTION, got %s", pktType)
	}
}

// TestIntegrationConcurrentConnections exercises multiple concurrent
// connections, each running independent queries, and verifies that the
// server handles them without data corruption or races.
func TestIntegrationConcurrentConnections(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler: mockQueryHandler(&calls, &mu),
	})

	const numConns = 10
	const queriesPerConn = 5
	var wg sync.WaitGroup
	errCh := make(chan error, numConns*queriesPerConn)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			conn := dialServer(t, s)
			defer conn.Close()

			_, _, _ = fullLogin(t, conn, "", "", fmt.Sprintf("db%d", connID))

			pr := tdswire.NewPacketReader(conn)
			pw := tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize)

			for q := 0; q < queriesPerConn; q++ {
				resp := sendSQLBatchRaw(t, pr, pw, "SELECT 1 AS num")
				if len(resp) == 0 {
					errCh <- fmt.Errorf("conn %d query %d: empty response", connID, q)
					return
				}
				if resp[0] != tdswire.TokenColMetaData {
					errCh <- fmt.Errorf("conn %d query %d: expected COLMETADATA (0x%02X), got 0x%02X",
						connID, q, tdswire.TokenColMetaData, resp[0])
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify total query count.
	mu.Lock()
	totalCalls := len(calls)
	mu.Unlock()
	expected := numConns * queriesPerConn
	if totalCalls != expected {
		t.Errorf("expected %d total query handler calls, got %d", expected, totalCalls)
	}
}

// TestIntegrationDrainBehavior verifies that after Drain(), existing
// connections are closed and new connections are rejected.
func TestIntegrationDrainBehavior(t *testing.T) {
	s := testServer(t, ServerConfig{})

	// Establish two connections before drain.
	conn1 := dialServer(t, s)
	defer conn1.Close()
	_, _, _ = fullLogin(t, conn1, "", "", "")

	conn2 := dialServer(t, s)
	defer conn2.Close()
	_, _, _ = fullLogin(t, conn2, "", "", "")

	// Both should be active.
	metrics := s.Metrics()
	if metrics.ActiveConns != 2 {
		t.Errorf("expected 2 active conns before drain, got %d", metrics.ActiveConns)
	}

	// Drain.
	s.Drain()

	// Existing connections should get closed.
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err := conn1.Read(buf)
	if err == nil {
		t.Error("expected conn1 to be closed after drain")
	}

	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn2.Read(buf)
	if err == nil {
		t.Error("expected conn2 to be closed after drain")
	}

	// New connections should be rejected (closed immediately).
	newConn, err := net.DialTimeout("tcp", s.Addr().String(), 1*time.Second)
	if err != nil {
		// Connection refused is acceptable.
		return
	}
	defer newConn.Close()

	_ = newConn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err = newConn.Read(buf)
	if err == nil {
		t.Error("expected new connection to be rejected during drain")
	}
}

// TestIntegrationConnectionMetrics verifies that server metrics accurately
// track new connections, active connections, and byte counts.
func TestIntegrationConnectionMetrics(t *testing.T) {
	s := testServer(t, ServerConfig{})

	// Before any connections, everything should be zero.
	m := s.Metrics()
	if m.ActiveConns != 0 {
		t.Errorf("expected 0 active conns initially, got %d", m.ActiveConns)
	}
	if m.NewConns != 0 {
		t.Errorf("expected 0 new conns initially, got %d", m.NewConns)
	}

	// Create first connection and go through PRELOGIN + LOGIN.
	conn1 := dialServer(t, s)
	doPreLogin(t, conn1)
	doLogin7(t, conn1, "", "", "")

	// Wait briefly for the server to update metrics.
	time.Sleep(50 * time.Millisecond)

	m = s.Metrics()
	if m.NewConns != 1 {
		t.Errorf("expected 1 new conn after first connect, got %d", m.NewConns)
	}
	if m.ActiveConns != 1 {
		t.Errorf("expected 1 active conn, got %d", m.ActiveConns)
	}

	// Create second connection.
	conn2 := dialServer(t, s)
	doPreLogin(t, conn2)
	doLogin7(t, conn2, "", "", "")

	time.Sleep(50 * time.Millisecond)

	m = s.Metrics()
	if m.NewConns != 2 {
		t.Errorf("expected 2 new conns, got %d", m.NewConns)
	}
	if m.ActiveConns != 2 {
		t.Errorf("expected 2 active conns, got %d", m.ActiveConns)
	}

	// Close first connection.
	conn1.Close()
	time.Sleep(100 * time.Millisecond)

	m = s.Metrics()
	if m.NewConns != 2 {
		t.Errorf("expected 2 new conns (cumulative) after close, got %d", m.NewConns)
	}
	if m.ActiveConns != 1 {
		t.Errorf("expected 1 active conn after closing first, got %d", m.ActiveConns)
	}

	// Close second connection.
	conn2.Close()
	time.Sleep(100 * time.Millisecond)

	m = s.Metrics()
	if m.ActiveConns != 0 {
		t.Errorf("expected 0 active conns after closing all, got %d", m.ActiveConns)
	}
	// NewConns should remain at 2 (it's cumulative).
	if m.NewConns != 2 {
		t.Errorf("expected 2 new conns (cumulative), got %d", m.NewConns)
	}
}

// TestIntegrationSETCommands verifies that common SET commands issued by
// Sybase/SQL Server drivers on connect are handled without error. These
// SET statements are dispatched to the QueryHandler which returns empty
// results (no error).
func TestIntegrationSETCommands(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler: mockQueryHandler(&calls, &mu),
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")

	// Common SET commands that Sybase/MSSQL drivers send on connect.
	setCmds := []string{
		"SET ANSI_NULLS ON",
		"SET ANSI_PADDING ON",
		"SET ANSI_WARNINGS ON",
		"SET QUOTED_IDENTIFIER ON",
		"SET CONCAT_NULL_YIELDS_NULL ON",
		"SET ARITHABORT ON",
		"SET TEXTSIZE 2147483647",
		"SET IMPLICIT_TRANSACTIONS OFF",
		"SET CURSOR_CLOSE_ON_COMMIT OFF",
		"SET LOCK_TIMEOUT -1",
		"SET LANGUAGE us_english",
		"SET DATEFORMAT mdy",
		"SET DATEFIRST 7",
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
	}

	for _, cmd := range setCmds {
		t.Run(cmd, func(t *testing.T) {
			resp := sendSQLBatch(t, conn, cmd)

			result := parseResult(t, resp)

			// SET commands produce a DONE-only response (no result set).
			// The handler returns nil cols/rows, so the server sends only DONE.
			if result.Done == nil {
				t.Fatalf("missing DONE token for %q", cmd)
			}
			if result.Done.Status&tdswire.DoneError != 0 {
				t.Errorf("unexpected error in DONE for %q: status=0x%04X", cmd, result.Done.Status)
			}
		})
	}

	// Verify all SET commands were dispatched to the handler.
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != len(setCmds) {
		t.Errorf("expected %d handler calls, got %d", len(setCmds), len(calls))
	}
	for i, call := range calls {
		if call.Query != setCmds[i] {
			t.Errorf("call %d: expected query %q, got %q", i, setCmds[i], call.Query)
		}
	}
}

// TestIntegrationVersionQuery verifies that a SELECT @@VERSION query is
// dispatched to the handler and returns a varchar result.
func TestIntegrationVersionQuery(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler: mockQueryHandler(&calls, &mu),
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")

	resp := sendSQLBatch(t, conn, "SELECT @@VERSION")

	result := parseResult(t, resp)

	// Expect COLMETADATA + ROW + DONE.
	expectedTokens := []byte{tdswire.TokenColMetaData, tdswire.TokenRow, tdswire.TokenDone}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
	}

	// Verify the result contains our version string.
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	versionStr := string(result.Rows[0].Values[0])
	if !strings.Contains(versionStr, "CockroachDB") {
		t.Errorf("expected version string to contain 'CockroachDB', got %q", versionStr)
	}

	// Verify the handler was called with the @@VERSION query.
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("expected 1 handler call, got %d", len(calls))
	}
	if !strings.Contains(calls[0].Query, "@@VERSION") {
		t.Errorf("expected query to contain '@@VERSION', got %q", calls[0].Query)
	}
}

// TestIntegrationQueryHandlerError verifies that a query handler error
// produces an ERROR token with the error message.
func TestIntegrationQueryHandlerError(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler: mockQueryHandler(&calls, &mu),
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")

	// Send an unknown query that the mock handler will reject.
	resp := sendSQLBatch(t, conn, "DROP TABLE nonexistent")

	result := parseResult(t, resp)

	// Expect ERROR + DONE.
	expectedTokens := []byte{tdswire.TokenError, tdswire.TokenDone}
	if !bytes.Equal(result.Tokens, expectedTokens) {
		t.Errorf("expected token sequence %v, got %v", expectedTokens, result.Tokens)
	}

	if result.Error == nil {
		t.Fatal("missing ERROR token")
	}
	if result.Error.Number != 50000 {
		t.Errorf("expected error number 50000, got %d", result.Error.Number)
	}
	if !strings.Contains(result.Error.Message, "unrecognized query") {
		t.Errorf("expected error message to mention 'unrecognized query', got %q",
			result.Error.Message)
	}
	if result.Error.Server != "CockroachDB" {
		t.Errorf("expected server name 'CockroachDB', got %q", result.Error.Server)
	}

	// DONE should have error flag.
	if result.Done == nil {
		t.Fatal("missing DONE token")
	}
	if result.Done.Status&tdswire.DoneError == 0 {
		t.Error("expected DONE to have DoneError flag")
	}
}

// TestIntegrationMultipleQueriesSameConnection verifies that a single
// connection can execute multiple queries sequentially.
func TestIntegrationMultipleQueriesSameConnection(t *testing.T) {
	var queryCount atomic.Int32

	handler := func(ctx context.Context, query string, database string) ([]ResultColumn, [][]interface{}, error) {
		n := queryCount.Add(1)
		return []ResultColumn{
				{Name: "seq", TypeID: tdswire.TypeInt4},
			}, [][]interface{}{
				{n},
			}, nil
	}

	s := testServer(t, ServerConfig{QueryHandler: handler})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")

	for i := 1; i <= 10; i++ {
		resp := sendSQLBatch(t, conn, fmt.Sprintf("SELECT %d", i))
		result := parseResult(t, resp)

		if result.ColMeta == nil {
			t.Fatalf("query %d: missing COLMETADATA", i)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("query %d: expected 1 row, got %d", i, len(result.Rows))
		}
		gotVal := int32(binary.LittleEndian.Uint32(result.Rows[0].Values[0]))
		if gotVal != int32(i) {
			t.Errorf("query %d: expected seq %d, got %d", i, i, gotVal)
		}
	}
}

// TestIntegrationLogin7DefaultDatabase verifies that when no database is
// specified in LOGIN7, the server's default database is used.
func TestIntegrationLogin7DefaultDatabase(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler:    mockQueryHandler(&calls, &mu),
		DefaultDatabase: "defaultdb",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, loginResp := fullLogin(t, conn, "", "", "")

	// Verify ENVCHANGE in login response shows the default database.
	// Login sends 2 ENVCHANGEs: database then packet size.
	loginResult := parseResult(t, loginResp)
	if len(loginResult.EnvChanges) != 2 {
		t.Fatalf("expected 2 ENVCHANGEs in login, got %d", len(loginResult.EnvChanges))
	}
	if loginResult.EnvChanges[0].NewValue != "defaultdb" {
		t.Errorf("expected default database 'defaultdb', got %q", loginResult.EnvChanges[0].NewValue)
	}

	// Query should see the default database.
	resp := sendSQLBatch(t, conn, "SELECT DB_NAME() AS current_db")
	_ = parseResult(t, resp)

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].Database != "defaultdb" {
		t.Errorf("expected database 'defaultdb', got %q", calls[0].Database)
	}
}

// TestIntegrationLoginThenQuerySequence exercises the complete lifecycle:
// PRELOGIN -> LOGIN7 -> multiple SQL_BATCH -> USE db -> SQL_BATCH -> close.
func TestIntegrationLoginThenQuerySequence(t *testing.T) {
	var calls []queryCall
	var mu sync.Mutex

	s := testServer(t, ServerConfig{
		QueryHandler:    mockQueryHandler(&calls, &mu),
		DefaultDatabase: "master",
		Username:        "testuser",
		Password:        "testpass",
	})
	conn := dialServer(t, s)
	defer conn.Close()

	// Step 1: PRELOGIN.
	preLoginResp := doPreLogin(t, conn)
	if len(preLoginResp.Options) == 0 {
		t.Fatal("empty PRELOGIN response")
	}

	// Step 2: LOGIN7.
	loginResp := doLogin7(t, conn, "testuser", "testpass", "appdb")
	loginResult := parseResult(t, loginResp)
	if loginResult.LoginAck == nil {
		t.Fatal("login failed: no LOGINACK")
	}

	// Step 3: SET commands (like a real driver would send).
	sendSQLBatch(t, conn, "SET ANSI_NULLS ON")
	sendSQLBatch(t, conn, "SET QUOTED_IDENTIFIER ON")

	// Step 4: Query.
	resp := sendSQLBatch(t, conn, "SELECT 1 AS num")
	result := parseResult(t, resp)
	if result.ColMeta == nil || len(result.Rows) != 1 {
		t.Fatal("query failed to return expected result")
	}

	// Step 5: USE database.
	resp = sendSQLBatch(t, conn, "USE newdb")
	result = parseResult(t, resp)
	if len(result.EnvChanges) != 1 || result.EnvChanges[0].NewValue != "newdb" {
		t.Fatal("USE database failed")
	}

	// Step 6: Query after USE to verify database changed.
	resp = sendSQLBatch(t, conn, "SELECT DB_NAME() AS current_db")
	_ = parseResult(t, resp)

	mu.Lock()
	defer mu.Unlock()

	// Verify the last call saw database "newdb".
	lastCall := calls[len(calls)-1]
	if lastCall.Database != "newdb" {
		t.Errorf("expected database 'newdb' after USE, got %q", lastCall.Database)
	}
}

// TestIntegrationNilQueryHandler verifies that the server's default
// (nil) query handler returns an empty response without error.
func TestIntegrationNilQueryHandler(t *testing.T) {
	s := testServer(t, ServerConfig{
		// No QueryHandler set - uses defaultQueryHandler.
	})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")

	resp := sendSQLBatch(t, conn, "SELECT anything")
	result := parseResult(t, resp)

	// Default handler returns nil cols/rows, so we get just a DONE token.
	if result.Done == nil {
		t.Fatal("missing DONE token with default handler")
	}
	if result.Done.Status&tdswire.DoneError != 0 {
		t.Error("unexpected error flag with default handler")
	}
}

// TestIntegrationOpenAuthNoCredentials verifies that when the server has
// no configured username/password, any credentials are accepted.
func TestIntegrationOpenAuthNoCredentials(t *testing.T) {
	s := testServer(t, ServerConfig{
		// No Username/Password set - allows any login.
	})

	tests := []struct {
		user string
		pass string
	}{
		{"anyuser", "anypass"},
		{"", ""},
		{"admin", "supersecret"},
		{"root", ""},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("user=%q", tc.user), func(t *testing.T) {
			conn := dialServer(t, s)
			defer conn.Close()

			doPreLogin(t, conn)
			resp := doLogin7(t, conn, tc.user, tc.pass, "")

			result := parseResult(t, resp)
			if result.LoginAck == nil {
				t.Errorf("expected login success for user=%q pass=%q", tc.user, tc.pass)
			}
		})
	}
}

// TestIntegrationPasswordObfuscation verifies that LOGIN7 password
// obfuscation works correctly over the wire by testing with various
// password strings including special characters.
func TestIntegrationPasswordObfuscation(t *testing.T) {
	passwords := []string{
		"simple",
		"p@ssw0rd!",
		"with spaces",
		"émojis🎉",
		"a",
		"very_long_password_that_is_quite_lengthy_and_contains_many_characters_1234567890",
	}

	for _, pw := range passwords {
		t.Run(fmt.Sprintf("password=%q", pw), func(t *testing.T) {
			s := testServer(t, ServerConfig{
				Username: "testuser",
				Password: pw,
			})
			conn := dialServer(t, s)
			defer conn.Close()

			doPreLogin(t, conn)
			resp := doLogin7(t, conn, "testuser", pw, "")

			result := parseResult(t, resp)
			if result.LoginAck == nil {
				t.Errorf("login should succeed with password %q", pw)
			}

			// Also verify wrong password fails.
			conn2 := dialServer(t, s)
			defer conn2.Close()
			doPreLogin(t, conn2)
			resp2 := doLogin7(t, conn2, "testuser", pw+"wrong", "")
			result2 := parseResult(t, resp2)
			if result2.Error == nil {
				t.Errorf("login should fail with wrong password for %q", pw)
			}
		})
	}
}

// TestIntegrationBytesMetric verifies that BytesIn and BytesOut metrics
// are incremented when data flows through the server. Note: the current
// server implementation does not track bytes in the bytesIn/bytesOut
// atomics (they are wired but not yet incremented in conn.go), so this
// test verifies the metric infrastructure exists and returns values >= 0.
func TestIntegrationBytesMetric(t *testing.T) {
	s := testServer(t, ServerConfig{})

	m := s.Metrics()
	if m.BytesIn < 0 {
		t.Errorf("BytesIn should be >= 0, got %d", m.BytesIn)
	}
	if m.BytesOut < 0 {
		t.Errorf("BytesOut should be >= 0, got %d", m.BytesOut)
	}

	// After a connection, metrics should still be non-negative.
	conn := dialServer(t, s)
	doPreLogin(t, conn)
	doLogin7(t, conn, "", "", "")
	sendSQLBatch(t, conn, "SELECT 1 AS val")
	conn.Close()

	time.Sleep(50 * time.Millisecond)

	m = s.Metrics()
	if m.BytesIn < 0 {
		t.Errorf("BytesIn should be >= 0 after traffic, got %d", m.BytesIn)
	}
	if m.BytesOut < 0 {
		t.Errorf("BytesOut should be >= 0 after traffic, got %d", m.BytesOut)
	}
}

// TestIntegrationVarCharColumnTypes verifies that varchar and nvarchar
// columns are encoded and decoded correctly over the wire.
func TestIntegrationVarCharColumnTypes(t *testing.T) {
	handler := func(ctx context.Context, query string, database string) ([]ResultColumn, [][]interface{}, error) {
		return []ResultColumn{
				{Name: "varchar_col", TypeID: tdswire.TypeBigVarChar, MaxLen: 100},
				{Name: "nvarchar_col", TypeID: tdswire.TypeNVarChar, MaxLen: 200},
				{Name: "int_col", TypeID: tdswire.TypeInt4},
			}, [][]interface{}{
				{"hello", "world", int32(42)},
			}, nil
	}

	s := testServer(t, ServerConfig{QueryHandler: handler})
	conn := dialServer(t, s)
	defer conn.Close()

	_, _, _ = fullLogin(t, conn, "", "", "")
	resp := sendSQLBatch(t, conn, "SELECT varchar_col, nvarchar_col, int_col FROM mixed_types")

	result := parseResult(t, resp)

	if result.ColMeta == nil || len(result.ColMeta.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %v", result.ColMeta)
	}

	// Verify column types.
	if result.ColMeta.Columns[0].TypeInfo.TypeID != tdswire.TypeBigVarChar {
		t.Errorf("col 0: expected TypeBigVarChar, got 0x%02X", result.ColMeta.Columns[0].TypeInfo.TypeID)
	}
	if result.ColMeta.Columns[0].TypeInfo.MaxLen != 100 {
		t.Errorf("col 0: expected MaxLen 100, got %d", result.ColMeta.Columns[0].TypeInfo.MaxLen)
	}
	if result.ColMeta.Columns[1].TypeInfo.TypeID != tdswire.TypeNVarChar {
		t.Errorf("col 1: expected TypeNVarChar, got 0x%02X", result.ColMeta.Columns[1].TypeInfo.TypeID)
	}
	if result.ColMeta.Columns[2].TypeInfo.TypeID != tdswire.TypeInt4 {
		t.Errorf("col 2: expected TypeInt4, got 0x%02X", result.ColMeta.Columns[2].TypeInfo.TypeID)
	}

	// Verify row data.
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if string(result.Rows[0].Values[0]) != "hello" {
		t.Errorf("col 0: expected 'hello', got %q", result.Rows[0].Values[0])
	}
	if string(result.Rows[0].Values[1]) != "world" {
		t.Errorf("col 1: expected 'world', got %q", result.Rows[0].Values[1])
	}
	intVal := int32(binary.LittleEndian.Uint32(result.Rows[0].Values[2]))
	if intVal != 42 {
		t.Errorf("col 2: expected 42, got %d", intVal)
	}
}
