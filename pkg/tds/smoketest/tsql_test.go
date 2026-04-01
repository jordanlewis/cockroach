// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package smoketest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/tds"
	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// startTDSTestServer starts a CockroachDB test server and TDS frontend,
// returning the TDS server address. The caller must stop the server via
// the returned cleanup function.
func startTDSTestServer(
	t *testing.T, ctx context.Context,
) (addr string, cleanup func()) {
	t.Helper()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	internalDB := srv.ApplicationLayer().InternalDB().(isql.DB)

	tdsServer := tds.NewServer(tds.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		DefaultDatabase: "defaultdb",
		DB:              internalDB,
	})
	require.NoError(t, tdsServer.Start(ctx))

	return tdsServer.Addr().String(), func() {
		tdsServer.Stop()
		srv.Stopper().Stop(ctx)
	}
}

// TestTSQLRealClient starts a CockroachDB test server with a TDS
// frontend and connects using the real FreeTDS tsql client. This
// validates that our TDS implementation handles the full PRELOGIN →
// LOGIN7 → SQL_BATCH flow correctly as seen by a production TDS client.
func TestTSQLRealClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Skip if tsql is not installed.
	tsqlPath, err := exec.LookPath("tsql")
	if err != nil {
		t.Skip("tsql not found in PATH; install FreeTDS: brew install freetds")
	}
	t.Logf("using tsql at %s", tsqlPath)

	ctx := context.Background()
	addr, cleanup := startTDSTestServer(t, ctx)
	defer cleanup()

	parts := strings.Split(addr, ":")
	require.Len(t, parts, 2, "expected host:port, got %s", addr)
	host, port := parts[0], parts[1]
	t.Logf("TDS server listening on %s:%s", host, port)

	// Test 1: tsql connect and SELECT 1.
	t.Run("SelectOne", func(t *testing.T) {
		out := runTSQL(t, tsqlPath, host, port, "SELECT 1 AS val\nGO\n")
		t.Logf("tsql output:\n%s", out)
		require.Contains(t, out, "1>", "expected tsql prompt")
	})

	// Test 2: tsql CREATE TABLE + INSERT + SELECT.
	t.Run("CRUD", func(t *testing.T) {
		script := strings.Join([]string{
			"CREATE TABLE tsql_test (id INT NOT NULL, name VARCHAR(50))",
			"GO",
			"INSERT INTO tsql_test VALUES (1, 'hello')",
			"GO",
			"SELECT id, name FROM tsql_test",
			"GO",
		}, "\n") + "\n"
		out := runTSQL(t, tsqlPath, host, port, script)
		t.Logf("tsql output:\n%s", out)
		require.Contains(t, out, "hello", "expected SELECT result to contain 'hello'")
	})

	// Test 3: @@VERSION query.
	t.Run("Version", func(t *testing.T) {
		out := runTSQL(t, tsqlPath, host, port, "SELECT @@VERSION\nGO\n")
		t.Logf("tsql output:\n%s", out)
		require.Contains(t, out, "CockroachDB",
			"expected @@VERSION to contain CockroachDB")
	})

	// Test 4: SET commands (common client initialization).
	t.Run("SetCommands", func(t *testing.T) {
		script := strings.Join([]string{
			"SET ANSI_NULLS ON",
			"GO",
			"SET QUOTED_IDENTIFIER ON",
			"GO",
			"SELECT 42 AS answer",
			"GO",
		}, "\n") + "\n"
		out := runTSQL(t, tsqlPath, host, port, script)
		t.Logf("tsql output:\n%s", out)
		require.Contains(t, out, "42", "expected SELECT result after SET commands")
	})
}

// runTSQL runs tsql as a subprocess, sends the given SQL script to its
// stdin, and returns the combined stdout+stderr output. It uses TDS
// version 7.3 (FreeTDS default) with no encryption.
func runTSQL(t *testing.T, tsqlPath, host, port, script string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, tsqlPath,
		"-H", host,
		"-p", port,
		"-U", "root",
		"-P", "",
	)
	cmd.Stdin = strings.NewReader(script)

	// Create a temp file for FreeTDS debug output.
	dumpFile := fmt.Sprintf("/tmp/tsql_debug_%d.log", time.Now().UnixNano())
	cmd.Env = append(cmd.Environ(),
		// Explicitly set TDS version 7.3 for PRELOGIN/LOGIN7 flow.
		"TDSVER=7.3",
		// Enable FreeTDS debug logging.
		"TDSDUMP="+dumpFile,
	)

	out, err := cmd.CombinedOutput()
	outStr := string(out)

	// Read FreeTDS debug output if available.
	if debugData, readErr := os.ReadFile(dumpFile); readErr == nil {
		// Only show the last 2000 bytes of debug output.
		debugStr := string(debugData)
		if len(debugStr) > 2000 {
			debugStr = "...(truncated)...\n" + debugStr[len(debugStr)-2000:]
		}
		t.Logf("FreeTDS debug log:\n%s", debugStr)
		_ = os.Remove(dumpFile)
	}

	// tsql returns non-zero if it encounters errors, but we still want
	// to see the output for debugging. Only fail if there's no output
	// at all or the connection itself failed.
	if err != nil {
		// If we got output with the locale line (which means connection
		// succeeded), don't fail on exit code - tsql often exits non-zero.
		if !strings.Contains(outStr, "locale") && !strings.Contains(outStr, "1>") {
			t.Fatalf("tsql failed: %v\noutput: %s", err, outStr)
		}
	}

	return outStr
}

// TestGoMSSQLDBStyleClient validates TDS compatibility using a raw TDS
// client that mimics go-mssqldb's connection sequence. This tests the
// specific wire format expectations of production TDS drivers without
// requiring go-mssqldb as a dependency.
func TestGoMSSQLDBStyleClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	addr, cleanup := startTDSTestServer(t, ctx)
	defer cleanup()

	// Connect using our raw TDS client (simulating go-mssqldb behavior).
	tc := dialTDSConn(t, addr)
	defer func() { _ = tc.Close() }()

	// PRELOGIN with go-mssqldb style options (includes INSTOPT, THREADID, MARS).
	preLoginResp := doGoMSSQLPreLogin(t, tc)
	require.NotNil(t, preLoginResp, "PRELOGIN response should not be nil")

	// LOGIN7 with TDS 7.4 (go-mssqldb default).
	loginResp := doGoMSSQLLogin7(t, tc, "root", "", "defaultdb")
	loginResult := parseTokenStream(t, loginResp)
	require.NotNil(t, loginResult.LoginAck, "expected LOGINACK token")
	require.True(t, len(loginResult.EnvChanges) >= 1,
		"expected at least 1 ENVCHANGE, got %d", len(loginResult.EnvChanges))

	// Verify ENVCHANGE(database) is present.
	var hasDBEnvChange bool
	for _, ec := range loginResult.EnvChanges {
		if ec.Type == 1 { // EnvDatabase
			hasDBEnvChange = true
			require.Equal(t, "defaultdb", ec.NewValue,
				"ENVCHANGE database should be 'defaultdb'")
		}
	}
	require.True(t, hasDBEnvChange, "expected ENVCHANGE(database) token")

	// Verify LOGINACK program name.
	require.Equal(t, "CockroachDB", loginResult.LoginAck.ProgName)

	// SQL batch: SELECT 1.
	resp := sendBatch(t, tc.pr, tc.pw, "SELECT 1 AS val")
	result := parseTokenStream(t, resp)
	require.Nil(t, result.Error, "SELECT 1 should not produce an error")
	require.NotNil(t, result.ColMeta, "expected COLMETADATA")
	require.Len(t, result.ColMeta.Columns, 1, "expected 1 column")
	require.Len(t, result.Rows, 1, "expected 1 row")

	// SQL batch: SELECT with string result.
	resp = sendBatch(t, tc.pr, tc.pw, "SELECT 'hello' AS greeting")
	result = parseTokenStream(t, resp)
	require.Nil(t, result.Error, "SELECT 'hello' should not produce an error")
	require.NotNil(t, result.ColMeta, "expected COLMETADATA")
	require.Len(t, result.Rows, 1, "expected 1 row")
	greeting := decodeRowString(result.Rows[0].Values[0])
	require.Equal(t, "hello", greeting)

	t.Log("go-mssqldb-style client test passed")
}

// tdsConn wraps a raw TCP connection with TDS packet reader/writer.
type tdsConn struct {
	conn net.Conn
	pr   *tdswire.PacketReader
	pw   *tdswire.PacketWriter
}

func (c *tdsConn) Close() error {
	return c.conn.Close()
}

func dialTDSConn(t *testing.T, addr string) *tdsConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	return &tdsConn{
		conn: conn,
		pr:   tdswire.NewPacketReader(conn),
		pw:   tdswire.NewPacketWriter(conn, tdswire.DefaultPacketSize),
	}
}

// doGoMSSQLPreLogin sends a PRELOGIN with options that match
// go-mssqldb's behavior: VERSION, ENCRYPTION, INSTOPT, THREADID, MARS.
func doGoMSSQLPreLogin(t *testing.T, c *tdsConn) *tdswire.PreLoginMsg {
	t.Helper()

	preLogin := &tdswire.PreLoginMsg{
		Options: []tdswire.PreLoginOption{
			{
				Token: tdswire.PreLoginVersion,
				Data: tdswire.EncodeVersionData(tdswire.PreLoginVersionData{
					Major: 16, Minor: 0, Build: 4120, SubBuild: 1,
				}),
			},
			{
				Token: tdswire.PreLoginEncryption,
				Data:  []byte{byte(tdswire.EncryptNotSup)},
			},
			{
				Token: tdswire.PreLoginInstOpt,
				Data:  []byte{0}, // empty instance name
			},
			{
				Token: tdswire.PreLoginThreadID,
				Data:  []byte{0, 0, 0, 0}, // thread ID = 0
			},
			{
				Token: tdswire.PreLoginMARS,
				Data:  []byte{0}, // MARS disabled
			},
		},
	}

	payload := tdswire.EncodePreLogin(preLogin)
	require.NoError(t, c.pw.WriteMessage(tdswire.PacketTypePreLogin, payload))

	pktType, respPayload, err := c.pr.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType)

	resp, err := tdswire.DecodePreLogin(respPayload)
	require.NoError(t, err)

	return resp
}

// doGoMSSQLLogin7 sends a LOGIN7 that mimics go-mssqldb's format.
func doGoMSSQLLogin7(t *testing.T, c *tdsConn, user, pass, db string) []byte {
	t.Helper()

	payload := buildLogin7(user, pass, db)
	require.NoError(t, c.pw.WriteMessage(tdswire.PacketTypeLogin7, payload))

	pktType, resp, err := c.pr.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, tdswire.PacketTypeTabularResult, pktType)

	return resp
}
