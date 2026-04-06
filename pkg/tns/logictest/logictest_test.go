// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package logictest_test provides a datadriven test runner for TNS
// (Oracle wire protocol) logic tests. Each testdata file is executed
// against a real CockroachDB test server with TNS enabled. The runner
// supports three directives:
//
//   - exec: run an Oracle SQL statement, expect success (empty output)
//   - query: run an Oracle SELECT, compare formatted results
//   - error: run an Oracle SQL statement, expect an error substring
//
// The query directive outputs column names on the first line followed
// by data rows, space-separated. Use the "rowsort" argument to sort
// rows before comparison when result ordering is non-deterministic:
//
//	query rowsort
//	SELECT a, b FROM t
//	----
//	A B
//	2 world
//	1 hello
package logictest_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/tns/auth"
	"github.com/cockroachdb/cockroach/pkg/tns/tnswire"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// startTNSServer starts a single-node CockroachDB cluster with TNS
// enabled on a random port. Returns the TNS listen address and a
// cleanup function.
func startTNSServer(t *testing.T) (tnsAddr string, cleanup func()) {
	t.Helper()

	st := cluster.MakeClusterSettings()
	server.TNSEnabled.Override(context.Background(), &st.SV, true)
	server.TNSPort.Override(context.Background(), &st.SV, 0)

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings:                   st,
		Insecure:                   true,
		DefaultTestTenant:          base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		DisableElasticCPUAdmission: true,
	})

	type tnsAddrGetter interface {
		TNSAddr() string
	}
	raw := srv.StorageLayer().(tnsAddrGetter)
	addr := raw.TNSAddr()
	require.NotEmpty(t, addr, "TNS server should be running")

	return addr, func() {
		srv.Stopper().Stop(context.Background())
	}
}

// testRunner manages a TNS wire protocol connection for executing
// test directives within a single datadriven test file.
type testRunner struct {
	conn net.Conn
}

// newRunner creates a testRunner connected to the TNS server at addr.
// It performs the full CONNECT + protocol negotiation + data type
// negotiation + O5LOGON authentication handshake before returning.
func newRunner(t *testing.T, addr string) *testRunner {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)

	// 1. CONNECT
	connectPayload := tnswire.EncodeConnect(tnswire.ConnectPacket{
		Version:    auth.ProtocolVersion,
		MinVersion: auth.MinProtocolVersion,
		SDUSize:    auth.DefaultSDUSize,
		TDUSize:    auth.DefaultTDUSize,
		ValueOfOne: 1,
		ConnectData: "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)" +
			"(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))",
	})
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(perOpTimeout)))
	require.NoError(t, tnswire.WritePacket(conn, tnswire.PacketTypeConnect, connectPayload))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(perOpTimeout)))
	hdr, _, err := tnswire.ReadPacket(conn)
	require.NoError(t, err)
	require.Equal(t, tnswire.PacketTypeAccept, hdr.Type)

	// 2. Protocol negotiation.
	protoNeg := []byte{byte(auth.TTIProtocolNeg), 0x06, 0x00, 0x00, 0x00, 0x00}
	writeDataPayload(t, conn, protoNeg)
	readDataPayload(t, conn)

	// 3. Data type negotiation — the server sends DTY proactively
	// after protocol negotiation (no client request needed).
	readDataPayload(t, conn)

	// 4. O5LOGON auth.
	authReq := buildAuthRequest("testuser")
	writeDataPayload(t, conn, authReq)

	challengePayload := readDataPayload(t, conn)
	require.True(t, len(challengePayload) > 0)

	sessKey, salt := parseChallenge(t, challengePayload)
	authResp := buildAuthResponse(t, sessKey, salt, "testpass")
	writeDataPayload(t, conn, authResp)

	successPayload := readDataPayload(t, conn)
	require.True(t, len(successPayload) > 0)
	require.Equal(t, byte(auth.TTIAuthResponse), successPayload[0])
	require.Equal(t, byte(0x00), successPayload[1])

	return &testRunner{conn: conn}
}

// close closes the underlying connection.
func (r *testRunner) close() {
	r.conn.Close()
}

// exec executes a SQL statement via TNS OPEN (which auto-executes
// DDL/DML) and returns "" on success. Fails the test on error.
func (r *testRunner) exec(t *testing.T, input string) string {
	t.Helper()
	sql := strings.TrimSpace(input)

	openPayload := tnswire.EncodeTTIOpen(tnswire.TTIOpenMsg{
		CursorID: 1,
		SQL:      sql,
	})
	writeDataPayload(t, r.conn, openPayload)

	respPayload := readDataPayload(t, r.conn)
	checkNoError(t, sql, respPayload)

	return ""
}

// query executes a SELECT via TNS OPEN + FETCH and returns formatted
// results: column names on the first line, then data rows,
// space-separated.
func (r *testRunner) query(t *testing.T, input string) string {
	t.Helper()
	sql := strings.TrimSpace(input)

	// OPEN cursor to get column metadata and buffer results.
	openPayload := tnswire.EncodeTTIOpen(tnswire.TTIOpenMsg{
		CursorID: 100,
		SQL:      sql,
	})
	writeDataPayload(t, r.conn, openPayload)

	respPayload := readDataPayload(t, r.conn)
	require.True(t, len(respPayload) >= 5, "OPEN response too short")

	resp, err := tnswire.DecodeTTIOpenResponse(respPayload)
	require.NoError(t, err)

	cols := resp.Columns
	if len(cols) == 0 {
		return ""
	}

	// FETCH all rows.
	var allRows [][][]byte
	for {
		fetchPayload := tnswire.EncodeTTIFetch(tnswire.TTIFetchMsg{
			CursorID:  100,
			FetchSize: 100,
		})
		writeDataPayload(t, r.conn, fetchPayload)

		fetchResp := readDataPayload(t, r.conn)
		fr, fetchErr := tnswire.DecodeTTIFetchResponse(fetchResp, len(cols))
		require.NoError(t, fetchErr)

		allRows = append(allRows, fr.Rows...)
		if fr.Flags&tnswire.FetchFlagMoreRows == 0 {
			break
		}
	}

	// CLOSE cursor.
	closePayload := tnswire.EncodeTTIClose(tnswire.TTICloseMsg{
		CursorID: 100,
	})
	writeDataPayload(t, r.conn, closePayload)

	// Format output.
	var buf strings.Builder
	for i, col := range cols {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(col.Name)
	}
	for _, row := range allRows {
		buf.WriteByte('\n')
		for j, val := range row {
			if j > 0 {
				buf.WriteByte(' ')
			}
			if val == nil {
				buf.WriteString("NULL")
			} else {
				buf.WriteString(string(val))
			}
		}
	}
	return buf.String()
}

// execError executes a SQL statement expecting failure and returns
// the error message.
func (r *testRunner) execError(t *testing.T, input string) string {
	t.Helper()
	sql := strings.TrimSpace(input)

	openPayload := tnswire.EncodeTTIOpen(tnswire.TTIOpenMsg{
		CursorID: 1,
		SQL:      sql,
	})
	writeDataPayload(t, r.conn, openPayload)

	respPayload := readDataPayload(t, r.conn)
	return extractError(t, respPayload)
}

// sortRows keeps the first line (column header) in place and sorts
// the remaining lines alphabetically.
func sortRows(s string) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= 2 {
		return s
	}
	dataLines := lines[1:]
	sort.Strings(dataLines)
	return lines[0] + "\n" + strings.Join(dataLines, "\n")
}

// checkNoError checks that a response payload does not indicate an
// error. For EXEC responses, error code is at bytes 5-6.
func checkNoError(t *testing.T, sql string, payload []byte) {
	t.Helper()
	if len(payload) < 7 {
		return
	}
	funcCode := tnswire.TTIFuncCode(payload[0])
	if funcCode == tnswire.TTIExec {
		errCode := binary.BigEndian.Uint16(payload[5:7])
		if errCode != 0 {
			msg := ""
			if len(payload) >= 9 {
				msgLen := int(binary.BigEndian.Uint16(payload[7:9]))
				if len(payload) >= 9+msgLen {
					msg = string(payload[9 : 9+msgLen])
				}
			}
			t.Fatalf("exec %q failed with error %d: %s", sql, errCode, msg)
		}
	}
}

// extractError extracts the error message from an EXEC error
// response. Returns the error string, or fails the test if no error.
func extractError(t *testing.T, payload []byte) string {
	t.Helper()
	if len(payload) < 7 {
		t.Fatal("expected error response but payload too short")
	}
	funcCode := tnswire.TTIFuncCode(payload[0])
	if funcCode != tnswire.TTIExec {
		t.Fatalf("expected EXEC response, got func code 0x%02x", byte(funcCode))
	}
	errCode := binary.BigEndian.Uint16(payload[5:7])
	if errCode == 0 {
		t.Fatal("expected error but got success")
	}
	msg := ""
	if len(payload) >= 9 {
		msgLen := int(binary.BigEndian.Uint16(payload[7:9]))
		if len(payload) >= 9+msgLen {
			msg = string(payload[9 : 9+msgLen])
		}
	}
	return msg
}

// --- Wire protocol helpers ---

// perOpTimeout is the deadline applied to each individual read or write
// on the TNS test connection. Unlike a single connection-wide deadline,
// this resets before every operation so that long-running test files
// (many queries) don't hit a cumulative timeout.
const perOpTimeout = 30 * time.Second

func writeDataPayload(t *testing.T, conn net.Conn, ttiPayload []byte) {
	t.Helper()
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(perOpTimeout)))
	dataPayload := tnswire.EncodeData(tnswire.DataPacket{
		Flags:   0,
		Payload: ttiPayload,
	})
	require.NoError(t, tnswire.WritePacket(conn, tnswire.PacketTypeData, dataPayload))
}

func readDataPayload(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(perOpTimeout)))
	hdr, payload, err := tnswire.ReadPacket(conn)
	require.NoError(t, err)
	require.Equal(t, tnswire.PacketTypeData, hdr.Type)

	dataPkt, err := tnswire.DecodeData(payload)
	require.NoError(t, err)
	return dataPkt.Payload
}

// --- Auth helpers ---

func buildAuthRequest(username string) []byte {
	kvPairs := map[string]string{
		"AUTH_ACE":        username,
		"AUTH_TERMINAL":   "test",
		"AUTH_PROGRAM_NM": "test",
		"AUTH_MACHINE":    "test",
		"AUTH_PID":        "1234",
		"AUTH_SID":        "test",
	}
	return append([]byte{byte(auth.TTIAuth)}, encodeKVPairs(kvPairs)...)
}

func parseChallenge(t *testing.T, payload []byte) (sessKey, salt string) {
	t.Helper()
	require.Equal(t, byte(auth.TTIAuth), payload[0])

	kv := decodeKVPairs(t, payload[1:])
	sessKey = kv["AUTH_SESSKEY"]
	salt = kv["AUTH_VFR_DATA"]
	require.NotEmpty(t, sessKey, "server session key missing")
	require.NotEmpty(t, salt, "salt missing")
	return sessKey, salt
}

func buildAuthResponse(t *testing.T, serverSessKeyHex, saltHex, password string) []byte {
	t.Helper()

	clientSessKey := make([]byte, auth.SessKeyLen())
	_, err := rand.Read(clientSessKey)
	require.NoError(t, err)

	encPwd, clientSessKeyHex, err := auth.EncryptO5LOGONPassword(
		serverSessKeyHex, clientSessKey, password, saltHex,
	)
	require.NoError(t, err)

	kvPairs := map[string]string{
		"AUTH_PASSWORD": encPwd,
		"AUTH_SESSKEY":  clientSessKeyHex,
	}
	return append([]byte{byte(auth.TTIAuth)}, encodeKVPairs(kvPairs)...)
}

func encodeKVPairs(pairs map[string]string) []byte {
	size := 2
	for k, v := range pairs {
		size += 2 + len(k) + 2 + len(v)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(pairs)))
	off := 2
	for k, v := range pairs {
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(v)))
		off += 2
		copy(buf[off:], v)
		off += len(v)
	}
	return buf
}

func decodeKVPairs(t *testing.T, data []byte) map[string]string {
	t.Helper()
	require.True(t, len(data) >= 2)
	numPairs := int(binary.BigEndian.Uint16(data[0:2]))
	result := make(map[string]string, numPairs)
	off := 2
	for i := 0; i < numPairs; i++ {
		require.True(t, off+2 <= len(data))
		keyLen := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		require.True(t, off+keyLen <= len(data))
		key := string(data[off : off+keyLen])
		off += keyLen
		require.True(t, off+2 <= len(data))
		valLen := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		require.True(t, off+valLen <= len(data))
		result[key] = string(data[off : off+valLen])
		off += valLen
	}
	return result
}

// TestLogic walks the testdata directory and runs each file as a TNS
// logic test against a real CockroachDB server.
func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	addr, cleanup := startTNSServer(t)
	defer cleanup()

	datadriven.Walk(t, datapathutils.TestDataPath(t),
		func(t *testing.T, path string) {
			runner := newRunner(t, addr)
			defer runner.close()

			datadriven.RunTest(t, path,
				func(t *testing.T, d *datadriven.TestData) string {
					switch d.Cmd {
					case "exec":
						return runner.exec(t, d.Input)
					case "query":
						result := runner.query(t, d.Input)
						if d.HasArg("rowsort") {
							result = sortRows(result)
						}
						return result
					case "error":
						return runner.execError(t, d.Input)
					default:
						d.Fatalf(t, "unknown command: %s", d.Cmd)
						return ""
					}
				})
		})
}
