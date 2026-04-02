// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tns_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/tns"
	"github.com/cockroachdb/cockroach/pkg/tns/auth"
	"github.com/cockroachdb/cockroach/pkg/tns/tnswire"
	"github.com/stretchr/testify/require"
)

// TestTNSServerStartStop verifies that a TNS server can start and stop
// cleanly without a CRDB backend.
func TestTNSServerStartStop(t *testing.T) {
	srv := tns.NewServer(tns.ServerConfig{
		ListenAddr: ":0", // random port
		Insecure:   true,
	})

	ctx := context.Background()
	require.NoError(t, srv.Start(ctx))
	require.NotNil(t, srv.Addr())
	t.Logf("TNS server listening on %s", srv.Addr())

	srv.Stop()

	metrics := srv.Metrics()
	require.Equal(t, int64(0), metrics.ActiveConns)
}

// TestTNSConnectAccept verifies the CONNECT/ACCEPT handshake works.
func TestTNSConnectAccept(t *testing.T) {
	srv := tns.NewServer(tns.ServerConfig{
		ListenAddr: ":0",
		Insecure:   true,
	})

	ctx := context.Background()
	require.NoError(t, srv.Start(ctx))
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Send CONNECT packet.
	connectData := "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))"
	connectPayload := tnswire.EncodeConnect(tnswire.ConnectPacket{
		Version:        auth.ProtocolVersion,
		MinVersion:     auth.MinProtocolVersion,
		ServiceOptions: 0,
		SDUSize:        auth.DefaultSDUSize,
		TDUSize:        auth.DefaultTDUSize,
		ValueOfOne:     1,
		ConnectData:    connectData,
	})
	require.NoError(t, tnswire.WritePacket(conn, tnswire.PacketTypeConnect, connectPayload))

	// Read ACCEPT response.
	hdr, payload, err := tnswire.ReadPacket(conn)
	require.NoError(t, err)
	require.Equal(t, tnswire.PacketTypeAccept, hdr.Type)

	acceptPkt, err := tnswire.DecodeAccept(payload)
	require.NoError(t, err)
	require.True(t, acceptPkt.Version >= auth.MinProtocolVersion)
	t.Logf("accepted with version %d, SDU=%d, TDU=%d",
		acceptPkt.Version, acceptPkt.SDUSize, acceptPkt.TDUSize)
}

// TestTNSFullQueryLifecycle tests the complete lifecycle: connect →
// authenticate → OPEN → EXEC → FETCH → CLOSE against a real CRDB
// test server.
func TestTNSFullQueryLifecycle(t *testing.T) {
	// Start a real CRDB test server.
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Start a TNS server wired to the CRDB internal executor.
	tnsServer := tns.NewServer(tns.ServerConfig{
		ListenAddr:      ":0",
		Insecure:        true,
		DefaultDatabase: "defaultdb",
		DB:              s.InternalDB().(isql.DB),
	})

	ctx := context.Background()
	require.NoError(t, tnsServer.Start(ctx))
	defer tnsServer.Stop()

	addr := tnsServer.Addr().String()
	t.Logf("TNS test server on %s", addr)

	// Connect and authenticate.
	conn := dialAndAuth(t, addr)
	defer conn.Close()

	// 1. CREATE TABLE via OPEN+EXEC.
	execDDL(t, conn, 1, "CREATE TABLE test_tns (id NUMBER, name VARCHAR2(100))")

	// 2. INSERT rows via OPEN+EXEC.
	execDML(t, conn, 2, "INSERT INTO test_tns (id, name) VALUES (1, 'alice')")
	execDML(t, conn, 3, "INSERT INTO test_tns (id, name) VALUES (2, 'bob')")

	// 3. SELECT via OPEN+FETCH.
	cols := openCursor(t, conn, 4, "SELECT id, name FROM test_tns ORDER BY id")
	require.Len(t, cols, 2)
	require.Equal(t, "ID", cols[0].Name)
	require.Equal(t, "NAME", cols[1].Name)

	rows := fetchAll(t, conn, 4, len(cols))
	require.Len(t, rows, 2)
	require.Equal(t, "1", string(rows[0][0]))
	require.Equal(t, "alice", string(rows[0][1]))
	require.Equal(t, "2", string(rows[1][0]))
	require.Equal(t, "bob", string(rows[1][1]))

	// 4. Close cursor.
	closeCursor(t, conn, 4)

	t.Log("full query lifecycle passed")
}

// --- Test helpers ---

// dialAndAuth connects to the TNS server and performs the full
// CONNECT + protocol negotiation + data type negotiation + O5LOGON
// authentication handshake.
func dialAndAuth(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
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
	require.NoError(t, tnswire.WritePacket(conn, tnswire.PacketTypeConnect, connectPayload))

	hdr, _, err := tnswire.ReadPacket(conn)
	require.NoError(t, err)
	require.Equal(t, tnswire.PacketTypeAccept, hdr.Type)

	// 2. Protocol negotiation.
	protoNeg := []byte{byte(auth.TTIProtocolNeg), 0x06, 0x00, 0x00, 0x00, 0x00}
	writeDataPayload(t, conn, protoNeg)
	readDataPayload(t, conn) // read server response

	// 3. Data type negotiation.
	dtNeg := []byte{byte(auth.TTIDataTypeNeg), 0x00, 0x00}
	writeDataPayload(t, conn, dtNeg)
	readDataPayload(t, conn) // read server response

	// 4. O5LOGON auth — send initial auth request.
	authReq := buildAuthRequest("testuser")
	writeDataPayload(t, conn, authReq)

	// Read server challenge.
	challengePayload := readDataPayload(t, conn)
	require.True(t, len(challengePayload) > 0)

	// Parse challenge to extract session key and salt.
	sessKey, salt := parseChallenge(t, challengePayload)

	// Send auth response with encrypted password.
	authResp := buildAuthResponse(t, sessKey, salt, "testpass")
	writeDataPayload(t, conn, authResp)

	// Read auth success.
	successPayload := readDataPayload(t, conn)
	require.True(t, len(successPayload) > 0)
	require.Equal(t, byte(auth.TTIAuthResponse), successPayload[0])
	require.Equal(t, byte(0x00), successPayload[1]) // success status

	return conn
}

// buildAuthRequest builds the initial O5LOGON auth request with username.
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

// parseChallenge extracts the session key and salt from the server's
// auth challenge.
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

// buildAuthResponse builds the client's auth response with the encrypted
// password.
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

// execDDL opens a cursor with DDL SQL and executes it.
func execDDL(t *testing.T, conn net.Conn, cursorID uint16, sql string) {
	t.Helper()

	// OPEN
	openPayload := tnswire.EncodeTTIOpen(tnswire.TTIOpenMsg{
		CursorID: cursorID,
		SQL:      sql,
	})
	writeDataPayload(t, conn, openPayload)

	respPayload := readDataPayload(t, conn)
	// Could be OPEN response or EXEC response; just check for errors.
	checkNoExecError(t, respPayload)
}

// execDML opens a cursor with DML SQL and executes it.
func execDML(t *testing.T, conn net.Conn, cursorID uint16, sql string) {
	t.Helper()
	execDDL(t, conn, cursorID, sql) // same flow for DDL and DML
}

// openCursor sends a TTI OPEN and returns column metadata.
func openCursor(t *testing.T, conn net.Conn, cursorID uint16, sql string) []tnswire.ColumnDesc {
	t.Helper()

	openPayload := tnswire.EncodeTTIOpen(tnswire.TTIOpenMsg{
		CursorID: cursorID,
		SQL:      sql,
	})
	writeDataPayload(t, conn, openPayload)

	respPayload := readDataPayload(t, conn)
	require.True(t, len(respPayload) >= 5)

	resp, err := tnswire.DecodeTTIOpenResponse(respPayload)
	require.NoError(t, err)
	return resp.Columns
}

// fetchAll fetches all rows from a cursor.
func fetchAll(t *testing.T, conn net.Conn, cursorID uint16, numCols int) [][][]byte {
	t.Helper()

	var allRows [][][]byte
	for {
		fetchPayload := tnswire.EncodeTTIFetch(tnswire.TTIFetchMsg{
			CursorID:  cursorID,
			FetchSize: 100,
		})
		writeDataPayload(t, conn, fetchPayload)

		respPayload := readDataPayload(t, conn)
		resp, err := tnswire.DecodeTTIFetchResponse(respPayload, numCols)
		require.NoError(t, err)

		allRows = append(allRows, resp.Rows...)
		if resp.Flags&tnswire.FetchFlagMoreRows == 0 {
			break
		}
	}
	return allRows
}

// closeCursor sends a TTI CLOSE for the given cursor.
func closeCursor(t *testing.T, conn net.Conn, cursorID uint16) {
	t.Helper()
	closePayload := tnswire.EncodeTTIClose(tnswire.TTICloseMsg{
		CursorID: cursorID,
	})
	writeDataPayload(t, conn, closePayload)
	// CLOSE has no response.
}

// checkNoExecError checks that a response payload does not contain an
// error. For EXEC responses (func code 0x04), error code is at bytes 5-6.
func checkNoExecError(t *testing.T, payload []byte) {
	t.Helper()
	if len(payload) < 7 {
		return
	}
	funcCode := tnswire.TTIFuncCode(payload[0])
	if funcCode == tnswire.TTIExec {
		errCode := binary.BigEndian.Uint16(payload[5:7])
		if errCode != 0 {
			// Extract error message.
			msg := ""
			if len(payload) >= 9 {
				msgLen := int(binary.BigEndian.Uint16(payload[7:9]))
				if len(payload) >= 9+msgLen {
					msg = string(payload[9 : 9+msgLen])
				}
			}
			t.Fatalf("unexpected exec error %d: %s", errCode, msg)
		}
	}
}

// writeDataPayload wraps a TTI payload in a DATA packet and sends it.
func writeDataPayload(t *testing.T, conn net.Conn, ttiPayload []byte) {
	t.Helper()
	dataPayload := tnswire.EncodeData(tnswire.DataPacket{
		Flags:   0,
		Payload: ttiPayload,
	})
	require.NoError(t, tnswire.WritePacket(conn, tnswire.PacketTypeData, dataPayload))
}

// readDataPayload reads a DATA packet and returns the TTI payload.
func readDataPayload(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	hdr, payload, err := tnswire.ReadPacket(conn)
	require.NoError(t, err)
	require.Equal(t, tnswire.PacketTypeData, hdr.Type)

	dataPkt, err := tnswire.DecodeData(payload)
	require.NoError(t, err)
	return dataPkt.Payload
}

// encodeKVPairs encodes auth KV pairs for the O5LOGON protocol.
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

// decodeKVPairs decodes auth KV pairs from the O5LOGON protocol.
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
