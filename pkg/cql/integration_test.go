// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

// startTestServer starts a CQL server on a random TCP port and returns
// the listener address and a cleanup function. The server runs in the
// background and is stopped when cleanup is called.
func startTestServer(t *testing.T, cfg ServerConfig) (addr string, cleanup func()) {
	t.Helper()
	stopper := stop.NewStopper()
	s := MakeServer(cfg)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, stopper.RunAsyncTask(ctx, "cql-serve", func(ctx context.Context) {
		_ = s.Serve(ctx, stopper, ln)
	}))

	return ln.Addr().String(), func() {
		stopper.Stop(ctx)
	}
}

// dialCQL dials the CQL server at addr and returns the connection.
func dialCQL(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	return conn
}

// cqlHandshake sends STARTUP and reads the READY response.
func cqlHandshake(t *testing.T, conn net.Conn) {
	t.Helper()
	sendStartup(t, conn, map[string]string{
		"CQL_VERSION": "3.4.5",
	})
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpReady, frame.Header.Opcode,
		"expected READY after STARTUP")
}

// cqlHandshakeWithAuth sends STARTUP, reads AUTHENTICATE, sends
// AUTH_RESPONSE, and reads AUTH_SUCCESS.
func cqlHandshakeWithAuth(t *testing.T, conn net.Conn, user, pass string) {
	t.Helper()
	sendStartup(t, conn, map[string]string{
		"CQL_VERSION": "3.4.5",
	})
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpAuthenticate, frame.Header.Opcode,
		"expected AUTHENTICATE after STARTUP")

	sendAuthResponse(t, conn, user, pass)
	frame, err = cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpAuthSuccess, frame.Header.Opcode,
		"expected AUTH_SUCCESS after AUTH_RESPONSE")
}

// sendQuery writes a CQL QUERY frame with the given query string and
// consistency level.
func sendQuery(
	t *testing.T, conn net.Conn, streamID int16, query string, consistency cqlwire.Consistency,
) {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteLongString(&body, query))
	require.NoError(t, cqlwire.WriteConsistency(&body, consistency))
	// Query flags: 0 (no values, no paging, etc.)
	require.NoError(t, cqlwire.WriteBytes(&body, []byte{0}))
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: streamID,
			Opcode:   cqlwire.OpQuery,
		}, body.Bytes(),
	))
}

// readError reads a CQL frame and asserts it is an ERROR response.
// Returns the error code and error message.
func readError(t *testing.T, conn net.Conn) (int32, string) {
	t.Helper()
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpError, frame.Header.Opcode,
		"expected ERROR frame")
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version)

	r := bytes.NewReader(frame.Body)
	code, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	msg, err := cqlwire.ReadString(r)
	require.NoError(t, err)
	return code, msg
}

// TestIntegrationHandshakeInsecure tests the CQL handshake over TCP
// without authentication.
func TestIntegrationHandshakeInsecure(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)
}

// TestIntegrationHandshakeWithAuth tests the CQL handshake over TCP
// with password authentication.
func TestIntegrationHandshakeWithAuth(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{
		Authenticator: AllowAllAuthenticator{},
	})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshakeWithAuth(t, conn, "cassandra", "cassandra")
}

// TestIntegrationOptionsBeforeStartup tests sending OPTIONS before
// STARTUP over TCP.
func TestIntegrationOptionsBeforeStartup(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	// Send OPTIONS.
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version: cqlwire.ProtoV4Request,
			Opcode:  cqlwire.OpOptions,
		}, nil,
	))

	// Expect SUPPORTED.
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpSupported, frame.Header.Opcode)
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version)

	// Parse the SUPPORTED body to verify CQL_VERSION is present.
	r := bytes.NewReader(frame.Body)
	supported, err := cqlwire.ReadStringMultiMap(r)
	require.NoError(t, err)
	require.Contains(t, supported, "CQL_VERSION")
	require.Contains(t, supported["CQL_VERSION"], "3.4.5")

	// Now send STARTUP.
	cqlHandshake(t, conn)
}

// TestIntegrationMultipleConnections tests that the server can handle
// multiple simultaneous connections.
func TestIntegrationMultipleConnections(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	const numConns = 5
	conns := make([]net.Conn, numConns)
	for i := range conns {
		conns[i] = dialCQL(t, addr)
		defer conns[i].Close()
	}

	// Handshake all connections.
	for _, c := range conns {
		cqlHandshake(t, c)
	}

	// Each connection should independently respond to queries.
	for i, c := range conns {
		streamID := int16(i + 1)
		sendQuery(t, c, streamID, "SELECT * FROM system.local",
			cqlwire.ConsistencyOne)
		frame, err := cqlwire.ReadFrame(c)
		require.NoError(t, err)
		require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
			"system.local should return a RESULT")
		require.Equal(t, streamID, frame.Header.StreamID,
			"stream ID should be echoed back")
	}
}

// TestIntegrationStreamIDPreservation tests that the server correctly
// echoes back stream IDs in responses.
func TestIntegrationStreamIDPreservation(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// Send queries with different stream IDs and verify echo-back.
	streamIDs := []int16{0, 1, 42, 127, 256, 32767}
	for _, sid := range streamIDs {
		sendQuery(t, conn, sid, "SELECT 1", cqlwire.ConsistencyOne)
		frame, err := cqlwire.ReadFrame(conn)
		require.NoError(t, err)
		require.Equal(t, sid, frame.Header.StreamID,
			"stream ID %d should be echoed", sid)
	}
}

// TestIntegrationQueryErrorResponse tests that QUERY frames get proper
// error responses with the current server (query execution not yet
// implemented).
func TestIntegrationQueryErrorResponse(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	tests := []struct {
		name  string
		query string
	}{
		{"create_keyspace", "CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"},
		{"use_keyspace", "USE test_ks"},
		{"create_table", "CREATE TABLE test_ks.users (id uuid PRIMARY KEY, name text)"},
		{"insert", "INSERT INTO test_ks.users (id, name) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Alice')"},
		{"select_star", "SELECT * FROM test_ks.users"},
		{"select_where", "SELECT name FROM test_ks.users WHERE id = 550e8400-e29b-41d4-a716-446655440000"},
		{"select_limit", "SELECT * FROM test_ks.users LIMIT 10"},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamID := int16(i + 1)
			sendQuery(t, conn, streamID, tt.query,
				cqlwire.ConsistencyOne)
			frame, err := cqlwire.ReadFrame(conn)
			require.NoError(t, err)
			require.Equal(t, cqlwire.OpError, frame.Header.Opcode,
				"QUERY should return ERROR")
			require.Equal(t, streamID, frame.Header.StreamID)
			require.Equal(t, cqlwire.ProtoV4Response,
				frame.Header.Version)

			// Parse the error to verify it is well-formed.
			r := bytes.NewReader(frame.Body)
			code, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			require.Equal(t, errCodeServerError, code,
				"error code should be SERVER_ERROR")
			msg, err := cqlwire.ReadString(r)
			require.NoError(t, err)
			require.NotEmpty(t, msg)
		})
	}
}

// sendPrepare writes a CQL PREPARE frame with the given query.
func sendPrepare(t *testing.T, conn net.Conn, streamID int16, query string) {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteLongString(&body, query))
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: streamID,
			Opcode:   cqlwire.OpPrepare,
		}, body.Bytes(),
	))
}

// readPreparedResult reads a CQL RESULT Prepared response and returns
// the prepared ID and bind variable count.
func readPreparedResult(t *testing.T, conn net.Conn) (preparedID []byte, bindCount int32) {
	t.Helper()
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
		"expected RESULT frame for PREPARE")
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version)

	r := bytes.NewReader(frame.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, resultKindPrepared, kind,
		"expected Prepared result kind")

	preparedID, err = cqlwire.ReadShortBytes(r)
	require.NoError(t, err)
	require.NotEmpty(t, preparedID, "prepared ID should not be empty")

	// Read bind variables metadata.
	_, err = cqlwire.ReadInt(r) // flags
	require.NoError(t, err)
	bindCount, err = cqlwire.ReadInt(r)
	require.NoError(t, err)

	return preparedID, bindCount
}

// sendExecute writes a CQL EXECUTE frame with the given prepared ID
// and bound string values.
func sendExecute(
	t *testing.T,
	conn net.Conn,
	streamID int16,
	preparedID []byte,
	consistency cqlwire.Consistency,
	values [][]byte,
) {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteShortBytes(&body, preparedID))
	require.NoError(t, cqlwire.WriteConsistency(&body, consistency))

	if len(values) > 0 {
		body.WriteByte(0x01) // flags: VALUES
		require.NoError(t, cqlwire.WriteShort(&body, uint16(len(values))))
		for _, val := range values {
			require.NoError(t, cqlwire.WriteBytes(&body, val))
		}
	} else {
		body.WriteByte(0x00) // flags: none
	}

	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: streamID,
			Opcode:   cqlwire.OpExecute,
		}, body.Bytes(),
	))
}

// TestIntegrationPrepareSuccess tests that a valid PREPARE frame
// returns a RESULT Prepared response with the correct bind variable
// count.
func TestIntegrationPrepareSuccess(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	tests := []struct {
		name              string
		query             string
		expectedBindCount int32
	}{
		{
			"no_bind_markers",
			"SELECT * FROM system.local",
			0,
		},
		{
			"one_bind_marker",
			"SELECT * FROM users WHERE id = ?",
			1,
		},
		{
			"two_bind_markers",
			"INSERT INTO users (id, name) VALUES (?, ?)",
			2,
		},
		{
			"bind_marker_in_string_ignored",
			"SELECT * FROM users WHERE name = '?literal'",
			0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sendPrepare(t, conn, int16(i+1), tt.query)
			preparedID, bindCount := readPreparedResult(t, conn)
			require.NotEmpty(t, preparedID)
			require.Equal(t, tt.expectedBindCount, bindCount)
		})
	}
}

// TestIntegrationPrepareSyntaxError tests that PREPARE with an
// invalid CQL query returns an ERROR response.
func TestIntegrationPrepareSyntaxError(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendPrepare(t, conn, 1, "NOT VALID CQL SYNTAX")
	code, _ := readError(t, conn)
	require.Equal(t, errCodeSyntax, code)
}

// TestIntegrationPrepareStreamID tests that PREPARE echoes the
// request's stream ID.
func TestIntegrationPrepareStreamID(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendPrepare(t, conn, 42, "SELECT * FROM system.local")

	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode)
	require.Equal(t, int16(42), frame.Header.StreamID)
}

// TestIntegrationPrepareSameQuerySameID tests that preparing the
// same query twice returns the same prepared ID (deterministic MD5).
func TestIntegrationPrepareSameQuerySameID(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	query := "SELECT * FROM system.local WHERE key = ?"

	sendPrepare(t, conn, 1, query)
	id1, _ := readPreparedResult(t, conn)

	sendPrepare(t, conn, 2, query)
	id2, _ := readPreparedResult(t, conn)

	require.Equal(t, id1, id2,
		"same query should produce same prepared ID")
}

// TestIntegrationExecuteUnprepared tests that EXECUTE with an
// unknown prepared ID returns an Unprepared error.
func TestIntegrationExecuteUnprepared(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	fakeID := make([]byte, 16)
	sendExecute(t, conn, 1, fakeID, cqlwire.ConsistencyOne, nil)

	// Read the error frame manually to check the Unprepared code.
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpError, frame.Header.Opcode)

	r := bytes.NewReader(frame.Body)
	code, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, errCodeUnprepared, code,
		"expected Unprepared error code 0x2500")
}

// TestIntegrationPrepareExecuteSystemLocal tests the full
// PREPARE → EXECUTE cycle for a system.local query. This is the
// most critical path for gocql driver compatibility.
func TestIntegrationPrepareExecuteSystemLocal(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// PREPARE: SELECT * FROM system.local
	sendPrepare(t, conn, 1,
		"SELECT * FROM system.local WHERE key='local'")
	preparedID, bindCount := readPreparedResult(t, conn)
	require.Equal(t, int32(0), bindCount,
		"no bind markers in this query")

	// EXECUTE: no bound values.
	sendExecute(t, conn, 2, preparedID, cqlwire.ConsistencyOne, nil)

	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
		"system.local via EXECUTE should return RESULT")
	require.Equal(t, int16(2), frame.Header.StreamID)

	// Verify it's a Rows result.
	r := bytes.NewReader(frame.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, resultKindRows, kind,
		"system.local should return Rows")
}

// TestIntegrationPrepareExecuteWithBindValues tests PREPARE and
// EXECUTE with bound string values substituted into the query.
func TestIntegrationPrepareExecuteWithBindValues(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// PREPARE: query with bind marker.
	sendPrepare(t, conn, 1,
		"SELECT * FROM system.local WHERE key = ?")
	preparedID, bindCount := readPreparedResult(t, conn)
	require.Equal(t, int32(1), bindCount)

	// EXECUTE: bind value 'local'.
	sendExecute(t, conn, 2, preparedID, cqlwire.ConsistencyOne,
		[][]byte{[]byte("local")})

	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
		"EXECUTE with bind values should return RESULT")

	r := bytes.NewReader(frame.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, resultKindRows, kind)
}

// TestIntegrationPrepareExecuteNullBindValue tests EXECUTE with a
// NULL bind value (nil byte slice).
func TestIntegrationPrepareExecuteNullBindValue(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendPrepare(t, conn, 1,
		"SELECT * FROM system.local WHERE key = ?")
	preparedID, _ := readPreparedResult(t, conn)

	// EXECUTE with NULL bind value.
	sendExecute(t, conn, 2, preparedID, cqlwire.ConsistencyOne,
		[][]byte{nil})

	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	// Should get a valid response (either RESULT or ERROR, but
	// not a protocol error).
	require.True(t,
		frame.Header.Opcode == cqlwire.OpResult ||
			frame.Header.Opcode == cqlwire.OpError,
		"expected RESULT or ERROR, got %s", frame.Header.Opcode)
}

// TestIntegrationBatchErrorResponse tests that BATCH frames get proper
// error responses.
func TestIntegrationBatchErrorResponse(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// Send a minimal BATCH frame (empty body is fine; the server
	// returns an error before parsing the body).
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: 2,
			Opcode:   cqlwire.OpBatch,
		}, nil,
	))

	code, msg := readError(t, conn)
	require.Equal(t, errCodeServerError, code)
	require.Contains(t, msg, "not yet implemented")
}

// TestIntegrationRegisterReady tests that REGISTER frames get a READY
// response. cqlsh sends REGISTER after STARTUP to subscribe to
// schema/topology change events.
func TestIntegrationRegisterReady(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// Build a REGISTER body with event types that cqlsh typically sends.
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteStringList(&body, []string{
		"TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE",
	}))
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: 3,
			Opcode:   cqlwire.OpRegister,
		}, body.Bytes(),
	))

	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpReady, frame.Header.Opcode,
		"REGISTER should return READY")
	require.Equal(t, int16(3), frame.Header.StreamID,
		"stream ID should be preserved")
}

// TestIntegrationDrainRejectsNewConnections tests that new TCP
// connections are rejected with an ERROR frame when the server is
// draining.
func TestIntegrationDrainRejectsNewConnections(t *testing.T) {
	stopper := stop.NewStopper()
	s := MakeServer(ServerConfig{Insecure: true})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, stopper.RunAsyncTask(ctx, "cql-serve", func(ctx context.Context) {
		_ = s.Serve(ctx, stopper, ln)
	}))
	defer stopper.Stop(ctx)

	addr := ln.Addr().String()

	// Verify a connection works before draining.
	conn1 := dialCQL(t, addr)
	cqlHandshake(t, conn1)
	conn1.Close()

	// Start draining.
	go func() {
		_ = s.Drain(ctx, 10*time.Millisecond)
	}()
	// Wait for drain to take effect.
	time.Sleep(50 * time.Millisecond)

	// New connection should be rejected with an ERROR frame.
	conn2 := dialCQL(t, addr)
	defer conn2.Close()

	// The server sends an ERROR frame before closing.
	frame, err := cqlwire.ReadFrame(conn2)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpError, frame.Header.Opcode)
}

// TestIntegrationOptionsAfterReady tests that OPTIONS is handled in
// the ready state (after handshake).
func TestIntegrationOptionsAfterReady(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// Send OPTIONS in the ready state.
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: 10,
			Opcode:   cqlwire.OpOptions,
		}, nil,
	))

	// Expect SUPPORTED response.
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpSupported, frame.Header.Opcode)
	require.Equal(t, int16(10), frame.Header.StreamID)
}

// TestIntegrationConnectionMetrics tests that server metrics are
// updated for connections.
func TestIntegrationConnectionMetrics(t *testing.T) {
	stopper := stop.NewStopper()
	s := MakeServer(ServerConfig{Insecure: true})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, stopper.RunAsyncTask(ctx, "cql-serve", func(ctx context.Context) {
		_ = s.Serve(ctx, stopper, ln)
	}))
	defer stopper.Stop(ctx)

	addr := ln.Addr().String()
	metrics := s.Metrics()

	initialNew := metrics.NewConns.Count()

	conn := dialCQL(t, addr)
	cqlHandshake(t, conn)

	// Wait briefly for metrics to update.
	time.Sleep(50 * time.Millisecond)
	require.Greater(t, metrics.NewConns.Count(), initialNew,
		"NewConns should increment on connection")
	require.GreaterOrEqual(t, metrics.Conns.Value(), int64(1),
		"active connections should be at least 1")

	conn.Close()
	// Wait for the connection to fully close.
	time.Sleep(100 * time.Millisecond)
}

// TestIntegrationStartupMissingVersion tests that the server rejects
// a STARTUP frame without CQL_VERSION.
func TestIntegrationStartupMissingVersion(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	// Send STARTUP without CQL_VERSION.
	sendStartup(t, conn, map[string]string{
		"COMPRESSION": "snappy",
	})

	// Expect ERROR response.
	code, msg := readError(t, conn)
	require.Equal(t, errCodeProtocol, code)
	require.Contains(t, msg, "CQL_VERSION")
}

// TestIntegrationBytesInMetric tests that the BytesIn metric is
// updated when frames are received.
func TestIntegrationBytesInMetric(t *testing.T) {
	stopper := stop.NewStopper()
	s := MakeServer(ServerConfig{Insecure: true})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, stopper.RunAsyncTask(ctx, "cql-serve", func(ctx context.Context) {
		_ = s.Serve(ctx, stopper, ln)
	}))
	defer stopper.Stop(ctx)

	addr := ln.Addr().String()
	metrics := s.Metrics()

	initialBytesIn := metrics.BytesIn.Count()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	// Wait for metrics to catch up.
	time.Sleep(50 * time.Millisecond)
	require.Greater(t, metrics.BytesIn.Count(), initialBytesIn,
		"BytesIn should increase after handshake")
}

// TestIntegrationGocqlConnection tests that the gocql driver can
// establish a TCP connection to the CQL server and create a session.
// The server returns synthetic results for system table queries
// (system.local, system.peers, system_schema.*) which allows gocql
// to complete its initialization handshake.
func TestIntegrationGocqlConnection(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	cluster := gocql.NewCluster(host)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	cluster.Port = p
	cluster.ProtoVersion = 4
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	// Disable authentication for insecure server.
	cluster.Authenticator = nil

	session, err := cluster.CreateSession()
	require.NoError(t, err, "gocql CreateSession should succeed")
	session.Close()
}

// TestIntegrationGocqlConnectionWithAuth tests that the gocql driver
// can perform password authentication and create a session against
// the CQL server.
func TestIntegrationGocqlConnectionWithAuth(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{
		Authenticator: AllowAllAuthenticator{},
	})
	defer cleanup()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	cluster := gocql.NewCluster(host)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	cluster.Port = p
	cluster.ProtoVersion = 4
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	session, err := cluster.CreateSession()
	require.NoError(t, err, "gocql CreateSession should succeed with auth")
	session.Close()
}

// TestIntegrationCreateKeyspaceViaWire tests sending a CREATE KEYSPACE
// query over the wire. Since query execution is not yet implemented, this
// verifies the error response is well-formed. This test is structured to
// be easily updated once query execution is implemented.
func TestIntegrationCreateKeyspaceViaWire(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendQuery(t, conn, 1,
		"CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
		cqlwire.ConsistencyOne,
	)

	code, msg := readError(t, conn)
	require.Equal(t, errCodeServerError, code)
	require.NotEmpty(t, msg)
}

// TestIntegrationUseKeyspaceViaWire tests sending a USE query over the
// wire.
func TestIntegrationUseKeyspaceViaWire(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendQuery(t, conn, 1, "USE test_ks", cqlwire.ConsistencyOne)

	code, msg := readError(t, conn)
	require.Equal(t, errCodeServerError, code)
	require.NotEmpty(t, msg)
}

// TestIntegrationCreateTableViaWire tests sending CREATE TABLE queries
// with various primary key structures over the wire.
func TestIntegrationCreateTableViaWire(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	tables := []struct {
		name  string
		query string
	}{
		{
			"single_pk",
			"CREATE TABLE users (id uuid, name text, email text, PRIMARY KEY (id))",
		},
		{
			"pk_with_clustering",
			"CREATE TABLE events (tenant_id text, ts timestamp, event_id uuid, data text, PRIMARY KEY (tenant_id, ts, event_id))",
		},
		{
			"composite_partition_key",
			"CREATE TABLE metrics (region text, host text, ts timestamp, val double, PRIMARY KEY ((region, host), ts))",
		},
		{
			"all_types",
			"CREATE TABLE type_test (a text, b int, c bigint, d float, e double, f boolean, g timestamp, h uuid, i blob, PRIMARY KEY (a))",
		},
	}

	for i, tt := range tables {
		t.Run(tt.name, func(t *testing.T) {
			sendQuery(t, conn, int16(i+1), tt.query,
				cqlwire.ConsistencyOne)
			code, msg := readError(t, conn)
			require.Equal(t, errCodeServerError, code)
			require.NotEmpty(t, msg)
		})
	}
}

// TestIntegrationInsertViaWire tests sending INSERT queries over the
// wire.
func TestIntegrationInsertViaWire(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	inserts := []struct {
		name  string
		query string
	}{
		{
			"text_values",
			"INSERT INTO users (id, name, email) VALUES ('abc', 'Alice', 'alice@example.com')",
		},
		{
			"int_values",
			"INSERT INTO counters (id, count) VALUES ('key1', 42)",
		},
		{
			"bool_and_null",
			"INSERT INTO flags (id, active, note) VALUES ('key2', true, null)",
		},
		{
			"if_not_exists",
			"INSERT INTO users (id, name) VALUES ('new', 'Bob') IF NOT EXISTS",
		},
	}

	for i, tt := range inserts {
		t.Run(tt.name, func(t *testing.T) {
			sendQuery(t, conn, int16(i+1), tt.query,
				cqlwire.ConsistencyOne)
			code, msg := readError(t, conn)
			require.Equal(t, errCodeServerError, code)
			require.NotEmpty(t, msg)
		})
	}
}

// TestIntegrationSelectViaWire tests sending SELECT queries over the
// wire, including partition key filtering and LIMIT.
func TestIntegrationSelectViaWire(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	selects := []struct {
		name  string
		query string
	}{
		{
			"select_star",
			"SELECT * FROM users",
		},
		{
			"select_columns",
			"SELECT name, email FROM users",
		},
		{
			"partition_key_filter",
			"SELECT * FROM users WHERE id = 'abc'",
		},
		{
			"select_with_limit",
			"SELECT * FROM users LIMIT 10",
		},
		{
			"where_and_limit",
			"SELECT name FROM events WHERE tenant_id = 'acme' LIMIT 100",
		},
		{
			"range_filter",
			"SELECT * FROM events WHERE tenant_id = 'acme' AND ts > 1000",
		},
	}

	for i, tt := range selects {
		t.Run(tt.name, func(t *testing.T) {
			sendQuery(t, conn, int16(i+1), tt.query,
				cqlwire.ConsistencyOne)
			code, msg := readError(t, conn)
			require.Equal(t, errCodeServerError, code)
			require.NotEmpty(t, msg)
		})
	}
}

// TestIntegrationProtocolVersionResponse verifies that all response
// frames use the correct CQL v4 response version byte (0x84).
func TestIntegrationProtocolVersionResponse(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	// OPTIONS response.
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version: cqlwire.ProtoV4Request,
			Opcode:  cqlwire.OpOptions,
		}, nil,
	))
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version,
		"SUPPORTED response must use v4 response version")

	// READY response.
	sendStartup(t, conn, map[string]string{"CQL_VERSION": "3.4.5"})
	frame, err = cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version,
		"READY response must use v4 response version")

	// ERROR response (from QUERY).
	sendQuery(t, conn, 1, "SELECT 1", cqlwire.ConsistencyOne)
	frame, err = cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version,
		"ERROR response must use v4 response version")
}

// TestIntegrationConnectionCloseCleanup tests that the server handles
// client disconnection gracefully.
func TestIntegrationConnectionCloseCleanup(t *testing.T) {
	stopper := stop.NewStopper()
	s := MakeServer(ServerConfig{Insecure: true})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, stopper.RunAsyncTask(ctx, "cql-serve", func(ctx context.Context) {
		_ = s.Serve(ctx, stopper, ln)
	}))
	defer stopper.Stop(ctx)

	addr := ln.Addr().String()
	metrics := s.Metrics()

	// Open and close several connections.
	for i := 0; i < 3; i++ {
		conn := dialCQL(t, addr)
		cqlHandshake(t, conn)
		conn.Close()
	}

	// Wait for cleanup.
	time.Sleep(200 * time.Millisecond)

	// All connections should be closed now.
	require.Equal(t, int64(0), metrics.Conns.Value(),
		"all connections should be cleaned up")
	require.Equal(t, int64(3), metrics.NewConns.Count(),
		"should have seen 3 connections total")
}

// TestIntegrationSystemLocal tests that system.local returns a valid
// RESULT with the expected cluster metadata.
func TestIntegrationSystemLocal(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendQuery(t, conn, 1, "SELECT * FROM system.local WHERE key='local'",
		cqlwire.ConsistencyOne)
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
		"system.local should return RESULT, not ERROR")

	// Parse the RESULT body to verify it has rows.
	r := bytes.NewReader(frame.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, resultKindRows, kind,
		"result should be Rows kind")

	// Read metadata flags and column count.
	_, err = cqlwire.ReadInt(r) // flags
	require.NoError(t, err)
	colCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Greater(t, colCount, int32(0),
		"system.local should have columns")
}

// TestIntegrationSystemPeers tests that system.peers returns a valid
// empty RESULT.
func TestIntegrationSystemPeers(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	sendQuery(t, conn, 1, "SELECT * FROM system.peers",
		cqlwire.ConsistencyOne)
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
		"system.peers should return RESULT")
}

// TestIntegrationSystemSchemaTables tests that system_schema tables
// return valid RESULT responses.
func TestIntegrationSystemSchemaTables(t *testing.T) {
	addr, cleanup := startTestServer(t, ServerConfig{Insecure: true})
	defer cleanup()

	conn := dialCQL(t, addr)
	defer conn.Close()

	cqlHandshake(t, conn)

	tables := []string{
		"SELECT * FROM system_schema.keyspaces",
		"SELECT * FROM system_schema.tables",
		"SELECT * FROM system_schema.columns",
	}

	for i, query := range tables {
		sendQuery(t, conn, int16(i+1), query, cqlwire.ConsistencyOne)
		frame, err := cqlwire.ReadFrame(conn)
		require.NoError(t, err)
		require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
			"query %q should return RESULT", query)
	}
}
