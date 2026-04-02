// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package smoketest_test contains end-to-end tests that start a full
// CockroachDB server with CQL support enabled and exercise the CQL
// native protocol over TCP using the cqlwire package.
package smoketest_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// sendStartup writes a CQL STARTUP frame with the given options.
func sendStartup(t *testing.T, conn net.Conn, opts map[string]string) {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteStringMap(&body, opts))
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version: cqlwire.ProtoV4Request,
			Opcode:  cqlwire.OpStartup,
		}, body.Bytes(),
	))
}

// sendQuery writes a CQL QUERY frame.
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

// readResult reads a CQL frame and returns it. Fails the test on
// read error.
func readResult(t *testing.T, conn net.Conn) cqlwire.Frame {
	t.Helper()
	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	return frame
}

// requireResult reads a CQL frame and asserts it is a RESULT (not
// ERROR). Returns the frame.
func requireResult(t *testing.T, conn net.Conn) cqlwire.Frame {
	t.Helper()
	frame := readResult(t, conn)
	if frame.Header.Opcode == cqlwire.OpError {
		r := bytes.NewReader(frame.Body)
		code, _ := cqlwire.ReadInt(r)
		msg, _ := cqlwire.ReadString(r)
		t.Fatalf("expected RESULT but got ERROR: code=0x%04x msg=%q",
			code, msg)
	}
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode,
		"expected RESULT frame")
	return frame
}

// startCQLServer starts a single-node CockroachDB cluster with CQL
// enabled on a random port. Returns the CQL listen address and a
// cleanup function.
func startCQLServer(t *testing.T) (cqlAddr string, cleanup func()) {
	t.Helper()

	// Create cluster settings with CQL enabled on port 0 (random).
	st := cluster.MakeClusterSettings()
	server.CQLEnabled.Override(
		context.Background(), &st.SV, true,
	)
	server.CQLPort.Override(
		context.Background(), &st.SV, 0,
	)

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings:                   st,
		Insecure:                   true,
		DefaultTestTenant:          base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		DisableElasticCPUAdmission: true,
	})

	// The CQL address is exposed through the underlying testServer
	// (via StorageLayer).
	type cqlAddrGetter interface {
		CQLAddr() string
	}
	raw := srv.StorageLayer().(cqlAddrGetter)
	addr := raw.CQLAddr()
	require.NotEmpty(t, addr, "CQL server should be running")

	return addr, func() {
		srv.Stopper().Stop(context.Background())
	}
}

// TestSmokeCreateKeyspaceAndTable verifies the full lifecycle of
// creating a keyspace (database), creating a table within it,
// inserting a row, and selecting it back -- all via CQL wire
// protocol against a real CockroachDB server.
func TestSmokeCreateKeyspaceAndTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	addr, cleanup := startCQLServer(t)
	defer cleanup()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(
		time.Now().Add(30*time.Second),
	))

	// CQL handshake.
	cqlHandshake(t, conn)

	// 1. CREATE KEYSPACE.
	sendQuery(t, conn, 1,
		"CREATE KEYSPACE smoke_ks WITH replication = "+
			"{'class': 'SimpleStrategy', 'replication_factor': '1'}",
		cqlwire.ConsistencyOne,
	)
	frame := requireResult(t, conn)
	r := bytes.NewReader(frame.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(0x0005), kind, // SCHEMA_CHANGE
		"CREATE KEYSPACE should return SCHEMA_CHANGE result")

	// 2. USE keyspace.
	sendQuery(t, conn, 2, "USE smoke_ks", cqlwire.ConsistencyOne)
	frame = requireResult(t, conn)
	r = bytes.NewReader(frame.Body)
	kind, err = cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(0x0003), kind, // SET_KEYSPACE
		"USE should return SET_KEYSPACE result")
	ks, err := cqlwire.ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "smoke_ks", ks)

	// 3. CREATE TABLE.
	sendQuery(t, conn, 3,
		"CREATE TABLE users "+
			"(id uuid, name text, age int, PRIMARY KEY (id))",
		cqlwire.ConsistencyOne,
	)
	frame = requireResult(t, conn)
	r = bytes.NewReader(frame.Body)
	kind, err = cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(0x0005), kind,
		"CREATE TABLE should return SCHEMA_CHANGE result")

	// 4. INSERT a row.
	sendQuery(t, conn, 4,
		"INSERT INTO users (id, name, age) VALUES "+
			"('550e8400-e29b-41d4-a716-446655440000', 'Alice', 30)",
		cqlwire.ConsistencyOne,
	)
	frame = requireResult(t, conn)
	r = bytes.NewReader(frame.Body)
	kind, err = cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(0x0001), kind, // VOID
		"INSERT should return VOID result")

	// 5. SELECT the row back.
	sendQuery(t, conn, 5,
		"SELECT * FROM users WHERE id = "+
			"'550e8400-e29b-41d4-a716-446655440000'",
		cqlwire.ConsistencyOne,
	)
	frame = requireResult(t, conn)
	r = bytes.NewReader(frame.Body)
	kind, err = cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(0x0002), kind, // ROWS
		"SELECT should return ROWS result")

	// Parse metadata.
	flags, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	_ = flags

	colCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.GreaterOrEqual(t, colCount, int32(3),
		"should have at least 3 columns (id, name, age)")

	// Skip column metadata.
	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // ks
		_, _ = cqlwire.ReadString(r) // table
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	// Row count.
	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(1), rowCount,
		"SELECT should return exactly 1 row")

	// Read the row data (id, name, age).
	// id: UUID (16 bytes).
	idBytes, err := cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, 16, len(idBytes),
		"UUID should be 16 bytes")

	// name: text.
	nameBytes, err := cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, "Alice", string(nameBytes))

	// age: int (4 bytes, big-endian).
	ageBytes, err := cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, 4, len(ageBytes), "int should be 4 bytes")
	age := int32(ageBytes[0])<<24 |
		int32(ageBytes[1])<<16 |
		int32(ageBytes[2])<<8 |
		int32(ageBytes[3])
	require.Equal(t, int32(30), age)
}

// TestSmokeHandshake verifies that a CQL handshake works against a
// real CockroachDB server.
func TestSmokeHandshake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	addr, cleanup := startCQLServer(t)
	defer cleanup()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(
		time.Now().Add(10*time.Second),
	))

	cqlHandshake(t, conn)
}
