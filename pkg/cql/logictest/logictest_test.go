// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package logictest_test provides a datadriven test runner for CQL
// logic tests. Each testdata file is executed against a real
// CockroachDB test server with CQL enabled. The runner supports three
// directives:
//
//   - exec: run a CQL statement, expect success (empty output)
//   - query: run a CQL SELECT, compare formatted results
//   - error: run a CQL statement, expect an error message
//
// The query directive outputs column names on the first line followed
// by data rows, space-separated. Use the "rowsort" argument to sort
// rows before comparison when result ordering is non-deterministic:
//
//	query rowsort
//	SELECT a, b FROM t
//	----
//	a b
//	2 world
//	1 hello
package logictest_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	cqltypes "github.com/cockroachdb/cockroach/pkg/cql/types"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// CQL RESULT kind constants (CQL native protocol v4, section 4.2.5).
const (
	resultKindVoid        int32 = 0x0001
	resultKindRows        int32 = 0x0002
	resultKindSetKeyspace int32 = 0x0003
)

// startCQLServer starts a single-node CockroachDB cluster with CQL
// enabled on a random port. Returns the CQL listen address and a
// cleanup function.
func startCQLServer(t *testing.T) (cqlAddr string, cleanup func()) {
	t.Helper()

	st := cluster.MakeClusterSettings()
	server.CQLEnabled.Override(context.Background(), &st.SV, true)
	server.CQLPort.Override(context.Background(), &st.SV, 0)

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings:                   st,
		Insecure:                   true,
		DefaultTestTenant:          base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		DisableElasticCPUAdmission: true,
	})

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

// testRunner manages a CQL wire protocol connection for executing
// test directives within a single datadriven test file.
type testRunner struct {
	conn     net.Conn
	streamID int16
}

// newRunner creates a testRunner connected to the CQL server at addr.
// It performs the STARTUP/READY handshake before returning.
func newRunner(t *testing.T, addr string) *testRunner {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Now().Add(60*time.Second)))

	// CQL handshake: STARTUP -> READY.
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteStringMap(&body, map[string]string{
		"CQL_VERSION": "3.4.5",
	}))
	require.NoError(t, cqlwire.WriteFrame(conn, cqlwire.FrameHeader{
		Version: cqlwire.ProtoV4Request,
		Opcode:  cqlwire.OpStartup,
	}, body.Bytes()))

	frame, err := cqlwire.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpReady, frame.Header.Opcode,
		"expected READY after STARTUP")

	return &testRunner{conn: conn}
}

// close closes the underlying connection.
func (r *testRunner) close() {
	r.conn.Close()
}

// execute sends a CQL QUERY frame and reads the response frame.
func (r *testRunner) execute(t *testing.T, query string) cqlwire.Frame {
	t.Helper()
	r.streamID++

	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteLongString(&body, query))
	require.NoError(t, cqlwire.WriteConsistency(
		&body, cqlwire.ConsistencyOne,
	))
	// Query flags: 0 (no values, no paging).
	require.NoError(t, cqlwire.WriteBytes(&body, []byte{0}))

	require.NoError(t, cqlwire.WriteFrame(r.conn, cqlwire.FrameHeader{
		Version:  cqlwire.ProtoV4Request,
		StreamID: r.streamID,
		Opcode:   cqlwire.OpQuery,
	}, body.Bytes()))

	frame, err := cqlwire.ReadFrame(r.conn)
	require.NoError(t, err)
	return frame
}

// extractError extracts the error message from an ERROR frame body.
func extractError(body []byte) string {
	rd := bytes.NewReader(body)
	_, _ = cqlwire.ReadInt(rd) // error code
	msg, _ := cqlwire.ReadString(rd)
	return msg
}

// exec executes a CQL statement and returns "" on success. Fails the
// test if the server returns an error.
func (r *testRunner) exec(t *testing.T, input string) string {
	t.Helper()
	query := strings.TrimSpace(input)
	frame := r.execute(t, query)

	if frame.Header.Opcode == cqlwire.OpError {
		t.Fatalf("exec %q failed: %s", query, extractError(frame.Body))
	}
	return ""
}

// query executes a CQL SELECT and returns formatted results: column
// names on the first line, then data rows, space-separated.
func (r *testRunner) query(t *testing.T, input string) string {
	t.Helper()
	query := strings.TrimSpace(input)
	frame := r.execute(t, query)

	if frame.Header.Opcode == cqlwire.OpError {
		t.Fatalf("query %q failed: %s", query, extractError(frame.Body))
	}
	require.Equal(t, cqlwire.OpResult, frame.Header.Opcode)

	return formatRows(t, frame.Body)
}

// execError executes a CQL statement expecting failure and returns
// the error message.
func (r *testRunner) execError(t *testing.T, input string) string {
	t.Helper()
	query := strings.TrimSpace(input)
	frame := r.execute(t, query)

	if frame.Header.Opcode != cqlwire.OpError {
		t.Fatalf("expected error for %q but got opcode %s",
			query, frame.Header.Opcode)
	}
	return extractError(frame.Body)
}

// columnMeta holds metadata for a single result column.
type columnMeta struct {
	name   string
	typeID cqltypes.CQLType
}

// formatRows parses a CQL RESULT Rows frame body and returns
// human-readable output: column names on the first line, followed by
// data rows, space-separated.
func formatRows(t *testing.T, body []byte) string {
	t.Helper()
	rd := bytes.NewReader(body)

	kind, err := cqlwire.ReadInt(rd)
	require.NoError(t, err)
	require.Equal(t, resultKindRows, kind, "expected ROWS result kind")

	_, err = cqlwire.ReadInt(rd) // flags
	require.NoError(t, err)

	colCount, err := cqlwire.ReadInt(rd)
	require.NoError(t, err)

	cols := make([]columnMeta, colCount)
	for i := range cols {
		_, _ = cqlwire.ReadString(rd) // keyspace
		_, _ = cqlwire.ReadString(rd) // table
		name, nameErr := cqlwire.ReadString(rd)
		require.NoError(t, nameErr)
		typeID, typeErr := cqlwire.ReadShort(rd)
		require.NoError(t, typeErr)
		cols[i] = columnMeta{
			name:   name,
			typeID: cqltypes.CQLType(typeID),
		}
	}

	rowCount, err := cqlwire.ReadInt(rd)
	require.NoError(t, err)

	var buf strings.Builder

	// Column header.
	for i, col := range cols {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(col.name)
	}

	// Data rows.
	for i := int32(0); i < rowCount; i++ {
		buf.WriteByte('\n')
		for j := range cols {
			if j > 0 {
				buf.WriteByte(' ')
			}
			val, valErr := cqlwire.ReadBytes(rd)
			require.NoError(t, valErr)
			if val == nil {
				buf.WriteString("NULL")
			} else {
				buf.WriteString(decodeCQLValue(cols[j].typeID, val))
			}
		}
	}

	return buf.String()
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

// decodeCQLValue converts CQL binary-encoded bytes to a
// human-readable string based on the CQL type.
func decodeCQLValue(typeID cqltypes.CQLType, val []byte) string {
	switch typeID {
	case cqltypes.CQLVarchar, cqltypes.CQLAscii:
		return string(val)
	case cqltypes.CQLInt:
		return fmt.Sprintf("%d",
			int32(binary.BigEndian.Uint32(val)))
	case cqltypes.CQLBigint, cqltypes.CQLCounter:
		return fmt.Sprintf("%d",
			int64(binary.BigEndian.Uint64(val)))
	case cqltypes.CQLFloat:
		return fmt.Sprintf("%g",
			math.Float32frombits(binary.BigEndian.Uint32(val)))
	case cqltypes.CQLDouble:
		return fmt.Sprintf("%g",
			math.Float64frombits(binary.BigEndian.Uint64(val)))
	case cqltypes.CQLBoolean:
		if len(val) == 1 && val[0] != 0 {
			return "true"
		}
		return "false"
	case cqltypes.CQLUuid, cqltypes.CQLTimeuuid:
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			val[0:4], val[4:6], val[6:8], val[8:10], val[10:16])
	case cqltypes.CQLTimestamp:
		millis := int64(binary.BigEndian.Uint64(val))
		return time.UnixMilli(millis).UTC().Format(time.RFC3339)
	case cqltypes.CQLBlob:
		return "0x" + hex.EncodeToString(val)
	case cqltypes.CQLInet:
		if len(val) == 4 {
			return fmt.Sprintf("%d.%d.%d.%d",
				val[0], val[1], val[2], val[3])
		}
		return fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x",
			binary.BigEndian.Uint16(val[0:2]),
			binary.BigEndian.Uint16(val[2:4]),
			binary.BigEndian.Uint16(val[4:6]),
			binary.BigEndian.Uint16(val[6:8]),
			binary.BigEndian.Uint16(val[8:10]),
			binary.BigEndian.Uint16(val[10:12]),
			binary.BigEndian.Uint16(val[12:14]),
			binary.BigEndian.Uint16(val[14:16]))
	default:
		return "0x" + hex.EncodeToString(val)
	}
}

// TestLogic walks the testdata directory and runs each file as a CQL
// logic test against a real CockroachDB server.
func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	addr, cleanup := startCQLServer(t)
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
