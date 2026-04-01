// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	cqltypes "github.com/cockroachdb/cockroach/pkg/cql/types"
)

// CQL collection type IDs from the CQL native protocol v4 spec,
// section 6. These extend the scalar type IDs defined in cqltypes.
const (
	cqlTypeSet cqltypes.CQLType = 0x0022
)

// isSystemQuery returns true if the parsed SELECT statement targets a
// CQL system keyspace (system or system_schema). These queries are
// intercepted and answered with synthetic results rather than being
// sent to the SQL executor.
func isSystemQuery(stmt *parser.SelectStatement) bool {
	ks := strings.ToLower(stmt.Keyspace)
	return ks == "system" || ks == "system_schema"
}

// handleSystemQuery returns a synthetic ExecuteResult for CQL system
// table queries. CQL clients (cqlsh, gocql) query these tables during
// connection setup to discover cluster topology and schema metadata.
func handleSystemQuery(stmt *parser.SelectStatement) ExecuteResult {
	ks := strings.ToLower(stmt.Keyspace)
	table := strings.ToLower(stmt.Table)

	switch ks {
	case "system":
		switch table {
		case "local":
			return handleSystemLocal()
		case "peers", "peers_v2":
			return handleSystemPeers(table)
		default:
			return errorResult(errCodeInvalid,
				"unknown system table: system."+table)
		}
	case "system_schema":
		switch table {
		case "keyspaces":
			return handleSystemSchemaKeyspaces()
		case "tables":
			return handleSystemSchemaTables()
		case "columns":
			return handleSystemSchemaColumns()
		default:
			return errorResult(errCodeInvalid,
				"unknown system_schema table: system_schema."+table)
		}
	default:
		return errorResult(errCodeInvalid,
			"unsupported system keyspace: "+ks)
	}
}

// catalogCol defines a column in a synthetic CQL result set.
type catalogCol struct {
	ks    string
	table string
	name  string
	typ   cqltypes.CQLType
	// elem is the element type for set<> columns. Zero for scalar
	// types.
	elem cqltypes.CQLType
}

// Fixed synthetic values used in system.local results.
var (
	localhostIPv4 = net.IPv4(127, 0, 0, 1).To4()

	// fixedHostID is a synthetic UUID for the host_id column.
	fixedHostID = []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00,
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}
	// fixedSchemaVersion is a synthetic UUID for the schema_version
	// column.
	fixedSchemaVersion = []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00,
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
	}
)

// handleSystemLocal returns a single row of synthetic cluster
// information for system.local. This is the most critical system
// table: without it, cqlsh and gocql cannot complete connection
// setup.
func handleSystemLocal() ExecuteResult {
	cols := []catalogCol{
		{"system", "local", "key", cqltypes.CQLVarchar, 0},
		{"system", "local", "bootstrapped", cqltypes.CQLVarchar, 0},
		{"system", "local", "broadcast_address", cqltypes.CQLInet, 0},
		{"system", "local", "cluster_name", cqltypes.CQLVarchar, 0},
		{"system", "local", "cql_version", cqltypes.CQLVarchar, 0},
		{"system", "local", "data_center", cqltypes.CQLVarchar, 0},
		{"system", "local", "host_id", cqltypes.CQLUuid, 0},
		{"system", "local", "listen_address", cqltypes.CQLInet, 0},
		{"system", "local", "native_protocol_version", cqltypes.CQLVarchar, 0},
		{"system", "local", "partitioner", cqltypes.CQLVarchar, 0},
		{"system", "local", "rack", cqltypes.CQLVarchar, 0},
		{"system", "local", "release_version", cqltypes.CQLVarchar, 0},
		{"system", "local", "rpc_address", cqltypes.CQLInet, 0},
		{"system", "local", "schema_version", cqltypes.CQLUuid, 0},
		{"system", "local", "tokens", cqlTypeSet, cqltypes.CQLVarchar},
	}

	row := [][]byte{
		[]byte("local"),
		[]byte("COMPLETED"),
		localhostIPv4,
		[]byte("CockroachDB"),
		[]byte("3.4.5"),
		[]byte("datacenter1"),
		fixedHostID,
		localhostIPv4,
		[]byte("4"),
		[]byte("org.apache.cassandra.dht.Murmur3Partitioner"),
		[]byte("rack1"),
		[]byte("3.11.17"),
		localhostIPv4,
		fixedSchemaVersion,
		encodeSetText([]string{"-1"}),
	}

	return ExecuteResult{
		Body: buildCatalogBody(cols, [][][]byte{row}),
	}
}

// handleSystemPeers returns an empty result set for system.peers
// (or system.peers_v2). CockroachDB presents as a single Cassandra
// node, so there are no peers.
func handleSystemPeers(table string) ExecuteResult {
	cols := []catalogCol{
		{"system", table, "peer", cqltypes.CQLInet, 0},
		{"system", table, "data_center", cqltypes.CQLVarchar, 0},
		{"system", table, "host_id", cqltypes.CQLUuid, 0},
		{"system", table, "preferred_ip", cqltypes.CQLInet, 0},
		{"system", table, "rack", cqltypes.CQLVarchar, 0},
		{"system", table, "release_version", cqltypes.CQLVarchar, 0},
		{"system", table, "rpc_address", cqltypes.CQLInet, 0},
		{"system", table, "schema_version", cqltypes.CQLUuid, 0},
		{"system", table, "tokens", cqlTypeSet, cqltypes.CQLVarchar},
	}
	// No rows: single-node cluster has no peers.
	return ExecuteResult{
		Body: buildCatalogBody(cols, nil),
	}
}

// handleSystemSchemaKeyspaces returns an empty result set for
// system_schema.keyspaces.
func handleSystemSchemaKeyspaces() ExecuteResult {
	cols := []catalogCol{
		{"system_schema", "keyspaces", "keyspace_name", cqltypes.CQLVarchar, 0},
		{"system_schema", "keyspaces", "durable_writes", cqltypes.CQLBoolean, 0},
	}
	return ExecuteResult{
		Body: buildCatalogBody(cols, nil),
	}
}

// handleSystemSchemaTables returns an empty result set for
// system_schema.tables.
func handleSystemSchemaTables() ExecuteResult {
	cols := []catalogCol{
		{"system_schema", "tables", "keyspace_name", cqltypes.CQLVarchar, 0},
		{"system_schema", "tables", "table_name", cqltypes.CQLVarchar, 0},
	}
	return ExecuteResult{
		Body: buildCatalogBody(cols, nil),
	}
}

// handleSystemSchemaColumns returns an empty result set for
// system_schema.columns.
func handleSystemSchemaColumns() ExecuteResult {
	cols := []catalogCol{
		{"system_schema", "columns", "keyspace_name", cqltypes.CQLVarchar, 0},
		{"system_schema", "columns", "table_name", cqltypes.CQLVarchar, 0},
		{"system_schema", "columns", "column_name", cqltypes.CQLVarchar, 0},
		{"system_schema", "columns", "type", cqltypes.CQLVarchar, 0},
	}
	return ExecuteResult{
		Body: buildCatalogBody(cols, nil),
	}
}

// buildCatalogBody builds a CQL RESULT Rows frame body from catalog
// column definitions and rows of pre-encoded values.
func buildCatalogBody(cols []catalogCol, rows [][][]byte) []byte {
	var buf bytes.Buffer

	// RESULT kind: Rows.
	_ = cqlwire.WriteInt(&buf, resultKindRows)

	// Metadata flags: 0 (no global table spec).
	_ = cqlwire.WriteInt(&buf, 0)
	// Column count.
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))

	// Per-column metadata: [ksname][tablename][name][type].
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, col.ks)
		_ = cqlwire.WriteString(&buf, col.table)
		_ = cqlwire.WriteString(&buf, col.name)
		_ = cqlwire.WriteShort(&buf, uint16(col.typ))
		// Collection types require element type metadata.
		if col.typ == cqlTypeSet {
			_ = cqlwire.WriteShort(&buf, uint16(col.elem))
		}
	}

	// Row count.
	_ = cqlwire.WriteInt(&buf, int32(len(rows)))

	// Row data: each value is [int length][bytes] or [int -1] for NULL.
	for _, row := range rows {
		for _, val := range row {
			if val == nil {
				_ = cqlwire.WriteInt(&buf, -1)
			} else {
				_ = cqlwire.WriteInt(&buf, int32(len(val)))
				_, _ = buf.Write(val)
			}
		}
	}

	return buf.Bytes()
}

// encodeSetText encodes a set<text> value in CQL binary format.
// Each element is written as [int length][bytes value].
func encodeSetText(values []string) []byte {
	var buf bytes.Buffer
	_ = cqlwire.WriteInt(&buf, int32(len(values)))
	for _, v := range values {
		b := []byte(v)
		_ = cqlwire.WriteInt(&buf, int32(len(b)))
		_, _ = buf.Write(b)
	}
	return buf.Bytes()
}
