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
	cqltypes "github.com/cockroachdb/cockroach/pkg/cql/types"
)

// systemSchemaColumn describes a single column in a system_schema virtual
// table stub.
type systemSchemaColumn struct {
	name    string
	cqlType cqltypes.CQLType
}

// systemSchemaTable defines the column schema for a system_schema table.
// These stubs return zero rows but must advertise the correct column
// metadata so that cqlsh (and other CQL drivers) can parse the response
// during their startup introspection queries.
type systemSchemaTable struct {
	columns []systemSchemaColumn
}

// systemSchemaTables maps system_schema table names to their column
// definitions. cqlsh queries all of these during startup; returning empty
// result sets (rather than errors) lets the handshake proceed.
var systemSchemaTables = map[string]systemSchemaTable{
	"keyspaces": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"durable_writes", cqltypes.CQLBoolean},
		{"replication", cqltypes.CQLVarchar},
	}},
	"tables": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"table_name", cqltypes.CQLVarchar},
		{"bloom_filter_fp_chance", cqltypes.CQLDouble},
		{"comment", cqltypes.CQLVarchar},
		{"compaction", cqltypes.CQLVarchar},
		{"compression", cqltypes.CQLVarchar},
		{"default_time_to_live", cqltypes.CQLInt},
		{"gc_grace_seconds", cqltypes.CQLInt},
		{"id", cqltypes.CQLUuid},
		{"max_index_interval", cqltypes.CQLInt},
		{"memtable_flush_period_in_ms", cqltypes.CQLInt},
		{"min_index_interval", cqltypes.CQLInt},
		{"speculative_retry", cqltypes.CQLVarchar},
	}},
	"columns": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"table_name", cqltypes.CQLVarchar},
		{"column_name", cqltypes.CQLVarchar},
		{"clustering_order", cqltypes.CQLVarchar},
		{"kind", cqltypes.CQLVarchar},
		{"position", cqltypes.CQLInt},
		{"type", cqltypes.CQLVarchar},
	}},
	"triggers": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"table_name", cqltypes.CQLVarchar},
		{"trigger_name", cqltypes.CQLVarchar},
		{"options", cqltypes.CQLVarchar},
	}},
	"views": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"view_name", cqltypes.CQLVarchar},
		{"base_table_id", cqltypes.CQLUuid},
		{"base_table_name", cqltypes.CQLVarchar},
		{"comment", cqltypes.CQLVarchar},
		{"id", cqltypes.CQLUuid},
		{"include_all_columns", cqltypes.CQLBoolean},
		{"where_clause", cqltypes.CQLVarchar},
	}},
	"functions": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"function_name", cqltypes.CQLVarchar},
		{"argument_types", cqltypes.CQLVarchar},
		{"argument_names", cqltypes.CQLVarchar},
		{"body", cqltypes.CQLVarchar},
		{"called_on_null_input", cqltypes.CQLBoolean},
		{"language", cqltypes.CQLVarchar},
		{"return_type", cqltypes.CQLVarchar},
	}},
	"aggregates": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"aggregate_name", cqltypes.CQLVarchar},
		{"argument_types", cqltypes.CQLVarchar},
		{"final_func", cqltypes.CQLVarchar},
		{"initcond", cqltypes.CQLVarchar},
		{"return_type", cqltypes.CQLVarchar},
		{"state_func", cqltypes.CQLVarchar},
		{"state_type", cqltypes.CQLVarchar},
	}},
	"types": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"type_name", cqltypes.CQLVarchar},
		{"field_names", cqltypes.CQLVarchar},
		{"field_types", cqltypes.CQLVarchar},
	}},
	"indexes": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"table_name", cqltypes.CQLVarchar},
		{"index_name", cqltypes.CQLVarchar},
		{"kind", cqltypes.CQLVarchar},
		{"options", cqltypes.CQLVarchar},
	}},
}

// systemLocalColumns defines the column schema for the system.local
// virtual table. cqlsh and the python cassandra-driver query this table
// on connection startup to discover cluster metadata. We return a
// single synthetic row with hardcoded values.
var systemLocalColumns = []systemSchemaColumn{
	{"key", cqltypes.CQLVarchar},
	{"bootstrapped", cqltypes.CQLVarchar},
	{"broadcast_address", cqltypes.CQLInet},
	{"cluster_name", cqltypes.CQLVarchar},
	{"cql_version", cqltypes.CQLVarchar},
	{"data_center", cqltypes.CQLVarchar},
	{"host_id", cqltypes.CQLUuid},
	{"listen_address", cqltypes.CQLInet},
	{"native_protocol_version", cqltypes.CQLVarchar},
	{"partitioner", cqltypes.CQLVarchar},
	{"rack", cqltypes.CQLVarchar},
	{"release_version", cqltypes.CQLVarchar},
	{"rpc_address", cqltypes.CQLInet},
	{"schema_version", cqltypes.CQLUuid},
}

// systemPeersColumns defines the column schema for the system.peers
// virtual table. For a single-node CQL frontend we return zero rows.
var systemPeersColumns = []systemSchemaColumn{
	{"peer", cqltypes.CQLInet},
	{"data_center", cqltypes.CQLVarchar},
	{"host_id", cqltypes.CQLUuid},
	{"preferred_ip", cqltypes.CQLInet},
	{"rack", cqltypes.CQLVarchar},
	{"release_version", cqltypes.CQLVarchar},
	{"rpc_address", cqltypes.CQLInet},
	{"schema_version", cqltypes.CQLUuid},
}

// systemVirtualSchemaTables maps system_virtual_schema table names to
// their column definitions. The cassandra-driver queries these during
// schema metadata discovery; empty results let the handshake proceed.
var systemVirtualSchemaTables = map[string]systemSchemaTable{
	"keyspaces": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
	}},
	"tables": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"table_name", cqltypes.CQLVarchar},
		{"comment", cqltypes.CQLVarchar},
	}},
	"columns": {columns: []systemSchemaColumn{
		{"keyspace_name", cqltypes.CQLVarchar},
		{"table_name", cqltypes.CQLVarchar},
		{"column_name", cqltypes.CQLVarchar},
		{"clustering_order", cqltypes.CQLVarchar},
		{"kind", cqltypes.CQLVarchar},
		{"position", cqltypes.CQLInt},
		{"type", cqltypes.CQLVarchar},
	}},
}

// handleSystemSelect checks whether the SELECT targets a table in the
// system, system_schema, or system_virtual_schema keyspace. If so, it
// returns a synthetic result. Returns (result, true) when handled, or
// (ExecuteResult{}, false) when the query should proceed through the
// normal path.
func handleSystemSelect(keyspace, table string) (ExecuteResult, bool) {
	ks := strings.ToLower(keyspace)
	tbl := strings.ToLower(table)

	switch ks {
	case "system_schema":
		schema, ok := systemSchemaTables[tbl]
		if !ok {
			return ExecuteResult{}, false
		}
		body, err := buildEmptyRowsBody(schema.columns)
		if err != nil {
			return errorResult(errCodeServerError, err.Error()), true
		}
		return ExecuteResult{Body: body}, true

	case "system_virtual_schema":
		schema, ok := systemVirtualSchemaTables[tbl]
		if !ok {
			return ExecuteResult{}, false
		}
		body, err := buildEmptyRowsBody(schema.columns)
		if err != nil {
			return errorResult(errCodeServerError, err.Error()), true
		}
		return ExecuteResult{Body: body}, true

	case "system":
		switch tbl {
		case "local":
			body, err := buildSystemLocalBody()
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		case "peers", "peers_v2":
			body, err := buildEmptyRowsBody(systemPeersColumns)
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		default:
			return ExecuteResult{}, false
		}

	default:
		return ExecuteResult{}, false
	}
}

// buildEmptyRowsBody builds a CQL RESULT Rows frame body with the
// given column metadata and zero rows. This is the same wire format
// as buildRowsBody but avoids the CRDB type mapping since we already
// have CQL types.
func buildEmptyRowsBody(cols []systemSchemaColumn) ([]byte, error) {
	var buf bytes.Buffer

	// RESULT kind: Rows.
	_ = cqlwire.WriteInt(&buf, resultKindRows)

	// Metadata flags: 0 (no global table spec).
	_ = cqlwire.WriteInt(&buf, 0)

	// Column count.
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))

	// Per-column metadata: keyspace, table, name, type.
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, "system_schema") // keyspace
		_ = cqlwire.WriteString(&buf, "")              // table
		_ = cqlwire.WriteString(&buf, col.name)
		_ = cqlwire.WriteShort(&buf, uint16(col.cqlType))
	}

	// Row count: 0.
	_ = cqlwire.WriteInt(&buf, 0)

	return buf.Bytes(), nil
}

// fixedHostID is a deterministic UUID used as the host_id for the
// synthetic system.local row. It is not derived from actual node
// identity — it exists solely to satisfy driver expectations.
var fixedHostID = [16]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x40, 0x00, // version 4
	0x80, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
}

// fixedSchemaVersion is a deterministic UUID used as the schema_version
// for synthetic system table rows.
var fixedSchemaVersion = [16]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x40, 0x00, // version 4
	0x80, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
}

// buildSystemLocalBody builds a CQL RESULT Rows frame body for
// system.local containing a single row with hardcoded cluster
// metadata. This is enough for cqlsh and the python cassandra-driver
// to complete their startup handshake.
func buildSystemLocalBody() ([]byte, error) {
	cols := systemLocalColumns
	var buf bytes.Buffer

	// RESULT kind: Rows.
	_ = cqlwire.WriteInt(&buf, resultKindRows)

	// Metadata flags: 0 (no global table spec).
	_ = cqlwire.WriteInt(&buf, 0)

	// Column count.
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))

	// Per-column metadata.
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, "system")
		_ = cqlwire.WriteString(&buf, "local")
		_ = cqlwire.WriteString(&buf, col.name)
		_ = cqlwire.WriteShort(&buf, uint16(col.cqlType))
	}

	// Row count: 1.
	_ = cqlwire.WriteInt(&buf, 1)

	// Row data — one [bytes] per column, matching systemLocalColumns
	// order.
	localhost := net.IPv4(127, 0, 0, 1).To4()

	writeText := func(s string) {
		_ = cqlwire.WriteInt(&buf, int32(len(s)))
		buf.WriteString(s)
	}
	writeInet := func(ip net.IP) {
		_ = cqlwire.WriteInt(&buf, int32(len(ip)))
		buf.Write(ip)
	}
	writeUUID := func(u [16]byte) {
		_ = cqlwire.WriteInt(&buf, 16)
		buf.Write(u[:])
	}

	writeText("local")                                                   // key
	writeText("COMPLETED")                                               // bootstrapped
	writeInet(localhost)                                                  // broadcast_address
	writeText("cockroachdb")                                             // cluster_name
	writeText("3.4.5")                                                   // cql_version
	writeText("datacenter1")                                             // data_center
	writeUUID(fixedHostID)                                               // host_id
	writeInet(localhost)                                                  // listen_address
	writeText("4")                                                       // native_protocol_version
	writeText("org.apache.cassandra.dht.Murmur3Partitioner")             // partitioner
	writeText("rack1")                                                   // rack
	writeText("4.0.0")                                                   // release_version
	writeInet(localhost)                                                  // rpc_address
	writeUUID(fixedSchemaVersion) // schema_version

	return buf.Bytes(), nil
}
