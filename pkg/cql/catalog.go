// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
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

// handleSystemSchemaSelect checks whether the SELECT targets a
// system_schema table and, if so, returns an empty Rows result with
// the correct column metadata. Returns (result, true) when handled,
// or (ExecuteResult{}, false) when the query should proceed through
// the normal execution path.
func handleSystemSchemaSelect(keyspace, table string) (ExecuteResult, bool) {
	if !strings.EqualFold(keyspace, "system_schema") {
		return ExecuteResult{}, false
	}
	schema, ok := systemSchemaTables[strings.ToLower(table)]
	if !ok {
		return ExecuteResult{}, false
	}
	body, err := buildEmptyRowsBody(schema.columns)
	if err != nil {
		return errorResult(errCodeServerError, err.Error()), true
	}
	return ExecuteResult{Body: body}, true
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
