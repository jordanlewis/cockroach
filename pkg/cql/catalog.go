// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"
	"net"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/cockroachdb/cockroach/pkg/cql/translate"
	cqltypes "github.com/cockroachdb/cockroach/pkg/cql/types"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/redact"
)

// whereFilters holds equality filter values extracted from a SELECT
// WHERE clause. The cassandra-driver uses these filters during
// targeted schema refreshes (e.g. after CREATE TABLE) to request
// metadata for a specific keyspace and table.
type whereFilters struct {
	keyspaceName string // empty = no filter
	tableName    string // empty = no filter
	viewName     string // empty = no filter
}

// extractWhereFilters pulls equality filter values from the parsed
// WHERE clauses. Only simple equality checks on known column names
// are extracted; all other clauses are ignored.
func extractWhereFilters(where []parser.WhereClause) whereFilters {
	var f whereFilters
	for _, w := range where {
		if w.Operator != "=" {
			continue
		}
		val := ""
		switch v := w.Value.(type) {
		case *parser.StringLiteral:
			val = v.Value
		default:
			continue
		}
		switch strings.ToLower(w.Column) {
		case "keyspace_name":
			f.keyspaceName = val
		case "table_name":
			f.tableName = val
		case "view_name":
			f.viewName = val
		}
	}
	return f
}

// systemSchemaColumn describes a single column in a system_schema virtual
// table stub.
type systemSchemaColumn struct {
	name    string
	cqlType cqltypes.CQLType
}

// writeColumnTypeOption writes the CQL type option for a column to buf.
// For simple types this is a single [short] type ID. For collection
// types (set, list) the element type is also written. All collection
// columns in our system tables use varchar elements.
func writeColumnTypeOption(buf *bytes.Buffer, col systemSchemaColumn) {
	_ = cqlwire.WriteShort(buf, uint16(col.cqlType))
	switch col.cqlType {
	case cqltypes.CQLSet, cqltypes.CQLList:
		_ = cqlwire.WriteShort(buf, uint16(cqltypes.CQLVarchar))
	}
}

// systemColumnTypeName returns the CQL type name for a system table
// column. For collection types it includes the element type, e.g.
// "set<varchar>".
func systemColumnTypeName(col systemSchemaColumn) string {
	switch col.cqlType {
	case cqltypes.CQLSet:
		return "set<varchar>"
	case cqltypes.CQLList:
		return "list<varchar>"
	default:
		return col.cqlType.String()
	}
}

// writeSetVarcharValue writes a CQL set<varchar> cell value to buf.
// The encoding is: [int] total_bytes, [int] element_count, then for
// each element: [int] length, [bytes] data.
func writeSetVarcharValue(buf *bytes.Buffer, values []string) {
	var inner bytes.Buffer
	_ = cqlwire.WriteInt(&inner, int32(len(values)))
	for _, v := range values {
		_ = cqlwire.WriteInt(&inner, int32(len(v)))
		inner.WriteString(v)
	}
	_ = cqlwire.WriteInt(buf, int32(inner.Len()))
	buf.Write(inner.Bytes())
}

// sortedKeys returns the keys of a map sorted in ascending order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
// single synthetic row with hardcoded values. The tokens column is
// required by gocql for token-aware routing.
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
	{"tokens", cqltypes.CQLSet},
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
	{"tokens", cqltypes.CQLSet},
}

// systemPeersV2Columns defines the column schema for the
// system.peers_v2 virtual table. peers_v2 extends the peers schema
// with port fields for mixed-port clusters. gocql queries peers_v2
// first and falls back to peers on error.
var systemPeersV2Columns = []systemSchemaColumn{
	{"peer", cqltypes.CQLInet},
	{"peer_port", cqltypes.CQLInt},
	{"data_center", cqltypes.CQLVarchar},
	{"host_id", cqltypes.CQLUuid},
	{"native_address", cqltypes.CQLInet},
	{"native_port", cqltypes.CQLInt},
	{"preferred_ip", cqltypes.CQLInet},
	{"preferred_port", cqltypes.CQLInt},
	{"rack", cqltypes.CQLVarchar},
	{"release_version", cqltypes.CQLVarchar},
	{"rpc_address", cqltypes.CQLInet},
	{"schema_version", cqltypes.CQLUuid},
	{"tokens", cqltypes.CQLSet},
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

// isSelectStar returns true when the column list represents SELECT *
// (either empty or a single star selector).
func isSelectStar(columns []parser.Selector) bool {
	return len(columns) == 0 ||
		(len(columns) == 1 && columns[0].Column == "*" && columns[0].Expr == nil)
}

// systemTableFromSQL returns a SQL FROM subquery for the given system
// table. For system.local this provides a single-row subquery with all
// column values; for peers tables it provides a zero-row subquery.
// Returns ("", false) for unknown or unsupported system tables.
func systemTableFromSQL(keyspace, table string) (string, bool) {
	ks := strings.ToLower(keyspace)
	tbl := strings.ToLower(table)
	if ks != "system" {
		return "", false
	}
	switch tbl {
	case "local":
		return systemLocalFromSQL(), true
	case "peers", "peers_v2":
		return systemPeersFromSQL(), true
	default:
		return "", false
	}
}

// handleSystemSelect checks whether the SELECT targets a table in the
// system, system_schema, or system_virtual_schema keyspace. If so, it
// returns a synthetic result. Returns (result, true) when handled, or
// (ExecuteResult{}, false) when the query should proceed through the
// normal path.
//
// For SELECT * queries, full synthetic results are returned with all
// columns. For non-star queries on system.local/peers tables,
// handleSystemSelect returns false so the executor can translate the
// query with a synthetic FROM subquery and apply proper column
// projection and expression evaluation via the SQL engine.
//
// The db parameter is used to query CRDB for real database names when
// populating system_schema.keyspaces. The where parameter carries any
// WHERE clause filters from the parsed SELECT; the cassandra-driver
// uses filtered queries like "WHERE keyspace_name = 'ks' AND
// table_name = 'tbl'" during targeted schema refreshes.
func handleSystemSelect(
	ctx context.Context,
	db isql.DB,
	keyspace, table string,
	where []parser.WhereClause,
	columns []parser.Selector,
	schema *translate.SchemaInfo,
) (ExecuteResult, bool) {
	ks := strings.ToLower(keyspace)
	tbl := strings.ToLower(table)
	filters := extractWhereFilters(where)

	switch ks {
	case "system_schema":
		switch tbl {
		case "keyspaces":
			body, err := buildSystemSchemaKeyspacesBody(ctx, db, filters)
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		case "tables":
			body, err := buildSystemSchemaTablesBody(ctx, db, filters)
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		case "columns":
			body, err := buildSystemSchemaColumnsBody(ctx, db, filters, schema)
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		default:
			schema, ok := systemSchemaTables[tbl]
			if !ok {
				return ExecuteResult{}, false
			}
			body, err := buildEmptyRowsBody("system_schema", tbl, schema.columns)
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		}

	case "system_virtual_schema":
		schema, ok := systemVirtualSchemaTables[tbl]
		if !ok {
			return ExecuteResult{}, false
		}
		body, err := buildEmptyRowsBody("system_virtual_schema", tbl, schema.columns)
		if err != nil {
			return errorResult(errCodeServerError, err.Error()), true
		}
		return ExecuteResult{Body: body}, true

	case "system":
		// For non-star selects on system tables, let the executor
		// handle projection via a SQL subquery rather than returning
		// the full synthetic result with all columns.
		if !isSelectStar(columns) {
			return ExecuteResult{}, false
		}
		switch tbl {
		case "local":
			body, err := buildSystemLocalBody()
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		case "peers":
			body, err := buildEmptyRowsBody("system", "peers", systemPeersColumns)
			if err != nil {
				return errorResult(errCodeServerError, err.Error()), true
			}
			return ExecuteResult{Body: body}, true
		case "peers_v2":
			body, err := buildEmptyRowsBody("system", "peers_v2", systemPeersV2Columns)
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
// given column metadata and zero rows. The ksName and tableName
// parameters are written into per-column metadata so drivers see
// correct provenance.
func buildEmptyRowsBody(ksName, tableName string, cols []systemSchemaColumn) ([]byte, error) {
	var buf bytes.Buffer

	// RESULT kind: Rows.
	_ = cqlwire.WriteInt(&buf, resultKindRows)

	// Metadata flags: 0 (no global table spec).
	_ = cqlwire.WriteInt(&buf, 0)

	// Column count.
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))

	// Per-column metadata: keyspace, table, name, type.
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, ksName)
		_ = cqlwire.WriteString(&buf, tableName)
		_ = cqlwire.WriteString(&buf, col.name)
		writeColumnTypeOption(&buf, col)
	}

	// Row count: 0.
	_ = cqlwire.WriteInt(&buf, 0)

	return buf.Bytes(), nil
}

// buildSystemSchemaKeyspacesBody builds a CQL RESULT Rows frame body
// for system_schema.keyspaces. It includes synthetic system keyspaces
// plus any user-created databases from CRDB. cqlsh uses this metadata
// to resolve keyspace names when formatting query results. If filters
// specifies a keyspace_name, only matching keyspaces are included.
func buildSystemSchemaKeyspacesBody(
	ctx context.Context, db isql.DB, filters whereFilters,
) ([]byte, error) {
	// Start with synthetic system keyspaces.
	keyspaceNames := []string{
		"system",
		"system_schema",
		"system_virtual_schema",
	}

	// Query CRDB for real databases and add them.
	if db != nil {
		executor := db.Executor()
		rows, err := executor.QueryBufferedEx(
			ctx,
			redact.Sprint("cql-list-databases"),
			nil, // txn
			sessiondata.InternalExecutorOverride{
				User: username.RootUserName(),
			},
			"SELECT database_name FROM [SHOW DATABASES]",
		)
		if err == nil {
			for _, row := range rows {
				if len(row) > 0 {
					name := string(*row[0].(*tree.DString))
					// Skip system databases that are already in the list.
					if name != "system" {
						keyspaceNames = append(keyspaceNames, name)
					}
				}
			}
		}
	}

	// Apply keyspace_name filter if present.
	if filters.keyspaceName != "" {
		filtered := keyspaceNames[:0]
		for _, name := range keyspaceNames {
			if name == filters.keyspaceName {
				filtered = append(filtered, name)
			}
		}
		keyspaceNames = filtered
	}

	cols := systemSchemaTables["keyspaces"].columns
	var buf bytes.Buffer

	_ = cqlwire.WriteInt(&buf, resultKindRows)
	_ = cqlwire.WriteInt(&buf, 0) // flags
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, "system_schema")
		_ = cqlwire.WriteString(&buf, "keyspaces")
		_ = cqlwire.WriteString(&buf, col.name)
		writeColumnTypeOption(&buf, col)
	}

	_ = cqlwire.WriteInt(&buf, int32(len(keyspaceNames)))

	writeText := func(s string) {
		_ = cqlwire.WriteInt(&buf, int32(len(s)))
		buf.WriteString(s)
	}
	writeBool := func(v bool) {
		_ = cqlwire.WriteInt(&buf, 1)
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}

	for _, name := range keyspaceNames {
		writeText(name)                          // keyspace_name
		writeBool(true)                          // durable_writes
		writeText("{'class': 'SimpleStrategy'}") // replication
	}

	return buf.Bytes(), nil
}

// buildSystemSchemaTablesBody builds a CQL RESULT Rows frame body for
// system_schema.tables. It includes the synthetic system tables plus
// any user-created tables discovered via CRDB's information_schema.
// cqlsh uses this to validate table names before issuing queries.
// If filters specifies keyspace_name and/or table_name, only matching
// rows are included.
func buildSystemSchemaTablesBody(
	ctx context.Context, db isql.DB, filters whereFilters,
) ([]byte, error) {
	cols := systemSchemaTables["tables"].columns
	var buf bytes.Buffer

	_ = cqlwire.WriteInt(&buf, resultKindRows)
	_ = cqlwire.WriteInt(&buf, 0) // flags
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, "system_schema")
		_ = cqlwire.WriteString(&buf, "tables")
		_ = cqlwire.WriteString(&buf, col.name)
		writeColumnTypeOption(&buf, col)
	}

	type tableRow struct {
		keyspaceName string
		tableName    string
	}
	tables := []tableRow{
		{"system", "local"},
		{"system", "peers"},
		{"system", "peers_v2"},
	}
	// Include system_schema's own tables so the catalog is
	// self-describing. Cassandra lists all system_schema tables
	// here and cqlsh DESCRIBE depends on this for keyspace
	// introspection. Sort for deterministic output.
	schemaTableNames := sortedKeys(systemSchemaTables)
	for _, name := range schemaTableNames {
		tables = append(tables, tableRow{"system_schema", name})
	}

	// Query CRDB for real user tables.
	if db != nil {
		executor := db.Executor()
		rows, err := executor.QueryBufferedEx(
			ctx,
			redact.Sprint("cql-list-tables"),
			nil, // txn
			sessiondata.InternalExecutorOverride{
				User: username.RootUserName(),
			},
			`SELECT table_catalog, table_name
			 FROM "".information_schema.tables
			 WHERE table_schema = 'public'
			   AND table_type = 'BASE TABLE'
			 ORDER BY table_catalog, table_name`,
		)
		if err == nil {
			for _, row := range rows {
				if len(row) >= 2 {
					dbName := string(*row[0].(*tree.DString))
					tblName := string(*row[1].(*tree.DString))
					tables = append(tables, tableRow{
						keyspaceName: dbName,
						tableName:    tblName,
					})
				}
			}
		}
	}

	// Apply filters.
	if filters.keyspaceName != "" || filters.tableName != "" {
		filtered := tables[:0]
		for _, tbl := range tables {
			if filters.keyspaceName != "" && tbl.keyspaceName != filters.keyspaceName {
				continue
			}
			if filters.tableName != "" && tbl.tableName != filters.tableName {
				continue
			}
			filtered = append(filtered, tbl)
		}
		tables = filtered
	}

	_ = cqlwire.WriteInt(&buf, int32(len(tables)))

	writeText := func(s string) {
		_ = cqlwire.WriteInt(&buf, int32(len(s)))
		buf.WriteString(s)
	}
	writeNull := func() {
		_ = cqlwire.WriteInt(&buf, -1) // CQL NULL
	}
	writeInt := func(v int32) {
		_ = cqlwire.WriteInt(&buf, 4) // [bytes] length = 4
		_ = cqlwire.WriteInt(&buf, v)
	}

	for _, tbl := range tables {
		writeText(tbl.keyspaceName) // keyspace_name
		writeText(tbl.tableName)    // table_name
		writeNull()                 // bloom_filter_fp_chance (double)
		writeText("")               // comment
		writeNull()                 // compaction
		writeNull()                 // compression
		writeInt(0)                 // default_time_to_live
		writeInt(864000)            // gc_grace_seconds
		writeNull()                 // id (uuid)
		writeInt(2048)              // max_index_interval
		writeInt(0)                 // memtable_flush_period_in_ms
		writeInt(128)               // min_index_interval
		writeText("99p")            // speculative_retry
	}

	return buf.Bytes(), nil
}

// crdbTypeToCQLTypeName maps a CRDB data_type string (from
// information_schema.columns) to a CQL type name. Returns "varchar"
// for unrecognized types since cqlsh needs something parseable.
func crdbTypeToCQLTypeName(crdbType string) string {
	// Normalize by lowercasing and stripping precision/length.
	t := strings.ToLower(crdbType)
	switch {
	case t == "uuid":
		return "uuid"
	case t == "text", t == "character varying", strings.HasPrefix(t, "varchar"),
		strings.HasPrefix(t, "string"), strings.HasPrefix(t, "char"):
		return "text"
	case t == "integer", t == "int4", t == "int2", t == "smallint":
		return "int"
	case t == "bigint", t == "int8", t == "int":
		return "bigint"
	case t == "boolean", t == "bool":
		return "boolean"
	case t == "double precision", t == "float8":
		return "double"
	case t == "real", t == "float4":
		return "float"
	case strings.HasPrefix(t, "timestamp"):
		return "timestamp"
	case t == "bytea", t == "bytes", t == "blob":
		return "blob"
	case t == "inet":
		return "inet"
	case t == "jsonb":
		return "text"
	default:
		return "text"
	}
}

// buildSystemSchemaColumnsBody builds a CQL RESULT Rows frame body
// for system_schema.columns listing columns for system tables and
// any user-created tables discovered via CRDB's information_schema.
// If filters specifies keyspace_name and/or table_name, only matching
// rows are included.
func buildSystemSchemaColumnsBody(
	ctx context.Context, db isql.DB, filters whereFilters, schema *translate.SchemaInfo,
) ([]byte, error) {
	cols := systemSchemaTables["columns"].columns
	var buf bytes.Buffer

	_ = cqlwire.WriteInt(&buf, resultKindRows)
	_ = cqlwire.WriteInt(&buf, 0) // flags
	_ = cqlwire.WriteInt(&buf, int32(len(cols)))
	for _, col := range cols {
		_ = cqlwire.WriteString(&buf, "system_schema")
		_ = cqlwire.WriteString(&buf, "columns")
		_ = cqlwire.WriteString(&buf, col.name)
		writeColumnTypeOption(&buf, col)
	}

	type colRow struct {
		keyspaceName    string
		tableName       string
		columnName      string
		clusteringOrder string
		kind            string
		position        int32
		colType         string
	}

	var columnRows []colRow

	// system.local columns.
	for _, c := range systemLocalColumns {
		kind := "regular"
		pos := int32(0)
		if c.name == "key" {
			kind = "partition_key"
		}
		columnRows = append(columnRows, colRow{
			keyspaceName:    "system",
			tableName:       "local",
			columnName:      c.name,
			clusteringOrder: "none",
			kind:            kind,
			position:        pos,
			colType:         systemColumnTypeName(c),
		})
	}

	// system.peers columns.
	for _, c := range systemPeersColumns {
		kind := "regular"
		pos := int32(0)
		if c.name == "peer" {
			kind = "partition_key"
		}
		columnRows = append(columnRows, colRow{
			keyspaceName:    "system",
			tableName:       "peers",
			columnName:      c.name,
			clusteringOrder: "none",
			kind:            kind,
			position:        pos,
			colType:         systemColumnTypeName(c),
		})
	}

	// system.peers_v2 columns.
	for _, c := range systemPeersV2Columns {
		kind := "regular"
		pos := int32(0)
		if c.name == "peer" {
			kind = "partition_key"
		}
		columnRows = append(columnRows, colRow{
			keyspaceName:    "system",
			tableName:       "peers_v2",
			columnName:      c.name,
			clusteringOrder: "none",
			kind:            kind,
			position:        pos,
			colType:         systemColumnTypeName(c),
		})
	}

	// system_schema table columns. Include column metadata for each
	// system_schema table so the catalog is self-describing. The
	// first column of each table (keyspace_name) is the partition key.
	// Iterate in sorted order for deterministic output.
	for _, tblName := range sortedKeys(systemSchemaTables) {
		schema := systemSchemaTables[tblName]
		for _, c := range schema.columns {
			kind := "regular"
			pos := int32(0)
			if c.name == "keyspace_name" {
				kind = "partition_key"
			}
			columnRows = append(columnRows, colRow{
				keyspaceName:    "system_schema",
				tableName:       tblName,
				columnName:      c.name,
				clusteringOrder: "none",
				kind:            kind,
				position:        pos,
				colType:         systemColumnTypeName(c),
			})
		}
	}

	// Query CRDB for real user-table columns.
	if db != nil {
		executor := db.Executor()
		rows, err := executor.QueryBufferedEx(
			ctx,
			redact.Sprint("cql-list-columns"),
			nil, // txn
			sessiondata.InternalExecutorOverride{
				User: username.RootUserName(),
			},
			`SELECT c.table_catalog, c.table_name, c.column_name, c.data_type,
			        c.ordinal_position,
			        COALESCE(
			          (SELECT 'partition_key'
			           FROM "".information_schema.table_constraints tc
			           JOIN "".information_schema.constraint_column_usage ccu
			             ON tc.constraint_name = ccu.constraint_name
			            AND tc.table_catalog = ccu.table_catalog
			            AND tc.table_schema = ccu.table_schema
			           WHERE tc.constraint_type = 'PRIMARY KEY'
			             AND tc.table_catalog = c.table_catalog
			             AND tc.table_schema = c.table_schema
			             AND tc.table_name = c.table_name
			             AND ccu.column_name = c.column_name
			           LIMIT 1),
			          'regular'
			        ) AS kind
			 FROM "".information_schema.columns c
			 JOIN "".information_schema.tables t
			   ON c.table_catalog = t.table_catalog
			  AND c.table_schema = t.table_schema
			  AND c.table_name = t.table_name
			 WHERE c.table_schema = 'public'
			   AND t.table_type = 'BASE TABLE'
			 ORDER BY c.table_catalog, c.table_name, c.ordinal_position`,
		)
		if err == nil {
			for _, row := range rows {
				if len(row) >= 6 {
					dbName := string(*row[0].(*tree.DString))
					tblName := string(*row[1].(*tree.DString))
					colName := string(*row[2].(*tree.DString))
					dataType := string(*row[3].(*tree.DString))
					kind := string(*row[5].(*tree.DString))
					pos := int32(0)
					clusteringOrder := "none"
					// Refine kind using schema info. The SQL query marks
					// all PRIMARY KEY columns as "partition_key", but CQL
					// distinguishes partition keys from clustering keys.
					if kind == "partition_key" && schema != nil {
						if meta, ok := schema.LookupTable(dbName, tblName); ok {
							lowerCol := strings.ToLower(colName)
							isPartition := false
							for i, pk := range meta.PartitionKeys {
								if strings.ToLower(pk) == lowerCol {
									isPartition = true
									pos = int32(i)
									break
								}
							}
							if !isPartition {
								for i, ck := range meta.ClusteringKeys {
									if strings.ToLower(ck) == lowerCol {
										kind = "clustering"
										pos = int32(i)
										clusteringOrder = "asc"
										if meta.ClusteringDesc != nil && meta.ClusteringDesc[lowerCol] {
											clusteringOrder = "desc"
										}
										break
									}
								}
							}
						}
					}
					columnRows = append(columnRows, colRow{
						keyspaceName:    dbName,
						tableName:       tblName,
						columnName:      colName,
						clusteringOrder: clusteringOrder,
						kind:            kind,
						position:        pos,
						colType:         crdbTypeToCQLTypeName(dataType),
					})
				}
			}
		}
	}

	// Apply filters.
	if filters.keyspaceName != "" || filters.tableName != "" {
		filtered := columnRows[:0]
		for _, cr := range columnRows {
			if filters.keyspaceName != "" && cr.keyspaceName != filters.keyspaceName {
				continue
			}
			if filters.tableName != "" && cr.tableName != filters.tableName {
				continue
			}
			filtered = append(filtered, cr)
		}
		columnRows = filtered
	}

	_ = cqlwire.WriteInt(&buf, int32(len(columnRows)))

	writeText := func(s string) {
		_ = cqlwire.WriteInt(&buf, int32(len(s)))
		buf.WriteString(s)
	}
	writeInt := func(v int32) {
		_ = cqlwire.WriteInt(&buf, 4) // [bytes] length = 4
		_ = cqlwire.WriteInt(&buf, v)
	}

	for _, cr := range columnRows {
		writeText(cr.keyspaceName)    // keyspace_name
		writeText(cr.tableName)       // table_name
		writeText(cr.columnName)      // column_name
		writeText(cr.clusteringOrder) // clustering_order
		writeText(cr.kind)            // kind
		writeInt(cr.position)         // position
		writeText(cr.colType)         // type
	}

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

// systemLocalFromSQL returns a SQL FROM subquery that provides the
// same single-row result as buildSystemLocalBody. This allows the SQL
// executor to handle column projection and expression evaluation for
// non-star SELECTs against system.local. The tokens column uses
// NULL::STRING since CQL set<varchar> has no SQL equivalent; this is
// acceptable because tokens is rarely projected individually.
func systemLocalFromSQL() string {
	return `(SELECT ` +
		`'local' AS "key", ` +
		`'COMPLETED' AS "bootstrapped", ` +
		`'127.0.0.1'::INET AS "broadcast_address", ` +
		`'cockroachdb' AS "cluster_name", ` +
		`'3.4.5' AS "cql_version", ` +
		`'datacenter1' AS "data_center", ` +
		`'00000000-0000-4000-8000-000000000001'::UUID AS "host_id", ` +
		`'127.0.0.1'::INET AS "listen_address", ` +
		`'4' AS "native_protocol_version", ` +
		`'org.apache.cassandra.dht.Murmur3Partitioner' AS "partitioner", ` +
		`'rack1' AS "rack", ` +
		`'4.0.0' AS "release_version", ` +
		`'127.0.0.1'::INET AS "rpc_address", ` +
		`'00000000-0000-4000-8000-000000000002'::UUID AS "schema_version", ` +
		`NULL::STRING AS "tokens"` +
		`) AS "local"`
}

// systemPeersFromSQL returns a SQL FROM subquery for system.peers that
// produces zero rows. Peers tables always return empty results in this
// CQL compatibility layer; the false WHERE clause ensures the SQL
// executor returns no rows while still allowing column references to
// resolve for projected SELECTs.
func systemPeersFromSQL() string {
	return `(SELECT 1 WHERE false) AS "peers"`
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
		writeColumnTypeOption(&buf, col)
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

	writeText("local")                                           // key
	writeText("COMPLETED")                                       // bootstrapped
	writeInet(localhost)                                         // broadcast_address
	writeText("cockroachdb")                                     // cluster_name
	writeText("3.4.5")                                           // cql_version
	writeText("datacenter1")                                     // data_center
	writeUUID(fixedHostID)                                       // host_id
	writeInet(localhost)                                         // listen_address
	writeText("4")                                               // native_protocol_version
	writeText("org.apache.cassandra.dht.Murmur3Partitioner")     // partitioner
	writeText("rack1")                                           // rack
	writeText("4.0.0")                                           // release_version
	writeInet(localhost)                                         // rpc_address
	writeUUID(fixedSchemaVersion)                                // schema_version
	writeSetVarcharValue(&buf, []string{"-9223372036854775808"}) // tokens

	return buf.Bytes(), nil
}
