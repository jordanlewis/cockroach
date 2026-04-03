// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package translate converts CQL (Cassandra Query Language) AST nodes into
// CockroachDB SQL strings. Each CQL statement type maps to one or more SQL
// statements:
//
//   - USE <keyspace>                        → SET database = '<keyspace>'
//   - CREATE KEYSPACE <ks>                  → CREATE DATABASE [IF NOT EXISTS] <ks>
//   - CREATE TABLE <tbl> (...)              → CREATE TABLE [IF NOT EXISTS] <tbl> (...)
//   - INSERT INTO <tbl> (cols) VALUES (vs)  → UPSERT INTO <tbl> (cols) VALUES (...)
//   - SELECT ... FROM <tbl> WHERE ...       → SELECT ... FROM <tbl> WHERE ...
//
// CQL's INSERT is an upsert by default (last-write-wins), so it maps to UPSERT
// unless IF NOT EXISTS is present, in which case it maps to INSERT.
//
// CQL primary keys (partition keys + clustering keys) are mapped to a composite
// PRIMARY KEY in SQL. CQL clustering key ordering (WITH CLUSTERING ORDER BY) is
// tracked in TableMeta and used by PER PARTITION LIMIT to order rows within
// each partition correctly.
package translate

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/cockroachdb/errors"
)

// TableMeta holds metadata about a CQL table needed for translations that
// require schema context, such as PER PARTITION LIMIT, INSERT IF NOT EXISTS
// (lightweight transaction) result sets, static column propagation, and UDT
// field access resolution.
type TableMeta struct {
	PartitionKeys  []string          // partition key columns from PRIMARY KEY
	ClusteringKeys []string          // clustering key columns from PRIMARY KEY
	ClusteringDesc map[string]bool   // clustering key column name → true if DESC
	Columns        []string          // all column names in declaration order
	StaticColumns  map[string]bool   // lowercase names of STATIC columns (nil if none)
	ColumnTypes    map[string]string // lowercase col name → CRDB SQL type (e.g. "JSONB")
	ColumnCQLTypes map[string]string // lowercase col name → original CQL type name (e.g. "set", "list")
}

// SchemaInfo tracks table metadata accumulated from CREATE TABLE statements,
// enabling query translations that require schema context (e.g. PER PARTITION
// LIMIT needs partition key columns to generate a ROW_NUMBER window function).
type SchemaInfo struct {
	tables map[string]TableMeta
}

// NewSchemaInfo creates an empty SchemaInfo.
func NewSchemaInfo() *SchemaInfo {
	return &SchemaInfo{tables: make(map[string]TableMeta)}
}

// RecordTable stores metadata for a table. The key is the lowercase
// unqualified or qualified table name.
func (s *SchemaInfo) RecordTable(keyspace, table string, meta TableMeta) {
	key := strings.ToLower(table)
	if keyspace != "" {
		s.tables[strings.ToLower(keyspace)+"."+key] = meta
	}
	// Always store unqualified so lookups work regardless of keyspace context.
	s.tables[key] = meta
}

// LookupTable retrieves metadata for a table, trying qualified then
// unqualified names.
func (s *SchemaInfo) LookupTable(keyspace, table string) (TableMeta, bool) {
	if keyspace != "" {
		key := strings.ToLower(keyspace) + "." + strings.ToLower(table)
		if meta, ok := s.tables[key]; ok {
			return meta, true
		}
	}
	meta, ok := s.tables[strings.ToLower(table)]
	return meta, ok
}

// cqlFunctionToSQL maps lowercase CQL function names to their CockroachDB SQL
// equivalents. Functions not in this map are unsupported.
var cqlFunctionToSQL = map[string]string{
	"now":    "now",
	"uuid":   "gen_random_uuid",
	"count":  "count",
	"sum":    "sum",
	"avg":    "avg",
	"min":    "min",
	"max":    "max",
	"length": "length",
	// toJson maps to CRDB's to_jsonb which converts any value to JSONB.
	"tojson": "to_jsonb",
}

// blobConversions maps CQL blobAs<Type> and <Type>AsBlob function names
// (lowercased) to the CRDB SQL target type for CAST. These Cassandra functions
// perform binary-level type conversions; the CRDB CAST approximation works for
// compatible types (e.g. text↔blob) but may produce runtime errors for types
// that CRDB cannot directly cast (e.g. int↔bytes).
var blobConversions = map[string]string{
	"blobasint":       "INT4",
	"intasblob":       "BYTES",
	"blobastext":      "STRING",
	"textasblob":      "BYTES",
	"blobasvarchar":   "STRING",
	"varcharasblob":   "BYTES",
	"blobasbigint":    "INT8",
	"bigintasblob":    "BYTES",
	"blobasfloat":     "FLOAT4",
	"floatasblob":     "BYTES",
	"blobasdouble":    "FLOAT8",
	"doubleasblob":    "BYTES",
	"blobasboolean":   "BOOL",
	"booleanasblob":   "BYTES",
	"blobastimestamp": "TIMESTAMPTZ",
	"timestampasblob": "BYTES",
	"blobasuuid":      "UUID",
	"uuidasblob":      "BYTES",
	"blobastimeuuid":  "UUID",
	"timeuuidasblob":  "BYTES",
	"blobasinet":      "INET",
	"inetasblob":      "BYTES",
	"blobasascii":     "STRING",
	"asciiasblob":     "BYTES",
	"blobasdecimal":   "DECIMAL",
	"decimalasblob":   "BYTES",
	"blobasvarint":    "INT8",
	"varintasblob":    "BYTES",
	"blobascounter":   "INT8",
	"counterasblob":   "BYTES",
}

// cqlTypeToCRDBSQL maps CQL type names (as produced by the parser's DataType.Name)
// to CockroachDB SQL type names.
var cqlTypeToCRDBSQL = map[string]string{
	"text":      "STRING",
	"varchar":   "STRING",
	"ascii":     "STRING",
	"int":       "INT4",
	"bigint":    "INT8",
	"smallint":  "INT2",
	"tinyint":   "INT2",
	"float":     "FLOAT4",
	"double":    "FLOAT8",
	"boolean":   "BOOL",
	"timestamp": "TIMESTAMPTZ",
	"date":      "DATE",
	"time":      "TIME",
	"duration":  "INTERVAL",
	"uuid":      "UUID",
	"timeuuid":  "UUID",
	"blob":      "BYTES",
	"inet":      "INET",
	"counter":   "INT8",
	"varint":    "INT8",
	"decimal":   "DECIMAL",
	// Collection and tuple types are stored as JSONB. Lists, sets, and
	// tuples become JSON arrays; maps become JSON objects.
	"list":   "JSONB",
	"set":    "JSONB",
	"map":    "JSONB",
	"tuple":  "JSONB",
	"frozen": "JSONB",
}

// Result holds the output of translating a CQL statement. SQL is the primary
// statement to execute. SetupSQL contains any additional statements that should
// be run before SQL (e.g. SET commands for keyspace context).
type Result struct {
	// SQL is the translated SQL statement.
	SQL string
	// Params contains positional parameter values extracted from CQL literals.
	// Bind markers (? and :name) are left as $N placeholders; literal values
	// are inlined into the SQL string.
	Params []interface{}
	// PropagateStaticSQL is an optional UPDATE that propagates static column
	// values across all rows in the same partition. Generated when an INSERT
	// or UPDATE modifies static columns in a CQL table.
	PropagateStaticSQL    string
	PropagateStaticParams []interface{}
	// Table and Keyspace identify the source table for SELECT results, enabling
	// the executor to look up original CQL column types for display formatting.
	Table    string
	Keyspace string
}

// Translate converts a CQL AST statement into a CockroachDB SQL Result.
// This is a convenience wrapper around TranslateWithSchema with no schema
// context; PER PARTITION LIMIT will return an error without schema info.
func Translate(stmt parser.Statement) (Result, error) {
	return TranslateWithSchema(stmt, nil)
}

// TranslateWithSchema converts a CQL AST statement into a CockroachDB SQL
// Result. When schema is non-nil, translations that require table metadata
// (e.g. PER PARTITION LIMIT) can use it to look up partition key columns.
func TranslateWithSchema(stmt parser.Statement, schema *SchemaInfo) (Result, error) {
	switch s := stmt.(type) {
	case *parser.UseStatement:
		return translateUse(s)
	case *parser.CreateKeyspaceStatement:
		return translateCreateKeyspace(s)
	case *parser.CreateTableStatement:
		return translateCreateTable(s)
	case *parser.InsertStatement:
		return translateInsert(s, schema)
	case *parser.SelectStatement:
		return translateSelect(s, schema)
	case *parser.UpdateStatement:
		return translateUpdate(s, schema)
	case *parser.DeleteStatement:
		return translateDelete(s)
	case *parser.CreateIndexStatement:
		return translateCreateIndex(s)
	case *parser.AlterTableStatement:
		return translateAlterTable(s, schema)
	case *parser.AlterKeyspaceStatement:
		return translateAlterKeyspace(s)
	case *parser.DropStatement:
		return translateDrop(s)
	case *parser.TruncateStatement:
		return translateTruncate(s)
	case *parser.BatchStatement:
		return translateBatch(s)
	case *parser.CreateTypeStatement:
		return translateCreateType(s)
	case *parser.AlterTypeStatement:
		return translateAlterType(s)
	default:
		return Result{}, errors.Newf("unsupported CQL statement type: %T", stmt)
	}
}

// translateUse maps USE <keyspace> to SET database = '<keyspace>'.
func translateUse(s *parser.UseStatement) (Result, error) {
	sql := fmt.Sprintf("SET database = %s", quoteLiteral(s.Keyspace))
	return Result{SQL: sql}, nil
}

// translateCreateKeyspace maps CREATE KEYSPACE to CREATE DATABASE.
// Replication options are acknowledged in a comment but cannot be directly
// mapped — CRDB uses zone configurations instead.
func translateCreateKeyspace(s *parser.CreateKeyspaceStatement) (Result, error) {
	var sb strings.Builder
	sb.WriteString("CREATE DATABASE ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(quoteIdent(s.Keyspace))
	return Result{SQL: sb.String()}, nil
}

// translateCreateTable maps a CQL CREATE TABLE to a CRDB CREATE TABLE. The
// primary key is formed from partition keys followed by clustering keys.
func translateCreateTable(s *parser.CreateTableStatement) (Result, error) {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	sb.WriteString(" (")

	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sqlType := CqlTypeToSQL(col.DataType)
		if sqlType == "" {
			return Result{}, errors.Newf(
				"unsupported CQL type %q for column %q", col.DataType.Name, col.Name,
			)
		}
		sb.WriteString(quoteIdent(col.Name))
		sb.WriteByte(' ')
		sb.WriteString(sqlType)
	}

	// PRIMARY KEY: partition keys + clustering keys.
	pkCols := make([]string, 0,
		len(s.PrimaryKey.PartitionKeys)+len(s.PrimaryKey.ClusteringKeys))
	pkCols = append(pkCols, s.PrimaryKey.PartitionKeys...)
	pkCols = append(pkCols, s.PrimaryKey.ClusteringKeys...)
	if len(pkCols) > 0 {
		sb.WriteString(", PRIMARY KEY (")
		for i, col := range pkCols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(col))
		}
		sb.WriteByte(')')
	}

	sb.WriteByte(')')
	return Result{SQL: sb.String()}, nil
}

// translateCreateIndex maps CQL CREATE INDEX to CRDB CREATE INDEX. Collection
// index functions (KEYS, VALUES, ENTRIES, FULL) and custom indexes (USING
// class) are not supported.
func translateCreateIndex(s *parser.CreateIndexStatement) (Result, error) {
	if s.IsCustom {
		return Result{}, errors.Newf("CUSTOM INDEX is not supported")
	}
	for _, col := range s.Columns {
		if col.Function != "" {
			return Result{}, errors.Newf(
				"collection index function %s() is not supported", col.Function,
			)
		}
	}

	var sb strings.Builder
	sb.WriteString("CREATE INDEX ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	if s.IndexName != "" {
		sb.WriteString(quoteIdent(s.IndexName))
		sb.WriteByte(' ')
	}
	sb.WriteString("ON ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	sb.WriteString(" (")
	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(col.Name))
	}
	sb.WriteByte(')')
	return Result{SQL: sb.String()}, nil
}

// translateInsert maps CQL INSERT INTO to CRDB SQL. CQL INSERT is an upsert
// (last-write-wins) unless IF NOT EXISTS is specified, in which case it is a
// conditional insert. When schema is available and the target table has static
// columns, a propagation UPDATE is generated to synchronize static values
// across all rows in the partition.
func translateInsert(s *parser.InsertStatement, schema *SchemaInfo) (Result, error) {
	if s.JSON {
		return translateInsertJSON(s)
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	if s.IfNotExists {
		sb.WriteString("INSERT INTO ")
	} else {
		// CQL INSERT without IF NOT EXISTS is an upsert.
		sb.WriteString("UPSERT INTO ")
	}
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))

	// Column list.
	sb.WriteString(" (")
	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(col))
	}
	sb.WriteString(") VALUES (")

	// Values.
	for i, val := range s.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sqlVal, param, err := exprToSQL(val, &paramIdx)
		if err != nil {
			return Result{}, errors.Wrap(err, "translating INSERT value")
		}
		sb.WriteString(sqlVal)
		if param != nil {
			params = append(params, param)
		}
	}
	sb.WriteByte(')')

	if s.IfNotExists {
		sb.WriteString(" ON CONFLICT DO NOTHING")
	}

	result := Result{SQL: sb.String(), Params: params}

	// Generate static column propagation if the table has static columns.
	if schema != nil {
		propSQL, propParams := buildInsertStaticPropagation(s, schema)
		if propSQL != "" {
			result.PropagateStaticSQL = propSQL
			result.PropagateStaticParams = propParams
		}
	}

	return result, nil
}

// translateSelect maps CQL SELECT to CRDB SQL SELECT.
func translateSelect(s *parser.SelectStatement, schema *SchemaInfo) (Result, error) {
	if s.JSON {
		return translateSelectJSON(s, schema)
	}
	if s.PerPartitionLimit != nil {
		return translateSelectPPL(s, schema)
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	if s.Distinct {
		sb.WriteString("SELECT DISTINCT ")
	} else {
		sb.WriteString("SELECT ")
	}

	// Column list.
	if err := writeSelectColumns(&sb, s.Columns, &params, &paramIdx, schema, s.Keyspace, s.Table); err != nil {
		return Result{}, err
	}

	if err := writeFromAndClauses(&sb, s, &params, &paramIdx); err != nil {
		return Result{}, err
	}

	return Result{
		SQL: sb.String(), Params: params,
		Table: s.Table, Keyspace: s.Keyspace,
	}, nil
}

// writeFromAndClauses appends FROM, WHERE, GROUP BY, ORDER BY, and LIMIT
// clauses to sb for a SELECT statement. Shared by normal and JSON SELECT
// translation.
func writeFromAndClauses(
	sb *strings.Builder, s *parser.SelectStatement, params *[]interface{}, paramIdx *int,
) error {
	sb.WriteString(" FROM ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))

	if len(s.Where) > 0 {
		sb.WriteString(" WHERE ")
		if err := writeWhereClauses(sb, s.Where, params, paramIdx); err != nil {
			return err
		}
	}

	if len(s.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, col := range s.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(col))
		}
	}

	if len(s.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ob := range s.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(ob.Column))
			if ob.Desc {
				sb.WriteString(" DESC")
			}
		}
	}

	if s.Limit != nil {
		sb.WriteString(" LIMIT ")
		sqlVal, param, err := exprToSQL(s.Limit, paramIdx)
		if err != nil {
			return errors.Wrap(err, "translating LIMIT value")
		}
		sb.WriteString(sqlVal)
		if param != nil {
			*params = append(*params, param)
		}
	}

	return nil
}

// translateSelectPPL translates a SELECT with PER PARTITION LIMIT into a
// subquery that uses ROW_NUMBER() OVER (PARTITION BY <pk_cols>) to limit
// rows per partition. The generated SQL has the form:
//
//	SELECT <cols> FROM (
//	  SELECT <cols>, row_number() OVER (PARTITION BY <pk>) AS "__cql_rn"
//	  FROM <table> [WHERE ...] [GROUP BY ...]
//	) AS "__cql_ppl" WHERE "__cql_rn" <= <N> [ORDER BY ...] [LIMIT <M>]
func translateSelectPPL(s *parser.SelectStatement, schema *SchemaInfo) (Result, error) {
	if schema == nil {
		return Result{}, errors.Newf(
			"PER PARTITION LIMIT requires schema info; table %q not registered",
			s.Table)
	}
	meta, ok := schema.LookupTable(s.Keyspace, s.Table)
	if !ok {
		return Result{}, errors.Newf(
			"PER PARTITION LIMIT: unknown table %q", s.Table)
	}
	if len(meta.PartitionKeys) == 0 {
		return Result{}, errors.Newf(
			"PER PARTITION LIMIT: no partition keys for table %q", s.Table)
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	isSelectStar := len(s.Columns) == 1 &&
		s.Columns[0].Column == "*" &&
		s.Columns[0].Expr == nil

	// Outer SELECT: reference columns by name to exclude __cql_rn.
	sb.WriteString("SELECT ")
	if s.Distinct {
		sb.WriteString("DISTINCT ")
	}
	if isSelectStar {
		for i, col := range meta.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(col))
		}
	} else {
		for i, sel := range s.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			if sel.Alias != "" {
				sb.WriteString(quoteIdent(sel.Alias))
			} else if sel.Expr == nil && sel.Column != "" {
				sb.WriteString(quoteIdent(sel.Column))
			} else {
				return Result{}, errors.Newf(
					"PER PARTITION LIMIT with expression selectors requires an AS alias")
			}
		}
	}

	// Inner subquery.
	sb.WriteString(" FROM (SELECT ")
	if err := writeSelectColumns(&sb, s.Columns, &params, &paramIdx, schema, s.Keyspace, s.Table); err != nil {
		return Result{}, err
	}

	// ROW_NUMBER window function partitioned by partition key columns,
	// ordered by clustering key columns to respect CLUSTERING ORDER BY.
	sb.WriteString(`, row_number() OVER (PARTITION BY `)
	for i, pk := range meta.PartitionKeys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(pk))
	}
	if len(meta.ClusteringKeys) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ck := range meta.ClusteringKeys {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(ck))
			if meta.ClusteringDesc[ck] {
				sb.WriteString(" DESC")
			}
		}
	}
	sb.WriteString(`) AS "__cql_rn"`)

	// FROM, WHERE, GROUP BY for the inner query.
	sb.WriteString(" FROM ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	if len(s.Where) > 0 {
		sb.WriteString(" WHERE ")
		if err := writeWhereClauses(&sb, s.Where, &params, &paramIdx); err != nil {
			return Result{}, err
		}
	}
	if len(s.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, col := range s.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(col))
		}
	}
	sb.WriteString(`) AS "__cql_ppl"`)

	// Outer WHERE: filter by row number.
	sb.WriteString(` WHERE "__cql_rn" <= `)
	pplVal, pplParam, err := exprToSQL(s.PerPartitionLimit, &paramIdx)
	if err != nil {
		return Result{}, errors.Wrap(err, "translating PER PARTITION LIMIT value")
	}
	sb.WriteString(pplVal)
	if pplParam != nil {
		params = append(params, pplParam)
	}

	// ORDER BY on outer query.
	if len(s.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ob := range s.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(ob.Column))
			if ob.Desc {
				sb.WriteString(" DESC")
			}
		}
	}

	// LIMIT on outer query.
	if s.Limit != nil {
		sb.WriteString(" LIMIT ")
		sqlVal, param, err := exprToSQL(s.Limit, &paramIdx)
		if err != nil {
			return Result{}, errors.Wrap(err, "translating LIMIT value")
		}
		sb.WriteString(sqlVal)
		if param != nil {
			params = append(params, param)
		}
	}

	return Result{
		SQL: sb.String(), Params: params,
		Table: s.Table, Keyspace: s.Keyspace,
	}, nil
}

// writeSelectColumns writes the column list from a SELECT statement's
// selectors. Shared by translateSelect and translateSelectPPL.
//
// When schema, keyspace, and table are provided, UDT field access
// expressions (col.field) are translated using JSONB extraction for
// JSONB-backed columns or composite type syntax for others.
//
// Function call selectors without an explicit alias get an automatic
// Cassandra-style alias of the form "system.<func>(<args>)" (W8). This
// matches real Cassandra's column naming convention for function results.
func writeSelectColumns(
	sb *strings.Builder,
	columns []parser.Selector,
	params *[]interface{},
	paramIdx *int,
	schema *SchemaInfo,
	keyspace, table string,
) error {
	for i, sel := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		if sel.Expr != nil {
			// Handle UDT field access with schema-aware translation.
			if fa, ok := sel.Expr.(*parser.FieldAccessExpr); ok {
				sb.WriteString(fieldAccessToSQL(fa, schema, keyspace, table))
			} else {
				sqlExpr, p, err := exprToSQL(sel.Expr, paramIdx)
				if err != nil {
					return errors.Wrap(err, "translating selector")
				}
				sb.WriteString(sqlExpr)
				if fc, ok := sel.Expr.(*parser.FunctionCall); ok && sel.Alias == "" {
					// Cassandra names function result columns as
					// "system.<func>(<args>)" when no explicit alias is given.
					sb.WriteString(` AS "`)
					sb.WriteString(cqlFuncAlias(fc))
					sb.WriteString(`"`)
				}
				if p != nil {
					*params = append(*params, p)
				}
			}
			if sel.Alias != "" {
				sb.WriteString(" AS ")
				sb.WriteString(quoteIdent(sel.Alias))
			}
		} else if sel.Column == "*" {
			sb.WriteByte('*')
		} else {
			sb.WriteString(quoteIdent(sel.Column))
			if sel.Alias != "" {
				sb.WriteString(" AS ")
				sb.WriteString(quoteIdent(sel.Alias))
			}
		}
	}
	return nil
}

// cqlFuncAlias generates a Cassandra-style column name for a function
// call result. Cassandra names unaliased function result columns as
// "system.<func>(<args>)" where func is the original CQL function name
// (not the translated SQL name) and args are the original CQL argument
// expressions. Metadata pseudo-functions (writetime, ttl) omit the
// "system." prefix, matching Cassandra's actual column naming.
func cqlFuncAlias(fc *parser.FunctionCall) string {
	var sb strings.Builder
	lower := strings.ToLower(fc.Name)
	// writetime() and ttl() are metadata pseudo-functions — Cassandra
	// names their result columns without the "system." prefix.
	if lower != "writetime" && lower != "ttl" {
		sb.WriteString("system.")
	}
	sb.WriteString(lower)
	sb.WriteByte('(')
	for i, arg := range fc.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		switch a := arg.(type) {
		case *parser.ColumnRef:
			sb.WriteString(a.Name)
		case *parser.StarExpr:
			sb.WriteByte('*')
		case *parser.FunctionCall:
			sb.WriteString(cqlFuncAlias(a))
		default:
			sb.WriteByte('?')
		}
	}
	sb.WriteByte(')')
	return sb.String()
}

// TranslateSelectWithFrom translates a CQL SELECT column list and
// appends the given fromClause instead of deriving one from a table
// reference. If fromClause is empty, no FROM is emitted, producing a
// standalone SELECT expression (e.g. "SELECT now()"). This is used
// for projected system table queries where the real table does not
// exist in CRDB.
func TranslateSelectWithFrom(columns []parser.Selector, fromClause string) (Result, error) {
	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("SELECT ")
	if err := writeSelectColumns(&sb, columns, &params, &paramIdx, nil, "", ""); err != nil {
		return Result{}, err
	}
	if fromClause != "" {
		sb.WriteString(" FROM ")
		sb.WriteString(fromClause)
	}
	return Result{SQL: sb.String(), Params: params}, nil
}

// writeWhereClauses writes WHERE conditions to sb, handling both plain column
// references and function call expressions on the left-hand side.
func writeWhereClauses(
	sb *strings.Builder, where []parser.WhereClause, params *[]interface{}, paramIdx *int,
) error {
	for i, w := range where {
		if i > 0 {
			sb.WriteString(" AND ")
		}

		// Left-hand side: function call or plain column.
		writeCol := func() error {
			if w.ColumnExpr != nil {
				colSQL, _, err := exprToSQL(w.ColumnExpr, paramIdx)
				if err != nil {
					return errors.Wrap(err, "translating WHERE expression")
				}
				sb.WriteString(colSQL)
			} else {
				sb.WriteString(quoteIdent(w.Column))
			}
			return nil
		}

		if w.Operator == "IN" {
			tuple, ok := w.Value.(*parser.TupleLiteral)
			if !ok {
				return errors.Newf("IN operator requires tuple value")
			}
			// Multi-column IN: (col1, col2) IN ((1,'a'), (2,'b')).
			if len(w.Columns) > 0 {
				sb.WriteByte('(')
				for j, c := range w.Columns {
					if j > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString(quoteIdent(c))
				}
				sb.WriteByte(')')
			} else {
				if err := writeCol(); err != nil {
					return err
				}
			}
			sb.WriteString(" IN (")
			for j, val := range tuple.Values {
				if j > 0 {
					sb.WriteString(", ")
				}
				// For multi-column IN, inner tuples should render as
				// SQL row constructors (val1, val2), not jsonb_build_array.
				if innerTup, ok := val.(*parser.TupleLiteral); ok && len(w.Columns) > 0 {
					sb.WriteByte('(')
					for k, elem := range innerTup.Values {
						if k > 0 {
							sb.WriteString(", ")
						}
						elemSQL, param, err := exprToSQL(elem, paramIdx)
						if err != nil {
							return errors.Wrap(err, "translating IN tuple element")
						}
						sb.WriteString(elemSQL)
						if param != nil {
							*params = append(*params, param)
						}
					}
					sb.WriteByte(')')
				} else {
					sqlVal, param, err := exprToSQL(val, paramIdx)
					if err != nil {
						return errors.Wrap(err, "translating IN value")
					}
					sb.WriteString(sqlVal)
					if param != nil {
						*params = append(*params, param)
					}
				}
			}
			sb.WriteByte(')')
		} else if w.Operator == "CONTAINS" {
			// CQL CONTAINS on list/set → CRDB JSONB array containment.
			if err := writeCol(); err != nil {
				return err
			}
			valSQL, param, err := exprToSQL(w.Value, paramIdx)
			if err != nil {
				return errors.Wrap(err, "translating CONTAINS value")
			}
			sb.WriteString(" @> jsonb_build_array(")
			sb.WriteString(valSQL)
			sb.WriteByte(')')
			if param != nil {
				*params = append(*params, param)
			}
		} else if w.Operator == "CONTAINS KEY" {
			// CQL CONTAINS KEY on map → CRDB JSONB key existence.
			if err := writeCol(); err != nil {
				return err
			}
			valSQL, param, err := exprToSQL(w.Value, paramIdx)
			if err != nil {
				return errors.Wrap(err, "translating CONTAINS KEY value")
			}
			sb.WriteString(" ? ")
			sb.WriteString(valSQL)
			if param != nil {
				*params = append(*params, param)
			}
		} else {
			if err := writeCol(); err != nil {
				return err
			}
			sb.WriteByte(' ')
			sb.WriteString(w.Operator)
			sb.WriteByte(' ')
			sqlVal, param, err := exprToSQL(w.Value, paramIdx)
			if err != nil {
				return errors.Wrap(err, "translating WHERE value")
			}
			sb.WriteString(sqlVal)
			if param != nil {
				*params = append(*params, param)
			}
		}
	}
	return nil
}

// translateUpdate maps CQL UPDATE to CRDB SQL UPDATE. When schema is
// available and the table has static columns, a propagation UPDATE is
// generated to synchronize static values across all partition rows.
//
// Counter UPDATEs (where all assignments are counter increment/decrement
// expressions like c = c + 1) are translated to INSERT ON CONFLICT DO UPDATE
// to provide Cassandra's upsert semantics: the row is created if it doesn't
// exist, starting from 0.
//
// CQL UPDATE with IF EXISTS or IF conditions uses conditional logic. IF EXISTS
// is a no-op guard (SQL UPDATE WHERE is already a no-op for missing rows).
// IF conditions are appended as additional WHERE predicates so the UPDATE
// only executes when the conditions are satisfied.
func translateUpdate(s *parser.UpdateStatement, schema *SchemaInfo) (Result, error) {
	// Counter UPDATEs get upsert semantics when schema info is available.
	// Cassandra counter UPDATEs implicitly create the row if it doesn't
	// exist; a plain SQL UPDATE would be a no-op on missing rows.
	if isCounterUpdate(s) && schema != nil {
		meta, ok := schema.LookupTable(s.Keyspace, s.Table)
		if ok && len(meta.PartitionKeys)+len(meta.ClusteringKeys) > 0 {
			return translateCounterUpdate(s, meta)
		}
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("UPDATE ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	sb.WriteString(" SET ")

	for i, a := range s.Assignments {
		if i > 0 {
			sb.WriteString(", ")
		}
		if a.Subscript != nil {
			// Subscript assignment: col['key'] = val or col[0] = val →
			// "col" = jsonb_set("col", ARRAY[key::TEXT], to_jsonb(val))
			// jsonb_set requires a text[] path, so we cast the index.
			keySQL, _, err := exprToSQL(a.Subscript, &paramIdx)
			if err != nil {
				return Result{}, errors.Wrap(err, "translating subscript key")
			}
			valSQL, param, err := exprToSQL(a.Value, &paramIdx)
			if err != nil {
				return Result{}, errors.Wrap(err, "translating subscript value")
			}
			col := quoteIdent(a.Column)
			sb.WriteString(fmt.Sprintf(
				"%s = jsonb_set(%s, ARRAY[CAST(%s AS TEXT)], to_jsonb(%s))",
				col, col, keySQL, valSQL,
			))
			if param != nil {
				params = append(params, param)
			}
		} else {
			sb.WriteString(quoteIdent(a.Column))
			sb.WriteString(" = ")
			sqlVal, param, err := exprToSQL(a.Value, &paramIdx)
			if err != nil {
				return Result{}, errors.Wrap(err, "translating SET value")
			}
			sb.WriteString(sqlVal)
			if param != nil {
				params = append(params, param)
			}
		}
	}

	sb.WriteString(" WHERE ")
	if err := writeWhereClauses(&sb, s.Where, &params, &paramIdx); err != nil {
		return Result{}, err
	}

	// IF conditions are appended as additional WHERE predicates.
	if len(s.IfConds) > 0 {
		sb.WriteString(" AND ")
		if err := writeWhereClauses(&sb, s.IfConds, &params, &paramIdx); err != nil {
			return Result{}, err
		}
	}

	result := Result{SQL: sb.String(), Params: params}

	// Generate static column propagation if the table has static columns.
	if schema != nil {
		propSQL, propParams := buildUpdateStaticPropagation(s, schema)
		if propSQL != "" {
			result.PropagateStaticSQL = propSQL
			result.PropagateStaticParams = propParams
		}
	}

	return result, nil
}

// isCounterUpdate returns true if all SET assignments in the UPDATE are
// counter increment/decrement expressions (e.g. c = c + 1). Collection
// appends (c = c + [1,2]) are excluded — those use JSONB concatenation,
// not counter semantics.
func isCounterUpdate(s *parser.UpdateStatement) bool {
	if len(s.Assignments) == 0 {
		return false
	}
	for _, a := range s.Assignments {
		ce, ok := a.Value.(*parser.CounterExpr)
		if !ok {
			return false
		}
		if isCollectionExpr(ce.Value) {
			return false
		}
	}
	return true
}

// translateCounterUpdate translates a CQL counter UPDATE into an INSERT
// ON CONFLICT DO UPDATE for upsert semantics. In Cassandra, counter
// UPDATEs create the row implicitly if it doesn't exist (starting from
// 0); a plain SQL UPDATE would be a no-op on missing rows.
//
// For example:
//
//	UPDATE counters SET c = c + 1 WHERE id = 1
//
// becomes:
//
//	INSERT INTO "counters" ("id", "c") VALUES (1, 1)
//	  ON CONFLICT ("id") DO UPDATE SET "c" = "counters"."c" + 1
func translateCounterUpdate(s *parser.UpdateStatement, meta TableMeta) (Result, error) {
	pkCols := make([]string, 0, len(meta.PartitionKeys)+len(meta.ClusteringKeys))
	pkCols = append(pkCols, meta.PartitionKeys...)
	pkCols = append(pkCols, meta.ClusteringKeys...)

	// Map WHERE clause columns to their value expressions for PK lookup.
	whereMap := make(map[string]parser.Expr, len(s.Where))
	for _, w := range s.Where {
		if w.Operator == "=" {
			whereMap[strings.ToLower(w.Column)] = w.Value
		}
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("INSERT INTO ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	sb.WriteString(" (")

	// Column list: PK columns followed by counter columns.
	for i, pk := range pkCols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(pk))
	}
	for _, a := range s.Assignments {
		sb.WriteString(", ")
		sb.WriteString(quoteIdent(a.Column))
	}

	sb.WriteString(") VALUES (")

	// PK values extracted from WHERE clause.
	for i, pk := range pkCols {
		if i > 0 {
			sb.WriteString(", ")
		}
		val, ok := whereMap[strings.ToLower(pk)]
		if !ok {
			return Result{}, errors.Newf(
				"counter UPDATE requires = condition for primary key column %q", pk)
		}
		sqlVal, param, err := exprToSQL(val, &paramIdx)
		if err != nil {
			return Result{}, errors.Wrap(err, "translating counter WHERE value")
		}
		sb.WriteString(sqlVal)
		if param != nil {
			params = append(params, param)
		}
	}

	// Counter initial values (row starts from 0, so + N → N, - N → 0 - N).
	for _, a := range s.Assignments {
		sb.WriteString(", ")
		ce := a.Value.(*parser.CounterExpr)
		valSQL, param, err := exprToSQL(ce.Value, &paramIdx)
		if err != nil {
			return Result{}, errors.Wrap(err, "translating counter value")
		}
		if ce.Op == "-" {
			sb.WriteString("0 - ")
		}
		sb.WriteString(valSQL)
		if param != nil {
			params = append(params, param)
		}
	}

	sb.WriteString(") ON CONFLICT (")

	for i, pk := range pkCols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(pk))
	}

	sb.WriteString(") DO UPDATE SET ")

	// Counter increment/decrement referencing the existing row via the
	// table name qualifier.
	tblRef := quoteIdent(s.Table)
	for i, a := range s.Assignments {
		if i > 0 {
			sb.WriteString(", ")
		}
		ce := a.Value.(*parser.CounterExpr)
		col := quoteIdent(a.Column)
		valSQL, param, err := exprToSQL(ce.Value, &paramIdx)
		if err != nil {
			return Result{}, errors.Wrap(err, "translating counter update value")
		}
		sb.WriteString(fmt.Sprintf(
			"%s = %s.%s %s %s", col, tblRef, col, ce.Op, valSQL))
		if param != nil {
			params = append(params, param)
		}
	}

	return Result{SQL: sb.String(), Params: params}, nil
}

// translateDelete maps CQL DELETE to CRDB SQL.
//
// When columns are specified (DELETE col1, col2 FROM ...), the statement is a
// column-level DELETE (Cassandra tombstone semantics): the named columns are
// set to NULL. This translates to UPDATE ... SET col1 = NULL, col2 = NULL
// WHERE ....
//
// When no columns are specified, the statement deletes entire rows.
//
// CQL DELETE with IF EXISTS or IF conditions is conditional. IF EXISTS is a
// no-op guard (the SQL DELETE WHERE already handles missing rows). IF conditions
// are appended as additional WHERE predicates so the statement only executes
// when the conditions are satisfied.
func translateDelete(s *parser.DeleteStatement) (Result, error) {
	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	if len(s.Columns) > 0 {
		// Column-level DELETE → UPDATE ... SET col = NULL (or JSONB key removal
		// for subscripted targets like col['key']).
		sb.WriteString("UPDATE ")
		sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
		sb.WriteString(" SET ")
		for i, target := range s.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			col := quoteIdent(target.Column)
			if target.Subscript != nil {
				// DELETE col['key'] → col = col - 'key' (JSONB key/element removal).
				keySQL, param, err := exprToSQL(target.Subscript, &paramIdx)
				if err != nil {
					return Result{}, errors.Wrap(err, "translating DELETE subscript")
				}
				sb.WriteString(fmt.Sprintf("%s = %s - %s", col, col, keySQL))
				if param != nil {
					params = append(params, param)
				}
			} else {
				sb.WriteString(col)
				sb.WriteString(" = NULL")
			}
		}
	} else {
		sb.WriteString("DELETE FROM ")
		sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	}

	sb.WriteString(" WHERE ")

	if err := writeWhereClauses(&sb, s.Where, &params, &paramIdx); err != nil {
		return Result{}, err
	}

	// IF conditions are appended as additional WHERE predicates.
	if len(s.IfConds) > 0 {
		sb.WriteString(" AND ")
		if err := writeWhereClauses(&sb, s.IfConds, &params, &paramIdx); err != nil {
			return Result{}, err
		}
	}

	return Result{SQL: sb.String(), Params: params}, nil
}

// ConsistencyToIsolation maps a CQL consistency level string to a CRDB
// transaction isolation level. CQL consistency levels don't map perfectly;
// we approximate:
//
//   - ONE, LOCAL_ONE, ANY           → READ COMMITTED (weaker)
//   - QUORUM, LOCAL_QUORUM, EACH_QUORUM → SERIALIZABLE (default)
//   - ALL                           → SERIALIZABLE
//   - SERIAL, LOCAL_SERIAL          → SERIALIZABLE (lightweight transactions)
func ConsistencyToIsolation(consistency string) string {
	switch strings.ToUpper(consistency) {
	case "ONE", "LOCAL_ONE", "ANY":
		return "READ COMMITTED"
	default:
		return "SERIALIZABLE"
	}
}

// fieldAccessToSQL translates a CQL field access expression (col.field) to
// CRDB SQL. When schema context is available, JSONB-backed columns use the
// JSONB text extraction operator (->>'field'); composite type columns use
// the standard (col).field syntax.
func fieldAccessToSQL(
	fa *parser.FieldAccessExpr, schema *SchemaInfo, keyspace, table string,
) string {
	if schema != nil {
		if meta, ok := schema.LookupTable(keyspace, table); ok {
			if colType, ok := meta.ColumnTypes[strings.ToLower(fa.Column)]; ok {
				if colType == "JSONB" {
					return fmt.Sprintf(
						"%s->>%s", quoteIdent(fa.Column), quoteLiteral(strings.ToLower(fa.Field)),
					)
				}
			}
		}
	}
	// Default to composite type field access.
	return fmt.Sprintf("(%s).%s", quoteIdent(fa.Column), quoteIdent(fa.Field))
}

// exprToSQL converts a CQL expression to its SQL representation. Literal values
// are inlined. Bind markers (? and :name) become $N positional placeholders.
func exprToSQL(e parser.Expr, paramIdx *int) (string, interface{}, error) {
	switch v := e.(type) {
	case *parser.StringLiteral:
		return quoteLiteral(v.Value), nil, nil
	case *parser.IntegerLiteral:
		return fmt.Sprintf("%d", v.Value), nil, nil
	case *parser.BigIntLiteral:
		return v.Value + "::DECIMAL", nil, nil
	case *parser.BlobLiteral:
		return fmt.Sprintf("'\\x%s'::BYTEA", strings.ToLower(v.Value)), nil, nil
	case *parser.FloatLiteral:
		return fmt.Sprintf("%g", v.Value), nil, nil
	case *parser.BoolLiteral:
		if v.Value {
			return "true", nil, nil
		}
		return "false", nil, nil
	case *parser.UUIDLiteral:
		return quoteLiteral(v.Value), nil, nil
	case *parser.NullLiteral:
		return "NULL", nil, nil
	case *parser.BindMarker:
		placeholder := fmt.Sprintf("$%d", *paramIdx)
		*paramIdx++
		return placeholder, placeholder, nil
	case *parser.NamedBindMarker:
		placeholder := fmt.Sprintf("$%d", *paramIdx)
		*paramIdx++
		return placeholder, placeholder, nil
	case *parser.CounterExpr:
		// Collection operations use JSONB concatenation (||) instead of
		// arithmetic +/-. Detect by checking the value expression type.
		if isCollectionExpr(v.Value) {
			return collectionBinaryToSQL(v.Column, v.Op, v.Value, paramIdx)
		}
		valSQL, param, err := exprToSQL(v.Value, paramIdx)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s %s", quoteIdent(v.Column), v.Op, valSQL), param, nil
	case *parser.CollectionOpExpr:
		leftSQL, lParam, err := exprToSQL(v.Left, paramIdx)
		if err != nil {
			return "", nil, err
		}
		rightSQL, rParam, err := exprToSQL(v.Right, paramIdx)
		if err != nil {
			return "", nil, err
		}
		var param interface{}
		if lParam != nil {
			param = lParam
		}
		if rParam != nil {
			param = rParam
		}
		return fmt.Sprintf("%s || %s", leftSQL, rightSQL), param, nil
	case *parser.ListLiteral:
		return listLiteralToSQL(v, paramIdx)
	case *parser.SetLiteral:
		return setLiteralToSQL(v, paramIdx)
	case *parser.MapExprLiteral:
		return mapLiteralToSQL(v, paramIdx)
	case *parser.TupleLiteral:
		return tupleLiteralToSQL(v, paramIdx)
	case *parser.ColumnRef:
		return quoteIdent(v.Name), nil, nil
	case *parser.StarExpr:
		return "*", nil, nil
	case *parser.FunctionCall:
		return functionCallToSQL(v, paramIdx)
	case *parser.CastExpr:
		return castExprToSQL(v, paramIdx)
	case *parser.SubscriptExpr:
		// CQL col[index] → CRDB "col"->index for JSONB element access.
		idxSQL, param, err := exprToSQL(v.Index, paramIdx)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s->%s", quoteIdent(v.Column), idxSQL), param, nil
	case *parser.FieldAccessExpr:
		// CQL col.field → CRDB ("col")."field" for composite type field access.
		return fmt.Sprintf("(%s).%s", quoteIdent(v.Column), quoteIdent(v.Field)), nil, nil
	default:
		return "", nil, errors.Newf("unsupported expression type: %T", e)
	}
}

// functionCallToSQL translates a CQL function call to CRDB SQL.
func functionCallToSQL(fc *parser.FunctionCall, paramIdx *int) (string, interface{}, error) {
	lower := strings.ToLower(fc.Name)

	// Handle special functions that translate to casts or expressions,
	// not simple function-name substitutions.
	switch lower {
	case "totimestamp", "dateof":
		return timeuuidStubTimestamp(fc, paramIdx)
	case "todate":
		return toDateSQL(fc, paramIdx)
	case "tounixtimestamp":
		return extractEpochToSQL(fc, paramIdx)
	case "unixtimestampof":
		return timeuuidStubEpoch(fc, paramIdx)
	case "mintimeuuid", "maxtimeuuid":
		// Cassandra functions that generate UUID boundaries for a timestamp.
		// CRDB does not have timeuuids; return gen_random_uuid() for syntax
		// compatibility.
		return "gen_random_uuid()", nil, nil
	case "token":
		return tokenToSQL(fc, paramIdx)
	case "writetime":
		// Cassandra per-cell write timestamp in microseconds. CRDB does
		// not track per-cell timestamps, but crdb_internal_mvcc_timestamp
		// gives the per-row MVCC timestamp in nanoseconds. Dividing by
		// 1000 converts to the microsecond precision CQL clients expect.
		if len(fc.Args) != 1 {
			return "", nil, errors.Newf("writetime() requires exactly one argument")
		}
		// Process arg for bind marker tracking; the value is unused since
		// CRDB tracks timestamps per-row, not per-cell.
		if _, _, err := exprToSQL(fc.Args[0], paramIdx); err != nil {
			return "", nil, err
		}
		return "(crdb_internal_mvcc_timestamp / 1000)::INT8", nil, nil
	case "ttl":
		// Cassandra per-cell TTL metadata. CRDB does not support per-cell
		// TTL; return NULL (same as Cassandra for rows without TTL).
		if len(fc.Args) != 1 {
			return "", nil, errors.Newf("ttl() requires exactly one argument")
		}
		if _, _, err := exprToSQL(fc.Args[0], paramIdx); err != nil {
			return "", nil, err
		}
		return "NULL::INT4", nil, nil
	case "fromjson":
		return singleArgCast(fc, paramIdx, "JSONB")
	}

	// Handle typeAsBlob / blobAsType conversion functions.
	if sqlType, ok := blobConversions[lower]; ok {
		if sqlType == "BYTES" {
			// <type>AsBlob: cast through STRING because CRDB cannot directly
			// cast most types (int, float, uuid, etc.) to BYTES.
			return singleArgCastThroughString(fc, paramIdx, "BYTES")
		}
		return singleArgCast(fc, paramIdx, sqlType)
	}

	// Generic function name mapping.
	sqlName, ok := cqlFunctionToSQL[lower]
	if !ok {
		return "", nil, errors.Newf("unsupported CQL function %q", fc.Name)
	}

	// Only COUNT accepts a * argument.
	for _, arg := range fc.Args {
		if _, isStar := arg.(*parser.StarExpr); isStar && lower != "count" {
			return "", nil, errors.Newf(
				"%s() does not accept * as an argument", fc.Name,
			)
		}
	}

	var sb strings.Builder
	sb.WriteString(sqlName)
	sb.WriteByte('(')

	if fc.Distinct {
		sb.WriteString("DISTINCT ")
	}

	for i, arg := range fc.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		argSQL, _, err := exprToSQL(arg, paramIdx)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(argSQL)
	}

	sb.WriteByte(')')
	return sb.String(), nil, nil
}

// singleArgCast translates a single-argument CQL function to a SQL CAST.
func singleArgCast(
	fc *parser.FunctionCall, paramIdx *int, sqlType string,
) (string, interface{}, error) {
	if len(fc.Args) != 1 {
		return "", nil, errors.Newf(
			"%s() requires exactly one argument", fc.Name,
		)
	}
	argSQL, param, err := exprToSQL(fc.Args[0], paramIdx)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("CAST(%s AS %s)", argSQL, sqlType), param, nil
}

// timeuuidStubTimestamp returns now()::TIMESTAMPTZ as a compatibility stub for
// toTimestamp/dateOf. Cassandra timeuuids embed a timestamp, but CRDB stores
// timeuuids as plain UUIDs without extractable timestamps. The argument is
// processed for bind marker tracking but its value is discarded.
func timeuuidStubTimestamp(fc *parser.FunctionCall, paramIdx *int) (string, interface{}, error) {
	if len(fc.Args) != 1 {
		return "", nil, errors.Newf(
			"%s() requires exactly one argument", fc.Name,
		)
	}
	if _, _, err := exprToSQL(fc.Args[0], paramIdx); err != nil {
		return "", nil, err
	}
	return "now()::TIMESTAMPTZ", nil, nil
}

// toDateSQL translates toDate to CAST(CAST(arg AS DATE) AS TIMESTAMPTZ).
// The inner DATE cast truncates to midnight; the outer TIMESTAMPTZ cast
// ensures the result has a CQL wire type mapping.
func toDateSQL(fc *parser.FunctionCall, paramIdx *int) (string, interface{}, error) {
	if len(fc.Args) != 1 {
		return "", nil, errors.Newf(
			"%s() requires exactly one argument", fc.Name,
		)
	}
	argSQL, param, err := exprToSQL(fc.Args[0], paramIdx)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(
		"CAST(CAST(%s AS DATE) AS TIMESTAMPTZ)", argSQL,
	), param, nil
}

// extractEpochToSQL translates toUnixTimestamp to
// CAST(extract(epoch FROM arg) AS INT8).
func extractEpochToSQL(fc *parser.FunctionCall, paramIdx *int) (string, interface{}, error) {
	if len(fc.Args) != 1 {
		return "", nil, errors.Newf(
			"%s() requires exactly one argument", fc.Name,
		)
	}
	argSQL, param, err := exprToSQL(fc.Args[0], paramIdx)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(
		"CAST(extract(epoch FROM %s) AS INT8)", argSQL,
	), param, nil
}

// timeuuidStubEpoch returns the current epoch seconds as a compatibility stub
// for unixTimestampOf. The argument is processed for bind marker tracking but
// its value is discarded.
func timeuuidStubEpoch(fc *parser.FunctionCall, paramIdx *int) (string, interface{}, error) {
	if len(fc.Args) != 1 {
		return "", nil, errors.Newf(
			"%s() requires exactly one argument", fc.Name,
		)
	}
	if _, _, err := exprToSQL(fc.Args[0], paramIdx); err != nil {
		return "", nil, err
	}
	return "CAST(extract(epoch FROM now()) AS INT8)", nil, nil
}

// singleArgCastThroughString translates a single-argument CQL function to
// CAST(CAST(arg AS STRING) AS targetType). Used for <type>AsBlob conversions
// where CRDB cannot directly cast the source type to the target.
func singleArgCastThroughString(
	fc *parser.FunctionCall, paramIdx *int, targetType string,
) (string, interface{}, error) {
	if len(fc.Args) != 1 {
		return "", nil, errors.Newf(
			"%s() requires exactly one argument", fc.Name,
		)
	}
	argSQL, param, err := exprToSQL(fc.Args[0], paramIdx)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(
		"CAST(CAST(%s AS STRING) AS %s)", argSQL, targetType,
	), param, nil
}

// tokenToSQL translates the CQL token() function to a CRDB hash function.
// CQL token() returns the partitioner hash of partition key values; this
// approximation uses fnv32a for syntax compatibility.
func tokenToSQL(fc *parser.FunctionCall, paramIdx *int) (string, interface{}, error) {
	if len(fc.Args) == 0 {
		return "", nil, errors.Newf("token() requires at least one argument")
	}
	var parts []string
	for _, arg := range fc.Args {
		argSQL, _, err := exprToSQL(arg, paramIdx)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, fmt.Sprintf("CAST(%s AS STRING)", argSQL))
	}
	var inner string
	if len(parts) == 1 {
		inner = parts[0]
	} else {
		inner = strings.Join(parts, " || ',' || ")
	}
	return fmt.Sprintf("fnv32a(CAST(%s AS BYTES))", inner), nil, nil
}

// translateInsertJSON maps CQL INSERT INTO <table> JSON '<json>' to a CRDB
// UPSERT (or INSERT if IF NOT EXISTS) by parsing the JSON value and
// extracting column names and values.
func translateInsertJSON(s *parser.InsertStatement) (Result, error) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(s.JSONValue), &jsonData); err != nil {
		return Result{}, errors.Wrap(err, "parsing INSERT JSON value")
	}
	if len(jsonData) == 0 {
		return Result{}, errors.New("INSERT JSON: empty JSON object")
	}

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(jsonData))
	for k := range jsonData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	if s.IfNotExists {
		sb.WriteString("INSERT INTO ")
	} else {
		sb.WriteString("UPSERT INTO ")
	}
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))

	sb.WriteString(" (")
	for i, key := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(key))
	}
	sb.WriteString(") VALUES (")
	for i, key := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(jsonValueToSQL(jsonData[key]))
	}
	sb.WriteByte(')')

	if s.IfNotExists {
		sb.WriteString(" ON CONFLICT DO NOTHING")
	}

	return Result{SQL: sb.String()}, nil
}

// jsonValueToSQL converts a JSON value (from encoding/json's Unmarshal) to a
// SQL literal string.
func jsonValueToSQL(v interface{}) string {
	switch val := v.(type) {
	case string:
		return quoteLiteral(val)
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	default:
		return "NULL"
	}
}

// translateSelectJSON maps CQL SELECT JSON to CRDB SQL that returns results
// as a JSON string column. For SELECT JSON *, wraps the query in a subquery
// and applies row_to_json. For specific columns, uses jsonb_build_object.
func translateSelectJSON(s *parser.SelectStatement, schema *SchemaInfo) (Result, error) {
	isSelectStar := len(s.Columns) == 1 &&
		s.Columns[0].Column == "*" &&
		s.Columns[0].Expr == nil

	if isSelectStar {
		// Build the inner SELECT without JSON.
		inner := *s
		inner.JSON = false
		innerResult, err := translateSelect(&inner, schema)
		if err != nil {
			return Result{}, err
		}
		sql := fmt.Sprintf(
			"SELECT row_to_json(sub)::STRING AS \"[json]\" FROM (%s) AS sub",
			innerResult.SQL,
		)
		return Result{SQL: sql, Params: innerResult.Params}, nil
	}

	// SELECT JSON with specific columns: use jsonb_build_object.
	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	if s.Distinct {
		sb.WriteString("SELECT DISTINCT ")
	} else {
		sb.WriteString("SELECT ")
	}
	sb.WriteString("jsonb_build_object(")
	for i, sel := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		if sel.Expr != nil {
			sqlExpr, _, err := exprToSQL(sel.Expr, &paramIdx)
			if err != nil {
				return Result{}, errors.Wrap(err, "translating JSON selector")
			}
			key := sel.Alias
			if key == "" {
				key = sqlExpr
			}
			sb.WriteString(quoteLiteral(key))
			sb.WriteString(", ")
			sb.WriteString(sqlExpr)
		} else {
			sb.WriteString(quoteLiteral(sel.Column))
			sb.WriteString(", ")
			sb.WriteString(quoteIdent(sel.Column))
		}
	}
	sb.WriteString(")::STRING AS \"[json]\"")

	if err := writeFromAndClauses(&sb, s, &params, &paramIdx); err != nil {
		return Result{}, err
	}

	return Result{SQL: sb.String(), Params: params}, nil
}

// castExprToSQL translates CAST(expr AS type) to CRDB SQL.
func castExprToSQL(c *parser.CastExpr, paramIdx *int) (string, interface{}, error) {
	sqlType, ok := cqlTypeToCRDBSQL[c.Type.Name]
	if !ok {
		return "", nil, errors.Newf("unsupported CQL type %q in CAST", c.Type.Name)
	}
	innerSQL, _, err := exprToSQL(c.Expr, paramIdx)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("CAST(%s AS %s)", innerSQL, sqlType), nil, nil
}

// CqlTypeToSQL maps a CQL DataType to its CRDB SQL type string. Known types
// map through cqlTypeToCRDBSQL; unknown type names without type parameters
// are treated as user-defined types (UDTs) and returned as quoted identifiers.
// Parameterized unknown types (like tuple<int, int>) are unsupported.
func CqlTypeToSQL(dt parser.DataType) string {
	if sqlType, ok := cqlTypeToCRDBSQL[dt.Name]; ok {
		return sqlType
	}
	if len(dt.Params) > 0 {
		// Parameterized types must be in the type map; if not, they're
		// unsupported (e.g. tuple<int, int> without frozen<>).
		return ""
	}
	// Bare identifier: treat as a user-defined composite type reference.
	return quoteIdent(dt.Name)
}

// translateCreateType maps CQL CREATE TYPE to CRDB CREATE TYPE AS (...).
// CQL CREATE TYPE defines a composite (record) type with named fields.
func translateCreateType(s *parser.CreateTypeStatement) (Result, error) {
	var sb strings.Builder
	sb.WriteString("CREATE TYPE ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	if s.Keyspace != "" {
		sb.WriteString(quoteIdent(s.Keyspace))
		sb.WriteByte('.')
	}
	sb.WriteString(quoteIdent(s.TypeName))
	sb.WriteString(" AS (")
	for i, field := range s.Fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(field.Name))
		sb.WriteByte(' ')
		sb.WriteString(CqlTypeToSQL(field.DataType))
	}
	sb.WriteByte(')')
	return Result{SQL: sb.String()}, nil
}

// translateAlterType maps CQL ALTER TYPE operations. CockroachDB does not
// support ALTER TYPE ADD ATTRIBUTE or RENAME ATTRIBUTE for composite types,
// so these operations return an error.
func translateAlterType(s *parser.AlterTypeStatement) (Result, error) {
	switch s.Op.(type) {
	case *parser.AlterTypeAddField:
		return Result{}, errors.Newf(
			"ALTER TYPE ADD field is not supported for composite types")
	case *parser.AlterTypeRenameField:
		return Result{}, errors.Newf(
			"ALTER TYPE RENAME field is not supported for composite types")
	case *parser.AlterTypeAlterField:
		return Result{}, errors.Newf(
			"ALTER TYPE ALTER field is not supported for composite types")
	default:
		return Result{}, errors.Newf("unsupported ALTER TYPE operation: %T", s.Op)
	}
}

// buildInsertStaticPropagation generates a propagation UPDATE for an INSERT
// into a table with static columns. For static columns included in the INSERT,
// the explicit value is propagated to all partition rows. For static columns
// not in the INSERT, COALESCE inherits the value from existing partition rows.
func buildInsertStaticPropagation(
	s *parser.InsertStatement, schema *SchemaInfo,
) (string, []interface{}) {
	meta, ok := schema.LookupTable(s.Keyspace, s.Table)
	if !ok || len(meta.StaticColumns) == 0 {
		return "", nil
	}

	// Map INSERT column names (lowercase) to their index in Values.
	colIdx := make(map[string]int, len(s.Columns))
	for i, col := range s.Columns {
		colIdx[strings.ToLower(col)] = i
	}

	// Find partition key value indices.
	type colVal struct {
		name   string
		valIdx int
	}
	var pkCols []colVal
	for _, pk := range meta.PartitionKeys {
		idx, ok := colIdx[strings.ToLower(pk)]
		if !ok {
			return "", nil // can't propagate without PK values
		}
		pkCols = append(pkCols, colVal{pk, idx})
	}

	// Collect static columns in sorted order for deterministic SQL.
	sortedStatic := make([]string, 0, len(meta.StaticColumns))
	for col := range meta.StaticColumns {
		sortedStatic = append(sortedStatic, col)
	}
	sort.Strings(sortedStatic)

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("UPDATE ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	sb.WriteString(" SET ")

	for i, col := range sortedStatic {
		if i > 0 {
			sb.WriteString(", ")
		}
		qCol := quoteIdent(col)
		sb.WriteString(qCol)
		sb.WriteString(" = ")

		if idx, ok := colIdx[col]; ok {
			// Static column is in the INSERT: propagate its explicit value.
			sqlVal, param, err := exprToSQL(s.Values[idx], &paramIdx)
			if err != nil {
				return "", nil
			}
			sb.WriteString(sqlVal)
			if param != nil {
				params = append(params, param)
			}
		} else {
			// Static column not in the INSERT: inherit from existing rows.
			sb.WriteString("COALESCE(")
			sb.WriteString(qCol)
			sb.WriteString(", (SELECT ")
			sb.WriteString(qCol)
			sb.WriteString(" FROM ")
			sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
			sb.WriteString(" AS \"__cql_src\" WHERE ")
			for j, pk := range pkCols {
				if j > 0 {
					sb.WriteString(" AND ")
				}
				sb.WriteString("\"__cql_src\".")
				sb.WriteString(quoteIdent(pk.name))
				sb.WriteString(" = ")
				sqlVal, param, err := exprToSQL(s.Values[pk.valIdx], &paramIdx)
				if err != nil {
					return "", nil
				}
				sb.WriteString(sqlVal)
				if param != nil {
					params = append(params, param)
				}
			}
			sb.WriteString(" AND \"__cql_src\".")
			sb.WriteString(qCol)
			sb.WriteString(" IS NOT NULL LIMIT 1))")
		}
	}

	sb.WriteString(" WHERE ")
	for i, pk := range pkCols {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString(quoteIdent(pk.name))
		sb.WriteString(" = ")
		sqlVal, param, err := exprToSQL(s.Values[pk.valIdx], &paramIdx)
		if err != nil {
			return "", nil
		}
		sb.WriteString(sqlVal)
		if param != nil {
			params = append(params, param)
		}
	}

	return sb.String(), params
}

// buildUpdateStaticPropagation generates a propagation UPDATE for an UPDATE
// that modifies static columns. The propagation UPDATE applies the new static
// values to all rows matching the partition key (not just the rows targeted
// by the original clustering key conditions).
func buildUpdateStaticPropagation(
	s *parser.UpdateStatement, schema *SchemaInfo,
) (string, []interface{}) {
	meta, ok := schema.LookupTable(s.Keyspace, s.Table)
	if !ok || len(meta.StaticColumns) == 0 {
		return "", nil
	}

	// Collect assignments that target static columns.
	var staticAssigns []parser.Assignment
	for _, a := range s.Assignments {
		if meta.StaticColumns[strings.ToLower(a.Column)] {
			staticAssigns = append(staticAssigns, a)
		}
	}
	if len(staticAssigns) == 0 {
		return "", nil
	}

	// Extract partition key conditions from the WHERE clause.
	pkSet := make(map[string]bool, len(meta.PartitionKeys))
	for _, pk := range meta.PartitionKeys {
		pkSet[strings.ToLower(pk)] = true
	}
	var pkWheres []parser.WhereClause
	for _, w := range s.Where {
		if pkSet[strings.ToLower(w.Column)] {
			pkWheres = append(pkWheres, w)
		}
	}
	if len(pkWheres) == 0 {
		return "", nil
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("UPDATE ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	sb.WriteString(" SET ")

	for i, a := range staticAssigns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(a.Column))
		sb.WriteString(" = ")
		sqlVal, param, err := exprToSQL(a.Value, &paramIdx)
		if err != nil {
			return "", nil
		}
		sb.WriteString(sqlVal)
		if param != nil {
			params = append(params, param)
		}
	}

	sb.WriteString(" WHERE ")
	if err := writeWhereClauses(&sb, pkWheres, &params, &paramIdx); err != nil {
		return "", nil
	}

	return sb.String(), params
}

// quoteIdent quotes a SQL identifier with double quotes. CQL identifiers are
// case-insensitive and stored lowercase; we lowercase and quote to preserve
// exact names.
func quoteIdent(name string) string {
	lower := strings.ToLower(name)
	return `"` + strings.ReplaceAll(lower, `"`, `""`) + `"`
}

// quoteLiteral quotes a SQL string literal with single quotes, escaping any
// embedded single quotes by doubling them.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// qualifiedTable returns the SQL table reference, optionally schema-qualified.
func qualifiedTable(keyspace, table string) string {
	if keyspace != "" {
		return quoteIdent(keyspace) + "." + quoteIdent(table)
	}
	return quoteIdent(table)
}

// BuildLWTExistingRowQuery generates a SELECT query that returns
// false AS "[applied]" plus all columns of the existing row, filtered by
// the primary key values from the INSERT statement. Used by the executor
// when INSERT IF NOT EXISTS finds a duplicate row.
func BuildLWTExistingRowQuery(
	stmt *parser.InsertStatement, meta TableMeta, keyspace string,
) (string, []interface{}) {
	if stmt.JSON {
		return buildLWTExistingRowQueryJSON(stmt, meta, keyspace)
	}

	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString(`SELECT false AS "[applied]"`)
	for _, col := range meta.Columns {
		sb.WriteString(", ")
		sb.WriteString(quoteIdent(col))
	}

	sb.WriteString(" FROM ")
	sb.WriteString(qualifiedTable(keyspace, stmt.Table))
	sb.WriteString(" WHERE ")

	pkCols := make([]string, 0, len(meta.PartitionKeys)+len(meta.ClusteringKeys))
	pkCols = append(pkCols, meta.PartitionKeys...)
	pkCols = append(pkCols, meta.ClusteringKeys...)

	written := 0
	for _, pk := range pkCols {
		for j, col := range stmt.Columns {
			if !strings.EqualFold(col, pk) {
				continue
			}
			if written > 0 {
				sb.WriteString(" AND ")
			}
			written++
			sb.WriteString(quoteIdent(pk))
			sb.WriteString(" = ")
			sqlVal, param, err := exprToSQL(stmt.Values[j], &paramIdx)
			if err != nil {
				return `SELECT false AS "[applied]"`, nil
			}
			sb.WriteString(sqlVal)
			if param != nil {
				params = append(params, param)
			}
			break
		}
	}

	return sb.String(), params
}

// BuildLWTExistingRowQueryFromWhere generates a SELECT query that
// returns false AS "[applied]" plus all columns of the existing row,
// filtered by the WHERE clause of a conditional DELETE or UPDATE. Used
// by the executor when a conditional DML (IF <conds>) does not apply
// and the client needs to see the current row state.
func BuildLWTExistingRowQueryFromWhere(
	table string, keyspace string, where []parser.WhereClause, meta TableMeta,
) (string, []interface{}) {
	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString(`SELECT false AS "[applied]"`)
	for _, col := range meta.Columns {
		sb.WriteString(", ")
		sb.WriteString(quoteIdent(col))
	}

	sb.WriteString(" FROM ")
	sb.WriteString(qualifiedTable(keyspace, table))
	sb.WriteString(" WHERE ")

	if err := writeWhereClauses(&sb, where, &params, &paramIdx); err != nil {
		return `SELECT false AS "[applied]"`, nil
	}

	return sb.String(), params
}

// buildLWTExistingRowQueryJSON generates the existing-row SELECT for
// INSERT JSON IF NOT EXISTS by extracting PK values from parsed JSON.
func buildLWTExistingRowQueryJSON(
	stmt *parser.InsertStatement, meta TableMeta, keyspace string,
) (string, []interface{}) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(stmt.JSONValue), &jsonData); err != nil {
		return `SELECT false AS "[applied]"`, nil
	}

	var sb strings.Builder
	sb.WriteString(`SELECT false AS "[applied]"`)
	for _, col := range meta.Columns {
		sb.WriteString(", ")
		sb.WriteString(quoteIdent(col))
	}

	sb.WriteString(" FROM ")
	sb.WriteString(qualifiedTable(keyspace, stmt.Table))
	sb.WriteString(" WHERE ")

	pkCols := make([]string, 0, len(meta.PartitionKeys)+len(meta.ClusteringKeys))
	pkCols = append(pkCols, meta.PartitionKeys...)
	pkCols = append(pkCols, meta.ClusteringKeys...)

	written := 0
	for _, pk := range pkCols {
		val, ok := jsonData[pk]
		if !ok {
			continue
		}
		if written > 0 {
			sb.WriteString(" AND ")
		}
		written++
		sb.WriteString(quoteIdent(pk))
		sb.WriteString(" = ")
		sb.WriteString(jsonValueToSQL(val))
	}

	return sb.String(), nil
}

// translateAlterKeyspace silently accepts ALTER KEYSPACE for compatibility.
// CQL keyspace properties (replication strategy, durable_writes) have no CRDB
// equivalent — CRDB uses zone configurations instead.
func translateAlterKeyspace(_ *parser.AlterKeyspaceStatement) (Result, error) {
	return Result{}, nil
}

// translateAlterTable maps CQL ALTER TABLE operations to CRDB SQL. When
// schema is available, ALTER TYPE on primary key columns is rejected early
// with a CQL-appropriate error (Cassandra also forbids PK type changes).
func translateAlterTable(s *parser.AlterTableStatement, schema *SchemaInfo) (Result, error) {
	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))

	switch op := s.Op.(type) {
	case *parser.AlterTableAdd:
		sqlType, ok := cqlTypeToCRDBSQL[op.DataType.Name]
		if !ok {
			return Result{}, errors.Newf("unsupported CQL type %q", op.DataType.Name)
		}
		sb.WriteString(" ADD COLUMN ")
		sb.WriteString(quoteIdent(op.Column))
		sb.WriteByte(' ')
		sb.WriteString(sqlType)
	case *parser.AlterTableDrop:
		sb.WriteString(" DROP COLUMN ")
		sb.WriteString(quoteIdent(op.Column))
	case *parser.AlterTableRename:
		sb.WriteString(" RENAME COLUMN ")
		sb.WriteString(quoteIdent(op.OldName))
		sb.WriteString(" TO ")
		sb.WriteString(quoteIdent(op.NewName))
	case *parser.AlterTableAlterType:
		// Reject type changes on primary key columns. Cassandra also
		// forbids this; CockroachDB would fail later with a less
		// helpful error about on-disk data rewrites.
		if schema != nil {
			if meta, ok := schema.LookupTable(s.Keyspace, s.Table); ok {
				for _, pk := range meta.PartitionKeys {
					if strings.EqualFold(pk, op.Column) {
						return Result{}, errors.Newf(
							"cannot alter type of primary key column %q", op.Column)
					}
				}
				for _, ck := range meta.ClusteringKeys {
					if strings.EqualFold(ck, op.Column) {
						return Result{}, errors.Newf(
							"cannot alter type of clustering key column %q", op.Column)
					}
				}
			}
		}
		sqlType, ok := cqlTypeToCRDBSQL[op.DataType.Name]
		if !ok {
			return Result{}, errors.Newf("unsupported CQL type %q", op.DataType.Name)
		}
		sb.WriteString(" ALTER COLUMN ")
		sb.WriteString(quoteIdent(op.Column))
		sb.WriteString(" SET DATA TYPE ")
		sb.WriteString(sqlType)
	case *parser.AlterTableWith:
		// CQL table properties (compaction, gc_grace_seconds, etc.) have no
		// CRDB equivalent. Silently accept for compatibility.
		return Result{}, nil
	default:
		return Result{}, errors.Newf("unsupported ALTER TABLE operation: %T", op)
	}
	return Result{SQL: sb.String()}, nil
}

// translateDrop maps CQL DROP TABLE/KEYSPACE/INDEX to CRDB SQL.
func translateDrop(s *parser.DropStatement) (Result, error) {
	var sb strings.Builder
	switch s.ObjectType {
	case "TABLE":
		sb.WriteString("DROP TABLE ")
		if s.IfExists {
			sb.WriteString("IF EXISTS ")
		}
		sb.WriteString(qualifiedTable(s.Keyspace, s.Name))
	case "KEYSPACE":
		sb.WriteString("DROP DATABASE ")
		if s.IfExists {
			sb.WriteString("IF EXISTS ")
		}
		sb.WriteString(quoteIdent(s.Name))
	case "INDEX":
		sb.WriteString("DROP INDEX ")
		if s.IfExists {
			sb.WriteString("IF EXISTS ")
		}
		sb.WriteString(qualifiedTable(s.Keyspace, s.Name))
	default:
		return Result{}, errors.Newf("unsupported DROP target: %s", s.ObjectType)
	}
	return Result{SQL: sb.String()}, nil
}

// translateTruncate maps CQL TRUNCATE TABLE to CRDB TRUNCATE TABLE.
func translateTruncate(s *parser.TruncateStatement) (Result, error) {
	var sb strings.Builder
	sb.WriteString("TRUNCATE TABLE ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
	return Result{SQL: sb.String()}, nil
}

// translateBatch maps CQL BATCH to a sequence of SQL statements
// wrapped in BEGIN/COMMIT. Each inner statement is translated
// individually and semicolon-separated.
func translateBatch(s *parser.BatchStatement) (Result, error) {
	if len(s.Statements) == 0 {
		return Result{}, errors.Newf("empty BATCH statement")
	}
	var sb strings.Builder
	var allParams []interface{}
	sb.WriteString("BEGIN; ")
	for i, innerStmt := range s.Statements {
		if i > 0 {
			sb.WriteString("; ")
		}
		r, err := Translate(innerStmt)
		if err != nil {
			return Result{}, errors.Wrap(err, "translating BATCH inner statement")
		}
		sb.WriteString(r.SQL)
		allParams = append(allParams, r.Params...)
	}
	sb.WriteString("; COMMIT")
	return Result{SQL: sb.String(), Params: allParams}, nil
}

// isCollectionExpr returns true if the expression is a collection
// literal (list, set, or map).
func isCollectionExpr(e parser.Expr) bool {
	switch e.(type) {
	case *parser.ListLiteral, *parser.SetLiteral, *parser.MapExprLiteral:
		return true
	default:
		return false
	}
}

// collectionBinaryToSQL translates a counter-style binary expression
// into a JSONB collection operation. For +, uses JSONB concatenation
// (||). For -, handles map key removal by extracting string values
// from a SetLiteral and chaining text removals.
func collectionBinaryToSQL(
	column, op string, value parser.Expr, paramIdx *int,
) (string, interface{}, error) {
	col := quoteIdent(column)
	if op == "+" {
		valSQL, param, err := exprToSQL(value, paramIdx)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s || %s", col, valSQL), param, nil
	}
	// op == "-": removal. For SetLiteral and ListLiteral values, chain
	// individual element removals. For sets this removes map keys; for
	// lists this removes matching values from the JSON array. Other types
	// fall through to generic JSONB subtraction.
	var elems []parser.Expr
	switch v := value.(type) {
	case *parser.SetLiteral:
		elems = v.Values
	case *parser.ListLiteral:
		elems = v.Values
	}
	if len(elems) > 0 {
		result := col
		for _, elem := range elems {
			elemSQL, _, err := exprToSQL(elem, paramIdx)
			if err != nil {
				return "", nil, err
			}
			result = fmt.Sprintf("(%s - %s)", result, elemSQL)
		}
		return result, nil, nil
	}
	valSQL, param, err := exprToSQL(value, paramIdx)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s - %s", col, valSQL), param, nil
}

// listLiteralToSQL translates a CQL list literal [v1, v2, ...] to
// CRDB's jsonb_build_array(v1, v2, ...).
func listLiteralToSQL(lit *parser.ListLiteral, paramIdx *int) (string, interface{}, error) {
	if len(lit.Values) == 0 {
		return "'[]'::JSONB", nil, nil
	}
	var sb strings.Builder
	sb.WriteString("jsonb_build_array(")
	for i, val := range lit.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sqlVal, _, err := exprToSQL(val, paramIdx)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(sqlVal)
	}
	sb.WriteByte(')')
	return sb.String(), nil, nil
}

// setLiteralToSQL translates a CQL set literal {v1, v2, ...} to
// CRDB's jsonb_build_array(v1, v2, ...). CQL sets are stored as
// JSON arrays (sorted uniqueness is an application-level concern).
func setLiteralToSQL(lit *parser.SetLiteral, paramIdx *int) (string, interface{}, error) {
	if len(lit.Values) == 0 {
		return "'[]'::JSONB", nil, nil
	}
	var sb strings.Builder
	sb.WriteString("jsonb_build_array(")
	for i, val := range lit.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sqlVal, _, err := exprToSQL(val, paramIdx)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(sqlVal)
	}
	sb.WriteByte(')')
	return sb.String(), nil, nil
}

// tupleLiteralToSQL translates a CQL tuple literal (v1, v2, ...) to
// CRDB's jsonb_build_array(v1, v2, ...). CQL tuples are stored as
// JSON arrays in JSONB columns.
func tupleLiteralToSQL(lit *parser.TupleLiteral, paramIdx *int) (string, interface{}, error) {
	if len(lit.Values) == 0 {
		return "'[]'::JSONB", nil, nil
	}
	var sb strings.Builder
	sb.WriteString("jsonb_build_array(")
	for i, val := range lit.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sqlVal, _, err := exprToSQL(val, paramIdx)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(sqlVal)
	}
	sb.WriteByte(')')
	return sb.String(), nil, nil
}

// mapLiteralToSQL translates a CQL map literal {k1: v1, k2: v2, ...}
// to CRDB's jsonb_build_object(k1, v1, k2, v2, ...).
func mapLiteralToSQL(lit *parser.MapExprLiteral, paramIdx *int) (string, interface{}, error) {
	if len(lit.Entries) == 0 {
		return "'{}'::JSONB", nil, nil
	}
	var sb strings.Builder
	sb.WriteString("jsonb_build_object(")
	for i, entry := range lit.Entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		keySQL, _, err := exprToSQL(entry.Key, paramIdx)
		if err != nil {
			return "", nil, err
		}
		valSQL, _, err := exprToSQL(entry.Value, paramIdx)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(keySQL)
		sb.WriteString(", ")
		sb.WriteString(valSQL)
	}
	sb.WriteByte(')')
	return sb.String(), nil, nil
}
