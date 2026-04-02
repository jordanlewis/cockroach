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
// PRIMARY KEY in SQL. CQL clustering key ordering is not yet supported but the
// column order in the PK definition reflects the intended sort.
package translate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/cockroachdb/errors"
)

// cqlTypeToCRDBSQL maps CQL type names (as produced by the parser's DataType.Name)
// to CockroachDB SQL type names.
var cqlTypeToCRDBSQL = map[string]string{
	"text":      "STRING",
	"varchar":   "STRING",
	"ascii":     "STRING",
	"int":       "INT4",
	"bigint":    "INT8",
	"float":     "FLOAT4",
	"double":    "FLOAT8",
	"boolean":   "BOOL",
	"timestamp": "TIMESTAMPTZ",
	"uuid":      "UUID",
	"timeuuid":  "UUID",
	"blob":      "BYTES",
	"inet":      "INET",
	"counter":   "INT8",
	"varint":    "INT8",
	"decimal":   "DECIMAL",
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
}

// Translate converts a CQL AST statement into a CockroachDB SQL Result.
func Translate(stmt parser.Statement) (Result, error) {
	switch s := stmt.(type) {
	case *parser.UseStatement:
		return translateUse(s)
	case *parser.CreateKeyspaceStatement:
		return translateCreateKeyspace(s)
	case *parser.CreateTableStatement:
		return translateCreateTable(s)
	case *parser.InsertStatement:
		return translateInsert(s)
	case *parser.SelectStatement:
		return translateSelect(s)
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
		sqlType, ok := cqlTypeToCRDBSQL[col.DataType.Name]
		if !ok {
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

// translateInsert maps CQL INSERT INTO to CRDB SQL. CQL INSERT is an upsert
// (last-write-wins) unless IF NOT EXISTS is specified, in which case it is a
// conditional insert.
func translateInsert(s *parser.InsertStatement) (Result, error) {
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

	return Result{SQL: sb.String(), Params: params}, nil
}

// cqlFuncToSQL maps CQL function names (lowercase) to their SQL
// equivalents. Functions not in this map are passed through unchanged
// (most aggregate functions like COUNT, SUM, AVG, MIN, MAX have
// identical names in SQL).
var cqlFuncToSQL = map[string]string{
	"uuid": "gen_random_uuid",
}

// translateSelect maps CQL SELECT to CRDB SQL SELECT.
func translateSelect(s *parser.SelectStatement) (Result, error) {
	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("SELECT ")

	// Column list.
	for i, sel := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		selectorToSQL(&sb, sel)
	}

	sb.WriteString(" FROM ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))

	// WHERE clauses.
	if len(s.Where) > 0 {
		sb.WriteString(" WHERE ")
		for i, w := range s.Where {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(quoteIdent(w.Column))
			sb.WriteByte(' ')
			sb.WriteString(w.Operator)
			sb.WriteByte(' ')
			sqlVal, param, err := exprToSQL(w.Value, &paramIdx)
			if err != nil {
				return Result{}, errors.Wrap(err, "translating WHERE value")
			}
			sb.WriteString(sqlVal)
			if param != nil {
				params = append(params, param)
			}
		}
	}

	// GROUP BY.
	if len(s.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, col := range s.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(col))
		}
	}

	// LIMIT.
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

	return Result{SQL: sb.String(), Params: params}, nil
}

// selectorToSQL writes a CQL Selector as SQL into the builder.
func selectorToSQL(sb *strings.Builder, sel parser.Selector) {
	if sel.FuncName != "" {
		sqlName := strings.ToLower(sel.FuncName)
		if mapped, ok := cqlFuncToSQL[sqlName]; ok {
			sqlName = mapped
		}
		sb.WriteString(sqlName)
		sb.WriteByte('(')
		for i, arg := range sel.FuncArgs {
			if i > 0 {
				sb.WriteString(", ")
			}
			selectorToSQL(sb, arg)
		}
		sb.WriteByte(')')
		return
	}
	if sel.Column == "*" {
		sb.WriteByte('*')
	} else {
		sb.WriteString(quoteIdent(sel.Column))
	}
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

// exprToSQL converts a CQL expression to its SQL representation. Literal values
// are inlined. Bind markers (? and :name) become $N positional placeholders.
func exprToSQL(e parser.Expr, paramIdx *int) (string, interface{}, error) {
	switch v := e.(type) {
	case *parser.StringLiteral:
		return quoteLiteral(v.Value), nil, nil
	case *parser.IntegerLiteral:
		return fmt.Sprintf("%d", v.Value), nil, nil
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
	default:
		return "", nil, errors.Newf("unsupported expression type: %T", e)
	}
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
