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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/cockroachdb/errors"
)

// cqlFunctionToSQL maps lowercase CQL function names to their CockroachDB SQL
// equivalents. Functions not in this map are unsupported.
var cqlFunctionToSQL = map[string]string{
	"now":   "now",
	"uuid":  "gen_random_uuid",
	"count": "count",
	"sum":   "sum",
	"avg":   "avg",
	"min":   "min",
	"max":   "max",
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
	// Collection types are stored as JSONB. Lists and sets become JSON
	// arrays; maps become JSON objects.
	"list":   "JSONB",
	"set":    "JSONB",
	"map":    "JSONB",
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
	case *parser.UpdateStatement:
		return translateUpdate(s)
	case *parser.DeleteStatement:
		return translateDelete(s)
	case *parser.CreateIndexStatement:
		return translateCreateIndex(s)
	case *parser.AlterTableStatement:
		return translateAlterTable(s)
	case *parser.AlterKeyspaceStatement:
		return translateAlterKeyspace(s)
	case *parser.DropStatement:
		return translateDrop(s)
	case *parser.TruncateStatement:
		return translateTruncate(s)
	case *parser.BatchStatement:
		return translateBatch(s)
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
// conditional insert.
func translateInsert(s *parser.InsertStatement) (Result, error) {
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

	return Result{SQL: sb.String(), Params: params}, nil
}

// translateSelect maps CQL SELECT to CRDB SQL SELECT.
func translateSelect(s *parser.SelectStatement) (Result, error) {
	if s.JSON {
		return translateSelectJSON(s)
	}
	if s.PerPartitionLimit != nil {
		return Result{}, errors.Newf("PER PARTITION LIMIT is not supported")
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
	for i, sel := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		if sel.Expr != nil {
			sqlExpr, _, err := exprToSQL(sel.Expr, &paramIdx)
			if err != nil {
				return Result{}, errors.Wrap(err, "translating selector")
			}
			sb.WriteString(sqlExpr)
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

	if err := writeFromAndClauses(&sb, s, &params, &paramIdx); err != nil {
		return Result{}, err
	}

	return Result{SQL: sb.String(), Params: params}, nil
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
			if err := writeCol(); err != nil {
				return err
			}
			sb.WriteString(" IN (")
			for j, val := range tuple.Values {
				if j > 0 {
					sb.WriteString(", ")
				}
				sqlVal, param, err := exprToSQL(val, paramIdx)
				if err != nil {
					return errors.Wrap(err, "translating IN value")
				}
				sb.WriteString(sqlVal)
				if param != nil {
					*params = append(*params, param)
				}
			}
			sb.WriteByte(')')
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

// translateUpdate maps CQL UPDATE to CRDB SQL UPDATE.
//
// CQL UPDATE with IF EXISTS or IF conditions uses conditional logic. IF EXISTS
// is a no-op guard (SQL UPDATE WHERE is already a no-op for missing rows).
// IF conditions are appended as additional WHERE predicates so the UPDATE
// only executes when the conditions are satisfied.
func translateUpdate(s *parser.UpdateStatement) (Result, error) {
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
			// Map subscript assignment: col['key'] = val →
			// "col" = jsonb_set("col", ARRAY[key], to_jsonb(val))
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
				"%s = jsonb_set(%s, ARRAY[%s], to_jsonb(%s))",
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

	return Result{SQL: sb.String(), Params: params}, nil
}

// translateDelete maps CQL DELETE to CRDB SQL DELETE.
//
// CQL DELETE with IF EXISTS or IF conditions is conditional. IF EXISTS is a
// no-op guard (the SQL DELETE WHERE already handles missing rows). IF conditions
// are appended as additional WHERE predicates so the DELETE only executes when
// the conditions are satisfied.
func translateDelete(s *parser.DeleteStatement) (Result, error) {
	var sb strings.Builder
	var params []interface{}
	paramIdx := 1

	sb.WriteString("DELETE FROM ")
	sb.WriteString(qualifiedTable(s.Keyspace, s.Table))
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
	case *parser.ColumnRef:
		return quoteIdent(v.Name), nil, nil
	case *parser.StarExpr:
		return "*", nil, nil
	case *parser.FunctionCall:
		return functionCallToSQL(v, paramIdx)
	case *parser.CastExpr:
		return castExprToSQL(v, paramIdx)
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
		// Cassandra per-cell write timestamp metadata. CRDB does not track
		// per-cell timestamps; return 0 for compatibility.
		return "0::INT8", nil, nil
	case "ttl":
		// Cassandra per-cell TTL metadata. CRDB does not support per-cell
		// TTL; return NULL (same as Cassandra for rows without TTL).
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
func translateSelectJSON(s *parser.SelectStatement) (Result, error) {
	isSelectStar := len(s.Columns) == 1 &&
		s.Columns[0].Column == "*" &&
		s.Columns[0].Expr == nil

	if isSelectStar {
		// Build the inner SELECT without JSON.
		inner := *s
		inner.JSON = false
		innerResult, err := translateSelect(&inner)
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

// translateAlterKeyspace silently accepts ALTER KEYSPACE for compatibility.
// CQL keyspace properties (replication strategy, durable_writes) have no CRDB
// equivalent — CRDB uses zone configurations instead.
func translateAlterKeyspace(_ *parser.AlterKeyspaceStatement) (Result, error) {
	return Result{}, nil
}

// translateAlterTable maps CQL ALTER TABLE operations to CRDB SQL.
func translateAlterTable(s *parser.AlterTableStatement) (Result, error) {
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
	// op == "-": removal. For SetLiteral values, chain individual text
	// removals (works for map key removal). For other types, use the
	// generic JSONB subtraction.
	if set, ok := value.(*parser.SetLiteral); ok && len(set.Values) > 0 {
		result := col
		for _, elem := range set.Values {
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
