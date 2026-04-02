// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package translate converts T-SQL AST nodes (as produced by the parser
// package) into CockroachDB-compatible SQL strings. It handles Sybase/SQL
// Server-specific syntax and function differences:
//
//   - USE <db> → SET database = '<db>'
//   - CREATE TABLE: maps Sybase types to CRDB types (MONEY → DECIMAL(19,4),
//     DATETIME → TIMESTAMP, NVARCHAR → VARCHAR, etc.) and applies Sybase's
//     default-nullable semantics (columns are nullable unless explicitly
//     declared NOT NULL).
//   - SELECT TOP N → LIMIT N
//   - Bracket-quoted identifiers [name] → double-quoted identifiers "name"
//   - ISNULL(a,b) → COALESCE(a,b)
//   - CONVERT(type, expr) → CAST(expr AS type)
//   - GETDATE() → now()
//   - String concatenation with + → ||
//   - @@ROWCOUNT → crdb_internal.num_rows_affected (placeholder)
//   - @@IDENTITY → lastval()
//   - @@VERSION → version()
package translate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/tds/parser"
)

// Batch translates a parsed T-SQL batch into a slice of CockroachDB-compatible
// SQL strings, one per statement.
func Batch(batch *parser.Batch) ([]string, error) {
	result := make([]string, 0, len(batch.Stmts))
	for _, stmt := range batch.Stmts {
		sql, err := Statement(stmt)
		if err != nil {
			return nil, err
		}
		result = append(result, sql)
	}
	return result, nil
}

// Statement translates a single T-SQL statement into a CockroachDB-compatible
// SQL string.
func Statement(stmt parser.Statement) (string, error) {
	switch s := stmt.(type) {
	case *parser.UseStmt:
		return translateUse(s), nil
	case *parser.CreateDatabaseStmt:
		return translateCreateDatabase(s), nil
	case *parser.DropTableStmt:
		return fmt.Sprintf("DROP TABLE %s", quoteIdent(s.Table)), nil
	case *parser.DropDatabaseStmt:
		return fmt.Sprintf("DROP DATABASE %s", quoteIdent(s.Database)), nil
	case *parser.CreateTableStmt:
		return translateCreateTable(s), nil
	case *parser.InsertStmt:
		return translateInsert(s), nil
	case *parser.SelectStmt:
		return translateSelect(s), nil
	case *parser.DeleteStmt:
		return translateDelete(s), nil
	case *parser.UpdateStmt:
		return translateUpdate(s), nil
	default:
		return "", fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// translateCreateDatabase converts CREATE DATABASE to CRDB syntax.
// CockroachDB supports CREATE DATABASE natively.
func translateCreateDatabase(s *parser.CreateDatabaseStmt) string {
	return fmt.Sprintf("CREATE DATABASE %s", quoteIdent(s.Database))
}

// translateUse converts USE <database> into SET database = '<database>'.
// CockroachDB supports the USE syntax directly as well, but SET database is
// the canonical form.
func translateUse(s *parser.UseStmt) string {
	return fmt.Sprintf("SET database = '%s'", s.Database)
}

// translateCreateTable converts a T-SQL CREATE TABLE to CRDB syntax. Sybase
// type names are mapped to their CockroachDB equivalents, and the
// Sybase-default nullable semantics are applied: columns without an explicit
// NULL/NOT NULL constraint are assumed nullable (matching Sybase/SQL Server
// behavior, which differs from CRDB where columns are also nullable by default
// but the explicit annotation helps clarity).
func translateCreateTable(s *parser.CreateTableStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (", quoteIdent(s.Table))
	for i, col := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s %s", quoteIdent(col.Name), mapDataType(col.DataType))
		if col.Nullable != nil {
			if *col.Nullable {
				b.WriteString(" NULL")
			} else {
				b.WriteString(" NOT NULL")
			}
		} else {
			// Sybase/SQL Server default: columns are nullable unless NOT NULL
			// is specified. Emit explicit NULL to match Sybase semantics.
			b.WriteString(" NULL")
		}
	}
	b.WriteString(")")
	return b.String()
}

// translateInsert converts a T-SQL INSERT INTO statement to CRDB syntax.
func translateInsert(s *parser.InsertStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s", quoteIdent(s.Table))
	if len(s.Columns) > 0 {
		b.WriteString(" (")
		for i, col := range s.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdent(col))
		}
		b.WriteString(")")
	}
	b.WriteString(" VALUES ")
	for i, row := range s.Values {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j, val := range row {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(val))
		}
		b.WriteString(")")
	}
	return b.String()
}

// translateSelect converts a T-SQL SELECT (with TOP, WHERE, ORDER BY) to CRDB
// syntax. TOP N is moved to a trailing LIMIT N clause.
func translateSelect(s *parser.SelectStmt) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}
	for i, col := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(translateExpr(col.Expr))
		if col.Alias != "" {
			fmt.Fprintf(&b, " AS %s", quoteIdent(col.Alias))
		}
	}
	if len(s.From) > 0 {
		b.WriteString(" FROM ")
		for i, ref := range s.From {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdent(ref.Name))
			if ref.Alias != "" {
				fmt.Fprintf(&b, " %s", quoteIdent(ref.Alias))
			}
		}
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
	}
	if len(s.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, gb := range s.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(gb))
		}
	}
	if s.Having != nil {
		fmt.Fprintf(&b, " HAVING %s", translateExpr(s.Having))
	}
	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, ob := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(ob.Expr))
			if ob.Desc {
				b.WriteString(" DESC")
			} else {
				b.WriteString(" ASC")
			}
		}
	}
	// TOP N → LIMIT N (placed after ORDER BY, per SQL standard).
	if s.Top != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Top)
	}
	return b.String()
}

// translateDelete converts a T-SQL DELETE to CRDB syntax.
func translateDelete(s *parser.DeleteStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "DELETE FROM %s", quoteIdent(s.Table))
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
	}
	return b.String()
}

// translateUpdate converts a T-SQL UPDATE to CRDB syntax.
func translateUpdate(s *parser.UpdateStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", quoteIdent(s.Table))
	for i, a := range s.Assignments {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s = %s", quoteIdent(a.Column), translateExpr(a.Value))
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
	}
	return b.String()
}

// translateExpr recursively translates a T-SQL expression into a
// CockroachDB-compatible SQL expression string.
func translateExpr(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.StarExpr:
		return "*"

	case *parser.IntLit:
		return fmt.Sprintf("%d", e.Value)

	case *parser.FloatLit:
		return fmt.Sprintf("%g", e.Value)

	case *parser.StringLit:
		escaped := strings.ReplaceAll(e.Value, "'", "''")
		return fmt.Sprintf("'%s'", escaped)

	case *parser.NullLit:
		return "NULL"

	case *parser.IdentExpr:
		return translateIdent(e)

	case *parser.BinaryExpr:
		return translateBinaryExpr(e)

	case *parser.UnaryExpr:
		return fmt.Sprintf("%s %s", e.Op, translateExpr(e.Expr))

	case *parser.ParenExpr:
		return fmt.Sprintf("(%s)", translateExpr(e.Expr))

	case *parser.FuncCallExpr:
		return translateFuncCall(e)

	case *parser.ConvertExpr:
		return translateConvert(e)

	case *parser.InExpr:
		return translateIn(e)

	case *parser.BetweenExpr:
		return translateBetween(e)

	default:
		// Fallback: use the AST node's own String() method.
		return expr.String()
	}
}

// translateIdent converts T-SQL identifiers. Bracket-quoted identifiers are
// converted to double-quoted identifiers. @@-prefixed system variables are
// translated to their CRDB equivalents.
func translateIdent(e *parser.IdentExpr) string {
	if len(e.Parts) == 1 {
		name := e.Parts[0]
		// Handle @@system variables.
		if strings.HasPrefix(name, "@@") {
			return translateSystemVariable(name)
		}
		return quoteIdent(name)
	}
	var parts []string
	for _, p := range e.Parts {
		if p == "*" {
			parts = append(parts, "*")
		} else {
			parts = append(parts, quoteIdent(p))
		}
	}
	return strings.Join(parts, ".")
}

// translateBinaryExpr handles binary operators. The key transformation is
// string concatenation: T-SQL uses + for string concat, but CRDB uses ||.
//
// We detect string concatenation when either operand is a string literal.
// For cases where the operand types aren't known at translation time, the +
// operator is left as-is (it will work for numeric types in CRDB).
func translateBinaryExpr(e *parser.BinaryExpr) string {
	left := translateExpr(e.Left)
	right := translateExpr(e.Right)
	op := e.Op

	// Convert + to || when either side is a string literal.
	if op == "+" && isStringExpr(e.Left, e.Right) {
		op = "||"
	}

	return fmt.Sprintf("%s %s %s", left, op, right)
}

// isStringExpr returns true if any of the given expressions is a string literal,
// indicating that a + operation is string concatenation rather than arithmetic.
func isStringExpr(exprs ...parser.Expr) bool {
	for _, expr := range exprs {
		switch expr.(type) {
		case *parser.StringLit:
			return true
		}
	}
	return false
}

// translateFuncCall translates T-SQL function calls to their CRDB equivalents.
func translateFuncCall(e *parser.FuncCallExpr) string {
	name := strings.ToUpper(e.Name)

	switch name {
	case "ISNULL":
		// ISNULL(a, b) → COALESCE(a, b)
		args := translateArgs(e.Args)
		return fmt.Sprintf("COALESCE(%s)", strings.Join(args, ", "))

	case "GETDATE":
		// GETDATE() → now()
		return "now()"

	default:
		// Pass through unknown functions.
		args := translateArgs(e.Args)
		return fmt.Sprintf("%s(%s)", name, strings.Join(args, ", "))
	}
}

// translateConvert converts CONVERT(type, expr) → CAST(expr AS type).
// The style parameter (third argument) is dropped since CRDB's CAST does not
// support it.
func translateConvert(e *parser.ConvertExpr) string {
	expr := translateExpr(e.Expr)
	crdbType := mapDataType(e.DataType)
	return fmt.Sprintf("CAST(%s AS %s)", expr, crdbType)
}

// translateIn converts an IN expression. The syntax is the same in CRDB.
func translateIn(e *parser.InExpr) string {
	expr := translateExpr(e.Expr)
	vals := translateArgs(e.Values)
	op := "IN"
	if e.Not {
		op = "NOT IN"
	}
	return fmt.Sprintf("%s %s (%s)", expr, op, strings.Join(vals, ", "))
}

// translateBetween converts a BETWEEN expression. The syntax is the same in
// CRDB.
func translateBetween(e *parser.BetweenExpr) string {
	expr := translateExpr(e.Expr)
	low := translateExpr(e.Low)
	high := translateExpr(e.High)
	op := "BETWEEN"
	if e.Not {
		op = "NOT BETWEEN"
	}
	return fmt.Sprintf("%s %s %s AND %s", expr, op, low, high)
}

// translateArgs translates a slice of expressions.
func translateArgs(args []parser.Expr) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = translateExpr(arg)
	}
	return result
}

// translateSystemVariable maps T-SQL @@variables to CRDB equivalents.
func translateSystemVariable(name string) string {
	switch strings.ToUpper(name) {
	case "@@ROWCOUNT":
		return "current_setting('crdb_internal.num_rows_affected')"
	case "@@IDENTITY":
		return "lastval()"
	case "@@VERSION":
		return "version()"
	default:
		// Return as a comment for unsupported variables.
		return fmt.Sprintf("NULL /* unsupported: %s */", name)
	}
}

// quoteIdent formats an identifier for CockroachDB. Simple identifiers
// (alphanumeric + underscore) are returned unquoted. Identifiers that contain
// special characters or are multi-part dotted names are double-quoted.
func quoteIdent(name string) string {
	// Handle dotted names like "dbo.tablename".
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		quoted := make([]string, len(parts))
		for i, p := range parts {
			quoted[i] = quoteIdentPart(p)
		}
		return strings.Join(quoted, ".")
	}
	return quoteIdentPart(name)
}

// quoteIdentPart quotes a single identifier part. If it's a simple
// alphanumeric identifier, it's returned as-is. Otherwise, it's wrapped in
// double quotes (the ANSI SQL standard, which CockroachDB supports).
func quoteIdentPart(name string) string {
	if isSimpleIdent(name) {
		return name
	}
	// Double any existing double-quotes per ANSI SQL.
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// isSimpleIdent returns true if the name consists only of alphanumeric
// characters and underscores, starting with a letter or underscore.
func isSimpleIdent(name string) bool {
	if len(name) == 0 {
		return false
	}
	for i, c := range name {
		if i == 0 {
			if !isLetter(c) && c != '_' {
				return false
			}
		} else {
			if !isLetter(c) && !isDigit(c) && c != '_' {
				return false
			}
		}
	}
	return true
}

func isLetter(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c rune) bool {
	return c >= '0' && c <= '9'
}

// mapDataType converts a Sybase/SQL Server data type name to its CockroachDB
// equivalent. The input is expected to be upper-cased (as produced by the
// parser's parseDataType).
func mapDataType(dt string) string {
	// Split into type name and optional arguments.
	name, args := splitTypeArgs(dt)

	switch name {
	// Integer types.
	case "TINYINT":
		return "INT2" // CockroachDB has no unsigned integer; INT2 is the closest.
	case "SMALLINT":
		return "INT2"
	case "INT", "INTEGER":
		return "INT4"
	case "BIGINT":
		return "INT8"

	// Float types.
	case "REAL":
		return "FLOAT4"
	case "FLOAT":
		return "FLOAT8"

	// Bit type.
	case "BIT":
		return "BOOL"

	// Character types. CockroachDB uses STRING / VARCHAR.
	case "CHAR":
		if args != "" {
			return fmt.Sprintf("CHAR(%s)", args)
		}
		return "CHAR"
	case "VARCHAR":
		if args != "" {
			if strings.ToUpper(args) == "MAX" {
				return "STRING"
			}
			return fmt.Sprintf("VARCHAR(%s)", args)
		}
		return "VARCHAR"
	case "NCHAR":
		// CRDB treats all strings as UTF-8; NCHAR maps to CHAR.
		if args != "" {
			return fmt.Sprintf("CHAR(%s)", args)
		}
		return "CHAR"
	case "NVARCHAR":
		if args != "" {
			if strings.ToUpper(args) == "MAX" {
				return "STRING"
			}
			return fmt.Sprintf("VARCHAR(%s)", args)
		}
		return "VARCHAR"
	case "TEXT", "NTEXT":
		return "STRING"

	// Binary types.
	case "BINARY":
		if args != "" {
			return fmt.Sprintf("BYTES")
		}
		return "BYTES"
	case "VARBINARY":
		return "BYTES"
	case "IMAGE":
		return "BYTES"

	// Date/time types.
	case "DATETIME", "DATETIME2":
		return "TIMESTAMP"
	case "SMALLDATETIME":
		return "TIMESTAMP"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "DATETIMEOFFSET":
		return "TIMESTAMPTZ"

	// Money types → DECIMAL with fixed precision/scale.
	case "MONEY":
		return "DECIMAL(19, 4)"
	case "SMALLMONEY":
		return "DECIMAL(10, 4)"

	// Numeric/Decimal — pass through with args.
	case "NUMERIC":
		if args != "" {
			return fmt.Sprintf("DECIMAL(%s)", args)
		}
		return "DECIMAL"
	case "DECIMAL":
		if args != "" {
			return fmt.Sprintf("DECIMAL(%s)", args)
		}
		return "DECIMAL"

	// GUID → UUID.
	case "UNIQUEIDENTIFIER":
		return "UUID"

	// Timestamp (rowversion in SQL Server) → BYTES.
	case "TIMESTAMP", "ROWVERSION":
		// SQL Server TIMESTAMP is a binary counter, not a date/time.
		// It's 8 bytes, used for optimistic concurrency.
		return "BYTES"

	default:
		// Unknown types are passed through unchanged.
		if args != "" {
			return fmt.Sprintf("%s(%s)", name, args)
		}
		return name
	}
}

// splitTypeArgs splits a data type string like "VARCHAR(255)" into
// name="VARCHAR" and args="255". For types without arguments, args is empty.
func splitTypeArgs(dt string) (name, args string) {
	idx := strings.IndexByte(dt, '(')
	if idx < 0 {
		return dt, ""
	}
	name = dt[:idx]
	// Strip the surrounding parentheses.
	args = dt[idx+1:]
	if strings.HasSuffix(args, ")") {
		args = args[:len(args)-1]
	}
	return name, args
}
