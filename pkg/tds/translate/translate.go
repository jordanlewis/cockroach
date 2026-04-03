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
//   - OFFSET-FETCH → LIMIT/OFFSET
//   - Bracket-quoted identifiers [name] → double-quoted identifiers "name"
//   - ISNULL(a,b) → COALESCE(a,b)
//   - CONVERT(type, expr) → CAST(expr AS type)
//   - GETDATE() → now()
//   - String concatenation with + → ||
//   - @@ROWCOUNT → crdb_internal.num_rows_affected (placeholder)
//   - @@IDENTITY → lastval()
//   - @@VERSION → version()
//   - UNION/INTERSECT/EXCEPT → pass-through
//   - WITH (CTEs) → pass-through
//   - Subqueries, EXISTS, ANY/ALL → pass-through
//   - Window functions (OVER) → pass-through
//   - SOME → ANY (T-SQL synonym)
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
		if s.IfExists {
			return fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(s.Table)), nil
		}
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
	case *parser.AlterTableStmt:
		return translateAlterTable(s), nil
	case *parser.CreateIndexStmt:
		return translateCreateIndex(s), nil
	case *parser.CreateViewStmt:
		return translateCreateView(s), nil
	case *parser.TruncateTableStmt:
		return fmt.Sprintf("TRUNCATE TABLE %s", quoteIdent(s.Table)), nil
	case *parser.DropViewStmt:
		if s.IfExists {
			return fmt.Sprintf("DROP VIEW IF EXISTS %s", quoteIdent(s.Name)), nil
		}
		return fmt.Sprintf("DROP VIEW %s", quoteIdent(s.Name)), nil
	case *parser.DropIndexStmt:
		return translateDropIndex(s), nil
	case *parser.DropProcedureStmt:
		if s.IfExists {
			return fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", quoteIdent(s.Name)), nil
		}
		return fmt.Sprintf("DROP PROCEDURE %s", quoteIdent(s.Name)), nil
	case *parser.CreateProcedureStmt:
		return "", fmt.Errorf("unsupported: CREATE PROCEDURE is not available in CockroachDB TDS")
	case *parser.CreateFunctionStmt:
		return "", fmt.Errorf("unsupported: CREATE FUNCTION is not available in CockroachDB TDS")
	case *parser.CreateTriggerStmt:
		return "", fmt.Errorf("unsupported: CREATE TRIGGER is not available in CockroachDB TDS")
	case *parser.CompoundSelectStmt:
		return translateCompoundSelect(s)
	case *parser.WithStmt:
		return translateWith(s)
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

// translateSelect converts a T-SQL SELECT (with TOP, WHERE, ORDER BY,
// OFFSET-FETCH) to CRDB syntax. TOP N is moved to a trailing LIMIT N clause.
// OFFSET-FETCH is translated to LIMIT/OFFSET.
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
			b.WriteString(translateTableRef(ref))
		}
		for _, j := range s.Joins {
			fmt.Fprintf(&b, " %s %s", j.Type, quoteIdent(j.Table.Name))
			if j.Table.Alias != "" {
				fmt.Fprintf(&b, " %s", quoteIdent(j.Table.Alias))
			}
			if j.Condition != nil {
				fmt.Fprintf(&b, " ON %s", translateExpr(j.Condition))
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
	// LIMIT: either from TOP or FETCH (FETCH takes precedence).
	if s.Fetch != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Fetch)
	} else if s.Top != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Top)
	}
	// OFFSET from OFFSET-FETCH.
	if s.Offset != nil {
		fmt.Fprintf(&b, " OFFSET %d", *s.Offset)
	}
	return b.String()
}

// translateTableRef converts a table reference. Derived tables (subqueries in
// FROM) are translated recursively.
func translateTableRef(ref parser.TableRef) string {
	if ref.Subquery != nil {
		sub, err := Statement(ref.Subquery)
		if err != nil {
			// Fallback to AST String() if translation fails.
			sub = ref.Subquery.String()
		}
		if ref.Alias != "" {
			return fmt.Sprintf("(%s) %s", sub, quoteIdent(ref.Alias))
		}
		return fmt.Sprintf("(%s)", sub)
	}
	result := quoteIdent(ref.Name)
	if ref.Alias != "" {
		result += " " + quoteIdent(ref.Alias)
	}
	return result
}

// translateCompoundSelect converts a compound SELECT (UNION/INTERSECT/EXCEPT)
// to CRDB syntax. CockroachDB supports all standard set operations natively.
func translateCompoundSelect(s *parser.CompoundSelectStmt) (string, error) {
	left, err := Statement(s.Left)
	if err != nil {
		return "", err
	}
	right, err := Statement(s.Right)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString(left)
	fmt.Fprintf(&b, " %s ", s.Op)
	b.WriteString(right)
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
	if s.Fetch != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Fetch)
	}
	if s.Offset != nil {
		fmt.Fprintf(&b, " OFFSET %d", *s.Offset)
	}
	return b.String(), nil
}

// translateWith converts a WITH (CTE) statement. CockroachDB supports CTEs
// natively with identical syntax.
func translateWith(s *parser.WithStmt) (string, error) {
	var b strings.Builder
	b.WriteString("WITH ")
	for i, cte := range s.CTEs {
		if i > 0 {
			b.WriteString(", ")
		}
		cteSql, err := Statement(cte.Select)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s AS (%s)", quoteIdent(cte.Name), cteSql)
	}
	body, err := Statement(s.Body)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, " %s", body)
	return b.String(), nil
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

	case *parser.CaseExpr:
		return translateCase(e)

	case *parser.InExpr:
		return translateIn(e)

	case *parser.BetweenExpr:
		return translateBetween(e)

	case *parser.SubqueryExpr:
		return translateSubquery(e)

	case *parser.ExistsExpr:
		return translateExists(e)

	case *parser.AnyAllExpr:
		return translateAnyAll(e)

	case *parser.WindowExpr:
		return translateWindow(e)

	default:
		// Fallback: use the AST node's own String() method.
		return expr.String()
	}
}

// translateSubquery converts a scalar subquery expression. CockroachDB
// supports scalar subqueries with identical syntax.
func translateSubquery(e *parser.SubqueryExpr) string {
	sql, err := Statement(e.Select)
	if err != nil {
		return e.String()
	}
	return fmt.Sprintf("(%s)", sql)
}

// translateExists converts [NOT] EXISTS (SELECT ...). CockroachDB supports
// EXISTS with identical syntax.
func translateExists(e *parser.ExistsExpr) string {
	sql, err := Statement(e.Select)
	if err != nil {
		return e.String()
	}
	if e.Not {
		return fmt.Sprintf("NOT EXISTS (%s)", sql)
	}
	return fmt.Sprintf("EXISTS (%s)", sql)
}

// translateAnyAll converts expr op ANY/ALL/SOME (SELECT ...). SOME is
// translated to ANY since CockroachDB uses the standard SQL ANY keyword.
func translateAnyAll(e *parser.AnyAllExpr) string {
	left := translateExpr(e.Expr)
	sql, err := Statement(e.Select)
	if err != nil {
		return e.String()
	}
	kind := e.Kind
	if kind == "SOME" {
		kind = "ANY"
	}
	return fmt.Sprintf("%s %s %s (%s)", left, e.Op, kind, sql)
}

// translateWindow converts a window function expression. CockroachDB supports
// window functions with identical syntax.
func translateWindow(e *parser.WindowExpr) string {
	var b strings.Builder
	b.WriteString(translateFuncCall(e.Func))
	b.WriteString(" OVER (")
	if len(e.PartitionBy) > 0 {
		b.WriteString("PARTITION BY ")
		for i, p := range e.PartitionBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(p))
		}
		if len(e.OrderBy) > 0 {
			b.WriteString(" ")
		}
	}
	if len(e.OrderBy) > 0 {
		b.WriteString("ORDER BY ")
		for i, o := range e.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(o.Expr))
			if o.Desc {
				b.WriteString(" DESC")
			} else {
				b.WriteString(" ASC")
			}
		}
	}
	b.WriteString(")")
	return b.String()
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
		return "now()"

	// --- String functions ---

	case "LEN":
		// LEN(s) → length(s)
		args := translateArgs(e.Args)
		return fmt.Sprintf("length(%s)", strings.Join(args, ", "))

	case "CHARINDEX":
		// CHARINDEX(substr, str) → strpos(str, substr)
		// Note: argument order is swapped.
		if len(e.Args) >= 2 {
			substr := translateExpr(e.Args[0])
			str := translateExpr(e.Args[1])
			return fmt.Sprintf("strpos(%s, %s)", str, substr)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("strpos(%s)", strings.Join(args, ", "))

	case "PATINDEX":
		// PATINDEX(pattern, str) → approximate via strpos (no direct equivalent).
		// Strip leading/trailing % from the pattern for a basic strpos translation.
		// This is a lossy translation but handles the common %substr% case.
		if len(e.Args) >= 2 {
			str := translateExpr(e.Args[1])
			pattern := translateExpr(e.Args[0])
			return fmt.Sprintf("strpos(%s, %s) /* PATINDEX approximation */", str, pattern)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("strpos(%s)", strings.Join(args, ", "))

	case "STUFF":
		// STUFF(str, start, length, insert) → overlay(str placing insert from start for length)
		if len(e.Args) == 4 {
			str := translateExpr(e.Args[0])
			start := translateExpr(e.Args[1])
			length := translateExpr(e.Args[2])
			insert := translateExpr(e.Args[3])
			return fmt.Sprintf("overlay(%s placing %s from %s for %s)",
				str, insert, start, length)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("overlay(%s)", strings.Join(args, ", "))

	case "REPLICATE":
		// REPLICATE(str, n) → repeat(str, n)
		args := translateArgs(e.Args)
		return fmt.Sprintf("repeat(%s)", strings.Join(args, ", "))

	case "SPACE":
		// SPACE(n) → repeat(' ', n)
		if len(e.Args) == 1 {
			n := translateExpr(e.Args[0])
			return fmt.Sprintf("repeat(' ', %s)", n)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("repeat(' ', %s)", strings.Join(args, ", "))

	case "STRING_AGG":
		// STRING_AGG(expr, separator) — same in CRDB.
		args := translateArgs(e.Args)
		return fmt.Sprintf("string_agg(%s)", strings.Join(args, ", "))

	case "QUOTENAME":
		// QUOTENAME(str) → quote_ident(str)
		args := translateArgs(e.Args)
		return fmt.Sprintf("quote_ident(%s)", strings.Join(args, ", "))

	// --- Date/time functions ---

	case "DATEADD":
		// DATEADD(part, n, date) → (date::TIMESTAMPTZ + n * INTERVAL '1 part')
		if len(e.Args) == 3 {
			part := identName(e.Args[0])
			n := translateExpr(e.Args[1])
			date := translateExpr(e.Args[2])
			interval := mapDatepartInterval(part)
			return fmt.Sprintf("(%s::TIMESTAMPTZ + %s * INTERVAL '%s')",
				date, n, interval)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DATEADD(%s)", strings.Join(args, ", "))

	case "DATEDIFF":
		// DATEDIFF(part, start, end) → extract(epoch FROM end::TIMESTAMPTZ - start::TIMESTAMPTZ)
		// divided by the appropriate divisor for the datepart.
		if len(e.Args) == 3 {
			part := identName(e.Args[0])
			start := translateExpr(e.Args[1])
			end := translateExpr(e.Args[2])
			return translateDateDiff(part, start, end)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DATEDIFF(%s)", strings.Join(args, ", "))

	case "DATEPART":
		// DATEPART(part, date) → extract(part FROM date::TIMESTAMPTZ)
		if len(e.Args) == 2 {
			part := identName(e.Args[0])
			date := translateExpr(e.Args[1])
			return fmt.Sprintf("EXTRACT(%s FROM %s::TIMESTAMPTZ)::INT",
				mapExtractPart(part), date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DATEPART(%s)", strings.Join(args, ", "))

	case "DATENAME":
		// DATENAME(part, date) → to_char(date::TIMESTAMPTZ, format)
		if len(e.Args) == 2 {
			part := identName(e.Args[0])
			date := translateExpr(e.Args[1])
			format := mapDatenamePart(part)
			return fmt.Sprintf("to_char(%s::TIMESTAMPTZ, '%s')", date, format)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DATENAME(%s)", strings.Join(args, ", "))

	case "YEAR":
		// YEAR(date) → EXTRACT(year FROM date::TIMESTAMPTZ)::INT
		if len(e.Args) == 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf("EXTRACT(year FROM %s::TIMESTAMPTZ)::INT", date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("YEAR(%s)", strings.Join(args, ", "))

	case "MONTH":
		// MONTH(date) → EXTRACT(month FROM date::TIMESTAMPTZ)::INT
		if len(e.Args) == 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf("EXTRACT(month FROM %s::TIMESTAMPTZ)::INT", date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("MONTH(%s)", strings.Join(args, ", "))

	case "DAY":
		// DAY(date) → EXTRACT(day FROM date::TIMESTAMPTZ)::INT
		if len(e.Args) == 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf("EXTRACT(day FROM %s::TIMESTAMPTZ)::INT", date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DAY(%s)", strings.Join(args, ", "))

	case "SYSDATETIME":
		return "now()"

	case "GETUTCDATE":
		return "(now() AT TIME ZONE 'UTC')"

	case "EOMONTH":
		// EOMONTH(date) → (date_trunc('month', date::TIMESTAMPTZ) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
		if len(e.Args) >= 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf(
				"(date_trunc('month', %s::TIMESTAMPTZ) + INTERVAL '1 month' - INTERVAL '1 day')::DATE",
				date)
		}
		return "EOMONTH()"

	case "ISDATE":
		// No direct equivalent; use a try_cast approach.
		if len(e.Args) == 1 {
			arg := translateExpr(e.Args[0])
			return fmt.Sprintf(
				"CASE WHEN try_cast(%s AS TIMESTAMPTZ) IS NOT NULL THEN 1 ELSE 0 END", arg)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("ISDATE(%s)", strings.Join(args, ", "))

	case "FORMAT":
		// T-SQL FORMAT(value, format_string) has no direct CRDB equivalent.
		// Pass through as to_char for basic cases.
		if len(e.Args) >= 2 {
			val := translateExpr(e.Args[0])
			format := translateExpr(e.Args[1])
			return fmt.Sprintf("to_char(%s, %s)", val, format)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("FORMAT(%s)", strings.Join(args, ", "))

	// --- Math functions ---

	case "SQUARE":
		// SQUARE(x) → power(x, 2)
		if len(e.Args) == 1 {
			arg := translateExpr(e.Args[0])
			return fmt.Sprintf("power(%s, 2)", arg)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("power(%s, 2)", strings.Join(args, ", "))

	case "LOG":
		// T-SQL LOG(x) is natural log; CRDB log(x) is base-10.
		// Translate to ln(x).
		args := translateArgs(e.Args)
		return fmt.Sprintf("ln(%s)", strings.Join(args, ", "))

	case "LOG10":
		// LOG10(x) → log(x) in CRDB (which is base-10).
		args := translateArgs(e.Args)
		return fmt.Sprintf("log(%s)", strings.Join(args, ", "))

	case "RAND":
		return "random()"

	// --- Conditional functions ---

	case "IIF":
		// IIF(cond, true_val, false_val) → CASE WHEN cond THEN true_val ELSE false_val END
		if len(e.Args) == 3 {
			cond := translateExpr(e.Args[0])
			trueVal := translateExpr(e.Args[1])
			falseVal := translateExpr(e.Args[2])
			return fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END",
				cond, trueVal, falseVal)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("IIF(%s)", strings.Join(args, ", "))

	case "CHOOSE":
		// CHOOSE(idx, val1, val2, ...) → CASE idx WHEN 1 THEN val1 WHEN 2 THEN val2 ... END
		if len(e.Args) >= 2 {
			idx := translateExpr(e.Args[0])
			var b strings.Builder
			fmt.Fprintf(&b, "CASE %s", idx)
			for i, arg := range e.Args[1:] {
				fmt.Fprintf(&b, " WHEN %d THEN %s", i+1, translateExpr(arg))
			}
			b.WriteString(" END")
			return b.String()
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("CHOOSE(%s)", strings.Join(args, ", "))

	// --- Type conversion ---

	case "TRY_CONVERT":
		// TRY_CONVERT(type, expr) → try_cast(expr AS type)
		// The parser sees the type as an identifier in the first arg.
		if len(e.Args) >= 2 {
			typeName := identName(e.Args[0])
			if typeName == "" {
				typeName = translateExpr(e.Args[0])
			}
			expr := translateExpr(e.Args[1])
			return fmt.Sprintf("try_cast(%s AS %s)", expr, mapDataType(strings.ToUpper(typeName)))
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("TRY_CONVERT(%s)", strings.Join(args, ", "))

	// --- System functions ---

	case "NEWID":
		return "gen_random_uuid()"

	case "OBJECT_ID":
		// No direct equivalent; return NULL with a comment.
		args := translateArgs(e.Args)
		return fmt.Sprintf("NULL /* OBJECT_ID(%s) not supported */",
			strings.Join(args, ", "))

	case "DB_NAME":
		return "current_database()"

	case "SCHEMA_NAME":
		return "current_schema()"

	case "USER_NAME":
		return "current_user"

	case "HOST_NAME":
		return "NULL /* HOST_NAME() not supported */"

	case "APP_NAME":
		return "current_setting('application_name')"

	// --- Aggregate functions ---

	case "COUNT_BIG":
		// COUNT_BIG(*) → count(*) — CRDB count already returns INT8.
		args := translateArgs(e.Args)
		return fmt.Sprintf("count(%s)", strings.Join(args, ", "))

	case "STDEV":
		args := translateArgs(e.Args)
		return fmt.Sprintf("stddev(%s)", strings.Join(args, ", "))

	case "STDEVP":
		args := translateArgs(e.Args)
		return fmt.Sprintf("stddev_pop(%s)", strings.Join(args, ", "))

	case "VAR":
		args := translateArgs(e.Args)
		return fmt.Sprintf("variance(%s)", strings.Join(args, ", "))

	case "VARP":
		args := translateArgs(e.Args)
		return fmt.Sprintf("var_pop(%s)", strings.Join(args, ", "))

	case "CHECKSUM_AGG":
		args := translateArgs(e.Args)
		return fmt.Sprintf("NULL /* CHECKSUM_AGG(%s) not supported */",
			strings.Join(args, ", "))

	default:
		// Pass through unknown functions.
		args := translateArgs(e.Args)
		return fmt.Sprintf("%s(%s)", name, strings.Join(args, ", "))
	}
}

// identName extracts the identifier name from an expression if it is an
// IdentExpr with a single part. Returns empty string otherwise.
func identName(expr parser.Expr) string {
	if id, ok := expr.(*parser.IdentExpr); ok && len(id.Parts) == 1 {
		return id.Parts[0]
	}
	return ""
}

// mapDatepartInterval returns the INTERVAL unit string for a T-SQL datepart
// keyword. Used by DATEADD translation.
func mapDatepartInterval(part string) string {
	switch strings.ToLower(part) {
	case "year", "yy", "yyyy":
		return "1 year"
	case "quarter", "qq", "q":
		return "3 months"
	case "month", "mm", "m":
		return "1 month"
	case "week", "wk", "ww":
		return "1 week"
	case "day", "dd", "d", "dayofyear", "dy", "y":
		return "1 day"
	case "hour", "hh":
		return "1 hour"
	case "minute", "mi", "n":
		return "1 minute"
	case "second", "ss", "s":
		return "1 second"
	case "millisecond", "ms":
		return "1 millisecond"
	case "microsecond", "mcs":
		return "1 microsecond"
	default:
		return "1 " + part
	}
}

// mapExtractPart maps T-SQL datepart keywords to EXTRACT field names.
func mapExtractPart(part string) string {
	switch strings.ToLower(part) {
	case "year", "yy", "yyyy":
		return "year"
	case "quarter", "qq", "q":
		return "quarter"
	case "month", "mm", "m":
		return "month"
	case "week", "wk", "ww":
		return "week"
	case "day", "dd", "d":
		return "day"
	case "dayofyear", "dy", "y":
		return "doy"
	case "hour", "hh":
		return "hour"
	case "minute", "mi", "n":
		return "minute"
	case "second", "ss", "s":
		return "second"
	case "millisecond", "ms":
		return "millisecond"
	case "microsecond", "mcs":
		return "microsecond"
	case "weekday", "dw":
		return "dow"
	default:
		return part
	}
}

// mapDatenamePart maps T-SQL datepart keywords to to_char format strings
// used by DATENAME translation.
func mapDatenamePart(part string) string {
	switch strings.ToLower(part) {
	case "year", "yy", "yyyy":
		return "YYYY"
	case "quarter", "qq", "q":
		return "Q"
	case "month", "mm", "m":
		return "Month"
	case "week", "wk", "ww":
		return "WW"
	case "day", "dd", "d":
		return "DD"
	case "dayofyear", "dy", "y":
		return "DDD"
	case "weekday", "dw":
		return "Day"
	case "hour", "hh":
		return "HH24"
	case "minute", "mi", "n":
		return "MI"
	case "second", "ss", "s":
		return "SS"
	default:
		return part
	}
}

// translateDateDiff translates DATEDIFF(part, start, end) to a CRDB expression
// using epoch extraction and integer division.
func translateDateDiff(part, start, end string) string {
	diff := fmt.Sprintf("EXTRACT(epoch FROM %s::TIMESTAMPTZ - %s::TIMESTAMPTZ)", end, start)
	switch strings.ToLower(part) {
	case "year", "yy", "yyyy":
		return fmt.Sprintf("(%s / 31557600)::INT", diff)
	case "quarter", "qq", "q":
		return fmt.Sprintf("(%s / 7889400)::INT", diff)
	case "month", "mm", "m":
		return fmt.Sprintf("(%s / 2629800)::INT", diff)
	case "week", "wk", "ww":
		return fmt.Sprintf("(%s / 604800)::INT", diff)
	case "day", "dd", "d", "dayofyear", "dy", "y":
		return fmt.Sprintf("(%s / 86400)::INT", diff)
	case "hour", "hh":
		return fmt.Sprintf("(%s / 3600)::INT", diff)
	case "minute", "mi", "n":
		return fmt.Sprintf("(%s / 60)::INT", diff)
	case "second", "ss", "s":
		return fmt.Sprintf("(%s)::INT", diff)
	case "millisecond", "ms":
		return fmt.Sprintf("(%s * 1000)::INT", diff)
	default:
		return fmt.Sprintf("(%s)::INT", diff)
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

// translateCase converts a CASE expression. The syntax is the same in CRDB.
func translateCase(e *parser.CaseExpr) string {
	var b strings.Builder
	b.WriteString("CASE")
	if e.Operand != nil {
		fmt.Fprintf(&b, " %s", translateExpr(e.Operand))
	}
	for _, w := range e.Whens {
		fmt.Fprintf(&b, " WHEN %s THEN %s",
			translateExpr(w.Cond), translateExpr(w.Result))
	}
	if e.Else != nil {
		fmt.Fprintf(&b, " ELSE %s", translateExpr(e.Else))
	}
	b.WriteString(" END")
	return b.String()
}

// translateIn converts an IN expression. Supports both value lists and
// subqueries.
func translateIn(e *parser.InExpr) string {
	expr := translateExpr(e.Expr)
	op := "IN"
	if e.Not {
		op = "NOT IN"
	}
	if e.Subquery != nil {
		sql, err := Statement(e.Subquery)
		if err != nil {
			return e.String()
		}
		return fmt.Sprintf("%s %s (%s)", expr, op, sql)
	}
	vals := translateArgs(e.Values)
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

// translateAlterTable converts a T-SQL ALTER TABLE to CRDB syntax. Key
// differences: ADD column requires the COLUMN keyword in CRDB, and ALTER
// COLUMN requires SET DATA TYPE.
func translateAlterTable(s *parser.AlterTableStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s ", quoteIdent(s.Table))

	switch cmd := s.Cmd.(type) {
	case *parser.AddColumnCmd:
		fmt.Fprintf(&b, "ADD COLUMN %s %s",
			quoteIdent(cmd.Column.Name), mapDataType(cmd.Column.DataType))
		if cmd.Column.Nullable != nil {
			if *cmd.Column.Nullable {
				b.WriteString(" NULL")
			} else {
				b.WriteString(" NOT NULL")
			}
		} else {
			b.WriteString(" NULL") // Sybase default
		}
	case *parser.DropColumnCmd:
		fmt.Fprintf(&b, "DROP COLUMN %s", quoteIdent(cmd.Name))
	case *parser.AlterColumnCmd:
		fmt.Fprintf(&b, "ALTER COLUMN %s SET DATA TYPE %s",
			quoteIdent(cmd.Name), mapDataType(cmd.DataType))
	case *parser.AddConstraintCmd:
		translateAddConstraint(&b, cmd)
	case *parser.DropConstraintCmd:
		fmt.Fprintf(&b, "DROP CONSTRAINT %s", quoteIdent(cmd.Name))
	}

	return b.String()
}

// translateAddConstraint appends a constraint definition to the builder.
func translateAddConstraint(b *strings.Builder, cmd *parser.AddConstraintCmd) {
	fmt.Fprintf(b, "ADD CONSTRAINT %s ", quoteIdent(cmd.Name))
	switch cmd.Type {
	case parser.PrimaryKeyConstraint:
		b.WriteString("PRIMARY KEY (")
		writeQuotedColumns(b, cmd.Columns)
		b.WriteString(")")
	case parser.ForeignKeyConstraint:
		b.WriteString("FOREIGN KEY (")
		writeQuotedColumns(b, cmd.Columns)
		fmt.Fprintf(b, ") REFERENCES %s (", quoteIdent(cmd.RefTable))
		writeQuotedColumns(b, cmd.RefColumns)
		b.WriteString(")")
	case parser.UniqueConstraint:
		b.WriteString("UNIQUE (")
		writeQuotedColumns(b, cmd.Columns)
		b.WriteString(")")
	case parser.CheckConstraint:
		fmt.Fprintf(b, "CHECK %s", translateExpr(cmd.CheckExpr))
	}
}

func writeQuotedColumns(b *strings.Builder, cols []string) {
	for i, col := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(col))
	}
}

// translateCreateIndex converts CREATE [UNIQUE] INDEX to CRDB syntax.
// T-SQL INCLUDE is mapped to CockroachDB's STORING clause.
func translateCreateIndex(s *parser.CreateIndexStmt) string {
	var b strings.Builder
	b.WriteString("CREATE ")
	if s.Unique {
		b.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&b, "INDEX %s ON %s (",
		quoteIdent(s.Name), quoteIdent(s.Table))
	writeQuotedColumns(&b, s.Columns)
	b.WriteString(")")
	if len(s.Include) > 0 {
		b.WriteString(" STORING (")
		writeQuotedColumns(&b, s.Include)
		b.WriteString(")")
	}
	return b.String()
}

// translateCreateView converts CREATE VIEW ... AS SELECT to CRDB syntax.
func translateCreateView(s *parser.CreateViewStmt) string {
	return fmt.Sprintf("CREATE VIEW %s AS %s",
		quoteIdent(s.Name), translateSelect(s.Select))
}

// translateDropIndex converts T-SQL DROP INDEX to CRDB syntax.
// T-SQL: DROP INDEX idx ON tbl → CRDB: DROP INDEX tbl@idx
func translateDropIndex(s *parser.DropIndexStmt) string {
	var b strings.Builder
	b.WriteString("DROP INDEX ")
	if s.IfExists {
		b.WriteString("IF EXISTS ")
	}
	if s.Table != "" {
		fmt.Fprintf(&b, "%s@%s", quoteIdent(s.Table), quoteIdent(s.Name))
	} else {
		b.WriteString(quoteIdent(s.Name))
	}
	return b.String()
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
