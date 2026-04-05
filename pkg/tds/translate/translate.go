// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package translate converts T-SQL AST nodes (as produced by the parser
// package) into CockroachDB-compatible SQL strings.
//
// # Dialect scope
//
// This package handles both SQL Server (Microsoft) and Sybase ASE dialect
// features. The two dialects share a common T-SQL heritage but diverged
// after the 2010 SAP acquisition of Sybase. Features are annotated below
// as [Both], [SQL Server], or [Sybase ASE] to indicate which dialect(s)
// introduced or support them.
//
// # Translations
//
// [Both] Common T-SQL features supported by both SQL Server and Sybase ASE:
//
//   - USE <db> → SET database = '<db>'
//   - CREATE TABLE: default-nullable semantics (columns are nullable unless
//     explicitly declared NOT NULL)
//   - SELECT TOP N → LIMIT N
//   - Bracket-quoted identifiers [name] → double-quoted identifiers "name"
//   - ISNULL(a,b) → COALESCE(a,b)
//   - CONVERT(type, expr) → CAST(expr AS type)
//   - GETDATE() → now()
//   - String concatenation with + → ||
//   - @@ROWCOUNT, @@IDENTITY, @@VERSION system variables
//   - UNION/INTERSECT/EXCEPT, WITH (CTEs), subqueries, EXISTS, ANY/ALL/SOME
//   - Window functions (OVER) → pass-through
//   - CASE, BETWEEN, IN → pass-through
//   - BEGIN/COMMIT/ROLLBACK TRAN, SAVE TRAN
//   - IDENTITY columns, DEFAULT expressions, computed columns
//   - Data types: INT/SMALLINT/BIGINT/TINYINT, FLOAT/REAL, CHAR/VARCHAR,
//     NCHAR/NVARCHAR, TEXT/NTEXT, BINARY/VARBINARY/IMAGE, DATETIME,
//     SMALLDATETIME, DATE/TIME, MONEY/SMALLMONEY, NUMERIC/DECIMAL,
//     UNIQUEIDENTIFIER, BIT
//   - Functions: LEN, CHARINDEX, STUFF, REPLICATE, SPACE, DATEADD,
//     DATEDIFF, DATEPART, DATENAME, YEAR/MONTH/DAY, LOG, LOG10
//   - DDL: ALTER TABLE, CREATE INDEX (with INCLUDE), CREATE/DROP VIEW,
//     TRUNCATE TABLE
//   - Multi-table DELETE/UPDATE...FROM
//
// [SQL Server] Features specific to Microsoft SQL Server (not in Sybase ASE):
//
//   - OFFSET n ROWS FETCH NEXT m ROWS ONLY (SQL Server 2012+)
//   - OUTPUT clause (inserted.*/deleted.*) on INSERT/UPDATE/DELETE/MERGE
//   - MERGE INTO ... USING ... ON ... WHEN MATCHED/NOT MATCHED
//   - DATETIME2, DATETIMEOFFSET (SQL Server 2008+)
//   - TIMESTAMP/ROWVERSION (binary counter, not datetime)
//   - STRING_AGG (SQL Server 2017+), IIF, CHOOSE, TRY_CONVERT, FORMAT
//     (all SQL Server 2012+)
//   - SYSDATETIME, GETUTCDATE, EOMONTH, ISDATE, NEWID
//   - COUNT_BIG, STDEV/STDEVP, VAR/VARP, CHECKSUM_AGG
//   - OBJECT_ID, DB_NAME, SCHEMA_NAME, USER_NAME, HOST_NAME, APP_NAME
//   - PIVOT → conditional aggregation (SUM(CASE WHEN ... THEN ... END))
//   - UNPIVOT → CROSS JOIN LATERAL with VALUES
//   - CREATE PROCEDURE/FUNCTION/TRIGGER (parsed but rejected)
//   - VARCHAR(MAX)/NVARCHAR(MAX)
//   - DROP INDEX idx ON tbl syntax, PATINDEX, QUOTENAME
//
// [Sybase ASE] Features specific to SAP Sybase ASE (not in SQL Server):
//
//   - ROWS LIMIT x [OFFSET y] pagination (Sybase ASE 15.7+)
//   - LIST(expr [, separator]) aggregate (Sybase equivalent of STRING_AGG)
//   - UNSIGNED integer types: UNSIGNED TINYINT/SMALLINT/INT/BIGINT
//   - BIGDATETIME, BIGTIME (microsecond-precision, Sybase ASE 15.5+)
//   - UNICHAR, UNIVARCHAR, UNITEXT (Sybase Unicode character types)
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
		return translateInsert(s)
	case *parser.SelectStmt:
		if idx := findPivotRef(s.From); idx >= 0 {
			return translateSelectWithPivot(s, idx), nil
		}
		if idx := findUnpivotRef(s.From); idx >= 0 {
			return translateSelectWithUnpivot(s, idx), nil
		}
		if s.IntoTable != "" {
			return translateSelectInto(s), nil
		}
		if len(s.Compute) > 0 {
			return translateSelectWithCompute(s)
		}
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
	case *parser.MergeStmt:
		return translateMerge(s)
	case *parser.CompoundSelectStmt:
		return translateCompoundSelect(s)
	case *parser.WithStmt:
		return translateWith(s)
	case *parser.BeginTranStmt:
		return translateBeginTran(s), nil
	case *parser.CommitTranStmt:
		return "COMMIT", nil
	case *parser.RollbackTranStmt:
		return translateRollbackTran(s), nil
	case *parser.SaveTranStmt:
		return fmt.Sprintf("SAVEPOINT %s", quoteIdent(s.Name)), nil
	case *parser.DeclareVarStmt:
		// Control flow: handled by the executor's interpreter.
		return "", nil
	case *parser.DeclareTableVarStmt:
		return translateDeclareTableVar(s), nil
	case *parser.SetVarStmt:
		return "", nil
	case *parser.IfStmt:
		return "", nil
	case *parser.WhileStmt:
		return "", nil
	case *parser.BeginEndBlock:
		return "", nil
	case *parser.BreakStmt:
		return "", nil
	case *parser.ContinueStmt:
		return "", nil
	case *parser.PrintStmt:
		return "", nil
	case *parser.RaiserrorStmt:
		return "", nil
	case *parser.ExecStmt:
		// EXEC statements are dispatched directly to execStoredProcedure
		// from the executor's execParsedStmt and should never reach the
		// translator. Return empty to be safe.
		return "", nil
	case *parser.ThrowStmt:
		return "", nil
	case *parser.GotoStmt:
		return "", nil
	case *parser.LabelStmt:
		return "", nil
	case *parser.ReturnStmt:
		return "", nil
	case *parser.WaitforStmt:
		return "", nil
	case *parser.BeginTryCatchStmt:
		return "", nil
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

// translateCreateTable converts a T-SQL CREATE TABLE to CRDB syntax.
// [Both] Type names are mapped to their CockroachDB equivalents (see
// mapDataType), and default-nullable semantics are applied: columns
// without an explicit NULL/NOT NULL constraint are assumed nullable,
// matching both SQL Server and Sybase ASE behavior.
func translateCreateTable(s *parser.CreateTableStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (", quoteIdent(s.Table))
	for i, col := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		translateColumnDef(&b, col)
	}
	b.WriteString(")")
	return b.String()
}

// translateDeclareTableVar converts DECLARE @var TABLE (...) to a CREATE
// TABLE statement. The table is created with the @-prefixed name (which
// CockroachDB accepts when double-quoted).
func translateDeclareTableVar(s *parser.DeclareTableVarStmt) string {
	return translateCreateTable(&parser.CreateTableStmt{
		Table:   s.Name,
		Columns: s.Columns,
	})
}

// translateSelectInto converts SELECT ... INTO <table> ... to
// CREATE TABLE <table> AS SELECT ... (CockroachDB's CTAS syntax).
func translateSelectInto(s *parser.SelectStmt) string {
	// Build a copy without IntoTable to get the plain SELECT.
	plain := *s
	plain.IntoTable = ""
	return fmt.Sprintf("CREATE TABLE %s AS %s",
		quoteIdent(s.IntoTable), translateSelect(&plain))
}

// translateColumnDef writes a translated column definition to the builder.
// [Both] Handles regular columns (with type, optional IDENTITY/DEFAULT)
// and computed columns (AS expr → STORED). Both SQL Server and Sybase ASE
// support IDENTITY and computed columns, though the PERSISTED keyword
// (SQL Server) vs AS expression (Sybase) differ slightly in source syntax.
func translateColumnDef(b *strings.Builder, col parser.ColumnDef) {
	if col.ComputedExpr != nil {
		// Computed column: CockroachDB requires an explicit type before AS,
		// even though T-SQL infers it. We perform best-effort type inference
		// from the expression; the declared type only needs to be compatible
		// with the expression result (CockroachDB validates this at DDL time).
		crdbType := inferExprType(col.ComputedExpr)
		fmt.Fprintf(b, "%s %s AS (%s) STORED", quoteIdent(col.Name),
			crdbType, translateExpr(col.ComputedExpr))
		return
	}

	fmt.Fprintf(b, "%s %s", quoteIdent(col.Name), mapDataType(col.DataType))

	// [Both] IDENTITY → GENERATED BY DEFAULT AS IDENTITY. "BY DEFAULT"
	// allows explicit value inserts, matching T-SQL behavior in both
	// SQL Server and Sybase ASE.
	if col.Identity != nil {
		b.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
	}

	if col.DefaultExpr != nil {
		fmt.Fprintf(b, " DEFAULT %s", translateExpr(col.DefaultExpr))
	}

	if col.Nullable != nil {
		if *col.Nullable {
			b.WriteString(" NULL")
		} else {
			b.WriteString(" NOT NULL")
		}
	} else if col.Identity == nil {
		// Sybase/SQL Server default: columns are nullable unless NOT NULL
		// is specified. Emit explicit NULL to match Sybase semantics.
		// IDENTITY columns are implicitly NOT NULL in both T-SQL and
		// CockroachDB, so we skip the explicit NULL for them.
		b.WriteString(" NULL")
	}
}

// translateInsert converts a T-SQL INSERT INTO statement to CRDB syntax.
// [Both] INSERT...VALUES and INSERT...SELECT are standard T-SQL.
// [SQL Server] OUTPUT → RETURNING (inserted.*/deleted.* pseudo-tables are
// SQL Server-specific; Sybase ASE has no OUTPUT clause).
func translateInsert(s *parser.InsertStmt) (string, error) {
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
	if s.Select != nil {
		sel, err := Statement(s.Select)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, " %s", sel)
	} else {
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
	}
	if len(s.Output) > 0 {
		b.WriteString(" RETURNING ")
		b.WriteString(translateOutputColumns(s.Output))
	}
	return b.String(), nil
}

// translateSelect converts a T-SQL SELECT to CRDB syntax.
// [Both] SELECT TOP N → LIMIT N (supported by both SQL Server and Sybase ASE).
// [SQL Server] OFFSET n ROWS FETCH NEXT m ROWS ONLY → LIMIT/OFFSET
// (SQL Server 2012+ pagination syntax).
// [Sybase ASE] ROWS LIMIT x OFFSET y → LIMIT/OFFSET (Sybase ASE 15.7+
// pagination syntax). Both pagination forms produce the same CRDB output.
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
			b.WriteString(translateJoinClause(j))
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

// translateSelectWithCompute translates a SELECT that has COMPUTE clauses.
// The base SELECT (without COMPUTE) is returned as the primary statement.
// The COMPUTE aggregate queries are appended, separated by semicolons,
// so the executor can split and run them as additional result sets.
func translateSelectWithCompute(s *parser.SelectStmt) (string, error) {
	base := translateSelect(s)
	computes := TranslateComputeQueries(s)
	parts := make([]string, 0, 1+len(computes))
	parts = append(parts, base)
	parts = append(parts, computes...)
	return strings.Join(parts, ";\n"), nil
}

// TranslateComputeQueries generates aggregate SELECT statements for each
// COMPUTE clause on the given SelectStmt. Each query mirrors the base
// SELECT's FROM/JOIN/WHERE clauses and adds GROUP BY from the COMPUTE BY
// columns. COMPUTE without BY produces a grand total with no GROUP BY.
func TranslateComputeQueries(s *parser.SelectStmt) []string {
	var queries []string
	for _, cc := range s.Compute {
		var b strings.Builder
		b.WriteString("SELECT ")

		// BY columns come first in the result.
		for i, byExpr := range cc.By {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(byExpr))
		}

		// Then the aggregate expressions.
		for i, agg := range cc.Aggregates {
			if len(cc.By) > 0 || i > 0 {
				b.WriteString(", ")
			}
			funcName := strings.ToUpper(agg.Func)
			b.WriteString(funcName)
			b.WriteString("(")
			b.WriteString(translateExpr(agg.Arg))
			b.WriteString(")")
		}

		// FROM clause (same as base query).
		if len(s.From) > 0 {
			b.WriteString(" FROM ")
			for i, ref := range s.From {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(translateTableRef(ref))
			}
			for _, j := range s.Joins {
				b.WriteString(translateJoinClause(j))
			}
		}

		// WHERE clause (same as base query).
		if s.Where != nil {
			fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
		}

		// GROUP BY from the COMPUTE BY columns.
		if len(cc.By) > 0 {
			b.WriteString(" GROUP BY ")
			for i, byExpr := range cc.By {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(translateExpr(byExpr))
			}
		}

		// ORDER BY the grouping columns to match the base query ordering.
		if len(cc.By) > 0 {
			b.WriteString(" ORDER BY ")
			for i, byExpr := range cc.By {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(translateExpr(byExpr))
				b.WriteString(" ASC")
			}
		}

		queries = append(queries, b.String())
	}
	return queries
}

// findPivotRef returns the index of the first FROM table reference that has a
// PIVOT clause, or -1 if none.
func findPivotRef(refs []parser.TableRef) int {
	for i, ref := range refs {
		if ref.Pivot != nil {
			return i
		}
	}
	return -1
}

// findUnpivotRef returns the index of the first FROM table reference that has
// an UNPIVOT clause, or -1 if none.
func findUnpivotRef(refs []parser.TableRef) int {
	for i, ref := range refs {
		if ref.Unpivot != nil {
			return i
		}
	}
	return -1
}

// translateSelectWithPivot translates a SELECT that uses a PIVOT table
// operator. PIVOT is expanded into conditional aggregation:
//
//	SELECT ... FROM src PIVOT(SUM(salary) FOR dept_id IN (10, 20, 30)) pvt
//	→ SELECT SUM(CASE WHEN dept_id = 10 THEN salary END) AS "10", ... FROM src
//
// [SQL Server] PIVOT is SQL Server 2005+. Sybase ASE does not support PIVOT.
func translateSelectWithPivot(s *parser.SelectStmt, pivotIdx int) string {
	ref := s.From[pivotIdx]
	pivot := ref.Pivot

	var b strings.Builder
	b.WriteString("SELECT ")

	aggFunc := strings.ToUpper(pivot.AggFunc)
	forCol := quoteIdent(pivot.ForCol)
	aggCol := translateExpr(pivot.AggCol)

	for i, val := range pivot.InValues {
		if i > 0 {
			b.WriteString(", ")
		}
		valStr := translateExpr(val)
		fmt.Fprintf(&b, "%s(CASE WHEN %s = %s THEN %s END) AS %s",
			aggFunc, forCol, valStr, aggCol, quoteIdent(valStr))
	}

	// FROM: emit the PIVOT source (subquery or table name).
	b.WriteString(" FROM ")
	sourceRef := parser.TableRef{
		Name:     ref.Name,
		Subquery: ref.Subquery,
	}
	b.WriteString(translateTableRef(sourceRef))

	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
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
	if s.Fetch != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Fetch)
	} else if s.Top != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Top)
	}
	if s.Offset != nil {
		fmt.Fprintf(&b, " OFFSET %d", *s.Offset)
	}
	return b.String()
}

// translateSelectWithUnpivot translates a SELECT that uses an UNPIVOT table
// operator. UNPIVOT is expanded into CROSS JOIN LATERAL with VALUES:
//
//	SELECT val, dept FROM src UNPIVOT(val FOR dept IN (eng, sales)) u
//	→ SELECT val, dept FROM src
//	  CROSS JOIN LATERAL (VALUES (eng, 'eng'), (sales, 'sales'))
//	  AS u(val, dept) WHERE u.val IS NOT NULL
//
// [SQL Server] UNPIVOT is SQL Server 2005+. Sybase ASE does not support UNPIVOT.
func translateSelectWithUnpivot(s *parser.SelectStmt, unpivotIdx int) string {
	ref := s.From[unpivotIdx]
	unpivot := ref.Unpivot

	var b strings.Builder
	b.WriteString("SELECT ")

	for i, col := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(translateExpr(col.Expr))
		if col.Alias != "" {
			fmt.Fprintf(&b, " AS %s", quoteIdent(col.Alias))
		}
	}

	// FROM: emit the source, then CROSS JOIN LATERAL VALUES.
	b.WriteString(" FROM ")
	sourceRef := parser.TableRef{
		Name:     ref.Name,
		Subquery: ref.Subquery,
	}
	b.WriteString(translateTableRef(sourceRef))

	b.WriteString(" CROSS JOIN LATERAL (VALUES ")
	for i, col := range unpivot.InCols {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "(%s, '%s')", quoteIdent(col), col)
	}

	alias := ref.Alias
	if alias == "" {
		alias = "_unpvt"
	}
	fmt.Fprintf(&b, ") AS %s(%s, %s)",
		quoteIdent(alias),
		quoteIdent(unpivot.ValueCol),
		quoteIdent(unpivot.ForCol))

	// UNPIVOT excludes NULL values by default.
	fmt.Fprintf(&b, " WHERE %s.%s IS NOT NULL",
		quoteIdent(alias), quoteIdent(unpivot.ValueCol))

	if s.Where != nil {
		fmt.Fprintf(&b, " AND %s", translateExpr(s.Where))
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
	if s.Fetch != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Fetch)
	} else if s.Top != nil {
		fmt.Fprintf(&b, " LIMIT %d", *s.Top)
	}
	if s.Offset != nil {
		fmt.Fprintf(&b, " OFFSET %d", *s.Offset)
	}
	return b.String()
}

// translateJoinClause converts a single JoinClause to CRDB SQL.
// CROSS APPLY → CROSS JOIN LATERAL, OUTER APPLY → LEFT JOIN LATERAL ... ON true.
func translateJoinClause(j parser.JoinClause) string {
	var b strings.Builder
	switch j.Type {
	case parser.CrossApplyJoin:
		fmt.Fprintf(&b, " CROSS JOIN LATERAL %s", translateTableRef(j.Table))
	case parser.OuterApplyJoin:
		fmt.Fprintf(&b, " LEFT JOIN LATERAL %s ON true", translateTableRef(j.Table))
	default:
		fmt.Fprintf(&b, " %s %s", j.Type, translateTableRef(j.Table))
		if j.Condition != nil {
			fmt.Fprintf(&b, " ON %s", translateExpr(j.Condition))
		}
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
// [Both] Multi-table DELETE (DELETE t FROM t JOIN s) is supported by both
// SQL Server and Sybase ASE, translated to DELETE FROM t USING s WHERE ...
// [SQL Server] OUTPUT → RETURNING (SQL Server-specific).
func translateDelete(s *parser.DeleteStmt) string {
	var b strings.Builder
	if len(s.From) > 0 {
		// Multi-table DELETE → DELETE FROM <target> [AS alias] USING <other_tables>.
		// s.Table may be an alias (e.g. DELETE d FROM dst_tbl d JOIN ...).
		// Resolve it to the actual table name from the FROM refs.
		targetName := s.Table
		targetAlias := ""
		for _, ref := range s.From {
			if ref.Alias == s.Table || ref.Name == s.Table {
				targetName = ref.Name
				if ref.Alias != "" && ref.Alias != ref.Name {
					targetAlias = ref.Alias
				}
				break
			}
		}
		fmt.Fprintf(&b, "DELETE FROM %s", quoteIdent(targetName))
		if targetAlias != "" {
			fmt.Fprintf(&b, " AS %s", quoteIdent(targetAlias))
		}
		var usingParts []string
		for _, ref := range s.From {
			if ref.Name == targetName {
				continue
			}
			usingParts = append(usingParts, translateTableRef(ref))
		}
		for _, j := range s.Joins {
			usingParts = append(usingParts, translateTableRef(j.Table))
		}
		if len(usingParts) > 0 {
			fmt.Fprintf(&b, " USING %s", strings.Join(usingParts, ", "))
		}
		// Merge JOIN ON conditions and WHERE into a single WHERE clause.
		var conditions []string
		for _, j := range s.Joins {
			if j.Condition != nil {
				conditions = append(conditions, translateExpr(j.Condition))
			}
		}
		if s.Where != nil {
			conditions = append(conditions, translateExpr(s.Where))
		}
		if len(conditions) > 0 {
			fmt.Fprintf(&b, " WHERE %s", strings.Join(conditions, " AND "))
		}
	} else {
		fmt.Fprintf(&b, "DELETE FROM %s", quoteIdent(s.Table))
		if s.Where != nil {
			fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
		}
	}
	if len(s.Output) > 0 {
		b.WriteString(" RETURNING ")
		b.WriteString(translateOutputColumns(s.Output))
	}
	return b.String()
}

// translateUpdate converts a T-SQL UPDATE to CRDB syntax.
// [Both] UPDATE...FROM (multi-table update) is supported by both dialects.
// [SQL Server] OUTPUT → RETURNING (SQL Server-specific).
func translateUpdate(s *parser.UpdateStmt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", quoteIdent(s.Table))
	for i, a := range s.Assignments {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s = %s",
			quoteColumnRef(a.Column), translateExpr(a.Value))
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
			b.WriteString(translateJoinClause(j))
		}
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Where))
	}
	if len(s.Output) > 0 {
		b.WriteString(" RETURNING ")
		b.WriteString(translateOutputColumns(s.Output))
	}
	return b.String()
}

// translateBeginTran converts BEGIN TRAN[SACTION] [name] to CRDB BEGIN.
// [Both] Transaction statements are supported by both SQL Server and Sybase
// ASE. T-SQL transaction names have no equivalent in CRDB; they are ignored.
func translateBeginTran(s *parser.BeginTranStmt) string {
	return "BEGIN"
}

// translateRollbackTran converts ROLLBACK [TRAN[SACTION]] [name].
// If a name is specified, it's treated as a savepoint rollback.
func translateRollbackTran(s *parser.RollbackTranStmt) string {
	if s.Name != "" {
		return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", quoteIdent(s.Name))
	}
	return "ROLLBACK"
}

// Expr translates a T-SQL expression into a CockroachDB-compatible SQL
// expression string. This is exported for use by the executor when it
// needs to evaluate expressions for control flow.
func Expr(expr parser.Expr) string {
	return translateExpr(expr)
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

	case *parser.CastExpr:
		return translateCast(e)

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

// translateAnyAll converts expr op ANY/ALL/SOME (SELECT ...).
// [Both] ANY/ALL/SOME are supported by both SQL Server and Sybase ASE.
// SOME is translated to ANY since CockroachDB uses the standard SQL keyword.
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
		// T-SQL @variables are left unquoted so the executor's variable
		// substitution can replace them with their current value.
		if strings.HasPrefix(name, "@") {
			return name
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

// translateFuncCall translates T-SQL function calls to their CRDB
// equivalents. Each case is annotated with [Both], [SQL Server], or
// [Sybase ASE] to indicate which dialect(s) support the function.
func translateFuncCall(e *parser.FuncCallExpr) string {
	name := strings.ToUpper(e.Name)

	switch name {
	case "ISNULL":
		// [Both] ISNULL(a, b) → COALESCE(a, b)
		args := translateArgs(e.Args)
		return fmt.Sprintf("COALESCE(%s)", strings.Join(args, ", "))

	case "GETDATE":
		// [Both] GETDATE() → now()
		return "now()"

	// --- String functions ---

	case "LEN":
		// [Both] LEN(s) → length(s)
		args := translateArgs(e.Args)
		return fmt.Sprintf("length(%s)", strings.Join(args, ", "))

	case "DATALENGTH":
		// [Both] DATALENGTH(expr) → octet_length(expr)
		// Returns the number of bytes used to represent the expression.
		args := translateArgs(e.Args)
		return fmt.Sprintf("octet_length(%s)", strings.Join(args, ", "))

	case "CHARINDEX":
		// [Both] CHARINDEX(substr, str) → strpos(str, substr)
		// Note: argument order is swapped.
		if len(e.Args) >= 2 {
			substr := translateExpr(e.Args[0])
			str := translateExpr(e.Args[1])
			return fmt.Sprintf("strpos(%s, %s)", str, substr)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("strpos(%s)", strings.Join(args, ", "))

	case "PATINDEX":
		// [SQL Server] PATINDEX(pattern, str) → approximate via strpos (no
		// direct equivalent). Sybase ASE uses PATINDEX too but with different
		// pattern semantics. This is a lossy translation for the common case.
		if len(e.Args) >= 2 {
			str := translateExpr(e.Args[1])
			pattern := translateExpr(e.Args[0])
			return fmt.Sprintf("strpos(%s, %s) /* PATINDEX approximation */", str, pattern)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("strpos(%s)", strings.Join(args, ", "))

	case "STUFF":
		// [Both] STUFF(str, start, length, insert) → overlay(str placing insert from start for length)
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
		// [Both] REPLICATE(str, n) → repeat(str, n)
		args := translateArgs(e.Args)
		return fmt.Sprintf("repeat(%s)", strings.Join(args, ", "))

	case "SPACE":
		// [Both] SPACE(n) → repeat(' ', n)
		if len(e.Args) == 1 {
			n := translateExpr(e.Args[0])
			return fmt.Sprintf("repeat(' ', %s)", n)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("repeat(' ', %s)", strings.Join(args, ", "))

	case "STRING_AGG":
		// [SQL Server] STRING_AGG(expr, separator) — SQL Server 2017+.
		args := translateArgs(e.Args)
		return fmt.Sprintf("string_agg(%s)", strings.Join(args, ", "))

	case "LIST":
		// [Sybase ASE] LIST(expr [, separator]) — Sybase ASE equivalent of
		// STRING_AGG. Default separator is comma when omitted.
		if len(e.Args) >= 2 {
			expr := translateExpr(e.Args[0])
			sep := translateExpr(e.Args[1])
			return fmt.Sprintf("string_agg(%s, %s)", expr, sep)
		}
		if len(e.Args) == 1 {
			expr := translateExpr(e.Args[0])
			return fmt.Sprintf("string_agg(%s, ',')", expr)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("string_agg(%s)", strings.Join(args, ", "))

	case "QUOTENAME":
		// [SQL Server] QUOTENAME(str) → quote_ident(str)
		args := translateArgs(e.Args)
		return fmt.Sprintf("quote_ident(%s)", strings.Join(args, ", "))

	// --- Date/time functions ---

	case "DATEADD":
		// [Both] DATEADD(part, n, date) → (date::TIMESTAMPTZ + n * INTERVAL '1 part')
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
		// [Both] DATEDIFF(part, start, end) → extract(epoch FROM end - start)
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
		// [Both] DATEPART(part, date) → extract(part FROM date::TIMESTAMPTZ)
		if len(e.Args) == 2 {
			part := identName(e.Args[0])
			date := translateExpr(e.Args[1])
			return fmt.Sprintf("EXTRACT(%s FROM %s::TIMESTAMPTZ)::INT",
				mapExtractPart(part), date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DATEPART(%s)", strings.Join(args, ", "))

	case "DATENAME":
		// [Both] DATENAME(part, date) → to_char(date::TIMESTAMPTZ, format)
		if len(e.Args) == 2 {
			part := identName(e.Args[0])
			date := translateExpr(e.Args[1])
			format := mapDatenamePart(part)
			return fmt.Sprintf("btrim(to_char(%s::TIMESTAMPTZ, '%s'))", date, format)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DATENAME(%s)", strings.Join(args, ", "))

	case "YEAR":
		// [Both] YEAR(date) → EXTRACT(year FROM date::TIMESTAMPTZ)::INT
		if len(e.Args) == 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf("EXTRACT(year FROM %s::TIMESTAMPTZ)::INT", date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("YEAR(%s)", strings.Join(args, ", "))

	case "MONTH":
		// [Both] MONTH(date) → EXTRACT(month FROM date::TIMESTAMPTZ)::INT
		if len(e.Args) == 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf("EXTRACT(month FROM %s::TIMESTAMPTZ)::INT", date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("MONTH(%s)", strings.Join(args, ", "))

	case "DAY":
		// [Both] DAY(date) → EXTRACT(day FROM date::TIMESTAMPTZ)::INT
		if len(e.Args) == 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf("EXTRACT(day FROM %s::TIMESTAMPTZ)::INT", date)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("DAY(%s)", strings.Join(args, ", "))

	case "SYSDATETIME":
		// [SQL Server] SYSDATETIME() — SQL Server 2008+ high-precision now().
		return "now()"

	case "GETUTCDATE":
		// [SQL Server] GETUTCDATE() — SQL Server-specific UTC timestamp.
		return "(now() AT TIME ZONE 'UTC')"

	case "EOMONTH":
		// [SQL Server] EOMONTH(date) — SQL Server 2012+ end-of-month.
		// EOMONTH(date) → (date_trunc('month', date::TIMESTAMPTZ) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
		if len(e.Args) >= 1 {
			date := translateExpr(e.Args[0])
			return fmt.Sprintf(
				"(date_trunc('month', %s::TIMESTAMPTZ) + INTERVAL '1 month' - INTERVAL '1 day')::DATE",
				date)
		}
		return "EOMONTH()"

	case "ISDATE":
		// [SQL Server] ISDATE() — no direct equivalent; use a try_cast approach.
		if len(e.Args) == 1 {
			arg := translateExpr(e.Args[0])
			return fmt.Sprintf(
				"CASE WHEN try_cast(%s AS TIMESTAMPTZ) IS NOT NULL THEN 1 ELSE 0 END", arg)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("ISDATE(%s)", strings.Join(args, ", "))

	case "FORMAT":
		// [SQL Server] FORMAT(value, format_string) — SQL Server 2012+.
		// No direct CRDB equivalent; pass through as to_char for basic cases.
		if len(e.Args) >= 2 {
			val := translateExpr(e.Args[0])
			format := translateExpr(e.Args[1])
			return fmt.Sprintf("to_char(%s, %s)", val, format)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("FORMAT(%s)", strings.Join(args, ", "))

	// --- Math functions ---

	case "SQUARE":
		// [SQL Server] SQUARE(x) → power(x, 2). Sybase ASE uses power() directly.
		if len(e.Args) == 1 {
			arg := translateExpr(e.Args[0])
			return fmt.Sprintf("power(%s, 2)", arg)
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("power(%s, 2)", strings.Join(args, ", "))

	case "LOG":
		// [Both] T-SQL LOG(x) is natural log; CRDB log(x) is base-10.
		// Translate to ln(x).
		args := translateArgs(e.Args)
		return fmt.Sprintf("ln(%s)", strings.Join(args, ", "))

	case "LOG10":
		// [Both] LOG10(x) → log(x) in CRDB (which is base-10).
		args := translateArgs(e.Args)
		return fmt.Sprintf("log(%s)", strings.Join(args, ", "))

	case "RAND":
		// [Both] RAND() → random()
		return "random()"

	// --- Conditional functions ---

	case "IIF":
		// [SQL Server] IIF(cond, true_val, false_val) — SQL Server 2012+.
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
		// [SQL Server] CHOOSE(idx, val1, val2, ...) — SQL Server 2012+.
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
		// [SQL Server] TRY_CONVERT(type, expr) — SQL Server 2012+.
		// CockroachDB does not support try_cast, so we fall back to CAST.
		// Failed conversions produce an error instead of returning NULL.
		if len(e.Args) >= 2 {
			typeName := identName(e.Args[0])
			if typeName == "" {
				typeName = translateExpr(e.Args[0])
			}
			expr := translateExpr(e.Args[1])
			return fmt.Sprintf("CAST(%s AS %s)", expr, mapDataType(strings.ToUpper(typeName)))
		}
		args := translateArgs(e.Args)
		return fmt.Sprintf("TRY_CONVERT(%s)", strings.Join(args, ", "))

	// --- System functions ---

	case "NEWID":
		// [SQL Server] NEWID() → gen_random_uuid()
		return "gen_random_uuid()"

	case "OBJECT_ID":
		// [SQL Server] OBJECT_ID() — no direct equivalent; return NULL.
		args := translateArgs(e.Args)
		return fmt.Sprintf("NULL /* OBJECT_ID(%s) not supported */",
			strings.Join(args, ", "))

	case "DB_NAME":
		// [SQL Server] DB_NAME() → current_database()
		return "current_database()"

	case "SCHEMA_NAME":
		// [SQL Server] SCHEMA_NAME() → current_schema()
		return "current_schema()"

	case "USER_NAME":
		// [SQL Server] USER_NAME() → current_user
		return "current_user"

	case "HOST_NAME":
		// [SQL Server] HOST_NAME() — no CRDB equivalent.
		return "NULL /* HOST_NAME() not supported */"

	case "APP_NAME":
		// [SQL Server] APP_NAME() → current_setting('application_name')
		return "current_setting('application_name')"

	// --- Aggregate functions ---

	case "COUNT_BIG":
		// [SQL Server] COUNT_BIG(*) → count(*) — CRDB count already returns INT8.
		args := translateArgs(e.Args)
		return fmt.Sprintf("count(%s)", strings.Join(args, ", "))

	case "STDEV":
		// [SQL Server] STDEV → stddev (Sybase ASE uses stddev directly).
		args := translateArgs(e.Args)
		return fmt.Sprintf("stddev(%s)", strings.Join(args, ", "))

	case "STDEVP":
		// [SQL Server] STDEVP → stddev_pop
		args := translateArgs(e.Args)
		return fmt.Sprintf("stddev_pop(%s)", strings.Join(args, ", "))

	case "VAR":
		// [SQL Server] VAR → variance
		args := translateArgs(e.Args)
		return fmt.Sprintf("variance(%s)", strings.Join(args, ", "))

	case "VARP":
		// [SQL Server] VARP → var_pop
		args := translateArgs(e.Args)
		return fmt.Sprintf("var_pop(%s)", strings.Join(args, ", "))

	case "SCOPE_IDENTITY":
		// [Both] SCOPE_IDENTITY() returns the last identity value inserted
		// in the current scope. In CockroachDB's TDS layer, this is
		// equivalent to @@IDENTITY since there are no nested scopes
		// (no stored procedures or triggers).
		return "@@IDENTITY"

	case "CHECKSUM_AGG":
		// [SQL Server] CHECKSUM_AGG — no CRDB equivalent.
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
// [Both] CONVERT is supported by both SQL Server and Sybase ASE. The style
// parameter (third argument) is SQL Server-specific and is dropped since
// CRDB's CAST does not support it.
func translateConvert(e *parser.ConvertExpr) string {
	expr := translateExpr(e.Expr)
	crdbType := mapDataType(e.DataType)
	return fmt.Sprintf("CAST(%s AS %s)", expr, crdbType)
}

// translateCast converts CAST(expr AS type) and TRY_CAST(expr AS type) to
// CAST(expr AS type) with type mapping. CockroachDB does not support
// try_cast syntax, so TRY_CAST falls back to CAST. This means failed
// conversions produce an error instead of returning NULL.
func translateCast(e *parser.CastExpr) string {
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
		b.WriteString("ADD COLUMN ")
		translateColumnDef(&b, cmd.Column)
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
// [SQL Server] INCLUDE clause is SQL Server-specific (mapped to
// CockroachDB's STORING clause). Sybase ASE does not support INCLUDE.
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
// [SQL Server] DROP INDEX idx ON tbl → CRDB: DROP INDEX tbl@idx.
// Sybase ASE uses DROP INDEX tbl.idx syntax (not yet supported).
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
// [Both] @@ROWCOUNT, @@IDENTITY, @@VERSION, and @@TRANCOUNT are supported
// by both SQL Server and Sybase ASE. [Both] @@SPID, @@SERVERNAME,
// @@LANGUAGE, and @@MAX_CONNECTIONS are also supported.
func translateSystemVariable(name string) string {
	switch strings.ToUpper(name) {
	case "@@ROWCOUNT":
		// [Both] @@ROWCOUNT tracks the number of rows affected by the last
		// DML statement. The executor tracks this state and substitutes the
		// value before execution (same pattern as @@TRANCOUNT).
		return "@@ROWCOUNT"
	case "@@IDENTITY":
		// [Both] @@IDENTITY returns the last identity value inserted in
		// the current session. The executor tracks this state and
		// substitutes the value before execution (same pattern as
		// @@ROWCOUNT and @@TRANCOUNT).
		return "@@IDENTITY"
	case "@@VERSION":
		return "version()"
	case "@@TRANCOUNT":
		// [Both] @@TRANCOUNT tracks transaction nesting depth. CRDB doesn't
		// support nested transactions. The executor tracks this state and
		// substitutes the value before execution.
		return "@@TRANCOUNT"
	case "@@SPID":
		// [Both] @@SPID returns the server process ID for the current
		// connection. The executor tracks this per-connection state and
		// substitutes the value before execution.
		return "@@SPID"
	case "@@SERVERNAME":
		// [Both] @@SERVERNAME returns the name of the local server. The
		// executor substitutes the server's hostname before execution.
		return "@@SERVERNAME"
	case "@@LANGUAGE":
		// [Both] @@LANGUAGE returns the current language name.
		// CockroachDB always uses us_english.
		return "'us_english'"
	case "@@MAX_CONNECTIONS":
		// [Both] @@MAX_CONNECTIONS returns the maximum number of
		// simultaneous user connections allowed. Returns 32767
		// (matching the SQL Server default).
		return "32767"
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

// inferExprType performs best-effort type inference on a T-SQL expression,
// returning a CockroachDB type name suitable for computed column definitions.
// T-SQL computed columns never declare a type (it is always inferred), but
// CockroachDB requires one. The declared type only needs to be compatible
// with the actual expression result — CockroachDB validates at DDL time.
func inferExprType(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.IntLit:
		return "INT8"
	case *parser.FloatLit:
		return "FLOAT8"
	case *parser.StringLit:
		return "STRING"
	case *parser.CastExpr:
		return mapDataType(e.DataType)
	case *parser.ConvertExpr:
		return mapDataType(e.DataType)
	case *parser.BinaryExpr:
		switch e.Op {
		case "||":
			return "STRING"
		case "+", "-", "*", "/", "%":
			// If either operand is a float, the result is float.
			if inferExprType(e.Left) == "FLOAT8" ||
				inferExprType(e.Right) == "FLOAT8" {
				return "FLOAT8"
			}
			return "INT8"
		default:
			return "INT8"
		}
	case *parser.FuncCallExpr:
		switch strings.ToUpper(e.Name) {
		case "CONCAT", "UPPER", "LOWER", "LTRIM", "RTRIM", "LEFT",
			"RIGHT", "SUBSTRING", "REPLACE", "STUFF", "REVERSE",
			"SPACE", "REPLICATE", "STR", "CHAR", "NCHAR",
			"FORMAT", "STRING_AGG":
			return "STRING"
		case "GETDATE", "SYSDATETIME", "CURRENT_TIMESTAMP",
			"DATEADD", "EOMONTH":
			return "TIMESTAMP"
		default:
			return "INT8"
		}
	case *parser.UnaryExpr:
		return inferExprType(e.Expr)
	default:
		// Column references and other expressions default to INT8.
		// CockroachDB validates the declared type against the actual
		// expression result at DDL time and rejects mismatches.
		return "INT8"
	}
}

// mapDataType converts a T-SQL data type name to its CockroachDB equivalent.
// Each type is annotated with [Both], [SQL Server], or [Sybase ASE] to
// indicate which dialect(s) support it. The input is expected to be
// upper-cased (as produced by the parser's parseDataType).
func mapDataType(dt string) string {
	// Split into type name and optional arguments.
	name, args := splitTypeArgs(dt)

	switch name {
	// [Both] Integer types — supported by both SQL Server and Sybase ASE.
	case "TINYINT":
		return "INT2" // CockroachDB has no unsigned integer; INT2 is the closest.
	case "SMALLINT":
		return "INT2"
	case "INT", "INTEGER":
		return "INT4"
	case "BIGINT":
		return "INT8"

	// [Sybase ASE] Unsigned integer types. CockroachDB has no unsigned
	// integers, so we map to the smallest signed type that covers the full
	// unsigned range. SQL Server does not support UNSIGNED integer types.
	case "UNSIGNED TINYINT":
		return "INT2" // unsigned 8-bit (0–255) fits in signed 16-bit
	case "UNSIGNED SMALLINT":
		return "INT4" // unsigned 16-bit (0–65535) fits in signed 32-bit
	case "UNSIGNED INT", "UNSIGNED INTEGER":
		return "INT8" // unsigned 32-bit fits in signed 64-bit
	case "UNSIGNED BIGINT":
		return "DECIMAL(20, 0)" // unsigned 64-bit (0–18446744073709551615) exceeds INT8 range

	// [Both] Float types.
	case "REAL":
		return "FLOAT4"
	case "FLOAT":
		return "FLOAT8"

	// [Both] Bit type. T-SQL BIT is semantically a 1-bit integer (0, 1,
	// or NULL), not a boolean. Mapping to INT2 preserves integer semantics
	// so that literal 0/1 inserts and arithmetic expressions work without
	// explicit casts.
	case "BIT":
		return "INT2"

	// [Both] Character types. CockroachDB uses STRING / VARCHAR.
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
		// [Both] CRDB treats all strings as UTF-8; NCHAR maps to CHAR.
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

	// [Sybase ASE] Unicode character types. CockroachDB stores all strings
	// as UTF-8, so UNICHAR/UNIVARCHAR map to CHAR/VARCHAR and UNITEXT to
	// STRING. SQL Server uses NCHAR/NVARCHAR for Unicode instead.
	case "UNICHAR":
		if args != "" {
			return fmt.Sprintf("CHAR(%s)", args)
		}
		return "CHAR"
	case "UNIVARCHAR":
		if args != "" {
			return fmt.Sprintf("VARCHAR(%s)", args)
		}
		return "VARCHAR"
	case "UNITEXT":
		return "STRING"

	// [Both] Binary types.
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
	case "DATETIME":
		// [Both] DATETIME — supported by both SQL Server and Sybase ASE.
		return "TIMESTAMP"
	case "DATETIME2":
		// [SQL Server] DATETIME2 — SQL Server 2008+ high-precision datetime.
		// Sybase ASE has no DATETIME2; it uses BIGDATETIME instead.
		return "TIMESTAMP"
	case "SMALLDATETIME":
		// [Both] SMALLDATETIME — supported by both dialects.
		return "TIMESTAMP"
	case "DATE":
		// [Both] DATE — supported by both dialects.
		return "DATE"
	case "TIME":
		// [Both] TIME — supported by both dialects.
		return "TIME"
	case "DATETIMEOFFSET":
		// [SQL Server] DATETIMEOFFSET — SQL Server 2008+ timezone-aware
		// datetime. Sybase ASE has no equivalent.
		return "TIMESTAMPTZ"

	// [Sybase ASE] Extended datetime types with microsecond precision
	// (Sybase ASE 15.5+). SQL Server uses DATETIME2 for high precision.
	case "BIGDATETIME":
		return "TIMESTAMP"
	case "BIGTIME":
		return "TIME"

	// [Both] Money types → DECIMAL with fixed precision/scale.
	case "MONEY":
		return "DECIMAL(19, 4)"
	case "SMALLMONEY":
		return "DECIMAL(10, 4)"

	// [Both] Numeric/Decimal — pass through with args.
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

	// [Both] GUID → UUID.
	case "UNIQUEIDENTIFIER":
		return "UUID"

	// [SQL Server] Timestamp (rowversion in SQL Server) → BYTES.
	// SQL Server TIMESTAMP is a binary counter, not a date/time. It's
	// 8 bytes, used for optimistic concurrency. ROWVERSION is the modern
	// alias. Sybase ASE TIMESTAMP is also a binary counter but with a
	// different internal format.
	case "TIMESTAMP", "ROWVERSION":
		return "BYTES"

	default:
		// Unknown types are passed through unchanged.
		if args != "" {
			return fmt.Sprintf("%s(%s)", name, args)
		}
		return name
	}
}

// translateMerge converts a T-SQL MERGE INTO ... USING ... ON ...
// to CockroachDB's INSERT ... ON CONFLICT DO UPDATE SET ... syntax.
// [SQL Server] MERGE is SQL Server 2008+ (and ANSI SQL:2003). Sybase ASE
// does not support MERGE; it uses separate INSERT/UPDATE/DELETE statements
// instead. This is a best-effort translation for common MERGE patterns.
func translateMerge(s *parser.MergeStmt) (string, error) {
	var b strings.Builder

	// Build: INSERT INTO <target> (<columns>) SELECT <values> FROM <source>
	// ON CONFLICT (<conflict_cols>) DO UPDATE SET ...
	targetName := s.Target.Name
	if s.Target.Alias != "" {
		targetName = s.Target.Name
	}

	if s.NotMatched != nil {
		fmt.Fprintf(&b, "INSERT INTO %s", quoteIdent(targetName))
		if len(s.NotMatched.Columns) > 0 {
			b.WriteString(" (")
			for i, col := range s.NotMatched.Columns {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(quoteIdent(col))
			}
			b.WriteString(")")
		}
		// Use a SELECT from the source with the VALUES expressions.
		b.WriteString(" SELECT ")
		for i, val := range s.NotMatched.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(translateExpr(val))
		}
		fmt.Fprintf(&b, " FROM %s", translateTableRef(s.Source))
		// WHERE NOT EXISTS for rows not already matched, but ON CONFLICT
		// handles this. Extract conflict columns from the ON condition.
		conflictCols := extractEqualityColumns(s.Condition, s.Target)
		if len(conflictCols) > 0 {
			b.WriteString(" ON CONFLICT (")
			for i, col := range conflictCols {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(quoteIdent(col))
			}
			b.WriteString(")")
		}
		if s.Matched != nil && !s.Matched.Delete {
			b.WriteString(" DO UPDATE SET ")
			for i, a := range s.Matched.Assignments {
				if i > 0 {
					b.WriteString(", ")
				}
				col := stripTablePrefix(a.Column)
				val := translateExpr(a.Value)
				// Replace source table references with excluded.
				val = replaceSourceRef(val, s.Source)
				fmt.Fprintf(&b, "%s = %s", quoteIdent(col), val)
			}
		} else if s.Matched != nil && s.Matched.Delete {
			// MERGE with DELETE on match doesn't map cleanly to INSERT ON
			// CONFLICT. Emit a comment.
			b.WriteString(" DO NOTHING /* WHEN MATCHED THEN DELETE not supported */")
		} else {
			b.WriteString(" DO NOTHING")
		}
	} else if s.Matched != nil {
		// MERGE with only WHEN MATCHED (no INSERT) — this is an UPDATE
		// against matching rows from the source.
		fmt.Fprintf(&b, "UPDATE %s SET ", quoteIdent(targetName))
		for i, a := range s.Matched.Assignments {
			if i > 0 {
				b.WriteString(", ")
			}
			col := stripTablePrefix(a.Column)
			fmt.Fprintf(&b, "%s = %s",
				quoteIdent(col), translateExpr(a.Value))
		}
		fmt.Fprintf(&b, " FROM %s", translateTableRef(s.Source))
		fmt.Fprintf(&b, " WHERE %s", translateExpr(s.Condition))
	}
	if len(s.Output) > 0 {
		b.WriteString(" RETURNING ")
		b.WriteString(translateOutputColumns(s.Output))
	}
	return b.String(), nil
}

// translateOutputColumns converts OUTPUT clause columns (which may reference
// inserted.*/deleted.*) to RETURNING column expressions by stripping the
// inserted./deleted. prefixes. [SQL Server] The OUTPUT clause with
// inserted/deleted pseudo-tables is SQL Server-specific.
func translateOutputColumns(cols []parser.SelectColumn) string {
	var parts []string
	for _, col := range cols {
		expr := translateExpr(col.Expr)
		// Strip inserted./deleted. prefixes — in CockroachDB RETURNING,
		// the columns refer to the row directly.
		expr = stripOutputPrefix(expr)
		if col.Alias != "" {
			parts = append(parts, fmt.Sprintf("%s AS %s",
				expr, quoteIdent(col.Alias)))
		} else {
			parts = append(parts, expr)
		}
	}
	return strings.Join(parts, ", ")
}

// stripOutputPrefix removes "inserted." and "deleted." prefixes from
// OUTPUT column references for RETURNING translation.
func stripOutputPrefix(expr string) string {
	for _, prefix := range []string{
		"inserted.", "INSERTED.", "deleted.", "DELETED.",
	} {
		if strings.HasPrefix(expr, prefix) {
			return expr[len(prefix):]
		}
	}
	return expr
}

// quoteColumnRef quotes a possibly dotted column reference, quoting each
// part individually.
func quoteColumnRef(col string) string {
	if strings.Contains(col, ".") {
		parts := strings.Split(col, ".")
		var quoted []string
		for _, p := range parts {
			quoted = append(quoted, quoteIdent(p))
		}
		return strings.Join(quoted, ".")
	}
	return quoteIdent(col)
}

// extractEqualityColumns extracts target table column names from equality
// conditions in a MERGE ON clause. For example, given
// target.id = source.id AND target.name = source.name, it returns
// ["id", "name"].
func extractEqualityColumns(expr parser.Expr, target parser.TableRef) []string {
	targetPrefix := target.Alias
	if targetPrefix == "" {
		targetPrefix = target.Name
	}
	var cols []string
	extractFromExpr(expr, targetPrefix, &cols)
	return cols
}

func extractFromExpr(expr parser.Expr, targetPrefix string, cols *[]string) {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		if e.Op == "AND" {
			extractFromExpr(e.Left, targetPrefix, cols)
			extractFromExpr(e.Right, targetPrefix, cols)
			return
		}
		if e.Op == "=" {
			if col := extractTargetColumn(e.Left, targetPrefix); col != "" {
				*cols = append(*cols, col)
			} else if col := extractTargetColumn(e.Right, targetPrefix); col != "" {
				*cols = append(*cols, col)
			}
		}
	}
}

func extractTargetColumn(expr parser.Expr, targetPrefix string) string {
	id, ok := expr.(*parser.IdentExpr)
	if !ok || len(id.Parts) != 2 {
		return ""
	}
	if strings.EqualFold(id.Parts[0], targetPrefix) {
		return id.Parts[1]
	}
	return ""
}

// stripTablePrefix removes the table qualifier from a dotted column reference
// (e.g. "t.name" → "name").
func stripTablePrefix(col string) string {
	if idx := strings.LastIndexByte(col, '.'); idx >= 0 {
		return col[idx+1:]
	}
	return col
}

// replaceSourceRef replaces source table references with "excluded." in
// MERGE → INSERT ON CONFLICT translations. The source's alias (or name)
// is replaced with "excluded" since ON CONFLICT uses the excluded pseudo-table
// to reference the conflicting row's values.
func replaceSourceRef(expr string, source parser.TableRef) string {
	sourcePrefix := source.Alias
	if sourcePrefix == "" {
		sourcePrefix = source.Name
	}
	// Replace quoted and unquoted references.
	expr = strings.ReplaceAll(expr, sourcePrefix+".", "excluded.")
	return expr
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
