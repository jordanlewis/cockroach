// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"fmt"
	"strings"
)

// Statement is the interface implemented by all T-SQL AST statement nodes.
type Statement interface {
	fmt.Stringer
	statementNode()
}

// Expr is the interface implemented by all T-SQL AST expression nodes.
type Expr interface {
	fmt.Stringer
	exprNode()
}

// Batch represents a sequence of T-SQL statements separated by GO or
// semicolons. In T-SQL, GO is a batch separator that causes the preceding
// statements to be sent to the server as a unit.
type Batch struct {
	Stmts []Statement
}

func (b *Batch) String() string {
	var parts []string
	for _, s := range b.Stmts {
		parts = append(parts, s.String())
	}
	return strings.Join(parts, ";\n")
}

// UseStmt represents USE <database>.
type UseStmt struct {
	Database string
}

func (*UseStmt) statementNode() {}

func (s *UseStmt) String() string {
	return fmt.Sprintf("USE %s", formatIdent(s.Database))
}

// ColumnDef represents a column definition in a CREATE TABLE statement.
// A column is either a regular column (with DataType) or a computed column
// (with ComputedExpr and no DataType).
type ColumnDef struct {
	Name         string
	DataType     string
	Nullable     *bool        // nil = unspecified, true = NULL, false = NOT NULL
	Identity     *IdentityDef // IDENTITY(seed, increment)
	DefaultExpr  Expr         // DEFAULT <expr>
	ComputedExpr Expr         // AS <expr> (computed column, no DataType)
}

// IdentityDef represents the IDENTITY(seed, increment) clause on a column.
type IdentityDef struct {
	Seed      int64
	Increment int64
}

func (c *ColumnDef) String() string {
	if c.ComputedExpr != nil {
		return fmt.Sprintf("%s AS %s", formatIdent(c.Name), c.ComputedExpr)
	}
	s := fmt.Sprintf("%s %s", formatIdent(c.Name), c.DataType)
	if c.Identity != nil {
		s += fmt.Sprintf(" IDENTITY(%d, %d)", c.Identity.Seed, c.Identity.Increment)
	}
	if c.DefaultExpr != nil {
		s += fmt.Sprintf(" DEFAULT %s", c.DefaultExpr)
	}
	if c.Nullable != nil {
		if *c.Nullable {
			s += " NULL"
		} else {
			s += " NOT NULL"
		}
	}
	return s
}

// CreateDatabaseStmt represents CREATE DATABASE <name>.
type CreateDatabaseStmt struct {
	Database string
}

func (*CreateDatabaseStmt) statementNode() {}

func (s *CreateDatabaseStmt) String() string {
	return fmt.Sprintf("CREATE DATABASE %s", formatIdent(s.Database))
}

// DropTableStmt represents DROP TABLE [IF EXISTS] <name>.
type DropTableStmt struct {
	Table    string
	IfExists bool
}

func (*DropTableStmt) statementNode() {}

func (s *DropTableStmt) String() string {
	if s.IfExists {
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", formatIdent(s.Table))
	}
	return fmt.Sprintf("DROP TABLE %s", formatIdent(s.Table))
}

// DropDatabaseStmt represents DROP DATABASE <name>.
type DropDatabaseStmt struct {
	Database string
}

func (*DropDatabaseStmt) statementNode() {}

func (s *DropDatabaseStmt) String() string {
	return fmt.Sprintf("DROP DATABASE %s", formatIdent(s.Database))
}

// CreateTableStmt represents CREATE TABLE <name> (<columns>).
type CreateTableStmt struct {
	Table   string
	Columns []ColumnDef
}

func (*CreateTableStmt) statementNode() {}

func (s *CreateTableStmt) String() string {
	var cols []string
	for _, c := range s.Columns {
		cols = append(cols, c.String())
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", formatIdent(s.Table), strings.Join(cols, ", "))
}

// InsertStmt represents INSERT INTO <table> [(<columns>)] VALUES (<values>), ...
// or INSERT INTO <table> [(<columns>)] SELECT ...
// [SQL Server] An optional OUTPUT clause maps to RETURNING in CockroachDB.
// Sybase ASE does not support OUTPUT on INSERT.
type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]Expr       // non-nil for VALUES inserts
	Select  Statement      // non-nil for INSERT...SELECT
	Output  []SelectColumn // [SQL Server] OUTPUT clause
}

func (*InsertStmt) statementNode() {}

func (s *InsertStmt) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s", formatIdent(s.Table))
	if len(s.Columns) > 0 {
		var cols []string
		for _, c := range s.Columns {
			cols = append(cols, formatIdent(c))
		}
		fmt.Fprintf(&b, " (%s)", strings.Join(cols, ", "))
	}
	if len(s.Output) > 0 {
		b.WriteString(" OUTPUT ")
		for i, o := range s.Output {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.String())
		}
	}
	if s.Select != nil {
		fmt.Fprintf(&b, " %s", s.Select)
	} else {
		b.WriteString(" VALUES ")
		for i, row := range s.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("(")
			for j, v := range row {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(v.String())
			}
			b.WriteString(")")
		}
	}
	return b.String()
}

// DeleteStmt represents DELETE [FROM] <table> [WHERE <expr>].
// [Both] Extended for multi-table DELETE (DELETE t FROM t JOIN s ON ...).
// [SQL Server] OUTPUT clause (Sybase ASE does not support OUTPUT on DELETE).
type DeleteStmt struct {
	Table  string // target table name (or alias for multi-table)
	Where  Expr
	From   []TableRef     // non-empty for multi-table DELETE
	Joins  []JoinClause   // JOINs for multi-table DELETE
	Output []SelectColumn // [SQL Server] OUTPUT clause
}

func (*DeleteStmt) statementNode() {}

func (s *DeleteStmt) String() string {
	var b strings.Builder
	if len(s.From) > 0 {
		// Multi-table DELETE: DELETE <target> FROM <tables> [JOIN ...] [WHERE ...]
		fmt.Fprintf(&b, "DELETE %s FROM ", formatIdent(s.Table))
		for i, ref := range s.From {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(ref.String())
		}
		for _, j := range s.Joins {
			fmt.Fprintf(&b, " %s", j.String())
		}
	} else {
		fmt.Fprintf(&b, "DELETE FROM %s", formatIdent(s.Table))
	}
	if len(s.Output) > 0 {
		b.WriteString(" OUTPUT ")
		for i, o := range s.Output {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.String())
		}
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", s.Where)
	}
	return b.String()
}

// UpdateStmt represents UPDATE <table> SET <assignments> [FROM ...] [WHERE <expr>].
// [Both] Extended for UPDATE...FROM (multi-table UPDATE).
// [SQL Server] OUTPUT clause (Sybase ASE does not support OUTPUT on UPDATE).
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	From        []TableRef   // FROM clause for multi-table UPDATE
	Joins       []JoinClause // JOINs in FROM clause
	Where       Expr
	Output      []SelectColumn // [SQL Server] OUTPUT clause
}

func (*UpdateStmt) statementNode() {}

func (s *UpdateStmt) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", formatIdent(s.Table))
	for i, a := range s.Assignments {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s = %s", formatColumnRef(a.Column), a.Value)
	}
	if len(s.Output) > 0 {
		b.WriteString(" OUTPUT ")
		for i, o := range s.Output {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.String())
		}
	}
	if len(s.From) > 0 {
		b.WriteString(" FROM ")
		for i, ref := range s.From {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(ref.String())
		}
		for _, j := range s.Joins {
			fmt.Fprintf(&b, " %s", j.String())
		}
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", s.Where)
	}
	return b.String()
}

// Assignment represents a column = value pair in an UPDATE SET clause.
type Assignment struct {
	Column string
	Value  Expr
}

// JoinType identifies the kind of JOIN operation.
type JoinType int

const (
	InnerJoin JoinType = iota + 1
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
)

func (j JoinType) String() string {
	switch j {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullJoin:
		return "FULL OUTER JOIN"
	case CrossJoin:
		return "CROSS JOIN"
	default:
		return "JOIN"
	}
}

// JoinClause represents a JOIN in a FROM clause with a table reference,
// join type, and optional ON condition (CROSS JOIN has no condition).
type JoinClause struct {
	Type      JoinType
	Table     TableRef
	Condition Expr // nil for CROSS JOIN
}

func (j *JoinClause) String() string {
	s := fmt.Sprintf("%s %s", j.Type, j.Table.String())
	if j.Condition != nil {
		s += fmt.Sprintf(" ON %s", j.Condition)
	}
	return s
}

// ComputeClause represents a COMPUTE clause:
// COMPUTE <agg>(expr) [, <agg>(expr) ...] [BY col [, col ...]].
// [Sybase ASE] COMPUTE BY is unique to Sybase ASE. SQL Server supported
// COMPUTE BY historically but deprecated it in SQL Server 2012 and removed
// it in later versions. COMPUTE without BY produces a grand total; with
// BY, it produces summary rows after each group (like a control break).
type ComputeClause struct {
	Aggregates []ComputeAgg
	By         []Expr // nil for grand total (COMPUTE without BY)
}

func (c *ComputeClause) String() string {
	var b strings.Builder
	b.WriteString("COMPUTE ")
	for i, agg := range c.Aggregates {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(agg.String())
	}
	if len(c.By) > 0 {
		b.WriteString(" BY ")
		for i, col := range c.By {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(col.String())
		}
	}
	return b.String()
}

// ComputeAgg represents a single aggregate in a COMPUTE clause,
// e.g. SUM(amount) or COUNT(*).
type ComputeAgg struct {
	Func string // aggregate function name: SUM, AVG, COUNT, MAX, MIN
	Arg  Expr   // expression to aggregate
}

func (a *ComputeAgg) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(a.Func), a.Arg)
}

// SelectStmt represents SELECT [DISTINCT] [TOP n] <columns> [FROM <table>]
// [JOIN ...] [WHERE <expr>] [GROUP BY <exprs>] [HAVING <expr>]
// [ORDER BY <exprs>] with two mutually exclusive pagination variants:
//
//   - [SQL Server] OFFSET n ROWS [FETCH NEXT m ROWS ONLY] (SQL Server 2012+)
//   - [Sybase ASE] ROWS LIMIT x [OFFSET y] (Sybase ASE 15.7+)
//
// The RowsLimitSyntax flag distinguishes which variant was parsed. An
// optional trailing COMPUTE clause ([Sybase ASE]) adds summary rows.
type SelectStmt struct {
	Distinct        bool
	Top             *int // [Both] SELECT TOP N
	Columns         []SelectColumn
	From            []TableRef
	Joins           []JoinClause
	Where           Expr
	GroupBy         []Expr
	Having          Expr
	OrderBy         []OrderByExpr
	Offset          *int            // [SQL Server] OFFSET n ROWS, or [Sybase ASE] OFFSET y
	Fetch           *int            // [SQL Server] FETCH NEXT m ROWS ONLY, or [Sybase ASE] LIMIT x
	RowsLimitSyntax bool            // true = [Sybase ASE] ROWS LIMIT syntax; false = [SQL Server] OFFSET-FETCH
	Compute         []ComputeClause // [Sybase ASE] COMPUTE [BY] clauses
}

func (*SelectStmt) statementNode() {}

func (s *SelectStmt) String() string {
	var b strings.Builder
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}
	if s.Top != nil {
		fmt.Fprintf(&b, "TOP %d ", *s.Top)
	}
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.String())
	}
	if len(s.From) > 0 {
		b.WriteString(" FROM ")
		for i, t := range s.From {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(t.String())
		}
		for _, j := range s.Joins {
			fmt.Fprintf(&b, " %s", j.String())
		}
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", s.Where)
	}
	if len(s.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, g := range s.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(g.String())
		}
	}
	if s.Having != nil {
		fmt.Fprintf(&b, " HAVING %s", s.Having)
	}
	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.String())
		}
	}
	if s.RowsLimitSyntax {
		if s.Fetch != nil {
			fmt.Fprintf(&b, " ROWS LIMIT %d", *s.Fetch)
		}
		if s.Offset != nil {
			fmt.Fprintf(&b, " OFFSET %d", *s.Offset)
		}
	} else {
		if s.Offset != nil {
			fmt.Fprintf(&b, " OFFSET %d ROWS", *s.Offset)
		}
		if s.Fetch != nil {
			fmt.Fprintf(&b, " FETCH NEXT %d ROWS ONLY", *s.Fetch)
		}
	}
	for _, c := range s.Compute {
		fmt.Fprintf(&b, " %s", c.String())
	}
	return b.String()
}

// SelectColumn represents a column in a SELECT list, optionally aliased.
type SelectColumn struct {
	Expr  Expr
	Alias string
}

func (c *SelectColumn) String() string {
	if c.Alias != "" {
		return fmt.Sprintf("%s AS %s", c.Expr, formatIdent(c.Alias))
	}
	return c.Expr.String()
}

// TableRef represents a table reference in a FROM clause, optionally aliased.
// When Subquery is non-nil, this is a derived table (inline view).
type TableRef struct {
	Name     string
	Alias    string
	Subquery Statement // non-nil for derived tables: (SELECT ...) alias
}

func (t *TableRef) String() string {
	if t.Subquery != nil {
		if t.Alias != "" {
			return fmt.Sprintf("(%s) %s", t.Subquery, formatIdent(t.Alias))
		}
		return fmt.Sprintf("(%s)", t.Subquery)
	}
	if t.Alias != "" {
		return fmt.Sprintf("%s %s", formatIdent(t.Name), formatIdent(t.Alias))
	}
	return formatIdent(t.Name)
}

// OrderByExpr represents an expression in an ORDER BY clause.
type OrderByExpr struct {
	Expr Expr
	Desc bool
}

func (o *OrderByExpr) String() string {
	if o.Desc {
		return fmt.Sprintf("%s DESC", o.Expr)
	}
	return fmt.Sprintf("%s ASC", o.Expr)
}

// CompoundSelectStmt represents two SELECT-like statements joined by a set
// operation (UNION, UNION ALL, INTERSECT, EXCEPT). [Both] Set operations are
// supported by both SQL Server and Sybase ASE. ORDER BY and pagination
// ([SQL Server] OFFSET-FETCH or [Sybase ASE] ROWS LIMIT) apply to the
// compound result when present.
type CompoundSelectStmt struct {
	Left            Statement // *SelectStmt or *CompoundSelectStmt
	Op              string    // "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
	Right           Statement // *SelectStmt
	OrderBy         []OrderByExpr
	Offset          *int
	Fetch           *int
	RowsLimitSyntax bool // true = [Sybase ASE] ROWS LIMIT; false = [SQL Server] OFFSET-FETCH
}

func (*CompoundSelectStmt) statementNode() {}

func (s *CompoundSelectStmt) String() string {
	var b strings.Builder
	b.WriteString(s.Left.String())
	fmt.Fprintf(&b, " %s ", s.Op)
	b.WriteString(s.Right.String())
	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.String())
		}
	}
	if s.RowsLimitSyntax {
		if s.Fetch != nil {
			fmt.Fprintf(&b, " ROWS LIMIT %d", *s.Fetch)
		}
		if s.Offset != nil {
			fmt.Fprintf(&b, " OFFSET %d", *s.Offset)
		}
	} else {
		if s.Offset != nil {
			fmt.Fprintf(&b, " OFFSET %d ROWS", *s.Offset)
		}
		if s.Fetch != nil {
			fmt.Fprintf(&b, " FETCH NEXT %d ROWS ONLY", *s.Fetch)
		}
	}
	return b.String()
}

// WithStmt represents WITH <cte_defs> <body> where body is a SELECT or
// compound SELECT.
type WithStmt struct {
	CTEs []CTEDef
	Body Statement
}

func (*WithStmt) statementNode() {}

func (s *WithStmt) String() string {
	var b strings.Builder
	b.WriteString("WITH ")
	for i, cte := range s.CTEs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s AS (%s)", formatIdent(cte.Name), cte.Select)
	}
	fmt.Fprintf(&b, " %s", s.Body)
	return b.String()
}

// CTEDef represents a single common table expression: name AS (SELECT ...).
type CTEDef struct {
	Name   string
	Select Statement
}

// IdentExpr represents an identifier (column name, table name, etc.).
// Supports dotted identifiers like "dbo.table.column".
type IdentExpr struct {
	Parts []string
}

func (*IdentExpr) exprNode() {}

func (e *IdentExpr) String() string {
	var formatted []string
	for _, p := range e.Parts {
		formatted = append(formatted, formatIdent(p))
	}
	return strings.Join(formatted, ".")
}

// StarExpr represents the * wildcard in SELECT *.
type StarExpr struct{}

func (*StarExpr) exprNode() {}

func (*StarExpr) String() string { return "*" }

// IntLit represents an integer literal.
type IntLit struct {
	Value int64
}

func (*IntLit) exprNode() {}

func (e *IntLit) String() string {
	return fmt.Sprintf("%d", e.Value)
}

// FloatLit represents a floating-point literal.
type FloatLit struct {
	Value float64
}

func (*FloatLit) exprNode() {}

func (e *FloatLit) String() string {
	return fmt.Sprintf("%g", e.Value)
}

// StringLit represents a string literal ('value').
type StringLit struct {
	Value string
}

func (*StringLit) exprNode() {}

func (e *StringLit) String() string {
	escaped := strings.ReplaceAll(e.Value, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}

// NullLit represents the NULL literal.
type NullLit struct{}

func (*NullLit) exprNode() {}

func (*NullLit) String() string { return "NULL" }

// BinaryExpr represents a binary operation (e.g. a + b, a = b, a AND b).
type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (*BinaryExpr) exprNode() {}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.Left, e.Op, e.Right)
}

// UnaryExpr represents a unary operation (e.g. NOT x, -x).
type UnaryExpr struct {
	Op   string
	Expr Expr
}

func (*UnaryExpr) exprNode() {}

func (e *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", e.Op, e.Expr)
}

// FuncCallExpr represents a function call (e.g. ISNULL(x, y), GETDATE()).
type FuncCallExpr struct {
	Name string
	Args []Expr
}

func (*FuncCallExpr) exprNode() {}

func (e *FuncCallExpr) String() string {
	var args []string
	for _, a := range e.Args {
		args = append(args, a.String())
	}
	return fmt.Sprintf("%s(%s)", strings.ToUpper(e.Name), strings.Join(args, ", "))
}

// ConvertExpr represents CONVERT(<type>, <expr>[, <style>]).
// [Both] CONVERT is supported by both SQL Server and Sybase ASE.
// [SQL Server] The optional third Style argument is SQL Server-specific.
type ConvertExpr struct {
	DataType string
	Expr     Expr
	Style    Expr // [SQL Server] optional style parameter
}

func (*ConvertExpr) exprNode() {}

func (e *ConvertExpr) String() string {
	if e.Style != nil {
		return fmt.Sprintf("CONVERT(%s, %s, %s)", e.DataType, e.Expr, e.Style)
	}
	return fmt.Sprintf("CONVERT(%s, %s)", e.DataType, e.Expr)
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

func (*ParenExpr) exprNode() {}

func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.Expr)
}

// InExpr represents <expr> [NOT] IN (<values>) or <expr> [NOT] IN (<subquery>).
type InExpr struct {
	Expr     Expr
	Values   []Expr    // non-nil for value lists: IN (1, 2, 3)
	Subquery Statement // non-nil for subqueries: IN (SELECT ...)
	Not      bool
}

func (*InExpr) exprNode() {}

func (e *InExpr) String() string {
	op := "IN"
	if e.Not {
		op = "NOT IN"
	}
	if e.Subquery != nil {
		return fmt.Sprintf("%s %s (%s)", e.Expr, op, e.Subquery)
	}
	var vals []string
	for _, v := range e.Values {
		vals = append(vals, v.String())
	}
	return fmt.Sprintf("%s %s (%s)", e.Expr, op, strings.Join(vals, ", "))
}

// BetweenExpr represents <expr> [NOT] BETWEEN <low> AND <high>.
type BetweenExpr struct {
	Expr Expr
	Low  Expr
	High Expr
	Not  bool
}

func (*BetweenExpr) exprNode() {}

func (e *BetweenExpr) String() string {
	if e.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", e.Expr, e.Low, e.High)
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", e.Expr, e.Low, e.High)
}

// CaseExpr represents CASE [<operand>] WHEN <cond> THEN <result> ... [ELSE
// <result>] END.
type CaseExpr struct {
	Operand Expr       // nil for searched CASE
	Whens   []WhenExpr // at least one
	Else    Expr       // nil if no ELSE
}

// WhenExpr represents a single WHEN ... THEN ... within a CASE expression.
type WhenExpr struct {
	Cond   Expr
	Result Expr
}

func (*CaseExpr) exprNode() {}

func (e *CaseExpr) String() string {
	var b strings.Builder
	b.WriteString("CASE")
	if e.Operand != nil {
		fmt.Fprintf(&b, " %s", e.Operand)
	}
	for _, w := range e.Whens {
		fmt.Fprintf(&b, " WHEN %s THEN %s", w.Cond, w.Result)
	}
	if e.Else != nil {
		fmt.Fprintf(&b, " ELSE %s", e.Else)
	}
	b.WriteString(" END")
	return b.String()
}

// AlterTableStmt represents ALTER TABLE <name> <cmd>.
type AlterTableStmt struct {
	Table string
	Cmd   AlterTableCmd
}

func (*AlterTableStmt) statementNode() {}

func (s *AlterTableStmt) String() string {
	return fmt.Sprintf("ALTER TABLE %s %s", formatIdent(s.Table), s.Cmd)
}

// AlterTableCmd is the interface for ALTER TABLE sub-commands.
type AlterTableCmd interface {
	fmt.Stringer
	alterTableCmd()
}

// AddColumnCmd represents ALTER TABLE ... ADD <column>.
type AddColumnCmd struct {
	Column ColumnDef
}

func (*AddColumnCmd) alterTableCmd() {}

func (c *AddColumnCmd) String() string {
	return fmt.Sprintf("ADD %s", c.Column.String())
}

// DropColumnCmd represents ALTER TABLE ... DROP COLUMN <name>.
type DropColumnCmd struct {
	Name string
}

func (*DropColumnCmd) alterTableCmd() {}

func (c *DropColumnCmd) String() string {
	return fmt.Sprintf("DROP COLUMN %s", formatIdent(c.Name))
}

// AlterColumnCmd represents ALTER TABLE ... ALTER COLUMN <name> <type>.
type AlterColumnCmd struct {
	Name     string
	DataType string
}

func (*AlterColumnCmd) alterTableCmd() {}

func (c *AlterColumnCmd) String() string {
	return fmt.Sprintf("ALTER COLUMN %s %s", formatIdent(c.Name), c.DataType)
}

// ConstraintType identifies the kind of table constraint.
type ConstraintType int

const (
	PrimaryKeyConstraint ConstraintType = iota + 1
	ForeignKeyConstraint
	UniqueConstraint
	CheckConstraint
)

// AddConstraintCmd represents ALTER TABLE ... ADD CONSTRAINT <name> <def>.
type AddConstraintCmd struct {
	Name       string
	Type       ConstraintType
	Columns    []string
	RefTable   string // FOREIGN KEY only
	RefColumns []string
	CheckExpr  Expr // CHECK only
}

func (*AddConstraintCmd) alterTableCmd() {}

func (c *AddConstraintCmd) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ADD CONSTRAINT %s ", formatIdent(c.Name))
	switch c.Type {
	case PrimaryKeyConstraint:
		b.WriteString("PRIMARY KEY (")
		b.WriteString(strings.Join(c.Columns, ", "))
		b.WriteString(")")
	case ForeignKeyConstraint:
		fmt.Fprintf(&b, "FOREIGN KEY (%s) REFERENCES %s (%s)",
			strings.Join(c.Columns, ", "),
			formatIdent(c.RefTable),
			strings.Join(c.RefColumns, ", "))
	case UniqueConstraint:
		b.WriteString("UNIQUE (")
		b.WriteString(strings.Join(c.Columns, ", "))
		b.WriteString(")")
	case CheckConstraint:
		fmt.Fprintf(&b, "CHECK %s", c.CheckExpr)
	}
	return b.String()
}

// DropConstraintCmd represents ALTER TABLE ... DROP CONSTRAINT <name>.
type DropConstraintCmd struct {
	Name string
}

func (*DropConstraintCmd) alterTableCmd() {}

func (c *DropConstraintCmd) String() string {
	return fmt.Sprintf("DROP CONSTRAINT %s", formatIdent(c.Name))
}

// CreateIndexStmt represents CREATE [UNIQUE] INDEX <name> ON <table> (<cols>)
// [INCLUDE (<cols>)].
type CreateIndexStmt struct {
	Unique  bool
	Name    string
	Table   string
	Columns []string
	Include []string
}

func (*CreateIndexStmt) statementNode() {}

func (s *CreateIndexStmt) String() string {
	var b strings.Builder
	b.WriteString("CREATE ")
	if s.Unique {
		b.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&b, "INDEX %s ON %s (%s)",
		formatIdent(s.Name), formatIdent(s.Table),
		strings.Join(s.Columns, ", "))
	if len(s.Include) > 0 {
		fmt.Fprintf(&b, " INCLUDE (%s)", strings.Join(s.Include, ", "))
	}
	return b.String()
}

// CreateViewStmt represents CREATE VIEW <name> AS <select>.
type CreateViewStmt struct {
	Name   string
	Select *SelectStmt
}

func (*CreateViewStmt) statementNode() {}

func (s *CreateViewStmt) String() string {
	return fmt.Sprintf("CREATE VIEW %s AS %s", formatIdent(s.Name), s.Select)
}

// DropViewStmt represents DROP VIEW [IF EXISTS] <name>.
type DropViewStmt struct {
	Name     string
	IfExists bool
}

func (*DropViewStmt) statementNode() {}

func (s *DropViewStmt) String() string {
	if s.IfExists {
		return fmt.Sprintf("DROP VIEW IF EXISTS %s", formatIdent(s.Name))
	}
	return fmt.Sprintf("DROP VIEW %s", formatIdent(s.Name))
}

// DropIndexStmt represents DROP INDEX [IF EXISTS] <name> [ON <table>].
type DropIndexStmt struct {
	Name     string
	Table    string
	IfExists bool
}

func (*DropIndexStmt) statementNode() {}

func (s *DropIndexStmt) String() string {
	var b strings.Builder
	b.WriteString("DROP INDEX ")
	if s.IfExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(formatIdent(s.Name))
	if s.Table != "" {
		fmt.Fprintf(&b, " ON %s", formatIdent(s.Table))
	}
	return b.String()
}

// DropProcedureStmt represents DROP PROCEDURE [IF EXISTS] <name>.
type DropProcedureStmt struct {
	Name     string
	IfExists bool
}

func (*DropProcedureStmt) statementNode() {}

func (s *DropProcedureStmt) String() string {
	if s.IfExists {
		return fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", formatIdent(s.Name))
	}
	return fmt.Sprintf("DROP PROCEDURE %s", formatIdent(s.Name))
}

// TruncateTableStmt represents TRUNCATE TABLE <name>.
type TruncateTableStmt struct {
	Table string
}

func (*TruncateTableStmt) statementNode() {}

func (s *TruncateTableStmt) String() string {
	return fmt.Sprintf("TRUNCATE TABLE %s", formatIdent(s.Table))
}

// CreateProcedureStmt represents CREATE PROCEDURE <name> (body consumed but
// not translated — CockroachDB TDS rejects these gracefully).
type CreateProcedureStmt struct {
	Name string
}

func (*CreateProcedureStmt) statementNode() {}

func (s *CreateProcedureStmt) String() string {
	return fmt.Sprintf("CREATE PROCEDURE %s ...", formatIdent(s.Name))
}

// CreateFunctionStmt represents CREATE FUNCTION <name> (body consumed but
// not translated).
type CreateFunctionStmt struct {
	Name string
}

func (*CreateFunctionStmt) statementNode() {}

func (s *CreateFunctionStmt) String() string {
	return fmt.Sprintf("CREATE FUNCTION %s ...", formatIdent(s.Name))
}

// CreateTriggerStmt represents CREATE TRIGGER <name> (body consumed but
// not translated).
type CreateTriggerStmt struct {
	Name string
}

func (*CreateTriggerStmt) statementNode() {}

func (s *CreateTriggerStmt) String() string {
	return fmt.Sprintf("CREATE TRIGGER %s ...", formatIdent(s.Name))
}

// SubqueryExpr represents a scalar subquery used as an expression: (SELECT ...).
type SubqueryExpr struct {
	Select Statement
}

func (*SubqueryExpr) exprNode() {}

func (e *SubqueryExpr) String() string {
	return fmt.Sprintf("(%s)", e.Select)
}

// ExistsExpr represents [NOT] EXISTS (SELECT ...).
type ExistsExpr struct {
	Select Statement
	Not    bool
}

func (*ExistsExpr) exprNode() {}

func (e *ExistsExpr) String() string {
	if e.Not {
		return fmt.Sprintf("NOT EXISTS (%s)", e.Select)
	}
	return fmt.Sprintf("EXISTS (%s)", e.Select)
}

// AnyAllExpr represents <expr> <op> ANY|ALL|SOME (SELECT ...).
type AnyAllExpr struct {
	Expr   Expr
	Op     string // comparison operator: =, <>, <, >, <=, >=
	Kind   string // "ANY", "ALL", or "SOME"
	Select Statement
}

func (*AnyAllExpr) exprNode() {}

func (e *AnyAllExpr) String() string {
	return fmt.Sprintf("%s %s %s (%s)", e.Expr, e.Op, e.Kind, e.Select)
}

// WindowExpr represents a window function call:
// <func>(<args>) OVER ([PARTITION BY <exprs>] [ORDER BY <exprs>]).
type WindowExpr struct {
	Func        *FuncCallExpr
	PartitionBy []Expr
	OrderBy     []OrderByExpr
}

func (*WindowExpr) exprNode() {}

func (e *WindowExpr) String() string {
	var b strings.Builder
	b.WriteString(e.Func.String())
	b.WriteString(" OVER (")
	if len(e.PartitionBy) > 0 {
		b.WriteString("PARTITION BY ")
		for i, p := range e.PartitionBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(p.String())
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
			b.WriteString(o.String())
		}
	}
	b.WriteString(")")
	return b.String()
}

// BeginTranStmt represents BEGIN TRAN[SACTION] [name].
type BeginTranStmt struct {
	Name string // optional transaction name
}

func (*BeginTranStmt) statementNode() {}

func (s *BeginTranStmt) String() string {
	if s.Name != "" {
		return fmt.Sprintf("BEGIN TRANSACTION %s", formatIdent(s.Name))
	}
	return "BEGIN TRANSACTION"
}

// CommitTranStmt represents COMMIT [TRAN[SACTION]] [name].
type CommitTranStmt struct {
	Name string // optional transaction name
}

func (*CommitTranStmt) statementNode() {}

func (s *CommitTranStmt) String() string {
	if s.Name != "" {
		return fmt.Sprintf("COMMIT TRANSACTION %s", formatIdent(s.Name))
	}
	return "COMMIT TRANSACTION"
}

// RollbackTranStmt represents ROLLBACK [TRAN[SACTION]] [name | savepoint].
type RollbackTranStmt struct {
	Name string // optional transaction name or savepoint name
}

func (*RollbackTranStmt) statementNode() {}

func (s *RollbackTranStmt) String() string {
	if s.Name != "" {
		return fmt.Sprintf("ROLLBACK TRANSACTION %s", formatIdent(s.Name))
	}
	return "ROLLBACK TRANSACTION"
}

// SaveTranStmt represents SAVE TRAN[SACTION] name.
type SaveTranStmt struct {
	Name string // required savepoint name
}

func (*SaveTranStmt) statementNode() {}

func (s *SaveTranStmt) String() string {
	return fmt.Sprintf("SAVE TRANSACTION %s", formatIdent(s.Name))
}

// MergeStmt represents MERGE INTO <target> USING <source> ON <condition>
// WHEN MATCHED THEN ... WHEN NOT MATCHED THEN ...
// [SQL Server] MERGE is SQL Server 2008+ (and ANSI SQL:2003). Sybase ASE
// does not support MERGE.
type MergeStmt struct {
	Target     TableRef
	Source     TableRef
	Condition  Expr
	Matched    *MergeWhenMatched
	NotMatched *MergeWhenNotMatched
	Output     []SelectColumn
}

func (*MergeStmt) statementNode() {}

func (s *MergeStmt) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "MERGE INTO %s", s.Target.String())
	fmt.Fprintf(&b, " USING %s", s.Source.String())
	fmt.Fprintf(&b, " ON %s", s.Condition)
	if s.Matched != nil {
		if s.Matched.Delete {
			b.WriteString(" WHEN MATCHED THEN DELETE")
		} else {
			b.WriteString(" WHEN MATCHED THEN UPDATE SET ")
			for i, a := range s.Matched.Assignments {
				if i > 0 {
					b.WriteString(", ")
				}
				fmt.Fprintf(&b, "%s = %s", formatColumnRef(a.Column), a.Value)
			}
		}
	}
	if s.NotMatched != nil {
		b.WriteString(" WHEN NOT MATCHED THEN INSERT")
		if len(s.NotMatched.Columns) > 0 {
			b.WriteString(" (")
			for i, c := range s.NotMatched.Columns {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(formatIdent(c))
			}
			b.WriteString(")")
		}
		b.WriteString(" VALUES (")
		for i, v := range s.NotMatched.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(v.String())
		}
		b.WriteString(")")
	}
	return b.String()
}

// MergeWhenMatched represents the WHEN MATCHED THEN clause of a MERGE:
// either UPDATE SET ... or DELETE.
type MergeWhenMatched struct {
	Assignments []Assignment // for UPDATE SET
	Delete      bool         // true for WHEN MATCHED THEN DELETE
}

// MergeWhenNotMatched represents the WHEN NOT MATCHED THEN INSERT clause.
type MergeWhenNotMatched struct {
	Columns []string
	Values  []Expr
}

// DeclareVarStmt represents DECLARE @var TYPE [= expr].
type DeclareVarStmt struct {
	Name     string // variable name including the @ prefix
	DataType string
	Default  Expr // optional default value
}

func (*DeclareVarStmt) statementNode() {}

func (s *DeclareVarStmt) String() string {
	if s.Default != nil {
		return fmt.Sprintf("DECLARE %s %s = %s", s.Name, s.DataType, s.Default)
	}
	return fmt.Sprintf("DECLARE %s %s", s.Name, s.DataType)
}

// SetVarStmt represents SET @var = expr (variable assignment).
type SetVarStmt struct {
	Name string // variable name including the @ prefix
	Expr Expr
}

func (*SetVarStmt) statementNode() {}

func (s *SetVarStmt) String() string {
	return fmt.Sprintf("SET %s = %s", s.Name, s.Expr)
}

// IfStmt represents IF condition body [ELSE elseBody].
// Body and ElseBody are individual Statements (often a BeginEndBlock
// for multi-statement branches, or a single SELECT/DML).
type IfStmt struct {
	Condition Expr
	Body      Statement
	ElseBody  Statement // nil if no ELSE
}

func (*IfStmt) statementNode() {}

func (s *IfStmt) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "IF %s %s", s.Condition, s.Body)
	if s.ElseBody != nil {
		fmt.Fprintf(&b, " ELSE %s", s.ElseBody)
	}
	return b.String()
}

// WhileStmt represents WHILE condition body. Body is typically a
// BeginEndBlock.
type WhileStmt struct {
	Condition Expr
	Body      Statement
}

func (*WhileStmt) statementNode() {}

func (s *WhileStmt) String() string {
	return fmt.Sprintf("WHILE %s %s", s.Condition, s.Body)
}

// BeginEndBlock represents BEGIN stmt1; stmt2; ... END — a statement
// block used as the body of IF, WHILE, or standalone grouping. This is
// distinct from BeginTranStmt which starts a transaction.
type BeginEndBlock struct {
	Stmts []Statement
}

func (*BeginEndBlock) statementNode() {}

func (s *BeginEndBlock) String() string {
	var parts []string
	for _, stmt := range s.Stmts {
		parts = append(parts, stmt.String())
	}
	return fmt.Sprintf("BEGIN %s END", strings.Join(parts, "; "))
}

// BreakStmt represents the BREAK statement inside a WHILE loop.
type BreakStmt struct{}

func (*BreakStmt) statementNode() {}

func (*BreakStmt) String() string { return "BREAK" }

// ContinueStmt represents the CONTINUE statement inside a WHILE loop.
type ContinueStmt struct{}

func (*ContinueStmt) statementNode() {}

func (*ContinueStmt) String() string { return "CONTINUE" }

// PrintStmt represents PRINT <expr>, which sends an informational
// message to the client. [Both] PRINT is supported by both SQL Server
// and Sybase ASE.
type PrintStmt struct {
	Expr Expr
}

func (*PrintStmt) statementNode() {}

func (s *PrintStmt) String() string {
	return fmt.Sprintf("PRINT %s", s.Expr)
}

// RaiserrorStmt represents the RAISERROR syntax.
// [Sybase ASE] Sybase form: RAISERROR <errnum> [, <message>].
// [SQL Server] SQL Server uses a different form: RAISERROR('msg',
// severity, state) — not yet supported by this parser.
type RaiserrorStmt struct {
	ErrNum  int
	Message string // optional
}

func (*RaiserrorStmt) statementNode() {}

func (s *RaiserrorStmt) String() string {
	if s.Message != "" {
		escaped := strings.ReplaceAll(s.Message, "'", "''")
		return fmt.Sprintf("RAISERROR %d, '%s'", s.ErrNum, escaped)
	}
	return fmt.Sprintf("RAISERROR %d", s.ErrNum)
}

// formatColumnRef formats a column reference that may contain dots
// (e.g., t.name from UPDATE...FROM). Each part is individually quoted
// if necessary.
func formatColumnRef(col string) string {
	if strings.Contains(col, ".") {
		parts := strings.Split(col, ".")
		var formatted []string
		for _, p := range parts {
			formatted = append(formatted, formatIdent(p))
		}
		return strings.Join(formatted, ".")
	}
	return formatIdent(col)
}

// formatIdent returns an identifier, quoting it with brackets if it contains
// special characters or is a reserved word. For simplicity, identifiers that
// are plain alphanumeric (plus underscore) are returned unquoted.
func formatIdent(name string) string {
	for _, c := range name {
		if !isIdentChar(c) {
			return fmt.Sprintf("[%s]", name)
		}
	}
	return name
}

func isIdentChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_' || c == '#' || c == '@'
}
