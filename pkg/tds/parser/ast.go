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
type ColumnDef struct {
	Name     string
	DataType string
	Nullable *bool // nil = unspecified, true = NULL, false = NOT NULL
}

func (c *ColumnDef) String() string {
	s := fmt.Sprintf("%s %s", formatIdent(c.Name), c.DataType)
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
type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]Expr
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
	return b.String()
}

// DeleteStmt represents DELETE [FROM] <table> [WHERE <expr>].
type DeleteStmt struct {
	Table string
	Where Expr
}

func (*DeleteStmt) statementNode() {}

func (s *DeleteStmt) String() string {
	result := fmt.Sprintf("DELETE FROM %s", formatIdent(s.Table))
	if s.Where != nil {
		result += fmt.Sprintf(" WHERE %s", s.Where)
	}
	return result
}

// UpdateStmt represents UPDATE <table> SET <assignments> [WHERE <expr>].
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr
}

func (*UpdateStmt) statementNode() {}

func (s *UpdateStmt) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", formatIdent(s.Table))
	for i, a := range s.Assignments {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s = %s", formatIdent(a.Column), a.Value)
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

// SelectStmt represents SELECT [DISTINCT] [TOP n] <columns> [FROM <table>]
// [JOIN ...] [WHERE <expr>] [GROUP BY <exprs>] [HAVING <expr>]
// [ORDER BY <exprs>] [OFFSET n ROWS [FETCH NEXT m ROWS ONLY]].
type SelectStmt struct {
	Distinct bool
	Top      *int
	Columns  []SelectColumn
	From     []TableRef
	Joins    []JoinClause
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []OrderByExpr
	Offset   *int // OFFSET n ROWS
	Fetch    *int // FETCH NEXT m ROWS ONLY
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
	if s.Offset != nil {
		fmt.Fprintf(&b, " OFFSET %d ROWS", *s.Offset)
	}
	if s.Fetch != nil {
		fmt.Fprintf(&b, " FETCH NEXT %d ROWS ONLY", *s.Fetch)
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
// operation (UNION, UNION ALL, INTERSECT, EXCEPT). ORDER BY and OFFSET-FETCH
// apply to the compound result when present.
type CompoundSelectStmt struct {
	Left    Statement // *SelectStmt or *CompoundSelectStmt
	Op      string    // "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
	Right   Statement // *SelectStmt
	OrderBy []OrderByExpr
	Offset  *int
	Fetch   *int
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
	if s.Offset != nil {
		fmt.Fprintf(&b, " OFFSET %d ROWS", *s.Offset)
	}
	if s.Fetch != nil {
		fmt.Fprintf(&b, " FETCH NEXT %d ROWS ONLY", *s.Fetch)
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
type ConvertExpr struct {
	DataType string
	Expr     Expr
	Style    Expr // optional
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
