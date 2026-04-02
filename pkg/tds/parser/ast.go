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

// SelectStmt represents SELECT [TOP n] <columns> [FROM <table>]
// [WHERE <expr>] [ORDER BY <exprs>].
type SelectStmt struct {
	Top     *int
	Columns []SelectColumn
	From    []TableRef
	Where   Expr
	OrderBy []OrderByExpr
}

func (*SelectStmt) statementNode() {}

func (s *SelectStmt) String() string {
	var b strings.Builder
	b.WriteString("SELECT ")
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
	}
	if s.Where != nil {
		fmt.Fprintf(&b, " WHERE %s", s.Where)
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
type TableRef struct {
	Name  string
	Alias string
}

func (t *TableRef) String() string {
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

// InExpr represents <expr> [NOT] IN (<values>).
type InExpr struct {
	Expr   Expr
	Values []Expr
	Not    bool
}

func (*InExpr) exprNode() {}

func (e *InExpr) String() string {
	var vals []string
	for _, v := range e.Values {
		vals = append(vals, v.String())
	}
	if e.Not {
		return fmt.Sprintf("%s NOT IN (%s)", e.Expr, strings.Join(vals, ", "))
	}
	return fmt.Sprintf("%s IN (%s)", e.Expr, strings.Join(vals, ", "))
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
