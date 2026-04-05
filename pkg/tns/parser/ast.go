// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package parser implements a recursive descent parser for Oracle SQL.
//
// The parser handles Oracle-specific syntax including ROWNUM, DUAL, SYSDATE,
// NVL, DECODE, TO_CHAR/TO_DATE, sequences (NEXTVAL/CURRVAL), and uppercase
// unquoted identifiers. The output is an Oracle-specific AST intended for
// translation to CockroachDB SQL.
package parser

import (
	"fmt"
	"strings"
)

// Node is the interface implemented by all AST nodes.
type Node interface {
	fmt.Stringer
}

// Statement is a top-level SQL statement.
type Statement interface {
	Node
	statementNode()
}

// Expr is an expression node.
type Expr interface {
	Node
	exprNode()
}

// -----------------------------------------------------------------------
// Statements
// -----------------------------------------------------------------------

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Distinct  bool
	Columns   []SelectColumn
	From      []TableExpr
	Where     Expr
	GroupBy   []Expr
	Having    Expr
	OrderBy   []OrderByItem
	Limit     Expr // for FETCH FIRST n ROWS ONLY or ROWNUM-based limits
	ForUpdate bool
}

func (*SelectStmt) statementNode() {}

func (s *SelectStmt) String() string {
	var b strings.Builder
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
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
		b.WriteString(" WHERE ")
		b.WriteString(s.Where.String())
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
		b.WriteString(" HAVING ")
		b.WriteString(s.Having.String())
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

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Table   *TableRef
	Columns []string
	Values  [][]Expr // each inner slice is a row
	Select  *SelectStmt
}

func (*InsertStmt) statementNode() {}

func (s *InsertStmt) String() string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(s.Table.String())
	if len(s.Columns) > 0 {
		b.WriteString(" (")
		b.WriteString(strings.Join(s.Columns, ", "))
		b.WriteString(")")
	}
	if s.Select != nil {
		b.WriteString(" ")
		b.WriteString(s.Select.String())
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

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Table       *TableRef
	Assignments []Assignment
	Where       Expr
}

func (*UpdateStmt) statementNode() {}

func (s *UpdateStmt) String() string {
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(s.Table.String())
	b.WriteString(" SET ")
	for i, a := range s.Assignments {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(a.String())
	}
	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(s.Where.String())
	}
	return b.String()
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Table *TableRef
	Where Expr
}

func (*DeleteStmt) statementNode() {}

func (s *DeleteStmt) String() string {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(s.Table.String())
	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(s.Where.String())
	}
	return b.String()
}

// CreateSequenceStmt represents CREATE SEQUENCE.
type CreateSequenceStmt struct {
	Name      string
	StartWith *int64
	Increment *int64
	MinValue  *int64
	MaxValue  *int64
	Cache     *int64
	Cycle     bool
}

func (*CreateSequenceStmt) statementNode() {}

func (s *CreateSequenceStmt) String() string {
	var b strings.Builder
	b.WriteString("CREATE SEQUENCE ")
	b.WriteString(s.Name)
	if s.StartWith != nil {
		fmt.Fprintf(&b, " START WITH %d", *s.StartWith)
	}
	if s.Increment != nil {
		fmt.Fprintf(&b, " INCREMENT BY %d", *s.Increment)
	}
	if s.MinValue != nil {
		fmt.Fprintf(&b, " MINVALUE %d", *s.MinValue)
	}
	if s.MaxValue != nil {
		fmt.Fprintf(&b, " MAXVALUE %d", *s.MaxValue)
	}
	if s.Cache != nil {
		fmt.Fprintf(&b, " CACHE %d", *s.Cache)
	}
	if s.Cycle {
		b.WriteString(" CYCLE")
	}
	return b.String()
}

// -----------------------------------------------------------------------
// Table expressions
// -----------------------------------------------------------------------

// TableExpr is a table reference in a FROM clause.
type TableExpr interface {
	Node
	tableExprNode()
}

// TableRef is a simple table name with optional schema qualifier and alias.
type TableRef struct {
	Schema string
	Name   string
	Alias  string
}

func (*TableRef) tableExprNode() {}

func (t *TableRef) String() string {
	var b strings.Builder
	if t.Schema != "" {
		b.WriteString(t.Schema)
		b.WriteString(".")
	}
	b.WriteString(t.Name)
	if t.Alias != "" {
		b.WriteString(" ")
		b.WriteString(t.Alias)
	}
	return b.String()
}

// DualTableRef represents the Oracle DUAL pseudo-table.
type DualTableRef struct{}

func (*DualTableRef) tableExprNode() {}
func (*DualTableRef) String() string { return "DUAL" }

// SubqueryTableExpr is a subquery used as a table expression.
type SubqueryTableExpr struct {
	Query *SelectStmt
	Alias string
}

func (*SubqueryTableExpr) tableExprNode() {}

func (s *SubqueryTableExpr) String() string {
	result := "(" + s.Query.String() + ")"
	if s.Alias != "" {
		result += " " + s.Alias
	}
	return result
}

// JoinTableExpr represents a JOIN between two table expressions.
type JoinTableExpr struct {
	Left  TableExpr
	Right TableExpr
	Type  JoinType
	Cond  Expr // ON condition (nil for CROSS JOIN)
}

func (*JoinTableExpr) tableExprNode() {}

func (j *JoinTableExpr) String() string {
	var b strings.Builder
	b.WriteString(j.Left.String())
	b.WriteString(" ")
	b.WriteString(string(j.Type))
	b.WriteString(" ")
	b.WriteString(j.Right.String())
	if j.Cond != nil {
		b.WriteString(" ON ")
		b.WriteString(j.Cond.String())
	}
	return b.String()
}

// JoinType classifies a JOIN.
type JoinType string

const (
	InnerJoin JoinType = "JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	FullJoin  JoinType = "FULL JOIN"
	CrossJoin JoinType = "CROSS JOIN"
)

// -----------------------------------------------------------------------
// Select column
// -----------------------------------------------------------------------

// SelectColumn is a single column in a SELECT list.
type SelectColumn struct {
	Expr  Expr
	Alias string
}

func (c SelectColumn) String() string {
	s := c.Expr.String()
	if c.Alias != "" {
		s += " AS " + c.Alias
	}
	return s
}

// -----------------------------------------------------------------------
// ORDER BY item
// -----------------------------------------------------------------------

// OrderByItem is a single item in an ORDER BY clause.
type OrderByItem struct {
	Expr Expr
	Desc bool
}

func (o OrderByItem) String() string {
	s := o.Expr.String()
	if o.Desc {
		s += " DESC"
	}
	return s
}

// -----------------------------------------------------------------------
// Assignment (for UPDATE SET)
// -----------------------------------------------------------------------

// Assignment is a column = expr pair in an UPDATE SET clause.
type Assignment struct {
	Column string
	Value  Expr
}

func (a Assignment) String() string {
	return a.Column + " = " + a.Value.String()
}

// -----------------------------------------------------------------------
// Expressions
// -----------------------------------------------------------------------

// ColumnRefExpr is a column reference, optionally qualified by table/schema.
type ColumnRefExpr struct {
	Table  string // optional table qualifier
	Column string
}

func (*ColumnRefExpr) exprNode() {}

func (c *ColumnRefExpr) String() string {
	if c.Table != "" {
		return c.Table + "." + c.Column
	}
	return c.Column
}

// NumberLit is a numeric literal.
type NumberLit struct {
	Value string
}

func (*NumberLit) exprNode() {}

func (n *NumberLit) String() string { return n.Value }

// StringLit is a string literal.
type StringLit struct {
	Value string
}

func (*StringLit) exprNode() {}

func (s *StringLit) String() string {
	escaped := strings.ReplaceAll(s.Value, "'", "''")
	return "'" + escaped + "'"
}

// NullLit represents a NULL literal.
type NullLit struct{}

func (*NullLit) exprNode()      {}
func (*NullLit) String() string { return "NULL" }

// BindExpr represents a :name bind variable.
type BindExpr struct {
	Name string
}

func (*BindExpr) exprNode() {}

func (b *BindExpr) String() string { return ":" + b.Name }

// StarExpr represents * in a SELECT list.
type StarExpr struct {
	Table string // optional table qualifier
}

func (*StarExpr) exprNode() {}

func (s *StarExpr) String() string {
	if s.Table != "" {
		return s.Table + ".*"
	}
	return "*"
}

// BinaryExpr represents a binary operation.
type BinaryExpr struct {
	Left  Expr
	Op    BinaryOp
	Right Expr
}

func (*BinaryExpr) exprNode() {}

func (b *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Op, b.Right)
}

// BinaryOp is an operator used in binary expressions.
type BinaryOp string

const (
	OpAdd    BinaryOp = "+"
	OpSub    BinaryOp = "-"
	OpMul    BinaryOp = "*"
	OpDiv    BinaryOp = "/"
	OpEq     BinaryOp = "="
	OpNeq    BinaryOp = "<>"
	OpLt     BinaryOp = "<"
	OpGt     BinaryOp = ">"
	OpLte    BinaryOp = "<="
	OpGte    BinaryOp = ">="
	OpAnd    BinaryOp = "AND"
	OpOr     BinaryOp = "OR"
	OpLike   BinaryOp = "LIKE"
	OpConcat BinaryOp = "||"
)

// UnaryExpr represents a unary operation.
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expr
}

func (*UnaryExpr) exprNode() {}

func (u *UnaryExpr) String() string {
	if u.Op == OpNot {
		return fmt.Sprintf("(NOT %s)", u.Expr)
	}
	return fmt.Sprintf("(%s%s)", u.Op, u.Expr)
}

// UnaryOp is an operator used in unary expressions.
type UnaryOp string

const (
	OpNeg UnaryOp = "-"
	OpPos UnaryOp = "+"
	OpNot UnaryOp = "NOT"
)

// IsNullExpr represents expr IS [NOT] NULL.
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*IsNullExpr) exprNode() {}

func (e *IsNullExpr) String() string {
	if e.Not {
		return fmt.Sprintf("(%s IS NOT NULL)", e.Expr)
	}
	return fmt.Sprintf("(%s IS NULL)", e.Expr)
}

// InExpr represents expr [NOT] IN (values...) or expr [NOT] IN (subquery).
type InExpr struct {
	Expr     Expr
	Values   []Expr      // value list form
	Subquery *SelectStmt // subquery form (mutually exclusive with Values)
	Not      bool
}

func (*InExpr) exprNode() {}

func (e *InExpr) String() string {
	var b strings.Builder
	b.WriteString("(")
	b.WriteString(e.Expr.String())
	if e.Not {
		b.WriteString(" NOT")
	}
	b.WriteString(" IN (")
	if e.Subquery != nil {
		b.WriteString(e.Subquery.String())
	} else {
		for i, v := range e.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(v.String())
		}
	}
	b.WriteString("))")
	return b.String()
}

// BetweenExpr represents expr [NOT] BETWEEN low AND high.
type BetweenExpr struct {
	Expr Expr
	Low  Expr
	High Expr
	Not  bool
}

func (*BetweenExpr) exprNode() {}

func (e *BetweenExpr) String() string {
	not := ""
	if e.Not {
		not = " NOT"
	}
	return fmt.Sprintf("(%s%s BETWEEN %s AND %s)", e.Expr, not, e.Low, e.High)
}

// ExistsExpr represents EXISTS (subquery).
type ExistsExpr struct {
	Query *SelectStmt
}

func (*ExistsExpr) exprNode() {}

func (e *ExistsExpr) String() string {
	return "EXISTS (" + e.Query.String() + ")"
}

// SubqueryExpr is a scalar subquery in expression context.
type SubqueryExpr struct {
	Query *SelectStmt
}

func (*SubqueryExpr) exprNode() {}

func (s *SubqueryExpr) String() string {
	return "(" + s.Query.String() + ")"
}

// FuncCallExpr is a generic function call.
type FuncCallExpr struct {
	Name string
	Args []Expr
}

func (*FuncCallExpr) exprNode() {}

func (f *FuncCallExpr) String() string {
	var b strings.Builder
	b.WriteString(f.Name)
	b.WriteString("(")
	for i, a := range f.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(a.String())
	}
	b.WriteString(")")
	return b.String()
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Operand Expr // nil for searched CASE
	Whens   []CaseWhen
	Else    Expr // nil if no ELSE
}

func (*CaseExpr) exprNode() {}

func (c *CaseExpr) String() string {
	var b strings.Builder
	b.WriteString("CASE")
	if c.Operand != nil {
		b.WriteString(" ")
		b.WriteString(c.Operand.String())
	}
	for _, w := range c.Whens {
		fmt.Fprintf(&b, " WHEN %s THEN %s", w.Cond, w.Result)
	}
	if c.Else != nil {
		b.WriteString(" ELSE ")
		b.WriteString(c.Else.String())
	}
	b.WriteString(" END")
	return b.String()
}

// CaseWhen is a WHEN clause in a CASE expression.
type CaseWhen struct {
	Cond   Expr
	Result Expr
}

// ParenExpr wraps an expression in parentheses.
type ParenExpr struct {
	Expr Expr
}

func (*ParenExpr) exprNode() {}

func (p *ParenExpr) String() string {
	return "(" + p.Expr.String() + ")"
}

// -----------------------------------------------------------------------
// Oracle-specific expressions
// -----------------------------------------------------------------------

// RowNumExpr represents the Oracle ROWNUM pseudo-column.
type RowNumExpr struct{}

func (*RowNumExpr) exprNode()      {}
func (*RowNumExpr) String() string { return "ROWNUM" }

// SysDateExpr represents the Oracle SYSDATE keyword.
type SysDateExpr struct{}

func (*SysDateExpr) exprNode()      {}
func (*SysDateExpr) String() string { return "SYSDATE" }

// SysTimestampExpr represents the Oracle SYSTIMESTAMP keyword.
type SysTimestampExpr struct{}

func (*SysTimestampExpr) exprNode()      {}
func (*SysTimestampExpr) String() string { return "SYSTIMESTAMP" }

// SequenceExpr represents seq.NEXTVAL or seq.CURRVAL.
type SequenceExpr struct {
	Sequence string
	Op       SequenceOp
}

func (*SequenceExpr) exprNode() {}

func (s *SequenceExpr) String() string {
	return s.Sequence + "." + string(s.Op)
}

// SequenceOp is the sequence operation.
type SequenceOp string

const (
	SeqNextVal SequenceOp = "NEXTVAL"
	SeqCurrVal SequenceOp = "CURRVAL"
)

// NVLExpr represents NVL(expr, default).
type NVLExpr struct {
	Expr    Expr
	Default Expr
}

func (*NVLExpr) exprNode() {}

func (n *NVLExpr) String() string {
	return fmt.Sprintf("NVL(%s, %s)", n.Expr, n.Default)
}

// NVL2Expr represents NVL2(expr, not_null_val, null_val).
type NVL2Expr struct {
	Expr       Expr
	NotNullVal Expr
	NullVal    Expr
}

func (*NVL2Expr) exprNode() {}

func (n *NVL2Expr) String() string {
	return fmt.Sprintf("NVL2(%s, %s, %s)", n.Expr, n.NotNullVal, n.NullVal)
}

// DecodeExpr represents DECODE(expr, search1, result1, ..., default).
type DecodeExpr struct {
	Expr    Expr
	Pairs   []DecodePair // search/result pairs
	Default Expr         // nil if no default
}

func (*DecodeExpr) exprNode() {}

func (d *DecodeExpr) String() string {
	var b strings.Builder
	b.WriteString("DECODE(")
	b.WriteString(d.Expr.String())
	for _, p := range d.Pairs {
		fmt.Fprintf(&b, ", %s, %s", p.Search, p.Result)
	}
	if d.Default != nil {
		b.WriteString(", ")
		b.WriteString(d.Default.String())
	}
	b.WriteString(")")
	return b.String()
}

// DecodePair is a search/result pair in a DECODE expression.
type DecodePair struct {
	Search Expr
	Result Expr
}
