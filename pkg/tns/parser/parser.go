// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
)

// Parse parses a single Oracle SQL statement and returns its AST.
func Parse(sql string) (Statement, error) {
	p := newParser(sql)
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// expect EOF or semicolon
	if p.cur.Type == SEMI {
		p.next()
	}
	if p.cur.Type != EOF {
		return nil, p.errorf("unexpected token %s after statement", p.cur)
	}
	return stmt, nil
}

// ParseExpr parses an Oracle SQL expression.
func ParseExpr(sql string) (Expr, error) {
	p := newParser(sql)
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.Type != EOF {
		return nil, p.errorf("unexpected token %s after expression", p.cur)
	}
	return expr, nil
}

// parser is a recursive descent parser for Oracle SQL.
type parser struct {
	lex  *Lexer
	cur  Token // current token
	prev Token // previous token (for error reporting)
}

func newParser(sql string) *parser {
	p := &parser{lex: NewLexer(sql)}
	p.next() // prime the parser with the first token
	return p
}

// next advances to the next token.
func (p *parser) next() {
	p.prev = p.cur
	p.cur = p.lex.NextToken()
}

// expect consumes the current token if it matches typ, otherwise returns an error.
func (p *parser) expect(typ TokenType) (Token, error) {
	if p.cur.Type != typ {
		return Token{}, p.errorf("expected %v, got %s", typ, p.cur)
	}
	tok := p.cur
	p.next()
	return tok, nil
}

// match returns true and advances if the current token matches any of the given types.
func (p *parser) match(types ...TokenType) bool {
	for _, t := range types {
		if p.cur.Type == t {
			p.next()
			return true
		}
	}
	return false
}

func (p *parser) errorf(format string, args ...interface{}) error {
	return errors.Newf("line %d, col %d: "+format, append([]interface{}{p.cur.Line, p.cur.Col}, args...)...)
}

// -----------------------------------------------------------------------
// Statement parsing
// -----------------------------------------------------------------------

func (p *parser) parseStatement() (Statement, error) {
	switch p.cur.Type {
	case SELECT:
		return p.parseSelect()
	case INSERT:
		return p.parseInsert()
	case UPDATE:
		return p.parseUpdate()
	case DELETE:
		return p.parseDelete()
	case CREATE:
		return p.parseCreate()
	default:
		return nil, p.errorf("unexpected token %s; expected statement", p.cur)
	}
}

// -----------------------------------------------------------------------
// SELECT
// -----------------------------------------------------------------------

func (p *parser) parseSelect() (*SelectStmt, error) {
	if _, err := p.expect(SELECT); err != nil {
		return nil, err
	}
	stmt := &SelectStmt{}

	// DISTINCT
	if p.match(DISTINCT) {
		stmt.Distinct = true
	}

	// select list
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if p.match(FROM) {
		from, err := p.parseFromClause()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	// WHERE
	if p.match(WHERE) {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// GROUP BY
	if p.cur.Type == GROUP {
		p.next()
		if _, err := p.expect(BY); err != nil {
			return nil, err
		}
		groupBy, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// HAVING
	if p.match(HAVING) {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// ORDER BY
	if p.cur.Type == ORDER {
		p.next()
		if _, err := p.expect(BY); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// FETCH FIRST n ROWS ONLY (Oracle 12c+ syntax)
	if p.cur.Type == FETCH {
		p.next()
		if !p.match(FIRST) && !p.match(NEXT) {
			return nil, p.errorf("expected FIRST or NEXT after FETCH")
		}
		limitExpr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if !p.match(ROWS) {
			return nil, p.errorf("expected ROWS after FETCH FIRST <n>")
		}
		if !p.match(ONLY) {
			return nil, p.errorf("expected ONLY after FETCH FIRST <n> ROWS")
		}
		stmt.Limit = limitExpr
	}

	return stmt, nil
}

func (p *parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if !p.match(COMMA) {
			break
		}
	}
	return cols, nil
}

func (p *parser) parseSelectColumn() (SelectColumn, error) {
	// handle table.* or *
	if p.cur.Type == STAR {
		p.next()
		return SelectColumn{Expr: &StarExpr{}}, nil
	}

	expr, err := p.parseExpr()
	if err != nil {
		return SelectColumn{}, err
	}

	// check for table.* pattern — already parsed as ColumnRef with column "*"
	// or as a qualified star

	var alias string
	if p.match(AS) {
		tok, err := p.expectIdent()
		if err != nil {
			return SelectColumn{}, err
		}
		alias = tok.Literal
	} else if p.cur.Type == IDENT && !p.isClauseKeyword() {
		// implicit alias (no AS keyword)
		alias = p.cur.Literal
		p.next()
	}
	return SelectColumn{Expr: expr, Alias: alias}, nil
}

// isClauseKeyword returns true if the current token is a keyword that starts a
// new clause, which prevents it from being consumed as an implicit alias.
func (p *parser) isClauseKeyword() bool {
	switch p.cur.Type {
	case FROM, WHERE, GROUP, HAVING, ORDER, UNION, SEMI, RPAREN, EOF,
		JOIN, LEFT, RIGHT, FULL, INNER, OUTER, CROSS, ON, FETCH:
		return true
	}
	return false
}

// -----------------------------------------------------------------------
// FROM clause
// -----------------------------------------------------------------------

func (p *parser) parseFromClause() ([]TableExpr, error) {
	first, err := p.parseTableExpr()
	if err != nil {
		return nil, err
	}

	// handle joins
	first, err = p.parseJoins(first)
	if err != nil {
		return nil, err
	}

	tables := []TableExpr{first}
	for p.match(COMMA) {
		tbl, err := p.parseTableExpr()
		if err != nil {
			return nil, err
		}
		tbl, err = p.parseJoins(tbl)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tbl)
	}
	return tables, nil
}

func (p *parser) parseTableExpr() (TableExpr, error) {
	if p.cur.Type == DUAL {
		p.next()
		return &DualTableRef{}, nil
	}

	// subquery
	if p.cur.Type == LPAREN {
		p.next()
		if p.cur.Type == SELECT {
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(RPAREN); err != nil {
				return nil, err
			}
			sub := &SubqueryTableExpr{Query: sel}
			if p.cur.Type == IDENT || p.cur.Type == AS {
				if p.match(AS) {
					// consume AS
				}
				tok, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				sub.Alias = tok.Literal
			}
			return sub, nil
		}
		return nil, p.errorf("expected SELECT after ( in table expression")
	}

	return p.parseTableRef()
}

func (p *parser) parseTableRef() (*TableRef, error) {
	tok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	ref := &TableRef{Name: tok.Literal}

	// optional schema.table
	if p.match(DOT) {
		tblTok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		ref.Schema = ref.Name
		ref.Name = tblTok.Literal
	}

	// optional alias
	if p.cur.Type == IDENT && !p.isJoinKeyword() && !p.isClauseKeyword() {
		ref.Alias = p.cur.Literal
		p.next()
	} else if p.match(AS) {
		tok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		ref.Alias = tok.Literal
	}

	return ref, nil
}

func (p *parser) isJoinKeyword() bool {
	switch p.cur.Type {
	case JOIN, LEFT, RIGHT, FULL, INNER, OUTER, CROSS, ON:
		return true
	}
	return false
}

func (p *parser) parseJoins(left TableExpr) (TableExpr, error) {
	for {
		jt, ok := p.matchJoinType()
		if !ok {
			return left, nil
		}
		right, err := p.parseTableExpr()
		if err != nil {
			return nil, err
		}
		var cond Expr
		if jt != CrossJoin {
			if _, err := p.expect(ON); err != nil {
				return nil, err
			}
			cond, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
		}
		left = &JoinTableExpr{Left: left, Right: right, Type: jt, Cond: cond}
	}
}

func (p *parser) matchJoinType() (JoinType, bool) {
	switch p.cur.Type {
	case JOIN:
		p.next()
		return InnerJoin, true
	case INNER:
		p.next()
		if _, err := p.expect(JOIN); err != nil {
			return "", false
		}
		return InnerJoin, true
	case LEFT:
		p.next()
		p.match(OUTER) // optional
		if _, err := p.expect(JOIN); err != nil {
			return "", false
		}
		return LeftJoin, true
	case RIGHT:
		p.next()
		p.match(OUTER) // optional
		if _, err := p.expect(JOIN); err != nil {
			return "", false
		}
		return RightJoin, true
	case FULL:
		p.next()
		p.match(OUTER) // optional
		if _, err := p.expect(JOIN); err != nil {
			return "", false
		}
		return FullJoin, true
	case CROSS:
		p.next()
		if _, err := p.expect(JOIN); err != nil {
			return "", false
		}
		return CrossJoin, true
	}
	return "", false
}

// -----------------------------------------------------------------------
// ORDER BY
// -----------------------------------------------------------------------

func (p *parser) parseOrderBy() ([]OrderByItem, error) {
	var items []OrderByItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item := OrderByItem{Expr: expr}
		if p.match(DESC) {
			item.Desc = true
		} else {
			p.match(ASC) // consume optional ASC
		}
		items = append(items, item)
		if !p.match(COMMA) {
			break
		}
	}
	return items, nil
}

// -----------------------------------------------------------------------
// INSERT
// -----------------------------------------------------------------------

func (p *parser) parseInsert() (*InsertStmt, error) {
	if _, err := p.expect(INSERT); err != nil {
		return nil, err
	}
	if _, err := p.expect(INTO); err != nil {
		return nil, err
	}
	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: table}

	// optional column list
	if p.match(LPAREN) {
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
	}

	// VALUES or SELECT
	if p.match(VALUES) {
		for {
			if _, err := p.expect(LPAREN); err != nil {
				return nil, err
			}
			row, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(RPAREN); err != nil {
				return nil, err
			}
			stmt.Values = append(stmt.Values, row)
			if !p.match(COMMA) {
				break
			}
		}
	} else if p.cur.Type == SELECT {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sel
	} else {
		return nil, p.errorf("expected VALUES or SELECT after INSERT INTO table")
	}

	return stmt, nil
}

// -----------------------------------------------------------------------
// UPDATE
// -----------------------------------------------------------------------

func (p *parser) parseUpdate() (*UpdateStmt, error) {
	if _, err := p.expect(UPDATE); err != nil {
		return nil, err
	}
	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(SET); err != nil {
		return nil, err
	}
	stmt := &UpdateStmt{Table: table}

	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(EQ); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Assignments = append(stmt.Assignments, Assignment{Column: col.Literal, Value: val})
		if !p.match(COMMA) {
			break
		}
	}

	if p.match(WHERE) {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// -----------------------------------------------------------------------
// DELETE
// -----------------------------------------------------------------------

func (p *parser) parseDelete() (*DeleteStmt, error) {
	if _, err := p.expect(DELETE); err != nil {
		return nil, err
	}
	if _, err := p.expect(FROM); err != nil {
		return nil, err
	}
	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt := &DeleteStmt{Table: table}

	if p.match(WHERE) {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// -----------------------------------------------------------------------
// CREATE SEQUENCE
// -----------------------------------------------------------------------

func (p *parser) parseCreate() (Statement, error) {
	if _, err := p.expect(CREATE); err != nil {
		return nil, err
	}
	if p.cur.Type != SEQUENCE {
		return nil, p.errorf("expected SEQUENCE after CREATE, got %s", p.cur)
	}
	p.next()

	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt := &CreateSequenceStmt{Name: name.Literal}

	// parse optional sequence options
	for p.cur.Type != SEMI && p.cur.Type != EOF {
		switch p.cur.Type {
		case START:
			p.next()
			if _, err := p.expect(WITH); err != nil {
				return nil, err
			}
			val, err := p.parseInt64()
			if err != nil {
				return nil, err
			}
			stmt.StartWith = &val
		case INCREMENT:
			p.next()
			if _, err := p.expect(BY); err != nil {
				return nil, err
			}
			val, err := p.parseInt64()
			if err != nil {
				return nil, err
			}
			stmt.Increment = &val
		case MINVALUE:
			p.next()
			val, err := p.parseInt64()
			if err != nil {
				return nil, err
			}
			stmt.MinValue = &val
		case NOMINVALUE:
			p.next()
			// no value
		case MAXVALUE:
			p.next()
			val, err := p.parseInt64()
			if err != nil {
				return nil, err
			}
			stmt.MaxValue = &val
		case NOMAXVALUE:
			p.next()
			// no value
		case CACHE:
			p.next()
			val, err := p.parseInt64()
			if err != nil {
				return nil, err
			}
			stmt.Cache = &val
		case NOCACHE:
			p.next()
			// nocache means cache=0 or no caching
		case CYCLE:
			p.next()
			stmt.Cycle = true
		case NOCYCLE:
			p.next()
			stmt.Cycle = false
		default:
			return nil, p.errorf("unexpected %s in CREATE SEQUENCE", p.cur)
		}
	}

	return stmt, nil
}

func (p *parser) parseInt64() (int64, error) {
	tok, err := p.expect(NUMBER)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(tok.Literal, 10, 64)
	if err != nil {
		return 0, p.errorf("invalid integer %q", tok.Literal)
	}
	return v, nil
}

// -----------------------------------------------------------------------
// Expression parsing (precedence climbing)
// -----------------------------------------------------------------------

// parseExpr parses an expression with full precedence handling.
func (p *parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == OR {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: OpOr, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == AND {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: OpAnd, Right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.cur.Type == NOT {
		p.next()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: OpNot, Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (Expr, error) {
	left, err := p.parseConcatenation()
	if err != nil {
		return nil, err
	}

	// IS [NOT] NULL
	if p.cur.Type == IS {
		p.next()
		not := p.match(NOT)
		if _, err := p.expect(NULL); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	// [NOT] IN (...)
	not := false
	if p.cur.Type == NOT {
		// peek ahead: NOT IN, NOT BETWEEN, NOT LIKE
		saved := *p
		p.next()
		switch p.cur.Type {
		case IN:
			not = true
			// fall through to IN handling
		case BETWEEN:
			not = true
			// fall through to BETWEEN handling
		case LIKE:
			not = true
			// fall through to LIKE handling
		default:
			// restore parser state — this NOT belongs to a higher precedence
			*p = saved
			return left, nil
		}
	}

	switch p.cur.Type {
	case IN:
		p.next()
		if _, err := p.expect(LPAREN); err != nil {
			return nil, err
		}
		vals, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return &InExpr{Expr: left, Values: vals, Not: not}, nil
	case BETWEEN:
		p.next()
		low, err := p.parseConcatenation()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(AND); err != nil {
			return nil, err
		}
		high, err := p.parseConcatenation()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high, Not: not}, nil
	case LIKE:
		p.next()
		pattern, err := p.parseConcatenation()
		if err != nil {
			return nil, err
		}
		op := OpLike
		expr := &BinaryExpr{Left: left, Op: op, Right: pattern}
		if not {
			return &UnaryExpr{Op: OpNot, Expr: expr}, nil
		}
		return expr, nil
	}

	// comparison operators
	var op BinaryOp
	switch p.cur.Type {
	case EQ:
		op = OpEq
	case NEQ:
		op = OpNeq
	case LT:
		op = OpLt
	case GT:
		op = OpGt
	case LTE:
		op = OpLte
	case GTE:
		op = OpGte
	default:
		return left, nil
	}
	p.next()
	right, err := p.parseConcatenation()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *parser) parseConcatenation() (Expr, error) {
	left, err := p.parseAddition()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == CONCAT {
		p.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: OpConcat, Right: right}
	}
	return left, nil
}

func (p *parser) parseAddition() (Expr, error) {
	left, err := p.parseMultiplication()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == PLUS || p.cur.Type == MINUS {
		op := OpAdd
		if p.cur.Type == MINUS {
			op = OpSub
		}
		p.next()
		right, err := p.parseMultiplication()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

func (p *parser) parseMultiplication() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == STAR || p.cur.Type == SLASH {
		op := OpMul
		if p.cur.Type == SLASH {
			op = OpDiv
		}
		p.next()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Expr, error) {
	if p.cur.Type == MINUS {
		p.next()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: OpNeg, Expr: expr}, nil
	}
	if p.cur.Type == PLUS {
		p.next()
		return p.parseUnary()
	}
	return p.parsePrimary()
}

// -----------------------------------------------------------------------
// Primary expressions
// -----------------------------------------------------------------------

func (p *parser) parsePrimary() (Expr, error) {
	switch p.cur.Type {
	case NUMBER:
		tok := p.cur
		p.next()
		return &NumberLit{Value: tok.Literal}, nil

	case STRING:
		tok := p.cur
		p.next()
		return &StringLit{Value: tok.Literal}, nil

	case NULL:
		p.next()
		return &NullLit{}, nil

	case BIND:
		tok := p.cur
		p.next()
		return &BindExpr{Name: tok.Literal}, nil

	case STAR:
		p.next()
		return &StarExpr{}, nil

	case ROWNUM:
		p.next()
		return &RowNumExpr{}, nil

	case SYSDATE:
		p.next()
		return &SysDateExpr{}, nil

	case SYSTIMESTAMP:
		p.next()
		return &SysTimestampExpr{}, nil

	case EXISTS:
		p.next()
		if _, err := p.expect(LPAREN); err != nil {
			return nil, err
		}
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return &ExistsExpr{Query: sel}, nil

	case CASE:
		return p.parseCaseExpr()

	case NVL:
		return p.parseNVL()

	case NVL2:
		return p.parseNVL2()

	case DECODE:
		return p.parseDecode()

	case TO_CHAR, TO_DATE, TO_NUMBER:
		return p.parseOracleFunc()

	case LPAREN:
		p.next()
		if p.cur.Type == SELECT {
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(RPAREN); err != nil {
				return nil, err
			}
			return &SubqueryExpr{Query: sel}, nil
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return &ParenExpr{Expr: expr}, nil

	case IDENT:
		return p.parseIdentExpr()

	default:
		return nil, p.errorf("unexpected token %s in expression", p.cur)
	}
}

// parseIdentExpr handles identifiers, which can be column references,
// function calls, sequence operations, or qualified table.column references.
func (p *parser) parseIdentExpr() (Expr, error) {
	tok := p.cur
	p.next()

	// check for function call: ident(...)
	if p.cur.Type == LPAREN {
		return p.parseFuncCall(tok.Literal)
	}

	// check for dot: could be table.column, table.*, or seq.NEXTVAL/CURRVAL
	if p.cur.Type == DOT {
		p.next()

		// seq.NEXTVAL / seq.CURRVAL
		if p.cur.Type == NEXTVAL {
			p.next()
			return &SequenceExpr{Sequence: tok.Literal, Op: SeqNextVal}, nil
		}
		if p.cur.Type == CURRVAL {
			p.next()
			return &SequenceExpr{Sequence: tok.Literal, Op: SeqCurrVal}, nil
		}

		// table.*
		if p.cur.Type == STAR {
			p.next()
			return &StarExpr{Table: tok.Literal}, nil
		}

		// table.column
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &ColumnRefExpr{Table: tok.Literal, Column: col.Literal}, nil
	}

	return &ColumnRefExpr{Column: tok.Literal}, nil
}

func (p *parser) parseFuncCall(name string) (Expr, error) {
	if _, err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	var args []Expr
	if p.cur.Type != RPAREN {
		var err error
		args, err = p.parseExprList()
		if err != nil {
			return nil, err
		}
	}
	if _, err := p.expect(RPAREN); err != nil {
		return nil, err
	}
	return &FuncCallExpr{Name: name, Args: args}, nil
}

// -----------------------------------------------------------------------
// Oracle-specific function parsing
// -----------------------------------------------------------------------

func (p *parser) parseNVL() (Expr, error) {
	p.next() // consume NVL
	if _, err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(COMMA); err != nil {
		return nil, err
	}
	def, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(RPAREN); err != nil {
		return nil, err
	}
	return &NVLExpr{Expr: expr, Default: def}, nil
}

func (p *parser) parseNVL2() (Expr, error) {
	p.next() // consume NVL2
	if _, err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(COMMA); err != nil {
		return nil, err
	}
	notNull, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(COMMA); err != nil {
		return nil, err
	}
	nullVal, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(RPAREN); err != nil {
		return nil, err
	}
	return &NVL2Expr{Expr: expr, NotNullVal: notNull, NullVal: nullVal}, nil
}

func (p *parser) parseDecode() (Expr, error) {
	p.next() // consume DECODE
	if _, err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	// parse pairs: search, result, search, result, ..., [default]
	var args []Expr
	for p.match(COMMA) {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	if _, err := p.expect(RPAREN); err != nil {
		return nil, err
	}

	// build pairs and optional default
	decode := &DecodeExpr{Expr: expr}
	for i := 0; i+1 < len(args); i += 2 {
		decode.Pairs = append(decode.Pairs, DecodePair{
			Search: args[i],
			Result: args[i+1],
		})
	}
	if len(args)%2 == 1 {
		decode.Default = args[len(args)-1]
	}

	return decode, nil
}

func (p *parser) parseOracleFunc() (Expr, error) {
	name := p.cur.Literal
	p.next()
	return p.parseFuncCall(name)
}

// -----------------------------------------------------------------------
// CASE expression
// -----------------------------------------------------------------------

func (p *parser) parseCaseExpr() (Expr, error) {
	if _, err := p.expect(CASE); err != nil {
		return nil, err
	}
	c := &CaseExpr{}

	// simple CASE (with operand) vs searched CASE
	if p.cur.Type != WHEN {
		operand, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Operand = operand
	}

	for p.cur.Type == WHEN {
		p.next()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(THEN); err != nil {
			return nil, err
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, CaseWhen{Cond: cond, Result: result})
	}

	if p.match(ELSE) {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Else = e
	}

	if _, err := p.expect(END); err != nil {
		return nil, err
	}
	return c, nil
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func (p *parser) parseExprList() ([]Expr, error) {
	var exprs []Expr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if !p.match(COMMA) {
			break
		}
	}
	return exprs, nil
}

func (p *parser) parseIdentList() ([]string, error) {
	var idents []string
	for {
		tok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		idents = append(idents, tok.Literal)
		if !p.match(COMMA) {
			break
		}
	}
	return idents, nil
}

// expectIdent consumes an IDENT token or a keyword that can be used as an
// identifier in certain positions (e.g., column names that clash with keywords).
func (p *parser) expectIdent() (Token, error) {
	if p.cur.Type == IDENT {
		tok := p.cur
		p.next()
		return tok, nil
	}
	// allow certain keywords as identifiers
	if p.cur.Type > keywordStart && p.cur.Type < keywordEnd {
		tok := p.cur
		tok.Type = IDENT
		p.next()
		return tok, nil
	}
	return Token{}, p.errorf("expected identifier, got %s", p.cur)
}

// Ensure Statement implementations.
var (
	_ Statement = (*SelectStmt)(nil)
	_ Statement = (*InsertStmt)(nil)
	_ Statement = (*UpdateStmt)(nil)
	_ Statement = (*DeleteStmt)(nil)
	_ Statement = (*CreateSequenceStmt)(nil)
)

// Ensure Expr implementations.
var (
	_ Expr = (*ColumnRefExpr)(nil)
	_ Expr = (*NumberLit)(nil)
	_ Expr = (*StringLit)(nil)
	_ Expr = (*NullLit)(nil)
	_ Expr = (*BindExpr)(nil)
	_ Expr = (*StarExpr)(nil)
	_ Expr = (*BinaryExpr)(nil)
	_ Expr = (*UnaryExpr)(nil)
	_ Expr = (*IsNullExpr)(nil)
	_ Expr = (*InExpr)(nil)
	_ Expr = (*BetweenExpr)(nil)
	_ Expr = (*ExistsExpr)(nil)
	_ Expr = (*SubqueryExpr)(nil)
	_ Expr = (*FuncCallExpr)(nil)
	_ Expr = (*CaseExpr)(nil)
	_ Expr = (*ParenExpr)(nil)
	_ Expr = (*RowNumExpr)(nil)
	_ Expr = (*SysDateExpr)(nil)
	_ Expr = (*SysTimestampExpr)(nil)
	_ Expr = (*SequenceExpr)(nil)
	_ Expr = (*NVLExpr)(nil)
	_ Expr = (*NVL2Expr)(nil)
	_ Expr = (*DecodeExpr)(nil)
)

// Verify unused variable to satisfy import.
var _ = fmt.Sprintf
