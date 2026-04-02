// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package parser implements a recursive descent parser for a subset of the
// T-SQL / Sybase SQL dialect. Phase 1 covers USE, CREATE TABLE, INSERT INTO,
// SELECT (with WHERE, ORDER BY, TOP), GO batch separators, and basic
// expressions including Sybase-specific constructs like bracket-quoted
// identifiers, ISNULL(), CONVERT(), and GETDATE().
//
// T-SQL is case-insensitive for keywords; the lexer normalizes keywords
// to upper case for matching but preserves original casing in identifiers.
package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse parses T-SQL input into a Batch of statements. The input may contain
// multiple statements separated by semicolons or the GO batch separator.
// GO terminates the current batch and starts a new one, but for simplicity
// this parser returns all statements in a single Batch.
func Parse(input string) (*Batch, error) {
	p := &parser{lex: newLexer(input)}
	return p.parseBatch()
}

type parser struct {
	lex *lexer
}

// parseBatch parses statements until EOF. Statements are separated by
// semicolons or GO batch separators.
func (p *parser) parseBatch() (*Batch, error) {
	batch := &Batch{}
	for {
		// Skip semicolons and GO between statements.
		for p.lex.peek().typ == tokenSemicolon || p.lex.peek().typ == tokenGO {
			p.lex.next()
		}
		if p.lex.peek().typ == tokenEOF {
			break
		}
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		batch.Stmts = append(batch.Stmts, stmt)
	}
	return batch, nil
}

// parseStatement dispatches to the appropriate statement parser based on the
// next token.
func (p *parser) parseStatement() (Statement, error) {
	tok := p.lex.peek()
	switch tok.typ {
	case tokenUSE:
		return p.parseUse()
	case tokenCREATE:
		return p.parseCreate()
	case tokenINSERT:
		return p.parseInsert()
	case tokenSELECT:
		return p.parseSelect()
	default:
		return nil, p.error(fmt.Sprintf("unexpected token %q at position %d", tok.val, tok.pos))
	}
}

// parseUse parses: USE <database>
func (p *parser) parseUse() (*UseStmt, error) {
	p.lex.next() // consume USE
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &UseStmt{Database: name}, nil
}

// parseCreate dispatches CREATE TABLE or CREATE DATABASE.
func (p *parser) parseCreate() (Statement, error) {
	p.lex.next() // consume CREATE
	switch p.lex.peek().typ {
	case tokenTABLE:
		p.lex.next() // consume TABLE
		return p.parseCreateTable()
	case tokenDATABASE:
		p.lex.next() // consume DATABASE
		return p.parseCreateDatabase()
	default:
		return nil, p.error(fmt.Sprintf(
			"expected TABLE or DATABASE after CREATE, got %q at position %d",
			p.lex.peek().val, p.lex.peek().pos))
	}
}

// parseCreateDatabase parses: CREATE DATABASE <name>
func (p *parser) parseCreateDatabase() (*CreateDatabaseStmt, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &CreateDatabaseStmt{Database: name}, nil
}

// parseCreateTable parses: CREATE TABLE <name> (<col_defs>)
func (p *parser) parseCreateTable() (*CreateTableStmt, error) {
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	var cols []ColumnDef
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next() // consume comma
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return &CreateTableStmt{Table: name, Columns: cols}, nil
}

// parseColumnDef parses: <name> <type>[(<args>)] [NULL | NOT NULL]
func (p *parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.expectIdent()
	if err != nil {
		return ColumnDef{}, err
	}
	dataType, err := p.parseDataType()
	if err != nil {
		return ColumnDef{}, err
	}
	col := ColumnDef{Name: name, DataType: dataType}

	// Parse optional NULL / NOT NULL.
	if p.lex.peek().typ == tokenNOT {
		p.lex.next()
		if err := p.expect(tokenNULL); err != nil {
			return ColumnDef{}, err
		}
		f := false
		col.Nullable = &f
	} else if p.lex.peek().typ == tokenNULL {
		p.lex.next()
		t := true
		col.Nullable = &t
	}
	return col, nil
}

// parseDataType parses a SQL data type, including types with parenthesized
// arguments like VARCHAR(255) or DECIMAL(10, 2).
func (p *parser) parseDataType() (string, error) {
	name, err := p.expectIdent()
	if err != nil {
		return "", err
	}
	typeName := strings.ToUpper(name)

	// Check for parenthesized arguments.
	if p.lex.peek().typ == tokenLParen {
		p.lex.next() // consume (
		var args []string
		for {
			tok := p.lex.next()
			if tok.typ == tokenInt || tok.typ == tokenIdent {
				args = append(args, tok.val)
			} else {
				return "", p.error(fmt.Sprintf("unexpected %q in data type arguments", tok.val))
			}
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next() // consume comma
		}
		if err := p.expect(tokenRParen); err != nil {
			return "", err
		}
		return fmt.Sprintf("%s(%s)", typeName, strings.Join(args, ", ")), nil
	}
	return typeName, nil
}

// parseInsert parses: INSERT [INTO] <table> [(<columns>)] VALUES (<values>), ...
func (p *parser) parseInsert() (*InsertStmt, error) {
	p.lex.next() // consume INSERT
	// INTO is optional in T-SQL.
	if p.lex.peek().typ == tokenINTO {
		p.lex.next()
	}
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: name}

	// Optional column list.
	if p.lex.peek().typ == tokenLParen {
		p.lex.next() // consume (
		for {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next()
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
	}

	if err := p.expect(tokenVALUES); err != nil {
		return nil, err
	}

	// Parse value rows.
	for {
		row, err := p.parseValueRow()
		if err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, row)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next()
	}
	return stmt, nil
}

// parseValueRow parses: (<expr>, <expr>, ...)
func (p *parser) parseValueRow() ([]Expr, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	var exprs []Expr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next()
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return exprs, nil
}

// parseSelect parses: SELECT [TOP n] <columns> [FROM <tables>]
// [WHERE <expr>] [ORDER BY <order_exprs>]
func (p *parser) parseSelect() (*SelectStmt, error) {
	p.lex.next() // consume SELECT
	stmt := &SelectStmt{}

	// Optional TOP n.
	if p.lex.peek().typ == tokenTOP {
		p.lex.next()
		tok := p.lex.next()
		if tok.typ != tokenInt {
			return nil, p.error(fmt.Sprintf("expected integer after TOP, got %q", tok.val))
		}
		n, err := strconv.Atoi(tok.val)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid TOP value: %s", tok.val))
		}
		stmt.Top = &n
	}

	// Parse select columns.
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next()
	}

	// Optional FROM.
	if p.lex.peek().typ == tokenFROM {
		p.lex.next()
		for {
			ref, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			stmt.From = append(stmt.From, ref)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next()
		}
	}

	// Optional WHERE.
	if p.lex.peek().typ == tokenWHERE {
		p.lex.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Optional ORDER BY.
	if p.lex.peek().typ == tokenORDER {
		p.lex.next()
		if err := p.expect(tokenBY); err != nil {
			return nil, err
		}
		for {
			ob, err := p.parseOrderByExpr()
			if err != nil {
				return nil, err
			}
			stmt.OrderBy = append(stmt.OrderBy, ob)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next()
		}
	}

	return stmt, nil
}

// parseSelectColumn parses a select column: <expr> [AS <alias>] or *.
func (p *parser) parseSelectColumn() (SelectColumn, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return SelectColumn{}, err
	}
	var alias string
	if p.lex.peek().typ == tokenAS {
		p.lex.next()
		alias, err = p.expectIdent()
		if err != nil {
			return SelectColumn{}, err
		}
	}
	return SelectColumn{Expr: expr, Alias: alias}, nil
}

// parseTableRef parses a table reference: <name> [<alias>]
func (p *parser) parseTableRef() (TableRef, error) {
	name, err := p.parseTableName()
	if err != nil {
		return TableRef{}, err
	}
	ref := TableRef{Name: name}
	// Check for optional alias (identifier that isn't a keyword like WHERE,
	// ORDER, etc.).
	if p.lex.peek().typ == tokenIdent || p.lex.peek().typ == tokenAS {
		if p.lex.peek().typ == tokenAS {
			p.lex.next() // consume AS
		}
		alias, err := p.expectIdent()
		if err != nil {
			return TableRef{}, err
		}
		ref.Alias = alias
	}
	return ref, nil
}

// parseTableName parses a possibly dotted table name like "dbo.users".
func (p *parser) parseTableName() (string, error) {
	name, err := p.expectIdent()
	if err != nil {
		return "", err
	}
	for p.lex.peek().typ == tokenDot {
		p.lex.next()
		part, err := p.expectIdent()
		if err != nil {
			return "", err
		}
		name = name + "." + part
	}
	return name, nil
}

// parseOrderByExpr parses: <expr> [ASC|DESC]
func (p *parser) parseOrderByExpr() (OrderByExpr, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return OrderByExpr{}, err
	}
	ob := OrderByExpr{Expr: expr}
	if p.lex.peek().typ == tokenASC {
		p.lex.next()
	} else if p.lex.peek().typ == tokenDESC {
		p.lex.next()
		ob.Desc = true
	}
	return ob, nil
}

// Expression parsing with precedence climbing.
//
// Precedence (lowest to highest):
//   1. OR
//   2. AND
//   3. NOT (unary)
//   4. Comparison: =, <>, !=, <, >, <=, >=, IS [NOT] NULL, IN, BETWEEN, LIKE
//   5. Addition: +, -
//   6. Multiplication: *, /, %
//   7. Unary: -, NOT
//   8. Primary: literals, identifiers, function calls, parenthesized exprs

// parseExpr parses an expression starting at the lowest precedence (OR).
func (p *parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.lex.peek().typ == tokenOR {
		p.lex.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.lex.peek().typ == tokenAND {
		p.lex.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.lex.peek().typ == tokenNOT {
		p.lex.next()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (Expr, error) {
	left, err := p.parseAddition()
	if err != nil {
		return nil, err
	}

	switch p.lex.peek().typ {
	case tokenEq:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "=", Right: right}, nil
	case tokenNeq:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "<>", Right: right}, nil
	case tokenLT:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "<", Right: right}, nil
	case tokenGT:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: ">", Right: right}, nil
	case tokenLTE:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "<=", Right: right}, nil
	case tokenGTE:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: ">=", Right: right}, nil
	case tokenIS:
		p.lex.next()
		if p.lex.peek().typ == tokenNOT {
			p.lex.next()
			if err := p.expect(tokenNULL); err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: "IS NOT", Right: &NullLit{}}, nil
		}
		if err := p.expect(tokenNULL); err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "IS", Right: &NullLit{}}, nil
	case tokenLIKE:
		p.lex.next()
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "LIKE", Right: right}, nil
	case tokenNOT:
		// Handle NOT LIKE, NOT IN, NOT BETWEEN
		p.lex.next()
		switch p.lex.peek().typ {
		case tokenLIKE:
			p.lex.next()
			right, err := p.parseAddition()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: "NOT LIKE", Right: right}, nil
		default:
			return nil, p.error("expected LIKE, IN, or BETWEEN after NOT")
		}
	}
	return left, nil
}

func (p *parser) parseAddition() (Expr, error) {
	left, err := p.parseMultiplication()
	if err != nil {
		return nil, err
	}
	for p.lex.peek().typ == tokenPlus || p.lex.peek().typ == tokenMinus {
		tok := p.lex.next()
		right, err := p.parseMultiplication()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: tok.val, Right: right}
	}
	return left, nil
}

func (p *parser) parseMultiplication() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.lex.peek().typ == tokenStar || p.lex.peek().typ == tokenSlash || p.lex.peek().typ == tokenPercent {
		tok := p.lex.next()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: tok.val, Right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Expr, error) {
	if p.lex.peek().typ == tokenMinus {
		p.lex.next()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "-", Expr: expr}, nil
	}
	return p.parsePrimary()
}

// parsePrimary parses primary expressions: literals, identifiers, function
// calls, and parenthesized expressions.
func (p *parser) parsePrimary() (Expr, error) {
	tok := p.lex.peek()

	switch tok.typ {
	case tokenInt:
		p.lex.next()
		val, err := strconv.ParseInt(tok.val, 10, 64)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid integer: %s", tok.val))
		}
		return &IntLit{Value: val}, nil

	case tokenFloat:
		p.lex.next()
		val, err := strconv.ParseFloat(tok.val, 64)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid float: %s", tok.val))
		}
		return &FloatLit{Value: val}, nil

	case tokenString:
		p.lex.next()
		return &StringLit{Value: tok.val}, nil

	case tokenNULL:
		p.lex.next()
		return &NullLit{}, nil

	case tokenStar:
		p.lex.next()
		return &StarExpr{}, nil

	case tokenLParen:
		p.lex.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &ParenExpr{Expr: expr}, nil

	case tokenGETDATE:
		p.lex.next()
		if err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &FuncCallExpr{Name: "GETDATE"}, nil

	case tokenISNULL:
		return p.parseISNULL()

	case tokenCONVERT:
		return p.parseCONVERT()

	case tokenIdent:
		return p.parseIdentOrFunc()

	default:
		return nil, p.error(fmt.Sprintf("unexpected token %q at position %d", tok.val, tok.pos))
	}
}

// parseISNULL parses: ISNULL(<expr>, <expr>)
func (p *parser) parseISNULL() (Expr, error) {
	p.lex.next() // consume ISNULL
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	arg1, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenComma); err != nil {
		return nil, err
	}
	arg2, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return &FuncCallExpr{Name: "ISNULL", Args: []Expr{arg1, arg2}}, nil
}

// parseCONVERT parses: CONVERT(<type>, <expr>[, <style>])
func (p *parser) parseCONVERT() (Expr, error) {
	p.lex.next() // consume CONVERT
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	dataType, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenComma); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	conv := &ConvertExpr{DataType: dataType, Expr: expr}
	if p.lex.peek().typ == tokenComma {
		p.lex.next()
		style, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		conv.Style = style
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return conv, nil
}

// parseIdentOrFunc parses an identifier, a dotted identifier (a.b.c), or a
// function call (func(args)).
func (p *parser) parseIdentOrFunc() (Expr, error) {
	tok := p.lex.next()
	name := tok.val

	// Check if this is a function call.
	if p.lex.peek().typ == tokenLParen {
		p.lex.next() // consume (
		var args []Expr
		if p.lex.peek().typ != tokenRParen {
			for {
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				if p.lex.peek().typ != tokenComma {
					break
				}
				p.lex.next()
			}
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &FuncCallExpr{Name: name, Args: args}, nil
	}

	// Parse dotted identifier (e.g., dbo.users.name).
	parts := []string{name}
	for p.lex.peek().typ == tokenDot {
		p.lex.next() // consume .
		// After a dot we might see * (as in t.*)
		if p.lex.peek().typ == tokenStar {
			p.lex.next()
			parts = append(parts, "*")
			break
		}
		part, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return &IdentExpr{Parts: parts}, nil
}

// expect consumes the next token and returns an error if it doesn't match the
// expected type.
func (p *parser) expect(typ tokenType) error {
	tok := p.lex.next()
	if tok.typ != typ {
		return p.error(fmt.Sprintf("expected %s, got %q at position %d", tokenName(typ), tok.val, tok.pos))
	}
	return nil
}

// expectIdent consumes the next token and returns its value if it's an
// identifier. Keywords that can also be used as identifiers are accepted.
func (p *parser) expectIdent() (string, error) {
	tok := p.lex.next()
	if tok.typ == tokenIdent {
		return tok.val, nil
	}
	// Many T-SQL keywords can be used as identifiers in certain contexts
	// (e.g., column named "order", table named "select"). Allow keyword
	// tokens to be treated as identifiers.
	if isKeywordToken(tok.typ) {
		return tok.val, nil
	}
	return "", p.error(fmt.Sprintf("expected identifier, got %q at position %d", tok.val, tok.pos))
}

func (p *parser) error(msg string) error {
	return fmt.Errorf("tsql parse error: %s", msg)
}

func isKeywordToken(typ tokenType) bool {
	switch typ {
	case tokenUSE, tokenCREATE, tokenTABLE, tokenDATABASE, tokenINSERT,
		tokenINTO, tokenVALUES, tokenSELECT, tokenFROM, tokenWHERE,
		tokenORDER, tokenBY, tokenTOP, tokenAS, tokenNOT, tokenNULL,
		tokenAND, tokenOR, tokenASC, tokenDESC, tokenGO, tokenIS, tokenIN,
		tokenBETWEEN, tokenLIKE, tokenISNULL, tokenCONVERT, tokenGETDATE:
		return true
	}
	return false
}

func tokenName(typ tokenType) string {
	switch typ {
	case tokenEOF:
		return "EOF"
	case tokenIdent:
		return "identifier"
	case tokenInt:
		return "integer"
	case tokenFloat:
		return "float"
	case tokenString:
		return "string"
	case tokenLParen:
		return "'('"
	case tokenRParen:
		return "')'"
	case tokenComma:
		return "','"
	case tokenDot:
		return "'.'"
	case tokenSemicolon:
		return "';'"
	case tokenStar:
		return "'*'"
	case tokenEq:
		return "'='"
	case tokenNULL:
		return "NULL"
	case tokenTABLE:
		return "TABLE"
	case tokenBY:
		return "BY"
	case tokenVALUES:
		return "VALUES"
	default:
		return fmt.Sprintf("token(%d)", typ)
	}
}

// ParseExpr parses a single T-SQL expression. This is exported for use by the
// semantic translator when it needs to parse expression fragments.
func ParseExpr(input string) (Expr, error) {
	p := &parser{lex: newLexer(input)}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.lex.peek().typ != tokenEOF {
		return nil, p.error(fmt.Sprintf(
			"unexpected trailing input at position %d: %q",
			p.lex.peek().pos, p.lex.peek().val,
		))
	}
	return expr, nil
}

// FormatBatch formats a batch of statements as a T-SQL string with GO
// separators.
func FormatBatch(b *Batch) string {
	var parts []string
	for _, s := range b.Stmts {
		parts = append(parts, s.String())
	}
	return strings.Join(parts, "\nGO\n")
}
