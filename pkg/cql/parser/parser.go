// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// Parse parses a single CQL statement and returns the corresponding AST node.
// The input may optionally end with a semicolon.
func Parse(input string) (Statement, error) {
	l, err := newLexer(input)
	if err != nil {
		return nil, err
	}
	p := &parser{lex: l}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// Allow optional trailing semicolon.
	if p.lex.peek().kind == tokSemicolon {
		p.lex.next()
	}
	if t := p.lex.peek(); t.kind != tokEOF {
		return nil, p.errorf("unexpected token %q after statement", t.val)
	}
	return stmt, nil
}

type parser struct {
	lex *lexer
}

func (p *parser) errorf(format string, args ...interface{}) error {
	t := p.lex.peek()
	return fmt.Errorf("at position %d: %s", t.pos, fmt.Sprintf(format, args...))
}

func (p *parser) parseStatement() (Statement, error) {
	t := p.lex.peek()
	if t.kind != tokIdent {
		return nil, p.errorf("expected statement keyword, got %q", t.val)
	}
	switch strings.ToUpper(t.val) {
	case "USE":
		return p.parseUse()
	case "CREATE":
		return p.parseCreate()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	default:
		return nil, p.errorf("unsupported statement %q", t.val)
	}
}

// parseUse parses: USE <keyspace>
func (p *parser) parseUse() (*UseStatement, error) {
	p.lex.next() // consume USE
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &UseStatement{Keyspace: name}, nil
}

// parseCreate dispatches CREATE KEYSPACE and CREATE TABLE.
func (p *parser) parseCreate() (Statement, error) {
	p.lex.next() // consume CREATE
	t := p.lex.peek()
	switch strings.ToUpper(t.val) {
	case "KEYSPACE":
		return p.parseCreateKeyspace()
	case "TABLE":
		return p.parseCreateTable()
	default:
		return nil, p.errorf("expected KEYSPACE or TABLE after CREATE, got %q", t.val)
	}
}

// parseCreateKeyspace parses:
//
//	CREATE KEYSPACE [IF NOT EXISTS] <name>
//	  WITH replication = { 'key': 'val', ... }
//	  [AND durable_writes = true|false]
func (p *parser) parseCreateKeyspace() (*CreateKeyspaceStatement, error) {
	p.lex.next() // consume KEYSPACE
	stmt := &CreateKeyspaceStatement{}

	// Optional IF NOT EXISTS.
	stmt.IfNotExists = p.tryIfNotExists()

	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt.Keyspace = name

	// WITH replication = { ... }
	if err := p.expectKeyword("WITH"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("replication"); err != nil {
		return nil, err
	}
	if err := p.expectToken(tokEq); err != nil {
		return nil, err
	}
	m, err := p.parseMapLiteral()
	if err != nil {
		return nil, err
	}
	stmt.Replication = m

	// Optional AND durable_writes = <bool>.
	if isKeyword(p.lex.peek(), "AND") {
		p.lex.next()
		if err := p.expectKeyword("durable_writes"); err != nil {
			return nil, err
		}
		if err := p.expectToken(tokEq); err != nil {
			return nil, err
		}
		b, err := p.parseBoolValue()
		if err != nil {
			return nil, err
		}
		stmt.DurableWrites = &b
	}

	return stmt, nil
}

// parseCreateTable parses:
//
//	CREATE TABLE [IF NOT EXISTS] [<ks>.]<table> (
//	  <col> <type>, ...,
//	  PRIMARY KEY ((<pk>, ...), <ck>, ...)
//	)
func (p *parser) parseCreateTable() (*CreateTableStatement, error) {
	p.lex.next() // consume TABLE
	stmt := &CreateTableStatement{}

	stmt.IfNotExists = p.tryIfNotExists()

	// Table name, optionally qualified.
	ks, tbl, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	stmt.Keyspace = ks
	stmt.Table = tbl

	if err := p.expectToken(tokLParen); err != nil {
		return nil, err
	}

	// Parse column definitions and the PRIMARY KEY clause.
	// CQL supports two forms:
	//   1. Inline: id uuid PRIMARY KEY, name text
	//   2. Trailing: id uuid, name text, PRIMARY KEY (id)
	var inlinePKColumn string
	for p.lex.peek().kind != tokRParen {
		if isKeyword(p.lex.peek(), "PRIMARY") {
			pk, err := p.parsePrimaryKeyClause()
			if err != nil {
				return nil, err
			}
			stmt.PrimaryKey = pk
			break
		}
		col, isPK, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)
		if isPK {
			inlinePKColumn = col.Name
		}

		if p.lex.peek().kind == tokComma {
			p.lex.next()
		}
	}

	// If we found an inline PRIMARY KEY, use it.
	if inlinePKColumn != "" {
		stmt.PrimaryKey = PrimaryKey{
			PartitionKeys: []string{inlinePKColumn},
		}
	}

	if err := p.expectToken(tokRParen); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseInsert parses:
//
//	INSERT INTO [<ks>.]<table> (<cols>) VALUES (<vals>) [IF NOT EXISTS]
func (p *parser) parseInsert() (*InsertStatement, error) {
	p.lex.next() // consume INSERT
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	stmt := &InsertStatement{}

	ks, tbl, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	stmt.Keyspace = ks
	stmt.Table = tbl

	// Column list.
	if err := p.expectToken(tokLParen); err != nil {
		return nil, err
	}
	cols, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols
	if err := p.expectToken(tokRParen); err != nil {
		return nil, err
	}

	// VALUES.
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	if err := p.expectToken(tokLParen); err != nil {
		return nil, err
	}
	vals, err := p.parseExprList()
	if err != nil {
		return nil, err
	}
	stmt.Values = vals
	if err := p.expectToken(tokRParen); err != nil {
		return nil, err
	}

	// Optional IF NOT EXISTS.
	stmt.IfNotExists = p.tryIfNotExists()

	return stmt, nil
}

// parseSelect parses:
//
//	SELECT [DISTINCT] <selectors> FROM [<ks>.]<table> [WHERE <conds>]
//	  [ORDER BY <col> [ASC|DESC], ...] [LIMIT <n>] [ALLOW FILTERING]
func (p *parser) parseSelect() (*SelectStatement, error) {
	p.lex.next() // consume SELECT
	stmt := &SelectStatement{}

	// Optional DISTINCT.
	if isKeyword(p.lex.peek(), "DISTINCT") {
		p.lex.next()
		stmt.Distinct = true
	}

	// Selectors.
	selectors, err := p.parseSelectors()
	if err != nil {
		return nil, err
	}
	stmt.Columns = selectors

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	ks, tbl, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	stmt.Keyspace = ks
	stmt.Table = tbl

	// Optional WHERE.
	if isKeyword(p.lex.peek(), "WHERE") {
		p.lex.next()
		where, err := p.parseWhereClauses()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Optional ORDER BY.
	if isKeyword(p.lex.peek(), "ORDER") {
		p.lex.next() // consume ORDER
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderByClauses()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Optional LIMIT.
	if isKeyword(p.lex.peek(), "LIMIT") {
		p.lex.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Limit = expr
	}

	// Optional ALLOW FILTERING (accepted and ignored — CRDB performs
	// full scans as needed).
	if isKeyword(p.lex.peek(), "ALLOW") {
		p.lex.next() // consume ALLOW
		if err := p.expectKeyword("FILTERING"); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

// parseUpdate parses:
//
//	UPDATE [<ks>.]<table> SET <col> = <val>, ... WHERE <conds>
//	  [IF EXISTS | IF <conds>]
func (p *parser) parseUpdate() (*UpdateStatement, error) {
	p.lex.next() // consume UPDATE
	stmt := &UpdateStatement{}

	ks, tbl, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	stmt.Keyspace = ks
	stmt.Table = tbl

	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}

	// Parse SET assignments: col = val [, col = val, ...]
	assignments, err := p.parseAssignments()
	if err != nil {
		return nil, err
	}
	stmt.Assignments = assignments

	// WHERE clause.
	if err := p.expectKeyword("WHERE"); err != nil {
		return nil, err
	}
	where, err := p.parseWhereClauses()
	if err != nil {
		return nil, err
	}
	stmt.Where = where

	// Optional IF EXISTS or IF conditions.
	if isKeyword(p.lex.peek(), "IF") {
		p.lex.next() // consume IF
		if isKeyword(p.lex.peek(), "EXISTS") {
			p.lex.next() // consume EXISTS
			stmt.IfExists = true
		} else {
			conds, err := p.parseIfConditions()
			if err != nil {
				return nil, err
			}
			stmt.IfConds = conds
		}
	}

	return stmt, nil
}

// parseDelete parses:
//
//	DELETE FROM [<ks>.]<table> WHERE <conds> [IF EXISTS | IF <conds>]
func (p *parser) parseDelete() (*DeleteStatement, error) {
	p.lex.next() // consume DELETE

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	stmt := &DeleteStatement{}

	ks, tbl, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	stmt.Keyspace = ks
	stmt.Table = tbl

	// WHERE clause.
	if err := p.expectKeyword("WHERE"); err != nil {
		return nil, err
	}
	where, err := p.parseWhereClauses()
	if err != nil {
		return nil, err
	}
	stmt.Where = where

	// Optional IF EXISTS or IF conditions.
	if isKeyword(p.lex.peek(), "IF") {
		p.lex.next() // consume IF
		if isKeyword(p.lex.peek(), "EXISTS") {
			p.lex.next() // consume EXISTS
			stmt.IfExists = true
		} else {
			conds, err := p.parseIfConditions()
			if err != nil {
				return nil, err
			}
			stmt.IfConds = conds
		}
	}

	return stmt, nil
}

// parseAssignments parses SET <col> = <val> [, <col> = <val>, ...].
func (p *parser) parseAssignments() ([]Assignment, error) {
	var assignments []Assignment
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(tokEq); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, Assignment{Column: col, Value: val})
		if p.lex.peek().kind != tokComma {
			break
		}
		p.lex.next() // consume comma
	}
	return assignments, nil
}

// parseIfConditions parses IF <col> <op> <val> [AND <col> <op> <val>, ...].
// This reuses the WHERE clause parser since CQL IF conditions have the same
// syntax as WHERE conditions.
func (p *parser) parseIfConditions() ([]WhereClause, error) {
	return p.parseWhereClauses()
}

// ---------------------------------------------------------------------------
// Helper parsers
// ---------------------------------------------------------------------------

func (p *parser) expectIdent() (string, error) {
	t := p.lex.next()
	if t.kind != tokIdent {
		return "", fmt.Errorf("at position %d: expected identifier, got %q", t.pos, t.val)
	}
	return t.val, nil
}

func (p *parser) expectKeyword(kw string) error {
	t := p.lex.next()
	if !isKeyword(t, kw) {
		return fmt.Errorf("at position %d: expected %s, got %q", t.pos, kw, t.val)
	}
	return nil
}

func (p *parser) expectToken(kind tokenKind) error {
	t := p.lex.next()
	if t.kind != kind {
		return fmt.Errorf("at position %d: expected %v, got %q", t.pos, kindName(kind), t.val)
	}
	return nil
}

// tryIfNotExists consumes IF NOT EXISTS if present, returning true.
func (p *parser) tryIfNotExists() bool {
	if !isKeyword(p.lex.peek(), "IF") {
		return false
	}
	// Save position to backtrack if it's not "IF NOT EXISTS".
	saved := p.lex.cur
	p.lex.next() // IF
	if !isKeyword(p.lex.peek(), "NOT") {
		p.lex.cur = saved
		return false
	}
	p.lex.next() // NOT
	if !isKeyword(p.lex.peek(), "EXISTS") {
		p.lex.cur = saved
		return false
	}
	p.lex.next() // EXISTS
	return true
}

// parseQualifiedName parses [<ks>.]<name> and returns (keyspace, name).
func (p *parser) parseQualifiedName() (string, string, error) {
	first, err := p.expectIdent()
	if err != nil {
		return "", "", err
	}
	if p.lex.peek().kind == tokDot {
		p.lex.next()
		second, err := p.expectIdent()
		if err != nil {
			return "", "", err
		}
		return first, second, nil
	}
	return "", first, nil
}

// parseMapLiteral parses { 'k': 'v', ... }.
func (p *parser) parseMapLiteral() (map[string]string, error) {
	if err := p.expectToken(tokLBrace); err != nil {
		return nil, err
	}
	m := make(map[string]string)
	for p.lex.peek().kind != tokRBrace {
		if len(m) > 0 {
			if err := p.expectToken(tokComma); err != nil {
				return nil, err
			}
		}
		kt := p.lex.next()
		if kt.kind != tokString {
			return nil, fmt.Errorf("at position %d: expected string key in map, got %q", kt.pos, kt.val)
		}
		if err := p.expectToken(tokColon); err != nil {
			return nil, err
		}
		vt := p.lex.next()
		if vt.kind != tokString {
			return nil, fmt.Errorf("at position %d: expected string value in map, got %q", vt.pos, vt.val)
		}
		m[kt.val] = vt.val
	}
	p.lex.next() // consume }
	return m, nil
}

func (p *parser) parseBoolValue() (bool, error) {
	t := p.lex.next()
	if t.kind != tokIdent {
		return false, fmt.Errorf("at position %d: expected boolean value, got %q", t.pos, t.val)
	}
	switch strings.ToUpper(t.val) {
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	default:
		return false, fmt.Errorf("at position %d: expected TRUE or FALSE, got %q", t.pos, t.val)
	}
}

// parseColumnDef parses <name> <type> [PRIMARY KEY]. The inline
// PRIMARY KEY is a Cassandra shorthand for single-partition-key
// tables.
func (p *parser) parseColumnDef() (ColumnDef, bool, error) {
	name, err := p.expectIdent()
	if err != nil {
		return ColumnDef{}, false, err
	}
	dt, err := p.parseDataType()
	if err != nil {
		return ColumnDef{}, false, err
	}
	// Check for inline PRIMARY KEY.
	isPK := false
	if isKeyword(p.lex.peek(), "PRIMARY") {
		saved := p.lex.cur
		p.lex.next() // PRIMARY
		if isKeyword(p.lex.peek(), "KEY") {
			p.lex.next() // KEY
			isPK = true
		} else {
			p.lex.cur = saved
		}
	}
	return ColumnDef{Name: name, DataType: dt}, isPK, nil
}

// parseDataType parses a CQL type name.
func (p *parser) parseDataType() (DataType, error) {
	t := p.lex.next()
	if t.kind != tokIdent {
		return DataType{}, fmt.Errorf("at position %d: expected data type, got %q", t.pos, t.val)
	}
	name := strings.ToLower(t.val)
	switch name {
	case "text", "varchar", "int", "bigint", "float", "double",
		"boolean", "timestamp", "uuid", "timeuuid", "blob",
		"inet", "counter", "ascii", "varint", "decimal":
		return DataType{Name: name}, nil
	default:
		return DataType{}, fmt.Errorf("at position %d: unsupported CQL type %q", t.pos, t.val)
	}
}

// parsePrimaryKeyClause parses PRIMARY KEY ((...), ...).
func (p *parser) parsePrimaryKeyClause() (PrimaryKey, error) {
	if err := p.expectKeyword("PRIMARY"); err != nil {
		return PrimaryKey{}, err
	}
	if err := p.expectKeyword("KEY"); err != nil {
		return PrimaryKey{}, err
	}
	if err := p.expectToken(tokLParen); err != nil {
		return PrimaryKey{}, err
	}

	pk := PrimaryKey{}

	if p.lex.peek().kind == tokLParen {
		// Composite partition key: ((pk1, pk2), ck1, ck2).
		p.lex.next() // consume inner (
		partKeys, err := p.parseIdentList()
		if err != nil {
			return PrimaryKey{}, err
		}
		pk.PartitionKeys = partKeys
		if err := p.expectToken(tokRParen); err != nil {
			return PrimaryKey{}, err
		}
		// Clustering keys follow after commas.
		for p.lex.peek().kind == tokComma {
			p.lex.next()
			ck, err := p.expectIdent()
			if err != nil {
				return PrimaryKey{}, err
			}
			pk.ClusteringKeys = append(pk.ClusteringKeys, ck)
		}
	} else {
		// Simple partition key: (pk, ck1, ck2).
		first, err := p.expectIdent()
		if err != nil {
			return PrimaryKey{}, err
		}
		pk.PartitionKeys = []string{first}
		for p.lex.peek().kind == tokComma {
			p.lex.next()
			ck, err := p.expectIdent()
			if err != nil {
				return PrimaryKey{}, err
			}
			pk.ClusteringKeys = append(pk.ClusteringKeys, ck)
		}
	}

	if err := p.expectToken(tokRParen); err != nil {
		return PrimaryKey{}, err
	}
	return pk, nil
}

// parseIdentList parses a comma-separated list of identifiers.
func (p *parser) parseIdentList() ([]string, error) {
	var result []string
	first, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	result = append(result, first)
	for p.lex.peek().kind == tokComma {
		p.lex.next()
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

// parseExprList parses a comma-separated list of expressions.
func (p *parser) parseExprList() ([]Expr, error) {
	var result []Expr
	first, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	result = append(result, first)
	for p.lex.peek().kind == tokComma {
		p.lex.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, nil
}

// parseExpr parses a single value expression (literal, bind marker, etc.).
func (p *parser) parseExpr() (Expr, error) {
	t := p.lex.peek()
	switch t.kind {
	case tokString:
		p.lex.next()
		return &StringLiteral{Value: t.val}, nil
	case tokInteger:
		p.lex.next()
		v, err := strconv.ParseInt(t.val, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "at position %d: invalid integer %q", t.pos, t.val)
		}
		return &IntegerLiteral{Value: v}, nil
	case tokFloat:
		p.lex.next()
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "at position %d: invalid float %q", t.pos, t.val)
		}
		return &FloatLiteral{Value: v}, nil
	case tokUUID:
		p.lex.next()
		return &UUIDLiteral{Value: t.val}, nil
	case tokQMark:
		p.lex.next()
		return &BindMarker{}, nil
	case tokColon:
		p.lex.next()
		name, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("at position %d: expected name after ':' for named bind marker", t.pos)
		}
		return &NamedBindMarker{Name: name}, nil
	case tokIdent:
		upper := strings.ToUpper(t.val)
		switch upper {
		case "TRUE":
			p.lex.next()
			return &BoolLiteral{Value: true}, nil
		case "FALSE":
			p.lex.next()
			return &BoolLiteral{Value: false}, nil
		case "NULL":
			p.lex.next()
			return &NullLiteral{}, nil
		default:
			// Check if this looks like a UUID (bare hex-dash string won't lex
			// as a single ident because of the dashes). UUIDs appear as
			// single-quoted strings in CQL, so this path is for identifiers
			// used in certain contexts. Fall through to error for now.
			return nil, p.errorf("expected expression, got identifier %q", t.val)
		}
	default:
		return nil, p.errorf("expected expression, got %q", t.val)
	}
}

// parseSelectors parses the SELECT list: either * or a comma-separated list of
// column names.
func (p *parser) parseSelectors() ([]Selector, error) {
	if p.lex.peek().kind == tokStar {
		p.lex.next()
		return []Selector{{Column: "*"}}, nil
	}
	var selectors []Selector
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	selectors = append(selectors, Selector{Column: name})
	for p.lex.peek().kind == tokComma {
		p.lex.next()
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, Selector{Column: name})
	}
	return selectors, nil
}

// parseWhereClauses parses <col> <op> <val> [AND ...] with support for
// the IN operator: <col> IN (<val>, <val>, ...).
func (p *parser) parseWhereClauses() ([]WhereClause, error) {
	var clauses []WhereClause
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		// Check for IN operator before trying comparison operators.
		if isKeyword(p.lex.peek(), "IN") {
			p.lex.next() // consume IN
			if err := p.expectToken(tokLParen); err != nil {
				return nil, err
			}
			vals, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			if err := p.expectToken(tokRParen); err != nil {
				return nil, err
			}
			clauses = append(clauses, WhereClause{
				Column:   col,
				Operator: "IN",
				Value:    &TupleLiteral{Values: vals},
			})
		} else {
			op, err := p.parseOperator()
			if err != nil {
				return nil, err
			}
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, WhereClause{Column: col, Operator: op, Value: val})
		}
		if !isKeyword(p.lex.peek(), "AND") {
			break
		}
		p.lex.next() // consume AND
	}
	return clauses, nil
}

// parseOrderByClauses parses <col> [ASC|DESC] [, <col> [ASC|DESC], ...].
func (p *parser) parseOrderByClauses() ([]OrderByClause, error) {
	var clauses []OrderByClause
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		clause := OrderByClause{Column: col}
		if isKeyword(p.lex.peek(), "DESC") {
			p.lex.next()
			clause.Desc = true
		} else if isKeyword(p.lex.peek(), "ASC") {
			p.lex.next()
		}
		clauses = append(clauses, clause)
		if p.lex.peek().kind != tokComma {
			break
		}
		p.lex.next() // consume comma
	}
	return clauses, nil
}

func (p *parser) parseOperator() (string, error) {
	t := p.lex.peek()
	switch t.kind {
	case tokEq:
		p.lex.next()
		return "=", nil
	case tokLT:
		p.lex.next()
		return "<", nil
	case tokGT:
		p.lex.next()
		return ">", nil
	case tokLTEq:
		p.lex.next()
		return "<=", nil
	case tokGTEq:
		p.lex.next()
		return ">=", nil
	case tokNE:
		p.lex.next()
		return "!=", nil
	default:
		return "", p.errorf("expected comparison operator, got %q", t.val)
	}
}

func kindName(kind tokenKind) string {
	switch kind {
	case tokEOF:
		return "EOF"
	case tokIdent:
		return "identifier"
	case tokString:
		return "string"
	case tokInteger:
		return "integer"
	case tokFloat:
		return "float"
	case tokUUID:
		return "UUID"
	case tokLParen:
		return "'('"
	case tokRParen:
		return "')'"
	case tokLBrace:
		return "'{'"
	case tokRBrace:
		return "'}'"
	case tokComma:
		return "','"
	case tokDot:
		return "'.'"
	case tokSemicolon:
		return "';'"
	case tokStar:
		return "'*'"
	case tokEq:
		return "'='"
	case tokLT:
		return "'<'"
	case tokGT:
		return "'>'"
	case tokLTEq:
		return "'<='"
	case tokGTEq:
		return "'>='"
	case tokNE:
		return "'!='"
	case tokColon:
		return "':'"
	case tokQMark:
		return "'?'"
	default:
		return "unknown"
	}
}
