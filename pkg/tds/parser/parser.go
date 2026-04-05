// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package parser implements a recursive descent parser for a subset of the
// T-SQL / Sybase SQL dialect. It accepts syntax from both Microsoft SQL Server
// and SAP Sybase ASE, which share a common T-SQL heritage but diverge in
// specific features.
//
// # Dialect scope
//
// The parser accepts the following features from each dialect:
//
// [Both] (common T-SQL supported by both SQL Server and Sybase ASE):
//
//   - USE, CREATE/DROP TABLE/DATABASE, INSERT, DELETE, UPDATE, SELECT
//   - WHERE, ORDER BY, TOP, GROUP BY, HAVING
//   - Subqueries (scalar, EXISTS, IN, ANY/ALL/SOME), CASE expressions
//   - Set operations (UNION/INTERSECT/EXCEPT), CTEs (WITH)
//   - Window functions (OVER), BETWEEN, LIKE
//   - JOINs (INNER/LEFT/RIGHT/FULL/CROSS)
//   - ALTER TABLE, CREATE INDEX, CREATE/DROP VIEW
//   - BEGIN/COMMIT/ROLLBACK TRAN, SAVE TRAN
//   - CAST(expr AS type), CONVERT(type, expr), ISNULL, GETDATE
//   - EXEC[UTE] procedure calls (with positional and named args)
//   - IDENTITY columns, DEFAULT, computed columns (AS expr)
//   - Bracket-quoted identifiers [name], @@system variables, N'...' strings
//
// [SQL Server] (Microsoft SQL Server specific):
//
//   - OFFSET n ROWS FETCH NEXT m ROWS ONLY (SQL Server 2012+)
//   - OUTPUT clause on INSERT/UPDATE/DELETE
//   - MERGE INTO ... USING ... ON ... WHEN MATCHED/NOT MATCHED
//   - CREATE PROCEDURE/FUNCTION/TRIGGER (parsed but not translated)
//   - VARCHAR(MAX) / NVARCHAR(MAX)
//
// [Sybase ASE] (SAP Sybase ASE specific):
//
//   - ROWS LIMIT x [OFFSET y] pagination (Sybase ASE 15.7+)
//   - UNSIGNED type modifier (UNSIGNED INT, UNSIGNED BIGINT, etc.)
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
	case tokenDROP:
		return p.parseDrop()
	case tokenALTER:
		return p.parseAlter()
	case tokenTRUNCATE:
		return p.parseTruncate()
	case tokenINSERT:
		return p.parseInsert()
	case tokenDELETE:
		return p.parseDelete()
	case tokenUPDATE:
		return p.parseUpdate()
	case tokenSELECT:
		return p.parseSelectOrCompound()
	case tokenWITH:
		return p.parseWith()
	case tokenDECLARE:
		return p.parseDeclare()
	case tokenSET:
		return p.parseSetVar()
	case tokenIF:
		return p.parseIf()
	case tokenWHILE:
		return p.parseWhile()
	case tokenBEGIN:
		return p.parseBegin()
	case tokenBREAK:
		p.lex.next()
		return &BreakStmt{}, nil
	case tokenCONTINUE:
		p.lex.next()
		return &ContinueStmt{}, nil
	case tokenPRINT:
		return p.parsePrint()
	case tokenCOMMIT:
		return p.parseCommitTran()
	case tokenROLLBACK:
		return p.parseRollbackTran()
	case tokenSAVE:
		return p.parseSaveTran()
	case tokenMERGE:
		return p.parseMerge()
	case tokenRAISERROR:
		return p.parseRaiserror()
	case tokenEXEC:
		return p.parseExec()
	case tokenTHROW:
		return p.parseThrow()
	case tokenRETURN:
		return p.parseReturn()
	case tokenGOTO:
		return p.parseGoto()
	case tokenWAITFOR:
		return p.parseWaitfor()
	case tokenIdent:
		// Check for label definition: <ident>:
		identTok := p.lex.next() // consume identifier
		if p.lex.peek().typ == tokenColon {
			p.lex.next() // consume colon
			return &LabelStmt{Label: identTok.val}, nil
		}
		return nil, p.error(fmt.Sprintf(
			"unexpected token %q at position %d", identTok.val, identTok.pos))
	default:
		return nil, p.error(fmt.Sprintf("unexpected token %q at position %d", tok.val, tok.pos))
	}
}

// parseWith parses: WITH <name> AS (<select>)[, ...] <select_or_compound>
func (p *parser) parseWith() (*WithStmt, error) {
	p.lex.next() // consume WITH
	stmt := &WithStmt{}
	for {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenAS); err != nil {
			return nil, err
		}
		if err := p.expect(tokenLParen); err != nil {
			return nil, err
		}
		sel, err := p.parseSelectOrCompound()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		stmt.CTEs = append(stmt.CTEs, CTEDef{Name: name, Select: sel})
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next() // consume comma
	}
	body, err := p.parseSelectOrCompound()
	if err != nil {
		return nil, err
	}
	stmt.Body = body
	return stmt, nil
}

// parseSelectOrCompound parses a SELECT statement optionally followed by set
// operations (UNION, UNION ALL, INTERSECT, EXCEPT). When set operations are
// present, ORDER BY and OFFSET-FETCH from the rightmost SELECT are lifted to
// the compound level since they apply to the combined result.
func (p *parser) parseSelectOrCompound() (Statement, error) {
	left, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	if !isSetOpToken(p.lex.peek().typ) {
		return left, nil
	}

	var result Statement = left
	for isSetOpToken(p.lex.peek().typ) {
		op := strings.ToUpper(p.lex.next().val)
		if op == "UNION" && p.lex.peek().typ == tokenALL {
			p.lex.next()
			op = "UNION ALL"
		}
		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		result = &CompoundSelectStmt{Left: result, Op: op, Right: right}
	}

	// Lift ORDER BY, OFFSET, and FETCH from the rightmost SELECT to the
	// compound level. In T-SQL, these clauses apply to the entire compound
	// result, not the individual SELECT.
	if cs, ok := result.(*CompoundSelectStmt); ok {
		if rightSel, ok := cs.Right.(*SelectStmt); ok {
			if len(rightSel.OrderBy) > 0 || rightSel.Offset != nil || rightSel.Fetch != nil {
				cs.OrderBy = rightSel.OrderBy
				rightSel.OrderBy = nil
				cs.Offset = rightSel.Offset
				rightSel.Offset = nil
				cs.Fetch = rightSel.Fetch
				rightSel.Fetch = nil
				cs.RowsLimitSyntax = rightSel.RowsLimitSyntax
				rightSel.RowsLimitSyntax = false
			}
		}
	}
	return result, nil
}

func isSetOpToken(typ tokenType) bool {
	return typ == tokenUNION || typ == tokenINTERSECT || typ == tokenEXCEPT
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

// parseCreate dispatches CREATE TABLE, DATABASE, INDEX, VIEW, PROCEDURE,
// FUNCTION, or TRIGGER.
func (p *parser) parseCreate() (Statement, error) {
	p.lex.next() // consume CREATE
	switch p.lex.peek().typ {
	case tokenTABLE:
		p.lex.next() // consume TABLE
		return p.parseCreateTable()
	case tokenDATABASE:
		p.lex.next() // consume DATABASE
		return p.parseCreateDatabase()
	case tokenINDEX:
		p.lex.next() // consume INDEX
		return p.parseCreateIndex(false)
	case tokenUNIQUE:
		p.lex.next() // consume UNIQUE
		if err := p.expect(tokenINDEX); err != nil {
			return nil, err
		}
		return p.parseCreateIndex(true)
	case tokenVIEW:
		p.lex.next() // consume VIEW
		return p.parseCreateView()
	case tokenPROCEDURE:
		p.lex.next() // consume PROCEDURE/PROC
		return p.parseCreateProcedure()
	case tokenFUNCTION:
		p.lex.next() // consume FUNCTION
		return p.parseCreateFunction()
	case tokenTRIGGER:
		p.lex.next() // consume TRIGGER
		return p.parseCreateTrigger()
	default:
		return nil, p.error(fmt.Sprintf(
			"expected TABLE, DATABASE, INDEX, VIEW, PROCEDURE, FUNCTION, or TRIGGER after CREATE, got %q at position %d",
			p.lex.peek().val, p.lex.peek().pos))
	}
}

// parseDrop dispatches DROP TABLE, DATABASE, VIEW, INDEX, or PROCEDURE.
// All support the optional IF EXISTS clause.
func (p *parser) parseDrop() (Statement, error) {
	p.lex.next() // consume DROP
	switch p.lex.peek().typ {
	case tokenTABLE:
		p.lex.next() // consume TABLE
		ifExists := p.tryIfExists()
		name, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		return &DropTableStmt{Table: name, IfExists: ifExists}, nil
	case tokenDATABASE:
		p.lex.next() // consume DATABASE
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &DropDatabaseStmt{Database: name}, nil
	case tokenVIEW:
		p.lex.next() // consume VIEW
		ifExists := p.tryIfExists()
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &DropViewStmt{Name: name, IfExists: ifExists}, nil
	case tokenINDEX:
		p.lex.next() // consume INDEX
		ifExists := p.tryIfExists()
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		stmt := &DropIndexStmt{Name: name, IfExists: ifExists}
		if p.lex.peek().typ == tokenON {
			p.lex.next() // consume ON
			table, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			stmt.Table = table
		}
		return stmt, nil
	case tokenPROCEDURE:
		p.lex.next() // consume PROCEDURE/PROC
		ifExists := p.tryIfExists()
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &DropProcedureStmt{Name: name, IfExists: ifExists}, nil
	default:
		return nil, p.error(fmt.Sprintf(
			"expected TABLE, DATABASE, VIEW, INDEX, or PROCEDURE after DROP, got %q at position %d",
			p.lex.peek().val, p.lex.peek().pos))
	}
}

// tryIfExists consumes IF EXISTS if present and returns true, else false.
func (p *parser) tryIfExists() bool {
	if p.lex.peek().typ == tokenIF {
		p.lex.next() // consume IF
		// Best-effort: expect EXISTS but don't fail the whole parse.
		if p.lex.peek().typ == tokenEXISTS {
			p.lex.next()
		}
		return true
	}
	return false
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

// parseColumnDef parses a column definition:
//
//	<name> <type>[(<args>)] [IDENTITY(seed,incr)] [DEFAULT <expr>] [NULL | NOT NULL]
//	<name> AS <expr>  (computed column)
func (p *parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.expectIdent()
	if err != nil {
		return ColumnDef{}, err
	}

	// Computed column: <name> AS <expr>
	if p.lex.peek().typ == tokenAS {
		p.lex.next() // consume AS
		expr, err := p.parseExpr()
		if err != nil {
			return ColumnDef{}, err
		}
		return ColumnDef{Name: name, ComputedExpr: expr}, nil
	}

	dataType, err := p.parseDataType()
	if err != nil {
		return ColumnDef{}, err
	}
	col := ColumnDef{Name: name, DataType: dataType}

	// Parse optional IDENTITY[(seed, increment)].
	if p.lex.peek().typ == tokenIDENTITY {
		p.lex.next() // consume IDENTITY
		if p.lex.peek().typ == tokenLParen {
			p.lex.next() // consume (
			seedTok := p.lex.next()
			if seedTok.typ != tokenInt {
				return ColumnDef{}, p.error(
					fmt.Sprintf("expected integer for IDENTITY seed, got %q", seedTok.val))
			}
			seed, _ := strconv.ParseInt(seedTok.val, 10, 64)
			if err := p.expect(tokenComma); err != nil {
				return ColumnDef{}, err
			}
			incrTok := p.lex.next()
			if incrTok.typ != tokenInt {
				return ColumnDef{}, p.error(
					fmt.Sprintf("expected integer for IDENTITY increment, got %q", incrTok.val))
			}
			incr, _ := strconv.ParseInt(incrTok.val, 10, 64)
			if err := p.expect(tokenRParen); err != nil {
				return ColumnDef{}, err
			}
			col.Identity = &IdentityDef{Seed: seed, Increment: incr}
		} else {
			col.Identity = &IdentityDef{Seed: 1, Increment: 1}
		}
	}

	// Parse optional DEFAULT <expr>.
	if p.lex.peek().typ == tokenDEFAULT {
		p.lex.next() // consume DEFAULT
		expr, err := p.parseExpr()
		if err != nil {
			return ColumnDef{}, err
		}
		col.DefaultExpr = expr
	}

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

	// [Sybase ASE] UNSIGNED type modifier (e.g. UNSIGNED INT, UNSIGNED
	// BIGINT). SQL Server does not support UNSIGNED integer types. Consume
	// the base type and combine into a compound type name so the translator
	// can map it.
	if typeName == "UNSIGNED" {
		base, err := p.expectIdent()
		if err != nil {
			return "", err
		}
		typeName = "UNSIGNED " + strings.ToUpper(base)
	}

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

// parseInsert parses:
//
//	INSERT [INTO] <table> [(<columns>)] [OUTPUT <cols>] VALUES (<values>), ...
//	INSERT [INTO] <table> [(<columns>)] [OUTPUT <cols>] SELECT ...
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

	// Optional OUTPUT clause.
	if p.lex.peek().typ == tokenOUTPUT {
		stmt.Output, err = p.parseOutputClause()
		if err != nil {
			return nil, err
		}
	}

	// VALUES or SELECT.
	switch p.lex.peek().typ {
	case tokenVALUES:
		p.lex.next() // consume VALUES
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
	case tokenSELECT:
		sel, err := p.parseSelectOrCompound()
		if err != nil {
			return nil, err
		}
		stmt.Select = sel
	default:
		return nil, p.error(fmt.Sprintf(
			"expected VALUES or SELECT after INSERT, got %q at position %d",
			p.lex.peek().val, p.lex.peek().pos))
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

// parseDelete parses:
//
//	DELETE [FROM] <table> [OUTPUT <cols>] [WHERE <expr>]
//	DELETE <target> FROM <table_refs> [JOIN ...] [OUTPUT <cols>] [WHERE <expr>]
//	DELETE [FROM] <table_ref> JOIN <table2> ON ... [OUTPUT <cols>] [WHERE <expr>]
func (p *parser) parseDelete() (*DeleteStmt, error) {
	p.lex.next() // consume DELETE
	stmt := &DeleteStmt{}

	hadFrom := false
	if p.lex.peek().typ == tokenFROM {
		p.lex.next()
		hadFrom = true
	}

	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.Table = ref.Name

	// Optional OUTPUT clause (before multi-table FROM).
	if p.lex.peek().typ == tokenOUTPUT {
		stmt.Output, err = p.parseOutputClause()
		if err != nil {
			return nil, err
		}
	}

	// Check for multi-table DELETE patterns.
	if !hadFrom && p.lex.peek().typ == tokenFROM {
		// DELETE <target> FROM <table_refs> [JOIN ...] [WHERE ...]
		p.lex.next() // consume FROM
		fromRef, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		stmt.From = append(stmt.From, fromRef)
		for isJoinStart(p.lex.peek().typ) {
			join, err := p.parseJoinClause()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, join)
		}
	} else if hadFrom && isJoinStart(p.lex.peek().typ) {
		// DELETE FROM <table_ref> JOIN <table2> ON ... [WHERE ...]
		stmt.From = append(stmt.From, ref)
		for isJoinStart(p.lex.peek().typ) {
			join, err := p.parseJoinClause()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, join)
		}
	}

	// OUTPUT clause can also appear after the FROM/JOIN clauses.
	if len(stmt.Output) == 0 && p.lex.peek().typ == tokenOUTPUT {
		stmt.Output, err = p.parseOutputClause()
		if err != nil {
			return nil, err
		}
	}

	if p.lex.peek().typ == tokenWHERE {
		p.lex.next()
		stmt.Where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

// parseUpdate parses:
//
//	UPDATE <table> SET <col>=<expr>, ... [OUTPUT <cols>] [FROM <tables> [JOIN ...]] [WHERE <expr>]
func (p *parser) parseUpdate() (*UpdateStmt, error) {
	p.lex.next() // consume UPDATE
	table, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenSET); err != nil {
		return nil, err
	}
	stmt := &UpdateStmt{Table: table}
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		// Handle dotted column references (e.g., t.name in UPDATE...FROM).
		for p.lex.peek().typ == tokenDot {
			p.lex.next() // consume .
			part, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			col = col + "." + part
		}
		if err := p.expect(tokenEq); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Assignments = append(stmt.Assignments, Assignment{Column: col, Value: val})
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next() // consume comma
	}

	// Optional OUTPUT clause.
	if p.lex.peek().typ == tokenOUTPUT {
		stmt.Output, err = p.parseOutputClause()
		if err != nil {
			return nil, err
		}
	}

	// Optional FROM clause (UPDATE...FROM for multi-table UPDATE).
	if p.lex.peek().typ == tokenFROM {
		p.lex.next()
		for {
			ref, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			stmt.From = append(stmt.From, ref)
			// Parse JOINs after each table ref.
			for isJoinStart(p.lex.peek().typ) {
				join, err := p.parseJoinClause()
				if err != nil {
					return nil, err
				}
				stmt.Joins = append(stmt.Joins, join)
			}
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next() // consume comma
		}
	}

	if p.lex.peek().typ == tokenWHERE {
		p.lex.next()
		stmt.Where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

// parseSelect parses a single SELECT statement (without trailing set
// operations). Set operations are handled by parseSelectOrCompound.
func (p *parser) parseSelect() (*SelectStmt, error) {
	p.lex.next() // consume SELECT
	stmt := &SelectStmt{}

	// Optional DISTINCT.
	if p.lex.peek().typ == tokenDISTINCT {
		p.lex.next()
		stmt.Distinct = true
	}

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

	// Optional INTO (SELECT ... INTO <table>).
	if p.lex.peek().typ == tokenINTO {
		p.lex.next() // consume INTO
		name, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		stmt.IntoTable = name
	}

	// Optional FROM.
	if p.lex.peek().typ == tokenFROM {
		p.lex.next()
		ref, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		stmt.From = append(stmt.From, ref)

		// Parse additional comma-separated table refs or JOIN clauses.
		for {
			if isJoinStart(p.lex.peek().typ) {
				join, err := p.parseJoinClause()
				if err != nil {
					return nil, err
				}
				stmt.Joins = append(stmt.Joins, join)
			} else if p.lex.peek().typ == tokenComma {
				p.lex.next()
				ref, err := p.parseTableRef()
				if err != nil {
					return nil, err
				}
				stmt.From = append(stmt.From, ref)
			} else {
				break
			}
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

	// Optional GROUP BY.
	if p.lex.peek().typ == tokenGROUP {
		p.lex.next()
		if err := p.expect(tokenBY); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, expr)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next()
		}
	}

	// Optional HAVING.
	if p.lex.peek().typ == tokenHAVING {
		p.lex.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
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

	// [Sybase ASE] Optional ROWS LIMIT x [OFFSET y] pagination (ASE 15.7+).
	// SQL Server does not support this syntax.
	if p.lex.peek().typ == tokenROWS {
		saved := p.lex.peek()
		p.lex.next() // consume ROWS
		if p.lex.peek().typ == tokenLIMIT {
			p.lex.next() // consume LIMIT
			tok := p.lex.next()
			if tok.typ != tokenInt {
				return nil, p.error(fmt.Sprintf(
					"expected integer after ROWS LIMIT, got %q", tok.val))
			}
			n, err := strconv.Atoi(tok.val)
			if err != nil {
				return nil, p.error(fmt.Sprintf(
					"invalid ROWS LIMIT value: %s", tok.val))
			}
			stmt.Fetch = &n
			stmt.RowsLimitSyntax = true
			// Optional OFFSET y.
			if p.lex.peek().typ == tokenOFFSET {
				p.lex.next()
				tok = p.lex.next()
				if tok.typ != tokenInt {
					return nil, p.error(fmt.Sprintf(
						"expected integer after OFFSET, got %q", tok.val))
				}
				m, err := strconv.Atoi(tok.val)
				if err != nil {
					return nil, p.error(fmt.Sprintf(
						"invalid OFFSET value: %s", tok.val))
				}
				stmt.Offset = &m
			}
			return stmt, nil
		}
		// Not ROWS LIMIT — push ROWS back for the next check.
		p.lex.peeked = &saved
	}

	// [SQL Server] Optional OFFSET-FETCH pagination (SQL Server 2012+).
	// Sybase ASE does not support this syntax.
	if p.lex.peek().typ == tokenOFFSET {
		p.lex.next()
		tok := p.lex.next()
		if tok.typ != tokenInt {
			return nil, p.error(fmt.Sprintf("expected integer after OFFSET, got %q", tok.val))
		}
		n, err := strconv.Atoi(tok.val)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid OFFSET value: %s", tok.val))
		}
		stmt.Offset = &n
		// Consume optional ROW/ROWS keyword.
		if p.lex.peek().typ == tokenROW || p.lex.peek().typ == tokenROWS {
			p.lex.next()
		}

		// Optional FETCH NEXT/FIRST n ROWS ONLY.
		if p.lex.peek().typ == tokenFETCH {
			p.lex.next()
			// NEXT or FIRST (both accepted).
			if p.lex.peek().typ == tokenNEXT || p.lex.peek().typ == tokenFIRST {
				p.lex.next()
			}
			tok = p.lex.next()
			if tok.typ != tokenInt {
				return nil, p.error(fmt.Sprintf("expected integer after FETCH, got %q", tok.val))
			}
			m, err := strconv.Atoi(tok.val)
			if err != nil {
				return nil, p.error(fmt.Sprintf("invalid FETCH value: %s", tok.val))
			}
			stmt.Fetch = &m
			// Consume optional ROW/ROWS keyword.
			if p.lex.peek().typ == tokenROW || p.lex.peek().typ == tokenROWS {
				p.lex.next()
			}
			// Consume optional ONLY keyword.
			if p.lex.peek().typ == tokenONLY {
				p.lex.next()
			}
		}
	}

	// Optional COMPUTE [aggregate](expr) [, ...] [BY col [, col ...]].
	// Multiple COMPUTE clauses are allowed.
	for p.lex.peek().typ == tokenCOMPUTE {
		cc, err := p.parseComputeClause()
		if err != nil {
			return nil, err
		}
		stmt.Compute = append(stmt.Compute, cc)
	}

	return stmt, nil
}

// parseComputeClause parses:
// COMPUTE <agg>(<expr>) [, <agg>(<expr>) ...] [BY <expr> [, <expr> ...]]
func (p *parser) parseComputeClause() (ComputeClause, error) {
	p.lex.next() // consume COMPUTE
	cc := ComputeClause{}

	// Parse aggregate list: SUM(col), AVG(col), etc.
	for {
		agg, err := p.parseComputeAgg()
		if err != nil {
			return ComputeClause{}, err
		}
		cc.Aggregates = append(cc.Aggregates, agg)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next() // consume comma
	}

	// Optional BY clause.
	if p.lex.peek().typ == tokenBY {
		p.lex.next() // consume BY
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return ComputeClause{}, err
			}
			cc.By = append(cc.By, expr)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next() // consume comma
		}
	}

	return cc, nil
}

// parseComputeAgg parses a single aggregate in a COMPUTE clause:
// <funcname>(<expr>)
func (p *parser) parseComputeAgg() (ComputeAgg, error) {
	name, err := p.expectIdent()
	if err != nil {
		return ComputeAgg{}, p.error(fmt.Sprintf(
			"expected aggregate function name in COMPUTE, got %q", p.lex.peek().val))
	}
	if err := p.expect(tokenLParen); err != nil {
		return ComputeAgg{}, err
	}
	arg, err := p.parseExpr()
	if err != nil {
		return ComputeAgg{}, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return ComputeAgg{}, err
	}
	return ComputeAgg{Func: strings.ToUpper(name), Arg: arg}, nil
}

// parseBegin disambiguates BEGIN TRAN (transaction) from BEGIN...END
// (statement block).
func (p *parser) parseBegin() (Statement, error) {
	p.lex.next() // consume BEGIN
	// BEGIN TRAN or BEGIN TRANSACTION → transaction start.
	if p.lex.peek().typ == tokenTRAN || p.lex.peek().typ == tokenTRANSACTION {
		p.lex.next() // consume TRAN/TRANSACTION
		stmt := &BeginTranStmt{}
		if p.lex.peek().typ == tokenIdent {
			stmt.Name = p.lex.next().val
		}
		return stmt, nil
	}
	// BEGIN TRY ... END TRY BEGIN CATCH ... END CATCH.
	if p.lex.peek().typ == tokenTRY {
		return p.parseBeginTryCatch()
	}
	// Otherwise BEGIN...END statement block.
	return p.parseBeginEndBody()
}

// parseBeginEndBody parses the body of a BEGIN...END block (BEGIN was
// already consumed).
func (p *parser) parseBeginEndBody() (*BeginEndBlock, error) {
	block := &BeginEndBlock{}
	for {
		// Skip semicolons between statements.
		for p.lex.peek().typ == tokenSemicolon {
			p.lex.next()
		}
		if p.lex.peek().typ == tokenEND {
			p.lex.next() // consume END
			break
		}
		if p.lex.peek().typ == tokenEOF {
			return nil, p.error("unterminated BEGIN...END block")
		}
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		block.Stmts = append(block.Stmts, stmt)
	}
	return block, nil
}

// parseDeclare parses:
//
//	DECLARE @var TYPE [= expr]          (scalar variable)
//	DECLARE @var TABLE (<column_defs>)  (table variable)
func (p *parser) parseDeclare() (Statement, error) {
	p.lex.next() // consume DECLARE
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(name, "@") {
		return nil, p.error(fmt.Sprintf(
			"expected @variable name after DECLARE, got %q", name))
	}

	// Table variable: DECLARE @t TABLE (columns...)
	if p.lex.peek().typ == tokenTABLE {
		p.lex.next() // consume TABLE
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
		return &DeclareTableVarStmt{Name: name, Columns: cols}, nil
	}

	// Scalar variable: DECLARE @var TYPE [= expr]
	dataType, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	stmt := &DeclareVarStmt{Name: name, DataType: dataType}
	// Optional default: = <expr>
	if p.lex.peek().typ == tokenEq {
		p.lex.next() // consume =
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Default = expr
	}
	return stmt, nil
}

// parseSetVar parses: SET @var = expr
func (p *parser) parseSetVar() (*SetVarStmt, error) {
	p.lex.next() // consume SET
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(name, "@") {
		return nil, p.error(fmt.Sprintf(
			"expected @variable name after SET, got %q", name))
	}
	if err := p.expect(tokenEq); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return &SetVarStmt{Name: name, Expr: expr}, nil
}

// parseIf parses: IF condition stmt [ELSE stmt]
// The condition is a boolean expression. The body can be a single
// statement or a BEGIN...END block.
func (p *parser) parseIf() (*IfStmt, error) {
	p.lex.next() // consume IF
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	body, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	stmt := &IfStmt{Condition: cond, Body: body}
	if p.lex.peek().typ == tokenELSE {
		p.lex.next() // consume ELSE
		elseBody, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		stmt.ElseBody = elseBody
	}
	return stmt, nil
}

// parseWhile parses: WHILE condition stmt
func (p *parser) parseWhile() (*WhileStmt, error) {
	p.lex.next() // consume WHILE
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	body, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	return &WhileStmt{Condition: cond, Body: body}, nil
}

// parsePrint parses: PRINT <expr>
func (p *parser) parsePrint() (*PrintStmt, error) {
	p.lex.next() // consume PRINT
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return &PrintStmt{Expr: expr}, nil
}

// parseCommitTran parses: COMMIT [TRAN[SACTION]] [name]
func (p *parser) parseCommitTran() (*CommitTranStmt, error) {
	p.lex.next() // consume COMMIT
	stmt := &CommitTranStmt{}
	// Optional TRAN or TRANSACTION keyword.
	if p.lex.peek().typ == tokenTRAN || p.lex.peek().typ == tokenTRANSACTION {
		p.lex.next()
	}
	// Optional transaction name.
	if p.lex.peek().typ == tokenIdent {
		stmt.Name = p.lex.next().val
	}
	return stmt, nil
}

// parseRollbackTran parses: ROLLBACK [TRAN[SACTION]] [name | savepoint]
func (p *parser) parseRollbackTran() (*RollbackTranStmt, error) {
	p.lex.next() // consume ROLLBACK
	stmt := &RollbackTranStmt{}
	// Optional TRAN or TRANSACTION keyword.
	if p.lex.peek().typ == tokenTRAN || p.lex.peek().typ == tokenTRANSACTION {
		p.lex.next()
	}
	// Optional transaction or savepoint name.
	if p.lex.peek().typ == tokenIdent {
		stmt.Name = p.lex.next().val
	}
	return stmt, nil
}

// parseSaveTran parses: SAVE TRAN[SACTION] name
func (p *parser) parseSaveTran() (*SaveTranStmt, error) {
	p.lex.next() // consume SAVE
	// Expect TRAN or TRANSACTION.
	if p.lex.peek().typ != tokenTRAN && p.lex.peek().typ != tokenTRANSACTION {
		return nil, p.error(fmt.Sprintf(
			"expected TRAN or TRANSACTION after SAVE, got %q", p.lex.peek().val))
	}
	p.lex.next() // consume TRAN/TRANSACTION
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &SaveTranStmt{Name: name}, nil
}

// parseRaiserror parses the Sybase ASE form: RAISERROR <errnum> [, <message>].
// [Sybase ASE] This is the Sybase ASE RAISERROR syntax. SQL Server uses a
// different form: RAISERROR('message', severity, state).
func (p *parser) parseRaiserror() (*RaiserrorStmt, error) {
	p.lex.next() // consume RAISERROR
	tok := p.lex.next()
	if tok.typ != tokenInt {
		return nil, p.error(fmt.Sprintf(
			"expected error number after RAISERROR, got %q", tok.val))
	}
	errNum, err := strconv.Atoi(tok.val)
	if err != nil {
		return nil, p.error(fmt.Sprintf("invalid error number: %s", tok.val))
	}
	stmt := &RaiserrorStmt{ErrNum: errNum}
	// Optional comma-separated message string.
	if p.lex.peek().typ == tokenComma {
		p.lex.next() // consume comma
		msgTok := p.lex.next()
		if msgTok.typ != tokenString {
			return nil, p.error(fmt.Sprintf(
				"expected string after RAISERROR errnum, got %q", msgTok.val))
		}
		stmt.Message = msgTok.val
	}
	return stmt, nil
}

// parseExec parses: EXEC[UTE] <procedure> [<arg1>, <arg2>, ...]
// Arguments can be positional expressions or named (@param = expr).
func (p *parser) parseExec() (*ExecStmt, error) {
	p.lex.next() // consume EXEC/EXECUTE

	// Procedure name: a dotted identifier (e.g., dbo.sp_help or sp_tables).
	name, err := p.parseTableName()
	if err != nil {
		return nil, p.error(fmt.Sprintf(
			"expected procedure name after EXEC, got %q", p.lex.peek().val))
	}
	stmt := &ExecStmt{Procedure: name}

	// Parse optional arguments (comma-separated).
	for {
		tok := p.lex.peek()
		if tok.typ == tokenEOF || tok.typ == tokenSemicolon || tok.typ == tokenGO {
			break
		}

		// Check for named parameter: @name = expr
		if tok.typ == tokenIdent && len(tok.val) > 0 && tok.val[0] == '@' {
			// Peek ahead to see if this is @name = expr
			p.lex.next() // consume @name
			paramName := tok.val
			if p.lex.peek().typ == tokenEq {
				p.lex.next() // consume =
				val, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				stmt.Args = append(stmt.Args, ExecArg{
					Name:  paramName,
					Value: val,
				})
			} else {
				// It's a positional @variable argument, not named.
				// Re-wrap it as an IdentExpr.
				stmt.Args = append(stmt.Args, ExecArg{
					Value: &IdentExpr{Parts: []string{paramName}},
				})
			}
		} else {
			// Positional argument: parse as expression.
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Args = append(stmt.Args, ExecArg{Value: val})
		}

		// Consume comma separator if present.
		if p.lex.peek().typ == tokenComma {
			p.lex.next()
		} else {
			break
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

// isJoinStart returns true if the token type indicates the start of a JOIN
// clause.
func isJoinStart(typ tokenType) bool {
	switch typ {
	case tokenINNER, tokenLEFT, tokenRIGHT, tokenFULL, tokenCROSS, tokenJOIN, tokenOUTER:
		return true
	}
	return false
}

// parseJoinClause parses: [INNER|LEFT [OUTER]|RIGHT [OUTER]|FULL [OUTER]|CROSS]
// JOIN <table_ref> [ON <expr>]
func (p *parser) parseJoinClause() (JoinClause, error) {
	var joinType JoinType

	switch p.lex.peek().typ {
	case tokenINNER:
		p.lex.next()
		joinType = InnerJoin
	case tokenLEFT:
		p.lex.next()
		if p.lex.peek().typ == tokenOUTER {
			p.lex.next()
		}
		joinType = LeftJoin
	case tokenRIGHT:
		p.lex.next()
		if p.lex.peek().typ == tokenOUTER {
			p.lex.next()
		}
		joinType = RightJoin
	case tokenFULL:
		p.lex.next()
		if p.lex.peek().typ == tokenOUTER {
			p.lex.next()
		}
		joinType = FullJoin
	case tokenCROSS:
		p.lex.next()
		if p.lex.peek().typ == tokenAPPLY {
			p.lex.next() // consume APPLY
			joinType = CrossApplyJoin
		} else {
			joinType = CrossJoin
		}
	case tokenOUTER:
		p.lex.next()
		if p.lex.peek().typ == tokenAPPLY {
			p.lex.next() // consume APPLY
			joinType = OuterApplyJoin
		} else {
			return JoinClause{}, p.error("expected APPLY after OUTER")
		}
	case tokenJOIN:
		// Plain JOIN = INNER JOIN.
		joinType = InnerJoin
	default:
		return JoinClause{}, p.error("expected JOIN keyword")
	}

	// APPLY operators don't use the JOIN keyword.
	if joinType != CrossApplyJoin && joinType != OuterApplyJoin {
		if err := p.expect(tokenJOIN); err != nil {
			return JoinClause{}, err
		}
	}

	table, err := p.parseTableRef()
	if err != nil {
		return JoinClause{}, err
	}

	join := JoinClause{Type: joinType, Table: table}

	// Parse ON condition (required for all except CROSS JOIN).
	if p.lex.peek().typ == tokenON {
		p.lex.next()
		cond, err := p.parseExpr()
		if err != nil {
			return JoinClause{}, err
		}
		join.Condition = cond
	}

	return join, nil
}

// parseTableRef parses a table reference: <name> [<alias>] or (<subquery>) <alias>,
// optionally followed by a PIVOT or UNPIVOT operator.
func (p *parser) parseTableRef() (TableRef, error) {
	var ref TableRef

	// Check for derived table: (SELECT ...) [AS] alias.
	if p.lex.peek().typ == tokenLParen {
		p.lex.next() // consume (
		sel, err := p.parseSelectOrCompound()
		if err != nil {
			return TableRef{}, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return TableRef{}, err
		}
		ref = TableRef{Subquery: sel}
		if p.lex.peek().typ == tokenAS {
			p.lex.next()
		}
		alias, err := p.expectIdent()
		if err != nil {
			return TableRef{}, err
		}
		ref.Alias = alias
	} else {
		name, err := p.parseTableName()
		if err != nil {
			return TableRef{}, err
		}
		ref = TableRef{Name: name}
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
	}

	// Check for PIVOT or UNPIVOT operator following the table reference.
	if p.lex.peek().typ == tokenPIVOT {
		return p.parsePivot(ref)
	}
	if p.lex.peek().typ == tokenUNPIVOT {
		return p.parseUnpivot(ref)
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
//  1. OR
//  2. AND
//  3. NOT (unary)
//  4. Comparison: =, <>, !=, <, >, <=, >=, IS [NOT] NULL, IN, BETWEEN, LIKE
//  5. Addition: +, -
//  6. Multiplication: *, /, %
//  7. Unary: -, NOT
//  8. Primary: literals, identifiers, function calls, parenthesized exprs,
//     subqueries, EXISTS

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
		// NOT EXISTS (SELECT ...) is a special construct.
		if p.lex.peek().typ == tokenEXISTS {
			p.lex.next()
			sel, err := p.parseSubqueryParens()
			if err != nil {
				return nil, err
			}
			return &ExistsExpr{Select: sel, Not: true}, nil
		}
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
		return p.parseComparisonRHS(left, "=")
	case tokenNeq:
		p.lex.next()
		return p.parseComparisonRHS(left, "<>")
	case tokenLT:
		p.lex.next()
		return p.parseComparisonRHS(left, "<")
	case tokenGT:
		p.lex.next()
		return p.parseComparisonRHS(left, ">")
	case tokenLTE:
		p.lex.next()
		return p.parseComparisonRHS(left, "<=")
	case tokenGTE:
		p.lex.next()
		return p.parseComparisonRHS(left, ">=")
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
	case tokenIN:
		p.lex.next()
		values, subquery, err := p.parseInList()
		if err != nil {
			return nil, err
		}
		return &InExpr{Expr: left, Values: values, Subquery: subquery, Not: false}, nil
	case tokenBETWEEN:
		p.lex.next()
		low, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenAND); err != nil {
			return nil, err
		}
		high, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high, Not: false}, nil
	case tokenNOT:
		// Handle NOT LIKE, NOT IN, NOT BETWEEN.
		p.lex.next()
		switch p.lex.peek().typ {
		case tokenLIKE:
			p.lex.next()
			right, err := p.parseAddition()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: "NOT LIKE", Right: right}, nil
		case tokenIN:
			p.lex.next()
			values, subquery, err := p.parseInList()
			if err != nil {
				return nil, err
			}
			return &InExpr{Expr: left, Values: values, Subquery: subquery, Not: true}, nil
		case tokenBETWEEN:
			p.lex.next()
			low, err := p.parseAddition()
			if err != nil {
				return nil, err
			}
			if err := p.expect(tokenAND); err != nil {
				return nil, err
			}
			high, err := p.parseAddition()
			if err != nil {
				return nil, err
			}
			return &BetweenExpr{Expr: left, Low: low, High: high, Not: true}, nil
		default:
			return nil, p.error("expected LIKE, IN, or BETWEEN after NOT")
		}
	}
	return left, nil
}

// parseComparisonRHS parses the right-hand side of a comparison operator. If
// the next token is ANY, ALL, or SOME, it parses a quantified comparison
// subquery (e.g. x > ANY (SELECT ...)). Otherwise it parses a regular
// arithmetic expression.
func (p *parser) parseComparisonRHS(left Expr, op string) (Expr, error) {
	if p.lex.peek().typ == tokenANY || p.lex.peek().typ == tokenALL || p.lex.peek().typ == tokenSOME {
		kind := strings.ToUpper(p.lex.next().val)
		sel, err := p.parseSubqueryParens()
		if err != nil {
			return nil, err
		}
		return &AnyAllExpr{Expr: left, Op: op, Kind: kind, Select: sel}, nil
	}
	right, err := p.parseAddition()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

// parseSubqueryParens parses: (SELECT ...) — a parenthesized SELECT used in
// EXISTS, ANY/ALL, and other subquery contexts.
func (p *parser) parseSubqueryParens() (Statement, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	sel, err := p.parseSelectOrCompound()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return sel, nil
}

// parseInList parses a parenthesized list for IN: either a value list
// (1, 2, 3) or a subquery (SELECT ...). It returns (values, nil) for value
// lists and (nil, subquery) for subqueries.
func (p *parser) parseInList() ([]Expr, Statement, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, nil, err
	}
	// Check for subquery: IN (SELECT ...)
	if p.lex.peek().typ == tokenSELECT {
		sel, err := p.parseSelectOrCompound()
		if err != nil {
			return nil, nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, nil, err
		}
		return nil, sel, nil
	}
	// Parse expression list.
	var values []Expr
	for {
		val, err := p.parseAddition()
		if err != nil {
			return nil, nil, err
		}
		values = append(values, val)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next() // consume comma
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, nil, err
	}
	return values, nil, nil
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
// calls, parenthesized expressions, subqueries, and EXISTS.
func (p *parser) parsePrimary() (Expr, error) {
	tok := p.lex.peek()

	switch tok.typ {
	case tokenBinaryLit:
		p.lex.next()
		return &BinaryLit{HexDigits: tok.val}, nil

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
		p.lex.next() // consume (
		// Check for subquery: (SELECT ...)
		if p.lex.peek().typ == tokenSELECT {
			sel, err := p.parseSelectOrCompound()
			if err != nil {
				return nil, err
			}
			if err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			return &SubqueryExpr{Select: sel}, nil
		}
		// Regular parenthesized expression.
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &ParenExpr{Expr: expr}, nil

	case tokenLEFT, tokenRIGHT:
		// LEFT and RIGHT are both JOIN keywords and T-SQL function names
		// (e.g. LEFT('hello', 3)). When followed by '(', parse as a
		// function call.
		p.lex.next() // consume LEFT/RIGHT
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
			return &FuncCallExpr{Name: tok.val, Args: args}, nil
		}
		return nil, p.error(fmt.Sprintf(
			"unexpected token %q at position %d", tok.val, tok.pos))

	case tokenEXISTS:
		p.lex.next()
		sel, err := p.parseSubqueryParens()
		if err != nil {
			return nil, err
		}
		return &ExistsExpr{Select: sel, Not: false}, nil

	case tokenCASE:
		return p.parseCASE()

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

	case tokenCAST:
		return p.parseCAST(false)
	case tokenTRY_CAST:
		return p.parseCAST(true)

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

// parseCAST parses: CAST(<expr> AS <type>) or TRY_CAST(<expr> AS <type>).
// The try parameter controls whether this is CAST (false) or TRY_CAST (true).
func (p *parser) parseCAST(try bool) (Expr, error) {
	p.lex.next() // consume CAST or TRY_CAST
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenAS); err != nil {
		return nil, err
	}
	dataType, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return &CastExpr{Expr: expr, DataType: dataType, Try: try}, nil
}

// parseCASE parses:
//
//	CASE [<operand>] WHEN <cond> THEN <result> ... [ELSE <result>] END
func (p *parser) parseCASE() (Expr, error) {
	p.lex.next() // consume CASE
	expr := &CaseExpr{}

	// Optional simple operand (e.g. CASE x WHEN 1 THEN ...).
	if p.lex.peek().typ != tokenWHEN {
		operand, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		expr.Operand = operand
	}

	// At least one WHEN ... THEN ... clause.
	for p.lex.peek().typ == tokenWHEN {
		p.lex.next() // consume WHEN
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenTHEN); err != nil {
			return nil, err
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		expr.Whens = append(expr.Whens, WhenExpr{Cond: cond, Result: result})
	}

	if len(expr.Whens) == 0 {
		return nil, p.error("expected at least one WHEN clause in CASE")
	}

	// Optional ELSE.
	if p.lex.peek().typ == tokenELSE {
		p.lex.next()
		elseResult, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		expr.Else = elseResult
	}

	if err := p.expect(tokenEND); err != nil {
		return nil, err
	}
	return expr, nil
}

// parseIdentOrFunc parses an identifier, a dotted identifier (a.b.c), or a
// function call (func(args)). If a function call is followed by OVER, it is
// parsed as a window function expression.
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
		fc := &FuncCallExpr{Name: name, Args: args}
		// Check for window function: func(...) OVER (...)
		if p.lex.peek().typ == tokenOVER {
			return p.parseWindowOver(fc)
		}
		return fc, nil
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

// parseAlter dispatches ALTER TABLE.
func (p *parser) parseAlter() (Statement, error) {
	p.lex.next() // consume ALTER
	if p.lex.peek().typ == tokenTABLE {
		p.lex.next() // consume TABLE
		return p.parseAlterTable()
	}
	return nil, p.error(fmt.Sprintf(
		"expected TABLE after ALTER, got %q at position %d",
		p.lex.peek().val, p.lex.peek().pos))
}

// parseAlterTable parses ALTER TABLE <name> (ADD|DROP|ALTER) ...
func (p *parser) parseAlterTable() (*AlterTableStmt, error) {
	table, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	stmt := &AlterTableStmt{Table: table}

	switch p.lex.peek().typ {
	case tokenADD:
		p.lex.next() // consume ADD
		if p.lex.peek().typ == tokenCONSTRAINT {
			cmd, err := p.parseAddConstraint()
			if err != nil {
				return nil, err
			}
			stmt.Cmd = cmd
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			stmt.Cmd = &AddColumnCmd{Column: col}
		}
	case tokenDROP:
		p.lex.next() // consume DROP
		switch p.lex.peek().typ {
		case tokenCOLUMN:
			p.lex.next() // consume COLUMN
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			stmt.Cmd = &DropColumnCmd{Name: name}
		case tokenCONSTRAINT:
			p.lex.next() // consume CONSTRAINT
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			stmt.Cmd = &DropConstraintCmd{Name: name}
		default:
			return nil, p.error("expected COLUMN or CONSTRAINT after ALTER TABLE ... DROP")
		}
	case tokenALTER:
		p.lex.next() // consume ALTER
		if err := p.expect(tokenCOLUMN); err != nil {
			return nil, err
		}
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		dataType, err := p.parseDataType()
		if err != nil {
			return nil, err
		}
		stmt.Cmd = &AlterColumnCmd{Name: name, DataType: dataType}
	default:
		return nil, p.error("expected ADD, DROP, or ALTER after ALTER TABLE <name>")
	}

	return stmt, nil
}

// parseAddConstraint parses ADD CONSTRAINT <name> (PRIMARY KEY|FOREIGN KEY|UNIQUE|CHECK).
func (p *parser) parseAddConstraint() (*AddConstraintCmd, error) {
	p.lex.next() // consume CONSTRAINT
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	switch p.lex.peek().typ {
	case tokenPRIMARY:
		p.lex.next() // consume PRIMARY
		if err := p.expect(tokenKEY); err != nil {
			return nil, err
		}
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		return &AddConstraintCmd{
			Name: name, Type: PrimaryKeyConstraint, Columns: cols,
		}, nil

	case tokenFOREIGN:
		p.lex.next() // consume FOREIGN
		if err := p.expect(tokenKEY); err != nil {
			return nil, err
		}
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenREFERENCES); err != nil {
			return nil, err
		}
		refTable, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		refCols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		return &AddConstraintCmd{
			Name: name, Type: ForeignKeyConstraint,
			Columns: cols, RefTable: refTable, RefColumns: refCols,
		}, nil

	case tokenUNIQUE:
		p.lex.next() // consume UNIQUE
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		return &AddConstraintCmd{
			Name: name, Type: UniqueConstraint, Columns: cols,
		}, nil

	case tokenCHECK:
		p.lex.next() // consume CHECK
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		return &AddConstraintCmd{
			Name: name, Type: CheckConstraint, CheckExpr: expr,
		}, nil

	default:
		return nil, p.error("expected PRIMARY, FOREIGN, UNIQUE, or CHECK after CONSTRAINT <name>")
	}
}

// parseColumnList parses a parenthesized, comma-separated list of column names:
// (col1, col2, ...).
func (p *parser) parseColumnList() ([]string, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	var cols []string
	for {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, name)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next() // consume comma
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return cols, nil
}

// parseCreateIndex parses: [UNIQUE] INDEX <name> ON <table> (<cols>)
// [INCLUDE (<cols>)]. The UNIQUE keyword is already consumed by parseCreate.
func (p *parser) parseCreateIndex(unique bool) (*CreateIndexStmt, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenON); err != nil {
		return nil, err
	}
	table, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	stmt := &CreateIndexStmt{
		Unique: unique, Name: name, Table: table, Columns: cols,
	}
	if p.lex.peek().typ == tokenINCLUDE {
		p.lex.next() // consume INCLUDE
		stmt.Include, err = p.parseColumnList()
		if err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

// parseCreateView parses: VIEW <name> AS SELECT ...
func (p *parser) parseCreateView() (*CreateViewStmt, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenAS); err != nil {
		return nil, err
	}
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	return &CreateViewStmt{Name: name, Select: sel}, nil
}

// parseCreateProcedure parses CREATE PROCEDURE <name> and consumes the rest
// of the body. CockroachDB TDS does not support stored procedures.
func (p *parser) parseCreateProcedure() (*CreateProcedureStmt, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	p.skipToEndOfStatement()
	return &CreateProcedureStmt{Name: name}, nil
}

// parseCreateFunction parses CREATE FUNCTION <name> and consumes the rest
// of the body. CockroachDB TDS does not support T-SQL functions.
func (p *parser) parseCreateFunction() (*CreateFunctionStmt, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	p.skipToEndOfStatement()
	return &CreateFunctionStmt{Name: name}, nil
}

// parseCreateTrigger parses CREATE TRIGGER <name> and consumes the rest
// of the body. CockroachDB TDS does not support triggers.
func (p *parser) parseCreateTrigger() (*CreateTriggerStmt, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	p.skipToEndOfStatement()
	return &CreateTriggerStmt{Name: name}, nil
}

// skipToEndOfStatement consumes tokens until a statement terminator
// (semicolon, GO, or EOF) is reached. Used to skip procedure/function/trigger
// bodies that we parse but don't translate.
func (p *parser) skipToEndOfStatement() {
	for {
		tok := p.lex.peek()
		if tok.typ == tokenEOF || tok.typ == tokenSemicolon || tok.typ == tokenGO {
			break
		}
		p.lex.next()
	}
}

// parseOutputClause parses: OUTPUT <select_column_list>.
// [SQL Server] OUTPUT with inserted.*/deleted.* pseudo-tables is SQL
// Server-specific. Sybase ASE has no OUTPUT clause.
func (p *parser) parseOutputClause() ([]SelectColumn, error) {
	p.lex.next() // consume OUTPUT
	var cols []SelectColumn
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next()
	}
	return cols, nil
}

// parseMerge parses:
//
//	MERGE [INTO] <target> USING <source> ON <condition>
//	[WHEN MATCHED THEN (UPDATE SET ...|DELETE)]
//	[WHEN NOT MATCHED THEN INSERT [(<cols>)] VALUES (<vals>)]
//	[OUTPUT <cols>]
//
// [SQL Server] MERGE is SQL Server 2008+ (and ANSI SQL:2003). Sybase ASE
// does not support MERGE; it uses separate INSERT/UPDATE/DELETE instead.
func (p *parser) parseMerge() (*MergeStmt, error) {
	p.lex.next() // consume MERGE
	if p.lex.peek().typ == tokenINTO {
		p.lex.next()
	}
	target, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenUSING); err != nil {
		return nil, err
	}
	source, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenON); err != nil {
		return nil, err
	}
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	stmt := &MergeStmt{
		Target:    target,
		Source:    source,
		Condition: cond,
	}

	// Parse WHEN clauses (order-independent).
	for p.lex.peek().typ == tokenWHEN {
		p.lex.next() // consume WHEN
		if p.lex.peek().typ == tokenNOT {
			// WHEN NOT MATCHED THEN INSERT ...
			p.lex.next() // consume NOT
			if err := p.expect(tokenMATCHED); err != nil {
				return nil, err
			}
			if err := p.expect(tokenTHEN); err != nil {
				return nil, err
			}
			if err := p.expect(tokenINSERT); err != nil {
				return nil, err
			}
			nm := &MergeWhenNotMatched{}
			// Optional column list.
			if p.lex.peek().typ == tokenLParen {
				p.lex.next()
				for {
					col, colErr := p.expectIdent()
					if colErr != nil {
						return nil, colErr
					}
					nm.Columns = append(nm.Columns, col)
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
			if err := p.expect(tokenLParen); err != nil {
				return nil, err
			}
			for {
				val, valErr := p.parseExpr()
				if valErr != nil {
					return nil, valErr
				}
				nm.Values = append(nm.Values, val)
				if p.lex.peek().typ != tokenComma {
					break
				}
				p.lex.next()
			}
			if err := p.expect(tokenRParen); err != nil {
				return nil, err
			}
			stmt.NotMatched = nm
		} else {
			// WHEN MATCHED THEN UPDATE SET ... | DELETE
			if err := p.expect(tokenMATCHED); err != nil {
				return nil, err
			}
			if err := p.expect(tokenTHEN); err != nil {
				return nil, err
			}
			matched := &MergeWhenMatched{}
			if p.lex.peek().typ == tokenDELETE {
				p.lex.next()
				matched.Delete = true
			} else if p.lex.peek().typ == tokenUPDATE {
				p.lex.next()
				if err := p.expect(tokenSET); err != nil {
					return nil, err
				}
				for {
					col, colErr := p.expectIdent()
					if colErr != nil {
						return nil, colErr
					}
					// Handle dotted columns (e.g., t.name = s.name).
					for p.lex.peek().typ == tokenDot {
						p.lex.next()
						part, partErr := p.expectIdent()
						if partErr != nil {
							return nil, partErr
						}
						col = col + "." + part
					}
					if err := p.expect(tokenEq); err != nil {
						return nil, err
					}
					val, valErr := p.parseExpr()
					if valErr != nil {
						return nil, valErr
					}
					matched.Assignments = append(matched.Assignments,
						Assignment{Column: col, Value: val})
					if p.lex.peek().typ != tokenComma {
						break
					}
					p.lex.next()
				}
			} else {
				return nil, p.error("expected UPDATE or DELETE after WHEN MATCHED THEN")
			}
			stmt.Matched = matched
		}
	}

	// Optional OUTPUT clause.
	if p.lex.peek().typ == tokenOUTPUT {
		stmt.Output, err = p.parseOutputClause()
		if err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

// parseTruncate parses: TRUNCATE TABLE <name>
func (p *parser) parseTruncate() (*TruncateTableStmt, error) {
	p.lex.next() // consume TRUNCATE
	if err := p.expect(tokenTABLE); err != nil {
		return nil, err
	}
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	return &TruncateTableStmt{Table: name}, nil
}

// parseWindowOver parses the OVER clause of a window function:
// OVER ([PARTITION BY <exprs>] [ORDER BY <exprs>])
func (p *parser) parseWindowOver(fn *FuncCallExpr) (Expr, error) {
	p.lex.next() // consume OVER
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	w := &WindowExpr{Func: fn}

	// Optional PARTITION BY.
	if p.lex.peek().typ == tokenPARTITION {
		p.lex.next()
		if err := p.expect(tokenBY); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			w.PartitionBy = append(w.PartitionBy, expr)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next()
		}
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
			w.OrderBy = append(w.OrderBy, ob)
			if p.lex.peek().typ != tokenComma {
				break
			}
			p.lex.next()
		}
	}

	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return w, nil
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
		tokenBETWEEN, tokenLIKE, tokenDELETE, tokenUPDATE, tokenSET,
		tokenDROP, tokenDISTINCT, tokenGROUP, tokenHAVING,
		tokenCASE, tokenWHEN, tokenTHEN, tokenELSE, tokenEND,
		tokenISNULL, tokenCONVERT, tokenCAST, tokenTRY_CAST, tokenGETDATE,
		tokenJOIN, tokenINNER, tokenLEFT, tokenRIGHT, tokenFULL,
		tokenOUTER, tokenCROSS, tokenON,
		tokenALTER, tokenCOLUMN, tokenCONSTRAINT, tokenINDEX,
		tokenVIEW, tokenPROCEDURE, tokenFUNCTION, tokenTRIGGER,
		tokenTRUNCATE, tokenIF, tokenEXISTS, tokenUNIQUE,
		tokenINCLUDE, tokenREFERENCES, tokenPRIMARY, tokenKEY,
		tokenFOREIGN, tokenCHECK, tokenADD,
		tokenMERGE, tokenUSING, tokenMATCHED, tokenOUTPUT,
		tokenUNION, tokenINTERSECT, tokenEXCEPT, tokenALL,
		tokenWITH, tokenANY, tokenSOME,
		tokenOVER, tokenPARTITION,
		tokenOFFSET, tokenFETCH, tokenNEXT, tokenFIRST,
		tokenONLY, tokenROWS, tokenROW, tokenLIMIT,
		tokenBEGIN, tokenTRAN, tokenTRANSACTION, tokenCOMMIT,
		tokenROLLBACK, tokenSAVE,
		tokenIDENTITY, tokenDEFAULT,
		tokenCOMPUTE,
		tokenDECLARE, tokenWHILE, tokenBREAK, tokenCONTINUE,
		tokenPRINT:
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
	case tokenColon:
		return "':'"
	case tokenNULL:
		return "NULL"
	case tokenTABLE:
		return "TABLE"
	case tokenBY:
		return "BY"
	case tokenVALUES:
		return "VALUES"
	case tokenAS:
		return "AS"
	case tokenCOLUMN:
		return "COLUMN"
	case tokenKEY:
		return "KEY"
	case tokenINDEX:
		return "INDEX"
	case tokenON:
		return "ON"
	case tokenEXISTS:
		return "EXISTS"
	case tokenREFERENCES:
		return "REFERENCES"
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

// parseThrow parses: THROW <errnum>, '<message>', <state>
func (p *parser) parseThrow() (*ThrowStmt, error) {
	p.lex.next() // consume THROW
	numTok := p.lex.next()
	if numTok.typ != tokenInt {
		return nil, p.error(fmt.Sprintf(
			"expected error number after THROW, got %q", numTok.val))
	}
	errNum, err := strconv.Atoi(numTok.val)
	if err != nil {
		return nil, p.error(fmt.Sprintf("invalid THROW error number: %s", numTok.val))
	}
	if err := p.expect(tokenComma); err != nil {
		return nil, err
	}
	msgTok := p.lex.next()
	if msgTok.typ != tokenString {
		return nil, p.error(fmt.Sprintf(
			"expected string message after THROW errnum, got %q", msgTok.val))
	}
	if err := p.expect(tokenComma); err != nil {
		return nil, err
	}
	stateTok := p.lex.next()
	if stateTok.typ != tokenInt {
		return nil, p.error(fmt.Sprintf(
			"expected state integer in THROW, got %q", stateTok.val))
	}
	state, err := strconv.Atoi(stateTok.val)
	if err != nil {
		return nil, p.error(fmt.Sprintf("invalid THROW state: %s", stateTok.val))
	}
	return &ThrowStmt{ErrNum: errNum, Message: msgTok.val, State: state}, nil
}

// parseReturn parses: RETURN [<value>]
func (p *parser) parseReturn() (*ReturnStmt, error) {
	p.lex.next() // consume RETURN
	stmt := &ReturnStmt{}
	if p.lex.peek().typ == tokenInt {
		tok := p.lex.next()
		n, err := strconv.Atoi(tok.val)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid RETURN value: %s", tok.val))
		}
		stmt.Value = &n
	}
	return stmt, nil
}

// parseGoto parses: GOTO <label>
// The label is an identifier. Any following "label:" definitions in the
// batch are consumed by parseBatch as labels but not tracked.
func (p *parser) parseGoto() (*GotoStmt, error) {
	p.lex.next() // consume GOTO
	label, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &GotoStmt{Label: label}, nil
}

// parseWaitfor parses: WAITFOR DELAY|TIME '<time_string>'
func (p *parser) parseWaitfor() (*WaitforStmt, error) {
	p.lex.next() // consume WAITFOR
	tok := p.lex.peek()
	var isDelay bool
	if tok.typ == tokenDELAY {
		isDelay = true
		p.lex.next()
	} else if tok.typ == tokenIdent && strings.EqualFold(tok.val, "TIME") {
		p.lex.next()
	} else {
		return nil, p.error(fmt.Sprintf(
			"expected DELAY or TIME after WAITFOR, got %q", tok.val))
	}
	timeTok := p.lex.next()
	if timeTok.typ != tokenString {
		return nil, p.error(fmt.Sprintf(
			"expected time string after WAITFOR DELAY/TIME, got %q", timeTok.val))
	}
	return &WaitforStmt{IsDelay: isDelay, Time: timeTok.val}, nil
}

// parseBeginTryCatch parses: TRY <stmts> END TRY BEGIN CATCH <stmts> END CATCH
// (BEGIN was already consumed; TRY is the next token).
func (p *parser) parseBeginTryCatch() (*BeginTryCatchStmt, error) {
	p.lex.next() // consume TRY
	stmt := &BeginTryCatchStmt{}
	// Parse TRY body until END TRY.
	for {
		for p.lex.peek().typ == tokenSemicolon {
			p.lex.next()
		}
		if p.lex.peek().typ == tokenEND {
			p.lex.next() // consume END
			if p.lex.peek().typ != tokenTRY {
				return nil, p.error("expected TRY after END in BEGIN TRY block")
			}
			p.lex.next() // consume TRY
			break
		}
		if p.lex.peek().typ == tokenEOF {
			return nil, p.error("unterminated BEGIN TRY block")
		}
		s, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		stmt.TryBody = append(stmt.TryBody, s)
	}
	// Expect BEGIN CATCH.
	if err := p.expect(tokenBEGIN); err != nil {
		return nil, err
	}
	if p.lex.peek().typ != tokenCATCH {
		return nil, p.error("expected CATCH after BEGIN in TRY/CATCH block")
	}
	p.lex.next() // consume CATCH
	// Parse CATCH body until END CATCH.
	for {
		for p.lex.peek().typ == tokenSemicolon {
			p.lex.next()
		}
		if p.lex.peek().typ == tokenEND {
			p.lex.next() // consume END
			if p.lex.peek().typ != tokenCATCH {
				return nil, p.error("expected CATCH after END in BEGIN CATCH block")
			}
			p.lex.next() // consume CATCH
			break
		}
		if p.lex.peek().typ == tokenEOF {
			return nil, p.error("unterminated BEGIN CATCH block")
		}
		s, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		stmt.CatchBody = append(stmt.CatchBody, s)
	}
	return stmt, nil
}

// parsePivot parses: PIVOT (agg_func(col) FOR pivot_col IN (v1, v2, ...)) alias
// The source table ref has already been parsed and is passed in.
func (p *parser) parsePivot(source TableRef) (TableRef, error) {
	p.lex.next() // consume PIVOT
	if err := p.expect(tokenLParen); err != nil {
		return TableRef{}, err
	}
	// Parse aggregate: func_name(col)
	aggName, err := p.expectIdent()
	if err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenLParen); err != nil {
		return TableRef{}, err
	}
	aggCol, err := p.parseExpr()
	if err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return TableRef{}, err
	}
	// FOR pivot_col IN (...)
	if err := p.expect(tokenFOR); err != nil {
		return TableRef{}, err
	}
	forCol, err := p.expectIdent()
	if err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenIN); err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenLParen); err != nil {
		return TableRef{}, err
	}
	var inValues []Expr
	for {
		val, err := p.parseExpr()
		if err != nil {
			return TableRef{}, err
		}
		inValues = append(inValues, val)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next()
	}
	if err := p.expect(tokenRParen); err != nil {
		return TableRef{}, err
	}
	// Close outer paren.
	if err := p.expect(tokenRParen); err != nil {
		return TableRef{}, err
	}
	// Parse optional alias.
	var alias string
	if p.lex.peek().typ == tokenAS {
		p.lex.next()
	}
	if p.lex.peek().typ == tokenIdent {
		alias = p.lex.next().val
	}
	pivot := &PivotClause{
		AggFunc:  aggName,
		AggCol:   aggCol,
		ForCol:   forCol,
		InValues: inValues,
	}
	return TableRef{
		Subquery: source.Subquery,
		Name:     source.Name,
		Alias:    alias,
		Pivot:    pivot,
	}, nil
}

// parseUnpivot parses: UNPIVOT (value_col FOR label_col IN (col1, col2, ...)) alias
func (p *parser) parseUnpivot(source TableRef) (TableRef, error) {
	p.lex.next() // consume UNPIVOT
	if err := p.expect(tokenLParen); err != nil {
		return TableRef{}, err
	}
	valueCol, err := p.expectIdent()
	if err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenFOR); err != nil {
		return TableRef{}, err
	}
	forCol, err := p.expectIdent()
	if err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenIN); err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenLParen); err != nil {
		return TableRef{}, err
	}
	var inCols []string
	for {
		col, err := p.expectIdent()
		if err != nil {
			return TableRef{}, err
		}
		inCols = append(inCols, col)
		if p.lex.peek().typ != tokenComma {
			break
		}
		p.lex.next()
	}
	if err := p.expect(tokenRParen); err != nil {
		return TableRef{}, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return TableRef{}, err
	}
	var alias string
	if p.lex.peek().typ == tokenAS {
		p.lex.next()
	}
	if p.lex.peek().typ == tokenIdent {
		alias = p.lex.next().val
	}
	unpivot := &UnpivotClause{
		ValueCol: valueCol,
		ForCol:   forCol,
		InCols:   inCols,
	}
	return TableRef{
		Subquery: source.Subquery,
		Name:     source.Name,
		Alias:    alias,
		Unpivot:  unpivot,
	}, nil
}
