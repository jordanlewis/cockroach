// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"strings"
	"testing"
)

func TestParseUse(t *testing.T) {
	batch, err := Parse("USE mydb")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(batch.Stmts))
	}
	use, ok := batch.Stmts[0].(*UseStmt)
	if !ok {
		t.Fatalf("expected *UseStmt, got %T", batch.Stmts[0])
	}
	if use.Database != "mydb" {
		t.Errorf("expected database=mydb, got %s", use.Database)
	}
}

func TestParseUseBracketQuoted(t *testing.T) {
	batch, err := Parse("USE [my database]")
	if err != nil {
		t.Fatal(err)
	}
	use := batch.Stmts[0].(*UseStmt)
	if use.Database != "my database" {
		t.Errorf("expected database='my database', got %s", use.Database)
	}
}

func TestParseCreateTable(t *testing.T) {
	sql := `CREATE TABLE dbo.users (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		email NVARCHAR(500) NULL,
		age TINYINT,
		balance DECIMAL(10, 2) NOT NULL
	)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(batch.Stmts))
	}
	ct, ok := batch.Stmts[0].(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", batch.Stmts[0])
	}
	if ct.Table != "dbo.users" {
		t.Errorf("expected table=dbo.users, got %s", ct.Table)
	}
	if len(ct.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(ct.Columns))
	}

	// Check column details.
	checks := []struct {
		name     string
		dataType string
		nullable *bool
	}{
		{"id", "INT", boolPtr(false)},
		{"name", "VARCHAR(255)", boolPtr(false)},
		{"email", "NVARCHAR(500)", boolPtr(true)},
		{"age", "TINYINT", nil},
		{"balance", "DECIMAL(10, 2)", boolPtr(false)},
	}
	for i, c := range checks {
		col := ct.Columns[i]
		if col.Name != c.name {
			t.Errorf("col %d: expected name=%s, got %s", i, c.name, col.Name)
		}
		if col.DataType != c.dataType {
			t.Errorf("col %d: expected type=%s, got %s", i, c.dataType, col.DataType)
		}
		if c.nullable == nil {
			if col.Nullable != nil {
				t.Errorf("col %d: expected nullable=nil, got %v", i, *col.Nullable)
			}
		} else if col.Nullable == nil || *col.Nullable != *c.nullable {
			t.Errorf("col %d: expected nullable=%v, got %v", i, *c.nullable, col.Nullable)
		}
	}
}

func TestParseInsert(t *testing.T) {
	sql := `INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins, ok := batch.Stmts[0].(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", batch.Stmts[0])
	}
	if ins.Table != "users" {
		t.Errorf("expected table=users, got %s", ins.Table)
	}
	if len(ins.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ins.Columns))
	}
	if ins.Columns[0] != "name" || ins.Columns[1] != "age" {
		t.Errorf("unexpected columns: %v", ins.Columns)
	}
	if len(ins.Values) != 2 {
		t.Fatalf("expected 2 value rows, got %d", len(ins.Values))
	}
	// Check first row values.
	if ins.Values[0][0].(*StringLit).Value != "Alice" {
		t.Errorf("expected Alice, got %s", ins.Values[0][0])
	}
	if ins.Values[0][1].(*IntLit).Value != 30 {
		t.Errorf("expected 30, got %s", ins.Values[0][1])
	}
}

func TestParseInsertWithoutInto(t *testing.T) {
	// T-SQL allows INSERT without INTO.
	sql := `INSERT users VALUES (1, 'test')`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := batch.Stmts[0].(*InsertStmt)
	if ins.Table != "users" {
		t.Errorf("expected table=users, got %s", ins.Table)
	}
}

func TestParseSelectBasic(t *testing.T) {
	sql := `SELECT name, age FROM users WHERE age > 21 ORDER BY name ASC`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := batch.Stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", batch.Stmts[0])
	}
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(sel.OrderBy))
	}
}

func TestParseSelectTop(t *testing.T) {
	sql := `SELECT TOP 10 * FROM orders`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Top == nil || *sel.Top != 10 {
		t.Errorf("expected TOP 10, got %v", sel.Top)
	}
	if _, ok := sel.Columns[0].Expr.(*StarExpr); !ok {
		t.Errorf("expected StarExpr, got %T", sel.Columns[0].Expr)
	}
}

func TestParseSelectWithAlias(t *testing.T) {
	sql := `SELECT u.name AS username FROM users u`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Columns[0].Alias != "username" {
		t.Errorf("expected alias=username, got %s", sel.Columns[0].Alias)
	}
	if sel.From[0].Alias != "u" {
		t.Errorf("expected table alias=u, got %s", sel.From[0].Alias)
	}
}

func TestParseGOBatchSeparator(t *testing.T) {
	sql := `USE mydb
GO
SELECT * FROM users
GO
SELECT * FROM orders`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(batch.Stmts))
	}
	if _, ok := batch.Stmts[0].(*UseStmt); !ok {
		t.Errorf("stmt 0: expected *UseStmt, got %T", batch.Stmts[0])
	}
	if _, ok := batch.Stmts[1].(*SelectStmt); !ok {
		t.Errorf("stmt 1: expected *SelectStmt, got %T", batch.Stmts[1])
	}
	if _, ok := batch.Stmts[2].(*SelectStmt); !ok {
		t.Errorf("stmt 2: expected *SelectStmt, got %T", batch.Stmts[2])
	}
}

func TestParseSemicolonSeparator(t *testing.T) {
	sql := `USE mydb; SELECT * FROM users; SELECT TOP 5 * FROM orders`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(batch.Stmts))
	}
}

func TestParseISNULL(t *testing.T) {
	sql := `SELECT ISNULL(name, 'unknown') FROM users`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	fc, ok := sel.Columns[0].Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expected *FuncCallExpr, got %T", sel.Columns[0].Expr)
	}
	if strings.ToUpper(fc.Name) != "ISNULL" {
		t.Errorf("expected ISNULL, got %s", fc.Name)
	}
	if len(fc.Args) != 2 {
		t.Errorf("expected 2 args, got %d", len(fc.Args))
	}
}

func TestParseCONVERT(t *testing.T) {
	sql := `SELECT CONVERT(VARCHAR(20), created_at, 120) FROM orders`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	conv, ok := sel.Columns[0].Expr.(*ConvertExpr)
	if !ok {
		t.Fatalf("expected *ConvertExpr, got %T", sel.Columns[0].Expr)
	}
	if conv.DataType != "VARCHAR(20)" {
		t.Errorf("expected VARCHAR(20), got %s", conv.DataType)
	}
	if conv.Style == nil {
		t.Fatal("expected style argument")
	}
	if conv.Style.(*IntLit).Value != 120 {
		t.Errorf("expected style=120, got %s", conv.Style)
	}
}

func TestParseCONVERTWithoutStyle(t *testing.T) {
	sql := `SELECT CONVERT(INT, '42')`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	conv := sel.Columns[0].Expr.(*ConvertExpr)
	if conv.DataType != "INT" {
		t.Errorf("expected INT, got %s", conv.DataType)
	}
	if conv.Style != nil {
		t.Errorf("expected no style, got %s", conv.Style)
	}
}

func TestParseGETDATE(t *testing.T) {
	sql := `SELECT GETDATE()`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	fc, ok := sel.Columns[0].Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expected *FuncCallExpr, got %T", sel.Columns[0].Expr)
	}
	if fc.Name != "GETDATE" {
		t.Errorf("expected GETDATE, got %s", fc.Name)
	}
	if len(fc.Args) != 0 {
		t.Errorf("expected 0 args, got %d", len(fc.Args))
	}
}

func TestParseStringConcat(t *testing.T) {
	sql := `SELECT first_name + ' ' + last_name AS full_name FROM users`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Columns[0].Alias != "full_name" {
		t.Errorf("expected alias=full_name, got %s", sel.Columns[0].Alias)
	}
	// The expression should be a binary + tree.
	bin, ok := sel.Columns[0].Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr, got %T", sel.Columns[0].Expr)
	}
	if bin.Op != "+" {
		t.Errorf("expected op=+, got %s", bin.Op)
	}
}

func TestParseISNULLCheck(t *testing.T) {
	sql := `SELECT * FROM users WHERE name IS NULL`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr, got %T", sel.Where)
	}
	if bin.Op != "IS" {
		t.Errorf("expected op=IS, got %s", bin.Op)
	}
}

func TestParseISNotNull(t *testing.T) {
	sql := `SELECT * FROM users WHERE name IS NOT NULL`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	bin := sel.Where.(*BinaryExpr)
	if bin.Op != "IS NOT" {
		t.Errorf("expected op='IS NOT', got %s", bin.Op)
	}
}

func TestParseBooleanLogic(t *testing.T) {
	sql := `SELECT * FROM users WHERE age > 18 AND name = 'Alice' OR active = 1`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	// Should parse as (age > 18 AND name = 'Alice') OR (active = 1)
	// because AND binds tighter than OR.
	or, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr (OR), got %T", sel.Where)
	}
	if or.Op != "OR" {
		t.Errorf("expected top-level OR, got %s", or.Op)
	}
	and, ok := or.Left.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr (AND), got %T", or.Left)
	}
	if and.Op != "AND" {
		t.Errorf("expected AND, got %s", and.Op)
	}
}

func TestParseNOT(t *testing.T) {
	sql := `SELECT * FROM users WHERE NOT active = 1`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	unary, ok := sel.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected *UnaryExpr, got %T", sel.Where)
	}
	if unary.Op != "NOT" {
		t.Errorf("expected NOT, got %s", unary.Op)
	}
}

func TestParseBracketIdentifiers(t *testing.T) {
	sql := `SELECT [user name], [order] FROM [my table]`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	ident := sel.Columns[0].Expr.(*IdentExpr)
	if ident.Parts[0] != "user name" {
		t.Errorf("expected 'user name', got %s", ident.Parts[0])
	}
}

func TestParseCaseInsensitiveKeywords(t *testing.T) {
	sql := `select TOP 5 * from Users where Age > 18 order by Name desc`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Top == nil || *sel.Top != 5 {
		t.Errorf("expected TOP 5")
	}
	if len(sel.OrderBy) != 1 || !sel.OrderBy[0].Desc {
		t.Errorf("expected DESC order")
	}
}

func TestParseComments(t *testing.T) {
	sql := `-- This is a comment
SELECT * FROM users /* inline comment */ WHERE id = 1`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(batch.Stmts))
	}
}

func TestParseOrderByMultiple(t *testing.T) {
	sql := `SELECT * FROM users ORDER BY last_name ASC, first_name DESC`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY, got %d", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Desc {
		t.Error("first ORDER BY should be ASC")
	}
	if !sel.OrderBy[1].Desc {
		t.Error("second ORDER BY should be DESC")
	}
}

func TestParseNullLiteral(t *testing.T) {
	sql := `INSERT INTO users (name) VALUES (NULL)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := batch.Stmts[0].(*InsertStmt)
	if _, ok := ins.Values[0][0].(*NullLit); !ok {
		t.Errorf("expected NullLit, got %T", ins.Values[0][0])
	}
}

func TestParseEscapedString(t *testing.T) {
	sql := `SELECT 'it''s a test'`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	str := sel.Columns[0].Expr.(*StringLit)
	if str.Value != "it's a test" {
		t.Errorf("expected \"it's a test\", got %s", str.Value)
	}
}

func TestParseExpr(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"1 + 2", "1 + 2"},
		{"1 + 2 * 3", "1 + 2 * 3"},
		{"(1 + 2) * 3", "(1 + 2) * 3"},
		{"a = 1 AND b = 2", "a = 1 AND b = 2"},
		{"NOT x = 1", "NOT x = 1"},
		{"-5", "- 5"},
		{"a.b.c", "a.b.c"},
		{"GETDATE()", "GETDATE()"},
		{"ISNULL(x, 0)", "ISNULL(x, 0)"},
		{"CONVERT(VARCHAR(10), x)", "CONVERT(VARCHAR(10), x)"},
		{"a + b + c", "a + b + c"},
		{"a LIKE '%test%'", "a LIKE '%test%'"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := ParseExpr(tt.input)
			if err != nil {
				t.Fatalf("ParseExpr(%q) error: %v", tt.input, err)
			}
			got := expr.String()
			if got != tt.want {
				t.Errorf("ParseExpr(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseComplexQuery(t *testing.T) {
	sql := `
		USE mydb
		GO
		CREATE TABLE dbo.employees (
			id INT NOT NULL,
			first_name NVARCHAR(100) NOT NULL,
			last_name NVARCHAR(100) NOT NULL,
			hire_date DATETIME NULL,
			salary DECIMAL(10, 2) NOT NULL
		)
		GO
		INSERT INTO dbo.employees (id, first_name, last_name, hire_date, salary)
		VALUES (1, 'John', 'Doe', GETDATE(), 75000.00)
		GO
		SELECT TOP 10
			e.first_name + ' ' + e.last_name AS full_name,
			CONVERT(VARCHAR(10), e.hire_date, 120) AS hire_date,
			ISNULL(e.salary, 0) AS salary
		FROM dbo.employees e
		WHERE e.salary > 50000 AND e.hire_date IS NOT NULL
		ORDER BY e.salary DESC, e.last_name ASC
	`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 4 {
		t.Fatalf("expected 4 statements, got %d", len(batch.Stmts))
	}
	if _, ok := batch.Stmts[0].(*UseStmt); !ok {
		t.Errorf("stmt 0: expected *UseStmt")
	}
	if _, ok := batch.Stmts[1].(*CreateTableStmt); !ok {
		t.Errorf("stmt 1: expected *CreateTableStmt")
	}
	if _, ok := batch.Stmts[2].(*InsertStmt); !ok {
		t.Errorf("stmt 2: expected *InsertStmt")
	}
	sel, ok := batch.Stmts[3].(*SelectStmt)
	if !ok {
		t.Fatalf("stmt 3: expected *SelectStmt")
	}
	if sel.Top == nil || *sel.Top != 10 {
		t.Error("expected TOP 10")
	}
	if len(sel.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(sel.Columns))
	}
	if len(sel.OrderBy) != 2 {
		t.Errorf("expected 2 ORDER BY, got %d", len(sel.OrderBy))
	}
}

func TestParseSelectWithoutFrom(t *testing.T) {
	sql := `SELECT 1, 'hello', GETDATE()`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(sel.Columns))
	}
	if len(sel.From) != 0 {
		t.Errorf("expected no FROM, got %d tables", len(sel.From))
	}
}

func TestParseFunctionCall(t *testing.T) {
	sql := `SELECT COUNT(id), SUM(salary) FROM employees`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	fn1 := sel.Columns[0].Expr.(*FuncCallExpr)
	if strings.ToUpper(fn1.Name) != "COUNT" {
		t.Errorf("expected COUNT, got %s", fn1.Name)
	}
	fn2 := sel.Columns[1].Expr.(*FuncCallExpr)
	if strings.ToUpper(fn2.Name) != "SUM" {
		t.Errorf("expected SUM, got %s", fn2.Name)
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"INVALID", "unexpected token"},
		{"SELECT", "unexpected token"},
		{"CREATE", "expected TABLE"},
		{"USE", "expected identifier"},
		{"SELECT * FROM users WHERE", "unexpected token"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err == nil {
				t.Fatalf("expected error for %q", tt.input)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestLexerTokenizes(t *testing.T) {
	input := "SELECT TOP 10 * FROM [my table] WHERE id = 1"
	lex := newLexer(input)
	expected := []tokenType{
		tokenSELECT, tokenTOP, tokenInt, tokenStar,
		tokenFROM, tokenIdent, tokenWHERE,
		tokenIdent, tokenEq, tokenInt,
	}
	for i, exp := range expected {
		tok := lex.next()
		if tok.typ != exp {
			t.Errorf("token %d: expected type %d, got %d (%q)", i, exp, tok.typ, tok.val)
		}
	}
	final := lex.next()
	if final.typ != tokenEOF {
		t.Errorf("expected EOF, got %d (%q)", final.typ, final.val)
	}
}

func TestLexerComparisonOperators(t *testing.T) {
	tests := []struct {
		input string
		typ   tokenType
		val   string
	}{
		{"=", tokenEq, "="},
		{"<>", tokenNeq, "<>"},
		{"!=", tokenNeq, "!="},
		{"<", tokenLT, "<"},
		{">", tokenGT, ">"},
		{"<=", tokenLTE, "<="},
		{">=", tokenGTE, ">="},
	}
	for _, tt := range tests {
		lex := newLexer(tt.input)
		tok := lex.next()
		if tok.typ != tt.typ || tok.val != tt.val {
			t.Errorf("lexer(%q) = (%d, %q), want (%d, %q)",
				tt.input, tok.typ, tok.val, tt.typ, tt.val)
		}
	}
}

func TestParseNotLike(t *testing.T) {
	sql := `SELECT * FROM users WHERE name NOT LIKE '%test%'`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	bin := sel.Where.(*BinaryExpr)
	if bin.Op != "NOT LIKE" {
		t.Errorf("expected op='NOT LIKE', got %s", bin.Op)
	}
}

func TestParseFloat(t *testing.T) {
	sql := `SELECT 3.14`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	fl := sel.Columns[0].Expr.(*FloatLit)
	if fl.Value != 3.14 {
		t.Errorf("expected 3.14, got %f", fl.Value)
	}
}

func TestFormatBatch(t *testing.T) {
	batch := &Batch{
		Stmts: []Statement{
			&UseStmt{Database: "mydb"},
			&SelectStmt{
				Columns: []SelectColumn{{Expr: &StarExpr{}}},
				From:    []TableRef{{Name: "users"}},
			},
		},
	}
	got := FormatBatch(batch)
	if !strings.Contains(got, "USE mydb") {
		t.Errorf("expected USE mydb in output: %s", got)
	}
	if !strings.Contains(got, "SELECT *") {
		t.Errorf("expected SELECT * in output: %s", got)
	}
}

func TestRoundTrip(t *testing.T) {
	// Parse, format, and re-parse to verify consistency.
	sql := `SELECT TOP 5 name, age FROM users WHERE age > 18 ORDER BY name ASC`
	batch1, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	formatted := batch1.Stmts[0].String()
	batch2, err := Parse(formatted)
	if err != nil {
		t.Fatalf("re-parse failed: %v\nformatted: %s", err, formatted)
	}
	if batch2.Stmts[0].String() != formatted {
		t.Errorf("round-trip mismatch:\n  first:  %s\n  second: %s",
			formatted, batch2.Stmts[0].String())
	}
}

func TestParseIN(t *testing.T) {
	sql := `SELECT * FROM users WHERE id IN (1, 2, 3)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	in, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected *InExpr, got %T", sel.Where)
	}
	if in.Not {
		t.Error("expected Not=false")
	}
	if len(in.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(in.Values))
	}
}

func TestParseNotIN(t *testing.T) {
	sql := `SELECT * FROM users WHERE id NOT IN (1, 2)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	in := sel.Where.(*InExpr)
	if !in.Not {
		t.Error("expected Not=true")
	}
	if len(in.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(in.Values))
	}
}

func TestParseBETWEEN(t *testing.T) {
	sql := `SELECT * FROM users WHERE age BETWEEN 18 AND 65`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	btwn, ok := sel.Where.(*BetweenExpr)
	if !ok {
		t.Fatalf("expected *BetweenExpr, got %T", sel.Where)
	}
	if btwn.Not {
		t.Error("expected Not=false")
	}
	if btwn.Low.(*IntLit).Value != 18 {
		t.Errorf("expected low=18, got %s", btwn.Low)
	}
	if btwn.High.(*IntLit).Value != 65 {
		t.Errorf("expected high=65, got %s", btwn.High)
	}
}

func TestParseNotBETWEEN(t *testing.T) {
	sql := `SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	btwn := sel.Where.(*BetweenExpr)
	if !btwn.Not {
		t.Error("expected Not=true")
	}
}

// Phase 2 tests: subqueries, UNION, CTE, window functions, OFFSET-FETCH.

func TestParseScalarSubquery(t *testing.T) {
	sql := `SELECT (SELECT MAX(id) FROM orders) AS max_id FROM users`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	sub, ok := sel.Columns[0].Expr.(*SubqueryExpr)
	if !ok {
		t.Fatalf("expected *SubqueryExpr, got %T", sel.Columns[0].Expr)
	}
	innerSel, ok := sub.Select.(*SelectStmt)
	if !ok {
		t.Fatalf("expected inner *SelectStmt, got %T", sub.Select)
	}
	if len(innerSel.Columns) != 1 {
		t.Errorf("expected 1 inner column, got %d", len(innerSel.Columns))
	}
}

func TestParseSubqueryInWhere(t *testing.T) {
	sql := `SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr, got %T", sel.Where)
	}
	if bin.Op != ">" {
		t.Errorf("expected op '>', got %s", bin.Op)
	}
	_, ok = bin.Right.(*SubqueryExpr)
	if !ok {
		t.Fatalf("expected *SubqueryExpr on RHS, got %T", bin.Right)
	}
}

func TestParseExists(t *testing.T) {
	sql := `SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	exists, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected *ExistsExpr, got %T", sel.Where)
	}
	if exists.Not {
		t.Error("expected Not=false")
	}
}

func TestParseNotExists(t *testing.T) {
	sql := `SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	exists, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected *ExistsExpr, got %T", sel.Where)
	}
	if !exists.Not {
		t.Error("expected Not=true")
	}
}

func TestParseINSubquery(t *testing.T) {
	sql := `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	in, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected *InExpr, got %T", sel.Where)
	}
	if in.Subquery == nil {
		t.Fatal("expected subquery in IN expression")
	}
	if in.Values != nil {
		t.Error("expected nil values for subquery IN")
	}
	if in.Not {
		t.Error("expected Not=false")
	}
}

func TestParseNotINSubquery(t *testing.T) {
	sql := `SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	in := sel.Where.(*InExpr)
	if in.Subquery == nil {
		t.Fatal("expected subquery in NOT IN expression")
	}
	if !in.Not {
		t.Error("expected Not=true")
	}
}

func TestParseDerivedTable(t *testing.T) {
	sql := `SELECT sub.name FROM (SELECT name FROM users WHERE age > 21) sub`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.From) != 1 {
		t.Fatalf("expected 1 FROM ref, got %d", len(sel.From))
	}
	ref := sel.From[0]
	if ref.Subquery == nil {
		t.Fatal("expected subquery in FROM")
	}
	if ref.Alias != "sub" {
		t.Errorf("expected alias=sub, got %s", ref.Alias)
	}
}

func TestParseAnyAll(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		op   string
		kind string
	}{
		{"any", "SELECT * FROM t WHERE x > ANY (SELECT y FROM t2)", ">", "ANY"},
		{"all", "SELECT * FROM t WHERE x = ALL (SELECT y FROM t2)", "=", "ALL"},
		{"some", "SELECT * FROM t WHERE x < SOME (SELECT y FROM t2)", "<", "SOME"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			sel := batch.Stmts[0].(*SelectStmt)
			aa, ok := sel.Where.(*AnyAllExpr)
			if !ok {
				t.Fatalf("expected *AnyAllExpr, got %T", sel.Where)
			}
			if aa.Op != tt.op {
				t.Errorf("expected op=%s, got %s", tt.op, aa.Op)
			}
			if aa.Kind != tt.kind {
				t.Errorf("expected kind=%s, got %s", tt.kind, aa.Kind)
			}
		})
	}
}

func TestParseUNION(t *testing.T) {
	sql := `SELECT name FROM users UNION SELECT name FROM admins`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cs, ok := batch.Stmts[0].(*CompoundSelectStmt)
	if !ok {
		t.Fatalf("expected *CompoundSelectStmt, got %T", batch.Stmts[0])
	}
	if cs.Op != "UNION" {
		t.Errorf("expected op=UNION, got %s", cs.Op)
	}
}

func TestParseUNIONALL(t *testing.T) {
	sql := `SELECT name FROM users UNION ALL SELECT name FROM admins`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cs := batch.Stmts[0].(*CompoundSelectStmt)
	if cs.Op != "UNION ALL" {
		t.Errorf("expected op='UNION ALL', got %s", cs.Op)
	}
}

func TestParseINTERSECT(t *testing.T) {
	sql := `SELECT id FROM users INTERSECT SELECT id FROM admins`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cs := batch.Stmts[0].(*CompoundSelectStmt)
	if cs.Op != "INTERSECT" {
		t.Errorf("expected op=INTERSECT, got %s", cs.Op)
	}
}

func TestParseEXCEPT(t *testing.T) {
	sql := `SELECT id FROM users EXCEPT SELECT id FROM banned`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cs := batch.Stmts[0].(*CompoundSelectStmt)
	if cs.Op != "EXCEPT" {
		t.Errorf("expected op=EXCEPT, got %s", cs.Op)
	}
}

func TestParseCompoundWithOrderBy(t *testing.T) {
	sql := `SELECT name FROM users UNION ALL SELECT name FROM admins ORDER BY name ASC`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cs := batch.Stmts[0].(*CompoundSelectStmt)
	if len(cs.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY on compound, got %d", len(cs.OrderBy))
	}
	// The right SELECT should NOT have ORDER BY (it was lifted).
	rightSel := cs.Right.(*SelectStmt)
	if len(rightSel.OrderBy) != 0 {
		t.Error("expected ORDER BY to be lifted from right SELECT")
	}
}

func TestParseChainedUnion(t *testing.T) {
	sql := `SELECT a FROM t1 UNION SELECT b FROM t2 UNION ALL SELECT c FROM t3`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cs := batch.Stmts[0].(*CompoundSelectStmt)
	if cs.Op != "UNION ALL" {
		t.Errorf("expected outer op='UNION ALL', got %s", cs.Op)
	}
	inner, ok := cs.Left.(*CompoundSelectStmt)
	if !ok {
		t.Fatalf("expected inner *CompoundSelectStmt, got %T", cs.Left)
	}
	if inner.Op != "UNION" {
		t.Errorf("expected inner op=UNION, got %s", inner.Op)
	}
}

func TestParseCTE(t *testing.T) {
	sql := `WITH active_users AS (SELECT * FROM users WHERE active = 1)
		SELECT * FROM active_users`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	with, ok := batch.Stmts[0].(*WithStmt)
	if !ok {
		t.Fatalf("expected *WithStmt, got %T", batch.Stmts[0])
	}
	if len(with.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(with.CTEs))
	}
	if with.CTEs[0].Name != "active_users" {
		t.Errorf("expected CTE name=active_users, got %s", with.CTEs[0].Name)
	}
}

func TestParseMultipleCTEs(t *testing.T) {
	sql := `WITH
		a AS (SELECT 1 AS x),
		b AS (SELECT 2 AS y)
		SELECT * FROM a, b`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	with := batch.Stmts[0].(*WithStmt)
	if len(with.CTEs) != 2 {
		t.Fatalf("expected 2 CTEs, got %d", len(with.CTEs))
	}
	if with.CTEs[0].Name != "a" {
		t.Errorf("expected CTE 0 name=a, got %s", with.CTEs[0].Name)
	}
	if with.CTEs[1].Name != "b" {
		t.Errorf("expected CTE 1 name=b, got %s", with.CTEs[1].Name)
	}
}

func TestParseWindowFunction(t *testing.T) {
	sql := `SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	w, ok := sel.Columns[0].Expr.(*WindowExpr)
	if !ok {
		t.Fatalf("expected *WindowExpr, got %T", sel.Columns[0].Expr)
	}
	if strings.ToUpper(w.Func.Name) != "ROW_NUMBER" {
		t.Errorf("expected ROW_NUMBER, got %s", w.Func.Name)
	}
	if len(w.OrderBy) != 1 {
		t.Errorf("expected 1 ORDER BY in window, got %d", len(w.OrderBy))
	}
	if len(w.PartitionBy) != 0 {
		t.Errorf("expected 0 PARTITION BY, got %d", len(w.PartitionBy))
	}
}

func TestParseWindowFunctionWithPartition(t *testing.T) {
	sql := `SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	w := sel.Columns[0].Expr.(*WindowExpr)
	if strings.ToUpper(w.Func.Name) != "RANK" {
		t.Errorf("expected RANK, got %s", w.Func.Name)
	}
	if len(w.PartitionBy) != 1 {
		t.Fatalf("expected 1 PARTITION BY, got %d", len(w.PartitionBy))
	}
	if len(w.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(w.OrderBy))
	}
	if !w.OrderBy[0].Desc {
		t.Error("expected DESC order")
	}
}

func TestParseWindowNTILE(t *testing.T) {
	sql := `SELECT NTILE(4) OVER (PARTITION BY region ORDER BY revenue DESC) FROM sales`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	w := sel.Columns[0].Expr.(*WindowExpr)
	if strings.ToUpper(w.Func.Name) != "NTILE" {
		t.Errorf("expected NTILE, got %s", w.Func.Name)
	}
	if len(w.Func.Args) != 1 {
		t.Errorf("expected 1 arg to NTILE, got %d", len(w.Func.Args))
	}
}

func TestParseOffsetFetch(t *testing.T) {
	sql := `SELECT * FROM users ORDER BY id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Offset == nil || *sel.Offset != 10 {
		t.Errorf("expected OFFSET 10, got %v", sel.Offset)
	}
	if sel.Fetch == nil || *sel.Fetch != 5 {
		t.Errorf("expected FETCH 5, got %v", sel.Fetch)
	}
}

func TestParseOffsetOnly(t *testing.T) {
	sql := `SELECT * FROM users ORDER BY id OFFSET 20 ROWS`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Offset == nil || *sel.Offset != 20 {
		t.Errorf("expected OFFSET 20, got %v", sel.Offset)
	}
	if sel.Fetch != nil {
		t.Errorf("expected no FETCH, got %v", sel.Fetch)
	}
}

func TestParseOffsetFetchFirst(t *testing.T) {
	// FETCH FIRST is a synonym for FETCH NEXT.
	sql := `SELECT * FROM users ORDER BY id OFFSET 0 ROW FETCH FIRST 10 ROW ONLY`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Offset == nil || *sel.Offset != 0 {
		t.Errorf("expected OFFSET 0, got %v", sel.Offset)
	}
	if sel.Fetch == nil || *sel.Fetch != 10 {
		t.Errorf("expected FETCH 10, got %v", sel.Fetch)
	}
}

func TestParseRowsLimit(t *testing.T) {
	sql := `SELECT * FROM users ORDER BY id ROWS LIMIT 10 OFFSET 5`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Fetch == nil || *sel.Fetch != 10 {
		t.Errorf("expected LIMIT (Fetch) 10, got %v", sel.Fetch)
	}
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("expected OFFSET 5, got %v", sel.Offset)
	}
	if !sel.RowsLimitSyntax {
		t.Error("expected RowsLimitSyntax to be true")
	}
}

func TestParseRowsLimitOnly(t *testing.T) {
	// ROWS LIMIT without OFFSET.
	sql := `SELECT * FROM users ORDER BY id ROWS LIMIT 20`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if sel.Fetch == nil || *sel.Fetch != 20 {
		t.Errorf("expected LIMIT (Fetch) 20, got %v", sel.Fetch)
	}
	if sel.Offset != nil {
		t.Errorf("expected no OFFSET, got %v", sel.Offset)
	}
	if !sel.RowsLimitSyntax {
		t.Error("expected RowsLimitSyntax to be true")
	}
}

func TestParseRowsLimitRoundTrip(t *testing.T) {
	sql := `SELECT * FROM users ORDER BY id ROWS LIMIT 10 OFFSET 5`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	// The String() method should reproduce the Sybase syntax.
	got := batch.Stmts[0].String()
	expected := "SELECT * FROM users ORDER BY id ASC ROWS LIMIT 10 OFFSET 5"
	if got != expected {
		t.Errorf("String() round-trip:\n  got:  %s\n  want: %s", got, expected)
	}
}

func TestParseUnionInSubquery(t *testing.T) {
	sql := `SELECT * FROM users WHERE id IN (SELECT id FROM admins UNION SELECT id FROM mods)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	in := sel.Where.(*InExpr)
	if in.Subquery == nil {
		t.Fatal("expected subquery in IN")
	}
	_, ok := in.Subquery.(*CompoundSelectStmt)
	if !ok {
		t.Fatalf("expected *CompoundSelectStmt in IN subquery, got %T", in.Subquery)
	}
}

func TestParseDerivedTableWithUnion(t *testing.T) {
	sql := `SELECT * FROM (SELECT a FROM t1 UNION SELECT b FROM t2) sub`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	ref := sel.From[0]
	if ref.Subquery == nil {
		t.Fatal("expected subquery in FROM")
	}
	_, ok := ref.Subquery.(*CompoundSelectStmt)
	if !ok {
		t.Fatalf("expected *CompoundSelectStmt in derived table, got %T", ref.Subquery)
	}
}

func TestParseInsertSelect(t *testing.T) {
	sql := `INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = 0`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := batch.Stmts[0].(*InsertStmt)
	if ins.Table != "archive" {
		t.Errorf("expected table=archive, got %s", ins.Table)
	}
	if len(ins.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ins.Columns))
	}
	if ins.Select == nil {
		t.Fatal("expected non-nil Select for INSERT...SELECT")
	}
	if len(ins.Values) != 0 {
		t.Error("expected no Values rows for INSERT...SELECT")
	}
}

func TestParseInsertOutput(t *testing.T) {
	sql := `INSERT INTO users (name) OUTPUT inserted.id VALUES ('alice')`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := batch.Stmts[0].(*InsertStmt)
	if len(ins.Output) != 1 {
		t.Fatalf("expected 1 OUTPUT column, got %d", len(ins.Output))
	}
}

func TestParseMerge(t *testing.T) {
	sql := `MERGE INTO target t
		USING source s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET t.name = s.name, t.val = s.val
		WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	merge, ok := batch.Stmts[0].(*MergeStmt)
	if !ok {
		t.Fatalf("expected *MergeStmt, got %T", batch.Stmts[0])
	}
	if merge.Target.Name != "target" {
		t.Errorf("expected target=target, got %s", merge.Target.Name)
	}
	if merge.Target.Alias != "t" {
		t.Errorf("expected target alias=t, got %s", merge.Target.Alias)
	}
	if merge.Source.Name != "source" {
		t.Errorf("expected source=source, got %s", merge.Source.Name)
	}
	if merge.Matched == nil {
		t.Fatal("expected non-nil Matched clause")
	}
	if len(merge.Matched.Assignments) != 2 {
		t.Errorf("expected 2 assignments in WHEN MATCHED, got %d",
			len(merge.Matched.Assignments))
	}
	if merge.NotMatched == nil {
		t.Fatal("expected non-nil NotMatched clause")
	}
	if len(merge.NotMatched.Columns) != 3 {
		t.Errorf("expected 3 columns in WHEN NOT MATCHED, got %d",
			len(merge.NotMatched.Columns))
	}
}

func TestParseMergeDelete(t *testing.T) {
	sql := `MERGE INTO target USING source ON target.id = source.id
		WHEN MATCHED THEN DELETE`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	merge := batch.Stmts[0].(*MergeStmt)
	if merge.Matched == nil || !merge.Matched.Delete {
		t.Error("expected WHEN MATCHED THEN DELETE")
	}
}

func TestParseDeleteJoin(t *testing.T) {
	// DELETE <alias> FROM <table alias> JOIN ... — the target is the alias.
	sql := `DELETE t FROM orders t JOIN cancelled c ON t.id = c.order_id`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := batch.Stmts[0].(*DeleteStmt)
	if del.Table != "t" {
		t.Errorf("expected table=t (alias target), got %s", del.Table)
	}
	if len(del.From) != 1 {
		t.Fatalf("expected 1 FROM ref, got %d", len(del.From))
	}
	if del.From[0].Name != "orders" {
		t.Errorf("expected FROM table=orders, got %s", del.From[0].Name)
	}
	if len(del.Joins) != 1 {
		t.Fatalf("expected 1 JOIN, got %d", len(del.Joins))
	}
}

func TestParseDeleteFromJoin(t *testing.T) {
	// DELETE FROM <table_ref> JOIN <table2> ON ... variant.
	sql := `DELETE FROM orders o JOIN cancelled c ON o.id = c.order_id WHERE c.reason = 'fraud'`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := batch.Stmts[0].(*DeleteStmt)
	if len(del.From) != 1 {
		t.Fatalf("expected 1 FROM ref, got %d", len(del.From))
	}
	if len(del.Joins) != 1 {
		t.Fatalf("expected 1 JOIN, got %d", len(del.Joins))
	}
	if del.Where == nil {
		t.Error("expected non-nil WHERE clause")
	}
}

func TestParseDeleteOutput(t *testing.T) {
	sql := `DELETE FROM users OUTPUT deleted.id, deleted.name WHERE active = 0`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := batch.Stmts[0].(*DeleteStmt)
	if len(del.Output) != 2 {
		t.Fatalf("expected 2 OUTPUT columns, got %d", len(del.Output))
	}
}

func TestParseUpdateFrom(t *testing.T) {
	sql := `UPDATE t SET t.name = s.name, t.val = s.val
		FROM target t JOIN source s ON t.id = s.id WHERE s.active = 1`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := batch.Stmts[0].(*UpdateStmt)
	if upd.Table != "t" {
		t.Errorf("expected table=t, got %s", upd.Table)
	}
	if len(upd.Assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(upd.Assignments))
	}
	if upd.Assignments[0].Column != "t.name" {
		t.Errorf("expected col=t.name, got %s", upd.Assignments[0].Column)
	}
	if len(upd.From) != 1 {
		t.Fatalf("expected 1 FROM ref, got %d", len(upd.From))
	}
	if len(upd.Joins) != 1 {
		t.Fatalf("expected 1 JOIN, got %d", len(upd.Joins))
	}
}

func TestParseUpdateOutput(t *testing.T) {
	sql := `UPDATE users SET name = 'bob' OUTPUT inserted.id, inserted.name WHERE id = 1`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := batch.Stmts[0].(*UpdateStmt)
	if len(upd.Output) != 2 {
		t.Fatalf("expected 2 OUTPUT columns, got %d", len(upd.Output))
	}
}

func TestParseIdentityColumn(t *testing.T) {
	sql := `CREATE TABLE t (id INT IDENTITY(1,1), name VARCHAR(50))`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}

	col := ct.Columns[0]
	if col.Name != "id" {
		t.Errorf("expected name=id, got %s", col.Name)
	}
	if col.DataType != "INT" {
		t.Errorf("expected type=INT, got %s", col.DataType)
	}
	if col.Identity == nil {
		t.Fatal("expected IDENTITY, got nil")
	}
	if col.Identity.Seed != 1 || col.Identity.Increment != 1 {
		t.Errorf("expected IDENTITY(1,1), got (%d,%d)",
			col.Identity.Seed, col.Identity.Increment)
	}
}

func TestParseIdentityColumnWithoutArgs(t *testing.T) {
	sql := `CREATE TABLE t (id INT IDENTITY, name VARCHAR(50))`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	col := ct.Columns[0]
	if col.Identity == nil {
		t.Fatal("expected IDENTITY, got nil")
	}
	if col.Identity.Seed != 1 || col.Identity.Increment != 1 {
		t.Errorf("expected default IDENTITY(1,1), got (%d,%d)",
			col.Identity.Seed, col.Identity.Increment)
	}
}

func TestParseDefaultValue(t *testing.T) {
	sql := `CREATE TABLE t (id INT, status INT DEFAULT 0, name VARCHAR(50) DEFAULT 'unknown')`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}

	// status INT DEFAULT 0
	col := ct.Columns[1]
	if col.DefaultExpr == nil {
		t.Fatal("expected DEFAULT expr, got nil")
	}
	if col.DefaultExpr.String() != "0" {
		t.Errorf("expected DEFAULT 0, got %s", col.DefaultExpr)
	}

	// name VARCHAR(50) DEFAULT 'unknown'
	col = ct.Columns[2]
	if col.DefaultExpr == nil {
		t.Fatal("expected DEFAULT expr, got nil")
	}
	if !strings.Contains(col.DefaultExpr.String(), "unknown") {
		t.Errorf("expected DEFAULT 'unknown', got %s", col.DefaultExpr)
	}
}

func TestParseDefaultGetdate(t *testing.T) {
	sql := `CREATE TABLE t (id INT, created DATETIME DEFAULT GETDATE())`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	col := ct.Columns[1]
	if col.DefaultExpr == nil {
		t.Fatal("expected DEFAULT expr, got nil")
	}
	fc, ok := col.DefaultExpr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expected FuncCallExpr, got %T", col.DefaultExpr)
	}
	if strings.ToUpper(fc.Name) != "GETDATE" {
		t.Errorf("expected GETDATE, got %s", fc.Name)
	}
}

func TestParseComputedColumn(t *testing.T) {
	sql := `CREATE TABLE t (price INT, qty INT, total AS price * qty)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}

	col := ct.Columns[2]
	if col.Name != "total" {
		t.Errorf("expected name=total, got %s", col.Name)
	}
	if col.DataType != "" {
		t.Errorf("expected empty DataType for computed column, got %s", col.DataType)
	}
	if col.ComputedExpr == nil {
		t.Fatal("expected ComputedExpr, got nil")
	}
	expr, ok := col.ComputedExpr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", col.ComputedExpr)
	}
	if expr.Op != "*" {
		t.Errorf("expected *, got %s", expr.Op)
	}
}

func TestParseIdentityWithNotNull(t *testing.T) {
	sql := `CREATE TABLE t (id INT IDENTITY(1,1) NOT NULL, name VARCHAR(50))`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	col := ct.Columns[0]
	if col.Identity == nil {
		t.Fatal("expected IDENTITY")
	}
	if col.Nullable == nil || *col.Nullable != false {
		t.Error("expected NOT NULL")
	}
}

func TestParseUnsignedTypes(t *testing.T) {
	sql := `CREATE TABLE t (
		a UNSIGNED INT NOT NULL,
		b UNSIGNED BIGINT,
		c UNSIGNED SMALLINT,
		d UNSIGNED TINYINT
	)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	if len(ct.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(ct.Columns))
	}
	expected := []string{
		"UNSIGNED INT",
		"UNSIGNED BIGINT",
		"UNSIGNED SMALLINT",
		"UNSIGNED TINYINT",
	}
	for i, exp := range expected {
		if ct.Columns[i].DataType != exp {
			t.Errorf("col %d: expected type=%s, got %s", i, exp, ct.Columns[i].DataType)
		}
	}
}

func TestParseSybaseTypes(t *testing.T) {
	sql := `CREATE TABLE t (
		a UNICHAR(20),
		b UNIVARCHAR(100),
		c UNITEXT,
		d BIGDATETIME,
		e BIGTIME
	)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := batch.Stmts[0].(*CreateTableStmt)
	if len(ct.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(ct.Columns))
	}
	expected := []string{
		"UNICHAR(20)",
		"UNIVARCHAR(100)",
		"UNITEXT",
		"BIGDATETIME",
		"BIGTIME",
	}
	for i, exp := range expected {
		if ct.Columns[i].DataType != exp {
			t.Errorf("col %d: expected type=%s, got %s", i, exp, ct.Columns[i].DataType)
		}
	}
}

func TestParseComputeBy(t *testing.T) {
	sql := `SELECT region, product, amount FROM sales
		ORDER BY region
		COMPUTE SUM(amount), AVG(amount) BY region`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(batch.Stmts))
	}
	sel, ok := batch.Stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", batch.Stmts[0])
	}
	if len(sel.Compute) != 1 {
		t.Fatalf("expected 1 COMPUTE clause, got %d", len(sel.Compute))
	}
	cc := sel.Compute[0]
	if len(cc.Aggregates) != 2 {
		t.Fatalf("expected 2 aggregates, got %d", len(cc.Aggregates))
	}
	if cc.Aggregates[0].Func != "SUM" {
		t.Errorf("expected SUM, got %s", cc.Aggregates[0].Func)
	}
	if cc.Aggregates[1].Func != "AVG" {
		t.Errorf("expected AVG, got %s", cc.Aggregates[1].Func)
	}
	if len(cc.By) != 1 {
		t.Fatalf("expected 1 BY column, got %d", len(cc.By))
	}
}

func TestParseComputeWithoutBy(t *testing.T) {
	sql := `SELECT product, amount FROM sales COMPUTE SUM(amount)`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.Compute) != 1 {
		t.Fatalf("expected 1 COMPUTE clause, got %d", len(sel.Compute))
	}
	cc := sel.Compute[0]
	if len(cc.Aggregates) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(cc.Aggregates))
	}
	if cc.Aggregates[0].Func != "SUM" {
		t.Errorf("expected SUM, got %s", cc.Aggregates[0].Func)
	}
	if len(cc.By) != 0 {
		t.Errorf("expected no BY columns, got %d", len(cc.By))
	}
}

func TestParseMultipleComputeClauses(t *testing.T) {
	sql := `SELECT region, product, amount FROM sales
		ORDER BY region, product
		COMPUTE SUM(amount) BY region, product
		COMPUTE SUM(amount) BY region`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := batch.Stmts[0].(*SelectStmt)
	if len(sel.Compute) != 2 {
		t.Fatalf("expected 2 COMPUTE clauses, got %d", len(sel.Compute))
	}
	// First COMPUTE has BY region, product.
	if len(sel.Compute[0].By) != 2 {
		t.Errorf("expected 2 BY columns in first COMPUTE, got %d",
			len(sel.Compute[0].By))
	}
	// Second COMPUTE has BY region.
	if len(sel.Compute[1].By) != 1 {
		t.Errorf("expected 1 BY column in second COMPUTE, got %d",
			len(sel.Compute[1].By))
	}
}

func TestComputeByString(t *testing.T) {
	sql := `SELECT region, amount FROM sales
		ORDER BY region
		COMPUTE SUM(amount) BY region`
	batch, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	result := batch.Stmts[0].String()
	if !strings.Contains(result, "COMPUTE SUM(amount) BY region") {
		t.Errorf("String() missing COMPUTE clause: %s", result)
	}
}

func boolPtr(b bool) *bool { return &b }
