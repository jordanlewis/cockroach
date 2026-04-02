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

func boolPtr(b bool) *bool { return &b }
