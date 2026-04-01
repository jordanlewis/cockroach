// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package translate

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/tds/parser"
	"github.com/stretchr/testify/require"
)

func TestTranslateUse(t *testing.T) {
	batch, err := parser.Parse("USE mydb")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SET database = 'mydb'", results[0])
}

func TestTranslateCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "basic types",
			input: "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), email NVARCHAR(255))",
			expected: "CREATE TABLE users (id INT4 NOT NULL, " +
				"name VARCHAR(100) NULL, " +
				"email VARCHAR(255) NULL)",
		},
		{
			name:  "money and datetime",
			input: "CREATE TABLE orders (id INT, amount MONEY NOT NULL, created DATETIME)",
			expected: "CREATE TABLE orders (id INT4 NULL, " +
				"amount DECIMAL(19, 4) NOT NULL, " +
				"created TIMESTAMP NULL)",
		},
		{
			name:  "various integer types",
			input: "CREATE TABLE nums (a TINYINT, b SMALLINT, c INT, d BIGINT)",
			expected: "CREATE TABLE nums (a INT2 NULL, " +
				"b INT2 NULL, " +
				"c INT4 NULL, " +
				"d INT8 NULL)",
		},
		{
			name:  "bit and decimal",
			input: "CREATE TABLE flags (active BIT NOT NULL, ratio DECIMAL(10, 2))",
			expected: "CREATE TABLE flags (active BOOL NOT NULL, " +
				"ratio DECIMAL(10, 2) NULL)",
		},
		{
			name:  "text and image",
			input: "CREATE TABLE docs (body TEXT, pic IMAGE)",
			expected: "CREATE TABLE docs (body STRING NULL, " +
				"pic BYTES NULL)",
		},
		{
			name:     "smallmoney and smalldatetime",
			input:    "CREATE TABLE small (amt SMALLMONEY, dt SMALLDATETIME)",
			expected: "CREATE TABLE small (amt DECIMAL(10, 4) NULL, dt TIMESTAMP NULL)",
		},
		{
			name:     "uniqueidentifier",
			input:    "CREATE TABLE guids (id UNIQUEIDENTIFIER NOT NULL)",
			expected: "CREATE TABLE guids (id UUID NOT NULL)",
		},
		{
			name:     "numeric type",
			input:    "CREATE TABLE nums (val NUMERIC(18, 4))",
			expected: "CREATE TABLE nums (val DECIMAL(18, 4) NULL)",
		},
		{
			name:     "float and real",
			input:    "CREATE TABLE floats (a REAL, b FLOAT)",
			expected: "CREATE TABLE floats (a FLOAT4 NULL, b FLOAT8 NULL)",
		},
		{
			name:     "ntext and nchar",
			input:    "CREATE TABLE nstrs (a NTEXT, b NCHAR(10))",
			expected: "CREATE TABLE nstrs (a STRING NULL, b CHAR(10) NULL)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateInsert(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple insert",
			input:    "INSERT INTO users (name, age) VALUES ('Alice', 30)",
			expected: "INSERT INTO users (name, age) VALUES ('Alice', 30)",
		},
		{
			name:     "insert without column list",
			input:    "INSERT INTO users VALUES ('Bob', 25)",
			expected: "INSERT INTO users VALUES ('Bob', 25)",
		},
		{
			name:     "insert with null",
			input:    "INSERT INTO users (name, email) VALUES ('Charlie', NULL)",
			expected: "INSERT INTO users (name, email) VALUES ('Charlie', NULL)",
		},
		{
			name:  "multi-row insert",
			input: "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')",
			expected: "INSERT INTO users (name) VALUES ('Alice'), " +
				"('Bob'), ('Charlie')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateSelectTopToLimit(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple top",
			input:    "SELECT TOP 10 * FROM users",
			expected: "SELECT * FROM users LIMIT 10",
		},
		{
			name:     "top with columns",
			input:    "SELECT TOP 5 name, age FROM users",
			expected: "SELECT name, age FROM users LIMIT 5",
		},
		{
			name:     "top with order by",
			input:    "SELECT TOP 3 name FROM users ORDER BY name ASC",
			expected: "SELECT name FROM users ORDER BY name ASC LIMIT 3",
		},
		{
			name:     "no top",
			input:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateBracketIdentifiers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "bracketed table name",
			input:    "SELECT * FROM [my table]",
			expected: `SELECT * FROM "my table"`,
		},
		{
			name:     "bracketed column name",
			input:    "SELECT [first name] FROM users",
			expected: `SELECT "first name" FROM users`,
		},
		{
			name:     "multiple bracketed identifiers",
			input:    "SELECT [col 1], [col 2] FROM [my table]",
			expected: `SELECT "col 1", "col 2" FROM "my table"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateISNULL(t *testing.T) {
	batch, err := parser.Parse("SELECT ISNULL(name, 'unknown') FROM users")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT COALESCE(name, 'unknown') FROM users", results[0])
}

func TestTranslateCONVERT(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "convert varchar",
			input:    "SELECT CONVERT(VARCHAR(10), age) FROM users",
			expected: "SELECT CAST(age AS VARCHAR(10)) FROM users",
		},
		{
			name:     "convert int",
			input:    "SELECT CONVERT(INT, price) FROM products",
			expected: "SELECT CAST(price AS INT4) FROM products",
		},
		{
			name:     "convert datetime",
			input:    "SELECT CONVERT(DATETIME, created) FROM orders",
			expected: "SELECT CAST(created AS TIMESTAMP) FROM orders",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateGETDATE(t *testing.T) {
	batch, err := parser.Parse("SELECT GETDATE()")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT now()", results[0])
}

func TestTranslateStringConcat(t *testing.T) {
	batch, err := parser.Parse("SELECT 'hello' + ' ' + 'world'")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT 'hello' || ' ' || 'world'", results[0])
}

func TestTranslateMultiStatementBatch(t *testing.T) {
	input := `
		USE mydb
		GO
		CREATE TABLE t (id INT NOT NULL)
		GO
		INSERT INTO t (id) VALUES (1)
		GO
		SELECT * FROM t
	`
	batch, err := parser.Parse(input)
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 4)
	require.Equal(t, "SET database = 'mydb'", results[0])
	require.Equal(t, "CREATE TABLE t (id INT4 NOT NULL)", results[1])
	require.Equal(t, "INSERT INTO t (id) VALUES (1)", results[2])
	require.Equal(t, "SELECT * FROM t", results[3])
}

func TestTranslateSystemVariables(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "@@ROWCOUNT",
			input:    "SELECT @@ROWCOUNT",
			expected: "SELECT current_setting('crdb_internal.num_rows_affected')",
		},
		{
			name:     "@@IDENTITY",
			input:    "SELECT @@IDENTITY",
			expected: "SELECT lastval()",
		},
		{
			name:     "@@VERSION",
			input:    "SELECT @@VERSION",
			expected: "SELECT version()",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateWhereClause(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple where",
			input:    "SELECT * FROM users WHERE age > 18",
			expected: "SELECT * FROM users WHERE age > 18",
		},
		{
			name:     "where with ISNULL",
			input:    "SELECT * FROM users WHERE ISNULL(email, '') <> ''",
			expected: "SELECT * FROM users WHERE COALESCE(email, '') <> ''",
		},
		{
			name:     "where with AND/OR",
			input:    "SELECT * FROM users WHERE age > 18 AND name = 'Alice'",
			expected: "SELECT * FROM users WHERE age > 18 AND name = 'Alice'",
		},
		{
			name:     "where IS NULL",
			input:    "SELECT * FROM users WHERE email IS NULL",
			expected: "SELECT * FROM users WHERE email IS NULL",
		},
		{
			name:     "where IS NOT NULL",
			input:    "SELECT * FROM users WHERE email IS NOT NULL",
			expected: "SELECT * FROM users WHERE email IS NOT NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateOrderBy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "order by asc",
			input:    "SELECT * FROM users ORDER BY name ASC",
			expected: "SELECT * FROM users ORDER BY name ASC",
		},
		{
			name:     "order by desc",
			input:    "SELECT * FROM users ORDER BY name DESC",
			expected: "SELECT * FROM users ORDER BY name DESC",
		},
		{
			name:     "order by multiple",
			input:    "SELECT * FROM users ORDER BY age DESC, name ASC",
			expected: "SELECT * FROM users ORDER BY age DESC, name ASC",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := parser.Parse(tt.input)
			require.NoError(t, err)
			results, err := Batch(batch)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tt.expected, results[0])
		})
	}
}

func TestTranslateSelectAlias(t *testing.T) {
	batch, err := parser.Parse("SELECT name AS user_name FROM users")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT name AS user_name FROM users", results[0])
}

func TestMapDataType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"TINYINT", "INT2"},
		{"SMALLINT", "INT2"},
		{"INT", "INT4"},
		{"BIGINT", "INT8"},
		{"REAL", "FLOAT4"},
		{"FLOAT", "FLOAT8"},
		{"BIT", "BOOL"},
		{"VARCHAR(100)", "VARCHAR(100)"},
		{"NVARCHAR(255)", "VARCHAR(255)"},
		{"NVARCHAR(MAX)", "STRING"},
		{"VARCHAR(MAX)", "STRING"},
		{"TEXT", "STRING"},
		{"NTEXT", "STRING"},
		{"CHAR(10)", "CHAR(10)"},
		{"NCHAR(10)", "CHAR(10)"},
		{"MONEY", "DECIMAL(19, 4)"},
		{"SMALLMONEY", "DECIMAL(10, 4)"},
		{"DATETIME", "TIMESTAMP"},
		{"DATETIME2", "TIMESTAMP"},
		{"SMALLDATETIME", "TIMESTAMP"},
		{"DATE", "DATE"},
		{"TIME", "TIME"},
		{"DATETIMEOFFSET", "TIMESTAMPTZ"},
		{"UNIQUEIDENTIFIER", "UUID"},
		{"DECIMAL(10, 2)", "DECIMAL(10, 2)"},
		{"NUMERIC(18, 4)", "DECIMAL(18, 4)"},
		{"BINARY(16)", "BYTES"},
		{"VARBINARY(100)", "BYTES"},
		{"IMAGE", "BYTES"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := mapDataType(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"users", "users"},
		{"my table", `"my table"`},
		{"column-name", `"column-name"`},
		{"simple_name", "simple_name"},
		{"dbo.users", "dbo.users"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := quoteIdent(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTranslateComplexQuery(t *testing.T) {
	input := `SELECT TOP 10 u.name, ISNULL(u.email, 'none') AS email,
		CONVERT(VARCHAR(20), u.created) AS created_str
		FROM users u
		WHERE u.age > 18 AND u.name LIKE 'A%'
		ORDER BY u.name ASC`
	batch, err := parser.Parse(input)
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	sql := results[0]

	// Verify key translations.
	require.Contains(t, sql, "LIMIT 10")
	require.NotContains(t, sql, "TOP")
	require.Contains(t, sql, "COALESCE(")
	require.NotContains(t, sql, "ISNULL(")
	require.Contains(t, sql, "CAST(")
	require.NotContains(t, sql, "CONVERT(")
	require.Contains(t, sql, "ORDER BY")
	require.Contains(t, sql, "ASC")
}

func TestTranslateEmptyBatch(t *testing.T) {
	batch, err := parser.Parse("")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestTranslateConvertWithStyleDropped(t *testing.T) {
	// CONVERT with a style argument (3rd param) - the style is dropped in CRDB
	// translation since CAST doesn't support styles.
	batch, err := parser.Parse("SELECT CONVERT(VARCHAR(10), GETDATE(), 101)")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT CAST(now() AS VARCHAR(10))", results[0])
}

func TestTranslateSelectWithTableAlias(t *testing.T) {
	batch, err := parser.Parse("SELECT u.name FROM users u WHERE u.id = 1")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT u.name FROM users u WHERE u.id = 1", results[0])
}

func TestSplitTypeArgs(t *testing.T) {
	tests := []struct {
		input        string
		expectedName string
		expectedArgs string
	}{
		{"VARCHAR(100)", "VARCHAR", "100"},
		{"DECIMAL(10, 2)", "DECIMAL", "10, 2"},
		{"INT", "INT", ""},
		{"NVARCHAR(MAX)", "NVARCHAR", "MAX"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			name, args := splitTypeArgs(tt.input)
			require.Equal(t, tt.expectedName, name)
			require.Equal(t, tt.expectedArgs, args)
		})
	}
}
