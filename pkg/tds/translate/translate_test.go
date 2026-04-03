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
			expected: "CREATE TABLE flags (active INT2 NOT NULL, " +
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
		{"BIT", "INT2"},
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

		// Sybase unsigned integer types.
		{"UNSIGNED TINYINT", "INT2"},
		{"UNSIGNED SMALLINT", "INT4"},
		{"UNSIGNED INT", "INT8"},
		{"UNSIGNED INTEGER", "INT8"},
		{"UNSIGNED BIGINT", "DECIMAL(20, 0)"},

		// Sybase Unicode character types.
		{"UNICHAR(20)", "CHAR(20)"},
		{"UNICHAR", "CHAR"},
		{"UNIVARCHAR(100)", "VARCHAR(100)"},
		{"UNIVARCHAR", "VARCHAR"},
		{"UNITEXT", "STRING"},

		// Sybase extended datetime types.
		{"BIGDATETIME", "TIMESTAMP"},
		{"BIGTIME", "TIME"},
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

func TestTranslateFunctionMappings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// String functions.
		{
			name:     "LEN",
			input:    "SELECT LEN('hello')",
			expected: "SELECT length('hello')",
		},
		{
			name:     "CHARINDEX",
			input:    "SELECT CHARINDEX('world', 'hello world')",
			expected: "SELECT strpos('hello world', 'world')",
		},
		{
			name:     "STUFF",
			input:    "SELECT STUFF('hello world', 6, 5, 'there')",
			expected: "SELECT overlay('hello world' placing 'there' from 6 for 5)",
		},
		{
			name:     "REPLICATE",
			input:    "SELECT REPLICATE('ab', 3)",
			expected: "SELECT repeat('ab', 3)",
		},
		{
			name:     "SPACE",
			input:    "SELECT SPACE(5)",
			expected: "SELECT repeat(' ', 5)",
		},

		// Date/time functions.
		{
			name:     "DATEADD",
			input:    "SELECT DATEADD(day, 1, '2026-01-01')",
			expected: "SELECT ('2026-01-01'::TIMESTAMPTZ + 1 * INTERVAL '1 day')",
		},
		{
			name:     "DATEDIFF",
			input:    "SELECT DATEDIFF(day, '2026-01-01', '2026-01-31')",
			expected: "SELECT (EXTRACT(epoch FROM '2026-01-31'::TIMESTAMPTZ - '2026-01-01'::TIMESTAMPTZ) / 86400)::INT",
		},
		{
			name:     "DATEPART",
			input:    "SELECT DATEPART(year, '2026-06-15')",
			expected: "SELECT EXTRACT(year FROM '2026-06-15'::TIMESTAMPTZ)::INT",
		},
		{
			name:     "DATENAME",
			input:    "SELECT DATENAME(month, '2026-06-15')",
			expected: "SELECT to_char('2026-06-15'::TIMESTAMPTZ, 'Month')",
		},
		{
			name:     "YEAR",
			input:    "SELECT YEAR('2026-06-15')",
			expected: "SELECT EXTRACT(year FROM '2026-06-15'::TIMESTAMPTZ)::INT",
		},
		{
			name:     "MONTH",
			input:    "SELECT MONTH('2026-06-15')",
			expected: "SELECT EXTRACT(month FROM '2026-06-15'::TIMESTAMPTZ)::INT",
		},
		{
			name:     "DAY",
			input:    "SELECT DAY('2026-06-15')",
			expected: "SELECT EXTRACT(day FROM '2026-06-15'::TIMESTAMPTZ)::INT",
		},
		{
			name:     "SYSDATETIME",
			input:    "SELECT SYSDATETIME()",
			expected: "SELECT now()",
		},
		{
			name:     "GETUTCDATE",
			input:    "SELECT GETUTCDATE()",
			expected: "SELECT (now() AT TIME ZONE 'UTC')",
		},
		{
			name:     "EOMONTH",
			input:    "SELECT EOMONTH('2026-02-15')",
			expected: "SELECT (date_trunc('month', '2026-02-15'::TIMESTAMPTZ) + INTERVAL '1 month' - INTERVAL '1 day')::DATE",
		},

		// Math functions.
		{
			name:     "LOG to ln",
			input:    "SELECT LOG(10)",
			expected: "SELECT ln(10)",
		},
		{
			name:     "LOG10 to log",
			input:    "SELECT LOG10(1000)",
			expected: "SELECT log(1000)",
		},

		// Conditional functions.
		{
			name:     "IIF",
			input:    "SELECT IIF(1 = 1, 'yes', 'no')",
			expected: "SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END",
		},
		{
			name:     "CHOOSE",
			input:    "SELECT CHOOSE(2, 'first', 'second', 'third')",
			expected: "SELECT CASE 2 WHEN 1 THEN 'first' WHEN 2 THEN 'second' WHEN 3 THEN 'third' END",
		},
		{
			name:     "TRY_CONVERT",
			input:    "SELECT TRY_CONVERT(INT, 'not_a_number')",
			expected: "SELECT try_cast('not_a_number' AS INT4)",
		},

		// System functions.
		{
			name:     "NEWID",
			input:    "SELECT NEWID()",
			expected: "SELECT gen_random_uuid()",
		},
		{
			name:     "DB_NAME",
			input:    "SELECT DB_NAME()",
			expected: "SELECT current_database()",
		},
		{
			name:     "SCHEMA_NAME",
			input:    "SELECT SCHEMA_NAME()",
			expected: "SELECT current_schema()",
		},
		{
			name:     "USER_NAME",
			input:    "SELECT USER_NAME()",
			expected: "SELECT current_user",
		},
		{
			name:     "APP_NAME",
			input:    "SELECT APP_NAME()",
			expected: "SELECT current_setting('application_name')",
		},

		// Aggregate functions.
		{
			name:     "COUNT_BIG",
			input:    "SELECT COUNT_BIG(*)",
			expected: "SELECT count(*)",
		},
		{
			name:     "STDEV",
			input:    "SELECT STDEV(col)",
			expected: "SELECT stddev(col)",
		},

		// LIST (Sybase STRING_AGG equivalent).
		{
			name:     "LIST with separator",
			input:    "SELECT LIST(name, ';')",
			expected: "SELECT string_agg(name, ';')",
		},
		{
			name:     "LIST default separator",
			input:    "SELECT LIST(name)",
			expected: "SELECT string_agg(name, ',')",
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

// Phase 2 translate tests: subqueries, UNION, CTE, window functions, OFFSET-FETCH.

func TestTranslateSubquery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "scalar subquery in SELECT",
			input:    "SELECT (SELECT MAX(id) FROM orders) AS max_id",
			expected: "SELECT (SELECT MAX(id) FROM orders) AS max_id",
		},
		{
			name:     "subquery in WHERE",
			input:    "SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)",
			expected: "SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)",
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

func TestTranslateExists(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "exists",
			input:    "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.uid = users.id)",
			expected: "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.uid = users.id)",
		},
		{
			name:     "not exists",
			input:    "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.uid = users.id)",
			expected: "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.uid = users.id)",
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

func TestTranslateINSubquery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "in subquery",
			input:    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			expected: "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
		},
		{
			name:     "not in subquery",
			input:    "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned)",
			expected: "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned)",
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

func TestTranslateDerivedTable(t *testing.T) {
	batch, err := parser.Parse("SELECT sub.name FROM (SELECT name FROM users WHERE age > 21) sub")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT sub.name FROM (SELECT name FROM users WHERE age > 21) sub", results[0])
}

func TestTranslateAnyAll(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "any",
			input:    "SELECT * FROM t WHERE x > ANY (SELECT y FROM t2)",
			expected: "SELECT * FROM t WHERE x > ANY (SELECT y FROM t2)",
		},
		{
			name:     "all",
			input:    "SELECT * FROM t WHERE x = ALL (SELECT y FROM t2)",
			expected: "SELECT * FROM t WHERE x = ALL (SELECT y FROM t2)",
		},
		{
			name:     "some translated to any",
			input:    "SELECT * FROM t WHERE x < SOME (SELECT y FROM t2)",
			expected: "SELECT * FROM t WHERE x < ANY (SELECT y FROM t2)",
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

func TestTranslateUnion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "union",
			input:    "SELECT name FROM users UNION SELECT name FROM admins",
			expected: "SELECT name FROM users UNION SELECT name FROM admins",
		},
		{
			name:     "union all",
			input:    "SELECT name FROM users UNION ALL SELECT name FROM admins",
			expected: "SELECT name FROM users UNION ALL SELECT name FROM admins",
		},
		{
			name:     "intersect",
			input:    "SELECT id FROM users INTERSECT SELECT id FROM admins",
			expected: "SELECT id FROM users INTERSECT SELECT id FROM admins",
		},
		{
			name:     "except",
			input:    "SELECT id FROM users EXCEPT SELECT id FROM banned",
			expected: "SELECT id FROM users EXCEPT SELECT id FROM banned",
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

func TestTranslateCompoundWithOrderBy(t *testing.T) {
	batch, err := parser.Parse("SELECT a FROM t1 UNION ALL SELECT b FROM t2 ORDER BY 1 ASC")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "SELECT a FROM t1 UNION ALL SELECT b FROM t2 ORDER BY 1 ASC", results[0])
}

func TestTranslateCTE(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple cte",
			input:    "WITH active AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active",
			expected: "WITH active AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active",
		},
		{
			name: "multiple ctes",
			input: `WITH
				a AS (SELECT 1 AS x),
				b AS (SELECT 2 AS y)
				SELECT * FROM a, b`,
			expected: "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b",
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

func TestTranslateWindowFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "row_number",
			input:    "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users",
			expected: "SELECT ROW_NUMBER() OVER (ORDER BY id ASC) AS rn FROM users",
		},
		{
			name:     "rank with partition",
			input:    "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp",
			expected: "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp",
		},
		{
			name:     "dense_rank",
			input:    "SELECT DENSE_RANK() OVER (ORDER BY score DESC) FROM students",
			expected: "SELECT DENSE_RANK() OVER (ORDER BY score DESC) FROM students",
		},
		{
			name:     "ntile",
			input:    "SELECT NTILE(4) OVER (ORDER BY revenue) FROM sales",
			expected: "SELECT NTILE(4) OVER (ORDER BY revenue ASC) FROM sales",
		},
		{
			name:     "count window",
			input:    "SELECT COUNT(*) OVER (PARTITION BY dept) FROM emp",
			expected: "SELECT COUNT(*) OVER (PARTITION BY dept) FROM emp",
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

func TestTranslateOffsetFetch(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "offset fetch",
			input:    "SELECT * FROM users ORDER BY id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
			expected: "SELECT * FROM users ORDER BY id ASC LIMIT 5 OFFSET 10",
		},
		{
			name:     "offset only",
			input:    "SELECT * FROM users ORDER BY id OFFSET 20 ROWS",
			expected: "SELECT * FROM users ORDER BY id ASC OFFSET 20",
		},
		{
			name:     "offset fetch first",
			input:    "SELECT * FROM users ORDER BY id OFFSET 0 ROW FETCH FIRST 10 ROW ONLY",
			expected: "SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 0",
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

func TestTranslateTopVsFetch(t *testing.T) {
	// When FETCH is present, it takes precedence over TOP for LIMIT.
	batch, err := parser.Parse("SELECT TOP 100 * FROM users ORDER BY id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	// FETCH → LIMIT 10, TOP is ignored.
	require.Contains(t, results[0], "LIMIT 10")
	require.NotContains(t, results[0], "LIMIT 100")
}

func TestTranslateRowsLimit(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "rows limit with offset",
			input:    "SELECT * FROM users ORDER BY id ROWS LIMIT 10 OFFSET 5",
			expected: "SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 5",
		},
		{
			name:     "rows limit without offset",
			input:    "SELECT * FROM users ORDER BY id ROWS LIMIT 20",
			expected: "SELECT * FROM users ORDER BY id ASC LIMIT 20",
		},
		{
			name:     "rows limit offset zero",
			input:    "SELECT * FROM users ORDER BY id ROWS LIMIT 50 OFFSET 0",
			expected: "SELECT * FROM users ORDER BY id ASC LIMIT 50 OFFSET 0",
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

func TestTranslateISNULLInSubquery(t *testing.T) {
	// Verify that ISNULL inside a subquery is still translated to COALESCE.
	batch, err := parser.Parse("SELECT * FROM users WHERE id IN (SELECT ISNULL(uid, 0) FROM orders)")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "COALESCE(uid, 0)")
	require.NotContains(t, results[0], "ISNULL")
}

func TestTranslateInsertSelect(t *testing.T) {
	batch, err := parser.Parse(
		"INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = 0")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "INSERT INTO archive")
	require.Contains(t, results[0], "SELECT")
	require.NotContains(t, results[0], "VALUES")
}

func TestTranslateInsertOutput(t *testing.T) {
	batch, err := parser.Parse(
		"INSERT INTO users (name) OUTPUT inserted.id VALUES ('alice')")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "RETURNING id")
	require.NotContains(t, results[0], "OUTPUT")
	require.NotContains(t, results[0], "inserted")
}

func TestTranslateDeleteOutput(t *testing.T) {
	batch, err := parser.Parse(
		"DELETE FROM users OUTPUT deleted.id, deleted.name WHERE active = 0")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "RETURNING id, name")
	require.NotContains(t, results[0], "OUTPUT")
	require.NotContains(t, results[0], "deleted")
}

func TestTranslateUpdateOutput(t *testing.T) {
	batch, err := parser.Parse(
		"UPDATE users SET name = 'bob' OUTPUT inserted.id, inserted.name WHERE id = 1")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "RETURNING id, name")
	require.NotContains(t, results[0], "OUTPUT")
}

func TestTranslateUpdateFrom(t *testing.T) {
	batch, err := parser.Parse(
		"UPDATE t SET t.name = s.name FROM target t JOIN source s ON t.id = s.id WHERE s.active = 1")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "UPDATE t SET")
	require.Contains(t, results[0], "FROM target t")
	require.Contains(t, results[0], "INNER JOIN source s ON")
}

func TestTranslateDeleteJoin(t *testing.T) {
	batch, err := parser.Parse(
		"DELETE t FROM orders t JOIN cancelled c ON t.id = c.order_id")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	// T-SQL multi-table DELETE → CockroachDB DELETE FROM ... USING ...
	require.Contains(t, results[0], "DELETE FROM")
	require.Contains(t, results[0], "USING")
}

func TestTranslateMerge(t *testing.T) {
	sql := `MERGE INTO target t
		USING source s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET t.name = s.name
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)`
	batch, err := parser.Parse(sql)
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	// MERGE → INSERT ... ON CONFLICT ... DO UPDATE SET ...
	require.Contains(t, results[0], "INSERT INTO target")
	require.Contains(t, results[0], "ON CONFLICT")
	require.Contains(t, results[0], "DO UPDATE SET")
}

func TestTranslateMergeInsertOnly(t *testing.T) {
	sql := `MERGE INTO target t
		USING source s ON t.id = s.id
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)`
	batch, err := parser.Parse(sql)
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "INSERT INTO target")
	require.Contains(t, results[0], "ON CONFLICT")
	require.Contains(t, results[0], "DO NOTHING")
}

func TestTranslateIdentityColumn(t *testing.T) {
	batch, err := parser.Parse(
		"CREATE TABLE t (id INT IDENTITY(1,1), name VARCHAR(50))")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "GENERATED BY DEFAULT AS IDENTITY")
	require.Contains(t, results[0], "INT4 GENERATED BY DEFAULT AS IDENTITY NULL")
}

func TestTranslateDefaultValue(t *testing.T) {
	batch, err := parser.Parse(
		"CREATE TABLE t (id INT, status INT DEFAULT 0)")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "DEFAULT 0")
}

func TestTranslateDefaultGetdate(t *testing.T) {
	batch, err := parser.Parse(
		"CREATE TABLE t (id INT, created DATETIME DEFAULT GETDATE())")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "DEFAULT now()")
}

func TestTranslateComputedColumn(t *testing.T) {
	batch, err := parser.Parse(
		"CREATE TABLE t (price INT, qty INT, total AS price * qty)")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "total AS (price * qty) STORED")
}

func TestTranslateBitToInt2(t *testing.T) {
	batch, err := parser.Parse(
		"CREATE TABLE t (id INT, flag BIT)")
	require.NoError(t, err)
	results, err := Batch(batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Contains(t, results[0], "flag INT2 NULL")
}
