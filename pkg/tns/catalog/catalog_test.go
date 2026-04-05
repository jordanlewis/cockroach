// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"strings"
	"testing"
)

func newTestCatalog() *Catalog {
	return New("v24.1.0", "testuser")
}

func TestHandleDual(t *testing.T) {
	c := newTestCatalog()

	// General DUAL queries (no SYS_CONTEXT or USER keyword) should NOT be
	// handled by the catalog. They fall through to the Oracle translator,
	// which handles both FROM DUAL stripping and Oracle function mapping
	// (LENGTHB, CEIL, etc.).
	tests := []string{
		"SELECT 1 FROM DUAL",
		"SELECT 1+1 FROM DUAL",
		"SELECT 'hello' FROM DUAL",
		"select 42 from dual",
		"SELECT 1 FROM DUAL;",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			resp := c.Handle(sql)
			if resp.Handled {
				t.Fatalf("expected Handled=false for general DUAL query")
			}
		})
	}
}

func TestHandleSelectUserFromDual(t *testing.T) {
	c := newTestCatalog()

	tests := []struct {
		name string
		sql  string
	}{
		{"uppercase", "SELECT USER FROM DUAL"},
		{"lowercase", "select user from dual"},
		{"mixed case", "Select User From Dual"},
		{"with semicolon", "SELECT USER FROM DUAL;"},
		{"with whitespace", "  SELECT USER FROM DUAL  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := c.Handle(tt.sql)
			if !resp.Handled {
				t.Fatal("expected Handled=true")
			}
			if resp.Result == nil {
				t.Fatal("expected static Result")
			}
			if len(resp.Result.Columns) != 1 ||
				resp.Result.Columns[0].Name != "USER" {
				t.Errorf("got columns %v, want [{USER}]",
					resp.Result.Columns)
			}
			if resp.Result.Rows[0][0] != "testuser" {
				t.Errorf("got %q, want %q",
					resp.Result.Rows[0][0], "testuser")
			}
		})
	}
}

func TestHandleVVersion(t *testing.T) {
	c := newTestCatalog()

	tests := []string{
		"SELECT * FROM V$VERSION",
		"select banner from v$version",
		"SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1",
		"SELECT * FROM PRODUCT_COMPONENT_VERSION",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			resp := c.Handle(sql)
			if !resp.Handled {
				t.Fatal("expected Handled=true")
			}
			if resp.Result == nil {
				t.Fatal("expected static Result")
			}
			if len(resp.Result.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(resp.Result.Rows))
			}
			banner := resp.Result.Rows[0][0]
			if !strings.Contains(banner, "v24.1.0") {
				t.Errorf("banner %q missing version", banner)
			}
			if !strings.Contains(banner, "Oracle Compatible") {
				t.Errorf("banner %q missing Oracle Compatible",
					banner)
			}
		})
	}
}

func TestHandleVSession(t *testing.T) {
	c := newTestCatalog()

	resp := c.Handle("SELECT * FROM V$SESSION")
	if !resp.Handled {
		t.Fatal("expected Handled=true")
	}
	if resp.Result == nil {
		t.Fatal("expected static Result")
	}
	if len(resp.Result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(resp.Result.Rows))
	}

	// Verify USERNAME column has the right value.
	usernameIdx := -1
	for i, col := range resp.Result.Columns {
		if col.Name == "USERNAME" {
			usernameIdx = i
			break
		}
	}
	if usernameIdx < 0 {
		t.Fatal("USERNAME column not found")
	}
	if resp.Result.Rows[0][usernameIdx] != "testuser" {
		t.Errorf("got username %q, want %q",
			resp.Result.Rows[0][usernameIdx], "testuser")
	}
}

func TestHandleNLSParams(t *testing.T) {
	c := newTestCatalog()

	tests := []string{
		"SELECT * FROM NLS_SESSION_PARAMETERS",
		"SELECT * FROM V$NLS_PARAMETERS",
		"SELECT * FROM NLS_DATABASE_PARAMETERS",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			resp := c.Handle(sql)
			if !resp.Handled {
				t.Fatal("expected Handled=true")
			}
			if resp.Result == nil {
				t.Fatal("expected static Result")
			}
			if len(resp.Result.Columns) != 2 {
				t.Fatalf("got %d columns, want 2",
					len(resp.Result.Columns))
			}

			// Check that NLS_DATE_FORMAT is present.
			found := false
			for _, row := range resp.Result.Rows {
				if row[0] == "NLS_DATE_FORMAT" {
					found = true
					if row[1] != "DD-MON-RR" {
						t.Errorf("NLS_DATE_FORMAT = %q, want DD-MON-RR",
							row[1])
					}
				}
			}
			if !found {
				t.Error("NLS_DATE_FORMAT not found in results")
			}
		})
	}
}

func TestHandleAlterSession(t *testing.T) {
	c := newTestCatalog()

	tests := []struct {
		name  string
		sql   string
		param string
		value string
	}{
		{
			name:  "date format quoted",
			sql:   "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
			param: "NLS_DATE_FORMAT",
			value: "YYYY-MM-DD",
		},
		{
			name:  "date format unquoted",
			sql:   "ALTER SESSION SET NLS_DATE_FORMAT = YYYY-MM-DD",
			param: "NLS_DATE_FORMAT",
			value: "YYYY-MM-DD",
		},
		{
			name:  "language",
			sql:   "ALTER SESSION SET NLS_LANGUAGE = 'FRENCH'",
			param: "NLS_LANGUAGE",
			value: "FRENCH",
		},
		{
			name:  "numeric characters",
			sql:   "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
			param: "NLS_NUMERIC_CHARACTERS",
			value: ".,",
		},
		{
			name:  "case insensitive",
			sql:   "alter session set nls_sort = 'GENERIC_M'",
			param: "NLS_SORT",
			value: "GENERIC_M",
		},
		{
			name:  "non-NLS param",
			sql:   "ALTER SESSION SET TIME_ZONE = 'UTC'",
			param: "TIME_ZONE",
			value: "UTC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := c.Handle(tt.sql)
			if !resp.Handled {
				t.Fatal("expected Handled=true")
			}
			if !resp.OK {
				t.Fatal("expected OK=true")
			}
			if resp.Result != nil {
				t.Fatal("expected nil Result for ALTER SESSION")
			}

			got := c.NLSParam(tt.param)
			if got != tt.value {
				t.Errorf("NLSParam(%q) = %q, want %q",
					tt.param, got, tt.value)
			}
		})
	}
}

func TestHandleAlterSessionPersistence(t *testing.T) {
	c := newTestCatalog()

	// Set a custom date format.
	c.Handle("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")

	// Verify it's reflected in NLS parameter queries.
	resp := c.Handle("SELECT * FROM NLS_SESSION_PARAMETERS")
	if !resp.Handled || resp.Result == nil {
		t.Fatal("NLS_SESSION_PARAMETERS query failed")
	}

	found := false
	for _, row := range resp.Result.Rows {
		if row[0] == "NLS_DATE_FORMAT" {
			found = true
			if row[1] != "YYYY-MM-DD HH24:MI:SS" {
				t.Errorf("NLS_DATE_FORMAT = %q, want YYYY-MM-DD HH24:MI:SS",
					row[1])
			}
		}
	}
	if !found {
		t.Error("NLS_DATE_FORMAT not found after ALTER SESSION SET")
	}

	// Verify SYS_CONTEXT also reflects the change.
	resp = c.Handle(
		"SELECT SYS_CONTEXT('USERENV', 'NLS_DATE_FORMAT') FROM DUAL",
	)
	if !resp.Handled || resp.Result == nil {
		t.Fatal("SYS_CONTEXT NLS_DATE_FORMAT query failed")
	}
	if resp.Result.Rows[0][0] != "YYYY-MM-DD HH24:MI:SS" {
		t.Errorf("SYS_CONTEXT NLS_DATE_FORMAT = %q, want YYYY-MM-DD HH24:MI:SS",
			resp.Result.Rows[0][0])
	}
}

func TestHandleAllTables(t *testing.T) {
	c := newTestCatalog()

	resp := c.Handle("SELECT * FROM ALL_TABLES")
	if !resp.Handled {
		t.Fatal("expected Handled=true")
	}
	if resp.RewriteSQL == "" {
		t.Fatal("expected non-empty RewriteSQL")
	}
	if !strings.Contains(resp.RewriteSQL, "information_schema.tables") {
		t.Error("rewrite should reference information_schema.tables")
	}
	if !strings.Contains(resp.RewriteSQL, `"OWNER"`) {
		t.Error("rewrite should alias to OWNER")
	}
	if !strings.Contains(resp.RewriteSQL, `"TABLE_NAME"`) {
		t.Error("rewrite should alias to TABLE_NAME")
	}
}

func TestHandleAllTabColumns(t *testing.T) {
	c := newTestCatalog()

	resp := c.Handle("SELECT * FROM ALL_TAB_COLUMNS")
	if !resp.Handled {
		t.Fatal("expected Handled=true")
	}
	if resp.RewriteSQL == "" {
		t.Fatal("expected non-empty RewriteSQL")
	}
	if !strings.Contains(resp.RewriteSQL, "information_schema.columns") {
		t.Error("rewrite should reference information_schema.columns")
	}
}

func TestHandleSysContext(t *testing.T) {
	c := newTestCatalog()

	tests := []struct {
		name    string
		sql     string
		wantCol string
		wantVal string
	}{
		{
			name:    "session id",
			sql:     "SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL",
			wantCol: "SYS_CONTEXT('USERENV','SID')",
			wantVal: "1",
		},
		{
			name:    "current user",
			sql:     "SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER') FROM DUAL",
			wantCol: "SYS_CONTEXT('USERENV','CURRENT_USER')",
			wantVal: "testuser",
		},
		{
			name:    "current schema",
			sql:     "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL",
			wantCol: "SYS_CONTEXT('USERENV','CURRENT_SCHEMA')",
			wantVal: "testuser",
		},
		{
			name:    "db name",
			sql:     "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL",
			wantCol: "SYS_CONTEXT('USERENV','DB_NAME')",
			wantVal: "defaultdb",
		},
		{
			name:    "language",
			sql:     "SELECT SYS_CONTEXT('USERENV', 'LANGUAGE') FROM DUAL",
			wantCol: "SYS_CONTEXT('USERENV','LANGUAGE')",
			wantVal: "AMERICAN_AMERICA.AL32UTF8",
		},
		{
			name:    "unknown param",
			sql:     "SELECT SYS_CONTEXT('USERENV', 'UNKNOWN_PARAM') FROM DUAL",
			wantCol: "SYS_CONTEXT('USERENV','UNKNOWN_PARAM')",
			wantVal: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := c.Handle(tt.sql)
			if !resp.Handled {
				t.Fatal("expected Handled=true")
			}
			if resp.Result == nil {
				t.Fatal("expected static Result")
			}
			if resp.Result.Columns[0].Name != tt.wantCol {
				t.Errorf("column name = %q, want %q",
					resp.Result.Columns[0].Name, tt.wantCol)
			}
			if resp.Result.Rows[0][0] != tt.wantVal {
				t.Errorf("value = %q, want %q",
					resp.Result.Rows[0][0], tt.wantVal)
			}
		})
	}
}

func TestHandleUnrecognizedQuery(t *testing.T) {
	c := newTestCatalog()

	tests := []string{
		"SELECT * FROM employees",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"CREATE TABLE t (id INT)",
		"SELECT 1",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			resp := c.Handle(sql)
			if resp.Handled {
				t.Errorf("expected Handled=false for %q", sql)
			}
		})
	}
}

func TestHandleUserKeywordInDual(t *testing.T) {
	c := newTestCatalog()

	// USER as part of an expression with other columns.
	resp := c.Handle("SELECT USER, 1 FROM DUAL")
	if !resp.Handled {
		t.Fatal("expected Handled=true")
	}
	if resp.RewriteSQL == "" {
		t.Fatal("expected RewriteSQL")
	}
	if !strings.Contains(resp.RewriteSQL, "current_user") {
		t.Error("USER should be replaced with current_user")
	}
	if strings.Contains(resp.RewriteSQL, "DUAL") {
		t.Error("FROM DUAL should be stripped")
	}
}

func TestNLSParamAccessors(t *testing.T) {
	c := newTestCatalog()

	// Default value.
	if got := c.NLSParam("NLS_DATE_FORMAT"); got != "DD-MON-RR" {
		t.Errorf("default NLS_DATE_FORMAT = %q, want DD-MON-RR", got)
	}

	// Set and get.
	c.SetNLSParam("NLS_DATE_FORMAT", "YYYY-MM-DD")
	if got := c.NLSParam("NLS_DATE_FORMAT"); got != "YYYY-MM-DD" {
		t.Errorf("NLS_DATE_FORMAT = %q, want YYYY-MM-DD", got)
	}

	// Case insensitive access.
	if got := c.NLSParam("nls_date_format"); got != "YYYY-MM-DD" {
		t.Errorf("nls_date_format = %q, want YYYY-MM-DD", got)
	}
}

func TestUsername(t *testing.T) {
	c := New("v24.1.0", "admin")
	if c.Username() != "admin" {
		t.Errorf("Username() = %q, want %q", c.Username(), "admin")
	}
}

func TestNLSParamsDeterministicOrder(t *testing.T) {
	c := newTestCatalog()

	// Query NLS params twice and verify order is the same.
	resp1 := c.Handle("SELECT * FROM NLS_SESSION_PARAMETERS")
	resp2 := c.Handle("SELECT * FROM NLS_SESSION_PARAMETERS")

	if len(resp1.Result.Rows) != len(resp2.Result.Rows) {
		t.Fatal("row count mismatch between calls")
	}

	for i, row := range resp1.Result.Rows {
		if row[0] != resp2.Result.Rows[i][0] ||
			row[1] != resp2.Result.Rows[i][1] {
			t.Errorf("row %d differs between calls: %v vs %v",
				i, row, resp2.Result.Rows[i])
		}
	}

	// Verify alphabetical ordering.
	for i := 1; i < len(resp1.Result.Rows); i++ {
		if resp1.Result.Rows[i][0] < resp1.Result.Rows[i-1][0] {
			t.Errorf("rows not sorted: %q before %q",
				resp1.Result.Rows[i-1][0],
				resp1.Result.Rows[i][0])
		}
	}
}
