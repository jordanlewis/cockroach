// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsCatalogQuery_Version(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"basic", "SELECT @@VERSION", true},
		{"lowercase", "select @@version", true},
		{"with semicolon", "SELECT @@VERSION;", true},
		{"with whitespace", "  SELECT  @@VERSION  ", true},
		{"not version", "SELECT @@ROWCOUNT", false},
		{"embedded in expr", "SELECT 1 + @@VERSION", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateVersion(t *testing.T) {
	result, err := TranslateCatalogQuery("SELECT @@VERSION")
	require.NoError(t, err)
	require.Contains(t, result, "Adaptive Server Enterprise/16.0")
	require.Contains(t, result, "CockroachDB")
	require.Contains(t, result, "SELECT '")
	require.Contains(t, result, "AS version")
}

func TestVersionStringFormat(t *testing.T) {
	v := versionString()
	require.True(t, strings.HasPrefix(v, "Adaptive Server Enterprise/16.0/"),
		"version should start with Adaptive Server Enterprise/16.0/, got: %s", v)
	require.Contains(t, v, "CockroachDB")
}

func TestIsCatalogQuery_SpHelpDB(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"bare", "sp_helpdb", true},
		{"with db", "sp_helpdb mydb", true},
		{"exec prefix", "EXEC sp_helpdb", true},
		{"execute prefix", "EXECUTE sp_helpdb master", true},
		{"case insensitive", "SP_HELPDB", true},
		{"with semicolon", "sp_helpdb;", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSpHelpDB(t *testing.T) {
	t.Run("no argument", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_helpdb")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.schemata")
		require.Contains(t, result, "catalog_name AS name")
		require.NotContains(t, result, "WHERE")
	})

	t.Run("with database name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_helpdb mydb")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.schemata")
		require.Contains(t, result, "WHERE catalog_name = 'mydb'")
	})

	t.Run("with EXEC prefix", func(t *testing.T) {
		result, err := TranslateCatalogQuery("EXEC sp_helpdb master")
		require.NoError(t, err)
		require.Contains(t, result, "WHERE catalog_name = 'master'")
	})
}

func TestIsCatalogQuery_SpHelp(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"bare", "sp_help", true},
		{"with table", "sp_help users", true},
		{"exec prefix", "EXEC sp_help orders", true},
		{"case insensitive", "SP_HELP", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSpHelp(t *testing.T) {
	t.Run("no argument lists tables", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_help")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.tables")
		require.Contains(t, result, "table_name AS name")
		require.NotContains(t, result, "information_schema.columns")
	})

	t.Run("with table name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_help users")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.columns")
		require.Contains(t, result, "WHERE table_name = 'users'")
		require.Contains(t, result, "column_name")
		require.Contains(t, result, "data_type")
		require.Contains(t, result, "ORDER BY ordinal_position")
	})

	t.Run("with quoted table name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_help 'my_table'")
		require.NoError(t, err)
		require.Contains(t, result, "WHERE table_name = 'my_table'")
	})
}

func TestIsCatalogQuery_Sysobjects(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"basic", "SELECT name, type FROM sysobjects WHERE type = 'U'", true},
		{"with dbo prefix", "SELECT name FROM dbo.sysobjects", true},
		{"case insensitive", "SELECT * FROM SYSOBJECTS", true},
		{"not sysobjects", "SELECT * FROM users", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSysobjects(t *testing.T) {
	t.Run("basic query", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT sysobjects.name FROM sysobjects WHERE type = 'U'")
		require.NoError(t, err)
		require.Contains(t, result, "pg_catalog.pg_class")
		require.Contains(t, result, "relname")
		require.Contains(t, result, "relkind = 'r'")
		require.NotContains(t, result, "sysobjects")
	})

	t.Run("view type filter", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT sysobjects.name FROM sysobjects WHERE type = 'V'")
		require.NoError(t, err)
		require.Contains(t, result, "relkind = 'v'")
	})

	t.Run("proc type filter", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT sysobjects.name FROM sysobjects WHERE type = 'P'")
		require.NoError(t, err)
		require.Contains(t, result, "relkind = 'p'")
	})

	t.Run("id column mapped", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT sysobjects.id, sysobjects.name FROM sysobjects")
		require.NoError(t, err)
		require.Contains(t, result, "oid")
		require.Contains(t, result, "relname")
	})

	t.Run("with dbo prefix", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT name FROM dbo.sysobjects WHERE type = 'U'")
		require.NoError(t, err)
		require.Contains(t, result, "pg_catalog.pg_class")
		require.NotContains(t, result, "dbo.")
	})

	t.Run("ORDER BY bare column", func(t *testing.T) {
		result, err := TranslateCatalogQuery(
			"SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name")
		require.NoError(t, err)
		require.Contains(t, result, "ORDER BY relname")
		require.NotContains(t, result, "ORDER BY name")
	})
}

func TestIsCatalogQuery_Syscolumns(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"basic", "SELECT name FROM syscolumns WHERE id = 123", true},
		{"with dbo prefix", "SELECT * FROM dbo.syscolumns", true},
		{"case insensitive", "SELECT * FROM SYSCOLUMNS", true},
		{"not syscolumns", "SELECT * FROM columns", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSyscolumns(t *testing.T) {
	t.Run("basic query", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT syscolumns.name FROM syscolumns")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.columns")
		require.Contains(t, result, "column_name")
		require.NotContains(t, result, "syscolumns")
	})

	t.Run("length column mapped", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT syscolumns.name, syscolumns.length FROM syscolumns")
		require.NoError(t, err)
		require.Contains(t, result, "column_name")
		require.Contains(t, result, "character_maximum_length")
	})

	t.Run("with dbo prefix", func(t *testing.T) {
		result, err := TranslateCatalogQuery("SELECT name FROM dbo.syscolumns")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.columns")
		require.NotContains(t, result, "dbo.")
	})
}

func TestIsCatalogQuery_Sysusers(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"basic", "SELECT * FROM sysusers WHERE uid = 1", true},
		{"with dbo prefix", "SELECT name FROM dbo.sysusers", true},
		{"case insensitive", "SELECT * FROM SYSUSERS", true},
		{"not sysusers", "SELECT * FROM users", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSysusers(t *testing.T) {
	t.Run("basic query", func(t *testing.T) {
		result, err := TranslateCatalogQuery(
			"SELECT sysusers.name FROM sysusers WHERE sysusers.uid = 1")
		require.NoError(t, err)
		require.Contains(t, result, "pg_catalog.pg_roles")
		require.Contains(t, result, "rolname")
		require.Contains(t, result, "oid = 1")
		require.NotContains(t, result, "sysusers")
	})

	t.Run("with dbo prefix", func(t *testing.T) {
		result, err := TranslateCatalogQuery(
			"SELECT name FROM dbo.sysusers WHERE uid = 1")
		require.NoError(t, err)
		require.Contains(t, result, "pg_catalog.pg_roles")
		require.NotContains(t, result, "dbo.")
	})

	t.Run("suid column mapped", func(t *testing.T) {
		result, err := TranslateCatalogQuery(
			"SELECT sysusers.suid FROM sysusers")
		require.NoError(t, err)
		require.Contains(t, result, "oid")
		require.NotContains(t, result, "suid")
	})
}

func TestIsCatalogQuery_SetCommands(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"quoted identifier on", "SET QUOTED_IDENTIFIER ON", true},
		{"quoted identifier off", "SET QUOTED_IDENTIFIER OFF", true},
		{"ansi nulls on", "SET ANSI_NULLS ON", true},
		{"ansi nulls off", "SET ANSI_NULLS OFF", true},
		{"textsize", "SET TEXTSIZE 2147483647", true},
		{"arithabort", "SET ARITHABORT ON", true},
		{"concat null", "SET CONCAT_NULL_YIELDS_NULL ON", true},
		{"case insensitive", "set quoted_identifier on", true},
		{"with semicolon", "SET ANSI_NULLS ON;", true},
		{"with whitespace", "  SET TEXTSIZE 65536  ", true},
		{"rowcount", "SET ROWCOUNT 100", true},
		{"rowcount zero", "SET ROWCOUNT 0", true},
		{"identity insert on", "SET IDENTITY_INSERT mytable ON", true},
		{"identity insert off", "SET IDENTITY_INSERT dbo.users OFF", true},
		{"chained off", "SET CHAINED OFF", true},
		{"chained on", "SET CHAINED ON", true},
		{"unknown set", "SET UNKNOWN_OPTION ON", false},
		{"not set", "SELECT 1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSetCommands(t *testing.T) {
	setCommands := []string{
		"SET QUOTED_IDENTIFIER ON",
		"SET ANSI_NULLS ON",
		"SET TEXTSIZE 2147483647",
		"SET ARITHABORT ON",
		"SET CONCAT_NULL_YIELDS_NULL ON",
		"SET ROWCOUNT 100",
		"SET IDENTITY_INSERT mytable ON",
		"SET CHAINED OFF",
	}
	for _, cmd := range setCommands {
		t.Run(cmd, func(t *testing.T) {
			result, err := TranslateCatalogQuery(cmd)
			require.NoError(t, err)
			require.Empty(t, result, "SET commands should return empty string")
		})
	}
}

func TestIsCatalogQuery_SpTables(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"bare", "sp_tables", true},
		{"exec prefix", "EXEC sp_tables", true},
		{"with table name", "sp_tables users", true},
		{"exec with table name", "EXEC sp_tables 'users'", true},
		{"case insensitive", "SP_TABLES", true},
		{"with semicolon", "sp_tables;", true},
		// Named parameters must NOT match the catalog regex — they are
		// handled by the parser/executor path.
		{"named param no spaces", "EXEC sp_tables @table_name='users'", false},
		{"named param with spaces", "EXEC sp_tables @table_name = 'users'", false},
		{"named param multiple", "EXEC sp_tables @table_name='t', @table_owner='public'", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSpTables(t *testing.T) {
	t.Run("no argument", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_tables")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.tables")
		require.Contains(t, result, "TABLE_QUALIFIER")
		require.Contains(t, result, "TABLE_OWNER")
		require.Contains(t, result, "TABLE_NAME")
		require.Contains(t, result, "TABLE_TYPE")
		require.NotContains(t, result, "AND table_name")
	})

	t.Run("with table name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_tables users")
		require.NoError(t, err)
		require.Contains(t, result, "AND table_name = 'users'")
	})

	t.Run("with quoted table name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_tables 'my_table'")
		require.NoError(t, err)
		require.Contains(t, result, "AND table_name = 'my_table'")
	})

	t.Run("exec prefix", func(t *testing.T) {
		result, err := TranslateCatalogQuery("EXEC sp_tables 'orders'")
		require.NoError(t, err)
		require.Contains(t, result, "AND table_name = 'orders'")
	})
}

func TestIsCatalogQuery_SpColumns(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"bare", "sp_columns", true},
		{"with table name", "sp_columns users", true},
		{"exec prefix", "EXEC sp_columns users", true},
		{"quoted arg", "EXEC sp_columns 'users'", true},
		{"case insensitive", "SP_COLUMNS", true},
		{"named param", "EXEC sp_columns @table_name='users'", false},
		{"named param spaces", "EXEC sp_columns @table_name = 'users'", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSpColumns(t *testing.T) {
	t.Run("no argument", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_columns")
		require.NoError(t, err)
		require.Contains(t, result, "information_schema.columns")
		require.Contains(t, result, "COLUMN_NAME")
		require.Contains(t, result, "TYPE_NAME")
		require.NotContains(t, result, "AND table_name")
	})

	t.Run("with table name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_columns users")
		require.NoError(t, err)
		require.Contains(t, result, "AND table_name = 'users'")
	})

	t.Run("exec prefix with quoted name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("EXEC sp_columns 'orders'")
		require.NoError(t, err)
		require.Contains(t, result, "AND table_name = 'orders'")
	})
}

func TestIsCatalogQuery_SpHelptext(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"bare with arg", "sp_helptext myview", true},
		{"exec prefix", "EXEC sp_helptext myview", true},
		{"quoted arg", "EXEC sp_helptext 'myview'", true},
		{"bare no arg", "sp_helptext", true},
		{"case insensitive", "SP_HELPTEXT myview", true},
		{"named param", "EXEC sp_helptext @objname='myview'", false},
		{"named param spaces", "EXEC sp_helptext @objname = 'myview'", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCatalogQuery(tt.input))
		})
	}
}

func TestTranslateSpHelptext(t *testing.T) {
	t.Run("with object name", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_helptext myview")
		require.NoError(t, err)
		require.Contains(t, result, "pg_catalog.pg_views")
		require.Contains(t, result, "viewname = 'myview'")
		require.Contains(t, result, "pg_catalog.pg_proc")
		require.Contains(t, result, "proname = 'myview'")
	})

	t.Run("no argument returns empty", func(t *testing.T) {
		result, err := TranslateCatalogQuery("sp_helptext")
		require.NoError(t, err)
		require.Contains(t, result, "WHERE false")
	})

	t.Run("exec prefix", func(t *testing.T) {
		result, err := TranslateCatalogQuery("EXEC sp_helptext 'my_proc'")
		require.NoError(t, err)
		require.Contains(t, result, "viewname = 'my_proc'")
	})
}

func TestIsCatalogQuery_NonCatalogQueries(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple select", "SELECT * FROM users"},
		{"insert", "INSERT INTO users (name) VALUES ('Alice')"},
		{"update", "UPDATE users SET name = 'Bob' WHERE id = 1"},
		{"delete", "DELETE FROM users WHERE id = 1"},
		{"create table", "CREATE TABLE t (id INT)"},
		{"use database", "USE mydb"},
		{"empty string", ""},
		{"whitespace only", "   "},
		{"select with version in name", "SELECT * FROM versions"},
		{"select version column", "SELECT version FROM settings"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.False(t, IsCatalogQuery(tt.input),
				"expected IsCatalogQuery to return false for: %q", tt.input)
		})
	}
}

func TestTranslateCatalogQuery_ErrorOnNonCatalog(t *testing.T) {
	_, err := TranslateCatalogQuery("SELECT * FROM users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a catalog query")
}

func TestStripQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"'mydb'", "mydb"},
		{`"mydb"`, "mydb"},
		{"[mydb]", "mydb"},
		{"mydb", "mydb"},
		{"", ""},
		{"a", "a"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.expected, stripQuotes(tt.input))
		})
	}
}
