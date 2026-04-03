// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package catalog provides system catalog emulation for the TDS protocol
// layer. It detects and translates system queries and SET commands into
// CockroachDB-compatible SQL, allowing Sybase/SQL Server drivers and
// applications to perform their usual startup and metadata queries against
// CockroachDB without modification.
//
// # Dialect scope
//
// Most features here are common to both SQL Server and Sybase ASE, since
// they share a common heritage of system stored procedures and system tables.
//
// [Both] Features used by both SQL Server and Sybase ASE:
//   - SELECT @@VERSION
//   - sp_helpdb, sp_help (stored procedure calls)
//   - SET QUOTED_IDENTIFIER, SET ANSI_NULLS, SET TEXTSIZE,
//     SET ARITHABORT, SET CONCAT_NULL_YIELDS_NULL
//
// [Sybase ASE] Features primarily used by Sybase ASE drivers:
//   - sysobjects/syscolumns system table queries (Sybase ASE uses these
//     extensively; SQL Server prefers sys.objects/sys.columns)
//   - The @@VERSION string is formatted as Sybase ASE ("Adaptive Server
//     Enterprise") since most TDS drivers that connect to the TDS port
//     identify themselves as Sybase clients.
package catalog

import (
	"fmt"
	"regexp"
	"strings"
)

// CRDBVersion is the CockroachDB version string embedded in the synthetic
// @@VERSION response. It can be overridden at init time for testing or to
// reflect the actual cluster version.
var CRDBVersion = "24.3.0"

// versionString returns a Sybase ASE-compatible @@VERSION string that
// also identifies the underlying CockroachDB instance.
func versionString() string {
	return fmt.Sprintf(
		"Adaptive Server Enterprise/16.0/CockroachDB %s/SP0/Enterprise Linux/x86_64",
		CRDBVersion,
	)
}

// Precompiled patterns for catalog query detection.
var (
	// reVersion matches SELECT @@VERSION (with optional whitespace/semicolons).
	reVersion = regexp.MustCompile(`(?i)^\s*SELECT\s+@@VERSION\s*;?\s*$`)

	// reSpHelpDB matches sp_helpdb with an optional database name argument.
	reSpHelpDB = regexp.MustCompile(`(?i)^\s*(?:EXEC(?:UTE)?\s+)?sp_helpdb(?:\s+(\S+))?\s*;?\s*$`)

	// reSpHelp matches sp_help with an optional table name argument.
	reSpHelp = regexp.MustCompile(`(?i)^\s*(?:EXEC(?:UTE)?\s+)?sp_help(?:\s+(\S+))?\s*;?\s*$`)

	// reSysobjects matches queries that reference the sysobjects system table.
	reSysobjects = regexp.MustCompile(`(?i)\bFROM\s+(?:dbo\.)?sysobjects\b`)

	// reSyscolumns matches queries that reference the syscolumns system table.
	reSyscolumns = regexp.MustCompile(`(?i)\bFROM\s+(?:dbo\.)?syscolumns\b`)

	// reSetCommand matches common Sybase/SQL Server SET commands that drivers
	// send during connection initialization. These are acknowledged silently.
	// [Both] Most SET options are common to both dialects.
	// [SQL Server] SET IDENTITY_INSERT is SQL Server-specific.
	// [Both] SET ROWCOUNT is supported by both dialects.
	reSetCommand = regexp.MustCompile(
		`(?i)^\s*SET\s+(?:` +
			`QUOTED_IDENTIFIER\s+(?:ON|OFF)` +
			`|ANSI_NULLS\s+(?:ON|OFF)` +
			`|TEXTSIZE\s+\d+` +
			`|ARITHABORT\s+(?:ON|OFF)` +
			`|CONCAT_NULL_YIELDS_NULL\s+(?:ON|OFF)` +
			`|ROWCOUNT\s+\d+` +
			`|IDENTITY_INSERT\s+\S+\s+(?:ON|OFF)` +
			`)\s*;?\s*$`,
	)
)

// QueryType classifies the kind of catalog query detected.
type QueryType int

const (
	// QueryNone indicates the SQL is not a recognized catalog query.
	QueryNone QueryType = iota
	// QueryVersion is a SELECT @@VERSION query.
	QueryVersion
	// QuerySpHelpDB is an sp_helpdb invocation.
	QuerySpHelpDB
	// QuerySpHelp is an sp_help invocation.
	QuerySpHelp
	// QuerySysobjects is a query referencing the sysobjects table.
	QuerySysobjects
	// QuerySyscolumns is a query referencing the syscolumns table.
	QuerySyscolumns
	// QuerySet is a recognized SET command.
	QuerySet
)

// IsCatalogQuery reports whether sql is a Sybase system catalog query or
// a SET command that should be handled by the catalog layer rather than
// being forwarded to the SQL executor.
func IsCatalogQuery(sql string) bool {
	return classifyQuery(sql) != QueryNone
}

// classifyQuery determines the type of catalog query, if any.
func classifyQuery(sql string) QueryType {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return QueryNone
	}

	if reVersion.MatchString(trimmed) {
		return QueryVersion
	}
	if reSpHelpDB.MatchString(trimmed) {
		return QuerySpHelpDB
	}
	if reSpHelp.MatchString(trimmed) {
		return QuerySpHelp
	}
	if reSetCommand.MatchString(trimmed) {
		return QuerySet
	}
	if reSysobjects.MatchString(trimmed) {
		return QuerySysobjects
	}
	if reSyscolumns.MatchString(trimmed) {
		return QuerySyscolumns
	}
	return QueryNone
}

// TranslateCatalogQuery translates a Sybase system catalog query into a
// CockroachDB-compatible SQL string. For SET commands and @@VERSION, the
// returned SQL is a synthetic query or an empty string (for SET). For
// queries that are not catalog queries, an error is returned.
func TranslateCatalogQuery(sql string) (string, error) {
	qt := classifyQuery(sql)
	switch qt {
	case QueryVersion:
		return translateVersion(), nil
	case QuerySpHelpDB:
		return translateSpHelpDB(sql), nil
	case QuerySpHelp:
		return translateSpHelp(sql), nil
	case QuerySysobjects:
		return translateSysobjects(sql), nil
	case QuerySyscolumns:
		return translateSyscolumns(sql), nil
	case QuerySet:
		// SET commands are acknowledged without executing anything.
		// Return empty string to signal "send DONE token with no results."
		return "", nil
	default:
		return "", fmt.Errorf("catalog: not a catalog query: %s", sql)
	}
}

// translateVersion returns a SELECT that produces a Sybase-compatible
// @@VERSION string.
func translateVersion() string {
	escaped := strings.ReplaceAll(versionString(), "'", "''")
	return fmt.Sprintf("SELECT '%s' AS version", escaped)
}

// translateSpHelpDB converts sp_helpdb to a query against
// information_schema.schemata. If a database name is provided, it filters
// by that name; otherwise it returns all databases.
func translateSpHelpDB(sql string) string {
	matches := reSpHelpDB.FindStringSubmatch(sql)
	if matches != nil && matches[1] != "" {
		dbName := stripQuotes(matches[1])
		return fmt.Sprintf(
			"SELECT catalog_name AS name, schema_owner AS db_owner, "+
				"'UTF-8' AS charset "+
				"FROM information_schema.schemata "+
				"WHERE catalog_name = '%s'",
			strings.ReplaceAll(dbName, "'", "''"),
		)
	}
	return "SELECT catalog_name AS name, schema_owner AS db_owner, " +
		"'UTF-8' AS charset " +
		"FROM information_schema.schemata"
}

// translateSpHelp converts sp_help to information_schema queries. Without
// a table argument, it lists all user tables. With a table argument, it
// returns column metadata for that table.
func translateSpHelp(sql string) string {
	matches := reSpHelp.FindStringSubmatch(sql)
	if matches != nil && matches[1] != "" {
		tableName := stripQuotes(matches[1])
		return fmt.Sprintf(
			"SELECT column_name, data_type, character_maximum_length, "+
				"is_nullable, column_default "+
				"FROM information_schema.columns "+
				"WHERE table_name = '%s' "+
				"ORDER BY ordinal_position",
			strings.ReplaceAll(tableName, "'", "''"),
		)
	}
	return "SELECT table_name AS name, table_type AS type " +
		"FROM information_schema.tables " +
		"WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'crdb_internal') " +
		"ORDER BY table_name"
}

// translateSysobjects translates queries against sysobjects to equivalent
// pg_catalog queries. The mapping is:
//
//	sysobjects.name   -> pg_class.relname
//	sysobjects.type   -> pg_class.relkind mapped to Sybase type codes:
//	                     'r' (table) -> 'U' (user table)
//	                     'v' (view)  -> 'V' (view)
//	                     'p' (proc)  -> 'P' (stored procedure)
//	sysobjects.id     -> pg_class.oid
//	sysobjects.uid    -> pg_class.relowner
//	sysobjects.crdate -> pg_class.reltuples (placeholder, no exact analog)
func translateSysobjects(sql string) string {
	translated := reSysobjects.ReplaceAllString(sql, "FROM pg_catalog.pg_class")

	// Map column references.
	translated = replaceColumnRef(translated, "sysobjects", "name", "relname")
	translated = replaceColumnRef(translated, "sysobjects", "id", "oid")
	translated = replaceColumnRef(translated, "sysobjects", "uid", "relowner")

	// Map type column: sysobjects type codes to pg_class relkind.
	// Replace type = 'U' with relkind = 'r', type = 'V' with relkind = 'v', etc.
	translated = replaceTypeFilter(translated, "U", "r")
	translated = replaceTypeFilter(translated, "V", "v")
	translated = replaceTypeFilter(translated, "P", "p")

	// Replace bare column references (without table prefix).
	translated = replaceBareColumnRef(translated, "name", "relname")
	translated = replaceBareColumnRef(translated, "type", "relkind")

	// Replace select-list sysobjects.type with a CASE expression.
	reSysobjectsType := regexp.MustCompile(`(?i)\bsysobjects\.type\b`)
	translated = reSysobjectsType.ReplaceAllString(translated,
		"CASE relkind WHEN 'r' THEN 'U' WHEN 'v' THEN 'V' WHEN 'p' THEN 'P' ELSE relkind END")

	// Clean up any remaining dbo. prefix.
	translated = regexp.MustCompile(`(?i)\bdbo\.\b`).ReplaceAllString(translated, "")

	return translated
}

// translateSyscolumns translates queries against syscolumns to
// information_schema.columns.
func translateSyscolumns(sql string) string {
	translated := reSyscolumns.ReplaceAllString(sql, "FROM information_schema.columns")

	// Map column references.
	translated = replaceColumnRef(translated, "syscolumns", "name", "column_name")
	translated = replaceColumnRef(translated, "syscolumns", "colid", "ordinal_position")
	translated = replaceColumnRef(translated, "syscolumns", "length", "character_maximum_length")
	translated = replaceColumnRef(translated, "syscolumns", "status", "is_nullable")

	// Replace bare column references.
	translated = replaceBareColumnRef(translated, "colid", "ordinal_position")

	// Clean up any remaining dbo. prefix.
	translated = regexp.MustCompile(`(?i)\bdbo\.\b`).ReplaceAllString(translated, "")

	return translated
}

// replaceColumnRef replaces table.column references with the translated
// column name. For example, replaceColumnRef(sql, "sysobjects", "name", "relname")
// replaces "sysobjects.name" with "relname".
func replaceColumnRef(sql, table, oldCol, newCol string) string {
	pattern := regexp.MustCompile(`(?i)\b` + table + `\.` + oldCol + `\b`)
	return pattern.ReplaceAllString(sql, newCol)
}

// replaceBareColumnRef replaces bare column references in SELECT lists
// (without table prefix), but only if they appear as standalone words.
// This is conservative: it only replaces exact word-boundary matches.
func replaceBareColumnRef(sql, oldCol, newCol string) string {
	// Only replace SELECT-list occurrences: look for the column name
	// preceded by SELECT/comma and followed by comma/FROM/AS/whitespace.
	pattern := regexp.MustCompile(`(?i)(?:SELECT|,)\s+` + oldCol + `\b`)
	return pattern.ReplaceAllStringFunc(sql, func(match string) string {
		reOld := regexp.MustCompile(`(?i)\b` + oldCol + `\b`)
		return reOld.ReplaceAllString(match, newCol)
	})
}

// replaceTypeFilter replaces WHERE-clause type comparisons. For example,
// type = 'U' is replaced with relkind = 'r'.
func replaceTypeFilter(sql, sybaseType, pgKind string) string {
	// Match type = 'X' or type='X' (with optional whitespace).
	pattern := regexp.MustCompile(
		`(?i)\btype\s*=\s*'` + sybaseType + `'`,
	)
	return pattern.ReplaceAllString(sql, fmt.Sprintf("relkind = '%s'", pgKind))
}

// stripQuotes removes surrounding single quotes, double quotes, or square
// brackets from an identifier or string literal.
func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') ||
			(s[0] == '"' && s[len(s)-1] == '"') ||
			(s[0] == '[' && s[len(s)-1] == ']') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
