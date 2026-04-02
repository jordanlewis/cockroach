// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package catalog provides Oracle catalog emulation for the TNS frontend.
//
// Oracle clients query system views (ALL_TABLES, V$VERSION, V$SESSION,
// DUAL) and issue session commands (ALTER SESSION SET) during connection
// setup and normal operation. The Catalog intercepts these, returning
// either static results or CockroachDB-compatible SQL rewrites.
//
// Typical usage:
//
//	cat := catalog.New("v24.1.0", "admin")
//	resp := cat.Handle("SELECT USER FROM DUAL")
//	if resp.Handled {
//	    // Use resp.Result for static data, or resp.RewriteSQL
//	    // for queries to forward to CockroachDB.
//	}
//
// The Catalog maintains session-level NLS parameter state that persists
// across queries and can be modified via ALTER SESSION SET statements.
// Create one Catalog per TNS session. Catalog is not safe for concurrent
// use.
package catalog

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Column describes a column in a catalog query result.
type Column struct {
	Name string
}

// Result holds a static result set returned directly to the Oracle client
// without forwarding to CockroachDB.
type Result struct {
	Columns []Column
	Rows    [][]string
}

// Response describes how the TNS frontend should handle a query.
type Response struct {
	// Handled is true when the catalog recognized and processed
	// the query. When false, the caller should forward the query
	// through normal Oracle-to-CockroachDB SQL translation.
	Handled bool

	// Result is non-nil for queries answered with static data
	// (DUAL, V$VERSION, V$SESSION, NLS parameters, USER).
	Result *Result

	// RewriteSQL is non-empty for queries that should be forwarded
	// to CockroachDB with different SQL (ALL_TABLES). The rewritten
	// SQL is in CockroachDB-compatible syntax and should bypass
	// Oracle SQL translation.
	RewriteSQL string

	// OK is true for statements that succeed with no result set,
	// such as ALTER SESSION SET.
	OK bool
}

// Catalog provides Oracle catalog emulation for a TNS session.
//
// It handles queries against Oracle system views and tables, returning
// static results where the view has no CockroachDB equivalent (V$VERSION)
// or SQL rewrites that target CockroachDB's information_schema
// (ALL_TABLES). It also manages session-level NLS parameters.
type Catalog struct {
	nlsParams map[string]string
	version   string
	username  string
}

// New creates a Catalog with default Oracle NLS parameters.
func New(version, username string) *Catalog {
	return &Catalog{
		nlsParams: defaultNLSParams(),
		version:   version,
		username:  username,
	}
}

func defaultNLSParams() map[string]string {
	return map[string]string{
		"NLS_LANGUAGE":            "AMERICAN",
		"NLS_TERRITORY":           "AMERICA",
		"NLS_CURRENCY":            "$",
		"NLS_ISO_CURRENCY":        "AMERICA",
		"NLS_NUMERIC_CHARACTERS":  ".,",
		"NLS_CALENDAR":            "GREGORIAN",
		"NLS_DATE_FORMAT":         "DD-MON-RR",
		"NLS_DATE_LANGUAGE":       "AMERICAN",
		"NLS_SORT":                "BINARY",
		"NLS_COMP":                "BINARY",
		"NLS_TIMESTAMP_FORMAT":    "DD-MON-RR HH.MI.SSXFF AM",
		"NLS_TIMESTAMP_TZ_FORMAT": "DD-MON-RR HH.MI.SSXFF AM TZR",
		"NLS_DUAL_CURRENCY":       "$",
		"NLS_LENGTH_SEMANTICS":    "BYTE",
		"NLS_NCHAR_CONV_EXCP":     "FALSE",
	}
}

// Compiled regexes for query detection and extraction. These cover
// the Oracle catalog queries commonly issued by drivers like go-ora
// during connection setup and metadata introspection.
var (
	// alterSessionRe extracts param and value from ALTER SESSION SET.
	alterSessionRe = regexp.MustCompile(
		`(?i)^\s*ALTER\s+SESSION\s+SET\s+(\w+)\s*=\s*'?([^']*?)'?\s*$`,
	)

	// fromDualRe detects and strips FROM DUAL in queries.
	fromDualRe = regexp.MustCompile(`(?i)\s+FROM\s+DUAL\b`)

	// selectUserFromDualRe matches the exact SELECT USER FROM DUAL.
	selectUserFromDualRe = regexp.MustCompile(
		`(?i)^\s*SELECT\s+USER\s+FROM\s+DUAL\s*$`,
	)

	// sysContextRe extracts namespace and parameter from
	// SYS_CONTEXT('namespace', 'param') calls.
	sysContextRe = regexp.MustCompile(
		`(?i)SYS_CONTEXT\s*\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)`,
	)

	// userKeywordRe matches standalone USER keyword (Oracle pseudo-column)
	// but not USERENV, USERNAME, CURRENT_USER, etc.
	userKeywordRe = regexp.MustCompile(`(?i)\bUSER\b`)
)

// Oracle catalog table names, normalized to uppercase.
const (
	tableDual               = "DUAL"
	tableAllTables          = "ALL_TABLES"
	tableVVersion           = "V$VERSION"
	tableVSession           = "V$SESSION"
	tableNLSSessionParams   = "NLS_SESSION_PARAMETERS"
	tableVNLSParams         = "V$NLS_PARAMETERS"
	tableNLSDatabaseParams  = "NLS_DATABASE_PARAMETERS"
	tableAllTabColumns      = "ALL_TAB_COLUMNS"
	tableProductComponentVn = "PRODUCT_COMPONENT_VERSION"
)

// Handle inspects a SQL statement and returns a Response indicating
// how the TNS frontend should process it.
//
// If Response.Handled is false, the statement is not catalog-related
// and should proceed through normal Oracle-to-CockroachDB translation.
func (c *Catalog) Handle(sql string) Response {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")
	sql = strings.TrimSpace(sql)

	// ALTER SESSION SET must be checked first since it's a statement,
	// not a query.
	if m := alterSessionRe.FindStringSubmatch(sql); m != nil {
		return c.handleAlterSession(m[1], m[2])
	}

	upper := strings.ToUpper(sql)

	// SELECT USER FROM DUAL — exact match handled before general DUAL.
	if selectUserFromDualRe.MatchString(sql) {
		return Response{
			Handled: true,
			Result: &Result{
				Columns: []Column{{Name: "USER"}},
				Rows:    [][]string{{c.username}},
			},
		}
	}

	// Route based on the catalog table referenced in FROM clause.
	switch {
	case containsFrom(upper, tableVVersion),
		containsFrom(upper, tableProductComponentVn):
		return c.handleVVersion()

	case containsFrom(upper, tableVSession):
		return c.handleVSession()

	case containsFrom(upper, tableNLSSessionParams),
		containsFrom(upper, tableVNLSParams),
		containsFrom(upper, tableNLSDatabaseParams):
		return c.handleNLSParams()

	case containsFrom(upper, tableAllTables):
		return c.handleAllTables()

	case containsFrom(upper, tableAllTabColumns):
		return c.handleAllTabColumns()

	case containsFrom(upper, tableDual):
		return c.handleDual(sql)
	}

	return Response{Handled: false}
}

// containsFrom checks whether the upper-cased SQL contains "FROM <table>".
func containsFrom(upper, table string) bool {
	return strings.Contains(upper, "FROM "+table)
}

func (c *Catalog) handleAlterSession(param, value string) Response {
	param = strings.ToUpper(strings.TrimSpace(param))
	value = strings.TrimSpace(value)
	c.nlsParams[param] = value
	return Response{Handled: true, OK: true}
}

func (c *Catalog) handleVVersion() Response {
	banner := fmt.Sprintf(
		"CockroachDB %s - Oracle Compatible", c.version,
	)
	return Response{
		Handled: true,
		Result: &Result{
			Columns: []Column{
				{Name: "BANNER"},
				{Name: "BANNER_FULL"},
				{Name: "BANNER_LEGACY"},
				{Name: "CON_ID"},
			},
			Rows: [][]string{
				{banner, banner, c.version, "0"},
			},
		},
	}
}

func (c *Catalog) handleVSession() Response {
	return Response{
		Handled: true,
		Result: &Result{
			Columns: []Column{
				{Name: "SID"},
				{Name: "SERIAL#"},
				{Name: "USERNAME"},
				{Name: "STATUS"},
				{Name: "SERVER"},
				{Name: "SCHEMANAME"},
				{Name: "OSUSER"},
				{Name: "MACHINE"},
				{Name: "PROGRAM"},
				{Name: "TYPE"},
			},
			Rows: [][]string{
				{
					"1", "1", c.username, "ACTIVE", "DEDICATED",
					c.username, "", "", "", "USER",
				},
			},
		},
	}
}

func (c *Catalog) handleNLSParams() Response {
	// Sort parameters for deterministic output.
	params := make([]string, 0, len(c.nlsParams))
	for p := range c.nlsParams {
		params = append(params, p)
	}
	sort.Strings(params)

	rows := make([][]string, len(params))
	for i, p := range params {
		rows[i] = []string{p, c.nlsParams[p]}
	}
	return Response{
		Handled: true,
		Result: &Result{
			Columns: []Column{
				{Name: "PARAMETER"},
				{Name: "VALUE"},
			},
			Rows: rows,
		},
	}
}

func (c *Catalog) handleAllTables() Response {
	// Map Oracle's ALL_TABLES to CockroachDB's information_schema.tables.
	// Oracle's ALL_TABLES shows tables accessible to the current user;
	// information_schema.tables provides the equivalent in CRDB.
	return Response{
		Handled:    true,
		RewriteSQL: allTablesRewrite,
	}
}

const allTablesRewrite = `SELECT
  table_schema AS "OWNER",
  table_name AS "TABLE_NAME",
  'DEFAULT' AS "TABLESPACE_NAME",
  '' AS "CLUSTER_NAME",
  0 AS "NUM_ROWS",
  0 AS "BLOCKS",
  0 AS "AVG_ROW_LEN"
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
ORDER BY table_schema, table_name`

func (c *Catalog) handleAllTabColumns() Response {
	// Map Oracle's ALL_TAB_COLUMNS to information_schema.columns.
	return Response{
		Handled:    true,
		RewriteSQL: allTabColumnsRewrite,
	}
}

const allTabColumnsRewrite = `SELECT
  table_schema AS "OWNER",
  table_name AS "TABLE_NAME",
  column_name AS "COLUMN_NAME",
  ordinal_position AS "COLUMN_ID",
  data_type AS "DATA_TYPE",
  COALESCE(character_maximum_length, 0) AS "DATA_LENGTH",
  COALESCE(numeric_precision, 0) AS "DATA_PRECISION",
  COALESCE(numeric_scale, 0) AS "DATA_SCALE",
  CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS "NULLABLE"
FROM information_schema.columns
ORDER BY table_schema, table_name, ordinal_position`

func (c *Catalog) handleDual(sql string) Response {
	// Handle SYS_CONTEXT calls embedded in DUAL queries, e.g.
	// SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL.
	if sysContextRe.MatchString(sql) {
		return c.handleSysContext(sql)
	}

	// General DUAL handling: strip FROM DUAL and rewrite Oracle-specific
	// pseudo-columns. The result is valid CockroachDB SQL.
	rewritten := fromDualRe.ReplaceAllString(sql, "")
	rewritten = strings.TrimSpace(rewritten)

	// Replace standalone USER keyword with current_user.
	rewritten = replaceUserKeyword(rewritten)

	return Response{
		Handled:    true,
		RewriteSQL: rewritten,
	}
}

// replaceUserKeyword replaces the Oracle USER pseudo-column with
// CockroachDB's current_user in a SQL fragment. It only matches
// standalone USER, not USERENV, USERNAME, or CURRENT_USER.
func replaceUserKeyword(sql string) string {
	return userKeywordRe.ReplaceAllStringFunc(sql, func(match string) string {
		return "current_user"
	})
}

func (c *Catalog) handleSysContext(sql string) Response {
	m := sysContextRe.FindStringSubmatch(sql)
	if m == nil {
		return Response{Handled: false}
	}

	namespace := strings.ToUpper(m[1])
	param := strings.ToUpper(m[2])

	var value string
	if namespace == "USERENV" {
		value = c.resolveSysContextUserenv(param)
	}

	colName := fmt.Sprintf(
		"SYS_CONTEXT('%s','%s')", namespace, param,
	)
	return Response{
		Handled: true,
		Result: &Result{
			Columns: []Column{{Name: colName}},
			Rows:    [][]string{{value}},
		},
	}
}

func (c *Catalog) resolveSysContextUserenv(param string) string {
	switch param {
	case "SID", "SESSION_ID":
		return "1"
	case "CURRENT_USER", "SESSION_USER":
		return c.username
	case "CURRENT_SCHEMA":
		return c.username
	case "DB_NAME", "DATABASE_NAME":
		return "defaultdb"
	case "LANGUAGE":
		return c.nlsParams["NLS_LANGUAGE"] + "_" +
			c.nlsParams["NLS_TERRITORY"] + ".AL32UTF8"
	case "NLS_DATE_FORMAT":
		return c.nlsParams["NLS_DATE_FORMAT"]
	case "NLS_CALENDAR":
		return c.nlsParams["NLS_CALENDAR"]
	case "NLS_CURRENCY":
		return c.nlsParams["NLS_CURRENCY"]
	case "NLS_SORT":
		return c.nlsParams["NLS_SORT"]
	default:
		return ""
	}
}

// NLSParam returns the current value of an NLS parameter.
func (c *Catalog) NLSParam(name string) string {
	return c.nlsParams[strings.ToUpper(name)]
}

// SetNLSParam sets an NLS parameter value.
func (c *Catalog) SetNLSParam(name, value string) {
	c.nlsParams[strings.ToUpper(name)] = value
}

// Username returns the connected username.
func (c *Catalog) Username() string {
	return c.username
}
