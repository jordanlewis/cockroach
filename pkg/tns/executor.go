// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tns

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/tns/catalog"
	"github.com/cockroachdb/cockroach/pkg/tns/tnswire"
	"github.com/cockroachdb/cockroach/pkg/tns/translate"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// cursorState tracks the state of a single Oracle cursor across the
// OPEN → EXEC → FETCH → CLOSE lifecycle.
type cursorState struct {
	// sql is the translated CockroachDB SQL for this cursor.
	sql string
	// columns is the column metadata from the OPEN response.
	columns []tnswire.ColumnDesc
	// rows holds buffered result rows from the last EXEC/SELECT.
	rows [][][]byte
	// fetchOffset tracks how many rows have been returned via FETCH.
	fetchOffset int
	// isDDL indicates a DDL statement that doesn't produce rows.
	isDDL bool
	// isRewrite indicates the SQL was a catalog rewrite (already CRDB SQL).
	isRewrite bool
	// bindParams maps positional param indices to Oracle bind var names.
	bindParams map[int]string
}

// Executor bridges the CockroachDB internal SQL executor and the TNS/TTI
// protocol. Each connection gets its own Executor to track per-connection
// state such as open cursors and the current database.
type Executor struct {
	db isql.DB

	currentDatabase string
	cursors         map[uint16]*cursorState
}

// NewExecutor creates an Executor bound to the given isql.DB.
func NewExecutor(db isql.DB, defaultDatabase string) *Executor {
	return &Executor{
		db:              db,
		currentDatabase: defaultDatabase,
		cursors:         make(map[uint16]*cursorState),
	}
}

// executorOverride returns an InternalExecutorOverride configured for
// the current connection's database.
func (e *Executor) executorOverride() sessiondata.InternalExecutorOverride {
	return sessiondata.InternalExecutorOverride{
		User:     username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
		Database: e.currentDatabase,
	}
}

// Open handles a TTI OPEN request. It translates the Oracle SQL to
// CockroachDB SQL, determines column metadata, and stores cursor state
// for subsequent EXEC/FETCH calls.
func (e *Executor) Open(
	ctx context.Context, cursorID uint16, oracleSQL string, cat *catalog.Catalog,
) ([]tnswire.ColumnDesc, error) {
	oracleSQL = strings.TrimSpace(oracleSQL)
	oracleSQL = strings.TrimRight(oracleSQL, ";")

	cs := &cursorState{}

	// Check if the catalog handles this query (system views, ALTER SESSION).
	if cat != nil {
		resp := cat.Handle(oracleSQL)
		if resp.Handled {
			return e.handleCatalogOpen(ctx, cursorID, resp, cs)
		}
	}

	// Determine if this is a DDL statement before parsing, since the Oracle
	// parser doesn't support all DDL forms (e.g. CREATE TABLE).
	upperSQL := strings.ToUpper(strings.TrimSpace(oracleSQL))
	cs.isDDL = strings.HasPrefix(upperSQL, "CREATE ") ||
		strings.HasPrefix(upperSQL, "ALTER ") ||
		strings.HasPrefix(upperSQL, "DROP ")

	if cs.isDDL {
		// For DDL, do basic Oracle→CRDB type substitution without
		// going through the full Oracle parser.
		cs.sql = translateDDLTypes(oracleSQL)
	} else {
		// Translate Oracle SQL → CockroachDB SQL via the full parser.
		// If the parser fails (unsupported syntax), fall back to
		// passthrough with basic type substitution.
		result, err := translate.Translate(oracleSQL)
		if err != nil {
			cs.sql = translateDDLTypes(oracleSQL)
		} else {
			cs.sql = result.SQL
			cs.bindParams = result.Params
		}
	}

	if cs.isDDL || isDML(upperSQL) {
		// Execute DDL/DML immediately at OPEN time — Oracle clients
		// often combine OPEN+EXEC for non-SELECT statements.
		executor := e.db.Executor()
		rowCount, err := executor.ExecEx(
			ctx,
			redact.Sprint("tns-open-exec"),
			nil, // txn
			e.executorOverride(),
			cs.sql,
		)
		if err != nil {
			return nil, err
		}
		cs.columns = []tnswire.ColumnDesc{
			{TypeCode: tnswire.OracleTypeNumber, Name: "ROWS_AFFECTED"},
		}
		cs.rows = [][][]byte{{[]byte(fmt.Sprintf("%d", rowCount))}}
	} else {
		// SELECT: execute to get column metadata and buffer results.
		cols, rows, err := e.executeSelect(ctx, cs.sql)
		if err != nil {
			return nil, err
		}
		cs.columns = cols
		cs.rows = rows
	}

	e.cursors[cursorID] = cs
	return cs.columns, nil
}

// handleCatalogOpen handles OPEN for queries intercepted by the catalog.
func (e *Executor) handleCatalogOpen(
	ctx context.Context, cursorID uint16, resp catalog.Response, cs *cursorState,
) ([]tnswire.ColumnDesc, error) {
	if resp.OK {
		// Statement like ALTER SESSION SET — no result set.
		cs.isDDL = true
		cs.columns = []tnswire.ColumnDesc{
			{TypeCode: tnswire.OracleTypeNumber, Name: "STATUS"},
		}
		cs.rows = [][][]byte{{[]byte("0")}}
		e.cursors[cursorID] = cs
		return cs.columns, nil
	}

	if resp.Result != nil {
		// Static catalog result.
		cols := make([]tnswire.ColumnDesc, len(resp.Result.Columns))
		for i, c := range resp.Result.Columns {
			cols[i] = tnswire.ColumnDesc{
				TypeCode: tnswire.OracleTypeVarchar2,
				Name:     c.Name,
			}
		}
		rows := make([][][]byte, len(resp.Result.Rows))
		for i, row := range resp.Result.Rows {
			brow := make([][]byte, len(row))
			for j, val := range row {
				brow[j] = []byte(val)
			}
			rows[i] = brow
		}
		cs.columns = cols
		cs.rows = rows
		e.cursors[cursorID] = cs
		return cs.columns, nil
	}

	if resp.RewriteSQL != "" {
		// Rewritten SQL — execute against CRDB.
		cs.isRewrite = true
		cs.sql = resp.RewriteSQL
		cols, rows, err := e.executeSelect(ctx, cs.sql)
		if err != nil {
			return nil, err
		}
		cs.columns = cols
		cs.rows = rows
		e.cursors[cursorID] = cs
		return cs.columns, nil
	}

	return nil, errors.New("catalog returned empty response")
}

// Exec handles a TTI EXEC request. For DML/DDL, it executes the statement
// and returns the rows affected. For SELECT, the rows were already buffered
// at OPEN time so this is a no-op.
func (e *Executor) Exec(
	ctx context.Context,
	cursorID uint16,
	sql string,
	bindVars []tnswire.BindVar,
	cat *catalog.Catalog,
) (int, error) {
	cs, ok := e.cursors[cursorID]
	if !ok {
		// Inline EXEC with SQL — open and execute in one step.
		if sql != "" {
			cols, err := e.Open(ctx, cursorID, sql, cat)
			if err != nil {
				return 0, err
			}
			cs = e.cursors[cursorID]
			_ = cols
		} else {
			return 0, errors.Newf("cursor %d not found", cursorID)
		}
	}

	// Re-prepare if new SQL was provided.
	if sql != "" && cs.sql == "" {
		cols, err := e.Open(ctx, cursorID, sql, cat)
		if err != nil {
			return 0, err
		}
		cs = e.cursors[cursorID]
		_ = cols
	}

	// For SELECT, rows were buffered at OPEN time.
	if !cs.isDDL && !isDML(strings.ToUpper(strings.TrimSpace(cs.sql))) {
		return len(cs.rows), nil
	}

	// Execute DML/DDL.
	executor := e.db.Executor()
	args := e.buildExecArgs(bindVars, cs.bindParams)
	rowCount, err := executor.ExecEx(
		ctx,
		redact.Sprint("tns-exec"),
		nil, // txn
		e.executorOverride(),
		cs.sql,
		args...,
	)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

// Fetch returns buffered rows for the given cursor, advancing the
// fetch offset.
func (e *Executor) Fetch(cursorID uint16, fetchSize int) (tnswire.TTIFetchResponse, int, error) {
	cs, ok := e.cursors[cursorID]
	if !ok {
		return tnswire.TTIFetchResponse{},
			0,
			errors.Newf("cursor %d not found", cursorID)
	}

	numCols := len(cs.columns)
	remaining := len(cs.rows) - cs.fetchOffset
	if remaining <= 0 {
		return tnswire.TTIFetchResponse{
			Rows:  nil,
			Flags: 0, // no more rows
		}, numCols, nil
	}

	count := fetchSize
	if count > remaining {
		count = remaining
	}

	rows := cs.rows[cs.fetchOffset : cs.fetchOffset+count]
	cs.fetchOffset += count

	var flags tnswire.FetchFlags
	if cs.fetchOffset < len(cs.rows) {
		flags = tnswire.FetchFlagMoreRows
	}

	return tnswire.TTIFetchResponse{
		Rows:  rows,
		Flags: flags,
	}, numCols, nil
}

// Close releases a cursor.
func (e *Executor) Close(cursorID uint16) {
	delete(e.cursors, cursorID)
}

// executeSelect runs a SELECT via the internal executor and returns
// column metadata and rows in TNS wire format.
func (e *Executor) executeSelect(
	ctx context.Context, sql string,
) ([]tnswire.ColumnDesc, [][][]byte, error) {
	executor := e.db.Executor()
	rows, resultCols, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("tns-select"),
		nil, // txn
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return nil, nil, err
	}

	// Map CRDB result columns to Oracle column descriptors.
	cols := make([]tnswire.ColumnDesc, len(resultCols))
	for i, rc := range resultCols {
		cols[i] = tnswire.ColumnDesc{
			TypeCode: mapCRDBTypeToOracle(rc.Typ),
			Name:     strings.ToUpper(rc.Name),
		}
	}

	// Convert tree.Datums rows to wire-format byte slices.
	wireRows := make([][][]byte, len(rows))
	for i, datumRow := range rows {
		wireRow := make([][]byte, len(datumRow))
		for j, d := range datumRow {
			if d == tree.DNull {
				wireRow[j] = nil
			} else {
				wireRow[j] = []byte(
					tree.AsStringWithFlags(d, tree.FmtBareStrings))
			}
		}
		wireRows[i] = wireRow
	}

	return cols, wireRows, nil
}

// buildExecArgs converts TNS bind variables to Go interface{} arguments
// for the internal executor.
func (e *Executor) buildExecArgs(bindVars []tnswire.BindVar, params map[int]string) []interface{} {
	if len(bindVars) == 0 {
		return nil
	}
	args := make([]interface{}, len(bindVars))
	for i, bv := range bindVars {
		if bv.Value == nil {
			args[i] = nil
		} else {
			args[i] = string(bv.Value)
		}
	}
	return args
}

// mapCRDBTypeToOracle maps a CockroachDB type to the closest Oracle type code.
func mapCRDBTypeToOracle(typ *types.T) tnswire.OracleTypeCode {
	switch typ.Family() {
	case types.IntFamily:
		return tnswire.OracleTypeNumber
	case types.FloatFamily:
		return tnswire.OracleTypeBinaryDouble
	case types.DecimalFamily:
		return tnswire.OracleTypeNumber
	case types.TimestampFamily, types.TimestampTZFamily, types.DateFamily:
		return tnswire.OracleTypeDate
	case types.BytesFamily:
		return tnswire.OracleTypeRaw
	default:
		return tnswire.OracleTypeVarchar2
	}
}

// isDML returns true if the uppercased SQL starts with INSERT, UPDATE, or DELETE.
func isDML(upper string) bool {
	return strings.HasPrefix(upper, "INSERT ") ||
		strings.HasPrefix(upper, "UPDATE ") ||
		strings.HasPrefix(upper, "DELETE ")
}

// translateDDLTypes performs basic Oracle→CockroachDB type name
// substitution for DDL statements that the Oracle SQL parser doesn't
// support. This is intentionally simple text replacement.
func translateDDLTypes(sql string) string {
	r := strings.NewReplacer(
		"NUMBER", "DECIMAL",
		"VARCHAR2", "VARCHAR",
		"NVARCHAR2", "VARCHAR",
		"CLOB", "TEXT",
		"NCLOB", "TEXT",
		"BLOB", "BYTEA",
		"RAW", "BYTEA",
		"BINARY_FLOAT", "REAL",
		"BINARY_DOUBLE", "DOUBLE PRECISION",
	)
	return r.Replace(sql)
}
