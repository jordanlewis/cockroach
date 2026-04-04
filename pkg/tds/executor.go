// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tds

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/tds/catalog"
	"github.com/cockroachdb/cockroach/pkg/tds/parser"
	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
	"github.com/cockroachdb/cockroach/pkg/tds/translate"
	tdstypes "github.com/cockroachdb/cockroach/pkg/tds/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Executor bridges the CockroachDB internal SQL executor (isql.DB) and
// the TDS token stream. Each connection gets its own Executor instance
// to track per-connection state such as the current database, the last
// row count for @@ROWCOUNT, and transaction state.
type Executor struct {
	db isql.DB

	// Per-connection state.
	currentDatabase  string
	lastRowsAffected int

	// Transaction state. When a BEGIN TRAN is executed, activeKVTxn holds
	// the open KV transaction and tranCount tracks the nesting depth
	// (always 0 or 1 since CRDB doesn't support nested transactions).
	activeKVTxn *kv.Txn
	tranCount   int

	// variables holds T-SQL session variables (DECLARE @var).
	// Keys are variable names including the @ prefix (e.g. "@x").
	// Values are SQL literal strings suitable for substitution.
	variables map[string]string
}

// NewExecutor creates an Executor bound to the given isql.DB.
func NewExecutor(db isql.DB, defaultDatabase string) *Executor {
	return &Executor{
		db:              db,
		currentDatabase: defaultDatabase,
	}
}

// Database returns the current database for this connection.
func (e *Executor) Database() string {
	return e.currentDatabase
}

// SetDatabase updates the current database.
func (e *Executor) SetDatabase(db string) {
	e.currentDatabase = db
}

// substituteTranCount replaces the @@TRANCOUNT placeholder (emitted by
// the translator) with the current transaction depth as an integer
// literal. This allows SELECT @@TRANCOUNT to work without a special
// CockroachDB function.
func (e *Executor) substituteTranCount(sql string) string {
	return strings.ReplaceAll(sql, "@@TRANCOUNT", fmt.Sprintf("%d", e.tranCount))
}

// substituteRowCount replaces the @@ROWCOUNT placeholder (emitted by
// the translator) with the last DML row count as an integer literal.
// This allows SELECT @@ROWCOUNT to work without a special CockroachDB
// session variable.
func (e *Executor) substituteRowCount(sql string) string {
	return strings.ReplaceAll(sql, "@@ROWCOUNT", fmt.Sprintf("%d", e.lastRowsAffected))
}

// currentKVTxn returns the active KV transaction, or nil if no
// transaction is open. All executor methods pass this to the internal
// executor so that DML/DDL/SELECT within a BEGIN...COMMIT block
// execute transactionally.
func (e *Executor) currentKVTxn() *kv.Txn {
	return e.activeKVTxn
}

// executorOverride returns an InternalExecutorOverride configured for
// the current connection's database.
func (e *Executor) executorOverride() sessiondata.InternalExecutorOverride {
	return sessiondata.InternalExecutorOverride{
		User:     username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
		Database: e.currentDatabase,
	}
}

// ExecuteBatch processes a SQL batch from a TDS client. It handles
// catalog queries, T-SQL parsing and translation, and dispatches to
// the internal SQL executor. The result is written directly to the
// TokenWriter.
func (e *Executor) ExecuteBatch(
	ctx context.Context, sqlBatch string, tw *tdswire.TokenWriter,
) error {
	trimmed := strings.TrimSpace(sqlBatch)
	if trimmed == "" {
		return writeDoneFinal(tw)
	}

	// Check for catalog queries first (@@VERSION, sp_help*, SET commands, etc.).
	if catalog.IsCatalogQuery(trimmed) {
		return e.handleCatalogQuery(ctx, trimmed, tw)
	}

	// Check for USE database.
	if len(trimmed) >= 4 && strings.EqualFold(trimmed[:4], "USE ") {
		return e.handleUseDatabase(strings.TrimSpace(trimmed[4:]), tw)
	}

	// Check for SET commands not caught by catalog (catch-all).
	// Don't swallow SET @variable assignments — those are parsed as
	// SetVarStmt and handled by the control flow interpreter.
	if len(trimmed) >= 4 && strings.EqualFold(trimmed[:4], "SET ") {
		rest := strings.TrimSpace(trimmed[4:])
		if len(rest) == 0 || rest[0] != '@' {
			return writeDoneFinal(tw)
		}
	}

	// Parse T-SQL.
	batch, err := parser.Parse(trimmed)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, fmt.Sprintf("T-SQL parse error: %s", err))
	}

	// Execute each parsed statement. Control flow statements are handled
	// directly by the interpreter; regular statements go through the
	// translate → execute path.
	for _, stmt := range batch.Stmts {
		if err := e.execParsedStmt(ctx, stmt, tw); err != nil {
			if isControlFlowSignal(err) {
				return writeErrorToken(tw, 50000, 1, 16,
					fmt.Sprintf("%s used outside of a WHILE loop", err.Error()))
			}
			return err
		}
	}

	return nil
}

// handleCatalogQuery handles Sybase system catalog queries.
func (e *Executor) handleCatalogQuery(
	ctx context.Context, sql string, tw *tdswire.TokenWriter,
) error {
	translated, err := catalog.TranslateCatalogQuery(sql)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, err.Error())
	}

	// Empty translation means SET command -- just acknowledge.
	if translated == "" {
		return writeDoneFinal(tw)
	}

	// Execute the translated catalog query as a SELECT.
	return e.executeSelect(ctx, translated, tw)
}

// handleUseDatabase processes a USE <database> command.
func (e *Executor) handleUseDatabase(database string, tw *tdswire.TokenWriter) error {
	database = stripQuotes(database)
	oldDB := e.currentDatabase
	e.currentDatabase = database

	if err := tw.WriteEnvChange(tdswire.EnvChangeToken{
		Type:     tdswire.EnvDatabase,
		NewValue: database,
		OldValue: oldDB,
	}); err != nil {
		return err
	}

	return writeDoneFinal(tw)
}

// executeStatement dispatches a single translated SQL statement to the
// appropriate execution path based on the original T-SQL AST node type.
func (e *Executor) executeStatement(
	ctx context.Context, stmt parser.Statement, crdbSQL string, tw *tdswire.TokenWriter,
) error {
	switch s := stmt.(type) {
	case *parser.UseStmt:
		// USE was translated to SET database = '...'; handle specially.
		return e.handleUseDatabase(s.Database, tw)

	case *parser.CreateDatabaseStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.CreateTableStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.DropTableStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.DropDatabaseStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.AlterTableStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.CreateIndexStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.CreateViewStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.DropViewStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.DropIndexStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.DropProcedureStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.TruncateTableStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.InsertStmt:
		return e.executeDML(ctx, crdbSQL, tw)

	case *parser.SelectStmt:
		if len(s.Compute) > 0 {
			return e.executeSelectWithCompute(ctx, s, crdbSQL, tw)
		}
		return e.executeSelect(ctx, crdbSQL, tw)

	case *parser.CompoundSelectStmt:
		return e.executeSelect(ctx, crdbSQL, tw)

	case *parser.WithStmt:
		return e.executeSelect(ctx, crdbSQL, tw)

	case *parser.BeginTranStmt:
		return e.executeBeginTran(ctx, tw)

	case *parser.CommitTranStmt:
		return e.executeCommitTran(ctx, tw)

	case *parser.RollbackTranStmt:
		return e.executeRollbackTran(ctx, s, tw)

	case *parser.SaveTranStmt:
		return e.executeDDL(ctx, crdbSQL, tw)

	case *parser.PrintStmt:
		return e.executePrint(s, tw)

	case *parser.RaiserrorStmt:
		return e.executeRaiserror(s, tw)

	case *parser.ThrowStmt:
		return e.executeThrow(s, tw)

	default:
		// Best-effort: try as DML.
		return e.executeDML(ctx, crdbSQL, tw)
	}
}

// executeBeginTran handles BEGIN TRAN by starting a CockroachDB KV
// transaction and incrementing the transaction counter. Subsequent
// DML/DDL/SELECT statements will execute within this transaction
// until COMMIT or ROLLBACK is issued.
func (e *Executor) executeBeginTran(ctx context.Context, tw *tdswire.TokenWriter) error {
	if e.activeKVTxn == nil {
		e.activeKVTxn = e.db.KV().NewTxn(ctx, "tds-txn")
	}
	e.tranCount++
	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
	})
}

// executeCommitTran handles COMMIT by committing the active KV
// transaction and decrementing the transaction counter.
func (e *Executor) executeCommitTran(ctx context.Context, tw *tdswire.TokenWriter) error {
	if e.tranCount > 0 {
		e.tranCount--
	}
	if e.tranCount == 0 && e.activeKVTxn != nil {
		if err := e.activeKVTxn.Commit(ctx); err != nil {
			e.activeKVTxn = nil
			return writeErrorToken(tw, 50000, 1, 16,
				fmt.Sprintf("COMMIT failed: %s", err))
		}
		e.activeKVTxn = nil
	}
	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
	})
}

// executeRollbackTran handles ROLLBACK. A plain ROLLBACK rolls back
// the active KV transaction and resets the transaction counter to 0.
// A ROLLBACK to a named savepoint leaves the counter unchanged
// (matching T-SQL semantics).
func (e *Executor) executeRollbackTran(
	ctx context.Context, stmt *parser.RollbackTranStmt, tw *tdswire.TokenWriter,
) error {
	if stmt.Name == "" {
		if e.activeKVTxn != nil {
			if err := e.activeKVTxn.Rollback(ctx); err != nil {
				e.activeKVTxn = nil
				e.tranCount = 0
				return writeErrorToken(tw, 50000, 1, 16,
					fmt.Sprintf("ROLLBACK failed: %s", err))
			}
			e.activeKVTxn = nil
		}
		e.tranCount = 0
	}
	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
	})
}

// executePrint handles PRINT <expr> by sending a TDS INFO token with
// the message text to the client.
func (e *Executor) executePrint(stmt *parser.PrintStmt, tw *tdswire.TokenWriter) error {
	msg := exprToString(stmt.Expr)
	if err := tw.WriteError(tdswire.ErrorToken{
		TokenType: tdswire.TokenInfo,
		Number:    0,
		State:     1,
		Class:     0, // informational
		Message:   msg,
		Server:    "CockroachDB",
	}); err != nil {
		return err
	}
	return writeDoneFinal(tw)
}

// executeRaiserror handles the Sybase ASE RAISERROR syntax by sending
// a TDS ERROR token with the specified error number and optional message.
func (e *Executor) executeRaiserror(stmt *parser.RaiserrorStmt, tw *tdswire.TokenWriter) error {
	msg := stmt.Message
	if msg == "" {
		msg = fmt.Sprintf("error %d", stmt.ErrNum)
	}
	return writeErrorToken(tw, int32(stmt.ErrNum), 1, 16, msg)
}

// executeThrow handles the SQL Server THROW syntax by sending a TDS ERROR
// token with the specified error number, message, and state.
func (e *Executor) executeThrow(stmt *parser.ThrowStmt, tw *tdswire.TokenWriter) error {
	return writeErrorToken(tw, int32(stmt.ErrNum), byte(stmt.State), 16, stmt.Message)
}

// executeBeginTryCatch executes the TRY body. If any statement in the TRY
// body produces an error, execution continues with the CATCH body.
func (e *Executor) executeBeginTryCatch(
	ctx context.Context, stmt *parser.BeginTryCatchStmt, tw *tdswire.TokenWriter,
) error {
	// Execute TRY body; on error, run CATCH body instead.
	for _, s := range stmt.TryBody {
		if err := e.execParsedStmt(ctx, s, tw); err != nil {
			// Error in TRY: execute CATCH body.
			for _, cs := range stmt.CatchBody {
				if catchErr := e.execParsedStmt(ctx, cs, tw); catchErr != nil {
					return catchErr
				}
			}
			return nil //nolint:returnerrcheck
		}
	}
	return nil
}

// exprToString extracts the string value from a parser expression.
// For string literals, it returns the unquoted value. For other
// expression types, it returns the String() representation.
func exprToString(expr parser.Expr) string {
	if lit, ok := expr.(*parser.StringLit); ok {
		return lit.Value
	}
	return expr.String()
}

// executeDDL executes a DDL statement (CREATE TABLE, etc.) and returns
// a DONE token with the row count.
func (e *Executor) executeDDL(ctx context.Context, sql string, tw *tdswire.TokenWriter) error {
	executor := e.db.Executor()
	rowCount, err := executor.ExecEx(
		ctx,
		redact.Sprint("tds-ddl"),
		e.currentKVTxn(),
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, err.Error())
	}

	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneCount,
		RowCount:  uint64(rowCount),
	})
}

// executeDML executes a DML statement (INSERT, UPDATE, DELETE) and
// returns a DONE token with the rows affected. It also stores the
// count for @@ROWCOUNT.
func (e *Executor) executeDML(ctx context.Context, sql string, tw *tdswire.TokenWriter) error {
	executor := e.db.Executor()
	rowCount, err := executor.ExecEx(
		ctx,
		redact.Sprint("tds-dml"),
		e.currentKVTxn(),
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, err.Error())
	}

	e.lastRowsAffected = rowCount

	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneCount,
		RowCount:  uint64(rowCount),
	})
}

// executeSelect executes a SELECT statement and writes COLMETADATA +
// ROW tokens + DONE to the token writer.
func (e *Executor) executeSelect(ctx context.Context, sql string, tw *tdswire.TokenWriter) error {
	executor := e.db.Executor()
	rows, resultCols, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("tds-select"),
		e.currentKVTxn(),
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, err.Error())
	}

	// Convert colinfo.ResultColumns to our internal representation.
	rcInfos := make([]resultColumnInfo, len(resultCols))
	for i, rc := range resultCols {
		rcInfos[i] = resultColumnInfo{Name: rc.Name, Typ: rc.Typ}
	}

	// Map CRDB ResultColumns to TDS COLMETADATA.
	md, typeInfos, err := mapResultColumns(rcInfos)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16,
			fmt.Sprintf("type mapping error: %s", err))
	}

	if err := tw.WriteColMetaData(md); err != nil {
		return err
	}

	// Write ROW tokens.
	for _, datums := range rows {
		row, err := mapDatumsToRow(datums, typeInfos)
		if err != nil {
			return err
		}
		if err := tw.WriteRow(md, row); err != nil {
			return err
		}
	}

	// DONE with row count.
	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneCount,
		RowCount:  uint64(len(rows)),
	})
}

// executeSelectWithCompute handles a SELECT with COMPUTE clauses. The
// translated SQL contains the base SELECT and one aggregate query per
// COMPUTE clause, separated by semicolons. The base query is executed
// first, followed by each aggregate query, producing multiple result
// sets on the TDS stream.
func (e *Executor) executeSelectWithCompute(
	ctx context.Context, stmt *parser.SelectStmt, crdbSQL string, tw *tdswire.TokenWriter,
) error {
	parts := strings.Split(crdbSQL, ";\n")

	// Execute the base SELECT (first part).
	baseSQL := e.substituteRowCount(e.substituteTranCount(parts[0]))
	if err := e.executeSelectNonFinal(ctx, baseSQL, tw); err != nil {
		return err
	}

	// Execute each COMPUTE aggregate query.
	for i := 1; i < len(parts); i++ {
		aggSQL := e.substituteRowCount(e.substituteTranCount(parts[i]))
		if i == len(parts)-1 {
			// Last result set gets DONE(FINAL).
			if err := e.executeSelect(ctx, aggSQL, tw); err != nil {
				return err
			}
		} else {
			if err := e.executeSelectNonFinal(ctx, aggSQL, tw); err != nil {
				return err
			}
		}
	}

	return nil
}

// executeSelectNonFinal executes a SELECT and writes COLMETADATA + ROW
// tokens + DONE (without FINAL flag) so additional result sets can
// follow on the same TDS stream.
func (e *Executor) executeSelectNonFinal(
	ctx context.Context, sql string, tw *tdswire.TokenWriter,
) error {
	executor := e.db.Executor()
	rows, resultCols, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("tds-select-compute"),
		e.currentKVTxn(),
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, err.Error())
	}

	rcInfos := make([]resultColumnInfo, len(resultCols))
	for i, rc := range resultCols {
		rcInfos[i] = resultColumnInfo{Name: rc.Name, Typ: rc.Typ}
	}

	md, typeInfos, err := mapResultColumns(rcInfos)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16,
			fmt.Sprintf("type mapping error: %s", err))
	}

	if err := tw.WriteColMetaData(md); err != nil {
		return err
	}

	for _, datums := range rows {
		row, err := mapDatumsToRow(datums, typeInfos)
		if err != nil {
			return err
		}
		if err := tw.WriteRow(md, row); err != nil {
			return err
		}
	}

	// DONE without FINAL — more result sets follow.
	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneCount,
		RowCount:  uint64(len(rows)),
	})
}

// mapResultColumns converts CRDB colinfo.ResultColumns to TDS
// ColMetaData and returns the TypeInfo for each column for value
// encoding.
func mapResultColumns(cols []resultColumnInfo) (tdswire.ColMetaData, []tdstypes.TypeInfo, error) {
	md := tdswire.ColMetaData{
		Columns: make([]tdswire.Column, len(cols)),
	}
	typeInfos := make([]tdstypes.TypeInfo, len(cols))

	for i, col := range cols {
		ti, err := tdstypes.CRDBToTDS(col.Typ)
		if err != nil {
			// Fallback: use NVARCHAR for unsupported types.
			ti = tdstypes.TypeInfo{
				TDSType:   tdstypes.NVarCharType,
				CRDBType:  types.String,
				MaxLength: 8000,
			}
		}
		typeInfos[i] = ti

		md.Columns[i] = tdswire.Column{
			ColName: col.Name,
			TypeInfo: tdswire.TypeInfo{
				TypeID: byte(ti.TDSType),
			},
		}

		// Set the appropriate length fields based on type category.
		wireTypeID := byte(ti.TDSType)
		if isFixedLenTDSType(wireTypeID) {
			// Fixed-length types need no extra metadata.
		} else if isByteLenTDSType(wireTypeID) {
			md.Columns[i].TypeInfo.ByteLen = byte(ti.MaxLength)
		} else if isVarLenTDSType(wireTypeID) {
			md.Columns[i].TypeInfo.MaxLen = uint16(ti.MaxLength)
		} else if isPrecScaleTDSType(wireTypeID) {
			md.Columns[i].TypeInfo.ByteLen = byte(ti.MaxLength)
			md.Columns[i].TypeInfo.Precision = ti.Precision
			md.Columns[i].TypeInfo.Scale = ti.Scale
		}
	}

	return md, typeInfos, nil
}

// resultColumnInfo is a minimal interface matching colinfo.ResultColumn
// fields we need. This allows us to work with both the real type and
// test mocks.
type resultColumnInfo struct {
	Name string
	Typ  *types.T
}

// mapDatumsToRow converts a row of tree.Datums to a TDS Row using the
// given TypeInfos for encoding.
func mapDatumsToRow(datums tree.Datums, typeInfos []tdstypes.TypeInfo) (tdswire.Row, error) {
	row := tdswire.Row{
		Values: make([][]byte, len(datums)),
	}

	for i, d := range datums {
		if d == tree.DNull {
			row.Values[i] = nil
			continue
		}

		goVal, err := datumToGoValue(d)
		if err != nil {
			return row, fmt.Errorf("column %d: %w", i, err)
		}

		var encoded []byte
		encoded, err = tdstypes.EncodeValue(nil, typeInfos[i], goVal)
		if err != nil {
			return row, fmt.Errorf("column %d encode error: %w", i, err)
		}
		row.Values[i] = encoded
	}

	return row, nil
}

// datumToGoValue converts a tree.Datum to a Go value suitable for
// tdstypes.EncodeValue.
func datumToGoValue(d tree.Datum) (interface{}, error) {
	switch v := d.(type) {
	case *tree.DBool:
		return bool(*v), nil
	case *tree.DInt:
		return int64(*v), nil
	case *tree.DFloat:
		return float64(*v), nil
	case *tree.DDecimal:
		return &v.Decimal, nil
	case *tree.DString:
		return string(*v), nil
	case *tree.DBytes:
		return []byte(*v), nil
	case *tree.DDate:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return t, nil
	case *tree.DTime:
		return timeofday.TimeOfDay(*v).ToTime(), nil
	case *tree.DTimestamp:
		return v.Time, nil
	case *tree.DTimestampTZ:
		return v.Time, nil
	case *tree.DUuid:
		return v.UUID, nil
	default:
		// Fallback: convert to string.
		return tree.AsStringWithFlags(d, tree.FmtSimple), nil
	}
}

// writeDoneFinal writes a DONE(FINAL) token with no row count.
func writeDoneFinal(tw *tdswire.TokenWriter) error {
	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
	})
}

// writeErrorToken writes an ERROR token followed by a DONE(FINAL|ERROR)
// token. It does not return an error for the original query error; the
// error is communicated to the TDS client via the token stream.
func writeErrorToken(
	tw *tdswire.TokenWriter, number int32, state uint8, class uint8, message string,
) error {
	if err := tw.WriteError(tdswire.ErrorToken{
		TokenType: tdswire.TokenError,
		Number:    number,
		State:     state,
		Class:     class,
		Message:   message,
		Server:    "CockroachDB",
	}); err != nil {
		return err
	}

	return tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneError,
	})
}

// IsFixedLenType checks if a TDS type ID is fixed-length. This wraps
// the tdswire package's fixedTypeLen which is unexported.
func isFixedLenTDSType(typeID byte) bool {
	switch typeID {
	case tdswire.TypeInt1, tdswire.TypeBit:
		return true
	case tdswire.TypeInt2:
		return true
	case tdswire.TypeInt4, tdswire.TypeFloat4:
		return true
	case tdswire.TypeFloat8, tdswire.TypeInt8, tdswire.TypeDateTime:
		return true
	default:
		return false
	}
}

// isByteLenTDSType checks if a TDS type ID uses a 1-byte length prefix.
func isByteLenTDSType(typeID byte) bool {
	switch typeID {
	case tdswire.TypeIntN, tdswire.TypeBitN, tdswire.TypeFloatN,
		tdswire.TypeDateTimeN, tdswire.TypeMoneyN, tdswire.TypeGuid,
		tdswire.TypeDateTimeOffN:
		return true
	default:
		return false
	}
}

// isVarLenTDSType checks if a TDS type ID uses a 2-byte length prefix.
func isVarLenTDSType(typeID byte) bool {
	switch typeID {
	case tdswire.TypeBigVarChar, tdswire.TypeBigChar, tdswire.TypeBigVarBin,
		tdswire.TypeNVarChar, tdswire.TypeNChar, tdswire.TypeBigBinary:
		return true
	default:
		return false
	}
}

// isPrecScaleTDSType checks if a TDS type ID requires precision/scale.
func isPrecScaleTDSType(typeID byte) bool {
	return typeID == tdswire.TypeDecimalN || typeID == tdswire.TypeNumericN
}

// Sentinel errors for BREAK and CONTINUE control flow signals.
var (
	errBreak    = fmt.Errorf("BREAK")
	errContinue = fmt.Errorf("CONTINUE")
)

func isControlFlowSignal(err error) bool {
	return errors.Is(err, errBreak) || errors.Is(err, errContinue)
}

// execParsedStmt translates and executes a single parsed T-SQL
// statement. Control flow statements (DECLARE, SET @var, IF, WHILE,
// BEGIN...END) are interpreted directly; others go through the
// standard translate → execute path.
func (e *Executor) execParsedStmt(
	ctx context.Context, stmt parser.Statement, tw *tdswire.TokenWriter,
) error {
	switch s := stmt.(type) {
	case *parser.DeclareVarStmt:
		return e.executeDeclare(ctx, s, tw)
	case *parser.SetVarStmt:
		return e.executeSetVar(ctx, s, tw)
	case *parser.IfStmt:
		return e.executeIf(ctx, s, tw)
	case *parser.WhileStmt:
		return e.executeWhile(ctx, s, tw)
	case *parser.BeginEndBlock:
		return e.executeBeginEndBlock(ctx, s, tw)
	case *parser.BreakStmt:
		return errBreak
	case *parser.ContinueStmt:
		return errContinue
	case *parser.PrintStmt:
		// PRINT is silently acknowledged (CockroachDB has no print channel).
		return nil
	case *parser.ExecStmt:
		return writeErrorToken(tw, 2812, 1, 16,
			fmt.Sprintf("unsupported: stored procedure '%s' is not available in CockroachDB TDS", s.Procedure))
	case *parser.ThrowStmt:
		return e.executeThrow(s, tw)
	case *parser.GotoStmt:
		// GOTO is silently acknowledged (label-based flow not supported).
		return nil
	case *parser.ReturnStmt:
		// RETURN is silently acknowledged (stored procedure context only).
		return nil
	case *parser.WaitforStmt:
		// WAITFOR is silently acknowledged (delay/scheduling not supported).
		return nil
	case *parser.BeginTryCatchStmt:
		return e.executeBeginTryCatch(ctx, s, tw)
	default:
		// Regular statement: translate then execute.
		crdbSQL, err := translate.Statement(stmt)
		if err != nil {
			return writeErrorToken(tw, 50000, 1, 16,
				fmt.Sprintf("T-SQL translation error: %s", err))
		}
		crdbSQL = e.substituteTranCount(crdbSQL)
		crdbSQL = e.substituteRowCount(crdbSQL)
		crdbSQL = e.substituteVars(crdbSQL)
		return e.executeStatement(ctx, stmt, crdbSQL, tw)
	}
}

// executeDeclare handles DECLARE @var TYPE [= expr].
func (e *Executor) executeDeclare(
	ctx context.Context, s *parser.DeclareVarStmt, tw *tdswire.TokenWriter,
) error {
	if e.variables == nil {
		e.variables = make(map[string]string)
	}
	if s.Default != nil {
		val, err := e.evaluateExpr(ctx, s.Default)
		if err != nil {
			return writeErrorToken(tw, 50000, 1, 16,
				fmt.Sprintf("error evaluating default for %s: %s", s.Name, err))
		}
		e.variables[s.Name] = val
	} else {
		e.variables[s.Name] = "NULL"
	}
	return nil
}

// executeSetVar handles SET @var = expr.
func (e *Executor) executeSetVar(
	ctx context.Context, s *parser.SetVarStmt, tw *tdswire.TokenWriter,
) error {
	if e.variables == nil {
		e.variables = make(map[string]string)
	}
	val, err := e.evaluateExpr(ctx, s.Expr)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16,
			fmt.Sprintf("error evaluating SET %s: %s", s.Name, err))
	}
	e.variables[s.Name] = val
	return nil
}

// executeIf handles IF condition body [ELSE elseBody].
func (e *Executor) executeIf(ctx context.Context, s *parser.IfStmt, tw *tdswire.TokenWriter) error {
	result, err := e.evaluateCondition(ctx, s.Condition)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16,
			fmt.Sprintf("error evaluating IF condition: %s", err))
	}
	if result {
		return e.execParsedStmt(ctx, s.Body, tw)
	}
	if s.ElseBody != nil {
		return e.execParsedStmt(ctx, s.ElseBody, tw)
	}
	return nil
}

// executeWhile handles WHILE condition body.
func (e *Executor) executeWhile(
	ctx context.Context, s *parser.WhileStmt, tw *tdswire.TokenWriter,
) error {
	const maxIterations = 10000
	for i := 0; i < maxIterations; i++ {
		result, err := e.evaluateCondition(ctx, s.Condition)
		if err != nil {
			return writeErrorToken(tw, 50000, 1, 16,
				fmt.Sprintf("error evaluating WHILE condition: %s", err))
		}
		if !result {
			return nil
		}
		if err := e.execParsedStmt(ctx, s.Body, tw); err != nil {
			if errors.Is(err, errBreak) {
				return nil
			}
			if errors.Is(err, errContinue) {
				continue
			}
			return err
		}
	}
	return writeErrorToken(tw, 50000, 1, 16,
		"WHILE loop exceeded maximum iterations (10000)")
}

// executeBeginEndBlock handles BEGIN...END statement blocks.
func (e *Executor) executeBeginEndBlock(
	ctx context.Context, s *parser.BeginEndBlock, tw *tdswire.TokenWriter,
) error {
	for _, stmt := range s.Stmts {
		if err := e.execParsedStmt(ctx, stmt, tw); err != nil {
			return err
		}
	}
	return nil
}

// evaluateExpr evaluates a T-SQL expression by translating it to CRDB
// SQL and executing SELECT <expr>. Returns the result as a SQL literal
// string.
func (e *Executor) evaluateExpr(ctx context.Context, expr parser.Expr) (string, error) {
	crdbExpr := translate.Expr(expr)
	crdbExpr = e.substituteVars(crdbExpr)
	sql := fmt.Sprintf("SELECT (%s)", crdbExpr)

	executor := e.db.Executor()
	rows, _, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("tds-eval-expr"),
		e.currentKVTxn(),
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "NULL", nil
	}
	d := rows[0][0]
	if d == tree.DNull {
		return "NULL", nil
	}
	return tree.AsStringWithFlags(d, tree.FmtSimple), nil
}

// evaluateCondition evaluates a T-SQL boolean expression. Returns true
// if the expression evaluates to a truthy value.
func (e *Executor) evaluateCondition(ctx context.Context, expr parser.Expr) (bool, error) {
	crdbExpr := translate.Expr(expr)
	crdbExpr = e.substituteVars(crdbExpr)
	sql := fmt.Sprintf("SELECT CASE WHEN (%s) THEN true ELSE false END", crdbExpr)

	executor := e.db.Executor()
	rows, _, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("tds-eval-cond"),
		e.currentKVTxn(),
		e.executorOverride(),
		sql,
	)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return false, nil
	}
	d := rows[0][0]
	if b, ok := d.(*tree.DBool); ok {
		return bool(*b), nil
	}
	return false, nil
}

// substituteVars replaces @variable references in SQL strings with
// their current literal values from the executor's variable map.
func (e *Executor) substituteVars(sql string) string {
	if len(e.variables) == 0 {
		return sql
	}
	for name, val := range e.variables {
		sql = strings.ReplaceAll(sql, name, val)
	}
	return sql
}

// ExecuteBatchToBytes is a convenience method that executes a SQL batch
// and returns the complete TDS token stream as bytes. This is used by
// the connection handler.
func (e *Executor) ExecuteBatchToBytes(ctx context.Context, sqlBatch string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := e.ExecuteBatch(ctx, sqlBatch, tw); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
