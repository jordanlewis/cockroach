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
	"github.com/cockroachdb/redact"
)

// Executor bridges the CockroachDB internal SQL executor (isql.DB) and
// the TDS token stream. Each connection gets its own Executor instance
// to track per-connection state such as the current database and the
// last row count for @@ROWCOUNT.
type Executor struct {
	db isql.DB

	// Per-connection state.
	currentDatabase  string
	lastRowsAffected int
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
	if len(trimmed) >= 4 && strings.EqualFold(trimmed[:4], "SET ") {
		return writeDoneFinal(tw)
	}

	// Parse T-SQL.
	batch, err := parser.Parse(trimmed)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, fmt.Sprintf("T-SQL parse error: %s", err))
	}

	// Translate to CRDB SQL.
	crdbStatements, err := translate.Batch(batch)
	if err != nil {
		return writeErrorToken(tw, 50000, 1, 16, fmt.Sprintf("T-SQL translation error: %s", err))
	}

	// Execute each translated statement.
	for i, crdbSQL := range crdbStatements {
		stmt := batch.Stmts[i]
		if err := e.executeStatement(ctx, stmt, crdbSQL, tw); err != nil {
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
	switch stmt.(type) {
	case *parser.UseStmt:
		// USE was translated to SET database = '...'; handle specially.
		useStmt := stmt.(*parser.UseStmt)
		return e.handleUseDatabase(useStmt.Database, tw)

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
		return e.executeSelect(ctx, crdbSQL, tw)

	default:
		// Best-effort: try as DML.
		return e.executeDML(ctx, crdbSQL, tw)
	}
}

// executeDDL executes a DDL statement (CREATE TABLE, etc.) and returns
// a DONE token with the row count.
func (e *Executor) executeDDL(ctx context.Context, sql string, tw *tdswire.TokenWriter) error {
	executor := e.db.Executor()
	rowCount, err := executor.ExecEx(
		ctx,
		redact.Sprint("tds-ddl"),
		nil, // txn
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
		nil, // txn
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
		nil, // txn
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
