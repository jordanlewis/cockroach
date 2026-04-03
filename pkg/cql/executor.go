// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/cockroachdb/cockroach/pkg/cql/translate"
	cqltypes "github.com/cockroachdb/cockroach/pkg/cql/types"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CQL RESULT kind constants from the CQL native protocol v4 spec,
// section 4.2.5.
const (
	resultKindVoid         int32 = 0x0001
	resultKindRows         int32 = 0x0002
	resultKindSetKeyspace  int32 = 0x0003
	resultKindPrepared     int32 = 0x0004
	resultKindSchemaChange int32 = 0x0005
)

// CQL error codes for query execution errors.
const (
	errCodeInvalid int32 = 0x2200
	errCodeSyntax  int32 = 0x2000
)

// ExecuteResult holds the result of executing a CQL query.
type ExecuteResult struct {
	// Body is the CQL RESULT or ERROR frame body bytes.
	Body []byte
	// IsError is true when Body contains an ERROR frame body rather
	// than a RESULT frame body.
	IsError bool
	// NewKeyspace is set when a USE statement changes the keyspace.
	NewKeyspace string
}

// Executor bridges CQL query processing with CockroachDB's internal
// SQL executor. It parses CQL queries, translates them to SQL, executes
// them via isql.DB, and encodes the results as CQL wire protocol frames.
type Executor struct {
	db     isql.DB
	schema *translate.SchemaInfo
}

// NewExecutor creates a new Executor backed by the given isql.DB.
func NewExecutor(db isql.DB) *Executor {
	return &Executor{db: db, schema: translate.NewSchemaInfo()}
}

// ExecuteQuery parses and executes a CQL query, returning an
// ExecuteResult containing the CQL RESULT or ERROR frame body. The
// keyspace parameter provides the current database context (set via
// USE). The user parameter identifies the authenticated CQL user
// whose privileges govern the query.
func (e *Executor) ExecuteQuery(
	ctx context.Context, cqlQuery string, keyspace string, user username.SQLUsername,
) ExecuteResult {
	// Parse the CQL query.
	stmt, err := parser.Parse(cqlQuery)
	if err != nil {
		return errorResult(errCodeSyntax, err.Error())
	}

	// Handle USE specially: just update connection state.
	if useStmt, ok := stmt.(*parser.UseStatement); ok {
		return ExecuteResult{
			Body:        buildSetKeyspaceBody(useStmt.Keyspace),
			NewKeyspace: useStmt.Keyspace,
		}
	}

	// Compute override early so it is available for projected system
	// table queries that need to execute SQL.
	override := sessiondata.InternalExecutorOverride{
		User: user,
	}
	if keyspace != "" {
		override.Database = keyspace
	}

	// Intercept system and system_schema queries. cqlsh and other CQL
	// drivers query system.local, system.peers, and system_schema.*
	// tables during startup. We return synthetic results for these
	// since CRDB does not have Cassandra system tables.
	//
	// For non-star SELECTs on system.local/peers, handleSystemSelect
	// returns false so we can translate the query with a synthetic
	// FROM subquery and execute it through the SQL engine, giving us
	// proper column projection and expression evaluation.
	if sel, ok := stmt.(*parser.SelectStatement); ok {
		if res, handled := handleSystemSelect(
			ctx, e.db, sel.Keyspace, sel.Table, sel.Where, sel.Columns,
		); handled {
			return res
		}
		// Non-star select on a system table: translate the SELECT
		// list with a FROM subquery that provides the synthetic row
		// data, then execute through the SQL engine.
		if fromSQL, ok := systemTableFromSQL(sel.Keyspace, sel.Table); ok {
			result, err := translate.TranslateSelectWithFrom(sel.Columns, fromSQL)
			if err != nil {
				return errorResult(errCodeInvalid, err.Error())
			}
			return e.executeSelect(ctx, result, override)
		}
	}

	// Translate the CQL AST to SQL.
	result, err := translate.TranslateWithSchema(stmt, e.schema)
	if err != nil {
		return errorResult(errCodeInvalid, err.Error())
	}

	switch s := stmt.(type) {
	case *parser.CreateKeyspaceStatement:
		return e.executeDDL(ctx, result, override, "CREATED", "KEYSPACE", s.Keyspace, "")
	case *parser.CreateIndexStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		return e.executeDDL(ctx, result, override, "UPDATED", "TABLE", ks, s.Table)
	case *parser.CreateTableStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		ddlResult := e.executeDDL(ctx, result, override, "CREATED", "TABLE", ks, s.Table)
		if !ddlResult.IsError {
			e.recordTableSchema(s, ks)
		}
		return ddlResult
	case *parser.CreateTypeStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		return e.executeDDL(ctx, result, override, "CREATED", "TYPE", ks, s.TypeName)
	case *parser.AlterTypeStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		return e.executeDDL(ctx, result, override, "UPDATED", "TYPE", ks, s.TypeName)
	case *parser.InsertStatement:
		if s.IfNotExists {
			ks := s.Keyspace
			if ks == "" {
				ks = keyspace
			}
			return e.executeInsertIfNotExists(ctx, s, result, override, ks)
		}
		return e.executeDML(ctx, result, override)
	case *parser.UpdateStatement:
		return e.executeDML(ctx, result, override)
	case *parser.DeleteStatement:
		return e.executeDML(ctx, result, override)
	case *parser.SelectStatement:
		return e.executeSelect(ctx, result, override)
	case *parser.AlterTableStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		if result.SQL == "" {
			// No-op translation (e.g. ALTER TABLE WITH properties).
			return ExecuteResult{
				Body: buildSchemaChangeBody("UPDATED", "TABLE", ks, s.Table),
			}
		}
		return e.executeDDL(ctx, result, override, "UPDATED", "TABLE", ks, s.Table)
	case *parser.AlterKeyspaceStatement:
		if result.SQL == "" {
			// No-op translation (ALTER KEYSPACE properties).
			return ExecuteResult{
				Body: buildSchemaChangeBody("UPDATED", "KEYSPACE", s.Keyspace, ""),
			}
		}
		return e.executeDDL(ctx, result, override, "UPDATED", "KEYSPACE", s.Keyspace, "")
	case *parser.DropStatement:
		switch s.ObjectType {
		case "KEYSPACE":
			return e.executeDDL(ctx, result, override, "DROPPED", "KEYSPACE", s.Name, "")
		case "TABLE":
			ks := s.Keyspace
			if ks == "" {
				ks = keyspace
			}
			return e.executeDDL(ctx, result, override, "DROPPED", "TABLE", ks, s.Name)
		case "INDEX":
			return e.executeDDL(ctx, result, override, "DROPPED", "TABLE", keyspace, s.Name)
		default:
			return errorResult(errCodeServerError, "unsupported DROP target")
		}
	case *parser.TruncateStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		return e.executeDDL(ctx, result, override, "UPDATED", "TABLE", ks, s.Table)
	case *parser.BatchStatement:
		return e.executeBatch(ctx, s, override)
	default:
		return errorResult(errCodeServerError, "unsupported statement type")
	}
}

// recordTableSchema extracts partition key, column, and static column
// metadata from a CREATE TABLE statement and stores it in the executor's
// schema tracker. This enables PER PARTITION LIMIT translations on
// subsequent SELECTs and static column propagation on writes.
func (e *Executor) recordTableSchema(s *parser.CreateTableStatement, keyspace string) {
	cols := make([]string, len(s.Columns))
	var staticCols map[string]bool
	for i, col := range s.Columns {
		cols[i] = col.Name
		if col.IsStatic {
			if staticCols == nil {
				staticCols = make(map[string]bool)
			}
			staticCols[strings.ToLower(col.Name)] = true
		}
	}
	e.schema.RecordTable(keyspace, s.Table, translate.TableMeta{
		PartitionKeys:  s.PrimaryKey.PartitionKeys,
		ClusteringKeys: s.PrimaryKey.ClusteringKeys,
		Columns:        cols,
		StaticColumns:  staticCols,
	})
}

// executeDDL executes a DDL statement and returns a SchemaChange
// RESULT. The keyspace parameter is the keyspace name for the schema
// change response; name is the object name (empty for KEYSPACE
// targets).
func (e *Executor) executeDDL(
	ctx context.Context,
	result translate.Result,
	override sessiondata.InternalExecutorOverride,
	changeType, target, keyspace, name string,
) ExecuteResult {
	executor := e.db.Executor()
	_, err := executor.ExecEx(
		ctx,
		redact.Sprint("cql-ddl"),
		nil, // txn
		override,
		result.SQL,
		result.Params...,
	)
	if err != nil {
		// Distinguish context cancellation (client disconnect) from
		// real DDL errors. When the client disconnects during a
		// schema change, the context is cancelled and ExecEx returns
		// a context error. The schema change may have already been
		// committed. Return an error result so the caller does not
		// attempt to write to the closed connection.
		if ctx.Err() != nil {
			return errorResult(errCodeServerError, "query cancelled: "+ctx.Err().Error())
		}
		return errorResult(errCodeServerError, err.Error())
	}
	return ExecuteResult{
		Body: buildSchemaChangeBody(changeType, target, keyspace, name),
	}
}

// executeDML executes a DML statement (INSERT/UPSERT/UPDATE/DELETE) and
// returns a Void RESULT. If the translation includes a static column
// propagation UPDATE, it is executed after the main statement to
// synchronize static values across all rows in the partition.
func (e *Executor) executeDML(
	ctx context.Context, result translate.Result, override sessiondata.InternalExecutorOverride,
) ExecuteResult {
	executor := e.db.Executor()
	_, err := executor.ExecEx(
		ctx,
		redact.Sprint("cql-dml"),
		nil, // txn
		override,
		result.SQL,
		result.Params...,
	)
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}

	// Propagate static column values across the partition if needed.
	if result.PropagateStaticSQL != "" {
		_, err := executor.ExecEx(
			ctx,
			redact.Sprint("cql-static-propagation"),
			nil, // txn
			override,
			result.PropagateStaticSQL,
			result.PropagateStaticParams...,
		)
		if err != nil {
			return errorResult(errCodeServerError, err.Error())
		}
	}

	return ExecuteResult{
		Body: buildVoidBody(),
	}
}

// executeInsertIfNotExists handles INSERT IF NOT EXISTS with Cassandra's
// lightweight transaction (LWT) semantics. The translated SQL uses
// ON CONFLICT DO NOTHING so duplicates are silently skipped instead of
// raising a constraint violation. The result is a rows result set
// containing an [applied] boolean column:
//   - [applied]=true when the insert succeeded (new row)
//   - [applied]=false plus the existing row when a duplicate was found
func (e *Executor) executeInsertIfNotExists(
	ctx context.Context,
	stmt *parser.InsertStatement,
	result translate.Result,
	override sessiondata.InternalExecutorOverride,
	keyspace string,
) ExecuteResult {
	executor := e.db.Executor()
	rowsAffected, err := executor.ExecEx(
		ctx,
		redact.Sprint("cql-dml-lwt"),
		nil, // txn
		override,
		result.SQL,
		result.Params...,
	)
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}

	// Build the SELECT query for the [applied] result set.
	var querySQL string
	var queryParams []interface{}

	if rowsAffected > 0 {
		// Insert succeeded → [applied]=true.
		querySQL = `SELECT true AS "[applied]"`

		// Propagate static column values if the insert succeeded.
		if result.PropagateStaticSQL != "" {
			_, propErr := executor.ExecEx(
				ctx,
				redact.Sprint("cql-static-propagation"),
				nil, // txn
				override,
				result.PropagateStaticSQL,
				result.PropagateStaticParams...,
			)
			if propErr != nil {
				return errorResult(errCodeServerError, propErr.Error())
			}
		}
	} else {
		// Duplicate found → [applied]=false + existing row.
		meta, ok := e.schema.LookupTable(keyspace, stmt.Table)
		if !ok {
			querySQL = `SELECT false AS "[applied]"`
		} else {
			querySQL, queryParams = translate.BuildLWTExistingRowQuery(
				stmt, meta, keyspace,
			)
		}
	}

	rows, cols, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("cql-lwt"),
		nil, // txn
		override,
		querySQL,
		queryParams...,
	)
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}

	body, err := buildRowsBody(cols, rows)
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}
	return ExecuteResult{Body: body}
}

// executeSelect executes a SELECT and returns a Rows RESULT with the
// result data encoded in CQL wire format.
func (e *Executor) executeSelect(
	ctx context.Context, result translate.Result, override sessiondata.InternalExecutorOverride,
) ExecuteResult {
	executor := e.db.Executor()
	rows, cols, err := executor.QueryBufferedExWithCols(
		ctx,
		redact.Sprint("cql-query"),
		nil, // txn
		override,
		result.SQL,
		result.Params...,
	)
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}

	body, err := buildRowsBody(cols, rows)
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}
	return ExecuteResult{Body: body}
}

// executeBatch executes a CQL BATCH by running each inner statement
// in a transaction using the isql.DB.Txn callback pattern.
func (e *Executor) executeBatch(
	ctx context.Context, batch *parser.BatchStatement, override sessiondata.InternalExecutorOverride,
) ExecuteResult {
	err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, innerStmt := range batch.Statements {
			result, err := translate.TranslateWithSchema(innerStmt, e.schema)
			if err != nil {
				return err
			}
			_, err = txn.ExecEx(
				ctx,
				redact.Sprint("cql-batch"),
				txn.KV(),
				override,
				result.SQL,
				result.Params...,
			)
			if err != nil {
				return err
			}

			// Propagate static column values within the batch transaction.
			if result.PropagateStaticSQL != "" {
				_, err = txn.ExecEx(
					ctx,
					redact.Sprint("cql-batch-static-propagation"),
					txn.KV(),
					override,
					result.PropagateStaticSQL,
					result.PropagateStaticParams...,
				)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return errorResult(errCodeServerError, err.Error())
	}
	return ExecuteResult{Body: buildVoidBody()}
}

// resultMetadataFlagNoMetadata indicates that the result metadata is
// not present and will be supplied at execution time (in the ROWS
// response). Used in PREPARED results so clients defer metadata
// parsing until EXECUTE.
const resultMetadataFlagNoMetadata int32 = 0x0004

// buildPreparedBody builds a CQL RESULT frame body with Prepared kind
// (section 4.2.5.4). The bind variable metadata describes each `?`
// placeholder as varchar so that clients encode bound values as UTF-8
// strings. Result metadata uses the No_metadata flag, deferring column
// metadata to the ROWS response sent after EXECUTE.
func buildPreparedBody(preparedID []byte, bindCount int) []byte {
	var buf bytes.Buffer

	// RESULT kind: Prepared.
	_ = cqlwire.WriteInt(&buf, resultKindPrepared)

	// Prepared statement ID: [short bytes].
	_ = cqlwire.WriteShortBytes(&buf, preparedID)

	// Bind variables metadata.
	_ = cqlwire.WriteInt(&buf, 0)                // flags: no global table spec
	_ = cqlwire.WriteInt(&buf, int32(bindCount)) // columns_count
	_ = cqlwire.WriteInt(&buf, 0)                // pk_count (v4)
	for i := 0; i < bindCount; i++ {
		_ = cqlwire.WriteString(&buf, "")                           // keyspace
		_ = cqlwire.WriteString(&buf, "")                           // table
		_ = cqlwire.WriteString(&buf, fmt.Sprintf("column%d", i+1)) // name
		_ = cqlwire.WriteShort(&buf, uint16(cqltypes.CQLVarchar))   // type
	}

	// Result metadata: No_metadata flag, 0 columns.
	_ = cqlwire.WriteInt(&buf, resultMetadataFlagNoMetadata)
	_ = cqlwire.WriteInt(&buf, 0) // columns_count

	return buf.Bytes()
}

// buildVoidBody builds a CQL RESULT frame body with Void kind.
func buildVoidBody() []byte {
	var buf bytes.Buffer
	_ = cqlwire.WriteInt(&buf, resultKindVoid)
	return buf.Bytes()
}

// buildSetKeyspaceBody builds a CQL RESULT frame body with
// SetKeyspace kind.
func buildSetKeyspaceBody(keyspace string) []byte {
	var buf bytes.Buffer
	_ = cqlwire.WriteInt(&buf, resultKindSetKeyspace)
	_ = cqlwire.WriteString(&buf, keyspace)
	return buf.Bytes()
}

// buildSchemaChangeBody builds a CQL RESULT frame body with
// SchemaChange kind. For KEYSPACE targets, only the keyspace name is
// written. For TABLE or TYPE targets, both keyspace and object name
// are written as required by the CQL native protocol v4 spec
// (section 4.2.5.5).
func buildSchemaChangeBody(changeType, target, keyspace, name string) []byte {
	var buf bytes.Buffer
	_ = cqlwire.WriteInt(&buf, resultKindSchemaChange)
	_ = cqlwire.WriteString(&buf, changeType)
	_ = cqlwire.WriteString(&buf, target)
	_ = cqlwire.WriteString(&buf, keyspace)
	if target != "KEYSPACE" {
		_ = cqlwire.WriteString(&buf, name)
	}
	return buf.Bytes()
}

// buildRowsBody builds a CQL RESULT Rows frame body from
// colinfo.ResultColumns and rows of tree.Datums. The metadata includes
// column names and CQL types; the data section encodes each datum in
// CQL binary format.
func buildRowsBody(cols colinfo.ResultColumns, rows []tree.Datums) ([]byte, error) {
	numCols := len(cols)

	// Map CRDB column types to CQL types.
	cqlTypes := make([]cqltypes.CQLType, numCols)
	for i, col := range cols {
		ct, err := cqltypes.CQLTypeFromCRDB(col.Typ)
		if err != nil {
			return nil, errors.Wrapf(err, "mapping column %q", col.Name)
		}
		cqlTypes[i] = ct
	}

	var buf bytes.Buffer

	// RESULT kind: Rows.
	_ = cqlwire.WriteInt(&buf, resultKindRows)

	// Metadata flags: no global table spec for simplicity.
	_ = cqlwire.WriteInt(&buf, 0) // flags
	// Column count.
	_ = cqlwire.WriteInt(&buf, int32(numCols))

	// Per-column metadata: [ksname][tablename][name][type].
	// Without global table spec, each column has its own ks/table.
	// We use empty strings since this is an internal executor result.
	for i, col := range cols {
		_ = cqlwire.WriteString(&buf, "") // keyspace
		_ = cqlwire.WriteString(&buf, "") // table
		_ = cqlwire.WriteString(&buf, col.Name)
		// Option ID: [short] type id.
		_ = cqlwire.WriteShort(&buf, uint16(cqlTypes[i]))
	}

	// Row count.
	_ = cqlwire.WriteInt(&buf, int32(len(rows)))

	// Row data: for each row, for each column, encode as CQL [bytes].
	for _, row := range rows {
		for j, datum := range row {
			val, isNull, err := cqltypes.EncodeDatum(datum, cqlTypes[j])
			if err != nil {
				return nil, errors.Wrapf(err, "encoding row value for column %q", cols[j].Name)
			}
			if isNull {
				// CQL NULL: length = -1.
				_ = cqlwire.WriteInt(&buf, -1)
			} else {
				_ = cqlwire.WriteInt(&buf, int32(len(val)))
				_, _ = buf.Write(val)
			}
		}
	}

	return buf.Bytes(), nil
}

// buildErrorBody builds a CQL ERROR frame body from an error code and
// message.
func buildErrorBody(code int32, msg string) []byte {
	var buf bytes.Buffer
	_ = cqlwire.WriteInt(&buf, code)
	_ = cqlwire.WriteString(&buf, msg)
	return buf.Bytes()
}

// errorResult creates an ExecuteResult containing a CQL ERROR frame
// body.
func errorResult(code int32, msg string) ExecuteResult {
	return ExecuteResult{
		Body:    buildErrorBody(code, msg),
		IsError: true,
	}
}
