// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"

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
	db isql.DB
}

// NewExecutor creates a new Executor backed by the given isql.DB.
func NewExecutor(db isql.DB) *Executor {
	return &Executor{db: db}
}

// ExecuteQuery parses and executes a CQL query, returning an
// ExecuteResult containing the CQL RESULT or ERROR frame body. The
// keyspace parameter provides the current database context (set via
// USE).
func (e *Executor) ExecuteQuery(
	ctx context.Context, cqlQuery string, keyspace string,
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

	// Intercept system and system_schema queries. cqlsh and other CQL
	// drivers query system.local, system.peers, and system_schema.*
	// tables during startup. We return synthetic results for these
	// since CRDB does not have Cassandra system tables.
	if sel, ok := stmt.(*parser.SelectStatement); ok {
		if res, handled := handleSystemSelect(
			ctx, e.db, sel.Keyspace, sel.Table, sel.Where,
		); handled {
			return res
		}
	}

	// Translate the CQL AST to SQL.
	result, err := translate.Translate(stmt)
	if err != nil {
		return errorResult(errCodeInvalid, err.Error())
	}

	override := sessiondata.InternalExecutorOverride{
		User: username.RootUserName(),
	}
	if keyspace != "" {
		override.Database = keyspace
	}

	switch s := stmt.(type) {
	case *parser.CreateKeyspaceStatement:
		return e.executeDDL(ctx, result, override, "CREATED", "KEYSPACE", s.Keyspace, "")
	case *parser.CreateTableStatement:
		ks := s.Keyspace
		if ks == "" {
			ks = keyspace
		}
		return e.executeDDL(ctx, result, override, "CREATED", "TABLE", ks, s.Table)
	case *parser.InsertStatement:
		return e.executeDML(ctx, result, override)
	case *parser.UpdateStatement:
		return e.executeDML(ctx, result, override)
	case *parser.DeleteStatement:
		return e.executeDML(ctx, result, override)
	case *parser.SelectStatement:
		return e.executeSelect(ctx, result, override)
	default:
		return errorResult(errCodeServerError, "unsupported statement type")
	}
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
		return errorResult(errCodeServerError, err.Error())
	}
	return ExecuteResult{
		Body: buildSchemaChangeBody(changeType, target, keyspace, name),
	}
}

// executeDML executes a DML statement (INSERT/UPSERT) and returns a
// Void RESULT.
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
	return ExecuteResult{
		Body: buildVoidBody(),
	}
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
