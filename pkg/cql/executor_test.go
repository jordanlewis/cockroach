// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// mockExecutor implements isql.Executor for testing. It records calls
// and returns preconfigured results.
type mockExecutor struct {
	// execSQL records the SQL string passed to ExecEx.
	execSQL string
	// execErr is returned by ExecEx if non-nil.
	execErr error
	// queryRows is returned by QueryBufferedExWithCols.
	queryRows []tree.Datums
	// queryCols is returned by QueryBufferedExWithCols.
	queryCols colinfo.ResultColumns
	// queryErr is returned by QueryBufferedExWithCols if non-nil.
	queryErr error
}

func (m *mockExecutor) Exec(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, stmt string, _ ...interface{},
) (int, error) {
	m.execSQL = stmt
	return 0, m.execErr
}

func (m *mockExecutor) ExecEx(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	stmt string,
	_ ...interface{},
) (int, error) {
	m.execSQL = stmt
	return 0, m.execErr
}

func (m *mockExecutor) ExecParsed(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ statements.Statement[tree.Statement],
	_ ...interface{},
) (int, error) {
	return 0, nil
}

func (m *mockExecutor) QueryRow(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) (tree.Datums, error) {
	return nil, nil
}

func (m *mockExecutor) QueryRowEx(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) (tree.Datums, error) {
	return nil, nil
}

func (m *mockExecutor) QueryRowExParsed(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ statements.Statement[tree.Statement],
	_ ...interface{},
) (tree.Datums, error) {
	return nil, nil
}

func (m *mockExecutor) QueryRowExWithCols(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	return nil, nil, nil
}

func (m *mockExecutor) QueryBuffered(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) ([]tree.Datums, error) {
	return nil, nil
}

func (m *mockExecutor) QueryBufferedEx(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) ([]tree.Datums, error) {
	return nil, nil
}

func (m *mockExecutor) QueryIterator(
	_ context.Context, _ redact.RedactableString, _ *kv.Txn, _ string, _ ...interface{},
) (isql.Rows, error) {
	return nil, nil
}

func (m *mockExecutor) QueryIteratorEx(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	_ string,
	_ ...interface{},
) (isql.Rows, error) {
	return nil, nil
}

func (m *mockExecutor) QueryBufferedExWithCols(
	_ context.Context,
	_ redact.RedactableString,
	_ *kv.Txn,
	_ sessiondata.InternalExecutorOverride,
	stmt string,
	_ ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	m.execSQL = stmt
	return m.queryRows, m.queryCols, m.queryErr
}

func (m *mockExecutor) WithSyntheticDescriptors(
	_ []catalog.Descriptor, run func() error,
) error {
	return run()
}

// mockDB implements isql.DB for testing. It returns a mock executor.
type mockDB struct {
	exec *mockExecutor
}

func (m *mockDB) KV() *kv.DB { return nil }

func (m *mockDB) Txn(_ context.Context, _ func(context.Context, isql.Txn) error, _ ...isql.TxnOption) error {
	return nil
}

func (m *mockDB) Executor(_ ...isql.ExecutorOption) isql.Executor {
	return m.exec
}

func (m *mockDB) Session(_ context.Context, _ string, _ ...isql.ExecutorOption) (isql.Session, error) {
	return nil, nil
}

// readResultKind reads the RESULT kind [int] from a frame body.
func readResultKind(t *testing.T, body []byte) int32 {
	t.Helper()
	r := bytes.NewReader(body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	return kind
}

// readErrorCode reads the error code [int] from an ERROR frame body.
func readErrorCode(t *testing.T, body []byte) int32 {
	t.Helper()
	r := bytes.NewReader(body)
	code, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	return code
}

func TestExecutorUseKeyspace(t *testing.T) {
	db := &mockDB{exec: &mockExecutor{}}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx, "USE mykeyspace", "")
	require.False(t, result.IsError)
	require.Equal(t, "mykeyspace", result.NewKeyspace)

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindSetKeyspace, kind)

	// Read the keyspace name from the body.
	r := bytes.NewReader(result.Body[4:]) // skip the kind int
	ks, err := cqlwire.ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "mykeyspace", ks)
}

func TestExecutorCreateKeyspace(t *testing.T) {
	mock := &mockExecutor{}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
		"",
	)
	require.False(t, result.IsError)
	require.Contains(t, mock.execSQL, "CREATE DATABASE")
	require.Contains(t, mock.execSQL, "test_ks")

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindSchemaChange, kind)
}

func TestExecutorCreateTable(t *testing.T) {
	mock := &mockExecutor{}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"CREATE TABLE users (id uuid, name text, PRIMARY KEY (id))",
		"mykeyspace",
	)
	require.False(t, result.IsError)
	require.Contains(t, mock.execSQL, "CREATE TABLE")

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindSchemaChange, kind)
}

func TestExecutorInsert(t *testing.T) {
	mock := &mockExecutor{}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"INSERT INTO users (id, name) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice')",
		"mykeyspace",
	)
	require.False(t, result.IsError)
	require.Contains(t, mock.execSQL, "UPSERT INTO")

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindVoid, kind)
}

func TestExecutorSelect(t *testing.T) {
	mock := &mockExecutor{
		queryCols: colinfo.ResultColumns{
			{Name: "id", Typ: types.Int4},
			{Name: "name", Typ: types.String},
		},
		queryRows: []tree.Datums{
			{tree.NewDInt(1), tree.NewDString("alice")},
			{tree.NewDInt(2), tree.NewDString("bob")},
		},
	}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"SELECT id, name FROM users",
		"mykeyspace",
	)
	require.False(t, result.IsError)
	require.Contains(t, mock.execSQL, "SELECT")

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindRows, kind)

	// Verify we can read the metadata and rows from the body.
	r := bytes.NewReader(result.Body[4:]) // skip kind

	// Metadata flags.
	flags, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(0), flags)

	// Column count.
	colCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(2), colCount)

	// Column 1: keyspace, table, name, type.
	ks, err := cqlwire.ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "", ks)
	tbl, err := cqlwire.ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "", tbl)
	colName, err := cqlwire.ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "id", colName)
	colType, err := cqlwire.ReadShort(r)
	require.NoError(t, err)
	require.Equal(t, uint16(0x0009), colType) // CQL int

	// Column 2.
	_, _ = cqlwire.ReadString(r) // ks
	_, _ = cqlwire.ReadString(r) // tbl
	colName, err = cqlwire.ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "name", colName)
	colType, err = cqlwire.ReadShort(r)
	require.NoError(t, err)
	require.Equal(t, uint16(0x000D), colType) // CQL varchar

	// Row count.
	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(2), rowCount)

	// Row 1, col 1 (id=1, 4-byte int).
	val, err := cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, 4, len(val))

	// Row 1, col 2 (name="alice").
	val, err = cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, "alice", string(val))

	// Row 2, col 1 (id=2).
	val, err = cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, 4, len(val))

	// Row 2, col 2 (name="bob").
	val, err = cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, "bob", string(val))
}

func TestExecutorDDLError(t *testing.T) {
	mock := &mockExecutor{
		execErr: errors.New("table already exists"),
	}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
		"",
	)
	require.True(t, result.IsError)

	code := readErrorCode(t, result.Body)
	require.Equal(t, errCodeServerError, code)
}

func TestExecutorDMLError(t *testing.T) {
	mock := &mockExecutor{
		execErr: errors.New("constraint violation"),
	}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"INSERT INTO users (id, name) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice')",
		"",
	)
	require.True(t, result.IsError)

	code := readErrorCode(t, result.Body)
	require.Equal(t, errCodeServerError, code)
}

func TestExecutorSelectError(t *testing.T) {
	mock := &mockExecutor{
		queryErr: errors.New("relation does not exist"),
	}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"SELECT * FROM nonexistent",
		"",
	)
	require.True(t, result.IsError)

	code := readErrorCode(t, result.Body)
	require.Equal(t, errCodeServerError, code)
}

func TestExecutorParseError(t *testing.T) {
	db := &mockDB{exec: &mockExecutor{}}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx, "INVALID QUERY SYNTAX", "")
	require.True(t, result.IsError)

	code := readErrorCode(t, result.Body)
	require.Equal(t, errCodeSyntax, code)
}

func TestExecutorSelectWithNulls(t *testing.T) {
	mock := &mockExecutor{
		queryCols: colinfo.ResultColumns{
			{Name: "id", Typ: types.Int4},
			{Name: "name", Typ: types.String},
		},
		queryRows: []tree.Datums{
			{tree.NewDInt(1), tree.DNull},
		},
	}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx, "SELECT id, name FROM users", "")
	require.False(t, result.IsError)

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindRows, kind)

	// Skip metadata to get to row data: kind(4) + flags(4) +
	// colcount(4) + 2 columns * (ks_string + tbl_string +
	// name_string + type_short).
	r := bytes.NewReader(result.Body[4:]) // skip kind
	_, _ = cqlwire.ReadInt(r)             // flags
	_, _ = cqlwire.ReadInt(r)             // col count

	// Skip 2 column defs.
	for i := 0; i < 2; i++ {
		_, _ = cqlwire.ReadString(r) // ks
		_, _ = cqlwire.ReadString(r) // tbl
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	// Row count.
	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(1), rowCount)

	// Row 1, col 1 (id=1).
	val, err := cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Equal(t, 4, len(val))

	// Row 1, col 2 (name=NULL). ReadBytes returns nil for null.
	val, err = cqlwire.ReadBytes(r)
	require.NoError(t, err)
	require.Nil(t, val)
}
