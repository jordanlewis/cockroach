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
	"github.com/stretchr/testify/require"
)

func TestHandleSystemSchemaSelectEmptyTables(t *testing.T) {
	// These system_schema tables return zero rows (stubs for cqlsh
	// startup).
	tables := []string{
		"triggers", "views", "functions",
		"aggregates", "types", "indexes",
	}
	ctx := context.Background()
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			res, handled := handleSystemSelect(ctx, nil, "system_schema", table, nil)
			require.True(t, handled, "system_schema.%s should be handled", table)
			require.False(t, res.IsError)

			r := bytes.NewReader(res.Body)
			kind, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			require.Equal(t, resultKindRows, kind)

			_, err = cqlwire.ReadInt(r) // flags
			require.NoError(t, err)

			colCount, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			schema := systemSchemaTables[table]
			require.Equal(t, int32(len(schema.columns)), colCount)

			for i := int32(0); i < colCount; i++ {
				_, _ = cqlwire.ReadString(r) // keyspace
				_, _ = cqlwire.ReadString(r) // table
				_, _ = cqlwire.ReadString(r) // name
				_, _ = cqlwire.ReadShort(r)  // type
			}

			rowCount, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			require.Equal(t, int32(0), rowCount)
		})
	}
}

func TestHandleSystemSchemaKeyspaces(t *testing.T) {
	// With db=nil, buildSystemSchemaKeyspacesBody returns only
	// synthetic system keyspaces.
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "keyspaces", nil)
	require.True(t, handled)
	require.False(t, res.IsError)

	r := bytes.NewReader(res.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, resultKindRows, kind)

	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)
	require.Equal(t, int32(3), colCount) // keyspace_name, durable_writes, replication

	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(3), rowCount) // system, system_schema, system_virtual_schema
}

func TestHandleSystemSchemaTables(t *testing.T) {
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "tables", nil)
	require.True(t, handled)
	require.False(t, res.IsError)

	r := bytes.NewReader(res.Body)
	kind, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, resultKindRows, kind)

	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)
	require.Equal(t, int32(len(systemSchemaTables["tables"].columns)), colCount)

	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	// system.local, system.peers, system.peers_v2
	require.Equal(t, int32(3), rowCount)
}

func TestHandleSystemSchemaColumns(t *testing.T) {
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "columns", nil)
	require.True(t, handled)
	require.False(t, res.IsError)

	r := bytes.NewReader(res.Body)
	kind, _ := cqlwire.ReadInt(r)
	require.Equal(t, resultKindRows, kind)

	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)
	require.Equal(t, int32(len(systemSchemaTables["columns"].columns)), colCount)

	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	rowCount, _ := cqlwire.ReadInt(r)
	// system.local (14 cols) + system.peers (8 cols) = 22
	expectedCols := int32(len(systemLocalColumns) + len(systemPeersColumns))
	require.Equal(t, expectedCols, rowCount)
}

func TestHandleSystemSchemaSelectCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "SYSTEM_SCHEMA", "keyspaces", nil)
	require.True(t, handled)
	require.False(t, res.IsError)
}

func TestHandleSystemSchemaSelectUnknownTable(t *testing.T) {
	ctx := context.Background()
	_, handled := handleSystemSelect(ctx, nil, "system_schema", "nonexistent", nil)
	require.False(t, handled)
}

func TestHandleSystemSchemaSelectOtherKeyspace(t *testing.T) {
	ctx := context.Background()
	_, handled := handleSystemSelect(ctx, nil, "my_keyspace", "triggers", nil)
	require.False(t, handled)
}

func TestExecutorSystemSchemaSelect(t *testing.T) {
	// Verify the full executor path intercepts system_schema queries
	// without hitting the SQL backend.
	mock := &mockExecutor{}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"SELECT * FROM system_schema.keyspaces", "")
	require.False(t, result.IsError)
	require.Empty(t, mock.execSQL,
		"system_schema query should not reach SQL executor")

	kind := readResultKind(t, result.Body)
	require.Equal(t, resultKindRows, kind)
}

func TestExecutorSystemSchemaTriggers(t *testing.T) {
	mock := &mockExecutor{}
	db := &mockDB{exec: mock}
	exec := NewExecutor(db)
	ctx := context.Background()

	result := exec.ExecuteQuery(ctx,
		"SELECT * FROM system_schema.triggers", "")
	require.False(t, result.IsError)
	require.Empty(t, mock.execSQL)
}

func TestExecutorSystemSchemaColumnNames(t *testing.T) {
	// Verify column names in the result metadata match the schema.
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "indexes", nil)
	require.True(t, handled)

	r := bytes.NewReader(res.Body)
	_, _ = cqlwire.ReadInt(r) // kind
	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)

	expectedCols := []string{
		"keyspace_name", "table_name", "index_name", "kind", "options",
	}
	require.Equal(t, int32(len(expectedCols)), colCount)

	for _, expected := range expectedCols {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		name, err := cqlwire.ReadString(r)
		require.NoError(t, err)
		require.Equal(t, expected, name)
		_, _ = cqlwire.ReadShort(r) // type
	}
}
