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

func TestHandleSystemSchemaSelect(t *testing.T) {
	// All six tables from the bead, plus keyspaces/tables/columns.
	tables := []string{
		"keyspaces", "tables", "columns",
		"triggers", "views", "functions",
		"aggregates", "types", "indexes",
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			res, handled := handleSystemSchemaSelect("system_schema", table)
			require.True(t, handled, "system_schema.%s should be handled", table)
			require.False(t, res.IsError)

			// Parse the result body: should be Rows with 0 rows.
			r := bytes.NewReader(res.Body)
			kind, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			require.Equal(t, resultKindRows, kind)

			// Metadata flags.
			_, err = cqlwire.ReadInt(r)
			require.NoError(t, err)

			// Column count should match schema definition.
			colCount, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			schema := systemSchemaTables[table]
			require.Equal(t, int32(len(schema.columns)), colCount)

			// Skip column metadata.
			for i := int32(0); i < colCount; i++ {
				_, _ = cqlwire.ReadString(r) // keyspace
				_, _ = cqlwire.ReadString(r) // table
				_, _ = cqlwire.ReadString(r) // name
				_, _ = cqlwire.ReadShort(r)  // type
			}

			// Row count should be 0.
			rowCount, err := cqlwire.ReadInt(r)
			require.NoError(t, err)
			require.Equal(t, int32(0), rowCount)
		})
	}
}

func TestHandleSystemSchemaSelectCaseInsensitive(t *testing.T) {
	res, handled := handleSystemSchemaSelect("SYSTEM_SCHEMA", "keyspaces")
	require.True(t, handled)
	require.False(t, res.IsError)
}

func TestHandleSystemSchemaSelectUnknownTable(t *testing.T) {
	_, handled := handleSystemSchemaSelect("system_schema", "nonexistent")
	require.False(t, handled)
}

func TestHandleSystemSchemaSelectOtherKeyspace(t *testing.T) {
	_, handled := handleSystemSchemaSelect("my_keyspace", "triggers")
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
	res, handled := handleSystemSchemaSelect("system_schema", "indexes")
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
