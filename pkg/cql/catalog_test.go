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
	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	cqltypes "github.com/cockroachdb/cockroach/pkg/cql/types"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
			res, handled := handleSystemSelect(ctx, nil, "system_schema", table, nil, nil)
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
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "keyspaces", nil, nil)
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
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "tables", nil, nil)
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
	// system.local, system.peers, system.peers_v2 + all system_schema tables
	expectedTables := int32(3 + len(systemSchemaTables))
	require.Equal(t, expectedTables, rowCount)
}

func TestHandleSystemSchemaColumns(t *testing.T) {
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "columns", nil, nil)
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
	// system.local + system.peers + system.peers_v2 + all system_schema tables
	systemSchemaColCount := 0
	for _, schema := range systemSchemaTables {
		systemSchemaColCount += len(schema.columns)
	}
	expectedCols := int32(
		len(systemLocalColumns) + len(systemPeersColumns) +
			len(systemPeersV2Columns) + systemSchemaColCount,
	)
	require.Equal(t, expectedCols, rowCount)
}

func TestHandleSystemSchemaTablesFilterSystemSchema(t *testing.T) {
	// Verify that filtering system_schema.tables by keyspace_name =
	// 'system_schema' returns all system_schema tables (self-describing
	// catalog). This is what cqlsh DESCRIBE KEYSPACE system_schema uses.
	ctx := context.Background()
	where := []parser.WhereClause{{
		Column:   "keyspace_name",
		Operator: "=",
		Value:    &parser.StringLiteral{Value: "system_schema"},
	}}
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "tables", where, nil)
	require.True(t, handled)
	require.False(t, res.IsError)

	r := bytes.NewReader(res.Body)
	_, _ = cqlwire.ReadInt(r) // kind
	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)
	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	require.Equal(t, int32(len(systemSchemaTables)), rowCount,
		"system_schema.tables should list all system_schema tables when filtered")
}

func TestHandleSystemSchemaColumnsFilterSystemSchema(t *testing.T) {
	// Verify that filtering system_schema.columns by keyspace_name =
	// 'system_schema' returns column metadata for system_schema tables.
	ctx := context.Background()
	where := []parser.WhereClause{{
		Column:   "keyspace_name",
		Operator: "=",
		Value:    &parser.StringLiteral{Value: "system_schema"},
	}}
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "columns", where, nil)
	require.True(t, handled)
	require.False(t, res.IsError)

	r := bytes.NewReader(res.Body)
	_, _ = cqlwire.ReadInt(r) // kind
	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)
	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		_, _ = cqlwire.ReadString(r) // name
		_, _ = cqlwire.ReadShort(r)  // type
	}

	rowCount, err := cqlwire.ReadInt(r)
	require.NoError(t, err)
	var expectedCols int32
	for _, schema := range systemSchemaTables {
		expectedCols += int32(len(schema.columns))
	}
	require.Equal(t, expectedCols, rowCount,
		"system_schema.columns should list all system_schema table columns when filtered")
}

func TestHandleSystemSchemaSelectCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "SYSTEM_SCHEMA", "keyspaces", nil, nil)
	require.True(t, handled)
	require.False(t, res.IsError)
}

func TestHandleSystemSchemaSelectUnknownTable(t *testing.T) {
	ctx := context.Background()
	_, handled := handleSystemSelect(ctx, nil, "system_schema", "nonexistent", nil, nil)
	require.False(t, handled)
}

func TestHandleSystemSchemaSelectOtherKeyspace(t *testing.T) {
	ctx := context.Background()
	_, handled := handleSystemSelect(ctx, nil, "my_keyspace", "triggers", nil, nil)
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
		"SELECT * FROM system_schema.keyspaces", "", username.RootUserName())
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
		"SELECT * FROM system_schema.triggers", "", username.RootUserName())
	require.False(t, result.IsError)
	require.Empty(t, mock.execSQL)
}

func TestExecutorSystemSchemaColumnNames(t *testing.T) {
	// Verify column names in the result metadata match the schema.
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system_schema", "indexes", nil, nil)
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

func TestHandleSystemLocalTokens(t *testing.T) {
	// Verify system.local includes the tokens column and returns a
	// set<varchar> value with a single token.
	ctx := context.Background()
	res, handled := handleSystemSelect(ctx, nil, "system", "local", nil, nil)
	require.True(t, handled)
	require.False(t, res.IsError)

	r := bytes.NewReader(res.Body)
	kind, _ := cqlwire.ReadInt(r)
	require.Equal(t, resultKindRows, kind)

	_, _ = cqlwire.ReadInt(r) // flags
	colCount, _ := cqlwire.ReadInt(r)
	require.Equal(t, int32(len(systemLocalColumns)), colCount)

	// Read column metadata and find the tokens column.
	var tokensIdx int32 = -1
	for i := int32(0); i < colCount; i++ {
		_, _ = cqlwire.ReadString(r) // keyspace
		_, _ = cqlwire.ReadString(r) // table
		name, _ := cqlwire.ReadString(r)
		typeID, _ := cqlwire.ReadShort(r)
		if typeID == uint16(cqltypes.CQLSet) {
			// Collection types have an element type short.
			elemType, _ := cqlwire.ReadShort(r)
			require.Equal(t, uint16(cqltypes.CQLVarchar), elemType)
		}
		if name == "tokens" {
			tokensIdx = i
		}
	}
	require.NotEqual(t, int32(-1), tokensIdx,
		"system.local should have a tokens column")

	// Read the single row and extract the tokens cell.
	rowCount, _ := cqlwire.ReadInt(r)
	require.Equal(t, int32(1), rowCount)

	for i := int32(0); i < colCount; i++ {
		cellLen, _ := cqlwire.ReadInt(r)
		if i == tokensIdx {
			require.Greater(t, cellLen, int32(0),
				"tokens cell should not be null or empty")
			// Read the set value: [int] element_count, then elements.
			data := make([]byte, cellLen)
			_, err := r.Read(data)
			require.NoError(t, err)
			elemR := bytes.NewReader(data)
			elemCount, _ := cqlwire.ReadInt(elemR)
			require.Equal(t, int32(1), elemCount,
				"tokens should have exactly one element")
		} else if cellLen >= 0 {
			data := make([]byte, cellLen)
			_, _ = r.Read(data)
		}
	}
}

func TestHandleSystemPeersV2Schema(t *testing.T) {
	// Verify system.peers_v2 has a different (wider) schema than
	// system.peers, including port columns for mixed-port clusters.
	ctx := context.Background()

	resV1, handled := handleSystemSelect(ctx, nil, "system", "peers", nil, nil)
	require.True(t, handled)
	resV2, handled := handleSystemSelect(ctx, nil, "system", "peers_v2", nil, nil)
	require.True(t, handled)

	readColCount := func(body []byte) int32 {
		r := bytes.NewReader(body)
		_, _ = cqlwire.ReadInt(r) // kind
		_, _ = cqlwire.ReadInt(r) // flags
		count, _ := cqlwire.ReadInt(r)
		return count
	}

	v1Cols := readColCount(resV1.Body)
	v2Cols := readColCount(resV2.Body)

	require.Equal(t, int32(len(systemPeersColumns)), v1Cols)
	require.Equal(t, int32(len(systemPeersV2Columns)), v2Cols)
	require.Greater(t, v2Cols, v1Cols,
		"peers_v2 should have more columns than peers")
}
