// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestOracleTypeToCRDB(t *testing.T) {
	tests := []struct {
		oracle   OracleType
		expected *types.T
	}{
		{Number, types.Decimal},
		{Varchar2, types.VarChar},
		{Date, types.Timestamp},
		{Clob, types.String},
		{Blob, types.Bytes},
		{Raw, types.Bytes},
		{BinaryFloat, types.Float4},
		{BinaryDouble, types.Float},
		{Rowid, types.String},
	}
	for _, tc := range tests {
		t.Run(tc.oracle.String(), func(t *testing.T) {
			got, err := OracleTypeToCRDB(tc.oracle)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestOracleTypeToCRDB_Invalid(t *testing.T) {
	_, err := OracleTypeToCRDB(OracleType(999))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported Oracle type")
}

func TestOracleTypeFromName(t *testing.T) {
	tests := []struct {
		name     string
		expected OracleType
	}{
		{"NUMBER", Number},
		{"number", Number},
		{"Number", Number},
		{"VARCHAR2", Varchar2},
		{"varchar2", Varchar2},
		{"DATE", Date},
		{"CLOB", Clob},
		{"BLOB", Blob},
		{"RAW", Raw},
		{"BINARY_FLOAT", BinaryFloat},
		{"binary_float", BinaryFloat},
		{"BINARY_DOUBLE", BinaryDouble},
		{"ROWID", Rowid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := OracleTypeFromName(tc.name)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestOracleTypeFromName_Invalid(t *testing.T) {
	_, err := OracleTypeFromName("NOSUCHTYPE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown Oracle type name")
}

func TestMapOracleTypeName(t *testing.T) {
	got, err := MapOracleTypeName("NUMBER")
	require.NoError(t, err)
	require.Equal(t, types.Decimal, got)
}

func TestMapOracleTypeName_CaseInsensitive(t *testing.T) {
	got, err := MapOracleTypeName("binary_double")
	require.NoError(t, err)
	require.Equal(t, types.Float, got)
}

func TestMapOracleTypeName_Invalid(t *testing.T) {
	_, err := MapOracleTypeName("NOPE")
	require.Error(t, err)
}

// TestOracleDateMapsToTimestamp verifies the critical mapping of Oracle DATE
// to TIMESTAMP (not DATE). Oracle's DATE type includes time components
// (hour, minute, second), so mapping to CockroachDB's date-only DATE type
// would lose time information.
func TestOracleDateMapsToTimestamp(t *testing.T) {
	got, err := OracleTypeToCRDB(Date)
	require.NoError(t, err)
	require.Equal(t, types.Timestamp, got,
		"Oracle DATE must map to TIMESTAMP because Oracle DATE includes time components")
	require.NotEqual(t, types.Date, got,
		"Oracle DATE must NOT map to CockroachDB DATE (which is date-only)")
}

func TestOracleTypeString(t *testing.T) {
	require.Equal(t, "NUMBER", Number.String())
	require.Equal(t, "VARCHAR2", Varchar2.String())
	require.Equal(t, "BINARY_FLOAT", BinaryFloat.String())
	require.Equal(t, "UNKNOWN", OracleType(999).String())
}
