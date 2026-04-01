// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTTIFuncCodeString(t *testing.T) {
	tests := []struct {
		code TTIFuncCode
		want string
	}{
		{TTIOpen, "OPEN"},
		{TTIExec, "EXEC"},
		{TTIFetch, "FETCH"},
		{TTIClose, "CLOSE"},
		{TTICommit, "COMMIT"},
		{TTIRollback, "ROLLBACK"},
		{TTIFuncCode(0xFF), "UNKNOWN"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.code.String())
	}
}

func TestTTIOpenRoundtrip(t *testing.T) {
	msg := TTIOpenMsg{
		CursorID: 42,
		SQL:      "SELECT * FROM employees WHERE dept_id = :1",
	}
	encoded := EncodeTTIOpen(msg)
	require.Equal(t, byte(TTIOpen), encoded[0])

	decoded, err := DecodeTTIOpen(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTIOpenEmptySQL(t *testing.T) {
	msg := TTIOpenMsg{CursorID: 1, SQL: ""}
	encoded := EncodeTTIOpen(msg)
	decoded, err := DecodeTTIOpen(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTIOpenDecodeTruncated(t *testing.T) {
	// Too short for fixed header.
	_, err := DecodeTTIOpen([]byte{0x03, 0x00})
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	// SQL length says 10 but payload is shorter.
	data := EncodeTTIOpen(TTIOpenMsg{CursorID: 1, SQL: "SELECT 1"})
	_, err = DecodeTTIOpen(data[:6])
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated")
}

func TestTTIOpenResponseRoundtrip(t *testing.T) {
	resp := TTIOpenResponse{
		CursorID: 42,
		Columns: []ColumnDesc{
			{TypeCode: OracleTypeVarchar2, Name: "NAME"},
			{TypeCode: OracleTypeNumber, Name: "SALARY"},
			{TypeCode: OracleTypeDate, Name: "HIRE_DATE"},
		},
	}
	encoded := EncodeTTIOpenResponse(resp)
	require.Equal(t, byte(TTIOpen), encoded[0])

	decoded, err := DecodeTTIOpenResponse(encoded)
	require.NoError(t, err)
	require.Equal(t, resp, decoded)
}

func TestTTIOpenResponseNoColumns(t *testing.T) {
	resp := TTIOpenResponse{CursorID: 7, Columns: []ColumnDesc{}}
	encoded := EncodeTTIOpenResponse(resp)
	decoded, err := DecodeTTIOpenResponse(encoded)
	require.NoError(t, err)
	require.Equal(t, resp, decoded)
}

func TestTTIExecRoundtrip(t *testing.T) {
	msg := TTIExecMsg{
		CursorID: 42,
		SQL:      "INSERT INTO t VALUES (:1, :2, :3)",
		BindVars: []BindVar{
			{TypeCode: OracleTypeNumber, Value: []byte{0x01, 0x02}},
			{TypeCode: OracleTypeVarchar2, Value: []byte("hello")},
			{TypeCode: OracleTypeDate, Value: nil}, // NULL
		},
	}
	encoded := EncodeTTIExec(msg)
	require.Equal(t, byte(TTIExec), encoded[0])

	decoded, err := DecodeTTIExec(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTIExecNoBindVars(t *testing.T) {
	msg := TTIExecMsg{
		CursorID: 1,
		SQL:      "COMMIT",
		BindVars: []BindVar{},
	}
	encoded := EncodeTTIExec(msg)
	decoded, err := DecodeTTIExec(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTIExecNoSQL(t *testing.T) {
	msg := TTIExecMsg{
		CursorID: 5,
		SQL:      "",
		BindVars: []BindVar{
			{TypeCode: OracleTypeNumber, Value: []byte{0x42}},
		},
	}
	encoded := EncodeTTIExec(msg)
	decoded, err := DecodeTTIExec(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTIExecResponseRoundtrip(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		resp := TTIExecResponse{
			RowsAffected: 42,
			ErrorCode:    0,
		}
		encoded := EncodeTTIExecResponse(resp)
		decoded, err := DecodeTTIExecResponse(encoded)
		require.NoError(t, err)
		require.Equal(t, resp, decoded)
	})

	t.Run("error", func(t *testing.T) {
		resp := TTIExecResponse{
			RowsAffected: 0,
			ErrorCode:    942,
			ErrorMsg:     "ORA-00942: table or view does not exist",
		}
		encoded := EncodeTTIExecResponse(resp)
		decoded, err := DecodeTTIExecResponse(encoded)
		require.NoError(t, err)
		require.Equal(t, resp, decoded)
	})
}

func TestTTIFetchRoundtrip(t *testing.T) {
	msg := TTIFetchMsg{CursorID: 42, FetchSize: 100}
	encoded := EncodeTTIFetch(msg)
	require.Equal(t, byte(TTIFetch), encoded[0])

	decoded, err := DecodeTTIFetch(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTIFetchResponseRoundtrip(t *testing.T) {
	resp := TTIFetchResponse{
		Rows: [][][]byte{
			{[]byte("Alice"), []byte{0x00, 0x64}, nil},    // row 1: name, salary, NULL
			{[]byte("Bob"), []byte{0x00, 0xC8}, []byte{}}, // row 2: name, salary, empty
		},
		Flags: FetchFlagMoreRows,
	}
	numCols := 3
	encoded := EncodeTTIFetchResponse(resp, numCols)
	require.Equal(t, byte(TTIFetch), encoded[0])

	decoded, err := DecodeTTIFetchResponse(encoded, numCols)
	require.NoError(t, err)
	require.Equal(t, resp.Flags, decoded.Flags)
	require.Len(t, decoded.Rows, 2)

	// Row 1.
	require.Equal(t, []byte("Alice"), decoded.Rows[0][0])
	require.Equal(t, []byte{0x00, 0x64}, decoded.Rows[0][1])
	require.Nil(t, decoded.Rows[0][2]) // NULL

	// Row 2.
	require.Equal(t, []byte("Bob"), decoded.Rows[1][0])
	require.Equal(t, []byte{0x00, 0xC8}, decoded.Rows[1][1])
	require.Equal(t, []byte{}, decoded.Rows[1][2]) // empty, not NULL
}

func TestTTIFetchResponseNoRows(t *testing.T) {
	resp := TTIFetchResponse{Rows: [][][]byte{}, Flags: 0}
	encoded := EncodeTTIFetchResponse(resp, 2)
	decoded, err := DecodeTTIFetchResponse(encoded, 2)
	require.NoError(t, err)
	require.Empty(t, decoded.Rows)
	require.Equal(t, FetchFlags(0), decoded.Flags)
}

func TestTTICloseRoundtrip(t *testing.T) {
	msg := TTICloseMsg{CursorID: 42}
	encoded := EncodeTTIClose(msg)
	require.Equal(t, byte(TTIClose), encoded[0])

	decoded, err := DecodeTTIClose(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
}

func TestTTICommitRoundtrip(t *testing.T) {
	encoded := EncodeTTICommit()
	require.Equal(t, []byte{byte(TTICommit)}, encoded)
	require.NoError(t, DecodeTTICommit(encoded))
}

func TestTTIRollbackRoundtrip(t *testing.T) {
	encoded := EncodeTTIRollback()
	require.Equal(t, []byte{byte(TTIRollback)}, encoded)
	require.NoError(t, DecodeTTIRollback(encoded))
}

func TestDecodeTTIFuncCode(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    TTIFuncCode
		wantErr bool
	}{
		{name: "OPEN", data: []byte{0x03}, want: TTIOpen},
		{name: "EXEC", data: []byte{0x04, 0x00, 0x01}, want: TTIExec},
		{name: "COMMIT", data: []byte{0x0E}, want: TTICommit},
		{name: "empty", data: []byte{}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeTTIFuncCode(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTTIDecodeTruncatedErrors(t *testing.T) {
	// Each decode function should return clear errors on truncated input.
	t.Run("EXEC truncated at bind", func(t *testing.T) {
		msg := TTIExecMsg{
			CursorID: 1,
			SQL:      "SELECT 1",
			BindVars: []BindVar{{TypeCode: OracleTypeNumber, Value: []byte{1, 2, 3}}},
		}
		encoded := EncodeTTIExec(msg)
		// Truncate the bind variable value.
		_, err := DecodeTTIExec(encoded[:len(encoded)-2])
		require.Error(t, err)
		require.Contains(t, err.Error(), "truncated")
	})

	t.Run("FETCH response truncated row", func(t *testing.T) {
		resp := TTIFetchResponse{
			Rows:  [][][]byte{{[]byte("val")}},
			Flags: 0,
		}
		encoded := EncodeTTIFetchResponse(resp, 1)
		// Truncate the value data.
		_, err := DecodeTTIFetchResponse(encoded[:len(encoded)-1], 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "truncated")
	})

	t.Run("OPEN response truncated column", func(t *testing.T) {
		resp := TTIOpenResponse{
			CursorID: 1,
			Columns:  []ColumnDesc{{TypeCode: OracleTypeVarchar2, Name: "COL"}},
		}
		encoded := EncodeTTIOpenResponse(resp)
		_, err := DecodeTTIOpenResponse(encoded[:6])
		require.Error(t, err)
		require.Contains(t, err.Error(), "truncated")
	})

	t.Run("EXEC response truncated error msg", func(t *testing.T) {
		resp := TTIExecResponse{ErrorCode: 1, ErrorMsg: "error text"}
		encoded := EncodeTTIExecResponse(resp)
		_, err := DecodeTTIExecResponse(encoded[:8])
		require.Error(t, err)
		require.Contains(t, err.Error(), "truncated")
	})

	t.Run("COMMIT empty", func(t *testing.T) {
		require.Error(t, DecodeTTICommit([]byte{}))
	})

	t.Run("ROLLBACK empty", func(t *testing.T) {
		require.Error(t, DecodeTTIRollback([]byte{}))
	})
}
