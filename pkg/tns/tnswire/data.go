// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

// TTI (Two-Task Interface) messages are carried inside the Payload of DATA
// packets. Each message begins with a single-byte function code that
// identifies the operation, followed by a function-specific body.
//
// The six core TTI function codes model the lifecycle of a SQL cursor:
//
//   OPEN       Open a new cursor and prepare a SQL statement.
//   EXEC       Execute a prepared cursor, optionally with bind variables.
//   FETCH      Retrieve result rows from an executed cursor.
//   CLOSE      Release a cursor and its server-side resources.
//   COMMIT     Commit the current transaction.
//   ROLLBACK   Roll back the current transaction.

// TTIFuncCode identifies the type of a TTI message inside a DATA packet.
type TTIFuncCode uint8

const (
	// TTIOpen opens a new cursor on the server and prepares a SQL statement
	// for execution. The server assigns a cursor ID and returns column
	// metadata.
	TTIOpen TTIFuncCode = 0x03

	// TTIExec executes a previously opened cursor. The message carries
	// optional bind variable values to substitute into the prepared
	// statement.
	TTIExec TTIFuncCode = 0x04

	// TTIFetch retrieves result rows from an executed cursor. The client
	// specifies the maximum number of rows to return per fetch call.
	TTIFetch TTIFuncCode = 0x05

	// TTIClose closes a cursor and releases its server-side resources.
	TTIClose TTIFuncCode = 0x08

	// TTICommit commits the current transaction, making all changes
	// permanent.
	TTICommit TTIFuncCode = 0x0E

	// TTIRollback rolls back the current transaction, discarding all
	// changes since the last commit.
	TTIRollback TTIFuncCode = 0x0F
)

// String returns the name of the TTI function code.
func (f TTIFuncCode) String() string {
	switch f {
	case TTIOpen:
		return "OPEN"
	case TTIExec:
		return "EXEC"
	case TTIFetch:
		return "FETCH"
	case TTIClose:
		return "CLOSE"
	case TTICommit:
		return "COMMIT"
	case TTIRollback:
		return "ROLLBACK"
	default:
		return "UNKNOWN"
	}
}

// OracleTypeCode identifies an Oracle data type in the TTI wire protocol.
// These appear in bind variable descriptors and column metadata.
type OracleTypeCode uint8

const (
	OracleTypeVarchar2     OracleTypeCode = 1
	OracleTypeNumber       OracleTypeCode = 2
	OracleTypeLong         OracleTypeCode = 8
	OracleTypeDate         OracleTypeCode = 12
	OracleTypeRaw          OracleTypeCode = 23
	OracleTypeLongRaw      OracleTypeCode = 24
	OracleTypeRowID        OracleTypeCode = 69
	OracleTypeChar         OracleTypeCode = 96
	OracleTypeBinaryFloat  OracleTypeCode = 100
	OracleTypeBinaryDouble OracleTypeCode = 101
	OracleTypeCLOB         OracleTypeCode = 112
	OracleTypeBLOB         OracleTypeCode = 113
)

// BindVar represents a single bind variable in a TTI EXEC message. The
// TypeCode identifies the Oracle type, and Value holds the raw wire-format
// bytes. A nil Value represents an SQL NULL.
type BindVar struct {
	TypeCode OracleTypeCode
	Value    []byte
}

// ColumnDesc describes a single result column returned in a TTI OPEN
// response. It carries the Oracle type and the column name as it appears
// in the query's select list.
type ColumnDesc struct {
	TypeCode OracleTypeCode
	Name     string
}

// TTIOpenMsg is the request to open a new cursor and prepare a SQL statement.
//
// Wire format:
//
//	Byte 0:       function code (0x03)
//	Bytes 1-2:    cursor ID (big-endian uint16)
//	Bytes 3-4:    SQL text length (big-endian uint16)
//	Bytes 5-N:    SQL text (UTF-8)
type TTIOpenMsg struct {
	CursorID uint16
	SQL      string
}

// TTIOpenResponse is the server's response to an OPEN request. It assigns
// a cursor ID and describes the columns that FETCH will return.
//
// Wire format:
//
//	Byte 0:       function code (0x03)
//	Bytes 1-2:    cursor ID (big-endian uint16)
//	Bytes 3-4:    number of columns (big-endian uint16)
//	Per column:
//	  Byte 0:       Oracle type code
//	  Bytes 1-2:    column name length (big-endian uint16)
//	  Bytes 3-N:    column name (UTF-8)
type TTIOpenResponse struct {
	CursorID uint16
	Columns  []ColumnDesc
}

// TTIExecMsg is the request to execute a prepared cursor. If SQL is non-empty,
// the server re-prepares the cursor with the new statement text before
// executing. BindVars carries positional parameter values.
//
// Wire format:
//
//	Byte 0:       function code (0x04)
//	Bytes 1-2:    cursor ID (big-endian uint16)
//	Bytes 3-4:    SQL text length (big-endian uint16, 0 if reusing)
//	Bytes 5-N:    SQL text (UTF-8, absent when length is 0)
//	Next 2 bytes: bind variable count (big-endian uint16)
//	Per bind variable:
//	  Byte 0:       Oracle type code
//	  Bytes 1-2:    value length (big-endian uint16, 0xFFFF = NULL)
//	  Bytes 3-N:    value bytes (absent for NULL)
type TTIExecMsg struct {
	CursorID uint16
	SQL      string
	BindVars []BindVar
}

// TTIExecResponse is the server's response to an EXEC request. RowsAffected
// reports the number of rows modified by DML statements. ErrorCode is zero
// on success; when non-zero, ErrorMsg carries a human-readable description.
//
// Wire format:
//
//	Byte 0:       function code (0x04)
//	Bytes 1-4:    rows affected (big-endian uint32)
//	Bytes 5-6:    error code (big-endian uint16, 0 = success)
//	Bytes 7-8:    error message length (big-endian uint16, present only if error code > 0)
//	Bytes 9-N:    error message (UTF-8, present only if error code > 0)
type TTIExecResponse struct {
	RowsAffected uint32
	ErrorCode    uint16
	ErrorMsg     string
}

// TTIFetchMsg is the request to fetch rows from an executed cursor.
// FetchSize specifies the maximum number of rows the server should return.
//
// Wire format:
//
//	Byte 0:       function code (0x05)
//	Bytes 1-2:    cursor ID (big-endian uint16)
//	Bytes 3-4:    fetch size (big-endian uint16)
type TTIFetchMsg struct {
	CursorID  uint16
	FetchSize uint16
}

// FetchFlags contains control bits in a TTI FETCH response.
type FetchFlags uint8

const (
	// FetchFlagMoreRows indicates that additional rows are available beyond
	// those returned in this fetch response.
	FetchFlagMoreRows FetchFlags = 0x01
)

// nullSentinel is the value length sentinel that indicates a SQL NULL column
// value in fetch response rows. Encoded as big-endian uint16.
const nullSentinel = 0xFFFF

// TTIFetchResponse carries result rows from a FETCH request. Each row is a
// slice of column values where a nil entry represents SQL NULL.
//
// Wire format:
//
//	Byte 0:       function code (0x05)
//	Bytes 1-2:    row count (big-endian uint16)
//	Byte 3:       flags (FetchFlags)
//	Per row, per column:
//	  Bytes 0-1:    value length (big-endian uint16, 0xFFFF = NULL)
//	  Bytes 2-N:    value bytes (absent for NULL)
type TTIFetchResponse struct {
	Rows  [][][]byte
	Flags FetchFlags
}

// TTICloseMsg is the request to close a cursor and release its resources.
//
// Wire format:
//
//	Byte 0:       function code (0x08)
//	Bytes 1-2:    cursor ID (big-endian uint16)
type TTICloseMsg struct {
	CursorID uint16
}
