// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// DecodeTTIFuncCode reads the function code byte from the start of a DATA
// packet payload (after the 2-byte data flags have been stripped). The caller
// can then dispatch to the appropriate Decode* function.
func DecodeTTIFuncCode(payload []byte) (TTIFuncCode, error) {
	if len(payload) < 1 {
		return 0, errors.New("TTI payload is empty")
	}
	return TTIFuncCode(payload[0]), nil
}

// EncodeTTIOpen serializes a TTIOpenMsg into wire format.
func EncodeTTIOpen(msg TTIOpenMsg) []byte {
	sqlLen := len(msg.SQL)
	buf := make([]byte, 1+2+2+sqlLen)
	buf[0] = byte(TTIOpen)
	binary.BigEndian.PutUint16(buf[1:3], msg.CursorID)
	binary.BigEndian.PutUint16(buf[3:5], uint16(sqlLen))
	copy(buf[5:], msg.SQL)
	return buf
}

// DecodeTTIOpen decodes a TTIOpenMsg from wire format. The input must include
// the leading function code byte.
func DecodeTTIOpen(data []byte) (TTIOpenMsg, error) {
	// 1 func code + 2 cursor ID + 2 SQL length = 5 bytes minimum.
	const fixedSize = 5
	if len(data) < fixedSize {
		return TTIOpenMsg{}, errors.Newf(
			"TTI OPEN too short: %d bytes, need at least %d", len(data), fixedSize,
		)
	}
	cursorID := binary.BigEndian.Uint16(data[1:3])
	sqlLen := int(binary.BigEndian.Uint16(data[3:5]))
	if len(data) < fixedSize+sqlLen {
		return TTIOpenMsg{}, errors.Newf(
			"TTI OPEN SQL truncated: need %d bytes, have %d",
			fixedSize+sqlLen, len(data),
		)
	}
	return TTIOpenMsg{
		CursorID: cursorID,
		SQL:      string(data[5 : 5+sqlLen]),
	}, nil
}

// EncodeTTIOpenResponse serializes a TTIOpenResponse into wire format.
func EncodeTTIOpenResponse(resp TTIOpenResponse) []byte {
	// Calculate total size: 1 func + 2 cursor + 2 num columns + columns.
	size := 1 + 2 + 2
	for _, col := range resp.Columns {
		size += 1 + 2 + len(col.Name) // type code + name length + name
	}
	buf := make([]byte, size)
	buf[0] = byte(TTIOpen)
	binary.BigEndian.PutUint16(buf[1:3], resp.CursorID)
	binary.BigEndian.PutUint16(buf[3:5], uint16(len(resp.Columns)))
	off := 5
	for _, col := range resp.Columns {
		buf[off] = byte(col.TypeCode)
		off++
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(col.Name)))
		off += 2
		copy(buf[off:], col.Name)
		off += len(col.Name)
	}
	return buf
}

// DecodeTTIOpenResponse decodes a TTIOpenResponse from wire format.
func DecodeTTIOpenResponse(data []byte) (TTIOpenResponse, error) {
	const fixedSize = 5 // 1 func + 2 cursor + 2 num columns
	if len(data) < fixedSize {
		return TTIOpenResponse{}, errors.Newf(
			"TTI OPEN response too short: %d bytes, need at least %d",
			len(data), fixedSize,
		)
	}
	resp := TTIOpenResponse{
		CursorID: binary.BigEndian.Uint16(data[1:3]),
	}
	numCols := int(binary.BigEndian.Uint16(data[3:5]))
	resp.Columns = make([]ColumnDesc, numCols)
	off := 5
	for i := range numCols {
		if off >= len(data) {
			return TTIOpenResponse{}, errors.Newf(
				"TTI OPEN response truncated at column %d", i,
			)
		}
		typeCode := OracleTypeCode(data[off])
		off++
		if off+2 > len(data) {
			return TTIOpenResponse{}, errors.Newf(
				"TTI OPEN response truncated at column %d name length", i,
			)
		}
		nameLen := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+nameLen > len(data) {
			return TTIOpenResponse{}, errors.Newf(
				"TTI OPEN response truncated at column %d name", i,
			)
		}
		resp.Columns[i] = ColumnDesc{
			TypeCode: typeCode,
			Name:     string(data[off : off+nameLen]),
		}
		off += nameLen
	}
	return resp, nil
}

// EncodeTTIExec serializes a TTIExecMsg into wire format.
func EncodeTTIExec(msg TTIExecMsg) []byte {
	sqlLen := len(msg.SQL)
	// 1 func + 2 cursor + 2 SQL length + SQL + 2 bind count + bind vars.
	size := 1 + 2 + 2 + sqlLen + 2
	for _, bv := range msg.BindVars {
		if bv.Value == nil {
			size += 1 + 2 // type code + NULL sentinel
		} else {
			size += 1 + 2 + len(bv.Value) // type code + length + value
		}
	}
	buf := make([]byte, size)
	buf[0] = byte(TTIExec)
	binary.BigEndian.PutUint16(buf[1:3], msg.CursorID)
	binary.BigEndian.PutUint16(buf[3:5], uint16(sqlLen))
	copy(buf[5:5+sqlLen], msg.SQL)
	off := 5 + sqlLen
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(msg.BindVars)))
	off += 2
	for _, bv := range msg.BindVars {
		buf[off] = byte(bv.TypeCode)
		off++
		if bv.Value == nil {
			binary.BigEndian.PutUint16(buf[off:off+2], nullSentinel)
			off += 2
		} else {
			binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(bv.Value)))
			off += 2
			copy(buf[off:], bv.Value)
			off += len(bv.Value)
		}
	}
	return buf
}

// DecodeTTIExec decodes a TTIExecMsg from wire format.
func DecodeTTIExec(data []byte) (TTIExecMsg, error) {
	// 1 func + 2 cursor + 2 SQL length = 5 minimum before SQL.
	const fixedSize = 5
	if len(data) < fixedSize {
		return TTIExecMsg{}, errors.Newf(
			"TTI EXEC too short: %d bytes, need at least %d", len(data), fixedSize,
		)
	}
	msg := TTIExecMsg{
		CursorID: binary.BigEndian.Uint16(data[1:3]),
	}
	sqlLen := int(binary.BigEndian.Uint16(data[3:5]))
	off := 5
	if off+sqlLen > len(data) {
		return TTIExecMsg{}, errors.Newf(
			"TTI EXEC SQL truncated: need %d bytes at offset %d, have %d",
			sqlLen, off, len(data),
		)
	}
	msg.SQL = string(data[off : off+sqlLen])
	off += sqlLen

	// Bind variables.
	if off+2 > len(data) {
		return TTIExecMsg{}, errors.New("TTI EXEC truncated at bind variable count")
	}
	numBinds := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	msg.BindVars = make([]BindVar, numBinds)
	for i := range numBinds {
		if off+3 > len(data) {
			return TTIExecMsg{}, errors.Newf(
				"TTI EXEC truncated at bind variable %d", i,
			)
		}
		bv := BindVar{TypeCode: OracleTypeCode(data[off])}
		off++
		valLen := binary.BigEndian.Uint16(data[off : off+2])
		off += 2
		if valLen == nullSentinel {
			bv.Value = nil
		} else {
			if off+int(valLen) > len(data) {
				return TTIExecMsg{}, errors.Newf(
					"TTI EXEC bind variable %d value truncated", i,
				)
			}
			bv.Value = make([]byte, valLen)
			copy(bv.Value, data[off:off+int(valLen)])
			off += int(valLen)
		}
		msg.BindVars[i] = bv
	}
	return msg, nil
}

// EncodeTTIExecResponse serializes a TTIExecResponse into wire format.
func EncodeTTIExecResponse(resp TTIExecResponse) []byte {
	// 1 func + 4 rows affected + 2 error code = 7 minimum.
	size := 7
	if resp.ErrorCode != 0 {
		size += 2 + len(resp.ErrorMsg) // error msg length + error msg
	}
	buf := make([]byte, size)
	buf[0] = byte(TTIExec)
	binary.BigEndian.PutUint32(buf[1:5], resp.RowsAffected)
	binary.BigEndian.PutUint16(buf[5:7], resp.ErrorCode)
	if resp.ErrorCode != 0 {
		binary.BigEndian.PutUint16(buf[7:9], uint16(len(resp.ErrorMsg)))
		copy(buf[9:], resp.ErrorMsg)
	}
	return buf
}

// DecodeTTIExecResponse decodes a TTIExecResponse from wire format.
func DecodeTTIExecResponse(data []byte) (TTIExecResponse, error) {
	const fixedSize = 7 // 1 func + 4 rows + 2 error code
	if len(data) < fixedSize {
		return TTIExecResponse{}, errors.Newf(
			"TTI EXEC response too short: %d bytes, need at least %d",
			len(data), fixedSize,
		)
	}
	resp := TTIExecResponse{
		RowsAffected: binary.BigEndian.Uint32(data[1:5]),
		ErrorCode:    binary.BigEndian.Uint16(data[5:7]),
	}
	if resp.ErrorCode != 0 {
		if len(data) < fixedSize+2 {
			return TTIExecResponse{}, errors.New(
				"TTI EXEC response truncated at error message length",
			)
		}
		msgLen := int(binary.BigEndian.Uint16(data[7:9]))
		if len(data) < fixedSize+2+msgLen {
			return TTIExecResponse{}, errors.New(
				"TTI EXEC response error message truncated",
			)
		}
		resp.ErrorMsg = string(data[9 : 9+msgLen])
	}
	return resp, nil
}

// EncodeTTIFetch serializes a TTIFetchMsg into wire format.
func EncodeTTIFetch(msg TTIFetchMsg) []byte {
	buf := make([]byte, 5) // 1 func + 2 cursor + 2 fetch size
	buf[0] = byte(TTIFetch)
	binary.BigEndian.PutUint16(buf[1:3], msg.CursorID)
	binary.BigEndian.PutUint16(buf[3:5], msg.FetchSize)
	return buf
}

// DecodeTTIFetch decodes a TTIFetchMsg from wire format.
func DecodeTTIFetch(data []byte) (TTIFetchMsg, error) {
	const fixedSize = 5 // 1 func + 2 cursor + 2 fetch size
	if len(data) < fixedSize {
		return TTIFetchMsg{}, errors.Newf(
			"TTI FETCH too short: %d bytes, need at least %d", len(data), fixedSize,
		)
	}
	return TTIFetchMsg{
		CursorID:  binary.BigEndian.Uint16(data[1:3]),
		FetchSize: binary.BigEndian.Uint16(data[3:5]),
	}, nil
}

// EncodeTTIFetchResponse serializes a TTIFetchResponse into wire format.
// The caller must provide numCols so the encoder knows how many column values
// to expect per row.
func EncodeTTIFetchResponse(resp TTIFetchResponse, numCols int) []byte {
	// 1 func + 2 row count + 1 flags = 4.
	size := 4
	for _, row := range resp.Rows {
		for _, val := range row {
			if val == nil {
				size += 2 // NULL sentinel
			} else {
				size += 2 + len(val)
			}
		}
	}
	buf := make([]byte, size)
	buf[0] = byte(TTIFetch)
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(resp.Rows)))
	buf[3] = byte(resp.Flags)
	off := 4
	for _, row := range resp.Rows {
		for _, val := range row {
			if val == nil {
				binary.BigEndian.PutUint16(buf[off:off+2], nullSentinel)
				off += 2
			} else {
				binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(val)))
				off += 2
				copy(buf[off:], val)
				off += len(val)
			}
		}
	}
	return buf
}

// DecodeTTIFetchResponse decodes a TTIFetchResponse from wire format. The
// caller must provide numCols (from the preceding OPEN response) so the
// decoder knows where row boundaries fall.
func DecodeTTIFetchResponse(data []byte, numCols int) (TTIFetchResponse, error) {
	const fixedSize = 4 // 1 func + 2 row count + 1 flags
	if len(data) < fixedSize {
		return TTIFetchResponse{}, errors.Newf(
			"TTI FETCH response too short: %d bytes, need at least %d",
			len(data), fixedSize,
		)
	}
	numRows := int(binary.BigEndian.Uint16(data[1:3]))
	resp := TTIFetchResponse{
		Flags: FetchFlags(data[3]),
		Rows:  make([][][]byte, numRows),
	}
	off := 4
	for i := range numRows {
		row := make([][]byte, numCols)
		for j := range numCols {
			if off+2 > len(data) {
				return TTIFetchResponse{}, errors.Newf(
					"TTI FETCH response truncated at row %d, column %d", i, j,
				)
			}
			valLen := binary.BigEndian.Uint16(data[off : off+2])
			off += 2
			if valLen == nullSentinel {
				row[j] = nil
			} else {
				if off+int(valLen) > len(data) {
					return TTIFetchResponse{}, errors.Newf(
						"TTI FETCH response value truncated at row %d, column %d",
						i, j,
					)
				}
				row[j] = make([]byte, valLen)
				copy(row[j], data[off:off+int(valLen)])
				off += int(valLen)
			}
		}
		resp.Rows[i] = row
	}
	return resp, nil
}

// EncodeTTIClose serializes a TTICloseMsg into wire format.
func EncodeTTIClose(msg TTICloseMsg) []byte {
	buf := make([]byte, 3) // 1 func + 2 cursor
	buf[0] = byte(TTIClose)
	binary.BigEndian.PutUint16(buf[1:3], msg.CursorID)
	return buf
}

// DecodeTTIClose decodes a TTICloseMsg from wire format.
func DecodeTTIClose(data []byte) (TTICloseMsg, error) {
	const fixedSize = 3 // 1 func + 2 cursor
	if len(data) < fixedSize {
		return TTICloseMsg{}, errors.Newf(
			"TTI CLOSE too short: %d bytes, need at least %d", len(data), fixedSize,
		)
	}
	return TTICloseMsg{
		CursorID: binary.BigEndian.Uint16(data[1:3]),
	}, nil
}

// EncodeTTICommit serializes a COMMIT message into wire format. COMMIT has no
// fields beyond the function code.
func EncodeTTICommit() []byte {
	return []byte{byte(TTICommit)}
}

// DecodeTTICommit validates that data contains a COMMIT message. COMMIT has no
// fields beyond the function code byte.
func DecodeTTICommit(data []byte) error {
	if len(data) < 1 {
		return errors.New("TTI COMMIT too short: 0 bytes, need at least 1")
	}
	return nil
}

// EncodeTTIRollback serializes a ROLLBACK message into wire format. ROLLBACK
// has no fields beyond the function code.
func EncodeTTIRollback() []byte {
	return []byte{byte(TTIRollback)}
}

// DecodeTTIRollback validates that data contains a ROLLBACK message. ROLLBACK
// has no fields beyond the function code byte.
func DecodeTTIRollback(data []byte) error {
	if len(data) < 1 {
		return errors.New("TTI ROLLBACK too short: 0 bytes, need at least 1")
	}
	return nil
}
