package block

import "bytes"

// ColumnType ...
type ColumnType uint8

// ColumnType definitions.
const (
	ColumnTypeInvalid ColumnType = 0
	ColumnTypeBool    ColumnType = 1
	ColumnTypeInt8    ColumnType = 2
	ColumnTypeInt16   ColumnType = 3
	ColumnTypeInt32   ColumnType = 4
	ColumnTypeInt64   ColumnType = 5
	ColumnTypeFloat32 ColumnType = 6
	ColumnTypeFloat64 ColumnType = 7
	// TODO(peter): Should "bytes" be replaced with a bit indicating variable
	// width data that can be applied to any fixed-width data type? This would
	// allow modeling both []int8, []int64, and []float64.
	ColumnTypeBytes ColumnType = 8
	// TODO(peter): decimal, uuid, ipaddr, timestamp, time, timetz, duration,
	// collated string, tuple.
)

var columnTypeAlignment = []int32{
	ColumnTypeInvalid: 0,
	ColumnTypeBool:    1,
	ColumnTypeInt8:    1,
	ColumnTypeInt16:   2,
	ColumnTypeInt32:   4,
	ColumnTypeInt64:   8,
	ColumnTypeFloat32: 4,
	ColumnTypeFloat64: 8,
	ColumnTypeBytes:   1,
}

var columnTypeName = []string{
	ColumnTypeInvalid: "invalid",
	ColumnTypeBool:    "bool",
	ColumnTypeInt8:    "int8",
	ColumnTypeInt16:   "int16",
	ColumnTypeInt32:   "int32",
	ColumnTypeInt64:   "int64",
	ColumnTypeFloat32: "float32",
	ColumnTypeFloat64: "float64",
	ColumnTypeBytes:   "bytes",
}

var columnTypeWidth = []int32{
	ColumnTypeInvalid: 0,
	ColumnTypeBool:    1,
	ColumnTypeInt8:    1,
	ColumnTypeInt16:   2,
	ColumnTypeInt32:   4,
	ColumnTypeInt64:   8,
	ColumnTypeFloat32: 4,
	ColumnTypeFloat64: 8,
	ColumnTypeBytes:   -1,
}

// Alignment ...
func (t ColumnType) Alignment() int32 {
	return columnTypeAlignment[t]
}

// String ...
func (t ColumnType) String() string {
	return columnTypeName[t]
}

// Width ...
func (t ColumnType) Width() int32 {
	return columnTypeWidth[t]
}

// ColumnTypes ...
type ColumnTypes []ColumnType

func (c ColumnTypes) String() string {
	var buf bytes.Buffer
	for i := range c {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(c[i].String())
	}
	return buf.String()
}
