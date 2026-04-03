// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package types provides mapping between TDS (Tabular Data Stream) wire types
// used by Sybase/MS-SQL and CockroachDB's internal type system. It handles
// the bidirectional conversion of type metadata and the encoding/decoding of
// column values in the TDS binary format.
//
// TDS uses single-byte type IDs on the wire. Some types are "fixed-length"
// (e.g. INT4TYPE = 0x38 is always 4 bytes) while others are "nullable"
// variable-length variants (e.g. INTNTYPE = 0x26 carries a length byte that
// determines the actual integer width). This package maps both forms to the
// corresponding CockroachDB *types.T.
//
// # Dialect note
//
// The TDS wire protocol is shared by both SQL Server and Sybase ASE. The
// type IDs defined here are used by both dialects and are part of the wire
// protocol specification, not dialect-specific SQL syntax. The SQL-level
// type names that map to these wire types differ between dialects (see the
// translate package's mapDataType for those mappings).
package types

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TDS wire type IDs. These are the single-byte tokens that appear in
// COLMETADATA and ROW tokens on the TDS wire.
//
// Fixed-length types encode their value directly. Nullable (N-suffix) types
// carry an additional length byte that doubles as a null indicator (0 = NULL).
const (
	// Fixed-length integer types.
	Int1Type TDSTypeID = 0x30 // 1-byte unsigned int (TINYINT)
	Int2Type TDSTypeID = 0x34 // 2-byte signed int (SMALLINT)
	Int4Type TDSTypeID = 0x38 // 4-byte signed int (INT)
	Int8Type TDSTypeID = 0x7F // 8-byte signed int (BIGINT)

	// Nullable integer type. The wire length byte (1/2/4/8) determines the
	// actual width; a length of 0 signals NULL.
	IntNType TDSTypeID = 0x26

	// Fixed-length float types.
	Float4Type TDSTypeID = 0x3B // 4-byte IEEE 754 (REAL)
	Float8Type TDSTypeID = 0x3E // 8-byte IEEE 754 (FLOAT)

	// Nullable float type. Wire length 4 or 8.
	FloatNType TDSTypeID = 0x6D

	// Bit types.
	BitType  TDSTypeID = 0x32 // fixed 1-byte bool
	BitNType TDSTypeID = 0x68 // nullable bool

	// Datetime types.
	DateTimeType  TDSTypeID = 0x3D // 8-byte datetime (DATETIME)
	DateTime4Type TDSTypeID = 0x3A // 4-byte datetime (SMALLDATETIME)
	DateTimeNType TDSTypeID = 0x6F // nullable datetime

	DateNType TDSTypeID = 0x28 // DATE (3 bytes, days since 0001-01-01)
	TimeNType TDSTypeID = 0x29 // TIME (3-5 bytes, variable scale)

	// Money types.
	MoneyType  TDSTypeID = 0x3C // 8-byte MONEY
	Money4Type TDSTypeID = 0x7A // 4-byte SMALLMONEY
	MoneyNType TDSTypeID = 0x6E // nullable money

	// Variable-length character types. These carry a 2-byte max-length prefix
	// in COLMETADATA and a 2-byte actual-length prefix per value.
	BigVarCharType TDSTypeID = 0xA7 // VARCHAR
	BigCharType    TDSTypeID = 0xAF // CHAR
	BigVarBinType  TDSTypeID = 0xA5 // VARBINARY
	BigBinaryType  TDSTypeID = 0xAD // BINARY
	NVarCharType   TDSTypeID = 0xE7 // NVARCHAR (UTF-16 on wire)
	NCharType      TDSTypeID = 0xEF // NCHAR (UTF-16 on wire)
	TextType       TDSTypeID = 0x23 // TEXT (legacy LOB)
	NTextType      TDSTypeID = 0x63 // NTEXT (legacy LOB, UTF-16)
	ImageType      TDSTypeID = 0x22 // IMAGE (legacy binary LOB)

	// Numeric/Decimal types.
	NumericNType TDSTypeID = 0x6C // NUMERIC with precision + scale
	DecimalNType TDSTypeID = 0x6A // DECIMAL with precision + scale

	// GUID type.
	GUIDType TDSTypeID = 0x24 // UNIQUEIDENTIFIER (16 bytes, nullable)
)

// TDSTypeID is the single-byte type identifier used on the TDS wire.
type TDSTypeID byte

// TypeInfo describes a TDS column's type and how to map it to a CockroachDB
// type. It is populated from COLMETADATA tokens and used to drive row
// encoding and decoding.
type TypeInfo struct {
	// TDSType is the wire type ID from COLMETADATA.
	TDSType TDSTypeID

	// CRDBType is the corresponding CockroachDB type.
	CRDBType *types.T

	// MaxLength is the maximum byte length declared in COLMETADATA for
	// variable-length types. For fixed-length types this is the fixed size.
	MaxLength int

	// Precision and Scale are set for NUMERIC/DECIMAL types.
	Precision byte
	Scale     byte
}

// CRDBToTDS returns a TypeInfo that maps the given CockroachDB type to its
// closest TDS wire representation. The returned TypeInfo includes the TDS
// type ID and the wire-format max length suitable for COLMETADATA emission.
func CRDBToTDS(t *types.T) (TypeInfo, error) {
	switch t.Family() {
	case types.BoolFamily:
		return TypeInfo{TDSType: BitNType, CRDBType: types.Bool, MaxLength: 1}, nil

	case types.IntFamily:
		switch t.Width() {
		case 16:
			return TypeInfo{TDSType: IntNType, CRDBType: types.Int2, MaxLength: 2}, nil
		case 32:
			return TypeInfo{TDSType: IntNType, CRDBType: types.Int4, MaxLength: 4}, nil
		default: // 64 or unspecified
			return TypeInfo{TDSType: IntNType, CRDBType: types.Int, MaxLength: 8}, nil
		}

	case types.FloatFamily:
		if t.Width() == 32 {
			return TypeInfo{TDSType: FloatNType, CRDBType: types.Float4, MaxLength: 4}, nil
		}
		return TypeInfo{TDSType: FloatNType, CRDBType: types.Float, MaxLength: 8}, nil

	case types.DecimalFamily:
		p := byte(t.Precision())
		s := byte(t.Scale())
		if p == 0 {
			p = 38 // TDS max precision
		}
		return TypeInfo{
			TDSType:   DecimalNType,
			CRDBType:  types.MakeDecimal(int32(p), int32(s)),
			MaxLength: decimalLength(p),
			Precision: p,
			Scale:     s,
		}, nil

	case types.StringFamily:
		maxLen := 8000
		if w := int(t.Width()); w > 0 && w < maxLen {
			maxLen = w
		}
		return TypeInfo{TDSType: NVarCharType, CRDBType: t, MaxLength: maxLen * 2}, nil

	case types.BytesFamily:
		return TypeInfo{TDSType: BigVarBinType, CRDBType: types.Bytes, MaxLength: 8000}, nil

	case types.DateFamily:
		return TypeInfo{TDSType: DateNType, CRDBType: types.Date, MaxLength: 3}, nil

	case types.TimeFamily:
		return TypeInfo{TDSType: TimeNType, CRDBType: types.Time, MaxLength: 5}, nil

	case types.TimestampFamily, types.TimestampTZFamily:
		return TypeInfo{TDSType: DateTimeNType, CRDBType: types.Timestamp, MaxLength: 8}, nil

	case types.UuidFamily:
		return TypeInfo{TDSType: GUIDType, CRDBType: types.Uuid, MaxLength: 16}, nil

	default:
		return TypeInfo{}, errors.Newf("unsupported CockroachDB type for TDS: %s", t)
	}
}

// TDSToCRDB converts a TDS type ID and associated metadata into a CockroachDB
// type. For nullable integer/float types, maxLen determines the width.
func TDSToCRDB(id TDSTypeID, maxLen int, precision, scale byte) (*types.T, error) {
	switch id {
	// Fixed-length integers.
	case Int1Type:
		return types.Int2, nil // TINYINT -> INT2 (no unsigned int in CRDB)
	case Int2Type:
		return types.Int2, nil
	case Int4Type:
		return types.Int4, nil
	case Int8Type:
		return types.Int, nil

	// Nullable integer — width is determined by maxLen.
	case IntNType:
		switch maxLen {
		case 1:
			return types.Int2, nil
		case 2:
			return types.Int2, nil
		case 4:
			return types.Int4, nil
		case 8:
			return types.Int, nil
		default:
			return nil, errors.Newf("unsupported INTNTYPE length %d", maxLen)
		}

	// Fixed-length floats.
	case Float4Type:
		return types.Float4, nil
	case Float8Type:
		return types.Float, nil

	// Nullable float.
	case FloatNType:
		switch maxLen {
		case 4:
			return types.Float4, nil
		case 8:
			return types.Float, nil
		default:
			return nil, errors.Newf("unsupported FLTNTYPE length %d", maxLen)
		}

	// Bit types.
	case BitType, BitNType:
		return types.Bool, nil

	// Character types — all map to STRING.
	case BigVarCharType, BigCharType, TextType:
		return types.String, nil
	case NVarCharType, NCharType, NTextType:
		return types.String, nil

	// Binary types — all map to BYTES.
	case BigVarBinType, BigBinaryType, ImageType:
		return types.Bytes, nil

	// Datetime types.
	case DateTimeType, DateTime4Type, DateTimeNType:
		return types.Timestamp, nil
	case DateNType:
		return types.Date, nil
	case TimeNType:
		return types.Time, nil

	// Money types — map to DECIMAL with appropriate precision/scale.
	case MoneyType:
		return types.MakeDecimal(19, 4), nil
	case Money4Type:
		return types.MakeDecimal(10, 4), nil
	case MoneyNType:
		if maxLen == 4 {
			return types.MakeDecimal(10, 4), nil
		}
		return types.MakeDecimal(19, 4), nil

	// Numeric/Decimal.
	case NumericNType, DecimalNType:
		return types.MakeDecimal(int32(precision), int32(scale)), nil

	// GUID.
	case GUIDType:
		return types.Uuid, nil

	default:
		return nil, errors.Newf("unsupported TDS type ID 0x%02X", byte(id))
	}
}

// decimalLength returns the number of bytes needed to encode a TDS DECIMAL
// value with the given precision, per the TDS specification.
func decimalLength(precision byte) int {
	switch {
	case precision <= 9:
		return 5 // 1 sign + 4 data
	case precision <= 19:
		return 9 // 1 sign + 8 data
	case precision <= 28:
		return 13 // 1 sign + 12 data
	default:
		return 17 // 1 sign + 16 data
	}
}

// IsFixedLength returns true for TDS type IDs that always produce the same
// number of bytes on the wire (no length prefix per value).
func IsFixedLength(id TDSTypeID) bool {
	switch id {
	case Int1Type, Int2Type, Int4Type, Int8Type,
		Float4Type, Float8Type,
		BitType,
		DateTimeType, DateTime4Type,
		MoneyType, Money4Type:
		return true
	default:
		return false
	}
}

// FixedSize returns the fixed byte size for a fixed-length TDS type. It panics
// if called on a variable-length type.
func FixedSize(id TDSTypeID) int {
	switch id {
	case Int1Type, BitType:
		return 1
	case Int2Type:
		return 2
	case Int4Type:
		return 4
	case Int8Type, Float8Type, DateTimeType, MoneyType:
		return 8
	case Float4Type, DateTime4Type, Money4Type:
		return 4
	default:
		panic(errors.AssertionFailedf(
			"FixedSize called on variable-length TDS type 0x%02X", byte(id),
		))
	}
}

// tdsEpoch is the TDS datetime epoch: January 1, 1900.
var tdsEpoch = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// tdsDateEpochJulian is the Julian day number for January 1, year 1
// (the TDS DATE type epoch). Used to compute day counts without
// overflowing time.Duration for dates far from Go's time zero.
var tdsDateEpochJulian = julianDay(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))

// EncodeValue encodes a Go value into TDS binary wire format and appends it to
// dst. It returns the extended slice. A nil value encodes as the appropriate
// NULL representation for the type (length 0 for nullable types).
//
// Supported value types:
//   - nil: NULL
//   - bool: BIT
//   - int64: INT (width from TypeInfo.MaxLength)
//   - float64: FLOAT
//   - string: VARCHAR / NVARCHAR
//   - []byte: VARBINARY
//   - time.Time: DATETIME / DATE / TIME
//   - *apd.Decimal: NUMERIC / DECIMAL / MONEY
//   - uuid.UUID: UNIQUEIDENTIFIER
func EncodeValue(dst []byte, ti TypeInfo, val interface{}) ([]byte, error) {
	if val == nil {
		return encodeNull(dst, ti), nil
	}
	switch ti.TDSType {
	case BitType:
		return encodeBool(dst, val, false /* nullable */)
	case BitNType:
		return encodeBool(dst, val, true /* nullable */)

	case Int1Type, Int2Type, Int4Type, Int8Type:
		return encodeFixedInt(dst, val, FixedSize(ti.TDSType))

	case IntNType:
		return encodeNullableInt(dst, val, ti.MaxLength)

	case Float4Type, Float8Type:
		return encodeFixedFloat(dst, val, FixedSize(ti.TDSType))

	case FloatNType:
		return encodeNullableFloat(dst, val, ti.MaxLength)

	case BigVarCharType, BigCharType, TextType:
		return encodeVarChar(dst, val)

	case NVarCharType, NCharType, NTextType:
		return encodeNVarChar(dst, val)

	case BigVarBinType, BigBinaryType, ImageType:
		return encodeVarBin(dst, val)

	case DateTimeType:
		return encodeDateTime(dst, val)
	case DateTime4Type:
		return encodeSmallDateTime(dst, val)
	case DateTimeNType:
		return encodeNullableDateTime(dst, val, ti.MaxLength)
	case DateNType:
		return encodeDate(dst, val)
	case TimeNType:
		return encodeTime(dst, val)

	case MoneyType:
		return encodeMoney(dst, val, 8)
	case Money4Type:
		return encodeMoney(dst, val, 4)
	case MoneyNType:
		return encodeNullableMoney(dst, val, ti.MaxLength)

	case NumericNType, DecimalNType:
		return encodeDecimal(dst, val, ti.Precision, ti.Scale)

	case GUIDType:
		return encodeGUID(dst, val)

	default:
		return nil, errors.Newf("encode: unsupported TDS type 0x%02X", byte(ti.TDSType))
	}
}

// DecodeValue reads a single TDS-encoded value from data and returns the
// decoded Go value plus the number of bytes consumed. It returns nil for NULL.
//
// Return types by TDS type:
//   - INT: int64
//   - FLOAT: float64
//   - BIT: bool
//   - VARCHAR/NVARCHAR/TEXT: string
//   - VARBINARY/IMAGE: []byte
//   - DATETIME/DATE/TIME: time.Time
//   - NUMERIC/DECIMAL/MONEY: *apd.Decimal
//   - GUID: uuid.UUID
func DecodeValue(data []byte, ti TypeInfo) (interface{}, int, error) {
	if IsFixedLength(ti.TDSType) {
		return decodeFixed(data, ti)
	}
	return decodeVariable(data, ti)
}

// encodeNull returns nil to signal NULL. The per-row NULL sentinel is
// written by writeRowValue in the tdswire package when it sees a nil
// value slice.
func encodeNull(dst []byte, ti TypeInfo) []byte {
	_ = ti
	return nil
}

func encodeBool(dst []byte, val interface{}, nullable bool) ([]byte, error) {
	// Note: the per-row length prefix for nullable types is written by
	// writeRowValue in the tdswire package; we only produce raw value bytes.
	switch v := val.(type) {
	case bool:
		if v {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case int64:
		// BIT→INT coercion: accept integer values (0 = false, nonzero = true).
		if v != 0 {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	default:
		return nil, errors.Newf("expected bool or int64, got %T", val)
	}
}

func encodeFixedInt(dst []byte, val interface{}, size int) ([]byte, error) {
	v, ok := val.(int64)
	if !ok {
		return nil, errors.Newf("expected int64, got %T", val)
	}
	return appendIntLE(dst, v, size), nil
}

func encodeNullableInt(dst []byte, val interface{}, maxLen int) ([]byte, error) {
	v, ok := val.(int64)
	if !ok {
		return nil, errors.Newf("expected int64, got %T", val)
	}
	return appendIntLE(dst, v, maxLen), nil
}

func encodeFixedFloat(dst []byte, val interface{}, size int) ([]byte, error) {
	v, ok := val.(float64)
	if !ok {
		return nil, errors.Newf("expected float64, got %T", val)
	}
	return appendFloatLE(dst, v, size), nil
}

func encodeNullableFloat(dst []byte, val interface{}, maxLen int) ([]byte, error) {
	v, ok := val.(float64)
	if !ok {
		return nil, errors.Newf("expected float64, got %T", val)
	}
	return appendFloatLE(dst, v, maxLen), nil
}

func encodeVarChar(dst []byte, val interface{}) ([]byte, error) {
	s, ok := val.(string)
	if !ok {
		return nil, errors.Newf("expected string, got %T", val)
	}
	// Note: the per-row 2-byte length prefix for variable-length types
	// is written by writeRowValue in the tdswire package; we only
	// produce raw value bytes.
	return append(dst, []byte(s)...), nil
}

func encodeNVarChar(dst []byte, val interface{}) ([]byte, error) {
	s, ok := val.(string)
	if !ok {
		return nil, errors.Newf("expected string, got %T", val)
	}
	// TDS NVARCHAR uses UCS-2/UTF-16LE on the wire. For the BMP (which covers
	// all practical SQL identifiers and data), each rune is 2 bytes LE.
	// Note: the per-row 2-byte length prefix is written by writeRowValue
	// in the tdswire package; we only produce raw value bytes.
	encoded := encodeUTF16LE(s)
	return append(dst, encoded...), nil
}

func encodeVarBin(dst []byte, val interface{}) ([]byte, error) {
	b, ok := val.([]byte)
	if !ok {
		return nil, errors.Newf("expected []byte, got %T", val)
	}
	// Note: the per-row 2-byte length prefix is written by writeRowValue
	// in the tdswire package; we only produce raw value bytes.
	return append(dst, b...), nil
}

func encodeDateTime(dst []byte, val interface{}) ([]byte, error) {
	t, ok := val.(time.Time)
	if !ok {
		return nil, errors.Newf("expected time.Time, got %T", val)
	}
	t = t.UTC()
	// TDS DATETIME: 4-byte day count since 1900-01-01, then 4-byte count of
	// 1/300th-second intervals since midnight.
	days := int32(t.Sub(tdsEpoch).Hours() / 24)
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	threeHundredths := int32(t.Sub(midnight).Seconds() * 300)
	dst = appendIntLE(dst, int64(days), 4)
	return appendIntLE(dst, int64(threeHundredths), 4), nil
}

func encodeSmallDateTime(dst []byte, val interface{}) ([]byte, error) {
	t, ok := val.(time.Time)
	if !ok {
		return nil, errors.Newf("expected time.Time, got %T", val)
	}
	t = t.UTC()
	days := uint16(t.Sub(tdsEpoch).Hours() / 24)
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	minutes := uint16(t.Sub(midnight).Minutes())
	dst = appendUint16LE(dst, days)
	return appendUint16LE(dst, minutes), nil
}

func encodeNullableDateTime(dst []byte, val interface{}, maxLen int) ([]byte, error) {
	t, ok := val.(time.Time)
	if !ok {
		return nil, errors.Newf("expected time.Time, got %T", val)
	}
	if maxLen == 4 {
		return encodeSmallDateTime(dst, t)
	}
	return encodeDateTime(dst, t)
}

func encodeDate(dst []byte, val interface{}) ([]byte, error) {
	t, ok := val.(time.Time)
	if !ok {
		return nil, errors.Newf("expected time.Time, got %T", val)
	}
	t = t.UTC()
	// TDS DATE: 3-byte unsigned int, days since 0001-01-01.
	days := julianDay(t) - tdsDateEpochJulian
	dst = append(dst, byte(days), byte(days>>8), byte(days>>16))
	return dst, nil
}

func encodeTime(dst []byte, val interface{}) ([]byte, error) {
	t, ok := val.(time.Time)
	if !ok {
		return nil, errors.Newf("expected time.Time, got %T", val)
	}
	// TDS TIME with scale 7 (100-nanosecond intervals): 5 bytes.
	midnight := time.Date(
		t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location(),
	)
	ticks := t.Sub(midnight).Nanoseconds() / 100
	dst = append(dst,
		byte(ticks), byte(ticks>>8), byte(ticks>>16),
		byte(ticks>>24), byte(ticks>>32))
	return dst, nil
}

func encodeMoney(dst []byte, val interface{}, size int) ([]byte, error) {
	d, ok := val.(*apd.Decimal)
	if !ok {
		return nil, errors.Newf("expected *apd.Decimal, got %T", val)
	}
	// MONEY is stored as a scaled int64 (units of 1/10000).
	scaled := apdToScaledInt64(d, 4)
	if size == 4 {
		return appendIntLE(dst, scaled, 4), nil
	}
	// 8-byte MONEY: high 4 bytes first, then low 4 bytes (TDS-specific layout).
	hi := int32(scaled >> 32)
	lo := int32(scaled)
	dst = appendIntLE(dst, int64(hi), 4)
	return appendIntLE(dst, int64(lo), 4), nil
}

func encodeNullableMoney(dst []byte, val interface{}, maxLen int) ([]byte, error) {
	return encodeMoney(dst, val, maxLen)
}

func encodeDecimal(dst []byte, val interface{}, precision, scale byte) ([]byte, error) {
	d, ok := val.(*apd.Decimal)
	if !ok {
		return nil, errors.Newf("expected *apd.Decimal, got %T", val)
	}
	totalLen := decimalLength(precision)

	// Sign byte: 1 = positive, 0 = negative.
	if d.Negative {
		dst = append(dst, 0)
	} else {
		dst = append(dst, 1)
	}

	// Produce the absolute unscaled integer at the target scale.
	// For example, 12345.67 with scale=2 becomes 1234567.
	abs := new(apd.Decimal).Abs(d)
	b := apdToScaledBytes(abs, int32(scale)) // big-endian

	// Encode as little-endian bytes.
	dataLen := totalLen - 1 // subtract sign byte
	buf := make([]byte, dataLen)
	for i, j := 0, len(b)-1; j >= 0 && i < dataLen; i, j = i+1, j-1 {
		buf[i] = b[j]
	}
	return append(dst, buf...), nil
}

func encodeGUID(dst []byte, val interface{}) ([]byte, error) {
	u, ok := val.(uuid.UUID)
	if !ok {
		return nil, errors.Newf("expected uuid.UUID, got %T", val)
	}
	// TDS GUIDs use mixed-endian encoding: the first three groups are
	// little-endian, the last two are big-endian. CockroachDB UUIDs store
	// the raw RFC 4122 bytes, so we re-order for TDS.
	b := u.GetBytes()
	// Group 1 (4 bytes LE), Group 2 (2 bytes LE), Group 3 (2 bytes LE),
	// Groups 4+5 (8 bytes BE, already in order).
	dst = append(dst, b[3], b[2], b[1], b[0]) // group 1 reversed
	dst = append(dst, b[5], b[4])             // group 2 reversed
	dst = append(dst, b[7], b[6])             // group 3 reversed
	dst = append(dst, b[8:]...)               // groups 4+5 as-is
	return dst, nil
}

// --- Decode helpers ---

func decodeFixed(data []byte, ti TypeInfo) (interface{}, int, error) {
	size := FixedSize(ti.TDSType)
	if len(data) < size {
		return nil, 0, errors.New("insufficient data for fixed-length TDS value")
	}
	switch ti.TDSType {
	case BitType:
		return data[0] != 0, 1, nil
	case Int1Type:
		return int64(data[0]), 1, nil
	case Int2Type:
		return int64(int16(binary.LittleEndian.Uint16(data[:2]))), 2, nil
	case Int4Type:
		return int64(int32(binary.LittleEndian.Uint32(data[:4]))), 4, nil
	case Int8Type:
		return int64(binary.LittleEndian.Uint64(data[:8])), 8, nil
	case Float4Type:
		v := math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))
		return float64(v), 4, nil
	case Float8Type:
		v := math.Float64frombits(binary.LittleEndian.Uint64(data[:8]))
		return v, 8, nil
	case DateTimeType:
		return decodeDateTimeBytes(data[:8])
	case DateTime4Type:
		return decodeSmallDateTimeBytes(data[:4])
	case MoneyType:
		return decodeMoneyBytes(data[:8], 8)
	case Money4Type:
		return decodeMoneyBytes(data[:4], 4)
	default:
		return nil, 0, errors.Newf(
			"decode: unhandled fixed type 0x%02X", byte(ti.TDSType),
		)
	}
}

func decodeVariable(data []byte, ti TypeInfo) (interface{}, int, error) {
	switch ti.TDSType {
	case IntNType, FloatNType, BitNType, DateTimeNType, MoneyNType, GUIDType,
		NumericNType, DecimalNType, DateNType, TimeNType:
		return decodeTokenByte(data, ti)

	case BigVarCharType, BigCharType, TextType:
		return decodeVarCharValue(data)
	case NVarCharType, NCharType, NTextType:
		return decodeNVarCharValue(data)
	case BigVarBinType, BigBinaryType, ImageType:
		return decodeVarBinValue(data)

	default:
		return nil, 0, errors.Newf(
			"decode: unhandled variable type 0x%02X", byte(ti.TDSType),
		)
	}
}

// decodeTokenByte decodes types that use a 1-byte length prefix (0 = NULL).
func decodeTokenByte(data []byte, ti TypeInfo) (interface{}, int, error) {
	if len(data) < 1 {
		return nil, 0, errors.New("insufficient data for length byte")
	}
	length := int(data[0])
	if length == 0 {
		return nil, 1, nil // NULL
	}
	if len(data) < 1+length {
		return nil, 0, errors.Newf(
			"insufficient data: need %d bytes, have %d", 1+length, len(data),
		)
	}
	payload := data[1 : 1+length]
	consumed := 1 + length

	switch ti.TDSType {
	case IntNType:
		switch length {
		case 1:
			return int64(payload[0]), consumed, nil
		case 2:
			v := int16(binary.LittleEndian.Uint16(payload))
			return int64(v), consumed, nil
		case 4:
			v := int32(binary.LittleEndian.Uint32(payload))
			return int64(v), consumed, nil
		case 8:
			return int64(binary.LittleEndian.Uint64(payload)), consumed, nil
		}
	case FloatNType:
		switch length {
		case 4:
			v := math.Float32frombits(binary.LittleEndian.Uint32(payload))
			return float64(v), consumed, nil
		case 8:
			v := math.Float64frombits(binary.LittleEndian.Uint64(payload))
			return v, consumed, nil
		}
	case BitNType:
		return payload[0] != 0, consumed, nil
	case DateTimeNType:
		if length == 4 {
			v, n, err := decodeSmallDateTimeBytes(payload)
			return v, 1 + n, err
		}
		v, n, err := decodeDateTimeBytes(payload)
		return v, 1 + n, err
	case MoneyNType:
		v, n, err := decodeMoneyBytes(payload, length)
		return v, 1 + n, err
	case GUIDType:
		v, _, err := decodeGUIDBytes(payload)
		return v, consumed, err
	case NumericNType, DecimalNType:
		v, _, err := decodeDecimalBytes(payload, ti.Scale)
		return v, consumed, err
	case DateNType:
		v, _, err := decodeDateBytes(payload)
		return v, consumed, err
	case TimeNType:
		v, _, err := decodeTimeBytes(payload)
		return v, consumed, err
	}
	return nil, 0, errors.Newf(
		"decode: unhandled nullable type 0x%02X len=%d",
		byte(ti.TDSType), length,
	)
}

func decodeVarCharValue(data []byte) (interface{}, int, error) {
	if len(data) < 2 {
		return nil, 0, errors.New("insufficient data for varchar length")
	}
	length := int(binary.LittleEndian.Uint16(data[:2]))
	if length == 0xFFFF {
		return nil, 2, nil // NULL
	}
	if len(data) < 2+length {
		return nil, 0, errors.Newf(
			"insufficient data for varchar: need %d, have %d",
			2+length, len(data),
		)
	}
	return string(data[2 : 2+length]), 2 + length, nil
}

func decodeNVarCharValue(data []byte) (interface{}, int, error) {
	if len(data) < 2 {
		return nil, 0, errors.New("insufficient data for nvarchar length")
	}
	length := int(binary.LittleEndian.Uint16(data[:2]))
	if length == 0xFFFF {
		return nil, 2, nil // NULL
	}
	if len(data) < 2+length {
		return nil, 0, errors.Newf(
			"insufficient data for nvarchar: need %d, have %d",
			2+length, len(data),
		)
	}
	s := decodeUTF16LE(data[2 : 2+length])
	return s, 2 + length, nil
}

func decodeVarBinValue(data []byte) (interface{}, int, error) {
	if len(data) < 2 {
		return nil, 0, errors.New("insufficient data for varbinary length")
	}
	length := int(binary.LittleEndian.Uint16(data[:2]))
	if length == 0xFFFF {
		return nil, 2, nil // NULL
	}
	if len(data) < 2+length {
		return nil, 0, errors.Newf(
			"insufficient data for varbinary: need %d, have %d",
			2+length, len(data),
		)
	}
	b := make([]byte, length)
	copy(b, data[2:2+length])
	return b, 2 + length, nil
}

func decodeDateTimeBytes(data []byte) (interface{}, int, error) {
	if len(data) < 8 {
		return nil, 0, errors.New("insufficient data for datetime")
	}
	days := int32(binary.LittleEndian.Uint32(data[:4]))
	threeHundredths := int32(binary.LittleEndian.Uint32(data[4:8]))
	t := tdsEpoch.AddDate(0, 0, int(days))
	t = t.Add(time.Duration(threeHundredths) * time.Second / 300)
	return t, 8, nil
}

func decodeSmallDateTimeBytes(data []byte) (interface{}, int, error) {
	if len(data) < 4 {
		return nil, 0, errors.New("insufficient data for smalldatetime")
	}
	days := binary.LittleEndian.Uint16(data[:2])
	minutes := binary.LittleEndian.Uint16(data[2:4])
	t := tdsEpoch.AddDate(0, 0, int(days))
	t = t.Add(time.Duration(minutes) * time.Minute)
	return t, 4, nil
}

func decodeDateBytes(data []byte) (interface{}, int, error) {
	if len(data) < 3 {
		return nil, 0, errors.New("insufficient data for date")
	}
	days := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	t := julianDayToTime(tdsDateEpochJulian + days)
	return t, 3, nil
}

func decodeTimeBytes(data []byte) (interface{}, int, error) {
	if len(data) < 3 {
		return nil, 0, errors.New("insufficient data for time")
	}
	var ticks int64
	for i := len(data) - 1; i >= 0; i-- {
		ticks = ticks<<8 | int64(data[i])
	}
	// Scale 7 means 100-nanosecond intervals.
	dur := time.Duration(ticks) * 100
	midnight := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	return midnight.Add(dur), len(data), nil
}

func decodeMoneyBytes(data []byte, size int) (interface{}, int, error) {
	var scaled int64
	if size == 4 {
		scaled = int64(int32(binary.LittleEndian.Uint32(data[:4])))
	} else {
		// 8-byte MONEY: hi 4 bytes, lo 4 bytes.
		hi := int32(binary.LittleEndian.Uint32(data[:4]))
		lo := binary.LittleEndian.Uint32(data[4:8])
		scaled = int64(hi)<<32 | int64(lo)
	}
	d := new(apd.Decimal)
	d.SetFinite(scaled, -4) // divide by 10000
	return d, size, nil
}

func decodeDecimalBytes(data []byte, scale byte) (interface{}, int, error) {
	if len(data) < 1 {
		return nil, 0, errors.New("insufficient data for decimal")
	}
	sign := data[0] // 1 = positive, 0 = negative
	rest := data[1:]
	// Convert from little-endian to big-endian for SetBytes.
	be := make([]byte, len(rest))
	for i, j := 0, len(rest)-1; j >= 0; i, j = i+1, j-1 {
		be[i] = rest[j]
	}
	d := new(apd.Decimal)
	d.Coeff.SetBytes(be)
	d.Exponent = -int32(scale)
	if sign == 0 {
		d.Negative = true
	}
	return d, len(data), nil
}

func decodeGUIDBytes(data []byte) (interface{}, int, error) {
	if len(data) < 16 {
		return nil, 0, errors.New("insufficient data for GUID")
	}
	// Reverse the mixed-endian TDS GUID back to RFC 4122 byte order.
	var b [16]byte
	b[0], b[1], b[2], b[3] = data[3], data[2], data[1], data[0] // group 1
	b[4], b[5] = data[5], data[4]                               // group 2
	b[6], b[7] = data[7], data[6]                               // group 3
	copy(b[8:], data[8:16])                                     // groups 4+5
	u, err := uuid.FromBytes(b[:])
	if err != nil {
		return nil, 0, errors.Wrap(err, "decoding TDS GUID")
	}
	return u, 16, nil
}

// --- Calendar helpers ---

// julianDay computes the Julian day number for the given time.
func julianDay(t time.Time) int {
	y, m, d := t.Date()
	a := (14 - int(m)) / 12
	y2 := y + 4800 - a
	m2 := int(m) + 12*a - 3
	return d + (153*m2+2)/5 + 365*y2 + y2/4 - y2/100 + y2/400 - 32045
}

// julianDayToTime converts a Julian day number back to a time.Time at midnight
// UTC.
func julianDayToTime(jdn int) time.Time {
	// Algorithm from https://en.wikipedia.org/wiki/Julian_day
	f := jdn + 1401 + (((4*jdn+274277)/146097)*3)/4 - 38
	e := 4*f + 3
	g := (e % 1461) / 4
	h := 5*g + 2
	day := (h%153)/5 + 1
	month := (h/153+2)%12 + 1
	year := e/1461 - 4716 + (14-month)/12
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

// --- apd helpers ---

// apdToScaledInt64 returns the value of d * 10^scale as an int64.
// For example, 1234.5678 with scale=4 returns 12345678.
func apdToScaledInt64(d *apd.Decimal, scale int32) int64 {
	// Quantize to the target scale so the coefficient is the scaled integer.
	scaled := new(apd.Decimal)
	_, _ = apd.BaseContext.WithPrecision(38).Quantize(scaled, d, -scale)
	// The coefficient is the absolute unscaled integer; apply sign.
	v := scaled.Coeff.Int64()
	if scaled.Negative {
		v = -v
	}
	return v
}

// apdToScaledBytes quantizes d to the given scale and returns the coefficient
// as big-endian bytes. The returned bytes represent the absolute value;
// the caller is responsible for sign handling.
func apdToScaledBytes(d *apd.Decimal, scale int32) []byte {
	scaled := new(apd.Decimal)
	_, _ = apd.BaseContext.WithPrecision(38).Quantize(scaled, d, -scale)
	return scaled.Coeff.Bytes()
}

// --- Encoding helpers ---

func appendIntLE(dst []byte, v int64, size int) []byte {
	switch size {
	case 1:
		return append(dst, byte(v))
	case 2:
		return binary.LittleEndian.AppendUint16(dst, uint16(v))
	case 4:
		return binary.LittleEndian.AppendUint32(dst, uint32(v))
	case 8:
		return binary.LittleEndian.AppendUint64(dst, uint64(v))
	default:
		panic(errors.AssertionFailedf("appendIntLE: invalid size %d", size))
	}
}

func appendUint16LE(dst []byte, v uint16) []byte {
	return binary.LittleEndian.AppendUint16(dst, v)
}

func appendFloatLE(dst []byte, v float64, size int) []byte {
	switch size {
	case 4:
		bits := math.Float32bits(float32(v))
		return binary.LittleEndian.AppendUint32(dst, bits)
	case 8:
		return binary.LittleEndian.AppendUint64(dst, math.Float64bits(v))
	default:
		panic(errors.AssertionFailedf("appendFloatLE: invalid size %d", size))
	}
}

// encodeUTF16LE converts a Go UTF-8 string to UTF-16LE bytes. Only the BMP
// is supported (code points above U+FFFF are replaced with U+FFFD).
func encodeUTF16LE(s string) []byte {
	out := make([]byte, 0, len(s)*2)
	for _, r := range s {
		if r > 0xFFFF {
			r = 0xFFFD
		}
		out = binary.LittleEndian.AppendUint16(out, uint16(r))
	}
	return out
}

// decodeUTF16LE converts UTF-16LE bytes to a Go UTF-8 string.
func decodeUTF16LE(b []byte) string {
	runes := make([]rune, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		runes = append(runes, rune(binary.LittleEndian.Uint16(b[i:])))
	}
	return string(runes)
}
