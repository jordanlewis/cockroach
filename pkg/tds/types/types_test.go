// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestTDSToCRDB(t *testing.T) {
	tests := []struct {
		name      string
		tdsType   TDSTypeID
		maxLen    int
		precision byte
		scale     byte
		expected  *types.T
	}{
		{"TINYINT", Int1Type, 1, 0, 0, types.Int2},
		{"SMALLINT", Int2Type, 2, 0, 0, types.Int2},
		{"INT", Int4Type, 4, 0, 0, types.Int4},
		{"BIGINT", Int8Type, 8, 0, 0, types.Int},
		{"INTN 1-byte", IntNType, 1, 0, 0, types.Int2},
		{"INTN 2-byte", IntNType, 2, 0, 0, types.Int2},
		{"INTN 4-byte", IntNType, 4, 0, 0, types.Int4},
		{"INTN 8-byte", IntNType, 8, 0, 0, types.Int},
		{"REAL", Float4Type, 4, 0, 0, types.Float4},
		{"FLOAT", Float8Type, 8, 0, 0, types.Float},
		{"FLTN 4-byte", FloatNType, 4, 0, 0, types.Float4},
		{"FLTN 8-byte", FloatNType, 8, 0, 0, types.Float},
		{"BIT", BitType, 1, 0, 0, types.Bool},
		{"BITN", BitNType, 1, 0, 0, types.Bool},
		{"VARCHAR", BigVarCharType, 100, 0, 0, types.String},
		{"CHAR", BigCharType, 50, 0, 0, types.String},
		{"NVARCHAR", NVarCharType, 200, 0, 0, types.String},
		{"NCHAR", NCharType, 100, 0, 0, types.String},
		{"TEXT", TextType, 0, 0, 0, types.String},
		{"NTEXT", NTextType, 0, 0, 0, types.String},
		{"VARBINARY", BigVarBinType, 100, 0, 0, types.Bytes},
		{"BINARY", BigBinaryType, 50, 0, 0, types.Bytes},
		{"IMAGE", ImageType, 0, 0, 0, types.Bytes},
		{"DATETIME", DateTimeType, 8, 0, 0, types.Timestamp},
		{"SMALLDATETIME", DateTime4Type, 4, 0, 0, types.Timestamp},
		{"DATETIMENTYPE", DateTimeNType, 8, 0, 0, types.Timestamp},
		{"DATE", DateNType, 3, 0, 0, types.Date},
		{"TIME", TimeNType, 5, 0, 0, types.Time},
		{"GUID", GUIDType, 16, 0, 0, types.Uuid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TDSToCRDB(tt.tdsType, tt.maxLen, tt.precision, tt.scale)
			require.NoError(t, err)
			require.Equal(t, tt.expected.Family(), got.Family())
		})
	}
}

func TestTDSToCRDB_Money(t *testing.T) {
	// MONEY -> DECIMAL(19,4)
	got, err := TDSToCRDB(MoneyType, 8, 0, 0)
	require.NoError(t, err)
	require.Equal(t, types.DecimalFamily, got.Family())
	require.Equal(t, int32(19), got.Precision())
	require.Equal(t, int32(4), got.Scale())

	// SMALLMONEY -> DECIMAL(10,4)
	got, err = TDSToCRDB(Money4Type, 4, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int32(10), got.Precision())
	require.Equal(t, int32(4), got.Scale())
}

func TestTDSToCRDB_Numeric(t *testing.T) {
	got, err := TDSToCRDB(NumericNType, 17, 18, 2)
	require.NoError(t, err)
	require.Equal(t, types.DecimalFamily, got.Family())
	require.Equal(t, int32(18), got.Precision())
	require.Equal(t, int32(2), got.Scale())
}

func TestTDSToCRDB_UnsupportedType(t *testing.T) {
	_, err := TDSToCRDB(TDSTypeID(0xFF), 0, 0, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported TDS type")
}

func TestCRDBToTDS(t *testing.T) {
	tests := []struct {
		name    string
		crdb    *types.T
		tdsType TDSTypeID
		maxLen  int
	}{
		{"BOOL", types.Bool, BitNType, 1},
		{"INT2", types.Int2, IntNType, 2},
		{"INT4", types.Int4, IntNType, 4},
		{"INT8", types.Int, IntNType, 8},
		{"FLOAT4", types.Float4, FloatNType, 4},
		{"FLOAT8", types.Float, FloatNType, 8},
		{"STRING", types.String, NVarCharType, 16000},
		{"BYTES", types.Bytes, BigVarBinType, 8000},
		{"DATE", types.Date, DateNType, 3},
		{"TIME", types.Time, TimeNType, 5},
		{"TIMESTAMP", types.Timestamp, DateTimeNType, 8},
		{"UUID", types.Uuid, GUIDType, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CRDBToTDS(tt.crdb)
			require.NoError(t, err)
			require.Equal(t, tt.tdsType, got.TDSType)
			require.Equal(t, tt.maxLen, got.MaxLength)
		})
	}
}

func TestCRDBToTDS_Decimal(t *testing.T) {
	dec := types.MakeDecimal(10, 2)
	got, err := CRDBToTDS(dec)
	require.NoError(t, err)
	require.Equal(t, DecimalNType, got.TDSType)
	require.Equal(t, byte(10), got.Precision)
	require.Equal(t, byte(2), got.Scale)
}

func TestIsFixedLength(t *testing.T) {
	require.True(t, IsFixedLength(Int1Type))
	require.True(t, IsFixedLength(Int4Type))
	require.True(t, IsFixedLength(Float8Type))
	require.True(t, IsFixedLength(BitType))
	require.True(t, IsFixedLength(DateTimeType))

	require.False(t, IsFixedLength(IntNType))
	require.False(t, IsFixedLength(FloatNType))
	require.False(t, IsFixedLength(BigVarCharType))
	require.False(t, IsFixedLength(NVarCharType))
	require.False(t, IsFixedLength(GUIDType))
}

// frameForDecode wraps raw EncodeValue output with the per-row framing
// that writeRowValue (in tdswire) adds on the wire. EncodeValue produces
// raw value bytes; DecodeValue expects wire-format data that includes
// length prefixes for nullable types. This helper bridges the gap for
// roundtrip tests.
func frameForDecode(ti TypeInfo, encoded []byte) []byte {
	if IsFixedLength(ti.TDSType) {
		return encoded
	}
	switch ti.TDSType {
	case IntNType, FloatNType, BitNType, DateTimeNType, MoneyNType,
		GUIDType, NumericNType, DecimalNType, DateNType, TimeNType:
		// Byte-length-prefix types: 1-byte length, 0 = NULL.
		if encoded == nil {
			return []byte{0x00}
		}
		return append([]byte{byte(len(encoded))}, encoded...)
	case BigVarCharType, BigCharType, TextType,
		NVarCharType, NCharType, NTextType,
		BigVarBinType, BigBinaryType, ImageType:
		// Variable-length types: 2-byte length prefix, 0xFFFF = NULL.
		if encoded == nil {
			return []byte{0xFF, 0xFF}
		}
		return append(appendUint16LE(nil, uint16(len(encoded))), encoded...)
	default:
		return encoded
	}
}

func TestEncodeDecodeInt(t *testing.T) {
	tests := []struct {
		name  string
		ti    TypeInfo
		value int64
	}{
		{"int8-nullable", TypeInfo{TDSType: IntNType, MaxLength: 8}, 42},
		{"int4-nullable", TypeInfo{TDSType: IntNType, MaxLength: 4}, -100},
		{"int2-nullable", TypeInfo{TDSType: IntNType, MaxLength: 2}, 256},
		{"int1-nullable", TypeInfo{TDSType: IntNType, MaxLength: 1}, 7},
		{"int4-fixed", TypeInfo{TDSType: Int4Type, MaxLength: 4}, 99},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeValue(nil, tt.ti, tt.value)
			require.NoError(t, err)

			framed := frameForDecode(tt.ti, encoded)
			decoded, n, err := DecodeValue(framed, tt.ti)
			require.NoError(t, err)
			require.Equal(t, len(framed), n)
			require.Equal(t, tt.value, decoded.(int64))
		})
	}
}

func TestEncodeDecodeNull(t *testing.T) {
	tests := []struct {
		name string
		ti   TypeInfo
	}{
		{"int-null", TypeInfo{TDSType: IntNType, MaxLength: 4}},
		{"float-null", TypeInfo{TDSType: FloatNType, MaxLength: 8}},
		{"bit-null", TypeInfo{TDSType: BitNType, MaxLength: 1}},
		{"varchar-null", TypeInfo{TDSType: BigVarCharType, MaxLength: 100}},
		{"nvarchar-null", TypeInfo{TDSType: NVarCharType, MaxLength: 200}},
		{"guid-null", TypeInfo{TDSType: GUIDType, MaxLength: 16}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeValue(nil, tt.ti, nil)
			require.NoError(t, err)

			framed := frameForDecode(tt.ti, encoded)
			decoded, _, err := DecodeValue(framed, tt.ti)
			require.NoError(t, err)
			require.Nil(t, decoded)
		})
	}
}

func TestEncodeDecodeFloat(t *testing.T) {
	ti8 := TypeInfo{TDSType: FloatNType, MaxLength: 8}
	encoded, err := EncodeValue(nil, ti8, 3.14)
	require.NoError(t, err)
	framed := frameForDecode(ti8, encoded)
	decoded, n, err := DecodeValue(framed, ti8)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	require.InDelta(t, 3.14, decoded.(float64), 1e-10)

	ti4 := TypeInfo{TDSType: FloatNType, MaxLength: 4}
	encoded, err = EncodeValue(nil, ti4, 2.5)
	require.NoError(t, err)
	framed = frameForDecode(ti4, encoded)
	decoded, n, err = DecodeValue(framed, ti4)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	require.InDelta(t, 2.5, decoded.(float64), 1e-6)
}

func TestEncodeDecodeBool(t *testing.T) {
	ti := TypeInfo{TDSType: BitNType, MaxLength: 1}
	for _, v := range []bool{true, false} {
		encoded, err := EncodeValue(nil, ti, v)
		require.NoError(t, err)
		framed := frameForDecode(ti, encoded)
		decoded, n, err := DecodeValue(framed, ti)
		require.NoError(t, err)
		require.Equal(t, len(framed), n)
		require.Equal(t, v, decoded.(bool))
	}
}

func TestEncodeDecodeVarChar(t *testing.T) {
	ti := TypeInfo{TDSType: BigVarCharType, MaxLength: 200}
	input := "hello, world!"
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	require.Equal(t, input, decoded.(string))
}

func TestEncodeDecodeNVarChar(t *testing.T) {
	ti := TypeInfo{TDSType: NVarCharType, MaxLength: 400}
	input := "hello"
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	require.Equal(t, input, decoded.(string))
}

func TestEncodeDecodeVarBin(t *testing.T) {
	ti := TypeInfo{TDSType: BigVarBinType, MaxLength: 200}
	input := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	require.Equal(t, input, decoded.([]byte))
}

func TestEncodeDecodeDateTime(t *testing.T) {
	ti := TypeInfo{TDSType: DateTimeNType, MaxLength: 8}
	input := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	// TDS DATETIME has ~3.33ms precision.
	got := decoded.(time.Time)
	require.WithinDuration(t, input, got, 4*time.Millisecond)
}

func TestEncodeDecodeDate(t *testing.T) {
	ti := TypeInfo{TDSType: DateNType, MaxLength: 3}
	input := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	got := decoded.(time.Time)
	require.Equal(t, input.Year(), got.Year())
	require.Equal(t, input.Month(), got.Month())
	require.Equal(t, input.Day(), got.Day())
}

func TestEncodeDecodeGUID(t *testing.T) {
	ti := TypeInfo{TDSType: GUIDType, MaxLength: 16}
	u := uuid.MakeV4()
	encoded, err := EncodeValue(nil, ti, u)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	require.Equal(t, u, decoded.(uuid.UUID))
}

func TestEncodeDecodeMoney(t *testing.T) {
	ti := TypeInfo{TDSType: MoneyNType, MaxLength: 8}
	// 1234.5678 as *apd.Decimal
	input := new(apd.Decimal)
	input.SetFinite(12345678, -4)
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	got := decoded.(*apd.Decimal)
	require.Equal(t, 0, input.Cmp(got))
}

func TestEncodeDecodeDecimalType(t *testing.T) {
	ti := TypeInfo{
		TDSType: DecimalNType, MaxLength: 9, Precision: 10, Scale: 2,
	}
	// 12345.67
	input := new(apd.Decimal)
	input.SetFinite(1234567, -2)
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	got := decoded.(*apd.Decimal)
	require.Equal(t, 0, input.Cmp(got))
}

func TestEncodeDecodeDecimalNegative(t *testing.T) {
	ti := TypeInfo{
		TDSType: DecimalNType, MaxLength: 9, Precision: 10, Scale: 2,
	}
	// -999.99
	input := new(apd.Decimal)
	input.SetFinite(-99999, -2)
	encoded, err := EncodeValue(nil, ti, input)
	require.NoError(t, err)
	framed := frameForDecode(ti, encoded)
	decoded, n, err := DecodeValue(framed, ti)
	require.NoError(t, err)
	require.Equal(t, len(framed), n)
	got := decoded.(*apd.Decimal)
	require.Equal(t, 0, input.Cmp(got))
}

func TestUTF16LECodec(t *testing.T) {
	tests := []string{
		"",
		"hello",
		"CockroachDB",
	}
	for _, s := range tests {
		encoded := encodeUTF16LE(s)
		decoded := decodeUTF16LE(encoded)
		require.Equal(t, s, decoded)
	}
}

func TestDecimalLength(t *testing.T) {
	// precision 1-9: 1 sign + 4 data = 5
	require.Equal(t, 5, decimalLength(1))
	require.Equal(t, 5, decimalLength(9))
	// precision 10-19: 1 sign + 8 data = 9
	require.Equal(t, 9, decimalLength(10))
	require.Equal(t, 9, decimalLength(19))
	// precision 20-28: 1 sign + 12 data = 13
	require.Equal(t, 13, decimalLength(20))
	require.Equal(t, 13, decimalLength(28))
	// precision 29-38: 1 sign + 16 data = 17
	require.Equal(t, 17, decimalLength(29))
	require.Equal(t, 17, decimalLength(38))
}
