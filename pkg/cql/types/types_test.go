// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestCQLTypeString(t *testing.T) {
	tests := []struct {
		typ  CQLType
		want string
	}{
		{CQLText, "varchar"},
		{CQLVarchar, "varchar"},
		{CQLInt, "int"},
		{CQLBigint, "bigint"},
		{CQLFloat, "float"},
		{CQLDouble, "double"},
		{CQLBoolean, "boolean"},
		{CQLTimestamp, "timestamp"},
		{CQLUuid, "uuid"},
		{CQLTimeuuid, "timeuuid"},
		{CQLBlob, "blob"},
		{CQLInet, "inet"},
		{CQLCounter, "counter"},
		{CQLAscii, "ascii"},
		{CQLType(0xFFFF), "unknown"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.typ.String(), "CQLType(0x%04x)", uint16(tt.typ))
	}
}

func TestCRDBType(t *testing.T) {
	tests := []struct {
		cql  CQLType
		want *types.T
	}{
		{CQLText, types.String},
		{CQLVarchar, types.String},
		{CQLAscii, types.String},
		{CQLInt, types.Int4},
		{CQLBigint, types.Int},
		{CQLCounter, types.Int},
		{CQLFloat, types.Float4},
		{CQLDouble, types.Float},
		{CQLBoolean, types.Bool},
		{CQLTimestamp, types.TimestampTZ},
		{CQLUuid, types.Uuid},
		{CQLTimeuuid, types.Uuid},
		{CQLBlob, types.Bytes},
		{CQLInet, types.INet},
	}
	for _, tt := range tests {
		got, err := tt.cql.CRDBType()
		require.NoError(t, err)
		require.Equal(t, tt.want, got, "CQLType %s", tt.cql)
	}

	// Unsupported type.
	_, err := CQLType(0xFFFF).CRDBType()
	require.Error(t, err)
}

func TestCQLTypeFromCRDB(t *testing.T) {
	tests := []struct {
		crdb *types.T
		want CQLType
	}{
		{types.Bool, CQLBoolean},
		{types.Int4, CQLInt},
		{types.Int2, CQLSmallint},
		{types.Int, CQLBigint},
		{types.Float4, CQLFloat},
		{types.Float, CQLDouble},
		{types.String, CQLVarchar},
		{types.Bytes, CQLBlob},
		{types.TimestampTZ, CQLTimestamp},
		{types.Timestamp, CQLTimestamp},
		{types.Date, CQLDate},
		{types.Time, CQLTime},
		{types.Interval, CQLDuration},
		{types.Uuid, CQLUuid},
		{types.INet, CQLInet},
		{types.Jsonb, CQLVarchar},
		{types.Decimal, CQLDecimal},
	}
	for _, tt := range tests {
		got, err := CQLTypeFromCRDB(tt.crdb)
		require.NoError(t, err)
		require.Equal(t, tt.want, got, "CRDB type %s", tt.crdb.SQLString())
	}

	// Unsupported CRDB type.
	_, err := CQLTypeFromCRDB(types.Oid)
	require.Error(t, err)
}

func TestEncodeDatumNull(t *testing.T) {
	val, isNull, err := EncodeDatum(tree.DNull, CQLText)
	require.NoError(t, err)
	require.True(t, isNull)
	require.Nil(t, val)
}

func TestEncodeDatumText(t *testing.T) {
	s := tree.NewDString("hello")
	val, isNull, err := EncodeDatum(s, CQLText)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, []byte("hello"), val)

	// Empty string.
	s = tree.NewDString("")
	val, isNull, err = EncodeDatum(s, CQLVarchar)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, []byte(""), val)
}

func TestEncodeDatumInt(t *testing.T) {
	// CQL int: 4-byte big-endian.
	d := tree.NewDInt(42)
	val, isNull, err := EncodeDatum(d, CQLInt)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 4)
	require.Equal(t, int32(42), int32(binary.BigEndian.Uint32(val)))

	// Negative value.
	d = tree.NewDInt(-1)
	val, _, err = EncodeDatum(d, CQLInt)
	require.NoError(t, err)
	require.Equal(t, int32(-1), int32(binary.BigEndian.Uint32(val)))
}

func TestEncodeDatumBigint(t *testing.T) {
	d := tree.NewDInt(1<<40 + 7)
	val, isNull, err := EncodeDatum(d, CQLBigint)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 8)
	require.Equal(t, int64(1<<40+7), int64(binary.BigEndian.Uint64(val)))
}

func TestEncodeDatumCounter(t *testing.T) {
	// Counter uses the same encoding as bigint.
	d := tree.NewDInt(99)
	val, _, err := EncodeDatum(d, CQLCounter)
	require.NoError(t, err)
	require.Len(t, val, 8)
	require.Equal(t, int64(99), int64(binary.BigEndian.Uint64(val)))
}

func TestEncodeDatumFloat(t *testing.T) {
	f := tree.DFloat(3.14)
	val, isNull, err := EncodeDatum(&f, CQLFloat)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 4)
	got := math.Float32frombits(binary.BigEndian.Uint32(val))
	require.InDelta(t, float32(3.14), got, 0.001)
}

func TestEncodeDatumDouble(t *testing.T) {
	f := tree.DFloat(2.718281828)
	val, isNull, err := EncodeDatum(&f, CQLDouble)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 8)
	got := math.Float64frombits(binary.BigEndian.Uint64(val))
	require.InDelta(t, 2.718281828, got, 1e-9)
}

func TestEncodeDatumBool(t *testing.T) {
	val, _, err := EncodeDatum(tree.DBoolTrue, CQLBoolean)
	require.NoError(t, err)
	require.Equal(t, []byte{0x01}, val)

	val, _, err = EncodeDatum(tree.DBoolFalse, CQLBoolean)
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, val)
}

func TestEncodeDatumTimestamp(t *testing.T) {
	// 2024-01-15 12:30:45 UTC.
	ts := tree.MustMakeDTimestampTZ(
		timeutil.Unix(1705318245, 0), time.Microsecond,
	)
	val, isNull, err := EncodeDatum(ts, CQLTimestamp)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 8)
	millis := int64(binary.BigEndian.Uint64(val))
	require.Equal(t, int64(1705318245000), millis)
}

func TestEncodeDatumUUID(t *testing.T) {
	u := uuid.FromUint128(uint128.FromInts(0x0123456789ABCDEF, 0xFEDCBA9876543210))
	d := tree.NewDUuid(tree.DUuid{UUID: u})
	val, isNull, err := EncodeDatum(d, CQLUuid)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 16)
	require.Equal(t, u.GetBytes(), val)
}

func TestEncodeDatumBlob(t *testing.T) {
	b := tree.NewDBytes(tree.DBytes("\x00\x01\x02\xff"))
	val, isNull, err := EncodeDatum(b, CQLBlob)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, []byte{0x00, 0x01, 0x02, 0xff}, val)
}

func TestEncodeDatumInetIPv4(t *testing.T) {
	ip := &tree.DIPAddr{IPAddr: ipaddr.IPAddr{
		Family: ipaddr.IPv4family,
		Mask:   32,
		Addr:   ipaddr.Addr{Hi: 0, Lo: 0xC0A80001}, // 192.168.0.1
	}}
	val, isNull, err := EncodeDatum(ip, CQLInet)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 4)
	require.Equal(t, []byte{192, 168, 0, 1}, val)
}

func TestEncodeDatumInetIPv6(t *testing.T) {
	// ::1 (loopback).
	ip := &tree.DIPAddr{IPAddr: ipaddr.IPAddr{
		Family: ipaddr.IPv6family,
		Mask:   128,
		Addr:   ipaddr.Addr{Hi: 0, Lo: 1},
	}}
	val, isNull, err := EncodeDatum(ip, CQLInet)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Len(t, val, 16)
	expected := make([]byte, 16)
	expected[15] = 1
	require.Equal(t, expected, val)
}

func TestEncodeDatumTypeMismatch(t *testing.T) {
	// Pass a DString where DInt is expected.
	_, _, err := EncodeDatum(tree.NewDString("oops"), CQLInt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected DInt")
}

func TestEncodeNullAndValue(t *testing.T) {
	// EncodeNull produces a 4-byte -1.
	buf := EncodeNull(nil)
	require.Len(t, buf, 4)
	require.Equal(t, int32(-1), int32(binary.BigEndian.Uint32(buf)))

	// EncodeValue produces length prefix + data.
	data := []byte("hi")
	buf = EncodeValue(nil, data)
	require.Len(t, buf, 6)
	require.Equal(t, int32(2), int32(binary.BigEndian.Uint32(buf[:4])))
	require.Equal(t, data, buf[4:])
}

func TestRoundTripMapping(t *testing.T) {
	// Verify that CQL -> CRDB -> CQL round-trips for all supported types.
	cqlTypes := []CQLType{
		CQLBoolean, CQLInt, CQLBigint, CQLFloat, CQLDouble,
		CQLVarchar, CQLBlob, CQLTimestamp, CQLUuid, CQLInet,
	}
	for _, cql := range cqlTypes {
		crdb, err := cql.CRDBType()
		require.NoError(t, err, "CQLType %s -> CRDB", cql)
		roundTripped, err := CQLTypeFromCRDB(crdb)
		require.NoError(t, err, "CRDB %s -> CQL", crdb.SQLString())
		require.Equal(t, cql, roundTripped,
			"round-trip failed: CQL %s -> CRDB %s -> CQL %s",
			cql, crdb.SQLString(), roundTripped,
		)
	}
}
