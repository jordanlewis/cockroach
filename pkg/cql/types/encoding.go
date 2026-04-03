// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"encoding/binary"
	"math"
	"math/big"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// EncodeDatum marshals a CockroachDB datum into the CQL binary protocol wire
// format. The returned byte slice contains only the value bytes (no length
// prefix). The caller is responsible for writing the [int] length prefix as
// required by the CQL native protocol's [bytes] encoding.
//
// For NULL datums, the function returns (nil, true, nil): the caller should
// write a length prefix of -1 and no value bytes.
func EncodeDatum(d tree.Datum, cqlType CQLType) ([]byte, bool, error) {
	if d == tree.DNull {
		return nil, true, nil
	}
	switch cqlType {
	case CQLVarchar, CQLAscii: // CQLText == CQLVarchar
		return encodeText(d)
	case CQLSmallint:
		return encodeSmallint(d)
	case CQLTinyint:
		return encodeTinyint(d)
	case CQLInt:
		return encodeInt(d)
	case CQLBigint, CQLCounter:
		return encodeBigint(d)
	case CQLFloat:
		return encodeFloat(d)
	case CQLDouble:
		return encodeDouble(d)
	case CQLBoolean:
		return encodeBool(d)
	case CQLTimestamp:
		return encodeTimestamp(d)
	case CQLUuid, CQLTimeuuid:
		return encodeUUID(d)
	case CQLBlob:
		return encodeBlob(d)
	case CQLInet:
		return encodeInet(d)
	case CQLDate:
		return encodeDate(d)
	case CQLTime:
		return encodeTime(d)
	case CQLDuration:
		return encodeDuration(d)
	case CQLDecimal:
		return encodeDecimal(d)
	case CQLTuple, CQLList, CQLMap, CQLSet:
		// Collection and tuple types are stored as JSONB. Format the
		// text representation to match Cassandra's display conventions:
		// sets use {}, tuples use (), lists use []. Empty collections
		// are treated as NULL (Cassandra semantics).
		return encodeCollection(d, cqlType)
	default:
		return nil, false, errors.Newf("unsupported CQL type for encoding: %s", cqlType)
	}
}

// EncodeNull writes a CQL NULL value (length = -1) to buf and returns the
// extended buffer.
func EncodeNull(buf []byte) []byte {
	return appendInt32(buf, -1)
}

// EncodeValue writes a CQL [bytes] value (length prefix + value bytes) to buf
// and returns the extended buffer.
func EncodeValue(buf []byte, val []byte) []byte {
	buf = appendInt32(buf, int32(len(val)))
	return append(buf, val...)
}

func encodeText(d tree.Datum) ([]byte, bool, error) {
	switch v := d.(type) {
	case *tree.DString:
		return []byte(string(*v)), false, nil
	case *tree.DJSON:
		// JSONB datums (used for CQL collection types) are encoded as
		// their JSON text representation.
		return []byte(v.JSON.String()), false, nil
	default:
		return nil, false, errors.Newf("expected DString or DJSON, got %T", d)
	}
}

// encodeCollection formats a DJSON datum as CQL collection text. Sets are
// wrapped in {}, tuples in (), and lists in []. Empty collections are encoded
// as NULL to match Cassandra semantics where empty collections equal null.
func encodeCollection(d tree.Datum, cqlType CQLType) ([]byte, bool, error) {
	v, ok := d.(*tree.DJSON)
	if !ok {
		return nil, false, errors.Newf(
			"expected DJSON for collection type %s, got %T", cqlType, d,
		)
	}

	j := v.JSON
	jType := j.Type()

	// Empty collections are NULL in Cassandra.
	if (jType == json.ArrayJSONType || jType == json.ObjectJSONType) && j.Len() == 0 {
		return nil, true, nil
	}

	switch cqlType {
	case CQLSet:
		return formatJSONArray(j, '{', '}')
	case CQLTuple:
		return formatJSONArray(j, '(', ')')
	case CQLList:
		return formatJSONArray(j, '[', ']')
	default:
		// Maps and unknown collection types: use JSON text as-is.
		return []byte(j.String()), false, nil
	}
}

// formatJSONArray formats a JSON array's elements separated by ", " and
// wrapped with the given open/close brackets. String elements use single
// quotes to match CQL display conventions.
func formatJSONArray(j json.JSON, open, close byte) ([]byte, bool, error) {
	n := j.Len()
	var sb strings.Builder
	sb.WriteByte(open)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		elem, err := j.FetchValIdx(i)
		if err != nil {
			return nil, false, errors.Wrap(err, "reading collection element")
		}
		sb.WriteString(jsonElemToCQL(elem))
	}
	sb.WriteByte(close)
	return []byte(sb.String()), false, nil
}

// jsonElemToCQL converts a single JSON value to its CQL text representation.
// Strings use single quotes; other types use their JSON representation.
func jsonElemToCQL(j json.JSON) string {
	if j.Type() == json.StringJSONType {
		text, err := j.AsText()
		if err != nil || text == nil {
			return j.String()
		}
		return "'" + *text + "'"
	}
	return j.String()
}

func encodeInt(d tree.Datum) ([]byte, bool, error) {
	i, ok := d.(*tree.DInt)
	if !ok {
		return nil, false, errors.Newf("expected DInt, got %T", d)
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(int32(*i)))
	return buf[:], false, nil
}

func encodeBigint(d tree.Datum) ([]byte, bool, error) {
	i, ok := d.(*tree.DInt)
	if !ok {
		return nil, false, errors.Newf("expected DInt, got %T", d)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(*i))
	return buf[:], false, nil
}

func encodeFloat(d tree.Datum) ([]byte, bool, error) {
	f, ok := d.(*tree.DFloat)
	if !ok {
		return nil, false, errors.Newf("expected DFloat, got %T", d)
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], math.Float32bits(float32(*f)))
	return buf[:], false, nil
}

func encodeDouble(d tree.Datum) ([]byte, bool, error) {
	f, ok := d.(*tree.DFloat)
	if !ok {
		return nil, false, errors.Newf("expected DFloat, got %T", d)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(float64(*f)))
	return buf[:], false, nil
}

func encodeBool(d tree.Datum) ([]byte, bool, error) {
	b, ok := d.(*tree.DBool)
	if !ok {
		return nil, false, errors.Newf("expected DBool, got %T", d)
	}
	if *b {
		return []byte{0x01}, false, nil
	}
	return []byte{0x00}, false, nil
}

// encodeTimestamp encodes a CRDB timestamp as CQL timestamp: 8-byte big-endian
// milliseconds since Unix epoch.
func encodeTimestamp(d tree.Datum) ([]byte, bool, error) {
	ts, ok := d.(*tree.DTimestampTZ)
	if !ok {
		return nil, false, errors.Newf("expected DTimestampTZ, got %T", d)
	}
	millis := ts.Time.UnixMilli()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(millis))
	return buf[:], false, nil
}

func encodeUUID(d tree.Datum) ([]byte, bool, error) {
	u, ok := d.(*tree.DUuid)
	if !ok {
		return nil, false, errors.Newf("expected DUuid, got %T", d)
	}
	b := u.UUID.GetBytes()
	return b, false, nil
}

func encodeBlob(d tree.Datum) ([]byte, bool, error) {
	b, ok := d.(*tree.DBytes)
	if !ok {
		return nil, false, errors.Newf("expected DBytes, got %T", d)
	}
	return []byte(string(*b)), false, nil
}

// encodeInet encodes a CRDB IP address as CQL inet: 4 bytes for IPv4 or 16
// bytes for IPv6. The CQL inet type carries only the address, not the mask.
func encodeInet(d tree.Datum) ([]byte, bool, error) {
	ip, ok := d.(*tree.DIPAddr)
	if !ok {
		return nil, false, errors.Newf("expected DIPAddr, got %T", d)
	}
	if ip.Family == ipaddr.IPv4family {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(ip.Addr.Lo))
		return buf[:], false, nil
	}
	// IPv6: 16 bytes.
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], ip.Addr.Hi)
	binary.BigEndian.PutUint64(buf[8:], ip.Addr.Lo)
	return buf[:], false, nil
}

// encodeDecimal encodes a CRDB decimal as CQL decimal: 4-byte big-endian scale
// followed by the unscaled value as a variable-length two's complement
// big-endian integer (matching the CQL native protocol varint encoding).
func encodeDecimal(d tree.Datum) ([]byte, bool, error) {
	dec, ok := d.(*tree.DDecimal)
	if !ok {
		return nil, false, errors.Newf("expected DDecimal, got %T", d)
	}

	// CQL decimal wire format: [int] scale + [varint] unscaled_value.
	// apd.Decimal: value = (-1)^Negative * Coeff * 10^Exponent.
	var scale int32
	unscaled := new(big.Int)
	coeff := dec.Decimal.Coeff.MathBigInt()

	if dec.Decimal.Exponent >= 0 {
		// No fractional part (e.g. 1200 = 12 * 10^2): scale=0, unscaled=coeff*10^exp.
		scale = 0
		exp := new(big.Int).Exp(
			big.NewInt(10), big.NewInt(int64(dec.Decimal.Exponent)), nil,
		)
		unscaled.Mul(coeff, exp)
	} else {
		// Fractional (e.g. 123.45 = 12345 * 10^-2): scale=2, unscaled=12345.
		scale = -dec.Decimal.Exponent
		unscaled.Set(coeff)
	}

	if dec.Decimal.Negative {
		unscaled.Neg(unscaled)
	}

	varintBytes := bigIntToTwosComplement(unscaled)
	buf := make([]byte, 4+len(varintBytes))
	binary.BigEndian.PutUint32(buf[:4], uint32(scale))
	copy(buf[4:], varintBytes)
	return buf, false, nil
}

// bigIntToTwosComplement converts a big.Int to its two's complement big-endian
// byte representation, matching Java's BigInteger.toByteArray().
func bigIntToTwosComplement(n *big.Int) []byte {
	if n.Sign() == 0 {
		return []byte{0}
	}
	if n.Sign() > 0 {
		b := n.Bytes()
		if b[0]&0x80 != 0 {
			// High bit set — prepend a zero byte so it reads as positive.
			return append([]byte{0}, b...)
		}
		return b
	}
	// Negative: two's complement is the bitwise complement of (|n| - 1).
	abs := new(big.Int).Neg(n)
	abs.Sub(abs, big.NewInt(1))
	b := abs.Bytes()
	for i := range b {
		b[i] = ^b[i]
	}
	if len(b) == 0 || b[0]&0x80 == 0 {
		return append([]byte{0xff}, b...)
	}
	return b
}

func encodeSmallint(d tree.Datum) ([]byte, bool, error) {
	i, ok := d.(*tree.DInt)
	if !ok {
		return nil, false, errors.Newf("expected DInt, got %T", d)
	}
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(int16(*i)))
	return buf[:], false, nil
}

func encodeTinyint(d tree.Datum) ([]byte, bool, error) {
	i, ok := d.(*tree.DInt)
	if !ok {
		return nil, false, errors.Newf("expected DInt, got %T", d)
	}
	return []byte{byte(int8(*i))}, false, nil
}

// encodeDate encodes a CRDB date as CQL date: 4-byte unsigned int where
// 2^31 (0x80000000) represents the Unix epoch (1970-01-01).
func encodeDate(d tree.Datum) ([]byte, bool, error) {
	date, ok := d.(*tree.DDate)
	if !ok {
		return nil, false, errors.Newf("expected DDate, got %T", d)
	}
	unixDays := date.UnixEpochDays()
	// CQL date is centered at 2^31: epoch day = 2^31.
	cqlDays := uint32(int64(1<<31) + unixDays)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], cqlDays)
	return buf[:], false, nil
}

// encodeTime encodes a CRDB time as CQL time: 8-byte big-endian nanoseconds
// since midnight.
func encodeTime(d tree.Datum) ([]byte, bool, error) {
	t, ok := d.(*tree.DTime)
	if !ok {
		return nil, false, errors.Newf("expected DTime, got %T", d)
	}
	// DTime is microseconds since midnight; CQL time is nanoseconds.
	nanos := int64(*t) * 1000
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(nanos))
	return buf[:], false, nil
}

// encodeDuration encodes a CRDB interval as CQL duration using the vint
// encoding: three signed variable-length integers for months, days, and
// nanoseconds.
func encodeDuration(d tree.Datum) ([]byte, bool, error) {
	iv, ok := d.(*tree.DInterval)
	if !ok {
		return nil, false, errors.Newf("expected DInterval, got %T", d)
	}
	var buf []byte
	buf = appendSignedVint(buf, iv.Months)
	buf = appendSignedVint(buf, iv.Days)
	buf = appendSignedVint(buf, iv.Nanos())
	return buf, false, nil
}

// appendSignedVint appends a zigzag-encoded variable-length integer to buf,
// matching the CQL vint encoding used in duration values.
func appendSignedVint(buf []byte, n int64) []byte {
	// Zigzag encode: (n << 1) ^ (n >> 63).
	z := uint64((n << 1) ^ (n >> 63))
	for z >= 0x80 {
		buf = append(buf, byte(z)|0x80)
		z >>= 7
	}
	return append(buf, byte(z))
}

func appendInt32(buf []byte, v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return append(buf, b[:]...)
}
