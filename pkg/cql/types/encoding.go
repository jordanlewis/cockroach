// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
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

func appendInt32(buf []byte, v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return append(buf, b[:]...)
}
