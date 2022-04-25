// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding

import "github.com/cockroachdb/cockroach/pkg/util/encoding/encodingtype"

// Type represents the type of a value encoded by
// Encode{Null,NotNull,Varint,Uvarint,Float,Bytes}.
//go:generate stringer -type=Type
type Type encodingtype.T

// Type values.
// TODO(dan, arjun): Make this into a proto enum.
// The 'Type' annotations are necessary for producing stringer-generated values.
const (
	Unknown   Type = 0
	Null      Type = 1
	NotNull   Type = 2
	Int       Type = 3
	Float     Type = 4
	Decimal   Type = 5
	Bytes     Type = 6
	BytesDesc Type = 7 // Bytes encoded descendingly
	Time      Type = 8
	Duration  Type = 9
	True      Type = 10
	False     Type = 11
	UUID      Type = 12
	Array     Type = 13
	IPAddr    Type = 14
	// SentinelType is used for bit manipulation to check if the encoded type
	// value requires more than 4 bits, and thus will be encoded in two bytes. It
	// is not used as a type value, and thus intentionally overlaps with the
	// subsequent type value. The 'Type' annotation is intentionally omitted here.
	SentinelType      = 15
	JSON         Type = 15
	Tuple        Type = 16
	BitArray     Type = 17
	BitArrayDesc Type = 18 // BitArray encoded descendingly
	TimeTZ       Type = 19
	Geo          Type = 20
	GeoDesc      Type = 21
	ArrayKeyAsc  Type = 22 // Array key encoding
	ArrayKeyDesc Type = 23 // Array key encoded descendingly
	Box2D        Type = 24
	Void         Type = 25
)

// typMap maps an encoded type byte to a decoded Type. It's got 256 slots, one
// for every possible byte value.
var typMap [256]Type

func init() {
	buf := []byte{0}
	for i := range typMap {
		buf[0] = byte(i)
		typMap[i] = slowPeekType(buf)
	}
}

// PeekType peeks at the type of the value encoded at the start of b.
func PeekType(b []byte) Type {
	if len(b) >= 1 {
		return typMap[b[0]]
	}
	return Unknown
}

// slowPeekType is the old implementation of PeekType. It's used to generate
// the lookup table for PeekType.
func slowPeekType(b []byte) Type {
	if len(b) >= 1 {
		m := b[0]
		switch {
		case m == encodedNull, m == encodedNullDesc:
			return Null
		case m == encodedNotNull, m == encodedNotNullDesc:
			return NotNull
		case m == arrayKeyMarker:
			return ArrayKeyAsc
		case m == arrayKeyDescendingMarker:
			return ArrayKeyDesc
		case m == bytesMarker:
			return Bytes
		case m == bytesDescMarker:
			return BytesDesc
		case m == bitArrayMarker:
			return BitArray
		case m == bitArrayDescMarker:
			return BitArrayDesc
		case m == timeMarker:
			return Time
		case m == timeTZMarker:
			return TimeTZ
		case m == geoMarker:
			return Geo
		case m == box2DMarker:
			return Box2D
		case m == geoDescMarker:
			return GeoDesc
		case m == byte(Array):
			return Array
		case m == byte(True):
			return True
		case m == byte(False):
			return False
		case m == durationBigNegMarker, m == durationMarker, m == durationBigPosMarker:
			return Duration
		case m >= IntMin && m <= IntMax:
			return Int
		case m >= floatNaN && m <= floatNaNDesc:
			return Float
		case m >= decimalNaN && m <= decimalNaNDesc:
			return Decimal
		case m == voidMarker:
			return Void
		}
	}
	return Unknown
}
