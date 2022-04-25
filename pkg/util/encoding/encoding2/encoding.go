// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding2

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

// DecodeNonsortingUvarint decodes a value encoded by EncodeNonsortingUvarint. It
// returns the length of the encoded varint and value.
func DecodeNonsortingUvarint(buf []byte) (remaining []byte, length int, value uint64, err error) {
	// TODO(dan): Handle overflow.
	for i, b := range buf {
		value += uint64(b & 0x7f)
		if b < 0x80 {
			return buf[i+1:], i + 1, value, nil
		}
		value <<= 7
	}
	return buf, 0, 0, nil
}

// DecodeNonsortingUvarint decodes a value encoded by EncodeNonsortingUvarint. It
// returns the length of the encoded varint and value.
func DecodeNonsortingUvarint2(buf []byte) (remaining []byte, length int, value uint64, err error) {
	length = PeekLengthNonsortingUvarint(buf)
	switch length {
	case 0:
		return buf, 0, 0, nil
	case 1:
		value = uint64(buf[0] & 0x7f)
	case 2:
		value = uint64(buf[0]&0x7f)<<7 + uint64(buf[1]&0x7f)
	case 3:
		value = uint64(buf[0]&0x7f)<<14 + uint64(buf[1]&0x7f)<<7 + uint64(buf[2]&0x7f)
	case 4:
		value = uint64(buf[0]&0x7f)<<21 + uint64(buf[1]&0x7f)<<14 + uint64(buf[2]&0x7f)<<7 +
			uint64(buf[3]&0x7f)
	case 5:
		value = uint64(buf[0]&0x7f)<<28 + uint64(buf[1]&0x7f)<<21 + uint64(buf[2]&0x7f)<<14 +
			uint64(buf[3]&0x7f)<<7 + uint64(buf[4]&0x7f)
	case 6:
		value = uint64(buf[0]&0x7f)<<35 + uint64(buf[1]&0x7f)<<28 + uint64(buf[2]&0x7f)<<21 +
			uint64(buf[3]&0x7f)<<14 + uint64(buf[4]&0x7f)<<7 + uint64(buf[5]&0x7f)
	case 7:
		value = uint64(buf[0]&0x7f)<<42 + uint64(buf[1]&0x7f)<<35 + uint64(buf[2]&0x7f)<<28 +
			uint64(buf[3]&0x7f)<<21 + uint64(buf[4]&0x7f)<<14 + uint64(buf[5]&0x7f)<<7 +
			uint64(buf[6]&0x7f)
	case 8:
		value = uint64(buf[0]&0x7f)<<49 + uint64(buf[1]&0x7f)<<42 + uint64(buf[2]&0x7f)<<35 +
			uint64(buf[3]&0x7f)<<28 + uint64(buf[4]&0x7f)<<21 + uint64(buf[5]&0x7f)<<14 +
			uint64(buf[6]&0x7f)<<7 + uint64(buf[7]&0x7f)
	case 9:
		value = uint64(buf[0]&0x7f)<<56 + uint64(buf[1]&0x7f)<<49 + uint64(buf[2]&0x7f)<<42 +
			uint64(buf[3]&0x7f)<<35 + uint64(buf[4]&0x7f)<<28 + uint64(buf[5]&0x7f)<<21 +
			uint64(buf[6]&0x7f)<<14 + uint64(buf[7]&0x7f)<<7 + uint64(buf[8]&0x7f)
	default:
		value = uint64(buf[0]&0x7f)<<63 + uint64(buf[1]&0x7f)<<56 + uint64(buf[2]&0x7f)<<49 +
			uint64(buf[3]&0x7f)<<42 + uint64(buf[4]&0x7f)<<35 + uint64(buf[5]&0x7f)<<28 +
			uint64(buf[6]&0x7f)<<21 + uint64(buf[7]&0x7f)<<14 + uint64(buf[8]&0x7f)<<7 +
			uint64(buf[9]&0x7f)
	}
	return buf[length:], length, value, nil
}

// DecodeNonsortingUvarint decodes a value encoded by EncodeNonsortingUvarint. It
// returns the length of the encoded varint and value.
func DecodeNonsortingUvarint3(buf []byte) (remaining []byte, length int, value uint64, err error) {
	// TODO(dan): Handle overflow.
	n := 10
	if len(buf) < 10 {
		n = len(buf)
	}
	for i, b := range buf[:n] {
		value += uint64(b & 0x7f)
		if b < 0x80 {
			return buf[i+1:], i + 1, value, nil
		}
		value <<= 7
	}
	return buf, 0, 0, nil
}

// PeekLengthNonsortingUvarint returns the length of the value that starts at
// the beginning of buf and was encoded by EncodeNonsortingUvarint.
func PeekLengthNonsortingUvarint(buf []byte) int {
	for i, b := range buf {
		if b&0x80 == 0 {
			return i + 1
		}
	}
	return 0
}

// MaxNonsortingUvarintLen is the maximum length of an EncodeNonsortingUvarint
// encoded value.
const MaxNonsortingUvarintLen = 10

// EncodeNonsortingUvarint encodes a uint64, appends it to the supplied buffer,
// and returns the final buffer. The encoding used is similar to
// encoding/binary, but with the most significant bits first:
// - Unsigned integers are serialized 7 bits at a time, starting with the
//   most significant bits.
// - The most significant bit (msb) in each output byte indicates if there
//   is a continuation byte (msb = 1).
func EncodeNonsortingUvarint(appendTo []byte, x uint64) []byte {
	switch {
	case x < (1 << 7):
		return append(appendTo, byte(x))
	case x < (1 << 14):
		return append(appendTo, 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 21):
		return append(appendTo, 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 28):
		return append(appendTo, 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 35):
		return append(appendTo, 0x80|byte(x>>28), 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 42):
		return append(appendTo, 0x80|byte(x>>35), 0x80|byte(x>>28), 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 49):
		return append(appendTo, 0x80|byte(x>>42), 0x80|byte(x>>35), 0x80|byte(x>>28), 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 56):
		return append(appendTo, 0x80|byte(x>>49), 0x80|byte(x>>42), 0x80|byte(x>>35), 0x80|byte(x>>28), 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	case x < (1 << 63):
		return append(appendTo, 0x80|byte(x>>56), 0x80|byte(x>>49), 0x80|byte(x>>42), 0x80|byte(x>>35), 0x80|byte(x>>28), 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	default:
		return append(appendTo, 0x80|byte(x>>63), 0x80|byte(x>>56), 0x80|byte(x>>49), 0x80|byte(x>>42), 0x80|byte(x>>35), 0x80|byte(x>>28), 0x80|byte(x>>21), 0x80|byte(x>>14), 0x80|byte(x>>7), 0x7f&byte(x))
	}
}

// EncodeUint64Ascending encodes the uint64 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint64Ascending(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// DecodeUint64Ascending decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation. The remainder
// of the input buffer and the decoded uint64 are returned.
func DecodeUint64Ascending2(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.Errorf("insufficient bytes to decode uint64 int value")
	}
	_ = b[7] // bounds check hint to compiler; see golang.org/issue/14808
	return b[8:], uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56, nil
}

// DecodeUint64Ascending decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation. The remainder
// of the input buffer and the decoded uint64 are returned.
func DecodeUint64Ascending(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.Errorf("insufficient bytes to decode uint64 int value")
	}
	v := binary.BigEndian.Uint64(b)
	return b[8:], v, nil
}
