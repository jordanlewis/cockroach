// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tdswire

import (
	"encoding/binary"
	"fmt"
	"unicode/utf16"
)

// Login7FixedLen is the size of the fixed-length portion of a LOGIN7 packet.
// This includes all fields up to and including the variable-length offset/length
// pairs. The fixed header spans bytes 0-93 (94 bytes).
const Login7FixedLen = 94

// Login7 represents a parsed TDS LOGIN7 packet.
type Login7 struct {
	// TDSVersion is the TDS protocol version.
	TDSVersion uint32
	// PacketSize is the requested packet size.
	PacketSize uint32
	// ClientVersion is the client program version.
	ClientVersion uint32
	// ClientPID is the client process ID.
	ClientPID uint32
	// ConnectionID is the connection ID.
	ConnectionID uint32

	// OptionFlags1 contains the first set of option flags.
	OptionFlags1 uint8
	// OptionFlags2 contains the second set of option flags.
	OptionFlags2 uint8
	// OptionFlags3 contains the third set of option flags.
	OptionFlags3 uint8
	// TypeFlags contains type flags.
	TypeFlags uint8

	// Timezone is the client timezone (in minutes, signed).
	Timezone int32
	// Collation is the client collation (LCID).
	Collation uint32

	// Hostname is the client hostname.
	Hostname string
	// Username is the login username.
	Username string
	// Password is the login password (deobfuscated).
	Password string
	// AppName is the client application name.
	AppName string
	// ServerName is the target server name.
	ServerName string
	// LibraryName is the client library name.
	LibraryName string
	// Language is the initial language.
	Language string
	// Database is the initial database.
	Database string
}

// login7VarField describes a variable-length field in the LOGIN7 packet.
// Each field is identified by its offset within the fixed header where
// the (offset, length) pair is stored.
type login7VarField struct {
	name       string
	offsetPos  int // position of the offset uint16 in the fixed header
	needsDeobf bool
}

// Positions of the offset/length pairs in the LOGIN7 fixed header.
// Each pair is 4 bytes: offset (uint16 LE) + length (uint16 LE, in chars for Unicode).
var login7VarFields = []login7VarField{
	{"hostname", 36, false},
	{"username", 40, false},
	{"password", 44, true},
	{"appname", 48, false},
	{"servername", 52, false},
	// Byte 56 is the "unused" / extension offset
	{"libraryname", 60, false},
	{"language", 64, false},
	{"database", 68, false},
}

// DecodeLogin7 parses a LOGIN7 message payload (the data after the packet
// header has been stripped). The payload uses little-endian byte order.
func DecodeLogin7(buf []byte) (*Login7, error) {
	if len(buf) < Login7FixedLen {
		return nil, fmt.Errorf("tds: LOGIN7 message too short: %d bytes (need at least %d)",
			len(buf), Login7FixedLen)
	}

	// The first 4 bytes are the total length of the LOGIN7 data.
	totalLen := binary.LittleEndian.Uint32(buf[0:4])
	if int(totalLen) > len(buf) {
		return nil, fmt.Errorf("tds: LOGIN7 declared length %d exceeds buffer size %d",
			totalLen, len(buf))
	}

	l := &Login7{
		TDSVersion:    binary.LittleEndian.Uint32(buf[4:8]),
		PacketSize:    binary.LittleEndian.Uint32(buf[8:12]),
		ClientVersion: binary.LittleEndian.Uint32(buf[12:16]),
		ClientPID:     binary.LittleEndian.Uint32(buf[16:20]),
		ConnectionID:  binary.LittleEndian.Uint32(buf[20:24]),
		OptionFlags1:  buf[24],
		OptionFlags2:  buf[25],
		TypeFlags:     buf[26],
		OptionFlags3:  buf[27],
		Timezone:      int32(binary.LittleEndian.Uint32(buf[28:32])),
		Collation:     binary.LittleEndian.Uint32(buf[32:36]),
	}

	// Parse variable-length fields.
	for _, field := range login7VarFields {
		offset := binary.LittleEndian.Uint16(buf[field.offsetPos : field.offsetPos+2])
		length := binary.LittleEndian.Uint16(buf[field.offsetPos+2 : field.offsetPos+4])

		if length == 0 {
			continue
		}

		// Length is in UCS-2 characters; each char is 2 bytes.
		byteLen := int(length) * 2
		byteOffset := int(offset)
		if byteOffset+byteLen > len(buf) {
			return nil, fmt.Errorf("tds: LOGIN7 field %s out of bounds: offset=%d, length=%d chars, buf=%d",
				field.name, offset, length, len(buf))
		}

		rawBytes := make([]byte, byteLen)
		copy(rawBytes, buf[byteOffset:byteOffset+byteLen])

		if field.needsDeobf {
			deobfuscatePassword(rawBytes)
		}

		s := decodeUTF16LE(rawBytes)

		switch field.name {
		case "hostname":
			l.Hostname = s
		case "username":
			l.Username = s
		case "password":
			l.Password = s
		case "appname":
			l.AppName = s
		case "servername":
			l.ServerName = s
		case "libraryname":
			l.LibraryName = s
		case "language":
			l.Language = s
		case "database":
			l.Database = s
		}
	}

	return l, nil
}

// deobfuscatePassword deobfuscates a TDS LOGIN7 password in-place.
// Each byte has its high and low nibbles swapped, then is XORed with 0xA5.
func deobfuscatePassword(buf []byte) {
	for i := range buf {
		b := buf[i]
		// Swap the high and low nibbles.
		b = (b << 4) | (b >> 4)
		// XOR with 0xA5.
		b ^= 0xA5
		buf[i] = b
	}
}

// ObfuscatePassword obfuscates a password for a TDS LOGIN7 packet.
// This is the inverse of deobfuscatePassword: XOR with 0xA5 then swap nibbles.
func ObfuscatePassword(buf []byte) {
	for i := range buf {
		b := buf[i]
		b ^= 0xA5
		b = (b << 4) | (b >> 4)
		buf[i] = b
	}
}

// decodeUTF16LE decodes a little-endian UTF-16 byte slice into a Go string.
func decodeUTF16LE(b []byte) string {
	if len(b)%2 != 0 {
		// Truncate to even length.
		b = b[:len(b)-1]
	}
	u16 := make([]uint16, len(b)/2)
	for i := range u16 {
		u16[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}
	return string(utf16.Decode(u16))
}

// encodeUTF16LE encodes a Go string into a little-endian UTF-16 byte slice.
func encodeUTF16LE(s string) []byte {
	u16 := utf16.Encode([]rune(s))
	b := make([]byte, len(u16)*2)
	for i, v := range u16 {
		binary.LittleEndian.PutUint16(b[i*2:i*2+2], v)
	}
	return b
}
