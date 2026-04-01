// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tdswire

import (
	"encoding/binary"
	"fmt"
)

// PreLoginOptionToken identifies a PRELOGIN option.
type PreLoginOptionToken uint8

const (
	// PreLoginVersion is the version option.
	PreLoginVersion PreLoginOptionToken = 0
	// PreLoginEncryption is the encryption negotiation option.
	PreLoginEncryption PreLoginOptionToken = 1
	// PreLoginInstOpt is the instance name option.
	PreLoginInstOpt PreLoginOptionToken = 2
	// PreLoginThreadID is the thread ID option.
	PreLoginThreadID PreLoginOptionToken = 3
	// PreLoginMARS is the Multiple Active Result Sets option.
	PreLoginMARS PreLoginOptionToken = 4
)

// preLoginTerminator marks the end of the option token list.
const preLoginTerminator = 0xFF

// EncryptionLevel represents the PRELOGIN encryption negotiation value.
type EncryptionLevel uint8

const (
	// EncryptOff means encryption is available but off.
	EncryptOff EncryptionLevel = 0
	// EncryptOn means encryption is on.
	EncryptOn EncryptionLevel = 1
	// EncryptNotSup means encryption is not supported.
	EncryptNotSup EncryptionLevel = 2
	// EncryptReq means encryption is required.
	EncryptReq EncryptionLevel = 3
)

// PreLoginOption represents a single option in a PRELOGIN message.
type PreLoginOption struct {
	// Token identifies the option type.
	Token PreLoginOptionToken
	// Data contains the option value bytes.
	Data []byte
}

// PreLoginMsg represents a PRELOGIN message containing a list of options.
type PreLoginMsg struct {
	Options []PreLoginOption
}

// optionHeaderSize is the size of each option entry in the token list:
// 1 byte token + 2 bytes offset + 2 bytes length.
const optionHeaderSize = 5

// EncodePreLogin encodes a PreLoginMsg into a byte slice suitable for
// use as a TDS packet payload. The format is:
//
//	[option headers...] [0xFF terminator] [option data...]
//
// Each option header is: token(1) + offset(2, big-endian) + length(2, big-endian).
func EncodePreLogin(msg *PreLoginMsg) []byte {
	// Calculate the offset where option data begins.
	// The header section contains one 5-byte entry per option plus the 1-byte terminator.
	headerLen := len(msg.Options)*optionHeaderSize + 1 // +1 for 0xFF terminator

	// Calculate total data size.
	dataLen := 0
	for _, opt := range msg.Options {
		dataLen += len(opt.Data)
	}

	buf := make([]byte, headerLen+dataLen)

	// Write option headers.
	dataOffset := headerLen
	for i, opt := range msg.Options {
		pos := i * optionHeaderSize
		buf[pos] = byte(opt.Token)
		binary.BigEndian.PutUint16(buf[pos+1:pos+3], uint16(dataOffset))
		binary.BigEndian.PutUint16(buf[pos+3:pos+5], uint16(len(opt.Data)))
		dataOffset += len(opt.Data)
	}

	// Write terminator.
	buf[len(msg.Options)*optionHeaderSize] = preLoginTerminator

	// Write option data.
	dataOffset = headerLen
	for _, opt := range msg.Options {
		copy(buf[dataOffset:], opt.Data)
		dataOffset += len(opt.Data)
	}

	return buf
}

// DecodePreLogin decodes a PRELOGIN message payload into a PreLoginMsg.
func DecodePreLogin(buf []byte) (*PreLoginMsg, error) {
	if len(buf) == 0 {
		return nil, fmt.Errorf("tds: empty PRELOGIN message")
	}

	msg := &PreLoginMsg{}

	// Parse option headers until we hit the terminator.
	pos := 0
	type optEntry struct {
		token  PreLoginOptionToken
		offset uint16
		length uint16
	}
	var entries []optEntry

	for pos < len(buf) {
		if buf[pos] == preLoginTerminator {
			pos++
			break
		}
		if pos+optionHeaderSize > len(buf) {
			return nil, fmt.Errorf("tds: PRELOGIN option header truncated at offset %d", pos)
		}
		token := PreLoginOptionToken(buf[pos])
		offset := binary.BigEndian.Uint16(buf[pos+1 : pos+3])
		length := binary.BigEndian.Uint16(buf[pos+3 : pos+5])
		entries = append(entries, optEntry{token: token, offset: offset, length: length})
		pos += optionHeaderSize
	}

	// Extract option data.
	for _, e := range entries {
		end := int(e.offset) + int(e.length)
		if end > len(buf) {
			return nil, fmt.Errorf("tds: PRELOGIN option data out of bounds: offset=%d, length=%d, buf=%d",
				e.offset, e.length, len(buf))
		}
		data := make([]byte, e.length)
		copy(data, buf[e.offset:end])
		msg.Options = append(msg.Options, PreLoginOption{
			Token: e.token,
			Data:  data,
		})
	}

	return msg, nil
}

// PreLoginVersion represents the version fields in a PRELOGIN VERSION option.
type PreLoginVersionData struct {
	// Major is the major version.
	Major uint8
	// Minor is the minor version.
	Minor uint8
	// Build is the build number.
	Build uint16
	// SubBuild is the sub-build number.
	SubBuild uint16
}

// EncodeVersionData encodes version data into 6 bytes.
func EncodeVersionData(v PreLoginVersionData) []byte {
	buf := make([]byte, 6)
	buf[0] = v.Major
	buf[1] = v.Minor
	binary.BigEndian.PutUint16(buf[2:4], v.Build)
	binary.BigEndian.PutUint16(buf[4:6], v.SubBuild)
	return buf
}

// DecodeVersionData decodes 6 bytes into version data.
func DecodeVersionData(buf []byte) (PreLoginVersionData, error) {
	if len(buf) < 6 {
		return PreLoginVersionData{}, fmt.Errorf("tds: version data too short: %d bytes", len(buf))
	}
	return PreLoginVersionData{
		Major:    buf[0],
		Minor:    buf[1],
		Build:    binary.BigEndian.Uint16(buf[2:4]),
		SubBuild: binary.BigEndian.Uint16(buf[4:6]),
	}, nil
}
