// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cqlwire implements the CQL native protocol v4 binary frame codec.
//
// The CQL native protocol is a frame-based protocol used by Apache Cassandra.
// Each frame has a 9-byte header followed by a variable-length body:
//
//	0         8        16        24        32
//	+---------+---------+---------+---------+
//	| version |  flags  |      stream       |
//	+---------+---------+---------+---------+
//	| opcode  |      length                 |
//	+---------+---------+---------+---------+
//	|                body ...               |
//	+-----------------------------------------+
//
// Reference: CQL native protocol v4 spec, section 2.
package cqlwire

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
)

// maxFrameBodySize limits the body of a single CQL frame to 256 MB. The spec
// uses a 4-byte length field, but we impose a practical limit.
const maxFrameBodySize = 256 << 20

// FrameHeader is the fixed 9-byte header of a CQL v4 frame.
type FrameHeader struct {
	Version  ProtocolVersion
	Flags    HeaderFlag
	StreamID int16
	Opcode   Opcode
	Length   int32
}

// Frame is a complete CQL v4 frame: header plus body bytes.
type Frame struct {
	Header FrameHeader
	Body   []byte
}

// ReadHeader reads a 9-byte CQL v4 frame header from r.
func ReadHeader(r io.Reader) (FrameHeader, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return FrameHeader{}, errors.Wrap(err, "reading frame header")
	}
	h := FrameHeader{
		Version:  ProtocolVersion(buf[0]),
		Flags:    HeaderFlag(buf[1]),
		StreamID: int16(binary.BigEndian.Uint16(buf[2:4])),
		Opcode:   Opcode(buf[4]),
		Length:   int32(binary.BigEndian.Uint32(buf[5:9])),
	}
	if h.Length < 0 {
		return FrameHeader{}, errors.Newf(
			"invalid frame length %d", h.Length,
		)
	}
	if h.Length > int32(maxFrameBodySize) {
		return FrameHeader{}, errors.Newf(
			"frame body too large: %d bytes (max %d)", h.Length, maxFrameBodySize,
		)
	}
	return h, nil
}

// ReadFrame reads a complete CQL v4 frame (header + body) from r.
func ReadFrame(r io.Reader) (Frame, error) {
	h, err := ReadHeader(r)
	if err != nil {
		return Frame{}, err
	}
	body := make([]byte, h.Length)
	if h.Length > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return Frame{}, errors.Wrap(err, "reading frame body")
		}
	}
	return Frame{Header: h, Body: body}, nil
}

// WriteHeader writes the 9-byte frame header to w.
func WriteHeader(w io.Writer, h FrameHeader) error {
	var buf [HeaderSize]byte
	buf[0] = byte(h.Version)
	buf[1] = byte(h.Flags)
	binary.BigEndian.PutUint16(buf[2:4], uint16(h.StreamID))
	buf[4] = byte(h.Opcode)
	binary.BigEndian.PutUint32(buf[5:9], uint32(h.Length))
	_, err := w.Write(buf[:])
	return err
}

// WriteFrame writes a complete CQL v4 frame to w. The header's Length field
// is set to len(body) before writing.
func WriteFrame(w io.Writer, h FrameHeader, body []byte) error {
	h.Length = int32(len(body))
	if err := WriteHeader(w, h); err != nil {
		return errors.Wrap(err, "writing frame header")
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return errors.Wrap(err, "writing frame body")
		}
	}
	return nil
}

// FrameBuilder helps construct CQL v4 frames by accumulating the body into an
// internal buffer, then writing the complete frame.
type FrameBuilder struct {
	buf bytes.Buffer
}

// Reset clears the internal buffer for reuse.
func (fb *FrameBuilder) Reset() {
	fb.buf.Reset()
}

// Body returns a writer for appending to the frame body. The caller can use
// the Write* functions from types.go to serialize CQL types into it.
func (fb *FrameBuilder) Body() *bytes.Buffer {
	return &fb.buf
}

// Finish writes the completed frame (header + accumulated body) to w and
// resets the builder.
func (fb *FrameBuilder) Finish(
	w io.Writer, version ProtocolVersion, flags HeaderFlag, stream int16, op Opcode,
) error {
	h := FrameHeader{
		Version:  version,
		Flags:    flags,
		StreamID: stream,
		Opcode:   op,
		Length:   int32(fb.buf.Len()),
	}
	if err := WriteHeader(w, h); err != nil {
		fb.buf.Reset()
		return err
	}
	_, err := fb.buf.WriteTo(w)
	fb.buf.Reset()
	return err
}
