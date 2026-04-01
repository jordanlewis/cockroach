// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

import (
	"encoding/binary"
	"io"
)

// EncodeHeader writes the 8-byte TNS header into b. The caller must ensure
// that len(b) >= HeaderSize.
func EncodeHeader(b []byte, hdr Header) {
	binary.BigEndian.PutUint16(b[0:2], hdr.Length)
	binary.BigEndian.PutUint16(b[2:4], hdr.PacketChecksum)
	b[4] = uint8(hdr.Type)
	b[5] = hdr.Reserved
	binary.BigEndian.PutUint16(b[6:8], hdr.HeaderChecksum)
}

// WritePacket writes a complete TNS packet (header + payload) to w. It sets
// the header's Length field to HeaderSize + len(payload) before encoding.
func WritePacket(w io.Writer, typ PacketType, payload []byte) error {
	totalLen := HeaderSize + len(payload)
	buf := make([]byte, totalLen)
	EncodeHeader(buf, Header{
		Length: uint16(totalLen),
		Type:   typ,
	})
	copy(buf[HeaderSize:], payload)
	_, err := w.Write(buf)
	return err
}

// EncodeConnect serializes a ConnectPacket into a byte slice suitable for
// passing to WritePacket as the payload.
func EncodeConnect(pkt ConnectPacket) []byte {
	// The fixed body is 26 bytes. The connect string follows immediately after.
	const fixedSize = 26
	connectDataOffset := uint16(HeaderSize + fixedSize)
	connectDataLength := uint16(len(pkt.ConnectData))
	buf := make([]byte, fixedSize+len(pkt.ConnectData))

	binary.BigEndian.PutUint16(buf[0:2], pkt.Version)
	binary.BigEndian.PutUint16(buf[2:4], pkt.MinVersion)
	binary.BigEndian.PutUint16(buf[4:6], pkt.ServiceOptions)
	binary.BigEndian.PutUint16(buf[6:8], pkt.SDUSize)
	binary.BigEndian.PutUint16(buf[8:10], pkt.TDUSize)
	binary.BigEndian.PutUint16(buf[10:12], pkt.ProtocolCharacteristics)
	binary.BigEndian.PutUint16(buf[12:14], pkt.LineTurnaround)
	binary.BigEndian.PutUint16(buf[14:16], pkt.ValueOfOne)
	binary.BigEndian.PutUint16(buf[16:18], connectDataLength)
	binary.BigEndian.PutUint16(buf[18:20], connectDataOffset)
	binary.BigEndian.PutUint32(buf[20:24], pkt.MaxRecvConnectData)
	buf[24] = pkt.ConnectFlags0
	buf[25] = pkt.ConnectFlags1

	copy(buf[fixedSize:], pkt.ConnectData)
	return buf
}

// EncodeAccept serializes an AcceptPacket into a byte slice suitable for
// passing to WritePacket as the payload.
func EncodeAccept(pkt AcceptPacket) []byte {
	const fixedSize = 16
	acceptDataOffset := uint16(HeaderSize + fixedSize)
	acceptDataLength := uint16(len(pkt.AcceptData))
	buf := make([]byte, fixedSize+len(pkt.AcceptData))

	binary.BigEndian.PutUint16(buf[0:2], pkt.Version)
	binary.BigEndian.PutUint16(buf[2:4], pkt.ServiceOptions)
	binary.BigEndian.PutUint16(buf[4:6], pkt.SDUSize)
	binary.BigEndian.PutUint16(buf[6:8], pkt.TDUSize)
	binary.BigEndian.PutUint16(buf[8:10], pkt.ValueOfOne)
	binary.BigEndian.PutUint16(buf[10:12], acceptDataLength)
	binary.BigEndian.PutUint16(buf[12:14], acceptDataOffset)
	buf[14] = pkt.ConnectFlags0
	buf[15] = pkt.ConnectFlags1

	copy(buf[fixedSize:], pkt.AcceptData)
	return buf
}

// EncodeRefuse serializes a RefusePacket into a byte slice suitable for
// passing to WritePacket as the payload.
func EncodeRefuse(pkt RefusePacket) []byte {
	const fixedSize = 4
	dataLength := uint16(len(pkt.Data))
	buf := make([]byte, fixedSize+len(pkt.Data))

	buf[0] = pkt.SystemReason
	buf[1] = pkt.UserReason
	binary.BigEndian.PutUint16(buf[2:4], dataLength)
	copy(buf[fixedSize:], pkt.Data)
	return buf
}

// EncodeRedirect serializes a RedirectPacket into a byte slice suitable for
// passing to WritePacket as the payload.
func EncodeRedirect(pkt RedirectPacket) []byte {
	const fixedSize = 2
	dataLength := uint16(len(pkt.Data))
	buf := make([]byte, fixedSize+len(pkt.Data))

	binary.BigEndian.PutUint16(buf[0:2], dataLength)
	copy(buf[fixedSize:], pkt.Data)
	return buf
}

// EncodeData serializes a DataPacket into a byte slice suitable for passing
// to WritePacket as the payload.
func EncodeData(pkt DataPacket) []byte {
	const fixedSize = 2
	buf := make([]byte, fixedSize+len(pkt.Payload))

	binary.BigEndian.PutUint16(buf[0:2], uint16(pkt.Flags))
	copy(buf[fixedSize:], pkt.Payload)
	return buf
}

// EncodeMarker serializes a MarkerPacket into a byte slice suitable for
// passing to WritePacket as the payload.
func EncodeMarker(pkt MarkerPacket) []byte {
	return []byte{byte(pkt.Type), pkt.Data}
}
