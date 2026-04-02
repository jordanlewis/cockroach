// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
)

// maxPacketSize is the maximum TNS packet size we accept. TNS packet lengths
// are uint16 so the protocol maximum is 65535, but typical SDU sizes are 8192
// or 32767.
const maxPacketSize = 65535

// ReadHeader reads an 8-byte TNS header from r.
func ReadHeader(r io.Reader) (Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, errors.Wrap(err, "reading TNS header")
	}
	return DecodeHeader(buf[:]), nil
}

// DecodeHeader decodes an 8-byte TNS header from b. The caller must ensure
// that len(b) >= HeaderSize.
func DecodeHeader(b []byte) Header {
	return Header{
		Length:         binary.BigEndian.Uint16(b[0:2]),
		PacketChecksum: binary.BigEndian.Uint16(b[2:4]),
		Type:           PacketType(b[4]),
		Reserved:       b[5],
		HeaderChecksum: binary.BigEndian.Uint16(b[6:8]),
	}
}

// ReadPacket reads a complete TNS packet (header + payload) from r. It returns
// the decoded header and the raw payload bytes.
func ReadPacket(r io.Reader) (Header, []byte, error) {
	hdr, err := ReadHeader(r)
	if err != nil {
		return Header{}, nil, err
	}
	if hdr.Length < HeaderSize {
		return Header{}, nil, errors.Newf(
			"TNS packet length %d is less than header size %d", hdr.Length, HeaderSize,
		)
	}
	if hdr.Length > maxPacketSize {
		return Header{}, nil, errors.Newf(
			"TNS packet length %d exceeds maximum %d", hdr.Length, maxPacketSize,
		)
	}
	payloadSize := hdr.PayloadSize()
	if payloadSize == 0 {
		return hdr, nil, nil
	}
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Header{}, nil, errors.Wrap(err, "reading TNS payload")
	}
	return hdr, payload, nil
}

// DecodeConnect decodes a ConnectPacket from the raw payload of a CONNECT
// packet. The payload must not include the 8-byte header.
func DecodeConnect(payload []byte) (ConnectPacket, error) {
	// The fixed portion of a CONNECT body is 26 bytes:
	//   2 version + 2 min version + 2 service options + 2 SDU +
	//   2 TDU + 2 protocol characteristics + 2 line turnaround +
	//   2 value of one + 2 connect data length + 2 connect data offset +
	//   4 max recv connect data + 1 connect flags 0 + 1 connect flags 1
	const fixedSize = 26
	if len(payload) < fixedSize {
		return ConnectPacket{}, errors.Newf(
			"CONNECT payload too short: %d bytes, need at least %d", len(payload), fixedSize,
		)
	}
	pkt := ConnectPacket{
		Version:                 binary.BigEndian.Uint16(payload[0:2]),
		MinVersion:              binary.BigEndian.Uint16(payload[2:4]),
		ServiceOptions:          binary.BigEndian.Uint16(payload[4:6]),
		SDUSize:                 binary.BigEndian.Uint16(payload[6:8]),
		TDUSize:                 binary.BigEndian.Uint16(payload[8:10]),
		ProtocolCharacteristics: binary.BigEndian.Uint16(payload[10:12]),
		LineTurnaround:          binary.BigEndian.Uint16(payload[12:14]),
		ValueOfOne:              binary.BigEndian.Uint16(payload[14:16]),
		ConnectDataLength:       binary.BigEndian.Uint16(payload[16:18]),
		ConnectDataOffset:       binary.BigEndian.Uint16(payload[18:20]),
		MaxRecvConnectData:      binary.BigEndian.Uint32(payload[20:24]),
		ConnectFlags0:           payload[24],
		ConnectFlags1:           payload[25],
	}

	// The connect data offset is relative to the start of the entire packet
	// (including the 8-byte header), so subtract HeaderSize to get the offset
	// within the payload.
	dataStart := int(pkt.ConnectDataOffset) - HeaderSize
	dataEnd := dataStart + int(pkt.ConnectDataLength)
	if pkt.ConnectDataLength > 0 {
		if dataStart < 0 || dataEnd > len(payload) {
			return ConnectPacket{}, errors.Newf(
				"CONNECT data range [%d:%d] out of bounds for payload length %d",
				dataStart, dataEnd, len(payload),
			)
		}
		pkt.ConnectData = string(payload[dataStart:dataEnd])
	}
	return pkt, nil
}

// DecodeAccept decodes an AcceptPacket from the raw payload of an ACCEPT
// packet.
func DecodeAccept(payload []byte) (AcceptPacket, error) {
	// Fixed portion: 2 version + 2 service options + 2 SDU + 2 TDU +
	//   2 value of one + 2 accept data length + 2 accept data offset +
	//   1 connect flags 0 + 1 connect flags 1 = 16 bytes.
	const fixedSize = 16
	if len(payload) < fixedSize {
		return AcceptPacket{}, errors.Newf(
			"ACCEPT payload too short: %d bytes, need at least %d", len(payload), fixedSize,
		)
	}
	pkt := AcceptPacket{
		Version:          binary.BigEndian.Uint16(payload[0:2]),
		ServiceOptions:   binary.BigEndian.Uint16(payload[2:4]),
		SDUSize:          binary.BigEndian.Uint16(payload[4:6]),
		TDUSize:          binary.BigEndian.Uint16(payload[6:8]),
		ValueOfOne:       binary.BigEndian.Uint16(payload[8:10]),
		AcceptDataLength: binary.BigEndian.Uint16(payload[10:12]),
		AcceptDataOffset: binary.BigEndian.Uint16(payload[12:14]),
		ConnectFlags0:    payload[14],
		ConnectFlags1:    payload[15],
	}
	if pkt.AcceptDataLength > 0 {
		dataStart := int(pkt.AcceptDataOffset) - HeaderSize
		dataEnd := dataStart + int(pkt.AcceptDataLength)
		if dataStart < 0 || dataEnd > len(payload) {
			return AcceptPacket{}, errors.Newf(
				"ACCEPT data range [%d:%d] out of bounds for payload length %d",
				dataStart, dataEnd, len(payload),
			)
		}
		pkt.AcceptData = make([]byte, pkt.AcceptDataLength)
		copy(pkt.AcceptData, payload[dataStart:dataEnd])
	}
	return pkt, nil
}

// DecodeRefuse decodes a RefusePacket from the raw payload of a REFUSE packet.
func DecodeRefuse(payload []byte) (RefusePacket, error) {
	// Fixed portion: 1 system reason + 1 user reason + 2 data length = 4 bytes.
	const fixedSize = 4
	if len(payload) < fixedSize {
		return RefusePacket{}, errors.Newf(
			"REFUSE payload too short: %d bytes, need at least %d", len(payload), fixedSize,
		)
	}
	pkt := RefusePacket{
		SystemReason: payload[0],
		UserReason:   payload[1],
		DataLength:   binary.BigEndian.Uint16(payload[2:4]),
	}
	if pkt.DataLength > 0 {
		dataEnd := fixedSize + int(pkt.DataLength)
		if dataEnd > len(payload) {
			return RefusePacket{}, errors.Newf(
				"REFUSE data length %d exceeds available payload %d",
				pkt.DataLength, len(payload)-fixedSize,
			)
		}
		pkt.Data = string(payload[fixedSize:dataEnd])
	}
	return pkt, nil
}

// DecodeRedirect decodes a RedirectPacket from the raw payload of a REDIRECT
// packet.
func DecodeRedirect(payload []byte) (RedirectPacket, error) {
	// Fixed portion: 2 data length = 2 bytes.
	const fixedSize = 2
	if len(payload) < fixedSize {
		return RedirectPacket{}, errors.Newf(
			"REDIRECT payload too short: %d bytes, need at least %d", len(payload), fixedSize,
		)
	}
	pkt := RedirectPacket{
		DataLength: binary.BigEndian.Uint16(payload[0:2]),
	}
	if pkt.DataLength > 0 {
		dataEnd := fixedSize + int(pkt.DataLength)
		if dataEnd > len(payload) {
			return RedirectPacket{}, errors.Newf(
				"REDIRECT data length %d exceeds available payload %d",
				pkt.DataLength, len(payload)-fixedSize,
			)
		}
		pkt.Data = string(payload[fixedSize:dataEnd])
	}
	return pkt, nil
}

// DecodeData decodes a DataPacket from the raw payload of a DATA packet.
func DecodeData(payload []byte) (DataPacket, error) {
	// Fixed portion: 2 data flags = 2 bytes.
	const fixedSize = 2
	if len(payload) < fixedSize {
		return DataPacket{}, errors.Newf(
			"DATA payload too short: %d bytes, need at least %d", len(payload), fixedSize,
		)
	}
	pkt := DataPacket{
		Flags: DataFlags(binary.BigEndian.Uint16(payload[0:2])),
	}
	if len(payload) > fixedSize {
		pkt.Payload = make([]byte, len(payload)-fixedSize)
		copy(pkt.Payload, payload[fixedSize:])
	}
	return pkt, nil
}

// DecodeMarker decodes a MarkerPacket from the raw payload of a MARKER packet.
func DecodeMarker(payload []byte) (MarkerPacket, error) {
	// Fixed portion: 1 marker type + 1 marker data = 2 bytes.
	const fixedSize = 2
	if len(payload) < fixedSize {
		return MarkerPacket{}, errors.Newf(
			"MARKER payload too short: %d bytes, need at least %d", len(payload), fixedSize,
		)
	}
	return MarkerPacket{
		Type: MarkerType(payload[0]),
		Data: payload[1],
	}, nil
}
