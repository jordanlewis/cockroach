// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tdswire implements the TDS (Tabular Data Stream) wire protocol
// packet codec used by Sybase and Microsoft SQL Server.
package tdswire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PacketType identifies the type of a TDS packet.
type PacketType uint8

const (
	// PacketTypeSQLBatch is a SQL batch request.
	PacketTypeSQLBatch PacketType = 1
	// PacketTypeTabularResult is a tabular result response.
	PacketTypeTabularResult PacketType = 4
	// PacketTypeAttention is an attention signal (cancel).
	PacketTypeAttention PacketType = 6
	// PacketTypeTransactionManager is a transaction manager request.
	PacketTypeTransactionManager PacketType = 14
	// PacketTypeLogin7 is a TDS 7.x login packet.
	PacketTypeLogin7 PacketType = 16
	// PacketTypePreLogin is a pre-login packet.
	PacketTypePreLogin PacketType = 18
)

// String returns a human-readable name for the packet type.
func (pt PacketType) String() string {
	switch pt {
	case PacketTypeSQLBatch:
		return "SQL_BATCH"
	case PacketTypeTabularResult:
		return "TABULAR_RESULT"
	case PacketTypeAttention:
		return "ATTENTION"
	case PacketTypeTransactionManager:
		return "TRANSACTION_MANAGER"
	case PacketTypeLogin7:
		return "LOGIN7"
	case PacketTypePreLogin:
		return "PRELOGIN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", pt)
	}
}

// PacketStatus represents status flags in a TDS packet header.
type PacketStatus uint8

const (
	// StatusNormal indicates a normal packet (not the last in a message).
	StatusNormal PacketStatus = 0x00
	// StatusEOM indicates end of message; this is the last packet.
	StatusEOM PacketStatus = 0x01
	// StatusIgnore indicates the packet should be ignored.
	StatusIgnore PacketStatus = 0x02
	// StatusResetConnection indicates the connection should be reset.
	StatusResetConnection PacketStatus = 0x08
)

// HeaderSize is the size of the TDS packet header in bytes.
const HeaderSize = 8

// DefaultPacketSize is the default maximum packet size (including header).
const DefaultPacketSize = 4096

// Header represents a TDS packet header.
type Header struct {
	// Type is the packet type.
	Type PacketType
	// Status contains status flags.
	Status PacketStatus
	// Length is the total packet length (header + data), big-endian on wire.
	Length uint16
	// SPID is the server process ID, big-endian on wire.
	SPID uint16
	// PacketID is the packet sequence number (mod 256).
	PacketID uint8
	// Window is reserved (should be 0).
	Window uint8
}

// MarshalBinary encodes the header into an 8-byte big-endian representation.
func (h *Header) MarshalBinary() []byte {
	buf := make([]byte, HeaderSize)
	buf[0] = byte(h.Type)
	buf[1] = byte(h.Status)
	binary.BigEndian.PutUint16(buf[2:4], h.Length)
	binary.BigEndian.PutUint16(buf[4:6], h.SPID)
	buf[6] = h.PacketID
	buf[7] = h.Window
	return buf
}

// UnmarshalBinary decodes an 8-byte big-endian header.
func (h *Header) UnmarshalBinary(buf []byte) error {
	if len(buf) < HeaderSize {
		return fmt.Errorf("tds: header too short: got %d bytes, need %d", len(buf), HeaderSize)
	}
	h.Type = PacketType(buf[0])
	h.Status = PacketStatus(buf[1])
	h.Length = binary.BigEndian.Uint16(buf[2:4])
	h.SPID = binary.BigEndian.Uint16(buf[4:6])
	h.PacketID = buf[6]
	h.Window = buf[7]
	return nil
}

// PacketReader reads TDS packets from an underlying io.Reader and reassembles
// multi-packet messages. The TDS protocol splits large messages across multiple
// packets; continuation is indicated by the absence of the EOM status flag.
type PacketReader struct {
	r io.Reader
}

// NewPacketReader creates a new PacketReader that reads from r.
func NewPacketReader(r io.Reader) *PacketReader {
	return &PacketReader{r: r}
}

// ReadMessage reads a complete TDS message, reassembling it from one or more
// packets. It returns the packet type from the first packet and the
// concatenated payload (without headers).
func (pr *PacketReader) ReadMessage() (PacketType, []byte, error) {
	var msgType PacketType
	var payload []byte
	first := true

	for {
		hdr, data, err := pr.readPacket()
		if err != nil {
			return 0, nil, err
		}
		if first {
			msgType = hdr.Type
			first = false
		}
		payload = append(payload, data...)
		if hdr.Status&StatusEOM != 0 {
			break
		}
	}
	return msgType, payload, nil
}

// readPacket reads a single TDS packet (header + data) from the underlying reader.
func (pr *PacketReader) readPacket() (Header, []byte, error) {
	var headerBuf [HeaderSize]byte
	if _, err := io.ReadFull(pr.r, headerBuf[:]); err != nil {
		return Header{}, nil, fmt.Errorf("tds: reading packet header: %w", err)
	}
	var hdr Header
	if err := hdr.UnmarshalBinary(headerBuf[:]); err != nil {
		return Header{}, nil, err
	}
	if hdr.Length < HeaderSize {
		return Header{}, nil, fmt.Errorf("tds: invalid packet length %d (less than header size)", hdr.Length)
	}
	dataLen := int(hdr.Length) - HeaderSize
	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(pr.r, data); err != nil {
			return Header{}, nil, fmt.Errorf("tds: reading packet data: %w", err)
		}
	}
	return hdr, data, nil
}

// PacketWriter writes TDS messages by splitting them into packets of at most
// maxPacketSize bytes (header included). The last packet is marked with EOM.
type PacketWriter struct {
	w             io.Writer
	maxPacketSize int
	packetID      uint8
}

// NewPacketWriter creates a new PacketWriter that writes to w with the given
// maximum packet size. If maxPacketSize is <= 0, DefaultPacketSize is used.
func NewPacketWriter(w io.Writer, maxPacketSize int) *PacketWriter {
	if maxPacketSize <= 0 {
		maxPacketSize = DefaultPacketSize
	}
	return &PacketWriter{
		w:             w,
		maxPacketSize: maxPacketSize,
	}
}

// WriteMessage writes a complete TDS message, splitting it into packets as
// needed. Each packet has the given packet type. The last packet is marked
// with StatusEOM.
func (pw *PacketWriter) WriteMessage(pktType PacketType, payload []byte) error {
	maxData := pw.maxPacketSize - HeaderSize
	if maxData <= 0 {
		return fmt.Errorf("tds: max packet size %d too small for header", pw.maxPacketSize)
	}

	offset := 0
	for {
		remaining := len(payload) - offset
		chunkSize := remaining
		if chunkSize > maxData {
			chunkSize = maxData
		}

		status := StatusNormal
		if offset+chunkSize >= len(payload) {
			status = StatusEOM
		}

		hdr := Header{
			Type:     pktType,
			Status:   status,
			Length:   uint16(HeaderSize + chunkSize),
			PacketID: pw.packetID,
		}
		pw.packetID++

		headerBytes := hdr.MarshalBinary()
		if _, err := pw.w.Write(headerBytes); err != nil {
			return fmt.Errorf("tds: writing packet header: %w", err)
		}
		if chunkSize > 0 {
			if _, err := pw.w.Write(payload[offset : offset+chunkSize]); err != nil {
				return fmt.Errorf("tds: writing packet data: %w", err)
			}
		}
		offset += chunkSize
		if status == StatusEOM {
			break
		}
	}
	return nil
}
