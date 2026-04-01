// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

// HeaderSize is the size of a TNS packet header in bytes.
const HeaderSize = 8

// PacketType identifies the type of a TNS packet.
type PacketType uint8

const (
	// PacketTypeConnect is a connection request packet. It carries protocol
	// version negotiation parameters and a connect string that identifies the
	// target database service.
	PacketTypeConnect PacketType = 1

	// PacketTypeAccept is sent by the server to accept a connection request.
	// It echoes back the negotiated protocol parameters.
	PacketTypeAccept PacketType = 2

	// PacketTypeRefuse is sent by the server to refuse a connection request.
	// It carries system and user reason codes along with an error message.
	PacketTypeRefuse PacketType = 4

	// PacketTypeRedirect is sent by the server to redirect the client to a
	// different network address. It carries the redirect address string.
	PacketTypeRedirect PacketType = 5

	// PacketTypeData carries SQL statements from the client and result data
	// from the server. A 2-byte data flags field precedes the payload.
	PacketTypeData PacketType = 6

	// PacketTypeMarker is an attention/reset signal. The marker type byte
	// distinguishes between attention (type 1) and reset (type 0) markers.
	PacketTypeMarker PacketType = 12
)

// String returns the name of the packet type.
func (t PacketType) String() string {
	switch t {
	case PacketTypeConnect:
		return "CONNECT"
	case PacketTypeAccept:
		return "ACCEPT"
	case PacketTypeRefuse:
		return "REFUSE"
	case PacketTypeRedirect:
		return "REDIRECT"
	case PacketTypeData:
		return "DATA"
	case PacketTypeMarker:
		return "MARKER"
	default:
		return "UNKNOWN"
	}
}

// Header is the 8-byte fixed header present at the start of every TNS packet.
//
// The Length field gives the total packet size including the header itself, so
// the payload size is always Length - HeaderSize.
type Header struct {
	// Length is the total packet length in bytes, including this header.
	Length uint16
	// PacketChecksum is a checksum over the packet data. Modern TNS
	// implementations typically set this to zero and rely on TCP checksums.
	PacketChecksum uint16
	// Type identifies the kind of packet (CONNECT, DATA, etc.).
	Type PacketType
	// Reserved is a reserved byte that must be set to zero.
	Reserved uint8
	// HeaderChecksum is a checksum over the header bytes. Like PacketChecksum,
	// this is typically zero in modern implementations.
	HeaderChecksum uint16
}

// PayloadSize returns the number of payload bytes following the header. It
// returns 0 if Length is less than HeaderSize (which would be a malformed
// packet).
func (h Header) PayloadSize() int {
	if int(h.Length) < HeaderSize {
		return 0
	}
	return int(h.Length) - HeaderSize
}

// DataFlags contains control flags in the 2-byte prefix of a DATA packet
// payload.
type DataFlags uint16

const (
	// DataFlagSendToken indicates that the packet contains a security token.
	DataFlagSendToken DataFlags = 0x0001
	// DataFlagRequestConfirmation asks the peer to confirm receipt.
	DataFlagRequestConfirmation DataFlags = 0x0002
	// DataFlagConfirmation is a receipt confirmation from the peer.
	DataFlagConfirmation DataFlags = 0x0004
	// DataFlagEOF signals end-of-file on the data stream.
	DataFlagEOF DataFlags = 0x0040
)

// ConnectPacket represents the payload of a CONNECT (type 1) packet. It
// carries protocol version negotiation and a connect descriptor string that
// identifies the target database service.
type ConnectPacket struct {
	// Version is the TNS protocol version requested by the client.
	Version uint16
	// MinVersion is the minimum protocol version the client supports.
	MinVersion uint16
	// ServiceOptions is a bitmask of requested service options.
	ServiceOptions uint16
	// SDUSize is the Session Data Unit size in bytes.
	SDUSize uint16
	// TDUSize is the Transport Data Unit size in bytes.
	TDUSize uint16
	// ProtocolCharacteristics is a bitmask of NT protocol characteristics.
	ProtocolCharacteristics uint16
	// LineTurnaround is the line turnaround value.
	LineTurnaround uint16
	// ValueOfOne is used for byte-order detection (always 0x0001).
	ValueOfOne uint16
	// ConnectDataLength is the length of the connect string in bytes.
	ConnectDataLength uint16
	// ConnectDataOffset is the byte offset from the start of the packet
	// (including header) where the connect string begins.
	ConnectDataOffset uint16
	// MaxRecvConnectData is the maximum connect data the client can receive.
	MaxRecvConnectData uint32
	// ConnectFlags0 is the first connect flags byte.
	ConnectFlags0 uint8
	// ConnectFlags1 is the second connect flags byte.
	ConnectFlags1 uint8
	// ConnectData is the connect descriptor string, for example:
	//   (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=...)(PORT=...))(CONNECT_DATA=...))
	ConnectData string
}

// AcceptPacket represents the payload of an ACCEPT (type 2) packet.
type AcceptPacket struct {
	// Version is the TNS protocol version accepted by the server.
	Version uint16
	// ServiceOptions is the negotiated service options bitmask.
	ServiceOptions uint16
	// SDUSize is the negotiated Session Data Unit size.
	SDUSize uint16
	// TDUSize is the negotiated Transport Data Unit size.
	TDUSize uint16
	// ValueOfOne is used for byte-order detection.
	ValueOfOne uint16
	// AcceptDataLength is the length of accept data in bytes.
	AcceptDataLength uint16
	// AcceptDataOffset is the offset where accept data begins.
	AcceptDataOffset uint16
	// ConnectFlags0 is the first connect flags byte.
	ConnectFlags0 uint8
	// ConnectFlags1 is the second connect flags byte.
	ConnectFlags1 uint8
	// AcceptData is optional additional data returned by the server.
	AcceptData []byte
}

// RefusePacket represents the payload of a REFUSE (type 4) packet.
type RefusePacket struct {
	// SystemReason is the system-level reason code for the refusal.
	SystemReason uint8
	// UserReason is the user-level reason code for the refusal.
	UserReason uint8
	// DataLength is the length of the error message data.
	DataLength uint16
	// Data is the error message explaining the refusal.
	Data string
}

// RedirectPacket represents the payload of a REDIRECT (type 5) packet.
type RedirectPacket struct {
	// DataLength is the length of the redirect address data.
	DataLength uint16
	// Data is the redirect address string.
	Data string
}

// DataPacket represents the payload of a DATA (type 6) packet.
type DataPacket struct {
	// Flags contains data transfer control flags.
	Flags DataFlags
	// Payload is the raw data (SQL text, result rows, etc.).
	Payload []byte
}

// MarkerType distinguishes the kind of marker signal.
type MarkerType uint8

const (
	// MarkerTypeReset requests a connection reset.
	MarkerTypeReset MarkerType = 0
	// MarkerTypeAttention signals an attention/break request (e.g. cancel).
	MarkerTypeAttention MarkerType = 1
)

// MarkerPacket represents the payload of a MARKER (type 12) packet.
type MarkerPacket struct {
	// Type is the marker type (attention or reset).
	Type MarkerType
	// Data is an additional marker data byte.
	Data uint8
}
