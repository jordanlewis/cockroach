// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
)

// NSN (Native Services Negotiation) handles the ANO (Advanced Networking
// Option) exchange that Oracle clients like sqlplus perform immediately
// after CONNECT/ACCEPT and before TTI protocol negotiation. The client
// proposes encryption, data integrity, authentication, and supervisor
// services. We respond declining all optional services (no encryption,
// no data integrity, no ANO auth — we use O5LOGON instead).
//
// The NSN packet starts with the 0xDEADBEEF magic marker and uses a
// TLV (type-length-value) encoding for each sub-packet within services.

// nsnMagic is the 4-byte magic marker that identifies NSN packets.
var nsnMagic = [4]byte{0xDE, 0xAD, 0xBE, 0xEF}

// NSN service type constants.
const (
	nsnServiceAuth          = 1
	nsnServiceEncryption    = 2
	nsnServiceDataIntegrity = 3
	nsnServiceSupervisor    = 4
)

// NSN TLV type constants for sub-packet encoding.
const (
	nsnTLVString  = 0 // UTF-8 string
	nsnTLVBytes   = 1 // raw bytes (also used for UB2Array)
	nsnTLVUB1     = 2 // 1-byte unsigned integer
	nsnTLVUB2     = 3 // 2-byte unsigned integer
	nsnTLVVersion = 5 // 4-byte version
	nsnTLVStatus  = 6 // 2-byte status code
)

// nsnHeaderSize is the size of the NSN header: DEADBEEF(4) +
// totalLen(2) + version(4) + serviceCount(2) + flags(1) = 13 bytes.
const nsnHeaderSize = 13

// nsnSupervisorStatusOK is the status value the supervisor service
// must return for the client to accept the negotiation.
const nsnSupervisorStatusOK = 31

// nsnAuthStatusInactive tells the client that ANO authentication is not
// active — the server will use standard Oracle authentication (O5LOGON)
// instead.
const nsnAuthStatusInactive = 0xFBFF

// handleNSN reads the NSN packet from the client and sends a response
// that declines all optional services (no encryption, no data integrity,
// no ANO authentication). This is called between CONNECT/ACCEPT and
// TTI protocol negotiation.
func (h *Handshaker) handleNSN() error {
	ttiPayload, err := h.readDataPayload()
	if err != nil {
		return err
	}

	// Verify the NSN magic marker.
	if len(ttiPayload) < 4 ||
		ttiPayload[0] != nsnMagic[0] || ttiPayload[1] != nsnMagic[1] ||
		ttiPayload[2] != nsnMagic[2] || ttiPayload[3] != nsnMagic[3] {
		// Not an NSN packet — the client might not use ANO (e.g. our
		// internal test client). Return the payload for the caller to
		// handle as a TTI message.
		h.pendingPayload = ttiPayload
		return nil
	}

	// Parse NSN header: DEADBEEF(4) + totalLen(2) + version(4) +
	// serviceCount(2) + flags(1) = 13 bytes.
	if len(ttiPayload) < nsnHeaderSize {
		return errors.Newf("NSN packet too short: %d bytes", len(ttiPayload))
	}
	serviceCount := int(binary.BigEndian.Uint16(ttiPayload[10:12]))

	// We don't need to fully parse the client's service proposals.
	// Just build a response that accepts the supervisor and declines
	// everything else.
	resp := BuildNSNResponse(serviceCount)
	return h.writeDataPayload(resp)
}

// BuildNSNResponse constructs the server's NSN response that declines
// all optional security services.
func BuildNSNResponse(clientServiceCount int) []byte {
	// We respond with 4 services regardless of what the client sent,
	// as Oracle clients expect all 4 service types in the response.
	const numServices = 4

	// Pre-calculate the total size.
	//
	// Each service has:
	//   8-byte service header (type:2 + sub-count:2 + error:4)
	//   + service-specific TLV data
	//
	// Supervisor (type 4):     8 + version(8) + status(6) + UB2Array(22) = 44
	// Auth (type 1):           8 + version(8) + status(6)                = 22
	// Encryption (type 2):     8 + version(8) + algoID(5)                = 21
	// Data integrity (type 3): 8 + version(8) + algoID(5)                = 21
	//
	// Total services = 44 + 22 + 21 + 21 = 108
	//
	// Header: DEADBEEF(4) + totalLen(2) + version(4) + serviceCount(2)
	//         + flags(1) = 13 bytes
	// Total = 13 (header) + 108 (services) = 121
	//
	// The length field is the total NSN packet size. Confirmed by
	// observing sqlplus 19.8 sending 0x0095 = 149 for a 149-byte
	// payload, with services starting at offset 13.
	const totalLen = 121

	buf := make([]byte, 0, totalLen)

	// NSN header.
	buf = append(buf, nsnMagic[:]...)
	buf = appendUint16(buf, totalLen)    // total packet length
	buf = appendUint32(buf, 0)           // ANO version
	buf = appendUint16(buf, numServices) // service count
	buf = append(buf, 0x00)              // flags

	// Service 1: Supervisor (type 4).
	buf = appendServiceHeader(buf, nsnServiceSupervisor, 3)
	buf = appendTLVVersion(buf, 0x13000000)
	buf = appendTLVStatus(buf, nsnSupervisorStatusOK)
	buf = appendTLVUB2Array(buf, []uint16{1, 1, 2, 1})

	// Service 2: Authentication (type 1).
	buf = appendServiceHeader(buf, nsnServiceAuth, 2)
	buf = appendTLVVersion(buf, 0x13000000)
	buf = appendTLVStatus(buf, nsnAuthStatusInactive)

	// Service 3: Encryption (type 2).
	buf = appendServiceHeader(buf, nsnServiceEncryption, 2)
	buf = appendTLVVersion(buf, 0x13000000)
	buf = appendTLVUB1(buf, 0) // algoID 0 = no encryption

	// Service 4: Data Integrity (type 3).
	buf = appendServiceHeader(buf, nsnServiceDataIntegrity, 2)
	buf = appendTLVVersion(buf, 0x13000000)
	buf = appendTLVUB1(buf, 0) // algoID 0 = no data integrity

	return buf
}

// appendServiceHeader appends an 8-byte service header.
func appendServiceHeader(buf []byte, serviceType int, subCount int) []byte {
	buf = appendUint16(buf, uint16(serviceType))
	buf = appendUint16(buf, uint16(subCount))
	buf = appendUint32(buf, 0) // error code
	return buf
}

// appendTLVVersion appends a version TLV (type 5, 4-byte value).
func appendTLVVersion(buf []byte, version uint32) []byte {
	buf = appendUint16(buf, 4)             // length
	buf = appendUint16(buf, nsnTLVVersion) // type
	buf = appendUint32(buf, version)
	return buf
}

// appendTLVStatus appends a status TLV (type 6, 2-byte value).
func appendTLVStatus(buf []byte, status uint16) []byte {
	buf = appendUint16(buf, 2)            // length
	buf = appendUint16(buf, nsnTLVStatus) // type
	buf = appendUint16(buf, status)
	return buf
}

// appendTLVUB1 appends a UB1 TLV (type 2, 1-byte value).
func appendTLVUB1(buf []byte, value byte) []byte {
	buf = appendUint16(buf, 1)         // length
	buf = appendUint16(buf, nsnTLVUB1) // type
	buf = append(buf, value)
	return buf
}

// appendTLVUB2Array appends a UB2Array TLV (type 1, DEADBEEF-prefixed array).
func appendTLVUB2Array(buf []byte, elements []uint16) []byte {
	// UB2Array data: DEADBEEF(4) + type_marker(2) + count(4) + elements(N*2)
	dataLen := 4 + 2 + 4 + len(elements)*2
	buf = appendUint16(buf, uint16(dataLen)) // length
	buf = appendUint16(buf, nsnTLVBytes)     // type = bytes
	buf = append(buf, nsnMagic[:]...)        // inner DEADBEEF
	buf = appendUint16(buf, 3)               // array type marker
	buf = appendUint32(buf, uint32(len(elements)))
	for _, e := range elements {
		buf = appendUint16(buf, e)
	}
	return buf
}

// appendUint16 appends a big-endian uint16.
func appendUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

// appendUint32 appends a big-endian uint32.
func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// isNSNPacket checks whether the given TTI payload starts with the
// DEADBEEF magic marker indicating an NSN packet.
func isNSNPacket(payload []byte) bool {
	return len(payload) >= 4 &&
		payload[0] == nsnMagic[0] && payload[1] == nsnMagic[1] &&
		payload[2] == nsnMagic[2] && payload[3] == nsnMagic[3]
}

// readDataPayloadOrPending returns the pending payload if one was saved
// by handleNSN (when the client didn't send NSN), or reads a new DATA
// packet.
func (h *Handshaker) readDataPayloadOrPending() ([]byte, error) {
	if h.pendingPayload != nil {
		p := h.pendingPayload
		h.pendingPayload = nil
		return p, nil
	}
	return h.readDataPayload()
}

// Compile-time check that io.ReadWriter is implemented by net.Conn.
var _ io.ReadWriter = (io.ReadWriter)(nil)
