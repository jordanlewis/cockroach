// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// tnsdebug is a standalone TNS protocol debug server. It listens on a
// configurable port, accepts connections from real Oracle clients (sqlplus),
// and hex-dumps all incoming bytes while attempting the TNS handshake.
// This is used to iteratively develop sqlplus compatibility.
//
// Usage:
//
//	go run ./pkg/tns/cmd/tnsdebug -port 1521
//	sqlplus user/pass@//localhost:1521/ORCL
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/cockroachdb/cockroach/pkg/tns/auth"
	"github.com/cockroachdb/cockroach/pkg/tns/tnswire"
)

var port = flag.Int("port", 1521, "TCP port to listen on")

func main() {
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("TNS debug server listening on %s\n", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept: %v\n", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("\n=== New connection from %s ===\n", conn.RemoteAddr())

	// Phase 1: CONNECT/ACCEPT.
	fmt.Println("\n--- Phase 1: CONNECT ---")
	hdr, payload, err := readAndDump(conn, "CONNECT")
	if err != nil {
		fmt.Printf("ERROR reading CONNECT: %v\n", err)
		return
	}

	if hdr.Type != tnswire.PacketTypeConnect {
		fmt.Printf("Expected CONNECT (type 1), got type %d (%s)\n", hdr.Type, hdr.Type)
		return
	}

	connPkt, err := tnswire.DecodeConnect(payload)
	if err != nil {
		fmt.Printf("ERROR decoding CONNECT: %v\n", err)
		return
	}
	fmt.Printf("  Version: %d, MinVersion: %d, SDU: %d, TDU: %d\n",
		connPkt.Version, connPkt.MinVersion, connPkt.SDUSize, connPkt.TDUSize)
	fmt.Printf("  ConnectFlags: 0x%02x, 0x%02x\n", connPkt.ConnectFlags0, connPkt.ConnectFlags1)
	fmt.Printf("  ConnectData: %q\n", connPkt.ConnectData)

	negotiatedVersion := uint16(auth.ProtocolVersion)
	if connPkt.Version < negotiatedVersion {
		negotiatedVersion = connPkt.Version
	}
	acceptPayload := tnswire.EncodeAccept(tnswire.AcceptPacket{
		Version:        negotiatedVersion,
		ServiceOptions: connPkt.ServiceOptions,
		SDUSize:        auth.DefaultSDUSize,
		TDUSize:        auth.DefaultTDUSize,
		ValueOfOne:     1,
		ConnectFlags0:  connPkt.ConnectFlags0,
		ConnectFlags1:  connPkt.ConnectFlags1,
	})
	if err := tnswire.WritePacket(conn, tnswire.PacketTypeAccept, acceptPayload); err != nil {
		fmt.Printf("ERROR writing ACCEPT: %v\n", err)
		return
	}
	fmt.Println("  -> Sent ACCEPT")

	// Phase 2: Native Services Negotiation (NSN/ANO).
	fmt.Println("\n--- Phase 2: NSN ---")
	hdr, payload, err = readAndDump(conn, "NSN")
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	if hdr.Type == tnswire.PacketTypeData {
		dataPkt, _ := tnswire.DecodeData(payload)
		if len(dataPkt.Payload) >= 4 &&
			dataPkt.Payload[0] == 0xDE && dataPkt.Payload[1] == 0xAD &&
			dataPkt.Payload[2] == 0xBE && dataPkt.Payload[3] == 0xEF {
			fmt.Println("  Detected NSN (DEADBEEF) packet")
			if err := handleNSN(conn, dataPkt.Payload); err != nil {
				fmt.Printf("ERROR handling NSN: %v\n", err)
				return
			}
		} else {
			fmt.Printf("  Unexpected DATA payload (first byte: 0x%02x)\n", dataPkt.Payload[0])
			return
		}
	}

	// Phase 3: TTI messages (protocol neg, data type neg, auth).
	for i := 0; i < 30; i++ {
		fmt.Printf("\n--- Phase 3: TTI Message %d ---\n", i+1)
		hdr, payload, err = readAndDump(conn, fmt.Sprintf("tti%d", i+1))
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}

		fmt.Printf("  Packet type: %d (%s), length: %d\n", hdr.Type, hdr.Type, hdr.Length)

		if hdr.Type == tnswire.PacketTypeData {
			dataPkt, err := tnswire.DecodeData(payload)
			if err != nil {
				fmt.Printf("  ERROR decoding DATA: %v\n", err)
				continue
			}
			fmt.Printf("  Data flags: 0x%04x\n", dataPkt.Flags)
			if len(dataPkt.Payload) > 0 {
				fmt.Printf("  TTI func code: 0x%02x\n", dataPkt.Payload[0])

				if err := handleTTI(conn, dataPkt.Payload); err != nil {
					fmt.Printf("  ERROR handling TTI: %v\n", err)
					return
				}
			}
		} else if hdr.Type == tnswire.PacketTypeMarker {
			fmt.Println("  (MARKER - ignored)")
		}
	}
}

// handleNSN responds to the Oracle Native Services Negotiation (ANO) packet.
// The client's NSN starts with DEADBEEF and proposes services (auth, encryption,
// data integrity, supervisor). We respond accepting no additional services.
func handleNSN(conn net.Conn, payload []byte) error {
	if len(payload) < 10 {
		return fmt.Errorf("NSN payload too short: %d bytes", len(payload))
	}

	// Parse NSN header.
	nsnLen := int(payload[4])<<8 | int(payload[5])
	nsnVersion := int(payload[6])<<8 | int(payload[7])
	nsnOptions := int(payload[8])<<8 | int(payload[9])
	fmt.Printf("  NSN: len=%d, version=%d, options=0x%04x\n",
		nsnLen, nsnVersion, nsnOptions)

	numServices := 0
	if len(payload) >= 12 {
		numServices = int(payload[10])<<8 | int(payload[11])
	}
	fmt.Printf("  NSN: %d service groups\n", numServices)

	// Use the auth package's BuildNSNResponse which constructs a full
	// response with 4 services (supervisor, auth, encryption, data
	// integrity) declining all optional services.
	resp := auth.BuildNSNResponse(numServices)

	fmt.Printf("  -> Sending NSN response (%d bytes)\n", len(resp))
	fmt.Println(hex.Dump(resp))

	return writeDataPayload(conn, resp)
}

func handleTTI(conn net.Conn, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	funcCode := tnswire.TTIFuncCode(payload[0])
	switch funcCode {
	case auth.TTIProtocolNeg:
		fmt.Println("  -> Handling protocol negotiation")
		fmt.Printf("  Proto neg payload (%d bytes):\n", len(payload))
		fmt.Println(hex.Dump(payload))

		resp := []byte{
			byte(auth.TTIProtocolNeg),
			0x06,       // protocol version
			0x00, 0x00, // flags
			0x00, 0x00, // server banner length
			0x03, 0x69, // character set (873 = AL32UTF8)
		}
		return writeDataPayload(conn, resp)

	case auth.TTIDataTypeNeg:
		fmt.Println("  -> Handling data type negotiation")
		fmt.Printf("  Data type neg payload (%d bytes):\n", len(payload))
		fmt.Println(hex.Dump(payload))

		resp := []byte{
			byte(auth.TTIDataTypeNeg),
			0x03, 0x69, // charset AL32UTF8
			0x01,       // charset form
			0x01,       // compiletime charset form
			0x00, 0x01, // ncharset ID
		}
		return writeDataPayload(conn, resp)

	case auth.TTIAuth:
		fmt.Println("  -> Handling auth message")
		fmt.Printf("  Auth payload (%d bytes):\n", len(payload))
		fmt.Println(hex.Dump(payload))
		return handleAuthMessage(conn, payload)

	default:
		fmt.Printf("  -> Unhandled TTI func code 0x%02x\n", byte(funcCode))
		fmt.Printf("  Full payload (%d bytes):\n", len(payload))
		fmt.Println(hex.Dump(payload))
		return nil
	}
}

func handleAuthMessage(conn net.Conn, payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("auth payload too short")
	}

	kvPairs, err := decodeAuthKVPairsLoose(payload[1:])
	if err != nil {
		fmt.Printf("  Could not decode KV pairs: %v\n", err)
		return nil
	}

	fmt.Printf("  Auth KV pairs:\n")
	for k, v := range kvPairs {
		fmt.Printf("    %s = %q\n", k, v)
	}

	if _, hasACE := kvPairs["AUTH_ACE"]; hasACE {
		fmt.Println("  -> Sending auth challenge")
		return sendAuthChallenge(conn)
	}

	if _, hasPwd := kvPairs["AUTH_PASSWORD"]; hasPwd {
		fmt.Println("  -> Sending auth success")
		return sendAuthSuccess(conn)
	}

	fmt.Println("  -> Unknown auth message type")
	return nil
}

func sendAuthChallenge(conn net.Conn) error {
	sessKey := make([]byte, 48)
	if _, err := rand.Read(sessKey); err != nil {
		return err
	}
	salt := make([]byte, 10)
	if _, err := rand.Read(salt); err != nil {
		return err
	}

	kvPairs := map[string]string{
		"AUTH_SESSKEY":              hex.EncodeToString(sessKey),
		"AUTH_VFR_DATA":             hex.EncodeToString(salt),
		"AUTH_GLOBALLY_UNIQUE_DBID": "CockroachDB",
	}
	challengePayload := append(
		[]byte{byte(auth.TTIAuth)}, encodeAuthKVPairs(kvPairs)...,
	)
	return writeDataPayload(conn, challengePayload)
}

func sendAuthSuccess(conn net.Conn) error {
	nlsParams := auth.DefaultNLSParams()
	kvPairs := make(map[string]string, len(nlsParams)+1)
	kvPairs["AUTH_ALTER_SESSION"] = ""
	for _, p := range nlsParams {
		kvPairs[p.Name] = p.Value
	}

	successPayload := append(
		[]byte{byte(auth.TTIAuthResponse), 0x00},
		encodeAuthKVPairs(kvPairs)...,
	)
	return writeDataPayload(conn, successPayload)
}

func readAndDump(conn net.Conn, label string) (tnswire.Header, []byte, error) {
	hdr, payload, err := tnswire.ReadPacket(conn)
	if err != nil {
		if err == io.EOF {
			return tnswire.Header{}, nil, fmt.Errorf("EOF (client disconnected)")
		}
		return tnswire.Header{}, nil, err
	}

	fmt.Printf("  [%s] Header: len=%d, type=%d (%s)\n",
		label, hdr.Length, hdr.Type, hdr.Type)
	if len(payload) > 0 && len(payload) <= 512 {
		fmt.Printf("  [%s] Payload (%d bytes):\n", label, len(payload))
		fmt.Println(hex.Dump(payload))
	} else if len(payload) > 512 {
		fmt.Printf("  [%s] Payload (%d bytes, showing first 256):\n", label, len(payload))
		fmt.Println(hex.Dump(payload[:256]))
	}
	return hdr, payload, nil
}

func writeDataPayload(conn net.Conn, ttiPayload []byte) error {
	dataPayload := tnswire.EncodeData(tnswire.DataPacket{
		Flags:   0,
		Payload: ttiPayload,
	})
	return tnswire.WritePacket(conn, tnswire.PacketTypeData, dataPayload)
}

func encodeAuthKVPairs(pairs map[string]string) []byte {
	size := 2
	for k, v := range pairs {
		size += 2 + len(k) + 2 + len(v)
	}
	buf := make([]byte, size)
	off := 2
	buf[0] = byte(len(pairs) >> 8)
	buf[1] = byte(len(pairs))
	for k, v := range pairs {
		buf[off] = byte(len(k) >> 8)
		buf[off+1] = byte(len(k))
		off += 2
		copy(buf[off:], k)
		off += len(k)
		buf[off] = byte(len(v) >> 8)
		buf[off+1] = byte(len(v))
		off += 2
		copy(buf[off:], v)
		off += len(v)
	}
	return buf
}

// decodeAuthKVPairsLoose tries to decode O5LOGON KV pairs but is lenient
// about failures.
func decodeAuthKVPairsLoose(data []byte) (map[string]string, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("too short for pair count")
	}
	numPairs := int(data[0])<<8 | int(data[1])
	if numPairs > 100 {
		return nil, fmt.Errorf("suspicious pair count %d (first bytes: %02x %02x)", numPairs, data[0], data[1])
	}
	result := make(map[string]string, numPairs)
	off := 2
	for i := 0; i < numPairs; i++ {
		if off+2 > len(data) {
			return result, fmt.Errorf("truncated at pair %d key length", i)
		}
		keyLen := int(data[off])<<8 | int(data[off+1])
		off += 2
		if off+keyLen > len(data) {
			return result, fmt.Errorf("truncated at pair %d key (need %d, have %d)", i, keyLen, len(data)-off)
		}
		key := string(data[off : off+keyLen])
		off += keyLen
		if off+2 > len(data) {
			return result, fmt.Errorf("truncated at pair %d value length", i)
		}
		valLen := int(data[off])<<8 | int(data[off+1])
		off += 2
		if off+valLen > len(data) {
			return result, fmt.Errorf("truncated at pair %d value (need %d, have %d)", i, valLen, len(data)-off)
		}
		result[key] = string(data[off : off+valLen])
		off += valLen
	}
	return result, nil
}
