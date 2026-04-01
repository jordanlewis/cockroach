// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tnswire

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeaderRoundTrip(t *testing.T) {
	hdr := Header{
		Length:         42,
		PacketChecksum: 0,
		Type:           PacketTypeData,
		Reserved:       0,
		HeaderChecksum: 0,
	}
	var buf [HeaderSize]byte
	EncodeHeader(buf[:], hdr)
	got := DecodeHeader(buf[:])
	require.Equal(t, hdr, got)
}

func TestHeaderPayloadSize(t *testing.T) {
	tests := []struct {
		name   string
		length uint16
		want   int
	}{
		{"header only", HeaderSize, 0},
		{"with payload", 100, 92},
		{"too short", 3, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := Header{Length: tt.length}
			require.Equal(t, tt.want, hdr.PayloadSize())
		})
	}
}

func TestReadWritePacket(t *testing.T) {
	payload := []byte("hello TNS")
	var buf bytes.Buffer
	require.NoError(t, WritePacket(&buf, PacketTypeData, payload))

	hdr, got, err := ReadPacket(&buf)
	require.NoError(t, err)
	require.Equal(t, PacketTypeData, hdr.Type)
	require.Equal(t, uint16(HeaderSize+len(payload)), hdr.Length)
	require.Equal(t, payload, got)
}

func TestReadPacketHeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WritePacket(&buf, PacketTypeMarker, nil))

	hdr, payload, err := ReadPacket(&buf)
	require.NoError(t, err)
	require.Equal(t, PacketTypeMarker, hdr.Type)
	require.Equal(t, uint16(HeaderSize), hdr.Length)
	require.Nil(t, payload)
}

func TestReadPacketTooShort(t *testing.T) {
	// Write a header with length < HeaderSize.
	var buf [HeaderSize]byte
	binary.BigEndian.PutUint16(buf[0:2], 4) // length = 4 (< 8)
	buf[4] = byte(PacketTypeData)
	r := bytes.NewReader(buf[:])
	_, _, err := ReadPacket(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "less than header size")
}

func TestConnectRoundTrip(t *testing.T) {
	connectStr := "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dbhost)(PORT=1521))" +
		"(CONNECT_DATA=(SERVICE_NAME=orcl)))"

	orig := ConnectPacket{
		Version:                 314,
		MinVersion:              300,
		ServiceOptions:          0x0C41,
		SDUSize:                 8192,
		TDUSize:                 32767,
		ProtocolCharacteristics: 0x7F08,
		LineTurnaround:          0,
		ValueOfOne:              1,
		MaxRecvConnectData:      512,
		ConnectFlags0:           0x41,
		ConnectFlags1:           0x41,
		ConnectData:             connectStr,
	}

	payload := EncodeConnect(orig)
	got, err := DecodeConnect(payload)
	require.NoError(t, err)

	require.Equal(t, orig.Version, got.Version)
	require.Equal(t, orig.MinVersion, got.MinVersion)
	require.Equal(t, orig.ServiceOptions, got.ServiceOptions)
	require.Equal(t, orig.SDUSize, got.SDUSize)
	require.Equal(t, orig.TDUSize, got.TDUSize)
	require.Equal(t, orig.ProtocolCharacteristics, got.ProtocolCharacteristics)
	require.Equal(t, orig.ValueOfOne, got.ValueOfOne)
	require.Equal(t, orig.MaxRecvConnectData, got.MaxRecvConnectData)
	require.Equal(t, orig.ConnectFlags0, got.ConnectFlags0)
	require.Equal(t, orig.ConnectFlags1, got.ConnectFlags1)
	require.Equal(t, orig.ConnectData, got.ConnectData)
}

func TestConnectPayloadTooShort(t *testing.T) {
	_, err := DecodeConnect([]byte{0x00, 0x01})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CONNECT payload too short")
}

func TestAcceptRoundTrip(t *testing.T) {
	orig := AcceptPacket{
		Version:        314,
		ServiceOptions: 0x0C41,
		SDUSize:        8192,
		TDUSize:        32767,
		ValueOfOne:     1,
		ConnectFlags0:  0x41,
		ConnectFlags1:  0x41,
		AcceptData:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}

	payload := EncodeAccept(orig)
	got, err := DecodeAccept(payload)
	require.NoError(t, err)

	require.Equal(t, orig.Version, got.Version)
	require.Equal(t, orig.ServiceOptions, got.ServiceOptions)
	require.Equal(t, orig.SDUSize, got.SDUSize)
	require.Equal(t, orig.TDUSize, got.TDUSize)
	require.Equal(t, orig.ValueOfOne, got.ValueOfOne)
	require.Equal(t, orig.ConnectFlags0, got.ConnectFlags0)
	require.Equal(t, orig.ConnectFlags1, got.ConnectFlags1)
	require.Equal(t, orig.AcceptData, got.AcceptData)
}

func TestRefuseRoundTrip(t *testing.T) {
	orig := RefusePacket{
		SystemReason: 2,
		UserReason:   34,
		Data:         "ORA-12505: TNS:listener does not know of SID",
	}

	payload := EncodeRefuse(orig)
	got, err := DecodeRefuse(payload)
	require.NoError(t, err)

	require.Equal(t, orig.SystemReason, got.SystemReason)
	require.Equal(t, orig.UserReason, got.UserReason)
	require.Equal(t, orig.Data, got.Data)
}

func TestRedirectRoundTrip(t *testing.T) {
	orig := RedirectPacket{
		Data: "(ADDRESS=(PROTOCOL=TCP)(HOST=newhost)(PORT=1521))",
	}

	payload := EncodeRedirect(orig)
	got, err := DecodeRedirect(payload)
	require.NoError(t, err)

	require.Equal(t, orig.Data, got.Data)
}

func TestDataRoundTrip(t *testing.T) {
	orig := DataPacket{
		Flags:   DataFlagEOF,
		Payload: []byte("SELECT 1 FROM DUAL"),
	}

	payload := EncodeData(orig)
	got, err := DecodeData(payload)
	require.NoError(t, err)

	require.Equal(t, orig.Flags, got.Flags)
	require.Equal(t, orig.Payload, got.Payload)
}

func TestDataEmptyPayload(t *testing.T) {
	orig := DataPacket{Flags: DataFlagConfirmation}

	payload := EncodeData(orig)
	got, err := DecodeData(payload)
	require.NoError(t, err)

	require.Equal(t, orig.Flags, got.Flags)
	require.Nil(t, got.Payload)
}

func TestMarkerRoundTrip(t *testing.T) {
	orig := MarkerPacket{
		Type: MarkerTypeAttention,
		Data: 0x01,
	}

	payload := EncodeMarker(orig)
	got, err := DecodeMarker(payload)
	require.NoError(t, err)

	require.Equal(t, orig.Type, got.Type)
	require.Equal(t, orig.Data, got.Data)
}

func TestPacketTypeString(t *testing.T) {
	tests := []struct {
		typ  PacketType
		want string
	}{
		{PacketTypeConnect, "CONNECT"},
		{PacketTypeAccept, "ACCEPT"},
		{PacketTypeRefuse, "REFUSE"},
		{PacketTypeRedirect, "REDIRECT"},
		{PacketTypeData, "DATA"},
		{PacketTypeMarker, "MARKER"},
		{PacketType(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, tt.typ.String())
		})
	}
}

func TestFullConnectPacketWireRoundTrip(t *testing.T) {
	// Test encoding a CONNECT packet through the full wire path:
	// ConnectPacket -> EncodeConnect -> WritePacket -> ReadPacket -> DecodeConnect
	connectStr := "(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=test)))"
	orig := ConnectPacket{
		Version:       314,
		MinVersion:    300,
		SDUSize:       8192,
		TDUSize:       32767,
		ValueOfOne:    1,
		ConnectFlags0: 0x41,
		ConnectFlags1: 0x41,
		ConnectData:   connectStr,
	}

	// Encode and write to wire.
	var buf bytes.Buffer
	payload := EncodeConnect(orig)
	require.NoError(t, WritePacket(&buf, PacketTypeConnect, payload))

	// Read from wire and decode.
	hdr, rawPayload, err := ReadPacket(&buf)
	require.NoError(t, err)
	require.Equal(t, PacketTypeConnect, hdr.Type)

	got, err := DecodeConnect(rawPayload)
	require.NoError(t, err)
	require.Equal(t, orig.ConnectData, got.ConnectData)
	require.Equal(t, orig.Version, got.Version)
	require.Equal(t, orig.SDUSize, got.SDUSize)
}
