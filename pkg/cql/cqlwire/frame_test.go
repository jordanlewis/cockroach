// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cqlwire

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeaderRoundTrip(t *testing.T) {
	cases := []FrameHeader{
		{Version: ProtoV4Request, Flags: 0, StreamID: 0, Opcode: OpStartup, Length: 0},
		{Version: ProtoV4Response, Flags: FlagTracing, StreamID: 42, Opcode: OpReady, Length: 100},
		{Version: ProtoV4Request, Flags: FlagCompressed | FlagWarning, StreamID: -1, Opcode: OpQuery, Length: 1024},
	}
	for _, want := range cases {
		var buf bytes.Buffer
		require.NoError(t, WriteHeader(&buf, want))
		require.Equal(t, HeaderSize, buf.Len())

		got, err := ReadHeader(&buf)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	body := []byte("SELECT * FROM system.local")
	h := FrameHeader{
		Version:  ProtoV4Request,
		Flags:    0,
		StreamID: 1,
		Opcode:   OpQuery,
	}

	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, h, body))

	got, err := ReadFrame(&buf)
	require.NoError(t, err)
	require.Equal(t, ProtoV4Request, got.Header.Version)
	require.Equal(t, OpQuery, got.Header.Opcode)
	require.Equal(t, int16(1), got.Header.StreamID)
	require.Equal(t, int32(len(body)), got.Header.Length)
	require.Equal(t, body, got.Body)
}

func TestFrameEmptyBody(t *testing.T) {
	h := FrameHeader{
		Version:  ProtoV4Request,
		Flags:    0,
		StreamID: 0,
		Opcode:   OpOptions,
	}
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, h, nil))

	got, err := ReadFrame(&buf)
	require.NoError(t, err)
	require.Equal(t, OpOptions, got.Header.Opcode)
	require.Empty(t, got.Body)
}

func TestFrameBuilderFinish(t *testing.T) {
	var fb FrameBuilder
	body := fb.Body()
	require.NoError(t, WriteString(body, "CQL_VERSION"))
	require.NoError(t, WriteString(body, "3.0.0"))

	var out bytes.Buffer
	require.NoError(t, fb.Finish(&out, ProtoV4Request, 0, 1, OpStartup))

	got, err := ReadFrame(&out)
	require.NoError(t, err)
	require.Equal(t, OpStartup, got.Header.Opcode)

	// Parse the body back.
	r := bytes.NewReader(got.Body)
	k, err := ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "CQL_VERSION", k)
	v, err := ReadString(r)
	require.NoError(t, err)
	require.Equal(t, "3.0.0", v)
}

func TestReadHeaderTooLargeBody(t *testing.T) {
	h := FrameHeader{
		Version: ProtoV4Request,
		Flags:   0,
		Opcode:  OpQuery,
		Length:  int32(maxFrameBodySize) + 1,
	}
	var buf bytes.Buffer
	// Write header bytes directly to test the size check.
	require.NoError(t, WriteHeader(&buf, h))
	_, err := ReadHeader(&buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "frame body too large")
}

func TestTypesShortRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteShort(&buf, 12345))
	v, err := ReadShort(&buf)
	require.NoError(t, err)
	require.Equal(t, uint16(12345), v)
}

func TestTypesIntRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteInt(&buf, -42))
	v, err := ReadInt(&buf)
	require.NoError(t, err)
	require.Equal(t, int32(-42), v)
}

func TestTypesLongRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteLong(&buf, 1<<40))
	v, err := ReadLong(&buf)
	require.NoError(t, err)
	require.Equal(t, int64(1<<40), v)
}

func TestTypesStringRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteString(&buf, "hello world"))
	v, err := ReadString(&buf)
	require.NoError(t, err)
	require.Equal(t, "hello world", v)
}

func TestTypesLongStringRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteLongString(&buf, "a longer string"))
	v, err := ReadLongString(&buf)
	require.NoError(t, err)
	require.Equal(t, "a longer string", v)
}

func TestTypesBytesRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	require.NoError(t, WriteBytes(&buf, data))
	v, err := ReadBytes(&buf)
	require.NoError(t, err)
	require.Equal(t, data, v)
}

func TestTypesBytesNull(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteBytes(&buf, nil))
	v, err := ReadBytes(&buf)
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestTypesShortBytesRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	data := []byte{0x01, 0x02, 0x03}
	require.NoError(t, WriteShortBytes(&buf, data))
	v, err := ReadShortBytes(&buf)
	require.NoError(t, err)
	require.Equal(t, data, v)
}

func TestTypesStringListRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	list := []string{"alpha", "bravo", "charlie"}
	require.NoError(t, WriteStringList(&buf, list))
	v, err := ReadStringList(&buf)
	require.NoError(t, err)
	require.Equal(t, list, v)
}

func TestTypesStringMapRoundTrip(t *testing.T) {
	// Use a single-entry map to avoid non-deterministic iteration order.
	var buf bytes.Buffer
	m := map[string]string{"CQL_VERSION": "3.0.0"}
	require.NoError(t, WriteStringMap(&buf, m))
	v, err := ReadStringMap(&buf)
	require.NoError(t, err)
	require.Equal(t, m, v)
}

func TestTypesStringMultiMapRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	m := map[string][]string{
		"COMPRESSION": {"snappy", "lz4"},
	}
	require.NoError(t, WriteStringMultiMap(&buf, m))
	v, err := ReadStringMultiMap(&buf)
	require.NoError(t, err)
	require.Equal(t, m, v)
}

func TestTypesConsistencyRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteConsistency(&buf, ConsistencyQuorum))
	v, err := ReadConsistency(&buf)
	require.NoError(t, err)
	require.Equal(t, ConsistencyQuorum, v)
}

func TestOpcodeString(t *testing.T) {
	require.Equal(t, "STARTUP", OpStartup.String())
	require.Equal(t, "QUERY", OpQuery.String())
	require.Equal(t, "ERROR", OpError.String())
	require.Equal(t, "READY", OpReady.String())
	require.Equal(t, "UNKNOWN", Opcode(0xFF).String())
}

func TestOpcodeIsRequest(t *testing.T) {
	require.True(t, OpStartup.IsRequest())
	require.True(t, OpQuery.IsRequest())
	require.False(t, OpReady.IsRequest())
	require.False(t, OpError.IsRequest())
}

func TestProtocolVersionDirection(t *testing.T) {
	require.True(t, ProtoV4Request.IsRequest())
	require.False(t, ProtoV4Request.IsResponse())
	require.True(t, ProtoV4Response.IsResponse())
	require.False(t, ProtoV4Response.IsRequest())
}

func TestConsistencyString(t *testing.T) {
	require.Equal(t, "ONE", ConsistencyOne.String())
	require.Equal(t, "QUORUM", ConsistencyQuorum.String())
	require.Equal(t, "LOCAL_QUORUM", ConsistencyLocalQuorum.String())
	require.Equal(t, "UNKNOWN", Consistency(0xFF).String())
}

// TestReadHeaderNegativeLength verifies that a header with a negative length
// (most significant bit set in the 4-byte length field) is rejected.
func TestReadHeaderNegativeLength(t *testing.T) {
	h := FrameHeader{
		Version: ProtoV4Request,
		Opcode:  OpQuery,
		Length:  -1,
	}
	var buf bytes.Buffer
	require.NoError(t, WriteHeader(&buf, h))
	_, err := ReadHeader(&buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid frame length")
}
