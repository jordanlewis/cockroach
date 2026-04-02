// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/stretchr/testify/require"
)

func TestMatch(t *testing.T) {
	// Valid CQL v4 STARTUP frame header.
	var header [cqlwire.HeaderSize]byte
	header[0] = byte(cqlwire.ProtoV4Request)
	header[4] = byte(cqlwire.OpStartup)
	require.True(t, Match(bytes.NewReader(header[:])))

	// Valid CQL v4 OPTIONS frame header.
	header[4] = byte(cqlwire.OpOptions)
	require.True(t, Match(bytes.NewReader(header[:])))

	// Response version should not match.
	header[0] = byte(cqlwire.ProtoV4Response)
	require.False(t, Match(bytes.NewReader(header[:])))

	// Too-short input should not match.
	require.False(t, Match(bytes.NewReader([]byte{0x04})))
}

// sendStartup writes a STARTUP frame with the given options.
func sendStartup(t *testing.T, conn net.Conn, opts map[string]string) {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteStringMap(&body, opts))
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version: cqlwire.ProtoV4Request,
			Opcode:  cqlwire.OpStartup,
		}, body.Bytes(),
	))
}

// sendAuthResponse writes an AUTH_RESPONSE frame with SASL PLAIN
// credentials.
func sendAuthResponse(t *testing.T, conn net.Conn, user, pass string) {
	t.Helper()
	token := append([]byte{0}, user...)
	token = append(token, 0)
	token = append(token, pass...)
	var body bytes.Buffer
	require.NoError(t, cqlwire.WriteBytes(&body, token))
	require.NoError(t, cqlwire.WriteFrame(
		conn, cqlwire.FrameHeader{
			Version: cqlwire.ProtoV4Request,
			Opcode:  cqlwire.OpAuthResponse,
		}, body.Bytes(),
	))
}

func TestServerHandshakeNoAuth(t *testing.T) {
	s := MakeServer(ServerConfig{Insecure: true})
	server, client := net.Pipe()
	defer client.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- s.ServeConn(ctx, server) }()

	// Send STARTUP.
	sendStartup(t, client, map[string]string{
		"CQL_VERSION": "3.4.5",
	})

	// Expect READY.
	frame, err := cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpReady, frame.Header.Opcode)
	require.Equal(t, cqlwire.ProtoV4Response, frame.Header.Version)

	client.Close()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestServerHandshakeWithAuth(t *testing.T) {
	s := MakeServer(ServerConfig{
		Authenticator: AllowAllAuthenticator{},
	})
	server, client := net.Pipe()
	defer client.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- s.ServeConn(ctx, server) }()

	// Send STARTUP.
	sendStartup(t, client, map[string]string{
		"CQL_VERSION": "3.4.5",
	})

	// Expect AUTHENTICATE.
	frame, err := cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpAuthenticate, frame.Header.Opcode)

	// Send AUTH_RESPONSE.
	sendAuthResponse(t, client, "cassandra", "cassandra")

	// Expect AUTH_SUCCESS.
	frame, err = cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpAuthSuccess, frame.Header.Opcode)

	client.Close()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestServerOptionsBeforeStartup(t *testing.T) {
	s := MakeServer(ServerConfig{Insecure: true})
	server, client := net.Pipe()
	defer client.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- s.ServeConn(ctx, server) }()

	// Send OPTIONS first.
	require.NoError(t, cqlwire.WriteFrame(
		client, cqlwire.FrameHeader{
			Version: cqlwire.ProtoV4Request,
			Opcode:  cqlwire.OpOptions,
		}, nil,
	))

	// Expect SUPPORTED.
	frame, err := cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpSupported, frame.Header.Opcode)

	// Send STARTUP.
	sendStartup(t, client, map[string]string{
		"CQL_VERSION": "3.4.5",
	})

	// Expect READY.
	frame, err = cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpReady, frame.Header.Opcode)

	client.Close()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestServerDrainRejectsNewConns(t *testing.T) {
	s := MakeServer(ServerConfig{Insecure: true})

	ctx := context.Background()
	// Start draining with a short wait.
	go func() {
		_ = s.Drain(ctx, 10*time.Millisecond)
	}()
	// Give drain time to set the flag.
	time.Sleep(20 * time.Millisecond)

	// New connection should be rejected.
	server, client := net.Pipe()
	defer client.Close()

	// Run ServeConn in a goroutine because net.Pipe is synchronous:
	// the error frame write blocks until the client reads it.
	errCh := make(chan error, 1)
	go func() { errCh <- s.ServeConn(ctx, server) }()

	// Client should receive an ERROR frame.
	frame, err := cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpError, frame.Header.Opcode)

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.Contains(t, err.Error(), "draining")
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestServerQueryNotImplemented(t *testing.T) {
	s := MakeServer(ServerConfig{Insecure: true})
	server, client := net.Pipe()
	defer client.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- s.ServeConn(ctx, server) }()

	// Complete handshake.
	sendStartup(t, client, map[string]string{
		"CQL_VERSION": "3.4.5",
	})
	frame, err := cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpReady, frame.Header.Opcode)

	// Send a QUERY frame for a non-system table (requires executor).
	var qbody bytes.Buffer
	_ = cqlwire.WriteLongString(&qbody, "SELECT * FROM my_keyspace.users")
	_ = cqlwire.WriteShort(&qbody, uint16(cqlwire.ConsistencyOne))
	_ = cqlwire.WriteBytes(&qbody, nil) // query flags
	require.NoError(t, cqlwire.WriteFrame(
		client, cqlwire.FrameHeader{
			Version:  cqlwire.ProtoV4Request,
			StreamID: 1,
			Opcode:   cqlwire.OpQuery,
		}, qbody.Bytes(),
	))

	// Expect ERROR response (not implemented without executor).
	frame, err = cqlwire.ReadFrame(client)
	require.NoError(t, err)
	require.Equal(t, cqlwire.OpError, frame.Header.Opcode)
	require.Equal(t, int16(1), frame.Header.StreamID)

	client.Close()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestParseAuthResponse(t *testing.T) {
	// Encode a valid SASL PLAIN token as CQL [bytes].
	var body bytes.Buffer
	token := []byte("\x00cassandra\x00secret")
	require.NoError(t, cqlwire.WriteBytes(&body, token))

	user, pass, err := parseAuthResponse(body.Bytes())
	require.NoError(t, err)
	require.Equal(t, "cassandra", user)
	require.Equal(t, "secret", pass)

	// Null token should error.
	body.Reset()
	require.NoError(t, cqlwire.WriteBytes(&body, nil))
	_, _, err = parseAuthResponse(body.Bytes())
	require.Error(t, err)
	require.Contains(t, err.Error(), "null auth token")
}
