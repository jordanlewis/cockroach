// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/errors"
)

// passwordAuthenticatorClass is the Cassandra authenticator class name
// sent in AUTHENTICATE response frames. CQL drivers use this to
// determine which SASL mechanism to use for AUTH_RESPONSE.
const passwordAuthenticatorClass = "org.apache.cassandra.auth.PasswordAuthenticator"

// Authenticator validates CQL client credentials during the
// connection handshake.
type Authenticator interface {
	// Authenticate checks whether the given username and password
	// are valid. Returns nil on success.
	Authenticate(ctx context.Context, username, password string) error
}

// AllowAllAuthenticator accepts any credentials. It is intended for
// testing and development configurations.
type AllowAllAuthenticator struct{}

// Authenticate always returns nil.
func (AllowAllAuthenticator) Authenticate(_ context.Context, _, _ string) error {
	return nil
}

// handleAuthentication performs the CQL handshake and authentication.
// The protocol flow is:
//
//  1. Client sends OPTIONS (optional) — server responds with
//     SUPPORTED.
//  2. Client sends STARTUP — server responds with READY (insecure)
//     or AUTHENTICATE.
//  3. If AUTHENTICATE: client sends AUTH_RESPONSE with credentials,
//     server validates and responds with AUTH_SUCCESS or ERROR.
func (c *conn) handleAuthentication(ctx context.Context, s *Server) error {
	// Read the first frame. Must be OPTIONS or STARTUP.
	frame, ok := c.nextFrame(ctx)
	if !ok {
		return errors.New("connection closed before handshake")
	}

	// Handle optional OPTIONS request.
	if frame.Header.Opcode == cqlwire.OpOptions {
		if err := c.sendSupported(frame.Header.StreamID); err != nil {
			return errors.Wrap(err, "sending SUPPORTED")
		}
		frame, ok = c.nextFrame(ctx)
		if !ok {
			return errors.New("connection closed after OPTIONS")
		}
	}

	if frame.Header.Opcode != cqlwire.OpStartup {
		return errors.Newf(
			"expected STARTUP frame, got %s",
			frame.Header.Opcode,
		)
	}

	// Parse and validate STARTUP options.
	opts, err := parseStartupOptions(frame.Body)
	if err != nil {
		_ = c.sendError(
			frame.Header.StreamID, errCodeProtocol,
			"invalid STARTUP frame: "+err.Error(),
		)
		return errors.Wrap(err, "parsing STARTUP")
	}
	if _, hasVersion := opts["CQL_VERSION"]; !hasVersion {
		_ = c.sendError(
			frame.Header.StreamID, errCodeProtocol,
			"STARTUP missing required CQL_VERSION option",
		)
		return errors.New("STARTUP missing CQL_VERSION")
	}

	streamID := frame.Header.StreamID

	// Insecure mode: skip authentication entirely.
	if s.cfg.Insecure {
		return c.sendReady(streamID)
	}

	if s.cfg.Authenticator == nil {
		_ = c.sendError(
			streamID, errCodeServerError,
			"server authentication not configured",
		)
		return errors.New("authenticator not configured")
	}

	// Request credentials from the client.
	if err := c.sendAuthenticate(
		streamID, passwordAuthenticatorClass,
	); err != nil {
		return errors.Wrap(err, "sending AUTHENTICATE")
	}

	// Read AUTH_RESPONSE.
	frame, ok = c.nextFrame(ctx)
	if !ok {
		return errors.New("connection closed during authentication")
	}
	if frame.Header.Opcode != cqlwire.OpAuthResponse {
		return errors.Newf(
			"expected AUTH_RESPONSE, got %s",
			frame.Header.Opcode,
		)
	}

	// Parse SASL PLAIN credentials.
	username, password, err := parseAuthResponse(frame.Body)
	if err != nil {
		_ = c.sendError(
			streamID, errCodeBadCredentials,
			"invalid auth response: "+err.Error(),
		)
		return errors.Wrap(err, "parsing AUTH_RESPONSE")
	}

	// Validate credentials.
	if err := s.cfg.Authenticator.Authenticate(
		ctx, username, password,
	); err != nil {
		_ = c.sendError(
			streamID, errCodeBadCredentials,
			"authentication failed",
		)
		return errors.Wrap(err, "authentication failed")
	}

	return c.sendAuthSuccess(streamID)
}

// parseStartupOptions decodes the STARTUP frame body, which is a CQL
// [string map] of connection options (e.g. CQL_VERSION, COMPRESSION).
func parseStartupOptions(body []byte) (map[string]string, error) {
	return cqlwire.ReadStringMap(bytes.NewReader(body))
}

// parseAuthResponse decodes the AUTH_RESPONSE frame body for
// PasswordAuthenticator. The body contains a [bytes] token in SASL
// PLAIN format (RFC 4616): \0<username>\0<password>.
func parseAuthResponse(body []byte) (username, password string, err error) {
	token, err := cqlwire.ReadBytes(bytes.NewReader(body))
	if err != nil {
		return "", "", errors.Wrap(err, "reading auth token")
	}
	if token == nil {
		return "", "", errors.New("null auth token")
	}

	// SASL PLAIN: [authzid] NUL authcid NUL passwd
	// authzid is empty for CQL password auth.
	parts := bytes.SplitN(token, []byte{0}, 3)
	if len(parts) != 3 {
		return "", "", errors.Newf(
			"invalid SASL PLAIN token: expected 3 parts, got %d",
			len(parts),
		)
	}
	return string(parts[1]), string(parts[2]), nil
}
