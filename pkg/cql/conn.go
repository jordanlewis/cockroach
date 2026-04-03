// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"io"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// preparedStmt holds a cached prepared statement. The CQL query is
// stored along with the number of bind markers so that EXECUTE can
// validate value counts. The prepared ID is an MD5 hash of the query
// string, matching the convention used by Cassandra drivers.
type preparedStmt struct {
	query     string
	bindCount int
}

// conn represents a single CQL client connection. It holds the
// network connection, buffered I/O, and the frame channel that
// bridges the reader and processor goroutines.
type conn struct {
	// netConn is the underlying network connection.
	netConn net.Conn
	// cancelConn cancels the connection's context, triggering
	// shutdown of all per-connection goroutines.
	cancelConn context.CancelFunc

	// rd buffers reads from the network connection. Only the reader
	// goroutine accesses this field.
	rd bufio.Reader
	// wr buffers writes to the network connection. Only the
	// processor goroutine accesses this field.
	wr *bufio.Writer

	// fb is used to build response frame bodies. Only the processor
	// goroutine accesses this field.
	fb cqlwire.FrameBuilder

	// frameCh carries request frames from the reader goroutine to
	// the processor goroutine. It is closed when the reader exits.
	frameCh chan cqlwire.Frame

	// authenticatedUser is the SQL username established during the
	// CQL handshake. In insecure mode this defaults to root. Only
	// accessed by the processor goroutine.
	authenticatedUser username.SQLUsername

	// keyspace is the current CQL keyspace (database) set via USE.
	// Only accessed by the processor goroutine.
	keyspace string

	// preparedStmts caches prepared statements for this connection,
	// keyed by the prepared ID (MD5 hash of the query string). Only
	// accessed by the processor goroutine.
	preparedStmts map[string]preparedStmt
}

func newConn(netConn net.Conn, cancel context.CancelFunc) *conn {
	return &conn{
		netConn:       netConn,
		cancelConn:    cancel,
		rd:            *bufio.NewReader(netConn),
		wr:            bufio.NewWriter(netConn),
		frameCh:       make(chan cqlwire.Frame, frameChanSize),
		preparedStmts: make(map[string]preparedStmt),
	}
}

// readFrames is the reader goroutine. It reads CQL frames from the
// network and pushes them to frameCh. It exits on read error, context
// cancellation, or client disconnect, and closes frameCh to signal
// the processor.
func (c *conn) readFrames(ctx context.Context, s *Server) {
	defer close(c.frameCh)
	for {
		frame, err := cqlwire.ReadFrame(&c.rd)
		if err != nil {
			if errors.Is(err, io.EOF) ||
				errors.Is(err, io.ErrUnexpectedEOF) {
				log.VEventf(ctx, 2, "CQL client disconnected")
			} else if ctx.Err() != nil {
				log.VEventf(
					ctx, 2, "CQL connection context cancelled",
				)
			} else {
				log.Ops.Infof(
					ctx, "error reading CQL frame: %v", err,
				)
			}
			return
		}

		s.metrics.BytesIn.Inc(
			int64(cqlwire.HeaderSize + len(frame.Body)),
		)

		if !frame.Header.Version.IsRequest() {
			log.Ops.Infof(
				ctx,
				"received non-request CQL frame (version 0x%02x)",
				byte(frame.Header.Version),
			)
			return
		}

		select {
		case c.frameCh <- frame:
		case <-ctx.Done():
			return
		}
	}
}

// processFrames is the processor goroutine. It handles the CQL
// connection lifecycle: authentication handshake followed by command
// processing.
func (c *conn) processFrames(ctx context.Context, s *Server) {
	// Phase 1: handshake. Read STARTUP (possibly preceded by
	// OPTIONS) and complete authentication.
	if err := c.handleAuthentication(ctx, s); err != nil {
		log.Ops.Infof(ctx, "CQL authentication failed: %v", err)
		return
	}

	// Phase 2: command processing.
	for {
		select {
		case frame, ok := <-c.frameCh:
			if !ok {
				return
			}
			if err := c.handleFrame(ctx, s, frame); err != nil {
				log.Ops.Infof(
					ctx, "error handling CQL frame: %v", err,
				)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// handleFrame dispatches a single CQL request frame received during
// the ready state.
func (c *conn) handleFrame(ctx context.Context, s *Server, frame cqlwire.Frame) error {
	streamID := frame.Header.StreamID
	switch frame.Header.Opcode {
	case cqlwire.OpOptions:
		return c.sendSupported(streamID)
	case cqlwire.OpQuery:
		return c.handleQuery(ctx, s, frame)
	case cqlwire.OpPrepare:
		return c.handlePrepare(ctx, s, frame)
	case cqlwire.OpExecute:
		return c.handleExecute(ctx, s, frame)
	case cqlwire.OpBatch:
		return c.sendError(
			streamID, errCodeServerError,
			"CQL batch not yet implemented",
		)
	case cqlwire.OpRegister:
		// Accept the REGISTER request and respond with READY.
		// cqlsh sends REGISTER after STARTUP to subscribe to
		// schema/topology change events. We acknowledge the
		// registration but never send events.
		return c.sendReady(streamID)
	default:
		return c.sendError(
			streamID, errCodeProtocol,
			"unexpected opcode in ready state",
		)
	}
}

// handleQuery processes a CQL QUERY frame. System table queries
// (system.local, system.peers, system_schema.*) are answered with
// synthetic results without requiring the SQL executor. All other
// queries are parsed, translated to SQL, and executed via the
// internal executor.
func (c *conn) handleQuery(ctx context.Context, s *Server, frame cqlwire.Frame) error {
	streamID := frame.Header.StreamID

	// Parse the QUERY frame body: [long string] query, [short]
	// consistency, [byte] flags, ...
	r := bytes.NewReader(frame.Body)
	query, err := cqlwire.ReadLongString(r)
	if err != nil {
		return c.sendError(
			streamID, errCodeProtocol,
			"invalid QUERY frame: "+err.Error(),
		)
	}

	log.VEventf(ctx, 2, "CQL QUERY: %s", query)

	// When there is no executor, try to handle system table queries
	// (system.local, system.peers, system_schema.*) with synthetic
	// results. These don't require the SQL executor. When an
	// executor is available, let it handle system tables so it can
	// include real CRDB database metadata.
	if s.executor == nil {
		if stmt, parseErr := parser.Parse(query); parseErr == nil {
			if sel, ok := stmt.(*parser.SelectStatement); ok {
				if result, handled := handleSystemSelect(
					ctx, nil, sel.Keyspace, sel.Table, sel.Where, sel.Columns,
					nil,
				); handled {
					if result.NewKeyspace != "" {
						c.keyspace = result.NewKeyspace
					}
					return c.sendResult(streamID, result)
				}
			}
		}
		return c.sendError(
			streamID, errCodeServerError,
			"CQL query processing not yet implemented",
		)
	}

	result := s.executor.ExecuteQuery(ctx, query, c.keyspace, c.authenticatedUser)

	// Update keyspace if a USE statement was executed.
	if result.NewKeyspace != "" {
		c.keyspace = result.NewKeyspace
	}

	return c.sendResult(streamID, result)
}

// handlePrepare processes a CQL PREPARE frame. It parses the query to
// validate syntax and count bind markers, generates a prepared ID
// (MD5 hash of the query), caches the statement, and returns a
// RESULT Prepared response with bind variable metadata typed as
// varchar. This allows clients (e.g. gocql) to encode bound values
// as UTF-8 strings.
func (c *conn) handlePrepare(ctx context.Context, s *Server, frame cqlwire.Frame) error {
	streamID := frame.Header.StreamID

	r := bytes.NewReader(frame.Body)
	query, err := cqlwire.ReadLongString(r)
	if err != nil {
		return c.sendError(
			streamID, errCodeProtocol,
			"invalid PREPARE frame: "+err.Error(),
		)
	}

	log.VEventf(ctx, 2, "CQL PREPARE: %s", query)

	// Validate that the query parses. We don't need the AST for
	// caching — just syntax validation.
	if _, err := parser.Parse(query); err != nil {
		return c.sendError(streamID, errCodeSyntax, err.Error())
	}

	bindCount := countBindMarkers(query)

	// Generate prepared ID as MD5 hash of the query string.
	h := md5.Sum([]byte(query))
	preparedID := h[:]

	c.preparedStmts[string(preparedID)] = preparedStmt{
		query:     query,
		bindCount: bindCount,
	}

	body := buildPreparedBody(preparedID, bindCount)
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpResult, body,
	)
}

// handleExecute processes a CQL EXECUTE frame. It looks up the
// cached prepared statement by ID, decodes bound values (as varchar
// strings), substitutes them into the original CQL query, and
// executes the resulting concrete query through the normal query
// path.
func (c *conn) handleExecute(ctx context.Context, s *Server, frame cqlwire.Frame) error {
	streamID := frame.Header.StreamID

	r := bytes.NewReader(frame.Body)

	// Read prepared statement ID: [short bytes].
	preparedID, err := cqlwire.ReadShortBytes(r)
	if err != nil {
		return c.sendError(
			streamID, errCodeProtocol,
			"invalid EXECUTE frame: "+err.Error(),
		)
	}

	stmt, ok := c.preparedStmts[string(preparedID)]
	if !ok {
		return c.sendUnprepared(streamID, preparedID)
	}

	// Read query parameters: [short] consistency, [byte] flags.
	if _, err := cqlwire.ReadConsistency(r); err != nil {
		return c.sendError(
			streamID, errCodeProtocol,
			"invalid EXECUTE frame: "+err.Error(),
		)
	}

	flagsBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, flagsBuf); err != nil {
		return c.sendError(
			streamID, errCodeProtocol,
			"invalid EXECUTE frame: "+err.Error(),
		)
	}
	flags := flagsBuf[0]

	// Decode bound values if present (flag bit 0x01 = VALUES).
	var values [][]byte
	if flags&0x01 != 0 {
		valCount, err := cqlwire.ReadShort(r)
		if err != nil {
			return c.sendError(
				streamID, errCodeProtocol,
				"invalid EXECUTE frame: "+err.Error(),
			)
		}
		values = make([][]byte, valCount)
		for i := 0; i < int(valCount); i++ {
			val, err := cqlwire.ReadBytes(r)
			if err != nil {
				return c.sendError(
					streamID, errCodeProtocol,
					"invalid EXECUTE frame value: "+err.Error(),
				)
			}
			values[i] = val
		}
	}

	// Substitute bind values into the query.
	query := substituteBindValues(stmt.query, values)

	log.VEventf(ctx, 2, "CQL EXECUTE: %s", query)

	// Execute through the same path as QUERY.
	if s.executor == nil {
		if parsedStmt, parseErr := parser.Parse(query); parseErr == nil {
			if sel, selOK := parsedStmt.(*parser.SelectStatement); selOK {
				if result, handled := handleSystemSelect(
					ctx, nil, sel.Keyspace, sel.Table, sel.Where, sel.Columns,
					nil,
				); handled {
					if result.NewKeyspace != "" {
						c.keyspace = result.NewKeyspace
					}
					return c.sendResult(streamID, result)
				}
			}
		}
		return c.sendError(
			streamID, errCodeServerError,
			"CQL query processing not yet implemented",
		)
	}

	result := s.executor.ExecuteQuery(ctx, query, c.keyspace, c.authenticatedUser)
	if result.NewKeyspace != "" {
		c.keyspace = result.NewKeyspace
	}
	return c.sendResult(streamID, result)
}

// countBindMarkers counts the number of `?` bind markers in a CQL
// query, skipping `?` characters inside string literals.
func countBindMarkers(query string) int {
	count := 0
	inString := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				i++ // skip escaped quote
				continue
			}
			inString = !inString
		} else if ch == '?' && !inString {
			count++
		}
	}
	return count
}

// substituteBindValues replaces `?` bind markers in a CQL query with
// the provided values encoded as CQL string literals. Null values
// (nil byte slices) are substituted as NULL. The function correctly
// skips `?` characters inside string literals.
func substituteBindValues(query string, values [][]byte) string {
	if len(values) == 0 {
		return query
	}

	var sb strings.Builder
	sb.Grow(len(query))
	valueIdx := 0
	inString := false

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				sb.WriteByte('\'')
				sb.WriteByte('\'')
				i++
				continue
			}
			inString = !inString
			sb.WriteByte(ch)
		} else if ch == '?' && !inString && valueIdx < len(values) {
			if values[valueIdx] == nil {
				sb.WriteString("NULL")
			} else {
				sb.WriteByte('\'')
				s := string(values[valueIdx])
				sb.WriteString(strings.ReplaceAll(s, "'", "''"))
				sb.WriteByte('\'')
			}
			valueIdx++
		} else {
			sb.WriteByte(ch)
		}
	}

	return sb.String()
}

// sendUnprepared writes a CQL ERROR response with the Unprepared
// error code (0x2500). Per the CQL spec, the error body includes the
// unknown prepared statement ID so the client can re-prepare.
func (c *conn) sendUnprepared(streamID int16, preparedID []byte) error {
	c.fb.Reset()
	body := c.fb.Body()
	_ = cqlwire.WriteInt(body, errCodeUnprepared)
	_ = cqlwire.WriteString(body, "prepared statement not found")
	_ = cqlwire.WriteShortBytes(body, preparedID)
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpError, body.Bytes(),
	)
}

// sendResult writes a CQL RESULT or ERROR response frame based on
// the ExecuteResult.
func (c *conn) sendResult(streamID int16, result ExecuteResult) error {
	opcode := cqlwire.OpResult
	if result.IsError {
		opcode = cqlwire.OpError
	}
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		opcode, result.Body,
	)
}

// writeFrame writes a complete CQL response frame and flushes the
// write buffer.
func (c *conn) writeFrame(
	version cqlwire.ProtocolVersion,
	flags cqlwire.HeaderFlag,
	streamID int16,
	opcode cqlwire.Opcode,
	body []byte,
) error {
	err := cqlwire.WriteFrame(c.wr, cqlwire.FrameHeader{
		Version:  version,
		Flags:    flags,
		StreamID: streamID,
		Opcode:   opcode,
	}, body)
	if err != nil {
		return errors.Wrap(err, "writing CQL frame")
	}
	return errors.Wrap(c.wr.Flush(), "flushing CQL frame")
}

// sendError writes a CQL ERROR response frame.
func (c *conn) sendError(streamID int16, code int32, msg string) error {
	c.fb.Reset()
	body := c.fb.Body()
	_ = cqlwire.WriteInt(body, code)
	_ = cqlwire.WriteString(body, msg)
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpError, body.Bytes(),
	)
}

// sendReady writes a CQL READY response frame, indicating the
// connection is authenticated and ready for queries.
func (c *conn) sendReady(streamID int16) error {
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpReady, nil,
	)
}

// sendSupported writes a CQL SUPPORTED response frame with the
// server's supported protocol options.
func (c *conn) sendSupported(streamID int16) error {
	c.fb.Reset()
	body := c.fb.Body()
	supported := map[string][]string{
		"CQL_VERSION": {"3.4.5"},
		"COMPRESSION": {},
	}
	_ = cqlwire.WriteStringMultiMap(body, supported)
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpSupported, body.Bytes(),
	)
}

// sendAuthenticate writes a CQL AUTHENTICATE response frame,
// requesting the client to authenticate with the named authenticator.
func (c *conn) sendAuthenticate(streamID int16, authenticator string) error {
	c.fb.Reset()
	body := c.fb.Body()
	_ = cqlwire.WriteString(body, authenticator)
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpAuthenticate, body.Bytes(),
	)
}

// sendAuthSuccess writes a CQL AUTH_SUCCESS response frame with a
// null token.
func (c *conn) sendAuthSuccess(streamID int16) error {
	c.fb.Reset()
	body := c.fb.Body()
	// Null token: [bytes] with length -1.
	_ = cqlwire.WriteInt(body, -1)
	return c.writeFrame(
		cqlwire.ProtoV4Response, 0, streamID,
		cqlwire.OpAuthSuccess, body.Bytes(),
	)
}

// nextFrame reads the next frame from the frame channel, respecting
// context cancellation.
func (c *conn) nextFrame(ctx context.Context) (cqlwire.Frame, bool) {
	select {
	case frame, ok := <-c.frameCh:
		return frame, ok
	case <-ctx.Done():
		return cqlwire.Frame{}, false
	}
}
