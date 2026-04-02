// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cql

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

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

	// keyspace is the current CQL keyspace (database) set via USE.
	// Only accessed by the processor goroutine.
	keyspace string
}

func newConn(netConn net.Conn, cancel context.CancelFunc) *conn {
	return &conn{
		netConn:    netConn,
		cancelConn: cancel,
		rd:         *bufio.NewReader(netConn),
		wr:         bufio.NewWriter(netConn),
		frameCh:    make(chan cqlwire.Frame, frameChanSize),
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
		return c.sendError(
			streamID, errCodeServerError,
			"CQL prepared statements not yet implemented",
		)
	case cqlwire.OpExecute:
		return c.sendError(
			streamID, errCodeServerError,
			"CQL prepared statements not yet implemented",
		)
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

	if s.executor == nil {
		return c.sendError(
			streamID, errCodeServerError,
			"CQL query processing not yet implemented",
		)
	}

	result := s.executor.ExecuteQuery(ctx, query, c.keyspace)

	// Update keyspace if a USE statement was executed.
	if result.NewKeyspace != "" {
		c.keyspace = result.NewKeyspace
	}

	return c.sendResult(streamID, result)
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
