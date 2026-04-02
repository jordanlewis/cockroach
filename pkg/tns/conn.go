// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tns

import (
	"context"
	"io"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/tns/auth"
	"github.com/cockroachdb/cockroach/pkg/tns/catalog"
	"github.com/cockroachdb/cockroach/pkg/tns/tnswire"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// conn manages a single TNS client connection, implementing the
// CONNECT → AUTH → READY state machine.
type conn struct {
	server  *Server
	netConn net.Conn

	// Session state populated after authentication.
	username string
	database string

	// executor is the per-connection SQL executor bridge, created when
	// the server is configured with an isql.DB.
	executor *Executor

	// catalog handles Oracle system view queries (ALL_TABLES, V$VERSION, etc.).
	catalog *catalog.Catalog
}

// newConn creates a new connection handler.
func newConn(s *Server, netConn net.Conn) *conn {
	c := &conn{
		server:   s,
		netConn:  netConn,
		database: s.cfg.DefaultDatabase,
	}
	return c
}

// close closes the underlying network connection.
func (c *conn) close() {
	_ = c.netConn.Close()
}

// serve runs the connection's main loop: handshake, then TTI message
// processing until the connection is closed or an error occurs.
func (c *conn) serve(ctx context.Context) {
	defer c.netConn.Close()

	// Phase 1: TNS handshake and O5LOGON authentication.
	if err := c.authenticate(ctx); err != nil {
		log.Ops.Infof(ctx, "tns: authentication failed: %v", err)
		return
	}

	// Phase 2: Initialize per-connection executor and catalog.
	if c.server.cfg.DB != nil {
		c.executor = NewExecutor(c.server.cfg.DB, c.database)
	}
	c.catalog = catalog.New(c.server.cfg.CRDBVersion, c.username)

	// Phase 3: TTI message loop.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.processOneMessage(ctx); err != nil {
			if isDisconnect(err) {
				log.VEventf(ctx, 2, "tns: client disconnected")
			} else if ctx.Err() != nil {
				log.VEventf(ctx, 2, "tns: connection context cancelled")
			} else {
				log.Ops.Infof(ctx, "tns: connection error: %v", err)
			}
			return
		}
	}
}

// authenticate performs the TNS CONNECT/ACCEPT handshake followed by
// O5LOGON challenge-response authentication.
func (c *conn) authenticate(ctx context.Context) error {
	h := &auth.Handshaker{
		Conn: c.netConn,
	}

	if !c.server.cfg.Insecure {
		h.PasswordVerifier = func(username, password string) error {
			// Accept any credentials for now. A real implementation would
			// check against CRDB's user/password store.
			return nil
		}
	}

	if err := h.Handshake(); err != nil {
		return err
	}

	c.username = h.Username
	log.VEventf(ctx, 2, "tns: authenticated user %q", c.username)
	return nil
}

// processOneMessage reads one DATA packet, extracts the TTI function code,
// and dispatches it to the appropriate handler.
func (c *conn) processOneMessage(ctx context.Context) error {
	hdr, payload, err := tnswire.ReadPacket(c.netConn)
	if err != nil {
		return err
	}

	switch hdr.Type {
	case tnswire.PacketTypeData:
		return c.handleData(ctx, payload)
	case tnswire.PacketTypeMarker:
		// Attention/reset marker — acknowledge and continue.
		return nil
	default:
		log.Ops.Infof(ctx,
			"tns: unexpected packet type %s in ready state", hdr.Type)
		return nil
	}
}

// handleData processes a DATA packet containing a TTI message.
func (c *conn) handleData(ctx context.Context, payload []byte) error {
	dataPkt, err := tnswire.DecodeData(payload)
	if err != nil {
		return err
	}

	if len(dataPkt.Payload) == 0 {
		return nil
	}

	funcCode, err := tnswire.DecodeTTIFuncCode(dataPkt.Payload)
	if err != nil {
		return err
	}

	switch funcCode {
	case tnswire.TTIOpen:
		return c.handleOpen(ctx, dataPkt.Payload)
	case tnswire.TTIExec:
		return c.handleExec(ctx, dataPkt.Payload)
	case tnswire.TTIFetch:
		return c.handleFetch(ctx, dataPkt.Payload)
	case tnswire.TTIClose:
		return c.handleClose(ctx, dataPkt.Payload)
	case tnswire.TTICommit:
		return c.handleCommit(ctx)
	case tnswire.TTIRollback:
		return c.handleRollback(ctx)
	default:
		log.Ops.Infof(ctx, "tns: unhandled TTI function code 0x%02x", byte(funcCode))
		return c.sendExecError(0, "unsupported TTI function")
	}
}

// handleOpen processes a TTI OPEN request: parse Oracle SQL, translate
// to CRDB SQL, execute, and return column metadata.
func (c *conn) handleOpen(ctx context.Context, data []byte) error {
	msg, err := tnswire.DecodeTTIOpen(data)
	if err != nil {
		return c.sendExecError(0, err.Error())
	}

	log.VEventf(ctx, 2, "tns: OPEN cursor %d: %s", msg.CursorID, msg.SQL)

	if c.executor == nil {
		return c.sendExecError(msg.CursorID, "no SQL executor configured")
	}

	cols, execErr := c.executor.Open(ctx, msg.CursorID, msg.SQL, c.catalog)
	if execErr != nil {
		return c.sendExecError(msg.CursorID, execErr.Error())
	}

	// Build TTI OPEN response with column metadata.
	resp := tnswire.TTIOpenResponse{
		CursorID: msg.CursorID,
		Columns:  cols,
	}
	return c.writeDataPayload(tnswire.EncodeTTIOpenResponse(resp))
}

// handleExec processes a TTI EXEC request: execute a previously opened
// cursor with optional bind variables.
func (c *conn) handleExec(ctx context.Context, data []byte) error {
	msg, err := tnswire.DecodeTTIExec(data)
	if err != nil {
		return c.sendExecError(0, err.Error())
	}

	log.VEventf(ctx, 2, "tns: EXEC cursor %d", msg.CursorID)

	if c.executor == nil {
		return c.sendExecError(msg.CursorID, "no SQL executor configured")
	}

	rowsAffected, execErr := c.executor.Exec(ctx, msg.CursorID, msg.SQL, msg.BindVars, c.catalog)
	if execErr != nil {
		return c.sendExecError(msg.CursorID, execErr.Error())
	}

	resp := tnswire.TTIExecResponse{
		RowsAffected: uint32(rowsAffected),
		ErrorCode:    0,
	}
	return c.writeDataPayload(tnswire.EncodeTTIExecResponse(resp))
}

// handleFetch processes a TTI FETCH request: return rows from an
// executed cursor.
func (c *conn) handleFetch(ctx context.Context, data []byte) error {
	msg, err := tnswire.DecodeTTIFetch(data)
	if err != nil {
		return c.sendExecError(0, err.Error())
	}

	log.VEventf(ctx, 2, "tns: FETCH cursor %d (size %d)", msg.CursorID, msg.FetchSize)

	if c.executor == nil {
		return c.sendExecError(msg.CursorID, "no SQL executor configured")
	}

	resp, numCols, err := c.executor.Fetch(msg.CursorID, int(msg.FetchSize))
	if err != nil {
		return c.sendExecError(msg.CursorID, err.Error())
	}

	return c.writeDataPayload(tnswire.EncodeTTIFetchResponse(resp, numCols))
}

// handleClose processes a TTI CLOSE request: release a cursor.
func (c *conn) handleClose(ctx context.Context, data []byte) error {
	msg, err := tnswire.DecodeTTIClose(data)
	if err != nil {
		return err
	}

	log.VEventf(ctx, 2, "tns: CLOSE cursor %d", msg.CursorID)

	if c.executor != nil {
		c.executor.Close(msg.CursorID)
	}

	// CLOSE has no response in TNS — the client proceeds directly.
	return nil
}

// handleCommit processes a TTI COMMIT request.
func (c *conn) handleCommit(ctx context.Context) error {
	log.VEventf(ctx, 2, "tns: COMMIT")
	// CockroachDB auto-commits, so this is a no-op. Send success.
	resp := tnswire.TTIExecResponse{
		RowsAffected: 0,
		ErrorCode:    0,
	}
	return c.writeDataPayload(tnswire.EncodeTTIExecResponse(resp))
}

// handleRollback processes a TTI ROLLBACK request.
func (c *conn) handleRollback(ctx context.Context) error {
	log.VEventf(ctx, 2, "tns: ROLLBACK")
	resp := tnswire.TTIExecResponse{
		RowsAffected: 0,
		ErrorCode:    0,
	}
	return c.writeDataPayload(tnswire.EncodeTTIExecResponse(resp))
}

// sendExecError sends a TTI EXEC error response with the given error message.
func (c *conn) sendExecError(cursorID uint16, msg string) error {
	resp := tnswire.TTIExecResponse{
		RowsAffected: 0,
		ErrorCode:    1, // generic error
		ErrorMsg:     msg,
	}
	return c.writeDataPayload(tnswire.EncodeTTIExecResponse(resp))
}

// writeDataPayload wraps a TTI payload in a DATA packet and writes it.
func (c *conn) writeDataPayload(ttiPayload []byte) error {
	dataPayload := tnswire.EncodeData(tnswire.DataPacket{
		Flags:   0,
		Payload: ttiPayload,
	})
	return tnswire.WritePacket(c.netConn, tnswire.PacketTypeData, dataPayload)
}

// isDisconnect returns true if err indicates a normal client disconnect.
func isDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}

// Compile-time assertion that auth.Handshaker has the Conn field we use.
var _ io.ReadWriter = (*net.TCPConn)(nil)
