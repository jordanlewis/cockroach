// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tds

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"unicode/utf16"

	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
)

// connState represents the state of a TDS connection.
type connState int

const (
	statePreLogin connState = iota
	stateLogin
	stateReady
)

// conn manages a single TDS client connection, implementing the
// PRELOGIN → LOGIN → READY state machine.
type conn struct {
	server   *Server
	netConn  net.Conn
	reader   *tdswire.PacketReader
	writer   *tdswire.PacketWriter
	state    connState
	database string
	username string
	// executor is the per-connection SQL executor bridge, created when
	// the server is configured with an isql.DB. It tracks per-connection
	// state like the current database and @@ROWCOUNT.
	executor *Executor
}

// newConn creates a new connection handler. If the server is configured
// with an isql.DB, a per-connection Executor is created.
func newConn(s *Server, netConn net.Conn) *conn {
	c := &conn{
		server:   s,
		netConn:  netConn,
		reader:   tdswire.NewPacketReader(netConn),
		writer:   tdswire.NewPacketWriter(netConn, tdswire.DefaultPacketSize),
		state:    statePreLogin,
		database: s.cfg.DefaultDatabase,
	}
	if s.cfg.DB != nil {
		c.executor = NewExecutor(s.cfg.DB, s.cfg.DefaultDatabase)
	}
	return c
}

// close closes the underlying network connection.
func (c *conn) close() {
	c.netConn.Close()
}

// serve runs the connection's main loop, processing TDS messages until
// the connection is closed or an error occurs.
func (c *conn) serve(ctx context.Context) {
	defer c.netConn.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pktType, payload, err := c.reader.ReadMessage()
		if err != nil {
			if err != io.EOF && !isConnClosed(err) {
				log.Printf("tds: conn read error: %v", err)
			}
			return
		}

		if err := c.dispatch(ctx, pktType, payload); err != nil {
			log.Printf("tds: conn dispatch error: %v", err)
			return
		}
	}
}

// dispatch handles a received message based on the current connection state
// and the packet type.
func (c *conn) dispatch(ctx context.Context, pktType tdswire.PacketType, payload []byte) error {
	switch pktType {
	case tdswire.PacketTypePreLogin:
		return c.handlePreLogin(payload)
	case tdswire.PacketTypeLogin7:
		return c.handleLogin7(payload)
	case tdswire.PacketTypeSQLBatch:
		return c.handleSQLBatch(ctx, payload)
	case tdswire.PacketTypeAttention:
		return c.handleAttention()
	default:
		return fmt.Errorf("tds: unexpected packet type %s in state %d", pktType, c.state)
	}
}

// handlePreLogin processes a PRELOGIN request and responds with the
// server's PRELOGIN options.
func (c *conn) handlePreLogin(payload []byte) error {
	if c.state != statePreLogin {
		return fmt.Errorf("tds: PRELOGIN received in state %d", c.state)
	}

	_, err := tdswire.DecodePreLogin(payload)
	if err != nil {
		return fmt.Errorf("tds: decoding PRELOGIN: %w", err)
	}

	// Build server PRELOGIN response.
	resp := &tdswire.PreLoginMsg{
		Options: []tdswire.PreLoginOption{
			{
				Token: tdswire.PreLoginVersion,
				Data: tdswire.EncodeVersionData(tdswire.PreLoginVersionData{
					Major: 16, Minor: 0, Build: 0, SubBuild: 0,
				}),
			},
			{
				Token: tdswire.PreLoginEncryption,
				Data:  []byte{byte(tdswire.EncryptNotSup)},
			},
		},
	}

	respPayload := tdswire.EncodePreLogin(resp)
	if err := c.writer.WriteMessage(tdswire.PacketTypeTabularResult, respPayload); err != nil {
		return fmt.Errorf("tds: writing PRELOGIN response: %w", err)
	}

	c.state = stateLogin
	return nil
}

// handleLogin7 processes a LOGIN7 packet, authenticates the user, and
// sends the appropriate response tokens.
func (c *conn) handleLogin7(payload []byte) error {
	if c.state != stateLogin {
		return fmt.Errorf("tds: LOGIN7 received in state %d", c.state)
	}

	login, err := tdswire.DecodeLogin7(payload)
	if err != nil {
		return fmt.Errorf("tds: decoding LOGIN7: %w", err)
	}

	if !authenticate(c.server.cfg, login) {
		return c.sendLoginFailed(login.Username)
	}

	c.username = login.Username
	if login.Database != "" {
		c.database = login.Database
		if c.executor != nil {
			c.executor.SetDatabase(login.Database)
		}
	}

	return c.sendLoginSuccess(login)
}

// sendLoginSuccess sends ENVCHANGE, LOGINACK, and DONE tokens that
// indicate a successful login. The ordering follows the SQL Server
// convention: ENVCHANGE tokens first, then LOGINACK, then DONE.
func (c *conn) sendLoginSuccess(login *tdswire.Login7) error {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	// Negotiate packet size. Use the client's requested size if
	// reasonable, otherwise default.
	packetSize := login.PacketSize
	if packetSize < 512 || packetSize > 32768 {
		packetSize = uint32(tdswire.DefaultPacketSize)
	}

	// ENVCHANGE(database) — must come before LOGINACK.
	if err := tw.WriteEnvChange(tdswire.EnvChangeToken{
		Type:     tdswire.EnvDatabase,
		NewValue: c.database,
		OldValue: "",
	}); err != nil {
		return err
	}

	// ENVCHANGE(packet size).
	if err := tw.WriteEnvChange(tdswire.EnvChangeToken{
		Type:     tdswire.EnvPacketSize,
		NewValue: fmt.Sprintf("%d", packetSize),
		OldValue: fmt.Sprintf("%d", tdswire.DefaultPacketSize),
	}); err != nil {
		return err
	}

	// LOGINACK token.
	if err := tw.WriteLoginAck(tdswire.LoginAckToken{
		Interface:   1, // TSQL
		TDSVersion:  login.TDSVersion,
		ProgName:    "CockroachDB",
		ProgVersion: [4]byte{24, 3, 0, 0},
	}); err != nil {
		return err
	}

	// DONE (login complete, final).
	if err := tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
	}); err != nil {
		return err
	}

	if err := c.writer.WriteMessage(tdswire.PacketTypeTabularResult, buf.Bytes()); err != nil {
		return fmt.Errorf("tds: writing login success: %w", err)
	}

	c.state = stateReady
	return nil
}

// sendLoginFailed sends an ERROR token and DONE token indicating
// authentication failure.
func (c *conn) sendLoginFailed(username string) error {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := tw.WriteError(tdswire.ErrorToken{
		TokenType: tdswire.TokenError,
		Number:    18456,
		State:     1,
		Class:     14,
		Message:   fmt.Sprintf("Login failed for user '%s'.", username),
		Server:    "CockroachDB",
	}); err != nil {
		return err
	}

	if err := tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneError,
	}); err != nil {
		return err
	}

	if err := c.writer.WriteMessage(tdswire.PacketTypeTabularResult, buf.Bytes()); err != nil {
		return fmt.Errorf("tds: writing login failure: %w", err)
	}
	return fmt.Errorf("tds: login failed for user %q", username)
}

// handleSQLBatch processes a SQL_BATCH packet containing UTF-16LE encoded
// SQL text. It extracts the SQL, dispatches it to the query handler or the
// internal SQL executor, and encodes the results as a TDS token stream.
func (c *conn) handleSQLBatch(ctx context.Context, payload []byte) error {
	if c.state != stateReady {
		return fmt.Errorf("tds: SQL_BATCH received in state %d", c.state)
	}

	// The SQL_BATCH payload contains a transaction descriptor header (variable
	// length) followed by UTF-16LE encoded SQL text. For simplicity, we look
	// for the ALL_HEADERS structure: first 4 bytes is total length of all
	// headers, then we skip past them.
	sql, err := extractSQLFromBatch(payload)
	if err != nil {
		return c.sendErrorResult(err)
	}

	// If we have an Executor (isql.DB-backed), use it for T-SQL parsing,
	// translation, and execution through CockroachDB's internal executor.
	if c.executor != nil {
		return c.handleSQLBatchWithExecutor(ctx, sql)
	}

	// Legacy path: use the QueryHandler callback.
	// Handle USE database specially.
	trimmed := strings.TrimSpace(sql)
	if len(trimmed) >= 4 && strings.EqualFold(trimmed[:4], "USE ") {
		return c.handleUseDatabase(strings.TrimSpace(trimmed[4:]))
	}

	// Dispatch to query handler.
	handler := c.server.cfg.QueryHandler
	if handler == nil {
		handler = defaultQueryHandler
	}

	cols, rows, err := handler(ctx, sql, c.database)
	if err != nil {
		return c.sendErrorResult(err)
	}

	return c.sendResultSet(cols, rows)
}

// handleSQLBatchWithExecutor processes a SQL batch through the
// per-connection Executor, which handles T-SQL parsing, translation,
// and dispatch to CockroachDB's internal SQL executor.
func (c *conn) handleSQLBatchWithExecutor(ctx context.Context, sql string) error {
	tokenBytes, err := c.executor.ExecuteBatchToBytes(ctx, sql)
	if err != nil {
		return c.sendErrorResult(err)
	}

	// Keep the connection's database in sync with the executor's.
	c.database = c.executor.Database()

	return c.writer.WriteMessage(tdswire.PacketTypeTabularResult, tokenBytes)
}

// handleUseDatabase processes a USE <database> command by changing the
// connection's current database and sending an ENVCHANGE token.
func (c *conn) handleUseDatabase(database string) error {
	// Strip surrounding brackets or quotes if present.
	database = stripQuotes(database)

	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	oldDB := c.database
	c.database = database

	if err := tw.WriteEnvChange(tdswire.EnvChangeToken{
		Type:     tdswire.EnvDatabase,
		NewValue: database,
		OldValue: oldDB,
	}); err != nil {
		return err
	}

	if err := tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
	}); err != nil {
		return err
	}

	return c.writer.WriteMessage(tdswire.PacketTypeTabularResult, buf.Bytes())
}

// sendResultSet encodes a result set as COLMETADATA + ROW* + DONE tokens.
func (c *conn) sendResultSet(cols []ResultColumn, rows [][]interface{}) error {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	// Build COLMETADATA.
	md := tdswire.ColMetaData{
		Columns: make([]tdswire.Column, len(cols)),
	}
	for i, col := range cols {
		md.Columns[i] = tdswire.Column{
			ColName: col.Name,
			TypeInfo: tdswire.TypeInfo{
				TypeID: col.TypeID,
				MaxLen: col.MaxLen,
			},
		}
	}

	if err := tw.WriteColMetaData(md); err != nil {
		return err
	}

	// Write ROW tokens.
	for _, row := range rows {
		r := tdswire.Row{
			Values: make([][]byte, len(row)),
		}
		for j, val := range row {
			encoded, err := encodeValue(val, cols[j].TypeID)
			if err != nil {
				return err
			}
			r.Values[j] = encoded
		}
		if err := tw.WriteRow(md, r); err != nil {
			return err
		}
	}

	// DONE with row count.
	if err := tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneCount,
		RowCount:  uint64(len(rows)),
	}); err != nil {
		return err
	}

	return c.writer.WriteMessage(tdswire.PacketTypeTabularResult, buf.Bytes())
}

// sendErrorResult sends an ERROR token followed by a DONE token.
func (c *conn) sendErrorResult(queryErr error) error {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := tw.WriteError(tdswire.ErrorToken{
		TokenType: tdswire.TokenError,
		Number:    50000,
		State:     1,
		Class:     16,
		Message:   queryErr.Error(),
		Server:    "CockroachDB",
	}); err != nil {
		return err
	}

	if err := tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal | tdswire.DoneError,
	}); err != nil {
		return err
	}

	return c.writer.WriteMessage(tdswire.PacketTypeTabularResult, buf.Bytes())
}

// handleAttention processes an ATTENTION (cancel) signal by sending a
// DONE(ATTN) response.
func (c *conn) handleAttention() error {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := tw.WriteDone(tdswire.DoneToken{
		TokenType: tdswire.TokenDone,
		Status:    tdswire.DoneFinal,
		CurCmd:    0,
	}); err != nil {
		return err
	}

	return c.writer.WriteMessage(tdswire.PacketTypeTabularResult, buf.Bytes())
}

// extractSQLFromBatch extracts the SQL text from a SQL_BATCH payload.
// The payload starts with an ALL_HEADERS structure (total_length as uint32
// LE, then header entries), followed by the UTF-16LE encoded SQL text.
// If the payload is too short to contain ALL_HEADERS, we treat the entire
// payload as UTF-16LE SQL text.
func extractSQLFromBatch(payload []byte) (string, error) {
	if len(payload) < 4 {
		if len(payload) == 0 {
			return "", nil
		}
		return decodeUTF16LE(payload), nil
	}

	// Try to read ALL_HEADERS total length.
	totalHeaderLen := binary.LittleEndian.Uint32(payload[0:4])

	// Validate: the total header length must be at least 4 (for the length
	// field itself) and not exceed the payload. If it looks invalid, treat
	// the entire payload as raw SQL.
	if totalHeaderLen >= 4 && int(totalHeaderLen) <= len(payload) {
		sqlBytes := payload[totalHeaderLen:]
		return decodeUTF16LE(sqlBytes), nil
	}

	// Fallback: treat entire payload as UTF-16LE SQL text.
	return decodeUTF16LE(payload), nil
}

// decodeUTF16LE decodes a little-endian UTF-16 byte slice into a Go string.
func decodeUTF16LE(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	u16 := make([]uint16, len(b)/2)
	for i := range u16 {
		u16[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}
	return string(utf16.Decode(u16))
}

// encodeValue converts a Go value into a TDS wire-format byte slice
// appropriate for the given type ID.
func encodeValue(val interface{}, typeID byte) ([]byte, error) {
	if val == nil {
		return nil, nil
	}
	switch typeID {
	case tdswire.TypeInt4:
		switch v := val.(type) {
		case int:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(int32(v)))
			return buf, nil
		case int32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(v))
			return buf, nil
		case int64:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(int32(v)))
			return buf, nil
		default:
			return nil, fmt.Errorf("tds: cannot encode %T as INT4", val)
		}
	case tdswire.TypeInt8:
		switch v := val.(type) {
		case int:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(int64(v)))
			return buf, nil
		case int64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			return buf, nil
		default:
			return nil, fmt.Errorf("tds: cannot encode %T as INT8", val)
		}
	case tdswire.TypeBigVarChar, tdswire.TypeNVarChar:
		switch v := val.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		default:
			return nil, fmt.Errorf("tds: cannot encode %T as VARCHAR", val)
		}
	default:
		// Best-effort: try string or []byte.
		switch v := val.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		default:
			return nil, fmt.Errorf("tds: unsupported type 0x%02X for value %T", typeID, val)
		}
	}
}

// stripQuotes removes surrounding brackets or double quotes from a string.
func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '[' && s[len(s)-1] == ']') ||
			(s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// defaultQueryHandler returns an empty result set for any query.
func defaultQueryHandler(_ context.Context, _ string, _ string) ([]ResultColumn, [][]interface{}, error) {
	return nil, nil, nil
}

// isConnClosed reports whether err indicates a closed connection.
func isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	// Check for common closed-connection errors.
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}
