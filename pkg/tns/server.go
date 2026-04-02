// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tns implements an Oracle TNS (Transparent Network Substrate)
// protocol server for CockroachDB. It accepts TCP connections from Oracle
// clients (go-ora, sqlplus, JDBC thin), performs the TNS/O5LOGON handshake,
// translates Oracle SQL to CockroachDB SQL, and executes queries via the
// internal SQL executor.
//
// The server follows the same connection management pattern as the pgwire
// and CQL servers: a central Server struct manages a TCP listener, tracks
// active connections, and supports graceful draining.
package tns

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ServerConfig holds configuration for the TNS server.
type ServerConfig struct {
	// ListenAddr is the address to listen on (e.g. ":1521").
	ListenAddr string
	// Insecure disables password verification when true.
	Insecure bool
	// DefaultDatabase is the initial database for new connections.
	DefaultDatabase string
	// DB is the CockroachDB internal SQL executor interface. When set,
	// Oracle SQL is parsed, translated to CRDB SQL, and executed via the
	// internal executor.
	DB isql.DB
	// CRDBVersion is the CockroachDB version string reported to clients
	// in catalog queries (V$VERSION, etc.).
	CRDBVersion string
}

// Server is a TNS protocol server that accepts connections and dispatches
// Oracle SQL queries to CockroachDB's internal SQL executor.
type Server struct {
	cfg      ServerConfig
	listener net.Listener

	mu       sync.Mutex
	conns    map[*conn]struct{}
	draining bool

	// Metrics.
	connCount    atomic.Int64
	newConnCount atomic.Int64
	bytesIn      atomic.Int64
	bytesOut     atomic.Int64

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewServer creates a new TNS server with the given configuration.
func NewServer(cfg ServerConfig) *Server {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":1521"
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = "defaultdb"
	}
	if cfg.CRDBVersion == "" {
		cfg.CRDBVersion = "v24.1.0"
	}
	return &Server{
		cfg:   cfg,
		conns: make(map[*conn]struct{}),
	}
}

// Start begins listening for TNS connections. It returns once the listener
// is established; connections are handled in background goroutines.
func (s *Server) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	var err error
	s.listener, err = net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		cancel()
		return fmt.Errorf("tns: listen on %s: %w", s.cfg.ListenAddr, err)
	}

	s.wg.Add(1)
	go s.acceptLoop(ctx)
	return nil
}

// Addr returns the listener's address, or nil if not started.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Stop shuts down the server, closing the listener and all active
// connections. It blocks until all connection goroutines have exited.
func (s *Server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.closeAllConns()
	s.wg.Wait()
}

// Drain initiates a graceful drain. New connections are rejected and
// existing connections are closed.
func (s *Server) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
	for c := range s.conns {
		c.close()
	}
}

// closeAllConns closes all active connections under the lock.
func (s *Server) closeAllConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.close()
	}
}

// Metrics returns a snapshot of server metrics.
func (s *Server) Metrics() ServerMetrics {
	return ServerMetrics{
		ActiveConns: s.connCount.Load(),
		NewConns:    s.newConnCount.Load(),
		BytesIn:     s.bytesIn.Load(),
		BytesOut:    s.bytesOut.Load(),
	}
}

// ServerMetrics holds a snapshot of server-level metrics.
type ServerMetrics struct {
	ActiveConns int64
	NewConns    int64
	BytesIn     int64
	BytesOut    int64
}

// acceptLoop runs in a goroutine and accepts new TCP connections.
func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		netConn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Ops.Errorf(ctx, "tns: accept error: %v", err)
				return
			}
		}

		c, ok := s.registerConn(netConn)
		if !ok {
			_ = netConn.Close()
			continue
		}

		s.connCount.Add(1)
		s.newConnCount.Add(1)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.unregisterConn(c)
			c.serve(ctx)
		}()
	}
}

// registerConn adds a new connection to the tracked set under the
// lock. It returns false if the server is draining.
func (s *Server) registerConn(netConn net.Conn) (*conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return nil, false
	}
	c := newConn(s, netConn)
	s.conns[c] = struct{}{}
	return c, true
}

// unregisterConn removes a connection from the tracked set and
// decrements the active connection count.
func (s *Server) unregisterConn(c *conn) {
	s.connCount.Add(-1)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}
