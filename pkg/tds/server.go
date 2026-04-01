// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tds implements a TDS (Tabular Data Stream) protocol server,
// providing Sybase/SQL Server wire-protocol compatibility for CockroachDB.
// It follows the pgwire pattern of reader/processor/writer goroutines
// per connection, with a central Server struct that manages the listener
// and tracks connection metrics.
package tds

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// ServerConfig holds configuration for the TDS server.
type ServerConfig struct {
	// ListenAddr is the address to listen on (e.g. ":1433").
	ListenAddr string
	// Username is the expected login username. Empty allows any.
	Username string
	// Password is the expected login password. Empty allows any.
	Password string
	// DefaultDatabase is the initial database for new connections.
	DefaultDatabase string
	// QueryHandler is called for each SQL batch. If nil, a default
	// handler returning an empty result set is used.
	QueryHandler QueryHandler
}

// QueryHandler processes a SQL query and returns column metadata and rows.
type QueryHandler func(ctx context.Context, query string, database string) ([]ResultColumn, [][]interface{}, error)

// ResultColumn describes a column in a query result.
type ResultColumn struct {
	Name   string
	TypeID byte
	MaxLen uint16 // for variable-length types
}

// Server is a TDS protocol server that accepts connections and dispatches
// queries to CockroachDB's internal SQL executor.
type Server struct {
	cfg      ServerConfig
	listener net.Listener

	mu       sync.Mutex
	conns    map[*conn]struct{}
	draining bool

	// Metrics
	connCount    atomic.Int64
	newConnCount atomic.Int64
	bytesIn      atomic.Int64
	bytesOut     atomic.Int64

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewServer creates a new TDS server with the given configuration.
func NewServer(cfg ServerConfig) *Server {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":1433"
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = "master"
	}
	return &Server{
		cfg:   cfg,
		conns: make(map[*conn]struct{}),
	}
}

// Start begins listening for TDS connections. It returns once the listener
// is established; connections are handled in background goroutines.
func (s *Server) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	var err error
	s.listener, err = net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		cancel()
		return fmt.Errorf("tds: listen on %s: %w", s.cfg.ListenAddr, err)
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
		s.listener.Close()
	}
	s.mu.Lock()
	for c := range s.conns {
		c.close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// Drain initiates a graceful drain. New connections are rejected and
// existing connections are closed after completing their current request.
func (s *Server) Drain() {
	s.mu.Lock()
	s.draining = true
	for c := range s.conns {
		c.close()
	}
	s.mu.Unlock()
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
				log.Printf("tds: accept error: %v", err)
				return
			}
		}

		s.mu.Lock()
		if s.draining {
			s.mu.Unlock()
			netConn.Close()
			continue
		}
		c := newConn(s, netConn)
		s.conns[c] = struct{}{}
		s.mu.Unlock()

		s.connCount.Add(1)
		s.newConnCount.Add(1)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.connCount.Add(-1)
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
			}()
			c.serve(ctx)
		}()
	}
}
