// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cql implements the CQL native protocol v4 server for
// CockroachDB. It is modeled after the pgwire package
// (pkg/sql/pgwire/) and provides:
//
//   - TCP connection acceptance with per-connection goroutines
//   - CQL v4 frame-based protocol handling
//   - STARTUP/AUTHENTICATE/READY handshake
//   - Graceful connection draining
//   - Integration hooks for CRDB server startup
//
// Each connection uses three goroutines:
//
//  1. Reader: reads CQL frames from the network and pushes them to a
//     per-connection channel.
//  2. Processor: consumes frames from the channel, handles
//     authentication, and processes CQL commands.
//  3. Drain watcher: monitors context cancellation and closes the
//     read side of the connection to unblock the reader.
package cql

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cql/cqlwire"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// CQL error codes used in ERROR response frames (CQL native protocol
// v4 spec, section 9).
const (
	errCodeServerError    int32 = 0x0000
	errCodeProtocol       int32 = 0x000A
	errCodeBadCredentials int32 = 0x0100
	errCodeOverloaded     int32 = 0x1001
)

// frameChanSize is the buffer depth of the per-connection frame
// channel. This allows limited request pipelining without excessive
// memory overhead.
const frameChanSize = 16

var (
	metaConns = metric.Metadata{
		Name:        "cql.conns",
		Help:        "Number of active CQL connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaNewConns = metric.Metadata{
		Name:        "cql.new_conns",
		Help:        "Total number of CQL connections accepted",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaBytesIn = metric.Metadata{
		Name:        "cql.bytes_in",
		Help:        "Total bytes received via CQL",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBytesOut = metric.Metadata{
		Name:        "cql.bytes_out",
		Help:        "Total bytes sent via CQL",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
)

// Metrics tracks operational statistics for the CQL server.
type Metrics struct {
	Conns    *metric.Gauge
	NewConns *metric.Counter
	BytesIn  *metric.Counter
	BytesOut *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		Conns:    metric.NewGauge(metaConns),
		NewConns: metric.NewCounter(metaNewConns),
		BytesIn:  metric.NewCounter(metaBytesIn),
		BytesOut: metric.NewCounter(metaBytesOut),
	}
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// ServerConfig configures the CQL protocol server.
type ServerConfig struct {
	// AmbientCtx is used for logging and tracing.
	AmbientCtx log.AmbientContext
	// Insecure disables authentication when true. When false,
	// Authenticator must be set.
	Insecure bool
	// Authenticator validates client credentials during the CQL
	// handshake. Ignored when Insecure is true.
	Authenticator Authenticator
}

// Server implements the server side of the CQL native protocol v4. It
// manages active connections, handles graceful draining, and provides
// metrics for monitoring.
//
// The concurrency model mirrors pgwire.Server: each accepted
// connection is served by a reader goroutine (which reads CQL frames
// and pushes them to a channel), a processor goroutine (which handles
// authentication and dispatches commands), and a drain-watcher
// goroutine (which closes the read side on context cancellation).
//
// Graceful draining follows the same two-phase approach as pgwire:
// new connections are rejected immediately, and existing connections
// are cancelled after a configurable wait period.
type Server struct {
	cfg     ServerConfig
	metrics Metrics

	mu struct {
		syncutil.Mutex
		// connCancelMap tracks active connections. Each entry maps a
		// done channel to the connection's cancel function. The done
		// channel is closed when the connection exits.
		connCancelMap map[chan struct{}]context.CancelFunc
		// draining is set when the server begins draining CQL
		// connections.
		draining bool
		// drainCh is closed when draining transitions to true.
		drainCh chan struct{}
		// rejectNewConnections prevents new connections from being
		// accepted during drain.
		rejectNewConnections bool
	}
}

// MakeServer creates a new CQL protocol server.
func MakeServer(cfg ServerConfig) *Server {
	s := &Server{
		cfg:     cfg,
		metrics: makeMetrics(),
	}
	s.mu.connCancelMap = make(map[chan struct{}]context.CancelFunc)
	s.mu.drainCh = make(chan struct{})
	return s
}

// Metrics returns the server's metrics for registration with the
// metric registry.
func (s *Server) Metrics() Metrics {
	return s.metrics
}

// Serve accepts CQL connections from ln and serves each in a new
// goroutine. It blocks until ln is closed or the stopper begins
// quiescing.
func (s *Server) Serve(ctx context.Context, stopper *stop.Stopper, ln net.Listener) error {
	for {
		netConn, err := ln.Accept()
		if err != nil {
			select {
			case <-stopper.ShouldQuiesce():
				return nil
			default:
			}
			return err
		}
		if err := stopper.RunAsyncTask(
			ctx, "cql-conn", func(ctx context.Context) {
				if err := s.ServeConn(ctx, netConn); err != nil {
					log.Ops.Errorf(ctx, "serving CQL conn: %v", err)
				}
			},
		); err != nil {
			_ = netConn.Close()
		}
	}
}

// ServeConn serves a single CQL client connection. It performs the
// CQL handshake, handles authentication, and processes request frames
// until the connection is closed or the server is drained.
func (s *Server) ServeConn(ctx context.Context, netConn net.Conn) error {
	defer netConn.Close()

	ctx, reject, onClose := s.registerConn(ctx)
	defer onClose()

	if reject {
		// Send an error frame before closing so the client gets a
		// meaningful error instead of a connection reset.
		c := newConn(netConn, nil)
		_ = c.sendError(0, errCodeOverloaded, "server is shutting down")
		return errors.New("rejecting CQL connection: server is draining")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := newConn(netConn, cancel)
	s.serveImpl(ctx, c)
	return nil
}

// serveImpl runs the three-goroutine connection handler. It spawns
// the drain watcher, spawns the processor, and runs the reader in the
// calling goroutine. It returns after all goroutines have exited.
func (s *Server) serveImpl(ctx context.Context, c *conn) {
	// Drain watcher: when the context is cancelled (either from
	// drain or normal shutdown), close the read side of the
	// connection to unblock the reader goroutine.
	go func() {
		<-ctx.Done()
		if tc, ok := c.netConn.(*net.TCPConn); ok {
			_ = tc.CloseRead()
		} else {
			_ = c.netConn.SetReadDeadline(time.Now())
		}
	}()

	// Processor goroutine: handles authentication and command
	// dispatch.
	var procWg sync.WaitGroup
	procWg.Add(1)
	go func() {
		defer procWg.Done()
		c.processFrames(ctx, s)
	}()

	// Reader runs in this goroutine. When it exits (network error,
	// context cancel, or client disconnect), it closes frameCh to
	// signal the processor.
	c.readFrames(ctx, s)

	// Wait for the processor to finish before returning.
	procWg.Wait()
}

// registerConn adds a connection to the active connection map and
// returns a cleanup function. If the server is rejecting new
// connections (during drain), the second return value is true.
func (s *Server) registerConn(
	ctx context.Context,
) (_ context.Context, reject bool, onClose func()) {
	ctx, cancel, done, ok := s.tryAddConn(ctx)
	if !ok {
		return ctx, true, func() {}
	}

	s.metrics.Conns.Inc(1)
	s.metrics.NewConns.Inc(1)

	return ctx, false, func() {
		cancel()
		close(done)
		s.removeConn(done)
		s.metrics.Conns.Dec(1)
	}
}

// tryAddConn attempts to register a new connection under the mutex.
// Returns false if the server is rejecting new connections.
func (s *Server) tryAddConn(
	ctx context.Context,
) (context.Context, context.CancelFunc, chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.rejectNewConnections {
		return ctx, nil, nil, false
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.mu.connCancelMap[done] = cancel
	return ctx, cancel, done, true
}

// removeConn removes a connection from the active connection map.
func (s *Server) removeConn(done chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.connCancelMap, done)
}

// startDrain atomically transitions the server to draining and
// snapshots the active connections.
func (s *Server) startDrain() (
	cancels []context.CancelFunc,
	dones []chan struct{},
	alreadyDraining bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.draining {
		return nil, nil, true
	}
	s.mu.draining = true
	s.mu.rejectNewConnections = true
	close(s.mu.drainCh)

	cancels = make([]context.CancelFunc, 0, len(s.mu.connCancelMap))
	dones = make([]chan struct{}, 0, len(s.mu.connCancelMap))
	for done, cancel := range s.mu.connCancelMap {
		cancels = append(cancels, cancel)
		dones = append(dones, done)
	}
	return cancels, dones, false
}

// Drain begins gracefully draining CQL connections. New connections
// are rejected immediately. After queryWait elapses, remaining
// connections are cancelled. The method blocks until all connections
// have closed or ctx is cancelled.
func (s *Server) Drain(ctx context.Context, queryWait time.Duration) error {
	cancels, dones, alreadyDraining := s.startDrain()
	if alreadyDraining {
		return nil
	}

	if len(cancels) == 0 {
		return nil
	}

	// Phase 1: wait for in-flight requests to complete.
	timer := time.NewTimer(queryWait)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Phase 2: cancel remaining connections.
	for _, cancel := range cancels {
		cancel()
	}

	// Wait for all connections to exit.
	for _, done := range dones {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Undrain reverses a previous Drain, allowing new connections.
func (s *Server) Undrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.draining = false
	s.mu.rejectNewConnections = false
	s.mu.drainCh = make(chan struct{})
}

// IsDraining reports whether the server is currently draining.
func (s *Server) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.draining
}

// DrainCh returns a channel that is closed when the server enters the
// draining state.
func (s *Server) DrainCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.drainCh
}

// Match returns true if rd starts with bytes that look like a CQL v4
// native protocol request frame. This is intended for use with a
// connection multiplexer (e.g. cmux) to distinguish CQL traffic from
// other protocols on a shared listener.
func Match(rd io.Reader) bool {
	var buf [cqlwire.HeaderSize]byte
	if _, err := io.ReadFull(rd, buf[:]); err != nil {
		return false
	}
	version := cqlwire.ProtocolVersion(buf[0])
	opcode := cqlwire.Opcode(buf[4])
	return version == cqlwire.ProtoV4Request &&
		(opcode == cqlwire.OpStartup || opcode == cqlwire.OpOptions)
}
