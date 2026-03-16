// Package mysql implements MySQL protocol compatibility for XxSql.
package mysql

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ServerConfig holds MySQL server configuration.
type ServerConfig struct {
	Bind           string
	Port           int
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	AcceptBacklog  int
}

// DefaultServerConfig returns default MySQL server configuration.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Bind:           "0.0.0.0",
		Port:           3306,
		MaxConnections: 100,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		AcceptBacklog:  128,
	}
}

// Server is a MySQL protocol server.
type Server struct {
	config    *ServerConfig
	listener  net.Listener
	running   int32 // atomic
	connCount int32 // atomic
	conns     map[*MySQLHandler]struct{}
	connsMu   sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// Connection ID counter
	connIDCounter uint32

	// Callbacks
	onAuth  func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error)
	onQuery func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error)
	onClose func(h *MySQLHandler)
}

// NewServer creates a new MySQL server.
func NewServer(config *ServerConfig) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config: config,
		conns:  make(map[*MySQLHandler]struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
}

// SetAuthHandler sets the authentication handler.
func (s *Server) SetAuthHandler(fn func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error)) {
	s.onAuth = fn
}

// SetQueryHandler sets the query handler.
func (s *Server) SetQueryHandler(fn func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error)) {
	s.onQuery = fn
}

// SetCloseHandler sets the close handler.
func (s *Server) SetCloseHandler(fn func(h *MySQLHandler)) {
	s.onClose = fn
}

// Start starts the MySQL server.
func (s *Server) Start() error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	addr := fmt.Sprintf("%s:%d", s.config.Bind, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the MySQL server.
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}

	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}

	s.closeAllConnections()
	s.wg.Wait()

	return nil
}

// closeAllConnections closes all active connections.
func (s *Server) closeAllConnections() {
	s.connsMu.Lock()
	conns := make([]*MySQLHandler, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.conns = make(map[*MySQLHandler]struct{})
	s.connsMu.Unlock()

	// Close connections outside the lock to prevent deadlock
	for _, conn := range conns {
		conn.Close()
	}
}

// acceptLoop accepts incoming connections.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		tcpConn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			continue
		}

		if int(atomic.LoadInt32(&s.connCount)) >= s.config.MaxConnections {
			tcpConn.Close()
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(tcpConn)
	}
}

// handleConnection handles a single MySQL connection.
func (s *Server) handleConnection(tcpConn net.Conn) {
	defer s.wg.Done()

	connID := atomic.AddUint32(&s.connIDCounter, 1)

	handler := NewMySQLHandler(tcpConn, connID,
		WithMySQLAuthHandler(s.onAuth),
		WithMySQLQueryHandler(s.onQuery),
		WithMySQLCloseHandler(func(h *MySQLHandler) {
			s.removeConnection(h)
		}),
	)

	s.addConnection(handler)

	if s.onClose != nil {
		go func() {
			for !handler.IsClosed() {
				time.Sleep(100 * time.Millisecond)
			}
			s.onClose(handler)
		}()
	}

	handler.Handle()
}

// addConnection adds a connection to tracking.
func (s *Server) addConnection(conn *MySQLHandler) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	s.conns[conn] = struct{}{}
	atomic.AddInt32(&s.connCount, 1)
}

// removeConnection removes a connection from tracking.
func (s *Server) removeConnection(conn *MySQLHandler) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	delete(s.conns, conn)
	atomic.AddInt32(&s.connCount, -1)
}

// ConnectionCount returns the current connection count.
func (s *Server) ConnectionCount() int {
	return int(atomic.LoadInt32(&s.connCount))
}

// IsRunning returns whether the server is running.
func (s *Server) IsRunning() bool {
	return atomic.LoadInt32(&s.running) == 1
}

// Addr returns the server address.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Connections returns a slice of active connections.
func (s *Server) Connections() []*MySQLHandler {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()

	conns := make([]*MySQLHandler, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	return conns
}

// Stats returns server statistics.
func (s *Server) Stats() ServerStats {
	return ServerStats{
		Running:         s.IsRunning(),
		ConnectionCount: s.ConnectionCount(),
		Addr:            s.Addr().String(),
	}
}

// ServerStats holds MySQL server statistics.
type ServerStats struct {
	Running         bool   `json:"running"`
	ConnectionCount int    `json:"connection_count"`
	Addr            string `json:"addr"`
}
