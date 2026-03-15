package protocol

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ServerConfig holds server configuration.
type ServerConfig struct {
	Bind            string
	Port            int
	MaxConnections  int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxMessageSize  uint32
	AcceptBacklog   int
}

// DefaultServerConfig returns the default server configuration.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Bind:           "0.0.0.0",
		Port:           9527,
		MaxConnections: 200,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxMessageSize: MaxMessageSize,
		AcceptBacklog:  128,
	}
}

// Server is a TCP server for the private protocol.
type Server struct {
	config     *ServerConfig
	listener   net.Listener
	running    int32 // atomic
	connCount  int32 // atomic
	conns      map[*ConnectionHandler]struct{}
	connsMu    sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	// Handlers
	onHandshake func(conn *ConnectionHandler, req *HandshakeRequest) (*HandshakeResponse, error)
	onAuth      func(conn *ConnectionHandler, req *AuthRequest) (*AuthResponse, error)
	onQuery     func(conn *ConnectionHandler, req *QueryRequest) (*QueryResponse, error)
	onConnect   func(conn *ConnectionHandler)
	onDisconnect func(conn *ConnectionHandler)
}

// NewServer creates a new TCP server.
func NewServer(config *ServerConfig) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config: config,
		conns:  make(map[*ConnectionHandler]struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
}

// SetHandshakeHandler sets the handshake handler.
func (s *Server) SetHandshakeHandler(fn func(conn *ConnectionHandler, req *HandshakeRequest) (*HandshakeResponse, error)) {
	s.onHandshake = fn
}

// SetAuthHandler sets the auth handler.
func (s *Server) SetAuthHandler(fn func(conn *ConnectionHandler, req *AuthRequest) (*AuthResponse, error)) {
	s.onAuth = fn
}

// SetQueryHandler sets the query handler.
func (s *Server) SetQueryHandler(fn func(conn *ConnectionHandler, req *QueryRequest) (*QueryResponse, error)) {
	s.onQuery = fn
}

// SetConnectHandler sets the connect handler.
func (s *Server) SetConnectHandler(fn func(conn *ConnectionHandler)) {
	s.onConnect = fn
}

// SetDisconnectHandler sets the disconnect handler.
func (s *Server) SetDisconnectHandler(fn func(conn *ConnectionHandler)) {
	s.onDisconnect = fn
}

// Start starts the server.
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

// Stop stops the server.
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}

	// Cancel context to stop accept loop
	s.cancel()

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.closeAllConnections()

	// Wait for all goroutines to finish
	s.wg.Wait()

	return nil
}

// closeAllConnections closes all active connections.
func (s *Server) closeAllConnections() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	for conn := range s.conns {
		conn.Close()
	}
	s.conns = make(map[*ConnectionHandler]struct{})
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
				return // Server is stopping
			}
			continue
		}

		// Check max connections
		if int(atomic.LoadInt32(&s.connCount)) >= s.config.MaxConnections {
			tcpConn.Close()
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(tcpConn)
	}
}

// handleConnection handles a single connection.
func (s *Server) handleConnection(tcpConn net.Conn) {
	defer s.wg.Done()

	// Create connection handler
	conn := NewConnectionHandler(tcpConn,
		WithReadTimeout(s.config.ReadTimeout),
		WithWriteTimeout(s.config.WriteTimeout),
		WithMaxMessageSize(s.config.MaxMessageSize),
		WithHandshakeHandler(s.onHandshake),
		WithAuthHandler(s.onAuth),
		WithQueryHandler(s.onQuery),
		WithCloseHandler(func(c *ConnectionHandler) {
			s.removeConnection(c)
		}),
	)

	// Track connection
	s.addConnection(conn)

	// Notify connect handler
	if s.onConnect != nil {
		s.onConnect(conn)
	}

	// Handle connection
	conn.Handle(s.ctx)
}

// addConnection adds a connection to the tracking map.
func (s *Server) addConnection(conn *ConnectionHandler) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	s.conns[conn] = struct{}{}
	atomic.AddInt32(&s.connCount, 1)

	// Notify disconnect handler when connection closes
	go func() {
		for !conn.IsClosed() {
			time.Sleep(100 * time.Millisecond)
		}
		if s.onDisconnect != nil {
			s.onDisconnect(conn)
		}
	}()
}

// removeConnection removes a connection from the tracking map.
func (s *Server) removeConnection(conn *ConnectionHandler) {
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
func (s *Server) Connections() []*ConnectionHandler {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()

	conns := make([]*ConnectionHandler, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	return conns
}

// Broadcast sends a message to all active connections.
func (s *Server) Broadcast(msgType byte, payload []byte) error {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()

	var lastErr error
	for conn := range s.conns {
		if err := conn.sendMessage(msgType, payload); err != nil {
			lastErr = err
		}
	}
	return lastErr
}