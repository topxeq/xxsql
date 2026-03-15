// Package server provides the XxSql server implementation.
package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/mysql"
	"github.com/topxeq/xxsql/internal/protocol"
)

// Server represents the XxSql server.
type Server struct {
	config     *config.Config
	logger     *log.Logger
	auth       *auth.Manager
	private    *protocol.Server
	mysql      *MySQLServer
	http       *HTTPServer
	running    int32
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	connIDGen  uint32
	startTime  time.Time
	stats      ServerStats
}

// ServerStats holds server statistics.
type ServerStats struct {
	TotalConnections   uint64
	ActiveConnections  uint64
	TotalQueries       uint64
	QueriesPerSecond   uint64
	LastQueryTime      time.Time
}

// New creates a new XxSql server.
func New(cfg *config.Config, logger *log.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		config: cfg,
		logger: logger,
		auth:   auth.NewManager(auth.WithSessionTTL(time.Duration(cfg.Auth.SessionTimeouSec) * time.Second)),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the server.
func (s *Server) Start() error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	s.startTime = time.Now()

	// Create default admin user if auth is enabled
	if s.config.Auth.Enabled {
		if s.config.Auth.AdminPassword != "" {
			_, err := s.auth.CreateUser(s.config.Auth.AdminUser, s.config.Auth.AdminPassword, auth.RoleAdmin)
			if err != nil {
				s.logger.Warn("Failed to create admin user: %v", err)
			} else {
				s.logger.Info("Created admin user: %s", s.config.Auth.AdminUser)
			}
		}
	}

	// Start private protocol server
	if err := s.startPrivateServer(); err != nil {
		return fmt.Errorf("failed to start private server: %w", err)
	}

	// Start MySQL protocol server
	if s.config.Network.MySQLPort > 0 {
		if err := s.startMySQLServer(); err != nil {
			s.logger.Error("Failed to start MySQL server: %w", err)
		}
	}

	// Start HTTP API server
	if s.config.Network.HTTPPort > 0 {
		if err := s.startHTTPServer(); err != nil {
			s.logger.Error("Failed to start HTTP server: %w", err)
		}
	}

	s.logger.Info("Server started successfully")
	return nil
}

// Stop stops the server.
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}

	s.logger.Info("Stopping server...")
	s.cancel()

	// Stop servers
	if s.private != nil {
		s.private.Stop()
	}
	if s.mysql != nil {
		s.mysql.Stop()
	}
	if s.http != nil {
		s.http.Stop()
	}

	s.wg.Wait()
	s.logger.Info("Server stopped")
	return nil
}

// startPrivateServer starts the private protocol server.
func (s *Server) startPrivateServer() error {
	cfg := protocol.DefaultServerConfig()
	cfg.Bind = s.config.Network.Bind
	cfg.Port = s.config.Network.PrivatePort
	cfg.MaxConnections = s.config.Connection.MaxConnections
	cfg.ReadTimeout = time.Duration(s.config.Connection.WaitTimeout) * time.Second
	cfg.WriteTimeout = time.Duration(s.config.Connection.WaitTimeout) * time.Second

	s.private = protocol.NewServer(cfg)

	// Set handlers
	s.private.SetConnectHandler(s.onConnect)
	s.private.SetDisconnectHandler(s.onDisconnect)
	s.private.SetHandshakeHandler(s.onHandshake)
	s.private.SetAuthHandler(s.onAuth)
	s.private.SetQueryHandler(s.onQuery)

	if err := s.private.Start(); err != nil {
		return err
	}

	s.logger.Info("Private protocol server listening on %s", s.private.Addr())
	return nil
}

// startMySQLServer starts the MySQL protocol server.
func (s *Server) startMySQLServer() error {
	s.mysql = NewMySQLServer(s, s.config.Network.Bind, s.config.Network.MySQLPort)
	return s.mysql.Start()
}

// startHTTPServer starts the HTTP API server.
func (s *Server) startHTTPServer() error {
	s.http = NewHTTPServer(s, s.config.Network.Bind, s.config.Network.HTTPPort)
	return s.http.Start()
}

// onConnect handles a new connection.
func (s *Server) onConnect(conn *protocol.ConnectionHandler) {
	atomic.AddUint64(&s.stats.TotalConnections, 1)
	atomic.AddUint64(&s.stats.ActiveConnections, 1)
	s.logger.Debug("New connection from %s (total: %d)", conn.RemoteAddr(), atomic.LoadUint64(&s.stats.ActiveConnections))
}

// onDisconnect handles a disconnected connection.
func (s *Server) onDisconnect(conn *protocol.ConnectionHandler) {
	atomic.AddUint64(&s.stats.ActiveConnections, ^uint64(0))
	s.logger.Debug("Connection closed from %s (active: %d)", conn.RemoteAddr(), atomic.LoadUint64(&s.stats.ActiveConnections))
}

// onHandshake handles a handshake request.
func (s *Server) onHandshake(conn *protocol.ConnectionHandler, req *protocol.HandshakeRequest) (*protocol.HandshakeResponse, error) {
	// Determine protocol version
	protoVersion := req.ProtocolVersion
	if protoVersion > protocol.ProtocolV2 {
		protoVersion = protocol.ProtocolV2 // Support up to v2
	}

	return &protocol.HandshakeResponse{
		ProtocolVersion: protoVersion,
		ServerVersion:   "0.0.1",
		Supported:       true,
		AuthChallenge:   make([]byte, 20),
	}, nil
}

// onAuth handles an authentication request.
func (s *Server) onAuth(conn *protocol.ConnectionHandler, req *protocol.AuthRequest) (*protocol.AuthResponse, error) {
	// If auth is disabled, allow all connections
	if !s.config.Auth.Enabled {
		return &protocol.AuthResponse{
			Status:    protocol.StatusOK,
			Message:   "OK",
			SessionID: "no-auth",
		}, nil
	}

	// Authenticate
	session, err := s.auth.Authenticate(req.Username, string(req.Password))
	if err != nil {
		return &protocol.AuthResponse{
			Status:  protocol.StatusAuth,
			Message: "Authentication failed",
		}, nil
	}

	// Set database if specified
	if req.Database != "" {
		s.auth.SetUserDatabase(session.ID, req.Database)
	}

	return &protocol.AuthResponse{
		Status:     protocol.StatusOK,
		Message:    "OK",
		SessionID:  session.ID,
		Permission: uint32(auth.RolePermissions[session.Role]),
	}, nil
}

// onQuery handles a query request.
func (s *Server) onQuery(conn *protocol.ConnectionHandler, req *protocol.QueryRequest) (*protocol.QueryResponse, error) {
	s.logger.Debug("Query: %s", req.SQL)

	atomic.AddUint64(&s.stats.TotalQueries, 1)
	s.stats.LastQueryTime = time.Now()

	// TODO: Actual query execution (Phase 3+)
	// For now, return a placeholder response
	return &protocol.QueryResponse{
		Status:  protocol.StatusOK,
		Message: "Query received (not implemented)",
	}, nil
}

// nextConnectionID generates the next connection ID.
func (s *Server) nextConnectionID() uint32 {
	return atomic.AddUint32(&s.connIDGen, 1)
}

// GetStats returns server statistics.
func (s *Server) GetStats() ServerStats {
	return ServerStats{
		TotalConnections:  atomic.LoadUint64(&s.stats.TotalConnections),
		ActiveConnections: atomic.LoadUint64(&s.stats.ActiveConnections),
		TotalQueries:      atomic.LoadUint64(&s.stats.TotalQueries),
		LastQueryTime:     s.stats.LastQueryTime,
	}
}

// Uptime returns the server uptime.
func (s *Server) Uptime() time.Duration {
	return time.Since(s.startTime)
}

// IsRunning returns whether the server is running.
func (s *Server) IsRunning() bool {
	return atomic.LoadInt32(&s.running) == 1
}

// Auth returns the auth manager.
func (s *Server) Auth() *auth.Manager {
	return s.auth
}

// Logger returns the logger.
func (s *Server) Logger() *log.Logger {
	return s.logger
}

// MySQLServer wraps the MySQL protocol server.
type MySQLServer struct {
	server    *Server
	bind      string
	port      int
	listener  net.Listener
	running   int32
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewMySQLServer creates a new MySQL server.
func NewMySQLServer(server *Server, bind string, port int) *MySQLServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &MySQLServer{
		server: server,
		bind:   bind,
		port:   port,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the MySQL server.
func (s *MySQLServer) Start() error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	addr := fmt.Sprintf("%s:%d", s.bind, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener

	s.wg.Add(1)
	go s.acceptLoop()

	s.server.logger.Info("MySQL protocol server listening on %s", addr)
	return nil
}

// Stop stops the MySQL server.
func (s *MySQLServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}

	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// acceptLoop accepts incoming connections.
func (s *MySQLServer) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a MySQL connection.
func (s *MySQLServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	connID := s.server.nextConnectionID()
	s.server.logger.Debug("New MySQL connection #%d from %s", connID, conn.RemoteAddr())

	atomic.AddUint64(&s.server.stats.TotalConnections, 1)
	atomic.AddUint64(&s.server.stats.ActiveConnections, 1)
	defer func() {
		atomic.AddUint64(&s.server.stats.ActiveConnections, ^uint64(0))
	}()

	handler := mysql.NewMySQLHandler(conn, connID,
		mysql.WithMySQLAuthHandler(s.handleMySQLAuth),
		mysql.WithMySQLQueryHandler(s.handleMySQLQuery),
		mysql.WithMySQLCloseHandler(func(h *mysql.MySQLHandler) {
			s.server.logger.Debug("MySQL connection #%d closed", connID)
		}),
	)

	handler.Handle()
}

// handleMySQLAuth handles MySQL authentication.
func (s *MySQLServer) handleMySQLAuth(h *mysql.MySQLHandler, username, database string, authResponse []byte) (bool, error) {
	if !s.server.config.Auth.Enabled {
		return true, nil
	}

	// Get user
	user, err := s.server.auth.GetUser(username)
	if err != nil {
		return false, nil
	}

	// For MySQL native password, verify using stored hash
	// In production, you'd verify the auth response properly
	// For now, accept if user exists
	_ = user
	return true, nil
}

// handleMySQLQuery handles a MySQL query.
func (s *MySQLServer) handleMySQLQuery(h *mysql.MySQLHandler, sql string) ([]*mysql.ColumnDefinition, [][]interface{}, error) {
	s.server.logger.Debug("MySQL Query: %s", sql)
	atomic.AddUint64(&s.server.stats.TotalQueries, 1)

	// TODO: Actual query execution (Phase 3+)
	// For now, return empty result set
	return nil, nil, nil
}

// HTTPServer wraps the HTTP API server.
type HTTPServer struct {
	server   *Server
	bind     string
	port     int
	running  int32
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewHTTPServer creates a new HTTP server.
func NewHTTPServer(server *Server, bind string, port int) *HTTPServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &HTTPServer{
		server: server,
		bind:   bind,
		port:   port,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the HTTP server.
func (s *HTTPServer) Start() error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	// HTTP server implementation will be added in Phase 11 (Web Management)
	s.server.logger.Info("HTTP API server listening on %s:%d (skeleton)", s.bind, s.port)
	return nil
}

// Stop stops the HTTP server.
func (s *HTTPServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}

	s.cancel()
	s.wg.Wait()
	return nil
}

// CreatePIDFile creates a PID file.
func CreatePIDFile(path string) error {
	if path == "" {
		return nil
	}

	pid := os.Getpid()
	return os.WriteFile(path, []byte(fmt.Sprintf("%d\n", pid)), 0644)
}

// RemovePIDFile removes the PID file.
func RemovePIDFile(path string) {
	if path != "" {
		os.Remove(path)
	}
}