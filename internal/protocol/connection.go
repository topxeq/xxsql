package protocol

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionState represents the state of a connection.
type ConnectionState int32

const (
	StateInit       ConnectionState = iota // Initial state
	StateHandshake                         // Waiting for handshake
	StateAuth                              // Waiting for authentication
	StateReady                             // Authenticated and ready
	StateQuery                             // Executing a query
	StateClosing                           // Connection closing
	StateClosed                            // Connection closed
)

// String returns a string representation of the connection state.
func (s ConnectionState) String() string {
	switch s {
	case StateInit:
		return "INIT"
	case StateHandshake:
		return "HANDSHAKE"
	case StateAuth:
		return "AUTH"
	case StateReady:
		return "READY"
	case StateQuery:
		return "QUERY"
	case StateClosing:
		return "CLOSING"
	case StateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// ConnectionHandler handles a single client connection.
type ConnectionHandler struct {
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	state        int32 // atomic, use getState/setState
	seqID        uint32
	createdAt    time.Time
	lastActiveAt time.Time
	sessionID    string
	username     string
	database     string
	closed       int32 // atomic

	// Callbacks
	onHandshake func(conn *ConnectionHandler, req *HandshakeRequest) (*HandshakeResponse, error)
	onAuth      func(conn *ConnectionHandler, req *AuthRequest) (*AuthResponse, error)
	onQuery     func(conn *ConnectionHandler, req *QueryRequest) (*QueryResponse, error)
	onClose     func(conn *ConnectionHandler)

	// Configuration
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxMsgSize   uint32

	mu sync.Mutex
}

// NewConnectionHandler creates a new connection handler.
func NewConnectionHandler(conn net.Conn, opts ...ConnectionOption) *ConnectionHandler {
	h := &ConnectionHandler{
		conn:         conn,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		state:        int32(StateInit),
		createdAt:    time.Now(),
		lastActiveAt: time.Now(),
		readTimeout:  30 * time.Second,
		writeTimeout: 30 * time.Second,
		maxMsgSize:   MaxMessageSize,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// ConnectionOption is a functional option for ConnectionHandler.
type ConnectionOption func(*ConnectionHandler)

// WithReadTimeout sets the read timeout.
func WithReadTimeout(d time.Duration) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.readTimeout = d
	}
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(d time.Duration) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.writeTimeout = d
	}
}

// WithMaxMessageSize sets the maximum message size.
func WithMaxMessageSize(size uint32) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.maxMsgSize = size
	}
}

// WithHandshakeHandler sets the handshake handler.
func WithHandshakeHandler(fn func(conn *ConnectionHandler, req *HandshakeRequest) (*HandshakeResponse, error)) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.onHandshake = fn
	}
}

// WithAuthHandler sets the auth handler.
func WithAuthHandler(fn func(conn *ConnectionHandler, req *AuthRequest) (*AuthResponse, error)) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.onAuth = fn
	}
}

// WithQueryHandler sets the query handler.
func WithQueryHandler(fn func(conn *ConnectionHandler, req *QueryRequest) (*QueryResponse, error)) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.onQuery = fn
	}
}

// WithCloseHandler sets the close handler.
func WithCloseHandler(fn func(conn *ConnectionHandler)) ConnectionOption {
	return func(h *ConnectionHandler) {
		h.onClose = fn
	}
}

// Handle handles the connection lifecycle.
func (h *ConnectionHandler) Handle(ctx context.Context) error {
	defer h.Close()

	// Transition to handshake state
	h.setState(StateHandshake)

	// Handle messages
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Set read deadline
		if h.readTimeout > 0 {
			h.conn.SetReadDeadline(time.Now().Add(h.readTimeout))
		}

		// Read message
		msg, err := ReadMessage(h.reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Check if connection is idle too long
				if time.Since(h.lastActiveAt) > h.readTimeout*2 {
					return fmt.Errorf("connection idle timeout")
				}
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		h.lastActiveAt = time.Now()

		// Process message
		if err := h.processMessage(ctx, msg); err != nil {
			return fmt.Errorf("process error: %w", err)
		}
	}
}

// processMessage processes a single message.
func (h *ConnectionHandler) processMessage(ctx context.Context, msg *Message) error {
	h.seqID = msg.Header.SeqID

	switch msg.Header.Type {
	case MsgHandshakeRequest:
		return h.handleHandshake(msg)
	case MsgAuthRequest:
		return h.handleAuth(msg)
	case MsgQueryRequest:
		return h.handleQuery(ctx, msg)
	case MsgPing:
		return h.handlePing()
	case MsgClose:
		return fmt.Errorf("client requested close")
	default:
		return h.sendError(0, fmt.Sprintf("unknown message type: %d", msg.Header.Type))
	}
}

// handleHandshake handles a handshake request.
func (h *ConnectionHandler) handleHandshake(msg *Message) error {
	state := h.getState()
	if state != StateHandshake {
		return h.sendError(0, "invalid state for handshake")
	}

	req, err := DecodeHandshakeRequest(msg.Payload)
	if err != nil {
		return h.sendError(0, fmt.Sprintf("invalid handshake request: %v", err))
	}

	var resp *HandshakeResponse
	if h.onHandshake != nil {
		resp, err = h.onHandshake(h, req)
	} else {
		// Default response
		resp = &HandshakeResponse{
			ProtocolVersion: req.ProtocolVersion,
			ServerVersion:   "0.0.1",
			Supported:       true,
			AuthChallenge:   make([]byte, 20),
		}
	}

	if err != nil {
		return h.sendError(0, err.Error())
	}

	h.setState(StateAuth)
	return h.sendMessage(MsgHandshakeResponse, resp.Encode())
}

// handleAuth handles an auth request.
func (h *ConnectionHandler) handleAuth(msg *Message) error {
	state := h.getState()
	if state != StateAuth {
		return h.sendError(0, "invalid state for auth")
	}

	req, err := DecodeAuthRequest(msg.Payload)
	if err != nil {
		return h.sendError(0, fmt.Sprintf("invalid auth request: %v", err))
	}

	var resp *AuthResponse
	if h.onAuth != nil {
		resp, err = h.onAuth(h, req)
	} else {
		// Default: allow all (for testing)
		resp = &AuthResponse{
			Status:    StatusOK,
			Message:   "OK",
			SessionID: "test-session",
		}
	}

	if err != nil {
		return h.sendError(0, err.Error())
	}

	h.username = req.Username
	h.database = req.Database
	h.sessionID = resp.SessionID
	h.setState(StateReady)

	return h.sendMessage(MsgAuthResponse, resp.Encode())
}

// handleQuery handles a query request.
func (h *ConnectionHandler) handleQuery(ctx context.Context, msg *Message) error {
	state := h.getState()
	if state != StateReady {
		return h.sendError(0, "not authenticated")
	}

	req, err := DecodeQueryRequest(msg.Payload)
	if err != nil {
		return h.sendError(0, fmt.Sprintf("invalid query request: %v", err))
	}

	h.setState(StateQuery)
	defer h.setState(StateReady)

	var resp *QueryResponse
	if h.onQuery != nil {
		resp, err = h.onQuery(h, req)
	} else {
		// Default response (no-op)
		resp = &QueryResponse{
			Status:   StatusOK,
			Message:  "Query executed (no handler)",
			RowCount: 0,
		}
	}

	if err != nil {
		return h.sendError(0, err.Error())
	}

	return h.sendMessage(MsgQueryResponse, resp.Encode())
}

// handlePing handles a ping request.
func (h *ConnectionHandler) handlePing() error {
	return h.sendMessage(MsgPong, nil)
}

// sendMessage sends a message to the client.
func (h *ConnectionHandler) sendMessage(msgType byte, payload []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.writeTimeout > 0 {
		h.conn.SetWriteDeadline(time.Now().Add(h.writeTimeout))
	}

	if err := WriteMessage(h.writer, msgType, h.seqID, payload); err != nil {
		return err
	}

	return h.writer.Flush()
}

// sendError sends an error message to the client.
func (h *ConnectionHandler) sendError(code uint32, message string) error {
	err := &ErrorPayload{
		Code:    code,
		Message: message,
	}
	return h.sendMessage(MsgError, err.Encode())
}

// Close closes the connection.
func (h *ConnectionHandler) Close() error {
	if !atomic.CompareAndSwapInt32(&h.closed, 0, 1) {
		return nil // Already closed
	}

	h.setState(StateClosed)

	if h.onClose != nil {
		h.onClose(h)
	}

	return h.conn.Close()
}

// getState returns the current connection state.
func (h *ConnectionHandler) getState() ConnectionState {
	return ConnectionState(atomic.LoadInt32(&h.state))
}

// setState sets the connection state.
func (h *ConnectionHandler) setState(state ConnectionState) {
	atomic.StoreInt32(&h.state, int32(state))
}

// RemoteAddr returns the remote address.
func (h *ConnectionHandler) RemoteAddr() net.Addr {
	return h.conn.RemoteAddr()
}

// LocalAddr returns the local address.
func (h *ConnectionHandler) LocalAddr() net.Addr {
	return h.conn.LocalAddr()
}

// SessionID returns the session ID.
func (h *ConnectionHandler) SessionID() string {
	return h.sessionID
}

// Username returns the authenticated username.
func (h *ConnectionHandler) Username() string {
	return h.username
}

// Database returns the current database.
func (h *ConnectionHandler) Database() string {
	return h.database
}

// CreatedAt returns the connection creation time.
func (h *ConnectionHandler) CreatedAt() time.Time {
	return h.createdAt
}

// LastActiveAt returns the last activity time.
func (h *ConnectionHandler) LastActiveAt() time.Time {
	return h.lastActiveAt
}

// IsClosed returns whether the connection is closed.
func (h *ConnectionHandler) IsClosed() bool {
	return atomic.LoadInt32(&h.closed) == 1
}