package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestReadHeader_Valid tests reading a valid header
func TestReadHeader_Valid(t *testing.T) {
	// Create a valid header
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], 100) // Length
	buf[8] = MsgPing                          // Type
	binary.BigEndian.PutUint32(buf[9:13], 42) // SeqID

	header, err := ReadHeader(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}

	if string(header.Magic[:]) != MagicPrivate {
		t.Errorf("Magic mismatch: got %q, want %q", string(header.Magic[:]), MagicPrivate)
	}
	if header.Length != 100 {
		t.Errorf("Length mismatch: got %d, want 100", header.Length)
	}
	if header.Type != MsgPing {
		t.Errorf("Type mismatch: got %d, want %d", header.Type, MsgPing)
	}
	if header.SeqID != 42 {
		t.Errorf("SeqID mismatch: got %d, want 42", header.SeqID)
	}
}

// TestReadHeader_ReadError tests reading with io error
func TestReadHeader_ReadError(t *testing.T) {
	// Create a reader that returns an error
	reader := &errorReader{err: errors.New("read error")}
	_, err := ReadHeader(reader)
	if err == nil {
		t.Error("Expected error for failed read")
	}
}

// TestReadHeader_InvalidMagic tests reading with invalid magic
func TestReadHeader_InvalidMagic(t *testing.T) {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], "XXXX") // Invalid magic
	binary.BigEndian.PutUint32(buf[4:8], 100)
	buf[8] = MsgPing
	binary.BigEndian.PutUint32(buf[9:13], 1)

	_, err := ReadHeader(bytes.NewReader(buf))
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

// TestReadHeader_MessageTooLarge tests reading with message too large
func TestReadHeader_MessageTooLarge(t *testing.T) {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], MaxMessageSize+1) // Too large
	buf[8] = MsgPing
	binary.BigEndian.PutUint32(buf[9:13], 1)

	_, err := ReadHeader(bytes.NewReader(buf))
	if err == nil {
		t.Error("Expected error for message too large")
	}
}

// TestReadHeader_ShortRead tests reading with insufficient data
func TestReadHeader_ShortRead(t *testing.T) {
	// Only 5 bytes, need at least HeaderSize
	buf := make([]byte, 5)
	_, err := ReadHeader(bytes.NewReader(buf))
	if err == nil {
		t.Error("Expected error for short read")
	}
}

// TestReadMessage_Valid tests reading a valid message
func TestReadMessage_Valid(t *testing.T) {
	payload := []byte("test payload")
	length := uint32(HeaderSize + len(payload))

	buf := make([]byte, length)
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], length)
	buf[8] = MsgQueryRequest
	binary.BigEndian.PutUint32(buf[9:13], 1)
	copy(buf[HeaderSize:], payload)

	msg, err := ReadMessage(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if msg.Header.Type != MsgQueryRequest {
		t.Errorf("Type mismatch: got %d, want %d", msg.Header.Type, MsgQueryRequest)
	}
	if string(msg.Payload) != string(payload) {
		t.Errorf("Payload mismatch: got %q, want %q", string(msg.Payload), string(payload))
	}
}

// TestReadMessage_NoPayload tests reading a message with no payload
func TestReadMessage_NoPayload(t *testing.T) {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], HeaderSize) // Length equals header size
	buf[8] = MsgPing
	binary.BigEndian.PutUint32(buf[9:13], 1)

	msg, err := ReadMessage(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if len(msg.Payload) != 0 {
		t.Errorf("Expected empty payload, got %d bytes", len(msg.Payload))
	}
}

// TestReadMessage_InvalidHeader tests reading with invalid header
func TestReadMessage_InvalidHeader(t *testing.T) {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], "XXXX") // Invalid magic
	binary.BigEndian.PutUint32(buf[4:8], HeaderSize)
	buf[8] = MsgPing
	binary.BigEndian.PutUint32(buf[9:13], 1)

	_, err := ReadMessage(bytes.NewReader(buf))
	if err == nil {
		t.Error("Expected error for invalid header")
	}
}

// TestReadMessage_PayloadReadError tests reading with payload read error
func TestReadMessage_PayloadReadError(t *testing.T) {
	// Create a reader that has header but fails on payload
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], HeaderSize+10) // Claims 10 bytes payload
	buf[8] = MsgPing
	binary.BigEndian.PutUint32(buf[9:13], 1)

	// Reader only has header bytes
	_, err := ReadMessage(bytes.NewReader(buf))
	if err == nil {
		t.Error("Expected error for failed payload read")
	}
}

// TestWriteMessage_Valid tests writing a valid message
func TestWriteMessage_Valid(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("test payload")

	err := WriteMessage(&buf, MsgQueryRequest, 42, payload)
	if err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	data := buf.Bytes()
	if len(data) != HeaderSize+len(payload) {
		t.Errorf("Length mismatch: got %d, want %d", len(data), HeaderSize+len(payload))
	}

	// Verify header
	if string(data[0:4]) != MagicPrivate {
		t.Error("Magic mismatch")
	}
	if binary.BigEndian.Uint32(data[4:8]) != uint32(HeaderSize+len(payload)) {
		t.Error("Length field mismatch")
	}
	if data[8] != MsgQueryRequest {
		t.Error("Type mismatch")
	}
	if binary.BigEndian.Uint32(data[9:13]) != 42 {
		t.Error("SeqID mismatch")
	}

	// Verify payload
	if string(data[HeaderSize:]) != string(payload) {
		t.Error("Payload mismatch")
	}
}

// TestWriteMessage_NoPayload tests writing a message with no payload
func TestWriteMessage_NoPayload(t *testing.T) {
	var buf bytes.Buffer

	err := WriteMessage(&buf, MsgPing, 1, nil)
	if err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	data := buf.Bytes()
	if len(data) != HeaderSize {
		t.Errorf("Length mismatch: got %d, want %d", len(data), HeaderSize)
	}
}

// TestWriteMessage_WriteError tests writing with io error
func TestWriteMessage_WriteError(t *testing.T) {
	writer := &errorWriter{err: errors.New("write error")}
	err := WriteMessage(writer, MsgPing, 1, nil)
	if err == nil {
		t.Error("Expected error for failed write")
	}
}

// TestWriteMessage_PayloadWriteError tests writing payload with io error
func TestWriteMessage_PayloadWriteError(t *testing.T) {
	writer := &partialWriter{failAfter: HeaderSize}
	err := WriteMessage(writer, MsgPing, 1, []byte("payload"))
	if err == nil {
		t.Error("Expected error for failed payload write")
	}
}

// TestDecodeMessage_TooShort tests decoding with insufficient data
func TestDecodeMessage_TooShort(t *testing.T) {
	data := make([]byte, HeaderSize-1)
	_, err := DecodeMessage(data)
	if err == nil {
		t.Error("Expected error for too short data")
	}
}

// TestDecodeMessage_LengthExceedsData tests decoding with invalid length
func TestDecodeMessage_LengthExceedsData(t *testing.T) {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], 1000) // Length exceeds data
	buf[8] = MsgPing
	binary.BigEndian.PutUint32(buf[9:13], 1)

	_, err := DecodeMessage(buf)
	if err == nil {
		t.Error("Expected error for length exceeding data")
	}
}

// TestMessageType_AllTypes tests all message type strings
func TestMessageType_AllTypes(t *testing.T) {
	tests := []struct {
		typ      byte
		expected string
	}{
		{MsgHandshakeRequest, "HandshakeRequest"},
		{MsgHandshakeResponse, "HandshakeResponse"},
		{MsgAuthRequest, "AuthRequest"},
		{MsgAuthResponse, "AuthResponse"},
		{MsgQueryRequest, "QueryRequest"},
		{MsgQueryResponse, "QueryResponse"},
		{MsgPing, "Ping"},
		{MsgPong, "Pong"},
		{MsgError, "Error"},
		{MsgClose, "Close"},
		{MsgBatchRequest, "BatchRequest"},
		{MsgBatchResponse, "BatchResponse"},
		{0xFF, "Unknown(255)"}, // Unknown type
	}

	for _, tt := range tests {
		result := MessageType(tt.typ)
		if result != tt.expected {
			t.Errorf("MessageType(%d) = %q, want %q", tt.typ, result, tt.expected)
		}
	}
}

// TestConnectionState_String_Unknown tests unknown state string
func TestConnectionState_String_Unknown(t *testing.T) {
	state := ConnectionState(99)
	if state.String() != "UNKNOWN" {
		t.Errorf("Unknown state: got %q, want UNKNOWN", state.String())
	}
}

// TestNewConnectionHandler tests creating a new connection handler
func TestNewConnectionHandler(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	h := NewConnectionHandler(server,
		WithReadTimeout(5*time.Second),
		WithWriteTimeout(5*time.Second),
		WithMaxMessageSize(1024),
	)

	if h == nil {
		t.Fatal("Handler is nil")
	}
	if h.readTimeout != 5*time.Second {
		t.Errorf("ReadTimeout: got %v, want 5s", h.readTimeout)
	}
	if h.writeTimeout != 5*time.Second {
		t.Errorf("WriteTimeout: got %v, want 5s", h.writeTimeout)
	}
	if h.maxMsgSize != 1024 {
		t.Errorf("MaxMessageSize: got %d, want 1024", h.maxMsgSize)
	}
}

// TestConnectionHandler_Accessors tests connection handler accessor methods
func TestConnectionHandler_Accessors(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	h := NewConnectionHandler(server)

	// Test RemoteAddr
	if h.RemoteAddr() == nil {
		t.Error("RemoteAddr should not be nil")
	}

	// Test LocalAddr
	if h.LocalAddr() == nil {
		t.Error("LocalAddr should not be nil")
	}

	// Test initial values
	if h.SessionID() != "" {
		t.Error("Initial SessionID should be empty")
	}
	if h.Username() != "" {
		t.Error("Initial Username should be empty")
	}
	if h.Database() != "" {
		t.Error("Initial Database should be empty")
	}

	// Test CreatedAt
	if h.CreatedAt().IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	// Test LastActiveAt
	if h.LastActiveAt().IsZero() {
		t.Error("LastActiveAt should not be zero")
	}

	// Test IsClosed
	if h.IsClosed() {
		t.Error("Connection should not be closed initially")
	}
}

// TestConnectionHandler_Close tests closing connection
func TestConnectionHandler_Close(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	h := NewConnectionHandler(server)

	// Close should succeed
	if err := h.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Should be closed
	if !h.IsClosed() {
		t.Error("Connection should be closed")
	}

	// Double close should be safe
	if err := h.Close(); err != nil {
		t.Errorf("Double close failed: %v", err)
	}
}

// TestConnectionHandler_CloseHandler tests close handler callback
func TestConnectionHandler_CloseHandler(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	called := false
	h := NewConnectionHandler(server,
		WithCloseHandler(func(c *ConnectionHandler) {
			called = true
		}),
	)

	h.Close()

	if !called {
		t.Error("Close handler was not called")
	}
}

// TestConnectionHandler_GetState tests getting connection state
func TestConnectionHandler_GetState(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	h := NewConnectionHandler(server)

	if h.getState() != StateInit {
		t.Errorf("Initial state: got %v, want %v", h.getState(), StateInit)
	}

	h.setState(StateHandshake)
	if h.getState() != StateHandshake {
		t.Errorf("After setState: got %v, want %v", h.getState(), StateHandshake)
	}
}

// TestConnectionHandler_Handle_ContextCancel tests handling with context cancellation
func TestConnectionHandler_Handle_ContextCancel(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	h := NewConnectionHandler(server)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := h.Handle(ctx)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestDefaultServerConfig tests default server configuration
func TestDefaultServerConfig(t *testing.T) {
	config := DefaultServerConfig()

	if config == nil {
		t.Fatal("DefaultServerConfig returned nil")
	}
	if config.Bind != "0.0.0.0" {
		t.Errorf("Bind: got %q, want 0.0.0.0", config.Bind)
	}
	if config.Port != 9527 {
		t.Errorf("Port: got %d, want 9527", config.Port)
	}
	if config.MaxConnections != 200 {
		t.Errorf("MaxConnections: got %d, want 200", config.MaxConnections)
	}
	if config.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout: got %v, want 30s", config.ReadTimeout)
	}
	if config.WriteTimeout != 30*time.Second {
		t.Errorf("WriteTimeout: got %v, want 30s", config.WriteTimeout)
	}
	if config.MaxMessageSize != MaxMessageSize {
		t.Errorf("MaxMessageSize: got %d, want %d", config.MaxMessageSize, MaxMessageSize)
	}
	if config.AcceptBacklog != 128 {
		t.Errorf("AcceptBacklog: got %d, want 128", config.AcceptBacklog)
	}
}

// TestNewServer_NilConfig tests creating server with nil config
func TestNewServer_NilConfig(t *testing.T) {
	server := NewServer(nil)

	if server == nil {
		t.Fatal("Server is nil")
	}
	if server.config == nil {
		t.Error("Server config should not be nil")
	}
	// Should use default config
	if server.config.Port != 9527 {
		t.Errorf("Port: got %d, want 9527 (default)", server.config.Port)
	}
}

// TestServer_SetHandlers tests setting server handlers
func TestServer_SetHandlers(t *testing.T) {
	server := NewServer(DefaultServerConfig())

	// Set handshake handler
	handshakeCalled := false
	server.SetHandshakeHandler(func(conn *ConnectionHandler, req *HandshakeRequest) (*HandshakeResponse, error) {
		handshakeCalled = true
		return nil, nil
	})

	// Set auth handler
	authCalled := false
	server.SetAuthHandler(func(conn *ConnectionHandler, req *AuthRequest) (*AuthResponse, error) {
		authCalled = true
		return nil, nil
	})

	// Set query handler
	queryCalled := false
	server.SetQueryHandler(func(conn *ConnectionHandler, req *QueryRequest) (*QueryResponse, error) {
		queryCalled = true
		return nil, nil
	})

	// Set connect handler
	connectCalled := false
	server.SetConnectHandler(func(conn *ConnectionHandler) {
		connectCalled = true
	})

	// Set disconnect handler
	disconnectCalled := false
	server.SetDisconnectHandler(func(conn *ConnectionHandler) {
		disconnectCalled = true
	})

	// Verify handlers are set
	if server.onHandshake == nil {
		t.Error("Handshake handler not set")
	}
	if server.onAuth == nil {
		t.Error("Auth handler not set")
	}
	if server.onQuery == nil {
		t.Error("Query handler not set")
	}
	if server.onConnect == nil {
		t.Error("Connect handler not set")
	}
	if server.onDisconnect == nil {
		t.Error("Disconnect handler not set")
	}

	// Just to avoid unused variable warnings
	_ = handshakeCalled
	_ = authCalled
	_ = queryCalled
	_ = connectCalled
	_ = disconnectCalled
}

// TestServer_Connections tests getting active connections
func TestServer_Connections(t *testing.T) {
	config := &ServerConfig{
		Bind:          "127.0.0.1",
		Port:          19540,
		MaxConnections: 10,
	}

	server := NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Initially no connections
	conns := server.Connections()
	if len(conns) != 0 {
		t.Errorf("Expected 0 connections, got %d", len(conns))
	}
}

// TestServer_Stop_WhenNotRunning tests stopping a non-running server
func TestServer_Stop_WhenNotRunning(t *testing.T) {
	server := NewServer(DefaultServerConfig())

	// Stop on non-running server should succeed
	if err := server.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

// TestServer_Addr_BeforeStart tests getting address before start
func TestServer_Addr_BeforeStart(t *testing.T) {
	server := NewServer(DefaultServerConfig())

	if server.Addr() != nil {
		t.Error("Addr should be nil before start")
	}
}

// TestDecodeHandshakeRequest_TooShort tests decoding with too short payload
func TestDecodeHandshakeRequest_TooShort(t *testing.T) {
	_, err := DecodeHandshakeRequest([]byte{0, 1, 2}) // Only 3 bytes
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeHandshakeRequest_InvalidClientVersion tests decoding with invalid client version length
func TestDecodeHandshakeRequest_InvalidClientVersion(t *testing.T) {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf[0:2], 1)        // ProtocolVersion
	binary.BigEndian.PutUint16(buf[2:4], 100)      // ClientVersion length (too large)
	buf[4] = 0
	buf[5] = 0

	_, err := DecodeHandshakeRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid client version length")
	}
}

// TestDecodeHandshakeRequest_InvalidClientInfo tests decoding with invalid client info length
func TestDecodeHandshakeRequest_InvalidClientInfo(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint16(buf[0:2], 1)   // ProtocolVersion
	binary.BigEndian.PutUint16(buf[2:4], 2)   // ClientVersion length
	copy(buf[4:6], "v1")                      // ClientVersion
	binary.BigEndian.PutUint16(buf[6:8], 100) // ClientInfo length (too large)

	_, err := DecodeHandshakeRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid client info length")
	}
}

// TestDecodeHandshakeRequest_InvalidExtension tests decoding with invalid extension length
func TestDecodeHandshakeRequest_InvalidExtension(t *testing.T) {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint16(buf[0:2], 1)   // ProtocolVersion
	binary.BigEndian.PutUint16(buf[2:4], 2)   // ClientVersion length
	copy(buf[4:6], "v1")                      // ClientVersion
	binary.BigEndian.PutUint16(buf[6:8], 0)   // ClientInfo length (empty)
	binary.BigEndian.PutUint16(buf[8:10], 1)  // Extension count
	binary.BigEndian.PutUint16(buf[10:12], 100) // Extension length (too large)

	_, err := DecodeHandshakeRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid extension length")
	}
}

// TestDecodeHandshakeResponse_TooShort tests decoding with too short payload
func TestDecodeHandshakeResponse_TooShort(t *testing.T) {
	_, err := DecodeHandshakeResponse([]byte{0, 1, 2, 3, 4, 5, 6}) // Only 7 bytes
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeHandshakeResponse_InvalidServerVersion tests decoding with invalid server version
func TestDecodeHandshakeResponse_InvalidServerVersion(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint16(buf[0:2], 1)    // ProtocolVersion
	binary.BigEndian.PutUint16(buf[2:4], 100)  // ServerVersion length (too large)

	_, err := DecodeHandshakeResponse(buf)
	if err == nil {
		t.Error("Expected error for invalid server version length")
	}
}

// TestDecodeHandshakeResponse_InvalidAuthChallenge tests decoding with invalid auth challenge
func TestDecodeHandshakeResponse_InvalidAuthChallenge(t *testing.T) {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint16(buf[0:2], 1)   // ProtocolVersion
	binary.BigEndian.PutUint16(buf[2:4], 2)   // ServerVersion length
	copy(buf[4:6], "v1")                     // ServerVersion
	buf[6] = 1                               // Supported
	buf[7] = 0                               // Downgrade
	buf[8] = 100                             // AuthChallenge length (too large)

	_, err := DecodeHandshakeResponse(buf)
	if err == nil {
		t.Error("Expected error for invalid auth challenge length")
	}
}

// TestDecodeAuthRequest_TooShort tests decoding with too short payload
func TestDecodeAuthRequest_TooShort(t *testing.T) {
	_, err := DecodeAuthRequest([]byte{0}) // Only 1 byte
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeAuthRequest_InvalidUsername tests decoding with invalid username length
func TestDecodeAuthRequest_InvalidUsername(t *testing.T) {
	buf := []byte{100} // Username length 100, but no data

	_, err := DecodeAuthRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid username length")
	}
}

// TestDecodeAuthRequest_InvalidPassword tests decoding with invalid password length
func TestDecodeAuthRequest_InvalidPassword(t *testing.T) {
	buf := make([]byte, 5)
	buf[0] = 2                     // Username length
	copy(buf[1:3], "u1")           // Username
	buf[3] = 100                   // Password length (too large)

	_, err := DecodeAuthRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid password length")
	}
}

// TestDecodeAuthRequest_InvalidDatabase tests decoding with invalid database length
func TestDecodeAuthRequest_InvalidDatabase(t *testing.T) {
	buf := make([]byte, 8)
	buf[0] = 2                     // Username length
	copy(buf[1:3], "u1")           // Username
	buf[3] = 2                     // Password length
	copy(buf[4:6], "pw")           // Password
	binary.BigEndian.PutUint16(buf[6:8], 100) // Database length (too large)

	_, err := DecodeAuthRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid database length")
	}
}

// TestDecodeAuthResponse_TooShort tests decoding with too short payload
func TestDecodeAuthResponse_TooShort(t *testing.T) {
	_, err := DecodeAuthResponse([]byte{0, 1, 2, 3, 4, 5, 6, 7}) // Only 8 bytes
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeAuthResponse_InvalidMessage tests decoding with invalid message length
func TestDecodeAuthResponse_InvalidMessage(t *testing.T) {
	buf := make([]byte, 9)
	buf[0] = StatusOK
	binary.BigEndian.PutUint16(buf[1:3], 100) // Message length (too large)

	_, err := DecodeAuthResponse(buf)
	if err == nil {
		t.Error("Expected error for invalid message length")
	}
}

// TestDecodeAuthResponse_InvalidSessionID tests decoding with invalid session id length
func TestDecodeAuthResponse_InvalidSessionID(t *testing.T) {
	buf := make([]byte, 12)
	buf[0] = StatusOK
	binary.BigEndian.PutUint16(buf[1:3], 2)   // Message length
	copy(buf[3:5], "OK")                     // Message
	binary.BigEndian.PutUint16(buf[5:7], 100) // SessionID length (too large)

	_, err := DecodeAuthResponse(buf)
	if err == nil {
		t.Error("Expected error for invalid session id length")
	}
}

// TestDecodeQueryRequest_TooShort tests decoding with too short payload
func TestDecodeQueryRequest_TooShort(t *testing.T) {
	_, err := DecodeQueryRequest([]byte{0, 1, 2, 3, 4, 5}) // Only 6 bytes
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeQueryRequest_InvalidSQL tests decoding with invalid sql length
func TestDecodeQueryRequest_InvalidSQL(t *testing.T) {
	buf := make([]byte, 7)
	binary.BigEndian.PutUint32(buf[0:4], 100) // SQL length (too large)
	buf[4] = 0                                // Flags
	binary.BigEndian.PutUint16(buf[5:7], 0)   // Param count

	_, err := DecodeQueryRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid sql length")
	}
}

// TestDecodeQueryRequest_InvalidParam tests decoding with invalid param length
func TestDecodeQueryRequest_InvalidParam(t *testing.T) {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], 3)   // SQL length
	copy(buf[4:7], "SQL")                    // SQL
	buf[7] = 0                               // Flags
	binary.BigEndian.PutUint16(buf[8:10], 1) // Param count
	binary.BigEndian.PutUint16(buf[10:12], 100) // Param length (too large)

	_, err := DecodeQueryRequest(buf)
	if err == nil {
		t.Error("Expected error for invalid param length")
	}
}

// TestDecodeQueryResponse_TooShort tests decoding with too short payload
func TestDecodeQueryResponse_TooShort(t *testing.T) {
	_, err := DecodeQueryResponse([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}) // 22 bytes
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeQueryResponse_InvalidMessage tests decoding with invalid message length
func TestDecodeQueryResponse_InvalidMessage(t *testing.T) {
	buf := make([]byte, 23)
	buf[0] = StatusOK
	binary.BigEndian.PutUint16(buf[1:3], 100) // Message length (too large)

	_, err := DecodeQueryResponse(buf)
	if err == nil {
		t.Error("Expected error for invalid message length")
	}
}

// TestDecodeErrorPayload_TooShort tests decoding with too short payload
func TestDecodeErrorPayload_TooShort(t *testing.T) {
	_, err := DecodeErrorPayload([]byte{0, 1, 2, 3, 4, 5, 6}) // Only 7 bytes
	if err == nil {
		t.Error("Expected error for too short payload")
	}
}

// TestDecodeErrorPayload_InvalidMessage tests decoding with invalid message length
func TestDecodeErrorPayload_InvalidMessage(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 1)   // Code
	binary.BigEndian.PutUint16(buf[4:6], 100) // Message length (too large)

	_, err := DecodeErrorPayload(buf)
	if err == nil {
		t.Error("Expected error for invalid message length")
	}
}

// TestDecodeErrorPayload_InvalidDetail tests decoding with invalid detail length
func TestDecodeErrorPayload_InvalidDetail(t *testing.T) {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], 1)  // Code
	binary.BigEndian.PutUint16(buf[4:6], 2)  // Message length
	copy(buf[6:8], "OK")                    // Message
	binary.BigEndian.PutUint16(buf[8:10], 100) // Detail length (too large)

	_, err := DecodeErrorPayload(buf)
	if err == nil {
		t.Error("Expected error for invalid detail length")
	}
}

// TestQueryRequest_BatchMode tests query request batch mode flag
func TestQueryRequest_BatchMode(t *testing.T) {
	req := &QueryRequest{
		SQL:       "INSERT INTO t VALUES (?)",
		Params:    [][]byte{[]byte("1")},
		BatchMode: true,
	}

	encoded := req.Encode()
	decoded, err := DecodeQueryRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeQueryRequest failed: %v", err)
	}

	if !decoded.BatchMode {
		t.Error("BatchMode should be true")
	}
}

// TestHandshakeRequest_EmptyExtensions tests handshake with no extensions
func TestHandshakeRequest_EmptyExtensions(t *testing.T) {
	req := &HandshakeRequest{
		ProtocolVersion: ProtocolV1,
		ClientVersion:   "1.0",
		ClientInfo:      "test",
		Extensions:      nil,
	}

	encoded := req.Encode()
	decoded, err := DecodeHandshakeRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeHandshakeRequest failed: %v", err)
	}

	if len(decoded.Extensions) != 0 {
		t.Errorf("Expected 0 extensions, got %d", len(decoded.Extensions))
	}
}

// TestHandshakeResponse_EmptyExtensions tests handshake response with no extensions
func TestHandshakeResponse_EmptyExtensions(t *testing.T) {
	resp := &HandshakeResponse{
		ProtocolVersion: ProtocolV2,
		ServerVersion:   "0.0.1",
		Supported:       false,
		Downgrade:       true,
		AuthChallenge:   nil,
		Extensions:      nil,
	}

	encoded := resp.Encode()
	decoded, err := DecodeHandshakeResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeHandshakeResponse failed: %v", err)
	}

	if decoded.Supported {
		t.Error("Supported should be false")
	}
	if !decoded.Downgrade {
		t.Error("Downgrade should be true")
	}
}

// TestAuthRequest_EmptyDatabase tests auth request with no database
func TestAuthRequest_EmptyDatabase(t *testing.T) {
	req := &AuthRequest{
		Username: "user",
		Password: []byte("pass"),
		Database: "",
	}

	encoded := req.Encode()
	decoded, err := DecodeAuthRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeAuthRequest failed: %v", err)
	}

	if decoded.Database != "" {
		t.Errorf("Database should be empty, got %q", decoded.Database)
	}
}

// TestQueryResponse_WithColumns tests query response with columns
func TestQueryResponse_WithColumns(t *testing.T) {
	resp := &QueryResponse{
		Status:   StatusOK,
		Message:  "OK",
		Columns:  []ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true},
		},
		RowCount:    10,
		Affected:    0,
		LastInsertID: 0,
		ExecuteTime: 50,
	}

	encoded := resp.Encode()
	decoded, err := DecodeQueryResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeQueryResponse failed: %v", err)
	}

	if len(decoded.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(decoded.Columns))
	}
	if decoded.Columns[0].Name != "id" {
		t.Errorf("Column 0 name: got %q, want id", decoded.Columns[0].Name)
	}
	if decoded.Columns[0].Nullable {
		t.Error("Column 0 should not be nullable")
	}
	if !decoded.Columns[1].Nullable {
		t.Error("Column 1 should be nullable")
	}
}

// TestErrorPayload_EmptyDetail tests error payload with no detail
func TestErrorPayload_EmptyDetail(t *testing.T) {
	errPayload := &ErrorPayload{
		Code:    1001,
		Message: "Test error",
		Detail:  "",
	}

	encoded := errPayload.Encode()
	decoded, err := DecodeErrorPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeErrorPayload failed: %v", err)
	}

	if decoded.Detail != "" {
		t.Errorf("Detail should be empty, got %q", decoded.Detail)
	}
}

// TestServer_MaxConnections tests max connection limit
func TestServer_MaxConnections(t *testing.T) {
	config := &ServerConfig{
		Bind:          "127.0.0.1",
		Port:          19541,
		MaxConnections: 2,
	}

	server := NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Connect 2 clients
	var conns []net.Conn
	for i := 0; i < 2; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:19541")
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		conns = append(conns, conn)
	}

	// Give time for connections to be tracked
	time.Sleep(100 * time.Millisecond)

	// Third connection should be rejected (max is 2)
	conn, err := net.Dial("tcp", "127.0.0.1:19541")
	if err == nil {
		// If connection succeeds, it should be closed immediately
		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}

	// Close all connections
	for _, c := range conns {
		c.Close()
	}
}

// TestServer_ConcurrentStop tests concurrent stop calls
func TestServer_ConcurrentStop(t *testing.T) {
	server := NewServer(&ServerConfig{
		Bind: "127.0.0.1",
		Port: 19542,
	})

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Stop() // Multiple stops should be safe
		}()
	}

	wg.Wait()
}

// TestConnectionHandler_Handle_ClosedConnection tests handling closed connection
func TestConnectionHandler_Handle_ClosedConnection(t *testing.T) {
	server, client := net.Pipe()
	client.Close() // Close client side

	h := NewConnectionHandler(server)
	defer h.Close()

	err := h.Handle(context.Background())
	if err == nil {
		t.Error("Expected error for closed connection")
	}
}

// Helper types for testing

// errorReader always returns an error
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

// errorWriter always returns an error
type errorWriter struct {
	err error
}

func (w *errorWriter) Write(p []byte) (n int, err error) {
	return 0, w.err
}

// partialWriter writes some bytes then fails
type partialWriter struct {
	failAfter int
	written   int
}

func (w *partialWriter) Write(p []byte) (n int, err error) {
	remaining := w.failAfter - w.written
	if remaining <= 0 {
		return 0, io.ErrShortWrite
	}

	toWrite := len(p)
	if toWrite > remaining {
		toWrite = remaining
	}

	w.written += toWrite
	return toWrite, nil
}