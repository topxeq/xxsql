// Package protocol_test provides tests for the private protocol.
package protocol_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/protocol"
)

func TestProtocolConstants(t *testing.T) {
	// Test magic bytes
	if protocol.MagicPrivate != "XXSQ" {
		t.Errorf("Expected magic XXSQ, got %s", protocol.MagicPrivate)
	}

	// Test message types
	tests := []struct {
		val      byte
		expected string
	}{
		{protocol.MsgHandshakeRequest, "HandshakeRequest"},
		{protocol.MsgHandshakeResponse, "HandshakeResponse"},
		{protocol.MsgAuthRequest, "AuthRequest"},
		{protocol.MsgAuthResponse, "AuthResponse"},
		{protocol.MsgQueryRequest, "QueryRequest"},
		{protocol.MsgQueryResponse, "QueryResponse"},
		{protocol.MsgPing, "Ping"},
		{protocol.MsgPong, "Pong"},
		{protocol.MsgError, "Error"},
		{protocol.MsgClose, "Close"},
		{protocol.MsgBatchRequest, "BatchRequest"},
		{protocol.MsgBatchResponse, "BatchResponse"},
	}

	for _, tt := range tests {
		result := protocol.MessageType(tt.val)
		if result != tt.expected {
			t.Errorf("MessageType(%d) = %s, expected %s", tt.val, result, tt.expected)
		}
	}
}

func TestMessageEncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		msgType byte
		seqID   uint32
		payload []byte
	}{
		{"empty payload", protocol.MsgPing, 1, nil},
		{"small payload", protocol.MsgQueryRequest, 2, []byte("SELECT 1")},
		{"auth request", protocol.MsgAuthRequest, 3, []byte("test data")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := protocol.NewMessage(tt.msgType, tt.seqID, tt.payload)

			encoded, err := msg.Encode()
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := protocol.DecodeMessage(encoded)
			if err != nil {
				t.Fatalf("DecodeMessage failed: %v", err)
			}

			if decoded.Header.Type != tt.msgType {
				t.Errorf("Type mismatch: got %d, expected %d", decoded.Header.Type, tt.msgType)
			}
			if decoded.Header.SeqID != tt.seqID {
				t.Errorf("SeqID mismatch: got %d, expected %d", decoded.Header.SeqID, tt.seqID)
			}
			if string(decoded.Payload) != string(tt.payload) {
				t.Errorf("Payload mismatch: got %q, expected %q", decoded.Payload, tt.payload)
			}
		})
	}
}

func TestHandshakeRequest(t *testing.T) {
	req := &protocol.HandshakeRequest{
		ProtocolVersion: protocol.ProtocolV1,
		ClientVersion:   "1.0.0",
		ClientInfo:      "test client",
		Extensions:      []string{"compression", "encryption"},
	}

	encoded := req.Encode()
	decoded, err := protocol.DecodeHandshakeRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeHandshakeRequest failed: %v", err)
	}

	if decoded.ProtocolVersion != req.ProtocolVersion {
		t.Errorf("ProtocolVersion mismatch")
	}
	if decoded.ClientVersion != req.ClientVersion {
		t.Errorf("ClientVersion mismatch: got %s, expected %s", decoded.ClientVersion, req.ClientVersion)
	}
	if decoded.ClientInfo != req.ClientInfo {
		t.Errorf("ClientInfo mismatch")
	}
	if len(decoded.Extensions) != len(req.Extensions) {
		t.Errorf("Extensions count mismatch")
	}
}

func TestHandshakeResponse(t *testing.T) {
	resp := &protocol.HandshakeResponse{
		ProtocolVersion: protocol.ProtocolV2,
		ServerVersion:   "0.0.1",
		Supported:       true,
		Downgrade:       false,
		AuthChallenge:   make([]byte, 20),
		Extensions:      []string{"compression"},
	}

	encoded := resp.Encode()
	decoded, err := protocol.DecodeHandshakeResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeHandshakeResponse failed: %v", err)
	}

	if decoded.ProtocolVersion != resp.ProtocolVersion {
		t.Errorf("ProtocolVersion mismatch")
	}
	if !decoded.Supported {
		t.Error("Expected Supported to be true")
	}
	if decoded.Downgrade {
		t.Error("Expected Downgrade to be false")
	}
}

func TestAuthRequest(t *testing.T) {
	req := &protocol.AuthRequest{
		Username: "testuser",
		Password: []byte("hashedpassword"),
		Database: "testdb",
	}

	encoded := req.Encode()
	decoded, err := protocol.DecodeAuthRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeAuthRequest failed: %v", err)
	}

	if decoded.Username != req.Username {
		t.Errorf("Username mismatch: got %s, expected %s", decoded.Username, req.Username)
	}
	if string(decoded.Password) != string(req.Password) {
		t.Errorf("Password mismatch")
	}
	if decoded.Database != req.Database {
		t.Errorf("Database mismatch")
	}
}

func TestAuthResponse(t *testing.T) {
	resp := &protocol.AuthResponse{
		Status:     protocol.StatusOK,
		Message:    "Authentication successful",
		SessionID:  "session-123",
		Permission: 0xFF,
	}

	encoded := resp.Encode()
	decoded, err := protocol.DecodeAuthResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeAuthResponse failed: %v", err)
	}

	if decoded.Status != resp.Status {
		t.Errorf("Status mismatch")
	}
	if decoded.Message != resp.Message {
		t.Errorf("Message mismatch")
	}
	if decoded.SessionID != resp.SessionID {
		t.Errorf("SessionID mismatch")
	}
}

func TestQueryRequest(t *testing.T) {
	req := &protocol.QueryRequest{
		SQL:       "SELECT * FROM users WHERE id = ?",
		Params:    [][]byte{[]byte("1")},
		BatchMode: false,
	}

	encoded := req.Encode()
	decoded, err := protocol.DecodeQueryRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeQueryRequest failed: %v", err)
	}

	if decoded.SQL != req.SQL {
		t.Errorf("SQL mismatch")
	}
	if len(decoded.Params) != len(req.Params) {
		t.Errorf("Params count mismatch")
	}
}

func TestQueryResponse(t *testing.T) {
	resp := &protocol.QueryResponse{
		Status:       protocol.StatusOK,
		Message:      "OK",
		Columns:      []protocol.ColumnInfo{{Name: "id", Type: "INT", Nullable: false}},
		RowCount:     10,
		Affected:     5,
		LastInsertID: 123,
		ExecuteTime:  100,
	}

	encoded := resp.Encode()
	decoded, err := protocol.DecodeQueryResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeQueryResponse failed: %v", err)
	}

	if decoded.Status != resp.Status {
		t.Errorf("Status mismatch")
	}
	if decoded.RowCount != resp.RowCount {
		t.Errorf("RowCount mismatch")
	}
	if len(decoded.Columns) != len(resp.Columns) {
		t.Errorf("Columns count mismatch")
	}
}

func TestErrorPayload(t *testing.T) {
	errPayload := &protocol.ErrorPayload{
		Code:    1001,
		Message: "Test error",
		Detail:  "Additional details",
	}

	encoded := errPayload.Encode()
	decoded, err := protocol.DecodeErrorPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeErrorPayload failed: %v", err)
	}

	if decoded.Code != errPayload.Code {
		t.Errorf("Code mismatch")
	}
	if decoded.Message != errPayload.Message {
		t.Errorf("Message mismatch")
	}
}

func TestServerStartStop(t *testing.T) {
	config := &protocol.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          19527,
		MaxConnections: 10,
	}

	server := protocol.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	if !server.IsRunning() {
		t.Error("Server should be running")
	}

	if server.Addr() == nil {
		t.Error("Server address should not be nil")
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	if server.IsRunning() {
		t.Error("Server should not be running")
	}
}

func TestServerConnectionCount(t *testing.T) {
	config := &protocol.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          19528,
		MaxConnections: 2,
	}

	server := protocol.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Initial count should be 0
	if server.ConnectionCount() != 0 {
		t.Errorf("Expected 0 connections, got %d", server.ConnectionCount())
	}
}

func TestServerMultipleStarts(t *testing.T) {
	config := &protocol.ServerConfig{
		Bind: "127.0.0.1",
		Port: 19529,
	}

	server := protocol.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Second start should fail
	if err := server.Start(); err == nil {
		t.Error("Expected error on second start")
	}

	server.Stop()
}

func TestServerBroadcast(t *testing.T) {
	config := &protocol.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          19530,
		MaxConnections: 10,
	}

	server := protocol.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Broadcast to no connections should succeed
	if err := server.Broadcast(protocol.MsgPing, nil); err != nil {
		t.Errorf("Broadcast to empty connections failed: %v", err)
	}
}

func TestConnectionState(t *testing.T) {
	tests := []struct {
		state    protocol.ConnectionState
		expected string
	}{
		{protocol.StateInit, "INIT"},
		{protocol.StateHandshake, "HANDSHAKE"},
		{protocol.StateAuth, "AUTH"},
		{protocol.StateReady, "READY"},
		{protocol.StateQuery, "QUERY"},
		{protocol.StateClosing, "CLOSING"},
		{protocol.StateClosed, "CLOSED"},
	}

	for _, tt := range tests {
		if tt.state.String() != tt.expected {
			t.Errorf("State %d: expected %s, got %s", tt.state, tt.expected, tt.state.String())
		}
	}
}

func TestConcurrentConnections(t *testing.T) {
	config := &protocol.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          19531,
		MaxConnections: 100,
	}

	server := protocol.NewServer(config)
	server.SetHandshakeHandler(func(conn *protocol.ConnectionHandler, req *protocol.HandshakeRequest) (*protocol.HandshakeResponse, error) {
		return &protocol.HandshakeResponse{
			ProtocolVersion: req.ProtocolVersion,
			ServerVersion:   "0.0.1",
			Supported:       true,
		}, nil
	})
	server.SetAuthHandler(func(conn *protocol.ConnectionHandler, req *protocol.AuthRequest) (*protocol.AuthResponse, error) {
		return &protocol.AuthResponse{
			Status:    protocol.StatusOK,
			SessionID: "test-session",
		}, nil
	})

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", "127.0.0.1:19531")
			if err != nil {
				return
			}
			defer conn.Close()

			// Read and ignore
			buf := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			conn.Read(buf)
		}()
	}

	wg.Wait()

	// Give time for connections to be tracked
	time.Sleep(200 * time.Millisecond)

	// Verify connection count
	count := server.ConnectionCount()
	if count > 10 {
		t.Errorf("Too many connections: %d", count)
	}
}

func TestMain(m *testing.M) {
	m.Run()
}
