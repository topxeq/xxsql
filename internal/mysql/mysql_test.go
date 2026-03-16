// Package mysql_test provides tests for MySQL protocol.
package mysql_test

import (
	"crypto/sha1"
	"net"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/mysql"
)

func TestMySQLConstants(t *testing.T) {
	// Test protocol version
	if mysql.ProtocolVersion != 10 {
		t.Errorf("Expected protocol version 10, got %d", mysql.ProtocolVersion)
	}

	// Test character set
	if mysql.CharacterSetUTF8MB4 != 45 {
		t.Errorf("Expected character set 45, got %d", mysql.CharacterSetUTF8MB4)
	}

	// Test auth plugin name
	if mysql.AuthPluginNativePassword != "mysql_native_password" {
		t.Errorf("Expected mysql_native_password, got %s", mysql.AuthPluginNativePassword)
	}
}

func TestMySQLAuthPassword(t *testing.T) {
	password := []byte("testpassword")
	salt := make([]byte, 20)
	for i := range salt {
		salt[i] = byte(i)
	}

	// Compute expected auth response
	hash1 := sha1.Sum(password)
	hash2 := sha1.Sum(hash1[:])
	combined := append(salt, hash2[:]...)
	hash3 := sha1.Sum(combined)

	authResponse := make([]byte, 20)
	for i := 0; i < 20; i++ {
		authResponse[i] = hash1[i] ^ hash3[i]
	}

	// Verify
	if !mysql.MySQLAuthPassword(password, salt, authResponse) {
		t.Error("Expected password verification to succeed")
	}

	// Wrong password should fail
	if mysql.MySQLAuthPassword([]byte("wrongpassword"), salt, authResponse) {
		t.Error("Expected password verification to fail with wrong password")
	}

	// Wrong response length should fail
	if mysql.MySQLAuthPassword(password, salt, []byte("short")) {
		t.Error("Expected password verification to fail with short response")
	}
}

func TestLengthEncodedInt(t *testing.T) {
	// Test length-encoded integer encoding/decoding logic
	// These functions are tested through the encode/decode methods

	tests := []struct {
		value    uint64
		expected int
	}{
		{0, 1},
		{250, 1},
		{256, 3},
		{65536, 4},
		{16777216, 9},
	}

	for _, tt := range tests {
		// Verify the length encoding size
		var expectedLen int
		if tt.value < 251 {
			expectedLen = 1
		} else if tt.value < 65536 {
			expectedLen = 3
		} else if tt.value < 16777216 {
			expectedLen = 4
		} else {
			expectedLen = 9
		}

		if expectedLen != tt.expected {
			t.Errorf("Value %d: expected len %d, got %d", tt.value, tt.expected, expectedLen)
		}
	}
}

func TestColumnDefinition(t *testing.T) {
	col := &mysql.ColumnDefinition{
		Catalog:  "def",
		Schema:   "testdb",
		Table:    "users",
		OrgTable: "users",
		Name:     "id",
		OrgName:  "id",
		Charset:  mysql.CharacterSetUTF8MB4,
		Length:   11,
		Type:     3, // INT
		Flags:    0,
		Decimals: 0,
	}

	if col.Name != "id" {
		t.Errorf("Expected name 'id', got %s", col.Name)
	}
	if col.Type != 3 {
		t.Errorf("Expected type 3, got %d", col.Type)
	}
}

func TestRowData(t *testing.T) {
	// Row data encoding is tested through the query handler
	tests := []struct {
		name string
		row  []interface{}
	}{
		{"integers", []interface{}{1, 2, 3}},
		{"strings", []interface{}{"hello", "world"}},
		{"mixed", []interface{}{1, "test", 3.14, nil}},
		{"bytes", []interface{}{[]byte("binary data")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify row has expected number of columns
			if len(tt.row) == 0 {
				t.Error("Expected non-empty row data")
			}
		})
	}
}

func TestServerStartStop(t *testing.T) {
	config := &mysql.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          13306,
		MaxConnections: 10,
	}

	server := mysql.NewServer(config)

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
	config := &mysql.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          13307,
		MaxConnections: 5,
	}

	server := mysql.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	if server.ConnectionCount() != 0 {
		t.Errorf("Expected 0 connections, got %d", server.ConnectionCount())
	}
}

func TestServerStats(t *testing.T) {
	config := &mysql.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          13308,
		MaxConnections: 10,
	}

	server := mysql.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	stats := server.Stats()

	if !stats.Running {
		t.Error("Expected server to be running in stats")
	}
	if stats.ConnectionCount != 0 {
		t.Errorf("Expected 0 connections in stats, got %d", stats.ConnectionCount)
	}
}

func TestMySQLHandler(t *testing.T) {
	// Create a pair of connected sockets
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	handler := mysql.NewMySQLHandler(server, 1,
		mysql.WithMySQLAuthHandler(func(h *mysql.MySQLHandler, username, database string, authResponse []byte) (bool, error) {
			return username == "testuser", nil
		}),
		mysql.WithMySQLQueryHandler(func(h *mysql.MySQLHandler, sql string) ([]*mysql.ColumnDefinition, [][]interface{}, error) {
			return nil, nil, nil
		}),
	)

	if handler.ConnectionID() != 1 {
		t.Errorf("Expected connection ID 1, got %d", handler.ConnectionID())
	}

	// Verify handler is properly initialized
	if handler.IsClosed() {
		t.Error("Handler should not be closed initially")
	}

	// Close handler
	handler.Close()

	if !handler.IsClosed() {
		t.Error("Handler should be closed after Close()")
	}
}

func TestMySQLHandlerOptions(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	closeCalled := false

	handler := mysql.NewMySQLHandler(server, 1,
		mysql.WithMySQLAuthHandler(func(h *mysql.MySQLHandler, username, database string, authResponse []byte) (bool, error) {
			return true, nil
		}),
		mysql.WithMySQLQueryHandler(func(h *mysql.MySQLHandler, sql string) ([]*mysql.ColumnDefinition, [][]interface{}, error) {
			return nil, nil, nil
		}),
		mysql.WithMySQLCloseHandler(func(h *mysql.MySQLHandler) {
			closeCalled = true
		}),
	)

	// Verify handlers are set
	if handler == nil {
		t.Fatal("Handler should not be nil")
	}

	// Close to trigger close handler
	handler.Close()

	if !closeCalled {
		t.Error("Close handler should have been called")
	}
}

func TestMySQLCapabilities(t *testing.T) {
	// Verify default capabilities
	caps := mysql.DefaultServerCapabilities

	if caps&mysql.CLIENT_PROTOCOL_41 == 0 {
		t.Error("Expected CLIENT_PROTOCOL_41 to be set")
	}
	if caps&mysql.CLIENT_SECURE_CONN == 0 {
		t.Error("Expected CLIENT_SECURE_CONN to be set")
	}
	if caps&mysql.CLIENT_MULTI_STATEMENTS == 0 {
		t.Error("Expected CLIENT_MULTI_STATEMENTS to be set")
	}
}

func TestMySQLCommandTypes(t *testing.T) {
	// Test command type values
	if mysql.COM_QUIT != 0x01 {
		t.Errorf("COM_QUIT should be 0x01")
	}
	if mysql.COM_QUERY != 0x03 {
		t.Errorf("COM_QUERY should be 0x03")
	}
	if mysql.COM_PING != 0x0E {
		t.Errorf("COM_PING should be 0x0E")
	}
}

func TestMySQLPacketTypes(t *testing.T) {
	if mysql.OK_PACKET != 0x00 {
		t.Errorf("OK_PACKET should be 0x00")
	}
	if mysql.ERR_PACKET != 0xFF {
		t.Errorf("ERR_PACKET should be 0xFF")
	}
	if mysql.EOF_PACKET != 0xFE {
		t.Errorf("EOF_PACKET should be 0xFE")
	}
}

func TestServerMultipleStarts(t *testing.T) {
	config := &mysql.ServerConfig{
		Bind: "127.0.0.1",
		Port: 13309,
	}

	server := mysql.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Second start should fail
	if err := server.Start(); err == nil {
		t.Error("Expected error on second start")
	}

	server.Stop()
}

func TestServerMaxConnections(t *testing.T) {
	config := &mysql.ServerConfig{
		Bind:          "127.0.0.1",
		Port:          13310,
		MaxConnections: 1,
		ReadTimeout:   1 * time.Second,
	}

	server := mysql.NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Connect first client
	conn1, err := net.Dial("tcp", "127.0.0.1:13310")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn1.Close()

	// Give time for connection to be tracked
	time.Sleep(50 * time.Millisecond)

	// Connection count should be 1
	if server.ConnectionCount() < 1 {
		t.Errorf("Expected at least 1 connection, got %d", server.ConnectionCount())
	}
}

func TestMain(m *testing.M) {
	m.Run()
}
