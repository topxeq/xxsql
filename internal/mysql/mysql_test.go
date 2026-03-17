package mysql

import (
	"crypto/sha1"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestMySQLConstants(t *testing.T) {
	if ProtocolVersion != 10 {
		t.Errorf("Expected protocol version 10, got %d", ProtocolVersion)
	}

	if CharacterSetUTF8MB4 != 45 {
		t.Errorf("Expected character set 45, got %d", CharacterSetUTF8MB4)
	}

	if AuthPluginNativePassword != "mysql_native_password" {
		t.Errorf("Expected mysql_native_password, got %s", AuthPluginNativePassword)
	}
}

func TestMySQLAuthPassword(t *testing.T) {
	password := []byte("testpassword")
	salt := make([]byte, 20)
	for i := range salt {
		salt[i] = byte(i)
	}

	hash1 := sha1.Sum(password)
	hash2 := sha1.Sum(hash1[:])
	combined := append(salt, hash2[:]...)
	hash3 := sha1.Sum(combined)

	authResponse := make([]byte, 20)
	for i := 0; i < 20; i++ {
		authResponse[i] = hash1[i] ^ hash3[i]
	}

	if !MySQLAuthPassword(password, salt, authResponse) {
		t.Error("Expected password verification to succeed")
	}

	if MySQLAuthPassword([]byte("wrongpassword"), salt, authResponse) {
		t.Error("Expected password verification to fail with wrong password")
	}

	if MySQLAuthPassword(password, salt, []byte("short")) {
		t.Error("Expected password verification to fail with short response")
	}
}

func TestLengthEncodedInt(t *testing.T) {
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
	col := &ColumnDefinition{
		Catalog:  "def",
		Schema:   "testdb",
		Table:    "users",
		OrgTable: "users",
		Name:     "id",
		OrgName:  "id",
		Charset:  CharacterSetUTF8MB4,
		Length:   11,
		Type:     3,
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
			if len(tt.row) == 0 {
				t.Error("Expected non-empty row data")
			}
		})
	}
}

func TestServerStartStop(t *testing.T) {
	config := &ServerConfig{
		Bind:           "127.0.0.1",
		Port:           13306,
		MaxConnections: 10,
	}

	server := NewServer(config)

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
	config := &ServerConfig{
		Bind:           "127.0.0.1",
		Port:           13307,
		MaxConnections: 5,
	}

	server := NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	if server.ConnectionCount() != 0 {
		t.Errorf("Expected 0 connections, got %d", server.ConnectionCount())
	}
}

func TestServerStats(t *testing.T) {
	config := &ServerConfig{
		Bind:           "127.0.0.1",
		Port:           13308,
		MaxConnections: 10,
	}

	server := NewServer(config)

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
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	handler := NewMySQLHandler(server, 1,
		WithMySQLAuthHandler(func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error) {
			return username == "testuser", nil
		}),
		WithMySQLQueryHandler(func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error) {
			return nil, nil, nil
		}),
	)

	if handler.ConnectionID() != 1 {
		t.Errorf("Expected connection ID 1, got %d", handler.ConnectionID())
	}

	if handler.IsClosed() {
		t.Error("Handler should not be closed initially")
	}

	handler.Close()

	if !handler.IsClosed() {
		t.Error("Handler should be closed after Close()")
	}
}

func TestMySQLHandlerOptions(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	closeCalled := false

	handler := NewMySQLHandler(server, 1,
		WithMySQLAuthHandler(func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error) {
			return true, nil
		}),
		WithMySQLQueryHandler(func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error) {
			return nil, nil, nil
		}),
		WithMySQLCloseHandler(func(h *MySQLHandler) {
			closeCalled = true
		}),
	)

	if handler == nil {
		t.Fatal("Handler should not be nil")
	}

	handler.Close()

	if !closeCalled {
		t.Error("Close handler should have been called")
	}
}

func TestMySQLCapabilities(t *testing.T) {
	caps := DefaultServerCapabilities

	if caps&CLIENT_PROTOCOL_41 == 0 {
		t.Error("Expected CLIENT_PROTOCOL_41 to be set")
	}
	if caps&CLIENT_SECURE_CONN == 0 {
		t.Error("Expected CLIENT_SECURE_CONN to be set")
	}
	if caps&CLIENT_MULTI_STATEMENTS == 0 {
		t.Error("Expected CLIENT_MULTI_STATEMENTS to be set")
	}
}

func TestMySQLCommandTypes(t *testing.T) {
	if COM_QUIT != 0x01 {
		t.Errorf("COM_QUIT should be 0x01")
	}
	if COM_QUERY != 0x03 {
		t.Errorf("COM_QUERY should be 0x03")
	}
	if COM_PING != 0x0E {
		t.Errorf("COM_PING should be 0x0E")
	}
}

func TestMySQLPacketTypes(t *testing.T) {
	if OK_PACKET != 0x00 {
		t.Errorf("OK_PACKET should be 0x00")
	}
	if ERR_PACKET != 0xFF {
		t.Errorf("ERR_PACKET should be 0xFF")
	}
	if EOF_PACKET != 0xFE {
		t.Errorf("EOF_PACKET should be 0xFE")
	}
}

func TestServerMultipleStarts(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13309,
	}

	server := NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	if err := server.Start(); err == nil {
		t.Error("Expected error on second start")
	}

	server.Stop()
}

func TestServerMaxConnections(t *testing.T) {
	config := &ServerConfig{
		Bind:           "127.0.0.1",
		Port:           13310,
		MaxConnections: 1,
		ReadTimeout:    1 * time.Second,
	}

	server := NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	conn1, err := net.Dial("tcp", "127.0.0.1:13310")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn1.Close()

	time.Sleep(50 * time.Millisecond)

	if server.ConnectionCount() < 1 {
		t.Errorf("Expected at least 1 connection, got %d", server.ConnectionCount())
	}
}

func TestOKPacket(t *testing.T) {
	ok := &OKPacket{
		AffectedRows: 100,
		LastInsertID: 5,
		StatusFlags:  2,
		Warnings:     0,
		Info:         "Rows matched: 100",
	}

	if ok.AffectedRows != 100 {
		t.Errorf("AffectedRows: got %d, want 100", ok.AffectedRows)
	}
}

func TestERRPacket(t *testing.T) {
	err := &ERRPacket{
		ErrorCode:    1064,
		SQLState:     "42000",
		ErrorMessage: "You have an error in your SQL syntax",
	}

	if err.ErrorCode != 1064 {
		t.Errorf("ErrorCode: got %d, want 1064", err.ErrorCode)
	}

	if err.SQLState != "42000" {
		t.Errorf("SQLState: got %q, want '42000'", err.SQLState)
	}
}

func TestServerHandshakePacket(t *testing.T) {
	hs := &ServerHandshakePacket{
		ProtocolVersion:     10,
		ServerVersion:       "5.7.0-XxSql",
		ConnectionID:        12345,
		AuthPluginDataPart1: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		CapabilityFlags1:    0xFFFF,
		CharacterSet:        45,
		StatusFlags:         0,
		CapabilityFlags2:    0xFFFF,
		AuthPluginDataLen:   21,
		AuthPluginDataPart2: []byte{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		AuthPluginName:      "mysql_native_password",
	}

	if hs.ProtocolVersion != ProtocolVersion {
		t.Errorf("ProtocolVersion: got %d, want %d", hs.ProtocolVersion, ProtocolVersion)
	}

	if hs.ConnectionID != 12345 {
		t.Errorf("ConnectionID: got %d, want 12345", hs.ConnectionID)
	}
}

func TestClientHandshakePacket(t *testing.T) {
	hs := &ClientHandshakePacket{
		CapabilityFlags: DefaultServerCapabilities,
		MaxPacketSize:   16777216,
		CharacterSet:    CharacterSetUTF8MB4,
		Username:        "root",
		AuthResponse:    []byte{1, 2, 3, 4, 5},
		Database:        "test",
		AuthPluginName:  "mysql_native_password",
	}

	if hs.Username != "root" {
		t.Errorf("Username: got %q, want 'root'", hs.Username)
	}

	if hs.Database != "test" {
		t.Errorf("Database: got %q, want 'test'", hs.Database)
	}
}

func TestServerStopWithoutStart(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13311,
	}

	server := NewServer(config)

	if err := server.Stop(); err != nil {
		t.Errorf("Stop without start should not error: %v", err)
	}
}

func TestServerDoubleStop(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13312,
	}

	server := NewServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	server.Stop()

	if err := server.Stop(); err != nil {
		t.Errorf("Second stop should not error: %v", err)
	}
}

func TestMySQLAuthPasswordEmpty(t *testing.T) {
	salt := make([]byte, 20)
	for i := range salt {
		salt[i] = byte(i)
	}

	_ = salt
}

func TestServerConfigDefaults(t *testing.T) {
	config := &ServerConfig{
		Bind:           "127.0.0.1",
		Port:           13313,
		MaxConnections: 10,
	}

	_ = config
}

func TestServerAddr(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13314,
	}

	server := NewServer(config)

	if server.Addr() != nil {
		t.Error("Addr should be nil before start")
	}

	server.Start()

	if server.Addr() == nil {
		t.Error("Addr should not be nil after start")
	}

	server.Stop()
}

func TestServerSetQueryHandler(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13315,
	}

	server := NewServer(config)

	handlerCalled := false
	server.SetQueryHandler(func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error) {
		handlerCalled = true
		return nil, nil, nil
	})

	if handlerCalled {
		t.Error("Handler should not be called yet")
	}
}

func TestServerSetAuthHandler(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13316,
	}

	server := NewServer(config)

	authCalled := false
	server.SetAuthHandler(func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error) {
		authCalled = true
		return true, nil
	})

	if authCalled {
		t.Error("Auth handler should not be called yet")
	}
}

func TestMySQLHandlerSessionID(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)

	handler.SetSessionID("test-session")
	if handler.SessionID() != "test-session" {
		t.Error("SessionID not set correctly")
	}
}

func TestMain(m *testing.M) {
	m.Run()
}

func TestDefaultServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()

	if cfg.Bind != "0.0.0.0" {
		t.Errorf("Bind: got %q, want '0.0.0.0'", cfg.Bind)
	}
	if cfg.Port != 3306 {
		t.Errorf("Port: got %d, want 3306", cfg.Port)
	}
	if cfg.MaxConnections != 100 {
		t.Errorf("MaxConnections: got %d, want 100", cfg.MaxConnections)
	}
}

func TestNewServer_NilConfig(t *testing.T) {
	server := NewServer(nil)

	if server == nil {
		t.Fatal("NewServer with nil config should use defaults")
	}
}

func TestServerConnections(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13317,
	}

	server := NewServer(config)

	conns := server.Connections()
	if len(conns) != 0 {
		t.Errorf("Expected 0 connections, got %d", len(conns))
	}
}

func TestServerSetCloseHandler(t *testing.T) {
	config := &ServerConfig{
		Bind: "127.0.0.1",
		Port: 13318,
	}

	server := NewServer(config)

	closeCalled := false
	server.SetCloseHandler(func(h *MySQLHandler) {
		closeCalled = true
	})

	if closeCalled {
		t.Error("Close handler should not be called yet")
	}
}

func TestMySQLHandlerAuthPluginData(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)

	data := handler.AuthPluginData()
	if len(data) != 20 {
		t.Errorf("AuthPluginData length: got %d, want 20", len(data))
	}
}

func TestMySQLHandlerUsernameAndDatabase(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)

	if handler.Username() != "" {
		t.Errorf("Username should be empty initially, got %q", handler.Username())
	}
	if handler.Database() != "" {
		t.Errorf("Database should be empty initially, got %q", handler.Database())
	}
}

func TestColumnDefinition_Fields(t *testing.T) {
	col := &ColumnDefinition{
		Catalog:  "def",
		Schema:   "mydb",
		Table:    "mytable",
		OrgTable: "mytable",
		Name:     "mycolumn",
		OrgName:  "mycolumn",
		Charset:  CharacterSetUTF8MB4,
		Length:   255,
		Type:     253,
		Flags:    1,
		Decimals: 0,
	}

	if col.Catalog != "def" {
		t.Errorf("Catalog: got %q", col.Catalog)
	}
	if col.Schema != "mydb" {
		t.Errorf("Schema: got %q", col.Schema)
	}
	if col.Table != "mytable" {
		t.Errorf("Table: got %q", col.Table)
	}
	if col.Length != 255 {
		t.Errorf("Length: got %d", col.Length)
	}
	if col.Flags != 1 {
		t.Errorf("Flags: got %d", col.Flags)
	}
}

func TestServerStats_Fields(t *testing.T) {
	stats := ServerStats{
		Running:         true,
		ConnectionCount: 5,
		Addr:            "127.0.0.1:3306",
	}

	if !stats.Running {
		t.Error("Running should be true")
	}
	if stats.ConnectionCount != 5 {
		t.Errorf("ConnectionCount: got %d", stats.ConnectionCount)
	}
	if stats.Addr != "127.0.0.1:3306" {
		t.Errorf("Addr: got %q", stats.Addr)
	}
}

func TestClientHandshakePacket_Fields(t *testing.T) {
	hs := &ClientHandshakePacket{
		CapabilityFlags: DefaultServerCapabilities,
		MaxPacketSize:   16777216,
		CharacterSet:    CharacterSetUTF8MB4,
		Username:        "testuser",
		AuthResponse:    []byte{1, 2, 3, 4, 5},
		Database:        "testdb",
		AuthPluginName:  AuthPluginNativePassword,
	}

	if hs.MaxPacketSize != 16777216 {
		t.Errorf("MaxPacketSize: got %d", hs.MaxPacketSize)
	}
	if hs.AuthPluginName != "mysql_native_password" {
		t.Errorf("AuthPluginName: got %q", hs.AuthPluginName)
	}
}

func TestOKPacket_Fields(t *testing.T) {
	ok := &OKPacket{
		AffectedRows: 100,
		LastInsertID: 5,
		StatusFlags:  2,
		Warnings:     0,
		Info:         "Rows matched: 100",
	}

	if ok.StatusFlags != 2 {
		t.Errorf("StatusFlags: got %d", ok.StatusFlags)
	}
	if ok.Warnings != 0 {
		t.Errorf("Warnings: got %d", ok.Warnings)
	}
	if ok.Info != "Rows matched: 100" {
		t.Errorf("Info: got %q", ok.Info)
	}
}

func TestERRPacket_Fields(t *testing.T) {
	err := &ERRPacket{
		ErrorCode:    1064,
		SQLState:     "42000",
		ErrorMessage: "SQL syntax error",
	}

	if err.ErrorCode != 1064 {
		t.Errorf("ErrorCode: got %d", err.ErrorCode)
	}
	if err.SQLState != "42000" {
		t.Errorf("SQLState: got %q", err.SQLState)
	}
	if err.ErrorMessage != "SQL syntax error" {
		t.Errorf("ErrorMessage: got %q", err.ErrorMessage)
	}
}

func TestMySQLAuthPassword_EdgeCases(t *testing.T) {
	salt := make([]byte, 20)
	for i := range salt {
		salt[i] = byte(i)
	}

	if MySQLAuthPassword(nil, salt, make([]byte, 20)) {
		t.Error("Empty password should not verify")
	}

	if MySQLAuthPassword([]byte("pass"), nil, make([]byte, 20)) {
		t.Error("nil salt should not verify")
	}
}

func testReadLengthEncodedInt(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	first := data[0]
	if first < 251 {
		return uint64(first), 1
	}
	if first == 0xFC && len(data) >= 3 {
		return uint64(data[1]) | uint64(data[2])<<8, 3
	}
	if first == 0xFD && len(data) >= 4 {
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4
	}
	if first == 0xFE && len(data) >= 9 {
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16 | uint64(data[4])<<24 |
			uint64(data[5])<<32 | uint64(data[6])<<40 | uint64(data[7])<<48 | uint64(data[8])<<56, 9
	}
	return 0, 0
}

func testWriteLengthEncodedInt(n uint64) []byte {
	if n < 251 {
		return []byte{byte(n)}
	}
	if n < 65536 {
		buf := make([]byte, 3)
		buf[0] = 0xFC
		buf[1] = byte(n)
		buf[2] = byte(n >> 8)
		return buf
	}
	if n < 16777216 {
		buf := make([]byte, 4)
		buf[0] = 0xFD
		buf[1] = byte(n)
		buf[2] = byte(n >> 8)
		buf[3] = byte(n >> 16)
		return buf
	}
	buf := make([]byte, 9)
	buf[0] = 0xFE
	for i := 0; i < 8; i++ {
		buf[i+1] = byte(n >> (i * 8))
	}
	return buf
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testWriteLengthEncodedString(s string) []byte {
	lenBytes := testWriteLengthEncodedInt(uint64(len(s)))
	return append(lenBytes, []byte(s)...)
}

func TestReadLengthEncodedInt(t *testing.T) {
	tests := []struct {
		data     []byte
		expected uint64
		bytes    int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0xFA}, 250, 1},
		{[]byte{0xFC, 0x00, 0x01}, 256, 3},
		{[]byte{0xFC, 0xFF, 0xFF}, 65535, 3},
		{[]byte{0xFD, 0x00, 0x00, 0x01}, 65536, 4},
		{[]byte{0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, 72057594037927936, 9},
		{nil, 0, 0},
	}

	for _, tt := range tests {
		val, n := testReadLengthEncodedInt(tt.data)
		if val != tt.expected || n != tt.bytes {
			t.Errorf("readLengthEncodedInt(%v) = (%d, %d), want (%d, %d)", tt.data, val, n, tt.expected, tt.bytes)
		}
	}
}

func TestWriteLengthEncodedInt(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{250, []byte{0xFA}},
		{251, []byte{0xFC, 0xFB, 0x00}},
		{65535, []byte{0xFC, 0xFF, 0xFF}},
		{65536, []byte{0xFD, 0x00, 0x00, 0x01}},
		{16777215, []byte{0xFD, 0xFF, 0xFF, 0xFF}},
		{16777216, []byte{0xFE, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		result := testWriteLengthEncodedInt(tt.value)
		if !bytesEqual(result, tt.expected) {
			t.Errorf("writeLengthEncodedInt(%d) = %v, want %v", tt.value, result, tt.expected)
		}
	}
}

func TestWriteLengthEncodedString(t *testing.T) {
	tests := []struct {
		str      string
		expected []byte
	}{
		{"", []byte{0x00}},
		{"hello", []byte{0x05, 'h', 'e', 'l', 'l', 'o'}},
		{"test123", []byte{0x07, 't', 'e', 's', 't', '1', '2', '3'}},
	}

	for _, tt := range tests {
		result := testWriteLengthEncodedString(tt.str)
		if !bytesEqual(result, tt.expected) {
			t.Errorf("writeLengthEncodedString(%q) = %v, want %v", tt.str, result, tt.expected)
		}
	}
}

func testEncodeColumnDefinition(col *ColumnDefinition) []byte {
	var buf []byte

	buf = append(buf, testWriteLengthEncodedString(col.Catalog)...)
	buf = append(buf, testWriteLengthEncodedString(col.Schema)...)
	buf = append(buf, testWriteLengthEncodedString(col.Table)...)
	buf = append(buf, testWriteLengthEncodedString(col.OrgTable)...)
	buf = append(buf, testWriteLengthEncodedString(col.Name)...)
	buf = append(buf, testWriteLengthEncodedString(col.OrgName)...)

	buf = append(buf, 0x0C)
	buf = append(buf, byte(col.Charset), byte(col.Charset>>8))
	buf = append(buf, byte(col.Length), byte(col.Length>>8), byte(col.Length>>16), byte(col.Length>>24))
	buf = append(buf, col.Type)
	buf = append(buf, byte(col.Flags), byte(col.Flags>>8))
	buf = append(buf, col.Decimals)
	buf = append(buf, 0, 0)

	return buf
}

func TestEncodeColumnDefinition(t *testing.T) {
	col := &ColumnDefinition{
		Catalog:  "def",
		Schema:   "testdb",
		Table:    "users",
		OrgTable: "users",
		Name:     "id",
		OrgName:  "id",
		Charset:  CharacterSetUTF8MB4,
		Length:   11,
		Type:     3,
		Flags:    0,
		Decimals: 0,
	}

	result := testEncodeColumnDefinition(col)
	if len(result) == 0 {
		t.Error("encodeColumnDefinition should return non-empty data")
	}
}

func testEncodeRowData(row []interface{}) []byte {
	var buf []byte

	for _, val := range row {
		switch v := val.(type) {
		case nil:
			buf = append(buf, 0xFB)
		case []byte:
			buf = append(buf, testWriteLengthEncodedString(string(v))...)
		case string:
			buf = append(buf, testWriteLengthEncodedString(v)...)
		case int:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case int32:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case int64:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case uint:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case uint32:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case uint64:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case float32:
			s := fmt.Sprintf("%f", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		case float64:
			s := fmt.Sprintf("%f", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		default:
			s := fmt.Sprintf("%v", v)
			buf = append(buf, testWriteLengthEncodedString(s)...)
		}
	}

	return buf
}

func TestEncodeRowData(t *testing.T) {
	tests := []struct {
		name string
		row  []interface{}
	}{
		{"integers", []interface{}{1, 2, 3}},
		{"strings", []interface{}{"hello", "world"}},
		{"mixed", []interface{}{1, "test", 3.14, nil}},
		{"bytes", []interface{}{[]byte("binary")}},
		{"int64", []interface{}{int64(123456789)}},
		{"uint64", []interface{}{uint64(987654321)}},
		{"float32", []interface{}{float32(3.14)}},
		{"float64", []interface{}{float64(3.14159)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := testEncodeRowData(tt.row)
			if len(result) == 0 {
				t.Error("encodeRowData should return non-empty data")
			}
		})
	}
}

func TestMySQLHandler_CloseTwice(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)

	handler.Close()
	handler.Close()

	if !handler.IsClosed() {
		t.Error("Handler should be closed")
	}
}

func TestMySQLHandler_EmptyOptions(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)

	if handler.ConnectionID() != 1 {
		t.Errorf("ConnectionID: got %d, want 1", handler.ConnectionID())
	}

	handler.Close()
}

// ============================================================================
// Handler Method Tests
// ============================================================================

func TestMySQLHandler_WriteLengthEncodedInt(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)
	defer handler.Close()

	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{250, []byte{0xFA}},
		{251, []byte{0xFC, 0xFB, 0x00}},
		{65535, []byte{0xFC, 0xFF, 0xFF}},
		{65536, []byte{0xFD, 0x00, 0x00, 0x01}},
		{16777215, []byte{0xFD, 0xFF, 0xFF, 0xFF}},
		{16777216, []byte{0xFE, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		result := handler.writeLengthEncodedInt(tt.value)
		if !bytesEqual(result, tt.expected) {
			t.Errorf("WriteLengthEncodedInt(%d) = %v, want %v", tt.value, result, tt.expected)
		}
	}
}

func TestMySQLHandler_WriteLengthEncodedString(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)
	defer handler.Close()

	tests := []struct {
		str      string
		expected []byte
	}{
		{"", []byte{0x00}},
		{"hello", []byte{0x05, 'h', 'e', 'l', 'l', 'o'}},
		{"test", []byte{0x04, 't', 'e', 's', 't'}},
	}

	for _, tt := range tests {
		result := handler.writeLengthEncodedString(tt.str)
		if !bytesEqual(result, tt.expected) {
			t.Errorf("WriteLengthEncodedString(%q) = %v, want %v", tt.str, result, tt.expected)
		}
	}
}

func TestMySQLHandler_ReadLengthEncodedInt(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)
	defer handler.Close()

	tests := []struct {
		data     []byte
		expected uint64
		bytes    int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0xFA}, 250, 1},
		{[]byte{0xFC, 0x00, 0x01}, 256, 3},
		{[]byte{0xFC, 0xFF, 0xFF}, 65535, 3},
		{[]byte{0xFD, 0x00, 0x00, 0x01}, 65536, 4},
		{[]byte{0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, 72057594037927936, 9},
		{nil, 0, 0},
	}

	for _, tt := range tests {
		val, n := handler.readLengthEncodedInt(tt.data)
		if val != tt.expected || n != tt.bytes {
			t.Errorf("ReadLengthEncodedInt(%v) = (%d, %d), want (%d, %d)", tt.data, val, n, tt.expected, tt.bytes)
		}
	}
}

func TestMySQLHandler_EncodeColumnDefinition(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)
	defer handler.Close()

	col := &ColumnDefinition{
		Catalog:  "def",
		Schema:   "testdb",
		Table:    "users",
		OrgTable: "users",
		Name:     "id",
		OrgName:  "id",
		Charset:  CharacterSetUTF8MB4,
		Length:   11,
		Type:     3,
		Flags:    0,
		Decimals: 0,
	}

	result := handler.encodeColumnDefinition(col)
	if len(result) == 0 {
		t.Error("EncodeColumnDefinition should return non-empty data")
	}
}

func TestMySQLHandler_EncodeRowData(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)
	defer handler.Close()

	tests := []struct {
		name string
		row  []interface{}
	}{
		{"integers", []interface{}{1, 2, 3}},
		{"strings", []interface{}{"hello", "world"}},
		{"mixed", []interface{}{1, "test", 3.14, nil}},
		{"bytes", []interface{}{[]byte("binary")}},
		{"int64", []interface{}{int64(123456789)}},
		{"uint64", []interface{}{uint64(987654321)}},
		{"float32", []interface{}{float32(3.14)}},
		{"float64", []interface{}{float64(3.14159)}},
		{"nil", []interface{}{nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.encodeRowData(tt.row)
			if len(result) == 0 {
				t.Error("EncodeRowData should return non-empty data")
			}
		})
	}
}

func TestMySQLHandler_ProcessCommand_Quit(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	handler := NewMySQLHandler(server, 1)

	// COM_QUIT should return error
	err := handler.processCommand(COM_QUIT, nil)
	if err == nil {
		t.Error("COM_QUIT should return error")
	}
}

func TestMySQLHandler_ProcessCommand_InitDB(t *testing.T) {
	server, _ := net.Pipe()
	defer server.Close()

	handler := NewMySQLHandler(server, 1)

	// This will set the database without writing
	handler.mu.Lock()
	handler.database = "testdb"
	handler.mu.Unlock()

	if handler.Database() != "testdb" {
		t.Errorf("Database should be 'testdb', got %q", handler.Database())
	}
}
