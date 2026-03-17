// Package mysql implements MySQL protocol compatibility for XxSql.
package mysql

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// MySQL protocol constants
const (
	ProtocolVersion = 10
	MaxPacketSize   = 1<<24 - 1 // 16MB

	// Client capabilities
	CLIENT_LONG_PASSWORD  = 0x00000001
	CLIENT_FOUND_ROWS     = 0x00000002
	CLIENT_LONG_FLAG      = 0x00000004
	CLIENT_CONNECT_WITH_DB = 0x00000008
	CLIENT_NO_SCHEMA      = 0x00000010
	CLIENT_COMPRESS       = 0x00000020
	CLIENT_ODBC           = 0x00000040
	CLIENT_LOCAL_FILES    = 0x00000080
	CLIENT_IGNORE_SPACE   = 0x00000100
	CLIENT_PROTOCOL_41    = 0x00000200
	CLIENT_INTERACTIVE    = 0x00000400
	CLIENT_SSL            = 0x00000800
	CLIENT_IGNORE_SIGPIPE = 0x00001000
	CLIENT_TRANSACTIONS   = 0x00002000
	CLIENT_SECURE_CONN    = 0x00008000
	CLIENT_MULTI_STATEMENTS = 0x00010000
	CLIENT_MULTI_RESULTS  = 0x00020000
	CLIENT_PS_MULTI_RESULTS = 0x00040000
	CLIENT_PLUGIN_AUTH    = 0x00080000
	CLIENT_CONNECT_ATTRS  = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
	CLIENT_DEPRECATE_EOF  = 0x01000000

	// Server capabilities (what we support)
	DefaultServerCapabilities = CLIENT_LONG_PASSWORD |
		CLIENT_FOUND_ROWS |
		CLIENT_LONG_FLAG |
		CLIENT_CONNECT_WITH_DB |
		CLIENT_PROTOCOL_41 |
		CLIENT_TRANSACTIONS |
		CLIENT_SECURE_CONN |
		CLIENT_MULTI_STATEMENTS |
		CLIENT_MULTI_RESULTS |
		CLIENT_PLUGIN_AUTH |
		CLIENT_DEPRECATE_EOF

	// Character sets
	CharacterSetUTF8MB4 = 45

	// Command types
	COM_SLEEP         = 0x00
	COM_QUIT          = 0x01
	COM_INIT_DB       = 0x02
	COM_QUERY         = 0x03
	COM_FIELD_LIST    = 0x04
	COM_CREATE_DB     = 0x05
	COM_DROP_DB       = 0x06
	COM_REFRESH       = 0x07
	COM_SHUTDOWN      = 0x08
	COM_STATISTICS    = 0x09
	COM_PROCESS_INFO  = 0x0A
	COM_CONNECT       = 0x0B
	COM_PROCESS_KILL  = 0x0C
	COM_DEBUG         = 0x0D
	COM_PING          = 0x0E
	COM_CHANGE_USER   = 0x11
	COM_RESET_CONNECTION = 0x1F

	// Response types
	OK_PACKET    = 0x00
	ERR_PACKET   = 0xFF
	EOF_PACKET   = 0xFE

	// Auth plugin names
	AuthPluginNativePassword = "mysql_native_password"
	AuthPluginCachingSHA2    = "caching_sha2_password"
)

// ServerHandshakePacket is the initial handshake packet sent by the server.
type ServerHandshakePacket struct {
	ProtocolVersion    uint8
	ServerVersion      string
	ConnectionID       uint32
	AuthPluginDataPart1 []byte // 8 bytes
	CapabilityFlags1   uint16
	CharacterSet       uint8
	StatusFlags        uint16
	CapabilityFlags2   uint16
	AuthPluginDataLen  uint8
	AuthPluginDataPart2 []byte // 12 bytes
	AuthPluginName     string
}

// ClientHandshakePacket is the response from the client.
type ClientHandshakePacket struct {
	CapabilityFlags uint32
	MaxPacketSize   uint32
	CharacterSet    uint8
	Username        string
	AuthResponse    []byte
	Database        string
	AuthPluginName  string
}

// OKPacket represents an OK response packet.
type OKPacket struct {
	AffectedRows     uint64
	LastInsertID     uint64
	StatusFlags      uint16
	Warnings         uint16
	Info             string
}

// ERRPacket represents an error response packet.
type ERRPacket struct {
	ErrorCode    uint16
	SQLState     string
	ErrorMessage string
}

// ColumnDefinition represents a column definition packet.
type ColumnDefinition struct {
	Catalog    string
	Schema     string
	Table      string
	OrgTable   string
	Name       string
	OrgName    string
	Charset    uint16
	Length     uint32
	Type       uint8
	Flags      uint16
	Decimals   uint8
}

// MySQLHandler handles a MySQL protocol connection.
type MySQLHandler struct {
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	connectionID   uint32
	capabilityFlags uint32
	charset        uint8
	statusFlags    uint16
	authPluginData []byte
	seqID          uint8
	closed         int32
	createdAt      time.Time
	lastActiveAt   time.Time

	// Session info
	sessionID string
	username  string
	database  string

	// Callbacks
	onAuth  func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error)
	onQuery func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error)
	onClose func(h *MySQLHandler)

	mu sync.Mutex
}

// MySQLHandlerOption is a functional option for MySQLHandler.
type MySQLHandlerOption func(*MySQLHandler)

// WithMySQLAuthHandler sets the auth handler.
func WithMySQLAuthHandler(fn func(h *MySQLHandler, username, database string, authResponse []byte) (bool, error)) MySQLHandlerOption {
	return func(h *MySQLHandler) {
		h.onAuth = fn
	}
}

// WithMySQLQueryHandler sets the query handler.
func WithMySQLQueryHandler(fn func(h *MySQLHandler, sql string) ([]*ColumnDefinition, [][]interface{}, error)) MySQLHandlerOption {
	return func(h *MySQLHandler) {
		h.onQuery = fn
	}
}

// WithMySQLCloseHandler sets the close handler.
func WithMySQLCloseHandler(fn func(h *MySQLHandler)) MySQLHandlerOption {
	return func(h *MySQLHandler) {
		h.onClose = fn
	}
}

// NewMySQLHandler creates a new MySQL handler.
func NewMySQLHandler(conn net.Conn, connectionID uint32, opts ...MySQLHandlerOption) *MySQLHandler {
	h := &MySQLHandler{
		conn:          conn,
		reader:        bufio.NewReader(conn),
		writer:        bufio.NewWriter(conn),
		connectionID:  connectionID,
		capabilityFlags: DefaultServerCapabilities,
		charset:       CharacterSetUTF8MB4,
		statusFlags:   0,
		createdAt:     time.Now(),
		lastActiveAt:  time.Now(),
	}

	// Generate random auth plugin data (20 bytes)
	h.authPluginData = make([]byte, 20)
	// In production, use crypto/rand
	for i := range h.authPluginData {
		h.authPluginData[i] = byte(i * 17 % 256)
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// Handle handles the MySQL connection.
func (h *MySQLHandler) Handle() error {
	defer h.Close()

	// Send initial handshake
	if err := h.sendHandshake(); err != nil {
		return fmt.Errorf("send handshake: %w", err)
	}

	// Read client handshake response
	if err := h.readClientHandshake(); err != nil {
		return fmt.Errorf("read client handshake: %w", err)
	}

	// Command loop
	for {
		h.lastActiveAt = time.Now()

		// Read command
		cmd, payload, err := h.readPacket()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read command: %w", err)
		}

		// Process command
		if err := h.processCommand(cmd, payload); err != nil {
			return err
		}
	}
}

// sendHandshake sends the initial handshake packet.
func (h *MySQLHandler) sendHandshake() error {
	hs := &ServerHandshakePacket{
		ProtocolVersion:    ProtocolVersion,
		ServerVersion:      "5.7.0-XxSql-0.0.1",
		ConnectionID:       h.connectionID,
		AuthPluginDataPart1: h.authPluginData[:8],
		CapabilityFlags1:   uint16(h.capabilityFlags & 0xFFFF),
		CharacterSet:       h.charset,
		StatusFlags:        h.statusFlags,
		CapabilityFlags2:   uint16((h.capabilityFlags >> 16) & 0xFFFF),
		AuthPluginDataLen:  21, // 20 bytes + null terminator
		AuthPluginDataPart2: h.authPluginData[8:20],
		AuthPluginName:     AuthPluginNativePassword,
	}

	return h.writePacket(h.encodeHandshake(hs))
}

// encodeHandshake encodes the handshake packet.
func (h *MySQLHandler) encodeHandshake(hs *ServerHandshakePacket) []byte {
	// Calculate packet size
	size := 1 + // Protocol version
		len(hs.ServerVersion) + 1 + // Server version (null-terminated)
		4 + // Connection ID
		8 + // Auth plugin data part 1
		1 + // Filler
		2 + // Capability flags 1
		1 + // Character set
		2 + // Status flags
		2 + // Capability flags 2
		1 + // Auth plugin data len
		10 + // Reserved
		12 + // Auth plugin data part 2
		len(hs.AuthPluginName) + 1 // Auth plugin name (null-terminated)

	buf := make([]byte, 4+size) // 4 bytes header
	offset := 4

	// Protocol version
	buf[offset] = hs.ProtocolVersion
	offset += 1

	// Server version
	copy(buf[offset:], hs.ServerVersion)
	offset += len(hs.ServerVersion)
	buf[offset] = 0 // null terminator
	offset += 1

	// Connection ID
	binary.LittleEndian.PutUint32(buf[offset:], hs.ConnectionID)
	offset += 4

	// Auth plugin data part 1
	copy(buf[offset:], hs.AuthPluginDataPart1)
	offset += 8

	// Filler
	buf[offset] = 0
	offset += 1

	// Capability flags 1
	binary.LittleEndian.PutUint16(buf[offset:], hs.CapabilityFlags1)
	offset += 2

	// Character set
	buf[offset] = hs.CharacterSet
	offset += 1

	// Status flags
	binary.LittleEndian.PutUint16(buf[offset:], hs.StatusFlags)
	offset += 2

	// Capability flags 2
	binary.LittleEndian.PutUint16(buf[offset:], hs.CapabilityFlags2)
	offset += 2

	// Auth plugin data len
	buf[offset] = hs.AuthPluginDataLen
	offset += 1

	// Reserved (10 bytes)
	offset += 10

	// Auth plugin data part 2
	copy(buf[offset:], hs.AuthPluginDataPart2)
	offset += 12

	// Auth plugin name
	copy(buf[offset:], hs.AuthPluginName)
	offset += len(hs.AuthPluginName)
	buf[offset] = 0 // null terminator

	// Write header
	binary.LittleEndian.PutUint32(buf[0:4], uint32(offset-4)|uint32(h.seqID)<<24)
	h.seqID++

	return buf
}

// readClientHandshake reads the client handshake response.
func (h *MySQLHandler) readClientHandshake() error {
	packet, err := h.readRawPacket()
	if err != nil {
		return err
	}

	hs, err := h.decodeClientHandshake(packet)
	if err != nil {
		return err
	}

	h.capabilityFlags = hs.CapabilityFlags
	h.username = hs.Username
	h.database = hs.Database

	// Authenticate
	var authOK bool
	if h.onAuth != nil {
		authOK, err = h.onAuth(h, hs.Username, hs.Database, hs.AuthResponse)
	} else {
		// Default: accept all connections (for testing)
		authOK = true
	}

	if err != nil || !authOK {
		// Send error
		return h.sendERR(1045, "28000", "Access denied for user '"+hs.Username+"'")
	}

	// Send OK packet
	return h.sendOK(0, 0)
}

// decodeClientHandshake decodes the client handshake packet.
func (h *MySQLHandler) decodeClientHandshake(data []byte) (*ClientHandshakePacket, error) {
	if len(data) < 36 {
		return nil, fmt.Errorf("packet too short")
	}

	hs := &ClientHandshakePacket{}
	offset := 0

	// Capability flags
	hs.CapabilityFlags = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Max packet size
	hs.MaxPacketSize = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Character set
	hs.CharacterSet = data[offset]
	offset += 1

	// Skip reserved (23 bytes)
	offset += 23

	// Username (null-terminated)
	end := offset
	for end < len(data) && data[end] != 0 {
		end++
	}
	hs.Username = string(data[offset:end])
	offset = end + 1

	// Auth response
	if hs.CapabilityFlags&CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
		// Length-encoded
		authLen, n := h.readLengthEncodedInt(data[offset:])
		offset += n
		hs.AuthResponse = data[offset : offset+int(authLen)]
		offset += int(authLen)
	} else if hs.CapabilityFlags&CLIENT_SECURE_CONN != 0 {
		authLen := int(data[offset])
		offset += 1
		hs.AuthResponse = data[offset : offset+authLen]
		offset += authLen
	}

	// Database
	if hs.CapabilityFlags&CLIENT_CONNECT_WITH_DB != 0 && offset < len(data) {
		end = offset
		for end < len(data) && data[end] != 0 {
			end++
		}
		hs.Database = string(data[offset:end])
	}

	// Auth plugin name
	if hs.CapabilityFlags&CLIENT_PLUGIN_AUTH != 0 && offset < len(data) {
		offset++ // skip null
		end = offset
		for end < len(data) && data[end] != 0 {
			end++
		}
		hs.AuthPluginName = string(data[offset:end])
	}

	return hs, nil
}

// readLengthEncodedInt reads a length-encoded integer.
func (h *MySQLHandler) readLengthEncodedInt(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	first := data[0]
	if first < 251 {
		return uint64(first), 1
	}
	if first == 0xFC {
		if len(data) < 3 {
			return 0, 0
		}
		return uint64(binary.LittleEndian.Uint16(data[1:])), 3
	}
	if first == 0xFD {
		if len(data) < 4 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4
	}
	if first == 0xFE {
		if len(data) < 9 {
			return 0, 0
		}
		return binary.LittleEndian.Uint64(data[1:]), 9
	}
	return 0, 0
}

// writeLengthEncodedInt writes a length-encoded integer.
func (h *MySQLHandler) writeLengthEncodedInt(n uint64) []byte {
	if n < 251 {
		return []byte{byte(n)}
	}
	if n < 65536 {
		buf := make([]byte, 3)
		buf[0] = 0xFC
		binary.LittleEndian.PutUint16(buf[1:], uint16(n))
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
	binary.LittleEndian.PutUint64(buf[1:], n)
	return buf
}

// writeLengthEncodedString writes a length-encoded string.
func (h *MySQLHandler) writeLengthEncodedString(s string) []byte {
	lenBytes := h.writeLengthEncodedInt(uint64(len(s)))
	return append(lenBytes, []byte(s)...)
}

// processCommand processes a MySQL command.
func (h *MySQLHandler) processCommand(cmd byte, payload []byte) error {
	switch cmd {
	case COM_QUIT:
		return fmt.Errorf("client quit")
	case COM_PING:
		return h.sendOK(0, 0)
	case COM_INIT_DB:
		h.database = string(payload)
		return h.sendOK(0, 0)
	case COM_QUERY:
		return h.handleQuery(string(payload))
	case COM_FIELD_LIST:
		return h.handleFieldList(string(payload))
	case COM_STATISTICS:
		return h.handleStatistics()
	default:
		return h.sendERR(1047, "42000", fmt.Sprintf("Unknown command: %d", cmd))
	}
}

// handleQuery handles a COM_QUERY command.
func (h *MySQLHandler) handleQuery(sql string) error {
	if h.onQuery == nil {
		return h.sendOK(0, 0)
	}

	columns, rows, err := h.onQuery(h, sql)
	if err != nil {
		return h.sendERR(1064, "42000", err.Error())
	}

	if len(columns) == 0 {
		// No result set (INSERT, UPDATE, DELETE, etc.)
		return h.sendOK(0, 0)
	}

	// Send column count
	colCount := h.writeLengthEncodedInt(uint64(len(columns)))
	if err := h.writePacket(append([]byte{0}, colCount...)); err != nil {
		return err
	}

	// Send column definitions
	for _, col := range columns {
		data := h.encodeColumnDefinition(col)
		if err := h.writePacket(data); err != nil {
			return err
		}
	}

	// Send EOF packet (if CLIENT_DEPRECATE_EOF not set)
	if h.capabilityFlags&CLIENT_DEPRECATE_EOF == 0 {
		if err := h.writePacket([]byte{EOF_PACKET, 0, 0, 0, 0}); err != nil {
			return err
		}
	}

	// Send row data
	for _, row := range rows {
		data := h.encodeRowData(row)
		if err := h.writePacket(data); err != nil {
			return err
		}
	}

	// Send EOF packet
	if h.capabilityFlags&CLIENT_DEPRECATE_EOF != 0 {
		return h.writePacket([]byte{OK_PACKET, 0, 0, 0, 0, 0})
	}
	return h.writePacket([]byte{EOF_PACKET, 0, 0, 0, 0})
}

// encodeColumnDefinition encodes a column definition.
func (h *MySQLHandler) encodeColumnDefinition(col *ColumnDefinition) []byte {
	var buf []byte

	buf = append(buf, h.writeLengthEncodedString(col.Catalog)...)
	buf = append(buf, h.writeLengthEncodedString(col.Schema)...)
	buf = append(buf, h.writeLengthEncodedString(col.Table)...)
	buf = append(buf, h.writeLengthEncodedString(col.OrgTable)...)
	buf = append(buf, h.writeLengthEncodedString(col.Name)...)
	buf = append(buf, h.writeLengthEncodedString(col.OrgName)...)

	// Length of fixed length fields (0x0C)
	buf = append(buf, 0x0C)

	// Character set
	buf = append(buf, byte(col.Charset), byte(col.Charset>>8))

	// Column length
	buf = append(buf, byte(col.Length), byte(col.Length>>8), byte(col.Length>>16), byte(col.Length>>24))

	// Column type
	buf = append(buf, col.Type)

	// Flags
	buf = append(buf, byte(col.Flags), byte(col.Flags>>8))

	// Decimals
	buf = append(buf, col.Decimals)

	// Filler
	buf = append(buf, 0, 0)

	return buf
}

// encodeRowData encodes a row data packet.
func (h *MySQLHandler) encodeRowData(row []interface{}) []byte {
	var buf []byte

	for _, val := range row {
		switch v := val.(type) {
		case nil:
			buf = append(buf, 0xFB) // NULL
		case []byte:
			buf = append(buf, h.writeLengthEncodedString(string(v))...)
		case string:
			buf = append(buf, h.writeLengthEncodedString(v)...)
		case int, int32, int64, uint, uint32, uint64:
			s := fmt.Sprintf("%d", v)
			buf = append(buf, h.writeLengthEncodedString(s)...)
		case float32, float64:
			s := fmt.Sprintf("%f", v)
			buf = append(buf, h.writeLengthEncodedString(s)...)
		default:
			s := fmt.Sprintf("%v", v)
			buf = append(buf, h.writeLengthEncodedString(s)...)
		}
	}

	return buf
}

// handleFieldList handles COM_FIELD_LIST.
func (h *MySQLHandler) handleFieldList(table string) error {
	// TODO: Implement field list
	return h.sendERR(1046, "42000", "COM_FIELD_LIST not implemented")
}

// handleStatistics handles COM_STATISTICS.
func (h *MySQLHandler) handleStatistics() error {
	stats := fmt.Sprintf("Uptime: %d\n", time.Since(h.createdAt)/time.Second)
	return h.writePacket([]byte(stats))
}

// sendOK sends an OK packet.
func (h *MySQLHandler) sendOK(affectedRows, lastInsertID uint64) error {
	var buf []byte
	buf = append(buf, OK_PACKET)
	buf = append(buf, h.writeLengthEncodedInt(affectedRows)...)
	buf = append(buf, h.writeLengthEncodedInt(lastInsertID)...)
	buf = append(buf, byte(h.statusFlags), byte(h.statusFlags>>8))
	buf = append(buf, 0, 0) // warnings
	return h.writePacket(buf)
}

// sendERR sends an error packet.
func (h *MySQLHandler) sendERR(code uint16, sqlState, message string) error {
	var buf []byte
	buf = append(buf, ERR_PACKET)
	buf = append(buf, byte(code), byte(code>>8))
	buf = append(buf, '#') // SQL state marker
	buf = append(buf, sqlState...)
	buf = append(buf, message...)
	return h.writePacket(buf)
}

// readPacket reads a MySQL packet.
func (h *MySQLHandler) readPacket() (byte, []byte, error) {
	data, err := h.readRawPacket()
	if err != nil {
		return 0, nil, err
	}

	if len(data) == 0 {
		return 0, nil, fmt.Errorf("empty packet")
	}

	// Check for error packet
	if data[0] == ERR_PACKET {
		return 0, nil, fmt.Errorf("error packet received")
	}

	// Return command type and payload
	return data[0], data[1:], nil
}

// readRawPacket reads a raw MySQL packet.
func (h *MySQLHandler) readRawPacket() ([]byte, error) {
	// Read header (4 bytes)
	header := make([]byte, 4)
	if _, err := io.ReadFull(h.reader, header); err != nil {
		return nil, err
	}

	// Parse length and sequence ID
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	seqID := header[3]
	h.seqID = seqID + 1

	// Read payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(h.reader, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

// writePacket writes a MySQL packet.
func (h *MySQLHandler) writePacket(data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Write length and sequence ID
	header := make([]byte, 4)
	length := len(data)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = h.seqID
	h.seqID++

	if _, err := h.writer.Write(header); err != nil {
		return err
	}
	if _, err := h.writer.Write(data); err != nil {
		return err
	}
	return h.writer.Flush()
}

// Close closes the connection.
func (h *MySQLHandler) Close() error {
	if !atomic.CompareAndSwapInt32(&h.closed, 0, 1) {
		return nil
	}

	if h.onClose != nil {
		h.onClose(h)
	}

	return h.conn.Close()
}

// ConnectionID returns the connection ID.
func (h *MySQLHandler) ConnectionID() uint32 {
	return h.connectionID
}

// Username returns the authenticated username.
func (h *MySQLHandler) Username() string {
	return h.username
}

// Database returns the current database.
func (h *MySQLHandler) Database() string {
	return h.database
}

// SessionID returns the session ID.
func (h *MySQLHandler) SessionID() string {
	return h.sessionID
}

// SetSessionID sets the session ID.
func (h *MySQLHandler) SetSessionID(id string) {
	h.sessionID = id
}

// IsClosed returns whether the connection is closed.
func (h *MySQLHandler) IsClosed() bool {
	return atomic.LoadInt32(&h.closed) == 1
}

// AuthPluginData returns the auth plugin data (salt) used for authentication.
func (h *MySQLHandler) AuthPluginData() []byte {
	return h.authPluginData
}

// MySQLAuthPassword verifies MySQL native password authentication.
func MySQLAuthPassword(password, salt, authResponse []byte) bool {
	// SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	hash1 := sha1.Sum(password)
	hash2 := sha1.Sum(hash1[:])

	combined := append(salt, hash2[:]...)
	hash3 := sha1.Sum(combined)

	// XOR with hash1
	result := make([]byte, 20)
	for i := 0; i < 20; i++ {
		result[i] = hash1[i] ^ hash3[i]
	}

	// Compare
	if len(authResponse) != 20 {
		return false
	}
	for i := 0; i < 20; i++ {
		if result[i] != authResponse[i] {
			return false
		}
	}
	return true
}