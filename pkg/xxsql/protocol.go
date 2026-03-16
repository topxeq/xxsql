package xxsql

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// MySQL protocol constants
const (
	ProtocolVersion = 10
	MaxPacketSize   = 1<<24 - 1 // 16MB

	// Client capabilities
	ClientLongPassword    = 0x00000001
	ClientFoundRows       = 0x00000002
	ClientLongFlag        = 0x00000004
	ClientConnectWithDB   = 0x00000008
	ClientNoSchema        = 0x00000010
	ClientCompress        = 0x00000020
	ClientODBC            = 0x00000040
	ClientLocalFiles      = 0x00000080
	ClientIgnoreSpace     = 0x00000100
	ClientProtocol41      = 0x00000200
	ClientInteractive     = 0x00000400
	ClientSSL             = 0x00000800
	ClientIgnoreSigpipe   = 0x00001000
	ClientTransactions    = 0x00002000
	ClientSecureConn      = 0x00008000
	ClientMultiStatements = 0x00010000
	ClientMultiResults    = 0x00020000
	ClientPluginAuth      = 0x00080000
	ClientDeprecateEOF    = 0x01000000

	// Default client capabilities
	DefaultClientCapabilities = ClientLongPassword |
		ClientFoundRows |
		ClientLongFlag |
		ClientConnectWithDB |
		ClientProtocol41 |
		ClientTransactions |
		ClientSecureConn |
		ClientMultiStatements |
		ClientMultiResults |
		ClientPluginAuth |
		ClientDeprecateEOF

	// Character sets
	CharacterSetUTF8MB4 = 45

	// Command types
	ComQuit     = 0x01
	ComInitDB   = 0x02
	ComQuery    = 0x03
	ComPing     = 0x0E
	ComStmtPrepare = 0x16
	ComStmtExecute = 0x17
	ComStmtClose   = 0x19

	// Response types
	OKPacket  = 0x00
	ERRPacket = 0xFF
	EOFPacket = 0xFE
)

// serverHandshake represents the server's initial handshake packet.
type serverHandshake struct {
	protocolVersion  uint8
	serverVersion    string
	connectionID     uint32
	authPluginData   []byte // 20 bytes
	capabilityFlags  uint32
	characterSet     uint8
	statusFlags      uint16
	authPluginName   string
}

// mysqlConn represents a MySQL protocol connection.
type mysqlConn struct {
	net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	seqID      uint8
	capability uint32
	salt       []byte
	cfg        *Config
	closed     bool
}

// newMySQLConn creates a new MySQL connection.
func newMySQLConn(conn net.Conn, cfg *Config) *mysqlConn {
	return &mysqlConn{
		Conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		capability: DefaultClientCapabilities,
		cfg:        cfg,
	}
}

// connect performs the MySQL handshake and authentication.
func (c *mysqlConn) connect() error {
	// Read server handshake
	hs, err := c.readHandshake()
	if err != nil {
		return fmt.Errorf("read handshake: %w", err)
	}

	c.salt = hs.authPluginData

	// Send authentication
	if err := c.sendAuth(hs); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// Read auth response
	packet, err := c.readPacket()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}

	if packet[0] == ERRPacket {
		return c.parseError(packet)
	}

	return nil
}

// readHandshake reads the server's initial handshake packet.
func (c *mysqlConn) readHandshake() (*serverHandshake, error) {
	packet, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	hs := &serverHandshake{}
	offset := 0

	// Protocol version
	hs.protocolVersion = packet[offset]
	offset++

	if hs.protocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", hs.protocolVersion)
	}

	// Server version (null-terminated)
	end := offset
	for end < len(packet) && packet[end] != 0 {
		end++
	}
	hs.serverVersion = string(packet[offset:end])
	offset = end + 1

	// Connection ID
	hs.connectionID = binary.LittleEndian.Uint32(packet[offset : offset+4])
	offset += 4

	// Auth plugin data part 1 (8 bytes)
	hs.authPluginData = make([]byte, 20)
	copy(hs.authPluginData[:8], packet[offset:offset+8])
	offset += 8

	// Filler
	offset++

	// Capability flags lower 2 bytes
	capLower := binary.LittleEndian.Uint16(packet[offset : offset+2])
	offset += 2

	// Character set
	hs.characterSet = packet[offset]
	offset++

	// Status flags
	hs.statusFlags = binary.LittleEndian.Uint16(packet[offset : offset+2])
	offset += 2

	// Capability flags upper 2 bytes
	capUpper := binary.LittleEndian.Uint16(packet[offset : offset+2])
	offset += 2

	hs.capabilityFlags = uint32(capLower) | (uint32(capUpper) << 16)

	// Auth plugin data length (if CLIENT_PLUGIN_AUTH)
	if hs.capabilityFlags&ClientPluginAuth != 0 {
		_ = packet[offset] // authDataLen - not used
	}
	offset++

	// Reserved (10 bytes)
	offset += 10

	// Auth plugin data part 2 (12 bytes)
	if hs.capabilityFlags&ClientSecureConn != 0 {
		copy(hs.authPluginData[8:], packet[offset:offset+12])
		offset += 12
	}

	// Auth plugin name (if CLIENT_PLUGIN_AUTH)
	if hs.capabilityFlags&ClientPluginAuth != 0 && offset < len(packet) {
		end = offset
		for end < len(packet) && packet[end] != 0 {
			end++
		}
		hs.authPluginName = string(packet[offset:end])
	}

	return hs, nil
}

// sendAuth sends the client authentication packet.
func (c *mysqlConn) sendAuth(hs *serverHandshake) error {
	var packet []byte

	// Capability flags
	capability := c.capability
	if c.cfg.DBName == "" {
		capability &^= ClientConnectWithDB
	}
	packet = append(packet, byte(capability), byte(capability>>8), byte(capability>>16), byte(capability>>24))

	// Max packet size
	packet = append(packet, 0xFF, 0xFF, 0xFF, 0x00)

	// Character set
	packet = append(packet, CharacterSetUTF8MB4)

	// Reserved (23 bytes)
	packet = append(packet, make([]byte, 23)...)

	// Username (null-terminated)
	packet = append(packet, c.cfg.User...)
	packet = append(packet, 0)

	// Auth response
	authResp := c.authPassword(c.cfg.Passwd, hs.authPluginData)
	if capability&ClientSecureConn != 0 {
		packet = append(packet, byte(len(authResp)))
		packet = append(packet, authResp...)
	} else {
		packet = append(packet, authResp...)
		packet = append(packet, 0)
	}

	// Database name (if CLIENT_CONNECT_WITH_DB)
	if c.cfg.DBName != "" && capability&ClientConnectWithDB != 0 {
		packet = append(packet, c.cfg.DBName...)
		packet = append(packet, 0)
	}

	// Auth plugin name
	if capability&ClientPluginAuth != 0 {
		packet = append(packet, "mysql_native_password"...)
		packet = append(packet, 0)
	}

	return c.writePacket(packet)
}

// authPassword computes the MySQL native password authentication response.
func (c *mysqlConn) authPassword(password string, salt []byte) []byte {
	if password == "" {
		return nil
	}

	// SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password))
	hash2 := sha1.Sum(hash1[:])

	// SHA1(salt + SHA1(SHA1(password)))
	combined := append(salt[:20], hash2[:]...)
	hash3 := sha1.Sum(combined)

	// XOR with hash1
	result := make([]byte, 20)
	for i := 0; i < 20; i++ {
		result[i] = hash1[i] ^ hash3[i]
	}

	return result
}

// query sends a query and returns the raw result.
func (c *mysqlConn) query(sql string) ([]byte, error) {
	// Send COM_QUERY
	packet := []byte{ComQuery}
	packet = append(packet, sql...)
	if err := c.writePacket(packet); err != nil {
		return nil, err
	}

	// Read response
	return c.readPacket()
}

// exec executes a query that doesn't return rows.
func (c *mysqlConn) exec(sql string) (affectedRows, lastInsertID int64, err error) {
	response, err := c.query(sql)
	if err != nil {
		return 0, 0, err
	}

	if response[0] == ERRPacket {
		return 0, 0, c.parseError(response)
	}

	if response[0] == OKPacket {
		affectedRows, lastInsertID, err = c.parseOKPacket(response)
		return affectedRows, lastInsertID, err
	}

	return 0, 0, nil
}

// parseOKPacket parses an OK packet and returns affected rows and last insert ID.
func (c *mysqlConn) parseOKPacket(packet []byte) (affectedRows, lastInsertID int64, err error) {
	offset := 1 // Skip OK byte

	// Affected rows (length-encoded)
	affectedRows, n := c.readLengthEncodedInt(packet[offset:])
	offset += n

	// Last insert ID (length-encoded)
	lastInsertID, n = c.readLengthEncodedInt(packet[offset:])
	offset += n

	return affectedRows, lastInsertID, nil
}

// parseError parses an error packet.
func (c *mysqlConn) parseError(packet []byte) error {
	if packet[0] != ERRPacket {
		return nil
	}

	offset := 1

	// Error code
	code := binary.LittleEndian.Uint16(packet[offset : offset+2])
	offset += 2

	// SQL state marker '#'
	if packet[offset] == '#' {
		offset++
	}

	// SQL state (5 bytes)
	state := ""
	if offset+5 <= len(packet) {
		state = string(packet[offset : offset+5])
		offset += 5
	}

	// Error message
	message := string(packet[offset:])

	return &mysqlError{
		code:    code,
		state:   state,
		message: message,
	}
}

// readPacket reads a MySQL packet.
func (c *mysqlConn) readPacket() ([]byte, error) {
	// Read header (4 bytes)
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return nil, err
	}

	// Parse length and sequence ID
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	c.seqID = header[3] + 1

	// Read payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

// writePacket writes a MySQL packet.
func (c *mysqlConn) writePacket(data []byte) error {
	// Write header
	header := make([]byte, 4)
	length := len(data)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = c.seqID
	c.seqID++

	if _, err := c.writer.Write(header); err != nil {
		return err
	}

	// Write payload
	if _, err := c.writer.Write(data); err != nil {
		return err
	}

	return c.writer.Flush()
}

// readLengthEncodedInt reads a length-encoded integer.
func (c *mysqlConn) readLengthEncodedInt(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	first := data[0]
	if first < 251 {
		return int64(first), 1
	}
	if first == 0xFC {
		return int64(binary.LittleEndian.Uint16(data[1:3])), 3
	}
	if first == 0xFD {
		return int64(binary.LittleEndian.Uint32(data[1:4]) & 0xFFFFFF), 4
	}
	if first == 0xFE {
		return int64(binary.LittleEndian.Uint64(data[1:9])), 9
	}
	return 0, 0
}

// writeLengthEncodedInt writes a length-encoded integer.
func (c *mysqlConn) writeLengthEncodedInt(n int64) []byte {
	if n < 251 {
		return []byte{byte(n)}
	}
	if n < 65536 {
		buf := make([]byte, 3)
		buf[0] = 0xFC
		binary.LittleEndian.PutUint16(buf[1:3], uint16(n))
		return buf
	}
	if n < 16777216 {
		buf := make([]byte, 4)
		buf[0] = 0xFD
		binary.LittleEndian.PutUint32(buf[1:4], uint32(n)&0xFFFFFF)
		return buf
	}
	buf := make([]byte, 9)
	buf[0] = 0xFE
	binary.LittleEndian.PutUint64(buf[1:9], uint64(n))
	return buf
}

// closeConnection closes the connection gracefully.
func (c *mysqlConn) closeConnection() error {
	if c.closed {
		return nil
	}
	c.closed = true

	// Send COM_QUIT
	c.writePacket([]byte{ComQuit})

	return c.Conn.Close()
}

// setDeadline sets the connection deadline from context or config.
func (c *mysqlConn) setDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		return c.Conn.SetDeadline(deadline)
	}
	return c.Conn.SetDeadline(time.Now().Add(c.cfg.ReadTimeout))
}
