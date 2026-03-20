package xxsql

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// privateConn implements driver.Conn using XxSql private protocol.
type privateConn struct {
	conn      net.Conn
	cfg       *Config
	closed    bool
	inTx      bool
	seqID     uint32
	mu        sync.Mutex
	connected bool
}

// newPrivateConn creates a new connection using private protocol.
func newPrivateConn(cfg *Config) (*privateConn, error) {
	// Dial network connection
	netConn, err := net.DialTimeout(cfg.Net, cfg.Addr, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("connection failed: %w", err)
	}

	pc := &privateConn{
		conn:  netConn,
		cfg:   cfg,
		seqID: 1,
	}

	// Perform handshake and authentication
	if err := pc.handshake(); err != nil {
		netConn.Close()
		return nil, err
	}

	pc.connected = true
	return pc, nil
}

// handshake performs the private protocol handshake and authentication.
func (c *privateConn) handshake() error {
	// Send handshake request
	handshakeReq := make([]byte, 2+2+4+2)
	offset := 0

	// Protocol version (v3)
	binary.BigEndian.PutUint16(handshakeReq[offset:], 3)
	offset += 2

	// Client version "1.0"
	clientVer := "1.0"
	binary.BigEndian.PutUint16(handshakeReq[offset:], uint16(len(clientVer)))
	offset += 2
	copy(handshakeReq[offset:], clientVer)
	offset += len(clientVer)

	// Client info "XxSql Go Driver"
	clientInfo := "XxSql Go Driver"
	handshakeReq[offset] = byte(len(clientInfo))
	offset += 1
	copy(handshakeReq[offset:], clientInfo)
	offset += len(clientInfo)

	// Extensions count = 0
	binary.BigEndian.PutUint16(handshakeReq[offset:], 0)

	// Build and send message
	msg, err := c.buildMessage(msgHandshakeRequest, handshakeReq)
	if err != nil {
		return err
	}

	if err := c.sendMessage(msg); err != nil {
		return fmt.Errorf("send handshake failed: %w", err)
	}

	// Read handshake response
	resp, err := c.readMessage()
	if err != nil {
		return fmt.Errorf("read handshake response failed: %w", err)
	}

	if resp.Header.Type != msgHandshakeResponse {
		return fmt.Errorf("unexpected response type: %d", resp.Header.Type)
	}

	// Parse handshake response
	hsResp, err := c.parseHandshakeResponse(resp.Payload)
	if err != nil {
		return err
	}

	if !hsResp.Supported {
		return fmt.Errorf("protocol version not supported")
	}

	// Send authentication request
	authReq := c.buildAuthRequest()
	msg, err = c.buildMessage(msgAuthRequest, authReq)
	if err != nil {
		return err
	}

	if err := c.sendMessage(msg); err != nil {
		return fmt.Errorf("send auth failed: %w", err)
	}

	// Read auth response
	resp, err = c.readMessage()
	if err != nil {
		return fmt.Errorf("read auth response failed: %w", err)
	}

	if resp.Header.Type == msgError {
		errMsg := string(resp.Payload[1:]) // Skip error code
		return fmt.Errorf("authentication failed: %s", errMsg)
	}

	if resp.Header.Type != msgAuthResponse {
		return fmt.Errorf("unexpected auth response type: %d", resp.Header.Type)
	}

	// Parse auth response
	authResp, err := c.parseAuthResponse(resp.Payload)
	if err != nil {
		return err
	}

	if !authResp.Success {
		return fmt.Errorf("authentication failed: %s", authResp.Message)
	}

	return nil
}

// buildAuthRequest builds the authentication request payload.
func (c *privateConn) buildAuthRequest() []byte {
	// Calculate size
	userLen := len(c.cfg.User)
	passLen := len(c.cfg.Passwd)
	dbLen := len(c.cfg.DBName)

	buf := make([]byte, 1+2+userLen+2+passLen+2+dbLen)
	offset := 0

	// Auth method (0 = plain text for now)
	buf[offset] = 0
	offset += 1

	// Username
	binary.BigEndian.PutUint16(buf[offset:], uint16(userLen))
	offset += 2
	copy(buf[offset:], c.cfg.User)
	offset += userLen

	// Password
	binary.BigEndian.PutUint16(buf[offset:], uint16(passLen))
	offset += 2
	copy(buf[offset:], c.cfg.Passwd)
	offset += passLen

	// Database
	binary.BigEndian.PutUint16(buf[offset:], uint16(dbLen))
	offset += 2
	copy(buf[offset:], c.cfg.DBName)

	return buf
}

// handshakeResponse represents parsed handshake response.
type handshakeResponse struct {
	ProtocolVersion uint16
	ServerVersion   string
	Supported       bool
}

// parseHandshakeResponse parses the handshake response payload.
func (c *privateConn) parseHandshakeResponse(payload []byte) (*handshakeResponse, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("handshake response too short")
	}

	resp := &handshakeResponse{}
	offset := 0

	// Protocol version
	resp.ProtocolVersion = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	// Server version length
	verLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	if offset+verLen > len(payload) {
		return nil, fmt.Errorf("invalid server version length")
	}
	resp.ServerVersion = string(payload[offset : offset+verLen])
	offset += verLen

	// Supported flag
	if offset >= len(payload) {
		return nil, fmt.Errorf("missing supported flag")
	}
	resp.Supported = payload[offset] == 1

	return resp, nil
}

// authResponse represents parsed auth response.
type authResponse struct {
	Success bool
	Message string
}

// parseAuthResponse parses the auth response payload.
func (c *privateConn) parseAuthResponse(payload []byte) (*authResponse, error) {
	if len(payload) < 1 {
		return nil, fmt.Errorf("auth response too short")
	}

	resp := &authResponse{}
	resp.Success = payload[0] == 1

	if len(payload) > 1 {
		// Read message length
		if len(payload) >= 3 {
			msgLen := int(binary.BigEndian.Uint16(payload[1:3]))
			if 3+msgLen <= len(payload) {
				resp.Message = string(payload[3 : 3+msgLen])
			}
		}
	}

	return resp, nil
}

// Prepare creates a prepared statement.
func (c *privateConn) Prepare(query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	return &privateStmt{conn: c, query: query}, nil
}

// Close closes the connection.
func (c *privateConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Send close message
	msg, _ := c.buildMessage(msgClose, nil)
	c.sendMessage(msg)

	return c.conn.Close()
}

// Begin starts a transaction.
func (c *privateConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with context and options.
func (c *privateConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	if c.inTx {
		return nil, fmt.Errorf("already in transaction")
	}

	c.inTx = true
	return &privateTx{conn: c}, nil
}

// ExecContext executes a query without returning rows.
func (c *privateConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.execute(query, args)
}

// QueryContext executes a query that returns rows.
func (c *privateConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.query(query, args)
}

// execute executes a query and returns the result.
func (c *privateConn) execute(query string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Build query request
	queryReq := c.buildQueryRequest(query, args)
	msg, err := c.buildMessage(msgQueryRequest, queryReq)
	if err != nil {
		return nil, err
	}

	if err := c.sendMessage(msg); err != nil {
		return nil, fmt.Errorf("send query failed: %w", err)
	}

	// Read response
	resp, err := c.readMessage()
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	if resp.Header.Type == msgError {
		errMsg := string(resp.Payload[1:])
		return nil, fmt.Errorf("query error: %s", errMsg)
	}

	// Parse response
	return c.parseQueryResult(resp.Payload)
}

// query executes a query and returns rows.
func (c *privateConn) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Build query request
	queryReq := c.buildQueryRequest(query, args)
	msg, err := c.buildMessage(msgQueryRequest, queryReq)
	if err != nil {
		return nil, err
	}

	if err := c.sendMessage(msg); err != nil {
		return nil, fmt.Errorf("send query failed: %w", err)
	}

	// Read response
	resp, err := c.readMessage()
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	if resp.Header.Type == msgError {
		errMsg := string(resp.Payload[1:])
		return nil, fmt.Errorf("query error: %s", errMsg)
	}

	// Parse response into rows
	return c.parseRows(resp.Payload)
}

// buildQueryRequest builds the query request payload.
func (c *privateConn) buildQueryRequest(query string, args []driver.NamedValue) []byte {
	// Simple implementation - just send the query string
	// TODO: Add parameter support
	buf := make([]byte, 2+len(query))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(query)))
	copy(buf[2:], query)
	return buf
}

// parseQueryResult parses the query result payload.
func (c *privateConn) parseQueryResult(payload []byte) (driver.Result, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("result payload too short")
	}

	rowsAffected := int64(binary.BigEndian.Uint32(payload[0:4]))
	lastInsertID := int64(binary.BigEndian.Uint32(payload[4:8]))

	return &privateResult{
		rowsAffected: rowsAffected,
		lastInsertID: lastInsertID,
	}, nil
}

// parseRows parses the query response into rows.
func (c *privateConn) parseRows(payload []byte) (driver.Rows, error) {
	// Parse column count
	if len(payload) < 2 {
		return nil, fmt.Errorf("rows payload too short")
	}

	colCount := int(binary.BigEndian.Uint16(payload[0:2]))
	offset := 2

	// Parse column names
	columns := make([]string, colCount)
	for i := 0; i < colCount; i++ {
		if offset+2 > len(payload) {
			return nil, fmt.Errorf("column %d name truncated", i)
		}
		colLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if offset+colLen > len(payload) {
			return nil, fmt.Errorf("column %d name data truncated", i)
		}
		columns[i] = string(payload[offset : offset+colLen])
		offset += colLen
	}

	// Parse row count
	if offset+2 > len(payload) {
		return nil, fmt.Errorf("row count truncated")
	}
	rowCount := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2

	// Parse rows
	rows := make([][]interface{}, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = make([]interface{}, colCount)
		for j := 0; j < colCount; j++ {
			if offset+1 > len(payload) {
				return nil, fmt.Errorf("row %d col %d type truncated", i, j)
			}
			valType := payload[offset]
			offset += 1

			switch valType {
			case 0: // NULL
				rows[i][j] = nil
			case 1: // int64
				if offset+8 > len(payload) {
					return nil, fmt.Errorf("row %d col %d int truncated", i, j)
				}
				rows[i][j] = int64(binary.BigEndian.Uint64(payload[offset : offset+8]))
				offset += 8
			case 2: // float64
				if offset+8 > len(payload) {
					return nil, fmt.Errorf("row %d col %d float truncated", i, j)
				}
				rows[i][j] = float64(binary.BigEndian.Uint64(payload[offset : offset+8]))
				offset += 8
			case 3: // string
				if offset+2 > len(payload) {
					return nil, fmt.Errorf("row %d col %d string len truncated", i, j)
				}
				strLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
				offset += 2
				if offset+strLen > len(payload) {
					return nil, fmt.Errorf("row %d col %d string truncated", i, j)
				}
				rows[i][j] = string(payload[offset : offset+strLen])
				offset += strLen
			case 4: // bool
				if offset+1 > len(payload) {
					return nil, fmt.Errorf("row %d col %d bool truncated", i, j)
				}
				rows[i][j] = payload[offset] == 1
				offset += 1
			default:
				return nil, fmt.Errorf("unknown value type %d at row %d col %d", valType, i, j)
			}
		}
	}

	return &privateRows{
		columns: columns,
		rows:    rows,
	}, nil
}

// Ping verifies the connection is still valid.
func (c *privateConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// Send ping
	msg, _ := c.buildMessage(msgPing, nil)
	if err := c.sendMessage(msg); err != nil {
		return err
	}

	// Read pong
	resp, err := c.readMessage()
	if err != nil {
		return err
	}

	if resp.Header.Type != msgPong {
		return fmt.Errorf("expected pong, got %d", resp.Header.Type)
	}

	return nil
}

// CheckNamedValue implements driver.NamedValueChecker.
func (c *privateConn) CheckNamedValue(nv *driver.NamedValue) error {
	return nil
}

// ResetSession implements driver.SessionResetter.
func (c *privateConn) ResetSession(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	return nil
}

// Message types
const (
	msgHandshakeRequest  byte = 0x01
	msgHandshakeResponse byte = 0x02
	msgAuthRequest       byte = 0x03
	msgAuthResponse      byte = 0x04
	msgQueryRequest      byte = 0x05
	msgQueryResponse     byte = 0x06
	msgPing              byte = 0x07
	msgPong              byte = 0x08
	msgError             byte = 0x09
	msgClose             byte = 0x0A
)

// Header size: Magic(4) + Length(4) + Type(1) + SeqID(4) = 13 bytes
const headerSize = 13

// magicPrivate is the protocol magic bytes
var magicPrivate = []byte("XXSQ")

// buildMessage builds a complete message with header.
func (c *privateConn) buildMessage(msgType byte, payload []byte) ([]byte, error) {
	payloadLen := 0
	if payload != nil {
		payloadLen = len(payload)
	}

	totalLen := headerSize + payloadLen
	msg := make([]byte, totalLen)

	// Magic
	copy(msg[0:4], magicPrivate)

	// Length (total message length including header)
	binary.BigEndian.PutUint32(msg[4:8], uint32(totalLen))

	// Type
	msg[8] = msgType

	// Sequence ID
	binary.BigEndian.PutUint32(msg[9:13], c.seqID)
	c.seqID++

	// Payload
	if payload != nil {
		copy(msg[13:], payload)
	}

	return msg, nil
}

// sendMessage sends a message to the server.
func (c *privateConn) sendMessage(msg []byte) error {
	if c.cfg.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}
	_, err := c.conn.Write(msg)
	return err
}

// message represents a received message.
type message struct {
	Header  messageHeader
	Payload []byte
}

// messageHeader represents a message header.
type messageHeader struct {
	Length uint32
	Type   byte
	SeqID  uint32
}

// readMessage reads a message from the server.
func (c *privateConn) readMessage() (*message, error) {
	if c.cfg.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
	}

	// Read header
	headerBuf := make([]byte, headerSize)
	_, err := readFull(c.conn, headerBuf)
	if err != nil {
		return nil, err
	}

	// Validate magic
	if string(headerBuf[0:4]) != "XXSQ" {
		return nil, fmt.Errorf("invalid magic: %q", string(headerBuf[0:4]))
	}

	// Parse header
	length := binary.BigEndian.Uint32(headerBuf[4:8])
	msgType := headerBuf[8]
	seqID := binary.BigEndian.Uint32(headerBuf[9:13])

	// Read payload
	var payload []byte
	if length > uint32(headerSize) {
		payloadLen := length - uint32(headerSize)
		payload = make([]byte, payloadLen)
		_, err = readFull(c.conn, payload)
		if err != nil {
			return nil, fmt.Errorf("read payload failed: %w", err)
		}
	}

	return &message{
		Header: messageHeader{
			Length: length,
			Type:   msgType,
			SeqID:  seqID,
		},
		Payload: payload,
	}, nil
}

// readFull reads exactly len(buf) bytes from the reader.
func readFull(r net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// privateResult implements driver.Result.
type privateResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *privateResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *privateResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// privateRows implements driver.Rows.
type privateRows struct {
	columns []string
	rows    [][]interface{}
	rowIdx  int
}

func (r *privateRows) Columns() []string {
	return r.columns
}

func (r *privateRows) Close() error {
	return nil
}

func (r *privateRows) Next(dest []driver.Value) error {
	if r.rowIdx >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.rowIdx]
	r.rowIdx++

	for i, val := range row {
		if i < len(dest) {
			dest[i] = val
		}
	}

	return nil
}

// privateStmt implements driver.Stmt.
type privateStmt struct {
	conn  *privateConn
	query string
}

func (s *privateStmt) Close() error {
	return nil
}

func (s *privateStmt) NumInput() int {
	return -1 // Let the driver handle it
}

func (s *privateStmt) Exec(args []driver.Value) (driver.Result, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{Value: arg}
	}
	return s.conn.execute(s.query, namedArgs)
}

func (s *privateStmt) Query(args []driver.Value) (driver.Rows, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{Value: arg}
	}
	return s.conn.query(s.query, namedArgs)
}

// privateTx implements driver.Tx.
type privateTx struct {
	conn *privateConn
}

func (t *privateTx) Commit() error {
	t.conn.inTx = false
	return nil
}

func (t *privateTx) Rollback() error {
	t.conn.inTx = false
	return nil
}