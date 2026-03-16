package xxsql

import (
	"context"
	"database/sql/driver"
	"net"
	"time"
)

// Isolation levels matching sql.IsolationLevel
const (
	IsolationLevelDefault         driver.IsolationLevel = iota // Default
	IsolationLevelReadUncommitted                              // ReadUncommitted
	IsolationLevelReadCommitted                                // ReadCommitted
	IsolationLevelRepeatableRead                               // RepeatableRead
	IsolationLevelSerializable                                 // Serializable
)

// conn implements driver.Conn and extended interfaces.
type conn struct {
	mysqlConn *mysqlConn
	cfg       *Config
	closed    bool
	inTx      bool
}

// newConn creates a new connection.
func newConn(cfg *Config) (*conn, error) {
	// Dial network connection
	netConn, err := net.DialTimeout(cfg.Net, cfg.Addr, cfg.Timeout)
	if err != nil {
		return nil, err
	}

	mc := newMySQLConn(netConn, cfg)

	// Perform MySQL handshake
	if err := mc.connect(); err != nil {
		netConn.Close()
		return nil, err
	}

	return &conn{
		mysqlConn: mc,
		cfg:       cfg,
	}, nil
}

// Prepare creates a prepared statement.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	return newStmt(c, query), nil
}

// Close closes the connection.
func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.mysqlConn.closeConnection()
}

// Begin starts a transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with options.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Set deadline
	if deadline, ok := ctx.Deadline(); ok {
		c.mysqlConn.setDeadline(deadline)
	} else {
		c.mysqlConn.setDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}

	// Set isolation level if specified
	if opts.Isolation != IsolationLevelDefault {
		level := isolationLevelToString(opts.Isolation)
		if _, _, err := c.mysqlConn.exec("SET TRANSACTION ISOLATION LEVEL " + level); err != nil {
			return nil, err
		}
	}

	// Set read-only if specified
	if opts.ReadOnly {
		if _, _, err := c.mysqlConn.exec("SET TRANSACTION READ ONLY"); err != nil {
			return nil, err
		}
	}

	// Begin transaction
	if _, _, err := c.mysqlConn.exec("BEGIN"); err != nil {
		return nil, err
	}

	c.inTx = true
	return &tx{conn: c}, nil
}

// ExecContext executes a query that doesn't return rows.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Set deadline
	if deadline, ok := ctx.Deadline(); ok {
		c.mysqlConn.setDeadline(deadline)
	} else {
		c.mysqlConn.setDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}

	// Interpolate parameters
	sqlStr, err := interpolateQuery(query, args)
	if err != nil {
		return nil, err
	}

	// Execute
	affectedRows, lastInsertID, err := c.mysqlConn.exec(sqlStr)
	if err != nil {
		return nil, err
	}

	// Check context after operation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return &result{
		affectedRows: affectedRows,
		lastInsertID: lastInsertID,
	}, nil
}

// QueryContext executes a query that returns rows.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Set deadline
	if deadline, ok := ctx.Deadline(); ok {
		c.mysqlConn.setDeadline(deadline)
	} else {
		c.mysqlConn.setDeadline(time.Now().Add(c.cfg.ReadTimeout))
	}

	// Interpolate parameters
	sqlStr, err := interpolateQuery(query, args)
	if err != nil {
		return nil, err
	}

	// Execute query
	response, err := c.mysqlConn.query(sqlStr)
	if err != nil {
		return nil, err
	}

	// Parse result set
	rows, err := c.parseResultSet(response)
	if err != nil {
		return nil, err
	}

	// Check context after operation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return rows, nil
}

// Ping checks if the connection is alive.
func (c *conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Set deadline
	if deadline, ok := ctx.Deadline(); ok {
		c.mysqlConn.setDeadline(deadline)
	}

	// Send COM_PING
	if err := c.mysqlConn.writePacket([]byte{ComPing}); err != nil {
		return err
	}

	// Read response
	response, err := c.mysqlConn.readPacket()
	if err != nil {
		return err
	}

	if response[0] == ERRPacket {
		return c.mysqlConn.parseError(response)
	}

	return nil
}

// ResetSession resets the session state.
func (c *conn) ResetSession(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}

	// Reset sequence ID
	c.mysqlConn.seqID = 0

	// Clear transaction state
	if c.inTx {
		c.mysqlConn.exec("ROLLBACK")
		c.inTx = false
	}

	return nil
}

// parseResultSet parses a MySQL result set.
func (c *conn) parseResultSet(response []byte) (*rows, error) {
	if response[0] == ERRPacket {
		return nil, c.mysqlConn.parseError(response)
	}

	if response[0] == OKPacket {
		// Query returned no rows (e.g., INSERT)
		return &rows{
			columns: []string{},
			colTypes: []byte{},
			rowData:  nil,
		}, nil
	}

	// Parse column count
	colCount, _ := c.mysqlConn.readLengthEncodedInt(response)

	// Read column definitions
	columns := make([]string, colCount)
	colTypes := make([]byte, colCount)

	for i := int64(0); i < colCount; i++ {
		packet, err := c.mysqlConn.readPacket()
		if err != nil {
			return nil, err
		}

		name, colType := parseColumnDefinition(packet)
		columns[i] = name
		colTypes[i] = colType
	}

	// Read EOF packet (if not deprecated)
	if c.mysqlConn.capability&ClientDeprecateEOF == 0 {
		_, err := c.mysqlConn.readPacket()
		if err != nil {
			return nil, err
		}
	}

	// Read row data
	var rowData [][]byte
	for {
		packet, err := c.mysqlConn.readPacket()
		if err != nil {
			return nil, err
		}

		// Check for EOF/OK packet
		if packet[0] == EOFPacket || packet[0] == OKPacket {
			break
		}

		rowData = append(rowData, packet)
	}

	return &rows{
		mysqlConn: c.mysqlConn,
		columns:   columns,
		colTypes:  colTypes,
		rowData:   rowData,
	}, nil
}

// parseColumnDefinition parses a column definition packet.
func parseColumnDefinition(packet []byte) (name string, colType byte) {
	// Skip catalog (length-encoded)
	_, n := readLengthEncodedInt(packet)
	offset := n

	// Skip schema
	_, n = readLengthEncodedInt(packet[offset:])
	offset += n

	// Skip table
	_, n = readLengthEncodedInt(packet[offset:])
	offset += n

	// Skip org_table
	_, n = readLengthEncodedInt(packet[offset:])
	offset += n

	// Read name
	nameLen, n := readLengthEncodedInt(packet[offset:])
	offset += n
	name = string(packet[offset : offset+int(nameLen)])
	offset += int(nameLen)

	// Skip org_name
	_, n = readLengthEncodedInt(packet[offset:])
	offset += n

	// Skip length of fixed fields
	offset++

	// Skip charset (2 bytes)
	offset += 2

	// Skip column length (4 bytes)
	offset += 4

	// Column type
	colType = packet[offset]

	return name, colType
}

// readLengthEncodedInt reads a length-encoded integer from a byte slice.
func readLengthEncodedInt(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	first := data[0]
	if first < 251 {
		return int64(first), 1
	}
	if first == 0xFC && len(data) >= 3 {
		return int64(uint32(data[1]) | uint32(data[2])<<8), 3
	}
	if first == 0xFD && len(data) >= 4 {
		return int64(uint32(data[1]) | uint32(data[2])<<8 | uint32(data[3])<<16), 4
	}
	if first == 0xFE && len(data) >= 9 {
		return int64(uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16 | uint64(data[4])<<24 |
			uint64(data[5])<<32 | uint64(data[6])<<40 | uint64(data[7])<<48 | uint64(data[8])<<56), 9
	}
	return 0, 0
}

// isolationLevelToString converts a driver isolation level to MySQL string.
func isolationLevelToString(level driver.IsolationLevel) string {
	switch level {
	case IsolationLevelReadUncommitted:
		return "READ UNCOMMITTED"
	case IsolationLevelReadCommitted:
		return "READ COMMITTED"
	case IsolationLevelRepeatableRead:
		return "REPEATABLE READ"
	case IsolationLevelSerializable:
		return "SERIALIZABLE"
	default:
		return ""
	}
}
