# Phase 16: Go SQL Driver Design

## Overview

Implement a Go SQL driver for XxSql that implements the `database/sql/driver` interfaces, allowing Go applications to connect to XxSql using the standard `database/sql` package.

## Goals

- Provide a standard Go SQL driver for XxSql
- Support MySQL protocol for compatibility
- Implement full-featured driver interfaces
- Enable client-side prepared statements

## Non-Goals

- Server-side prepared statements (deferred to future phase)
- Connection pooling (handled by `database/sql` package)
- Named parameters support

## Package Structure

```
pkg/xxsql/
├── driver.go        # Driver registration, Connector implementation
├── conn.go          # Conn, ConnBeginTx, ExecerContext, QueryerContext, Pinger
├── stmt.go          # Stmt, StmtExecContext, StmtQueryContext
├── rows.go          # Rows, RowsNextResultSet
├── tx.go            # Tx isolation level support
├── dsn.go           # MySQL-style DSN parsing
├── protocol.go      # MySQL client protocol (handshake, auth, query)
├── errors.go        # Error type conversions
└── driver_test.go   # Integration tests using database/sql
```

**Import path:** `github.com/topxeq/xxsql/pkg/xxsql`

**Registration:** `sql.Register("xxsql", &driver{})`

## Connection String (DSN) Format

MySQL-style DSN with support for common parameters:

```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

### Examples

- `root@tcp(localhost:3306)/testdb`
- `admin:secret@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&timeout=10s`
- `user:pass@/dbname` (uses defaults: localhost:3306)

### Supported Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout` | 10s | Connection timeout |
| `readTimeout` | 30s | Read timeout |
| `writeTimeout` | 30s | Write timeout |
| `charset` | utf8mb4 | Character set |
| `collation` | utf8mb4_general_ci | Collation |
| `tls` | false | Enable TLS |
| `allowOldPasswords` | false | Allow old password auth |
| `maxAllowedPacket` | 4MB | Max packet size |

### Config Struct

```go
type Config struct {
    User     string
    Passwd   string
    Net      string      // "tcp"
    Addr     string      // "host:port"
    DBName   string
    Timeout  time.Duration
    // ... other params
}

func ParseDSN(dsn string) (*Config, error)
func (c *Config) FormatDSN() string
```

## Driver Interfaces

### Core Interfaces (Required)

| Interface | File | Description |
|-----------|------|-------------|
| `driver.Driver` | driver.go | Entry point, opens connections |
| `driver.Conn` | conn.go | Connection handle, prepares statements |
| `driver.Stmt` | stmt.go | Prepared statement, parameter binding |
| `driver.Rows` | rows.go | Result set iteration |
| `driver.Tx` | tx.go | Transaction management |

### Extended Interfaces (Full-Featured)

| Interface | File | Description |
|-----------|------|-------------|
| `driver.DriverContext` | driver.go | Open with context |
| `driver.Connector` | driver.go | Connection factory for `sql.OpenDB` |
| `driver.ConnBeginTx` | conn.go | Begin transaction with options |
| `driver.ExecerContext` | conn.go | Execute without preparing |
| `driver.QueryerContext` | conn.go | Query without preparing |
| `driver.Pinger` | conn.go | Health check (`db.Ping()`) |
| `driver.SessionResetter` | conn.go | Reset for connection reuse |
| `driver.StmtExecContext` | stmt.go | Execute with context |
| `driver.StmtQueryContext` | stmt.go | Query with context |
| `driver.RowsNextResultSet` | rows.go | Multi-result set support |

### Not Implemented

- `driver.NamedValueChecker` - named parameters
- `driver.ColumnConverter` - column type conversion
- `driver.Validator` - connection validation

## MySQL Client Protocol

### Connection Flow

1. TCP Connect to server
2. Read Server Handshake (greeting + auth salt)
3. Send Client Handshake (credentials + auth response)
4. Read OK/ERR packet
5. Ready for commands

### Key Types and Functions

```go
// protocol.go
type mysqlConn struct {
    net.Conn
    seqID      uint8
    capability uint32
    salt       []byte
}

func (c *mysqlConn) readHandshake() (*serverHandshake, error)
func (c *mysqlConn) sendAuth(user, password, database string) error
func (c *mysqlConn) query(sql string) (*mysqlRows, error)
func (c *mysqlConn) exec(sql string) (rowsAffected, lastInsertID int64, error)
func (c *mysqlConn) begin() error
func (c *mysqlConn) commit() error
func (c *mysqlConn) rollback() error
func (c *mysqlConn) readPacket() ([]byte, error)
func (c *mysqlConn) writePacket(data []byte) error
```

### Authentication

- MySQL native password (SHA1-based challenge-response)
- Matches `internal/mysql/mysql.go:MySQLAuthPassword` implementation

### Error Handling

```go
type mysqlError struct {
    code    uint16
    state   string
    message string
}

func (e *mysqlError) Error() string { return e.message }
func (e *mysqlError) Number() uint16 { return e.code }
```

### Error Code Mapping

Common MySQL error codes are mapped to Go error patterns:

| MySQL Code | SQL State | Description | Go Handling |
|------------|-----------|-------------|-------------|
| 1045 | 28000 | Access denied | Return error with message |
| 1062 | 23000 | Duplicate entry | Return error for unique constraint |
| 1064 | 42000 | Syntax error | Return parse error |
| 1146 | 42S02 | Table doesn't exist | Return error with table name |
| 1213 | 40001 | Deadlock | Return error, caller may retry |
| 2006 | HY000 | MySQL server has gone away | Close connection, return error |
| 2013 | HY000 | Lost connection | Close connection, return error |

```go
func (e *mysqlError) Is(target error) bool {
    switch target {
    case ErrDuplicateEntry:
        return e.code == 1062
    case ErrTableNotExist:
        return e.code == 1146
    case ErrDeadlock:
        return e.code == 1213
    }
    return false
}
```

## Client-Side Prepared Statements

Since XxSql doesn't have server-side prepared statements, the driver handles parameter interpolation client-side.

### Parameter Placeholder Syntax

- `?` - positional placeholder (MySQL style)
- Parameters are 1-indexed

### Example

```go
// User code
db.Query("SELECT * FROM users WHERE id = ? AND name = ?", 123, "Alice")

// Driver interpolates to
"SELECT * FROM users WHERE id = 123 AND name = 'Alice'"
```

### Type Handling

| Go Type | MySQL Representation |
|---------|---------------------|
| `int`, `int32`, `int64` | Decimal string |
| `float32`, `float64` | Decimal string |
| `string` | Quoted, escaped string |
| `[]byte` | Hex literal `0x...` |
| `bool` | `1` or `0` |
| `time.Time` | `'2006-01-02 15:04:05.999999'` |
| `nil` | `NULL` |

### String Escaping

Escape sequences handled:
- `\0` - NUL byte
- `\n` - Newline
- `\r` - Carriage return
- `\\` - Backslash
- `\'` - Single quote
- `\"` - Double quote
- `\Z` - ASCII 26 (Ctrl+Z)

### Stmt Implementation

```go
type stmt struct {
    conn     *conn
    query    string
    paramLen int
}

func (s *stmt) Close() error
func (s *stmt) NumInput() int
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error)
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error)
func (s *stmt) interpolate(args []driver.NamedValue) string
```

### NamedValue Handling

The `interpolate()` function receives `[]driver.NamedValue` from `database/sql`. Since named parameters are not supported, only the `Ordinal` and `Value` fields are used:

```go
func (s *stmt) interpolate(args []driver.NamedValue) string {
    values := make([]driver.Value, len(args))
    for _, arg := range args {
        values[arg.Ordinal-1] = arg.Value // Ordinal is 1-indexed
    }
    return interpolateValues(s.query, values)
}
```

## Result Implementation

The `driver.Result` interface is implemented by parsing the MySQL OK packet:

```go
type result struct {
    affectedRows int64
    lastInsertID int64
}

func (r *result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r *result) RowsAffected() (int64, error) { return r.affectedRows, nil }
```

### OK Packet Parsing

After executing INSERT/UPDATE/DELETE, the server returns an OK packet:

```
[0x00] affected_rows (length-encoded)
       last_insert_id (length-encoded)
       status_flags (2 bytes)
       warnings (2 bytes)
```

The driver extracts `affected_rows` and `last_insert_id` from this packet.

## Rows Implementation

The `driver.Rows` interface requires three methods:

```go
type rows struct {
    conn     *mysqlConn
    columns  []string
    colTypes []byte      // MySQL column type codes
    rowData  [][]byte    // Buffered row data
    pos      int
}

// Columns returns column names from column definition packets
func (r *rows) Columns() []string {
    return r.columns
}

// Close releases resources
func (r *rows) Close() error {
    r.rowData = nil
    return nil
}

// Next populates dest with values from the next row
func (r *rows) Next(dest []driver.Value) error {
    if r.pos >= len(r.rowData) {
        return io.EOF
    }

    data := r.rowData[r.pos]
    r.pos++

    // Parse length-encoded strings/values from row data
    return r.parseRow(data, dest)
}
```

### Column Type to Go Type Conversion

MySQL column types are mapped to Go types in `Next()`:

| MySQL Type Code | MySQL Type | Go Type |
|-----------------|------------|---------|
| 0x01 | TINY | int64 |
| 0x02 | SHORT | int64 |
| 0x03 | LONG | int64 |
| 0x04 | FLOAT | float64 |
| 0x05 | DOUBLE | float64 |
| 0x06 | NULL | nil |
| 0x07 | TIMESTAMP | time.Time |
| 0x08 | LONGLONG | int64 |
| 0x09 | INT24 | int64 |
| 0x0a | DATE | time.Time |
| 0x0b | TIME | time.Duration |
| 0x0c | DATETIME | time.Time |
| 0x0f | VARCHAR | string |
| 0xfd | VAR_STRING | string |
| 0xfe | BLOB | []byte |

### NULL Value Handling

MySQL uses `0xFB` prefix in row data to indicate NULL:

```go
func (r *rows) parseRow(data []byte, dest []driver.Value) error {
    offset := 0
    for i := range dest {
        if data[offset] == 0xFB {
            dest[i] = nil
            offset++
            continue
        }
        // Parse length-encoded value
        length, n := readLengthEncodedInt(data[offset:])
        offset += n
        dest[i] = convertValue(data[offset:offset+int(length)], r.colTypes[i])
        offset += int(length)
    }
    return nil
}
```

## Context Cancellation

All context-aware methods check for cancellation and set deadlines:

```go
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
    // Check for cancellation before starting
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    // Set deadline from context
    if deadline, ok := ctx.Deadline(); ok {
        c.conn.SetDeadline(deadline)
    } else {
        c.conn.SetDeadline(time.Now().Add(c.cfg.ReadTimeout))
    }

    // Execute query
    result, err := c.exec(interpolateValues(query, args))

    // Check for cancellation after operation
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
        return result, err
    }
}
```

### Deadline Propagation

- `ctx.Deadline()` returns the deadline if set
- `net.Conn.SetDeadline()` applies to both read and write operations
- On cancellation, the connection may be in an inconsistent state and should be closed

## Transaction Support

### Isolation Levels

The driver maps `driver.TxOptions.Isolation` to MySQL `SET TRANSACTION` statements:

| Go Level | MySQL Level | Statement |
|----------|-------------|-----------|
| `driver.Default` | Use server default | None |
| `driver.ReadUncommitted` | READ UNCOMMITTED | `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED` |
| `driver.ReadCommitted` | READ COMMITTED | `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| `driver.RepeatableRead` | REPEATABLE READ | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` |
| `driver.Serializable` | SERIALIZABLE | `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` |

### BeginTx Implementation

```go
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
    // Set isolation level if specified
    if opts.Isolation != driver.Default {
        level := isolationLevelToString(opts.Isolation)
        if _, err := c.exec("SET TRANSACTION ISOLATION LEVEL " + level); err != nil {
            return nil, err
        }
    }

    // Set read-only if specified
    if opts.ReadOnly {
        if _, err := c.exec("SET TRANSACTION READ ONLY"); err != nil {
            return nil, err
        }
    }

    // Begin transaction
    if _, err := c.exec("BEGIN"); err != nil {
        return nil, err
    }

    return &tx{conn: c}, nil
}
```

### Transaction Lifecycle

```go
type tx struct {
    conn   *conn
    closed bool
}

func (t *tx) Commit() error {
    if t.closed {
        return ErrTxDone
    }
    t.closed = true
    _, err := t.conn.exec("COMMIT")
    return err
}

func (t *tx) Rollback() error {
    if t.closed {
        return ErrTxDone
    }
    t.closed = true
    _, err := t.conn.exec("ROLLBACK")
    return err
}
```

## Connection Lifecycle

### Conn.Close()

```go
func (c *conn) Close() error {
    if c.closed {
        return nil
    }
    c.closed = true

    // Send COM_QUIT packet
    c.writePacket([]byte{COM_QUIT})

    return c.Conn.Close()
}
```

### SessionResetter

The `ResetSession` method is called by `database/sql` before reusing a connection from the pool:

```go
func (c *conn) ResetSession(ctx context.Context) error {
    if c.closed {
        return driver.ErrBadConn
    }

    // Reset sequence ID for new session
    c.seqID = 0

    // Clear any transaction state
    // If there was an active transaction, roll it back
    if c.inTx {
        c.exec("ROLLBACK")
        c.inTx = false
    }

    return nil
}
```

### Bad Connection Handling

The driver returns `driver.ErrBadConn` when:
- Connection is already closed
- Network read/write fails
- Context deadline exceeded during I/O
- Server returns unexpected packet format

This signals `database/sql` to discard the connection and create a new one.
```

## Testing Strategy

### Unit Tests

- DSN parsing validation
- Parameter interpolation correctness
- String escaping edge cases
- Error handling

### Integration Tests

```go
// +build integration

func TestConnection(t *testing.T) {
    db := sql.Open("xxsql", "root@tcp(localhost:3306)/test")
    defer db.Close()

    err := db.Ping()
    if err != nil { t.Fatal(err) }
}

func TestCRUD(t *testing.T) {
    // CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
}

func TestTransactions(t *testing.T) {
    // BEGIN, COMMIT, ROLLBACK
}
```

### Running Tests

```bash
# Unit tests only
go test ./pkg/xxsql/...

# Integration tests (requires running server)
go test ./pkg/xxsql/... -tags=integration
```

## Usage Example

```go
package main

import (
    "database/sql"
    "fmt"

    _ "github.com/topxeq/xxsql/pkg/xxsql"
)

func main() {
    // Connect
    db, err := sql.Open("xxsql", "root:password@tcp(localhost:3306)/mydb")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Ping
    if err := db.Ping(); err != nil {
        panic(err)
    }

    // Query
    rows, err := db.Query("SELECT id, name FROM users WHERE id > ?", 10)
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            panic(err)
        }
        fmt.Printf("%d: %s\n", id, name)
    }

    // Insert
    result, err := db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
    if err != nil {
        panic(err)
    }

    lastID, _ := result.LastInsertId()
    fmt.Printf("Inserted ID: %d\n", lastID)
}
```

## Implementation Order

1. **dsn.go** - DSN parsing
2. **protocol.go** - MySQL client protocol
3. **errors.go** - Error types
4. **conn.go** - Connection implementation
5. **rows.go** - Result set handling
6. **stmt.go** - Prepared statements
7. **tx.go** - Transaction support
8. **driver.go** - Driver registration
9. **driver_test.go** - Unit and integration tests

## Dependencies

- `database/sql/driver` - Standard library interfaces
- `crypto/sha1` - MySQL native password auth
- `encoding/binary` - Binary protocol encoding
- `net` - TCP connections
- `time` - Timeouts and time handling

No external dependencies required.
