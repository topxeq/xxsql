# Phase 16: Go SQL Driver Implementation Plan

## Overview

Implement a Go SQL driver for XxSql following the design specification in `docs/superpowers/specs/2026-03-16-go-sql-driver-design.md`.

## Prerequisites

- XxSql server running with MySQL protocol support
- Go 1.21+ installed
- Understanding of `database/sql/driver` interfaces

## Task Breakdown

### Task 1: Create Package Structure and DSN Parser

**File:** `pkg/xxsql/dsn.go`

**Description:** Create the package directory structure and implement MySQL-style DSN parsing.

**Details:**
- Create `pkg/xxsql/` directory
- Define `Config` struct with all connection parameters
- Implement `ParseDSN(dsn string) (*Config, error)` function
- Handle formats:
  - `user@tcp(host:port)/dbname`
  - `user:pass@tcp(host:port)/dbname?param=value`
  - `user:pass@/dbname` (defaults to localhost:3306)
- Parse query parameters: timeout, readTimeout, writeTimeout, charset, collation, tls, maxAllowedPacket
- Implement `FormatDSN() string` for Config

**Acceptance Criteria:**
- Parse all valid DSN formats
- Return error for invalid DSN
- Default values applied correctly
- Unit tests for DSN parsing

**Estimated Lines:** ~150

---

### Task 2: Implement MySQL Client Protocol

**File:** `pkg/xxsql/protocol.go`

**Description:** Implement MySQL client-side protocol for communicating with XxSql server.

**Details:**
- Define `mysqlConn` struct embedding `net.Conn`
- Implement packet I/O:
  - `readPacket() ([]byte, error)` - read MySQL packet with length prefix and sequence ID
  - `writePacket(data []byte) error` - write MySQL packet
- Implement handshake:
  - `readHandshake() (*serverHandshake, error)` - parse server greeting
  - `sendAuth(user, password, database string) error` - send client handshake with auth response
- Implement authentication:
  - MySQL native password (SHA1 challenge-response)
  - `authPassword(password, salt []byte) []byte` - compute auth response
- Implement commands:
  - `query(sql string) ([]byte, error)` - send COM_QUERY, return raw result
  - `exec(sql string) (affectedRows, lastInsertID int64, error)` - for INSERT/UPDATE/DELETE
- Define constants for:
  - Protocol version, capabilities, character sets
  - Command types (COM_QUERY, COM_QUIT, COM_PING, etc.)
  - Packet types (OK, ERR, EOF)

**Acceptance Criteria:**
- Successfully handshake with XxSql server
- Authenticate using MySQL native password
- Send and receive MySQL packets correctly
- Handle length-encoded integers and strings

**Estimated Lines:** ~350

---

### Task 3: Implement Error Types

**File:** `pkg/xxsql/errors.go`

**Description:** Define error types and MySQL error code handling.

**Details:**
- Define `mysqlError` struct with code, state, message
- Implement `Error()`, `Number()`, `SQLState()` methods
- Implement `Is(target error) bool` for error comparison
- Define sentinel errors:
  - `ErrBadConn` - connection is invalid
  - `ErrTxDone` - transaction already committed/rolled back
  - `ErrDuplicateEntry` - unique constraint violation
  - `ErrTableNotExist` - table not found
- Implement `parseErrorPacket(data []byte) *mysqlError`

**Acceptance Criteria:**
- Parse MySQL error packets correctly
- Support `errors.Is()` for common errors
- Return appropriate error messages

**Estimated Lines:** ~80

---

### Task 4: Implement Connection

**File:** `pkg/xxsql/conn.go`

**Description:** Implement the `driver.Conn` interface and all extended interfaces.

**Details:**
- Define `conn` struct:
  - `*mysqlConn` - protocol handler
  - `*Config` - connection configuration
  - `closed bool` - connection state
  - `inTx bool` - transaction state
- Implement required interface:
  - `Prepare(query string) (driver.Stmt, error)` - create prepared statement
  - `Close() error` - close connection
  - `Begin() (driver.Tx, error)` - begin transaction (deprecated but required)
- Implement extended interfaces:
  - `BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error)` - begin with options
  - `ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)` - execute directly
  - `QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)` - query directly
  - `Ping(ctx context.Context) error` - health check
  - `ResetSession(ctx context.Context) error` - reset for connection reuse
- Implement helper methods:
  - `exec(sql string) (affectedRows, lastInsertID int64, error)` - internal execute
  - `query(sql string) (*rows, error)` - internal query
- Handle context cancellation and deadlines

**Acceptance Criteria:**
- All interface methods implemented
- Context cancellation works
- Connection lifecycle managed correctly
- Ping returns nil for healthy connection

**Estimated Lines:** ~200

---

### Task 5: Implement Result Set Handling

**File:** `pkg/xxsql/rows.go`

**Description:** Implement the `driver.Rows` interface for result set iteration.

**Details:**
- Define `rows` struct:
  - `*mysqlConn` - protocol handler
  - `columns []string` - column names
  - `colTypes []byte` - MySQL type codes
  - `rowData [][]byte` - buffered row data
  - `pos int` - current position
- Implement required interface:
  - `Columns() []string` - return column names
  - `Close() error` - release resources
  - `Next(dest []driver.Value) error` - populate next row
- Implement `HasNextResultSet() bool` and `NextResultSet() error` for multi-result sets
- Implement helper functions:
  - `parseColumnDefinitions(data []byte, count int) ([]string, []byte, error)`
  - `parseRow(data []byte, dest []driver.Value, colTypes []byte) error`
  - `convertValue(data []byte, colType byte) driver.Value`
- Handle NULL values (0xFB prefix)
- Convert MySQL types to Go types per spec table

**Acceptance Criteria:**
- Columns() returns correct names
- Next() populates dest with correct values
- NULL values handled correctly
- Type conversions work for all supported types
- Returns io.EOF when no more rows

**Estimated Lines:** ~180

---

### Task 6: Implement Prepared Statements

**File:** `pkg/xxsql/stmt.go`

**Description:** Implement client-side prepared statements with parameter interpolation.

**Details:**
- Define `stmt` struct:
  - `*conn` - parent connection
  - `query string` - original query with `?` placeholders
  - `paramLen int` - number of parameters
- Implement required interface:
  - `Close() error` - no-op for client-side
  - `NumInput() int` - return parameter count
  - `Exec(args []driver.Value) (driver.Result, error)` - deprecated but required
  - `Query(args []driver.Value) (driver.Rows, error)` - deprecated but required
- Implement extended interfaces:
  - `ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error)`
  - `QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error)`
- Implement interpolation:
  - `interpolate(args []driver.NamedValue) string` - build final SQL
  - `escapeString(s string) string` - escape special characters
  - `formatValue(v driver.Value) string` - convert Go value to SQL literal
- Count `?` placeholders in query

**Acceptance Criteria:**
- Parameter interpolation works correctly
- All Go types formatted correctly
- String escaping prevents SQL injection
- Returns error for wrong parameter count

**Estimated Lines:** ~150

---

### Task 7: Implement Transaction Support

**File:** `pkg/xxsql/tx.go`

**Description:** Implement transaction management with isolation level support.

**Details:**
- Define `tx` struct:
  - `*conn` - parent connection
  - `closed bool` - transaction state
- Implement `driver.Tx` interface:
  - `Commit() error` - send COMMIT
  - `Rollback() error` - send ROLLBACK
- Implement isolation level mapping:
  - `isolationLevelToString(level driver.IsolationLevel) string`
- Prevent double-commit/rollback with `ErrTxDone`
- Update `conn.inTx` state

**Acceptance Criteria:**
- Commit sends COMMIT and returns nil on success
- Rollback sends ROLLBACK and returns nil on success
- Double-commit/rollback returns ErrTxDone
- Isolation levels mapped correctly

**Estimated Lines:** ~60

---

### Task 8: Implement Driver Registration

**File:** `pkg/xxsql/driver.go`

**Description:** Implement driver registration and connector.

**Details:**
- Define `driver` struct (empty, uses functional pattern)
- Implement `driver.Driver` interface:
  - `Open(dsn string) (driver.Conn, error)` - parse DSN and connect
- Implement `driver.DriverContext` interface:
  - `OpenConnector(dsn string) (driver.Connector, error)`
- Define `connector` struct:
  - `dsn string`
  - `*Config`
- Implement `driver.Connector` interface:
  - `Connect(ctx context.Context) (driver.Conn, error)`
  - `Driver() driver.Driver`
- Register driver in `init()`:
  - `sql.Register("xxsql", &driver{})`
- Create `Open(dsn string) (*sql.DB, error)` convenience function

**Acceptance Criteria:**
- Driver registered as "xxsql"
- `sql.Open("xxsql", dsn)` works
- `sql.OpenDB(&connector)` works
- Convenience `Open()` function available

**Estimated Lines:** ~100

---

### Task 9: Write Unit Tests

**File:** `pkg/xxsql/driver_test.go`

**Description:** Write comprehensive unit tests for all components.

**Details:**
- DSN parsing tests:
  - Valid DSN formats
  - Invalid DSN formats
  - Default values
  - Query parameters
- Parameter interpolation tests:
  - All Go types
  - NULL values
  - String escaping
  - Special characters
- Error handling tests:
  - Error packet parsing
  - `errors.Is()` matching
- Mock connection tests where possible

**Acceptance Criteria:**
- All tests pass
- Edge cases covered
- No external dependencies for unit tests

**Estimated Lines:** ~200

---

### Task 10: Write Integration Tests

**File:** `pkg/xxsql/integration_test.go`

**Description:** Write integration tests that connect to a running XxSql server.

**Details:**
- Use `// +build integration` build tag
- Test cases:
  - `TestConnection` - connect and ping
  - `TestCreateTable` - CREATE TABLE
  - `TestInsert` - INSERT with various types
  - `TestSelect` - SELECT and scan results
  - `TestUpdate` - UPDATE and check affected rows
  - `TestDelete` - DELETE and check affected rows
  - `TestTransaction` - BEGIN, COMMIT, ROLLBACK
  - `TestPreparedStmt` - prepared statement execution
  - `TestNullValues` - NULL handling
  - `TestMultipleRows` - iterate over multiple rows
- Use test fixture for setup/teardown

**Acceptance Criteria:**
- All tests pass with running server
- Tests can be skipped with `-tags=!integration`
- Tests cover main use cases

**Estimated Lines:** ~250

---

## Implementation Sequence

```
Task 1 (DSN Parser)
    ↓
Task 2 (Protocol) ←─ depends on Task 1 for Config
    ↓
Task 3 (Errors) ←─ independent, can be parallel
    ↓
Task 4 (Connection) ←─ depends on Tasks 1, 2, 3
    ↓
Task 5 (Rows) ←─ depends on Task 2
    ↓
Task 6 (Stmt) ←─ depends on Task 4
    ↓
Task 7 (Tx) ←─ depends on Task 4
    ↓
Task 8 (Driver) ←─ depends on Tasks 1, 4
    ↓
Task 9 (Unit Tests) ←─ depends on all implementation
    ↓
Task 10 (Integration Tests) ←─ depends on all implementation
```

## Total Estimated Lines

~1,720 lines of code

## Verification

After implementation:
1. Run `go test ./pkg/xxsql/...` - all unit tests pass
2. Run `go test ./pkg/xxsql/... -tags=integration` - all integration tests pass
3. Run `go vet ./pkg/xxsql/...` - no issues
4. Run `go build ./pkg/xxsql/...` - builds successfully
5. Test with example program from spec
