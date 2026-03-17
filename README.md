# XxSql

[![Go Report Card](https://goreportcard.com/badge/github.com/topxeq/xxsql)](https://goreportcard.com/report/github.com/topxeq/xxsql)
[![Go Reference](https://pkg.go.dev/badge/github.com/topxeq/xxsql.svg)](https://pkg.go.dev/github.com/topxeq/xxsql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://golang.org/)

A lightweight SQL database implemented in pure Go, featuring a B+ tree storage engine and MySQL-compatible protocol.

## Goal

XxSql aims to provide a lightweight SQL database with **better concurrency than SQLite** while maintaining similar ease of deployment.

| Aspect | SQLite | XxSql |
|--------|--------|-------|
| Deployment | Single file | Single binary |
| Concurrency | Database-level locking | Multi-granularity locking (table/page/row) |
| Connections | Single writer or multiple readers | 100+ simultaneous read/write connections |
| Protocol | Embedded library | MySQL-compatible network protocol |
| Language | C | Pure Go (no CGO) |

**Why XxSql?**

SQLite is excellent for embedded scenarios but has concurrency limitations - writes block all reads, and only one writer at a time. XxSql addresses this with:

- **Fine-grained locking** - Multi-level locks (global, catalog, table, page, row) allow concurrent operations
- **Multiple writers** - Concurrent write transactions with deadlock detection
- **Connection pooling** - Handle 100+ simultaneous client connections
- **Network protocol** - MySQL compatibility means no driver changes needed for existing applications

If you need:
- A lightweight database that handles concurrent access better than SQLite
- MySQL protocol compatibility without MySQL's resource overhead
- Pure Go implementation for easy cross-platform deployment

Then XxSql might be the right choice.

## Features

### Core Features

- **Pure Go** - No CGO dependencies, easy cross-compilation
- **Single-binary deployment** - Simple installation and distribution
- **MySQL-compatible protocol** - Works with existing MySQL clients and drivers
- **B+ tree storage engine** - Efficient indexing and range queries
- **High concurrency** - Supports 100+ simultaneous connections
- **ACID transactions** - WAL-based durability with ARIES-style crash recovery

### Storage & Concurrency

- **B+ Tree Index** - Primary and secondary indexes with efficient range scans
- **Buffer Pool** - LRU-based page cache with configurable size
- **WAL (Write-Ahead Log)** - Durability with configurable sync intervals
- **Checkpoints** - Periodic checkpoints for faster recovery
- **Lock Manager** - Multi-granularity locking (global, table, page, row)
- **Deadlock Detection** - Wait-for graph algorithm
- **Sequence Manager** - Atomic auto-increment with persistence

### SQL Support

- **DDL** - CREATE/ALTER/DROP TABLE, CREATE/DROP INDEX
- **DML** - SELECT, INSERT, UPDATE, DELETE, TRUNCATE
- **JOINs** - INNER, LEFT, RIGHT, CROSS JOIN with multiple table support
- **UNION** - UNION and UNION ALL with duplicate elimination
- **Aggregates** - COUNT, SUM, AVG, MIN, MAX
- **Subqueries** - WHERE clause subqueries
- **Constraints** - PRIMARY KEY, UNIQUE, NOT NULL, DEFAULT, CHECK

### Security

- **Authentication** - MySQL native password authentication (SHA1)
- **Role-based Access Control** - Admin and User roles
- **Table-level Permissions** - GRANT/REVOKE at global, database, and table level
- **Audit Logging** - Track security events with file rotation
- **Rate Limiting** - Prevent brute force attacks
- **IP Access Control** - Whitelist/blacklist with CIDR support
- **Password Policy** - Configurable length, complexity, expiration
- **TLS/SSL** - Encrypted connections (optional)

### Tools & Interfaces

- **Go SQL Driver** - Native Go database/sql driver
- **CLI Client** - Interactive REPL with readline support
- **Web Management** - Browser-based admin interface
- **Backup/Recovery** - Full backup with compression support

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/topxeq/xxsql.git
cd xxsql

# Build the server
go build -o xxsqls ./cmd/xxsqls

# Build the CLI client (optional)
go build -o xxsqlc ./cmd/xxsqlc
```

### Running the Server

```bash
# Create a data directory
mkdir -p data

# Start with default settings
./xxsqls -data-dir ./data

# Or with a configuration file
./xxsqls -config configs/xxsql.json

# Command-line options
./xxsqls -data-dir ./data -mysql-port 3306 -http-port 8080 -log-level INFO
```

### Connecting to XxSql

**Using MySQL Client:**
```bash
mysql -h 127.0.0.1 -P 3306 -u admin -p
# Default password: admin
```

**Using CLI Client:**
```bash
./xxsqlc -h localhost -P 3306 -u admin -d testdb
```

**Using Go Application:**
```go
import "github.com/topxeq/xxsql/pkg/xxsql"

db, err := sql.Open("xxsql", "admin:password@tcp(localhost:3306)/testdb")
```

**Using Web Interface:**
```
http://localhost:8080
```

## SQL Support

### Data Types

| Type | Description |
|------|-------------|
| SEQ | Auto-increment integer (like MySQL AUTO_INCREMENT) |
| INT | 64-bit integer |
| FLOAT | 64-bit floating point |
| DECIMAL(p,s) | Exact numeric with precision and scale |
| CHAR(n) | Fixed-length string |
| VARCHAR(n) | Variable-length string |
| TEXT | Large text |
| BOOL | Boolean |
| DATE, TIME, DATETIME | Date/time types |

**DECIMAL Example:**
```sql
CREATE TABLE products (
    id SEQ PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2)    -- 10 total digits, 2 after decimal
);

INSERT INTO products (id, name, price) VALUES (1, 'Widget', 99.99);
INSERT INTO products (id, name, price) VALUES (2, 'Gadget', -1234.56);
```

### DDL Statements

```sql
-- Create table with constraints
CREATE TABLE users (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INT DEFAULT 0,
    CHECK (age >= 0)
);

-- Create indexes
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);

-- Alter table
ALTER TABLE users ADD COLUMN created_at DATETIME;
ALTER TABLE users DROP COLUMN age;
ALTER TABLE users MODIFY COLUMN name VARCHAR(200);
ALTER TABLE users RENAME COLUMN name TO username;
ALTER TABLE users RENAME TO customers;

-- Drop
DROP TABLE users;
DROP INDEX idx_name ON users;
```

### DML Statements

```sql
-- Insert
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');

-- Select
SELECT * FROM users;
SELECT name, email FROM users WHERE id = 1;
SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 5;

-- Update
UPDATE users SET email = 'new@example.com' WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 1;

-- Truncate
TRUNCATE TABLE users;
```

### JOIN Support

```sql
-- Inner join
SELECT u.name, o.order_id
FROM users u
INNER JOIN orders o ON u.id = o.user_id;

-- Left join
SELECT u.name, o.order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- Right join
SELECT u.name, o.order_id
FROM users u
RIGHT JOIN orders o ON u.id = o.user_id;

-- Cross join
SELECT u.name, p.product_name
FROM users u
CROSS JOIN products p;

-- Multiple joins
SELECT u.name, o.order_id, p.product_name
FROM users u
INNER JOIN orders o ON u.id = o.user_id
INNER JOIN products p ON o.product_id = p.id;
```

### UNION Support

```sql
SELECT name FROM users
UNION
SELECT name FROM customers;

SELECT name FROM users
UNION ALL
SELECT name FROM customers;
```

### User Management

```sql
-- Create user
CREATE USER 'myuser' IDENTIFIED BY 'password';

-- Grant permissions
GRANT ALL ON *.* TO 'myuser';
GRANT SELECT, INSERT ON mydb.users TO 'myuser';

-- Revoke permissions
REVOKE INSERT ON mydb.users FROM 'myuser';

-- Show grants
SHOW GRANTS FOR 'myuser';

-- Drop user
DROP USER 'myuser';
```

### Backup & Recovery

```sql
-- Create backup
BACKUP DATABASE TO '/path/to/backup.xbak' WITH COMPRESS;

-- Restore from backup
RESTORE DATABASE FROM '/path/to/backup.xbak';
```

## Go SQL Driver

XxSql provides a native Go driver compatible with the `database/sql` package.

### Installation

```bash
go get github.com/topxeq/xxsql/pkg/xxsql
```

### Usage

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/topxeq/xxsql/pkg/xxsql"
)

func main() {
    // Open connection
    db, err := sql.Open("xxsql", "admin:password@tcp(localhost:3306)/testdb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create table
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
        id SEQ PRIMARY KEY,
        name VARCHAR(100)
    )`)
    if err != nil {
        log.Fatal(err)
    }

    // Insert
    result, err := db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
    if err != nil {
        log.Fatal(err)
    }
    id, _ := result.LastInsertId()
    fmt.Printf("Inserted ID: %d\n", id)

    // Query
    rows, err := db.Query("SELECT id, name FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        rows.Scan(&id, &name)
        fmt.Printf("ID: %d, Name: %s\n", id, name)
    }
}
```

### DSN Format

```
[username[:password]@][protocol[(address)]]/dbname[?options]
```

Examples:
- `admin:password@tcp(localhost:3306)/testdb`
- `root@tcp(127.0.0.1:3306)/mydb`
- `/testdb`

## CLI Client

The `xxsqlc` CLI client provides an interactive REPL for SQL execution.

### Usage

```bash
./xxsqlc -u admin -h localhost -P 3306 -d testdb
```

### Features

- Multi-line SQL input (continue until `;`)
- Command history (up/down arrows)
- Tab completion for SQL keywords
- Query timing display
- Multiple output formats

### Meta Commands

| Command | Description |
|---------|-------------|
| `\d` | List tables |
| `\d table` | Describe table structure |
| `\l` | List databases |
| `\u dbname` | Use database |
| `\timing` | Toggle query timing |
| `\g` | Execute current query |
| `\j` | JSON output format |
| `\t` | TSV output format |
| `\q` | Quit |

## Web Management Interface

XxSql includes a built-in web interface for database administration.

### Access

Navigate to `http://localhost:8080` (default HTTP port).

### Features

- **Dashboard** - Server status, uptime, metrics
- **Query Editor** - Execute SQL with Ctrl+Enter, view results
- **Table Browser** - View tables, schemas, and data
- **User Management** - Create, edit, delete users
- **Backup/Restore** - Create and restore backups with compression
- **Log Viewer** - View server and audit logs with auto-refresh
- **Configuration** - View server configuration

### Authentication

Web interface uses cookie-based sessions with 24-hour expiry. Login with database user credentials.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Interfaces                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │  MySQL   │  │   CLI    │  │  Go Driver│  │  Web Browser │   │
│  │  Client  │  │  Client  │  │           │  │              │   │
│  └────┬─────┘  └────┬─────┘  └─────┬─────┘  └──────┬───────┘   │
├───────┴─────────────┴──────────────┴────────────────┴───────────┤
│                      Protocol Layer                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐  │
│  │ MySQL Protocol   │  │ Private Protocol │  │ HTTP/REST API│  │
│  └──────────────────┘  └──────────────────┘  └──────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                      Query Executor                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌──────────┐ │
│  │  Parser │ │ Planner │ │Executor │ │  Auth   │ │ Security │ │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └──────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                      Storage Engine                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │
│  │ B+ Tree │ │ Buffer  │ │   WAL   │ │  Lock   │              │
│  │ Index   │ │  Pool   │ │         │ │ Manager │              │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │
│  │Sequence │ │Checkpoint│ │ Recovery│ │  Backup │              │
│  │ Manager │ │          │ │         │ │ Manager │              │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘              │
├─────────────────────────────────────────────────────────────────┤
│                         Data Files                               │
│  .xdb (data)  .xmeta (metadata)  .xwal (WAL)  .xidx (index)    │
│  .xbak (backup)  users.json  grants.json  audit.log            │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

Configuration file example (`configs/xxsql.json`):

```json
{
  "server": {
    "name": "xxsql",
    "data_dir": "./data",
    "pid_file": "./xxsql.pid"
  },
  "network": {
    "private_port": 9527,
    "mysql_port": 3306,
    "http_port": 8080,
    "bind": "0.0.0.0"
  },
  "storage": {
    "page_size": 4096,
    "buffer_pool_size": 1000,
    "wal_max_size_mb": 100,
    "wal_sync_interval": 100,
    "checkpoint_pages": 1000,
    "checkpoint_int_sec": 300
  },
  "auth": {
    "enabled": true,
    "admin_user": "admin",
    "admin_password": "admin",
    "session_timeout_sec": 3600
  },
  "security": {
    "audit_enabled": true,
    "audit_file": "audit.log",
    "rate_limit_enabled": true,
    "rate_limit_max_attempts": 5,
    "password_min_length": 8
  },
  "backup": {
    "auto_interval_hours": 24,
    "keep_count": 7,
    "backup_dir": "./backup"
  },
  "log": {
    "level": "INFO",
    "file": "",
    "max_size_mb": 100,
    "max_backups": 5
  }
}
```

### Command-Line Options

| Flag | Description |
|------|-------------|
| `-config` | Path to configuration file |
| `-data-dir` | Data directory path |
| `-mysql-port` | MySQL protocol port |
| `-http-port` | HTTP/Web interface port |
| `-private-port` | Private protocol port |
| `-bind` | Bind address |
| `-log-level` | Log level (DEBUG, INFO, WARN, ERROR) |
| `-version` | Print version information |
| `-init-config` | Print example configuration |

## Development

### Project Structure

```
xxsql/
├── cmd/
│   ├── xxsqls/          # Server executable
│   └── xxsqlc/          # CLI client executable
├── internal/
│   ├── auth/            # Authentication and authorization
│   ├── backup/          # Backup and recovery
│   ├── config/          # Configuration management
│   ├── executor/        # SQL query execution
│   ├── log/             # Logging
│   ├── mysql/           # MySQL protocol handler
│   ├── protocol/        # Private network protocol
│   ├── security/        # Security features
│   ├── server/          # Server management
│   ├── sql/             # SQL parser and AST
│   ├── storage/         # Storage engine
│   │   ├── btree/       # B+ tree index
│   │   ├── buffer/      # Buffer pool
│   │   ├── catalog/     # Table catalog
│   │   ├── checkpoint/  # Checkpoint management
│   │   ├── lock/        # Lock manager
│   │   ├── page/        # Page management
│   │   ├── recovery/    # Crash recovery
│   │   ├── row/         # Row serialization
│   │   ├── sequence/    # Sequence manager
│   │   ├── table/       # Table operations
│   │   ├── types/       # Data types
│   │   └── wal/         # Write-ahead log
│   └── web/             # Web management interface
│       ├── static/      # CSS, JS assets
│       └── templates/   # HTML templates
├── pkg/
│   └── xxsql/           # Go SQL driver
└── configs/             # Configuration files
```

### Running Tests

```bash
# Run all tests
make test
# or
go test ./...

# Run with race detector
make test-race

# Run with coverage
make test-coverage

# Run integration tests
make test-integration

# View coverage report
make coverage-report

# Run benchmarks
make bench

# Run linter
make lint
```

### Test Coverage

| Package | Coverage |
|---------|----------|
| Pkg/Errors | 98.0% |
| Storage/Page | 100.0% |
| Storage/Catalog | 90.5% |
| Storage/Storage | 89.4% |
| Storage/Row | 89.1% |
| Storage/Types | 88.1% |
| Internal/Log | 82.5% |
| Storage/Sequence | 85.1% |
| Storage/Table | 73.8% |
| Storage/Checkpoint | 79.5% |
| Config | 75.0% |
| Protocol | 73.6% |
| Auth | 72.5% |
| Storage/Lock | 68.0% |
| Storage/WAL | 66.3% |
| Storage/Recovery | 66.2% |
| Executor | 64.0% |
| Backup | 62.6% |
| Storage/Buffer | 59.4% |
| Storage/BTree | 52.2% |
| Security | 50.1% |
| Cmd/Xxsqlc | 48.0% |
| **Total** | **59.1%** |

See [docs/TESTING.md](docs/TESTING.md) for testing guidelines.

## Roadmap

### Completed ✅

- [x] Basic framework + Config + Logging
- [x] SQL Parser
- [x] Storage Engine with B+ Tree
- [x] Buffer Pool + WAL + Checkpoint
- [x] Concurrency Control (Lock Manager, Deadlock Detection)
- [x] Sequence/Auto-increment
- [x] Crash Recovery (ARIES-style)
- [x] MySQL Protocol
- [x] Authentication & Permissions
- [x] Security Features (Audit, Rate Limiting, IP Access, TLS)
- [x] JOIN Support
- [x] UNION Support
- [x] DDL Enhancement (Constraints, ALTER TABLE)
- [x] Backup/Recovery
- [x] Go SQL Driver
- [x] CLI Client
- [x] Web Management Interface

### Future Plans

- [ ] Query optimization (index hints, query planner)
- [ ] Replication support (master-slave)
- [ ] Connection pooling improvements
- [ ] More SQL functions
- [ ] Performance benchmarks

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by SQLite and MySQL
- B+ tree implementation based on classic database literature
- MySQL protocol implementation follows MySQL documentation
- ARIES recovery algorithm from "Transaction Processing" by Mohan et al.
