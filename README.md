# XxSql

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

- **Pure Go** - No CGO dependencies, easy cross-compilation
- **Single-file deployment** - Simple installation and distribution
- **MySQL-compatible protocol** - Works with existing MySQL clients
- **B+ tree storage engine** - Efficient indexing and queries
- **High concurrency** - Supports 100+ simultaneous connections
- **ACID transactions** - WAL-based durability with crash recovery
- **Rich SQL support** - SELECT, INSERT, UPDATE, DELETE, JOIN, UNION, and more

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/topxeq/xxsql.git
cd xxsql

# Build the server
go build -o xxsqls ./cmd/xxsqls

# Build the client (optional)
go build -o xxsqlc ./cmd/xxsqlc
```

### Running the Server

```bash
# Create a data directory
mkdir -p data

# Start the server with default configuration
./xxsqls -config configs/xxsql.json.example

# Or start with custom options
./xxsqls -data ./data -port 3306
```

### Connecting with MySQL Client

```bash
# Connect using standard MySQL client
mysql -h 127.0.0.1 -P 3306 -u admin -p
# Default password: admin
```

## SQL Support

### Data Types

| Type | Description |
|------|-------------|
| SEQ | Auto-increment integer (like MySQL AUTO_INCREMENT) |
| INT | 64-bit integer |
| FLOAT | 64-bit floating point |
| CHAR(n) | Fixed-length string |
| VARCHAR(n) | Variable-length string |
| TEXT | Large text |
| BOOL | Boolean |
| DATE, TIME, DATETIME | Date/time types |

### DDL Statements

```sql
-- Create table
CREATE TABLE users (
    id SEQ PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

-- Create index
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);

-- Alter table
ALTER TABLE users ADD COLUMN age INT;
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
SELECT * FROM users ORDER BY name LIMIT 10;

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

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        MySQL Protocol                        │
├─────────────────────────────────────────────────────────────┤
│                      Query Executor                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐          │
│  │  Parser │ │  Planner│ │Executor │ │  Auth   │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘          │
├─────────────────────────────────────────────────────────────┤
│                      Storage Engine                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐          │
│  │ B+ Tree │ │ Buffer  │ │   WAL   │ │  Lock   │           │
│  │ Index   │ │  Pool   │ │         │ │ Manager │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐                       │
│  │Sequence │ │Checkpoint│ │ Recovery│                      │
│  │ Manager │ │          │ │         │                      │
│  └─────────┘ └─────────┘ └─────────┘                       │
├─────────────────────────────────────────────────────────────┤
│                         Data Files                           │
│  .xdb (data)  .xmeta (metadata)  .xwal (WAL)  .xidx (index) │
└─────────────────────────────────────────────────────────────┘
```

## Configuration

Configuration file example (`configs/xxsql.json.example`):

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 3306,
    "max_connections": 100
  },
  "storage": {
    "data_dir": "./data",
    "page_size": 4096,
    "buffer_pool_size": 1000
  },
  "wal": {
    "max_size": 67108864,
    "sync_interval": 100
  },
  "checkpoint": {
    "interval": 300
  },
  "log": {
    "level": "info",
    "file": "./logs/xxsql.log"
  }
}
```

## Security Features

- **Authentication** - MySQL native password authentication
- **Role-based access control** - Admin and User roles
- **Table-level permissions** - GRANT/REVOKE support
- **Audit logging** - Track security events
- **Rate limiting** - Prevent brute force attacks
- **IP access control** - Whitelist/blacklist support
- **TLS/SSL** - Encrypted connections

## Development

### Project Structure

```
xxsql/
├── cmd/
│   ├── xxsqls/          # Server executable
│   └── xxsqlc/          # Client executable
├── internal/
│   ├── auth/            # Authentication and authorization
│   ├── config/          # Configuration management
│   ├── executor/        # SQL query execution
│   ├── log/             # Logging
│   ├── mysql/           # MySQL protocol handler
│   ├── protocol/        # Network protocol
│   ├── security/        # Security features
│   ├── server/          # Server management
│   ├── sql/             # SQL parser and AST
│   └── storage/         # Storage engine
│       ├── btree/       # B+ tree index
│       ├── buffer/      # Buffer pool
│       ├── catalog/     # Table catalog
│       ├── checkpoint/  # Checkpoint management
│       ├── lock/        # Lock manager
│       ├── page/        # Page management
│       ├── recovery/    # Crash recovery
│       ├── row/         # Row serialization
│       ├── sequence/    # Sequence manager
│       ├── table/       # Table operations
│       ├── types/       # Data types
│       └── wal/         # Write-ahead log
├── pkg/
│   └── errors/          # Error definitions
├── configs/             # Configuration files
└── data/                # Data directory
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package tests
go test -v ./internal/storage/...
```

### Benchmarks

```bash
go test -bench=. ./internal/storage/btree/...
```

## Roadmap

- [ ] DDL Enhancement (constraints, foreign keys)
- [ ] Backup/Recovery tools
- [ ] Go SQL driver
- [ ] CLI client improvements
- [ ] Web management interface
- [ ] Query optimization
- [ ] Replication support

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by SQLite and MySQL
- B+ tree implementation based on classic database literature
- MySQL protocol implementation follows MySQL documentation
