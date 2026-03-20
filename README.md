# XxSql

[![Go Report Card](https://goreportcard.com/badge/github.com/topxeq/xxsql)](https://goreportcard.com/report/github.com/topxeq/xxsql)
[![Go Reference](https://pkg.go.dev/badge/github.com/topxeq/xxsql.svg)](https://pkg.go.dev/github.com/topxeq/xxsql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://golang.org/)
[![Release](https://img.shields.io/github/v/release/topxeq/xxsql)](https://github.com/topxeq/xxsql/releases)

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
- **JOINs** - INNER, LEFT, RIGHT, CROSS, FULL OUTER JOIN with multiple table support
- **UNION** - UNION and UNION ALL with duplicate elimination
- **Aggregates** - COUNT, SUM, AVG, MIN, MAX
- **Subqueries** - Comprehensive subquery support (see details below)
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

### Download Pre-built Binaries

Download from [GitHub Releases](https://github.com/topxeq/xxsql/releases):

**Latest Release: v0.0.4**

| Platform | Architecture | Download |
|----------|-------------|----------|
| Linux | amd64 | [xxsql-v0.0.4-linux-amd64.tar.gz](https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-linux-amd64.tar.gz) |
| Linux | arm64 | [xxsql-v0.0.4-linux-arm64.tar.gz](https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-linux-arm64.tar.gz) |
| macOS | amd64 (Intel) | [xxsql-v0.0.4-darwin-amd64.tar.gz](https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-darwin-amd64.tar.gz) |
| macOS | arm64 (Apple Silicon) | [xxsql-v0.0.4-darwin-arm64.tar.gz](https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-darwin-arm64.tar.gz) |
| Windows | amd64 | [xxsql-v0.0.4-windows-amd64.zip](https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-windows-amd64.zip) |

```bash
# Linux/macOS example
tar -xzf xxsql-v0.0.4-linux-amd64.tar.gz
./xxsqls -data-dir ./data

# Windows example (PowerShell)
Expand-Archive xxsql-v0.0.4-windows-amd64.zip
.\xxsqls.exe -data-dir .\data
```

### Build from Source

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
| TINYINT, SMALLINT, INT, INTEGER, BIGINT | Integer types (64-bit) |
| FLOAT, DOUBLE | Floating point types |
| DECIMAL(p,s), NUMERIC(p,s) | Exact numeric with precision and scale |
| CHAR(n) | Fixed-length string |
| VARCHAR(n) | Variable-length string |
| TEXT | Large text |
| BLOB | Binary large object |
| BOOL, BOOLEAN | Boolean (TRUE/FALSE) |
| DATE | Date (YYYY-MM-DD) |
| TIME | Time (HH:MM:SS) |
| DATETIME, TIMESTAMP | Date and time |

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

### Subquery Support

XxSql provides comprehensive subquery support across various SQL contexts:

#### Supported Subquery Types

| Subquery Type | Description | Example |
|---------------|-------------|---------|
| **Scalar Subquery** | Returns a single value | `SELECT (SELECT MAX(x) FROM t)` |
| **IN Subquery** | Checks membership in subquery results | `WHERE id IN (SELECT ...)` |
| **NOT IN Subquery** | Checks non-membership | `WHERE id NOT IN (SELECT ...)` |
| **EXISTS Subquery** | Checks if subquery returns rows | `WHERE EXISTS (SELECT ...)` |
| **NOT EXISTS Subquery** | Checks if subquery returns no rows | `WHERE NOT EXISTS (SELECT ...)` |
| **ANY Subquery** | Comparison with any subquery result | `WHERE x > ANY (SELECT ...)` |
| **ALL Subquery** | Comparison with all subquery results | `WHERE x > ALL (SELECT ...)` |
| **Derived Table** | Subquery in FROM clause | `FROM (SELECT ...) AS alias` |

#### Subquery Locations

| Location | Supported | Notes |
|----------|-----------|-------|
| **SELECT list** | ✅ | Scalar subqueries with alias support |
| **WHERE clause** | ✅ | All subquery types supported |
| **HAVING clause** | ✅ | With aggregate and subquery comparisons |
| **FROM clause** | ✅ | Derived tables with required alias |

#### Examples

**1. Scalar Subquery in SELECT List:**
```sql
-- Simple scalar subquery
SELECT (SELECT MAX(amount) FROM orders) AS max_amount;

-- Multiple scalar subqueries
SELECT
    (SELECT MIN(price) FROM products) AS min_price,
    (SELECT MAX(price) FROM products) AS max_price;

-- Correlated scalar subquery
SELECT
    id,
    name,
    (SELECT SUM(amount) FROM orders WHERE user_id = users.id) AS total
FROM users;
```

**2. IN / NOT IN Subqueries:**
```sql
-- Find users who have placed orders
SELECT * FROM users
WHERE id IN (SELECT DISTINCT user_id FROM orders);

-- Find users who haven't placed orders
SELECT * FROM users
WHERE id NOT IN (SELECT user_id FROM orders);
```

**3. EXISTS / NOT EXISTS Subqueries:**
```sql
-- Users with orders over 100
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.user_id = u.id AND o.amount > 100
);

-- Products never ordered
SELECT * FROM products p
WHERE NOT EXISTS (
    SELECT 1 FROM order_items oi
    WHERE oi.product_id = p.id
);
```

**4. ANY / ALL Subqueries:**
```sql
-- Products priced higher than any product in category 1
SELECT * FROM products
WHERE price > ANY (
    SELECT price FROM products WHERE category_id = 1
);

-- Products priced higher than all products in category 1
SELECT * FROM products
WHERE price > ALL (
    SELECT price FROM products WHERE category_id = 1
);
```

**5. Derived Tables (Subquery in FROM):**
```sql
-- Derived table with filtering
SELECT * FROM (
    SELECT user_id, SUM(amount) AS total
    FROM orders
    GROUP BY user_id
) AS user_totals
WHERE total > 1000;
```

**6. HAVING Clause with Subqueries:**
```sql
-- Groups with count above average
SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > (
    SELECT AVG(cnt) FROM (
        SELECT COUNT(*) AS cnt
        FROM orders
        GROUP BY customer_id
    ) AS counts
);

-- Groups where total exceeds threshold
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 500;

-- HAVING with EXISTS
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING EXISTS (
    SELECT 1 FROM orders
    WHERE orders.customer_id = customer_id
    AND amount > 200
);
```

#### Correlated Subqueries

Correlated subqueries reference columns from the outer query and are evaluated for each row:

```sql
-- Find users with above-average order amounts for their city
SELECT u.name, u.city, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > (
    SELECT AVG(o2.amount)
    FROM users u2
    JOIN orders o2 ON u2.id = o2.user_id
    WHERE u2.city = u.city
);
```

#### Performance Notes

- Non-correlated subqueries are executed once
- Correlated subqueries are executed for each outer row
- Consider using JOINs for better performance on large datasets
- Indexes on join columns improve correlated subquery performance

### JOIN Support

XxSql supports all standard SQL JOIN types:

| JOIN Type | Description |
|-----------|-------------|
| INNER JOIN | Returns rows when there is a match in both tables |
| LEFT [OUTER] JOIN | Returns all rows from left table, with matched rows from right (NULL if no match) |
| RIGHT [OUTER] JOIN | Returns all rows from right table, with matched rows from left (NULL if no match) |
| FULL [OUTER] JOIN | Returns all rows from both tables, with NULLs where there is no match |
| CROSS JOIN | Returns Cartesian product of both tables |

```sql
-- Inner join
SELECT u.name, o.order_id
FROM users u
INNER JOIN orders o ON u.id = o.user_id;

-- Left join
SELECT u.name, o.order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- Left outer join (equivalent to LEFT JOIN)
SELECT u.name, o.order_id
FROM users u
LEFT OUTER JOIN orders o ON u.id = o.user_id;

-- Right join
SELECT u.name, o.order_id
FROM users u
RIGHT JOIN orders o ON u.id = o.user_id;

-- Full outer join - returns all rows from both tables
SELECT u.name, o.order_id
FROM users u
FULL JOIN orders o ON u.id = o.user_id;

-- Full outer join with OUTER keyword
SELECT u.name, o.order_id
FROM users u
FULL OUTER JOIN orders o ON u.id = o.user_id;

-- Cross join
SELECT u.name, p.product_name
FROM users u
CROSS JOIN products p;

-- Multiple joins
SELECT u.name, o.order_id, p.product_name
FROM users u
INNER JOIN orders o ON u.id = o.user_id
INNER JOIN products p ON o.product_id = p.id;

-- Join with WHERE clause
SELECT u.name, o.order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE o.order_id IS NOT NULL;
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

XxSql driver supports two DSN formats:

**1. MySQL-style DSN:**
```
[username[:password]@][protocol[(address)]]/dbname[?options]
```

Examples:
- `admin:password@tcp(localhost:3306)/testdb`
- `root@tcp(127.0.0.1:3306)/mydb`
- `/testdb`

**2. URL-style DSN:**
```
xxsql://[username[:password]@]host[:port]/dbname[?options]
```

Examples:
- `xxsql://admin:password@localhost:3306/testdb`
- `xxsql://root@127.0.0.1:3306/mydb`
- `xxsql://localhost/testdb`

**Supported Options:**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `timeout` | Connection timeout | `10s` |
| `readTimeout` | Read timeout | `30s` |
| `writeTimeout` | Write timeout | `30s` |
| `charset` | Character set | `utf8mb4` |
| `parseTime` | Parse DATE/DATETIME to time.Time | `false` |

## CLI Client

The `xxsqlc` CLI client provides an interactive REPL for SQL execution.

### Usage

```bash
# Basic connection
./xxsqlc -u admin -host localhost -port 3306 -d testdb

# Using DSN
./xxsqlc -dsn "xxsql://admin:password@localhost:3306/testdb"

# Execute SQL from command line
./xxsqlc -u admin -d testdb -e "SELECT * FROM users"

# Execute SQL from file
./xxsqlc -u admin -d testdb -f script.sql -progress

# Specify output format
./xxsqlc -u admin -d testdb -format json -e "SELECT * FROM users"
```

### Command-Line Options

| Flag | Description |
|------|-------------|
| `-host` | Server host (default: localhost) |
| `-port` | Server port (default: 3306) |
| `-u` | Username |
| `-p` | Password |
| `-d` | Database name |
| `-dsn` | Connection string (URL format) |
| `-e` | Execute command and exit |
| `-f` | Execute SQL from file and exit |
| `-format` | Output format: table, vertical, json, tsv |
| `-progress` | Show progress when executing SQL file |
| `-q` | Suppress welcome message |
| `-version` | Print version information |

### Features

- Multi-line SQL input (continue until `;`)
- Command history (up/down arrows)
- Tab completion for SQL keywords
- Query timing display
- Multiple output formats

### Meta Commands

| Command | Description |
|---------|-------------|
| `\h`, `\?` | Show help |
| `\q` | Quit |
| `\c` | Clear screen / Clear current query |
| `\v` | Show version |
| `\l` | List databases |
| `\d [table]` | Describe table or list tables |
| `\u <db>` | Use database |
| `\conninfo` | Show connection info |
| `\timing` | Toggle query timing |
| `\g`, `\vertical` | Switch to vertical output format |
| `\j`, `\json` | Switch to JSON output format |
| `\t`, `\tsv` | Switch to TSV output format |
| `\table` | Switch to table output format (default) |
| `\format` | Show current output format |

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

## RESTful API

XxSql provides a comprehensive RESTful API for programmatic access. The API runs on the HTTP port (default: 8080).

### Authentication

API requests require authentication. Two methods are supported:

#### Session Authentication (Cookie-based)

For web interface and interactive use:

```bash
# Login and save session cookie
curl -c cookies.txt -X POST http://localhost:8080/api/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'

# Use session for subsequent requests
curl -b cookies.txt http://localhost:8080/api/tables
```

#### API Key Authentication (Header-based)

For programmatic access and scripts:

```bash
# Use API key in header
curl -H "X-API-Key: xxsql_ak_xxx..." http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

**API Key Features:**
- Stateless authentication (no session management needed)
- Scoped permissions (can limit what operations the key can perform)
- Optional expiration time
- Can be enabled/disabled or revoked
- Key is shown only once when created - store securely!

### API Endpoints

#### Server Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Get server status (version, uptime, table count) |
| GET | `/api/metrics` | Get storage engine metrics and statistics |
| GET | `/api/config` | Get current configuration |
| PUT | `/api/config` | Update configuration (requires restart) |

#### Query Execution

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/query` | Execute SQL query |

**Query Request:**
```json
{
  "sql": "SELECT * FROM users WHERE id = 1"
}
```

**Query Response:**
```json
{
  "columns": [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR"}],
  "rows": [[1, "Alice"], [2, "Bob"]],
  "row_count": 2,
  "affected": 0,
  "message": "",
  "duration": "1.23ms"
}
```

#### Table Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/tables` | List all tables |
| GET | `/api/tables/{name}` | Get table schema and metadata |
| GET | `/api/tables/{name}/data` | Get table data with pagination |

**Table Data Pagination:**
```bash
# Get page 2 (50 rows per page)
curl -b cookies.txt "http://localhost:8080/api/api/tables/users/data?page=2"
```

#### User Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/users` | List all users |
| POST | `/api/users` | Create new user |
| GET | `/api/users/{name}` | Get user details |
| PUT | `/api/users/{name}` | Update user |
| DELETE | `/api/users/{name}` | Delete user |

**Create User Request:**
```json
{
  "username": "newuser",
  "password": "securepassword",
  "role": "user"
}
```

#### API Key Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/keys` | List API keys (user's own, or all for admin) |
| POST | `/api/keys` | Create new API key |
| GET | `/api/keys/{id}` | Get API key details |
| PUT | `/api/keys/{id}` | Update API key (enable/disable) |
| DELETE | `/api/keys/{id}` | Revoke API key |

**Create API Key Request:**
```json
{
  "name": "my-app-key",
  "expires_in": 0,
  "permissions": 0
}
```

- `name` - Human-readable name for the key
- `expires_in` - Expiration time in seconds (0 = no expiration)
- `permissions` - Permission bits (0 = use user's role permissions)

**Create API Key Response:**
```json
{
  "message": "API key created",
  "id": "ak_abc12345",
  "name": "my-app-key",
  "key": "xxsql_ak_abc12345_def67890...",
  "_warning": "Store this key securely. It will not be shown again."
}
```

**Using API Key:**
```bash
# Create a new API key (using session auth)
curl -b cookies.txt -X POST http://localhost:8080/api/keys \
  -H "Content-Type: application/json" \
  -d '{"name": "script-key"}'

# Use the API key for requests
curl -H "X-API-Key: xxsql_ak_xxx..." \
  http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'

# List your API keys
curl -b cookies.txt http://localhost:8080/api/keys

# Revoke an API key
curl -b cookies.txt -X DELETE http://localhost:8080/api/keys/ak_abc12345
```

#### Backup & Restore

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/backups` | List all backups |
| POST | `/api/backups` | Create new backup |
| GET | `/api/backups/{name}` | Download backup file |
| POST | `/api/restore` | Restore from backup |

**Create Backup Request:**
```json
{
  "path": "/path/to/backup.xbak",
  "compress": true
}
```

**Restore Request:**
```json
{
  "path": "/path/to/backup.xbak"
}
```

#### Logs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/logs/server` | Get server logs |
| GET | `/api/logs/audit` | Get audit logs |

**Query Parameters:**
- `lines` - Number of lines to return (default: 100, max: 1000)

```bash
curl -b cookies.txt "http://localhost:8080/api/logs/server?lines=50"
```

#### Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/login` | Login and create session |
| POST | `/api/logout` | Logout and destroy session |

### Example Usage

```bash
# Check server status
curl -b cookies.txt http://localhost:8080/api/status

# Create a table
curl -b cookies.txt -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE products (id SEQ PRIMARY KEY, name VARCHAR(100), price FLOAT)"}'

# Insert data
curl -b cookies.txt -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO products (name, price) VALUES (\"Widget\", 29.99)"}'

# Query data
curl -b cookies.txt -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM products"}'

# Create backup
curl -b cookies.txt -X POST http://localhost:8080/api/backups \
  -H "Content-Type: application/json" \
  -d '{"compress": true}'

# List users
curl -b cookies.txt http://localhost:8080/api/users

# Create new user
curl -b cookies.txt -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"username": "appuser", "password": "secret123", "role": "user"}'
```

### Error Handling

API errors return JSON with an `error` field:

```json
{
  "error": "table not found: nonexistent",
  "duration": "0.05ms"
}
```

### Disabling the API

To disable the HTTP API server, set `http_enabled: false` in configuration:

```json
{
  "network": {
    "http_port": 8080,
    "http_enabled": false
  }
}
```

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
    "bind": "0.0.0.0",
    "private_enabled": true,
    "mysql_enabled": true,
    "http_enabled": true
  },
  "storage": {
    "page_size": 4096,
    "buffer_pool_size": 1000,
    "wal_max_size_mb": 100,
    "wal_sync_interval": 100,
    "checkpoint_pages": 1000,
    "checkpoint_int_sec": 300
  },
  "worker": {
    "pool_size": 32,
    "max_connection": 200
  },
  "worker_pool": {
    "worker_count": 32,
    "task_queue_size": 1000,
    "task_timeout": "30s",
    "strategy": "round_robin"
  },
  "connection": {
    "max_connections": 200,
    "wait_timeout": 30,
    "idle_timeout": 28800,
    "strategy": "fifo"
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
    "audit_max_size_mb": 100,
    "audit_max_backups": 10,
    "rate_limit_enabled": true,
    "rate_limit_max_attempts": 5,
    "rate_limit_window_min": 15,
    "rate_limit_block_min": 30,
    "password_min_length": 8,
    "password_require_upper": true,
    "password_require_lower": true,
    "password_require_digit": true,
    "tls_enabled": false,
    "tls_mode": "optional",
    "ip_access_mode": "allow_all"
  },
  "backup": {
    "auto_interval_hours": 24,
    "keep_count": 7,
    "backup_dir": "./backup"
  },
  "recovery": {
    "wal_sync_interval_ms": 100,
    "checkpoint_interval_sec": 300,
    "wal_retention_sec": 86400
  },
  "safety": {
    "enable_checksum": true,
    "max_recovery_attempts": 3
  },
  "log": {
    "level": "INFO",
    "file": "",
    "max_size_mb": 100,
    "max_backups": 5,
    "max_age_days": 30,
    "compress": false
  }
}
```

### Network Configuration

The `network` section controls which services are started:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `private_port` | int | 9527 | Private protocol port |
| `mysql_port` | int | 3306 | MySQL compatible port |
| `http_port` | int | 8080 | HTTP/Web interface port |
| `bind` | string | "0.0.0.0" | Bind address |
| `private_enabled` | bool | true | Enable/disable private protocol server |
| `mysql_enabled` | bool | true | Enable/disable MySQL protocol server |
| `http_enabled` | bool | true | Enable/disable HTTP API server |

**Example: Disable HTTP interface**
```json
{
  "network": {
    "private_port": 9527,
    "mysql_port": 3306,
    "http_port": 8080,
    "bind": "0.0.0.0",
    "http_enabled": false
  }
}
```

**Example: MySQL-only server**
```json
{
  "network": {
    "private_port": 9527,
    "mysql_port": 3306,
    "http_port": 8080,
    "bind": "0.0.0.0",
    "private_enabled": false,
    "http_enabled": false
  }
}
```

**Environment Variables:**
- `XXSQL_PRIVATE_ENABLED` - Enable/disable private protocol server
- `XXSQL_MYSQL_ENABLED` - Enable/disable MySQL protocol server
- `XXSQL_HTTP_ENABLED` - Enable/disable HTTP API server

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
| Config | 96.7% |
| Auth | 93.9% |
| Storage/Catalog | 90.5% |
| Storage/Storage | 89.4% |
| Storage/Row | 89.1% |
| Storage/BTree | 89.0% |
| Storage/Checkpoint | 88.9% |
| Storage/Types | 88.1% |
| Security | 88.8% |
| Protocol | 86.4% |
| Storage/Buffer | 86.2% |
| Web | 86.1% |
| Storage/Table | 85.9% |
| Cmd/Xxsqls | 85.7% |
| Storage/Lock | 84.4% |
| Storage/WAL | 84.8% |
| MySQL | 84.8% |
| Storage/Sequence | 85.1% |
| Storage/Recovery | 83.3% |
| Backup | 83.1% |
| Pkg/Xxsql | 83.5% |
| Server | 83.8% |
| Log | 82.0% |
| Executor | 81.5% |
| Cmd/Xxsqlc | 77.9% |
| **Average** | **87.5%** |

See [docs/TESTING.md](docs/TESTING.md) for testing guidelines.

## Roadmap

### v0.0.4 (Current Release) ✅

**New Features:**
- **BLOB Type Support** - Full binary large object support with hex notation
  - Hex literals: `X'48656c6c6f'` and `0xdeadbeef`
  - BLOB storage with 4-byte length prefix
  - Byte-by-byte comparison operations

- **CAST Expressions** - Type conversion between data types
  - `CAST(expr AS INT)` - Convert to integer
  - `CAST(expr AS FLOAT)` - Convert to floating point
  - `CAST(expr AS VARCHAR/CHAR/TEXT)` - Convert to string
  - `CAST(expr AS BLOB)` - Convert to binary
  - `CAST(expr AS BOOL)` - Convert to boolean

- **Built-in Functions**:
  - `HEX(value)` - Convert BLOB/string to hexadecimal string
  - `UNHEX(string)` - Convert hexadecimal string to BLOB
  - `LENGTH(value)` / `OCTET_LENGTH(value)` - Get byte length
  - `UPPER(string)` / `LOWER(string)` - Case conversion
  - `CONCAT(str1, str2, ...)` - String concatenation
  - `SUBSTRING(str, start, len)` - Substring extraction

- **User-Defined Functions (UDF)**:
  - Create SQL functions with `CREATE FUNCTION`
  - Support for IF expressions, LET variables, and BEGIN/END blocks
  - Default parameter values
  - Automatic persistence to disk
  ```sql
  -- Simple UDF
  CREATE FUNCTION double(x INT) RETURNS INT RETURN x * 2;

  -- UDF with IF expression
  CREATE FUNCTION abs_val(x INT) RETURNS INT
      RETURN IF x < 0 THEN -x ELSE x END;

  -- UDF with LET and BEGIN/END block
  CREATE FUNCTION complex_calc(x INT, y INT) RETURNS INT
  BEGIN
      LET a = x * 2;
      LET b = y + 1;
      RETURN a + b;
  END;

  -- UDF with default parameter
  CREATE FUNCTION greet(name VARCHAR DEFAULT 'World') RETURNS VARCHAR
      RETURN CONCAT('Hello, ', name);

  -- Use in queries
  SELECT double(5);        -- Returns 10
  SELECT abs_val(-5);      -- Returns 5
  SELECT complex_calc(3, 4);  -- Returns 11
  SELECT greet();          -- Returns 'Hello, World'
  SELECT greet('Alice');   -- Returns 'Hello, Alice'

  -- Drop a UDF
  DROP FUNCTION double;
  ```

- **SQL Syntax Improvements**:
  - `IS NULL` / `IS NOT NULL` expressions
  - Proper `AND` / `OR` operator handling in WHERE clauses
  - SEQ type now automatically enables auto-increment

- **Comprehensive Subquery Support**:
  - Scalar subqueries in SELECT list (e.g., `SELECT (SELECT MAX(x) FROM t)`)
  - IN / NOT IN subqueries in WHERE clause
  - EXISTS / NOT EXISTS subqueries
  - ANY / ALL comparison subqueries
  - Derived tables (subqueries in FROM clause)
  - Subqueries in HAVING clause
  - Full correlated subquery support

- **Multi-platform Builds**:
  - Linux (amd64, arm64)
  - macOS (amd64 Intel, arm64 Apple Silicon)
  - Windows (amd64)
  - Automated GitHub Actions release workflow

**Example Usage:**
```sql
-- Create a table with BLOB column
CREATE TABLE files (
    id SEQ PRIMARY KEY,
    name VARCHAR(255),
    data BLOB
);

-- Insert BLOB data with hex notation
INSERT INTO files (name, data) VALUES ('hello', X'48656c6c6f');

-- Query with functions
SELECT id, name, HEX(data) AS hex_data, LENGTH(data) AS size FROM files;

-- Use CAST expressions
SELECT CAST('123' AS INT), CAST(0xdeadbeef AS BLOB);
```

### v0.0.1 ✅

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
- [x] JOIN Support (INNER, LEFT, RIGHT, CROSS, FULL OUTER)
- [x] UNION Support
- [x] DDL Enhancement (Constraints, ALTER TABLE)
- [x] Backup/Recovery
- [x] Go SQL Driver
- [x] CLI Client with REPL
- [x] Web Management Interface
- [x] Comprehensive test suite (87.5% average coverage)

### Future Plans

- [ ] Query optimization (index hints, query planner)
- [ ] Replication support (master-slave)
- [ ] Connection pooling improvements
- [ ] More SQL functions
- [ ] Performance benchmarks
- [ ] Subquery optimization (decorrelation, result caching)
- [ ] Transaction isolation levels

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by SQLite and MySQL
- B+ tree implementation based on classic database literature
- MySQL protocol implementation follows MySQL documentation
- ARIES recovery algorithm from "Transaction Processing" by Mohan et al.
