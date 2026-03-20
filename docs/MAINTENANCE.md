# XxSql Maintenance Manual

This document provides comprehensive guidance for installing, configuring, maintaining, and administering XxSql database servers.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Upgrading](#upgrading)
- [Server Management](#server-management)
- [Backup and Recovery](#backup-and-recovery)
- [Database Migration](#database-migration)
- [Web Management Interface](#web-management-interface)
- [Monitoring and Logging](#monitoring-and-logging)
- [Security Administration](#security-administration)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)

---

## Installation

### System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| **OS** | Linux, macOS, Windows | Linux (Ubuntu 20.04+, CentOS 8+) |
| **CPU** | 1 core | 2+ cores |
| **Memory** | 512 MB | 2 GB+ |
| **Disk** | 1 GB | SSD with 10 GB+ |
| **Go Version** | 1.21+ | Latest stable |

### Pre-built Binaries

Download pre-built binaries from [GitHub Releases](https://github.com/topxeq/xxsql/releases).

#### Linux

```bash
# Download (adjust version and architecture as needed)
wget https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-linux-amd64.tar.gz

# Extract
tar -xzf xxsql-v0.0.4-linux-amd64.tar.gz

# Move to system path
sudo mv xxsqls xxsqlc /usr/local/bin/

# Make executable
chmod +x /usr/local/bin/xxsqls /usr/local/bin/xxsqlc
```

#### macOS

```bash
# Apple Silicon (M1/M2)
wget https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-darwin-arm64.tar.gz
tar -xzf xxsql-v0.0.4-darwin-arm64.tar.gz

# Intel Mac
wget https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-darwin-amd64.tar.gz
tar -xzf xxsql-v0.0.4-darwin-amd64.tar.gz

# Move to system path
sudo mv xxsqls xxsqlc /usr/local/bin/
```

#### Windows

```powershell
# Download
Invoke-WebRequest -Uri "https://github.com/topxeq/xxsql/releases/download/v0.0.4/xxsql-v0.0.4-windows-amd64.zip" -OutFile "xxsql.zip"

# Extract
Expand-Archive xxsql.zip

# Move to a directory in PATH
Move-Item xxsql\*.exe C:\Windows\
```

### Build from Source

```bash
# Install Go 1.21+ if not already installed
# See: https://golang.org/doc/install

# Clone repository
git clone https://github.com/topxeq/xxsql.git
cd xxsql

# Build server
go build -o xxsqls ./cmd/xxsqls

# Build CLI client
go build -o xxsqlc ./cmd/xxsqlc

# Optional: Install to GOPATH/bin
go install ./cmd/xxsqls
go install ./cmd/xxsqlc
```

### Docker Installation

```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git
RUN git clone https://github.com/topxeq/xxsql.git .
RUN go build -o xxsqls ./cmd/xxsqls

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/xxsqls /usr/local/bin/
EXPOSE 9527 3306 8080
VOLUME /data
CMD ["xxsqls", "-data-dir", "/data"]
```

```bash
# Build image
docker build -t xxsql:latest .

# Run container
docker run -d \
    --name xxsql \
    -p 9527:9527 \
    -p 3306:3306 \
    -p 8080:8080 \
    -v /path/to/data:/data \
    xxsql:latest
```

### Systemd Service (Linux)

Create a systemd service file for automatic startup:

```bash
sudo nano /etc/systemd/system/xxsql.service
```

```ini
[Unit]
Description=XxSql Database Server
After=network.target

[Service]
Type=simple
User=xxsql
Group=xxsql
WorkingDirectory=/var/lib/xxsql
ExecStart=/usr/local/bin/xxsqls -config /etc/xxsql/xxsql.json
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
# Create user and directories
sudo useradd -r -s /bin/false xxsql
sudo mkdir -p /var/lib/xxsql /etc/xxsql
sudo chown xxsql:xxsql /var/lib/xxsql

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable xxsql
sudo systemctl start xxsql

# Check status
sudo systemctl status xxsql
```

---

## Configuration

### Configuration File

XxSql uses a JSON configuration file. Generate a default configuration:

```bash
./xxsqls -init-config > xxsql.json
```

### Complete Configuration Reference

```json
{
  "server": {
    "name": "xxsql",                    // Server name (for identification)
    "data_dir": "./data",               // Data directory path
    "pid_file": "./xxsql.pid"           // PID file location
  },

  "network": {
    "private_port": 9527,               // Private protocol port
    "mysql_port": 3306,                 // MySQL protocol port
    "http_port": 8080,                  // HTTP/Web interface port
    "bind": "0.0.0.0",                  // Bind address (0.0.0.0 = all interfaces)
    "private_enabled": true,            // Enable private protocol
    "mysql_enabled": true,              // Enable MySQL protocol
    "http_enabled": true                // Enable HTTP interface
  },

  "storage": {
    "page_size": 4096,                  // Page size in bytes (4096-65536)
    "buffer_pool_size": 1000,           // Number of pages in buffer pool
    "wal_max_size_mb": 100,             // Maximum WAL file size (MB)
    "wal_sync_interval": 100,           // WAL sync interval (ms)
    "checkpoint_pages": 1000,           // Pages before auto-checkpoint
    "checkpoint_int_sec": 300           // Checkpoint interval (seconds)
  },

  "worker": {
    "pool_size": 32,                    // Worker pool size
    "max_connection": 200               // Maximum connections
  },

  "worker_pool": {
    "worker_count": 32,                 // Number of worker goroutines
    "task_queue_size": 1000,            // Task queue size
    "task_timeout": "30s",              // Task timeout
    "strategy": "round_robin"           // Scheduling strategy
  },

  "connection": {
    "max_connections": 200,             // Maximum simultaneous connections
    "wait_timeout": 30,                 // Connection wait timeout (seconds)
    "idle_timeout": 28800,              // Idle connection timeout (seconds)
    "strategy": "fifo"                  // Connection handling strategy
  },

  "auth": {
    "enabled": true,                    // Enable authentication
    "admin_user": "admin",              // Admin username
    "admin_password": "admin",          // Admin password (CHANGE THIS!)
    "session_timeout_sec": 3600         // Session timeout (seconds)
  },

  "security": {
    "audit_enabled": true,              // Enable audit logging
    "audit_file": "audit.log",          // Audit log file
    "audit_max_size_mb": 100,           // Max audit log size
    "audit_max_backups": 10,            // Number of rotated audit logs
    "rate_limit_enabled": true,         // Enable rate limiting
    "rate_limit_max_attempts": 5,       // Max login attempts
    "rate_limit_window_min": 15,        // Rate limit window (minutes)
    "rate_limit_block_min": 30,         // Block duration (minutes)
    "password_min_length": 8,           // Minimum password length
    "password_require_upper": true,     // Require uppercase letters
    "password_require_lower": true,     // Require lowercase letters
    "password_require_digit": true,     // Require digits
    "tls_enabled": false,               // Enable TLS
    "tls_mode": "optional",             // TLS mode: optional, required
    "ip_access_mode": "allow_all"       // IP access: allow_all, whitelist, blacklist
  },

  "backup": {
    "auto_interval_hours": 24,          // Auto backup interval (hours, 0=disabled)
    "keep_count": 7,                    // Number of backups to keep
    "backup_dir": "./backup"            // Backup directory
  },

  "recovery": {
    "wal_sync_interval_ms": 100,        // WAL sync interval (ms)
    "checkpoint_interval_sec": 300,     // Checkpoint interval (seconds)
    "wal_retention_sec": 86400          // WAL retention period (seconds)
  },

  "safety": {
    "enable_checksum": true,            // Enable page checksums
    "max_recovery_attempts": 3          // Max crash recovery attempts
  },

  "log": {
    "level": "INFO",                    // Log level: DEBUG, INFO, WARN, ERROR
    "file": "",                         // Log file (empty = stdout)
    "max_size_mb": 100,                 // Max log file size
    "max_backups": 5,                   // Number of rotated logs
    "max_age_days": 30,                 // Log retention days
    "compress": false                   // Compress rotated logs
  }
}
```

### Environment Variables

Configuration can be overridden with environment variables:

```bash
# Server settings
export XXSQL_DATA_DIR=/var/lib/xxsql
export XXSQL_BIND=0.0.0.0

# Network settings
export XXSQL_PRIVATE_PORT=9527
export XXSQL_MYSQL_PORT=3306
export XXSQL_HTTP_PORT=8080
export XXSQL_PRIVATE_ENABLED=true
export XXSQL_MYSQL_ENABLED=true
export XXSQL_HTTP_ENABLED=true

# Log settings
export XXSQL_LOG_LEVEL=INFO

# Storage settings
export XXSQL_BUFFER_POOL_SIZE=2000
export XXSQL_WAL_MAX_SIZE_MB=200
```

### Command-Line Options

```bash
./xxsqls [options]

Options:
  -config string         Path to configuration file
  -data-dir string       Data directory path
  -mysql-port int        MySQL protocol port (default 3306)
  -http-port int         HTTP/Web interface port (default 8080)
  -private-port int      Private protocol port (default 9527)
  -bind string           Bind address (default "0.0.0.0")
  -log-level string      Log level: DEBUG, INFO, WARN, ERROR
  -version               Print version information
  -init-config           Print example configuration
```

---

## Upgrading

### Upgrade Procedure

1. **Backup before upgrading:**
   ```bash
   # Create backup
   ./xxsqlc -u admin -e "BACKUP DATABASE TO '/backup/pre-upgrade.xbak' WITH COMPRESS"

   # Or copy data directory
   cp -r /var/lib/xxsql /backup/xxsql-pre-upgrade
   ```

2. **Stop the server:**
   ```bash
   # Systemd
   sudo systemctl stop xxsql

   # Or manually
   pkill xxsqls
   ```

3. **Install new version:**
   ```bash
   # Download new version
   wget https://github.com/topxeq/xxsql/releases/download/v0.0.5/xxsql-v0.0.5-linux-amd64.tar.gz
   tar -xzf xxsql-v0.0.5-linux-amd64.tar.gz

   # Replace binary
   sudo mv xxsqls /usr/local/bin/xxsqls
   sudo chmod +x /usr/local/bin/xxsqls
   ```

4. **Check configuration compatibility:**
   ```bash
   # Generate new default config
   ./xxsqls -init-config > /tmp/new-config.json

   # Compare with existing
   diff /etc/xxsql/xxsql.json /tmp/new-config.json
   ```

5. **Start the server:**
   ```bash
   sudo systemctl start xxsql

   # Check logs
   tail -f /var/log/xxsql/server.log
   ```

6. **Verify upgrade:**
   ```bash
   ./xxsqlc -u admin -e "SELECT VERSION()"
   ./xxsqlc -u admin -e "SHOW TABLES"
   ```

### Rolling Back

If issues occur after upgrade:

```bash
# Stop new version
sudo systemctl stop xxsql

# Restore old binary
sudo cp /backup/xxsql-pre-upgrade/xxsqls /usr/local/bin/

# Restore data if needed
sudo rm -rf /var/lib/xxsql
sudo cp -r /backup/xxsql-pre-upgrade /var/lib/xxsql

# Start old version
sudo systemctl start xxsql
```

---

## Server Management

### Starting the Server

```bash
# Foreground (for testing)
./xxsqls -data-dir ./data

# Background
nohup ./xxsqls -config /etc/xxsql/xxsql.json > /var/log/xxsql/server.log 2>&1 &

# Systemd
sudo systemctl start xxsql
```

### Stopping the Server

```bash
# Graceful shutdown (recommended)
./xxsqlc -u admin -e "SHUTDOWN"
# Or send SIGTERM
kill -TERM <pid>

# Force stop (may cause recovery on restart)
kill -9 <pid>

# Systemd
sudo systemctl stop xxsql
```

### Restarting

```bash
sudo systemctl restart xxsql
```

### Checking Status

```bash
# Via CLI
./xxsqlc -u admin -e "SHOW STATUS"

# Via REST API
curl http://localhost:8080/api/status

# Check process
ps aux | grep xxsqls

# Check port
netstat -tlnp | grep xxsql
```

---

## Backup and Recovery

### Creating Backups

#### SQL Backup (via CLI)

```sql
-- Simple backup
BACKUP DATABASE TO '/backup/db-2024-01-15.xbak';

-- Compressed backup
BACKUP DATABASE TO '/backup/db-2024-01-15.xbak' WITH COMPRESS;
```

#### Via REST API

```bash
curl -b cookies.txt -X POST http://localhost:8080/api/backups \
  -H "Content-Type: application/json" \
  -d '{"path": "/backup/db.xbak", "compress": true}'
```

#### Automated Backups

Set up automated backups via cron:

```bash
# Edit crontab
crontab -e

# Daily backup at 2 AM
0 2 * * * /usr/local/bin/xxsqlc -u admin -p password -e "BACKUP DATABASE TO '/backup/daily-$(date +\%Y\%m\%d).xbak' WITH COMPRESS"

# Or use the built-in auto-backup feature in configuration
```

### Restoring from Backup

```sql
-- Restore database
RESTORE DATABASE FROM '/backup/db-2024-01-15.xbak';
```

```bash
# Via REST API
curl -b cookies.txt -X POST http://localhost:8080/api/restore \
  -H "Content-Type: application/json" \
  -d '{"path": "/backup/db-2024-01-15.xbak"}'
```

**Warning:** Restore overwrites existing data. Always backup before restoring.

### Backup Retention Policy

```bash
#!/bin/bash
# backup-cleanup.sh - Run weekly via cron

BACKUP_DIR="/backup"
KEEP_DAYS=30

find $BACKUP_DIR -name "*.xbak" -mtime +$KEEP_DAYS -delete
find $BACKUP_DIR -name "*.xbak.gz" -mtime +$KEEP_DAYS -delete
```

---

## Database Migration

### Exporting Data

#### SQL Dump

```bash
# Export all tables
./xxsqlc -u admin -d mydb -format sql -e "SELECT * FROM users" > users.sql

# Export with INSERT statements
./xxsqlc -u admin -d mydb -e "SELECT CONCAT('INSERT INTO users VALUES(', id, ', ''', name, ''');') FROM users" > insert_users.sql
```

### Importing Data

#### From SQL File

```bash
# Execute SQL file
./xxsqlc -u admin -d mydb -f import.sql

# With progress
./xxsqlc -u admin -d mydb -f import.sql -progress
```

#### CSV Import

```sql
-- Create table matching CSV structure
CREATE TABLE import_data (
    col1 INT,
    col2 VARCHAR(100),
    col3 DECIMAL(10,2)
);

-- Use INSERT statements generated from CSV
INSERT INTO import_data VALUES (1, 'value1', 10.50);
INSERT INTO import_data VALUES (2, 'value2', 20.75);
```

### Migrating from MySQL

1. **Export from MySQL:**
   ```bash
   mysqldump -u root -p --compatible=ansi --skip-add-locks \
       --skip-disable-keys --skip-set-charset \
       --no-create-db --no-tablespaces \
       mydb > mysql_dump.sql
   ```

2. **Adjust SQL for XxSql:**
   - Replace MySQL-specific types (TINYINT(1) → BOOL)
   - Remove MySQL-specific syntax (ENGINE=InnoDB, etc.)
   - Adjust AUTO_INCREMENT → SEQ

3. **Import to XxSql:**
   ```bash
   ./xxsqlc -u admin -d mydb -f adjusted_dump.sql -progress
   ```

### Migrating from SQLite

1. **Export from SQLite:**
   ```bash
   sqlite3 mydb.db .dump > sqlite_dump.sql
   ```

2. **Adjust SQL for XxSql:**
   - Replace INTEGER PRIMARY KEY → SEQ PRIMARY KEY
   - Adjust TEXT columns to VARCHAR(n) where appropriate

3. **Import to XxSql:**
   ```bash
   ./xxsqlc -u admin -d mydb -f adjusted_dump.sql -progress
   ```

---

## Web Management Interface

### Accessing the Web Interface

1. Open browser and navigate to `http://localhost:8080`
2. Login with database credentials (default: admin/admin)

### Dashboard Features

#### Server Status
- Server version and uptime
- Connection count
- Memory usage
- Table count

#### Query Editor
- Execute SQL queries
- View results in table format
- Query history
- Export results as JSON/CSV

#### Table Browser
- List all tables
- View table schema
- Browse table data with pagination
- Edit data inline

#### User Management
- Create/delete users
- Assign roles (Admin/User)
- Set permissions
- Reset passwords

#### Backup/Restore
- Create compressed backups
- Download backup files
- Restore from backup
- View backup history

#### Log Viewer
- View server logs
- View audit logs
- Auto-refresh option
- Filter by log level

### Security Configuration

Configure web interface security in `xxsql.json`:

```json
{
  "security": {
    "tls_enabled": true,
    "tls_mode": "required",
    "tls_cert": "/path/to/cert.pem",
    "tls_key": "/path/to/key.pem"
  }
}
```

---

## Monitoring and Logging

### Log Levels

| Level | Description |
|-------|-------------|
| `DEBUG` | Detailed debugging information |
| `INFO` | General operational messages |
| `WARN` | Warning conditions |
| `ERROR` | Error conditions |

### Changing Log Level

```bash
# Via command line
./xxsqls -log-level DEBUG

# Via configuration
{
  "log": {
    "level": "DEBUG"
  }
}
```

### Log Files

```
/var/log/xxsql/
├── server.log      # Main server log
├── audit.log       # Security audit log
├── server.log.1    # Rotated logs
└── server.log.2.gz # Compressed rotated logs
```

### Monitoring via REST API

```bash
# Server status
curl http://localhost:8080/api/status

# Metrics
curl http://localhost:8080/api/metrics

# Server logs
curl http://localhost:8080/api/logs/server?lines=100

# Audit logs
curl http://localhost:8080/api/logs/audit?lines=100
```

### Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| Connection count | Active connections | > 80% of max_connections |
| Buffer pool hit rate | Page cache efficiency | < 90% |
| WAL size | Write-ahead log size | > 80% of max |
| Disk usage | Data directory size | > 80% of disk |
| Query latency | Average query time | > 1000ms |

---

## Security Administration

### User Roles

| Role | Permissions |
|------|-------------|
| `admin` | Full access: create tables, manage users, configure server |
| `user` | Read/write access to tables, cannot modify schema or users |

### Creating Users

```sql
-- Create user
CREATE USER 'appuser' IDENTIFIED BY 'SecurePassword123!';

-- Grant permissions
GRANT ALL ON mydb.* TO 'appuser';
GRANT SELECT, INSERT ON mydb.users TO 'readonly_user';
```

### Password Policy

Configure in `xxsql.json`:

```json
{
  "security": {
    "password_min_length": 8,
    "password_require_upper": true,
    "password_require_lower": true,
    "password_require_digit": true
  }
}
```

### IP Access Control

```json
{
  "security": {
    "ip_access_mode": "whitelist",
    "ip_whitelist": ["192.168.1.0/24", "10.0.0.0/8"]
  }
}
```

Or blacklist mode:

```json
{
  "security": {
    "ip_access_mode": "blacklist",
    "ip_blacklist": ["192.168.100.0/24"]
  }
}
```

### TLS Configuration

```json
{
  "security": {
    "tls_enabled": true,
    "tls_mode": "required",
    "tls_cert": "/etc/ssl/certs/xxsql.crt",
    "tls_key": "/etc/ssl/private/xxsql.key"
  }
}
```

### Audit Logging

Enable audit logging to track security events:

```json
{
  "security": {
    "audit_enabled": true,
    "audit_file": "/var/log/xxsql/audit.log",
    "audit_max_size_mb": 100,
    "audit_max_backups": 10
  }
}
```

---

## Performance Tuning

### Buffer Pool Configuration

The buffer pool is the most critical performance setting:

```json
{
  "storage": {
    "buffer_pool_size": 10000  // Pages (4KB each = 40MB)
  }
}
```

**Recommendation:** Set buffer pool to 50-80% of available RAM.

### WAL Configuration

```json
{
  "storage": {
    "wal_max_size_mb": 500,
    "wal_sync_interval": 100,  // ms (lower = more durable, slower)
    "checkpoint_pages": 5000,
    "checkpoint_int_sec": 600
  }
}
```

### Connection Pool

```json
{
  "connection": {
    "max_connections": 500,
    "idle_timeout": 28800
  },
  "worker_pool": {
    "worker_count": 64,
    "task_queue_size": 2000
  }
}
```

### Index Optimization

```sql
-- Create indexes for frequently queried columns
CREATE INDEX idx_user_email ON users(email);
CREATE INDEX idx_order_date ON orders(created_at);

-- Use EXPLAIN to analyze queries
EXPLAIN SELECT * FROM users WHERE email = 'test@example.com';
```

---

## Troubleshooting

### Common Issues

#### Server Won't Start

```bash
# Check if port is in use
netstat -tlnp | grep 3306

# Check permissions
ls -la /var/lib/xxsql

# Check logs
tail -100 /var/log/xxsql/server.log
```

#### Connection Refused

```bash
# Check if server is running
ps aux | grep xxsqls

# Check bind address
# Make sure bind is "0.0.0.0" not "127.0.0.1" for external access

# Check firewall
sudo ufw status
sudo iptables -L -n
```

#### Recovery After Crash

XxSql automatically recovers from crashes using WAL:

```bash
# Check recovery status in logs
grep "recovery" /var/log/xxsql/server.log

# If recovery fails repeatedly, check data integrity
./xxsqls -data-dir ./data -verify
```

#### Slow Queries

```sql
-- Enable query timing in CLI
\timing

-- Analyze query plan
EXPLAIN SELECT ...

-- Check for missing indexes
SHOW INDEX FROM table_name;
```

### Diagnostic Commands

```bash
# Server version
./xxsqls -version

# Configuration validation
./xxsqls -config /etc/xxsql/xxsql.json -validate

# Data directory info
ls -la /var/lib/xxsql/
du -sh /var/lib/xxsql/*

# Process info
ps aux | grep xxsqls
cat /var/run/xxsql.pid
```

### Getting Help

1. Check the logs: `/var/log/xxsql/server.log`
2. Search [GitHub Issues](https://github.com/topxeq/xxsql/issues)
3. Open a new issue with:
   - XxSql version (`./xxsqls -version`)
   - Operating system
   - Configuration (remove sensitive data)
   - Error messages from logs
   - Steps to reproduce

---

## Appendix: File Reference

### Data Files

| File | Description |
|------|-------------|
| `*.xdb` | Table data files |
| `*.xmeta` | Table metadata |
| `*.xwal` | Write-ahead log |
| `*.xidx` | Index files |
| `*.xbak` | Backup files |
| `users.json` | User accounts |
| `grants.json` | Permission grants |
| `audit.log` | Audit log |

### Default Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 9527 | Private | XxSql native protocol |
| 3306 | MySQL | MySQL-compatible protocol |
| 8080 | HTTP | Web interface and REST API |

### Default Credentials

| Username | Password | Role |
|----------|----------|------|
| admin | admin | Administrator |

**Important:** Change the default password immediately after installation!