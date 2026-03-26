# XxSql REST API Reference

This document provides complete reference for XxSql's RESTful API. The API runs on the HTTP port (default: 8080).

## Authentication

API requests require authentication. Two methods are supported:

### Session Authentication (Cookie-based)

For web interface and interactive use:

```bash
# Login and save session cookie
curl -c cookies.txt -X POST http://localhost:8080/api/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'

# Use session for subsequent requests
curl -b cookies.txt http://localhost:8080/api/tables
```

### API Key Authentication (Header-based)

For programmatic access and scripts:

```bash
# Use API key in header
curl -H "X-API-Key: xxsql_ak_xxx..." http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

---

## Server Status

### GET /api/status

Get server status including version, uptime, and table count.

**Response:**
```json
{
  "version": "0.0.6",
  "uptime": "2h30m15s",
  "uptime_sec": 9015,
  "table_count": 5,
  "server_name": "xxsql",
  "data_dir": "./data"
}
```

### GET /api/metrics

Get storage engine metrics and statistics.

**Response:**
```json
{
  "storage": {
    "table_count": 5,
    "tables": [
      {"name": "users", "row_count": 100, "page_count": 10}
    ]
  },
  "buffer_pool": {
    "size": 1000
  },
  "wal": {
    "max_size_mb": 100,
    "sync_interval": 100
  }
}
```

### GET /api/config

Get current server configuration.

**Response:**
```json
{
  "server": {"name": "xxsql", "data_dir": "./data"},
  "network": {"private_port": 9527, "mysql_port": 3306, "http_port": 8080},
  "storage": {"buffer_pool_size": 1000},
  "log": {"level": "INFO"}
}
```

### PUT /api/config

Update server configuration. Some changes require restart.

**Request:**
```json
{
  "log": {"level": "DEBUG"}
}
```

---

## Query Execution

### POST /api/query

Execute SQL query.

**Request:**
```json
{
  "sql": "SELECT * FROM users WHERE id = 1"
}
```

**Response (SELECT):**
```json
{
  "columns": [
    {"name": "id", "type": "INT"},
    {"name": "name", "type": "VARCHAR"}
  ],
  "rows": [[1, "Alice"], [2, "Bob"]],
  "row_count": 2,
  "affected": 0,
  "message": "",
  "duration": "1.23ms"
}
```

**Response (INSERT/UPDATE/DELETE):**
```json
{
  "columns": [],
  "rows": [],
  "row_count": 0,
  "affected": 5,
  "message": "5 rows affected",
  "duration": "2.45ms"
}
```

**Error Response:**
```json
{
  "error": "table not found: nonexistent",
  "duration": "0.05ms"
}
```

---

## Table Management

### GET /api/tables

List all tables with basic info.

**Response:**
```json
{
  "tables": [
    {"name": "users", "row_count": 100, "page_count": 10},
    {"name": "orders", "row_count": 500, "page_count": 50}
  ]
}
```

### GET /api/tables/{name}

Get table schema and metadata.

**Response:**
```json
{
  "name": "users",
  "row_count": 100,
  "page_count": 10,
  "columns": [
    {"name": "id", "type": "INT", "nullable": false, "primary": true, "auto_incr": false, "size": 0},
    {"name": "name", "type": "VARCHAR", "nullable": false, "primary": false, "auto_incr": false, "size": 100}
  ]
}
```

### GET /api/tables/{name}/data

Get table data with pagination.

**Query Parameters:**
- `page` - Page number (default: 1)
- `limit` - Rows per page (default: 50)

**Example:**
```bash
curl -b cookies.txt "http://localhost:8080/api/tables/users/data?page=2&limit=20"
```

**Response:**
```json
{
  "columns": [...],
  "rows": [...],
  "row_count": 20,
  "page": 2,
  "total_pages": 5,
  "total_rows": 100
}
```

---

## User Management

### GET /api/users

List all users.

**Response:**
```json
[
  {"username": "admin", "role": "admin", "created_at": "2026-01-01T00:00:00Z"},
  {"username": "appuser", "role": "user", "created_at": "2026-02-15T10:30:00Z"}
]
```

### POST /api/users

Create new user.

**Request:**
```json
{
  "username": "newuser",
  "password": "securepassword",
  "role": "user"
}
```

**Response:**
```json
{
  "message": "user created",
  "username": "newuser"
}
```

### GET /api/users/{name}

Get user details.

**Response:**
```json
{
  "username": "appuser",
  "role": "user",
  "created_at": "2026-02-15T10:30:00Z",
  "grants": [
    {"database": "mydb", "table": "users", "permissions": ["SELECT", "INSERT"]}
  ]
}
```

### PUT /api/users/{name}

Update user (password or role).

**Request:**
```json
{
  "password": "newpassword"
}
```

### DELETE /api/users/{name}

Delete user.

---

## API Key Management

### GET /api/keys

List API keys (user's own keys, or all for admin).

**Response:**
```json
[
  {
    "id": "ak_abc123",
    "name": "my-app-key",
    "created_at": "2026-03-01T00:00:00Z",
    "expires_at": null,
    "is_active": true
  }
]
```

### POST /api/keys

Create new API key.

**Request:**
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

**Response:**
```json
{
  "message": "API key created",
  "id": "ak_abc12345",
  "name": "my-app-key",
  "key": "xxsql_ak_abc12345_def67890...",
  "_warning": "Store this key securely. It will not be shown again."
}
```

### GET /api/keys/{id}

Get API key details (without the actual key).

### PUT /api/keys/{id}

Enable/disable API key.

**Request:**
```json
{
  "is_active": false
}
```

### DELETE /api/keys/{id}

Revoke API key.

---

## Backup & Restore

### GET /api/backups

List all backups.

**Response:**
```json
[
  {
    "name": "backup-2026-03-26.xbak",
    "size": 1048576,
    "created_at": "2026-03-26T02:00:00Z",
    "compressed": true
  }
]
```

### POST /api/backups

Create new backup.

**Request:**
```json
{
  "path": "/backup/backup-2026-03-26.xbak",
  "compress": true
}
```

**Response:**
```json
{
  "message": "backup created",
  "path": "/backup/backup-2026-03-26.xbak",
  "size": 1048576
}
```

### GET /api/backups/{name}

Download backup file.

### POST /api/restore

Restore from backup.

**Request:**
```json
{
  "path": "/backup/backup-2026-03-26.xbak"
}
```

**Response:**
```json
{
  "message": "restore completed",
  "tables_restored": 5
}
```

---

## Logs

### GET /api/logs/server

Get server logs.

**Query Parameters:**
- `lines` - Number of lines to return (default: 100, max: 1000)

### GET /api/logs/audit

Get audit logs.

**Query Parameters:**
- `lines` - Number of lines to return (default: 100, max: 1000)

**Example:**
```bash
curl -b cookies.txt "http://localhost:8080/api/logs/server?lines=50"
```

---

## Authentication Endpoints

### POST /api/login

Login and create session.

**Request:**
```json
{
  "username": "admin",
  "password": "admin"
}
```

**Response:**
```json
{
  "message": "login successful",
  "username": "admin",
  "role": "admin"
}
```

### POST /api/logout

Logout and destroy session.

---

## Projects

### GET /api/projects

List all projects.

**Response:**
```json
[
  {
    "name": "myapp",
    "version": "1.0.0",
    "created_at": "2026-03-01T00:00:00Z",
    "tables": "users,posts"
  }
]
```

### POST /api/projects

Create new project.

**Request:**
```json
{
  "name": "myapp",
  "version": "1.0.0"
}
```

### DELETE /api/projects/{name}

Delete project and its files.

### POST /api/projects/import

Import project from ZIP file.

**Request:** Multipart form with `project` field containing ZIP file.

**ZIP Structure:**
```
myproject.zip
├── project.json         # Required: Project metadata
├── setup.sql            # Optional: Database setup
├── index.html           # Static files
└── static/
    └── style.css
```

### GET /api/projects/{name}/files

List project files.

### POST /api/projects/{name}/files

Create file or folder.

**Request:**
```json
{
  "path": "index.html",
  "content": "<h1>Hello</h1>",
  "isDir": false
}
```

### GET /api/projects/{name}/files/{path}

Get file content.

### PUT /api/projects/{name}/files/{path}

Update file content.

### DELETE /api/projects/{name}/files/{path}

Delete file or folder.

---

## Microservices

### GET /api/microservices

List all microservices.

**Response:**
```json
[
  {
    "skey": "api/users",
    "description": "List all users",
    "created_at": "2026-03-01T00:00:00Z"
  }
]
```

### POST /api/microservices

Create microservice.

**Request:**
```json
{
  "skey": "api/users",
  "script": "var users = db.query(\"SELECT * FROM users\"); http.json(users)",
  "description": "List all users"
}
```

### GET /api/microservices/{skey}

Get microservice details.

### PUT /api/microservices/{skey}

Update microservice script.

### DELETE /api/microservices/{skey}

Delete microservice.

### Execute Microservice

Access microservice via HTTP:

```
GET /ms/{table}/{skey}
POST /ms/{table}/{skey}
```

Example:
```bash
curl http://localhost:8080/ms/_sys_ms/api/users
```

---

## Plugins

### GET /api/plugins

List installed plugins.

**Response:**
```json
[
  {
    "name": "example-plugin",
    "version": "1.0.0",
    "enabled": true,
    "description": "Example plugin"
  }
]
```

### GET /api/plugins/available

List available plugins from registry.

### GET /api/plugins/{name}

Get plugin details.

### POST /api/plugins/import

Import plugin from ZIP file.

### POST /api/plugins/install

Install plugin from registry.

**Request:**
```json
{
  "name": "example-plugin"
}
```

### POST /api/plugins/{name}/uninstall

Uninstall plugin.

### POST /api/plugins/{name}/enable

Enable plugin.

### POST /api/plugins/{name}/disable

Disable plugin.

---

## Admin Operations

### POST /api/admin/reset

Admin reset operation (requires admin role).

**Request:**
```json
{
  "action": "clear_logs"
}
```

---

## Error Handling

All endpoints return errors in JSON format:

```json
{
  "error": "error message here"
}
```

Common HTTP status codes:

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid input |
| 401 | Unauthorized - Authentication required |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource doesn't exist |
| 405 | Method Not Allowed |
| 500 | Internal Server Error |

---

## Rate Limiting

The API respects server rate limiting configuration. If rate limited:

```json
{
  "error": "rate limit exceeded",
  "retry_after": 300
}
```

---

## CORS

CORS is enabled for the API. Allowed origins can be configured in server settings.

---

## Disabling the API

To disable the HTTP API server, set `http_enabled: false` in configuration:

```json
{
  "network": {
    "http_port": 8080,
    "http_enabled": false
  }
}
```