# XxSql Microservices and Projects Guide

This guide covers XxSql's powerful microservice capabilities and project management features, allowing you to build and deploy web applications directly on the database server.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Microservices](#microservices)
4. [Projects](#projects)
5. [Web Management Interface](#web-management-interface)
6. [Complete Examples](#complete-examples)
7. [API Reference](#api-reference)

## Overview

XxSql combines database and web server functionality, enabling you to:

- **Create RESTful API endpoints** using XxScript
- **Deploy web applications** with static files and backend logic
- **Manage projects** through a web interface or API
- **Import/export projects** as ZIP files

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      XxSql Server                               │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐│
│  │   Storage   │  │   Query     │  │    Web Interface        ││
│  │   Engine    │  │   Executor  │  │    (port 8080)          ││
│  └──────┬──────┘  └──────┬──────┘  └─────────────┬───────────┘│
│         │                │                       │              │
│         └────────────────┼───────────────────────┘              │
│                          │                                      │
│  ┌───────────────────────┴───────────────────────┐              │
│  │              XxScript Runtime                 │              │
│  │  - HTTP request/response handling             │              │
│  │  - Database operations                        │              │
│  │  - File system access                         │              │
│  └───────────────────────────────────────────────┘              │
│                          │                                      │
│  ┌───────────────────────┴───────────────────────┐              │
│  │              Project Structure                 │              │
│  │  {data_dir}/projects/{project_name}/          │              │
│  │    ├── index.html                             │              │
│  │    ├── static/                                │              │
│  │    └── ...                                    │              │
│  └───────────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Start the Server

```bash
./xxsqls -data-dir ./data
```

The server starts three ports by default:
- **9527**: Private protocol (for Go clients)
- **3306**: MySQL-compatible protocol
- **8080**: HTTP/Web interface

### 2. Access the Web Interface

Open http://localhost:8080 in your browser. Login with:
- Username: `admin`
- Password: `xxsql` (or your configured password)

### 3. Create Your First Microservice

Navigate to **Microservices** → **+ New Microservice**:

- **Service Key**: `hello`
- **Description**: `Simple greeting service`
- **Script**:
```javascript
var name = http.param("name") || "World"
http.json({"message": "Hello, " + name + "!"})
```

Click **Save**, then test at: http://localhost:8080/ms/_sys_ms/hello?name=XxSql

## Microservices

Microservices are HTTP endpoints that execute XxScript code. Each microservice is stored in the `_sys_ms` table with a unique `SKEY` (service key).

### URL Pattern

```
/ms/{table}/{skey}
```

- **table**: Usually `_sys_ms` for system microservices
- **skey**: The service key you defined

Examples:
- `/ms/_sys_ms/hello` - Access the `hello` service
- `/ms/_sys_ms/user/list` - Access the `user/list` service

### Creating Microservices

#### Via Web Interface

1. Navigate to **Microservices** page
2. Click **+ New Microservice**
3. Fill in the form:
   - **Service Key**: URL path segment (e.g., `api/users`)
   - **Description**: What the service does
   - **Script**: XxScript code

#### Via SQL

```sql
INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at)
VALUES (
    'api/users',
    'var users = db.query("SELECT * FROM users"); http.json(users)',
    'List all users',
    datetime('now')
);
```

#### Via API

```bash
curl -X POST http://localhost:8080/api/microservices \
  -u admin:xxsql \
  -H "Content-Type: application/json" \
  -d '{
    "skey": "api/users",
    "script": "var users = db.query(\"SELECT * FROM users\"); http.json(users)",
    "description": "List all users"
  }'
```

### HTTP Object Reference

The `http` object provides access to request data and response methods.

#### Request Information

| Property/Method | Description | Example |
|-----------------|-------------|---------|
| `http.method` | HTTP method | `"GET"`, `"POST"` |
| `http.path` | Request path | `"/ms/_sys_ms/hello"` |
| `http.query` | Raw query string | `"name=John&age=30"` |
| `http.remoteAddr` | Client IP address | `"192.168.1.1:12345"` |
| `http.param("name")` | Get query parameter | Returns `"John"` |
| `http.header("Name")` | Get request header | Returns header value |
| `http.cookie("name")` | Get cookie value | Returns cookie value |
| `http.body()` | Get raw body as string | Returns body content |
| `http.bodyJSON()` | Parse body as JSON | Returns parsed object |

#### Response Methods

| Method | Description | Example |
|--------|-------------|---------|
| `http.status(code)` | Set HTTP status | `http.status(404)` |
| `http.setHeader(name, value)` | Set response header | `http.setHeader("X-Custom", "value")` |
| `http.json(object)` | Send JSON response | `http.json({"status": "ok"})` |
| `http.write(string)` | Write raw response | `http.write("Hello")` |
| `http.setCookie(name, value, maxAge)` | Set cookie | `http.setCookie("session", "abc", 3600)` |
| `http.redirect(url)` | Redirect (302) | `http.redirect("/login")` |
| `http.redirect(url, code)` | Redirect with code | `http.redirect("/new", 301)` |

### Database Object Reference

The `db` object provides database operations.

| Method | Description | Returns |
|--------|-------------|---------|
| `db.query(sql)` | Execute SELECT query | Array of row objects |
| `db.queryRow(sql)` | Query single row | Row object or `null` |
| `db.exec(sql)` | Execute INSERT/UPDATE/DELETE | `{"insert_id": N, "affected": N}` |

### Example: CRUD API

```javascript
// GET /ms/api/users - List users
if (http.method == "GET") {
    var page = int(http.param("page")) || 1
    var limit = 20
    var offset = (page - 1) * limit

    var users = db.query(
        "SELECT * FROM users ORDER BY id LIMIT " + string(limit) +
        " OFFSET " + string(offset)
    )
    var total = db.queryRow("SELECT COUNT(*) as count FROM users")

    http.json({
        "users": users,
        "page": page,
        "total": total.count,
        "pages": ceil(total.count / limit)
    })
}
```

```javascript
// POST /ms/api/users - Create user
if (http.method == "POST") {
    var data = http.bodyJSON()

    // Validate
    if (!data.name || !data.email) {
        http.status(400)
        http.json({"error": "name and email required"})
    }

    // Insert
    var result = db.exec(
        "INSERT INTO users (name, email, created_at) VALUES ('" +
        data.name + "', '" + data.email + "', datetime('now'))"
    )

    http.status(201)
    http.json({
        "id": result.insert_id,
        "name": data.name,
        "email": data.email
    })
}
```

```javascript
// Combined handler with method routing
var parts = split(http.path, "/")
var userId = parts[len(parts) - 1]

if (http.method == "GET") {
    if (userId != "" && userId != "users") {
        // GET /ms/api/users/{id}
        var user = db.queryRow("SELECT * FROM users WHERE id = " + userId)
        if (user == null) {
            http.status(404)
            http.json({"error": "User not found"})
        }
        http.json(user)
    } else {
        // GET /ms/api/users - List
        var users = db.query("SELECT * FROM users")
        http.json(users)
    }
} else if (http.method == "PUT") {
    // PUT /ms/api/users/{id}
    var data = http.bodyJSON()
    var result = db.exec(
        "UPDATE users SET name = '" + data.name + "', email = '" +
        data.email + "' WHERE id = " + userId
    )
    http.json({"updated": result.affected})
} else if (http.method == "DELETE") {
    // DELETE /ms/api/users/{id}
    var result = db.exec("DELETE FROM users WHERE id = " + userId)
    http.json({"deleted": result.affected})
}
```

## Projects

Projects are web applications deployed on XxSql. Each project can have:
- Static files (HTML, CSS, JavaScript, images)
- Backend microservices
- Database tables

### Project Structure

```
{data_dir}/projects/{project_name}/
├── index.html           # Main page (served at /projects/{name}/)
├── static/              # Static assets
│   ├── css/
│   │   └── style.css
│   ├── js/
│   │   └── app.js
│   └── images/
│       └── logo.png
├── api/                 # Optional API pages
│   └── data.html
└── ...
```

### Creating Projects

#### Via Web Interface

1. Navigate to **Projects** page
2. Click **+ New Project**
3. Enter name and version
4. Click **Create**

Then use **Files** button to manage files.

#### Via API

```bash
# Create project
curl -X POST http://localhost:8080/api/projects \
  -u admin:xxsql \
  -H "Content-Type: application/json" \
  -d '{"name": "myapp", "version": "1.0.0"}'

# Create file
curl -X POST http://localhost:8080/api/projects/myapp/files \
  -u admin:xxsql \
  -H "Content-Type: application/json" \
  -d '{"path": "index.html", "content": "<h1>Hello</h1>"}'
```

### Project ZIP Import

Create a ZIP file with the following structure:

```
myproject.zip
├── project.json         # Required: Project metadata
├── setup.sql            # Optional: Database setup
├── index.html           # Static files
├── static/
│   └── style.css
└── ...
```

#### project.json Format

```json
{
    "name": "myproject",
    "version": "1.0.0",
    "tables": "users,posts,comments",
    "microservices": [
        {
            "skey": "api/users",
            "script": "var users = db.query(\"SELECT * FROM users\"); http.json(users)",
            "description": "List users"
        },
        {
            "skey": "api/posts",
            "script": "var posts = db.query(\"SELECT * FROM posts ORDER BY created_at DESC\"); http.json(posts)",
            "description": "List posts"
        }
    ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Project name (used in URLs) |
| `version` | No | Version string (default: "1.0.0") |
| `tables` | No | Comma-separated table names (for reference) |
| `microservices` | No | Array of microservice definitions |

#### setup.sql Format

```sql
-- Create tables
CREATE TABLE users (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    created_at DATETIME DEFAULT datetime('now')
);

CREATE TABLE posts (
    id SEQ PRIMARY KEY,
    user_id INT REFERENCES users(id),
    title VARCHAR(200),
    content TEXT,
    created_at DATETIME DEFAULT datetime('now')
);

-- Insert initial data
INSERT INTO users (name, email) VALUES ('Admin', 'admin@example.com');
```

#### Import via API

```bash
curl -X POST http://localhost:8080/api/projects/import \
  -u admin:xxsql \
  -F "project=@myproject.zip"
```

#### Import via Web Interface

1. Navigate to **Projects** page
2. Click **Import ZIP**
3. Select the ZIP file
4. Click **Import**

### Accessing Project Files

Project files are served at:

```
/projects/{project_name}/
/projects/{project_name}/path/to/file
```

Examples:
- `/projects/myapp/` → Serves `index.html`
- `/projects/myapp/static/style.css` → Serves `static/style.css`
- `/projects/myapp/about.html` → Serves `about.html`

### File Management

#### Via Web Interface

1. Navigate to **Projects** → Click **Files** on a project
2. View all files in the project
3. Create new files or folders
4. Edit text files directly
5. Delete files or folders

#### Via API

```bash
# List files
curl -u admin:xxsql http://localhost:8080/api/projects/myapp/files

# Get file content
curl -u admin:xxsql http://localhost:8080/api/projects/myapp/files/index.html

# Create file
curl -X POST -u admin:xxsql \
  -H "Content-Type: application/json" \
  -d '{"path": "new.html", "content": "<h1>New Page</h1>"}' \
  http://localhost:8080/api/projects/myapp/files

# Update file
curl -X PUT -u admin:xxsql \
  -H "Content-Type: application/json" \
  -d '{"content": "<h1>Updated</h1>"}' \
  http://localhost:8080/api/projects/myapp/files/index.html

# Delete file
curl -X DELETE -u admin:xxsql \
  http://localhost:8080/api/projects/myapp/files/old.html

# Create folder
curl -X POST -u admin:xxsql \
  -H "Content-Type: application/json" \
  -d '{"path": "images", "isDir": true}' \
  http://localhost:8080/api/projects/myapp/files
```

## Web Management Interface

The web interface provides complete management capabilities.

### Navigation

| Page | Description |
|------|-------------|
| Dashboard | Server status, metrics |
| Query | Execute SQL queries |
| Tables | Browse tables and schemas |
| Projects | Manage projects and files |
| Microservices | Create and manage microservices |
| Users | User management |
| Backup | Backup and restore |
| Logs | Server and audit logs |
| Config | Server configuration |

### Projects Page Features

- **New Project**: Create empty project
- **Import ZIP**: Import from ZIP file
- **View**: Open project in new tab
- **Files**: File manager
- **Delete**: Delete project (with tables and files)

### Microservices Page Features

- **New Microservice**: Create new endpoint
- **Edit**: Modify script
- **Test**: Test endpoint directly
- **Delete**: Remove microservice

## Complete Examples

### Example 1: Simple Todo List

#### project.json
```json
{
    "name": "todo",
    "version": "1.0.0",
    "tables": "todos",
    "microservices": [
        {
            "skey": "todo/list",
            "script": "var todos = db.query(\"SELECT * FROM todos ORDER BY created_at DESC\"); http.json(todos)",
            "description": "List all todos"
        },
        {
            "skey": "todo/add",
            "script": "var data = http.bodyJSON(); var result = db.exec(\"INSERT INTO todos (title, done) VALUES ('\" + data.title + \"', 0)\"); http.json({\"id\": result.insert_id, \"title\": data.title})",
            "description": "Add new todo"
        },
        {
            "skey": "todo/toggle",
            "script": "var id = http.param(\"id\"); db.exec(\"UPDATE todos SET done = NOT done WHERE id = \" + id); http.json({\"success\": true})",
            "description": "Toggle todo status"
        },
        {
            "skey": "todo/delete",
            "script": "var id = http.param(\"id\"); db.exec(\"DELETE FROM todos WHERE id = \" + id); http.json({\"success\": true})",
            "description": "Delete todo"
        }
    ]
}
```

#### setup.sql
```sql
CREATE TABLE todos (
    id SEQ PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    done BOOL DEFAULT 0,
    created_at DATETIME DEFAULT datetime('now')
);
```

#### index.html
```html
<!DOCTYPE html>
<html>
<head>
    <title>Todo List</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
        .todo { padding: 10px; border: 1px solid #ddd; margin: 5px 0; display: flex; justify-content: space-between; }
        .todo.done { text-decoration: line-through; opacity: 0.6; }
        button { cursor: pointer; }
        input { padding: 10px; font-size: 16px; }
    </style>
</head>
<body>
    <h1>Todo List</h1>

    <form id="addForm">
        <input type="text" id="title" placeholder="New todo..." required>
        <button type="submit">Add</button>
    </form>

    <div id="todos"></div>

    <script>
        async function loadTodos() {
            const resp = await fetch('/ms/_sys_ms/todo/list');
            const todos = await resp.json();
            const container = document.getElementById('todos');
            container.innerHTML = todos.map(t => `
                <div class="todo ${t.done ? 'done' : ''}">
                    <span onclick="toggle(${t.id})">${t.title}</span>
                    <button onclick="del(${t.id})">Delete</button>
                </div>
            `).join('');
        }

        document.getElementById('addForm').onsubmit = async (e) => {
            e.preventDefault();
            const title = document.getElementById('title').value;
            await fetch('/ms/_sys_ms/todo/add', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title})
            });
            document.getElementById('title').value = '';
            loadTodos();
        };

        async function toggle(id) {
            await fetch(`/ms/_sys_ms/todo/toggle?id=${id}`);
            loadTodos();
        }

        async function del(id) {
            await fetch(`/ms/_sys_ms/todo/delete?id=${id}`);
            loadTodos();
        }

        loadTodos();
    </script>
</body>
</html>
```

### Example 2: URL Shortener

#### project.json
```json
{
    "name": "shorturl",
    "version": "1.0.0",
    "tables": "urls",
    "microservices": [
        {
            "skey": "url/shorten",
            "script": "var data = http.bodyJSON(); var code = \"\"; var chars = \"abcdefghijklmnopqrstuvwxyz0123456789\"; for (var i = 0; i < 6; i = i + 1) { code = code + chars[rand(0, len(chars) - 1)] }; db.exec(\"INSERT INTO urls (code, url, created_at) VALUES ('\" + code + \"', '\" + data.url + \"', datetime('now'))\"); http.json({\"code\": code, \"short_url\": \"http://localhost:8080/projects/shorturl/go.html?c=\" + code})",
            "description": "Shorten URL"
        },
        {
            "skey": "url/get",
            "script": "var code = http.param(\"c\"); var row = db.queryRow(\"SELECT url FROM urls WHERE code = '\" + code + \"'\"); if (row == null) { http.status(404); http.json({\"error\": \"Not found\"}) } else { http.json({\"url\": row.url}) }",
            "description": "Get original URL"
        }
    ]
}
```

#### setup.sql
```sql
CREATE TABLE urls (
    id SEQ PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    url TEXT NOT NULL,
    visits INT DEFAULT 0,
    created_at DATETIME
);

CREATE INDEX idx_code ON urls(code);
```

### Example 3: Blog API

#### Microservice: blog/posts (GET, POST)
```javascript
if (http.method == "GET") {
    var page = int(http.param("page")) || 1
    var limit = 10
    var offset = (page - 1) * limit

    var posts = db.query(
        "SELECT p.*, u.name as author FROM posts p " +
        "JOIN users u ON p.user_id = u.id " +
        "ORDER BY p.created_at DESC " +
        "LIMIT " + string(limit) + " OFFSET " + string(offset)
    )

    var total = db.queryRow("SELECT COUNT(*) as count FROM posts")

    http.json({
        posts: posts,
        page: page,
        total: total.count,
        pages: ceil(total.count / limit)
    })
} else if (http.method == "POST") {
    var data = http.bodyJSON()

    if (!data.title || !data.content) {
        http.status(400)
        http.json({error: "title and content required"})
    }

    var result = db.exec(
        "INSERT INTO posts (user_id, title, content, created_at) VALUES (" +
        string(data.user_id) + ", '" + data.title + "', '" +
        data.content + "', datetime('now'))"
    )

    http.status(201)
    http.json({id: result.insert_id, title: data.title})
}
```

## API Reference

### Projects API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/projects` | List all projects |
| POST | `/api/projects` | Create new project |
| DELETE | `/api/projects/{name}` | Delete project |
| POST | `/api/projects/import` | Import project from ZIP |
| GET | `/api/projects/{name}/files` | List project files |
| POST | `/api/projects/{name}/files` | Create file/folder |
| GET | `/api/projects/{name}/files/{path}` | Get file content |
| PUT | `/api/projects/{name}/files/{path}` | Update file |
| DELETE | `/api/projects/{name}/files/{path}` | Delete file |

### Microservices API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/microservices` | List all microservices |
| POST | `/api/microservices` | Create microservice |
| GET | `/api/microservices/{skey}` | Get microservice details |
| PUT | `/api/microservices/{skey}` | Update microservice |
| DELETE | `/api/microservices/{skey}` | Delete microservice |

### Microservice Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| * | `/ms/{table}/{skey}` | Execute microservice |
| * | `/ms/{table}/{skey}?params` | With query parameters |

## Best Practices

### Security

1. **Validate Input**: Always validate user input
```javascript
var id = http.param("id")
if (!id || !isNumeric(id)) {
    http.status(400)
    http.json({error: "Invalid id"})
}
```

2. **Escape SQL Values**: Be careful with SQL injection
```javascript
// Simple escaping for strings
var name = replace(http.param("name"), "'", "''")
var query = "SELECT * FROM users WHERE name = '" + name + "'"
```

3. **Use Authentication**: Check for valid sessions/API keys
```javascript
var apiKey = http.header("X-API-Key")
if (!apiKey) {
    http.status(401)
    http.json({error: "Authentication required"})
}
```

### Performance

1. **Use Indexes**: Create indexes for frequently queried columns
```sql
CREATE INDEX idx_users_email ON users(email);
```

2. **Limit Results**: Always use LIMIT for large queries
```javascript
var users = db.query("SELECT * FROM users LIMIT 100")
```

3. **Avoid N+1 Queries**: Use JOINs instead of multiple queries

### Organization

1. **Naming Convention**: Use descriptive service keys
   - `api/users/list`
   - `api/users/create`
   - `api/users/{id}`

2. **Group Related Services**: Use prefixes
   - `user/profile`
   - `user/settings`
   - `user/avatar`

3. **Version Your API**: Include version in service key
   - `v1/users`
   - `v2/users`