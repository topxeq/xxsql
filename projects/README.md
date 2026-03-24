# XxSql Example Projects

This directory contains example projects demonstrating how to build microservices and applications using XxSql.

## Project Structure

Each project folder contains:

| File | Description |
|------|-------------|
| `project.json` | Project configuration and metadata |
| `setup.sql` | SQL script to create tables and microservice endpoints |
| `*.html` | Static web files |
| `README.md` | Project documentation |

## Available Projects

| Project | Description |
|---------|-------------|
| [md5Server](./md5Server/) | Simple MD5/SHA hash generation microservice with web UI |

## Setting Up a Project

### Method 1: Using xxsqlc Client

```bash
# Start xxsqlc and connect to server
xxsqlc -host 127.0.0.1 -port 9527

# Execute setup script
\i setup.sql
```

### Method 2: Using HTTP API

```bash
# Login
curl -c cookies.txt -X POST -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"your_password"}' \
  http://localhost:8080/api/login

# Execute SQL
curl -b cookies.txt -X POST -H "Content-Type: application/json" \
  -d @setup.sql \
  http://localhost:8080/api/query
```

### Method 3: Using Web Admin

1. Open `http://localhost:8080/` in your browser
2. Login with admin credentials
3. Navigate to SQL Editor
4. Paste and execute the setup.sql content

## Deploying Static Files

Static files (HTML, CSS, JS, images) should be placed in:

```
<data_directory>/static/
```

For example, if your data directory is `/var/xxsql/data`:

```bash
cp index.html /var/xxsql/data/static/
```

The file will then be accessible at `http://localhost:8080/index.html`.

## Creating a New Project

1. Create a new folder: `mkdir myproject`
2. Create `project.json` with project metadata
3. Create `setup.sql` with table definitions and microservice scripts
4. Create any static files needed (HTML, etc.)
5. Document in `README.md`

## Microservice Development

XxSql microservices use XxScript stored in database tables. See the [XxScript documentation](../docs/xxscript.md) for details.

Basic pattern:

```sql
CREATE TABLE api (
    SKEY VARCHAR(50) PRIMARY KEY,
    SCRIPT TEXT
);

INSERT INTO api (SKEY, SCRIPT) VALUES ('myendpoint', '
    var data = http.bodyJSON()
    http.json({"result": "hello"})
');
```

Access at: `POST /ms/api/myendpoint`