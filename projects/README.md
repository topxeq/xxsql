# XxSql Example Projects

This directory contains example projects demonstrating how to build microservices and applications using XxSql.

## Project Structure

Each project folder contains:

| File | Description |
|------|-------------|
| `project.json` | Project configuration and metadata |
| `setup.sql` | SQL script to create tables and microservice endpoints |
| `static/` | Static web files directory |
| `README.md` | Project documentation |

## Available Projects

| Project | Description |
|---------|-------------|
| [md5Server](./md5Server/) | Simple MD5/SHA hash generation microservice with web UI |

## Quick Deploy

Use xxsqlc with the `--project` flag to deploy a project:

```bash
xxsqlc --project ./projects/md5Server -host localhost -port 9527 -u admin -p password
```

This will:
1. Read `project.json` for project metadata
2. Execute `setup.sql` to create tables and microservices
3. Upload all files from `static/` to `projects/<project_name>/`
4. Register the project in `_sys_projects` table

## Project Configuration

### project.json

```json
{
  "name": "projectName",
  "version": "1.0.0",
  "description": "Project description",
  "tables": "table1,table2"
}
```

### setup.sql

Contains SQL statements to:
- Create tables needed by the project
- Insert microservice scripts into tables

```sql
CREATE TABLE api (
    SKEY VARCHAR(50) PRIMARY KEY,
    SCRIPT TEXT
);

INSERT INTO api (SKEY, SCRIPT) VALUES ('endpoint', '
    // XxScript code here
');
```

### Static Files

Files in `static/` are uploaded to `<data_dir>/projects/<project_name>/`.

Access at: `http://localhost:8080/projects/<project_name>/filename.html`

## System Tables

XxSql automatically creates these system tables on first startup:

| Table | Purpose |
|-------|---------|
| `_sys_ms` | System microservice scripts |
| `_sys_projects` | Installed projects registry |

## System Microservices

Preset microservices for file operations:

| Endpoint | Description |
|----------|-------------|
| `POST /ms/_sys/file/upload` | Upload text file |
| `POST /ms/_sys/file/uploadBinary` | Upload binary file (base64) |
| `GET /ms/_sys/file/read?path=...` | Read file |
| `POST /ms/_sys/file/delete` | Delete file |
| `GET /ms/_sys/dir/list?path=...` | List directory |
| `POST /ms/_sys/dir/create` | Create directory |
| `POST /ms/_sys/project/check` | Check if project installed |
| `POST /ms/_sys/project/register` | Register installed project |

## Creating a New Project

1. Create a new folder: `mkdir projects/myproject`
2. Create `project.json` with project metadata
3. Create `setup.sql` with table definitions and microservice scripts
4. Create `static/` directory with web files
5. Document in `README.md`

## Project Isolation

To avoid conflicts between projects:

- Tables should use project-specific prefixes (e.g., `md5_api` instead of `api`)
- Static files are automatically namespaced under `projects/<project_name>/`
- Each project is registered with a unique name in `_sys_projects`

## File Storage

Project files are stored at:

```
<data_dir>/projects/
├── md5Server/
│   └── index.html
└── anotherProject/
    └── static files...
```

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