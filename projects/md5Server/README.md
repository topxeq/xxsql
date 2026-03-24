# md5Server - MD5 Hash Generator Microservice

A simple web-based MD5 hash generation service built with XxSql.

## Features

- Web interface for generating MD5, SHA256, and SHA512 hashes
- REST API endpoint for programmatic access
- Copy hash to clipboard with one click
- Clean, modern UI

## Project Structure

```
md5Server/
├── project.json    # Project configuration
├── setup.sql       # SQL script to set up the microservice
├── static/         # Static files directory
│   └── index.html  # Web interface
└── README.md       # This file
```

## Quick Start

### 1. Start XxSql Server

```bash
xxsqls -config your-config.json
```

### 2. Deploy the Project

Using xxsqlc client with --project flag:

```bash
xxsqlc --project ./projects/md5Server -host 127.0.0.1 -port 9527 -u admin -p your_password
```

This will:
1. Execute `setup.sql` to create tables and microservice endpoints
2. Upload static files to the server
3. Register the project in the system

### 3. Access the Service

- **Web Interface**: `GET /ms/_sys_ms/file/serve?path=projects/md5Server/index.html`
- **API Endpoint**: `POST /ms/api/md5`

## API Usage

### Generate MD5 Hash

**Request:**
```http
POST /ms/api/md5
Content-Type: application/json

{"text": "hello world"}
```

**Response:**
```json
{
  "hash": "5eb63bbbe01eeed093cb22bb8f5acdc3",
  "text": "hello world"
}
```

### Health Check

**Request:**
```http
GET /ms/api/health
```

**Response:**
```json
{
  "status": "ok",
  "service": "xxsql"
}
```

## Using SQL Functions Directly

You can also use the hash functions directly in SQL:

```sql
SELECT MD5('hello');        -- 5d41402abc4b2a76b9719d911017c592
SELECT SHA256('hello');     -- 2cf24dba5fb0a30e26e83b2ac5b9e29e...
SELECT SHA512('hello');     -- 9b71d224bd62f3785d96d46ad3ea3d...
```

## How It Works

1. **Microservice Table**: The `api` table stores XxScript code for each endpoint
2. **SKEY**: The route key (e.g., 'md5') becomes part of the URL path
3. **SCRIPT**: XxScript code that handles the HTTP request and returns a response

The web interface uses XxSql's HTTP API to execute SQL hash functions directly.

## Manual Setup (Alternative)

If you prefer to set up manually:

```bash
# Execute setup script
xxsqlc -host 127.0.0.1 -port 9527 -f setup.sql

# Static files are stored in <data_dir>/projects/md5Server/
```