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
├── index.html      # Web interface (static file)
└── README.md       # This file
```

## Quick Start

### 1. Start XxSql Server

```bash
xxsqls -config your-config.json
```

### 2. Set Up the Project

Using xxsqlc client:

```bash
# Connect to server
xxsqlc -host 127.0.0.1 -port 9527

# Execute setup script
\i setup.sql
```

Or via HTTP API:

```bash
# Login first
curl -c cookies.txt -X POST -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"your_password"}' \
  http://localhost:8080/api/login

# Execute setup script
curl -b cookies.txt -X POST -H "Content-Type: application/json" \
  -d '{"sql":"CREATE TABLE IF NOT EXISTS api (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT);"}' \
  http://localhost:8080/api/query
```

### 3. Upload Static Files

Place `index.html` in the static files directory configured in your XxSql server (typically `<data_dir>/static/`).

### 4. Access the Service

- **Web Interface**: `http://localhost:8080/` (serves index.html)
- **API Endpoint**: `POST http://localhost:8080/ms/api/md5`

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
  "service": "md5Server"
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