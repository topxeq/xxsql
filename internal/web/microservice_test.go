package web

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestMicroserviceEndpoint(t *testing.T) {
	// Create temp database
	tmpDir := t.TempDir()

	// Create engine
	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	// Create auth manager
	authMgr := auth.NewManager()
	authMgr.CreateUser("admin", "admin", auth.RoleAdmin)

	// Create backup manager
	backupMgr := backup.NewManager(tmpDir)

	// Create config
	cfg := &config.Config{
		Server: config.ServerConfig{
			Name:   "test",
			DataDir: tmpDir,
		},
	}

	// Create server
	srv := NewServer(cfg, engine, authMgr, backupMgr)

	// Create executor
	exec := executor.NewExecutor(engine)
	srv.executor = exec

	// Create microservice table
	if _, err := exec.Execute(`CREATE TABLE scripts (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT)`); err != nil {
		t.Fatalf("Failed to create scripts table: %v", err)
	}

	// Insert test scripts
	testScripts := []struct {
		skey   string
		script string
	}{
		{
			skey: "hello",
			script: `// Simple hello endpoint
http.json({"message": "Hello, World!", "status": "ok"})`,
		},
		{
			skey: "greet",
			script: `// Greet with parameter
var name = http.param("name")
if (name == "") {
    name = "Guest"
}
http.json({"greeting": "Hello, " + name + "!"})`,
		},
		{
			skey: "math",
			script: `// Math operations
var a = int(http.param("a"))
var b = int(http.param("b"))
http.json({
    "a": a,
    "b": b,
    "sum": a + b,
    "product": a * b
})`,
		},
		{
			skey: "dbtest",
			script: `// Database test
var result = db.query("SELECT 1 + 1 AS sum")
http.json({"result": result})`,
		},
	}

	for _, ts := range testScripts {
		if _, err := exec.Execute(`INSERT INTO scripts (SKEY, SCRIPT) VALUES ('` + ts.skey + `', '` + ts.script + `')`); err != nil {
			t.Fatalf("Failed to insert script %s: %v", ts.skey, err)
		}
	}

	tests := []struct {
		name       string
		path       string
		query      string
		expectCode int
		checkBody  func(t *testing.T, body string)
	}{
		{
			name:       "hello endpoint",
			path:       "/ms/scripts/hello",
			expectCode: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(body), &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}
				if result["message"] != "Hello, World!" {
					t.Errorf("Expected message 'Hello, World!', got %v", result["message"])
				}
				if result["status"] != "ok" {
					t.Errorf("Expected status 'ok', got %v", result["status"])
				}
			},
		},
		{
			name:       "greet with name",
			path:       "/ms/scripts/greet",
			query:      "name=Alice",
			expectCode: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(body), &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}
				if result["greeting"] != "Hello, Alice!" {
					t.Errorf("Expected greeting 'Hello, Alice!', got %v", result["greeting"])
				}
			},
		},
		{
			name:       "greet without name",
			path:       "/ms/scripts/greet",
			expectCode: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(body), &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}
				if result["greeting"] != "Hello, Guest!" {
					t.Errorf("Expected greeting 'Hello, Guest!', got %v", result["greeting"])
				}
			},
		},
		{
			name:       "math operations",
			path:       "/ms/scripts/math",
			query:      "a=5&b=3",
			expectCode: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(body), &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}
				if result["sum"].(float64) != 8 {
					t.Errorf("Expected sum 8, got %v", result["sum"])
				}
				if result["product"].(float64) != 15 {
					t.Errorf("Expected product 15, got %v", result["product"])
				}
			},
		},
		{
			name:       "database query",
			path:       "/ms/scripts/dbtest",
			expectCode: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(body), &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}
				// result.result should be an array of rows
				resultArr, ok := result["result"].([]interface{})
				if !ok {
					t.Errorf("Expected result to be array, got %T", result["result"])
					return
				}
				if len(resultArr) == 0 {
					t.Error("Expected at least one row")
					return
				}
				row, ok := resultArr[0].(map[string]interface{})
				if !ok {
					t.Errorf("Expected row to be object, got %T", resultArr[0])
					return
				}
				if row["sum"].(float64) != 2 {
					t.Errorf("Expected sum 2, got %v", row["sum"])
				}
			},
		},
		{
			name:       "nonexistent script",
			path:       "/ms/scripts/notfound",
			expectCode: http.StatusNotFound,
		},
		{
			name:       "invalid path",
			path:       "/ms/scripts",
			expectCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := tt.path
			if tt.query != "" {
				url += "?" + tt.query
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()

			srv.handleMicroservice(rec, req)

			resp := rec.Result()
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectCode {
				t.Errorf("Expected status %d, got %d", tt.expectCode, resp.StatusCode)
			}

			if tt.checkBody != nil {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("Failed to read body: %v", err)
				}
				tt.checkBody(t, string(body))
			}
		})
	}
}

func TestMicroserviceWithHTTPMethods(t *testing.T) {
	// Create temp database
	tmpDir := t.TempDir()

	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	authMgr := auth.NewManager()
	backupMgr := backup.NewManager(tmpDir)
	cfg := &config.Config{
		Server: config.ServerConfig{
			Name:   "test",
			DataDir: tmpDir,
		},
	}
	srv := NewServer(cfg, engine, authMgr, backupMgr)
	exec := executor.NewExecutor(engine)
	srv.executor = exec

	// Create table and script
	exec.Execute(`CREATE TABLE api (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT)`)
	exec.Execute(`INSERT INTO api (SKEY, SCRIPT) VALUES ('method', 'http.json({"method": http.method, "path": http.path})')`)

	tests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/ms/api/method"},
		{method: http.MethodPost, path: "/ms/api/method"},
		{method: http.MethodPut, path: "/ms/api/method"},
		{method: http.MethodDelete, path: "/ms/api/method"},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()

			srv.handleMicroservice(rec, req)

			resp := rec.Result()
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			var result map[string]interface{}
			if err := json.Unmarshal(body, &result); err != nil {
				t.Fatalf("Failed to parse JSON: %v", err)
			}

			if result["method"] != tt.method {
				t.Errorf("Expected method %s, got %v", tt.method, result["method"])
			}
			if result["path"] != tt.path {
				t.Errorf("Expected path %s, got %v", tt.path, result["path"])
			}
		})
	}
}

func TestMicroserviceWithComplexScript(t *testing.T) {
	// Create temp database
	tmpDir := t.TempDir()

	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	authMgr := auth.NewManager()
	backupMgr := backup.NewManager(tmpDir)
	cfg := &config.Config{
		Server: config.ServerConfig{
			Name:   "test",
			DataDir: tmpDir,
		},
	}
	srv := NewServer(cfg, engine, authMgr, backupMgr)
	exec := executor.NewExecutor(engine)
	srv.executor = exec

	// Create table with data
	exec.Execute(`CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)`)
	exec.Execute(`INSERT INTO users VALUES (1, 'Alice', 30)`)
	exec.Execute(`INSERT INTO users VALUES (2, 'Bob', 25)`)
	exec.Execute(`INSERT INTO users VALUES (3, 'Charlie', 35)`)

	// Create microservice table
	exec.Execute(`CREATE TABLE api (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT)`)

	// Script that queries users and returns JSON
	complexScript := `
var minAge = int(http.param("min_age"))
if (minAge == 0) {
    minAge = 0
}

var users = db.query("SELECT id, name, age FROM users WHERE age >= " + string(minAge) + " ORDER BY age")

var result = {
    "query": "users with age >= " + string(minAge),
    "count": len(users),
    "users": users
}

http.json(result)
`
	exec.Execute(`INSERT INTO api (SKEY, SCRIPT) VALUES ('users', '` + complexScript + `')`)

	// Test without filter
	t.Run("all users", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ms/api/users", nil)
		rec := httptest.NewRecorder()

		srv.handleMicroservice(rec, req)

		resp := rec.Result()
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("Failed to parse JSON: %v", err)
		}

		if result["count"].(float64) != 3 {
			t.Errorf("Expected count 3, got %v", result["count"])
		}
	})

	// Test with age filter
	t.Run("users over 28", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ms/api/users?min_age=28", nil)
		rec := httptest.NewRecorder()

		srv.handleMicroservice(rec, req)

		resp := rec.Result()
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("Failed to parse JSON: %v", err)
		}

		if result["count"].(float64) != 2 {
			t.Errorf("Expected count 2, got %v", result["count"])
		}
	})
}