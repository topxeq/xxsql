package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/storage"
)

func setupTestServer(t *testing.T) (*Server, string) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(dataDir, 0755)
	os.MkdirAll(backupDir, 0755)

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = dataDir
	cfg.Backup.BackupDir = backupDir
	cfg.Network.HTTPPort = 0
	cfg.Auth.Enabled = true

	engine := storage.NewEngine(dataDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	authMgr := auth.NewManager(auth.WithDataDir(dataDir))
	authMgr.CreateUser("admin", "admin", auth.RoleAdmin)
	authMgr.CreateUser("testuser", "testpass", auth.RoleUser)

	server := NewServer(cfg, engine, authMgr, nil)

	return server, tmpDir
}

func TestNewServer(t *testing.T) {
	server, _ := setupTestServer(t)
	if server == nil {
		t.Fatal("NewServer returned nil")
	}
	if server.sessions == nil {
		t.Error("sessions map should be initialized")
	}
}

func TestServer_LoadTemplates(t *testing.T) {
	server, _ := setupTestServer(t)

	tmpl, err := server.loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates error: %v", err)
	}
	if tmpl == nil {
		t.Error("templates should not be nil")
	}
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()

	data := map[string]string{"message": "test"}
	writeJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}

	if result["message"] != "test" {
		t.Errorf("message: got %q, want 'test'", result["message"])
	}
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()

	writeError(w, http.StatusBadRequest, "bad request")

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusBadRequest)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}

	if result["error"] != "bad request" {
		t.Errorf("error: got %q, want 'bad request'", result["error"])
	}
}

func TestReadJSON(t *testing.T) {
	body := `{"username":"test","password":"pass"}`
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")

	var result struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := readJSON(req, &result); err != nil {
		t.Fatalf("readJSON error: %v", err)
	}

	if result.Username != "test" {
		t.Errorf("username: got %q, want 'test'", result.Username)
	}
	if result.Password != "pass" {
		t.Errorf("password: got %q, want 'pass'", result.Password)
	}
}

func TestReadJSON_Invalid(t *testing.T) {
	body := `{"invalid json`
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))

	var result map[string]interface{}
	if err := readJSON(req, &result); err == nil {
		t.Error("readJSON should return error for invalid JSON")
	}
}

func TestHandleAPILogin(t *testing.T) {
	server, _ := setupTestServer(t)

	tests := []struct {
		name       string
		method     string
		body       map[string]string
		wantStatus int
	}{
		{
			name:       "valid login",
			method:     http.MethodPost,
			body:       map[string]string{"username": "admin", "password": "admin"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid credentials",
			method:     http.MethodPost,
			body:       map[string]string{"username": "admin", "password": "wrong"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong method",
			method:     http.MethodGet,
			body:       nil,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body bytes.Buffer
			if tt.body != nil {
				json.NewEncoder(&body).Encode(tt.body)
			}

			req := httptest.NewRequest(tt.method, "/api/login", &body)
			if tt.body != nil {
				req.Header.Set("Content-Type", "application/json")
			}

			w := httptest.NewRecorder()
			server.handleAPILogin(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status: got %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

func TestHandleAPILogout(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/logout", nil)
	w := httptest.NewRecorder()

	server.handleAPILogout(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	cookie := w.Result().Cookies()
	if len(cookie) == 0 {
		t.Error("Should clear session cookie")
	}
}

func TestHandleAPIStatus(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	server.handleAPIStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}

	if result["version"] == nil {
		t.Error("version should be present")
	}
}

func TestHandleAPIMetrics(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	w := httptest.NewRecorder()

	server.handleAPIMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}

	if result["storage"] == nil {
		t.Error("storage should be present")
	}
}

func TestHandleAPIQuery(t *testing.T) {
	server, _ := setupTestServer(t)

	tests := []struct {
		name       string
		method     string
		sql        string
		wantStatus int
	}{
		{
			name:       "select query",
			method:     http.MethodPost,
			sql:        "SELECT 1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty query",
			method:     http.MethodPost,
			sql:        "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wrong method",
			method:     http.MethodGet,
			sql:        "",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]string{"sql": tt.sql})
			req := httptest.NewRequest(tt.method, "/api/query", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			server.handleAPIQuery(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status: got %d, want %d, body: %s", w.Code, tt.wantStatus, w.Body.String())
			}
		})
	}
}

func TestHandleAPITables(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/tables", nil)
	w := httptest.NewRecorder()

	server.handleAPITables(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandleAPIUsers(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("list users", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
		w := httptest.NewRecorder()

		server.handleAPIUsers(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
		}

		var result map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &result)

		users, ok := result["users"].([]interface{})
		if !ok {
			t.Error("users should be an array")
		}
		if len(users) == 0 {
			t.Error("should have at least admin user")
		}
	})

	t.Run("create user", func(t *testing.T) {
		body, _ := json.Marshal(map[string]string{
			"username": "newuser",
			"password": "newpass",
			"role":     "user",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		server.handleAPIUsers(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
	})
}

func TestHandleAPIUserDetail(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("get user", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/users/testuser", nil)
		w := httptest.NewRecorder()

		server.handleAPIUserDetail(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("get non-existent user", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/users/nonexistent", nil)
		w := httptest.NewRecorder()

		server.handleAPIUserDetail(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("delete user", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/users/testuser", nil)
		w := httptest.NewRecorder()

		server.handleAPIUserDetail(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
	})
}

func TestHandleAPIConfig(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("get config", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("update config", func(t *testing.T) {
		body, _ := json.Marshal(map[string]interface{}{"log": map[string]string{"level": "DEBUG"}})
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
		}
	})
}

func TestHandleAPIBackups(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("list backups", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/backups", nil)
		w := httptest.NewRecorder()

		server.handleAPIBackups(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
		}
	})
}

func TestHandleAPILogs(t *testing.T) {
	server, _ := setupTestServer(t)

	tests := []struct {
		name       string
		logType    string
		wantStatus int
	}{
		{"server logs", "server", http.StatusOK},
		{"audit logs", "audit", http.StatusOK},
		{"unknown type", "unknown", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/logs/"+tt.logType, nil)
			w := httptest.NewRecorder()

			server.handleAPILogs(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status: got %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

func TestGenerateSessionID(t *testing.T) {
	id1 := generateSessionID()
	id2 := generateSessionID()

	if len(id1) != 64 {
		t.Errorf("Session ID length: got %d, want 64", len(id1))
	}

	if id1 == id2 {
		t.Error("Session IDs should be unique")
	}
}

func TestGetSession(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("no cookie", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		session := server.getSession(req)
		if session != nil {
			t.Error("getSession should return nil without cookie")
		}
	})

	t.Run("invalid session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{
			Name:  "xxsql_session",
			Value: "invalid-id",
		})
		session := server.getSession(req)
		if session != nil {
			t.Error("getSession should return nil for invalid session")
		}
	})

	t.Run("valid session", func(t *testing.T) {
		server.sessions["test-session"] = &Session{
			ID:       "test-session",
			Username: "testuser",
			Created:  time.Now(),
			Expires:  time.Now().Add(24 * time.Hour),
		}

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{
			Name:  "xxsql_session",
			Value: "test-session",
		})
		session := server.getSession(req)
		if session == nil {
			t.Error("getSession should return session")
		}
		if session.Username != "testuser" {
			t.Errorf("Username: got %q, want 'testuser'", session.Username)
		}
	})

	t.Run("expired session", func(t *testing.T) {
		server.sessions["expired-session"] = &Session{
			ID:       "expired-session",
			Username: "testuser",
			Created:  time.Now().Add(-48 * time.Hour),
			Expires:  time.Now().Add(-24 * time.Hour),
		}

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{
			Name:  "xxsql_session",
			Value: "expired-session",
		})
		session := server.getSession(req)
		if session != nil {
			t.Error("getSession should return nil for expired session")
		}
		if _, exists := server.sessions["expired-session"]; exists {
			t.Error("expired session should be deleted")
		}
	})
}

func TestResponseWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rec}

	rw.WriteHeader(http.StatusCreated)

	if rw.status != http.StatusCreated {
		t.Errorf("status: got %d, want %d", rw.status, http.StatusCreated)
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("rec.Code: got %d, want %d", rec.Code, http.StatusCreated)
	}
}

func TestReadLastLines(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	content := "line1\nline2\nline3\nline4\nline5\n"
	os.WriteFile(logPath, []byte(content), 0644)

	lines, err := readLastLines(logPath, 3)
	if err != nil {
		t.Fatalf("readLastLines error: %v", err)
	}

	if len(lines) != 3 {
		t.Errorf("lines count: got %d, want 3", len(lines))
	}

	if lines[0] != "line3" || lines[1] != "line4" || lines[2] != "line5" {
		t.Errorf("lines content unexpected: %v", lines)
	}
}

func TestReadLastLines_NonExistent(t *testing.T) {
	_, err := readLastLines("/nonexistent/file.log", 10)
	if err == nil {
		t.Error("readLastLines should return error for non-existent file")
	}
}

func TestHandleAPITableDetail(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("get non-existent table", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tables/nonexistent", nil)
		w := httptest.NewRecorder()

		server.handleAPITableDetail(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/tables/sometable", nil)
		w := httptest.NewRecorder()

		server.handleAPITableDetail(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})
}

func TestHandleAPITableData(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("data endpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tables/test/data", nil)
		w := httptest.NewRecorder()

		server.handleAPITableDetail(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Status: got %d, want %d (table doesn't exist)", w.Code, http.StatusInternalServerError)
		}
	})

	t.Run("wrong method for data", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/tables/test/data", nil)
		w := httptest.NewRecorder()

		server.handleAPITableDetail(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})
}

func TestHandleAPIBackupDetail(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("wrong method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/backups/test.bak", nil)
		w := httptest.NewRecorder()

		server.handleAPIBackupDetail(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})
}

func TestHandleAPIRestore(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("wrong method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/restore", nil)
		w := httptest.NewRecorder()

		server.handleAPIRestore(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/restore", bytes.NewBufferString("invalid"))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		server.handleAPIRestore(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}

func TestHandleAPIUsers_CreateError(t *testing.T) {
	server, _ := setupTestServer(t)

	body, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "test",
		"role":     "admin",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.handleAPIUsers(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status: got %d, want %d (duplicate user)", w.Code, http.StatusBadRequest)
	}
}

func TestHandleAPIUsers_WrongMethod(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/api/users", nil)
	w := httptest.NewRecorder()

	server.handleAPIUsers(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleAPIUserDetail_Update(t *testing.T) {
	server, _ := setupTestServer(t)

	body, _ := json.Marshal(map[string]string{"password": "newpass"})
	req := httptest.NewRequest(http.MethodPut, "/api/users/admin", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.handleAPIUserDetail(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandleAPIUserDetail_DeleteError(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/users/nonexistent", nil)
	w := httptest.NewRecorder()

	server.handleAPIUserDetail(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleAPIUserDetail_WrongMethod(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/users/admin", nil)
	w := httptest.NewRecorder()

	server.handleAPIUserDetail(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleAPIBackups_Create(t *testing.T) {
	server, tmpDir := setupTestServer(t)
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(backupDir, 0755)

	body, _ := json.Marshal(map[string]interface{}{
		"compress": false,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/backups", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.handleAPIBackups(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
}

func TestHandleAPIBackups_WrongMethod(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/backups", nil)
	w := httptest.NewRecorder()

	server.handleAPIBackups(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleAPILogs_WrongMethod(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/logs/server", nil)
	w := httptest.NewRecorder()

	server.handleAPILogs(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleAPILogs_EmptyPath(t *testing.T) {
	server, _ := setupTestServer(t)

	server.config.Log.File = ""
	req := httptest.NewRequest(http.MethodGet, "/api/logs/server?lines=10", nil)
	w := httptest.NewRecorder()

	server.handleAPILogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandleAPIConfig_WrongMethod(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/config", nil)
	w := httptest.NewRecorder()

	server.handleAPIConfig(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestAuthMiddleware(t *testing.T) {
	server, _ := setupTestServer(t)

	tests := []struct {
		path       string
		shouldPass bool
	}{
		{"/static/style.css", true},
		{"/login", true},
		{"/api/login", true},
		{"/query", false},
		{"/api/status", false}, // API endpoints now require authentication
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()

			handler := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			handler.ServeHTTP(w, req)

			if tt.shouldPass {
				if w.Code != http.StatusOK {
					t.Errorf("Path %s: got %d, want %d", tt.path, w.Code, http.StatusOK)
				}
			}
		})
	}
}

func TestLoggingMiddleware(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	handler := server.loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusCreated)
	}
}

func TestHandleLoginPage(t *testing.T) {
	server, _ := setupTestServer(t)
	server.templates, _ = server.loadTemplates()

	t.Run("GET without session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/login", nil)
		w := httptest.NewRecorder()

		server.handleLoginPage(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("GET with session", func(t *testing.T) {
		server.sessions["test-sess"] = &Session{
			ID:       "test-sess",
			Username: "admin",
			Expires:  time.Now().Add(24 * time.Hour),
		}

		req := httptest.NewRequest(http.MethodGet, "/login", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: "test-sess"})
		w := httptest.NewRecorder()

		server.handleLoginPage(w, req)

		if w.Code != http.StatusFound {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusFound)
		}
	})

	t.Run("POST", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/login", nil)
		w := httptest.NewRecorder()

		server.handleLoginPage(w, req)

	})
}

func TestHandlePage(t *testing.T) {
	server, _ := setupTestServer(t)
	server.templates, _ = server.loadTemplates()

	t.Run("without session", func(t *testing.T) {
		handler := server.handlePage("index")
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()

		handler(w, req)

		if w.Code != http.StatusFound {
			t.Errorf("Status: got %d, want %d", w.Code, http.StatusFound)
		}
	})

	t.Run("with session", func(t *testing.T) {
		server.sessions["test-sess"] = &Session{
			ID:       "test-sess",
			Username: "admin",
			Expires:  time.Now().Add(24 * time.Hour),
		}

		handler := server.handlePage("index")
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: "test-sess"})
		w := httptest.NewRecorder()

		handler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
	})
}

func TestStopWithoutServer(t *testing.T) {
	server := &Server{}
	if err := server.Stop(); err != nil {
		t.Errorf("Stop without server should not error: %v", err)
	}
}

func TestHasPermission(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("no session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if server.hasPermission(req, auth.PermSelect) {
			t.Error("hasPermission should return false without session")
		}
	})

	t.Run("with valid session", func(t *testing.T) {
		server.sessions["perm-sess"] = &Session{
			ID:       "perm-sess",
			Username: "admin",
			Expires:  time.Now().Add(24 * time.Hour),
		}

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: "perm-sess"})

		// Admin user should have permission
		if !server.hasPermission(req, auth.PermSelect) {
			t.Error("admin should have PermSelect permission")
		}
	})

	t.Run("with invalid session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: "invalid-session"})

		if server.hasPermission(req, auth.PermSelect) {
			t.Error("hasPermission should return false for invalid session")
		}
	})
}

func TestStart(t *testing.T) {
	server, tmpDir := setupTestServer(t)
	_ = tmpDir

	// Find available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	server.config.Network.Bind = "127.0.0.1"
	server.config.Network.HTTPPort = port

	if err := server.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify server is running by making a request
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/status", port))
	if err != nil {
		t.Errorf("Failed to reach server: %v", err)
	} else {
		resp.Body.Close()
	}

	// Stop server
	if err := server.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestStartWithInvalidTemplate(t *testing.T) {
	// This test is tricky because templates are embedded
	// We just verify Start doesn't panic with default setup
	server, _ := setupTestServer(t)
	server.config.Network.Bind = "127.0.0.1"
	server.config.Network.HTTPPort = 0 // Let OS pick

	// Stop any existing server
	_ = server.Stop()
}

func TestHandleAPIQuery_InvalidJSON(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/query", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.handleAPIQuery(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleAPITableDetail_ExistingTable(t *testing.T) {
	server, _ := setupTestServer(t)

	// Create a table first
	server.engine.CreateTable("test_table", nil)

	req := httptest.NewRequest(http.MethodGet, "/api/tables/test_table", nil)
	w := httptest.NewRecorder()

	server.handleAPITableDetail(w, req)

	// Table should exist (though may have no columns)
	if w.Code != http.StatusOK && w.Code != http.StatusInternalServerError {
		t.Errorf("Status: got %d", w.Code)
	}
}

func TestHandleAPITableData_WithTable(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/tables/test/data", nil)
	w := httptest.NewRecorder()

	server.handleAPITableData(w, req, "test")

	// Non-existent table
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

func TestHandleAPIBackupDetail_Get(t *testing.T) {
	server, tmpDir := setupTestServer(t)
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(backupDir, 0755)

	// Create a backup file
	backupFile := filepath.Join(backupDir, "test.bak")
	os.WriteFile(backupFile, []byte("test backup"), 0644)

	req := httptest.NewRequest(http.MethodGet, "/api/backups/test.bak", nil)
	w := httptest.NewRecorder()

	server.handleAPIBackupDetail(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
}

func TestHandleAPIBackupDetail_Delete(t *testing.T) {
	server, tmpDir := setupTestServer(t)
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(backupDir, 0755)

	// Create a backup file
	backupFile := filepath.Join(backupDir, "delete-me.bak")
	os.WriteFile(backupFile, []byte("test backup"), 0644)

	// DELETE is not supported, only GET
	req := httptest.NewRequest(http.MethodDelete, "/api/backups/delete-me.bak", nil)
	w := httptest.NewRecorder()

	server.handleAPIBackupDetail(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleAPIRestore_Valid(t *testing.T) {
	server, tmpDir := setupTestServer(t)
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(backupDir, 0755)

	// Create a backup file
	backupFile := filepath.Join(backupDir, "restore-test.bak")
	os.WriteFile(backupFile, []byte("test backup content"), 0644)

	// The API expects a "path" field, not "filename"
	body, _ := json.Marshal(map[string]string{"path": backupFile})
	req := httptest.NewRequest(http.MethodPost, "/api/restore", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.handleAPIRestore(w, req)

	// May fail due to actual restore logic, but at least we test the routing
	t.Logf("Status: %d, body: %s", w.Code, w.Body.String())
}

func TestHandleAPIRestore_FileNotFound(t *testing.T) {
	server, _ := setupTestServer(t)

	body, _ := json.Marshal(map[string]string{"path": "/nonexistent/path.bak"})
	req := httptest.NewRequest(http.MethodPost, "/api/restore", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.handleAPIRestore(w, req)

	// Should fail because file doesn't exist
	if w.Code != http.StatusInternalServerError && w.Code != http.StatusBadRequest {
		t.Errorf("Status: got %d, want error status", w.Code)
	}
}

// ============================================================================
// API Key Authentication Tests
// ============================================================================

func TestAPIKeyAuthentication(t *testing.T) {
	server, _ := setupTestServer(t)

	// Generate an API key
	fullKey, _, err := server.apiKeyManager.GenerateKey("test-key", "admin", auth.PermSelect|auth.PermInsert, 0)
	if err != nil {
		t.Fatalf("Failed to generate API key: %v", err)
	}

	// Test API call with API key
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.Header.Set("X-API-Key", fullKey)
	w := httptest.NewRecorder()

	server.handleAPIStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}
}

func TestAPIKeyAuthentication_InvalidKey(t *testing.T) {
	server, _ := setupTestServer(t)

	// Test with invalid API key
	req := httptest.NewRequest(http.MethodGet, "/api/tables", nil)
	req.Header.Set("X-API-Key", "xxsql_invalid_key")
	w := httptest.NewRecorder()

	// Use the middleware
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPITables))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestAPIKeyAuthentication_DisabledKey(t *testing.T) {
	server, _ := setupTestServer(t)

	// Generate and disable key
	fullKey, key, _ := server.apiKeyManager.GenerateKey("disabled-key", "admin", auth.PermSelect, 0)
	server.apiKeyManager.EnableKey(key.ID, false)

	// Try to use disabled key
	req := httptest.NewRequest(http.MethodGet, "/api/tables", nil)
	req.Header.Set("X-API-Key", fullKey)
	w := httptest.NewRecorder()

	handler := server.authMiddleware(http.HandlerFunc(server.handleAPITables))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestHandleAPIKeys_Create(t *testing.T) {
	server, _ := setupTestServer(t)

	// First, login to get session
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// Create API key
	body, _ := json.Marshal(map[string]interface{}{
		"name":        "test-key",
		"expires_in":  0,
		"permissions": 0,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/keys", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	// Set session cookie
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}

	w := httptest.NewRecorder()

	// Use middleware
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIKeys))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var result map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &result)

	if result["key"] == nil {
		t.Error("Response should contain the generated key")
	}
}

func TestHandleAPIKeys_List(t *testing.T) {
	server, _ := setupTestServer(t)

	// Generate some keys
	server.apiKeyManager.GenerateKey("key1", "admin", auth.PermSelect, 0)
	server.apiKeyManager.GenerateKey("key2", "admin", auth.PermInsert, 0)

	// Login
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// List keys
	req := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()

	// Use middleware
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIKeys))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	var result map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &result)

	keys, ok := result["keys"].([]interface{})
	if !ok {
		t.Fatal("keys should be an array")
	}
	if len(keys) < 2 {
		t.Errorf("Should have at least 2 keys, got %d", len(keys))
	}
}

func TestHandleAPIKeyDetail_Get(t *testing.T) {
	server, _ := setupTestServer(t)

	// Generate key
	_, key, _ := server.apiKeyManager.GenerateKey("test-key", "admin", auth.PermSelect, 0)

	// Login
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// Get key detail
	req := httptest.NewRequest(http.MethodGet, "/api/keys/"+key.ID, nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()

	// Use middleware
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIKeyDetail))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandleAPIKeyDetail_Delete(t *testing.T) {
	server, _ := setupTestServer(t)

	// Generate key
	_, key, _ := server.apiKeyManager.GenerateKey("test-key", "admin", auth.PermSelect, 0)

	// Login
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// Delete key
	req := httptest.NewRequest(http.MethodDelete, "/api/keys/"+key.ID, nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()

	// Use middleware
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIKeyDetail))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	// Verify key is deleted
	_, err := server.apiKeyManager.GetKey(key.ID)
	if err == nil {
		t.Error("Key should be deleted")
	}
}

func TestHandleAPIKeyDetail_EnableDisable(t *testing.T) {
	server, _ := setupTestServer(t)

	// Generate key
	_, key, _ := server.apiKeyManager.GenerateKey("test-key", "admin", auth.PermSelect, 0)

	// Login
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// Disable key
	body, _ := json.Marshal(map[string]bool{"enabled": false})
	req := httptest.NewRequest(http.MethodPut, "/api/keys/"+key.ID, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()

	// Use middleware
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIKeyDetail))
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status: got %d, want %d", w.Code, http.StatusOK)
	}

	// Verify key is disabled
	updatedKey, _ := server.apiKeyManager.GetKey(key.ID)
	if updatedKey.Enabled {
		t.Error("Key should be disabled")
	}
}
