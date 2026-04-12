package web

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/executor"
)

func makeMultipartRequest(t *testing.T, method, url, fieldName, fileName string, fileData []byte) *http.Request {
	t.Helper()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if fieldName != "" {
		part, err := writer.CreateFormFile(fieldName, fileName)
		if err != nil {
			t.Fatalf("create form file: %v", err)
		}
		if _, err := part.Write(fileData); err != nil {
			t.Fatalf("write form file: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(method, url, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func makeZipData(t *testing.T, files map[string]string) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	for name, content := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatalf("create zip entry %s: %v", name, err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatalf("write zip entry %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}

	return buf.Bytes()
}

func TestHandleAPIProjectImport_Branches(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/projects/import", nil)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("missing file in multipart", func(t *testing.T) {
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "", "", nil)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
		if !strings.Contains(w.Body.String(), "no file uploaded") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("invalid zip", func(t *testing.T) {
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "bad.zip", []byte("not-a-zip"))
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
		if !strings.Contains(w.Body.String(), "failed to extract zip") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("project json required", func(t *testing.T) {
		zipData := makeZipData(t, map[string]string{"README.md": "demo"})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "project.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
		if !strings.Contains(w.Body.String(), "project.json not found") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("project name required", func(t *testing.T) {
		zipData := makeZipData(t, map[string]string{
			"demo/project.json": `{"name":"","version":"1.2.3"}`,
		})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "project.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
		if !strings.Contains(w.Body.String(), "project name is required") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("successful import with nested project root", func(t *testing.T) {
		projectName := "imported-demo"
		zipData := makeZipData(t, map[string]string{
			"project/project.json": `{
					"name":"imported-demo",
					"tables":"users",
					"microservices":[{"skey":"demo/hello","script":"return 'ok'","description":"hello"}]
				}`,
			"project/setup.sql":   "CREATE TABLE IF NOT EXISTS users (id INT)",
			"project/index.html":  "<h1>hello</h1>",
			"project/assets/a.js": "console.log('ok')",
		})

		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "project.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		if resp["name"] != projectName {
			t.Fatalf("expected project name %q got %v", projectName, resp["name"])
		}
		if resp["version"] != "1.0.0" {
			t.Fatalf("expected default version 1.0.0 got %v", resp["version"])
		}
		if resp["services"] != float64(1) {
			t.Fatalf("expected 1 service got %v", resp["services"])
		}

		projectDir := filepath.Join(server.config.Server.DataDir, "projects", projectName)
		if _, err := os.Stat(filepath.Join(projectDir, "index.html")); err != nil {
			t.Fatalf("expected index.html copied: %v", err)
		}
		if _, err := os.Stat(filepath.Join(projectDir, "assets", "a.js")); err != nil {
			t.Fatalf("expected assets file copied: %v", err)
		}
		if _, err := os.Stat(filepath.Join(projectDir, "project.json")); !os.IsNotExist(err) {
			t.Fatalf("project.json should not be copied into deployed dir")
		}

		exec := executor.NewExecutor(server.engine)
		projectRes, err := exec.Execute("SELECT name, version FROM _sys_projects WHERE name = 'imported-demo'")
		if err != nil {
			t.Fatalf("query imported project: %v", err)
		}
		if len(projectRes.Rows) != 1 {
			t.Fatalf("expected 1 project row got %d", len(projectRes.Rows))
		}

		msRes, err := exec.Execute("SELECT SKEY FROM _sys_ms WHERE SKEY = 'demo/hello'")
		if err != nil {
			t.Fatalf("query microservice: %v", err)
		}
		if len(msRes.Rows) != 1 {
			t.Fatalf("expected imported microservice to be registered")
		}
	})
}

func TestCreateProjectFile_Branches(t *testing.T) {
	server, _ := setupTestServer(t)
	projectName := "api-file-branches"

	t.Run("invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("empty path", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString(`{"path":"","content":"x"}`))
		w := httptest.NewRecorder()

		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("path traversal blocked", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString(`{"path":"../evil.txt","content":"x"}`))
		w := httptest.NewRecorder()

		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("create directory", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString(`{"path":"assets/js","isDir":true}`))
		w := httptest.NewRecorder()

		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}

		fullPath := filepath.Join(server.config.Server.DataDir, "projects", projectName, "assets", "js")
		info, err := os.Stat(fullPath)
		if err != nil {
			t.Fatalf("expected directory created: %v", err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be directory", fullPath)
		}
	})

	t.Run("create file", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString(`{"path":"assets/js/app.js","content":"console.log('ok')"}`))
		w := httptest.NewRecorder()

		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}

		fullPath := filepath.Join(server.config.Server.DataDir, "projects", projectName, "assets", "js", "app.js")
		data, err := os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("expected file created: %v", err)
		}
		if string(data) != "console.log('ok')" {
			t.Fatalf("unexpected file content: %s", string(data))
		}
	})
}

func TestHandleAPIPluginImport_Branches(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/plugins/import", nil)
		w := httptest.NewRecorder()

		server.handleAPIPluginImport(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("bad multipart form", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/plugins/import", bytes.NewBufferString("x"))
		req.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()

		server.handleAPIPluginImport(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing plugin file", func(t *testing.T) {
		req := makeMultipartRequest(t, http.MethodPost, "/api/plugins/import", "", "", nil)
		w := httptest.NewRecorder()

		server.handleAPIPluginImport(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
		if !strings.Contains(w.Body.String(), "no plugin file provided") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("install not implemented", func(t *testing.T) {
		req := makeMultipartRequest(t, http.MethodPost, "/api/plugins/import", "plugin", "plugin.zip", []byte("zip-content"))
		w := httptest.NewRecorder()

		server.handleAPIPluginImport(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d", w.Code, http.StatusInternalServerError)
		}
		if !strings.Contains(w.Body.String(), "failed to install plugin") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})
}

func TestCreateMicroservice_Branches(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/microservices", bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.createMicroservice(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("service key required", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/microservices", bytes.NewBufferString(`{"script":"return 1"}`))
		w := httptest.NewRecorder()

		server.createMicroservice(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("script required", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/microservices", bytes.NewBufferString(`{"skey":"ms/test"}`))
		w := httptest.NewRecorder()

		server.createMicroservice(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("create success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/microservices", bytes.NewBufferString(`{"skey":"ms/test","script":"return 'ok'","description":"desc"}`))
		w := httptest.NewRecorder()

		server.createMicroservice(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}

		exec := executor.NewExecutor(server.engine)
		res, err := exec.Execute("SELECT SKEY FROM _sys_ms WHERE SKEY = 'ms/test'")
		if err != nil {
			t.Fatalf("query microservice: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("expected 1 microservice row got %d", len(res.Rows))
		}
	})
}

func TestHandleAPIPluginInstall_Branches(t *testing.T) {
	t.Run("method not allowed", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodGet, "/api/plugins/install", nil)
		w := httptest.NewRecorder()

		server.handleAPIPluginInstall(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.handleAPIPluginInstall(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("plugin name required", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString(`{"name":""}`))
		w := httptest.NewRecorder()

		server.handleAPIPluginInstall(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("plugin not found", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString(`{"name":"unknown-plugin"}`))
		w := httptest.NewRecorder()

		server.handleAPIPluginInstall(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusNotFound, w.Body.String())
		}
	})

	t.Run("install and already installed", func(t *testing.T) {
		server, _ := setupTestServer(t)

		firstReq := httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString(`{"name":"logging"}`))
		firstW := httptest.NewRecorder()
		server.handleAPIPluginInstall(firstW, firstReq)
		if firstW.Code != http.StatusOK {
			t.Fatalf("first install status got %d want %d body=%s", firstW.Code, http.StatusOK, firstW.Body.String())
		}

		secondReq := httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString(`{"name":"logging"}`))
		secondW := httptest.NewRecorder()
		server.handleAPIPluginInstall(secondW, secondReq)
		if secondW.Code != http.StatusBadRequest {
			t.Fatalf("second install status got %d want %d body=%s", secondW.Code, http.StatusBadRequest, secondW.Body.String())
		}
	})
}

func TestHandleAPIConfig_Branches(t *testing.T) {
	server, _ := setupTestServer(t)
	server.config.Auth.AdminPassword = "top-secret"

	t.Run("get hides admin password", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}

		var cfg map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &cfg); err != nil {
			t.Fatalf("unmarshal config: %v", err)
		}
		authCfg, ok := cfg["auth"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected auth config in response")
		}
		if got, _ := authCfg["admin_password"].(string); got != "" {
			t.Fatalf("expected hidden admin password, got %q", got)
		}
	})

	t.Run("put invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("put unknown key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewBufferString(`{"no_such_key":{"a":1}}`))
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), "unknown config key") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "No changes made") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("put updates with restart and save error", func(t *testing.T) {
		server.SetConfigPath(filepath.Join(t.TempDir(), "missing", "cfg.json"))
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewBufferString(`{
			"log": {"level": "DEBUG", "max_size_mb": 16, "compress": true},
			"connection": {"max_connections": 2048},
			"network": {"bind": "127.0.0.1"}
		}`))
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "needs_restart") {
			t.Fatalf("expected needs_restart in response: %s", w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "failed to save config") {
			t.Fatalf("expected save error in response: %s", w.Body.String())
		}
		if server.config.Log.Level != "DEBUG" {
			t.Fatalf("expected log level updated to DEBUG, got %q", server.config.Log.Level)
		}
		if server.config.Connection.MaxConnections != 2048 {
			t.Fatalf("expected max_connections updated")
		}
		if server.config.Network.Bind != "127.0.0.1" {
			t.Fatalf("expected network.bind updated")
		}
	})

	t.Run("put no changes due to wrong types", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewBufferString(`{"log":{"level":123}}`))
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), "No changes made") {
			t.Fatalf("expected no changes response: %s", w.Body.String())
		}
	})

	t.Run("put updates all sections", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewBufferString(`{
			"log": {
				"level": "WARN",
				"max_size_mb": 32,
				"max_backups": 7,
				"max_age_days": 10,
				"compress": true
			},
			"backup": {
				"auto_interval_hours": 12,
				"keep_count": 8,
				"backup_dir": "/tmp/backups"
			},
			"security": {
				"audit_enabled": false,
				"rate_limit_enabled": true,
				"rate_limit_max_attempts": 5,
				"rate_limit_window_min": 15,
				"rate_limit_block_min": 20
			},
			"connection": {
				"max_connections": 99,
				"wait_timeout": 61,
				"idle_timeout": 62
			},
			"network": {"bind": "0.0.0.0"},
			"server": {"name": "xxsql-test"}
		}`))
		w := httptest.NewRecorder()

		server.handleAPIConfig(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "Configuration updated") {
			t.Fatalf("expected update response: %s", w.Body.String())
		}
		if server.config.Server.Name != "xxsql-test" {
			t.Fatalf("expected server.name updated, got %q", server.config.Server.Name)
		}
		if server.config.Backup.KeepCount != 8 {
			t.Fatalf("expected backup.keep_count updated")
		}
	})
}

func TestHandleAPIAdminReset_Branches(t *testing.T) {
	t.Run("method not allowed", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodGet, "/api/admin/reset", nil)
		w := httptest.NewRecorder()

		server.handleAPIAdminReset(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("missing authentication", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/admin/reset", bytes.NewBufferString(`{"confirm":"RESET"}`))
		w := httptest.NewRecorder()

		server.handleAPIAdminReset(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status got %d want %d", w.Code, http.StatusUnauthorized)
		}
	})

	t.Run("non admin forbidden", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/admin/reset", bytes.NewBufferString(`{"confirm":"RESET"}`))
		req = req.WithContext(setUsername(context.Background(), "testuser"))
		w := httptest.NewRecorder()

		server.handleAPIAdminReset(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("status got %d want %d", w.Code, http.StatusForbidden)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/admin/reset", bytes.NewBufferString("{"))
		req = req.WithContext(setUsername(context.Background(), "admin"))
		w := httptest.NewRecorder()

		server.handleAPIAdminReset(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("confirm required", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/admin/reset", bytes.NewBufferString(`{"confirm":"NOPE"}`))
		req = req.WithContext(setUsername(context.Background(), "admin"))
		w := httptest.NewRecorder()

		server.handleAPIAdminReset(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "confirmation required") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("full reset success", func(t *testing.T) {
		server, _ := setupTestServer(t)
		if _, err := server.auth.CreateUser("u2", "pass", auth.RoleUser); err != nil {
			t.Fatalf("create extra user: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/admin/reset", bytes.NewBufferString(`{"confirm":"RESET","full":true}`))
		req = req.WithContext(setUsername(context.Background(), "admin"))
		w := httptest.NewRecorder()

		server.handleAPIAdminReset(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "Server reset to initial state") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
		if _, err := server.auth.GetUser("u2"); err == nil {
			t.Fatalf("expected non-admin user removed by full reset")
		}
	})
}

func TestProjectFileMutations_Branches(t *testing.T) {
	server, _ := setupTestServer(t)
	projectName := "mutation-branches"
	baseDir := filepath.Join(server.config.Server.DataDir, "projects", projectName)
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("mkdir base dir: %v", err)
	}

	t.Run("update invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/projects/"+projectName+"/files/a.txt", bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.updateProjectFile(w, req, projectName, "a.txt")

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("update invalid path", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/projects/"+projectName+"/files/../a.txt", bytes.NewBufferString(`{"content":"x"}`))
		w := httptest.NewRecorder()

		server.updateProjectFile(w, req, projectName, "../a.txt")

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("update file write error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/projects/"+projectName+"/files/missing/dir/a.txt", bytes.NewBufferString(`{"content":"x"}`))
		w := httptest.NewRecorder()

		server.updateProjectFile(w, req, projectName, "missing/dir/a.txt")

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d", w.Code, http.StatusInternalServerError)
		}
	})

	t.Run("update success", func(t *testing.T) {
		filePath := filepath.Join(baseDir, "ok.txt")
		if err := os.WriteFile(filePath, []byte("old"), 0644); err != nil {
			t.Fatalf("seed file: %v", err)
		}

		req := httptest.NewRequest(http.MethodPut, "/api/projects/"+projectName+"/files/ok.txt", bytes.NewBufferString(`{"content":"new"}`))
		w := httptest.NewRecorder()

		server.updateProjectFile(w, req, projectName, "ok.txt")

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("read file: %v", err)
		}
		if string(data) != "new" {
			t.Fatalf("expected updated content, got %q", string(data))
		}
	})

	t.Run("delete invalid path", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/projects/"+projectName+"/files/../ok.txt", nil)
		w := httptest.NewRecorder()

		server.deleteProjectFile(w, req, projectName, "../ok.txt")

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("delete success", func(t *testing.T) {
		target := filepath.Join(baseDir, "delete-me.txt")
		if err := os.WriteFile(target, []byte("x"), 0644); err != nil {
			t.Fatalf("seed delete file: %v", err)
		}

		req := httptest.NewRequest(http.MethodDelete, "/api/projects/"+projectName+"/files/delete-me.txt", nil)
		w := httptest.NewRecorder()

		server.deleteProjectFile(w, req, projectName, "delete-me.txt")

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("expected target removed")
		}
	})
}

func TestHandleAPIKeys_Branches(t *testing.T) {
	t.Run("service unavailable when key manager nil", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.apiKeyManager = nil

		req := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status got %d want %d", w.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("method not allowed", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPatch, "/api/keys", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("post invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/keys", bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("post user not found for default permissions", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/keys", bytes.NewBufferString(`{"name":"k"}`))
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req.WithContext(setUsername(context.Background(), "ghost-user")))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
	})

	t.Run("post success with defaults", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/keys", bytes.NewBufferString(`{"expires_in":60}`))
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "Store this key securely") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("get non admin only sees own keys", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, _, _ = server.apiKeyManager.GenerateKey("admin-key", "admin", auth.PermSelect, 0)
		_, _, _ = server.apiKeyManager.GenerateKey("user-key", "testuser", auth.PermSelect, 0)

		req := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req.WithContext(setUsername(context.Background(), "testuser")))

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}

		var resp struct {
			Keys []map[string]interface{} `json:"keys"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		if len(resp.Keys) != 1 {
			t.Fatalf("expected only 1 key for non-admin, got %d", len(resp.Keys))
		}
		if resp.Keys[0]["username"] != "testuser" {
			t.Fatalf("expected only testuser key, got %v", resp.Keys[0]["username"])
		}
	})

	t.Run("get admin sees all keys", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, _, _ = server.apiKeyManager.GenerateKey("admin-key", "admin", auth.PermSelect, 0)
		_, _, _ = server.apiKeyManager.GenerateKey("user-key", "testuser", auth.PermSelect, 0)

		req := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeys(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}

		var resp struct {
			Keys []map[string]interface{} `json:"keys"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		if len(resp.Keys) < 2 {
			t.Fatalf("expected admin to see all keys, got %d", len(resp.Keys))
		}
	})
}

func TestHandleAPIKeyDetail_Branches(t *testing.T) {
	t.Run("service unavailable when key manager nil", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.apiKeyManager = nil

		req := httptest.NewRequest(http.MethodGet, "/api/keys/abc", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeyDetail(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status got %d want %d", w.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("missing key id", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodGet, "/api/keys/", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeyDetail(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("not found", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodGet, "/api/keys/not-found", nil)
		w := httptest.NewRecorder()

		server.handleAPIKeyDetail(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("forbidden for other user", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, key, err := server.apiKeyManager.GenerateKey("admin-key", "admin", auth.PermSelect, 0)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/keys/"+key.ID, nil)
		w := httptest.NewRecorder()

		server.handleAPIKeyDetail(w, req.WithContext(setUsername(context.Background(), "testuser")))

		if w.Code != http.StatusForbidden {
			t.Fatalf("status got %d want %d", w.Code, http.StatusForbidden)
		}
	})

	t.Run("put invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, key, err := server.apiKeyManager.GenerateKey("admin-key", "admin", auth.PermSelect, 0)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}

		req := httptest.NewRequest(http.MethodPut, "/api/keys/"+key.ID, bytes.NewBufferString("{"))
		w := httptest.NewRecorder()

		server.handleAPIKeyDetail(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("method not allowed", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, key, err := server.apiKeyManager.GenerateKey("admin-key", "admin", auth.PermSelect, 0)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}

		req := httptest.NewRequest(http.MethodPatch, "/api/keys/"+key.ID, nil)
		w := httptest.NewRecorder()

		server.handleAPIKeyDetail(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

}

func TestHandleAPIPluginRouteAndActions_Branches(t *testing.T) {
	t.Run("route unknown action", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/plugins/auth/nope", nil)
		w := httptest.NewRecorder()

		server.handleAPIPluginRoutes(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("route bare plugin with non get returns not found", func(t *testing.T) {
		server, _ := setupTestServer(t)
		req := httptest.NewRequest(http.MethodPost, "/api/plugins/auth", nil)
		w := httptest.NewRecorder()

		server.handleAPIPluginRoutes(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("enable and disable wrong method", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w1 := httptest.NewRecorder()
		server.handleAPIPluginEnable(w1, httptest.NewRequest(http.MethodGet, "/api/plugins/auth/enable", nil), "auth")
		if w1.Code != http.StatusMethodNotAllowed {
			t.Fatalf("enable status got %d want %d", w1.Code, http.StatusMethodNotAllowed)
		}

		w2 := httptest.NewRecorder()
		server.handleAPIPluginDisable(w2, httptest.NewRequest(http.MethodGet, "/api/plugins/auth/disable", nil), "auth")
		if w2.Code != http.StatusMethodNotAllowed {
			t.Fatalf("disable status got %d want %d", w2.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("plugin get not found then success", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wNF := httptest.NewRecorder()
		server.handleAPIPluginGet(wNF, httptest.NewRequest(http.MethodGet, "/api/plugins/not-installed", nil), "not-installed")
		if wNF.Code != http.StatusNotFound {
			t.Fatalf("not-found status got %d want %d", wNF.Code, http.StatusNotFound)
		}

		installReq := httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString(`{"name":"auth"}`))
		installW := httptest.NewRecorder()
		server.handleAPIPluginInstall(installW, installReq)
		if installW.Code != http.StatusOK {
			t.Fatalf("install status got %d want %d body=%s", installW.Code, http.StatusOK, installW.Body.String())
		}

		wOK := httptest.NewRecorder()
		server.handleAPIPluginGet(wOK, httptest.NewRequest(http.MethodGet, "/api/plugins/auth", nil), "auth")
		if wOK.Code != http.StatusOK {
			t.Fatalf("get status got %d want %d body=%s", wOK.Code, http.StatusOK, wOK.Body.String())
		}
		if !strings.Contains(wOK.Body.String(), `"name":"auth"`) {
			t.Fatalf("unexpected get body: %s", wOK.Body.String())
		}
	})
}

func TestHandleAPIBackups_Branches(t *testing.T) {
	t.Run("method not allowed", func(t *testing.T) {
		server, _ := setupTestServer(t)
		w := httptest.NewRecorder()
		server.handleAPIBackups(w, httptest.NewRequest(http.MethodPatch, "/api/backups", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("get when backup dir missing", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.config.Backup.BackupDir = filepath.Join(t.TempDir(), "not-exist")

		w := httptest.NewRecorder()
		server.handleAPIBackups(w, httptest.NewRequest(http.MethodGet, "/api/backups", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), `"backups":[]`) {
			t.Fatalf("expected empty backups list, got %s", w.Body.String())
		}
	})

	t.Run("post invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)
		w := httptest.NewRecorder()
		server.handleAPIBackups(w, httptest.NewRequest(http.MethodPost, "/api/backups", bytes.NewBufferString("{")))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("post create backup and list", func(t *testing.T) {
		server, _ := setupTestServer(t)

		postW := httptest.NewRecorder()
		server.handleAPIBackups(postW, httptest.NewRequest(http.MethodPost, "/api/backups", bytes.NewBufferString(`{"compress":false}`)))
		if postW.Code != http.StatusOK {
			t.Fatalf("create backup status got %d want %d body=%s", postW.Code, http.StatusOK, postW.Body.String())
		}

		getW := httptest.NewRecorder()
		server.handleAPIBackups(getW, httptest.NewRequest(http.MethodGet, "/api/backups", nil))
		if getW.Code != http.StatusOK {
			t.Fatalf("list backups status got %d want %d", getW.Code, http.StatusOK)
		}
		if !strings.Contains(getW.Body.String(), "backup_") {
			t.Fatalf("expected generated backup in list, got %s", getW.Body.String())
		}
	})
}

func TestProjectRoutesAndCRUD_Branches(t *testing.T) {
	t.Run("project routes dispatch and bad methods", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w1 := httptest.NewRecorder()
		server.handleAPIProjectRoutes(w1, httptest.NewRequest(http.MethodGet, "/api/projects/nope/files", nil))
		if w1.Code != http.StatusNotFound {
			t.Fatalf("files list dispatch status got %d want %d", w1.Code, http.StatusNotFound)
		}

		w2 := httptest.NewRecorder()
		server.handleAPIProjectRoutes(w2, httptest.NewRequest(http.MethodPost, "/api/projects/demo/anything", nil))
		if w2.Code != http.StatusMethodNotAllowed {
			t.Fatalf("detail dispatch status got %d want %d", w2.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("list projects error branch", func(t *testing.T) {
		server, _ := setupTestServer(t)
		exec := executor.NewExecutor(server.engine)
		if _, err := exec.Execute("DROP TABLE _sys_projects"); err != nil {
			t.Fatalf("drop _sys_projects: %v", err)
		}

		w := httptest.NewRecorder()
		server.listProjects(w, httptest.NewRequest(http.MethodGet, "/api/projects", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
	})

	t.Run("create project duplicate triggers cleanup", func(t *testing.T) {
		server, _ := setupTestServer(t)

		firstW := httptest.NewRecorder()
		server.createProject(firstW, httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBufferString(`{"name":"dup-project"}`)))
		if firstW.Code != http.StatusOK {
			t.Fatalf("first create status got %d want %d", firstW.Code, http.StatusOK)
		}

		secondW := httptest.NewRecorder()
		server.createProject(secondW, httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBufferString(`{"name":"dup-project"}`)))
		if secondW.Code != http.StatusInternalServerError {
			t.Fatalf("duplicate create status got %d want %d body=%s", secondW.Code, http.StatusInternalServerError, secondW.Body.String())
		}

		projectDir := filepath.Join(server.config.Server.DataDir, "projects", "dup-project")
		if _, err := os.Stat(projectDir); !os.IsNotExist(err) {
			t.Fatalf("expected duplicate cleanup to remove directory: %s", projectDir)
		}
	})

	t.Run("delete project drops tables and unregisters", func(t *testing.T) {
		server, _ := setupTestServer(t)
		exec := executor.NewExecutor(server.engine)
		if _, err := exec.Execute("CREATE TABLE proj_tbl_for_delete (id INT)"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := exec.Execute("INSERT INTO _sys_projects (name, version, installed_at, tables) VALUES ('proj-del', '1.0.0', datetime('now'), 'proj_tbl_for_delete')"); err != nil {
			t.Fatalf("insert project row: %v", err)
		}

		projectDir := filepath.Join(server.config.Server.DataDir, "projects", "proj-del")
		if err := os.MkdirAll(projectDir, 0755); err != nil {
			t.Fatalf("mkdir project dir: %v", err)
		}

		w := httptest.NewRecorder()
		server.deleteProject(w, httptest.NewRequest(http.MethodDelete, "/api/projects/proj-del", nil), "proj-del")
		if w.Code != http.StatusOK {
			t.Fatalf("delete project status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}

		if server.engine.TableExists("proj_tbl_for_delete") {
			t.Fatalf("expected project table to be dropped")
		}
		if _, err := os.Stat(projectDir); !os.IsNotExist(err) {
			t.Fatalf("expected project directory removed")
		}
		res, err := exec.Execute("SELECT name FROM _sys_projects WHERE name = 'proj-del'")
		if err != nil {
			t.Fatalf("query _sys_projects: %v", err)
		}
		if len(res.Rows) != 0 {
			t.Fatalf("expected project row removed")
		}
	})
}

func TestProjectFileAndMicroservice_Branches(t *testing.T) {
	t.Run("project file detail invalid paths and methods", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w1 := httptest.NewRecorder()
		server.handleAPIProjectFileDetail(w1, httptest.NewRequest(http.MethodGet, "/api/projects/p1/other/a.txt", nil))
		if w1.Code != http.StatusBadRequest {
			t.Fatalf("invalid format status got %d want %d", w1.Code, http.StatusBadRequest)
		}

		w2 := httptest.NewRecorder()
		server.handleAPIProjectFileDetail(w2, httptest.NewRequest(http.MethodGet, "/api/projects/p1/files/", nil))
		if w2.Code != http.StatusNotFound {
			t.Fatalf("empty file path redirect status got %d want %d", w2.Code, http.StatusNotFound)
		}

		projectDir := filepath.Join(server.config.Server.DataDir, "projects", "p2")
		if err := os.MkdirAll(projectDir, 0755); err != nil {
			t.Fatalf("mkdir project dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(projectDir, "a.txt"), []byte("x"), 0644); err != nil {
			t.Fatalf("seed file: %v", err)
		}

		w3 := httptest.NewRecorder()
		server.handleAPIProjectFileDetail(w3, httptest.NewRequest(http.MethodPatch, "/api/projects/p2/files/a.txt", nil))
		if w3.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", w3.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("get project file invalid and missing", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w1 := httptest.NewRecorder()
		server.getProjectFile(w1, httptest.NewRequest(http.MethodGet, "/", nil), "p", "../x.txt")
		if w1.Code != http.StatusBadRequest {
			t.Fatalf("invalid path status got %d want %d", w1.Code, http.StatusBadRequest)
		}

		w2 := httptest.NewRecorder()
		server.getProjectFile(w2, httptest.NewRequest(http.MethodGet, "/", nil), "p", "nope.txt")
		if w2.Code != http.StatusNotFound {
			t.Fatalf("missing file status got %d want %d", w2.Code, http.StatusNotFound)
		}
	})

	t.Run("get project file success", func(t *testing.T) {
		server, _ := setupTestServer(t)

		projectDir := filepath.Join(server.config.Server.DataDir, "projects", "pf-ok")
		if err := os.MkdirAll(projectDir, 0755); err != nil {
			t.Fatalf("mkdir project dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(projectDir, "hello.txt"), []byte("hello"), 0644); err != nil {
			t.Fatalf("write project file: %v", err)
		}

		w := httptest.NewRecorder()
		server.getProjectFile(w, httptest.NewRequest(http.MethodGet, "/", nil), "pf-ok", "hello.txt")
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"content":"hello"`) {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("microservice route and CRUD branches", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w1 := httptest.NewRecorder()
		server.handleAPIMicroservices(w1, httptest.NewRequest(http.MethodPatch, "/api/microservices", nil))
		if w1.Code != http.StatusMethodNotAllowed {
			t.Fatalf("microservices method status got %d want %d", w1.Code, http.StatusMethodNotAllowed)
		}

		w2 := httptest.NewRecorder()
		server.handleAPIMicroserviceDetail(w2, httptest.NewRequest(http.MethodGet, "/api/microservices/", nil))
		if w2.Code != http.StatusBadRequest {
			t.Fatalf("empty skey status got %d want %d", w2.Code, http.StatusBadRequest)
		}

		w3 := httptest.NewRecorder()
		server.getMicroservice(w3, httptest.NewRequest(http.MethodGet, "/", nil), "not-found")
		if w3.Code != http.StatusNotFound {
			t.Fatalf("get not found status got %d want %d", w3.Code, http.StatusNotFound)
		}

		errServer, _ := setupTestServer(t)
		if _, err := executor.NewExecutor(errServer.engine).Execute("DROP TABLE _sys_ms"); err != nil {
			t.Fatalf("drop _sys_ms: %v", err)
		}

		w3err := httptest.NewRecorder()
		errServer.getMicroservice(w3err, httptest.NewRequest(http.MethodGet, "/", nil), "any")
		if w3err.Code != http.StatusInternalServerError {
			t.Fatalf("get error status got %d want %d", w3err.Code, http.StatusInternalServerError)
		}

		w3delErr := httptest.NewRecorder()
		errServer.deleteMicroservice(w3delErr, httptest.NewRequest(http.MethodDelete, "/", nil), "any")
		if w3delErr.Code != http.StatusInternalServerError {
			t.Fatalf("delete error status got %d want %d", w3delErr.Code, http.StatusInternalServerError)
		}

		w4 := httptest.NewRecorder()
		server.updateMicroservice(w4, httptest.NewRequest(http.MethodPut, "/", bytes.NewBufferString("{")), "ms/1")
		if w4.Code != http.StatusBadRequest {
			t.Fatalf("update invalid json status got %d want %d", w4.Code, http.StatusBadRequest)
		}

		if _, err := executor.NewExecutor(server.engine).Execute("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('ms/1', 'return 1', 'd', datetime('now'))"); err != nil {
			t.Fatalf("seed microservice: %v", err)
		}

		w5 := httptest.NewRecorder()
		server.updateMicroservice(w5, httptest.NewRequest(http.MethodPut, "/", bytes.NewBufferString(`{"script":"return 2","description":"new"}`)), "ms/1")
		if w5.Code != http.StatusOK {
			t.Fatalf("update success status got %d want %d body=%s", w5.Code, http.StatusOK, w5.Body.String())
		}

		w7 := httptest.NewRecorder()
		server.deleteMicroservice(w7, httptest.NewRequest(http.MethodDelete, "/", nil), "ms/1")
		if w7.Code != http.StatusOK {
			t.Fatalf("delete success status got %d want %d", w7.Code, http.StatusOK)
		}

		w8 := httptest.NewRecorder()
		server.handleAPIMicroserviceDetail(w8, httptest.NewRequest(http.MethodPatch, "/api/microservices/ms/1", nil))
		if w8.Code != http.StatusMethodNotAllowed {
			t.Fatalf("detail method status got %d want %d", w8.Code, http.StatusMethodNotAllowed)
		}
	})
}

func TestAdditionalAPIBranches(t *testing.T) {
	t.Run("project files wrapper guards", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w1 := httptest.NewRecorder()
		server.handleAPIProjectFiles(w1, httptest.NewRequest(http.MethodGet, "/api/projects/", nil))
		if w1.Code != http.StatusBadRequest {
			t.Fatalf("missing project status got %d want %d", w1.Code, http.StatusBadRequest)
		}

		w2 := httptest.NewRecorder()
		server.handleAPIProjectFiles(w2, httptest.NewRequest(http.MethodPatch, "/api/projects/p1/files", nil))
		if w2.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", w2.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("list microservices error branch", func(t *testing.T) {
		server, _ := setupTestServer(t)
		if _, err := executor.NewExecutor(server.engine).Execute("DROP TABLE _sys_ms"); err != nil {
			t.Fatalf("drop _sys_ms: %v", err)
		}

		w := httptest.NewRecorder()
		server.listMicroservices(w, httptest.NewRequest(http.MethodGet, "/api/microservices", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
	})

	t.Run("plugins list method and error branches", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wMethod := httptest.NewRecorder()
		server.handleAPIPlugins(wMethod, httptest.NewRequest(http.MethodPost, "/api/plugins", nil))
		if wMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", wMethod.Code, http.StatusMethodNotAllowed)
		}

		if _, err := executor.NewExecutor(server.engine).Execute("DROP TABLE _sys_plugins"); err != nil {
			t.Fatalf("drop _sys_plugins: %v", err)
		}

		wErr := httptest.NewRecorder()
		server.handleAPIPlugins(wErr, httptest.NewRequest(http.MethodGet, "/api/plugins", nil))
		if wErr.Code != http.StatusInternalServerError {
			t.Fatalf("error status got %d want %d", wErr.Code, http.StatusInternalServerError)
		}
	})

	t.Run("plugins available method and installed flag", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wMethod := httptest.NewRecorder()
		server.handleAPIPluginsAvailable(wMethod, httptest.NewRequest(http.MethodPost, "/api/plugins/available", nil))
		if wMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", wMethod.Code, http.StatusMethodNotAllowed)
		}

		installW := httptest.NewRecorder()
		server.handleAPIPluginInstall(installW, httptest.NewRequest(http.MethodPost, "/api/plugins/install", bytes.NewBufferString(`{"name":"auth"}`)))
		if installW.Code != http.StatusOK {
			t.Fatalf("install status got %d want %d body=%s", installW.Code, http.StatusOK, installW.Body.String())
		}

		w := httptest.NewRecorder()
		server.handleAPIPluginsAvailable(w, httptest.NewRequest(http.MethodGet, "/api/plugins/available", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("available status got %d want %d", w.Code, http.StatusOK)
		}

		var resp struct {
			Plugins []map[string]interface{} `json:"plugins"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}

		foundAuthInstalled := false
		for _, p := range resp.Plugins {
			if p["name"] == "auth" {
				if v, ok := p["installed"].(bool); ok && v {
					foundAuthInstalled = true
				}
			}
		}
		if !foundAuthInstalled {
			t.Fatalf("expected auth plugin marked installed")
		}
	})

	t.Run("plugin uninstall method and error branches", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wMethod := httptest.NewRecorder()
		server.handleAPIPluginUninstall(wMethod, httptest.NewRequest(http.MethodGet, "/api/plugins/auth/uninstall", nil), "auth")
		if wMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", wMethod.Code, http.StatusMethodNotAllowed)
		}

		wErr := httptest.NewRecorder()
		server.handleAPIPluginUninstall(wErr, httptest.NewRequest(http.MethodPost, "/api/plugins/none/uninstall", nil), "none")
		if wErr.Code != http.StatusInternalServerError {
			t.Fatalf("error status got %d want %d body=%s", wErr.Code, http.StatusInternalServerError, wErr.Body.String())
		}
	})
}

func TestStatusMetricsTables_MethodGuards(t *testing.T) {
	t.Run("status supports only GET", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wMethod := httptest.NewRecorder()
		server.handleAPIStatus(wMethod, httptest.NewRequest(http.MethodPost, "/api/status", nil))
		if wMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", wMethod.Code, http.StatusMethodNotAllowed)
		}

		w := httptest.NewRecorder()
		server.handleAPIStatus(w, httptest.NewRequest(http.MethodGet, "/api/status", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("metrics supports only GET", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wMethod := httptest.NewRecorder()
		server.handleAPIMetrics(wMethod, httptest.NewRequest(http.MethodPut, "/api/metrics", nil))
		if wMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", wMethod.Code, http.StatusMethodNotAllowed)
		}

		w := httptest.NewRecorder()
		server.handleAPIMetrics(w, httptest.NewRequest(http.MethodGet, "/api/metrics", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("tables supports only GET", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wMethod := httptest.NewRecorder()
		server.handleAPITables(wMethod, httptest.NewRequest(http.MethodDelete, "/api/tables", nil))
		if wMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method status got %d want %d", wMethod.Code, http.StatusMethodNotAllowed)
		}

		exec := executor.NewExecutor(server.engine)
		if _, err := exec.Execute("CREATE TABLE cov_table_guard (id INT PRIMARY KEY, name VARCHAR(20))"); err != nil {
			t.Fatalf("create table: %v", err)
		}

		w := httptest.NewRecorder()
		server.handleAPITables(w, httptest.NewRequest(http.MethodGet, "/api/tables", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})
}

func TestHandleMicroservice_ExtraBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	exec := executor.NewExecutor(server.engine)

	t.Run("invalid path after prefix", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleMicroservice(w, httptest.NewRequest(http.MethodGet, "/ms/", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("table name without skey", func(t *testing.T) {
		if _, err := exec.Execute("CREATE TABLE svc_tbl (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT)"); err != nil {
			t.Fatalf("create service table: %v", err)
		}

		w := httptest.NewRecorder()
		server.handleMicroservice(w, httptest.NewRequest(http.MethodGet, "/ms/svc_tbl", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
		}
	})

	t.Run("existing custom table route", func(t *testing.T) {
		if _, err := exec.Execute("INSERT INTO svc_tbl (SKEY, SCRIPT) VALUES ('hello', 'http.json({" + `"ok":true` + "})')"); err != nil {
			t.Fatalf("insert service script: %v", err)
		}

		w := httptest.NewRecorder()
		server.handleMicroservice(w, httptest.NewRequest(http.MethodGet, "/ms/svc_tbl/hello", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("skey with slash in default table", func(t *testing.T) {
		if _, err := exec.Execute("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('nested/path', 'http.json({" + `"path":"ok"` + "})', 'd', datetime('now'))"); err != nil {
			t.Fatalf("insert nested skey: %v", err)
		}

		w := httptest.NewRecorder()
		server.handleMicroservice(w, httptest.NewRequest(http.MethodGet, "/ms/nested/path", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("empty script returns not found", func(t *testing.T) {
		if _, err := exec.Execute("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('empty-script', '', 'd', datetime('now'))"); err != nil {
			t.Fatalf("insert empty script: %v", err)
		}

		w := httptest.NewRecorder()
		server.handleMicroservice(w, httptest.NewRequest(http.MethodGet, "/ms/empty-script", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusNotFound, w.Body.String())
		}
	})

	t.Run("script execution error", func(t *testing.T) {
		if _, err := exec.Execute("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('boom', 'unknown_func()', 'd', datetime('now'))"); err != nil {
			t.Fatalf("insert failing script: %v", err)
		}

		w := httptest.NewRecorder()
		server.handleMicroservice(w, httptest.NewRequest(http.MethodGet, "/ms/boom", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
	})
}

func TestAuthHandlers_ExtraBranches(t *testing.T) {
	t.Run("login invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPILogin(w, httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBufferString("{")))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("login when auth disabled", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.config.Auth.Enabled = false

		w := httptest.NewRecorder()
		server.handleAPILogin(w, httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBufferString(`{"username":"any","password":"ignored"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		if resp["role"] != "admin" {
			t.Fatalf("expected role admin got %v", resp["role"])
		}

		cookies := w.Result().Cookies()
		if len(cookies) == 0 || cookies[0].Name != "xxsql_session" || cookies[0].Value == "" {
			t.Fatalf("expected session cookie to be set")
		}
	})

	t.Run("logout without cookie still succeeds", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPILogout(w, httptest.NewRequest(http.MethodPost, "/api/logout", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("logout wrong method", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPILogout(w, httptest.NewRequest(http.MethodGet, "/api/logout", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("admin can access another users key", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, key, err := server.apiKeyManager.GenerateKey("user-key", "testuser", auth.PermSelect, 0)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/keys/"+key.ID, nil)
		server.handleAPIKeyDetail(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("put without enabled still updates", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, key, err := server.apiKeyManager.GenerateKey("admin-key", "admin", auth.PermSelect, 0)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/api/keys/"+key.ID, bytes.NewBufferString(`{}`))
		server.handleAPIKeyDetail(w, req.WithContext(setUsername(context.Background(), "admin")))

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "API key updated") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("empty username key can be fetched without user context", func(t *testing.T) {
		server, _ := setupTestServer(t)
		_, key, err := server.apiKeyManager.GenerateKey("orphan", "", auth.PermSelect, 0)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/keys/"+key.ID, nil)
		server.handleAPIKeyDetail(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
	})
}

func TestHandleAPIProjectRoutes_AllPaths(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("routes to file detail when contains files slash", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleAPIProjectRoutes(w, httptest.NewRequest(http.MethodGet, "/api/projects/p1/files/a.txt", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("routes to files listing when ends with files", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleAPIProjectRoutes(w, httptest.NewRequest(http.MethodGet, "/api/projects/p1/files", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("routes to project detail otherwise", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleAPIProjectRoutes(w, httptest.NewRequest(http.MethodGet, "/api/projects/p1", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})
}

func TestPluginEnableDisableGet_ErrorBranches(t *testing.T) {
	t.Run("enable and disable db error", func(t *testing.T) {
		server, _ := setupTestServer(t)
		if _, err := executor.NewExecutor(server.engine).Execute("DROP TABLE _sys_plugins"); err != nil {
			t.Fatalf("drop _sys_plugins: %v", err)
		}

		wEnable := httptest.NewRecorder()
		server.handleAPIPluginEnable(wEnable, httptest.NewRequest(http.MethodPost, "/api/plugins/p/enable", nil), "p")
		if wEnable.Code != http.StatusInternalServerError {
			t.Fatalf("enable status got %d want %d", wEnable.Code, http.StatusInternalServerError)
		}

		wDisable := httptest.NewRecorder()
		server.handleAPIPluginDisable(wDisable, httptest.NewRequest(http.MethodPost, "/api/plugins/p/disable", nil), "p")
		if wDisable.Code != http.StatusInternalServerError {
			t.Fatalf("disable status got %d want %d", wDisable.Code, http.StatusInternalServerError)
		}

		wGet := httptest.NewRecorder()
		server.handleAPIPluginGet(wGet, httptest.NewRequest(http.MethodGet, "/api/plugins/p", nil), "p")
		if wGet.Code != http.StatusInternalServerError {
			t.Fatalf("get status got %d want %d", wGet.Code, http.StatusInternalServerError)
		}
	})

	t.Run("enable and disable wrong method", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wEnable := httptest.NewRecorder()
		server.handleAPIPluginEnable(wEnable, httptest.NewRequest(http.MethodGet, "/api/plugins/p/enable", nil), "p")
		if wEnable.Code != http.StatusMethodNotAllowed {
			t.Fatalf("enable method status got %d want %d", wEnable.Code, http.StatusMethodNotAllowed)
		}

		wDisable := httptest.NewRecorder()
		server.handleAPIPluginDisable(wDisable, httptest.NewRequest(http.MethodGet, "/api/plugins/p/disable", nil), "p")
		if wDisable.Code != http.StatusMethodNotAllowed {
			t.Fatalf("disable method status got %d want %d", wDisable.Code, http.StatusMethodNotAllowed)
		}
	})
}

func TestMicroserviceCreateUpdate_ErrorBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	if _, err := executor.NewExecutor(server.engine).Execute("DROP TABLE _sys_ms"); err != nil {
		t.Fatalf("drop _sys_ms: %v", err)
	}

	wCreate := httptest.NewRecorder()
	server.createMicroservice(wCreate, httptest.NewRequest(http.MethodPost, "/api/microservices", bytes.NewBufferString(`{"skey":"m/1","script":"return 1"}`)))
	if wCreate.Code != http.StatusInternalServerError {
		t.Fatalf("create status got %d want %d", wCreate.Code, http.StatusInternalServerError)
	}

	wUpdate := httptest.NewRecorder()
	server.updateMicroservice(wUpdate, httptest.NewRequest(http.MethodPut, "/api/microservices/m/1", bytes.NewBufferString(`{"script":"return 2","description":"d"}`)), "m/1")
	if wUpdate.Code != http.StatusInternalServerError {
		t.Fatalf("update status got %d want %d", wUpdate.Code, http.StatusInternalServerError)
	}
}

func TestHandleAPILogs_ExtraBranches(t *testing.T) {
	t.Run("unknown log type", func(t *testing.T) {
		server, _ := setupTestServer(t)
		w := httptest.NewRecorder()
		server.handleAPILogs(w, httptest.NewRequest(http.MethodGet, "/api/logs/unknown", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("empty log path returns empty lines", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.config.Log.File = ""

		w := httptest.NewRecorder()
		server.handleAPILogs(w, httptest.NewRequest(http.MethodGet, "/api/logs/server", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), "\"lines\":[]") {
			t.Fatalf("expected empty lines response, body=%s", w.Body.String())
		}
	})

	t.Run("missing file with large lines query", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.config.Log.File = filepath.Join(server.config.Server.DataDir, "no-such.log")

		w := httptest.NewRecorder()
		server.handleAPILogs(w, httptest.NewRequest(http.MethodGet, "/api/logs/server?lines=99999", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
	})
}

func TestHandleAPIPluginRoutes_ExtraBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("import route delegation", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleAPIPluginRoutes(w, httptest.NewRequest(http.MethodGet, "/api/plugins/import", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("install route delegation", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleAPIPluginRoutes(w, httptest.NewRequest(http.MethodGet, "/api/plugins/install", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status got %d want %d", w.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("single plugin non-GET returns not found", func(t *testing.T) {
		w := httptest.NewRecorder()
		server.handleAPIPluginRoutes(w, httptest.NewRequest(http.MethodPost, "/api/plugins/auth", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestProjectFiles_ErrorBranches(t *testing.T) {
	t.Run("create directory error", func(t *testing.T) {
		server, _ := setupTestServer(t)
		projectName := "pf-err-dir"

		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString(`{"path":"bad\u0000dir","isDir":true}`))
		w := httptest.NewRecorder()
		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
	})

	t.Run("create file error", func(t *testing.T) {
		server, _ := setupTestServer(t)
		projectName := "pf-err-file"

		req := httptest.NewRequest(http.MethodPost, "/api/projects/"+projectName+"/files", bytes.NewBufferString(`{"path":"bad\u0000.txt","content":"x"}`))
		w := httptest.NewRecorder()
		server.createProjectFile(w, req, projectName)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
	})
}

func TestHandleAPIProjectImport_ExtraBranches(t *testing.T) {
	t.Run("multipart parse failure", func(t *testing.T) {
		server, _ := setupTestServer(t)

		req := httptest.NewRequest(http.MethodPost, "/api/projects/import", bytes.NewBufferString("not-multipart"))
		req.Header.Set("Content-Type", "multipart/form-data")
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
		}
	})

	t.Run("write zip file error from directory filename", func(t *testing.T) {
		server, _ := setupTestServer(t)

		zipData := makeZipData(t, map[string]string{
			"project.json": `{"name":"nested-write-fail"}`,
		})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "..", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "failed to write zip file") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("invalid project json", func(t *testing.T) {
		server, _ := setupTestServer(t)

		zipData := makeZipData(t, map[string]string{
			"project/project.json": `{"name":`,
		})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "bad-project.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "invalid project.json") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})
}

func TestUserAndProjectFile_ExtraErrorBranches(t *testing.T) {
	t.Run("users invalid json", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPIUsers(w, httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewBufferString("{")))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("user detail update invalid json and update without password", func(t *testing.T) {
		server, _ := setupTestServer(t)

		wBad := httptest.NewRecorder()
		server.handleAPIUserDetail(wBad, httptest.NewRequest(http.MethodPut, "/api/users/admin", bytes.NewBufferString("{")))
		if wBad.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", wBad.Code, http.StatusBadRequest)
		}

		wOK := httptest.NewRecorder()
		server.handleAPIUserDetail(wOK, httptest.NewRequest(http.MethodPut, "/api/users/admin", bytes.NewBufferString(`{"role":"user"}`)))
		if wOK.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", wOK.Code, http.StatusOK, wOK.Body.String())
		}
		if !strings.Contains(wOK.Body.String(), "user updated") {
			t.Fatalf("unexpected body: %s", wOK.Body.String())
		}
	})

	t.Run("create project mkdir failure and delete file remove failure", func(t *testing.T) {
		server, _ := setupTestServer(t)

		projectsPath := filepath.Join(server.config.Server.DataDir, "projects")
		if err := os.WriteFile(projectsPath, []byte("x"), 0644); err != nil {
			t.Fatalf("seed projects file: %v", err)
		}

		wCreate := httptest.NewRecorder()
		server.createProject(wCreate, httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBufferString(`{"name":"mkerr"}`)))
		if wCreate.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", wCreate.Code, http.StatusInternalServerError, wCreate.Body.String())
		}

		wDelete := httptest.NewRecorder()
		server.deleteProjectFile(wDelete, httptest.NewRequest(http.MethodDelete, "/", nil), "proj", "a.txt")
		if wDelete.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", wDelete.Code, http.StatusInternalServerError, wDelete.Body.String())
		}
	})
}

func TestAuthMiddleware_ExtraBranches(t *testing.T) {
	t.Run("auth disabled allows api", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.config.Auth.Enabled = false

		hit := false
		h := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hit = true
			w.WriteHeader(http.StatusNoContent)
		}))

		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/status", nil))
		if !hit || w.Code != http.StatusNoContent {
			t.Fatalf("expected pass-through when auth disabled, hit=%v status=%d", hit, w.Code)
		}
	})

	t.Run("public microservice paths bypass auth", func(t *testing.T) {
		server, _ := setupTestServer(t)
		paths := []string{"/ms/auth/register", "/ms/auth/login", "/ms/auth/check", "/ms/health"}

		for _, p := range paths {
			hit := false
			h := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit = true
				w.WriteHeader(http.StatusAccepted)
			}))

			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, p, nil))
			if !hit || w.Code != http.StatusAccepted {
				t.Fatalf("path %s expected bypass, hit=%v status=%d", p, hit, w.Code)
			}
		}
	})

	t.Run("api route with valid session sets username context", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.sessions["sess-ok"] = &Session{ID: "sess-ok", Username: "admin", Expires: time.Now().Add(time.Hour)}

		gotUser := ""
		h := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotUser = getUsername(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/tables", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: "sess-ok"})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK || gotUser != "admin" {
			t.Fatalf("expected authenticated session path, status=%d user=%q", w.Code, gotUser)
		}
	})

	t.Run("api route with basic auth is accepted", func(t *testing.T) {
		server, _ := setupTestServer(t)

		h := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/tables", nil)
		req.SetBasicAuth("admin", "admin")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected basic auth accepted, status=%d", w.Code)
		}
	})
}

func TestHandleAPIPluginImport_CreateTempFileError(t *testing.T) {
	server, _ := setupTestServer(t)

	badTmp := filepath.Join(t.TempDir(), "missing", "tmp")
	t.Setenv("TMPDIR", badTmp)

	req := makeMultipartRequest(t, http.MethodPost, "/api/plugins/import", "plugin", "plugin.zip", []byte("zip-content"))
	w := httptest.NewRecorder()

	server.handleAPIPluginImport(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "failed to create temp file") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestHandleAPILogs_ReadErrorAndLineClamp(t *testing.T) {
	t.Run("read error returns payload error", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.config.Log.File = server.config.Server.DataDir

		w := httptest.NewRecorder()
		server.handleAPILogs(w, httptest.NewRequest(http.MethodGet, "/api/logs/server?lines=abc", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), "\"error\":") {
			t.Fatalf("expected error payload, body=%s", w.Body.String())
		}
	})

	t.Run("large lines query is clamped to 1000", func(t *testing.T) {
		server, _ := setupTestServer(t)
		logPath := filepath.Join(server.config.Server.DataDir, "server.log")

		var b strings.Builder
		for i := 0; i < 1205; i++ {
			b.WriteString("line\n")
		}
		if err := os.WriteFile(logPath, []byte(b.String()), 0644); err != nil {
			t.Fatalf("write log file: %v", err)
		}
		server.config.Log.File = logPath

		w := httptest.NewRecorder()
		server.handleAPILogs(w, httptest.NewRequest(http.MethodGet, "/api/logs/server?lines=99999", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		lines, ok := resp["lines"].([]interface{})
		if !ok {
			t.Fatalf("lines has unexpected type: %T", resp["lines"])
		}
		if len(lines) != 1000 {
			t.Fatalf("expected 1000 lines after clamp, got %d", len(lines))
		}
	})
}

func TestHandleAPIPluginRoutes_UnknownAction(t *testing.T) {
	server, _ := setupTestServer(t)

	w := httptest.NewRecorder()
	server.handleAPIPluginRoutes(w, httptest.NewRequest(http.MethodGet, "/api/plugins/auth/reload", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
	}
	if !strings.Contains(w.Body.String(), "unknown action") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestHandleAPIPluginRoutes_ActionDelegations(t *testing.T) {
	server, _ := setupTestServer(t)

	wEnable := httptest.NewRecorder()
	server.handleAPIPluginRoutes(wEnable, httptest.NewRequest(http.MethodGet, "/api/plugins/auth/enable", nil))
	if wEnable.Code != http.StatusMethodNotAllowed {
		t.Fatalf("enable delegation status got %d want %d", wEnable.Code, http.StatusMethodNotAllowed)
	}

	wDisable := httptest.NewRecorder()
	server.handleAPIPluginRoutes(wDisable, httptest.NewRequest(http.MethodGet, "/api/plugins/auth/disable", nil))
	if wDisable.Code != http.StatusMethodNotAllowed {
		t.Fatalf("disable delegation status got %d want %d", wDisable.Code, http.StatusMethodNotAllowed)
	}

	wUninstall := httptest.NewRecorder()
	server.handleAPIPluginRoutes(wUninstall, httptest.NewRequest(http.MethodDelete, "/api/plugins/not-installed/uninstall", nil))
	if wUninstall.Code != http.StatusMethodNotAllowed {
		t.Fatalf("uninstall delegation status got %d want %d", wUninstall.Code, http.StatusMethodNotAllowed)
	}

	wEmpty := httptest.NewRecorder()
	server.handleAPIPluginRoutes(wEmpty, httptest.NewRequest(http.MethodGet, "/api/plugins/", nil))
	if wEmpty.Code != http.StatusNotFound {
		t.Fatalf("empty plugin path status got %d want %d", wEmpty.Code, http.StatusNotFound)
	}
}

func TestListProjectFiles_SuccessAndNotFound(t *testing.T) {
	server, _ := setupTestServer(t)

	wMissing := httptest.NewRecorder()
	server.listProjectFiles(wMissing, httptest.NewRequest(http.MethodGet, "/", nil), "missing-project")
	if wMissing.Code != http.StatusNotFound {
		t.Fatalf("missing project status got %d want %d", wMissing.Code, http.StatusNotFound)
	}

	projectDir := filepath.Join(server.config.Server.DataDir, "projects", "p-list")
	if err := os.MkdirAll(filepath.Join(projectDir, "assets"), 0755); err != nil {
		t.Fatalf("mkdir project assets: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "assets", "a.txt"), []byte("x"), 0644); err != nil {
		t.Fatalf("write project file: %v", err)
	}

	w := httptest.NewRecorder()
	server.listProjectFiles(w, httptest.NewRequest(http.MethodGet, "/", nil), "p-list")
	if w.Code != http.StatusOK {
		t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"path":"assets"`) {
		t.Fatalf("expected assets directory in response: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"path":"assets/a.txt"`) {
		t.Fatalf("expected assets/a.txt in response: %s", w.Body.String())
	}
}

func TestHandleAPIProjectFileDetail_ProjectNameRequired(t *testing.T) {
	server, _ := setupTestServer(t)

	w := httptest.NewRecorder()
	server.handleAPIProjectFileDetail(w, httptest.NewRequest(http.MethodGet, "/api/projects//files/index.html", nil))

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "project name required") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestHandleAPIProjectImport_AdditionalBranches(t *testing.T) {
	t.Run("project json path points to directory", func(t *testing.T) {
		server, _ := setupTestServer(t)

		zipData := makeZipData(t, map[string]string{
			"project/project.json/": "",
		})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "dir-project-json.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "project.json not found in ZIP") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("project directory creation failure", func(t *testing.T) {
		server, _ := setupTestServer(t)

		projectsPath := filepath.Join(server.config.Server.DataDir, "projects")
		if err := os.RemoveAll(projectsPath); err != nil {
			t.Fatalf("remove projects dir: %v", err)
		}
		if err := os.WriteFile(projectsPath, []byte("x"), 0644); err != nil {
			t.Fatalf("seed projects file: %v", err)
		}

		zipData := makeZipData(t, map[string]string{
			"project.json": `{"name":"mkdir-fail"}`,
		})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "project.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "failed to create project directory") {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("existing project record is deleted during import flow", func(t *testing.T) {
		server, _ := setupTestServer(t)
		exec := executor.NewExecutor(server.engine)
		if _, err := exec.Execute("INSERT INTO _sys_projects (name, version, installed_at, tables) VALUES ('reimportdemo', '0.0.1', datetime('now'), '')"); err != nil {
			t.Fatalf("seed existing project row: %v", err)
		}
		beforeRes, err := exec.Execute("SELECT name FROM _sys_projects WHERE name = 'reimportdemo'")
		if err != nil {
			t.Fatalf("query seeded project: %v", err)
		}
		if len(beforeRes.Rows) != 1 {
			t.Fatalf("expected seeded row present, got %d", len(beforeRes.Rows))
		}

		zipData := makeZipData(t, map[string]string{
			"project.json": `{"name":"reimportdemo","version":"2.0.0","tables":"users"}`,
		})
		req := makeMultipartRequest(t, http.MethodPost, "/api/projects/import", "project", "project.zip", zipData)
		w := httptest.NewRecorder()

		server.handleAPIProjectImport(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}

		res, err := exec.Execute("SELECT name, version FROM _sys_projects WHERE name = 'reimportdemo'")
		if err != nil {
			t.Fatalf("query imported project: %v", err)
		}
		if len(res.Rows) != 0 {
			t.Fatalf("expected seeded project row to be removed, got %d", len(res.Rows))
		}
	})
}

func TestHandleAPIQuery_ExtraBranches(t *testing.T) {
	t.Run("invalid json body", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPIQuery(w, httptest.NewRequest(http.MethodPost, "/api/query", bytes.NewBufferString("{")))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("sql execution error returned in success envelope", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPIQuery(w, httptest.NewRequest(http.MethodPost, "/api/query", bytes.NewBufferString(`{"sql":"SELECT * FROM no_such_table"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("expected error field in body: %s", w.Body.String())
		}
	})
}

func TestHandleAPIKeyDetail_ExtraGuards(t *testing.T) {
	t.Run("service unavailable when api key manager is nil", func(t *testing.T) {
		server, _ := setupTestServer(t)
		server.apiKeyManager = nil

		w := httptest.NewRecorder()
		server.handleAPIKeyDetail(w, httptest.NewRequest(http.MethodGet, "/api/keys/any", nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status got %d want %d", w.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("empty key id", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		server.handleAPIKeyDetail(w, httptest.NewRequest(http.MethodGet, "/api/keys/", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusBadRequest, w.Body.String())
		}
	})

	t.Run("key not found", func(t *testing.T) {
		server, _ := setupTestServer(t)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/keys/not-found", nil)
		server.handleAPIKeyDetail(w, req.WithContext(setUsername(context.Background(), "admin")))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusNotFound, w.Body.String())
		}
	})
}

func TestGetMicroservice_DirectSuccess(t *testing.T) {
	server, _ := setupTestServer(t)

	exec := executor.NewExecutor(server.engine)
	if _, err := exec.Execute("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('direct-ok', 'return 1', 'd', datetime('now'))"); err != nil {
		t.Fatalf("insert microservice: %v", err)
	}

	w := httptest.NewRecorder()
	server.getMicroservice(w, httptest.NewRequest(http.MethodGet, "/", nil), "direct-ok")
	if w.Code != http.StatusOK {
		t.Fatalf("status got %d want %d body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "direct-ok") {
		t.Fatalf("expected microservice payload in body: %s", w.Body.String())
	}
}
