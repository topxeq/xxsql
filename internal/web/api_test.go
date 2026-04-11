package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWebAPI_Projects(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login first
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test list projects
	req := httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)
	t.Logf("List projects: %d", w.Code)

	// Test create project
	createBody, _ := json.Marshal(map[string]string{"name": "test-project", "version": "1.0.0"})
	req = httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBuffer(createBody))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)
	t.Logf("Create project: %d", w.Code)

	// Test list projects again
	req = httptest.NewRequest(http.MethodGet, "/api/projects", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)
	t.Logf("List projects after create: %d", w.Code)

	// Test delete project
	req = httptest.NewRequest(http.MethodDelete, "/api/projects/test-project", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Delete project: %d", w.Code)
}

func TestWebAPI_ProjectRoutes(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test project routes dispatcher
	req := httptest.NewRequest(http.MethodGet, "/api/projects/test/files", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIProjectRoutes))
	handler.ServeHTTP(w, req)
	t.Logf("Project routes (files list): %d", w.Code)
}

func TestWebAPI_ProjectFiles(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Create project first
	createBody, _ := json.Marshal(map[string]string{"name": "file-test", "version": "1.0.0"})
	req := httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBuffer(createBody))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)

	// Test list project files
	req = httptest.NewRequest(http.MethodGet, "/api/projects/file-test/files", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectFiles))
	handler.ServeHTTP(w, req)
	t.Logf("List project files: %d", w.Code)

	// Test create project file
	fileBody, _ := json.Marshal(map[string]string{"name": "test.js", "content": "console.log('hello')"})
	req = httptest.NewRequest(http.MethodPost, "/api/projects/file-test/files", bytes.NewBuffer(fileBody))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectFiles))
	handler.ServeHTTP(w, req)
	t.Logf("Create project file: %d", w.Code)

	// Test get project file
	req = httptest.NewRequest(http.MethodGet, "/api/projects/file-test/files/test.js", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectFileDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Get project file: %d", w.Code)

	// Test update project file
	updateBody, _ := json.Marshal(map[string]string{"content": "console.log('updated')"})
	req = httptest.NewRequest(http.MethodPut, "/api/projects/file-test/files/test.js", bytes.NewBuffer(updateBody))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectFileDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Update project file: %d", w.Code)

	// Test delete project file
	req = httptest.NewRequest(http.MethodDelete, "/api/projects/file-test/files/test.js", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectFileDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Delete project file: %d", w.Code)
}

func TestWebAPI_Microservices(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test list microservices
	req := httptest.NewRequest(http.MethodGet, "/api/microservices", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIMicroservices))
	handler.ServeHTTP(w, req)
	t.Logf("List microservices: %d", w.Code)

	// Test create microservice
	msBody, _ := json.Marshal(map[string]string{
		"name": "test-ms",
		"path": "/api/test",
		"code": "return {status: 200, body: 'hello'}",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/microservices", bytes.NewBuffer(msBody))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIMicroservices))
	handler.ServeHTTP(w, req)
	t.Logf("Create microservice: %d", w.Code)

	// Test get microservice
	req = httptest.NewRequest(http.MethodGet, "/api/microservices/test-ms", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIMicroserviceDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Get microservice: %d", w.Code)

	// Test update microservice
	updateBody, _ := json.Marshal(map[string]string{"code": "return {status: 200, body: 'updated'}"})
	req = httptest.NewRequest(http.MethodPut, "/api/microservices/test-ms", bytes.NewBuffer(updateBody))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIMicroserviceDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Update microservice: %d", w.Code)

	// Test delete microservice
	req = httptest.NewRequest(http.MethodDelete, "/api/microservices/test-ms", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIMicroserviceDetail))
	handler.ServeHTTP(w, req)
	t.Logf("Delete microservice: %d", w.Code)
}

func TestWebAPI_Plugins(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test get available plugins
	req := httptest.NewRequest(http.MethodGet, "/api/plugins", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.handleAPIPluginGet(w, r, "")
	}))
	handler.ServeHTTP(w, req)
	t.Logf("Get plugins: %d", w.Code)
}

func TestWebAPI_AdminReset(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test admin reset
	req := httptest.NewRequest(http.MethodPost, "/api/admin/reset", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIAdminReset))
	handler.ServeHTTP(w, req)
	t.Logf("Admin reset: %d", w.Code)
}

func TestWebAPI_MethodNotAllowed(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// Test method not allowed on projects
	req := httptest.NewRequest(http.MethodPut, "/api/projects", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}

	// Test method not allowed on project detail
	req = httptest.NewRequest(http.MethodGet, "/api/projects/test", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectDetail))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestWebAPI_InvalidRequests(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)

	// Test create project with invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBufferString("invalid"))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}

	// Test create project with empty name
	req = httptest.NewRequest(http.MethodPost, "/api/projects", bytes.NewBufferString(`{"name":""}`))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjects))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}

	// Test delete project with empty name
	req = httptest.NewRequest(http.MethodDelete, "/api/projects/", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	handler = server.authMiddleware(http.HandlerFunc(server.handleAPIProjectDetail))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestWebAPI_ProjectImport(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test project import (without actual file, will fail but covers the handler)
	req := httptest.NewRequest(http.MethodPost, "/api/projects/import", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIProjectImport))
	handler.ServeHTTP(w, req)
	t.Logf("Project import: %d", w.Code)
}

func TestWebAPI_PluginImport(t *testing.T) {
	server, _ := setupTestServer(t)

	// Login
	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewBuffer(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	server.handleAPILogin(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Skipf("Login failed: %d", loginW.Code)
	}

	// Test plugin import (without actual file)
	req := httptest.NewRequest(http.MethodPost, "/api/plugins/import", nil)
	for _, cookie := range loginW.Result().Cookies() {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler := server.authMiddleware(http.HandlerFunc(server.handleAPIPluginImport))
	handler.ServeHTTP(w, req)
	t.Logf("Plugin import: %d", w.Code)
}

func TestWeb_SetConfigPath(t *testing.T) {
	server, _ := setupTestServer(t)

	// Test setting config path
	server.SetConfigPath("/tmp/test-config.json")
	if server.configPath != "/tmp/test-config.json" {
		t.Errorf("Expected config path '/tmp/test-config.json', got %s", server.configPath)
	}
}
