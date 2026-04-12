package web

import (
	"context"
	"html/template"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
)

func TestGetAPIKeyFromContext(t *testing.T) {
	ctx := setAPIKey(context.Background(), &auth.APIKey{ID: "ak_test", Username: "admin"})
	key := getAPIKey(ctx)
	if key == nil {
		t.Fatal("expected API key in context")
	}
	if key.Username != "admin" {
		t.Fatalf("expected username admin, got %q", key.Username)
	}

	if getAPIKey(context.Background()) != nil {
		t.Fatal("expected nil API key when context has no key")
	}
}

func TestHandleProjectFiles_DirectoryWithoutIndex(t *testing.T) {
	server, _ := setupTestServer(t)

	dir := filepath.Join(server.config.Server.DataDir, "projects", "demo2", "docs")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/projects/demo2/docs", nil)
	w := httptest.NewRecorder()
	server.handleProjectFiles(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status got %d want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleProjectFiles(t *testing.T) {
	server, _ := setupTestServer(t)

	projectsDir := filepath.Join(server.config.Server.DataDir, "projects")
	if err := os.MkdirAll(filepath.Join(projectsDir, "demo", "docs"), 0755); err != nil {
		t.Fatalf("mkdir projects dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectsDir, "demo", "index.html"), []byte("demo home"), 0644); err != nil {
		t.Fatalf("write demo index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectsDir, "demo", "docs", "index.html"), []byte("docs home"), 0644); err != nil {
		t.Fatalf("write docs index: %v", err)
	}

	tests := []struct {
		name         string
		path         string
		wantStatus   int
		wantContains string
	}{
		{name: "missing project name", path: "/projects/", wantStatus: http.StatusBadRequest, wantContains: "Project name required"},
		{name: "directory traversal blocked", path: "/projects/demo/../../etc/passwd", wantStatus: http.StatusBadRequest, wantContains: "Invalid path"},
		{name: "file not found", path: "/projects/demo/missing.txt", wantStatus: http.StatusNotFound, wantContains: "File not found"},
		{name: "default index served", path: "/projects/demo", wantStatus: http.StatusOK, wantContains: "demo home"},
		{name: "directory index served", path: "/projects/demo/docs", wantStatus: http.StatusOK, wantContains: "docs home"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()

			server.handleProjectFiles(w, req)

			if w.Code != tt.wantStatus {
				t.Fatalf("status got %d want %d", w.Code, tt.wantStatus)
			}
			if !strings.Contains(w.Body.String(), tt.wantContains) {
				t.Fatalf("body %q does not contain %q", w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestHandleProjectFileManager(t *testing.T) {
	server, _ := setupTestServer(t)

	tmpl, err := server.loadTemplates()
	if err != nil {
		t.Fatalf("load templates: %v", err)
	}
	server.templates = tmpl

	t.Run("redirect when unauthenticated", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/files/demo", nil)
		w := httptest.NewRecorder()

		server.handleProjectFileManager(w, req)

		if w.Code != http.StatusFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusFound)
		}
		if got := w.Result().Header.Get("Location"); got != "/login" {
			t.Fatalf("location got %q want %q", got, "/login")
		}
	})

	sessionID := "sess_test"
	server.sessions[sessionID] = &Session{
		ID:       sessionID,
		Username: "admin",
		Created:  time.Now(),
		Expires:  time.Now().Add(time.Hour),
	}

	t.Run("redirect when project name missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/files/", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: sessionID})
		w := httptest.NewRecorder()

		server.handleProjectFileManager(w, req)

		if w.Code != http.StatusFound {
			t.Fatalf("status got %d want %d", w.Code, http.StatusFound)
		}
		if got := w.Result().Header.Get("Location"); got != "/projects" {
			t.Fatalf("location got %q want %q", got, "/projects")
		}
	})

	t.Run("render project file manager", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/files/demo", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: sessionID})
		w := httptest.NewRecorder()

		server.handleProjectFileManager(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status got %d want %d", w.Code, http.StatusOK)
		}
		if ct := w.Result().Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
			t.Fatalf("unexpected content type %q", ct)
		}
		if !strings.Contains(w.Body.String(), "demo") {
			t.Fatalf("expected body to contain project name, got %q", w.Body.String())
		}
	})

	t.Run("template execution error", func(t *testing.T) {
		server.templates = template.Must(template.New("only-login").Parse(`{{define "login"}}x{{end}}`))

		req := httptest.NewRequest(http.MethodGet, "/files/demo", nil)
		req.AddCookie(&http.Cookie{Name: "xxsql_session", Value: sessionID})
		w := httptest.NewRecorder()

		server.handleProjectFileManager(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status got %d want %d", w.Code, http.StatusInternalServerError)
		}
	})
}
