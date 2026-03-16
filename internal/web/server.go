// Package web provides a web management interface for XxSql.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/storage"
)

//go:embed templates/*.html static/*
var assets embed.FS

// Server represents the web management server.
type Server struct {
	config     *config.Config
	engine     *storage.Engine
	auth       *auth.Manager
	backup     *backup.Manager
	server     *http.Server
	templates  *template.Template
	sessions   map[string]*Session
	startTime  time.Time
}

// Session represents a web session.
type Session struct {
	ID       string
	Username string
	Created  time.Time
	Expires  time.Time
}

// NewServer creates a new web management server.
func NewServer(cfg *config.Config, engine *storage.Engine, authMgr *auth.Manager, backupMgr *backup.Manager) *Server {
	return &Server{
		config:    cfg,
		engine:    engine,
		auth:      authMgr,
		backup:    backupMgr,
		sessions:  make(map[string]*Session),
		startTime: time.Now(),
	}
}

// Start starts the web server.
func (s *Server) Start() error {
	// Load templates
	var err error
	s.templates, err = s.loadTemplates()
	if err != nil {
		return fmt.Errorf("load templates: %w", err)
	}

	// Create mux
	mux := http.NewServeMux()

	// Static files
	staticFS, _ := fs.Sub(assets, "static")
	fileServer := http.FileServer(http.FS(staticFS))
	mux.Handle("/static/", http.StripPrefix("/static/", fileServer))

	// Page routes
	mux.HandleFunc("/", s.handlePage("index"))
	mux.HandleFunc("/query", s.handlePage("query"))
	mux.HandleFunc("/tables", s.handlePage("tables"))
	mux.HandleFunc("/users", s.handlePage("users"))
	mux.HandleFunc("/backup", s.handlePage("backup"))
	mux.HandleFunc("/logs", s.handlePage("logs"))
	mux.HandleFunc("/config", s.handlePage("config"))
	mux.HandleFunc("/login", s.handleLoginPage)

	// API routes
	mux.HandleFunc("/api/status", s.handleAPIStatus)
	mux.HandleFunc("/api/metrics", s.handleAPIMetrics)
	mux.HandleFunc("/api/query", s.handleAPIQuery)
	mux.HandleFunc("/api/tables", s.handleAPITables)
	mux.HandleFunc("/api/tables/", s.handleAPITableDetail)
	mux.HandleFunc("/api/users", s.handleAPIUsers)
	mux.HandleFunc("/api/users/", s.handleAPIUserDetail)
	mux.HandleFunc("/api/backups", s.handleAPIBackups)
	mux.HandleFunc("/api/backups/", s.handleAPIBackupDetail)
	mux.HandleFunc("/api/restore", s.handleAPIRestore)
	mux.HandleFunc("/api/logs/", s.handleAPILogs)
	mux.HandleFunc("/api/config", s.handleAPIConfig)
	mux.HandleFunc("/api/login", s.handleAPILogin)
	mux.HandleFunc("/api/logout", s.handleAPILogout)

	// Create server
	addr := fmt.Sprintf("%s:%d", s.config.Network.Bind, s.config.Network.HTTPPort)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.loggingMiddleware(s.authMiddleware(mux)),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start listener
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	go s.server.Serve(listener)

	return nil
}

// Stop stops the web server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// loadTemplates loads HTML templates from embedded assets.
func (s *Server) loadTemplates() (*template.Template, error) {
	tmpl := template.New("")

	// Define template functions
	tmpl.Funcs(template.FuncMap{
		"json": func(v interface{}) string {
			data, _ := json.Marshal(v)
			return string(data)
		},
		"lower": strings.ToLower,
		"upper": strings.ToUpper,
	})

	// Walk templates directory
	entries, err := assets.ReadDir("templates")
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".html") {
			path := "templates/" + entry.Name()
			data, err := assets.ReadFile(path)
			if err != nil {
				return nil, err
			}

			name := strings.TrimSuffix(entry.Name(), ".html")
			tmpl = template.Must(tmpl.New(name).Parse(string(data)))
		}
	}

	return tmpl, nil
}

// handlePage returns a handler that renders a page template.
func (s *Server) handlePage(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check authentication
		session := s.getSession(r)
		if session == nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		data := map[string]interface{}{
			"Page":     name,
			"User":     session.Username,
			"Server":   s.config.Server.Name,
			"Version":  "0.0.1",
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := s.templates.ExecuteTemplate(w, name, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// handleLoginPage handles the login page.
func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Already logged in?
		if session := s.getSession(r); session != nil {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		s.templates.ExecuteTemplate(w, "login", nil)
		return
	}
}

// loggingMiddleware logs HTTP requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status
		rw := &responseWriter{ResponseWriter: w}

		next.ServeHTTP(rw, r)

		duration := time.Since(start)
		fmt.Printf("[WEB] %s %s %d %v\n", r.Method, r.URL.Path, rw.status, duration)
	})
}

// authMiddleware checks authentication for protected routes.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow static files and login without auth
		if strings.HasPrefix(r.URL.Path, "/static/") ||
			r.URL.Path == "/login" ||
			r.URL.Path == "/api/login" {
			next.ServeHTTP(w, r)
			return
		}

		// Check session
		session := s.getSession(r)
		if session == nil && !strings.HasPrefix(r.URL.Path, "/api/") {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// readJSON reads JSON from request body.
func readJSON(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
