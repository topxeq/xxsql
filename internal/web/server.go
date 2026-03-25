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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

//go:embed templates/*.html static/*
var assets embed.FS

// Server represents the web management server.
type Server struct {
	config        *config.Config
	configPath    string
	engine        *storage.Engine
	auth          *auth.Manager
	apiKeyManager *auth.APIKeyManager
	backup        *backup.Manager
	server        *http.Server
	templates     *template.Template
	sessions      map[string]*Session
	startTime     time.Time
	executor      *executor.Executor
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
	var apiKeyMgr *auth.APIKeyManager
	if cfg.Server.DataDir != "" {
		apiKeyMgr = auth.NewAPIKeyManager(cfg.Server.DataDir)
		if err := apiKeyMgr.Load(); err != nil {
			fmt.Printf("[WEB] Warning: failed to load API keys: %v\n", err)
		}
	}

	return &Server{
		config:        cfg,
		engine:        engine,
		auth:          authMgr,
		apiKeyManager: apiKeyMgr,
		backup:        backupMgr,
		sessions:      make(map[string]*Session),
		startTime:     time.Now(),
	}
}

// SetConfigPath sets the configuration file path.
func (s *Server) SetConfigPath(path string) {
	s.configPath = path
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

	// Set singleton engine for plugin operations
	SetSingletonEngine(s.engine)

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
	mux.HandleFunc("/projects", s.handlePage("projects"))
	mux.HandleFunc("/microservices", s.handlePage("microservices"))
	mux.HandleFunc("/plugins", s.handlePage("plugins"))
	mux.HandleFunc("/files/", s.handleProjectFileManager)

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
	mux.HandleFunc("/api/keys", s.handleAPIKeys)
	mux.HandleFunc("/api/keys/", s.handleAPIKeyDetail)
	mux.HandleFunc("/api/admin/reset", s.handleAPIAdminReset)

	// Project API routes
	mux.HandleFunc("/api/projects", s.handleAPIProjects)
	mux.HandleFunc("/api/projects/import", s.handleAPIProjectImport)
	mux.HandleFunc("/api/projects/", s.handleAPIProjectRoutes)

	// Microservice API routes
	mux.HandleFunc("/api/microservices", s.handleAPIMicroservices)
	mux.HandleFunc("/api/microservices/", s.handleAPIMicroserviceDetail)

	// Plugin API routes
	mux.HandleFunc("/api/plugins", s.handleAPIPlugins)
	mux.HandleFunc("/api/plugins/available", s.handleAPIPluginsAvailable)
	mux.HandleFunc("/api/plugins/", s.handleAPIPluginRoutes)

	// Microservice routes (XxScript)
	mux.HandleFunc("/ms/", s.handleMicroservice)

	// Project static files
	mux.HandleFunc("/projects/", s.handleProjectFiles)

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
		// If authentication is disabled, allow all requests
		if !s.config.Auth.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Allow static files and login without auth
		if strings.HasPrefix(r.URL.Path, "/static/") ||
			strings.HasPrefix(r.URL.Path, "/projects/") ||
			r.URL.Path == "/login" ||
			r.URL.Path == "/api/login" {
			next.ServeHTTP(w, r)
			return
		}

		// Allow public microservice endpoints (auth/register, auth/login, etc.)
		if strings.HasPrefix(r.URL.Path, "/ms/auth/register") ||
			strings.HasPrefix(r.URL.Path, "/ms/auth/login") ||
			strings.HasPrefix(r.URL.Path, "/ms/auth/check") ||
			strings.HasPrefix(r.URL.Path, "/ms/health") {
			next.ServeHTTP(w, r)
			return
		}

		// For API routes and microservice routes, check session, API key, or Basic Auth
		if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/ms/") {
			// First, try session authentication
			session := s.getSession(r)
			if session != nil {
				// Set username in context for session
				r = r.WithContext(setUsername(r.Context(), session.Username))
				next.ServeHTTP(w, r)
				return
			}

			// Try API key authentication
			if s.authenticateAPIKey(w, r, next) {
				return
			}

			// Try HTTP Basic Auth
			if s.authenticateBasicAuth(w, r, next) {
				return
			}

			// No valid authentication
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}

		// For non-API routes, check session only
		session := s.getSession(r)
		if session == nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		r = r.WithContext(setUsername(r.Context(), session.Username))
		next.ServeHTTP(w, r)
	})
}

// authenticateBasicAuth authenticates using HTTP Basic Auth.
func (s *Server) authenticateBasicAuth(w http.ResponseWriter, r *http.Request, next http.Handler) bool {
	username, password, ok := r.BasicAuth()
	if !ok {
		return false
	}

	// Validate credentials
	session, err := s.auth.Authenticate(username, password)
	if err != nil {
		return false
	}

	// Set username in context
	r = r.WithContext(setUsername(r.Context(), session.Username))
	next.ServeHTTP(w, r)
	return true
}

// authenticateAPIKey authenticates using API key header.
func (s *Server) authenticateAPIKey(w http.ResponseWriter, r *http.Request, next http.Handler) bool {
	if s.apiKeyManager == nil {
		return false
	}

	// Check X-API-Key header
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		return false
	}

	// Validate key
	key, err := s.apiKeyManager.ValidateKey(apiKey)
	if err != nil {
		return false
	}

	// Set username in context
	r = r.WithContext(setUsername(r.Context(), key.Username))
	r = r.WithContext(setAPIKey(r.Context(), key))

	next.ServeHTTP(w, r)
	return true
}

// contextKey type for context keys.
type contextKey string

const (
	usernameKey contextKey = "username"
	apiKeyKey   contextKey = "apiKey"
)

// setUsername sets username in context.
func setUsername(ctx context.Context, username string) context.Context {
	return context.WithValue(ctx, usernameKey, username)
}

// getUsername gets username from context.
func getUsername(ctx context.Context) string {
	if v := ctx.Value(usernameKey); v != nil {
		return v.(string)
	}
	return ""
}

// setAPIKey sets API key in context.
func setAPIKey(ctx context.Context, key *auth.APIKey) context.Context {
	return context.WithValue(ctx, apiKeyKey, key)
}

// getAPIKey gets API key from context.
func getAPIKey(ctx context.Context) *auth.APIKey {
	if v := ctx.Value(apiKeyKey); v != nil {
		return v.(*auth.APIKey)
	}
	return nil
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

// handleProjectFiles serves static files from deployed projects.
func (s *Server) handleProjectFiles(w http.ResponseWriter, r *http.Request) {
	// Path format: /projects/{projectName}/...
	path := strings.TrimPrefix(r.URL.Path, "/projects/")
	if path == "" {
		http.Error(w, "Project name required", http.StatusBadRequest)
		return
	}

	// Split path into project name and file path
	parts := strings.SplitN(path, "/", 2)
	projectName := parts[0]
	var filePath string
	if len(parts) > 1 {
		filePath = parts[1]
	} else {
		filePath = "index.html"
	}

	// Build full file path
	fullPath := filepath.Join(s.config.Server.DataDir, "projects", projectName, filePath)

	// Security: prevent directory traversal
	if strings.Contains(filePath, "..") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Check if file exists
	info, err := os.Stat(fullPath)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// If it's a directory, try index.html
	if info.IsDir() {
		fullPath = filepath.Join(fullPath, "index.html")
		info, err = os.Stat(fullPath)
		if err != nil {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
	}

	// Serve the file
	http.ServeFile(w, r, fullPath)
}

// handleProjectFileManager handles the file management page.
func (s *Server) handleProjectFileManager(w http.ResponseWriter, r *http.Request) {
	// Check authentication
	session := s.getSession(r)
	if session == nil {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	// Extract project name from path /files/{projectName}
	path := strings.TrimPrefix(r.URL.Path, "/files/")
	if path == "" {
		http.Redirect(w, r, "/projects", http.StatusFound)
		return
	}

	// Split to get project name
	parts := strings.SplitN(path, "/", 2)
	projectName := parts[0]

	data := map[string]interface{}{
		"Page":        "files",
		"User":        session.Username,
		"Server":      s.config.Server.Name,
		"Version":     "0.0.1",
		"ProjectName": projectName,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates.ExecuteTemplate(w, "project-files", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
