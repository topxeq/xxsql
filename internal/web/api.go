package web

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/xxscript"
)

// handleAPIStatus handles GET /api/status.
func (s *Server) handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := s.engine.Stats()
	uptime := time.Since(s.startTime)

	status := map[string]interface{}{
		"version":     "0.0.1",
		"uptime":      uptime.String(),
		"uptime_sec":  int(uptime.Seconds()),
		"table_count": stats.TableCount,
		"server_name": s.config.Server.Name,
		"data_dir":    s.config.Server.DataDir,
	}

	writeJSON(w, http.StatusOK, status)
}

// handleAPIMetrics handles GET /api/metrics.
func (s *Server) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := s.engine.Stats()

	tables := make([]map[string]interface{}, len(stats.Tables))
	for i, t := range stats.Tables {
		tables[i] = map[string]interface{}{
			"name":       t.Name,
			"row_count":  t.RowCount,
			"page_count": t.PageCount,
		}
	}

	metrics := map[string]interface{}{
		"storage": map[string]interface{}{
			"table_count": stats.TableCount,
			"tables":      tables,
		},
		"buffer_pool": map[string]interface{}{
			"size": s.config.Storage.BufferPoolSize,
		},
		"wal": map[string]interface{}{
			"max_size_mb":    s.config.Storage.WALMaxSizeMB,
			"sync_interval": s.config.Storage.WALSyncInterval,
		},
	}

	writeJSON(w, http.StatusOK, metrics)
}

// handleAPIQuery handles POST /api/query.
func (s *Server) handleAPIQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		SQL string `json:"sql"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		writeError(w, http.StatusBadRequest, "empty query")
		return
	}

	// Execute query
	start := time.Now()
	exec := executor.NewExecutor(s.engine)

	result, err := exec.Execute(req.SQL)

	duration := time.Since(start)

	if err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"error":    err.Error(),
			"duration": duration.String(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"columns":   result.Columns,
		"rows":      result.Rows,
		"row_count": result.RowCount,
		"affected":  result.Affected,
		"message":   result.Message,
		"duration":  duration.String(),
	})
}

// handleAPITables handles GET /api/tables.
func (s *Server) handleAPITables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tableNames := s.engine.ListTables()
	tables := make([]map[string]interface{}, len(tableNames))

	for i, name := range tableNames {
		tbl, err := s.engine.GetTable(name)
		if err != nil {
			continue
		}
		info := tbl.GetInfo()

		tables[i] = map[string]interface{}{
			"name":       name,
			"row_count":  info.RowCount,
			"page_count": info.NextPageID - 1,
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables": tables,
	})
}

// handleAPITableDetail handles /api/tables/{name} and /api/tables/{name}/data.
func (s *Server) handleAPITableDetail(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tables/")

	// Check if it's a data request
	if strings.HasSuffix(path, "/data") {
		s.handleAPITableData(w, r, strings.TrimSuffix(path, "/data"))
		return
	}

	// Table detail
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tableName := path
	tbl, err := s.engine.GetTable(tableName)
	if err != nil {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	info := tbl.GetInfo()
	columns := make([]map[string]interface{}, len(info.Columns))

	for i, col := range info.Columns {
		columns[i] = map[string]interface{}{
			"name":       col.Name,
			"type":       col.Type.String(),
			"nullable":   col.Nullable,
			"primary":    col.PrimaryKey,
			"auto_incr":  col.AutoIncr,
			"size":       col.Size,
		}
	}

	result := map[string]interface{}{
		"name":       tableName,
		"row_count":  info.RowCount,
		"page_count": info.NextPageID - 1,
		"columns":    columns,
	}

	writeJSON(w, http.StatusOK, result)
}

// handleAPITableData handles GET /api/tables/{name}/data.
func (s *Server) handleAPITableData(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Get pagination params
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize := 50
	offset := (page - 1) * pageSize

	// Execute query
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, pageSize, offset))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"columns": result.Columns,
		"rows":    result.Rows,
		"page":    page,
	})
}

// handleAPIUsers handles GET /api/users and POST /api/users.
func (s *Server) handleAPIUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		users := s.auth.ListUsers()
		result := make([]map[string]interface{}, len(users))
		for i, u := range users {
			result[i] = map[string]interface{}{
				"username": u.Username,
				"role":     u.Role.String(),
			}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"users": result})

	case http.MethodPost:
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		role := auth.RoleUser
		if req.Role == "admin" {
			role = auth.RoleAdmin
		}

		_, err := s.auth.CreateUser(req.Username, req.Password, role)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"message": "user created"})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIUserDetail handles /api/users/{name}.
func (s *Server) handleAPIUserDetail(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimPrefix(r.URL.Path, "/api/users/")

	switch r.Method {
	case http.MethodGet:
		user, err := s.auth.GetUser(username)
		if err != nil {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"username": user.Username,
			"role":     user.Role.String(),
		})

	case http.MethodPut:
		var req struct {
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		if req.Password != "" {
			// ChangePassword requires old password, but for admin we'll use a workaround
			// In a real app, you'd have an admin password reset function
			writeJSON(w, http.StatusOK, map[string]string{"message": "password change requires old password"})
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"message": "user updated"})

	case http.MethodDelete:
		if err := s.auth.DeleteUser(username); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"message": "user deleted"})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIBackups handles GET /api/backups and POST /api/backups.
func (s *Server) handleAPIBackups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		backupDir := s.config.Backup.BackupDir
		backups, err := backup.ListBackups(backupDir)
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]interface{}{"backups": []string{}})
			return
		}

		result := make([]map[string]interface{}, len(backups))
		for i, b := range backups {
			name := filepath.Base(b)
			stat, _ := os.Stat(b)
			result[i] = map[string]interface{}{
				"name": name,
				"path": b,
				"size": stat.Size(),
			}
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"backups": result})

	case http.MethodPost:
		var req struct {
			Path     string `json:"path"`
			Compress bool   `json:"compress"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		if req.Path == "" {
			req.Path = filepath.Join(s.config.Backup.BackupDir,
				fmt.Sprintf("backup_%s.xbak", time.Now().Format("20060102_150405")))
		}

		mgr := backup.NewManager(s.engine.GetDataDir())
		manifest, err := mgr.Backup(backup.BackupOptions{
			Path:     req.Path,
			Compress: req.Compress,
			Database: s.config.Server.Name,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"message":     "backup created",
			"table_count": manifest.TableCount,
			"timestamp":   manifest.Timestamp,
		})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIBackupDetail handles GET /api/backups/{name}.
func (s *Server) handleAPIBackupDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/api/backups/")
	path := filepath.Join(s.config.Backup.BackupDir, name)

	// Serve file download
	http.ServeFile(w, r, path)
}

// handleAPIRestore handles POST /api/restore.
func (s *Server) handleAPIRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	mgr := backup.NewManager(s.engine.GetDataDir())
	manifest, err := mgr.Restore(backup.RestoreOptions{
		Path: req.Path,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":     "restore completed",
		"table_count": manifest.TableCount,
	})
}

// handleAPILogs handles GET /api/logs/{type}.
func (s *Server) handleAPILogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	logType := strings.TrimPrefix(r.URL.Path, "/api/logs/")
	lines := 100
	if l := r.URL.Query().Get("lines"); l != "" {
		lines, _ = strconv.Atoi(l)
		if lines > 1000 {
			lines = 1000
		}
	}

	var logPath string
	switch logType {
	case "server":
		logPath = s.config.Log.File
	case "audit":
		logPath = s.config.Security.AuditFile
	default:
		writeError(w, http.StatusBadRequest, "unknown log type")
		return
	}

	if logPath == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{"lines": []string{}})
		return
	}

	// Read last N lines
	logLines, err := readLastLines(logPath, lines)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"lines": []string{}, "error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"lines": logLines})
}

// handleAPIConfig handles GET /api/config and PUT /api/config.
func (s *Server) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// Return config (hide sensitive fields)
		cfgCopy := *s.config
		cfgCopy.Auth.AdminPassword = "" // Don't expose password
		writeJSON(w, http.StatusOK, cfgCopy)

	case http.MethodPut:
		// Parse updates
		var updates map[string]interface{}
		if err := readJSON(r, &updates); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		// Track what was updated and what needs restart
		updated := make([]string, 0)
		needsRestart := make([]string, 0)
		errors := make([]string, 0)

		// Apply updates to config
		for key, value := range updates {
			switch key {
			// Log config - hot reloadable
			case "log":
				if logMap, ok := value.(map[string]interface{}); ok {
					if level, ok := logMap["level"].(string); ok {
						s.config.Log.Level = level
						updated = append(updated, "log.level")
						// Update logger level dynamically
						if s.executor != nil {
							// Logger update would go here if we had access
						}
					}
					if maxSize, ok := logMap["max_size_mb"].(float64); ok {
						s.config.Log.MaxSizeMB = int(maxSize)
						updated = append(updated, "log.max_size_mb")
					}
					if maxBackups, ok := logMap["max_backups"].(float64); ok {
						s.config.Log.MaxBackups = int(maxBackups)
						updated = append(updated, "log.max_backups")
					}
					if maxAge, ok := logMap["max_age_days"].(float64); ok {
						s.config.Log.MaxAgeDays = int(maxAge)
						updated = append(updated, "log.max_age_days")
					}
					if compress, ok := logMap["compress"].(bool); ok {
						s.config.Log.Compress = compress
						updated = append(updated, "log.compress")
					}
				}

			// Backup config
			case "backup":
				if backupMap, ok := value.(map[string]interface{}); ok {
					if interval, ok := backupMap["auto_interval_hours"].(float64); ok {
						s.config.Backup.AutoIntervalHours = int(interval)
						updated = append(updated, "backup.auto_interval_hours")
					}
					if keepCount, ok := backupMap["keep_count"].(float64); ok {
						s.config.Backup.KeepCount = int(keepCount)
						updated = append(updated, "backup.keep_count")
					}
					if dir, ok := backupMap["backup_dir"].(string); ok {
						s.config.Backup.BackupDir = dir
						updated = append(updated, "backup.backup_dir")
					}
				}

			// Security config
			case "security":
				if secMap, ok := value.(map[string]interface{}); ok {
					if auditEnabled, ok := secMap["audit_enabled"].(bool); ok {
						s.config.Security.AuditEnabled = auditEnabled
						updated = append(updated, "security.audit_enabled")
					}
					if rateLimitEnabled, ok := secMap["rate_limit_enabled"].(bool); ok {
						s.config.Security.RateLimitEnabled = rateLimitEnabled
						updated = append(updated, "security.rate_limit_enabled")
					}
					if rateLimitMaxAttempts, ok := secMap["rate_limit_max_attempts"].(float64); ok {
						s.config.Security.RateLimitMaxAttempts = int(rateLimitMaxAttempts)
						updated = append(updated, "security.rate_limit_max_attempts")
					}
					if rateLimitWindow, ok := secMap["rate_limit_window_min"].(float64); ok {
						s.config.Security.RateLimitWindowMin = int(rateLimitWindow)
						updated = append(updated, "security.rate_limit_window_min")
					}
					if rateLimitBlock, ok := secMap["rate_limit_block_min"].(float64); ok {
						s.config.Security.RateLimitBlockMin = int(rateLimitBlock)
						updated = append(updated, "security.rate_limit_block_min")
					}
				}

			// Connection config - needs restart
			case "connection":
				if connMap, ok := value.(map[string]interface{}); ok {
					if maxConn, ok := connMap["max_connections"].(float64); ok {
						s.config.Connection.MaxConnections = int(maxConn)
						updated = append(updated, "connection.max_connections")
						needsRestart = append(needsRestart, "connection.max_connections")
					}
					if waitTimeout, ok := connMap["wait_timeout"].(float64); ok {
						s.config.Connection.WaitTimeout = int(waitTimeout)
						updated = append(updated, "connection.wait_timeout")
					}
					if idleTimeout, ok := connMap["idle_timeout"].(float64); ok {
						s.config.Connection.IdleTimeout = int(idleTimeout)
						updated = append(updated, "connection.idle_timeout")
					}
				}

			// Network config - needs restart
			case "network":
				if netMap, ok := value.(map[string]interface{}); ok {
					if bind, ok := netMap["bind"].(string); ok {
						s.config.Network.Bind = bind
						updated = append(updated, "network.bind")
						needsRestart = append(needsRestart, "network.bind")
					}
				}

			// Server config - needs restart
			case "server":
				if srvMap, ok := value.(map[string]interface{}); ok {
					if name, ok := srvMap["name"].(string); ok {
						s.config.Server.Name = name
						updated = append(updated, "server.name")
					}
				}

			default:
				errors = append(errors, fmt.Sprintf("unknown config key: %s", key))
			}
		}

		// Save config to file if path is set
		if s.configPath != "" && len(updated) > 0 {
			if err := config.Save(s.config, s.configPath); err != nil {
				errors = append(errors, fmt.Sprintf("failed to save config: %v", err))
			}
		}

		// Build response
		response := map[string]interface{}{
			"success":        len(updated) > 0,
			"updated":        updated,
			"needs_restart":  needsRestart,
			"errors":         errors,
			"config_path":    s.configPath,
		}

		if len(needsRestart) > 0 {
			response["message"] = "Configuration updated. Some changes require server restart to take effect."
		} else if len(updated) > 0 {
			response["message"] = "Configuration updated successfully."
		} else {
			response["message"] = "No changes made."
		}

		writeJSON(w, http.StatusOK, response)

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// readLastLines reads the last N lines from a file.
func readLastLines(path string, n int) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}

	return lines, scanner.Err()
}

// ============================================================================
// API Key Management
// ============================================================================

// handleAPIKeys handles GET /api/keys and POST /api/keys.
func (s *Server) handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	if s.apiKeyManager == nil {
		writeError(w, http.StatusServiceUnavailable, "API key management not available")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// List API keys for current user (or all for admin)
		username := getUsername(r.Context())
		var keys []*auth.APIKey

		// Check if admin
		user, err := s.auth.GetUser(username)
		if err == nil && user.Role == auth.RoleAdmin {
			keys = s.apiKeyManager.ListAllKeys()
		} else {
			keys = s.apiKeyManager.ListKeys(username)
		}

		// Sanitize - don't expose key hashes
		result := make([]map[string]interface{}, len(keys))
		for i, k := range keys {
			result[i] = map[string]interface{}{
				"id":           k.ID,
				"name":         k.Name,
				"username":     k.Username,
				"permissions":  k.Permissions,
				"created_at":   k.CreatedAt,
				"expires_at":   k.ExpiresAt,
				"last_used_at": k.LastUsedAt,
				"enabled":      k.Enabled,
			}
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"keys": result})

	case http.MethodPost:
		// Create new API key
		var req struct {
			Name        string `json:"name"`
			ExpiresIn   int64  `json:"expires_in"`  // seconds, 0 = no expiration
			Permissions uint32 `json:"permissions"` // permission bits
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		if req.Name == "" {
			req.Name = "API Key"
		}

		username := getUsername(r.Context())
		perms := auth.Permission(req.Permissions)
		if perms == 0 {
			// Default to user's role permissions
			user, err := s.auth.GetUser(username)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "user not found")
				return
			}
			perms = auth.RolePermissions[user.Role]
		}

		var expiresIn time.Duration
		if req.ExpiresIn > 0 {
			expiresIn = time.Duration(req.ExpiresIn) * time.Second
		}

		fullKey, key, err := s.apiKeyManager.GenerateKey(req.Name, username, perms, expiresIn)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"message":      "API key created",
			"id":           key.ID,
			"name":         key.Name,
			"key":          fullKey, // Only shown once!
			"permissions":  key.Permissions,
			"created_at":   key.CreatedAt,
			"expires_at":   key.ExpiresAt,
			"_warning":     "Store this key securely. It will not be shown again.",
		})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIKeyDetail handles /api/keys/{id}.
func (s *Server) handleAPIKeyDetail(w http.ResponseWriter, r *http.Request) {
	if s.apiKeyManager == nil {
		writeError(w, http.StatusServiceUnavailable, "API key management not available")
		return
	}

	keyID := strings.TrimPrefix(r.URL.Path, "/api/keys/")
	if keyID == "" {
		writeError(w, http.StatusBadRequest, "key ID required")
		return
	}

	key, err := s.apiKeyManager.GetKey(keyID)
	if err != nil {
		writeError(w, http.StatusNotFound, "API key not found")
		return
	}

	// Check permission - user can only manage their own keys (unless admin)
	username := getUsername(r.Context())
	user, err := s.auth.GetUser(username)
	isAdmin := err == nil && user.Role == auth.RoleAdmin

	if key.Username != username && !isAdmin {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":           key.ID,
			"name":         key.Name,
			"username":     key.Username,
			"permissions":  key.Permissions,
			"created_at":   key.CreatedAt,
			"expires_at":   key.ExpiresAt,
			"last_used_at": key.LastUsedAt,
			"enabled":      key.Enabled,
		})

	case http.MethodPut:
		var req struct {
			Enabled *bool `json:"enabled"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		if req.Enabled != nil {
			if err := s.apiKeyManager.EnableKey(keyID, *req.Enabled); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		writeJSON(w, http.StatusOK, map[string]string{"message": "API key updated"})

	case http.MethodDelete:
		if err := s.apiKeyManager.RevokeKey(keyID); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"message": "API key revoked"})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleMicroservice handles /ms/<table>/<skey> or /ms/<skey> requests.
// It looks up the script from the specified table (defaults to _sys_ms) and executes it.
func (s *Server) handleMicroservice(w http.ResponseWriter, r *http.Request) {
	// Parse path: /ms/<table>/<skey> or /ms/<skey>
	// skey can contain slashes, so we need to handle it specially
	path := strings.TrimPrefix(r.URL.Path, "/ms/")

	// Check for empty path after /ms/
	if path == "" || path == "/" {
		writeError(w, http.StatusBadRequest, "invalid path format")
		return
	}

	var tableName, skey string

	// Split path into parts
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		// Format: /ms/<skey> - default to _sys_ms table
		tableName = "_sys_ms"
		skey = parts[0]
		// Check if this looks like a table name (the skey matches an existing table)
		// If so, it's missing the actual skey
		if _, err := s.engine.GetTable(skey); err == nil {
			// skey matches an existing table name - this is /ms/<table> without skey
			writeError(w, http.StatusBadRequest, "missing skey - use format /ms/<table>/<skey>")
			return
		}
	} else {
		// Format: /ms/<table>/<skey> or /ms/<skey-with-slash>
		// Check if the first part is an existing table
		if _, err := s.engine.GetTable(parts[0]); err == nil {
			// First part is a table name
			tableName = parts[0]
			skey = parts[1]
		} else {
			// First part is not a table, treat entire path as skey in _sys_ms
			tableName = "_sys_ms"
			skey = path
		}
	}

	if skey == "" {
		writeError(w, http.StatusBadRequest, "invalid path format - missing skey")
		return
	}

	// Query the script from the table
	// Table must have SKEY (primary key) and SCRIPT columns
	// Use LIKE instead of = to work around a bug with VARCHAR primary key equality
	query := fmt.Sprintf("SELECT SCRIPT FROM %s WHERE SKEY LIKE '%s'", tableName, skey)

	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("query error: %v", err))
		return
	}

	if len(result.Rows) == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("script not found: %s/%s", tableName, skey))
		return
	}

	// Get the script
	var script string
	if len(result.Rows[0]) > 0 {
		script = fmt.Sprintf("%v", result.Rows[0][0])
	}

	if script == "" {
		writeError(w, http.StatusNotFound, "empty script")
		return
	}

	// Create execution context
	ctx := xxscript.NewContext()
	ctx.Executor = exec
	ctx.Engine = s.engine
	ctx.HTTPWriter = w
	ctx.HTTPRequest = r
	ctx.BaseDir = s.config.Server.DataDir // Set base directory for file operations
	ctx.SetupBuiltins()

	// Execute the script
	_, err = xxscript.Run(script, ctx)
	if err != nil {
		// Return error as JSON if no content written yet
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": %q}`, err.Error())
		return
	}
}

// handleAPIAdminReset handles POST /api/admin/reset.
// It resets the server to its initial state by:
// - Dropping all user tables (keeping _sys_ms, _sys_projects)
// - Clearing _sys_projects table
// - Deleting all files in projects/ directory
func (s *Server) handleAPIAdminReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Check if user is admin
	username := getUsername(r.Context())
	if username == "" {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	user, err := s.auth.GetUser(username)
	if err != nil || user.Role != auth.RoleAdmin {
		writeError(w, http.StatusForbidden, "admin access required")
		return
	}

	// Parse request
	var req struct {
		Confirm string `json:"confirm"`
		Full    bool   `json:"full"` // Full reset also deletes user accounts (except admin)
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	// Require confirmation
	if req.Confirm != "RESET" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "confirmation required: send {\"confirm\": \"RESET\"}",
		})
		return
	}

	// Perform reset
	result := s.engine.ResetToInitialState(s.config.Server.DataDir)

	// Full reset: delete all users except admin
	if req.Full {
		users := s.auth.ListUsers()
		for _, u := range users {
			if u.Username != "admin" {
				s.auth.DeleteUser(u.Username)
			}
		}
		result["users_deleted"] = len(users) - 1
	}

	result["success"] = true
	result["message"] = "Server reset to initial state"

	writeJSON(w, http.StatusOK, result)
}

// ============================================================================
// Projects API
// ============================================================================

// handleAPIProjects handles GET/POST /api/projects
func (s *Server) handleAPIProjects(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listProjects(w, r)
	case http.MethodPost:
		s.createProject(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIProjectDetail handles DELETE /api/projects/{name}
func (s *Server) handleAPIProjectDetail(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/projects/")
	if name == "" {
		writeError(w, http.StatusBadRequest, "project name required")
		return
	}

	switch r.Method {
	case http.MethodDelete:
		s.deleteProject(w, r, name)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIProjectRoutes handles all /api/projects/* routes including files
func (s *Server) handleAPIProjectRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/projects/")

	// Check if it's a file operation (contains /files/)
	if strings.Contains(path, "/files/") {
		s.handleAPIProjectFileDetail(w, r)
		return
	}

	// Check if it's listing files (ends with /files)
	if strings.HasSuffix(path, "/files") {
		s.handleAPIProjectFiles(w, r)
		return
	}

	// Otherwise it's project detail
	s.handleAPIProjectDetail(w, r)
}

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute("SELECT name, version, installed_at, tables FROM _sys_projects")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"projects": result.Rows,
		"columns":  result.Columns,
	})
}

func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "project name required")
		return
	}

	if req.Version == "" {
		req.Version = "1.0.0"
	}

	// Create project directory
	projectDir := filepath.Join(s.config.Server.DataDir, "projects", req.Name)
	if err := os.MkdirAll(projectDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create project directory")
		return
	}

	// Register project
	exec := executor.NewExecutor(s.engine)
	sql := fmt.Sprintf("INSERT INTO _sys_projects (name, version, installed_at) VALUES ('%s', '%s', datetime('now'))",
		req.Name, req.Version)
	_, err := exec.Execute(sql)
	if err != nil {
		// Clean up directory if insert fails
		os.RemoveAll(projectDir)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Project created successfully",
		"name":    req.Name,
	})
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request, name string) {
	// Get tables to drop
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(fmt.Sprintf("SELECT tables FROM _sys_projects WHERE name = '%s'", name))
	if err == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		if tables, ok := result.Rows[0][0].(string); ok && tables != "" {
			for _, tbl := range strings.Split(tables, ",") {
				tbl = strings.TrimSpace(tbl)
				if tbl != "" {
					exec.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
				}
			}
		}
	}

	// Delete project files
	projectDir := filepath.Join(s.config.Server.DataDir, "projects", name)
	os.RemoveAll(projectDir)

	// Unregister from database
	exec.Execute(fmt.Sprintf("DELETE FROM _sys_projects WHERE name = '%s'", name))

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Project deleted successfully",
	})
}

// handleAPIProjectImport handles POST /api/projects/import - upload and install project from ZIP
func (s *Server) handleAPIProjectImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse multipart form (max 50MB)
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse form: "+err.Error())
		return
	}

	file, header, err := r.FormFile("project")
	if err != nil {
		writeError(w, http.StatusBadRequest, "no file uploaded")
		return
	}
	defer file.Close()

	// Read ZIP content
	zipData, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	// Create temp directory for extraction
	tempDir, err := os.MkdirTemp("", "project-import-*")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create temp dir")
		return
	}
	defer os.RemoveAll(tempDir)

	// Write ZIP to temp file
	zipPath := filepath.Join(tempDir, header.Filename)
	if err := os.WriteFile(zipPath, zipData, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to write zip file")
		return
	}

	// Extract ZIP
	extractDir := filepath.Join(tempDir, "extracted")
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create extract dir")
		return
	}

	if err := extractZip(zipPath, extractDir); err != nil {
		writeError(w, http.StatusBadRequest, "failed to extract zip: "+err.Error())
		return
	}

	// Find project root (may be inside a subdirectory)
	projectRoot, err := findProjectRoot(extractDir)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Read project.json
	projectJsonPath := filepath.Join(projectRoot, "project.json")
	projectData, err := os.ReadFile(projectJsonPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, "project.json not found in ZIP")
		return
	}

	var projectInfo struct {
		Name        string                   `json:"name"`
		Version     string                   `json:"version"`
		Tables      string                   `json:"tables"`
		Microservices []struct {
			SKEY        string `json:"skey"`
			Script      string `json:"script"`
			Description string `json:"description"`
		} `json:"microservices"`
	}

	if err := json.Unmarshal(projectData, &projectInfo); err != nil {
		writeError(w, http.StatusBadRequest, "invalid project.json: "+err.Error())
		return
	}

	if projectInfo.Name == "" {
		writeError(w, http.StatusBadRequest, "project name is required in project.json")
		return
	}

	if projectInfo.Version == "" {
		projectInfo.Version = "1.0.0"
	}

	exec := executor.NewExecutor(s.engine)

	// Check if project already exists
	existingResult, _ := exec.Execute(fmt.Sprintf("SELECT name FROM _sys_projects WHERE name = '%s'", projectInfo.Name))
	if len(existingResult.Rows) > 0 {
		// Delete existing project
		exec.Execute(fmt.Sprintf("DELETE FROM _sys_projects WHERE name = '%s'", projectInfo.Name))
	}

	// Execute setup.sql if exists
	setupSqlPath := filepath.Join(projectRoot, "setup.sql")
	if _, err := os.Stat(setupSqlPath); err == nil {
		sqlContent, err := os.ReadFile(setupSqlPath)
		if err == nil {
			// Execute SQL statements
			sqlStatements := string(sqlContent)
			exec.Execute(sqlStatements)
		}
	}

	// Create project directory
	projectDir := filepath.Join(s.config.Server.DataDir, "projects", projectInfo.Name)
	os.RemoveAll(projectDir) // Remove existing if any
	if err := os.MkdirAll(projectDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create project directory")
		return
	}

	// Copy all files from project root to project directory
	filepath.Walk(projectRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || path == projectRoot {
			return nil
		}

		relPath, _ := filepath.Rel(projectRoot, path)
		targetPath := filepath.Join(projectDir, relPath)

		if info.IsDir() {
			os.MkdirAll(targetPath, info.Mode())
		} else {
			// Skip project.json and setup.sql from being copied to project dir
			if relPath == "project.json" || relPath == "setup.sql" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err == nil {
				os.WriteFile(targetPath, data, info.Mode())
			}
		}
		return nil
	})

	// Register microservices
	for _, ms := range projectInfo.Microservices {
		if ms.SKEY != "" && ms.Script != "" {
			// Delete existing if any
			exec.Execute(fmt.Sprintf("DELETE FROM _sys_ms WHERE SKEY = '%s'", ms.SKEY))
			// Insert new
			desc := strings.ReplaceAll(ms.Description, "'", "''")
			script := strings.ReplaceAll(ms.Script, "'", "''")
			exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('%s', '%s', '%s', datetime('now'))",
				ms.SKEY, script, desc))
		}
	}

	// Register project in database
	exec.Execute(fmt.Sprintf("INSERT INTO _sys_projects (name, version, installed_at, tables) VALUES ('%s', '%s', datetime('now'), '%s')",
		projectInfo.Name, projectInfo.Version, projectInfo.Tables))

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"message":  "Project imported successfully",
		"name":     projectInfo.Name,
		"version":  projectInfo.Version,
		"tables":   projectInfo.Tables,
		"services": len(projectInfo.Microservices),
	})
}

// extractZip extracts a ZIP file to a directory
func extractZip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		destPath := filepath.Join(destDir, f.Name)

		// Security: prevent path traversal
		if strings.Contains(f.Name, "..") {
			continue
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(destPath, f.Mode())
			continue
		}

		// Create parent directories
		os.MkdirAll(filepath.Dir(destPath), 0755)

		// Extract file
		srcFile, err := f.Open()
		if err != nil {
			return err
		}

		destFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			srcFile.Close()
			return err
		}

		io.Copy(destFile, srcFile)
		destFile.Close()
		srcFile.Close()
	}

	return nil
}

// findProjectRoot finds the directory containing project.json
func findProjectRoot(dir string) (string, error) {
	// Check if project.json exists in current dir
	if _, err := os.Stat(filepath.Join(dir, "project.json")); err == nil {
		return dir, nil
	}

	// Check subdirectories (single level)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("project.json not found")
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subPath := filepath.Join(dir, entry.Name())
			if _, err := os.Stat(filepath.Join(subPath, "project.json")); err == nil {
				return subPath, nil
			}
		}
	}

	return "", fmt.Errorf("project.json not found in ZIP")
}

// ============================================================================
// Project Files API
// ============================================================================

// handleAPIProjectFiles handles GET/POST /api/projects/{name}/files
func (s *Server) handleAPIProjectFiles(w http.ResponseWriter, r *http.Request) {
	// Extract project name
	path := strings.TrimPrefix(r.URL.Path, "/api/projects/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "project name required")
		return
	}
	projectName := parts[0]

	switch r.Method {
	case http.MethodGet:
		s.listProjectFiles(w, r, projectName)
	case http.MethodPost:
		s.createProjectFile(w, r, projectName)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIProjectFileDetail handles GET/PUT/DELETE /api/projects/{name}/files/{path}
func (s *Server) handleAPIProjectFileDetail(w http.ResponseWriter, r *http.Request) {
	// Extract project name and file path
	path := strings.TrimPrefix(r.URL.Path, "/api/projects/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "project name required")
		return
	}
	projectName := parts[0]

	// Find "/files/" in the remaining path
	remaining := ""
	if len(parts) > 1 {
		remaining = parts[1]
	}

	if !strings.HasPrefix(remaining, "files/") {
		writeError(w, http.StatusBadRequest, "invalid path format")
		return
	}
	filePath := strings.TrimPrefix(remaining, "files/")
	if filePath == "" {
		// Redirect to list
		s.listProjectFiles(w, r, projectName)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getProjectFile(w, r, projectName, filePath)
	case http.MethodPut:
		s.updateProjectFile(w, r, projectName, filePath)
	case http.MethodDelete:
		s.deleteProjectFile(w, r, projectName, filePath)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) listProjectFiles(w http.ResponseWriter, r *http.Request, projectName string) {
	projectDir := filepath.Join(s.config.Server.DataDir, "projects", projectName)

	// Check if project exists
	if _, err := os.Stat(projectDir); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}

	// List files recursively
	type FileInfo struct {
		Name  string `json:"name"`
		Path  string `json:"path"`
		IsDir bool   `json:"isDir"`
		Size  int64  `json:"size"`
	}

	var files []FileInfo
	filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		relPath, _ := filepath.Rel(projectDir, path)
		if relPath == "." {
			return nil
		}
		files = append(files, FileInfo{
			Name:  info.Name(),
			Path:  filepath.ToSlash(relPath),
			IsDir: info.IsDir(),
			Size:  info.Size(),
		})
		return nil
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"project": projectName,
		"files":   files,
	})
}

func (s *Server) createProjectFile(w http.ResponseWriter, r *http.Request, projectName string) {
	var req struct {
		Path    string `json:"path"`
		Content string `json:"content"`
		IsDir   bool   `json:"isDir"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.Path == "" {
		writeError(w, http.StatusBadRequest, "path required")
		return
	}

	// Security check
	if strings.Contains(req.Path, "..") {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}

	projectDir := filepath.Join(s.config.Server.DataDir, "projects", projectName)
	fullPath := filepath.Join(projectDir, req.Path)

	if req.IsDir {
		if err := os.MkdirAll(fullPath, 0755); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create directory")
			return
		}
	} else {
		// Ensure parent directory exists
		os.MkdirAll(filepath.Dir(fullPath), 0755)
		if err := os.WriteFile(fullPath, []byte(req.Content), 0644); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create file")
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "created successfully",
		"path":    req.Path,
	})
}

func (s *Server) getProjectFile(w http.ResponseWriter, r *http.Request, projectName, filePath string) {
	projectDir := filepath.Join(s.config.Server.DataDir, "projects", projectName)
	fullPath := filepath.Join(projectDir, filePath)

	// Security check
	if strings.Contains(filePath, "..") {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}

	content, err := os.ReadFile(fullPath)
	if err != nil {
		writeError(w, http.StatusNotFound, "file not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"project": projectName,
		"path":    filePath,
		"content": string(content),
	})
}

func (s *Server) updateProjectFile(w http.ResponseWriter, r *http.Request, projectName, filePath string) {
	var req struct {
		Content string `json:"content"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	// Security check
	if strings.Contains(filePath, "..") {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}

	projectDir := filepath.Join(s.config.Server.DataDir, "projects", projectName)
	fullPath := filepath.Join(projectDir, filePath)

	if err := os.WriteFile(fullPath, []byte(req.Content), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update file")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "file updated",
	})
}

func (s *Server) deleteProjectFile(w http.ResponseWriter, r *http.Request, projectName, filePath string) {
	// Security check
	if strings.Contains(filePath, "..") {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}

	projectDir := filepath.Join(s.config.Server.DataDir, "projects", projectName)
	fullPath := filepath.Join(projectDir, filePath)

	if err := os.RemoveAll(fullPath); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "deleted successfully",
	})
}

// ============================================================================
// Microservices API
// ============================================================================

// handleAPIMicroservices handles GET/POST /api/microservices
func (s *Server) handleAPIMicroservices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listMicroservices(w, r)
	case http.MethodPost:
		s.createMicroservice(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAPIMicroserviceDetail handles GET/PUT/DELETE /api/microservices/{skey}
func (s *Server) handleAPIMicroserviceDetail(w http.ResponseWriter, r *http.Request) {
	skey := strings.TrimPrefix(r.URL.Path, "/api/microservices/")
	if skey == "" {
		writeError(w, http.StatusBadRequest, "service key required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getMicroservice(w, r, skey)
	case http.MethodPut:
		s.updateMicroservice(w, r, skey)
	case http.MethodDelete:
		s.deleteMicroservice(w, r, skey)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) listMicroservices(w http.ResponseWriter, r *http.Request) {
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute("SELECT SKEY, SCRIPT, description, created_at FROM _sys_ms ORDER BY SKEY")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"microservices": result.Rows,
		"columns":       result.Columns,
	})
}

func (s *Server) getMicroservice(w http.ResponseWriter, r *http.Request, skey string) {
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(fmt.Sprintf("SELECT SKEY, SCRIPT, description, created_at FROM _sys_ms WHERE SKEY = '%s'", skey))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if len(result.Rows) == 0 {
		writeError(w, http.StatusNotFound, "microservice not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"microservice": result.Rows[0],
		"columns":      result.Columns,
	})
}

func (s *Server) createMicroservice(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SKEY        string `json:"skey"`
		SCRIPT      string `json:"script"`
		Description string `json:"description"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.SKEY == "" {
		writeError(w, http.StatusBadRequest, "service key required")
		return
	}

	if req.SCRIPT == "" {
		writeError(w, http.StatusBadRequest, "script required")
		return
	}

	exec := executor.NewExecutor(s.engine)
	desc := strings.ReplaceAll(req.Description, "'", "''")
	sql := fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('%s', '%s', '%s', datetime('now'))",
		req.SKEY, strings.ReplaceAll(req.SCRIPT, "'", "''"), desc)
	_, err := exec.Execute(sql)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "microservice created",
		"skey":    req.SKEY,
	})
}

func (s *Server) updateMicroservice(w http.ResponseWriter, r *http.Request, skey string) {
	var req struct {
		SCRIPT      string `json:"script"`
		Description string `json:"description"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	exec := executor.NewExecutor(s.engine)
	desc := strings.ReplaceAll(req.Description, "'", "''")
	sql := fmt.Sprintf("UPDATE _sys_ms SET SCRIPT = '%s', description = '%s' WHERE SKEY = '%s'",
		strings.ReplaceAll(req.SCRIPT, "'", "''"), desc, skey)
	_, err := exec.Execute(sql)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "microservice updated",
	})
}

func (s *Server) deleteMicroservice(w http.ResponseWriter, r *http.Request, skey string) {
	exec := executor.NewExecutor(s.engine)
	_, err := exec.Execute(fmt.Sprintf("DELETE FROM _sys_ms WHERE SKEY = '%s'", skey))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "microservice deleted",
	})
}

// ============================================================================
// Plugins API
// ============================================================================

// handleAPIPlugins handles GET /api/plugins - list installed plugins
func (s *Server) handleAPIPlugins(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute("SELECT name, version, latest_version, author, description, category, enabled, installed_at, tables, has_update, source FROM _sys_plugins ORDER BY name")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	plugins := make([]map[string]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		plugin := map[string]interface{}{
			"name":           row[0],
			"version":        row[1],
			"latest_version": row[2],
			"author":         row[3],
			"description":    row[4],
			"category":       row[5],
			"enabled":        row[6] == true || fmt.Sprintf("%v", row[6]) == "true",
			"installed_at":   row[7],
			"tables":         row[8],
			"has_update":     row[9] == true || fmt.Sprintf("%v", row[9]) == "true",
			"source":         row[10],
		}
		plugins = append(plugins, plugin)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"plugins": plugins,
	})
}

// handleAPIPluginsAvailable handles GET /api/plugins/available - list available plugins from registry
func (s *Server) handleAPIPluginsAvailable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Get installed plugins to check which are already installed
	exec := executor.NewExecutor(s.engine)
	result, _ := exec.Execute("SELECT name FROM _sys_plugins")
	installed := make(map[string]bool)
	for _, row := range result.Rows {
		installed[fmt.Sprintf("%v", row[0])] = true
	}

	// Get available plugins from registry
	available := []map[string]interface{}{}
	for _, pi := range getAvailablePlugins() {
		p := pi.(map[string]interface{})
		plugin := map[string]interface{}{
			"name":         p["name"],
			"version":      p["version"],
			"author":       p["author"],
			"description":  p["description"],
			"category":     p["category"],
			"tables":       p["tables"],
			"endpoints":    p["endpoints"],
			"download_url": p["download_url"],
			"installed":    installed[p["name"].(string)],
		}
		available = append(available, plugin)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"plugins": available,
	})
}

// handleAPIPluginRoutes handles /api/plugins/* routes
func (s *Server) handleAPIPluginRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/plugins/")

	// Check for import endpoint
	if path == "import" {
		s.handleAPIPluginImport(w, r)
		return
	}

	// Check for install endpoint
	if path == "install" {
		s.handleAPIPluginInstall(w, r)
		return
	}

	// Check for other actions
	if strings.Contains(path, "/") {
		parts := strings.SplitN(path, "/", 2)
		pluginName := parts[0]
		action := parts[1]

		switch action {
		case "uninstall":
			s.handleAPIPluginUninstall(w, r, pluginName)
		case "enable":
			s.handleAPIPluginEnable(w, r, pluginName)
		case "disable":
			s.handleAPIPluginDisable(w, r, pluginName)
		default:
			writeError(w, http.StatusNotFound, "unknown action")
		}
		return
	}

	// Get single plugin
	if r.Method == http.MethodGet {
		s.handleAPIPluginGet(w, r, path)
		return
	}

	writeError(w, http.StatusNotFound, "not found")
}

// handleAPIPluginImport handles POST /api/plugins/import - import plugin from ZIP
func (s *Server) handleAPIPluginImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse multipart form
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse form: "+err.Error())
		return
	}

	file, _, err := r.FormFile("plugin")
	if err != nil {
		writeError(w, http.StatusBadRequest, "no plugin file provided")
		return
	}
	defer file.Close()

	// Save to temp file
	tempFile, err := os.CreateTemp("", "plugin-*.zip")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create temp file")
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	if _, err := io.Copy(tempFile, file); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save file")
		return
	}
	tempFile.Close()

	// Install plugin
	if err := installPluginFromZIP(tempFile.Name()); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to install plugin: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "plugin installed successfully",
	})
}

// handleAPIPluginInstall handles POST /api/plugins/install - install from registry
func (s *Server) handleAPIPluginInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "plugin name required")
		return
	}

	// Find plugin in registry
	plugin := getPluginFromRegistry(req.Name)
	if plugin == nil {
		writeError(w, http.StatusNotFound, "plugin not found in registry")
		return
	}

	// Check if already installed
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(fmt.Sprintf("SELECT name FROM _sys_plugins WHERE name = '%s'", req.Name))
	if err == nil && result != nil && len(result.Rows) > 0 {
		writeError(w, http.StatusBadRequest, "plugin already installed")
		return
	}

	// Create plugin files locally (since we don't have GitHub download yet)
	if err := s.createPluginLocally(plugin); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create plugin: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "plugin installed successfully",
		"name":    req.Name,
	})
}

// handleAPIPluginUninstall handles POST /api/plugins/{name}/uninstall
func (s *Server) handleAPIPluginUninstall(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := s.uninstallPlugin(name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "plugin uninstalled",
	})
}

// handleAPIPluginEnable handles POST /api/plugins/{name}/enable
func (s *Server) handleAPIPluginEnable(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	exec := executor.NewExecutor(s.engine)
	_, err := exec.Execute(fmt.Sprintf("UPDATE _sys_plugins SET enabled = true WHERE name = '%s'", name))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "plugin enabled",
	})
}

// handleAPIPluginDisable handles POST /api/plugins/{name}/disable
func (s *Server) handleAPIPluginDisable(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	exec := executor.NewExecutor(s.engine)
	_, err := exec.Execute(fmt.Sprintf("UPDATE _sys_plugins SET enabled = false WHERE name = '%s'", name))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "plugin disabled",
	})
}

// handleAPIPluginGet handles GET /api/plugins/{name}
func (s *Server) handleAPIPluginGet(w http.ResponseWriter, r *http.Request, name string) {
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(fmt.Sprintf("SELECT name, version, latest_version, author, description, category, enabled, installed_at, tables, has_update, source FROM _sys_plugins WHERE name = '%s'", name))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if len(result.Rows) == 0 {
		writeError(w, http.StatusNotFound, "plugin not found")
		return
	}

	row := result.Rows[0]
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":           row[0],
		"version":        row[1],
		"latest_version": row[2],
		"author":         row[3],
		"description":    row[4],
		"category":       row[5],
		"enabled":        row[6] == true || fmt.Sprintf("%v", row[6]) == "true",
		"installed_at":   row[7],
		"tables":         row[8],
		"has_update":     row[9] == true || fmt.Sprintf("%v", row[9]) == "true",
		"source":         row[10],
	})
}

// Helper functions for plugin operations (these will use the plugin manager)

func getAvailablePlugins() []interface{} {
	// Return the built-in registry
	return []interface{}{
		map[string]interface{}{
			"name":        "auth",
			"version":     "1.0.0",
			"author":      "XxSql Team",
			"description": "User authentication with session management",
			"category":    "auth",
			"tables":      "_plugin_auth_users,_plugin_auth_sessions",
			"endpoints": []map[string]string{
				{"skey": "auth/login", "description": "User login"},
				{"skey": "auth/register", "description": "Register new user"},
				{"skey": "auth/logout", "description": "User logout"},
				{"skey": "auth/check", "description": "Check authentication"},
			},
		},
		map[string]interface{}{
			"name":        "logging",
			"version":     "1.0.0",
			"author":      "XxSql Team",
			"description": "Centralized logging service",
			"category":    "logging",
			"tables":      "_plugin_log_entries",
			"endpoints": []map[string]string{
				{"skey": "log/write", "description": "Write log entry"},
				{"skey": "log/query", "description": "Query logs"},
			},
		},
		map[string]interface{}{
			"name":        "ratelimit",
			"version":     "1.0.0",
			"author":      "XxSql Team",
			"description": "Request rate limiting",
			"category":    "utility",
			"tables":      "_plugin_ratelimit_rules,_plugin_ratelimit_counters",
			"endpoints": []map[string]string{
				{"skey": "ratelimit/check", "description": "Check rate limit"},
				{"skey": "ratelimit/rules", "description": "Manage rules"},
			},
		},
		map[string]interface{}{
			"name":        "storage",
			"version":     "1.0.0",
			"author":      "XxSql Team",
			"description": "File storage service",
			"category":    "storage",
			"tables":      "_plugin_storage_files",
			"endpoints": []map[string]string{
				{"skey": "storage/upload", "description": "Upload file"},
				{"skey": "storage/download", "description": "Download file"},
				{"skey": "storage/list", "description": "List files"},
			},
		},
	}
}

func getPluginFromRegistry(name string) map[string]interface{} {
	for _, p := range getAvailablePlugins() {
		plugin := p.(map[string]interface{})
		if plugin["name"] == name {
			return plugin
		}
	}
	return nil
}

func installPluginFromZIP(zipPath string) error {
	// This will be implemented using the plugin manager
	// For now, return a placeholder
	return fmt.Errorf("ZIP import not yet implemented - use /api/plugins/install instead")
}

func (s *Server) uninstallPlugin(name string) error {
	// Get plugin info first
	exec := executor.NewExecutor(s.engine)
	result, err := exec.Execute(fmt.Sprintf("SELECT tables FROM _sys_plugins WHERE name = '%s'", name))
	if err != nil {
		return err
	}

	if len(result.Rows) == 0 {
		return fmt.Errorf("plugin not found")
	}

	// Get tables
	tables := fmt.Sprintf("%v", result.Rows[0][0])
	if tables != "" && tables != "<nil>" {
		for _, table := range strings.Split(tables, ",") {
			table = strings.TrimSpace(table)
			if table != "" {
				exec.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
			}
		}
	}

	// Remove microservices for this plugin
	// Get endpoints from registry
	plugin := getPluginFromRegistry(name)
	if plugin != nil {
		if endpoints, ok := plugin["endpoints"].([]map[string]string); ok {
			for _, ep := range endpoints {
				exec.Execute(fmt.Sprintf("DELETE FROM _sys_ms WHERE SKEY = '%s'", ep["skey"]))
			}
		}
	}

	// Delete from _sys_plugins
	exec.Execute(fmt.Sprintf("DELETE FROM _sys_plugins WHERE name = '%s'", name))

	return nil
}

func (s *Server) createPluginLocally(plugin map[string]interface{}) error {
	name := plugin["name"].(string)
	version := plugin["version"].(string)
	author := plugin["author"].(string)
	description := plugin["description"].(string)
	category := plugin["category"].(string)
	tables := plugin["tables"].(string)

	exec := executor.NewExecutor(s.engine)

	// Create tables based on plugin type
	switch name {
	case "auth":
		// Create auth tables
		exec.Execute(`CREATE TABLE IF NOT EXISTS _plugin_auth_users (
			id SEQ PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			password VARCHAR(255) NOT NULL,
			email VARCHAR(100),
			role VARCHAR(20) DEFAULT 'user',
			created_at VARCHAR(30),
			last_login VARCHAR(30)
		)`)
		exec.Execute(`CREATE TABLE IF NOT EXISTS _plugin_auth_sessions (
			id VARCHAR(64) PRIMARY KEY,
			user_id INT NOT NULL,
			created_at VARCHAR(30),
			expires_at VARCHAR(30),
			ip_address VARCHAR(45)
		)`)

		// Register auth microservices
		authLoginScript := `
var data = http.bodyJSON()
var username = data.username
var pwd = data.password
if username == "" || pwd == "" {
    http.status(400)
    http.json({"success": false, "error": "Username and password required"})
    return
}
var user = db.queryRow("SELECT id, username, password AS pwd, role FROM _plugin_auth_users WHERE username = '" + username + "'")
if user == null {
    http.status(401)
    http.json({"success": false, "error": "Invalid credentials"})
    return
}
var hash = user.pwd
if md5(pwd) != hash {
    http.status(401)
    http.json({"success": false, "error": "Invalid credentials"})
    return
}
var sessionId = md5(username + string(now()) + string(rand()))
var expireTime = now() + 86400
var expireStr = formatTime(expireTime, "2006-01-02 15:04:05")
var nowStr = formatTime(now(), "2006-01-02 15:04:05")
db.exec("INSERT INTO _plugin_auth_sessions (id, user_id, created_at, expires_at, ip_address) VALUES ('" + sessionId + "', " + string(user.id) + ", '" + nowStr + "', '" + expireStr + "', '" + http.remoteAddr + "')")
db.exec("UPDATE _plugin_auth_users SET last_login = '" + nowStr + "' WHERE id = " + string(user.id))
http.setCookie("session_id", sessionId, 86400)
http.json({"success": true, "user": {"id": user.id, "username": user.username, "role": user.role}})
`
		exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description) VALUES ('auth/login', '%s', 'User login')", strings.ReplaceAll(authLoginScript, "'", "''")))

		authRegisterScript := `
var data = http.bodyJSON()
var username = data.username
var pwd = data.password
var email = data.email
if username == "" || pwd == "" {
    http.status(400)
    http.json({"success": false, "error": "Username and password required"})
    return
}
var existing = db.queryRow("SELECT id FROM _plugin_auth_users WHERE username = '" + username + "'")
if existing != null {
    http.status(400)
    http.json({"success": false, "error": "Username already exists"})
    return
}
var hash = md5(pwd)
var nowStr = formatTime(now(), "2006-01-02 15:04:05")
var emailStr = string(email)
if emailStr == "" || emailStr == "<nil>" {
    emailStr = ""
}
var result = db.exec("INSERT INTO _plugin_auth_users (username, password, email, role, created_at) VALUES ('" + username + "', '" + hash + "', '" + emailStr + "', 'user', '" + nowStr + "')")
http.json({"success": true, "user_id": result.insert_id})
`
		exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description) VALUES ('auth/register', '%s', 'Register new user')", strings.ReplaceAll(authRegisterScript, "'", "''")))

		authLogoutScript := `
var sessionId = http.cookie("session_id")
if sessionId != "" {
    db.exec("DELETE FROM _plugin_auth_sessions WHERE id = '" + sessionId + "'")
}
http.setCookie("session_id", "", -1)
http.json({"success": true, "message": "Logged out"})
`
		exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description) VALUES ('auth/logout', '%s', 'User logout')", strings.ReplaceAll(authLogoutScript, "'", "''")))

		authCheckScript := `
var sessionId = http.cookie("session_id")
if sessionId == "" {
    http.json({"authenticated": false})
    return
}
var session = db.queryRow("SELECT id, user_id, expires_at FROM _plugin_auth_sessions WHERE id = '" + sessionId + "'")
if session == null {
    http.json({"authenticated": false})
    return
}
var expireStr = session.expires_at
if expireStr == "" || expireStr == null {
    http.json({"authenticated": false})
    return
}
var expireTime = parseTime(expireStr, "2006-01-02 15:04:05")
if now() > expireTime {
    http.json({"authenticated": false})
    return
}
var user = db.queryRow("SELECT id, username, role FROM _plugin_auth_users WHERE id = " + string(session.user_id))
if user == null {
    http.json({"authenticated": false})
    return
}
http.json({"authenticated": true, "user": {"id": user.id, "username": user.username, "role": user.role}})
`
		exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description) VALUES ('auth/check', '%s', 'Check authentication status')", strings.ReplaceAll(authCheckScript, "'", "''")))

	case "logging":
		exec.Execute(`CREATE TABLE IF NOT EXISTS _plugin_log_entries (
			id SEQ PRIMARY KEY,
			level VARCHAR(10),
			message TEXT,
			source VARCHAR(100),
			data TEXT,
			created_at DATETIME
		)`)

		logWriteScript := `
var data = http.bodyJSON()
if data == null || data.message == "" {
    http.status(400)
    http.json({"success": false, "error": "Message required"})
    return
}
var level = data.level
if level == "" { level = "INFO" }
var result = db.exec("INSERT INTO _plugin_log_entries (level, message, source, data, created_at) VALUES ('" + level + "', '" + strings.ReplaceAll(data.message, "'", "''") + "', '" + data.source + "', '" + json(data.data) + "', NOW())")
http.json({"success": true, "id": result.insert_id})
`
		exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description) VALUES ('log/write', '%s', 'Write log entry')", strings.ReplaceAll(logWriteScript, "'", "''")))

		logQueryScript := `
var level = http.param("level")
var source = http.param("source")
var limit = http.param("limit")
if limit == "" { limit = "100" }
var sql = "SELECT * FROM _plugin_log_entries WHERE 1=1"
if level != "" { sql = sql + " AND level = '" + level + "'" }
if source != "" { sql = sql + " AND source = '" + source + "'" }
sql = sql + " ORDER BY created_at DESC LIMIT " + limit
var logs = db.query(sql)
http.json({"logs": logs})
`
		exec.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description) VALUES ('log/query', '%s', 'Query logs')", strings.ReplaceAll(logQueryScript, "'", "''")))

	case "ratelimit":
		exec.Execute(`CREATE TABLE IF NOT EXISTS _plugin_ratelimit_rules (
			id SEQ PRIMARY KEY,
			name VARCHAR(50) UNIQUE,
			key_type VARCHAR(20),
			limit INT,
			window INT,
			enabled BOOL DEFAULT true
		)`)
		exec.Execute(`CREATE TABLE IF NOT EXISTS _plugin_ratelimit_counters (
			id SEQ PRIMARY KEY,
			rule_id INT,
			key_value VARCHAR(100),
			count INT,
			window_start DATETIME
		)`)

	case "storage":
		exec.Execute(`CREATE TABLE IF NOT EXISTS _plugin_storage_files (
			id SEQ PRIMARY KEY,
			name VARCHAR(255),
			path VARCHAR(500) UNIQUE,
			size INT,
			mime_type VARCHAR(100),
			created_at DATETIME
		)`)
	}

	// Insert into _sys_plugins
	exec.Execute(fmt.Sprintf("INSERT INTO _sys_plugins (name, version, author, description, category, enabled, installed_at, tables, source) VALUES ('%s', '%s', '%s', '%s', '%s', true, NOW(), '%s', 'registry')",
		name, version, author, description, category, tables))

	return nil
}

// Singleton engine reference for helper functions
var singletonEngine *storage.Engine

func SetSingletonEngine(engine *storage.Engine) {
	singletonEngine = engine
}
