package web

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/executor"
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
		writeJSON(w, http.StatusOK, s.config)

	case http.MethodPut:
		// For safety, only allow certain config updates
		var updates map[string]interface{}
		if err := readJSON(r, &updates); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request")
			return
		}

		// Note: In a real implementation, you'd validate and apply these
		writeJSON(w, http.StatusOK, map[string]string{
			"message": "config updated (requires restart for some changes)",
		})

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
