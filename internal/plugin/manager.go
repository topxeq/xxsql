// Package plugin provides plugin management for XxSql.
package plugin

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

// Plugin represents an installed or available plugin.
type Plugin struct {
	Name          string     `json:"name"`
	Version       string     `json:"version"`
	LatestVersion string     `json:"latest_version"`
	Author        string     `json:"author"`
	Description   string     `json:"description"`
	Category      string     `json:"category"`
	Enabled       bool       `json:"enabled"`
	InstalledAt   string     `json:"installed_at"`
	Tables        string     `json:"tables"`
	HasUpdate     bool       `json:"has_update"`
	Source        string     `json:"source"`
	Endpoints     []Endpoint `json:"endpoints"`
}

// Endpoint represents a plugin endpoint.
type Endpoint struct {
	SKEY        string `json:"skey"`
	Script      string `json:"script"`
	Description string `json:"description"`
}

// Manager manages plugins in XxSql.
type Manager struct {
	engine   *storage.Engine
	executor *executor.Executor
	dataDir  string
}

// NewManager creates a new plugin manager.
func NewManager(engine *storage.Engine, exec *executor.Executor, dataDir string) *Manager {
	return &Manager{
		engine:   engine,
		executor: exec,
		dataDir:  dataDir,
	}
}

// ListInstalled returns all installed plugins.
func (m *Manager) ListInstalled() ([]*Plugin, error) {
	result, err := m.executor.Execute("SELECT name, version, latest_version, author, description, category, enabled, installed_at, tables, has_update, source FROM _sys_plugins ORDER BY name")
	if err != nil {
		return nil, err
	}

	plugins := make([]*Plugin, 0, len(result.Rows))
	for _, row := range result.Rows {
		plugin := &Plugin{
			Name:          fmt.Sprintf("%v", row[0]),
			Version:       fmt.Sprintf("%v", row[1]),
			LatestVersion: fmt.Sprintf("%v", row[2]),
			Author:        fmt.Sprintf("%v", row[3]),
			Description:   fmt.Sprintf("%v", row[4]),
			Category:      fmt.Sprintf("%v", row[5]),
			Enabled:       row[6] == true || fmt.Sprintf("%v", row[6]) == "true",
			InstalledAt:   fmt.Sprintf("%v", row[7]),
			Tables:        fmt.Sprintf("%v", row[8]),
			HasUpdate:     row[9] == true || fmt.Sprintf("%v", row[9]) == "true",
			Source:        fmt.Sprintf("%v", row[10]),
		}
		if plugin.Version == "<nil>" {
			plugin.Version = ""
		}
		if plugin.Author == "<nil>" {
			plugin.Author = ""
		}
		if plugin.Description == "<nil>" {
			plugin.Description = ""
		}
		plugins = append(plugins, plugin)
	}

	return plugins, nil
}

// GetPlugin returns a specific plugin by name.
func (m *Manager) GetPlugin(name string) (*Plugin, error) {
	result, err := m.executor.Execute(fmt.Sprintf("SELECT name, version, latest_version, author, description, category, enabled, installed_at, tables, has_update, source FROM _sys_plugins WHERE name = '%s'", name))
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("plugin not found: %s", name)
	}

	row := result.Rows[0]
	return &Plugin{
		Name:          fmt.Sprintf("%v", row[0]),
		Version:       fmt.Sprintf("%v", row[1]),
		LatestVersion: fmt.Sprintf("%v", row[2]),
		Author:        fmt.Sprintf("%v", row[3]),
		Description:   fmt.Sprintf("%v", row[4]),
		Category:      fmt.Sprintf("%v", row[5]),
		Enabled:       row[6] == true || fmt.Sprintf("%v", row[6]) == "true",
		InstalledAt:   fmt.Sprintf("%v", row[7]),
		Tables:        fmt.Sprintf("%v", row[8]),
		HasUpdate:     row[9] == true || fmt.Sprintf("%v", row[9]) == "true",
		Source:        fmt.Sprintf("%v", row[10]),
	}, nil
}

// InstallFromZIP installs a plugin from a ZIP file.
func (m *Manager) InstallFromZIP(zipPath string) error {
	// Open ZIP file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip: %w", err)
	}
	defer reader.Close()

	// Create temp directory for extraction
	tempDir := filepath.Join(m.dataDir, "temp", fmt.Sprintf("plugin_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Extract ZIP
	for _, file := range reader.File {
		destPath := filepath.Join(tempDir, file.Name)

		if file.FileInfo().IsDir() {
			os.MkdirAll(destPath, 0755)
			continue
		}

		os.MkdirAll(filepath.Dir(destPath), 0755)

		destFile, err := os.Create(destPath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", destPath, err)
		}

		srcFile, err := file.Open()
		if err != nil {
			destFile.Close()
			return fmt.Errorf("failed to open zip entry %s: %w", file.Name, err)
		}

		io.Copy(destFile, srcFile)
		srcFile.Close()
		destFile.Close()
	}

	// Read plugin.json
	pluginJSONPath := filepath.Join(tempDir, "plugin.json")
	pluginData, err := os.ReadFile(pluginJSONPath)
	if err != nil {
		return fmt.Errorf("failed to read plugin.json: %w", err)
	}

	var pluginInfo Plugin
	if err := json.Unmarshal(pluginData, &pluginInfo); err != nil {
		return fmt.Errorf("invalid plugin.json: %w", err)
	}

	if pluginInfo.Name == "" {
		return fmt.Errorf("plugin name is required in plugin.json")
	}

	// Check if already installed
	existing, _ := m.GetPlugin(pluginInfo.Name)
	if existing != nil {
		// Uninstall existing first
		m.Uninstall(pluginInfo.Name)
	}

	// Execute setup.sql if exists
	setupSQLPath := filepath.Join(tempDir, "setup.sql")
	if _, err := os.Stat(setupSQLPath); err == nil {
		setupSQL, err := os.ReadFile(setupSQLPath)
		if err != nil {
			return fmt.Errorf("failed to read setup.sql: %w", err)
		}

		// Execute SQL statements
		statements := splitSQLStatements(string(setupSQL))
		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if _, err := m.executor.Execute(stmt); err != nil {
				return fmt.Errorf("failed to execute setup.sql: %w", err)
			}
		}
	}

	// Copy plugin files to plugins directory
	pluginDir := filepath.Join(m.dataDir, "plugins", pluginInfo.Name)
	if err := os.MkdirAll(pluginDir, 0755); err != nil {
		return fmt.Errorf("failed to create plugin dir: %w", err)
	}

	// Copy all files from temp to plugin directory
	filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		relPath, _ := filepath.Rel(tempDir, path)
		destPath := filepath.Join(pluginDir, relPath)

		os.MkdirAll(filepath.Dir(destPath), 0755)

		srcFile, _ := os.Open(path)
		defer srcFile.Close()

		destFile, _ := os.Create(destPath)
		defer destFile.Close()

		io.Copy(destFile, srcFile)
		return nil
	})

	// Register microservices
	for _, ep := range pluginInfo.Endpoints {
		// Read script file if script path is provided
		if ep.Script != "" && !strings.Contains(ep.Script, "\n") {
			scriptPath := filepath.Join(pluginDir, ep.Script)
			if _, err := os.Stat(scriptPath); err == nil {
				scriptData, err := os.ReadFile(scriptPath)
				if err == nil {
					ep.Script = string(scriptData)
				}
			}
		}

		// Insert into _sys_ms
		m.executor.Execute(fmt.Sprintf("DELETE FROM _sys_ms WHERE SKEY = '%s'", ep.SKEY))
		m.executor.Execute(fmt.Sprintf("INSERT INTO _sys_ms (SKEY, SCRIPT, description, created_at) VALUES ('%s', '%s', '%s', NOW())",
			ep.SKEY, escapeSQL(ep.Script), escapeSQL(ep.Description)))
	}

	// Insert into _sys_plugins
	m.executor.Execute(fmt.Sprintf("INSERT INTO _sys_plugins (name, version, author, description, category, enabled, installed_at, tables, source) VALUES ('%s', '%s', '%s', '%s', '%s', true, NOW(), '%s', 'local')",
		escapeSQL(pluginInfo.Name),
		escapeSQL(pluginInfo.Version),
		escapeSQL(pluginInfo.Author),
		escapeSQL(pluginInfo.Description),
		escapeSQL(pluginInfo.Category),
		escapeSQL(pluginInfo.Tables),
	))

	return nil
}

// Uninstall removes a plugin.
func (m *Manager) Uninstall(name string) error {
	// Get plugin info
	plugin, err := m.GetPlugin(name)
	if err != nil {
		return err
	}

	// Drop tables created by plugin
	if plugin.Tables != "" {
		tables := strings.Split(plugin.Tables, ",")
		for _, table := range tables {
			table = strings.TrimSpace(table)
			if table != "" {
				m.executor.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
			}
		}
	}

	// Remove microservices for this plugin
	// Find endpoints from plugin.json
	pluginDir := filepath.Join(m.dataDir, "plugins", name)
	pluginJSONPath := filepath.Join(pluginDir, "plugin.json")
	if pluginData, err := os.ReadFile(pluginJSONPath); err == nil {
		var pluginInfo Plugin
		if json.Unmarshal(pluginData, &pluginInfo) == nil {
			for _, ep := range pluginInfo.Endpoints {
				m.executor.Execute(fmt.Sprintf("DELETE FROM _sys_ms WHERE SKEY = '%s'", ep.SKEY))
			}
		}
	}

	// Delete from _sys_plugins
	m.executor.Execute(fmt.Sprintf("DELETE FROM _sys_plugins WHERE name = '%s'", name))

	// Remove plugin directory
	os.RemoveAll(pluginDir)

	return nil
}

// Enable enables a plugin.
func (m *Manager) Enable(name string) error {
	_, err := m.executor.Execute(fmt.Sprintf("UPDATE _sys_plugins SET enabled = true WHERE name = '%s'", name))
	return err
}

// Disable disables a plugin.
func (m *Manager) Disable(name string) error {
	_, err := m.executor.Execute(fmt.Sprintf("UPDATE _sys_plugins SET enabled = false WHERE name = '%s'", name))
	return err
}

// IsInstalled checks if a plugin is installed.
func (m *Manager) IsInstalled(name string) bool {
	_, err := m.GetPlugin(name)
	return err == nil
}

// splitSQLStatements splits SQL file into individual statements.
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := rune(0)

	for _, char := range sql {
		if !inString && (char == '\'' || char == '"') {
			inString = true
			stringChar = char
			current.WriteRune(char)
		} else if inString && char == stringChar {
			inString = false
			current.WriteRune(char)
		} else if !inString && char == ';' {
			statements = append(statements, current.String())
			current.Reset()
		} else {
			current.WriteRune(char)
		}
	}

	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}

// escapeSQL escapes single quotes in SQL strings.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}