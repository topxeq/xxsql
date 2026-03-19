// Package config provides configuration management for XxSql.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Loader handles configuration loading with priority.
type Loader struct {
	configPath string
	envPrefix  string
}

// NewLoader creates a new configuration loader.
func NewLoader(configPath string) *Loader {
	return &Loader{
		configPath: configPath,
		envPrefix:  "XXSQL_",
	}
}

// Load loads configuration with the following priority:
// CLI flags > Config file > Environment variables > Defaults
func (l *Loader) Load() (*Config, error) {
	// Start with defaults
	cfg := DefaultConfig()

	// Apply environment variables
	if err := l.applyEnvironment(cfg); err != nil {
		return nil, fmt.Errorf("failed to apply environment: %w", err)
	}

	// Load from config file if specified
	if l.configPath != "" {
		if err := l.loadFromFile(cfg, l.configPath); err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// LoadFromFile loads configuration from a specific file.
func (l *Loader) LoadFromFile(path string) (*Config, error) {
	l.configPath = path
	return l.Load()
}

// loadFromFile loads configuration from a JSON file into the provided config.
func (l *Loader) loadFromFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	// Expand environment variables in the config file content
	content := os.ExpandEnv(string(data))

	if err := json.Unmarshal([]byte(content), cfg); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return nil
}

// applyEnvironment applies environment variables to the configuration.
func (l *Loader) applyEnvironment(cfg *Config) error {
	// Server config
	if v := os.Getenv(l.envKey("SERVER_NAME")); v != "" {
		cfg.Server.Name = v
	}
	if v := os.Getenv(l.envKey("DATA_DIR")); v != "" {
		cfg.Server.DataDir = v
	}
	if v := os.Getenv(l.envKey("PID_FILE")); v != "" {
		cfg.Server.PIDFile = v
	}

	// Network config
	if v := os.Getenv(l.envKey("PRIVATE_PORT")); v != "" {
		if port, err := parseInt(v); err == nil {
			cfg.Network.PrivatePort = port
		}
	}
	if v := os.Getenv(l.envKey("MYSQL_PORT")); v != "" {
		if port, err := parseInt(v); err == nil {
			cfg.Network.MySQLPort = port
		}
	}
	if v := os.Getenv(l.envKey("HTTP_PORT")); v != "" {
		if port, err := parseInt(v); err == nil {
			cfg.Network.HTTPPort = port
		}
	}
	if v := os.Getenv(l.envKey("BIND")); v != "" {
		cfg.Network.Bind = v
	}
	// Service enabled flags
	if v := os.Getenv(l.envKey("PRIVATE_ENABLED")); v != "" {
		enabled := parseBool(v)
		cfg.Network.PrivateEnabled = &enabled
	}
	if v := os.Getenv(l.envKey("MYSQL_ENABLED")); v != "" {
		enabled := parseBool(v)
		cfg.Network.MySQLEnabled = &enabled
	}
	if v := os.Getenv(l.envKey("HTTP_ENABLED")); v != "" {
		enabled := parseBool(v)
		cfg.Network.HTTPEnabled = &enabled
	}

	// Log config
	if v := os.Getenv(l.envKey("LOG_LEVEL")); v != "" {
		cfg.Log.Level = strings.ToUpper(v)
	}
	if v := os.Getenv(l.envKey("LOG_FILE")); v != "" {
		cfg.Log.File = v
	}

	// Auth config
	if v := os.Getenv(l.envKey("AUTH_ENABLED")); v != "" {
		cfg.Auth.Enabled = parseBool(v)
	}
	if v := os.Getenv(l.envKey("ADMIN_USER")); v != "" {
		cfg.Auth.AdminUser = v
	}
	if v := os.Getenv(l.envKey("ADMIN_PASSWORD")); v != "" {
		cfg.Auth.AdminPassword = v
	}

	// Storage config
	if v := os.Getenv(l.envKey("PAGE_SIZE")); v != "" {
		if size, err := parseInt(v); err == nil {
			cfg.Storage.PageSize = size
		}
	}
	if v := os.Getenv(l.envKey("BUFFER_POOL_SIZE")); v != "" {
		if size, err := parseInt(v); err == nil {
			cfg.Storage.BufferPoolSize = size
		}
	}

	// Connection config
	if v := os.Getenv(l.envKey("MAX_CONNECTIONS")); v != "" {
		if max, err := parseInt(v); err == nil {
			cfg.Connection.MaxConnections = max
		}
	}

	return nil
}

// envKey generates an environment variable key with the prefix.
func (l *Loader) envKey(key string) string {
	return l.envPrefix + key
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	// Validate network ports (only if service is enabled)
	if c.Network.IsPrivateEnabled() {
		if c.Network.PrivatePort < 1 || c.Network.PrivatePort > 65535 {
			return fmt.Errorf("invalid private_port: %d", c.Network.PrivatePort)
		}
	}
	if c.Network.IsMySQLEnabled() {
		if c.Network.MySQLPort < 1 || c.Network.MySQLPort > 65535 {
			return fmt.Errorf("invalid mysql_port: %d", c.Network.MySQLPort)
		}
	}
	if c.Network.IsHTTPEnabled() {
		if c.Network.HTTPPort < 1 || c.Network.HTTPPort > 65535 {
			return fmt.Errorf("invalid http_port: %d", c.Network.HTTPPort)
		}
	}

	// Validate log level
	validLevels := map[string]bool{
		"DEBUG": true,
		"INFO":  true,
		"WARN":  true,
		"ERROR": true,
	}
	if !validLevels[strings.ToUpper(c.Log.Level)] {
		return fmt.Errorf("invalid log level: %s", c.Log.Level)
	}

	// Validate storage config
	if c.Storage.PageSize < 512 || c.Storage.PageSize > 65536 {
		return fmt.Errorf("invalid page_size: %d (must be between 512 and 65536)", c.Storage.PageSize)
	}
	if c.Storage.BufferPoolSize < 10 {
		return fmt.Errorf("buffer_pool_size must be at least 10")
	}

	// Validate connection config
	if c.Connection.MaxConnections < 1 {
		return fmt.Errorf("max_connections must be at least 1")
	}

	// Validate worker pool config
	if c.WorkerPool.WorkerCount < 1 {
		return fmt.Errorf("worker_count must be at least 1")
	}

	return nil
}

// GenerateExampleConfig generates an example configuration file.
func GenerateExampleConfig() ([]byte, error) {
	cfg := DefaultConfig()
	return json.MarshalIndent(cfg, "", "  ")
}

// SaveConfig saves the configuration to a file.
func SaveConfig(cfg *Config, path string) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Helper functions

func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &result)
	return result, err
}

func parseBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes" || s == "on"
}