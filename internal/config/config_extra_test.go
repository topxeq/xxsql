package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestLoader_LoadFromFile_Public tests the LoadFromFile public method
func TestLoader_LoadFromFile_Public(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-loadfromfile-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config file
	cfg := DefaultConfig()
	cfg.Server.Name = "loadfromfile-test"
	cfg.Network.PrivatePort = 7777

	configPath := filepath.Join(tmpDir, "config.json")
	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, data, 0644)

	// Create loader with empty path
	loader := NewLoader("")

	// Use LoadFromFile to set path and load
	loaded, err := loader.LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	if loaded.Server.Name != "loadfromfile-test" {
		t.Errorf("Server.Name = %s, want loadfromfile-test", loaded.Server.Name)
	}
	if loaded.Network.PrivatePort != 7777 {
		t.Errorf("PrivatePort = %d, want 7777", loaded.Network.PrivatePort)
	}

	// Verify loader's configPath was updated
	if loader.configPath != configPath {
		t.Errorf("loader.configPath = %s, want %s", loader.configPath, configPath)
	}
}

// TestLoader_LoadFromFile_NonExistent tests LoadFromFile with non-existent file
func TestLoader_LoadFromFile_NonExistent(t *testing.T) {
	loader := NewLoader("")

	_, err := loader.LoadFromFile("/nonexistent/config.json")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// TestApplyEnvironment_AllEnvVars tests all environment variables
func TestApplyEnvironment_AllEnvVars(t *testing.T) {
	envVars := map[string]string{
		"XXSQL_SERVER_NAME":      "env-test-server",
		"XXSQL_DATA_DIR":         "/env/data/dir",
		"XXSQL_PRIVATE_PORT":     "9001",
		"XXSQL_MYSQL_PORT":       "3307",
		"XXSQL_HTTP_PORT":        "8081",
		"XXSQL_BIND":             "127.0.0.1",
		"XXSQL_LOG_LEVEL":        "debug",
		"XXSQL_LOG_FILE":         "/var/log/xxsql.log",
		"XXSQL_AUTH_ENABLED":     "true",
		"XXSQL_ADMIN_USER":       "admin_user",
		"XXSQL_ADMIN_PASSWORD":   "admin_pass",
		"XXSQL_PAGE_SIZE":        "8192",
		"XXSQL_BUFFER_POOL_SIZE": "500",
		"XXSQL_MAX_CONNECTIONS":  "500",
	}

	// Set all env vars
	for k, v := range envVars {
		os.Setenv(k, v)
	}
	defer func() {
		for k := range envVars {
			os.Unsetenv(k)
		}
	}()

	loader := NewLoader("")
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify all env vars were applied
	if cfg.Server.Name != "env-test-server" {
		t.Errorf("Server.Name = %s, want env-test-server", cfg.Server.Name)
	}
	if cfg.Server.DataDir != "/env/data/dir" {
		t.Errorf("Server.DataDir = %s, want /env/data/dir", cfg.Server.DataDir)
	}
	if cfg.Network.PrivatePort != 9001 {
		t.Errorf("Network.PrivatePort = %d, want 9001", cfg.Network.PrivatePort)
	}
	if cfg.Network.MySQLPort != 3307 {
		t.Errorf("Network.MySQLPort = %d, want 3307", cfg.Network.MySQLPort)
	}
	if cfg.Network.HTTPPort != 8081 {
		t.Errorf("Network.HTTPPort = %d, want 8081", cfg.Network.HTTPPort)
	}
	if cfg.Network.Bind != "127.0.0.1" {
		t.Errorf("Network.Bind = %s, want 127.0.0.1", cfg.Network.Bind)
	}
	if cfg.Log.Level != "DEBUG" { // Should be uppercase
		t.Errorf("Log.Level = %s, want DEBUG", cfg.Log.Level)
	}
	if cfg.Log.File != "/var/log/xxsql.log" {
		t.Errorf("Log.File = %s, want /var/log/xxsql.log", cfg.Log.File)
	}
	if !cfg.Auth.Enabled {
		t.Error("Auth.Enabled should be true")
	}
	if cfg.Auth.AdminUser != "admin_user" {
		t.Errorf("Auth.AdminUser = %s, want admin_user", cfg.Auth.AdminUser)
	}
	if cfg.Auth.AdminPassword != "admin_pass" {
		t.Errorf("Auth.AdminPassword = %s, want admin_pass", cfg.Auth.AdminPassword)
	}
	if cfg.Storage.PageSize != 8192 {
		t.Errorf("Storage.PageSize = %d, want 8192", cfg.Storage.PageSize)
	}
	if cfg.Storage.BufferPoolSize != 500 {
		t.Errorf("Storage.BufferPoolSize = %d, want 500", cfg.Storage.BufferPoolSize)
	}
	if cfg.Connection.MaxConnections != 500 {
		t.Errorf("Connection.MaxConnections = %d, want 500", cfg.Connection.MaxConnections)
	}
}

// TestApplyEnvironment_InvalidIntValues tests env vars with invalid int values
func TestApplyEnvironment_InvalidIntValues(t *testing.T) {
	// Set invalid int values - should be silently ignored
	os.Setenv("XXSQL_PRIVATE_PORT", "not-a-number")
	os.Setenv("XXSQL_MYSQL_PORT", "invalid")
	os.Setenv("XXSQL_HTTP_PORT", "abc")
	os.Setenv("XXSQL_PAGE_SIZE", "xxx")
	os.Setenv("XXSQL_BUFFER_POOL_SIZE", "yyy")
	os.Setenv("XXSQL_MAX_CONNECTIONS", "zzz")
	defer func() {
		os.Unsetenv("XXSQL_PRIVATE_PORT")
		os.Unsetenv("XXSQL_MYSQL_PORT")
		os.Unsetenv("XXSQL_HTTP_PORT")
		os.Unsetenv("XXSQL_PAGE_SIZE")
		os.Unsetenv("XXSQL_BUFFER_POOL_SIZE")
		os.Unsetenv("XXSQL_MAX_CONNECTIONS")
	}()

	loader := NewLoader("")
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Invalid values should be ignored, defaults remain
	if cfg.Network.PrivatePort == 0 {
		t.Error("PrivatePort should use default value when env is invalid")
	}
}

// TestApplyEnvironment_AuthEnabled tests various auth enabled values
func TestApplyEnvironment_AuthEnabled(t *testing.T) {
	tests := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"1", true},
		{"yes", true},
		{"on", true},
		{"TRUE", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"anything", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			os.Setenv("XXSQL_AUTH_ENABLED", tt.value)
			defer os.Unsetenv("XXSQL_AUTH_ENABLED")

			loader := NewLoader("")
			cfg, err := loader.Load()
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}

			if cfg.Auth.Enabled != tt.expected {
				t.Errorf("Auth.Enabled = %v, want %v", cfg.Auth.Enabled, tt.expected)
			}
		})
	}
}

// TestSaveConfig_InvalidPath tests saving to an invalid path
func TestSaveConfig_InvalidPath(t *testing.T) {
	cfg := DefaultConfig()

	// Try to save to a path where a parent is a file
	tmpFile, err := os.CreateTemp("", "config-blocker-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Try to create config inside this file (should fail)
	err = SaveConfig(cfg, tmpFile.Name()+"/subdir/config.json")
	if err == nil {
		t.Error("expected error when saving to invalid path")
	}
}

// TestSaveConfig_ExistingFile tests overwriting an existing file
func TestSaveConfig_ExistingFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-overwrite-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.json")

	// Create initial file
	cfg1 := DefaultConfig()
	cfg1.Server.Name = "first-version"
	SaveConfig(cfg1, configPath)

	// Overwrite
	cfg2 := DefaultConfig()
	cfg2.Server.Name = "second-version"
	err = SaveConfig(cfg2, configPath)
	if err != nil {
		t.Fatalf("SaveConfig overwrite failed: %v", err)
	}

	// Verify overwrite
	data, _ := os.ReadFile(configPath)
	var loaded Config
	json.Unmarshal(data, &loaded)
	if loaded.Server.Name != "second-version" {
		t.Errorf("Server.Name = %s, want second-version", loaded.Server.Name)
	}
}

// TestLoad_InvalidConfig tests loading with invalid config that fails validation
func TestLoad_InvalidConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-invalid-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config with invalid port
	configContent := `{
		"server": {"name": "test", "data_dir": "./data"},
		"network": {"private_port": 0, "mysql_port": 3306, "http_port": 8080, "bind": "0.0.0.0"},
		"storage": {"page_size": 4096, "buffer_pool_size": 1000},
		"log": {"level": "INFO"},
		"connection": {"max_connections": 100},
		"worker_pool": {"worker_count": 10}
	}`

	configPath := filepath.Join(tmpDir, "config.json")
	os.WriteFile(configPath, []byte(configContent), 0644)

	loader := NewLoader(configPath)
	_, err = loader.Load()
	if err == nil {
		t.Error("expected error for invalid config (port 0)")
	}
}

// TestEnvKey tests the envKey method
func TestEnvKey(t *testing.T) {
	loader := NewLoader("")

	key := loader.envKey("TEST")
	if key != "XXSQL_TEST" {
		t.Errorf("envKey(TEST) = %s, want XXSQL_TEST", key)
	}
}
