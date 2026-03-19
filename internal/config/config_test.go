package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	// Check server defaults
	if cfg.Server.Name == "" {
		t.Error("Server.Name should not be empty")
	}
	if cfg.Server.DataDir == "" {
		t.Error("Server.DataDir should not be empty")
	}

	// Check network defaults
	if cfg.Network.PrivatePort <= 0 {
		t.Error("Network.PrivatePort should be positive")
	}
	if cfg.Network.MySQLPort <= 0 {
		t.Error("Network.MySQLPort should be positive")
	}

	// Check storage defaults
	if cfg.Storage.PageSize <= 0 {
		t.Error("Storage.PageSize should be positive")
	}
	if cfg.Storage.BufferPoolSize <= 0 {
		t.Error("Storage.BufferPoolSize should be positive")
	}

	// Check log defaults
	if cfg.Log.Level == "" {
		t.Error("Log.Level should not be empty")
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := DefaultConfig()

	err := cfg.Validate()
	if err != nil {
		t.Errorf("valid config should pass validation: %v", err)
	}
}

func TestConfig_Validate_InvalidPrivatePort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.PrivatePort = 0

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for invalid private port")
	}
}

func TestConfig_Validate_InvalidPrivatePort_TooHigh(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.PrivatePort = 70000

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for port too high")
	}
}

func TestConfig_Validate_InvalidMySQLPort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.MySQLPort = -1

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for invalid MySQL port")
	}
}

func TestConfig_Validate_InvalidHTTPPort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.HTTPPort = -1

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for invalid HTTP port")
	}
}

func TestConfig_Validate_InvalidLogLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Log.Level = "INVALID"

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for invalid log level")
	}
}

func TestConfig_Validate_InvalidPageSize_TooSmall(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.PageSize = 100

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for page size too small")
	}
}

func TestConfig_Validate_InvalidPageSize_TooLarge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.PageSize = 100000

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for page size too large")
	}
}

func TestConfig_Validate_InvalidBufferPoolSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.BufferPoolSize = 5

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for buffer pool size too small")
	}
}

func TestConfig_Validate_InvalidMaxConnections(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Connection.MaxConnections = 0

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for max connections too small")
	}
}

func TestConfig_Validate_InvalidWorkerCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerPool.WorkerCount = 0

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for worker count too small")
	}
}

func TestLoader_NewLoader(t *testing.T) {
	loader := NewLoader("/path/to/config.json")

	if loader == nil {
		t.Fatal("NewLoader returned nil")
	}
	if loader.configPath != "/path/to/config.json" {
		t.Errorf("configPath = %s, want /path/to/config.json", loader.configPath)
	}
}

func TestLoader_Load_Defaults(t *testing.T) {
	loader := NewLoader("")

	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg == nil {
		t.Fatal("Load returned nil config")
	}

	// Should have default values
	if cfg.Server.Name == "" {
		t.Error("should have default server name")
	}
}

func TestLoader_LoadFromFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config file
	cfg := DefaultConfig()
	cfg.Server.Name = "test-server"
	cfg.Network.PrivatePort = 9999

	configPath := filepath.Join(tmpDir, "config.json")
	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, data, 0644)

	// Load from file
	loader := NewLoader(configPath)
	loaded, err := loader.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Server.Name != "test-server" {
		t.Errorf("Server.Name = %s, want test-server", loaded.Server.Name)
	}
	if loaded.Network.PrivatePort != 9999 {
		t.Errorf("PrivatePort = %d, want 9999", loaded.Network.PrivatePort)
	}
}

func TestLoader_LoadFromFile_NotExists(t *testing.T) {
	loader := NewLoader("/nonexistent/path/config.json")

	_, err := loader.Load()
	if err == nil {
		t.Error("expected error for nonexistent config file")
	}
}

func TestLoader_LoadFromFile_InvalidJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.json")
	os.WriteFile(configPath, []byte("invalid json {{{"), 0644)

	loader := NewLoader(configPath)
	_, err = loader.Load()
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestLoader_EnvironmentVariables(t *testing.T) {
	// Set environment variables
	os.Setenv("XXSQL_SERVER_NAME", "env-server")
	os.Setenv("XXSQL_DATA_DIR", "/env/data")
	os.Setenv("XXSQL_PRIVATE_PORT", "8888")
	os.Setenv("XXSQL_LOG_LEVEL", "DEBUG")
	defer func() {
		os.Unsetenv("XXSQL_SERVER_NAME")
		os.Unsetenv("XXSQL_DATA_DIR")
		os.Unsetenv("XXSQL_PRIVATE_PORT")
		os.Unsetenv("XXSQL_LOG_LEVEL")
	}()

	loader := NewLoader("")
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Server.Name != "env-server" {
		t.Errorf("Server.Name = %s, want env-server", cfg.Server.Name)
	}
	if cfg.Server.DataDir != "/env/data" {
		t.Errorf("DataDir = %s, want /env/data", cfg.Server.DataDir)
	}
	if cfg.Network.PrivatePort != 8888 {
		t.Errorf("PrivatePort = %d, want 8888", cfg.Network.PrivatePort)
	}
	if cfg.Log.Level != "DEBUG" {
		t.Errorf("LogLevel = %s, want DEBUG", cfg.Log.Level)
	}
}

func TestGenerateExampleConfig(t *testing.T) {
	data, err := GenerateExampleConfig()
	if err != nil {
		t.Fatalf("GenerateExampleConfig failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("GenerateExampleConfig returned empty data")
	}

	// Verify it's valid JSON
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Errorf("generated config is not valid JSON: %v", err)
	}
}

func TestSaveConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig()
	cfg.Server.Name = "saved-server"

	configPath := filepath.Join(tmpDir, "config.json")
	err = SaveConfig(cfg, configPath)
	if err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("config file was not created")
	}

	// Load and verify
	data, _ := os.ReadFile(configPath)
	var loaded Config
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to parse saved config: %v", err)
	}

	if loaded.Server.Name != "saved-server" {
		t.Errorf("Server.Name = %s, want saved-server", loaded.Server.Name)
	}
}

func TestSaveConfig_CreateDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig()
	configPath := filepath.Join(tmpDir, "subdir", "deep", "config.json")

	err = SaveConfig(cfg, configPath)
	if err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("config file was not created in nested directory")
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		{"123", 123, false},
		{"  456  ", 456, false},
		{"0", 0, false},
		{"-10", -10, false},
		{"abc", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		result, err := parseInt(tt.input)
		if tt.hasError {
			if err == nil {
				t.Errorf("parseInt(%q) should fail", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("parseInt(%q) failed: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("parseInt(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		}
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"1", true},
		{"yes", true},
		{"YES", true},
		{"on", true},
		{"ON", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"off", false},
		{"anything", false},
	}

	for _, tt := range tests {
		result := parseBool(tt.input)
		if result != tt.expected {
			t.Errorf("parseBool(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestConfig_AllLogLevels(t *testing.T) {
	validLevels := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for _, level := range validLevels {
		cfg := DefaultConfig()
		cfg.Log.Level = level

		err := cfg.Validate()
		if err != nil {
			t.Errorf("log level %s should be valid: %v", level, err)
		}
	}
}

func TestConfig_PortRanges(t *testing.T) {
	cfg := DefaultConfig()

	// Valid port range
	cfg.Network.PrivatePort = 1
	cfg.Network.MySQLPort = 65535
	cfg.Network.HTTPPort = 8080

	err := cfg.Validate()
	if err != nil {
		t.Errorf("valid port range failed: %v", err)
	}
}

func TestConfig_PageSizeRange(t *testing.T) {
	// Minimum valid
	cfg := DefaultConfig()
	cfg.Storage.PageSize = 512
	if err := cfg.Validate(); err != nil {
		t.Errorf("page size 512 should be valid: %v", err)
	}

	// Maximum valid
	cfg.Storage.PageSize = 65536
	if err := cfg.Validate(); err != nil {
		t.Errorf("page size 65536 should be valid: %v", err)
	}
}

func TestLoader_LoadFromFile_WithEnvExpansion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set env var
	os.Setenv("TEST_SERVER_NAME", "expanded-name")
	defer os.Unsetenv("TEST_SERVER_NAME")

	// Create config with env var reference
	configContent := `{
		"server": {
			"name": "${TEST_SERVER_NAME}",
			"data_dir": "./data"
		},
		"network": {
			"private_port": 9527,
			"mysql_port": 3306,
			"http_port": 8080,
			"bind": "0.0.0.0"
		},
		"storage": {
			"page_size": 4096,
			"buffer_pool_size": 1000
		},
		"log": {
			"level": "INFO"
		},
		"connection": {
			"max_connections": 200
		},
		"worker_pool": {
			"worker_count": 32
		}
	}`

	configPath := filepath.Join(tmpDir, "config.json")
	os.WriteFile(configPath, []byte(configContent), 0644)

	loader := NewLoader(configPath)
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Server.Name != "expanded-name" {
		t.Errorf("Server.Name = %s, want expanded-name", cfg.Server.Name)
	}
}

func BenchmarkDefaultConfig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DefaultConfig()
	}
}

func BenchmarkConfig_Validate(b *testing.B) {
	cfg := DefaultConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.Validate()
	}
}

func BenchmarkLoader_Load(b *testing.B) {
	loader := NewLoader("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loader.Load()
	}
}

// TestNetworkEnabledFlags tests the service enabled flags
func TestNetworkEnabledFlags(t *testing.T) {
	t.Run("default all enabled", func(t *testing.T) {
		cfg := DefaultConfig()
		if !cfg.Network.IsPrivateEnabled() {
			t.Error("Private should be enabled by default")
		}
		if !cfg.Network.IsMySQLEnabled() {
			t.Error("MySQL should be enabled by default")
		}
		if !cfg.Network.IsHTTPEnabled() {
			t.Error("HTTP should be enabled by default")
		}
	})

	t.Run("explicit disable", func(t *testing.T) {
		falseVal := false
		cfg := DefaultConfig()
		cfg.Network.PrivateEnabled = &falseVal
		cfg.Network.MySQLEnabled = &falseVal
		cfg.Network.HTTPEnabled = &falseVal

		if cfg.Network.IsPrivateEnabled() {
			t.Error("Private should be disabled")
		}
		if cfg.Network.IsMySQLEnabled() {
			t.Error("MySQL should be disabled")
		}
		if cfg.Network.IsHTTPEnabled() {
			t.Error("HTTP should be disabled")
		}
	})

	t.Run("nil means default true", func(t *testing.T) {
		cfg := &Config{
			Network: NetworkConfig{
				PrivateEnabled: nil,
				MySQLEnabled:   nil,
				HTTPEnabled:    nil,
			},
		}

		if !cfg.Network.IsPrivateEnabled() {
			t.Error("nil PrivateEnabled should default to true")
		}
		if !cfg.Network.IsMySQLEnabled() {
			t.Error("nil MySQLEnabled should default to true")
		}
		if !cfg.Network.IsHTTPEnabled() {
			t.Error("nil HTTPEnabled should default to true")
		}
	})
}

// TestValidateWithDisabledServices tests validation with disabled services
func TestValidateWithDisabledServices(t *testing.T) {
	falseVal := false
	cfg := DefaultConfig()

	// Disable all services
	cfg.Network.PrivateEnabled = &falseVal
	cfg.Network.MySQLEnabled = &falseVal
	cfg.Network.HTTPEnabled = &falseVal

	// Set invalid ports (should not fail validation since services are disabled)
	cfg.Network.PrivatePort = 0
	cfg.Network.MySQLPort = 0
	cfg.Network.HTTPPort = 0

	if err := cfg.Validate(); err != nil {
		t.Errorf("Validation should pass with disabled services: %v", err)
	}
}
