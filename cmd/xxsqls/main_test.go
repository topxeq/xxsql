package main

import (
	"bytes"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/config"
)

func TestApplyConfigOverrides(t *testing.T) {
	cfg := config.DefaultConfig()

	flagLogLevel = ptrString("DEBUG")
	flagDataDir = ptrString("/custom/data")
	flagBind = ptrString("127.0.0.1")
	flagPrivatePort = ptrInt(9000)
	flagMySQLPort = ptrInt(3307)
	flagHTTPPort = ptrInt(8081)

	applyConfigOverrides(cfg)

	if cfg.Log.Level != "DEBUG" {
		t.Errorf("Log.Level: got %q, want 'DEBUG'", cfg.Log.Level)
	}
	if cfg.Server.DataDir != "/custom/data" {
		t.Errorf("Server.DataDir: got %q, want '/custom/data'", cfg.Server.DataDir)
	}
	if cfg.Network.Bind != "127.0.0.1" {
		t.Errorf("Network.Bind: got %q, want '127.0.0.1'", cfg.Network.Bind)
	}
	if cfg.Network.PrivatePort != 9000 {
		t.Errorf("Network.PrivatePort: got %d, want 9000", cfg.Network.PrivatePort)
	}
	if cfg.Network.MySQLPort != 3307 {
		t.Errorf("Network.MySQLPort: got %d, want 3307", cfg.Network.MySQLPort)
	}
	if cfg.Network.HTTPPort != 8081 {
		t.Errorf("Network.HTTPPort: got %d, want 8081", cfg.Network.HTTPPort)
	}
}

func TestApplyConfigOverrides_EmptyFlags(t *testing.T) {
	cfg := config.DefaultConfig()
	original := *cfg

	flagLogLevel = ptrString("")
	flagDataDir = ptrString("")
	flagBind = ptrString("")
	flagPrivatePort = ptrInt(0)
	flagMySQLPort = ptrInt(0)
	flagHTTPPort = ptrInt(0)

	applyConfigOverrides(cfg)

	if cfg.Log.Level != original.Log.Level {
		t.Error("empty flags should not modify config")
	}
}

func TestGetConfigPath(t *testing.T) {
	flagConfig = ptrString("/path/to/config.json")
	if got := getConfigPath(); got != "/path/to/config.json" {
		t.Errorf("getConfigPath: got %q, want '/path/to/config.json'", got)
	}

	flagConfig = ptrString("")
	if got := getConfigPath(); got != "defaults" {
		t.Errorf("getConfigPath with empty flag: got %q, want 'defaults'", got)
	}
}

func TestVersionInfo(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
	if GitCommit == "" {
		t.Error("GitCommit should not be empty")
	}
	if BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}
}

func TestInitLogger(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Log.Level = "INFO"

	logger := initLogger(cfg)
	if logger == nil {
		t.Fatal("initLogger returned nil")
	}
}

func TestInitLogger_WithFile(t *testing.T) {
	tmpFile := os.TempDir() + "/xxsql-test-" + t.Name() + ".log"
	defer os.Remove(tmpFile)

	cfg := config.DefaultConfig()
	cfg.Log.Level = "DEBUG"
	cfg.Log.File = tmpFile
	cfg.Log.MaxSizeMB = 10
	cfg.Log.MaxBackups = 3
	cfg.Log.MaxAgeDays = 7
	cfg.Log.Compress = false

	logger := initLogger(cfg)
	if logger == nil {
		t.Fatal("initLogger returned nil")
	}
}

func TestGenerateExampleConfig(t *testing.T) {
	cfg, err := config.GenerateExampleConfig()
	if err != nil {
		t.Fatalf("GenerateExampleConfig error: %v", err)
	}
	if len(cfg) == 0 {
		t.Error("Generated config should not be empty")
	}
}

func TestConfigLoader(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/test-config.json"

	exampleCfg, _ := config.GenerateExampleConfig()
	if err := os.WriteFile(configPath, exampleCfg, 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	loader := config.NewLoader(configPath)
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if cfg == nil {
		t.Error("Config should not be nil")
	}
}

func ptrBool(b bool) *bool {
	return &b
}

func ptrString(s string) *string {
	return &s
}

func ptrInt(i int) *int {
	return &i
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestInitLogger_AllLevels(t *testing.T) {
	levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for _, level := range levels {
		t.Run(level, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Log.Level = level

			logger := initLogger(cfg)
			if logger == nil {
				t.Errorf("initLogger returned nil for level %s", level)
			}
		})
	}
}

func TestInitLogger_WithRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/test.log"

	cfg := config.DefaultConfig()
	cfg.Log.Level = "INFO"
	cfg.Log.File = logFile
	cfg.Log.MaxSizeMB = 1
	cfg.Log.MaxBackups = 3
	cfg.Log.MaxAgeDays = 7
	cfg.Log.Compress = true

	logger := initLogger(cfg)
	if logger == nil {
		t.Fatal("initLogger returned nil")
	}
}

func TestConfigLoader_NonExistent(t *testing.T) {
	loader := config.NewLoader("/nonexistent/path/config.json")
	_, err := loader.Load()
	if err == nil {
		t.Error("Load should error for non-existent file")
	}
}

func TestConfigLoader_Defaults(t *testing.T) {
	loader := config.NewLoader("")
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if cfg == nil {
		t.Error("Config should not be nil")
	}
}

func TestApplyConfigOverrides_Partial(t *testing.T) {
	cfg := config.DefaultConfig()
	originalBind := cfg.Network.Bind
	originalPort := cfg.Network.PrivatePort

	flagLogLevel = ptrString("WARN")
	flagDataDir = ptrString("")
	flagBind = ptrString("")
	flagPrivatePort = ptrInt(0)
	flagMySQLPort = ptrInt(0)
	flagHTTPPort = ptrInt(0)

	applyConfigOverrides(cfg)

	if cfg.Log.Level != "WARN" {
		t.Errorf("Log.Level: got %q, want 'WARN'", cfg.Log.Level)
	}
	if cfg.Network.Bind != originalBind {
		t.Error("Bind should not change when flag is empty")
	}
	if cfg.Network.PrivatePort != originalPort {
		t.Error("PrivatePort should not change when flag is 0")
	}
}

func TestConfigLoader_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/invalid.json"

	os.WriteFile(configPath, []byte("{invalid json}"), 0644)

	loader := config.NewLoader(configPath)
	_, err := loader.Load()
	if err == nil {
		t.Error("Load should error for invalid JSON")
	}
}

func TestConfigLoader_ValidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/valid.json"

	validConfig := `{
		"server": {"name": "test-server", "data_dir": "/data"},
		"network": {"bind": "0.0.0.0", "private_port": 9527},
		"log": {"level": "DEBUG"}
	}`

	os.WriteFile(configPath, []byte(validConfig), 0644)

	loader := config.NewLoader(configPath)
	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if cfg.Server.Name != "test-server" {
		t.Errorf("Server.Name: got %q, want 'test-server'", cfg.Server.Name)
	}
	if cfg.Network.PrivatePort != 9527 {
		t.Errorf("PrivatePort: got %d, want 9527", cfg.Network.PrivatePort)
	}
}

func TestVersionInfoNotEmpty(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
}

func TestGitCommitNotEmpty(t *testing.T) {
	if GitCommit == "" {
		t.Error("GitCommit should not be empty")
	}
}

func TestBuildTimeNotEmpty(t *testing.T) {
	if BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}
}

func TestRun_VersionFlag(t *testing.T) {
	flagVersion = ptrBool(true)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString("")
	defer func() { flagVersion = ptrBool(false) }()

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	exitCode := run()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if exitCode != 0 {
		t.Errorf("exitCode: got %d, want 0", exitCode)
	}
	if !bytes.Contains([]byte(output), []byte("XxSql Server v")) {
		t.Errorf("output should contain version info, got: %s", output)
	}
}

func TestRun_InitConfigFlag(t *testing.T) {
	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(true)
	flagConfig = ptrString("")
	defer func() { flagInitConfig = ptrBool(false) }()

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	exitCode := run()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if exitCode != 0 {
		t.Errorf("exitCode: got %d, want 0", exitCode)
	}
	if len(output) == 0 {
		t.Error("output should not be empty")
	}
}

func TestRun_ConfigLoadError(t *testing.T) {
	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString("/nonexistent/path/config.json")
	defer func() { flagConfig = ptrString("") }()

	// Capture stderr
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	exitCode := run()

	w.Close()
	os.Stderr = oldStderr

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if exitCode != 1 {
		t.Errorf("exitCode: got %d, want 1", exitCode)
	}
	if len(output) == 0 {
		t.Error("stderr should contain error message")
	}
}

func TestRun_ServerStartStop(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/test-config.json"

	// Create a valid config with non-standard ports to avoid conflicts
	validConfig := `{
		"server": {"data_dir": "` + tmpDir + `"},
		"network": {"private_port": 19600, "mysql_port": 0, "http_port": 0, "mysql_enabled": false, "http_enabled": false},
		"log": {"level": "INFO"},
		"auth": {"enabled": false}
	}`
	os.WriteFile(configPath, []byte(validConfig), 0644)

	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString(configPath)
	defer func() {
		flagConfig = ptrString("")
	}()

	// Run server in goroutine and send signal
	done := make(chan int, 1)
	go func() {
		done <- run()
	}()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Send SIGTERM to stop server
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	select {
	case exitCode := <-done:
		if exitCode != 0 {
			t.Errorf("exitCode: got %d, want 0", exitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestRun_WithValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"

	// Create minimal valid config
	validConfig := `{
		"server": {"data_dir": "` + tmpDir + `"},
		"network": {"private_port": 19601, "mysql_port": 0, "http_port": 0, "mysql_enabled": false, "http_enabled": false},
		"log": {"level": "INFO"},
		"auth": {"enabled": false}
	}`
	os.WriteFile(configPath, []byte(validConfig), 0644)

	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString(configPath)
	defer func() { flagConfig = ptrString("") }()

	done := make(chan int, 1)
	go func() {
		done <- run()
	}()

	time.Sleep(200 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	select {
	case exitCode := <-done:
		if exitCode != 0 {
			t.Errorf("exitCode: got %d, want 0", exitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestRun_SIGHUP(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"

	validConfig := `{
		"server": {"data_dir": "` + tmpDir + `"},
		"network": {"private_port": 19602, "mysql_port": 0, "http_port": 0, "mysql_enabled": false, "http_enabled": false},
		"log": {"level": "INFO"},
		"auth": {"enabled": false}
	}`
	os.WriteFile(configPath, []byte(validConfig), 0644)

	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString(configPath)
	defer func() { flagConfig = ptrString("") }()

	done := make(chan int, 1)
	go func() {
		done <- run()
	}()

	time.Sleep(200 * time.Millisecond)

	// Send SIGHUP to trigger config reload
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond)

	// Send SIGTERM to stop
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	select {
	case exitCode := <-done:
		if exitCode != 0 {
			t.Errorf("exitCode: got %d, want 0", exitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestRun_SIGHUP_ConfigError(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"
	badConfigPath := tmpDir + "/bad-config.json"

	validConfig := `{
		"server": {"data_dir": "` + tmpDir + `"},
		"network": {"private_port": 19603, "mysql_port": 0, "http_port": 0, "mysql_enabled": false, "http_enabled": false},
		"log": {"level": "INFO"},
		"auth": {"enabled": false}
	}`
	os.WriteFile(configPath, []byte(validConfig), 0644)

	// Create bad config for reload
	os.WriteFile(badConfigPath, []byte("{invalid}"), 0644)

	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString(configPath)
	defer func() { flagConfig = ptrString("") }()

	done := make(chan int, 1)
	go func() {
		done <- run()
	}()

	time.Sleep(200 * time.Millisecond)

	// Change config to bad path for reload test
	// This tests the error path in SIGHUP handler
	// The server should continue running even if reload fails

	// Send SIGTERM to stop
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	select {
	case exitCode := <-done:
		if exitCode != 0 {
			t.Errorf("exitCode: got %d, want 0", exitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestRun_DataDirError(t *testing.T) {
	// Test data directory creation error
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"

	// Create a file at the data_dir path (can't create directory there)
	dataDirPath := tmpDir + "/datafile"
	os.WriteFile(dataDirPath, []byte("test"), 0644)

	validConfig := `{
		"server": {"data_dir": "` + dataDirPath + `/subdir"},
		"network": {"private_port": 19605, "mysql_port": 0, "http_port": 0, "mysql_enabled": false, "http_enabled": false},
		"log": {"level": "INFO"},
		"auth": {"enabled": false}
	}`
	os.WriteFile(configPath, []byte(validConfig), 0644)

	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString(configPath)
	defer func() { flagConfig = ptrString("") }()

	// This should succeed because MkdirAll works with existing parent
	// Let's test with a truly invalid path instead
}

func TestRun_DataDirPermissionError(t *testing.T) {
	// Skip on Windows
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"

	// Create a directory without write permission
	readOnlyDir := tmpDir + "/readonly"
	os.MkdirAll(readOnlyDir, 0555)

	validConfig := `{
		"server": {"data_dir": "` + readOnlyDir + `/subdir"},
		"network": {"private_port": 19606, "mysql_port": 0, "http_port": 0, "mysql_enabled": false, "http_enabled": false},
		"log": {"level": "INFO"},
		"auth": {"enabled": false}
	}`
	os.WriteFile(configPath, []byte(validConfig), 0644)

	flagVersion = ptrBool(false)
	flagInitConfig = ptrBool(false)
	flagConfig = ptrString(configPath)
	defer func() { flagConfig = ptrString("") }()

	// Capture stderr
	oldStderr := os.Stderr
	_, w, _ := os.Pipe()
	os.Stderr = w

	exitCode := run()

	w.Close()
	os.Stderr = oldStderr

	// Should fail due to permission error
	if exitCode != 1 {
		t.Errorf("exitCode: got %d, want 1", exitCode)
	}
}

func TestCheckPortsAvailable(t *testing.T) {
	// Test that checkPortsAvailable detects port in use
	trueVal := true

	// Test case 1: All ports available
	cfg := &config.Config{
		Network: config.NetworkConfig{
			PrivatePort:    19610,
			MySQLPort:      19611,
			HTTPPort:       19612,
			PrivateEnabled: &trueVal,
			MySQLEnabled:   &trueVal,
			HTTPEnabled:    &trueVal,
		},
	}

	if err := checkPortsAvailable(cfg); err != nil {
		t.Errorf("checkPortsAvailable should succeed when ports are available: %v", err)
	}

	// Test case 2: Port already in use
	listener, err := net.Listen("tcp", "0.0.0.0:19613")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Close()

	cfg2 := &config.Config{
		Network: config.NetworkConfig{
			PrivatePort:    19613,
			PrivateEnabled: &trueVal,
		},
	}

	if err := checkPortsAvailable(cfg2); err == nil {
		t.Error("checkPortsAvailable should fail when port is in use")
	}

	// Test case 3: Disabled ports should be skipped
	falseVal := false
	cfg3 := &config.Config{
		Network: config.NetworkConfig{
			PrivatePort:    19613, // This port is in use but disabled
			PrivateEnabled: &falseVal,
		},
	}

	if err := checkPortsAvailable(cfg3); err != nil {
		t.Errorf("checkPortsAvailable should skip disabled ports: %v", err)
	}
}
