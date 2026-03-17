package server

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestServerStartStop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	cfg := &config.Config{
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: 19527,
			MySQLPort:   13306,
			HTTPPort:    18080,
		},
		Connection: config.ConnectionConfig{
			MaxConnections: 10,
			WaitTimeout:    30,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
	}

	logger := log.NewLogger(log.WithLevel(log.INFO))

	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	if !srv.IsRunning() {
		t.Error("Server should be running")
	}

	time.Sleep(100 * time.Millisecond)

	if err := srv.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	if srv.IsRunning() {
		t.Error("Server should not be running after stop")
	}
}

func TestServerStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	cfg := &config.Config{
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: 19528,
		},
		Connection: config.ConnectionConfig{
			MaxConnections: 10,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
	}

	logger := log.NewLogger(log.WithLevel(log.INFO))

	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	stats := srv.GetStats()

	if stats.TotalConnections != 0 {
		t.Errorf("Expected 0 total connections, got %d", stats.TotalConnections)
	}
}

func TestServerAuthManager(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = true

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, nil)

	auth := srv.Auth()
	if auth == nil {
		t.Error("Auth should not be nil")
	}
}

func TestServerLogger(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.DEBUG))
	srv := New(cfg, logger, nil)

	srvLogger := srv.Logger()
	if srvLogger == nil {
		t.Error("Logger should not be nil")
	}
}

func TestMySQLServerStartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19537
	cfg.Network.MySQLPort = 13337
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)
}

func TestHTTPServerStartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19538
	cfg.Network.HTTPPort = 18082
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)
}

func TestServerWithAuthEnabled(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19539
	cfg.Auth.Enabled = true
	cfg.Auth.AdminUser = "admin"
	cfg.Auth.AdminPassword = "password"

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	auth := srv.Auth()
	user, err := auth.GetUser("admin")
	if err != nil {
		t.Fatalf("Admin user should exist: %v", err)
	}
	if user.Username != "admin" {
		t.Errorf("Username: got %q, want 'admin'", user.Username)
	}
}

func TestServerUptime(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19540
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	uptime := srv.Uptime()
	if uptime < 50*time.Millisecond {
		t.Errorf("Uptime should be at least 50ms, got %v", uptime)
	}
}

func TestServerStatsValues(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19541
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	stats := srv.GetStats()
	if stats.TotalConnections != 0 {
		t.Logf("TotalConnections: %d", stats.TotalConnections)
	}
	if stats.ActiveConnections != 0 {
		t.Logf("ActiveConnections: %d", stats.ActiveConnections)
	}
}

func TestServerAllPortsEnabled(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19542
	cfg.Network.MySQLPort = 13342
	cfg.Network.HTTPPort = 18083
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)
}

func TestCreatePIDFile_Twice(t *testing.T) {
	tmpDir := t.TempDir()
	pidFile := tmpDir + "/test.pid"

	if err := CreatePIDFile(pidFile); err != nil {
		t.Fatalf("First CreatePIDFile failed: %v", err)
	}

	if err := CreatePIDFile(pidFile); err != nil {
		t.Fatalf("Second CreatePIDFile should overwrite: %v", err)
	}

	RemovePIDFile(pidFile)
}

func TestServerStopWithoutStart(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, nil)

	if err := srv.Stop(); err != nil {
		t.Errorf("Stop without start should not error: %v", err)
	}
}

func TestServerDoubleStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19543
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	srv.Stop()

	if err := srv.Stop(); err != nil {
		t.Errorf("Second stop should not error: %v", err)
	}
}

func TestServerConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19544
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.IsRunning()
			_ = srv.GetStats()
			_ = srv.Uptime()
		}()
	}
	wg.Wait()
}

func TestMysqlTypeFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected byte
	}{
		{"SEQ", 0x03},
		{"INT", 0x03},
		{"FLOAT", 0x05},
		{"VARCHAR", 0xFD},
		{"TEXT", 0xFD},
		{"DATE", 0x0A},
		{"TIME", 0x0B},
		{"DATETIME", 0x0C},
		{"CHAR", 0xFD},
		{"BOOL", 0x01},
		{"DOUBLE", 0x05},
		{"UNKNOWN", 0xFD},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := mysqlTypeFromString(tt.input)
			if result != tt.expected {
				t.Errorf("mysqlTypeFromString(%q): got 0x%02X, want 0x%02X", tt.input, result, tt.expected)
			}
		})
	}
}
