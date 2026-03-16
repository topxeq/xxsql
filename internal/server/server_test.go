package server_test

import (
	"os"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/server"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestServerStartStop(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create config
	cfg := &config.Config{
		Network: config.NetworkConfig{
			Bind:         "127.0.0.1",
			PrivatePort:  19527,
			MySQLPort:    13306,
			HTTPPort:     18080,
		},
		Connection: config.ConnectionConfig{
			MaxConnections: 10,
			WaitTimeout:    30,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
	}

	// Create logger
	logger := log.NewLogger(log.WithLevel(log.INFO))

	// Create server
	srv := server.New(cfg, logger, engine)

	// Start server
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	if !srv.IsRunning() {
		t.Error("Server should be running")
	}

	// Give servers time to start
	time.Sleep(100 * time.Millisecond)

	// Stop server
	if err := srv.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	if srv.IsRunning() {
		t.Error("Server should not be running after stop")
	}
}

func TestServerStats(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create config
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

	// Create logger
	logger := log.NewLogger(log.WithLevel(log.INFO))

	// Create server
	srv := server.New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	// Get stats
	stats := srv.GetStats()

	if stats.TotalConnections != 0 {
		t.Errorf("Expected 0 total connections, got %d", stats.TotalConnections)
	}
}

func TestServerUptime(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create config
	cfg := &config.Config{
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: 19529,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
	}

	// Create logger
	logger := log.NewLogger(log.WithLevel(log.INFO))

	// Create server
	srv := server.New(cfg, logger, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Check uptime
	uptime := srv.Uptime()
	if uptime < 50*time.Millisecond {
		t.Errorf("Uptime should be at least 50ms, got %v", uptime)
	}
}
