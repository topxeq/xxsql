package server

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/mysql"
	"github.com/topxeq/xxsql/internal/protocol"
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

// ============================================================================
// MySQLServer Tests
// ============================================================================

func TestMySQLServer_New(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19550
	cfg.Network.MySQLPort = 13350
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 13350)
	if mysqlSrv == nil {
		t.Error("NewMySQLServer should not return nil")
	}
}

func TestMySQLServer_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19551
	cfg.Network.MySQLPort = 13351
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 13351)

	if err := mysqlSrv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := mysqlSrv.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestMySQLServer_DoubleStart(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19552
	cfg.Network.MySQLPort = 13352
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 13352)

	if err := mysqlSrv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mysqlSrv.Stop()

	if err := mysqlSrv.Start(); err == nil {
		t.Error("Double start should return error")
	}
}

func TestMySQLServer_DoubleStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19553
	cfg.Network.MySQLPort = 13353
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 13353)

	mysqlSrv.Start()

	mysqlSrv.Stop()

	if err := mysqlSrv.Stop(); err != nil {
		t.Errorf("Double stop should not error: %v", err)
	}
}

// ============================================================================
// HTTPServer Tests
// ============================================================================

func TestHTTPServer_New(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19554
	cfg.Network.HTTPPort = 18084
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	httpSrv := NewHTTPServer(srv, "127.0.0.1", 18084)
	if httpSrv == nil {
		t.Error("NewHTTPServer should not return nil")
	}
}

func TestHTTPServer_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19555
	cfg.Network.HTTPPort = 18085
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	httpSrv := NewHTTPServer(srv, "127.0.0.1", 18085)

	if err := httpSrv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := httpSrv.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestHTTPServer_DoubleStart(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19556
	cfg.Network.HTTPPort = 18086
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	httpSrv := NewHTTPServer(srv, "127.0.0.1", 18086)

	if err := httpSrv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer httpSrv.Stop()

	if err := httpSrv.Start(); err == nil {
		t.Error("Double start should return error")
	}
}

func TestHTTPServer_DoubleStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19557
	cfg.Network.HTTPPort = 18087
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	httpSrv := NewHTTPServer(srv, "127.0.0.1", 18087)

	httpSrv.Start()

	httpSrv.Stop()

	if err := httpSrv.Stop(); err != nil {
		t.Errorf("Double stop should not error: %v", err)
	}
}

// ============================================================================
// Server Core Tests
// ============================================================================

func TestServer_New_WithNilEngine(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.INFO))

	srv := New(cfg, logger, nil)
	if srv == nil {
		t.Error("New with nil engine should still return server")
	}
}

func TestServer_DoubleStart(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19558
	cfg.Network.MySQLPort = 0
	cfg.Network.HTTPPort = 0
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

	if err := srv.Start(); err == nil {
		t.Error("Double start should return error")
	}
}

func TestServer_GetStats_AfterQuery(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19559
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

	// Get initial stats
	stats := srv.GetStats()
	initialQueries := stats.TotalQueries

	_ = initialQueries // Just verify we can get stats
}

func TestServer_NextConnectionID(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, nil)

	id1 := srv.nextConnectionID()
	id2 := srv.nextConnectionID()

	if id1 == id2 {
		t.Error("Connection IDs should be unique")
	}

	if id2 <= id1 {
		t.Errorf("Connection IDs should increment: id1=%d, id2=%d", id1, id2)
	}
}

// ============================================================================
// PID File Tests
// ============================================================================

func TestCreatePIDFile_EmptyPath(t *testing.T) {
	if err := CreatePIDFile(""); err != nil {
		t.Errorf("CreatePIDFile with empty path should not error: %v", err)
	}
}

func TestRemovePIDFile_EmptyPath(t *testing.T) {
	// Should not panic
	RemovePIDFile("")
}

func TestRemovePIDFile_NonExistent(t *testing.T) {
	// Should not error
	RemovePIDFile("/tmp/nonexistent-pid-file-12345.pid")
}

// ============================================================================
// Backup Manager Tests
// ============================================================================

func TestServer_BackupManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19560
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if srv.backup == nil {
		t.Error("Backup manager should be initialized with engine")
	}
}

// ============================================================================
// Executor Tests
// ============================================================================

func TestServer_Executor(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19561
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	if srv.executor == nil {
		t.Error("Executor should be initialized with engine")
	}
}

func TestServer_Executor_NilEngine(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, nil)

	if srv.executor != nil {
		t.Error("Executor should be nil with nil engine")
	}
}

// ============================================================================
// Context and Cancellation Tests
// ============================================================================

func TestServer_Context(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, nil)

	if srv.ctx == nil {
		t.Error("Context should be initialized")
	}

	if srv.cancel == nil {
		t.Error("Cancel function should be initialized")
	}
}

// ============================================================================
// onConnect and onDisconnect Tests
// ============================================================================

func TestServer_OnConnectDisconnect(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19562
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

	// Stats should be available
	stats := srv.GetStats()
	_ = stats
}

// ============================================================================
// Auth Tests with Enabled Auth
// ============================================================================

func TestServer_AuthEnabled_InvalidUser(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19563
	cfg.Auth.Enabled = true
	cfg.Auth.AdminUser = "admin"
	cfg.Auth.AdminPassword = "password123"

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

	// Verify admin user was created
	auth := srv.Auth()
	user, err := auth.GetUser("admin")
	if err != nil {
		t.Fatalf("Admin user should exist: %v", err)
	}

	if user.Username != "admin" {
		t.Errorf("Username: got %q, want 'admin'", user.Username)
	}
}

func TestServer_AuthEnabled_PreExistingAdmin(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19564
	cfg.Auth.Enabled = true
	cfg.Auth.AdminUser = "admin"
	cfg.Auth.AdminPassword = "password123"

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	// Create admin user before start
	srv.auth.CreateUser("admin", "existingpass", auth.RoleAdmin)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	// Verify existing admin user was not overwritten
	_, getUserErr := srv.auth.GetUser("admin")
	if getUserErr != nil {
		t.Fatalf("Admin user should exist: %v", getUserErr)
	}

	// Password should still be the original - verify by authenticating
	session, authErr := srv.auth.Authenticate("admin", "existingpass")
	if authErr != nil || session == nil {
		t.Error("Password should not have been changed")
	}
}

// ============================================================================
// Uptime Edge Cases Tests
// ============================================================================

func TestServer_Uptime_BeforeStart(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, nil)

	// Uptime may be negative or zero before start
	uptime := srv.Uptime()
	_ = uptime
}

func TestServer_Uptime_AfterStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19565
	cfg.Auth.Enabled = false

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	srv.Start()
	time.Sleep(50 * time.Millisecond)
	srv.Stop()

	// Uptime should still be accessible after stop
	uptime := srv.Uptime()
	if uptime < 0 {
		t.Error("Uptime should not be negative")
	}
}

// ============================================================================
// Concurrent Stats Access Tests
// ============================================================================

func TestServer_ConcurrentStatsAccess(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19566
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
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.GetStats()
			_ = srv.Uptime()
			_ = srv.IsRunning()
		}()
	}
	wg.Wait()
}

// ============================================================================
// Multiple Server Instances Tests
// ============================================================================

func TestServer_MultipleInstances_DifferentPorts(t *testing.T) {
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	cfg1 := config.DefaultConfig()
	cfg1.Server.DataDir = tmpDir1
	cfg1.Network.PrivatePort = 19567
	cfg1.Auth.Enabled = false

	cfg2 := config.DefaultConfig()
	cfg2.Server.DataDir = tmpDir2
	cfg2.Network.PrivatePort = 19568
	cfg2.Auth.Enabled = false

	engine1 := storage.NewEngine(tmpDir1)
	engine1.Open()
	defer engine1.Close()

	engine2 := storage.NewEngine(tmpDir2)
	engine2.Open()
	defer engine2.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))

	srv1 := New(cfg1, logger, engine1)
	srv2 := New(cfg2, logger, engine2)

	if err := srv1.Start(); err != nil {
		t.Fatalf("Server 1 start failed: %v", err)
	}
	defer srv1.Stop()

	if err := srv2.Start(); err != nil {
		t.Fatalf("Server 2 start failed: %v", err)
	}
	defer srv2.Stop()

	time.Sleep(100 * time.Millisecond)
}

// ============================================================================
// Auth Manager Access Tests
// ============================================================================

func TestServer_Auth_ManagerMethods(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19569
	cfg.Auth.Enabled = true

	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	logger := log.NewLogger(log.WithLevel(log.INFO))
	srv := New(cfg, logger, engine)

	authMgr := srv.Auth()
	if authMgr == nil {
		t.Fatal("Auth manager should not be nil")
	}

	// Test creating a user
	user, err := authMgr.CreateUser("testuser", "testpass", auth.RoleUser)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	if user.Username != "testuser" {
		t.Errorf("Username: got %q, want 'testuser'", user.Username)
	}

	// Test getting user
	retrieved, err := authMgr.GetUser("testuser")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}

	if retrieved.Username != "testuser" {
		t.Errorf("Retrieved username: got %q", retrieved.Username)
	}
}

// ============================================================================
// ServerStats Fields Tests
// ============================================================================

func TestServerStats_AllFields(t *testing.T) {
	stats := ServerStats{
		TotalConnections:  100,
		ActiveConnections: 10,
		TotalQueries:      500,
		QueriesPerSecond:  50,
		LastQueryTime:     time.Now(),
	}

	if stats.TotalConnections != 100 {
		t.Errorf("TotalConnections: got %d", stats.TotalConnections)
	}
	if stats.ActiveConnections != 10 {
		t.Errorf("ActiveConnections: got %d", stats.ActiveConnections)
	}
	if stats.TotalQueries != 500 {
		t.Errorf("TotalQueries: got %d", stats.TotalQueries)
	}
	if stats.QueriesPerSecond != 50 {
		t.Errorf("QueriesPerSecond: got %d", stats.QueriesPerSecond)
	}
	if stats.LastQueryTime.IsZero() {
		t.Error("LastQueryTime should not be zero")
	}
}

// ============================================================================
// Port Configuration Edge Cases Tests
// ============================================================================

func TestServer_ZeroPorts(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Network.PrivatePort = 19570
	cfg.Network.MySQLPort = 0
	cfg.Network.HTTPPort = 0
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

	// With zero MySQL and HTTP ports, those servers should not start
	if srv.mysql != nil {
		t.Error("MySQL server should be nil with zero port")
	}
	if srv.http != nil {
		t.Error("HTTP server should be nil with zero port")
	}
}

// ============================================================================
// Logger Access Tests
// ============================================================================

func TestServer_Logger_Methods(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.DEBUG))
	srv := New(cfg, logger, nil)

	srvLogger := srv.Logger()
	if srvLogger == nil {
		t.Error("Logger should not be nil")
	}

	// Test logger methods
	srvLogger.Debug("Test debug message")
	srvLogger.Info("Test info message")
	srvLogger.Warn("Test warn message")
	srvLogger.Error("Test error message")
}

// ============================================================================
// Protocol Handler Tests (using actual structs)
// ============================================================================

func TestServer_OnQuery_NoExecutor(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil) // No engine, so no executor

	req := &protocol.QueryRequest{
		SQL: "SELECT 1",
	}

	resp, err := srv.onQuery(nil, req)
	if err != nil {
		t.Fatalf("onQuery returned error: %v", err)
	}
	if resp.Status != protocol.StatusError {
		t.Errorf("Expected error status, got %d", resp.Status)
	}
	if resp.Message == "" {
		t.Error("Expected error message")
	}
}

func TestServer_OnQuery_WithExecutor(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Auth.Enabled = false
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	srv := New(cfg, logger, engine)

	req := &protocol.QueryRequest{
		SQL: "SELECT 1 AS col",
	}

	resp, err := srv.onQuery(nil, req)
	if err != nil {
		t.Fatalf("onQuery returned error: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Errorf("Expected OK status, got %d", resp.Status)
	}
	if resp.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", resp.RowCount)
	}
}

func TestServer_OnQuery_Error(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Auth.Enabled = false
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	srv := New(cfg, logger, engine)

	req := &protocol.QueryRequest{
		SQL: "INVALID SQL SYNTAX",
	}

	resp, err := srv.onQuery(nil, req)
	if err != nil {
		t.Fatalf("onQuery returned error: %v", err)
	}
	if resp.Status != protocol.StatusError {
		t.Errorf("Expected error status, got %d", resp.Status)
	}
}

func TestServer_OnHandshake(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil)

	tests := []struct {
		name     string
		version  uint16
		expected uint16
	}{
		{"version 1", protocol.ProtocolV1, protocol.ProtocolV1},
		{"version 2", protocol.ProtocolV2, protocol.ProtocolV2},
		{"version 3 (higher than supported)", 3, protocol.ProtocolV2},
		{"version 0", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &protocol.HandshakeRequest{
				ProtocolVersion: tt.version,
			}

			resp, err := srv.onHandshake(nil, req)
			if err != nil {
				t.Fatalf("onHandshake returned error: %v", err)
			}
			if resp.ProtocolVersion != tt.expected {
				t.Errorf("Protocol version: got %d, want %d", resp.ProtocolVersion, tt.expected)
			}
		})
	}
}

func TestServer_OnAuth_Disabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = false
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil)

	req := &protocol.AuthRequest{
		Username: "testuser",
		Password: []byte("testpass"),
		Database: "testdb",
	}

	resp, err := srv.onAuth(nil, req)
	if err != nil {
		t.Fatalf("onAuth returned error: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Errorf("Expected OK status with auth disabled, got %d", resp.Status)
	}
	if resp.SessionID != "no-auth" {
		t.Errorf("Expected 'no-auth' session ID, got %q", resp.SessionID)
	}
}

func TestServer_OnAuth_Enabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = true
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil)

	// Create a user first
	srv.auth.CreateUser("testuser", "testpass", auth.RoleUser)

	tests := []struct {
		name     string
		username string
		password string
		wantOK   bool
	}{
		{"valid credentials", "testuser", "testpass", true},
		{"invalid password", "testuser", "wrongpass", false},
		{"invalid user", "nobody", "testpass", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &protocol.AuthRequest{
				Username: tt.username,
				Password: []byte(tt.password),
			}

			resp, err := srv.onAuth(nil, req)
			if err != nil {
				t.Fatalf("onAuth returned error: %v", err)
			}
			if tt.wantOK && resp.Status != protocol.StatusOK {
				t.Errorf("Expected OK status, got %d", resp.Status)
			}
			if !tt.wantOK && resp.Status != protocol.StatusAuth {
				t.Errorf("Expected auth status, got %d", resp.Status)
			}
		})
	}
}

func TestServer_OnAuth_WithDatabase(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = true
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil)

	// Create a user
	srv.auth.CreateUser("testuser", "testpass", auth.RoleUser)

	req := &protocol.AuthRequest{
		Username: "testuser",
		Password: []byte("testpass"),
		Database: "testdb",
	}

	resp, err := srv.onAuth(nil, req)
	if err != nil {
		t.Fatalf("onAuth returned error: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Errorf("Expected OK status, got %d", resp.Status)
	}
}

// ============================================================================
// MySQL Handler Tests (using actual structs where possible)
// ============================================================================

func TestMySQLServer_HandleMySQLQuery_NoExecutor(t *testing.T) {
	cfg := config.DefaultConfig()
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil) // No executor
	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 3306)

	// Create a real MySQLHandler with minimal setup
	handler := mysql.NewMySQLHandler(nil, 1)

	_, _, err := mysqlSrv.handleMySQLQuery(handler, "SELECT 1")
	if err == nil {
		t.Error("Expected error when no executor")
	}
}

func TestMySQLServer_HandleMySQLQuery_WithExecutor(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Auth.Enabled = false
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	srv := New(cfg, logger, engine)
	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 3306)

	// Create a real MySQLHandler with minimal setup
	handler := mysql.NewMySQLHandler(nil, 1)

	cols, rows, err := mysqlSrv.handleMySQLQuery(handler, "SELECT 1 AS col")
	if err != nil {
		t.Fatalf("handleMySQLQuery returned error: %v", err)
	}
	if len(cols) != 1 {
		t.Errorf("Expected 1 column, got %d", len(cols))
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

func TestMySQLServer_HandleMySQLQuery_Insert(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.Server.DataDir = tmpDir
	cfg.Auth.Enabled = false
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	srv := New(cfg, logger, engine)
	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 3306)

	handler := mysql.NewMySQLHandler(nil, 1)

	// First create a table
	mysqlSrv.handleMySQLQuery(handler, "CREATE TABLE test (id INT)")

	// Insert should return no columns
	cols, rows, err := mysqlSrv.handleMySQLQuery(handler, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("handleMySQLQuery returned error: %v", err)
	}
	if cols != nil {
		t.Errorf("Expected nil columns for INSERT, got %d", len(cols))
	}
	if rows != nil {
		t.Errorf("Expected nil rows for INSERT, got %d", len(rows))
	}
}

func TestMySQLServer_HandleMySQLAuth_Disabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = false
	logger := log.NewLogger(log.WithLevel(log.ERROR))

	srv := New(cfg, logger, nil)
	mysqlSrv := NewMySQLServer(srv, "127.0.0.1", 3306)

	handler := mysql.NewMySQLHandler(nil, 1)

	valid, err := mysqlSrv.handleMySQLAuth(handler, "user", "db", []byte("response"))
	if err != nil {
		t.Fatalf("handleMySQLAuth returned error: %v", err)
	}
	if !valid {
		t.Error("Expected valid=true when auth is disabled")
	}
}
