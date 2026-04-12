package server

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/protocol"
)

func TestServerSetConfigPath(t *testing.T) {
	srv := New(config.DefaultConfig(), log.NewLogger(log.WithLevel(log.INFO)), nil)
	srv.SetConfigPath("/tmp/test-config.json")

	if srv.configPath != "/tmp/test-config.json" {
		t.Fatalf("configPath: got %q", srv.configPath)
	}
}

func TestServerConnectDisconnectStats(t *testing.T) {
	srv := New(config.DefaultConfig(), log.NewLogger(log.WithLevel(log.INFO)), nil)

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	h := protocol.NewConnectionHandler(c1)
	srv.onConnect(h)

	stats := srv.GetStats()
	if stats.TotalConnections != 1 {
		t.Fatalf("TotalConnections after connect: got %d, want 1", stats.TotalConnections)
	}
	if stats.ActiveConnections != 1 {
		t.Fatalf("ActiveConnections after connect: got %d, want 1", stats.ActiveConnections)
	}

	srv.onDisconnect(h)

	stats = srv.GetStats()
	if stats.ActiveConnections != 0 {
		t.Fatalf("ActiveConnections after disconnect: got %d, want 0", stats.ActiveConnections)
	}
}

func TestServerOnQueryWithoutExecutor(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = false

	srv := New(cfg, log.NewLogger(log.WithLevel(log.INFO)), nil)

	resp, err := srv.onQuery(nil, &protocol.QueryRequest{SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("onQuery returned error: %v", err)
	}
	if resp == nil || resp.Status != protocol.StatusError {
		t.Fatalf("onQuery response status: got %+v", resp)
	}
	if !strings.Contains(resp.Message, "Storage engine not initialized") {
		t.Fatalf("onQuery response message: got %q", resp.Message)
	}

	stats := srv.GetStats()
	if stats.TotalQueries != 1 {
		t.Fatalf("TotalQueries: got %d, want 1", stats.TotalQueries)
	}
	if stats.LastQueryTime.IsZero() {
		t.Fatal("LastQueryTime should be set")
	}
	if time.Since(stats.LastQueryTime) > time.Second {
		t.Fatalf("LastQueryTime too old: %v", stats.LastQueryTime)
	}
}

func TestGenerateRandomPasswordLength(t *testing.T) {
	pass := generateRandomPassword(24)
	if len(pass) != 24 {
		t.Fatalf("password length: got %d, want 24", len(pass))
	}
}
