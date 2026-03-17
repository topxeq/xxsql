package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/wal"
)

func TestNewManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := ManagerConfig{
		DataDir:     tmpDir,
		Interval:    time.Minute,
		MaxWALSize:  1024 * 1024,
		AutoEnabled: false,
	}

	m := NewManager(config)
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
}

func TestNewManager_DefaultValues(t *testing.T) {
	config := ManagerConfig{
		DataDir: os.TempDir(),
	}

	m := NewManager(config)
	if m.interval <= 0 {
		t.Error("interval should have default value")
	}
	if m.maxWALSize <= 0 {
		t.Error("maxWALSize should have default value")
	}
}

func TestManager_Create(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	ckpt, err := m.Create()
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if ckpt == nil {
		t.Fatal("checkpoint is nil")
	}
	if ckpt.ID == 0 {
		t.Error("checkpoint ID should not be zero")
	}
	if ckpt.Timestamp == 0 {
		t.Error("checkpoint timestamp should not be zero")
	}

	// Verify file was created
	metaPath := filepath.Join(tmpDir, CheckpointMetaFile)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("checkpoint metadata file was not created")
	}
}

func TestManager_GetLastCheckpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// No checkpoint yet
	ckpt := m.GetLastCheckpoint()
	if ckpt != nil {
		t.Error("expected nil checkpoint when none exists")
	}

	// Create a checkpoint
	created, _ := m.Create()

	// Get last checkpoint
	ckpt = m.GetLastCheckpoint()
	if ckpt == nil {
		t.Fatal("expected checkpoint to exist")
	}
	if ckpt.ID != created.ID {
		t.Errorf("checkpoint ID = %d, want %d", ckpt.ID, created.ID)
	}
}

func TestManager_GetLastCheckpoint_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager and checkpoint
	m1 := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})
	created, _ := m1.Create()

	// Create new manager (simulates restart)
	m2 := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	ckpt := m2.GetLastCheckpoint()
	if ckpt == nil {
		t.Fatal("expected checkpoint to persist")
	}
	if ckpt.ID != created.ID {
		t.Errorf("checkpoint ID = %d, want %d", ckpt.ID, created.ID)
	}
}

func TestManager_NeedsCheckpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
		Interval:    time.Hour,
	})

	// No checkpoint yet - should need one
	needed, err := m.NeedsCheckpoint()
	if err != nil {
		t.Fatalf("NeedsCheckpoint failed: %v", err)
	}
	if !needed {
		t.Error("should need checkpoint when none exists")
	}

	// Create checkpoint
	m.Create()

	// Just created - should not need one
	needed, err = m.NeedsCheckpoint()
	if err != nil {
		t.Fatalf("NeedsCheckpoint failed: %v", err)
	}
	if needed {
		t.Error("should not need checkpoint right after creation")
	}
}

func TestManager_GetRecoveryInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// No checkpoint
	info, err := m.GetRecoveryInfo()
	if err != nil {
		t.Fatalf("GetRecoveryInfo failed: %v", err)
	}
	if info == nil {
		t.Fatal("RecoveryInfo is nil")
	}

	// Create checkpoint
	m.Create()

	// Get info with checkpoint
	info, err = m.GetRecoveryInfo()
	if err != nil {
		t.Fatalf("GetRecoveryInfo failed: %v", err)
	}
}

func TestManager_DeleteCheckpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// Create checkpoint
	m.Create()

	// Delete checkpoint
	err = m.DeleteCheckpoint()
	if err != nil {
		t.Fatalf("DeleteCheckpoint failed: %v", err)
	}

	// Verify it's gone
	ckpt := m.GetLastCheckpoint()
	if ckpt != nil {
		t.Error("checkpoint should be deleted")
	}
}

func TestManager_Stats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: true,
		Interval:    5 * time.Minute,
	})

	// No checkpoint yet
	stats := m.Stats()
	if stats.AutoEnabled != true {
		t.Error("AutoEnabled should be true")
	}
	if stats.Interval == "" {
		t.Error("Interval should not be empty")
	}

	// Create checkpoint
	m.Create()

	// Stats with checkpoint
	stats = m.Stats()
	if stats.LastCheckpointID == 0 {
		t.Error("LastCheckpointID should not be zero")
	}
	if stats.LastCheckpointTime == "" {
		t.Error("LastCheckpointTime should not be empty")
	}
}

func TestManager_SetWALManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     os.TempDir(),
		AutoEnabled: false,
	})

	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	m.SetWALManager(walMgr)

	// Create checkpoint with WAL
	ckpt, err := m.Create()
	if err != nil {
		t.Fatalf("Create with WAL failed: %v", err)
	}
	if ckpt == nil {
		t.Fatal("checkpoint is nil")
	}
}

func TestManager_SetBufferPool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "buffer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     os.TempDir(),
		AutoEnabled: false,
	})

	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize: 10,
		PageSize: 4096,
	})

	m.SetBufferPool(bp)

	// Create checkpoint with buffer pool
	ckpt, err := m.Create()
	if err != nil {
		t.Fatalf("Create with buffer pool failed: %v", err)
	}
	if ckpt == nil {
		t.Fatal("checkpoint is nil")
	}
}

func TestManager_StartStop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: true,
		Interval:    100 * time.Millisecond,
	})

	// Start
	err = m.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait a bit
	time.Sleep(150 * time.Millisecond)

	// Stop
	m.Stop()

	// Double stop should be safe
	m.Stop()
}

func TestManager_Start_Disabled(t *testing.T) {
	m := NewManager(ManagerConfig{
		DataDir:     os.TempDir(),
		AutoEnabled: false,
	})

	// Start when disabled should do nothing
	err := m.Start()
	if err != nil {
		t.Fatalf("Start should not fail when disabled: %v", err)
	}
}

func TestManager_Recover_NoCheckpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// Recover with no checkpoint should succeed
	err = m.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
}

func TestManager_TruncateWAL(t *testing.T) {
	m := NewManager(ManagerConfig{
		DataDir:     os.TempDir(),
		AutoEnabled: false,
	})

	// Truncate with no WAL manager should succeed
	err := m.TruncateWAL()
	if err != nil {
		t.Fatalf("TruncateWAL failed: %v", err)
	}
}

func TestCheckpoint_Data(t *testing.T) {
	ckpt := &Checkpoint{
		ID:         12345,
		LSN:        100,
		Timestamp:  time.Now().UnixNano(),
		TableCount: 5,
		RowCount:   1000,
		PageCount:  50,
		ActiveTxns: []uint64{1, 2, 3},
		Metadata:   map[string]string{"key": "value"},
	}

	if ckpt.ID != 12345 {
		t.Errorf("ID = %d, want 12345", ckpt.ID)
	}
	if ckpt.TableCount != 5 {
		t.Errorf("TableCount = %d, want 5", ckpt.TableCount)
	}
	if len(ckpt.ActiveTxns) != 3 {
		t.Errorf("ActiveTxns length = %d, want 3", len(ckpt.ActiveTxns))
	}
}

func BenchmarkManager_Create(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Create()
	}
}

func BenchmarkManager_GetLastCheckpoint(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})
	m.Create()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetLastCheckpoint()
	}
}
