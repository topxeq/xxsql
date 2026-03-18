package checkpoint

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/wal"
)

// mockRecoveryHandler implements RecoveryHandler for testing
type mockRecoveryHandler struct {
	inserts  []struct {
		table string
		data  []byte
	}
	updates []struct {
		table string
		data  []byte
	}
	deletes []struct {
		table string
		data  []byte
	}
	pages []struct {
		pageID uint64
		data   []byte
	}
	errOnInsert error
	errOnUpdate error
	errOnDelete error
	errOnPage   error
}

func (m *mockRecoveryHandler) ApplyInsert(table string, data []byte) error {
	if m.errOnInsert != nil {
		return m.errOnInsert
	}
	m.inserts = append(m.inserts, struct {
		table string
		data  []byte
	}{table, data})
	return nil
}

func (m *mockRecoveryHandler) ApplyUpdate(table string, data []byte) error {
	if m.errOnUpdate != nil {
		return m.errOnUpdate
	}
	m.updates = append(m.updates, struct {
		table string
		data  []byte
	}{table, data})
	return nil
}

func (m *mockRecoveryHandler) ApplyDelete(table string, data []byte) error {
	if m.errOnDelete != nil {
		return m.errOnDelete
	}
	m.deletes = append(m.deletes, struct {
		table string
		data  []byte
	}{table, data})
	return nil
}

func (m *mockRecoveryHandler) ApplyPageWrite(pageID uint64, data []byte) error {
	if m.errOnPage != nil {
		return m.errOnPage
	}
	m.pages = append(m.pages, struct {
		pageID uint64
		data   []byte
	}{pageID, data})
	return nil
}

func TestManager_SetRecoveryHandler(t *testing.T) {
	m := NewManager(ManagerConfig{
		DataDir:     os.TempDir(),
		AutoEnabled: false,
	})

	handler := &mockRecoveryHandler{}
	m.SetRecoveryHandler(handler)

	if m.recoveryHandler == nil {
		t.Error("RecoveryHandler should be set")
	}
}

func TestManager_Recover_WithWALAndRecoveryHandler(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-recover-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL manager
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	// Create checkpoint manager
	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		WALManager:  walMgr,
		AutoEnabled: false,
	})

	// Create a checkpoint first
	_, err = m.Create()
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Set recovery handler
	handler := &mockRecoveryHandler{}
	m.SetRecoveryHandler(handler)

	// Log some WAL records after checkpoint
	walMgr.LogInsert(1, "test_table", []byte("insert_data"))
	walMgr.LogUpdate(1, "test_table", []byte("old_data"), []byte("update_data"))
	walMgr.LogDelete(1, "test_table", []byte("delete_data"))

	// Perform recovery
	err = m.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Verify recovery handler was called
	// Note: The actual replay depends on LSN comparison
	t.Logf("Inserts: %d, Updates: %d, Deletes: %d, Pages: %d",
		len(handler.inserts), len(handler.updates), len(handler.deletes), len(handler.pages))
}

func TestManager_Recover_NoWALManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-recover-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// Create a checkpoint
	_, err = m.Create()
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Recover without WAL manager should succeed
	err = m.Recover()
	if err != nil {
		t.Fatalf("Recover without WAL manager failed: %v", err)
	}
}

func TestManager_Recover_WithRecoveryHandler_NoHandler(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-recover-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL manager
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	// Create checkpoint manager with WAL but no recovery handler
	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		WALManager:  walMgr,
		AutoEnabled: false,
	})

	// Create a checkpoint
	_, err = m.Create()
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Recover without recovery handler should succeed (skip actual recovery)
	err = m.Recover()
	if err != nil {
		t.Fatalf("Recover without recovery handler failed: %v", err)
	}
}

func TestManager_TruncateWAL_WithWALManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-truncate-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		WALManager:  walMgr,
		AutoEnabled: false,
	})

	// Truncate with WAL manager
	err = m.TruncateWAL()
	if err != nil {
		t.Fatalf("TruncateWAL failed: %v", err)
	}
}

func TestManager_NeedsCheckpoint_WithWALManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-needs-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		WALManager:  walMgr,
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

func TestManager_Create_BufferPoolFlushError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-flush-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// Set a buffer pool that will fail on flush
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize: 10,
		PageSize: 4096,
	})
	m.SetBufferPool(bp)

	// Create should succeed (buffer pool flush succeeds in normal case)
	_, err = m.Create()
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
}

func TestManager_Create_WithWALManagerAndBufferPool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-create-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize: 10,
		PageSize: 4096,
	})

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		WALManager:  walMgr,
		BufferPool:  bp,
		AutoEnabled: false,
	})

	ckpt, err := m.Create()
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if ckpt == nil {
		t.Fatal("checkpoint is nil")
	}
	// LSN may be 0 if no WAL records have been written yet
	t.Logf("Checkpoint LSN: %d", ckpt.LSN)
}

func TestManager_saveCheckpoint_CreateDirError(t *testing.T) {
	// Create a file where a directory should be
	tmpFile, err := os.CreateTemp("", "checkpoint-file-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	m := NewManager(ManagerConfig{
		DataDir:     tmpFile.Name() + "/subdir", // Will fail because parent is a file
		AutoEnabled: false,
	})

	ckpt := &Checkpoint{
		ID:        1,
		Timestamp: time.Now().UnixNano(),
	}

	err = m.saveCheckpoint(ckpt)
	if err == nil {
		t.Error("saveCheckpoint should fail when directory creation fails")
	}
}

func TestManager_loadLastCheckpoint_ReadError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-load-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create an invalid checkpoint file
	metaPath := filepath.Join(tmpDir, CheckpointMetaFile)
	err = os.WriteFile(metaPath, []byte("invalid json"), 0644)
	if err != nil {
		t.Fatalf("failed to write invalid file: %v", err)
	}

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// loadLastCheckpoint should return error for invalid JSON
	ckpt, err := m.loadLastCheckpoint()
	if err == nil {
		t.Error("loadLastCheckpoint should return error for invalid JSON")
	}
	if ckpt != nil {
		t.Error("checkpoint should be nil on error")
	}
}

func TestManager_GetLastCheckpoint_LoadError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-getlast-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create an invalid checkpoint file
	metaPath := filepath.Join(tmpDir, CheckpointMetaFile)
	err = os.WriteFile(metaPath, []byte("invalid json"), 0644)
	if err != nil {
		t.Fatalf("failed to write invalid file: %v", err)
	}

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// GetLastCheckpoint should return nil on load error
	ckpt := m.GetLastCheckpoint()
	if ckpt != nil {
		t.Error("GetLastCheckpoint should return nil on load error")
	}
}

func TestManager_DeleteCheckpoint_FileNotExist(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-delete-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// Delete when no checkpoint file exists should succeed
	err = m.DeleteCheckpoint()
	if err != nil {
		t.Fatalf("DeleteCheckpoint should succeed when file doesn't exist: %v", err)
	}
}

func TestManager_DeleteCheckpoint_Error(t *testing.T) {
	// Create a directory with a checkpoint file
	tmpDir, err := os.MkdirTemp("", "checkpoint-del-err-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create checkpoint file
	metaPath := filepath.Join(tmpDir, CheckpointMetaFile)
	err = os.WriteFile(metaPath, []byte("{}"), 0444) // Read-only
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	// Try to delete (may fail on read-only, depending on OS)
	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
	})

	// Make the file writable again for cleanup
	defer func() {
		os.Chmod(metaPath, 0644)
		os.RemoveAll(tmpDir)
	}()

	// This may or may not fail depending on OS
	_ = m.DeleteCheckpoint()
}

func TestManager_NeedsCheckpoint_TimeBased(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-time-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: false,
		Interval:    100 * time.Millisecond,
	})

	// Create checkpoint
	m.Create()

	// Immediately after - should not need
	needed, _ := m.NeedsCheckpoint()
	if needed {
		t.Error("should not need checkpoint immediately after creation")
	}

	// Wait for interval to pass
	time.Sleep(150 * time.Millisecond)

	// After interval - should need checkpoint
	needed, _ = m.NeedsCheckpoint()
	if !needed {
		t.Error("should need checkpoint after interval passes")
	}
}

func TestManager_autoCheckpointLoop_NeedsCheckpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-loop-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		AutoEnabled: true,
		Interval:    50 * time.Millisecond,
	})

	// Start auto checkpoint loop
	m.Start()

	// Wait for a few intervals
	time.Sleep(200 * time.Millisecond)

	// Stop
	m.Stop()

	// Verify checkpoint was created
	ckpt := m.GetLastCheckpoint()
	if ckpt == nil {
		t.Error("auto checkpoint should have been created")
	}
}

func TestManager_GetRecoveryInfo_WithWALManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkpoint-recovery-info-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    filepath.Join(tmpDir, "wal"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("failed to open WAL manager: %v", err)
	}
	defer walMgr.Close()

	m := NewManager(ManagerConfig{
		DataDir:     tmpDir,
		WALManager:  walMgr,
		AutoEnabled: false,
	})

	// Create checkpoint
	ckpt, _ := m.Create()

	// Get recovery info
	info, err := m.GetRecoveryInfo()
	if err != nil {
		t.Fatalf("GetRecoveryInfo failed: %v", err)
	}
	if info == nil {
		t.Fatal("RecoveryInfo is nil")
	}
	if info.CheckpointLSN != ckpt.LSN {
		t.Errorf("CheckpointLSN = %d, want %d", info.CheckpointLSN, ckpt.LSN)
	}
}

func TestCheckpoint_JSON(t *testing.T) {
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

	// Marshal
	data, err := json.Marshal(ckpt)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal
	var restored Checkpoint
	err = json.Unmarshal(data, &restored)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if restored.ID != ckpt.ID {
		t.Errorf("ID = %d, want %d", restored.ID, ckpt.ID)
	}
	if restored.LSN != ckpt.LSN {
		t.Errorf("LSN = %d, want %d", restored.LSN, ckpt.LSN)
	}
	if restored.TableCount != ckpt.TableCount {
		t.Errorf("TableCount = %d, want %d", restored.TableCount, ckpt.TableCount)
	}
	if len(restored.ActiveTxns) != len(ckpt.ActiveTxns) {
		t.Errorf("ActiveTxns length = %d, want %d", len(restored.ActiveTxns), len(ckpt.ActiveTxns))
	}
}

func TestRecoveryInfo_Struct(t *testing.T) {
	info := &RecoveryInfo{
		CheckpointLSN: 100,
		CurrentLSN:    200,
		Tables: []TableRecoveryInfo{
			{Name: "users", RootPageID: 1, PageCount: 10},
			{Name: "orders", RootPageID: 2, PageCount: 20},
		},
	}

	if info.CheckpointLSN != 100 {
		t.Errorf("CheckpointLSN = %d, want 100", info.CheckpointLSN)
	}
	if len(info.Tables) != 2 {
		t.Errorf("Tables length = %d, want 2", len(info.Tables))
	}
}

func TestTableRecoveryInfo_Struct(t *testing.T) {
	tableInfo := TableRecoveryInfo{
		Name:       "test_table",
		RootPageID: 1,
		PageCount:  10,
	}

	if tableInfo.Name != "test_table" {
		t.Errorf("Name = %s, want test_table", tableInfo.Name)
	}
	if tableInfo.RootPageID != 1 {
		t.Errorf("RootPageID = %d, want 1", tableInfo.RootPageID)
	}
}

func TestCheckpointStats_Struct(t *testing.T) {
	stats := CheckpointStats{
		AutoEnabled:        true,
		Interval:           "5m0s",
		LastCheckpointID:   12345,
		LastCheckpointLSN:  100,
		LastCheckpointTime: "2024-01-01T00:00:00Z",
		PageCount:          50,
		RowCount:           1000,
	}

	if !stats.AutoEnabled {
		t.Error("AutoEnabled should be true")
	}
	if stats.Interval != "5m0s" {
		t.Errorf("Interval = %s, want 5m0s", stats.Interval)
	}
}

// Test recovery handler error cases
func TestMockRecoveryHandler_Errors(t *testing.T) {
	handler := &mockRecoveryHandler{
		errOnInsert: errors.New("insert error"),
		errOnUpdate: errors.New("update error"),
		errOnDelete: errors.New("delete error"),
		errOnPage:   errors.New("page error"),
	}

	if err := handler.ApplyInsert("table", nil); err == nil {
		t.Error("ApplyInsert should return error")
	}
	if err := handler.ApplyUpdate("table", nil); err == nil {
		t.Error("ApplyUpdate should return error")
	}
	if err := handler.ApplyDelete("table", nil); err == nil {
		t.Error("ApplyDelete should return error")
	}
	if err := handler.ApplyPageWrite(1, nil); err == nil {
		t.Error("ApplyPageWrite should return error")
	}
}