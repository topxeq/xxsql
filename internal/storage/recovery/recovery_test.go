// Package recovery_test provides tests for crash recovery.
package recovery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/checkpoint"
	"github.com/topxeq/xxsql/internal/storage/recovery"
	"github.com/topxeq/xxsql/internal/storage/wal"
)

func TestRecoveryManagerNew(t *testing.T) {
	tmpDir := t.TempDir()

	config := recovery.ManagerConfig{
		DataDir: tmpDir,
	}

	mgr := recovery.NewManager(config)
	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
}

func TestRecoveryState(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Get initial state
	state := mgr.GetState()
	if state.Phase != "" {
		t.Errorf("Expected empty phase, got %s", state.Phase)
	}

	// Save state
	if err := mgr.SaveRecoveryState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	// Load state
	loaded, err := mgr.LoadRecoveryState()
	if err != nil {
		t.Fatalf("Failed to load state: %v", err)
	}

	if loaded == nil {
		t.Error("Expected non-nil state")
	}
}

func TestNeedRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Initially no recovery needed
	needed, err := mgr.NeedRecovery()
	if err != nil {
		t.Fatalf("Failed to check recovery need: %v", err)
	}
	if needed {
		t.Error("Should not need recovery initially")
	}

	// Mark unclean
	if err := mgr.MarkUnclean(); err != nil {
		t.Fatalf("Failed to mark unclean: %v", err)
	}

	// Now should need recovery
	needed, err = mgr.NeedRecovery()
	if err != nil {
		t.Fatalf("Failed to check recovery need: %v", err)
	}
	if !needed {
		t.Error("Should need recovery after unclean mark")
	}

	// Mark clean
	if err := mgr.MarkClean(); err != nil {
		t.Fatalf("Failed to mark clean: %v", err)
	}

	// No longer need recovery
	needed, err = mgr.NeedRecovery()
	if err != nil {
		t.Fatalf("Failed to check recovery need: %v", err)
	}
	if needed {
		t.Error("Should not need recovery after clean mark")
	}
}

func TestRecoveryLog(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Log recovery events
	if err := mgr.LogRecovery("analysis", "Starting analysis phase", true); err != nil {
		t.Fatalf("Failed to log: %v", err)
	}

	if err := mgr.LogRecovery("redo", "Redoing 10 records", true); err != nil {
		t.Fatalf("Failed to log: %v", err)
	}

	if err := mgr.LogRecovery("undo", "Undoing 5 transactions", true); err != nil {
		t.Fatalf("Failed to log: %v", err)
	}

	// Load logs
	logs, err := mgr.GetRecoveryLogs()
	if err != nil {
		t.Fatalf("Failed to get logs: %v", err)
	}

	if len(logs) != 3 {
		t.Errorf("Expected 3 logs, got %d", len(logs))
	}

	// Verify logs
	if logs[0].Phase != "analysis" {
		t.Errorf("Expected analysis phase, got %s", logs[0].Phase)
	}
	if logs[1].Phase != "redo" {
		t.Errorf("Expected redo phase, got %s", logs[1].Phase)
	}
	if logs[2].Phase != "undo" {
		t.Errorf("Expected undo phase, got %s", logs[2].Phase)
	}
}

func TestRecoveryWithWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Log some operations
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test_table", []byte("test data"))
	walMgr.LogCommit(1)

	walMgr.LogBegin(2)
	walMgr.LogInsert(2, "test_table", []byte("uncommitted data"))
	// No commit for txn 2 - should be rolled back

	// Create recovery manager
	recoveryMgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	// Perform recovery
	state, err := recoveryMgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify state
	if !state.Success {
		t.Error("Expected successful recovery")
	}
	if state.RecordsRead < 3 {
		t.Errorf("Expected at least 3 records read, got %d", state.RecordsRead)
	}
}

func TestRecoveryWithCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Create buffer pool
	bufPool := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize: 100,
	})

	// Create checkpoint manager
	ckptMgr := checkpoint.NewManager(checkpoint.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
		BufferPool: bufPool,
	})

	// Log some operations before checkpoint
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "table1", []byte("data1"))
	walMgr.LogCommit(1)

	// Create checkpoint
	ckpt, err := ckptMgr.Create()
	if err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}
	if ckpt == nil {
		t.Fatal("Expected non-nil checkpoint")
	}

	// Log more operations after checkpoint
	walMgr.LogBegin(2)
	walMgr.LogInsert(2, "table1", []byte("data2"))
	walMgr.LogCommit(2)

	// Create recovery manager
	recoveryMgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:       tmpDir,
		WALManager:    walMgr,
		CheckpointMgr: ckptMgr,
		BufferPool:    bufPool,
	})

	// Perform recovery
	state, err := recoveryMgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify state
	if !state.Success {
		t.Error("Expected successful recovery")
	}

	// Recovery should start from checkpoint LSN
	if state.RecordsRead < 2 {
		t.Errorf("Expected at least 2 records after checkpoint, got %d", state.RecordsRead)
	}
}

func TestRecoveryUncommittedTransaction(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Log operations with uncommitted transaction
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test_table", []byte("committed"))
	walMgr.LogCommit(1)

	walMgr.LogBegin(2)
	walMgr.LogInsert(2, "test_table", []byte("uncommitted"))
	// Transaction 2 is not committed - should be undone

	// Create recovery manager
	recoveryMgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	// Perform recovery
	state, err := recoveryMgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify uncommitted transaction was detected
	if state.TxnsRecovered < 1 {
		t.Errorf("Expected at least 1 uncommitted txn, got %d", state.TxnsRecovered)
	}
	if state.RecordsUndone < 1 {
		t.Errorf("Expected at least 1 record undone, got %d", state.RecordsUndone)
	}
}

func TestRecoveryClearState(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Create a recovery state file
	statePath := filepath.Join(tmpDir, "recovery.state")
	os.WriteFile(statePath, []byte(`{"phase":"test"}`), 0644)

	// Clear it
	if err := mgr.ClearRecoveryState(); err != nil {
		t.Fatalf("Failed to clear state: %v", err)
	}

	// Verify it's gone
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Error("State file should be deleted")
	}
}

func TestRecoveryPhases(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager with some data
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Add various record types
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "table1", []byte("insert data"))
	walMgr.LogUpdate(1, "table1", []byte("old"), []byte("new"))
	walMgr.LogDelete(1, "table1", []byte("deleted"))
	walMgr.LogCommit(1)

	// Create recovery manager
	recoveryMgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	// Perform recovery and check phases
	state, err := recoveryMgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify final state
	if state.Phase != "completed" {
		t.Errorf("Expected completed phase, got %s", state.Phase)
	}
	if !state.Success {
		t.Error("Expected success")
	}
	if state.StartTime.IsZero() {
		t.Error("Expected start time")
	}
	if state.EndTime.IsZero() {
		t.Error("Expected end time")
	}
}

func TestRecoveryNoWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Recovery manager without WAL
	recoveryMgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir: tmpDir,
	})

	// Should succeed with nothing to recover
	state, err := recoveryMgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if !state.Success {
		t.Error("Expected success with no WAL")
	}
}

func TestRecoveryMultipleTransactions(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Multiple transactions with mixed commit status
	for i := uint64(1); i <= 5; i++ {
		walMgr.LogBegin(i)
		walMgr.LogInsert(i, "test", []byte("data"))

		// Commit only odd-numbered transactions
		if i%2 == 1 {
			walMgr.LogCommit(i)
		}
	}

	// Create recovery manager
	recoveryMgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	// Perform recovery
	state, err := recoveryMgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Should have 2 uncommitted transactions (2, 4)
	if state.TxnsRecovered != 2 {
		t.Errorf("Expected 2 uncommitted txns, got %d", state.TxnsRecovered)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
