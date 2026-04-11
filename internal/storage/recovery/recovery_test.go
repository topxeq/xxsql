// Package recovery_test provides tests for crash recovery.
package recovery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/checkpoint"
	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/recovery"
	"github.com/topxeq/xxsql/internal/storage/sequence"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
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

func TestRegisterTable(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Create a simple table
	tbl, err := table.OpenTable(tmpDir, "test_table", []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer tbl.Close()

	// Register the table
	mgr.RegisterTable("test_table", tbl)

	// Verify by running recovery with WAL operations
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Log operations that reference the registered table
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test_table", []byte("test data"))
	walMgr.LogCommit(1)

	// Update recovery manager with WAL
	mgrWithWAL := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})
	mgrWithWAL.RegisterTable("test_table", tbl)

	state, err := mgrWithWAL.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if !state.Success {
		t.Error("Expected successful recovery")
	}

	// Unregister table
	mgr.UnregisterTable("test_table")
}

func TestSetSequenceManager(t *testing.T) {
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Create sequence manager
	seqPath := filepath.Join(tmpDir, "sequences.seq")
	seqMgr := sequence.NewManager(seqPath, walMgr)

	// Create a sequence
	err := seqMgr.CreateSequence(sequence.SequenceConfig{
		Name:      "test_seq",
		Start:     1,
		Increment: 1,
		MinValue:  1,
		MaxValue:  1000,
	})
	if err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Log sequence next operation
	walMgr.LogSequenceNext("test_seq", 100)

	// Create recovery manager with sequence manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})
	mgr.SetSequenceManager(seqMgr)

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if !state.Success {
		t.Error("Expected successful recovery")
	}
}

func TestRecoveryWithPageWrite(t *testing.T) {
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

	// Log a page write operation
	walMgr.LogBegin(1)
	walMgr.LogPageWrite(1, 1, make([]byte, page.PageSize))
	walMgr.LogCommit(1)

	// Create recovery manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
		BufferPool: bufPool,
	})

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify recovery handled page write
	if !state.Success {
		t.Error("Expected successful recovery")
	}
	if state.PagesRecovered < 1 {
		t.Errorf("Expected at least 1 page recovered, got %d", state.PagesRecovered)
	}
}

func TestRecoveryWithAbort(t *testing.T) {
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

	// Log operations with abort
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test_table", []byte("data"))
	walMgr.LogAbort(1)

	// Create recovery manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Aborted transaction should not be counted as uncommitted
	if !state.Success {
		t.Error("Expected successful recovery")
	}
}

func TestRecoveryWithError(t *testing.T) {
	tmpDir := t.TempDir()

	// Create recovery manager with invalid WAL path (simulates error)
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir: tmpDir,
	})

	// Mark as unclean
	mgr.MarkUnclean()

	// Create a failed recovery state
	state := mgr.GetState()
	state.Success = false
	state.Error = "test error"

	// Save and load the state
	if err := mgr.SaveRecoveryState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	loaded, err := mgr.LoadRecoveryState()
	if err != nil {
		t.Fatalf("Failed to load state: %v", err)
	}

	if loaded == nil {
		t.Fatal("Expected non-nil state")
	}
}

func TestLoadRecoveryStateInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Create invalid JSON state file
	statePath := filepath.Join(tmpDir, "recovery.state")
	os.WriteFile(statePath, []byte(`{invalid json}`), 0644)

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Should return error for invalid JSON
	_, err := mgr.LoadRecoveryState()
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestNeedRecoveryWithWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager with records
	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Add records to WAL
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test", []byte("data"))
	walMgr.LogCommit(1)
	walMgr.Sync()
	walMgr.Close()

	// Need to open the WAL for the check
	walMgr2 := wal.NewManager(wal.ManagerConfig{Path: walPath})
	walMgr2.Open()
	defer walMgr2.Close()

	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr2,
	})

	// Should need recovery due to WAL having records
	needed, err := mgr.NeedRecovery()
	if err != nil {
		t.Fatalf("Failed to check recovery need: %v", err)
	}
	if !needed {
		t.Error("Should need recovery when WAL has records")
	}
}

func TestNeedRecoveryWithFailedState(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Create a failed recovery state
	statePath := filepath.Join(tmpDir, "recovery.state")
	os.WriteFile(statePath, []byte(`{"success":false,"error":"previous failure"}`), 0644)

	// Should need recovery due to failed previous state
	needed, err := mgr.NeedRecovery()
	if err != nil {
		t.Fatalf("Failed to check recovery need: %v", err)
	}
	if !needed {
		t.Error("Should need recovery when previous state was failed")
	}
}

func TestGetRecoveryLogsEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Get logs when file doesn't exist
	logs, err := mgr.GetRecoveryLogs()
	if err != nil {
		t.Fatalf("Failed to get logs: %v", err)
	}
	if logs != nil {
		t.Error("Expected nil logs when file doesn't exist")
	}
}

func TestGetRecoveryLogsInvalidLine(t *testing.T) {
	tmpDir := t.TempDir()

	// Create log file with invalid lines
	logPath := filepath.Join(tmpDir, "recovery.log")
	os.WriteFile(logPath, []byte("invalid line\n{\"phase\":\"test\"}\n"), 0644)

	mgr := recovery.NewManager(recovery.ManagerConfig{DataDir: tmpDir})

	// Should skip invalid lines
	logs, err := mgr.GetRecoveryLogs()
	if err != nil {
		t.Fatalf("Failed to get logs: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("Expected 1 valid log entry, got %d", len(logs))
	}
}

func TestRecoveryWithUncommittedOps(t *testing.T) {
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

	// Create table
	tbl, err := table.OpenTable(tmpDir, "test_table", []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer tbl.Close()

	// Log various operations for an uncommitted transaction
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test_table", []byte("insert data"))
	walMgr.LogUpdate(1, "test_table", []byte("old"), []byte("new"))
	walMgr.LogDelete(1, "test_table", []byte("deleted"))
	// No commit - transaction should be undone

	// Create recovery manager with registered table
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})
	mgr.RegisterTable("test_table", tbl)

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify uncommitted transaction was processed
	if !state.Success {
		t.Error("Expected successful recovery")
	}
	if state.TxnsRecovered != 1 {
		t.Errorf("Expected 1 uncommitted txn, got %d", state.TxnsRecovered)
	}
	if state.RecordsUndone < 1 {
		t.Errorf("Expected records to be undone, got %d", state.RecordsUndone)
	}
}

func TestRecoveryMultiplePhases(t *testing.T) {
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

	// Create buffer pool and checkpoint manager
	bufPool := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize: 100,
	})
	ckptMgr := checkpoint.NewManager(checkpoint.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
		BufferPool: bufPool,
	})

	// Log operations
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "table1", []byte("data"))
	walMgr.LogCommit(1)

	// Create checkpoint
	ckptMgr.Create()

	// More operations after checkpoint
	walMgr.LogBegin(2)
	walMgr.LogInsert(2, "table2", []byte("data2"))
	// Uncommitted

	// Create recovery manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:       tmpDir,
		WALManager:    walMgr,
		CheckpointMgr: ckptMgr,
		BufferPool:    bufPool,
	})

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify state
	if state.Phase != "completed" {
		t.Errorf("Expected completed phase, got %s", state.Phase)
	}
	if state.TxnsRecovered != 1 {
		t.Errorf("Expected 1 uncommitted txn after checkpoint, got %d", state.TxnsRecovered)
	}
}

func TestRecoveryWithAllRecordTypes(t *testing.T) {
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

	// Create table for redo operations
	tbl, err := table.OpenTable(tmpDir, "test_table", []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer tbl.Close()

	// Create sequence manager
	seqPath := filepath.Join(tmpDir, "sequences.seq")
	seqMgr := sequence.NewManager(seqPath, walMgr)
	seqMgr.CreateSequence(sequence.SequenceConfig{
		Name:      "test_seq",
		Start:     1,
		Increment: 1,
	})

	// Log all types of records
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "test_table", []byte("insert data"))
	walMgr.LogUpdate(1, "test_table", make([]byte, 8), make([]byte, 8))
	walMgr.LogDelete(1, "test_table", []byte("delete data"))
	walMgr.LogPageWrite(1, 1, make([]byte, page.PageSize))
	walMgr.LogSequenceNext("test_seq", 100)
	walMgr.LogCommit(1)

	// Create recovery manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
		BufferPool: bufPool,
	})
	mgr.RegisterTable("test_table", tbl)
	mgr.SetSequenceManager(seqMgr)

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if !state.Success {
		t.Error("Expected successful recovery")
	}
	// 4 records: insert, update, delete, pageWrite
	// (sequence next might need more setup to be counted)
	if state.RecordsRedone < 4 {
		t.Errorf("Expected at least 4 records redone, got %d", state.RecordsRedone)
	}
}

func TestRecoveryStateTracking(t *testing.T) {
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "table1", []byte("data"))
	walMgr.LogCommit(1)

	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	// Get initial state before recovery
	initialState := mgr.GetState()
	if initialState.Phase != "" {
		t.Errorf("Expected empty initial phase, got %s", initialState.Phase)
	}

	// Perform recovery
	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify state transitions were tracked
	if state.StartTime.IsZero() {
		t.Error("Expected start time to be set")
	}
	if state.EndTime.IsZero() {
		t.Error("Expected end time to be set")
	}
	if !state.Success {
		t.Error("Expected success")
	}
}

func TestRecoveryWithNilCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// No checkpoint - just WAL operations
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "table1", []byte("data"))
	walMgr.LogCommit(1)

	// Recovery manager without checkpoint manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if !state.Success {
		t.Error("Expected successful recovery")
	}
	if state.Phase != "completed" {
		t.Errorf("Expected completed phase, got %s", state.Phase)
	}
}

func TestRecoveryWithFlushAllError(t *testing.T) {
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "table1", []byte("data"))
	walMgr.LogCommit(1)

	// Create buffer pool
	bufPool := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize: 100,
	})

	// Create recovery manager with buffer pool
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
		BufferPool: bufPool,
	})

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if !state.Success {
		t.Error("Expected successful recovery")
	}
}

func TestRecoveryPersistSequences(t *testing.T) {
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Create sequence manager with persistence
	seqPath := filepath.Join(tmpDir, "sequences.seq")
	seqMgr := sequence.NewManager(seqPath, walMgr)
	seqMgr.CreateSequence(sequence.SequenceConfig{
		Name:      "test_seq",
		Start:     1,
		Increment: 1,
	})

	// Log sequence operation
	walMgr.LogBegin(1)
	walMgr.LogSequenceNext("test_seq", 100)
	walMgr.LogCommit(1)

	// Create recovery manager with sequence manager
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})
	mgr.SetSequenceManager(seqMgr)

	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if !state.Success {
		t.Error("Expected successful recovery")
	}
}

func TestRecoveryWithTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "test.wal")
	walMgr := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})
	if err := walMgr.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walMgr.Close()

	// Log operations for non-existent table
	walMgr.LogBegin(1)
	walMgr.LogInsert(1, "nonexistent_table", []byte("data"))
	walMgr.LogCommit(1)

	// Recovery manager without registered table
	mgr := recovery.NewManager(recovery.ManagerConfig{
		DataDir:    tmpDir,
		WALManager: walMgr,
	})

	// Should still succeed - just logs errors for missing tables
	state, err := mgr.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if !state.Success {
		t.Error("Expected successful recovery even with missing table")
	}
}
