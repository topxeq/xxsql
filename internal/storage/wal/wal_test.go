// Package wal_test provides tests for WAL manager.
package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/wal"
)

func TestWALBasic(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	// Open WAL
	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	// Append a record
	lsn, err := m.Append(wal.RecordTypeInsert, 1, 0, "users", []byte("data"))
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	if lsn == 0 {
		t.Error("LSN should not be 0")
	}

	// Check current LSN
	currentLSN := m.GetCurrentLSN()
	if currentLSN != lsn {
		t.Errorf("Current LSN mismatch: expected %d, got %d", lsn, currentLSN)
	}
}

func TestWALTransaction(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "wal-txn-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	txnID := uint64(1)

	// Begin transaction
	_, err = m.LogBegin(txnID)
	if err != nil {
		t.Fatalf("Failed to log begin: %v", err)
	}

	// Insert
	_, err = m.LogInsert(txnID, "users", []byte("row1"))
	if err != nil {
		t.Fatalf("Failed to log insert: %v", err)
	}

	// Commit
	_, err = m.LogCommit(txnID)
	if err != nil {
		t.Fatalf("Failed to log commit: %v", err)
	}

	// Check LSN
	if m.GetCurrentLSN() < 3 {
		t.Errorf("Expected at least 3 records, got LSN %d", m.GetCurrentLSN())
	}
}

func TestWALReplay(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "wal-replay-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write some records
	for i := 0; i < 10; i++ {
		m.LogInsert(uint64(i), "users", []byte("data"))
	}

	m.Close()

	// Reopen and replay
	m2 := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	if err := m2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer m2.Close()

	// Replay records
	count := 0
	err = m2.Replay(func(r *wal.Record) error {
		count++
		if r.Type != wal.RecordTypeInsert {
			t.Errorf("Expected INSERT record, got %s", r.Type)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected 10 records, got %d", count)
	}
}

func TestWALTruncate(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "wal-truncate-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	// Write records
	for i := 0; i < 5; i++ {
		m.LogInsert(uint64(i), "users", []byte("data"))
	}

	// Truncate
	if err := m.Truncate(); err != nil {
		t.Fatalf("Failed to truncate WAL: %v", err)
	}

	// Check LSN
	if m.GetCurrentLSN() != 0 {
		t.Errorf("Expected LSN 0 after truncate, got %d", m.GetCurrentLSN())
	}

	// Check size
	size, err := m.Size()
	if err != nil {
		t.Fatalf("Failed to get WAL size: %v", err)
	}

	if size != 0 {
		t.Errorf("Expected WAL size 0, got %d", size)
	}
}

func TestWALRecordTypes(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "wal-types-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	// Test various record types
	tests := []struct {
		name   string
		logFn  func() (wal.LSN, error)
	}{
		{"Begin", func() (wal.LSN, error) { return m.LogBegin(1) }},
		{"Commit", func() (wal.LSN, error) { return m.LogCommit(1) }},
		{"Abort", func() (wal.LSN, error) { return m.LogAbort(1) }},
		{"Insert", func() (wal.LSN, error) { return m.LogInsert(1, "t", []byte("d")) }},
		{"Delete", func() (wal.LSN, error) { return m.LogDelete(1, "t", []byte("d")) }},
		{"CreateTable", func() (wal.LSN, error) { return m.LogCreateTable(1, "t", []byte("s")) }},
		{"DropTable", func() (wal.LSN, error) { return m.LogDropTable(1, "t") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsn, err := tc.logFn()
			if err != nil {
				t.Fatalf("Failed to log %s: %v", tc.name, err)
			}
			if lsn == 0 {
				t.Errorf("LSN should not be 0 for %s", tc.name)
			}
		})
	}
}

func TestWALSync(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "wal-sync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path:         walPath,
		SyncInterval: 0, // Disable auto sync
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	// Write records
	m.LogInsert(1, "users", []byte("data"))

	// Sync
	if err := m.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Check flushed LSN
	if m.GetFlushedLSN() == 0 {
		t.Error("Flushed LSN should not be 0 after sync")
	}
}
