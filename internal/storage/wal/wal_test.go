// Package wal_test provides tests for WAL manager.
package wal_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
		name  string
		logFn func() (wal.LSN, error)
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

func TestWALRecordTypeString(t *testing.T) {
	tests := []struct {
		rt       wal.RecordType
		expected string
	}{
		{wal.RecordTypeBegin, "BEGIN"},
		{wal.RecordTypeCommit, "COMMIT"},
		{wal.RecordTypeAbort, "ABORT"},
		{wal.RecordTypeInsert, "INSERT"},
		{wal.RecordTypeUpdate, "UPDATE"},
		{wal.RecordTypeDelete, "DELETE"},
		{wal.RecordTypeCreateTable, "CREATE_TABLE"},
		{wal.RecordTypeDropTable, "DROP_TABLE"},
		{wal.RecordType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.rt.String()
			if result != tt.expected {
				t.Errorf("RecordType.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestWALRecordMarshal(t *testing.T) {
	r := &wal.Record{
		Type:      wal.RecordTypeInsert,
		LSN:       1,
		TxnID:     100,
		PageID:    5,
		TableName: "users",
		Data:      []byte("testdata"),
	}

	data := r.Marshal()
	if len(data) == 0 {
		t.Error("Marshal should return non-empty data")
	}

	r2, n, err := wal.UnmarshalRecord(data)
	if err != nil {
		t.Fatalf("UnmarshalRecord error: %v", err)
	}
	if n != len(data) {
		t.Errorf("UnmarshalRecord bytes read: got %d, want %d", n, len(data))
	}
	if r2.Type != r.Type {
		t.Errorf("Type: got %v, want %v", r2.Type, r.Type)
	}
	if r2.LSN != r.LSN {
		t.Errorf("LSN: got %d, want %d", r2.LSN, r.LSN)
	}
	if r2.TxnID != r.TxnID {
		t.Errorf("TxnID: got %d, want %d", r2.TxnID, r.TxnID)
	}
	if r2.TableName != r.TableName {
		t.Errorf("TableName: got %q, want %q", r2.TableName, r.TableName)
	}
}

func TestWALUnmarshalInvalidData(t *testing.T) {
	_, _, err := wal.UnmarshalRecord([]byte{})
	if err == nil {
		t.Error("UnmarshalRecord should error for empty data")
	}

	_, _, err = wal.UnmarshalRecord([]byte{0x00})
	if err == nil {
		t.Error("UnmarshalRecord should error for short data")
	}
}

func TestWALUpdate(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-update-test-*")
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

	lsn, err := m.LogUpdate(1, "users", []byte("olddata"), []byte("newdata"))
	if err != nil {
		t.Fatalf("LogUpdate error: %v", err)
	}
	if lsn == 0 {
		t.Error("LSN should not be 0")
	}
}

func TestWALPageWrite(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-page-test-*")
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

	lsn, err := m.LogPageWrite(1, 42, []byte("pagedata"))
	if err != nil {
		t.Fatalf("LogPageWrite error: %v", err)
	}
	if lsn == 0 {
		t.Error("LSN should not be 0")
	}
}

func TestWALCheckpoint(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-checkpoint-test-*")
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

	lsn, err := m.LogCheckpoint(1, []byte("checkpointdata"))
	if err != nil {
		t.Fatalf("LogCheckpoint error: %v", err)
	}
	if lsn == 0 {
		t.Error("LSN should not be 0")
	}
}

func TestWALSequenceOps(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-seq-test-*")
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

	if err := m.LogSequenceCreate("seq1", 1); err != nil {
		t.Fatalf("LogSequenceCreate error: %v", err)
	}

	if err := m.LogSequenceNext("seq1", 2); err != nil {
		t.Fatalf("LogSequenceNext error: %v", err)
	}

	if err := m.LogSequenceDrop("seq1"); err != nil {
		t.Fatalf("LogSequenceDrop error: %v", err)
	}
}

func TestWALAppendRecord(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-append-test-*")
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

	r := &wal.Record{
		Type:      wal.RecordTypeInsert,
		TxnID:     1,
		TableName: "users",
		Data:      []byte("data"),
	}

	lsn, err := m.AppendRecord(r)
	if err != nil {
		t.Fatalf("AppendRecord error: %v", err)
	}
	if lsn == 0 {
		t.Error("LSN should not be 0")
	}
}

func TestWALNeedsCheckpoint(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-checkpoint-need-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path:    walPath,
		MaxSize: 1024 * 1024,
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	needs, err := m.NeedsCheckpoint()
	if err != nil {
		t.Fatalf("NeedsCheckpoint error: %v", err)
	}

	t.Logf("NeedsCheckpoint: %v", needs)
}

func TestWALReopen(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-reopen-test-*")
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

	for i := 0; i < 5; i++ {
		m.LogInsert(uint64(i), "users", []byte("data"))
	}

	lsnBefore := m.GetCurrentLSN()
	m.Close()

	m2 := wal.NewManager(wal.ManagerConfig{
		Path: walPath,
	})

	if err := m2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer m2.Close()

	lsnAfter := m2.GetCurrentLSN()
	if lsnAfter != lsnBefore {
		t.Errorf("LSN after reopen: got %d, want %d", lsnAfter, lsnBefore)
	}
}

func TestWALAutoSync(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-autosync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.xwal")
	m := wal.NewManager(wal.ManagerConfig{
		Path:         walPath,
		SyncInterval: 100,
	})

	if err := m.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer m.Close()

	for i := 0; i < 5; i++ {
		m.LogInsert(uint64(i), "users", []byte("data"))
	}

	time.Sleep(200 * time.Millisecond)
}

func TestWALManyRecords(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-many-test-*")
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

	for i := 0; i < 100; i++ {
		_, err := m.LogInsert(uint64(i), "users", []byte("data"))
		if err != nil {
			t.Fatalf("LogInsert error at %d: %v", i, err)
		}
	}

	if m.GetCurrentLSN() != 100 {
		t.Errorf("LSN: got %d, want 100", m.GetCurrentLSN())
	}
}
