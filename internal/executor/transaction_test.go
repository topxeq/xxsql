package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestTransactionIsolationLevels(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-tx-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	// Create a test table
	_, err = exec.Execute("CREATE TABLE test_tx (id INT PRIMARY KEY, value VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test 1: BEGIN (default DEFERRED)
	result, err := exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	t.Logf("BEGIN result: %v", result.Rows)

	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Test 2: BEGIN DEFERRED
	result, err = exec.Execute("BEGIN DEFERRED")
	if err != nil {
		t.Fatalf("BEGIN DEFERRED failed: %v", err)
	}
	t.Logf("BEGIN DEFERRED result: %v", result.Rows)

	_, err = exec.Execute("INSERT INTO test_tx (id, value) VALUES (1, 'test1')")
	if err != nil {
		t.Fatalf("INSERT in transaction failed: %v", err)
	}

	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Test 3: BEGIN IMMEDIATE
	result, err = exec.Execute("BEGIN IMMEDIATE")
	if err != nil {
		t.Fatalf("BEGIN IMMEDIATE failed: %v", err)
	}
	t.Logf("BEGIN IMMEDIATE result: %v", result.Rows)

	_, err = exec.Execute("INSERT INTO test_tx (id, value) VALUES (2, 'test2')")
	if err != nil {
		t.Fatalf("INSERT in IMMEDIATE transaction failed: %v", err)
	}

	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Test 4: BEGIN EXCLUSIVE
	result, err = exec.Execute("BEGIN EXCLUSIVE")
	if err != nil {
		t.Fatalf("BEGIN EXCLUSIVE failed: %v", err)
	}
	t.Logf("BEGIN EXCLUSIVE result: %v", result.Rows)

	_, err = exec.Execute("INSERT INTO test_tx (id, value) VALUES (3, 'test3')")
	if err != nil {
		t.Fatalf("INSERT in EXCLUSIVE transaction failed: %v", err)
	}

	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Test 5: BEGIN IMMEDIATE TRANSACTION
	result, err = exec.Execute("BEGIN IMMEDIATE TRANSACTION")
	if err != nil {
		t.Fatalf("BEGIN IMMEDIATE TRANSACTION failed: %v", err)
	}
	t.Logf("BEGIN IMMEDIATE TRANSACTION result: %v", result.Rows)

	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify all inserts
	result, err = exec.Execute("SELECT * FROM test_tx ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
	t.Logf("Final data: %v", result.Rows)
}

func TestTransactionModePersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-tx-mode-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	// Start EXCLUSIVE transaction
	_, err = exec.Execute("BEGIN EXCLUSIVE")
	if err != nil {
		t.Fatalf("BEGIN EXCLUSIVE failed: %v", err)
	}

	// Check that the mode is set
	if exec.txMode != "EXCLUSIVE" {
		t.Errorf("Expected txMode 'EXCLUSIVE', got '%s'", exec.txMode)
	}

	// Try to start another transaction (should fail)
	_, err = exec.Execute("BEGIN")
	if err == nil {
		t.Error("Expected error for nested transaction")
	}
	t.Logf("Nested transaction error (expected): %v", err)

	// Commit and check mode is cleared
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Start a new DEFERRED transaction
	_, err = exec.Execute("BEGIN DEFERRED")
	if err != nil {
		t.Fatalf("BEGIN DEFERRED failed: %v", err)
	}

	if exec.txMode != "DEFERRED" {
		t.Errorf("Expected txMode 'DEFERRED', got '%s'", exec.txMode)
	}

	_, err = exec.Execute("ROLLBACK")
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
}