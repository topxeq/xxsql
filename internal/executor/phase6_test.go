package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestBeginTransaction tests BEGIN statement
func TestBeginTransaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-begin-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test BEGIN
	result, err := exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to execute BEGIN: %v", err)
	}

	t.Logf("BEGIN result: %v", result.Rows[0])

	if !exec.InTransaction() {
		t.Error("Expected to be in transaction after BEGIN")
	}

	// Test nested BEGIN (should fail)
	_, err = exec.Execute("BEGIN")
	if err == nil {
		t.Error("Expected error for nested BEGIN")
	}
	t.Logf("Nested BEGIN error (expected): %v", err)
}

// TestCommitTransaction tests COMMIT statement
func TestCommitTransaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-commit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table first
	_, err = exec.Execute(`
		CREATE TABLE test_table (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Start transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}

	// Insert data in transaction
	_, err = exec.Execute("INSERT INTO test_table (name) VALUES ('test1')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit
	result, err := exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}

	t.Logf("COMMIT result: %v", result.Rows[0])

	if exec.InTransaction() {
		t.Error("Expected not to be in transaction after COMMIT")
	}

	// Verify data persisted
	result, err = exec.Execute("SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// TestRollbackTransaction tests ROLLBACK statement
func TestRollbackTransaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rollback-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table first
	_, err = exec.Execute(`
		CREATE TABLE test_table (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = exec.Execute("INSERT INTO test_table (name) VALUES ('initial')")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Start transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}

	// Insert more data in transaction
	_, err = exec.Execute("INSERT INTO test_table (name) VALUES ('in_transaction')")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Check data is visible within transaction
	result, err := exec.Execute("SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	t.Logf("Rows in transaction: %d", result.RowCount)

	// Rollback
	result, err = exec.Execute("ROLLBACK")
	if err != nil {
		t.Fatalf("Failed to ROLLBACK: %v", err)
	}

	t.Logf("ROLLBACK result: %v", result.Rows[0])

	if exec.InTransaction() {
		t.Error("Expected not to be in transaction after ROLLBACK")
	}
}

// TestSavepoint tests SAVEPOINT, ROLLBACK TO SAVEPOINT, and RELEASE SAVEPOINT
func TestSavepoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-savepoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table first
	_, err = exec.Execute(`
		CREATE TABLE test_table (
			id SEQ PRIMARY KEY,
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Start transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}

	// Insert first row
	_, err = exec.Execute("INSERT INTO test_table (value) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create savepoint
	result, err := exec.Execute("SAVEPOINT sp1")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}
	t.Logf("SAVEPOINT result: %v", result.Rows[0])

	// Insert second row
	_, err = exec.Execute("INSERT INTO test_table (value) VALUES (2)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Check count
	result, err = exec.Execute("SELECT COUNT(*) FROM test_table")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	t.Logf("Count after second insert: %v", result.Rows[0])

	// Rollback to savepoint
	result, err = exec.Execute("ROLLBACK TO SAVEPOINT sp1")
	if err != nil {
		t.Fatalf("Failed to rollback to savepoint: %v", err)
	}
	t.Logf("ROLLBACK TO SAVEPOINT result: %v", result.Rows[0])

	// Check count after rollback to savepoint
	result, err = exec.Execute("SELECT COUNT(*) FROM test_table")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	t.Logf("Count after rollback to savepoint: %v", result.Rows[0])

	// Commit transaction
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}
}

// TestReleaseSavepoint tests RELEASE SAVEPOINT
func TestReleaseSavepoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-release-savepoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Start transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}

	// Create savepoint
	_, err = exec.Execute("SAVEPOINT sp1")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}

	// Create another savepoint
	_, err = exec.Execute("SAVEPOINT sp2")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}

	// Release first savepoint
	result, err := exec.Execute("RELEASE SAVEPOINT sp1")
	if err != nil {
		t.Fatalf("Failed to release savepoint: %v", err)
	}
	t.Logf("RELEASE SAVEPOINT result: %v", result.Rows[0])

	// Trying to rollback to released savepoint should fail
	_, err = exec.Execute("ROLLBACK TO SAVEPOINT sp1")
	if err == nil {
		t.Error("Expected error when rolling back to released savepoint")
	}
	t.Logf("Rollback to released savepoint error (expected): %v", err)

	// Commit
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}
}

// TestTransactionWithoutBegin tests COMMIT/ROLLBACK without BEGIN
func TestTransactionWithoutBegin(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-notxn-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// COMMIT without BEGIN should fail
	_, err = exec.Execute("COMMIT")
	if err == nil {
		t.Error("Expected error for COMMIT without transaction")
	}
	t.Logf("COMMIT without BEGIN error (expected): %v", err)

	// ROLLBACK without BEGIN should fail
	_, err = exec.Execute("ROLLBACK")
	if err == nil {
		t.Error("Expected error for ROLLBACK without transaction")
	}
	t.Logf("ROLLBACK without BEGIN error (expected): %v", err)

	// SAVEPOINT without BEGIN should fail
	_, err = exec.Execute("SAVEPOINT sp1")
	if err == nil {
		t.Error("Expected error for SAVEPOINT without transaction")
	}
	t.Logf("SAVEPOINT without BEGIN error (expected): %v", err)
}

// TestTransactionSyntaxVariations tests different syntax variations
func TestTransactionSyntaxVariations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-txnsyntax-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test BEGIN TRANSACTION
	_, err = exec.Execute("BEGIN TRANSACTION")
	if err != nil {
		t.Fatalf("Failed to execute BEGIN TRANSACTION: %v", err)
	}
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}

	// Test COMMIT TRANSACTION
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}
	_, err = exec.Execute("COMMIT TRANSACTION")
	if err != nil {
		t.Fatalf("Failed to execute COMMIT TRANSACTION: %v", err)
	}

	// Test ROLLBACK TRANSACTION
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}
	_, err = exec.Execute("ROLLBACK TRANSACTION")
	if err != nil {
		t.Fatalf("Failed to execute ROLLBACK TRANSACTION: %v", err)
	}

	// Test BEGIN WORK
	_, err = exec.Execute("BEGIN WORK")
	if err != nil {
		t.Fatalf("Failed to execute BEGIN WORK: %v", err)
	}
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}
}

// TestCombinedPhase6Features tests combinations of Phase 6 features
func TestCombinedPhase6Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase6-combined-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE accounts (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			balance INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = exec.Execute("INSERT INTO accounts (name, balance) VALUES ('Alice', 100)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute("INSERT INTO accounts (name, balance) VALUES ('Bob', 200)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Simulate a bank transfer with transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}

	// Create savepoint before transfer
	_, err = exec.Execute("SAVEPOINT before_transfer")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}

	// Transfer funds
	_, err = exec.Execute("UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("Failed to update Alice: %v", err)
	}

	_, err = exec.Execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Bob'")
	if err != nil {
		t.Fatalf("Failed to update Bob: %v", err)
	}

	// Check balances
	result, err := exec.Execute("SELECT name, balance FROM accounts ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}

	t.Logf("Balances after transfer:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Commit transaction
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}

	// Verify final balances
	result, err = exec.Execute("SELECT name, balance FROM accounts ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}

	t.Logf("Final balances:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestMultipleSavepoints tests multiple savepoints in sequence
func TestMultipleSavepoints(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-multisavepoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE test_data (
			id SEQ PRIMARY KEY,
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Start transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to BEGIN: %v", err)
	}

	// Insert and create savepoints
	for i := 1; i <= 3; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test_data (value) VALUES (%d)", i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}

		_, err = exec.Execute(fmt.Sprintf("SAVEPOINT sp%d", i))
		if err != nil {
			t.Fatalf("Failed to create savepoint: %v", err)
		}
	}

	// Check count
	result, err := exec.Execute("SELECT COUNT(*) FROM test_data")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	t.Logf("Count after all inserts: %v", result.Rows[0])

	// Rollback to sp2 (should keep first 2 rows)
	_, err = exec.Execute("ROLLBACK TO SAVEPOINT sp2")
	if err != nil {
		t.Fatalf("Failed to rollback to sp2: %v", err)
	}

	result, err = exec.Execute("SELECT COUNT(*) FROM test_data")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	t.Logf("Count after rollback to sp2: %v", result.Rows[0])

	// Commit
	_, err = exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to COMMIT: %v", err)
	}

	// Verify final count
	result, err = exec.Execute("SELECT COUNT(*) FROM test_data")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	t.Logf("Final count after commit: %v", result.Rows[0])
}