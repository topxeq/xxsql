package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestBlobFunctionality tests BLOB type operations
func TestBlobFunctionality(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-blob-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create engine and executor
	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table with BLOB column
	result, err := exec.Execute(`
		CREATE TABLE files (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			data BLOB
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	t.Logf("Created table: %s", result.Message)

	// Insert BLOB data using X'...' notation
	result, err = exec.Execute(`INSERT INTO files (name, data) VALUES ('test1', X'48656c6c6f')`)
	if err != nil {
		t.Fatalf("Failed to insert BLOB with X'...' notation: %v", err)
	}
	t.Logf("Inserted BLOB with X'...' notation: %s", result.Message)

	// Insert BLOB data using 0x notation
	result, err = exec.Execute(`INSERT INTO files (name, data) VALUES ('test2', 0xdeadbeef)`)
	if err != nil {
		t.Fatalf("Failed to insert BLOB with 0x notation: %v", err)
	}
	t.Logf("Inserted BLOB with 0x notation: %s", result.Message)

	// Insert NULL BLOB
	result, err = exec.Execute(`INSERT INTO files (name, data) VALUES ('test3', NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert NULL BLOB: %v", err)
	}
	t.Logf("Inserted NULL BLOB: %s", result.Message)

	// Select BLOB data
	result, err = exec.Execute(`SELECT * FROM files`)
	if err != nil {
		t.Fatalf("Failed to select BLOB data: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
	for i, row := range result.Rows {
		t.Logf("Row %d: %v", i, row)
	}
}

// TestBlobFunctions tests BLOB-related functions
func TestBlobFunctions(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-blob-func-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create engine and executor
	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table with BLOB column
	_, err = exec.Execute(`
		CREATE TABLE blobs (
			id SEQ PRIMARY KEY,
			data BLOB
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO blobs (data) VALUES (X'48656c6c6f20576f726c64')`) // "Hello World"
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test HEX function
	result, err := exec.Execute(`SELECT HEX(data) FROM blobs`)
	if err != nil {
		t.Fatalf("Failed to execute HEX: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("HEX: Expected 1 row, got %d", result.RowCount)
	}
	hexVal, ok := result.Rows[0][0].(string)
	if !ok {
		t.Errorf("HEX: Expected string result")
	} else {
		expected := "48656c6c6f20576f726c64"
		if hexVal != expected {
			t.Errorf("HEX: got %s, want %s", hexVal, expected)
		} else {
			t.Logf("HEX function works correctly: %s", hexVal)
		}
	}

	// Test LENGTH function
	result, err = exec.Execute(`SELECT LENGTH(data) FROM blobs`)
	if err != nil {
		t.Fatalf("Failed to execute LENGTH: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("LENGTH: Expected 1 row, got %d", result.RowCount)
	}
	t.Logf("LENGTH result: %v", result.Rows[0])

	// Test CAST to BLOB
	result, err = exec.Execute(`SELECT CAST('48656c6c6f' AS BLOB)`)
	if err != nil {
		t.Fatalf("Failed to execute CAST to BLOB: %v", err)
	}
	t.Logf("CAST to BLOB result: %v", result.Rows[0])
}

// TestCastExpression tests CAST expressions
func TestCastExpression(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create engine and executor
	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test CAST string to INT
	result, err := exec.Execute(`SELECT CAST('123' AS INT)`)
	if err != nil {
		t.Fatalf("Failed to CAST string to INT: %v", err)
	}
	t.Logf("CAST('123' AS INT): %v", result.Rows[0])

	// Test CAST INT to VARCHAR
	result, err = exec.Execute(`SELECT CAST(456 AS VARCHAR)`)
	if err != nil {
		t.Fatalf("Failed to CAST INT to VARCHAR: %v", err)
	}
	t.Logf("CAST(456 AS VARCHAR): %v", result.Rows[0])

	// Test CAST string to BLOB
	result, err = exec.Execute(`SELECT CAST('0xdeadbeef' AS BLOB)`)
	if err != nil {
		t.Fatalf("Failed to CAST string to BLOB: %v", err)
	}
	t.Logf("CAST('0xdeadbeef' AS BLOB): %v", result.Rows[0])

	// Test CAST to BOOL
	result, err = exec.Execute(`SELECT CAST(1 AS BOOL), CAST(0 AS BOOL), CAST('true' AS BOOL)`)
	if err != nil {
		t.Fatalf("Failed to CAST to BOOL: %v", err)
	}
	t.Logf("CAST to BOOL: %v", result.Rows[0])
}