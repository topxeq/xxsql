package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestBlobComparison tests BLOB comparison operations
func TestBlobComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-blob-cmp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

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
	_, err = exec.Execute(`INSERT INTO blobs (data) VALUES (X'48656c6c6f')`) // "Hello"
	if err != nil {
		t.Fatalf("Failed to insert first: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO blobs (data) VALUES (X'576f726c64')`) // "World"
	if err != nil {
		t.Fatalf("Failed to insert second: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO blobs (data) VALUES (X'48656c6c6f')`) // "Hello" again
	if err != nil {
		t.Fatalf("Failed to insert third: %v", err)
	}

	// Test comparison with X'...'
	result, err := exec.Execute(`SELECT * FROM blobs WHERE data = X'48656c6c6f'`)
	if err != nil {
		t.Fatalf("Failed to select with =: %v", err)
	}
	t.Logf("SELECT WHERE data = X'Hello': %d rows", result.RowCount)
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows with 'Hello', got %d", result.RowCount)
	}

	// Test comparison with !=
	result, err = exec.Execute(`SELECT * FROM blobs WHERE data != X'48656c6c6f'`)
	if err != nil {
		t.Fatalf("Failed to select with !=: %v", err)
	}
	t.Logf("SELECT WHERE data != X'Hello': %d rows", result.RowCount)
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row not 'Hello', got %d", result.RowCount)
	}

	// Test IS NULL
	_, err = exec.Execute(`INSERT INTO blobs (data) VALUES (NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert NULL: %v", err)
	}

	result, err = exec.Execute(`SELECT * FROM blobs WHERE data IS NULL`)
	if err != nil {
		t.Fatalf("Failed to select IS NULL: %v", err)
	}
	t.Logf("SELECT WHERE data IS NULL: %d rows", result.RowCount)
	if result.RowCount != 1 {
		t.Errorf("Expected 1 NULL row, got %d", result.RowCount)
	}

	// Test IS NOT NULL
	result, err = exec.Execute(`SELECT * FROM blobs WHERE data IS NOT NULL`)
	if err != nil {
		t.Fatalf("Failed to select IS NOT NULL: %v", err)
	}
	t.Logf("SELECT WHERE data IS NOT NULL: %d rows", result.RowCount)
	for i, row := range result.Rows {
		t.Logf("Row %d: %v", i, row)
	}
	if result.RowCount != 3 {
		// Check all rows first
		allRows, err := exec.Execute(`SELECT * FROM blobs`)
		if err != nil {
			t.Logf("Failed to select all: %v", err)
		} else {
			t.Logf("All rows: %d", allRows.RowCount)
			for i, row := range allRows.Rows {
				t.Logf("All row %d: %v", i, row)
			}
		}
		t.Errorf("Expected 3 non-NULL rows, got %d", result.RowCount)
	}
}