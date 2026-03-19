package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestSimpleInsert tests simple INSERT with auto-increment
func TestSimpleInsert(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-simple-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create simple table
	result, err := exec.Execute(`
		CREATE TABLE test (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	t.Logf("Created table: %s", result.Message)

	// Show table structure
	result, err = exec.Execute(`SHOW COLUMNS FROM test`)
	if err != nil {
		t.Fatalf("Failed to show columns: %v", err)
	}
	t.Logf("Columns: %v", result.Rows)

	// First insert
	result, err = exec.Execute(`INSERT INTO test (name) VALUES ('first')`)
	if err != nil {
		t.Fatalf("Failed to insert first row: %v", err)
	}
	t.Logf("Inserted first row: %s", result.Message)

	// Select after first insert
	result, err = exec.Execute(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to select after first insert: %v", err)
	}
	t.Logf("After first insert - Rows: %d", result.RowCount)
	for i, row := range result.Rows {
		t.Logf("Row %d: %v", i, row)
	}

	// Second insert
	result, err = exec.Execute(`INSERT INTO test (name) VALUES ('second')`)
	if err != nil {
		t.Fatalf("Failed to insert second row: %v", err)
	}
	t.Logf("Inserted second row: %s", result.Message)

	// Select
	result, err = exec.Execute(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	t.Logf("Rows: %d", result.RowCount)
	for i, row := range result.Rows {
		t.Logf("Row %d: %v", i, row)
	}
}