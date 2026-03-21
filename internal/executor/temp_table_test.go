package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestCreateTempTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-test-*")
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

	// Create temp table
	_, err = exec.Execute("CREATE TEMP TABLE temp_users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE failed: %v", err)
	}

	// Verify temp table exists
	if !engine.TempTableExists("temp_users") {
		t.Error("Temp table should exist")
	}

	// Insert data into temp table
	_, err = exec.Execute("INSERT INTO temp_users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT into temp table failed: %v", err)
	}

	// Query temp table
	result, err := exec.Execute("SELECT * FROM temp_users")
	if err != nil {
		t.Fatalf("SELECT from temp table failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	t.Logf("Temp table query result: %v", result.Rows)
}

func TestCreateTemporaryTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temporary-test-*")
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

	// Create temp table using TEMPORARY keyword
	_, err = exec.Execute("CREATE TEMPORARY TABLE temp_data (id INT, value VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE failed: %v", err)
	}

	// Verify temp table exists
	if !engine.TempTableExists("temp_data") {
		t.Error("Temp table should exist")
	}
}

func TestTempTableIsolation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-isolation-test-*")
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

	// Create regular table
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create temp table with same name
	_, err = exec.Execute("CREATE TEMP TABLE users (id INT PRIMARY KEY, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE with same name failed: %v", err)
	}

	// Insert into temp table
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query should return data from temp table (temp has priority)
	result, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	// Check columns - temp table has 'email' not 'name'
	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	t.Logf("Result columns: %v, rows: %v", result.Columns, result.Rows)

	// Drop temp table
	_, err = exec.Execute("DROP TABLE users")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	// Now regular table should still exist
	if !engine.TableExists("users") {
		t.Error("Regular table should still exist after dropping temp table")
	}

	// Query regular table
	result, err = exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT from regular table failed: %v", err)
	}

	// Regular table should be empty
	if result.RowCount != 0 {
		t.Errorf("Expected 0 rows from regular table, got %d", result.RowCount)
	}
}

func TestTempTableWithDefaultValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-default-test-*")
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

	// Create temp table with default values
	_, err = exec.Execute("CREATE TEMP TABLE temp_items (id INT PRIMARY KEY, name VARCHAR, status VARCHAR DEFAULT 'active')")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE failed: %v", err)
	}

	// Insert without specifying status
	_, err = exec.Execute("INSERT INTO temp_items (id, name) VALUES (1, 'Item1')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query and check default was applied
	result, err := exec.Execute("SELECT * FROM temp_items")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	t.Logf("Temp table with default result: %v", result.Rows)
}

func TestTempTableClearOnSessionEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-clear-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	exec := NewExecutor(engine)

	// Create temp table
	_, err = exec.Execute("CREATE TEMP TABLE temp_session (id INT)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE failed: %v", err)
	}

	// Verify temp table exists
	if !engine.TempTableExists("temp_session") {
		t.Error("Temp table should exist")
	}

	// Clear temp tables (simulating session end)
	engine.ClearTempTables()

	// Verify temp table is gone
	if engine.TempTableExists("temp_session") {
		t.Error("Temp table should be cleared after ClearTempTables")
	}

	engine.Close()
}

func TestTempTableWithConstraints(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-constraint-test-*")
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

	// Create temp table with primary key
	_, err = exec.Execute("CREATE TEMP TABLE temp_pk (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE failed: %v", err)
	}

	// Insert
	_, err = exec.Execute("INSERT INTO temp_pk (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Try duplicate primary key - should fail
	_, err = exec.Execute("INSERT INTO temp_pk (id, name) VALUES (1, 'Bob')")
	if err == nil {
		t.Error("Expected error for duplicate primary key in temp table")
	}
	t.Logf("Expected error for duplicate PK: %v", err)
}

func TestTempTableWithIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-index-test-*")
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

	// Create temp table
	_, err = exec.Execute("CREATE TEMP TABLE temp_idx (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE failed: %v", err)
	}

	// Insert
	_, err = exec.Execute("INSERT INTO temp_idx (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Try duplicate unique column - should fail
	_, err = exec.Execute("INSERT INTO temp_idx (id, email) VALUES (2, 'alice@example.com')")
	if err == nil {
		t.Error("Expected error for duplicate unique value in temp table")
	}
	t.Logf("Expected error for duplicate unique: %v", err)
}

func TestTempTableIfNotExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-temp-ifnotexists-test-*")
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

	// Create temp table
	_, err = exec.Execute("CREATE TEMP TABLE temp_ifne (id INT)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE failed: %v", err)
	}

	// Try to create again without IF NOT EXISTS - should fail
	_, err = exec.Execute("CREATE TEMP TABLE temp_ifne (id INT)")
	if err == nil {
		t.Error("Expected error when creating existing temp table")
	}

	// Try to create again with IF NOT EXISTS - should succeed
	_, err = exec.Execute("CREATE TEMP TABLE IF NOT EXISTS temp_ifne (id INT)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE IF NOT EXISTS failed: %v", err)
	}
}