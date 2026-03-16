package executor_test

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestExecutorCreateTable(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Test CREATE TABLE
	result, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Verify table exists
	if !engine.TableExists("users") {
		t.Error("Table 'users' should exist")
	}
}

func TestExecutorShowTables(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create a table first
	_, err = exec.Execute("CREATE TABLE test1 (id INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW TABLES
	result, err := exec.Execute("SHOW TABLES")
	if err != nil {
		t.Fatalf("Failed to show tables: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 table, got %d", result.RowCount)
	}
}

func TestExecutorSelectLiteral(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Test SELECT 1
	result, err := exec.Execute("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to select 1: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestExecutorDropTable(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Create a table
	_, err = exec.Execute("CREATE TABLE todrop (id INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Drop the table
	result, err := exec.Execute("DROP TABLE todrop")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Try to drop non-existent table (should fail)
	_, err = exec.Execute("DROP TABLE nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent table")
	}

	// Drop with IF EXISTS (should succeed)
	result, err = exec.Execute("DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("Failed to drop table with IF EXISTS: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

func TestExecutorUseDatabase(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Test USE DATABASE
	result, err := exec.Execute("USE mydb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}
	if result.Message != "Database changed" {
		t.Errorf("Expected 'Database changed', got %s", result.Message)
	}
}

func TestExecutorShowColumns(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Create a table with multiple columns
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100), active BOOL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW COLUMNS
	result, err := exec.Execute("SHOW COLUMNS FROM test")
	if err != nil {
		t.Fatalf("Failed to show columns: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 columns, got %d", result.RowCount)
	}
}
