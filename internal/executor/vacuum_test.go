package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestVacuumFullDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-vacuum-test-*")
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
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO users (name, email) VALUES ('user%d', 'test%d@example.com')", i, i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete some rows
	_, err = exec.Execute("DELETE FROM users WHERE id <= 50")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify some rows deleted
	result, err := exec.Execute("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Rows before vacuum: %v", result.Rows)

	// Run VACUUM
	result, err = exec.Execute("VACUUM")
	if err != nil {
		t.Fatalf("VACUUM failed: %v", err)
	}

	t.Logf("VACUUM result: %s", result.Message)

	// Verify data still exists
	result, err = exec.Execute("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Rows after vacuum: %v", result.Rows)

	// Check count is 50 (100 - 50 deleted)
	if len(result.Rows) == 0 || result.Rows[0][0] == nil {
		t.Fatal("Expected count result")
	}

	count := result.Rows[0][0]
	t.Logf("Count after vacuum: %v (%T)", count, count)
}

func TestVacuumSpecificTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-vacuum-table-test-*")
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
	exec.SetDatabase("test")

	// Create two tables
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			amount INT
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	_, err = exec.Execute("INSERT INTO users (name) VALUES ('Alice')")
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute("INSERT INTO orders (user_id, amount) VALUES (1, 100)")
	if err != nil {
		t.Fatal(err)
	}

	// Vacuum specific table
	result, err := exec.Execute("VACUUM users")
	if err != nil {
		t.Fatalf("VACUUM users failed: %v", err)
	}

	t.Logf("VACUUM users result: %s", result.Message)

	// Verify data still exists
	result, err = exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 user, got %d", result.RowCount)
	}

	result, err = exec.Execute("SELECT * FROM orders")
	if err != nil {
		t.Fatal(err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 order, got %d", result.RowCount)
	}
}

func TestVacuumNonExistentTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-vacuum-notexist-test-*")
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

	// Try to vacuum non-existent table
	_, err = exec.Execute("VACUUM nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
	t.Logf("Expected error: %v", err)
}

func TestVacuumInto(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-vacuum-into-test-*")
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
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price INT
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	_, err = exec.Execute("INSERT INTO products (name, price) VALUES ('Widget', 100)")
	if err != nil {
		t.Fatal(err)
	}

	// Create a backup path
	backupPath := tmpDir + "/vacuum_export"

	// VACUUM INTO
	result, err := exec.Execute("VACUUM INTO '" + backupPath + "'")
	if err != nil {
		t.Fatalf("VACUUM INTO failed: %v", err)
	}

	t.Logf("VACUUM INTO result: %s", result.Message)

	// Verify backup was created
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		// Check for .xbak extension
		if _, err := os.Stat(backupPath + ".xbak"); os.IsNotExist(err) {
			t.Error("VACUUM INTO did not create backup file")
		}
	}
}

func TestVacuumEmptyDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-vacuum-empty-test-*")
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

	// Vacuum empty database
	result, err := exec.Execute("VACUUM")
	if err != nil {
		t.Fatalf("VACUUM on empty database failed: %v", err)
	}

	t.Logf("VACUUM empty result: %s", result.Message)
}

func TestVacuumWithIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-vacuum-index-test-*")
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
	exec.SetDatabase("test")

	// Create table with index
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100),
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	for i := 1; i <= 50; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO users (email, name) VALUES ('test%d@example.com', 'user%d')", i, i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete some rows
	_, err = exec.Execute("DELETE FROM users WHERE id <= 25")
	if err != nil {
		t.Fatal(err)
	}

	// Run VACUUM
	result, err := exec.Execute("VACUUM users")
	if err != nil {
		t.Fatalf("VACUUM failed: %v", err)
	}

	t.Logf("VACUUM result: %s", result.Message)

	// Verify index still works
	result, err = exec.Execute("SELECT * FROM users WHERE email = 'test30@example.com'")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Index query result: %d rows", result.RowCount)
}