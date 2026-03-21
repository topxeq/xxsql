package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestReturningInsert(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-returning-test-*")
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

	// Create table
	_, err = exec.Execute("CREATE TABLE users (id SEQ PRIMARY KEY, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test INSERT with RETURNING *
	result, err := exec.Execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING *")
	if err != nil {
		t.Fatalf("INSERT RETURNING failed: %v", err)
	}

	t.Logf("INSERT RETURNING * result: columns=%v, rows=%v", result.Columns, result.Rows)

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row returned, got %d", result.RowCount)
	}

	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Columns))
	}

	// Check the returned values
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		t.Logf("Returned row: id=%v, name=%v, email=%v", row[0], row[1], row[2])

		// id should be auto-generated (SEQ type)
		if row[0] == nil {
			t.Error("Expected non-nil id")
		}
		if row[1] != "Alice" {
			t.Errorf("Expected name 'Alice', got %v", row[1])
		}
		if row[2] != "alice@example.com" {
			t.Errorf("Expected email 'alice@example.com', got %v", row[2])
		}
	}
}

func TestReturningInsertSpecificColumns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-returning-col-test-*")
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

	// Create table
	_, err = exec.Execute("CREATE TABLE products (id SEQ PRIMARY KEY, name VARCHAR, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test INSERT with RETURNING specific columns
	result, err := exec.Execute("INSERT INTO products (name, price) VALUES ('Widget', 19.99) RETURNING id, name")
	if err != nil {
		t.Fatalf("INSERT RETURNING failed: %v", err)
	}

	t.Logf("INSERT RETURNING id, name result: columns=%v, rows=%v", result.Columns, result.Rows)

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	if result.Columns[0].Name != "id" {
		t.Errorf("Expected first column 'id', got '%s'", result.Columns[0].Name)
	}

	if result.Columns[1].Name != "name" {
		t.Errorf("Expected second column 'name', got '%s'", result.Columns[1].Name)
	}
}

func TestReturningUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-returning-update-test-*")
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

	// Create table and insert data
	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR, status VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items (id, name, status) VALUES (1, 'Item1', 'pending')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items (id, name, status) VALUES (2, 'Item2', 'pending')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test UPDATE with RETURNING
	result, err := exec.Execute("UPDATE items SET status = 'done' WHERE id = 1 RETURNING *")
	if err != nil {
		t.Fatalf("UPDATE RETURNING failed: %v", err)
	}

	t.Logf("UPDATE RETURNING result: columns=%v, rows=%v", result.Columns, result.Rows)

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row returned, got %d", result.RowCount)
	}

	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if row[2] != "done" {
			t.Errorf("Expected status 'done', got %v", row[2])
		}
	}
}

func TestReturningUpdateMultipleRows(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-returning-multi-test-*")
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

	// Create table and insert data
	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders (id, status, amount) VALUES (1, 'pending', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders (id, status, amount) VALUES (2, 'pending', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders (id, status, amount) VALUES (3, 'shipped', 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test UPDATE multiple rows with RETURNING
	result, err := exec.Execute("UPDATE orders SET status = 'processed' WHERE status = 'pending' RETURNING id, status")
	if err != nil {
		t.Fatalf("UPDATE RETURNING failed: %v", err)
	}

	t.Logf("UPDATE multiple rows RETURNING result: columns=%v, rows=%v", result.Columns, result.Rows)

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows returned, got %d", result.RowCount)
	}

	// Verify all returned rows have status 'processed'
	for i, row := range result.Rows {
		if row[1] != "processed" {
			t.Errorf("Row %d: expected status 'processed', got %v", i, row[1])
		}
	}
}

func TestReturningDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-returning-delete-test-*")
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

	// Create table and insert data
	_, err = exec.Execute("CREATE TABLE archive (id INT PRIMARY KEY, data VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO archive (id, data) VALUES (1, 'data1')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO archive (id, data) VALUES (2, 'data2')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test DELETE with RETURNING
	result, err := exec.Execute("DELETE FROM archive WHERE id = 1 RETURNING *")
	if err != nil {
		t.Fatalf("DELETE RETURNING failed: %v", err)
	}

	t.Logf("DELETE RETURNING result: columns=%v, rows=%v", result.Columns, result.Rows)

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row returned, got %d", result.RowCount)
	}

	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if row[0] != int64(1) {
			t.Errorf("Expected id 1, got %v", row[0])
		}
		if row[1] != "data1" {
			t.Errorf("Expected data 'data1', got %v", row[1])
		}
	}

	// Verify row was deleted
	result, err = exec.Execute("SELECT * FROM archive")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row remaining, got %d", result.RowCount)
	}
}

func TestReturningWithGeneratedColumn(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-returning-gen-test-*")
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

	// Create table with generated column
	_, err = exec.Execute("CREATE TABLE calc (id INT PRIMARY KEY, a INT, b INT, total INT GENERATED ALWAYS AS (a + b))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test INSERT with RETURNING including generated column
	result, err := exec.Execute("INSERT INTO calc (id, a, b) VALUES (1, 10, 20) RETURNING *")
	if err != nil {
		t.Fatalf("INSERT RETURNING failed: %v", err)
	}

	t.Logf("INSERT RETURNING with generated column result: columns=%v, rows=%v", result.Columns, result.Rows)

	if len(result.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) > 0 {
		row := result.Rows[0]
		// total should be 30 (10 + 20)
		t.Logf("Returned row: id=%v, a=%v, b=%v, total=%v", row[0], row[1], row[2], row[3])
	}
}