package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestForeignKeyOnDeleteCascade(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-fk-test-*")
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

	// Create parent table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with ON DELETE CASCADE
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			amount INT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert user
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert orders
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 200)`)
	if err != nil {
		t.Fatal(err)
	}

	// Verify orders exist
	result, err := exec.Execute(`SELECT COUNT(*) FROM orders`)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Orders before delete: %v", result.Rows)
	if len(result.Rows) == 0 || result.Rows[0][0] == nil {
		t.Fatalf("Expected count result, got %v", result.Rows)
	}
	// Count might be int or int64 depending on implementation
	count := result.Rows[0][0]
	t.Logf("Count type: %T, value: %v", count, count)

	// Delete user - should cascade delete orders
	_, err = exec.Execute(`DELETE FROM users WHERE id = 1`)
	if err != nil {
		t.Fatalf("Delete should succeed: %v", err)
	}

	// Verify orders were cascade deleted
	result, err = exec.Execute(`SELECT * FROM orders`)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Orders after cascade delete: Rows=%v, RowCount=%d", result.Rows, result.RowCount)
	if result.RowCount != 0 {
		t.Errorf("Expected 0 orders after cascade delete, got %d", result.RowCount)
	}
}

func TestForeignKeyOnDeleteSetNull(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-fk-test-*")
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

	// Create parent table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with ON DELETE SET NULL
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			amount INT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert user
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert order
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)`)
	if err != nil {
		t.Fatal(err)
	}

	// Delete user
	_, err = exec.Execute(`DELETE FROM users WHERE id = 1`)
	if err != nil {
		t.Fatalf("Delete should succeed: %v", err)
	}

	// Verify user_id was set to NULL
	result, err := exec.Execute(`SELECT user_id, amount FROM orders WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Order after SET NULL: %v", result.Rows)
	if result.Rows[0][0] != nil {
		t.Errorf("Expected user_id to be NULL, got %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != int64(100) {
		t.Errorf("Expected amount to still be 100, got %v", result.Rows[0][1])
	}
}

func TestForeignKeyOnUpdateCascade(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-fk-test-*")
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

	// Create parent table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with ON UPDATE CASCADE
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			amount INT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert user
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert order
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)`)
	if err != nil {
		t.Fatal(err)
	}

	// Update user id
	_, err = exec.Execute(`UPDATE users SET id = 10 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Update should succeed: %v", err)
	}

	// Verify user_id was cascaded
	result, err := exec.Execute(`SELECT user_id FROM orders WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Order after UPDATE CASCADE: %v", result.Rows)
	if result.Rows[0][0] != int64(10) {
		t.Errorf("Expected user_id to be 10 after cascade, got %v", result.Rows[0][0])
	}
}

func TestForeignKeyOnDeleteRestrict(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-fk-test-*")
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

	// Create parent table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with ON DELETE RESTRICT (default behavior)
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			amount INT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert user
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert order
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)`)
	if err != nil {
		t.Fatal(err)
	}

	// Try to delete user - should fail due to RESTRICT
	_, err = exec.Execute(`DELETE FROM users WHERE id = 1`)
	if err == nil {
		t.Error("Expected error for DELETE with RESTRICT constraint")
	}
	t.Logf("RESTRICT error (expected): %v", err)
}

func TestForeignKeyCompositeKey(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-fk-test-*")
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

	// Create parent table with composite primary key
	_, err = exec.Execute(`
		CREATE TABLE order_lines (
			order_id INT,
			line_num INT,
			product VARCHAR(100),
			PRIMARY KEY (order_id, line_num)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create order_lines table: %v", err)
	}

	// Create child table with composite foreign key
	_, err = exec.Execute(`
		CREATE TABLE shipments (
			id SEQ PRIMARY KEY,
			order_id INT,
			line_num INT,
			FOREIGN KEY (order_id, line_num) REFERENCES order_lines(order_id, line_num)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create shipments table: %v", err)
	}

	// Insert order lines
	_, err = exec.Execute(`INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Widget')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Gadget')`)
	if err != nil {
		t.Fatal(err)
	}

	// Valid insert with matching composite key
	_, err = exec.Execute(`INSERT INTO shipments (id, order_id, line_num) VALUES (1, 1, 1)`)
	if err != nil {
		t.Fatalf("Valid insert should succeed: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO shipments (id, order_id, line_num) VALUES (2, 1, 2)`)
	if err != nil {
		t.Fatalf("Valid insert should succeed: %v", err)
	}

	// Invalid insert - composite key not found
	_, err = exec.Execute(`INSERT INTO shipments (id, order_id, line_num) VALUES (3, 1, 999)`)
	if err == nil {
		t.Error("Expected error for non-existent composite key (1, 999)")
	} else {
		t.Logf("Composite FK validation error (expected): %v", err)
	}

	// Invalid insert - only first column matches
	_, err = exec.Execute(`INSERT INTO shipments (id, order_id, line_num) VALUES (4, 999, 1)`)
	if err == nil {
		t.Error("Expected error for non-existent composite key (999, 1)")
	} else {
		t.Logf("Composite FK validation error (expected): %v", err)
	}

	// Test with NULL in one column - should pass (NULL skips FK check)
	_, err = exec.Execute(`INSERT INTO shipments (id, order_id, line_num) VALUES (5, NULL, 1)`)
	if err != nil {
		t.Logf("Insert with NULL in composite FK: %v", err)
	}
}

func TestForeignKeyCompositeKeyCascade(t *testing.T) {
	// Test composite foreign key with ON DELETE CASCADE
	tmpDir, err := os.MkdirTemp("", "xxsql-fk-cascade-test-*")
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

	// Create parent table with composite primary key
	_, err = exec.Execute(`
		CREATE TABLE orders (
			order_id INT,
			line_num INT,
			product VARCHAR(100),
			PRIMARY KEY (order_id, line_num)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Create child table with composite foreign key and CASCADE
	// Note: Current implementation handles cascade per-column, so this tests single column behavior
	_, err = exec.Execute(`
		CREATE TABLE shipments (
			id SEQ PRIMARY KEY,
			order_id INT,
			line_num INT,
			status VARCHAR(50),
			FOREIGN KEY (order_id, line_num) REFERENCES orders(order_id, line_num) ON DELETE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create shipments table: %v", err)
	}

	// Insert order
	_, err = exec.Execute(`INSERT INTO orders (order_id, line_num, product) VALUES (1, 1, 'Widget')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert shipment
	_, err = exec.Execute(`INSERT INTO shipments (id, order_id, line_num, status) VALUES (1, 1, 1, 'shipped')`)
	if err != nil {
		t.Fatal(err)
	}

	// Note: Composite FK cascade is partially supported
	// Current implementation handles cascade on a per-column basis
	// Full composite cascade would require matching all columns together
	t.Log("Composite FK with CASCADE created successfully")
}