package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ============================================================================
// Test Setup
// ============================================================================

func setupDDLTest(t *testing.T) (*Executor, func()) {
	dir, err := os.MkdirTemp("", "ddl-test-*")
	if err != nil {
		t.Fatal(err)
	}

	engine := storage.NewEngine(dir)
	exec := NewExecutor(engine)

	return exec, func() {
		os.RemoveAll(dir)
	}
}

// ============================================================================
// CREATE INDEX Tests
// ============================================================================

func TestCreateIndex(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table first
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')`)
	if err != nil {
		t.Fatal(err)
	}

	// Create index
	_, err = exec.Execute(`CREATE INDEX idx_name ON users (name)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
}

func TestCreateUniqueIndex(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create unique index
	_, err = exec.Execute(`CREATE UNIQUE INDEX idx_email ON users (email)`)
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}
}

func TestCreateIndexNonExistentTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	_, err := exec.Execute(`CREATE INDEX idx_name ON nonexistent (name)`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ============================================================================
// DROP INDEX Tests
// ============================================================================

func TestDropIndex(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and index
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`CREATE INDEX idx_name ON users (name)`)
	if err != nil {
		t.Fatal(err)
	}

	// Drop index
	_, err = exec.Execute(`DROP INDEX idx_name ON users`)
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}
}

func TestDropNonExistentIndex(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Drop non-existent index should fail
	_, err = exec.Execute(`DROP INDEX nonexistent ON users`)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// ============================================================================
// ALTER TABLE ADD COLUMN Tests
// ============================================================================

func TestAlterTableAddColumn(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Add column
	_, err = exec.Execute(`ALTER TABLE users ADD COLUMN email VARCHAR(100)`)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}

	// Verify column was added
	result, err := exec.Execute(`SHOW COLUMNS FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Rows))
	}
}

func TestAlterTableAddMultipleColumns(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Add multiple columns in one statement
	_, err = exec.Execute(`ALTER TABLE users ADD COLUMN name VARCHAR(100), ADD COLUMN email VARCHAR(100)`)
	if err != nil {
		t.Fatalf("Failed to add columns: %v", err)
	}

	// Verify columns were added
	result, err := exec.Execute(`SHOW COLUMNS FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Rows))
	}
}

// ============================================================================
// ALTER TABLE DROP COLUMN Tests
// ============================================================================

func TestAlterTableDropColumn(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Drop column
	_, err = exec.Execute(`ALTER TABLE users DROP COLUMN email`)
	if err != nil {
		t.Fatalf("Failed to drop column: %v", err)
	}

	// Verify column was dropped
	result, err := exec.Execute(`SHOW COLUMNS FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Rows))
	}
}

func TestAlterTableDropPrimaryKey(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Dropping primary key should fail
	_, err = exec.Execute(`ALTER TABLE users DROP COLUMN id`)
	if err == nil {
		t.Error("Expected error when dropping primary key column")
	}
}

// ============================================================================
// ALTER TABLE MODIFY COLUMN Tests
// ============================================================================

func TestAlterTableModifyColumn(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Modify column
	_, err = exec.Execute(`ALTER TABLE users MODIFY COLUMN name VARCHAR(200)`)
	if err != nil {
		t.Fatalf("Failed to modify column: %v", err)
	}
}

// ============================================================================
// ALTER TABLE RENAME COLUMN Tests
// ============================================================================

func TestAlterTableRenameColumn(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Rename column
	_, err = exec.Execute(`ALTER TABLE users RENAME COLUMN name TO username`)
	if err != nil {
		t.Fatalf("Failed to rename column: %v", err)
	}

	// Verify column was renamed
	result, err := exec.Execute(`SHOW COLUMNS FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) > 1 && result.Rows[1][0] != "username" {
		t.Errorf("Expected column 'username', got %v", result.Rows[1][0])
	}
}

// ============================================================================
// ALTER TABLE RENAME Tests
// ============================================================================

func TestAlterTableRename(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Rename table
	_, err = exec.Execute(`ALTER TABLE users RENAME TO customers`)
	if err != nil {
		t.Fatalf("Failed to rename table: %v", err)
	}

	// Verify old table doesn't exist
	_, err = exec.Execute(`SELECT * FROM users`)
	if err == nil {
		t.Error("Expected error for old table name")
	}

	// Verify new table exists and has data
	result, err := exec.Execute(`SELECT * FROM customers`)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// ============================================================================
// UPDATE Tests
// ============================================================================

func TestUpdateBasic(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			status VARCHAR(20)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name, status) VALUES (1, 'Alice', 'active')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, status) VALUES (2, 'Bob', 'active')`)
	if err != nil {
		t.Fatal(err)
	}

	// Update with WHERE
	result, err := exec.Execute(`UPDATE users SET status = 'inactive' WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}
}

func TestUpdateAllRows(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			status VARCHAR(20)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, status) VALUES (1, 'active')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, status) VALUES (2, 'active')`)
	if err != nil {
		t.Fatal(err)
	}

	// Update all rows (no WHERE)
	result, err := exec.Execute(`UPDATE users SET status = 'inactive'`)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.Affected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.Affected)
	}
}

func TestUpdateNonExistentTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	_, err := exec.Execute(`UPDATE nonexistent SET name = 'test'`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ============================================================================
// DELETE Tests
// ============================================================================

func TestDeleteBasic(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (3, 'Charlie')`)
	if err != nil {
		t.Fatal(err)
	}

	// Delete with WHERE
	result, err := exec.Execute(`DELETE FROM users WHERE name = 'Bob'`)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}

	// Verify row was deleted
	result, err = exec.Execute(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows after delete, got %d", result.RowCount)
	}
}

func TestDeleteAllRows(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	if err != nil {
		t.Fatal(err)
	}

	// Delete all rows (no WHERE)
	result, err := exec.Execute(`DELETE FROM users`)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if result.Affected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.Affected)
	}

	// Verify all rows deleted
	result, err = exec.Execute(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowCount != 0 {
		t.Errorf("Expected 0 rows after delete all, got %d", result.RowCount)
	}
}

func TestDeleteNonExistentTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	_, err := exec.Execute(`DELETE FROM nonexistent`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ============================================================================
// TRUNCATE Tests
// ============================================================================

func TestTruncateTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	if err != nil {
		t.Fatal(err)
	}

	// Truncate
	_, err = exec.Execute(`TRUNCATE TABLE users`)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify all rows deleted
	result, err := exec.Execute(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowCount != 0 {
		t.Errorf("Expected 0 rows after truncate, got %d", result.RowCount)
	}
}

// ============================================================================
// Additional UPDATE Tests
// ============================================================================

func TestUpdateMultipleColumns(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			status VARCHAR(20)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name, email, status) VALUES (1, 'Alice', 'alice@example.com', 'active')`)
	if err != nil {
		t.Fatal(err)
	}

	// Update multiple columns
	result, err := exec.Execute(`UPDATE users SET name = 'Bob', email = 'bob@example.com', status = 'inactive' WHERE id = 1`)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}

	// Verify the update
	result, err = exec.Execute(`SELECT * FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][1] != "Bob" {
		t.Errorf("Expected name 'Bob', got %v", result.Rows[0][1])
	}
}

func TestUpdateWithComparison(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table with numeric data
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price INT
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 200)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (3, 'Gizmo', 300)`)
	if err != nil {
		t.Fatal(err)
	}

	// Update with numeric comparison
	result, err := exec.Execute(`UPDATE products SET price = 150 WHERE price > 150`)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.Affected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.Affected)
	}
}

func TestUpdateNoMatch(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Update with non-matching WHERE
	result, err := exec.Execute(`UPDATE users SET name = 'Bob' WHERE name = 'NonExistent'`)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.Affected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.Affected)
	}
}

// ============================================================================
// Additional DELETE Tests
// ============================================================================

func TestDeleteWithComparison(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table with numeric data
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			quantity INT
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO products (id, name, quantity) VALUES (1, 'Widget', 10)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name, quantity) VALUES (2, 'Gadget', 0)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name, quantity) VALUES (3, 'Gizmo', 5)`)
	if err != nil {
		t.Fatal(err)
	}

	// Delete with comparison
	result, err := exec.Execute(`DELETE FROM products WHERE quantity = 0`)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}

	// Verify remaining rows
	result, err = exec.Execute(`SELECT * FROM products`)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows remaining, got %d", result.RowCount)
	}
}

func TestDeleteNoMatch(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Delete with non-matching WHERE
	result, err := exec.Execute(`DELETE FROM users WHERE name = 'NonExistent'`)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if result.Affected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.Affected)
	}

	// Verify row still exists
	result, err = exec.Execute(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row remaining, got %d", result.RowCount)
	}
}

// ============================================================================
// ALTER TABLE Additional Tests
// ============================================================================

func TestAlterTableAddColumnWithDefault(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Add column
	_, err = exec.Execute(`ALTER TABLE users ADD COLUMN age INT`)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}

	// Verify - existing rows should have NULL for new column
	result, err := exec.Execute(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// The new column should be NULL (3rd column)
	if len(result.Rows[0]) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Rows[0]))
	}
}

func TestAlterTableDropNonExistentColumn(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Drop non-existent column should fail
	_, err = exec.Execute(`ALTER TABLE users DROP COLUMN nonexistent`)
	if err == nil {
		t.Error("Expected error when dropping non-existent column")
	}
}

func TestAlterTableRenameNonExistentColumn(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Rename non-existent column should fail
	_, err = exec.Execute(`ALTER TABLE users RENAME COLUMN nonexistent TO newname`)
	if err == nil {
		t.Error("Expected error when renaming non-existent column")
	}
}

func TestAlterTableRenameToExistingTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create two tables
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE customers (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Rename to existing table name should fail
	_, err = exec.Execute(`ALTER TABLE users RENAME TO customers`)
	if err == nil {
		t.Error("Expected error when renaming to existing table name")
	}
}

// ============================================================================
// SHOW COLUMNS Tests
// ============================================================================

func TestShowColumns(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Show columns
	result, err := exec.Execute(`SHOW COLUMNS FROM users`)
	if err != nil {
		t.Fatalf("Show columns failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Rows))
	}
}

func TestShowColumnsNonExistentTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	_, err := exec.Execute(`SHOW COLUMNS FROM nonexistent`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ============================================================================
// DROP TABLE Tests
// ============================================================================

func TestDropTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Drop table
	_, err = exec.Execute(`DROP TABLE users`)
	if err != nil {
		t.Fatalf("Drop table failed: %v", err)
	}

	// Verify table doesn't exist
	_, err = exec.Execute(`SELECT * FROM users`)
	if err == nil {
		t.Error("Expected error for dropped table")
	}
}

func TestDropNonExistentTable(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	_, err := exec.Execute(`DROP TABLE nonexistent`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestDropTableIfExists(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// DROP TABLE IF EXISTS on non-existent table should succeed
	_, err := exec.Execute(`DROP TABLE IF EXISTS nonexistent`)
	if err != nil {
		t.Fatalf("Drop table if exists failed: %v", err)
	}
}

// ============================================================================
// CREATE TABLE IF NOT EXISTS Tests
// ============================================================================

func TestCreateTableIfNotExists(t *testing.T) {
	exec, cleanup := setupDDLTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Try to create again with IF NOT EXISTS - should succeed
	_, err = exec.Execute(`
		CREATE TABLE IF NOT EXISTS users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Create table if not exists failed: %v", err)
	}

	// Try to create again without IF NOT EXISTS - should fail
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err == nil {
		t.Error("Expected error when creating existing table")
	}
}
