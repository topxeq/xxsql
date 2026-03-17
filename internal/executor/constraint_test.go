package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ============================================================================
// Test Setup
// ============================================================================

func setupConstraintTest(t *testing.T) (*Executor, func()) {
	dir, err := os.MkdirTemp("", "constraint-test-*")
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
// NOT NULL Constraint Tests
// ============================================================================

func TestNotNullConstraintOnInsert(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with NOT NULL column
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with name provided - should succeed
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatalf("Insert with name should succeed: %v", err)
	}

	// Insert without name (NULL) - should fail
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'bob@example.com')`)
	if err == nil {
		t.Error("Expected error for NULL value in NOT NULL column")
	}

	// Insert with explicit NULL - should fail
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (3, NULL, 'charlie@example.com')`)
	if err == nil {
		t.Error("Expected error for explicit NULL in NOT NULL column")
	}
}

func TestNotNullConstraintOnUpdate(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with NOT NULL column
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert valid data
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Update to NULL - should fail
	_, err = exec.Execute(`UPDATE users SET name = NULL WHERE id = 1`)
	if err == nil {
		t.Error("Expected error for updating NOT NULL column to NULL")
	}

	// Valid update - should succeed
	_, err = exec.Execute(`UPDATE users SET name = 'Bob' WHERE id = 1`)
	if err != nil {
		t.Fatalf("Valid update should succeed: %v", err)
	}
}

func TestNullableColumnAllowsNull(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with nullable column
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

	// Insert without optional columns - should succeed
	_, err = exec.Execute(`INSERT INTO users (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Insert without optional columns should succeed: %v", err)
	}

	// Insert with explicit NULL - should succeed
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (2, NULL, NULL)`)
	if err != nil {
		t.Fatalf("Insert with NULL in nullable columns should succeed: %v", err)
	}
}

// ============================================================================
// DEFAULT Value Tests
// ============================================================================

func TestDefaultValueOnInsert(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with DEFAULT values
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			status VARCHAR(20) DEFAULT 'active'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert without specifying status - should use default
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatalf("Insert should succeed: %v", err)
	}

	// Verify default was applied
	result, err := exec.Execute(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][0] != "active" {
		t.Errorf("Expected default value 'active', got %v", result.Rows[0][0])
	}
}

func TestDefaultValueOverride(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with DEFAULT
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			status VARCHAR(20) DEFAULT 'active'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with explicit value - should override default
	_, err = exec.Execute(`INSERT INTO users (id, status) VALUES (1, 'inactive')`)
	if err != nil {
		t.Fatal(err)
	}

	// Verify explicit value was used
	result, err := exec.Execute(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if result.Rows[0][0] != "inactive" {
		t.Errorf("Expected 'inactive', got %v", result.Rows[0][0])
	}
}

func TestDefaultValueWithNotNull(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with NOT NULL and DEFAULT
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100) NOT NULL DEFAULT 'Anonymous'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert without name - should use default and satisfy NOT NULL
	_, err = exec.Execute(`INSERT INTO users (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Insert should succeed with default: %v", err)
	}

	// Verify default was applied
	result, err := exec.Execute(`SELECT name FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if result.Rows[0][0] != "Anonymous" {
		t.Errorf("Expected default value 'Anonymous', got %v", result.Rows[0][0])
	}
}

func TestDefaultIntValue(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with INT default
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			quantity INT DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert without quantity
	_, err = exec.Execute(`INSERT INTO products (id) VALUES (1)`)
	if err != nil {
		t.Fatal(err)
	}

	// Verify default
	result, err := exec.Execute(`SELECT quantity FROM products WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if result.Rows[0][0] != int64(0) {
		t.Errorf("Expected default value 0, got %v (%T)", result.Rows[0][0], result.Rows[0][0])
	}
}

// ============================================================================
// UNIQUE Constraint Tests
// ============================================================================

func TestUniqueConstraintOnInsert(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with UNIQUE column
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100) UNIQUE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// First insert - should succeed
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (1, 'test@example.com')`)
	if err != nil {
		t.Fatalf("First insert should succeed: %v", err)
	}

	// Duplicate insert - should fail
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'test@example.com')`)
	if err == nil {
		t.Error("Expected error for duplicate UNIQUE value")
	}

	// Different value - should succeed
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'other@example.com')`)
	if err != nil {
		t.Fatalf("Insert with different value should succeed: %v", err)
	}
}

func TestUniqueConstraintOnUpdate(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with UNIQUE column
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100) UNIQUE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert two rows
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (1, 'alice@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'bob@example.com')`)
	if err != nil {
		t.Fatal(err)
	}

	// Update to duplicate value - should fail
	_, err = exec.Execute(`UPDATE users SET email = 'alice@example.com' WHERE id = 2`)
	if err == nil {
		t.Error("Expected error for updating to duplicate UNIQUE value")
	}

	// Update to new value - should succeed
	_, err = exec.Execute(`UPDATE users SET email = 'charlie@example.com' WHERE id = 2`)
	if err != nil {
		t.Fatalf("Valid update should succeed: %v", err)
	}
}

func TestUniqueAllowsMultipleNulls(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with UNIQUE column (nullable)
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100) UNIQUE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with NULL - should succeed
	_, err = exec.Execute(`INSERT INTO users (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Insert with NULL should succeed: %v", err)
	}

	// Another insert with NULL - should also succeed (SQL standard: NULL != NULL)
	_, err = exec.Execute(`INSERT INTO users (id) VALUES (2)`)
	if err != nil {
		t.Fatalf("Second insert with NULL should succeed: %v", err)
	}
}

func TestUniqueConstraintInline(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with inline UNIQUE (not via table constraint)
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100),
			UNIQUE (email)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test uniqueness
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (1, 'test@example.com')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'test@example.com')`)
	if err == nil {
		t.Error("Expected error for duplicate value")
	}
}

// ============================================================================
// Combined Constraint Tests
// ============================================================================

func TestNotNullWithUnique(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with NOT NULL UNIQUE
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			email VARCHAR(100) NOT NULL UNIQUE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert without email - should fail (NOT NULL)
	_, err = exec.Execute(`INSERT INTO users (id) VALUES (1)`)
	if err == nil {
		t.Error("Expected error for missing NOT NULL column")
	}

	// Insert with valid value
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (1, 'test@example.com')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert duplicate - should fail (UNIQUE)
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'test@example.com')`)
	if err == nil {
		t.Error("Expected error for duplicate UNIQUE value")
	}
}

func TestMultipleConstraints(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with multiple constraints
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) NOT NULL UNIQUE,
			status VARCHAR(20) DEFAULT 'active'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatalf("Valid insert should succeed: %v", err)
	}

	// Verify status default
	result, err := exec.Execute(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0][0] != "active" {
		t.Errorf("Expected default 'active', got %v", result.Rows[0][0])
	}

	// Missing name - should fail
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'bob@example.com')`)
	if err == nil {
		t.Error("Expected error for missing NOT NULL column 'name'")
	}

	// Missing email - should fail
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	if err == nil {
		t.Error("Expected error for missing NOT NULL column 'email'")
	}

	// Duplicate email - should fail
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'alice@example.com')`)
	if err == nil {
		t.Error("Expected error for duplicate email")
	}
}

// ============================================================================
// CHECK Constraint Tests
// ============================================================================

func TestCheckConstraintOnInsert(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with CHECK constraint
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT,
			quantity INT,
			CHECK (price > 0)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert - should succeed
	_, err = exec.Execute(`INSERT INTO products (id, price, quantity) VALUES (1, 100, 10)`)
	if err != nil {
		t.Fatalf("Valid insert should succeed: %v", err)
	}

	// Insert with price <= 0 - should fail
	_, err = exec.Execute(`INSERT INTO products (id, price, quantity) VALUES (2, 0, 10)`)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation (price <= 0)")
	}

	// Insert with negative price - should fail
	_, err = exec.Execute(`INSERT INTO products (id, price, quantity) VALUES (3, -10, 10)`)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation (negative price)")
	}
}

func TestCheckConstraintMultiple(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with multiple CHECK constraints
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT,
			quantity INT,
			CHECK (price > 0),
			CHECK (quantity >= 0)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert
	_, err = exec.Execute(`INSERT INTO products (id, price, quantity) VALUES (1, 100, 50)`)
	if err != nil {
		t.Fatal(err)
	}

	// Negative quantity - should fail
	_, err = exec.Execute(`INSERT INTO products (id, price, quantity) VALUES (2, 100, -5)`)
	if err == nil {
		t.Error("Expected error for negative quantity")
	}
}

func TestCheckConstraintWithColumnComparison(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with column comparison in CHECK
	_, err := exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			min_qty INT,
			max_qty INT,
			CHECK (max_qty > min_qty)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert
	_, err = exec.Execute(`INSERT INTO orders (id, min_qty, max_qty) VALUES (1, 10, 100)`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConstraintNamed(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with named CHECK constraint
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT,
			CONSTRAINT chk_positive_price CHECK (price > 0)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Violation should mention constraint name
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (1, -5)`)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation")
	}
}

// ============================================================================
// FOREIGN KEY Constraint Tests
// ============================================================================

func TestForeignKeyConstraintOnInsert(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with FOREIGN KEY
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			amount INT,
			FOREIGN KEY (user_id) REFERENCES users(id)
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

	// Insert order with valid user_id - should succeed
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)`)
	if err != nil {
		t.Fatalf("Valid insert should succeed: %v", err)
	}

	// Insert order with invalid user_id - should fail
	_, err = exec.Execute(`INSERT INTO orders (id, user_id, amount) VALUES (2, 999, 200)`)
	if err == nil {
		t.Error("Expected error for FOREIGN KEY constraint violation")
	}
}

func TestForeignKeyConstraintWithNull(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Create child table with nullable FK
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert with NULL user_id - should succeed (NULL is allowed in FK)
	_, err = exec.Execute(`INSERT INTO orders (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Insert with NULL FK should succeed: %v", err)
	}
}

func TestForeignKeyConstraintNonExistentTable(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with FK to non-existent table - table creation should succeed
	// (FK validation happens on INSERT)
	_, err := exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			user_id INT,
			FOREIGN KEY (user_id) REFERENCES nonexistent(id)
		)
	`)
	if err != nil {
		t.Fatalf("Table creation should succeed: %v", err)
	}

	// Insert should fail because referenced table doesn't exist
	_, err = exec.Execute(`INSERT INTO orders (id, user_id) VALUES (1, 1)`)
	if err == nil {
		t.Error("Expected error for FK to non-existent table")
	}
}

func TestForeignKeyConstraintMultiple(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create parent tables
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
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Create table with multiple FKs
	_, err = exec.Execute(`
		CREATE TABLE order_items (
			id SEQ PRIMARY KEY,
			user_id INT,
			product_id INT,
			FOREIGN KEY (user_id) REFERENCES users(id),
			FOREIGN KEY (product_id) REFERENCES products(id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert references
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name) VALUES (1, 'Widget')`)
	if err != nil {
		t.Fatal(err)
	}

	// Valid insert
	_, err = exec.Execute(`INSERT INTO order_items (id, user_id, product_id) VALUES (1, 1, 1)`)
	if err != nil {
		t.Fatal(err)
	}

	// Invalid user_id
	_, err = exec.Execute(`INSERT INTO order_items (id, user_id, product_id) VALUES (2, 999, 1)`)
	if err == nil {
		t.Error("Expected error for invalid user_id")
	}

	// Invalid product_id
	_, err = exec.Execute(`INSERT INTO order_items (id, user_id, product_id) VALUES (3, 1, 999)`)
	if err == nil {
		t.Error("Expected error for invalid product_id")
	}
}

// ============================================================================
// DESCRIBE Statement Tests
// ============================================================================

func TestDescribeTable(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE,
			status VARCHAR(20) DEFAULT 'active'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// DESCRIBE table
	result, err := exec.Execute(`DESCRIBE users`)
	if err != nil {
		t.Fatalf("DESCRIBE should succeed: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Check first column (id)
	if result.Rows[0][0] != "id" {
		t.Errorf("Expected first column name 'id', got %v", result.Rows[0][0])
	}
	if result.Rows[0][3] != "PRI" {
		t.Errorf("Expected id to have PRI key, got %v", result.Rows[0][3])
	}

	// Check second column (name) - NOT NULL
	if result.Rows[1][2] != "NO" {
		t.Errorf("Expected name to be NOT NULL, got %v", result.Rows[1][2])
	}
}

func TestDescTable(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// DESC table (short form)
	result, err := exec.Execute(`DESC products`)
	if err != nil {
		t.Fatalf("DESC should succeed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
}

func TestDescribeWithCheckConstraint(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with CHECK constraint
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT,
			CHECK (price > 0)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// SHOW CREATE TABLE should include CHECK constraint
	result, err := exec.Execute(`SHOW CREATE TABLE products`)
	if err != nil {
		t.Fatal(err)
	}

	createStmt := result.Rows[0][1].(string)
	if !strings.Contains(createStmt, "CHECK") {
		t.Error("SHOW CREATE TABLE should include CHECK constraint")
	}
}

func TestDescribeNonExistentTable(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	_, err := exec.Execute(`DESCRIBE nonexistent`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ============================================================================
// SHOW CREATE TABLE Tests
// ============================================================================

func TestShowCreateTable(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// SHOW CREATE TABLE
	result, err := exec.Execute(`SHOW CREATE TABLE users`)
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE should succeed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Check table name
	if result.Rows[0][0] != "users" {
		t.Errorf("Expected table name 'users', got %v", result.Rows[0][0])
	}

	// Check that CREATE TABLE statement contains expected elements
	createStmt := result.Rows[0][1].(string)
	if !strings.Contains(createStmt, "CREATE TABLE") {
		t.Error("CREATE TABLE statement missing CREATE TABLE keyword")
	}
	if !strings.Contains(createStmt, "id") {
		t.Error("CREATE TABLE statement missing column 'id'")
	}
	if !strings.Contains(createStmt, "PRIMARY KEY") {
		t.Error("CREATE TABLE statement missing PRIMARY KEY")
	}
}

func TestShowCreateTableWithConstraints(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with CHECK constraint
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT,
			CHECK (price > 0)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// SHOW CREATE TABLE
	result, err := exec.Execute(`SHOW CREATE TABLE products`)
	if err != nil {
		t.Fatal(err)
	}

	createStmt := result.Rows[0][1].(string)
	if !strings.Contains(createStmt, "CHECK") {
		t.Error("CREATE TABLE statement missing CHECK constraint")
	}
}

func TestShowCreateTableNonExistent(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	_, err := exec.Execute(`SHOW CREATE TABLE nonexistent`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ============================================================================
// ALTER TABLE ADD CONSTRAINT Tests
// ============================================================================

func TestAlterTableAddCheckConstraint(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table without constraints
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert valid data
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (1, 100)`)
	if err != nil {
		t.Fatal(err)
	}

	// Add CHECK constraint
	_, err = exec.Execute(`ALTER TABLE products ADD CONSTRAINT chk_price CHECK (price > 0)`)
	if err != nil {
		t.Fatalf("ADD CONSTRAINT should succeed: %v", err)
	}

	// Verify constraint is enforced - invalid insert should fail
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (2, -5)`)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation")
	}

	// Valid insert should still succeed
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (3, 50)`)
	if err != nil {
		t.Errorf("Valid insert should succeed: %v", err)
	}
}

func TestAlterTableAddUniqueConstraint(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
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

	// Insert data
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (1, 'test@example.com')`)
	if err != nil {
		t.Fatal(err)
	}

	// Add UNIQUE constraint
	_, err = exec.Execute(`ALTER TABLE users ADD UNIQUE (email)`)
	if err != nil {
		t.Fatalf("ADD UNIQUE should succeed: %v", err)
	}

	// Duplicate should fail
	_, err = exec.Execute(`INSERT INTO users (id, email) VALUES (2, 'test@example.com')`)
	if err == nil {
		t.Error("Expected error for duplicate UNIQUE value")
	}
}

// ============================================================================
// ALTER TABLE DROP CONSTRAINT Tests
// ============================================================================

func TestAlterTableDropCheckConstraint(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table with named CHECK constraint
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price INT,
			CONSTRAINT chk_positive_price CHECK (price > 0)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify constraint exists
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (1, -5)`)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation")
	}

	// Drop constraint
	_, err = exec.Execute(`ALTER TABLE products DROP CONSTRAINT chk_positive_price`)
	if err != nil {
		t.Fatalf("DROP CONSTRAINT should succeed: %v", err)
	}

	// Now negative price should be allowed
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (2, -5)`)
	if err != nil {
		t.Errorf("Insert should succeed after dropping constraint: %v", err)
	}
}

func TestAlterTableDropNonExistentConstraint(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to drop non-existent constraint
	_, err = exec.Execute(`ALTER TABLE users DROP CONSTRAINT nonexistent`)
	if err == nil {
		t.Error("Expected error for non-existent constraint")
	}
}

// ============================================================================
// Backup and Restore Tests
// ============================================================================

func TestBackupDatabase(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	backupDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "test_backup")
	// Use forward slashes in SQL string
	backupPathSQL := strings.ReplaceAll(backupPath, "\\", "/")

	result, err := exec.Execute(fmt.Sprintf("BACKUP DATABASE TO '%s'", backupPathSQL))
	if err != nil {
		t.Fatalf("BACKUP should succeed: %v", err)
	}

	t.Logf("Backup result: %s", result.Message)

	if result.Message == "" {
		t.Error("Expected backup message")
	}

	// Verify backup file exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		// Try listing files in backup directory
		entries, _ := os.ReadDir(backupDir)
		t.Logf("Files in backup dir: %v", entries)
		t.Error("Backup file was not created")
	}
}

func TestBackupDatabaseCompressed(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create backup with compression
	backupDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "compressed_backup")
	backupPathSQL := strings.ReplaceAll(backupPath, "\\", "/")

	result, err := exec.Execute(fmt.Sprintf("BACKUP DATABASE TO '%s' WITH COMPRESS", backupPathSQL))
	if err != nil {
		t.Fatalf("BACKUP WITH COMPRESS should succeed: %v", err)
	}

	if result.Message == "" {
		t.Error("Expected backup message")
	}

	// Verify compressed backup file exists (with .xbak extension)
	compressedPath := backupPath + ".xbak"
	if _, err := os.Stat(compressedPath); os.IsNotExist(err) {
		t.Error("Compressed backup file was not created")
	}
}

func TestRestoreDatabase(t *testing.T) {
	exec, cleanup := setupConstraintTest(t)
	defer cleanup()

	// Create table and data
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	backupDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := backupDir + "/test_backup"

	_, err = exec.Execute(fmt.Sprintf("BACKUP DATABASE TO '%s'", backupPath))
	if err != nil {
		t.Fatalf("BACKUP failed: %v", err)
	}

	// Create new executor for restore
	restoreDir, err := os.MkdirTemp("", "restore-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(restoreDir)

	restoreEngine := storage.NewEngine(restoreDir)
	restoreExec := NewExecutor(restoreEngine)

	// Restore
	result, err := restoreExec.Execute(fmt.Sprintf("RESTORE DATABASE FROM '%s'", backupPath))
	if err != nil {
		t.Fatalf("RESTORE should succeed: %v", err)
	}

	if result.Message == "" {
		t.Error("Expected restore message")
	}
}
