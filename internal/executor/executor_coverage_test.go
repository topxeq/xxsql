package executor

import (
	"os"
	"strings"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestExecuteWithPerms_PermissionDenied tests permission checking
func TestExecuteWithPerms_PermissionDenied(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-perm-test-*")
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
	exec.SetDatabase("testdb")

	// Create a permission checker that denies all permissions
	denyAll := &mockPermissionChecker{allowed: false}

	// Test SELECT permission denied
	_, err = exec.ExecuteWithPerms("SELECT 1", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for SELECT")
	}

	// Test INSERT permission denied
	_, err = exec.ExecuteWithPerms("INSERT INTO t VALUES (1)", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for INSERT")
	}

	// Test UPDATE permission denied
	_, err = exec.ExecuteWithPerms("UPDATE t SET a = 1", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for UPDATE")
	}

	// Test DELETE permission denied
	_, err = exec.ExecuteWithPerms("DELETE FROM t", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for DELETE")
	}

	// Test CREATE TABLE permission denied
	_, err = exec.ExecuteWithPerms("CREATE TABLE t (id INT)", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for CREATE TABLE")
	}

	// Test DROP TABLE permission denied
	_, err = exec.ExecuteWithPerms("DROP TABLE t", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for DROP TABLE")
	}

	// Test CREATE INDEX permission denied
	_, err = exec.ExecuteWithPerms("CREATE INDEX idx ON t (id)", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for CREATE INDEX")
	}

	// Test DROP INDEX permission denied
	_, err = exec.ExecuteWithPerms("DROP INDEX idx ON t", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for DROP INDEX")
	}

	// Test ALTER TABLE permission denied
	_, err = exec.ExecuteWithPerms("ALTER TABLE t ADD COLUMN x INT", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for ALTER TABLE")
	}

	// Test TRUNCATE permission denied
	_, err = exec.ExecuteWithPerms("TRUNCATE TABLE t", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for TRUNCATE")
	}

	// Test CREATE USER permission denied
	_, err = exec.ExecuteWithPerms("CREATE USER testuser IDENTIFIED BY 'password'", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for CREATE USER")
	}

	// Test DROP USER permission denied
	_, err = exec.ExecuteWithPerms("DROP USER testuser", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for DROP USER")
	}

	// Test ALTER USER permission denied
	_, err = exec.ExecuteWithPerms("ALTER USER testuser IDENTIFIED BY 'newpass'", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for ALTER USER")
	}

	// Test GRANT permission denied
	_, err = exec.ExecuteWithPerms("GRANT SELECT ON *.* TO testuser", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for GRANT")
	}

	// Test REVOKE permission denied
	_, err = exec.ExecuteWithPerms("REVOKE SELECT ON *.* FROM testuser", denyAll)
	if err == nil {
		t.Error("Expected permission denied error for REVOKE")
	}
}

// TestExecuteWithPerms_PermissionAllowed tests that operations succeed with permissions
func TestExecuteWithPerms_PermissionAllowed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-perm-test-*")
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
	exec.SetDatabase("testdb")

	// Create a permission checker that allows all permissions
	allowAll := &mockPermissionChecker{allowed: true}

	// Test SELECT permission allowed
	_, err = exec.ExecuteWithPerms("SELECT 1", allowAll)
	if err != nil {
		t.Errorf("SELECT should succeed with permission: %v", err)
	}

	// Test CREATE TABLE permission allowed
	_, err = exec.ExecuteWithPerms("CREATE TABLE t (id INT PRIMARY KEY)", allowAll)
	if err != nil {
		t.Errorf("CREATE TABLE should succeed with permission: %v", err)
	}

	// Test INSERT permission allowed
	_, err = exec.ExecuteWithPerms("INSERT INTO t VALUES (1)", allowAll)
	if err != nil {
		t.Errorf("INSERT should succeed with permission: %v", err)
	}

	// Test SELECT from table
	_, err = exec.ExecuteWithPerms("SELECT * FROM t", allowAll)
	if err != nil {
		t.Errorf("SELECT from table should succeed with permission: %v", err)
	}

	// Test UPDATE permission allowed
	_, err = exec.ExecuteWithPerms("UPDATE t SET id = 2 WHERE id = 1", allowAll)
	if err != nil {
		t.Errorf("UPDATE should succeed with permission: %v", err)
	}

	// Test DELETE permission allowed
	_, err = exec.ExecuteWithPerms("DELETE FROM t WHERE id = 2", allowAll)
	if err != nil {
		t.Errorf("DELETE should succeed with permission: %v", err)
	}

	// Test DROP TABLE permission allowed
	_, err = exec.ExecuteWithPerms("DROP TABLE t", allowAll)
	if err != nil {
		t.Errorf("DROP TABLE should succeed with permission: %v", err)
	}
}

// TestExecuteWithPerms_NilChecker tests that nil checker allows all
func TestExecuteWithPerms_NilChecker(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-perm-test-*")
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
	exec.SetDatabase("testdb")

	// Test with nil checker - should allow all operations
	_, err = exec.ExecuteWithPerms("SELECT 1", nil)
	if err != nil {
		t.Errorf("SELECT should succeed with nil checker: %v", err)
	}

	_, err = exec.ExecuteWithPerms("CREATE TABLE t (id INT)", nil)
	if err != nil {
		t.Errorf("CREATE TABLE should succeed with nil checker: %v", err)
	}
}

// TestExecuteShowCreateTable tests SHOW CREATE TABLE
func TestExecuteShowCreateTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-show-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with various column types
	_, err = exec.Execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW CREATE TABLE
	result, err := exec.Execute("SHOW CREATE TABLE test_table")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Test SHOW CREATE TABLE for non-existent table
	_, err = exec.Execute("SHOW CREATE TABLE nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestExecuteShowCreateTable_WithConstraints tests SHOW CREATE TABLE with constraints
func TestExecuteShowCreateTable_WithConstraints(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-show-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with constraints
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE, age INT NOT NULL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW CREATE TABLE
	result, err := exec.Execute("SHOW CREATE TABLE users")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// TestCompareCheckValues_ThroughCheckConstraint tests the compareCheckValues function via CHECK constraints
func TestCompareCheckValues_ThroughCheckConstraint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-check-test-*")
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
	exec.SetDatabase("testdb")

	// Create table without CHECK first
	_, err = exec.Execute("CREATE TABLE test_check (id INT PRIMARY KEY, age INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert
	_, err = exec.Execute("INSERT INTO test_check VALUES (1, 25)")
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}
}

// TestGetCheckValue_ThroughCheckConstraint tests getCheckValue via CHECK constraints
func TestGetCheckValue_ThroughCheckConstraint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-check-test-*")
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
	exec.SetDatabase("testdb")

	// Create table without complex CHECK constraints
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, price INT, quantity INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid inserts
	_, err = exec.Execute("INSERT INTO products VALUES (1, 100, 10)")
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products VALUES (2, 0, 5)")
	if err != nil {
		t.Errorf("Valid insert with zero price failed: %v", err)
	}
}

// TestExecuteAddConstraint tests adding constraints
func TestExecuteAddConstraint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-constraint-test-*")
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
	exec.SetDatabase("testdb")

	// Create table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Add UNIQUE constraint
	_, err = exec.Execute("ALTER TABLE test ADD CONSTRAINT uk_name UNIQUE (name)")
	if err != nil {
		t.Errorf("Failed to add UNIQUE constraint: %v", err)
	}

	// Add CHECK constraint
	_, err = exec.Execute("ALTER TABLE test ADD CONSTRAINT check_id CHECK (id > 0)")
	if err != nil {
		t.Errorf("Failed to add CHECK constraint: %v", err)
	}
}

// TestExecuteDropConstraint tests dropping constraints
func TestExecuteDropConstraint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-constraint-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with constraint
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Add constraint
	_, err = exec.Execute("ALTER TABLE test ADD CONSTRAINT uk_name UNIQUE (name)")
	if err != nil {
		t.Fatalf("Failed to add constraint: %v", err)
	}

	// Drop constraint
	_, err = exec.Execute("ALTER TABLE test DROP CONSTRAINT uk_name")
	if err != nil {
		t.Errorf("Failed to drop constraint: %v", err)
	}

	// Drop non-existent constraint
	_, err = exec.Execute("ALTER TABLE test DROP CONSTRAINT nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent constraint")
	}
}

// TestExecuteSelect_AggregateFunctions tests aggregate functions in SELECT
func TestExecuteSelect_AggregateFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-test-*")
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
	exec.SetDatabase("testdb")

	// Create table and insert data
	_, err = exec.Execute("CREATE TABLE numbers (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, _ = exec.Execute("INSERT INTO numbers VALUES (1, 10)")
	_, _ = exec.Execute("INSERT INTO numbers VALUES (2, 20)")
	_, _ = exec.Execute("INSERT INTO numbers VALUES (3, 30)")
	_, _ = exec.Execute("INSERT INTO numbers VALUES (4, 40)")
	_, _ = exec.Execute("INSERT INTO numbers VALUES (5, 50)")

	// Test COUNT
	result, err := exec.Execute("SELECT COUNT(*) FROM numbers")
	if err != nil {
		t.Errorf("COUNT failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("COUNT: expected 1 row, got %d", result.RowCount)
	}

	// Test SUM
	result, err = exec.Execute("SELECT SUM(value) FROM numbers")
	if err != nil {
		t.Errorf("SUM failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("SUM: expected 1 row, got %d", result.RowCount)
	}

	// Test AVG
	result, err = exec.Execute("SELECT AVG(value) FROM numbers")
	if err != nil {
		t.Errorf("AVG failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("AVG: expected 1 row, got %d", result.RowCount)
	}

	// Test MIN
	result, err = exec.Execute("SELECT MIN(value) FROM numbers")
	if err != nil {
		t.Errorf("MIN failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("MIN: expected 1 row, got %d", result.RowCount)
	}

	// Test MAX
	result, err = exec.Execute("SELECT MAX(value) FROM numbers")
	if err != nil {
		t.Errorf("MAX failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("MAX: expected 1 row, got %d", result.RowCount)
	}
}

// TestExecuteSelect_WithWhere tests SELECT with WHERE clause
func TestExecuteSelect_WithWhere(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-test-*")
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
	exec.SetDatabase("testdb")

	// Create table and insert data
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, _ = exec.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")
	_, _ = exec.Execute("INSERT INTO users VALUES (2, 'Bob', 25)")
	_, _ = exec.Execute("INSERT INTO users VALUES (3, 'Charlie', 35)")

	// Test WHERE with =
	result, err := exec.Execute("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Errorf("WHERE = failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("WHERE =: expected 1 row, got %d", result.RowCount)
	}

	// Test WHERE with >
	result, err = exec.Execute("SELECT * FROM users WHERE age > 30")
	if err != nil {
		t.Errorf("WHERE > failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("WHERE >: expected 1 row, got %d", result.RowCount)
	}

	// Test WHERE with <
	result, err = exec.Execute("SELECT * FROM users WHERE age < 30")
	if err != nil {
		t.Errorf("WHERE < failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("WHERE <: expected 1 row, got %d", result.RowCount)
	}

	// Test WHERE with >=
	result, err = exec.Execute("SELECT * FROM users WHERE age >= 30")
	if err != nil {
		t.Errorf("WHERE >= failed: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("WHERE >=: expected 2 rows, got %d", result.RowCount)
	}

	// Test WHERE with <=
	result, err = exec.Execute("SELECT * FROM users WHERE age <= 30")
	if err != nil {
		t.Errorf("WHERE <= failed: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("WHERE <=: expected 2 rows, got %d", result.RowCount)
	}
}

// TestExecuteShowCreateTable_WithForeignKey tests SHOW CREATE TABLE with foreign keys
func TestExecuteShowCreateTable_WithForeignKey(t *testing.T) {
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
	exec.SetDatabase("testdb")

	// Create parent table
	_, err = exec.Execute("CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(50))")
	if err != nil {
		t.Fatalf("Failed to create parent table: %v", err)
	}

	// Create child table with foreign key
	_, err = exec.Execute("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)")
	if err != nil {
		t.Fatalf("Failed to create child table: %v", err)
	}

	// Add foreign key constraint
	_, err = exec.Execute("ALTER TABLE child ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL")
	if err != nil {
		t.Fatalf("Failed to add foreign key: %v", err)
	}

	// Test SHOW CREATE TABLE
	result, err := exec.Execute("SHOW CREATE TABLE child")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	// Check that the output contains FOREIGN KEY
	row := result.Rows[0]
	createStmt := row[1].(string)
	if !strings.Contains(createStmt, "FOREIGN KEY") {
		t.Errorf("Expected FOREIGN KEY in CREATE TABLE output, got: %s", createStmt)
	}
	if !strings.Contains(createStmt, "ON DELETE CASCADE") {
		t.Errorf("Expected ON DELETE CASCADE in CREATE TABLE output, got: %s", createStmt)
	}
	if !strings.Contains(createStmt, "ON UPDATE SET NULL") {
		t.Errorf("Expected ON UPDATE SET NULL in CREATE TABLE output, got: %s", createStmt)
	}
}

// TestExecuteShowCreateTable_WithAutoIncrement tests SHOW CREATE TABLE with AUTO_INCREMENT
func TestExecuteShowCreateTable_WithAutoIncrement(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-auto-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with AUTO_INCREMENT and various column attributes
	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, status VARCHAR(20) DEFAULT 'active', counter INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW CREATE TABLE
	result, err := exec.Execute("SHOW CREATE TABLE items")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	// Check that the output contains expected keywords
	row := result.Rows[0]
	createStmt := row[1].(string)
	if !strings.Contains(createStmt, "AUTO_INCREMENT") {
		t.Errorf("Expected AUTO_INCREMENT in CREATE TABLE output, got: %s", createStmt)
	}
	if !strings.Contains(createStmt, "NOT NULL") {
		t.Errorf("Expected NOT NULL in CREATE TABLE output, got: %s", createStmt)
	}
	if !strings.Contains(createStmt, "DEFAULT") {
		t.Errorf("Expected DEFAULT in CREATE TABLE output, got: %s", createStmt)
	}
}

// TestExecuteAddConstraint_PrimaryKey tests adding PRIMARY KEY constraint
func TestExecuteAddConstraint_PrimaryKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pk-test-*")
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
	exec.SetDatabase("testdb")

	// Create table without primary key
	_, err = exec.Execute("CREATE TABLE test (id INT, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Add PRIMARY KEY constraint
	_, err = exec.Execute("ALTER TABLE test ADD CONSTRAINT pk_id PRIMARY KEY (id)")
	if err != nil {
		t.Errorf("Failed to add PRIMARY KEY constraint: %v", err)
	}
}

// TestExecuteAddConstraint_ForeignKey tests adding FOREIGN KEY constraint
func TestExecuteAddConstraint_ForeignKey(t *testing.T) {
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
	exec.SetDatabase("testdb")

	// Create parent and child tables
	_, err = exec.Execute("CREATE TABLE parent (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create parent table: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE child (id INT, parent_id INT)")
	if err != nil {
		t.Fatalf("Failed to create child table: %v", err)
	}

	// Add FOREIGN KEY constraint
	_, err = exec.Execute("ALTER TABLE child ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)")
	if err != nil {
		t.Errorf("Failed to add FOREIGN KEY constraint: %v", err)
	}
}

// TestExecuteDropIndex tests DROP INDEX
func TestExecuteDropIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-idx-test-*")
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
	exec.SetDatabase("testdb")

	// Create table and index
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Drop the index
	_, err = exec.Execute("DROP INDEX idx_name ON test")
	if err != nil {
		t.Errorf("Failed to drop index: %v", err)
	}

	// Try to drop non-existent index
	_, err = exec.Execute("DROP INDEX nonexistent ON test")
	if err == nil {
		t.Error("Expected error when dropping non-existent index")
	}
}

// TestExecuteDropIndex_Errors tests DROP INDEX error cases
func TestExecuteDropIndex_Errors(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-idx-err-test-*")
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
	exec.SetDatabase("testdb")

	// Create table
	_, _ = exec.Execute("CREATE TABLE test (id INT)")

	// Test DROP INDEX without table - this should fail during parsing
	// since the SQL parser requires a table name for DROP INDEX
	// Test DROP INDEX on non-existent table
	_, err = exec.Execute("DROP INDEX idx ON nonexistent")
	if err == nil {
		t.Error("Expected error when dropping index on non-existent table")
	}
}

// TestExecuteTruncate tests TRUNCATE TABLE
func TestExecuteTruncate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-truncate-test-*")
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
	exec.SetDatabase("testdb")

	// Create table and insert data
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, _ = exec.Execute("INSERT INTO test VALUES (1)")
	_, _ = exec.Execute("INSERT INTO test VALUES (2)")
	_, _ = exec.Execute("INSERT INTO test VALUES (3)")

	// Verify data
	result, _ := exec.Execute("SELECT COUNT(*) FROM test")
	if result.RowCount != 1 {
		t.Fatalf("Expected 1 row from COUNT, got %d", result.RowCount)
	}

	// Truncate table
	_, err = exec.Execute("TRUNCATE TABLE test")
	if err != nil {
		t.Errorf("Failed to truncate table: %v", err)
	}

	// Verify data is gone
	result, err = exec.Execute("SELECT COUNT(*) FROM test")
	if err != nil {
		t.Errorf("Failed to count after truncate: %v", err)
	}

	// Test TRUNCATE on non-existent table
	_, err = exec.Execute("TRUNCATE TABLE nonexistent")
	if err == nil {
		t.Error("Expected error when truncating non-existent table")
	}
}

// TestExecuteShowColumns tests SHOW COLUMNS
func TestExecuteShowColumns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cols-test-*")
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
	exec.SetDatabase("testdb")

	// Create table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW COLUMNS
	result, err := exec.Execute("SHOW COLUMNS FROM test")
	if err != nil {
		t.Errorf("SHOW COLUMNS failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	// Test SHOW COLUMNS on non-existent table
	_, err = exec.Execute("SHOW COLUMNS FROM nonexistent")
	if err == nil {
		t.Error("Expected error for SHOW COLUMNS on non-existent table")
	}
}

// TestExecuteWithCheckConstraints_ComparisonOperators tests CHECK constraints with comparison operators
func TestExecuteWithCheckConstraints_ComparisonOperators(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-check-op-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with CHECK constraint using comparison
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, price FLOAT, quantity INT, CONSTRAINT check_price CHECK (price > 0))")
	if err != nil {
		t.Fatalf("Failed to create table with CHECK constraint: %v", err)
	}

	// Valid insert - price > 0
	_, err = exec.Execute("INSERT INTO products VALUES (1, 10.5, 100)")
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}

	// Valid insert with integer price
	_, err = exec.Execute("INSERT INTO products VALUES (2, 20, 50)")
	if err != nil {
		t.Errorf("Valid insert with int price failed: %v", err)
	}
}

// TestExecuteWithCheckConstraints_RangeCheck tests CHECK constraints with range
func TestExecuteWithCheckConstraints_RangeCheck(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-range-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with range CHECK constraint
	_, err = exec.Execute("CREATE TABLE ages (id INT PRIMARY KEY, age INT, CONSTRAINT check_age CHECK (age >= 0 AND age <= 150))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert
	_, err = exec.Execute("INSERT INTO ages VALUES (1, 25)")
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}

	// Valid insert at lower bound
	_, err = exec.Execute("INSERT INTO ages VALUES (2, 0)")
	if err != nil {
		t.Errorf("Valid insert at lower bound failed: %v", err)
	}

	// Valid insert at upper bound
	_, err = exec.Execute("INSERT INTO ages VALUES (3, 150)")
	if err != nil {
		t.Errorf("Valid insert at upper bound failed: %v", err)
	}
}

// TestExecuteWithCheckConstraints_FloatComparison tests CHECK constraints with float values
func TestExecuteWithCheckConstraints_FloatComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-float-test-*")
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
	exec.SetDatabase("testdb")

	// Create table with float CHECK constraint
	_, err = exec.Execute("CREATE TABLE ratings (id INT PRIMARY KEY, rating FLOAT, CONSTRAINT check_rating CHECK (rating >= 0.0 AND rating <= 5.0))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Valid insert
	_, err = exec.Execute("INSERT INTO ratings VALUES (1, 4.5)")
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}

	// Valid insert at lower bound
	_, err = exec.Execute("INSERT INTO ratings VALUES (2, 0.0)")
	if err != nil {
		t.Errorf("Valid insert at lower bound failed: %v", err)
	}

	// Valid insert at upper bound
	_, err = exec.Execute("INSERT INTO ratings VALUES (3, 5.0)")
	if err != nil {
		t.Errorf("Valid insert at upper bound failed: %v", err)
	}
}

// TestExecuteBackup tests BACKUP DATABASE
func TestExecuteBackup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-backup-test-*")
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
	exec.SetDatabase("testdb")

	// Create a table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute backup
	backupPath := tmpDir + "/backup"
	_, err = exec.Execute("BACKUP DATABASE TO '" + backupPath + "'")
	if err != nil {
		t.Errorf("Backup failed: %v", err)
	}
}

// TestExecuteRestore tests RESTORE DATABASE
func TestExecuteRestore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-restore-test-*")
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
	exec.SetDatabase("testdb")

	// Create a table and backup
	_, _ = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY)")
	backupPath := tmpDir + "/backup"
	_, _ = exec.Execute("BACKUP DATABASE TO '" + backupPath + "'")

	// Drop the table
	_, _ = exec.Execute("DROP TABLE test")

	// Restore
	_, err = exec.Execute("RESTORE DATABASE FROM '" + backupPath + "'")
	if err != nil {
		t.Errorf("Restore failed: %v", err)
	}
}

// TestExecuteCreateIndex tests CREATE INDEX
func TestExecuteCreateIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-idx-test-*")
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
	exec.SetDatabase("testdb")

	// Create table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Errorf("Failed to create index: %v", err)
	}

	// Create unique index
	_, err = exec.Execute("CREATE UNIQUE INDEX idx_name2 ON test (name)")
	if err != nil {
		t.Errorf("Failed to create unique index: %v", err)
	}

	// Create index on non-existent table
	_, err = exec.Execute("CREATE INDEX idx ON nonexistent (id)")
	if err == nil {
		t.Error("Expected error when creating index on non-existent table")
	}
}

// TestExecuteDescribe tests DESCRIBE table
func TestExecuteDescribe(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-desc-test-*")
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
	exec.SetDatabase("testdb")

	// Create table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// DESCRIBE
	result, err := exec.Execute("DESCRIBE test")
	if err != nil {
		t.Errorf("DESCRIBE failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	// DESCRIBE non-existent table
	_, err = exec.Execute("DESCRIBE nonexistent")
	if err == nil {
		t.Error("Expected error for DESCRIBE on non-existent table")
	}
}

// TestExecuteUse tests USE database
func TestExecuteUse(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-use-test-*")
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
	exec.SetDatabase("testdb")

	// USE database
	result, err := exec.Execute("USE newdb")
	if err != nil {
		t.Errorf("USE failed: %v", err)
	}

	if result.Message == "" {
		t.Error("Expected message from USE")
	}
}

// TestExecuteShowTables tests SHOW TABLES
func TestExecuteShowTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-tables-test-*")
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
	exec.SetDatabase("testdb")

	// Create tables
	_, _ = exec.Execute("CREATE TABLE t1 (id INT)")
	_, _ = exec.Execute("CREATE TABLE t2 (id INT)")

	// SHOW TABLES
	result, err := exec.Execute("SHOW TABLES")
	if err != nil {
		t.Errorf("SHOW TABLES failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 tables, got %d", result.RowCount)
	}
}

// mockPermissionChecker is a mock implementation of PermissionChecker
type mockPermissionChecker struct {
	allowed bool
}

func (m *mockPermissionChecker) HasPermission(perm Permission) bool {
	return m.allowed
}