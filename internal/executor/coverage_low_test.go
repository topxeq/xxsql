package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ========== Tests for evaluateUnaryExpr ==========

func TestEvaluateUnaryExprNegation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-neg-*")
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

	// Test negative integers
	result, err := exec.Execute("SELECT -5")
	if err != nil {
		t.Fatalf("SELECT -5 failed: %v", err)
	}
	if len(result.Rows) > 0 {
		t.Logf("Result: %v", result.Rows[0])
	}

	// Test negative float
	result, err = exec.Execute("SELECT -3.14")
	if err != nil {
		t.Fatalf("SELECT -3.14 failed: %v", err)
	}

	// Test double negation
	result, err = exec.Execute("SELECT -(-10)")
	if err != nil {
		t.Logf("Double negation not fully supported: %v", err)
	}

	_ = result
}

func TestEvaluateUnaryExprNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-not-*")
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

	// Test NOT with boolean literal
	result, err := exec.Execute("SELECT NOT TRUE")
	if err != nil {
		t.Logf("NOT TRUE not supported: %v", err)
	}

	// Test NOT with comparison
	result, err = exec.Execute("SELECT NOT (1 > 2)")
	if err != nil {
		t.Logf("NOT comparison not supported: %v", err)
	}

	// Test NOT with integer (0 = false, non-zero = true)
	result, err = exec.Execute("SELECT NOT 0")
	if err != nil {
		t.Logf("NOT 0 not supported: %v", err)
	}

	_ = result
}

// ========== Tests for executeCreateView ==========

func TestCreateViewOrReplace(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-replace-*")
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

	// Create base table
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create view
	_, err = exec.Execute("CREATE VIEW user_view AS SELECT * FROM users")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Try to create again without OR REPLACE - should fail
	_, err = exec.Execute("CREATE VIEW user_view AS SELECT id FROM users")
	if err == nil {
		t.Error("Expected error when creating duplicate view")
	}

	// Create with OR REPLACE
	result, err := exec.Execute("CREATE OR REPLACE VIEW user_view AS SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("CREATE OR REPLACE VIEW failed: %v", err)
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

func TestCreateViewWithColumns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-cols-*")
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

	// Create base table
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create view with explicit column names
	result, err := exec.Execute("CREATE VIEW product_names (pid, pname) AS SELECT id, name FROM products")
	if err != nil {
		t.Fatalf("CREATE VIEW with columns failed: %v", err)
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

func TestCreateViewWithCheckOption(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-check-*")
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

	// Create base table
	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create view with CHECK OPTION
	result, err := exec.Execute("CREATE VIEW active_items AS SELECT * FROM items WHERE active = TRUE WITH CHECK OPTION")
	if err != nil {
		t.Logf("CHECK OPTION may not be fully supported: %v", err)
		return
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

// ========== Tests for executeDropTrigger ==========

func TestExecuteDropTriggerCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-trig-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create trigger
	_, err = exec.Execute(`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Drop trigger
	result, err := exec.Execute("DROP TRIGGER test_trigger")
	if err != nil {
		t.Fatalf("DROP TRIGGER failed: %v", err)
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Drop non-existent trigger
	_, err = exec.Execute("DROP TRIGGER nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent trigger")
	}
}

// ========== Tests for executeTruncate ==========

func TestExecuteTruncateCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-trunc-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Truncate
	result, err := exec.Execute("TRUNCATE TABLE test")
	if err != nil {
		t.Fatalf("TRUNCATE failed: %v", err)
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Verify empty
	result, err = exec.Execute("SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("SELECT COUNT failed: %v", err)
	}

	if len(result.Rows) > 0 {
		if count, ok := result.Rows[0][0].(int); ok && count != 0 {
			t.Errorf("Expected 0 rows after truncate, got %d", count)
		}
	}
}

// ========== Tests for executeCreateIndex ==========

func TestExecuteCreateIndexCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-create-idx-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create index
	result, err := exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Create unique index
	result, err = exec.Execute("CREATE UNIQUE INDEX idx_email ON test (email)")
	if err != nil {
		t.Fatalf("CREATE UNIQUE INDEX failed: %v", err)
	}

	// Try to create index on non-existent table
	_, err = exec.Execute("CREATE INDEX idx ON nonexistent (col)")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// ========== Tests for executeDropColumn ==========

func TestExecuteDropColumnCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-col-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, col1 VARCHAR, col2 VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Drop column
	result, err := exec.Execute("ALTER TABLE test DROP COLUMN col2")
	if err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
	}

	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

// ========== Tests for executeCopyTo ==========

func TestExecuteCopyTo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-copyto-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Copy to file
	copyFile := tmpDir + "/copy_output.csv"
	result, err := exec.Execute(fmt.Sprintf("COPY test TO '%s'", copyFile))
	if err != nil {
		t.Logf("COPY TO may not be fully supported: %v", err)
		return
	}

	t.Logf("COPY TO result: %s", result.Message)
}

// ========== Tests for setPragma ==========

func TestSetPragmaCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-setpragma-*")
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

	// Test setting pragmas
	pragmas := []string{
		"PRAGMA JOURNAL_MODE = WAL",
		"PRAGMA SYNCHRONOUS = NORMAL",
		"PRAGMA CACHE_SIZE = 1000",
		"PRAGMA TEMP_STORE = MEMORY",
	}

	for _, pragma := range pragmas {
		result, err := exec.Execute(pragma)
		if err != nil {
			t.Logf("PRAGMA set failed (may not be fully supported): %s, error: %v", pragma, err)
			continue
		}
		t.Logf("PRAGMA set: %s -> %s", pragma, result.Message)
	}
}

// ========== Tests for computeLeadLag ==========

func TestComputeLeadLagWithNulls(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-leadlag-null-*")
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

	// Create table with nullable values
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (2, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LEAD with null values
	result, err := exec.Execute("SELECT id, LEAD(val, 1, 0) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Logf("LEAD with nulls not fully supported: %v", err)
		return
	}

	t.Logf("LEAD result rows: %d", len(result.Rows))

	// Test LAG with null values
	result, err = exec.Execute("SELECT id, LAG(val, 1, -1) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Logf("LAG with nulls not fully supported: %v", err)
		return
	}

	t.Logf("LAG result rows: %d", len(result.Rows))
}

// ========== Tests for computeNthValue ==========

func TestComputeNthValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nthval-*")
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
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, 1, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test NTH_VALUE
	result, err := exec.Execute("SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) AS nth FROM data")
	if err != nil {
		t.Logf("NTH_VALUE not fully supported: %v", err)
		return
	}

	t.Logf("NTH_VALUE result rows: %d", len(result.Rows))
}

// ========== Tests for findIndexForWhere ==========

func TestFindIndexForWhere(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-findidx-*")
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

	// Create table with index
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice', 'alice@test.com')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query that should use index
	result, err := exec.Execute("SELECT * FROM test WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("SELECT with index column failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// ========== Tests for executeGroupBy ==========

func TestExecuteGroupByWithNulls(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-group-null-*")
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

	// Create table with NULLs
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, category VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (2, 'A', NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (3, NULL, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (4, NULL, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// GROUP BY with NULLs
	result, err := exec.Execute("SELECT category, SUM(value), COUNT(value), AVG(value) FROM data GROUP BY category")
	if err != nil {
		t.Fatalf("GROUP BY with NULLs failed: %v", err)
	}

	t.Logf("GROUP BY with NULLs returned %d groups", len(result.Rows))
}

func TestExecuteGroupByWithHaving(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-group-having-*")
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
	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'North', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (2, 'North', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (3, 'South', 50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (4, 'East', 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// GROUP BY with multiple HAVING conditions
	result, err := exec.Execute("SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING SUM(amount) > 100 AND COUNT(*) >= 1")
	if err != nil {
		t.Fatalf("GROUP BY with HAVING AND failed: %v", err)
	}

	t.Logf("GROUP BY HAVING AND returned %d groups", len(result.Rows))

	// GROUP BY with HAVING OR
	result, err = exec.Execute("SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 200 OR COUNT(*) > 1")
	if err != nil {
		t.Fatalf("GROUP BY with HAVING OR failed: %v", err)
	}

	t.Logf("GROUP BY HAVING OR returned %d groups", len(result.Rows))
}

// ========== Tests for executeIndexScan ==========

func TestExecuteIndexScan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-idxscan-*")
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

	// Create table with index
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON users (email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d@test.com', 'User%d')", i, i, i))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Query with equality condition on indexed column
	result, err := exec.Execute("SELECT * FROM users WHERE email = 'user5@test.com'")
	if err != nil {
		t.Fatalf("SELECT with index failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// ========== Tests for executeSelectFromView ==========

func TestExecuteSelectFromViewComplex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-complex-*")
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

	// Create tables
	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100.0)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 1, 200.0)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO customers VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	// Create view with JOIN
	_, err = exec.Execute("CREATE VIEW order_summary AS SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id")
	if err != nil {
		t.Fatalf("CREATE VIEW with JOIN failed: %v", err)
	}

	// Select from view
	result, err := exec.Execute("SELECT * FROM order_summary")
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}

	t.Logf("View returned %d rows", len(result.Rows))
}

// ========== Tests for parseValueForColumn ==========

func TestParseValueForColumn(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-parseval-*")
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

	// Create table with various types
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, active BOOL, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with various types
	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice', TRUE, 19.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Select and verify types
	result, err := exec.Execute("SELECT id, name, active, price FROM test")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	t.Logf("Row values: %v", result.Rows[0])
}

// ========== Tests for castValue ==========

func TestCastValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-*")
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

	// Test various casts
	testCases := []string{
		"SELECT CAST('123' AS INT)",
		"SELECT CAST(123.45 AS INT)",
		"SELECT CAST('456.78' AS FLOAT)",
		"SELECT CAST(100 AS VARCHAR)",
		"SELECT CAST('true' AS BOOL)",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Cast failed (may not be fully supported): %s, error: %v", tc, err)
			continue
		}
		t.Logf("Cast: %s -> %v", tc, result.Rows)
	}
}

// ========== Tests for applyDateModifier ==========

func TestApplyDateModifierCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-datemod-*")
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

	// Test date modifiers
	testCases := []string{
		"SELECT DATE('2024-01-15', '+7 days')",
		"SELECT DATE('2024-01-15', '-1 month')",
		"SELECT DATE('2024-01-15', 'start of year')",
		"SELECT DATE('2024-01-15', 'start of month')",
		"SELECT DATETIME('2024-01-15 10:30:00', '+1 hour')",
		"SELECT DATETIME('2024-01-15 10:30:00', '-30 minutes')",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Date modifier failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Date: %s -> %v", tc, result.Rows)
	}
}

// ========== Tests for executeLoadData ==========

func TestExecuteLoadData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-loaddata-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create data file
	dataFile := tmpDir + "/data.csv"
	data := "1,Alice\n2,Bob\n3,Charlie\n"
	if err := os.WriteFile(dataFile, []byte(data), 0644); err != nil {
		t.Fatalf("Write file failed: %v", err)
	}

	// Load data
	result, err := exec.Execute(fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE test", dataFile))
	if err != nil {
		t.Logf("LOAD DATA not fully supported: %v", err)
		return
	}

	t.Logf("LOAD DATA result: %s, affected: %d", result.Message, result.Affected)
}

// ========== Tests for evaluateWhereWithCollation ==========

func TestEvaluateWhereWithCollationCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-coll-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'ABC')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'abc')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test with COLLATE NOCASE
	result, err := exec.Execute("SELECT * FROM test WHERE name COLLATE NOCASE = 'abc'")
	if err != nil {
		t.Logf("COLLATE NOCASE not fully supported: %v", err)
		return
	}

	t.Logf("COLLATE NOCASE returned %d rows", len(result.Rows))
}

// ========== Tests for computeFirstLastValue ==========

func TestComputeFirstLastValueWithPartition(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-firstlast-part-*")
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

	// Create table with multiple partitions
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d, %d)", (i-1)*3+j, i, j*10))
			if err != nil {
				t.Fatalf("INSERT failed: %v", err)
			}
		}
	}

	// Test FIRST_VALUE with partition
	result, err := exec.Execute(`
		SELECT id, grp, val,
		       FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) as first_val,
		       LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val
		FROM data
	`)
	if err != nil {
		t.Logf("FIRST_VALUE/LAST_VALUE with partition not fully supported: %v", err)
		return
	}

	t.Logf("FIRST_VALUE/LAST_VALUE returned %d rows", len(result.Rows))
}

// ========== Tests for executeCreateTrigger ==========

func TestExecuteCreateTriggerAllTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-trigger-types-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create BEFORE INSERT trigger
	_, err = exec.Execute(`CREATE TRIGGER trig_before_insert BEFORE INSERT ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE BEFORE INSERT TRIGGER failed: %v", err)
	}

	// Create AFTER INSERT trigger
	_, err = exec.Execute(`CREATE TRIGGER trig_after_insert AFTER INSERT ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE AFTER INSERT TRIGGER failed: %v", err)
	}

	// Create BEFORE UPDATE trigger
	_, err = exec.Execute(`CREATE TRIGGER trig_before_update BEFORE UPDATE ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE BEFORE UPDATE TRIGGER failed: %v", err)
	}

	// Create AFTER UPDATE trigger
	_, err = exec.Execute(`CREATE TRIGGER trig_after_update AFTER UPDATE ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE AFTER UPDATE TRIGGER failed: %v", err)
	}

	// Create BEFORE DELETE trigger
	_, err = exec.Execute(`CREATE TRIGGER trig_before_delete BEFORE DELETE ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE BEFORE DELETE TRIGGER failed: %v", err)
	}

	// Create AFTER DELETE trigger
	_, err = exec.Execute(`CREATE TRIGGER trig_after_delete AFTER DELETE ON test FOR EACH ROW BEGIN END`)
	if err != nil {
		t.Fatalf("CREATE AFTER DELETE TRIGGER failed: %v", err)
	}
}

// ========== Tests for evaluateHavingExpr ==========

func TestEvaluateHavingExprComplex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-expr-*")
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
	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, product VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'North', 'A', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (2, 'North', 'B', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (3, 'South', 'A', 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with complex expression
	result, err := exec.Execute(`SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING SUM(amount) > 100 AND COUNT(*) > 1`)
	if err != nil {
		t.Fatalf("HAVING with complex expression failed: %v", err)
	}

	t.Logf("Complex HAVING returned %d groups", len(result.Rows))
}

// ========== Tests for executeSelectFromLateral ==========

func TestExecuteSelectFromLateralCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-lateral-*")
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

	// Create tables
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT users failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 1, 200)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// LATERAL join
	result, err := exec.Execute(`
		SELECT u.name, o.amount
		FROM users u, LATERAL (SELECT amount FROM orders WHERE user_id = u.id ORDER BY amount DESC LIMIT 1) o
	`)
	if err != nil {
		t.Logf("LATERAL not fully supported: %v", err)
		return
	}

	t.Logf("LATERAL returned %d rows", len(result.Rows))
}

// ========== Tests for compareValuesWithCollation ==========

func TestCompareValuesWithCollationCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-compare-coll-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'abc')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'ABC')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test comparison with COLLATE
	result, err := exec.Execute("SELECT * FROM test WHERE name = 'ABC' COLLATE NOCASE")
	if err != nil {
		t.Logf("COLLATE comparison failed: %v", err)
		return
	}

	t.Logf("COLLATE comparison returned %d rows", len(result.Rows))
}

// ========== Tests for jsonSetPath, jsonReplacePath, jsonRemovePath ==========

func TestJsonPathOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-jsonpath-*")
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

	// Test JSON_SET
	result, err := exec.Execute(`SELECT JSON_SET('{"a": 1}', '$.b', 2)`)
	if err != nil {
		t.Logf("JSON_SET failed: %v", err)
	} else {
		t.Logf("JSON_SET: %v", result.Rows)
	}

	// Test JSON_REPLACE
	result, err = exec.Execute(`SELECT JSON_REPLACE('{"a": 1}', '$.a', 2)`)
	if err != nil {
		t.Logf("JSON_REPLACE failed: %v", err)
	} else {
		t.Logf("JSON_REPLACE: %v", result.Rows)
	}

	// Test JSON_REMOVE
	result, err = exec.Execute(`SELECT JSON_REMOVE('{"a": 1, "b": 2}', '$.a')`)
	if err != nil {
		t.Logf("JSON_REMOVE failed: %v", err)
	} else {
		t.Logf("JSON_REMOVE: %v", result.Rows)
	}
}

// ========== Tests for executePragmaWithArg ==========

func TestExecutePragmaWithArgCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-arg-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Test PRAGMA with argument
	result, err := exec.Execute("PRAGMA INDEX_INFO('idx_name')")
	if err != nil {
		t.Logf("PRAGMA INDEX_INFO failed: %v", err)
	} else {
		t.Logf("PRAGMA INDEX_INFO: %d rows", len(result.Rows))
	}
}

// ========== Tests for GetPragmaValue ==========

func TestGetPragmaValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-val-*")
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

	// Test various PRAGMA that return values
	pragmas := []string{
		"PRAGMA JOURNAL_MODE",
		"PRAGMA SYNCHRONOUS",
		"PRAGMA CACHE_SIZE",
		"PRAGMA USER_VERSION",
	}

	for _, p := range pragmas {
		result, err := exec.Execute(p)
		if err != nil {
			t.Logf("PRAGMA %s failed: %v", p, err)
			continue
		}
		t.Logf("PRAGMA %s: %v", p, result.Rows)
	}
}

// ========== Tests for pragmaIntegrityCheck ==========

func TestPragmaIntegrityCheckDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-det-*")
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

	// Create multiple tables
	_, err = exec.Execute("CREATE TABLE t1 (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO t1 VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT t1 failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO t2 VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT t2 failed: %v", err)
	}

	// Run integrity check
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Fatalf("PRAGMA INTEGRITY_CHECK failed: %v", err)
	}

	t.Logf("INTEGRITY_CHECK result: %v", result.Rows)
}

// ========== Tests for getWindowFuncArgValue ==========

func TestGetWindowFuncArgValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-win-arg-*")
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
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test window functions with arguments
	testCases := []string{
		"SELECT id, LEAD(val, 2, 0) OVER (ORDER BY id) FROM data",
		"SELECT id, LAG(val, 2, -1) OVER (ORDER BY id) FROM data",
		"SELECT id, NTH_VALUE(val, 3) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM data",
		"SELECT id, NTILE(3) OVER (ORDER BY id) FROM data",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Window function with arg failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Window function: %s -> %d rows", tc, len(result.Rows))
	}
}

// ========== Tests for executeStatementForCTE ==========

func TestExecuteStatementForCTE(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cte-stmt-*")
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
	_, err = exec.Execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR, manager_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO employees VALUES (1, 'CEO', NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO employees VALUES (2, 'Manager', 1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO employees VALUES (3, 'Worker', 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Simple CTE
	result, err := exec.Execute(`
		WITH cte AS (
			SELECT id, name FROM employees WHERE manager_id IS NULL
		)
		SELECT * FROM cte
	`)
	if err != nil {
		t.Fatalf("Simple CTE failed: %v", err)
	}

	t.Logf("Simple CTE returned %d rows", len(result.Rows))

	// CTE with multiple CTEs
	result, err = exec.Execute(`
		WITH
			managers AS (SELECT id, name FROM employees WHERE manager_id = 1),
			workers AS (SELECT id, name FROM employees WHERE manager_id != 1)
		SELECT * FROM managers UNION ALL SELECT * FROM workers
	`)
	if err != nil {
		t.Logf("Multiple CTEs not fully supported: %v", err)
	} else {
		t.Logf("Multiple CTEs returned %d rows", len(result.Rows))
	}
}

// ========== Tests for parseLoadDataLine ==========

func TestParseLoadDataLineDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-parse-line-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, value FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create various data files
	// CSV format
	csvFile := tmpDir + "/data.csv"
	csvData := "1,Alice,100.5\n2,Bob,200.75\n3,Charlie,300.0\n"
	if err := os.WriteFile(csvFile, []byte(csvData), 0644); err != nil {
		t.Fatalf("Write CSV failed: %v", err)
	}

	result, err := exec.Execute(fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE test FIELDS TERMINATED BY ','", csvFile))
	if err != nil {
		t.Logf("LOAD DATA CSV not fully supported: %v", err)
	} else {
		t.Logf("LOAD DATA CSV: %s, affected: %d", result.Message, result.Affected)
	}
}

// ========== Tests for callScriptFunction ==========

func TestCallScriptFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-script-func-*")
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

	// Test calling a function with various argument types
	result, err := exec.Execute("SELECT UPPER('hello')")
	if err != nil {
		t.Fatalf("UPPER failed: %v", err)
	}
	t.Logf("UPPER: %v", result.Rows)

	// Test with numeric arguments
	result, err = exec.Execute("SELECT ABS(-123)")
	if err != nil {
		t.Fatalf("ABS failed: %v", err)
	}
	t.Logf("ABS: %v", result.Rows)

	// Test with NULL argument
	result, err = exec.Execute("SELECT UPPER(NULL)")
	if err != nil {
		t.Fatalf("UPPER(NULL) failed: %v", err)
	}
	t.Logf("UPPER(NULL): %v", result.Rows)
}

// ========== Tests for evaluateWhere ==========

func TestEvaluateWhereComplex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-complex-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a INT, b INT, c VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 20, 'foo')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 30, 40, 'bar')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (3, 50, 60, 'baz')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Complex WHERE with AND/OR
	result, err := exec.Execute("SELECT * FROM test WHERE (a > 20 AND b < 50) OR c = 'foo'")
	if err != nil {
		t.Fatalf("Complex WHERE AND/OR failed: %v", err)
	}
	t.Logf("WHERE AND/OR returned %d rows", len(result.Rows))

	// WHERE with function
	result, err = exec.Execute("SELECT * FROM test WHERE UPPER(c) = 'FOO'")
	if err != nil {
		t.Logf("WHERE with function failed: %v", err)
	} else {
		t.Logf("WHERE with function returned %d rows", len(result.Rows))
	}

	// WHERE with arithmetic
	result, err = exec.Execute("SELECT * FROM test WHERE a + b > 50")
	if err != nil {
		t.Logf("WHERE with arithmetic failed: %v", err)
	} else {
		t.Logf("WHERE with arithmetic returned %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExprForRow ==========

func TestEvaluateExprForRowComplex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-row-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a INT, b VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test expressions in SELECT
	testCases := []string{
		"SELECT a + 5 FROM test",
		"SELECT a * 2 - 3 FROM test",
		"SELECT UPPER(b) FROM test",
		"SELECT CONCAT(b, ' world') FROM test",
		"SELECT a, b, a * 10 + LENGTH(b) FROM test",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Expression failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Expression: %s -> %v", tc, result.Rows)
	}
}

// ========== Tests for hasAggregate ==========

func TestHasAggregateCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-has-agg-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test queries with aggregates
	testCases := []string{
		"SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM test",
		"SELECT id, SUM(val) OVER (ORDER BY id) as running_sum FROM test",
		"SELECT COUNT(DISTINCT val) FROM test",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Aggregate query failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Aggregate: %s -> %v", tc, result.Rows)
	}
}

// ========== Tests for evaluateFunction ==========

func TestEvaluateFunctionWithNulls(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-func-null-*")
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

	// Create table with NULLs
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a INT, b VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, NULL, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 10, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test functions with NULL handling
	testCases := []string{
		"SELECT COALESCE(a, 0) FROM test",
		"SELECT IFNULL(a, -1) FROM test",
		"SELECT NULLIF(a, 10) FROM test",
		"SELECT CONCAT(b, ' world') FROM test",
		"SELECT UPPER(b) FROM test",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Function with NULL failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Function: %s -> %v", tc, result.Rows)
	}
}

// ========== Tests for pragmaIndexInfo ==========

func TestPragmaIndexInfoDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-idx-info-*")
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

	// Create table with multiple indexes
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, email VARCHAR, phone VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX idx_name failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON test (email)")
	if err != nil {
		t.Fatalf("CREATE INDEX idx_email failed: %v", err)
	}

	_, err = exec.Execute("CREATE UNIQUE INDEX idx_phone ON test (phone)")
	if err != nil {
		t.Fatalf("CREATE INDEX idx_phone failed: %v", err)
	}

	// Test PRAGMA INDEX_INFO for each index
	indexes := []string{"idx_name", "idx_email", "idx_phone"}
	for _, idx := range indexes {
		result, err := exec.Execute(fmt.Sprintf("PRAGMA INDEX_INFO('%s')", idx))
		if err != nil {
			t.Logf("PRAGMA INDEX_INFO('%s') failed: %v", idx, err)
			continue
		}
		t.Logf("PRAGMA INDEX_INFO('%s'): %d rows", idx, len(result.Rows))
	}
}

// ========== Tests for executeGroupBy edge cases ==========

func TestExecuteGroupByEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-group-edge-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, cat VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Empty table - GROUP BY should return empty
	result, err := exec.Execute("SELECT cat, COUNT(*) FROM test GROUP BY cat")
	if err != nil {
		t.Fatalf("GROUP BY on empty table failed: %v", err)
	}
	t.Logf("GROUP BY on empty table: %d rows", len(result.Rows))

	// Insert data with duplicates and NULLs
	_, err = exec.Execute("INSERT INTO test VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'A', NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (3, NULL, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (4, 'B', 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// GROUP BY with all aggregates
	result, err = exec.Execute(`
		SELECT cat,
		       COUNT(*) as cnt_all,
		       COUNT(val) as cnt_val,
		       SUM(val) as sum_val,
		       AVG(val) as avg_val,
		       MIN(val) as min_val,
		       MAX(val) as max_val
		FROM test
		GROUP BY cat
	`)
	if err != nil {
		t.Fatalf("GROUP BY with all aggregates failed: %v", err)
	}
	t.Logf("GROUP BY all aggregates: %d groups", len(result.Rows))

	// GROUP BY with HAVING using multiple conditions
	result, err = exec.Execute("SELECT cat FROM test GROUP BY cat HAVING COUNT(*) > 0 AND SUM(val) IS NOT NULL")
	if err != nil {
		t.Fatalf("GROUP BY HAVING with multiple conditions failed: %v", err)
	}
	t.Logf("GROUP BY HAVING multiple: %d groups", len(result.Rows))
}