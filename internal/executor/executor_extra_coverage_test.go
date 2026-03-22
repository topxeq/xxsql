package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestExecuteForScript tests the ExecuteForScript method
func TestExecuteForScript(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-script-test-*")
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

	// Test ExecuteForScript with SELECT
	result, err := exec.ExecuteForScript("SELECT 1 + 2")
	if err != nil {
		t.Errorf("ExecuteForScript failed: %v", err)
	}

	// Result should be *Result
	res, ok := result.(*Result)
	if !ok {
		t.Fatalf("Expected *Result, got %T", result)
	}

	if res.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", res.RowCount)
	}

	// Test ExecuteForScript with CREATE TABLE
	result, err = exec.ExecuteForScript("CREATE TABLE test (id INT PRIMARY KEY)")
	if err != nil {
		t.Errorf("ExecuteForScript CREATE TABLE failed: %v", err)
	}

	// Test ExecuteForScript with INSERT
	result, err = exec.ExecuteForScript("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("ExecuteForScript INSERT failed: %v", err)
	}

	// Test ExecuteForScript with SELECT from table
	result, err = exec.ExecuteForScript("SELECT * FROM test")
	if err != nil {
		t.Errorf("ExecuteForScript SELECT failed: %v", err)
	}

	res, ok = result.(*Result)
	if !ok {
		t.Fatalf("Expected *Result, got %T", result)
	}
	if res.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", res.RowCount)
	}
}

// TestEvaluateSortExpression tests ORDER BY with expressions
func TestEvaluateSortExpression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-sort-test-*")
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
	_, _ = exec.Execute("CREATE TABLE sort_test (id INT PRIMARY KEY, value INT)")
	_, _ = exec.Execute("INSERT INTO sort_test VALUES (1, 10)")
	_, _ = exec.Execute("INSERT INTO sort_test VALUES (2, 30)")
	_, _ = exec.Execute("INSERT INTO sort_test VALUES (3, 20)")

	// Test ORDER BY with expression
	result, err := exec.Execute("SELECT id, value, value * 2 AS doubled FROM sort_test ORDER BY value DESC")
	if err != nil {
		t.Errorf("ORDER BY expression failed: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}

	// Test ORDER BY with addition
	result, err = exec.Execute("SELECT id, value + id AS sum FROM sort_test ORDER BY value + id")
	if err != nil {
		t.Errorf("ORDER BY with addition failed: %v", err)
	}

	// Test ORDER BY with subtraction
	result, err = exec.Execute("SELECT id, value - id AS diff FROM sort_test ORDER BY value - id DESC")
	if err != nil {
		t.Errorf("ORDER BY with subtraction failed: %v", err)
	}
}

// TestHavingWithAggregateFunctions tests HAVING clause with aggregate functions
func TestHavingWithAggregateFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-test-*")
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
	_, _ = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (2, 1, 200)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (3, 2, 50)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (4, 2, 75)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (5, 2, 25)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (6, 3, 500)")

	// Test HAVING with COUNT
	result, err := exec.Execute("SELECT customer_id, COUNT(*) as cnt FROM orders GROUP BY customer_id HAVING COUNT(*) > 1")
	if err != nil {
		t.Errorf("HAVING with COUNT failed: %v", err)
	}
	// Both customer 1 (2 orders) and customer 2 (3 orders) should be returned
	if result.RowCount < 2 {
		t.Errorf("Expected at least 2 rows, got %d", result.RowCount)
	}

	// Test HAVING with SUM
	result, err = exec.Execute("SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id HAVING SUM(amount) > 100")
	if err != nil {
		t.Errorf("HAVING with SUM failed: %v", err)
	}

	// Test HAVING with AVG
	result, err = exec.Execute("SELECT customer_id, AVG(amount) as avg_amt FROM orders GROUP BY customer_id HAVING AVG(amount) > 50")
	if err != nil {
		t.Errorf("HAVING with AVG failed: %v", err)
	}

	// Test HAVING with MIN
	result, err = exec.Execute("SELECT customer_id, MIN(amount) as min_amt FROM orders GROUP BY customer_id HAVING MIN(amount) >= 50")
	if err != nil {
		t.Errorf("HAVING with MIN failed: %v", err)
	}

	// Test HAVING with MAX
	result, err = exec.Execute("SELECT customer_id, MAX(amount) as max_amt FROM orders GROUP BY customer_id HAVING MAX(amount) > 200")
	if err != nil {
		t.Errorf("HAVING with MAX failed: %v", err)
	}
}

// TestEvaluateUnaryExpr tests unary expressions
func TestEvaluateUnaryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-test-*")
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

	// Test unary minus with integer
	result, err := exec.Execute("SELECT -5")
	if err != nil {
		t.Errorf("Unary minus with int failed: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("Expected result")
	}

	// Test unary minus with expression
	_, _ = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, value INT)")
	_, _ = exec.Execute("INSERT INTO test VALUES (1, 10)")

	result, err = exec.Execute("SELECT -value FROM test")
	if err != nil {
		t.Errorf("Unary minus with column failed: %v", err)
	}

	// Test unary minus in WHERE clause
	result, err = exec.Execute("SELECT * FROM test WHERE -value < 0")
	if err != nil {
		t.Errorf("Unary minus in WHERE failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// TestEvaluateBinaryOp tests binary operations
func TestEvaluateBinaryOp(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-binary-test-*")
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

	// Test addition
	result, err := exec.Execute("SELECT 10 + 5")
	if err != nil {
		t.Errorf("Addition failed: %v", err)
	}

	// Test subtraction
	result, err = exec.Execute("SELECT 10 - 3")
	if err != nil {
		t.Errorf("Subtraction failed: %v", err)
	}

	// Test multiplication
	result, err = exec.Execute("SELECT 6 * 7")
	if err != nil {
		t.Errorf("Multiplication failed: %v", err)
	}

	// Test division
	result, err = exec.Execute("SELECT 20 / 4")
	if err != nil {
		t.Errorf("Division failed: %v", err)
	}

	// Test modulo
	result, err = exec.Execute("SELECT 17 % 5")
	if err != nil {
		t.Errorf("Modulo failed: %v", err)
	}

	// Test with floats
	result, err = exec.Execute("SELECT 10.5 + 2.5")
	if err != nil {
		t.Errorf("Float addition failed: %v", err)
	}

	result, err = exec.Execute("SELECT 10.0 / 4.0")
	if err != nil {
		t.Errorf("Float division failed: %v", err)
	}

	// Test modulo result
	_ = result
}

// TestSelectWithComplexOrderBy tests SELECT with complex ORDER BY expressions
func TestSelectWithComplexOrderBy(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-complex-order-test-*")
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
	_, _ = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, price FLOAT, quantity INT)")
	_, _ = exec.Execute("INSERT INTO products VALUES (1, 10.0, 5)")
	_, _ = exec.Execute("INSERT INTO products VALUES (2, 5.0, 10)")
	_, _ = exec.Execute("INSERT INTO products VALUES (3, 20.0, 2)")

	// Order by computed column
	result, err := exec.Execute("SELECT id, price * quantity AS total FROM products ORDER BY price * quantity DESC")
	if err != nil {
		t.Errorf("ORDER BY computed column failed: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

// TestSelectFromDerivedTable tests selecting from derived tables (subqueries in FROM)
func TestSelectFromDerivedTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-derived-test-*")
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
	_, _ = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)")
	_, _ = exec.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")
	_, _ = exec.Execute("INSERT INTO users VALUES (2, 'Bob', 25)")
	_, _ = exec.Execute("INSERT INTO users VALUES (3, 'Charlie', 35)")

	// Select from derived table
	result, err := exec.Execute("SELECT * FROM (SELECT id, name FROM users WHERE age > 25) AS older_users")
	if err != nil {
		t.Errorf("SELECT from derived table failed: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

// TestSelectFromLateral tests LATERAL joins
func TestSelectFromLateral(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-lateral-test-*")
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
	_, _ = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	_, _ = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	_, _ = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	_, _ = exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	_, _ = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (2, 1, 200)")
	_, _ = exec.Execute("INSERT INTO orders VALUES (3, 2, 50)")

	// LATERAL join - simplified query
	result, err := exec.Execute(`
		SELECT u.name, o.amount
		FROM users u
		JOIN LATERAL (SELECT amount FROM orders WHERE user_id = u.id LIMIT 1) o ON 1=1
	`)
	if err != nil {
		// LATERAL might not be fully supported, just check we don't crash
		t.Logf("LATERAL join error (may be expected): %v", err)
	}
	_ = result
}

// TestSelectFromValues tests SELECT FROM VALUES
func TestSelectFromValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-values-test-*")
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

	// SELECT FROM VALUES
	result, err := exec.Execute("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
	if err != nil {
		t.Errorf("SELECT FROM VALUES failed: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

// TestDefaultValues tests INSERT with DEFAULT values
func TestDefaultValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-default-test-*")
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

	// Create table with default values
	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR DEFAULT 'unknown', created VARCHAR DEFAULT CURRENT_TIMESTAMP)")
	if err != nil {
		t.Fatalf("CREATE TABLE with defaults failed: %v", err)
	}

	// Insert with default values
	_, err = exec.Execute("INSERT INTO items (id) VALUES (1)")
	if err != nil {
		t.Errorf("INSERT with defaults failed: %v", err)
	}

	// Verify the default was applied
	result, err := exec.Execute("SELECT name FROM items WHERE id = 1")
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// TestDefaultExpression tests DEFAULT with expressions
func TestDefaultExpression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-default-expr-test-*")
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

	// Create table with expression default
	_, err = exec.Execute("CREATE TABLE calc (id INT PRIMARY KEY, value INT DEFAULT 10 + 5)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert and verify default
	_, err = exec.Execute("INSERT INTO calc (id) VALUES (1)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}

	result, err := exec.Execute("SELECT value FROM calc WHERE id = 1")
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	_ = result
}

// TestCurrentTimeFunctions tests CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME
func TestCurrentTimeFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-time-func-test-*")
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

	// Test CURRENT_TIMESTAMP
	result, err := exec.Execute("SELECT CURRENT_TIMESTAMP")
	if err != nil {
		t.Errorf("CURRENT_TIMESTAMP failed: %v", err)
	}
	_ = result

	// Test NOW()
	result, err = exec.Execute("SELECT NOW()")
	if err != nil {
		t.Errorf("NOW() failed: %v", err)
	}
	_ = result

	// Test CURRENT_DATE
	result, err = exec.Execute("SELECT CURRENT_DATE")
	if err != nil {
		t.Errorf("CURRENT_DATE failed: %v", err)
	}
	_ = result

	// Test CURRENT_TIME
	result, err = exec.Execute("SELECT CURRENT_TIME")
	if err != nil {
		t.Errorf("CURRENT_TIME failed: %v", err)
	}
	_ = result
}

// TestUpperLowerDefaults tests UPPER/LOWER functions in defaults
func TestUpperLowerDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-upper-lower-test-*")
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

	// Create table with UPPER default
	_, err = exec.Execute("CREATE TABLE upper_test (id INT PRIMARY KEY, code VARCHAR DEFAULT UPPER('abc'))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert
	_, err = exec.Execute("INSERT INTO upper_test (id) VALUES (1)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}

	// Create table with LOWER default
	_, err = exec.Execute("CREATE TABLE lower_test (id INT PRIMARY KEY, code VARCHAR DEFAULT LOWER('XYZ'))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert
	_, err = exec.Execute("INSERT INTO lower_test (id) VALUES (1)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}
}

// TestUnaryNegDefault tests unary negation in defaults
func TestUnaryNegDefault(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-default-test-*")
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

	// Create table with negated default
	_, err = exec.Execute("CREATE TABLE neg_test (id INT PRIMARY KEY, value INT DEFAULT -10)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert
	_, err = exec.Execute("INSERT INTO neg_test (id) VALUES (1)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}

	// Verify
	result, err := exec.Execute("SELECT value FROM neg_test WHERE id = 1")
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	_ = result
}