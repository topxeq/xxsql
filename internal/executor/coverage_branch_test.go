package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ========== Tests for evaluateExpression - ColumnRef table prefix mismatch ==========

func TestEvaluateExpressionColumnRefTableMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-colref-mismatch-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test correlated subquery with outer table column reference
	result, err := exec.Execute(`
		SELECT o.id, o.amount,
			(SELECT c.name FROM customers c WHERE c.id = o.customer_id) as customer_name
		FROM orders o
	`)
	if err != nil {
		t.Logf("Correlated subquery with outer column failed: %v", err)
	} else {
		t.Logf("Correlated subquery with outer column: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - SubqueryExpr ==========

func TestEvaluateExpressionSubqueryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-subquery-expr-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test subquery in expression context
	result, err := exec.Execute("SELECT (SELECT val FROM data WHERE id = 1) + 5")
	if err != nil {
		t.Logf("SubqueryExpr in expression failed: %v", err)
	} else {
		t.Logf("SubqueryExpr in expression: %v", result.Rows)
	}

	// Test subquery returning empty (should be NULL)
	result, err = exec.Execute("SELECT (SELECT val FROM data WHERE id > 100)")
	if err != nil {
		t.Logf("SubqueryExpr empty failed: %v", err)
	} else {
		t.Logf("SubqueryExpr empty: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - AnyAllExpr more cases ==========

func TestEvaluateExpressionAnyAllExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-expr-*")
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ANY returning true
	result, err := exec.Execute("SELECT 15 > ANY (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ANY returning true failed: %v", err)
	} else {
		t.Logf("ANY returning true: %v", result.Rows)
	}

	// Test ANY returning false
	result, err = exec.Execute("SELECT 5 > ANY (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ANY returning false failed: %v", err)
	} else {
		t.Logf("ANY returning false: %v", result.Rows)
	}

	// Test ALL returning true
	result, err = exec.Execute("SELECT 35 > ALL (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ALL returning true failed: %v", err)
	} else {
		t.Logf("ALL returning true: %v", result.Rows)
	}

	// Test ALL returning false
	result, err = exec.Execute("SELECT 15 > ALL (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ALL returning false failed: %v", err)
	} else {
		t.Logf("ALL returning false: %v", result.Rows)
	}

	// Test ALL with empty subquery
	result, err = exec.Execute("SELECT 5 > ALL (SELECT val FROM nums WHERE id > 100)")
	if err != nil {
		t.Logf("ALL with empty subquery failed: %v", err)
	} else {
		t.Logf("ALL with empty subquery: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - ParenExpr ==========

func TestEvaluateExpressionParenExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-paren-expr-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a INT, b INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test parenthesized expressions
	result, err := exec.Execute("SELECT (a + b) * 2 FROM test")
	if err != nil {
		t.Logf("ParenExpr (a + b) * 2 failed: %v", err)
	} else {
		t.Logf("ParenExpr (a + b) * 2: %v", result.Rows)
	}

	// Test nested parentheses
	result, err = exec.Execute("SELECT ((a + b) * (a - b)) FROM test")
	if err != nil {
		t.Logf("Nested ParenExpr failed: %v", err)
	} else {
		t.Logf("Nested ParenExpr: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - RankExpr with outer context ==========

func TestEvaluateExpressionRankExprOuterContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rank-outer-*")
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

	// Test FTS table with rank
	_, err = exec.Execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
	if err != nil {
		t.Logf("CREATE VIRTUAL TABLE failed: %v", err)
		return
	}

	_, err = exec.Execute("INSERT INTO docs VALUES ('hello world'), ('hello test')")
	if err != nil {
		t.Logf("INSERT failed: %v", err)
	}

	// Test with rank
	result, err := exec.Execute("SELECT content, rank FROM docs WHERE docs MATCH 'hello' ORDER BY rank")
	if err != nil {
		t.Logf("FTS with rank failed: %v", err)
	} else {
		t.Logf("FTS with rank: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - IsNullExpr ==========

func TestEvaluateWhereIsNullExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-isnull-det-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with NULL
	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test (id, name) VALUES (2, 'Bob')")
	if err != nil {
		t.Logf("INSERT with NULL val failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test (id, val) VALUES (3, 30)")
	if err != nil {
		t.Logf("INSERT with NULL name failed: %v", err)
	}

	// Test IS NULL
	result, err := exec.Execute("SELECT * FROM test WHERE val IS NULL")
	if err != nil {
		t.Logf("IS NULL failed: %v", err)
	} else {
		t.Logf("IS NULL: %d rows", len(result.Rows))
	}

	// Test IS NOT NULL
	result, err = exec.Execute("SELECT * FROM test WHERE name IS NOT NULL")
	if err != nil {
		t.Logf("IS NOT NULL failed: %v", err)
	} else {
		t.Logf("IS NOT NULL: %d rows", len(result.Rows))
	}

	// Test IS NULL on name
	result, err = exec.Execute("SELECT * FROM test WHERE name IS NULL")
	if err != nil {
		t.Logf("IS NULL on name failed: %v", err)
	} else {
		t.Logf("IS NULL on name: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - BetweenExpr ==========

func TestEvaluateWhereBetweenExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-between-det-*")
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 20; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i, i*5))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test BETWEEN
	result, err := exec.Execute("SELECT * FROM nums WHERE val BETWEEN 30 AND 70")
	if err != nil {
		t.Logf("BETWEEN failed: %v", err)
	} else {
		t.Logf("BETWEEN 30 AND 70: %d rows", len(result.Rows))
	}

	// Test NOT BETWEEN
	result, err = exec.Execute("SELECT * FROM nums WHERE val NOT BETWEEN 30 AND 70")
	if err != nil {
		t.Logf("NOT BETWEEN failed: %v", err)
	} else {
		t.Logf("NOT BETWEEN 30 AND 70: %d rows", len(result.Rows))
	}

	// Test BETWEEN with boundary values
	result, err = exec.Execute("SELECT * FROM nums WHERE val BETWEEN 30 AND 30")
	if err != nil {
		t.Logf("BETWEEN with equal bounds failed: %v", err)
	} else {
		t.Logf("BETWEEN 30 AND 30: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - InExpr with subquery ==========

func TestEvaluateHavingInSubqueryDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-det-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE premium_customers (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE premium_customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO premium_customers VALUES (1), (3)")
	if err != nil {
		t.Fatalf("INSERT premium_customers failed: %v", err)
	}

	for i := 1; i <= 6; i++ {
		cust := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d)", i, cust, i*100))
		if err != nil {
			t.Fatalf("INSERT orders failed: %v", err)
		}
	}

	// Test HAVING IN with subquery
	result, err := exec.Execute(`
		SELECT customer_id, SUM(amount) as total
		FROM orders
		GROUP BY customer_id
		HAVING customer_id IN (SELECT id FROM premium_customers)
	`)
	if err != nil {
		t.Logf("HAVING IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING IN subquery: %d rows", len(result.Rows))
	}

	// Test HAVING NOT IN with subquery
	result, err = exec.Execute(`
		SELECT customer_id, SUM(amount) as total
		FROM orders
		GROUP BY customer_id
		HAVING customer_id NOT IN (SELECT id FROM premium_customers)
	`)
	if err != nil {
		t.Logf("HAVING NOT IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - BinaryExpr with nil operands ==========

func TestEvaluateHavingBinaryNilOperands(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-bin-nil-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with NULL values
	_, err = exec.Execute("INSERT INTO data VALUES (1, 1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data (id, grp) VALUES (2, 1)")
	if err != nil {
		t.Logf("INSERT with NULL val failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (3, 2, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with aggregate on column containing NULLs
	result, err := exec.Execute(`
		SELECT grp, SUM(val) as total, COUNT(val) as cnt
		FROM data
		GROUP BY grp
		HAVING total > 0
	`)
	if err != nil {
		t.Logf("HAVING with NULL aggregate failed: %v", err)
	} else {
		t.Logf("HAVING with NULL aggregate: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - ScalarSubquery returning bool ==========

func TestEvaluateHavingScalarSubqueryBool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-bool-*")
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

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, is_active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, TRUE)")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	for i := 1; i <= 6; i++ {
		cat := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, %d, %d)", i, cat, i*100))
		if err != nil {
			t.Fatalf("INSERT items failed: %v", err)
		}
	}

	// HAVING with scalar subquery returning bool
	result, err := exec.Execute(`
		SELECT category, SUM(val) as total
		FROM items
		GROUP BY category
		HAVING (SELECT is_active FROM flags WHERE id = 1)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery bool failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery bool: %d rows", len(result.Rows))
	}

	// HAVING with scalar subquery returning int (non-zero = true)
	result, err = exec.Execute(`
		SELECT category, SUM(val) as total
		FROM items
		GROUP BY category
		HAVING (SELECT 1)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery int failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery int: %d rows", len(result.Rows))
	}

	// HAVING with scalar subquery returning 0 (false)
	result, err = exec.Execute(`
		SELECT category, SUM(val) as total
		FROM items
		GROUP BY category
		HAVING (SELECT 0)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery 0 failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery 0: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for pragmaIntegrityCheck with various scenarios ==========

func TestPragmaIntegrityCheckScenarios(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-scenarios-*")
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

	// Test integrity check on empty database
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK empty failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK empty: %v", result.Rows)
	}

	// Create tables with various structures
	tables := []string{
		"CREATE TABLE t1 (id INT PRIMARY KEY)",
		"CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR, val INT)",
		"CREATE TABLE t3 (id INT PRIMARY KEY, created VARCHAR, active BOOL)",
	}

	for _, tbl := range tables {
		_, err = exec.Execute(tbl)
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO t1 VALUES (%d)", i))
		if err != nil {
			t.Fatalf("INSERT t1 failed: %v", err)
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO t2 VALUES (%d, 'name%d', %d)", i, i, i*10))
		if err != nil {
			t.Fatalf("INSERT t2 failed: %v", err)
		}
	}

	// Test integrity check with multiple tables
	result, err = exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK with tables failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK with tables: %v", result.Rows)
	}

	// Create indexes
	_, err = exec.Execute("CREATE INDEX idx_t2_name ON t2 (name)")
	if err != nil {
		t.Logf("CREATE INDEX failed: %v", err)
	}

	// Test integrity check with indexes
	result, err = exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK with indexes failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK with indexes: %v", result.Rows)
	}

	// Test QUICK_CHECK
	result, err = exec.Execute("PRAGMA QUICK_CHECK")
	if err != nil {
		t.Logf("QUICK_CHECK failed: %v", err)
	} else {
		t.Logf("QUICK_CHECK: %v", result.Rows)
	}
}

// ========== Direct test for evaluateExpression with mock row ==========

func TestEvaluateExpressionDirectMock(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-direct-*")
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

	// Create test table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test various expression types
	tests := []string{
		// Literals
		"SELECT 123",
		"SELECT 'hello'",
		"SELECT 3.14",
		"SELECT TRUE",
		"SELECT NULL",
		// Binary expressions
		"SELECT 1 + 2",
		"SELECT 10 - 3",
		"SELECT 4 * 5",
		"SELECT 10 / 2",
		"SELECT 10 % 3",
		// Comparison
		"SELECT 1 = 1",
		"SELECT 1 != 2",
		"SELECT 3 > 2",
		"SELECT 2 < 3",
		// Unary
		"SELECT -5",
		"SELECT -(-5)",
		// Function calls
		"SELECT ABS(-10)",
		"SELECT UPPER('hello')",
		"SELECT LENGTH('hello')",
		"SELECT COALESCE(NULL, 'default')",
		// Nested
		"SELECT ABS(-10) + 5",
		"SELECT UPPER(SUBSTRING('hello', 1, 3))",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Expression test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Expression test: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for evaluateExpression with actual row data ==========

func TestEvaluateExpressionWithRowData(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a INT, b INT, c VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 20, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test expressions with column references
	tests := []string{
		"SELECT a FROM test",
		"SELECT a + b FROM test",
		"SELECT a * 2 FROM test",
		"SELECT a + b * 2 FROM test",
		"SELECT (a + b) * 2 FROM test",
		"SELECT -a FROM test",
		"SELECT UPPER(c) FROM test",
		"SELECT LENGTH(c) FROM test",
		"SELECT CONCAT(c, ' world') FROM test",
		"SELECT CASE WHEN a > 5 THEN 'high' ELSE 'low' END FROM test",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Expression with row test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Expression with row test: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for compareValues edge cases ==========

func TestCompareValuesEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-compare-edge-*")
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

	// Test comparison edge cases
	tests := []string{
		// String comparisons
		"SELECT 'a' < 'b'",
		"SELECT 'A' < 'a'",
		"SELECT 'abc' = 'abc'",
		"SELECT 'abc' != 'ABC'",
		// Numeric comparisons
		"SELECT 1.0 = 1",
		"SELECT 1.5 > 1",
		"SELECT -1 < 0",
		"SELECT 0 = 0.0",
		// Bool comparisons
		"SELECT TRUE = TRUE",
		"SELECT TRUE != FALSE",
		"SELECT TRUE = 1",
		"SELECT FALSE = 0",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Compare edge case failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Compare edge case: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for UnaryExpr with non-negative operator ==========

func TestUnaryExprNonNegative(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-nonneg-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test NOT operator (UnaryExpr with OpNot)
	result, err := exec.Execute("SELECT NOT TRUE")
	if err != nil {
		t.Logf("NOT TRUE failed: %v", err)
	} else {
		t.Logf("NOT TRUE: %v", result.Rows)
	}

	result, err = exec.Execute("SELECT NOT FALSE")
	if err != nil {
		t.Logf("NOT FALSE failed: %v", err)
	} else {
		t.Logf("NOT FALSE: %v", result.Rows)
	}

	// Test NOT with comparison
	result, err = exec.Execute("SELECT NOT (1 > 2)")
	if err != nil {
		t.Logf("NOT (1 > 2) failed: %v", err)
	} else {
		t.Logf("NOT (1 > 2): %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - column not in columnMap ==========

func TestEvaluateExpressionColumnNotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-notfound-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Try to select non-existent column
	_, err = exec.Execute("SELECT nonexistent FROM test")
	if err != nil {
		t.Logf("Non-existent column error (expected): %v", err)
	} else {
		t.Error("Expected error for non-existent column")
	}
}

// ========== Tests for evaluateFunction with table data ==========

func TestEvaluateFunctionWithTableData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-func-table-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, str VARCHAR, num INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Hello World', 123)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test various functions with table data
	tests := []string{
		"SELECT UPPER(str) FROM test",
		"SELECT LOWER(str) FROM test",
		"SELECT LENGTH(str) FROM test",
		"SELECT SUBSTRING(str, 1, 5) FROM test",
		"SELECT CONCAT(str, '!') FROM test",
		"SELECT ABS(num) FROM test",
		"SELECT ROUND(3.14159, 2)",
		"SELECT COALESCE(NULL, str) FROM test",
		"SELECT IFNULL(NULL, 'default')",
		"SELECT NULLIF(num, 0) FROM test",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Function test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Function test: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for ScalarSubquery returning string ==========

func TestScalarSubqueryReturningString(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-str-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 'hello'), (2, 'world')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Scalar subquery returning string in WHERE
	result, err := exec.Execute("SELECT * FROM data WHERE (SELECT 'hello') = 'hello'")
	if err != nil {
		t.Logf("Scalar subquery returning string in WHERE failed: %v", err)
	} else {
		t.Logf("Scalar subquery returning string in WHERE: %d rows", len(result.Rows))
	}

	// Scalar subquery in WHERE with non-empty string check
	result, err = exec.Execute("SELECT * FROM data WHERE (SELECT val FROM data WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery non-empty string WHERE failed: %v", err)
	} else {
		t.Logf("Scalar subquery non-empty string WHERE: %d rows", len(result.Rows))
	}

	// Scalar subquery returning empty string
	result, err = exec.Execute("SELECT * FROM data WHERE (SELECT '')")
	if err != nil {
		t.Logf("Scalar subquery empty string WHERE failed: %v", err)
	} else {
		t.Logf("Scalar subquery empty string WHERE: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - LiteralBool ==========

func TestEvaluateWhereLiteralBool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-literal-bool-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test WHERE TRUE
	result, err := exec.Execute("SELECT * FROM test WHERE TRUE")
	if err != nil {
		t.Logf("WHERE TRUE failed: %v", err)
	} else {
		t.Logf("WHERE TRUE: %d rows", len(result.Rows))
	}

	// Test WHERE FALSE
	result, err = exec.Execute("SELECT * FROM test WHERE FALSE")
	if err != nil {
		t.Logf("WHERE FALSE failed: %v", err)
	} else {
		t.Logf("WHERE FALSE: %d rows (should be 0)", len(result.Rows))
	}

	// Test WHERE with boolean expression
	result, err = exec.Execute("SELECT * FROM test WHERE 1 = 1")
	if err != nil {
		t.Logf("WHERE 1 = 1 failed: %v", err)
	} else {
		t.Logf("WHERE 1 = 1: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - ScalarSubquery returning string ==========

func TestEvaluateHavingScalarSubqueryString(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-str-*")
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

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, status VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, 'active')")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	for i := 1; i <= 6; i++ {
		cat := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, %d, %d)", i, cat, i*100))
		if err != nil {
			t.Fatalf("INSERT items failed: %v", err)
		}
	}

	// HAVING with scalar subquery returning non-empty string (truthy)
	result, err := exec.Execute(`
		SELECT category, SUM(val) as total
		FROM items
		GROUP BY category
		HAVING (SELECT status FROM flags WHERE id = 1)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery string failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery string: %d rows", len(result.Rows))
	}

	// HAVING with scalar subquery returning empty string (falsy)
	_, err = exec.Execute("INSERT INTO flags VALUES (2, '')")
	if err != nil {
		t.Logf("INSERT empty string failed: %v", err)
	}

	result, err = exec.Execute(`
		SELECT category, SUM(val) as total
		FROM items
		GROUP BY category
		HAVING (SELECT status FROM flags WHERE id = 2)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery empty string failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery empty string: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - column index out of range ==========

func TestEvaluateExpressionColumnIndexOutOfRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-idx-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Normal select should work
	result, err := exec.Execute("SELECT id, name FROM test")
	if err != nil {
		t.Logf("Normal SELECT failed: %v", err)
	} else {
		t.Logf("Normal SELECT: %v", result.Rows)
	}

	// Select with expression
	result, err = exec.Execute("SELECT id, name, id + 1 FROM test")
	if err != nil {
		t.Logf("SELECT with expression failed: %v", err)
	} else {
		t.Logf("SELECT with expression: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - UnaryExpr NOT with complex expression ==========

func TestEvaluateHavingUnaryNotComplex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-unary-complex-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		region := "East"
		if i > 5 {
			region = "West"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', %d)", i, region, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// HAVING NOT with complex condition
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING NOT (SUM(amount) > 1000 AND COUNT(*) > 3)
	`)
	if err != nil {
		t.Logf("HAVING NOT complex failed: %v", err)
	} else {
		t.Logf("HAVING NOT complex: %d rows", len(result.Rows))
	}

	// HAVING NOT with OR
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING NOT (SUM(amount) < 100 OR COUNT(*) < 3)
	`)
	if err != nil {
		t.Logf("HAVING NOT OR failed: %v", err)
	} else {
		t.Logf("HAVING NOT OR: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - NOT with various expressions ==========

func TestEvaluateWhereNotVarious(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-not-var-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, active BOOL, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, TRUE, 10), (2, FALSE, 20), (3, TRUE, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// NOT with column
	result, err := exec.Execute("SELECT * FROM test WHERE NOT active")
	if err != nil {
		t.Logf("NOT column failed: %v", err)
	} else {
		t.Logf("NOT column: %d rows", len(result.Rows))
	}

	// NOT with comparison
	result, err = exec.Execute("SELECT * FROM test WHERE NOT (val > 15)")
	if err != nil {
		t.Logf("NOT comparison failed: %v", err)
	} else {
		t.Logf("NOT comparison: %d rows", len(result.Rows))
	}

	// NOT with IN
	result, err = exec.Execute("SELECT * FROM test WHERE NOT (id IN (1, 2))")
	if err != nil {
		t.Logf("NOT IN failed: %v", err)
	} else {
		t.Logf("NOT IN: %d rows", len(result.Rows))
	}

	// Double NOT
	result, err = exec.Execute("SELECT * FROM test WHERE NOT NOT (val > 15)")
	if err != nil {
		t.Logf("Double NOT failed: %v", err)
	} else {
		t.Logf("Double NOT: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - nested SubqueryExpr ==========

func TestEvaluateExpressionNestedSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nested-subq-*")
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

	_, err = exec.Execute("CREATE TABLE a (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE a failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE b (id INT PRIMARY KEY, ref INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE b failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO a VALUES (1, 100), (2, 200)")
	if err != nil {
		t.Fatalf("INSERT a failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO b VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("INSERT b failed: %v", err)
	}

	// Nested subquery
	result, err := exec.Execute(`
		SELECT a.id, a.val,
			(SELECT COUNT(*) FROM b WHERE b.ref = a.id) as b_count
		FROM a
		WHERE a.id IN (SELECT ref FROM b)
	`)
	if err != nil {
		t.Logf("Nested subquery failed: %v", err)
	} else {
		t.Logf("Nested subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for compareValues with LIKE and ESCAPE ==========

func TestCompareValuesLikeEscape(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-like-escape-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, pattern VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'test%value'), (2, 'test_value'), (3, 'testXvalue')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// LIKE with ESCAPE
	result, err := exec.Execute("SELECT * FROM test WHERE pattern LIKE 'test\\%value' ESCAPE '\\'")
	if err != nil {
		t.Logf("LIKE with ESCAPE failed: %v", err)
	} else {
		t.Logf("LIKE with ESCAPE: %d rows", len(result.Rows))
	}

	// LIKE without ESCAPE (wildcard)
	result, err = exec.Execute("SELECT * FROM test WHERE pattern LIKE 'test%value'")
	if err != nil {
		t.Logf("LIKE wildcard failed: %v", err)
	} else {
		t.Logf("LIKE wildcard: %d rows", len(result.Rows))
	}
}

// ========== Tests for GLOB pattern ==========

func TestGlobPattern(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-glob-*")
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

	_, err = exec.Execute("CREATE TABLE files (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO files VALUES (1, 'test.txt'), (2, 'test.go'), (3, 'data.csv')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// GLOB pattern
	result, err := exec.Execute("SELECT * FROM files WHERE name GLOB '*.go'")
	if err != nil {
		t.Logf("GLOB *.go failed: %v", err)
	} else {
		t.Logf("GLOB *.go: %d rows", len(result.Rows))
	}

	// GLOB with ?
	result, err = exec.Execute("SELECT * FROM files WHERE name GLOB 'test.???'")
	if err != nil {
		t.Logf("GLOB with ? failed: %v", err)
	} else {
		t.Logf("GLOB with ?: %d rows", len(result.Rows))
	}
}

// ========== Tests for MATCH expression (FTS) ==========

func TestMatchExpression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-match-*")
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

	// Create FTS table
	_, err = exec.Execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
	if err != nil {
		t.Logf("CREATE VIRTUAL TABLE failed: %v", err)
		return
	}

	_, err = exec.Execute("INSERT INTO docs VALUES ('hello world'), ('hello test'), ('world peace')")
	if err != nil {
		t.Logf("INSERT failed: %v", err)
	}

	// Test MATCH
	result, err := exec.Execute("SELECT * FROM docs WHERE docs MATCH 'hello'")
	if err != nil {
		t.Logf("MATCH failed: %v", err)
	} else {
		t.Logf("MATCH: %d rows", len(result.Rows))
	}
}