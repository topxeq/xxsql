package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ========== Tests for evaluateHaving - IN with subquery ==========

func TestEvaluateHavingInSubqueryLeft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-left-*")
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

	_, err = exec.Execute("CREATE TABLE groups (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE groups failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE members (id INT PRIMARY KEY, group_id INT, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE members failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO groups VALUES (1, 'A'), (2, 'B')")
	if err != nil {
		t.Fatalf("INSERT groups failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO members VALUES (1, 1, TRUE), (2, 1, FALSE), (3, 2, TRUE)")
	if err != nil {
		t.Fatalf("INSERT members failed: %v", err)
	}

	// Test HAVING with IN subquery
	result, err := exec.Execute(`
		SELECT group_id, COUNT(*) as cnt
		FROM members
		GROUP BY group_id
		HAVING group_id IN (SELECT id FROM groups WHERE name = 'A')
	`)
	if err != nil {
		t.Logf("HAVING IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING IN subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - NOT IN with subquery ==========

func TestEvaluateHavingNotInSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-notin-*")
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

	for i := 1; i <= 6; i++ {
		grp := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d, %d)", i, grp, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING NOT IN with subquery
	result, err := exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING grp NOT IN (SELECT grp FROM data WHERE val > 30)
	`)
	if err != nil {
		t.Logf("HAVING NOT IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - BinaryExpr with nil values ==========

func TestEvaluateHavingWithNullComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-null-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category VARCHAR, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO items VALUES (2, 'A', NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO items VALUES (3, 'B', 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test HAVING with aggregate that might have NULL
	result, err := exec.Execute(`
		SELECT category, SUM(price) as total
		FROM items
		GROUP BY category
		HAVING total > 0
	`)
	if err != nil {
		t.Logf("HAVING with NULL failed: %v", err)
	} else {
		t.Logf("HAVING with NULL: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - IN with parenthesized subquery ==========

func TestEvaluateWhereInParenSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-paren-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, TRUE), (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1), (2, 2), (3, 1)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test WHERE with parenthesized subquery
	result, err := exec.Execute(`
		SELECT * FROM orders
		WHERE customer_id IN (SELECT id FROM customers WHERE active = TRUE)
	`)
	if err != nil {
		t.Logf("WHERE IN paren subquery failed: %v", err)
	} else {
		t.Logf("WHERE IN paren subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - InExpr with value list ==========

func TestEvaluateWhereInExprList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-inlist-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test WHERE with IN value list
	result, err := exec.Execute("SELECT * FROM test WHERE id IN (1, 2)")
	if err != nil {
		t.Logf("WHERE IN list failed: %v", err)
	} else {
		t.Logf("WHERE IN list: %d rows", len(result.Rows))
	}

	// Test WHERE with NOT IN value list
	result, err = exec.Execute("SELECT * FROM test WHERE id NOT IN (1, 2)")
	if err != nil {
		t.Logf("WHERE NOT IN list failed: %v", err)
	} else {
		t.Logf("WHERE NOT IN list: %d rows", len(result.Rows))
	}
}

// ========== Tests for hasAggregate - nested in function args ==========

func TestHasAggregateNestedArgs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-nested-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, %d)", i, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test aggregate nested in function - ABS(SUM(amount))
	result, err := exec.Execute("SELECT ABS(SUM(amount)) FROM sales")
	if err != nil {
		t.Logf("Nested aggregate ABS(SUM) failed: %v", err)
	} else {
		t.Logf("Nested aggregate ABS(SUM): %v", result.Rows)
	}

	// Test aggregate in COALESCE
	result, err = exec.Execute("SELECT COALESCE(SUM(amount), 0) FROM sales")
	if err != nil {
		t.Logf("Aggregate in COALESCE failed: %v", err)
	} else {
		t.Logf("Aggregate in COALESCE: %v", result.Rows)
	}
}

// ========== Tests for hasAggregate - BinaryExpr ==========

func TestHasAggregateBinaryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-bin-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, a INT, b INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d, %d)", i, i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test SUM(a) + SUM(b)
	result, err := exec.Execute("SELECT SUM(a) + SUM(b) FROM data")
	if err != nil {
		t.Logf("SUM(a) + SUM(b) failed: %v", err)
	} else {
		t.Logf("SUM(a) + SUM(b): %v", result.Rows)
	}

	// Test SUM(a) * 2
	result, err = exec.Execute("SELECT SUM(a) * 2 FROM data")
	if err != nil {
		t.Logf("SUM(a) * 2 failed: %v", err)
	} else {
		t.Logf("SUM(a) * 2: %v", result.Rows)
	}

	// Test SUM(a + b)
	result, err = exec.Execute("SELECT SUM(a + b) FROM data")
	if err != nil {
		t.Logf("SUM(a + b) failed: %v", err)
	} else {
		t.Logf("SUM(a + b): %v", result.Rows)
	}
}

// ========== Tests for hasAggregate - UnaryExpr ==========

func TestHasAggregateUnaryExprExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-unary-*")
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

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test -SUM(val)
	result, err := exec.Execute("SELECT -SUM(val) FROM data")
	if err != nil {
		t.Logf("-SUM(val) failed: %v", err)
	} else {
		t.Logf("-SUM(val): %v", result.Rows)
	}

	// Test -COUNT(*)
	result, err = exec.Execute("SELECT -COUNT(*) FROM data")
	if err != nil {
		t.Logf("-COUNT(*) failed: %v", err)
	} else {
		t.Logf("-COUNT(*): %v", result.Rows)
	}
}

// ========== Tests for hasAggregate - CaseExpr ==========

func TestHasAggregateCaseExprExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-case-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, amount INT, category VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		cat := "A"
		if i > 3 {
			cat = "B"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, %d, '%s')", i, i*100, cat))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test CASE with aggregate in condition
	result, err := exec.Execute(`
		SELECT CASE
			WHEN SUM(amount) > 200 THEN 'high'
			ELSE 'low'
		END
		FROM sales
	`)
	if err != nil {
		t.Logf("CASE with aggregate condition failed: %v", err)
	} else {
		t.Logf("CASE with aggregate condition: %v", result.Rows)
	}

	// Test CASE with aggregate in result
	result, err = exec.Execute(`
		SELECT CASE category
			WEN 'A' THEN SUM(amount)
			ELSE 0
		END
		FROM sales
		GROUP BY category
	`)
	if err != nil {
		t.Logf("CASE with aggregate result failed: %v", err)
	} else {
		t.Logf("CASE with aggregate result: %v rows", len(result.Rows))
	}

	// Test CASE with aggregate in ELSE
	result, err = exec.Execute(`
		SELECT CASE
			WHEN category = 'A' THEN 1
			ELSE COUNT(*)
		END as cnt
		FROM sales
		GROUP BY category
	`)
	if err != nil {
		t.Logf("CASE with aggregate ELSE failed: %v", err)
	} else {
		t.Logf("CASE with aggregate ELSE: %v rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - ColumnRef with table prefix ==========

func TestEvaluateExpressionTablePrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-prefix-*")
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

	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test column with matching table prefix
	result, err := exec.Execute("SELECT users.name FROM users WHERE users.id = 1")
	if err != nil {
		t.Logf("Column with matching prefix failed: %v", err)
	} else {
		t.Logf("Column with matching prefix: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - ScalarSubquery multiple rows error ==========

func TestEvaluateExpressionScalarSubqueryMultiRows(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-multi-*")
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

	_, err = exec.Execute("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test scalar subquery returning multiple rows - should error
	result, err := exec.Execute("SELECT (SELECT val FROM data) as val")
	if err != nil {
		t.Logf("Scalar subquery multi-row error (expected): %v", err)
	} else {
		t.Logf("Scalar subquery multi-row: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - AnyAllExpr ==========

func TestEvaluateExpressionAnyAllCompare(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-cmp-*")
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

	_, err = exec.Execute("CREATE TABLE numbers (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ANY with > comparison
	result, err := exec.Execute("SELECT * FROM numbers WHERE val > ANY (SELECT val FROM numbers WHERE id < 3)")
	if err != nil {
		t.Logf("ANY > comparison failed: %v", err)
	} else {
		t.Logf("ANY > comparison: %d rows", len(result.Rows))
	}

	// Test ALL with < comparison
	result, err = exec.Execute("SELECT * FROM numbers WHERE val < ALL (SELECT val FROM numbers WHERE id > 2)")
	if err != nil {
		t.Logf("ALL < comparison failed: %v", err)
	} else {
		t.Logf("ALL < comparison: %d rows", len(result.Rows))
	}

	// Test ALL with = comparison
	result, err = exec.Execute("SELECT * FROM numbers WHERE val = ALL (SELECT val FROM numbers WHERE id = 1)")
	if err != nil {
		t.Logf("ALL = comparison failed: %v", err)
	} else {
		t.Logf("ALL = comparison: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - RankExpr ==========

func TestEvaluateExpressionRankExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rank-expr-*")
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

	// Test with rank
	result, err := exec.Execute("SELECT content, rank FROM docs WHERE docs MATCH 'hello' ORDER BY rank")
	if err != nil {
		t.Logf("FTS with rank failed: %v", err)
	} else {
		t.Logf("FTS with rank: %d rows", len(result.Rows))
	}
}

// ========== Tests for pragmaIntegrityCheck with data ==========

func TestPragmaIntegrityCheckWithData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-data-*")
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
	_, err = exec.Execute("CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO t1 VALUES (%d, 'name%d')", i, i))
		if err != nil {
			t.Fatalf("INSERT t1 failed: %v", err)
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i, i*100))
		if err != nil {
			t.Fatalf("INSERT t2 failed: %v", err)
		}
	}

	// Create indexes
	_, err = exec.Execute("CREATE INDEX idx_name ON t1 (name)")
	if err != nil {
		t.Logf("CREATE INDEX failed: %v", err)
	}

	// Test integrity check
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK: %v", result.Rows)
	}

	// Test quick check
	result, err = exec.Execute("PRAGMA QUICK_CHECK")
	if err != nil {
		t.Logf("QUICK_CHECK failed: %v", err)
	} else {
		t.Logf("QUICK_CHECK: %v", result.Rows)
	}
}

// ========== Tests for correlated subqueries ==========

func TestCorrelatedSubqueryWhere(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-correlated-where-*")
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

	_, err = exec.Execute("CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, salary INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE employees failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE departments failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO departments VALUES (1, 'HR'), (2, 'Engineering')")
	if err != nil {
		t.Fatalf("INSERT departments failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO employees VALUES (1, 1, 50000), (2, 1, 60000), (3, 2, 70000)")
	if err != nil {
		t.Fatalf("INSERT employees failed: %v", err)
	}

	// Correlated subquery in WHERE
	result, err := exec.Execute(`
		SELECT * FROM employees e
		WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id)
	`)
	if err != nil {
		t.Logf("Correlated subquery WHERE failed: %v", err)
	} else {
		t.Logf("Correlated subquery WHERE: %d rows", len(result.Rows))
	}
}

// ========== Tests for aggregate in HAVING with comparison ==========

func TestHavingAggregateComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-agg-cmp-*")
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

	// HAVING with SUM comparison
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > 500
	`)
	if err != nil {
		t.Logf("HAVING SUM comparison failed: %v", err)
	} else {
		t.Logf("HAVING SUM comparison: %d rows", len(result.Rows))
	}

	// HAVING with COUNT comparison
	result, err = exec.Execute(`
		SELECT region, COUNT(*) as cnt
		FROM sales
		GROUP BY region
		HAVING COUNT(*) >= 5
	`)
	if err != nil {
		t.Logf("HAVING COUNT comparison failed: %v", err)
	} else {
		t.Logf("HAVING COUNT comparison: %d rows", len(result.Rows))
	}

	// HAVING with AVG comparison
	result, err = exec.Execute(`
		SELECT region, AVG(amount) as avg_amount
		FROM sales
		GROUP BY region
		HAVING AVG(amount) < 500
	`)
	if err != nil {
		t.Logf("HAVING AVG comparison failed: %v", err)
	} else {
		t.Logf("HAVING AVG comparison: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - BinaryExpr with various operators ==========

func TestEvaluateWhereBinaryOperators(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-ops-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 20), (2, 20, 10), (3, 15, 15)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test various operators
	ops := []string{"=", "!=", "<>", "<", ">", "<=", ">="}
	for _, op := range ops {
		result, err := exec.Execute(fmt.Sprintf("SELECT * FROM test WHERE a %s b", op))
		if err != nil {
			t.Logf("WHERE with %s failed: %v", op, err)
		} else {
			t.Logf("WHERE %s: %d rows", op, len(result.Rows))
		}
	}

	// Test LIKE operator
	result, err := exec.Execute("SELECT * FROM test WHERE CAST(a AS VARCHAR) LIKE '1%'")
	if err != nil {
		t.Logf("WHERE LIKE failed: %v", err)
	} else {
		t.Logf("WHERE LIKE: %d rows", len(result.Rows))
	}

	// Test GLOB operator
	result, err = exec.Execute("SELECT * FROM test WHERE CAST(a AS VARCHAR) GLOB '1*'")
	if err != nil {
		t.Logf("WHERE GLOB failed: %v", err)
	} else {
		t.Logf("WHERE GLOB: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving with different comparison operators ==========

func TestEvaluateHavingOperators(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-ops-*")
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

	for i := 1; i <= 10; i++ {
		grp := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d, %d)", i, grp, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING with different operators
	ops := []string{"=", "!=", "<", ">", "<=", ">="}
	for _, op := range ops {
		result, err := exec.Execute(fmt.Sprintf("SELECT grp, SUM(val) as total FROM data GROUP BY grp HAVING total %s 100", op))
		if err != nil {
			t.Logf("HAVING %s failed: %v", op, err)
		} else {
			t.Logf("HAVING %s: %d rows", op, len(result.Rows))
		}
	}
}

// ========== Tests for evaluateExpression with outer context ==========

func TestEvaluateExpressionOuterContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-outer-ctx-*")
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

	_, err = exec.Execute("CREATE TABLE outer_table (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_table failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE inner_table (id INT PRIMARY KEY, ref_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_table failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO outer_table VALUES (1, 100), (2, 200)")
	if err != nil {
		t.Fatalf("INSERT outer_table failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO inner_table VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("INSERT inner_table failed: %v", err)
	}

	// Test correlated subquery accessing outer table
	result, err := exec.Execute(`
		SELECT o.id, o.val,
			(SELECT COUNT(*) FROM inner_table i WHERE i.ref_id = o.id) as inner_count
		FROM outer_table o
	`)
	if err != nil {
		t.Logf("Correlated subquery with outer context failed: %v", err)
	} else {
		t.Logf("Correlated subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for castValue with various types ==========

func TestCastValueTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-types-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, '123')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test CAST to INT
	result, err := exec.Execute("SELECT CAST(val AS INT) FROM test")
	if err != nil {
		t.Logf("CAST to INT failed: %v", err)
	} else {
		t.Logf("CAST to INT: %v", result.Rows)
	}

	// Test CAST to FLOAT
	result, err = exec.Execute("SELECT CAST(val AS FLOAT) FROM test")
	if err != nil {
		t.Logf("CAST to FLOAT failed: %v", err)
	} else {
		t.Logf("CAST to FLOAT: %v", result.Rows)
	}

	// Test CAST to VARCHAR
	result, err = exec.Execute("SELECT CAST(123 AS VARCHAR)")
	if err != nil {
		t.Logf("CAST to VARCHAR failed: %v", err)
	} else {
		t.Logf("CAST to VARCHAR: %v", result.Rows)
	}

	// Test CAST to BOOL
	result, err = exec.Execute("SELECT CAST(1 AS BOOL), CAST(0 AS BOOL)")
	if err != nil {
		t.Logf("CAST to BOOL failed: %v", err)
	} else {
		t.Logf("CAST to BOOL: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving with EXISTS returning true ==========

func TestEvaluateHavingExistsTrue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-exists-true-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, premium BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, TRUE), (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test HAVING EXISTS where subquery returns rows
	result, err := exec.Execute(`
		SELECT customer_id, COUNT(*) as order_count
		FROM orders
		GROUP BY customer_id
		HAVING EXISTS (SELECT 1 FROM customers c WHERE c.id = customer_id AND c.premium = TRUE)
	`)
	if err != nil {
		t.Logf("HAVING EXISTS true failed: %v", err)
	} else {
		t.Logf("HAVING EXISTS true: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving with NOT EXISTS ==========

func TestEvaluateHavingNotExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-notexists-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category VARCHAR, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE inactive_cats (name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE inactive_cats failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO inactive_cats VALUES ('old'), ('deleted')")
	if err != nil {
		t.Fatalf("INSERT inactive_cats failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 'new', TRUE), (2, 'old', TRUE), (3, 'active', FALSE)")
	if err != nil {
		t.Fatalf("INSERT items failed: %v", err)
	}

	// Test HAVING NOT EXISTS
	result, err := exec.Execute(`
		SELECT category, COUNT(*) as cnt
		FROM items
		GROUP BY category
		HAVING NOT EXISTS (SELECT 1 FROM inactive_cats WHERE name = category)
	`)
	if err != nil {
		t.Logf("HAVING NOT EXISTS failed: %v", err)
	} else {
		t.Logf("HAVING NOT EXISTS: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow with BETWEEN ==========

func TestEvaluateWhereBetween(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-between-*")
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

	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test BETWEEN
	result, err := exec.Execute("SELECT * FROM nums WHERE val BETWEEN 20 AND 80")
	if err != nil {
		t.Logf("WHERE BETWEEN failed: %v", err)
	} else {
		t.Logf("WHERE BETWEEN: %d rows", len(result.Rows))
	}

	// Test NOT BETWEEN
	result, err = exec.Execute("SELECT * FROM nums WHERE val NOT BETWEEN 30 AND 70")
	if err != nil {
		t.Logf("WHERE NOT BETWEEN failed: %v", err)
	} else {
		t.Logf("WHERE NOT BETWEEN: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression with NULL column ==========

func TestEvaluateExpressionNullColumn(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-null-*")
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

	// Insert row without specifying val (NULL)
	_, err = exec.Execute("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}

	// Test expression with NULL
	result, err := exec.Execute("SELECT val + 1 FROM test WHERE id = 1")
	if err != nil {
		t.Logf("Expression with NULL failed: %v", err)
	} else {
		t.Logf("Expression with NULL: %v", result.Rows)
	}

	// Test comparison with NULL
	result, err = exec.Execute("SELECT * FROM test WHERE val = NULL")
	if err != nil {
		t.Logf("Comparison with NULL failed: %v", err)
	} else {
		t.Logf("Comparison with NULL: %d rows", len(result.Rows))
	}
}

// ========== Tests for GROUP_CONCAT with ORDER BY ==========

func TestGroupConcatOrderBy(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-groupcat-order-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, grp INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 1, 'c'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'x'), (5, 2, 'y')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test GROUP_CONCAT
	result, err := exec.Execute("SELECT grp, GROUP_CONCAT(name) as names FROM items GROUP BY grp")
	if err != nil {
		t.Logf("GROUP_CONCAT failed: %v", err)
	} else {
		t.Logf("GROUP_CONCAT: %v", result.Rows)
	}

	// Test GROUP_CONCAT with separator
	result, err = exec.Execute("SELECT grp, GROUP_CONCAT(name, ', ') as names FROM items GROUP BY grp")
	if err != nil {
		t.Logf("GROUP_CONCAT with separator failed: %v", err)
	} else {
		t.Logf("GROUP_CONCAT with separator: %v", result.Rows)
	}
}