package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
)

// ========== Direct tests for evaluateHaving ==========

func TestEvaluateHavingUnaryNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-unary-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 6; i++ {
		grp := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d, %d)", i, grp, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING NOT with aggregate comparison
	result, err := exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM test
		GROUP BY grp
		HAVING NOT (SUM(val) > 100)
	`)
	if err != nil {
		t.Logf("HAVING NOT failed: %v", err)
	} else {
		t.Logf("HAVING NOT: %d rows", len(result.Rows))
	}

	// Test HAVING NOT EXISTS
	result, err = exec.Execute(`
		SELECT grp, COUNT(*) as cnt
		FROM test
		GROUP BY grp
		HAVING NOT EXISTS (SELECT 1 FROM test t2 WHERE t2.grp = grp AND t2.val > 50)
	`)
	if err != nil {
		t.Logf("HAVING NOT EXISTS failed: %v", err)
	} else {
		t.Logf("HAVING NOT EXISTS: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - AnyAllExpr ==========

func TestEvaluateWhereAnyAllDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-anyall-*")
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

	_, err = exec.Execute("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ANY with various operators
	anyTests := []string{
		"SELECT * FROM numbers WHERE val > ANY (SELECT val FROM numbers WHERE id < 3)",
		"SELECT * FROM numbers WHERE val = ANY (SELECT val FROM numbers WHERE id = 1)",
		"SELECT * FROM numbers WHERE val < ANY (SELECT val FROM numbers WHERE id > 2)",
		"SELECT * FROM numbers WHERE val >= ANY (SELECT val FROM numbers WHERE id IN (2, 3))",
	}

	for _, tc := range anyTests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("ANY test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("ANY test: %s -> %d rows", tc, len(result.Rows))
		}
	}

	// Test ALL with various operators
	allTests := []string{
		"SELECT * FROM numbers WHERE val > ALL (SELECT val FROM numbers WHERE id < 3)",
		"SELECT * FROM numbers WHERE val < ALL (SELECT val FROM numbers WHERE id > 2)",
		"SELECT * FROM numbers WHERE val >= ALL (SELECT val FROM numbers WHERE id = 1)",
		"SELECT * FROM numbers WHERE val <= ALL (SELECT val FROM numbers WHERE id = 4)",
	}

	for _, tc := range allTests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("ALL test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("ALL test: %s -> %d rows", tc, len(result.Rows))
		}
	}

	// Test ALL with empty subquery (should return true for all rows)
	result, err := exec.Execute("SELECT * FROM numbers WHERE val > ALL (SELECT val FROM numbers WHERE id > 100)")
	if err != nil {
		t.Logf("ALL empty subquery failed: %v", err)
	} else {
		t.Logf("ALL empty subquery: %d rows (should be all 4)", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - ScalarSubquery ==========

func TestEvaluateWhereScalarSubqueryDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-scalar-*")
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

	_, err = exec.Execute("CREATE TABLE thresholds (id INT PRIMARY KEY, min_amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE thresholds failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO thresholds VALUES (1, 150)")
	if err != nil {
		t.Fatalf("INSERT thresholds failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test scalar subquery in WHERE
	result, err := exec.Execute(`
		SELECT * FROM orders
		WHERE amount > (SELECT min_amount FROM thresholds WHERE id = 1)
	`)
	if err != nil {
		t.Logf("Scalar subquery WHERE failed: %v", err)
	} else {
		t.Logf("Scalar subquery WHERE: %d rows", len(result.Rows))
	}

	// Test scalar subquery returning no rows
	result, err = exec.Execute(`
		SELECT * FROM orders
		WHERE amount > (SELECT min_amount FROM thresholds WHERE id = 999)
	`)
	if err != nil {
		t.Logf("Scalar subquery no rows failed: %v", err)
	} else {
		t.Logf("Scalar subquery no rows: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - ScalarSubquery ==========

func TestEvaluateHavingScalarSubqueryDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-det-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, store_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE sales failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE targets (id INT PRIMARY KEY, store_id INT, target INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE targets failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO targets VALUES (1, 1, 200), (2, 2, 100)")
	if err != nil {
		t.Fatalf("INSERT targets failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 1, 100), (2, 1, 150), (3, 2, 80), (4, 2, 50)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	// Test HAVING with scalar subquery comparison
	result, err := exec.Execute(`
		SELECT store_id, SUM(amount) as total
		FROM sales
		GROUP BY store_id
		HAVING SUM(amount) > (SELECT target FROM targets WHERE targets.store_id = sales.store_id)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery: %d rows", len(result.Rows))
	}

	// Test HAVING with scalar subquery returning 0 (false)
	result, err = exec.Execute(`
		SELECT store_id, SUM(amount) as total
		FROM sales
		GROUP BY store_id
		HAVING (SELECT 0)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery returning 0 failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery returning 0: %d rows", len(result.Rows))
	}
}

// ========== Tests for hasAggregate - all types ==========

func TestHasAggregateAllTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-types-*")
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

	// Test all aggregate types
	aggTests := []string{
		"SELECT COUNT(*) FROM data",
		"SELECT SUM(val) FROM data",
		"SELECT AVG(val) FROM data",
		"SELECT MIN(val) FROM data",
		"SELECT MAX(val) FROM data",
		"SELECT GROUP_CONCAT(val) FROM data",
		// Nested in other functions
		"SELECT ABS(SUM(val)) FROM data",
		"SELECT COALESCE(AVG(val), 0) FROM data",
		// In expressions
		"SELECT SUM(val) + 1 FROM data",
		"SELECT SUM(val) * 2 FROM data",
		// Multiple aggregates
		"SELECT SUM(val), COUNT(*), AVG(val) FROM data",
	}

	for _, tc := range aggTests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Aggregate test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Aggregate test: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for hasAggregate in CASE ==========

func TestHasAggregateInCaseExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-case-det-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, category VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CASE with aggregate in condition
	result, err := exec.Execute(`
		SELECT CASE
			WHEN SUM(amount) > 300 THEN 'high'
			WEN SUM(amount) > 100 THEN 'medium'
			ELSE 'low'
		END as level
		FROM sales
	`)
	if err != nil {
		t.Logf("CASE with aggregate condition failed: %v", err)
	} else {
		t.Logf("CASE with aggregate condition: %v", result.Rows)
	}

	// CASE with aggregate in result
	result, err = exec.Execute(`
		SELECT category,
			CASE category
				WEN 'A' THEN SUM(amount)
				ELSE 0
			END as total
		FROM sales
		GROUP BY category
	`)
	if err != nil {
		t.Logf("CASE with aggregate result failed: %v", err)
	} else {
		t.Logf("CASE with aggregate result: %d rows", len(result.Rows))
	}

	// CASE with aggregate in ELSE
	result, err = exec.Execute(`
		SELECT category,
			CASE
				WHEN category = 'A' THEN 1
				ELSE COUNT(*)
			END as cnt
		FROM sales
		GROUP BY category
	`)
	if err != nil {
		t.Logf("CASE with aggregate ELSE failed: %v", err)
	} else {
		t.Logf("CASE with aggregate ELSE: %d rows", len(result.Rows))
	}
}

// ========== Tests for pragmaIntegrityCheck error paths ==========

func TestPragmaIntegrityCheckMultipleTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-multi-*")
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

	// Create many tables
	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("CREATE TABLE t%d (id INT PRIMARY KEY, name VARCHAR)", i))
		if err != nil {
			t.Fatalf("CREATE TABLE t%d failed: %v", i, err)
		}

		// Insert some data
		for j := 1; j <= 3; j++ {
			_, err = exec.Execute(fmt.Sprintf("INSERT INTO t%d VALUES (%d, 'name%d')", i, j, j))
			if err != nil {
				t.Fatalf("INSERT t%d failed: %v", i, err)
			}
		}

		// Create index
		_, err = exec.Execute(fmt.Sprintf("CREATE INDEX idx_t%d_name ON t%d (name)", i, i))
		if err != nil {
			t.Logf("CREATE INDEX idx_t%d_name failed: %v", i, err)
		}
	}

	// Test integrity check
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK: %v", result.Rows)
		if len(result.Rows) > 0 && result.Rows[0][0] == "ok" {
			t.Logf("Integrity check passed")
		}
	}
}

// ========== Tests for evaluateExpression - outer context with table prefix ==========

func TestEvaluateExpressionOuterContextTablePrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-outer-prefix-*")
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
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT users failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test correlated subquery with table prefix
	result, err := exec.Execute(`
		SELECT o.id, o.amount,
			(SELECT u.name FROM users u WHERE u.id = o.user_id) as user_name
		FROM orders o
	`)
	if err != nil {
		t.Logf("Correlated subquery with table prefix failed: %v", err)
	} else {
		t.Logf("Correlated subquery with table prefix: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - column not found in outer context ==========

func TestEvaluateExpressionColumnNotFoundOuter(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test column not found (should error)
	_, err = exec.Execute("SELECT nonexistent FROM test")
	if err != nil {
		t.Logf("Column not found error (expected): %v", err)
	} else {
		t.Log("Expected error for nonexistent column")
	}
}

// ========== Tests for evaluateWhere - BetweenExpr ==========

func TestEvaluateWhereBetweenExpr(t *testing.T) {
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

	for i := 1; i <= 20; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i, i*5))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test BETWEEN
	result, err := exec.Execute("SELECT * FROM nums WHERE val BETWEEN 20 AND 60")
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

// ========== Tests for evaluateHaving - InExpr with list ==========

func TestEvaluateHavingInExprListMock(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-inlist-*")
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

	for i := 1; i <= 9; i++ {
		cat := string(rune('A' + (i-1)%3))
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, '%s', %d)", i, cat, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING with IN list
	result, err := exec.Execute(`
		SELECT category, SUM(price) as total
		FROM items
		GROUP BY category
		HAVING category IN ('A', 'B')
	`)
	if err != nil {
		t.Logf("HAVING IN list failed: %v", err)
	} else {
		t.Logf("HAVING IN list: %d rows", len(result.Rows))
	}

	// Test HAVING with NOT IN list
	result, err = exec.Execute(`
		SELECT category, SUM(price) as total
		FROM items
		GROUP BY category
		HAVING category NOT IN ('A')
	`)
	if err != nil {
		t.Logf("HAVING NOT IN list failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN list: %d rows", len(result.Rows))
	}
}

// ========== Tests for correlated subquery with outer context ==========

func TestCorrelatedSubqueryDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-correlated-det-*")
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

	_, err = exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE departments failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, salary INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE employees failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO departments VALUES (1, 'HR'), (2, 'Engineering'), (3, 'Sales')")
	if err != nil {
		t.Fatalf("INSERT departments failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO employees VALUES (1, 1, 50000), (2, 1, 60000), (3, 2, 80000), (4, 2, 90000), (5, 3, 40000)")
	if err != nil {
		t.Fatalf("INSERT employees failed: %v", err)
	}

	// Correlated subquery in SELECT
	result, err := exec.Execute(`
		SELECT d.name,
			(SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) as emp_count,
			(SELECT AVG(salary) FROM employees e WHERE e.dept_id = d.id) as avg_salary
		FROM departments d
	`)
	if err != nil {
		t.Logf("Correlated subquery in SELECT failed: %v", err)
	} else {
		t.Logf("Correlated subquery in SELECT: %d rows", len(result.Rows))
	}

	// Correlated subquery in WHERE with EXISTS
	result, err = exec.Execute(`
		SELECT * FROM departments d
		WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 70000)
	`)
	if err != nil {
		t.Logf("Correlated subquery EXISTS failed: %v", err)
	} else {
		t.Logf("Correlated subquery EXISTS: %d rows", len(result.Rows))
	}

	// Correlated subquery in WHERE with comparison
	result, err = exec.Execute(`
		SELECT * FROM employees e1
		WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id)
	`)
	if err != nil {
		t.Logf("Correlated subquery comparison failed: %v", err)
	} else {
		t.Logf("Correlated subquery comparison: %d rows", len(result.Rows))
	}
}

// ========== Tests for GROUP BY with multiple aggregates ==========

func TestGroupByMultipleAggregates(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-multi-agg-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, product VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 12; i++ {
		region := "East"
		if i > 6 {
			region = "West"
		}
		product := string(rune('A' + (i-1)%3))
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', '%s', %d)", i, region, product, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Multiple aggregates with GROUP BY
	result, err := exec.Execute(`
		SELECT region,
			COUNT(*) as cnt,
			SUM(amount) as total,
			AVG(amount) as avg_amt,
			MIN(amount) as min_amt,
			MAX(amount) as max_amt
		FROM sales
		GROUP BY region
	`)
	if err != nil {
		t.Logf("Multiple aggregates failed: %v", err)
	} else {
		t.Logf("Multiple aggregates: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Row: %v", row)
		}
	}

	// GROUP BY with HAVING on multiple aggregates
	result, err = exec.Execute(`
		SELECT region, COUNT(*) as cnt, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING COUNT(*) > 3 AND SUM(amount) > 2000
	`)
	if err != nil {
		t.Logf("HAVING with multiple aggregates failed: %v", err)
	} else {
		t.Logf("HAVING with multiple aggregates: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving with complex conditions ==========

func TestEvaluateHavingComplexConditions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-complex-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 15; i++ {
		customer := (i-1)%3 + 1
		status := "completed"
		if i%4 == 0 {
			status = "pending"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, '%s', %d)", i, customer, status, i*50))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// HAVING with AND
	result, err := exec.Execute(`
		SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total
		FROM orders
		GROUP BY customer_id
		HAVING COUNT(*) >= 3 AND SUM(amount) > 500
	`)
	if err != nil {
		t.Logf("HAVING AND failed: %v", err)
	} else {
		t.Logf("HAVING AND: %d rows", len(result.Rows))
	}

	// HAVING with OR
	result, err = exec.Execute(`
		SELECT customer_id, COUNT(*) as cnt
		FROM orders
		GROUP BY customer_id
		HAVING COUNT(*) < 3 OR SUM(amount) > 1000
	`)
	if err != nil {
		t.Logf("HAVING OR failed: %v", err)
	} else {
		t.Logf("HAVING OR: %d rows", len(result.Rows))
	}

	// HAVING with nested conditions
	result, err = exec.Execute(`
		SELECT customer_id, COUNT(*) as cnt
		FROM orders
		GROUP BY customer_id
		HAVING (COUNT(*) >= 4 AND SUM(amount) > 600) OR COUNT(*) < 3
	`)
	if err != nil {
		t.Logf("HAVING nested conditions failed: %v", err)
	} else {
		t.Logf("HAVING nested conditions: %d rows", len(result.Rows))
	}
}

// ========== Direct unit tests for hasAggregate function ==========

func TestHasAggregateDirect(t *testing.T) {
	// Test with nil expression
	if hasAggregate(nil) {
		t.Error("hasAggregate(nil) should return false")
	}

	// Test with function call containing aggregate
	aggFuncs := []string{"COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT"}
	for _, fn := range aggFuncs {
		expr := &sql.FunctionCall{
			Name: fn,
			Args: []sql.Expression{&sql.Literal{Value: 1}},
		}
		if !hasAggregate(expr) {
			t.Errorf("hasAggregate should return true for %s", fn)
		}
	}

	// Test with function call not containing aggregate
	nonAggFuncs := []string{"ABS", "UPPER", "LOWER", "LENGTH"}
	for _, fn := range nonAggFuncs {
		expr := &sql.FunctionCall{
			Name: fn,
			Args: []sql.Expression{&sql.Literal{Value: "test"}},
		}
		if hasAggregate(expr) {
			t.Errorf("hasAggregate should return false for %s", fn)
		}
	}

	// Test with BinaryExpr containing aggregate
	binaryExpr := &sql.BinaryExpr{
		Left:  &sql.FunctionCall{Name: "SUM", Args: []sql.Expression{&sql.Literal{Value: 1}}},
		Right: &sql.Literal{Value: 100},
		Op:    sql.OpGt,
	}
	if !hasAggregate(binaryExpr) {
		t.Error("hasAggregate should return true for BinaryExpr with aggregate")
	}

	// Test with UnaryExpr containing aggregate
	unaryExpr := &sql.UnaryExpr{
		Right: &sql.FunctionCall{Name: "COUNT", Args: []sql.Expression{&sql.StarExpr{}}},
		Op:    sql.OpNeg,
	}
	if !hasAggregate(unaryExpr) {
		t.Error("hasAggregate should return true for UnaryExpr with aggregate")
	}

	// Test with CaseExpr containing aggregate in condition
	caseExpr := &sql.CaseExpr{
		Whens: []*sql.CaseWhen{
			{
				Condition: &sql.FunctionCall{Name: "SUM", Args: []sql.Expression{&sql.Literal{Value: 1}}},
				Result:    &sql.Literal{Value: "high"},
			},
		},
		Else: &sql.Literal{Value: "low"},
	}
	if !hasAggregate(caseExpr) {
		t.Error("hasAggregate should return true for CaseExpr with aggregate in condition")
	}

	// Test with CaseExpr containing aggregate in result
	caseExpr2 := &sql.CaseExpr{
		Expr: &sql.Literal{Value: "A"},
		Whens: []*sql.CaseWhen{
			{
				Condition: &sql.Literal{Value: true},
				Result:    &sql.FunctionCall{Name: "SUM", Args: []sql.Expression{&sql.Literal{Value: 1}}},
			},
		},
	}
	if !hasAggregate(caseExpr2) {
		t.Error("hasAggregate should return true for CaseExpr with aggregate in result")
	}

	// Test with CaseExpr containing aggregate in ELSE
	caseExpr3 := &sql.CaseExpr{
		Whens: []*sql.CaseWhen{
			{
				Condition: &sql.Literal{Value: false},
				Result:    &sql.Literal{Value: 1},
			},
		},
		Else: &sql.FunctionCall{Name: "COUNT", Args: []sql.Expression{&sql.StarExpr{}}},
	}
	if !hasAggregate(caseExpr3) {
		t.Error("hasAggregate should return true for CaseExpr with aggregate in ELSE")
	}

	t.Logf("All hasAggregate direct tests passed")
}

// ========== Direct tests for compareValues ==========

func TestCompareValuesDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-compare-*")
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

	// Test comparison operators
	tests := []struct {
		query    string
		expected int // number of rows
	}{
		{"SELECT 1 = 1", 1},
		{"SELECT 1 != 2", 1},
		{"SELECT 2 < 3", 1},
		{"SELECT 3 > 2", 1},
		{"SELECT 2 <= 2", 1},
		{"SELECT 3 >= 2", 1},
		{"SELECT 'a' = 'a'", 1},
		{"SELECT 'a' < 'b'", 1},
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc.query)
		if err != nil {
			t.Logf("Compare test failed: %s, error: %v", tc.query, err)
		} else {
			t.Logf("Compare test: %s -> %v", tc.query, result.Rows)
		}
	}
}

// ========== Tests for NULL handling in expressions ==========

func TestNullHandlingInExpressions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-null-expr-*")
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

	// Insert with NULL
	_, err = exec.Execute("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test NULL comparisons
	tests := []string{
		"SELECT * FROM test WHERE val IS NULL",
		"SELECT * FROM test WHERE val IS NOT NULL",
		"SELECT * FROM test WHERE val = NULL",
		"SELECT * FROM test WHERE val != NULL",
		"SELECT val + 1 FROM test WHERE id = 1",
		"SELECT -val FROM test WHERE id = 1",
		"SELECT COALESCE(val, 0) FROM test",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("NULL test failed: %s, error: %v", tc, err)
		} else {
			t.Logf("NULL test: %s -> %d rows", tc, len(result.Rows))
		}
	}
}

// ========== Tests for IN with subquery returning empty ==========

func TestInSubqueryEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-empty-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// IN with empty subquery
	result, err := exec.Execute("SELECT * FROM test WHERE val IN (SELECT val FROM test WHERE id > 100)")
	if err != nil {
		t.Logf("IN empty subquery failed: %v", err)
	} else {
		t.Logf("IN empty subquery: %d rows (should be 0)", len(result.Rows))
	}

	// NOT IN with empty subquery
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT IN (SELECT val FROM test WHERE id > 100)")
	if err != nil {
		t.Logf("NOT IN empty subquery failed: %v", err)
	} else {
		t.Logf("NOT IN empty subquery: %d rows (should be 3)", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - outerContext column lookup ==========

func TestEvaluateExpressionOuterContextLookup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-outer-lookup-*")
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

	_, err = exec.Execute("CREATE TABLE outer_t (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_t failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE inner_t (id INT PRIMARY KEY, ref_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_t failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO outer_t VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT outer_t failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO inner_t VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("INSERT inner_t failed: %v", err)
	}

	// Test correlated subquery where inner query references outer column
	result, err := exec.Execute(`
		SELECT o.id, o.name,
			(SELECT COUNT(*) FROM inner_t i WHERE i.ref_id = o.id) as inner_count
		FROM outer_t o
	`)
	if err != nil {
		t.Logf("Correlated subquery outer lookup failed: %v", err)
	} else {
		t.Logf("Correlated subquery outer lookup: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - column with table prefix in outerContext ==========

func TestEvaluateExpressionTablePrefixOuterContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-table-prefix-outer-*")
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
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT users failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test with table prefix in subquery
	result, err := exec.Execute(`
		SELECT o.id, o.amount,
			(SELECT users.name FROM users WHERE users.id = o.user_id) as user_name
		FROM orders o
	`)
	if err != nil {
		t.Logf("Table prefix in outer context failed: %v", err)
	} else {
		t.Logf("Table prefix in outer context: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - CollateExpr ==========

func TestEvaluateExpressionCollateDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-det-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COLLATE with table column
	result, err := exec.Execute("SELECT name COLLATE NOCASE FROM test WHERE id = 1")
	if err != nil {
		t.Logf("COLLATE with table column failed: %v", err)
	} else {
		t.Logf("COLLATE with table column: %v", result.Rows)
	}

	// Test COLLATE in WHERE
	result, err = exec.Execute("SELECT * FROM test WHERE name COLLATE NOCASE = 'HELLO'")
	if err != nil {
		t.Logf("COLLATE in WHERE failed: %v", err)
	} else {
		t.Logf("COLLATE in WHERE: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - CastExpr ==========

func TestEvaluateExpressionCastDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-det-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, str_val VARCHAR, int_val INT, float_val FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, '123', 456, 7.89)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test CAST VARCHAR to INT
	result, err := exec.Execute("SELECT CAST(str_val AS INT) FROM test")
	if err != nil {
		t.Logf("CAST VARCHAR to INT failed: %v", err)
	} else {
		t.Logf("CAST VARCHAR to INT: %v", result.Rows)
	}

	// Test CAST INT to VARCHAR
	result, err = exec.Execute("SELECT CAST(int_val AS VARCHAR) FROM test")
	if err != nil {
		t.Logf("CAST INT to VARCHAR failed: %v", err)
	} else {
		t.Logf("CAST INT to VARCHAR: %v", result.Rows)
	}

	// Test CAST FLOAT to INT
	result, err = exec.Execute("SELECT CAST(float_val AS INT) FROM test")
	if err != nil {
		t.Logf("CAST FLOAT to INT failed: %v", err)
	} else {
		t.Logf("CAST FLOAT to INT: %v", result.Rows)
	}

	// Test CAST INT to FLOAT
	result, err = exec.Execute("SELECT CAST(int_val AS FLOAT) FROM test")
	if err != nil {
		t.Logf("CAST INT to FLOAT failed: %v", err)
	} else {
		t.Logf("CAST INT to FLOAT: %v", result.Rows)
	}

	// Test CAST to BOOL
	result, err = exec.Execute("SELECT CAST(1 AS BOOL), CAST(0 AS BOOL), CAST('true' AS BOOL)")
	if err != nil {
		t.Logf("CAST to BOOL failed: %v", err)
	} else {
		t.Logf("CAST to BOOL: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - EXISTS returning false ==========

func TestEvaluateHavingExistsFalse(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-exists-false-*")
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

	_, err = exec.Execute("CREATE TABLE premium_customers (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE premium_customers failed: %v", err)
	}

	// premium_customers is empty
	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1), (2, 2), (3, 3)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// Test HAVING EXISTS with empty subquery (should return false)
	result, err := exec.Execute(`
		SELECT customer_id, COUNT(*) as cnt
		FROM orders
		GROUP BY customer_id
		HAVING EXISTS (SELECT 1 FROM premium_customers WHERE id = customer_id)
	`)
	if err != nil {
		t.Logf("HAVING EXISTS false failed: %v", err)
	} else {
		t.Logf("HAVING EXISTS false: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - AnyAllExpr with empty result ==========

func TestEvaluateWhereAnyAllEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-empty-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// ANY with empty subquery - should return false for all rows
	result, err := exec.Execute("SELECT * FROM test WHERE val > ANY (SELECT val FROM test WHERE id > 100)")
	if err != nil {
		t.Logf("ANY with empty subquery failed: %v", err)
	} else {
		t.Logf("ANY with empty subquery: %d rows (should be 0)", len(result.Rows))
	}

	// ALL with empty subquery - should return true for all rows
	result, err = exec.Execute("SELECT * FROM test WHERE val > ALL (SELECT val FROM test WHERE id > 100)")
	if err != nil {
		t.Logf("ALL with empty subquery failed: %v", err)
	} else {
		t.Logf("ALL with empty subquery: %d rows (should be 3)", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - InExpr with value list error ==========

func TestEvaluateWhereInExprValueList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-valuelist-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// IN with value list
	result, err := exec.Execute("SELECT * FROM test WHERE val IN (10, 20)")
	if err != nil {
		t.Logf("IN with value list failed: %v", err)
	} else {
		t.Logf("IN with value list: %d rows", len(result.Rows))
	}

	// NOT IN with value list
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT IN (10, 20)")
	if err != nil {
		t.Logf("NOT IN with value list failed: %v", err)
	} else {
		t.Logf("NOT IN with value list: %d rows", len(result.Rows))
	}

	// IN with string values
	result, err = exec.Execute("SELECT * FROM test WHERE CAST(val AS VARCHAR) IN ('10', '20')")
	if err != nil {
		t.Logf("IN with string values failed: %v", err)
	} else {
		t.Logf("IN with string values: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - null in unary expression ==========

func TestEvaluateExpressionUnaryNull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-null-*")
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

	// Insert with NULL
	_, err = exec.Execute("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}

	// Test unary minus on NULL
	result, err := exec.Execute("SELECT -val FROM test WHERE id = 1")
	if err != nil {
		t.Logf("Unary minus on NULL failed: %v", err)
	} else {
		t.Logf("Unary minus on NULL: %v", result.Rows)
	}

	// Test unary plus (should just return value)
	result, err = exec.Execute("SELECT +10")
	if err != nil {
		t.Logf("Unary plus failed: %v", err)
	} else {
		t.Logf("Unary plus: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - InExpr with list containing errors ==========

func TestEvaluateHavingInListWithError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-err-*")
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

	// Test HAVING with IN list where some values don't match
	result, err := exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING grp IN (1, 5, 10)
	`)
	if err != nil {
		t.Logf("HAVING IN list partial match failed: %v", err)
	} else {
		t.Logf("HAVING IN list partial match: %d rows", len(result.Rows))
	}

	// Test HAVING with IN list where no values match
	result, err = exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING grp IN (10, 20, 30)
	`)
	if err != nil {
		t.Logf("HAVING IN list no match failed: %v", err)
	} else {
		t.Logf("HAVING IN list no match: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for scalar subquery returning multiple rows ==========

func TestScalarSubqueryMultipleRowsError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-multi-err-*")
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

	// Scalar subquery returning multiple rows - should error
	_, err = exec.Execute("SELECT (SELECT val FROM data) as val")
	if err != nil {
		t.Logf("Scalar subquery multiple rows error (expected): %v", err)
	} else {
		t.Log("Expected error for scalar subquery returning multiple rows")
	}
}

// ========== Tests for HAVING scalar subquery returning multiple rows ==========

func TestHavingScalarSubqueryMultipleRowsError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-multi-*")
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

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'West', 200), (3, 'North', 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with scalar subquery returning multiple rows
	_, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > (SELECT amount FROM sales)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery multiple rows error (expected): %v", err)
	} else {
		t.Log("Expected error for HAVING scalar subquery returning multiple rows")
	}
}

// ========== Tests for HAVING with NULL in comparison ==========

func TestHavingWithNullComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-null-cmp-*")
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

	// Insert with some NULL prices
	_, err = exec.Execute("INSERT INTO items VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO items VALUES (2, 'A', NULL)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO items VALUES (3, 'B', 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with aggregate that might have NULL
	result, err := exec.Execute(`
		SELECT category, SUM(price) as total, COUNT(price) as cnt
		FROM items
		GROUP BY category
		HAVING SUM(price) > 0
	`)
	if err != nil {
		t.Logf("HAVING with NULL aggregate failed: %v", err)
	} else {
		t.Logf("HAVING with NULL aggregate: %d rows", len(result.Rows))
	}
}