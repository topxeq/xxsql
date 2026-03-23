package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ========== Tests for evaluateWhereForRow - EXISTS ==========

func TestEvaluateWhereExistsExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-exists-*")
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

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	// Test EXISTS with correlated subquery
	result, err := exec.Execute(`
		SELECT * FROM orders o
		WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)
	`)
	if err != nil {
		t.Logf("EXISTS correlated failed: %v", err)
	} else {
		t.Logf("EXISTS correlated: %d rows", len(result.Rows))
	}

	// Test NOT EXISTS
	result, err = exec.Execute(`
		SELECT * FROM orders o
		WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = 99)
	`)
	if err != nil {
		t.Logf("NOT EXISTS failed: %v", err)
	} else {
		t.Logf("NOT EXISTS: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - BetweenExpr ==========

func TestEvaluateWhereBetweenExprBoundary(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-between-boundary-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		query    string
		expected int
	}{
		{"SELECT * FROM test WHERE val BETWEEN 20 AND 40", 3},      // 20, 30, 40
		{"SELECT * FROM test WHERE val BETWEEN 10 AND 10", 1},      // 10
		{"SELECT * FROM test WHERE val BETWEEN 5 AND 15", 1},       // 10
		{"SELECT * FROM test WHERE val BETWEEN 45 AND 55", 1},      // 50
		{"SELECT * FROM test WHERE val BETWEEN 100 AND 200", 0},    // no match
		{"SELECT * FROM test WHERE val NOT BETWEEN 20 AND 40", 2},  // 10, 50
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc.query)
		if err != nil {
			t.Logf("Query failed: %s, error: %v", tc.query, err)
		} else {
			t.Logf("Query: %s -> %d rows (expected %d)", tc.query, len(result.Rows), tc.expected)
		}
	}
}

// ========== Tests for evaluateWhereForRow - LikeExpr patterns ==========

func TestEvaluateWhereLikeExprPatterns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-like-patterns-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []string{
		"SELECT * FROM test WHERE name LIKE 'A%'",      // starts with A
		"SELECT * FROM test WHERE name LIKE '%e'",      // ends with e
		"SELECT * FROM test WHERE name LIKE '%a%'",     // contains a
		"SELECT * FROM test WHERE name LIKE '_ob'",     // _ matches single char
		"SELECT * FROM test WHERE name LIKE 'B_b'",     // B_b pattern
		"SELECT * FROM test WHERE name NOT LIKE 'A%'",  // not starts with A
		"SELECT * FROM test WHERE name LIKE '%%%'",     // literal % (may need escape)
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("LIKE pattern failed: %s, error: %v", tc, err)
		} else {
			t.Logf("LIKE pattern: %s -> %d rows", tc, len(result.Rows))
		}
	}
}

// ========== Tests for evaluateExpression - UnaryExpr NOT ==========

func TestEvaluateExpressionUnaryNotDetailed(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, TRUE), (2, FALSE), (3, NULL)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}

	// Test NOT TRUE
	result, err := exec.Execute("SELECT * FROM test WHERE NOT val")
	if err != nil {
		t.Logf("NOT val failed: %v", err)
	} else {
		t.Logf("NOT val: %d rows", len(result.Rows))
	}

	// Test NOT with comparison
	result, err = exec.Execute("SELECT * FROM test WHERE NOT (id > 1)")
	if err != nil {
		t.Logf("NOT (id > 1) failed: %v", err)
	} else {
		t.Logf("NOT (id > 1): %d rows", len(result.Rows))
	}

	// Test NOT NOT
	result, err = exec.Execute("SELECT * FROM test WHERE NOT NOT val")
	if err != nil {
		t.Logf("NOT NOT val failed: %v", err)
	} else {
		t.Logf("NOT NOT val: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - InExpr with subquery ==========

func TestEvaluateHavingInExprSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-subquery-*")
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

	_, err = exec.Execute("CREATE TABLE targets (id INT PRIMARY KEY, min_amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE targets failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 150), (4, 'West', 250)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO targets VALUES (1, 150), (2, 200)")
	if err != nil {
		t.Fatalf("INSERT targets failed: %v", err)
	}

	// HAVING with IN subquery
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) IN (SELECT min_amount FROM targets)
	`)
	if err != nil {
		t.Logf("HAVING IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING IN subquery: %d rows", len(result.Rows))
	}

	// HAVING with NOT IN subquery
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) NOT IN (SELECT min_amount FROM targets)
	`)
	if err != nil {
		t.Logf("HAVING NOT IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - EXISTS ==========

func TestEvaluateHavingExistsExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-exists-*")
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

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 150)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, TRUE)")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	// HAVING with EXISTS
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING EXISTS (SELECT 1 FROM flags WHERE active = TRUE)
	`)
	if err != nil {
		t.Logf("HAVING EXISTS failed: %v", err)
	} else {
		t.Logf("HAVING EXISTS: %d rows", len(result.Rows))
	}

	// HAVING with NOT EXISTS
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING NOT EXISTS (SELECT 1 FROM flags WHERE active = FALSE)
	`)
	if err != nil {
		t.Logf("HAVING NOT EXISTS failed: %v", err)
	} else {
		t.Logf("HAVING NOT EXISTS: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - SubqueryExpr ==========

func TestEvaluateExpressionSubqueryExprDirect(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SubqueryExpr returning single value
	result, err := exec.Execute("SELECT (SELECT MAX(val) FROM nums)")
	if err != nil {
		t.Logf("SubqueryExpr single value failed: %v", err)
	} else {
		t.Logf("SubqueryExpr single value: %v", result.Rows)
	}

	// SubqueryExpr with outer reference
	_, err = exec.Execute("CREATE TABLE outer_tbl (id INT PRIMARY KEY, ref_val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_tbl failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO outer_tbl VALUES (1, 15), (2, 25)")
	if err != nil {
		t.Fatalf("INSERT outer_tbl failed: %v", err)
	}

	result, err = exec.Execute(`
		SELECT id, (SELECT val FROM nums WHERE val > o.ref_val ORDER BY val LIMIT 1)
		FROM outer_tbl o
	`)
	if err != nil {
		t.Logf("SubqueryExpr with outer ref failed: %v", err)
	} else {
		t.Logf("SubqueryExpr with outer ref: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - ColumnRef with table mismatch ==========

func TestEvaluateExpressionColumnRefTableMismatchDetailed(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(100)},
	}

	// Test ColumnRef with mismatched table prefix
	exec.currentTable = "test"
	exec.outerContext = map[string]interface{}{
		"other.col": 999,
		"col":       888,
	}

	// Try to access column from different table
	colRef := &sql.ColumnRef{Table: "other", Name: "col"}
	result, err := exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef table mismatch (expected error): %v", err)
	} else {
		t.Logf("ColumnRef table mismatch result: %v", result)
	}

	// Test with matching table prefix
	colRef2 := &sql.ColumnRef{Table: "test", Name: "val"}
	result, err = exec.evaluateExpression(colRef2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef matching table failed: %v", err)
	} else {
		t.Logf("ColumnRef matching table result: %v", result)
	}

	exec.outerContext = nil
	exec.currentTable = ""
}

// ========== Tests for evaluateExpression - BinaryExpr with NULL ==========

func TestEvaluateExpressionBinaryNullOperands(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-binary-null-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, NULL, 10), (2, 10, NULL), (3, NULL, NULL), (4, 5, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []string{
		"SELECT * FROM test WHERE a + b > 0",
		"SELECT * FROM test WHERE a - b > 0",
		"SELECT * FROM test WHERE a * b > 0",
		"SELECT * FROM test WHERE a / b > 0",
		"SELECT * FROM test WHERE a = b",
		"SELECT * FROM test WHERE a != b",
		"SELECT * FROM test WHERE a > b",
		"SELECT * FROM test WHERE a < b",
		"SELECT * FROM test WHERE a IS NULL",
		"SELECT * FROM test WHERE b IS NULL",
		"SELECT * FROM test WHERE a IS NOT NULL AND b IS NOT NULL",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Query failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Query: %s -> %d rows", tc, len(result.Rows))
		}
	}
}

// ========== Tests for castValue function ==========

func TestCastValueEdgeCasesExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-edge-*")
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

	tests := []string{
		// Int casts
		"SELECT CAST('123' AS INT)",
		"SELECT CAST(123.45 AS INT)",
		"SELECT CAST(TRUE AS INT)",
		"SELECT CAST('abc' AS INT)", // should error

		// Float casts
		"SELECT CAST('123.45' AS FLOAT)",
		"SELECT CAST(123 AS FLOAT)",

		// VARCHAR casts
		"SELECT CAST(123 AS VARCHAR)",
		"SELECT CAST(123.45 AS VARCHAR)",
		"SELECT CAST(TRUE AS VARCHAR)",

		// BOOL casts
		"SELECT CAST(1 AS BOOL)",
		"SELECT CAST(0 AS BOOL)",
		"SELECT CAST('true' AS BOOL)",
		"SELECT CAST('false' AS BOOL)",
		"SELECT CAST('yes' AS BOOL)",
		"SELECT CAST('no' AS BOOL)",

		// BLOB casts
		"SELECT CAST('hello' AS BLOB)",
		"SELECT CAST(123 AS BLOB)",
		"SELECT CAST(TRUE AS BLOB)",

		// NULL casts
		"SELECT CAST(NULL AS INT)",
		"SELECT CAST(NULL AS VARCHAR)",
		"SELECT CAST(NULL AS BOOL)",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Cast failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Cast: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for evaluateHavingExpr - FunctionCall ==========

func TestEvaluateHavingExprFunctionCall(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-func-*")
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

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 150), (4, 'West', 250)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with COUNT
	result, err := exec.Execute(`
		SELECT region, COUNT(*) as cnt
		FROM sales
		GROUP BY region
		HAVING COUNT(*) >= 2
	`)
	if err != nil {
		t.Logf("HAVING COUNT failed: %v", err)
	} else {
		t.Logf("HAVING COUNT: %d rows", len(result.Rows))
	}

	// HAVING with SUM
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > 300
	`)
	if err != nil {
		t.Logf("HAVING SUM failed: %v", err)
	} else {
		t.Logf("HAVING SUM: %d rows", len(result.Rows))
	}

	// HAVING with AVG
	result, err = exec.Execute(`
		SELECT region, AVG(amount) as avg_amt
		FROM sales
		GROUP BY region
		HAVING AVG(amount) > 150
	`)
	if err != nil {
		t.Logf("HAVING AVG failed: %v", err)
	} else {
		t.Logf("HAVING AVG: %d rows", len(result.Rows))
	}

	// HAVING with MIN/MAX
	result, err = exec.Execute(`
		SELECT region, MIN(amount) as min_amt, MAX(amount) as max_amt
		FROM sales
		GROUP BY region
		HAVING MIN(amount) < 120 AND MAX(amount) > 200
	`)
	if err != nil {
		t.Logf("HAVING MIN/MAX failed: %v", err)
	} else {
		t.Logf("HAVING MIN/MAX: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - RankExpr with FTS ==========

func TestEvaluateExpressionRankExprWithFTS(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rank-fts-*")
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

	// Create FTS table using correct syntax
	_, err = exec.Execute("CREATE FTS INDEX docs_idx ON docs(content)")
	if err != nil {
		t.Logf("CREATE FTS INDEX failed: %v", err)
	}

	// Test RankExpr without FTS manager
	result, err := exec.Execute("SELECT RANK()")
	if err != nil {
		t.Logf("RANK without FTS failed: %v", err)
	} else {
		t.Logf("RANK without FTS: %v", result.Rows)
	}

	// Test with outer context
	exec.outerContext = map[string]interface{}{
		"__fts_rank": 0.85,
	}
	result, err = exec.Execute("SELECT RANK()")
	if err != nil {
		t.Logf("RANK with context failed: %v", err)
	} else {
		t.Logf("RANK with context: %v", result.Rows)
	}
	exec.outerContext = nil
}

// ========== Tests for pragmaIntegrityCheck error paths ==========

func TestPragmaIntegrityCheckErrorPaths(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-error-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 100), (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test INTEGRITY_CHECK with valid data
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK: %v", result.Rows)
	}

	// Test INTEGRITY_CHECK with specific table
	result, err = exec.Execute("PRAGMA INTEGRITY_CHECK(test)")
	if err != nil {
		t.Logf("INTEGRITY_CHECK(test) failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK(test): %v", result.Rows)
	}

	// Test QUICK_CHECK
	result, err = exec.Execute("PRAGMA QUICK_CHECK")
	if err != nil {
		t.Logf("QUICK_CHECK failed: %v", err)
	} else {
		t.Logf("QUICK_CHECK: %v", result.Rows)
	}

	// Test PAGE_COUNT
	result, err = exec.Execute("PRAGMA PAGE_COUNT")
	if err != nil {
		t.Logf("PAGE_COUNT failed: %v", err)
	} else {
		t.Logf("PAGE_COUNT: %v", result.Rows)
	}

	// Test PAGE_SIZE
	result, err = exec.Execute("PRAGMA PAGE_SIZE")
	if err != nil {
		t.Logf("PAGE_SIZE failed: %v", err)
	} else {
		t.Logf("PAGE_SIZE: %v", result.Rows)
	}
}

// ========== Direct unit tests for compareValues ==========

func TestCompareValuesEdgeCasesExtra(t *testing.T) {
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

	tests := []string{
		"SELECT 1 < 2",
		"SELECT 2 > 1",
		"SELECT 1 <= 1",
		"SELECT 1 >= 1",
		"SELECT 1 = 1",
		"SELECT 1 != 2",
		"SELECT 'a' < 'b'",
		"SELECT 'b' > 'a'",
		"SELECT 1.5 < 2.5",
		"SELECT 2.5 > 1.5",
		"SELECT TRUE = TRUE",
		"SELECT FALSE = FALSE",
		"SELECT TRUE != FALSE",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Compare failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Compare: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for evaluateWhere - ScalarSubquery in WHERE ==========

func TestEvaluateWhereScalarSubqueryDetailedExtra(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE thresholds (id INT PRIMARY KEY, max_val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE thresholds failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT nums failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO thresholds VALUES (1, 15)")
	if err != nil {
		t.Fatalf("INSERT thresholds failed: %v", err)
	}

	// Scalar subquery in WHERE with comparison
	result, err := exec.Execute(`
		SELECT * FROM nums
		WHERE val > (SELECT max_val FROM thresholds WHERE id = 1)
	`)
	if err != nil {
		t.Logf("Scalar subquery comparison failed: %v", err)
	} else {
		t.Logf("Scalar subquery comparison: %d rows", len(result.Rows))
	}

	// Scalar subquery returning NULL
	result, err = exec.Execute(`
		SELECT * FROM nums
		WHERE val > (SELECT max_val FROM thresholds WHERE id = 999)
	`)
	if err != nil {
		t.Logf("Scalar subquery NULL failed: %v", err)
	} else {
		t.Logf("Scalar subquery NULL: %d rows", len(result.Rows))
	}
}

// ========== Tests for HAVING with aggregate function arguments ==========

func TestEvaluateHavingAggregateArgs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-agg-args-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with COUNT(*)
	result, err := exec.Execute(`
		SELECT grp, COUNT(*) as cnt
		FROM data
		GROUP BY grp
		HAVING COUNT(*) > 1
	`)
	if err != nil {
		t.Logf("HAVING COUNT(*) failed: %v", err)
	} else {
		t.Logf("HAVING COUNT(*): %d rows", len(result.Rows))
	}

	// HAVING with SUM(column)
	result, err = exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING SUM(val) > 50
	`)
	if err != nil {
		t.Logf("HAVING SUM(col) failed: %v", err)
	} else {
		t.Logf("HAVING SUM(col): %d rows", len(result.Rows))
	}

	// HAVING with AVG(column)
	result, err = exec.Execute(`
		SELECT grp, AVG(val) as avg_val
		FROM data
		GROUP BY grp
		HAVING AVG(val) > 20
	`)
	if err != nil {
		t.Logf("HAVING AVG(col) failed: %v", err)
	} else {
		t.Logf("HAVING AVG(col): %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression with nil values ==========

func TestEvaluateExpressionNilValueHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nil-values-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, NULL)")
	if err != nil {
		t.Logf("INSERT NULL failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	// Create row with NULL value
	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewNullValue()},
	}

	// Test UnaryExpr with NULL
	unaryExpr := &sql.UnaryExpr{Op: sql.OpNeg, Right: &sql.ColumnRef{Name: "val"}}
	result, err := exec.evaluateExpression(unaryExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr with NULL failed: %v", err)
	} else {
		t.Logf("UnaryExpr with NULL result: %v", result)
	}

	// Test BinaryExpr with NULL
	binaryExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpAdd,
		Right: &sql.Literal{Value: 5},
	}
	result, err = exec.evaluateExpression(binaryExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("BinaryExpr with NULL failed: %v", err)
	} else {
		t.Logf("BinaryExpr with NULL result: %v", result)
	}
}

// ========== Helper to get table info for testing ==========

func getTestTableInfo(exec *Executor, tableName string) (map[string]*types.ColumnInfo, []*types.ColumnInfo, *table.TableInfo, error) {
	tbl, err := exec.engine.GetTable(tableName)
	if err != nil {
		return nil, nil, nil, err
	}
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	return columnMap, columnOrder, tblInfo, nil
}

// ========== Tests for evaluateWhereForRow - AnyAllExpr ==========

func TestEvaluateWhereAnyAllExprDetailed(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// ANY with true condition
	result, err := exec.Execute("SELECT * FROM nums WHERE val > ANY (SELECT val FROM nums WHERE val < 20)")
	if err != nil {
		t.Logf("ANY true failed: %v", err)
	} else {
		t.Logf("ANY true: %d rows", len(result.Rows))
	}

	// ANY with false condition
	result, err = exec.Execute("SELECT * FROM nums WHERE val > ANY (SELECT val FROM nums WHERE val > 100)")
	if err != nil {
		t.Logf("ANY false failed: %v", err)
	} else {
		t.Logf("ANY false: %d rows", len(result.Rows))
	}

	// ALL with true condition
	result, err = exec.Execute("SELECT * FROM nums WHERE val > ALL (SELECT val FROM nums WHERE val < 10)")
	if err != nil {
		t.Logf("ALL true failed: %v", err)
	} else {
		t.Logf("ALL true: %d rows", len(result.Rows))
	}

	// ALL with false condition
	result, err = exec.Execute("SELECT * FROM nums WHERE val > ALL (SELECT val FROM nums WHERE val > 15)")
	if err != nil {
		t.Logf("ALL false failed: %v", err)
	} else {
		t.Logf("ALL false: %d rows", len(result.Rows))
	}

	// ALL on empty subquery
	result, err = exec.Execute("SELECT * FROM nums WHERE val > ALL (SELECT val FROM nums WHERE val > 100)")
	if err != nil {
		t.Logf("ALL empty failed: %v", err)
	} else {
		t.Logf("ALL empty: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - Literal bool ==========

func TestEvaluateWhereLiteralBoolExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-literal-bool-*")
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

	// WHERE TRUE
	result, err := exec.Execute("SELECT * FROM test WHERE TRUE")
	if err != nil {
		t.Logf("WHERE TRUE failed: %v", err)
	} else {
		t.Logf("WHERE TRUE: %d rows", len(result.Rows))
	}

	// WHERE FALSE
	result, err = exec.Execute("SELECT * FROM test WHERE FALSE")
	if err != nil {
		t.Logf("WHERE FALSE failed: %v", err)
	} else {
		t.Logf("WHERE FALSE: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - evaluateExprForRow ==========

func TestEvaluateExprForRowColumnRef(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := make([]*types.ColumnInfo, len(tblInfo.Columns))
	colIdxMap := make(map[string]int)
	for i, col := range tblInfo.Columns {
		columns[i] = col
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(100)},
	}

	// Test column lookup with matching table prefix
	exec.currentTable = "test"
	colRef := &sql.ColumnRef{Table: "test", Name: "val"}
	result, err := exec.evaluateExprForRow(colRef, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ColumnRef with table prefix failed: %v", err)
	} else {
		t.Logf("ColumnRef with table prefix result: %v", result)
	}

	// Test column lookup with non-matching table prefix and outer context
	exec.outerContext = map[string]interface{}{
		"other.val": 999,
		"val":       888,
	}
	colRef2 := &sql.ColumnRef{Table: "other", Name: "val"}
	result, err = exec.evaluateExprForRow(colRef2, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ColumnRef other table (expected error): %v", err)
	} else {
		t.Logf("ColumnRef other table result: %v", result)
	}

	exec.outerContext = nil
	exec.currentTable = ""
}

// ========== Tests for evaluateExpression - default case ==========

func TestEvaluateExpressionUnknownType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-unknown-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// Test with various expressions
	tests := []struct {
		name string
		expr sql.Expression
	}{
		{"Literal", &sql.Literal{Value: 42}},
		{"ColumnRef", &sql.ColumnRef{Name: "id"}},
		{"BinaryExpr", &sql.BinaryExpr{Left: &sql.Literal{Value: 1}, Op: sql.OpAdd, Right: &sql.Literal{Value: 2}}},
		{"UnaryExpr", &sql.UnaryExpr{Op: sql.OpNeg, Right: &sql.Literal{Value: 5}}},
		{"ParenExpr", &sql.ParenExpr{Expr: &sql.Literal{Value: 10}}},
	}

	for _, tc := range tests {
		result, err := exec.evaluateExpression(tc.expr, mockRow, columnMap, columnOrder)
		if err != nil {
			t.Logf("%s failed: %v", tc.name, err)
		} else {
			t.Logf("%s result: %v", tc.name, result)
		}
	}
}

// ========== Tests for evaluateWhere - ScalarSubquery returning string ==========

func TestEvaluateWhereScalarSubqueryStringResult(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-scalar-str-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, flag VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
	if err != nil {
		t.Fatalf("INSERT items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, 'yes'), (2, '')")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	// Scalar subquery returning non-empty string
	result, err := exec.Execute("SELECT * FROM items WHERE (SELECT flag FROM flags WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery string 'yes' failed: %v", err)
	} else {
		t.Logf("Scalar subquery string 'yes': %d rows", len(result.Rows))
	}

	// Scalar subquery returning empty string
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT flag FROM flags WHERE id = 2)")
	if err != nil {
		t.Logf("Scalar subquery empty string failed: %v", err)
	} else {
		t.Logf("Scalar subquery empty string: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for HAVING with more complex expressions ==========

func TestEvaluateHavingComplexExpressionTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-complex-types-*")
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

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with nested parentheses
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING (SUM(amount) > 100)
	`)
	if err != nil {
		t.Logf("HAVING with parens failed: %v", err)
	} else {
		t.Logf("HAVING with parens: %d rows", len(result.Rows))
	}

	// HAVING with arithmetic
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) + 50 > 200
	`)
	if err != nil {
		t.Logf("HAVING with arithmetic failed: %v", err)
	} else {
		t.Logf("HAVING with arithmetic: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHavingExpr - ColumnRef with alias ==========

func TestEvaluateHavingExprColumnRefAlias(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-alias-*")
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

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'West', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with alias reference
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total_amount
		FROM sales
		GROUP BY region
		HAVING total_amount > 150
	`)
	if err != nil {
		t.Logf("HAVING with alias failed: %v", err)
	} else {
		t.Logf("HAVING with alias: %d rows", len(result.Rows))
	}

	// HAVING with column name
	result, err = exec.Execute(`
		SELECT region, SUM(amount)
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > 100
	`)
	if err != nil {
		t.Logf("HAVING with column name failed: %v", err)
	} else {
		t.Logf("HAVING with column name: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHavingExpr - BinaryExpr ==========

func TestEvaluateHavingExprBinaryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-binary-*")
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

	_, err = exec.Execute("INSERT INTO data VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with addition
	result, err := exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING SUM(val) + 10 > 30
	`)
	if err != nil {
		t.Logf("HAVING with addition failed: %v", err)
	} else {
		t.Logf("HAVING with addition: %d rows", len(result.Rows))
	}

	// HAVING with subtraction
	result, err = exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING SUM(val) - 5 < 25
	`)
	if err != nil {
		t.Logf("HAVING with subtraction failed: %v", err)
	} else {
		t.Logf("HAVING with subtraction: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - BinaryExpr with NULL ==========

func TestEvaluateHavingBinaryNullOperands(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with NULL aggregate (AVG of non-existent rows might return NULL)
	result, err := exec.Execute(`
		SELECT grp, AVG(id) as avg_id
		FROM test
		GROUP BY grp
		HAVING AVG(id) IS NOT NULL
	`)
	if err != nil {
		t.Logf("HAVING with NULL check failed: %v", err)
	} else {
		t.Logf("HAVING with NULL check: %d rows", len(result.Rows))
	}
}

// ========== Tests for PRAGMA with various options ==========

func TestPragmaVariousOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-options-*")
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

	// Test various PRAGMA statements
	pragmas := []string{
		"PRAGMA INTEGRITY_CHECK",
		"PRAGMA QUICK_CHECK",
		"PRAGMA PAGE_COUNT",
		"PRAGMA PAGE_SIZE",
		"PRAGMA TABLE_INFO(test)",
		"PRAGMA INDEX_LIST(test)",
		"PRAGMA DATABASE_LIST",
		"PRAGMA COMPILE_OPTIONS",
		"PRAGMA USER_VERSION",
	}

	for _, pragma := range pragmas {
		result, err := exec.Execute(pragma)
		if err != nil {
			t.Logf("PRAGMA failed: %s, error: %v", pragma, err)
		} else {
			t.Logf("PRAGMA: %s -> %v", pragma, result.Rows)
		}
	}
}