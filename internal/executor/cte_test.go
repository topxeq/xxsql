package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestCTEDebug(t *testing.T) {
	// Test simple SELECT without FROM works
	parser := sql.NewParser("SELECT 1 AS a")
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*sql.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	t.Logf("SELECT stmt: %+v", selectStmt)
	t.Logf("SELECT stmt.From: %+v", selectStmt.From)

	// Test WITH clause parsing
	parser2 := sql.NewParser("WITH cte AS (SELECT 1 AS a) SELECT * FROM cte")
	stmt2, err := parser2.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	withStmt, ok := stmt2.(*sql.WithStmt)
	if !ok {
		t.Fatalf("Expected WithStmt, got %T", stmt2)
	}

	t.Logf("WITH stmt: %+v", withStmt)
	t.Logf("CTEs: %+v", withStmt.CTEs)
	if len(withStmt.CTEs) > 0 {
		t.Logf("First CTE name: %s", withStmt.CTEs[0].Name)
		t.Logf("First CTE query type: %T", withStmt.CTEs[0].Query)
		if q, ok := withStmt.CTEs[0].Query.(*sql.SelectStmt); ok {
			t.Logf("First CTE query: %+v", q)
			t.Logf("First CTE query.From: %+v", q.From)
		}
	}
	t.Logf("Main query type: %T", withStmt.MainQuery)
	if mq, ok := withStmt.MainQuery.(*sql.SelectStmt); ok {
		t.Logf("Main query: %+v", mq)
		t.Logf("Main query.From: %+v", mq.From)
		if mq.From != nil && mq.From.Table != nil {
			t.Logf("Main query table name: %s", mq.From.Table.Name)
		}
	}
}

func TestCTEExecution(t *testing.T) {
	// Create a test engine with a temporary directory
	tmpDir, err := os.MkdirTemp("", "xxsql_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	engine := storage.NewEngine(dataDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create an executor
	exec := NewExecutor(engine)

	// Test 1: Simple SELECT without FROM (should work)
	t.Run("SelectWithoutFrom", func(t *testing.T) {
		result, err := exec.Execute("SELECT 1 AS a")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)
		t.Logf("Columns: %+v", result.Columns)
		t.Logf("Rows: %+v", result.Rows)
		t.Logf("RowCount: %d", result.RowCount)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Test 2: Simple CTE
	t.Run("SimpleCTE", func(t *testing.T) {
		result, err := exec.Execute("WITH cte AS (SELECT 1 AS a) SELECT * FROM cte")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)
		t.Logf("Columns: %+v", result.Columns)
		t.Logf("Rows: %+v", result.Rows)
		t.Logf("RowCount: %d", result.RowCount)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Test 3: CTE with multiple columns
	t.Run("CTEMultipleColumns", func(t *testing.T) {
		result, err := exec.Execute("WITH cte AS (SELECT 1 AS a, 2 AS b, 'hello' AS c) SELECT a, c FROM cte")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
	})

	// Test 4: Multiple CTEs with separate queries
	t.Run("MultipleCTEs", func(t *testing.T) {
		// Query using multiple CTEs where second references first
		result, err := exec.Execute("WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT 2 AS b) SELECT a FROM cte1")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Test 4b: CTE referencing another CTE
	t.Run("CTEReferencingCTE", func(t *testing.T) {
		// First test: simple reference without expression
		result, err := exec.Execute("WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT a FROM cte1) SELECT a FROM cte2")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Test 4c: CTE referencing another CTE with expression
	t.Run("CTEReferencingCTEWithExpr", func(t *testing.T) {
		// Query where second CTE references first with expression
		result, err := exec.Execute("WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT a + 1 AS b FROM cte1) SELECT b FROM cte2")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Test 5: CTE with table
	t.Run("CTEWithTable", func(t *testing.T) {
		// First create a test table
		_, err := exec.Execute("CREATE TABLE test_users (id INT, name VARCHAR)")
		if err != nil {
			t.Fatalf("Create table error: %v", err)
		}

		// Insert test data
		_, err = exec.Execute("INSERT INTO test_users (id, name) VALUES (1, 'Alice')")
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
		_, err = exec.Execute("INSERT INTO test_users (id, name) VALUES (2, 'Bob')")
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}

		// Query using CTE
		result, err := exec.Execute("WITH user_cte AS (SELECT id, name FROM test_users WHERE id > 1) SELECT * FROM user_cte")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Test 6: CTE with column aliases
	t.Run("CTEWithColumnAliases", func(t *testing.T) {
		result, err := exec.Execute("WITH cte(x, y) AS (SELECT 1, 2) SELECT x, y FROM cte")
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
		// Check column names
		if result.Columns[0].Name != "x" {
			t.Errorf("Expected column 0 name 'x', got '%s'", result.Columns[0].Name)
		}
		if result.Columns[1].Name != "y" {
			t.Errorf("Expected column 1 name 'y', got '%s'", result.Columns[1].Name)
		}
	})
}

func TestRecursiveCTE(t *testing.T) {
	// Create a test engine with a temporary directory
	tmpDir, err := os.MkdirTemp("", "xxsql_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	engine := storage.NewEngine(dataDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	// Test recursive CTE for generating numbers 1-5
	t.Run("GenerateSequence", func(t *testing.T) {
		result, err := exec.Execute(`
			WITH RECURSIVE nums AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM nums WHERE n < 5
			)
			SELECT n FROM nums
		`)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 5 {
			t.Errorf("Expected 5 rows, got %d", result.RowCount)
		}
	})

	// Test recursive CTE with table data
	t.Run("RecursiveWithTable", func(t *testing.T) {
		// Create a simple numbers table
		_, err := exec.Execute("CREATE TABLE start_nums (n INT)")
		if err != nil {
			t.Fatalf("Create table error: %v", err)
		}

		// Insert test data
		_, err = exec.Execute("INSERT INTO start_nums (n) VALUES (1)")
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}

		// Query with recursive CTE starting from table
		result, err := exec.Execute(`
			WITH RECURSIVE numbers AS (
				SELECT n FROM start_nums
				UNION ALL
				SELECT n + 1 FROM numbers WHERE n < 5
			)
			SELECT n FROM numbers
		`)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 5 {
			t.Errorf("Expected 5 rows, got %d", result.RowCount)
		}
	})
}

func TestCTEWithAggregates(t *testing.T) {
	// Create a test engine with a temporary directory
	tmpDir, err := os.MkdirTemp("", "xxsql_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	engine := storage.NewEngine(dataDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	// Create a sales table
	_, err = exec.Execute("CREATE TABLE sales (product VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO sales (product, amount) VALUES ('A', 100)")
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales (product, amount) VALUES ('A', 200)")
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales (product, amount) VALUES ('B', 150)")
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Test CTE with aggregation
	t.Run("CTEWithGroupBy", func(t *testing.T) {
		result, err := exec.Execute(`
			WITH product_totals AS (
				SELECT product, SUM(amount) AS total
				FROM sales
				GROUP BY product
			)
			SELECT * FROM product_totals
		`)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 2 {
			t.Errorf("Expected 2 rows, got %d", result.RowCount)
		}
	})

	// Test CTE with HAVING
	t.Run("CTEWithHaving", func(t *testing.T) {
		result, err := exec.Execute(`
			WITH high_sellers AS (
				SELECT product, SUM(amount) AS total
				FROM sales
				GROUP BY product
				HAVING SUM(amount) > 200
			)
			SELECT * FROM high_sellers
		`)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
		t.Logf("Result: %+v", result)

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})
}