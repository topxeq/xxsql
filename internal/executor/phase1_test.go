package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestNullsFirstLast tests ORDER BY with NULLS FIRST and NULLS LAST
func TestNullsFirstLast(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nulls-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table with nullable column
	_, err = exec.Execute(`
		CREATE TABLE items (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows with some NULL prices
	_, err = exec.Execute(`INSERT INTO items (name, price) VALUES ('apple', 10)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO items (name, price) VALUES ('banana', NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO items (name, price) VALUES ('cherry', 20)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO items (name, price) VALUES ('date', NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO items (name, price) VALUES ('elderberry', 15)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test NULLS FIRST
	result, err := exec.Execute(`SELECT name, price FROM items ORDER BY price NULLS FIRST`)
	if err != nil {
		t.Fatalf("Failed to select with NULLS FIRST: %v", err)
	}

	// Verify NULLs come first
	if result.RowCount != 5 {
		t.Fatalf("Expected 5 rows, got %d", result.RowCount)
	}

	// First two rows should have NULL prices
	for i := 0; i < 2; i++ {
		if result.Rows[i][1] != nil {
			t.Errorf("Expected NULL in row %d price, got %v", i, result.Rows[i][1])
		}
	}

	t.Logf("NULLS FIRST order:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: name=%v, price=%v", i, row[0], row[1])
	}

	// Test NULLS LAST
	result, err = exec.Execute(`SELECT name, price FROM items ORDER BY price NULLS LAST`)
	if err != nil {
		t.Fatalf("Failed to select with NULLS LAST: %v", err)
	}

	// Last two rows should have NULL prices
	for i := 3; i < 5; i++ {
		if result.Rows[i][1] != nil {
			t.Errorf("Expected NULL in row %d price, got %v", i, result.Rows[i][1])
		}
	}

	t.Logf("NULLS LAST order:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: name=%v, price=%v", i, row[0], row[1])
	}

	// Test NULLS FIRST with DESC
	result, err = exec.Execute(`SELECT name, price FROM items ORDER BY price DESC NULLS FIRST`)
	if err != nil {
		t.Fatalf("Failed to select with NULLS FIRST DESC: %v", err)
	}

	t.Logf("NULLS FIRST DESC order:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: name=%v, price=%v", i, row[0], row[1])
	}

	// Test NULLS LAST with DESC
	result, err = exec.Execute(`SELECT name, price FROM items ORDER BY price DESC NULLS LAST`)
	if err != nil {
		t.Fatalf("Failed to select with NULLS LAST DESC: %v", err)
	}

	t.Logf("NULLS LAST DESC order:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: name=%v, price=%v", i, row[0], row[1])
	}
}

// TestAggregateFilterClause tests FILTER clause for aggregate functions
func TestAggregateFilterClause(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-filter-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			product VARCHAR(100),
			amount INT,
			status VARCHAR(20)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO orders (product, amount, status) VALUES ('Widget', 100, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO orders (product, amount, status) VALUES ('Gadget', 200, 'pending')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO orders (product, amount, status) VALUES ('Widget', 150, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO orders (product, amount, status) VALUES ('Gadget', 50, 'cancelled')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO orders (product, amount, status) VALUES ('Widget', 75, 'pending')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test COUNT with FILTER
	result, err := exec.Execute(`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
			COUNT(*) FILTER (WHERE status = 'pending') AS pending_count
		FROM orders
	`)
	if err != nil {
		t.Fatalf("Failed to select with FILTER: %v", err)
	}

	if result.RowCount != 1 {
		t.Fatalf("Expected 1 row, got %d", result.RowCount)
	}

	// Verify counts
	total := result.Rows[0][0].(int)
	if total != 5 {
		t.Errorf("Expected total count 5, got %d", total)
	}

	t.Logf("COUNT with FILTER result: %v", result.Rows[0])

	// Test SUM with FILTER
	result, err = exec.Execute(`
		SELECT
			SUM(amount) AS total_amount,
			SUM(amount) FILTER (WHERE status = 'completed') AS completed_amount
		FROM orders
	`)
	if err != nil {
		t.Fatalf("Failed to select SUM with FILTER: %v", err)
	}

	t.Logf("SUM with FILTER result: %v", result.Rows[0])

	// Test AVG with FILTER
	result, err = exec.Execute(`
		SELECT
			AVG(amount) AS avg_amount,
			AVG(amount) FILTER (WHERE status = 'completed') AS avg_completed
		FROM orders
	`)
	if err != nil {
		t.Fatalf("Failed to select AVG with FILTER: %v", err)
	}

	t.Logf("AVG with FILTER result: %v", result.Rows[0])

	// Test GROUP BY with FILTER
	result, err = exec.Execute(`
		SELECT
			product,
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed
		FROM orders
		GROUP BY product
		ORDER BY product
	`)
	if err != nil {
		t.Fatalf("Failed to select GROUP BY with FILTER: %v", err)
	}

	t.Logf("GROUP BY with FILTER results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestWithClauseDML tests WITH clause for DML statements
func TestWithClauseDML(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-with-dml-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create source table
	_, err = exec.Execute(`
		CREATE TABLE source (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Create target table
	_, err = exec.Execute(`
		CREATE TABLE target (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create target table: %v", err)
	}

	// Insert source data
	_, err = exec.Execute(`INSERT INTO source (name, value) VALUES ('Alice', 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO source (name, value) VALUES ('Bob', 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO source (name, value) VALUES ('Charlie', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test INSERT with WITH clause (using VALUES)
	result, err := exec.Execute(`
		WITH bonus AS (
			SELECT 50 AS extra
		)
		INSERT INTO target (name, value) VALUES ('Dave', 400)
	`)
	if err != nil {
		t.Fatalf("Failed to INSERT with WITH clause: %v", err)
	}

	t.Logf("INSERT with WITH clause affected %d rows", result.Affected)

	// Insert more data into target
	_, err = exec.Execute(`INSERT INTO target (name, value) VALUES ('Eve', 500)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify the inserts
	result, err = exec.Execute(`SELECT * FROM target ORDER BY value`)
	if err != nil {
		t.Fatalf("Failed to select from target: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows in target, got %d", result.RowCount)
	}

	t.Logf("Target after INSERT with WITH:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test UPDATE with WITH clause using subquery
	result, err = exec.Execute(`
		WITH bonus AS (
			SELECT 50 AS bonus_amount
		)
		UPDATE target SET value = value + (SELECT bonus_amount FROM bonus)
	`)
	if err != nil {
		t.Fatalf("Failed to UPDATE with WITH clause: %v", err)
	}

	t.Logf("UPDATE with WITH clause affected %d rows", result.Affected)

	// Verify the update
	result, err = exec.Execute(`SELECT * FROM target ORDER BY value`)
	if err != nil {
		t.Fatalf("Failed to select after update: %v", err)
	}

	t.Logf("Target after UPDATE with WITH:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Insert some records to source that match target names
	_, err = exec.Execute(`INSERT INTO source (name, value) VALUES ('Dave', 999)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test DELETE with WITH clause using subquery
	result, err = exec.Execute(`
		WITH low_value AS (
			SELECT name FROM target WHERE value < 500
		)
		DELETE FROM source WHERE name IN (SELECT name FROM low_value)
	`)
	if err != nil {
		t.Fatalf("Failed to DELETE with WITH clause: %v", err)
	}

	t.Logf("DELETE with WITH clause affected %d rows", result.Affected)

	// Verify the delete
	result, err = exec.Execute(`SELECT * FROM source ORDER BY value`)
	if err != nil {
		t.Fatalf("Failed to select after delete: %v", err)
	}

	t.Logf("Source after DELETE with WITH:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestCombinedPhase1Features tests combinations of Phase 1 features
func TestCombinedPhase1Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase1-combined-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE sales (
			id SEQ PRIMARY KEY,
			product VARCHAR(100),
			region VARCHAR(50),
			amount INT,
			status VARCHAR(20)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with some NULLs
	_, err = exec.Execute(`INSERT INTO sales (product, region, amount, status) VALUES ('Widget', 'North', 100, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (product, region, amount, status) VALUES ('Widget', 'South', 150, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (product, region, amount, status) VALUES ('Widget', 'East', NULL, 'pending')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (product, region, amount, status) VALUES ('Gadget', 'North', 200, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (product, region, amount, status) VALUES ('Gadget', 'West', NULL, 'cancelled')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (product, region, amount, status) VALUES ('Gadget', 'South', 250, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test: FILTER with NULLS FIRST/LAST
	result, err := exec.Execute(`
		SELECT
			product,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status = 'pending') AS pending,
			SUM(amount) FILTER (WHERE status = 'completed') AS total_completed
		FROM sales
		GROUP BY product
		ORDER BY total_completed DESC NULLS LAST
	`)
	if err != nil {
		t.Fatalf("Failed to execute combined query: %v", err)
	}

	t.Logf("Combined FILTER + NULLS LAST results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test: WITH clause + FILTER + NULLS
	result, err = exec.Execute(`
		WITH completed_sales AS (
			SELECT product, region, amount FROM sales WHERE status = 'completed'
		)
		SELECT
			product,
			SUM(amount) AS total,
			COUNT(*) FILTER (WHERE region = 'North') AS north_count
		FROM completed_sales
		GROUP BY product
		ORDER BY total DESC NULLS FIRST
	`)
	if err != nil {
		t.Fatalf("Failed to execute WITH + FILTER + NULLS query: %v", err)
	}

	t.Logf("WITH + FILTER + NULLS results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}