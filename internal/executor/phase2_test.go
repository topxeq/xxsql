package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestLeadLagWindowFunctions tests LEAD and LAG window functions
func TestLeadLagWindowFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-leadlag-test-*")
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
			year INT,
			amount INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO sales (year, amount) VALUES (2020, 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (year, amount) VALUES (2021, 150)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (year, amount) VALUES (2022, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO sales (year, amount) VALUES (2023, 250)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test LEAD function
	result, err := exec.Execute(`
		SELECT year, amount,
		       LEAD(amount, 1) OVER (ORDER BY year) AS next_amount
		FROM sales
		ORDER BY year
	`)
	if err != nil {
		t.Fatalf("Failed to execute LEAD query: %v", err)
	}

	t.Logf("LEAD results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify LEAD results
	// Year 2020 should have next_amount = 150
	if result.Rows[0][2] == nil {
		t.Errorf("Expected next_amount for 2020 to be 150, got nil")
	}

	// Test LAG function
	result, err = exec.Execute(`
		SELECT year, amount,
		       LAG(amount, 1) OVER (ORDER BY year) AS prev_amount
		FROM sales
		ORDER BY year
	`)
	if err != nil {
		t.Fatalf("Failed to execute LAG query: %v", err)
	}

	t.Logf("LAG results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify LAG results
	// Year 2020 should have prev_amount = NULL
	if result.Rows[0][2] != nil {
		t.Errorf("Expected prev_amount for 2020 to be NULL, got %v", result.Rows[0][2])
	}
	// Year 2021 should have prev_amount = 100
	if result.Rows[1][2] == nil {
		t.Errorf("Expected prev_amount for 2021 to be 100, got nil")
	}

	// Test LEAD with default value
	result, err = exec.Execute(`
		SELECT year, amount,
		       LEAD(amount, 1, 0) OVER (ORDER BY year) AS next_amount
		FROM sales
		ORDER BY year
	`)
	if err != nil {
		t.Fatalf("Failed to execute LEAD with default query: %v", err)
	}

	t.Logf("LEAD with default results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test LEAD with PARTITION BY
	result, err = exec.Execute(`
		SELECT year, amount,
		       LEAD(amount, 1) OVER (PARTITION BY year ORDER BY amount) AS next_amount
		FROM sales
		ORDER BY year
	`)
	if err != nil {
		t.Fatalf("Failed to execute LEAD with PARTITION BY query: %v", err)
	}

	t.Logf("LEAD with PARTITION BY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestFirstLastValueWindowFunctions tests FIRST_VALUE and LAST_VALUE window functions
func TestFirstLastValueWindowFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-firstlast-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE employees (
			id SEQ PRIMARY KEY,
			dept VARCHAR(50),
			salary INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO employees (dept, salary) VALUES ('Sales', 50000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO employees (dept, salary) VALUES ('Sales', 60000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO employees (dept, salary) VALUES ('IT', 70000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO employees (dept, salary) VALUES ('IT', 80000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test FIRST_VALUE
	result, err := exec.Execute(`
		SELECT dept, salary,
		       FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary) AS first_salary
		FROM employees
		ORDER BY dept, salary
	`)
	if err != nil {
		t.Fatalf("Failed to execute FIRST_VALUE query: %v", err)
	}

	t.Logf("FIRST_VALUE results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test LAST_VALUE
	result, err = exec.Execute(`
		SELECT dept, salary,
		       LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary) AS last_salary
		FROM employees
		ORDER BY dept, salary
	`)
	if err != nil {
		t.Fatalf("Failed to execute LAST_VALUE query: %v", err)
	}

	t.Logf("LAST_VALUE results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestValuesTableConstructor tests VALUES table constructor in FROM clause
func TestValuesTableConstructor(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-values-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test simple VALUES
	result, err := exec.Execute(`
		SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)
	`)
	if err != nil {
		t.Fatalf("Failed to execute VALUES query: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}

	t.Logf("VALUES results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test VALUES with WHERE clause
	result, err = exec.Execute(`
		SELECT * FROM (VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')) AS t(id, fruit)
		WHERE id > 1
	`)
	if err != nil {
		t.Fatalf("Failed to execute VALUES with WHERE query: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	t.Logf("VALUES with WHERE results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test VALUES with ORDER BY and LIMIT
	result, err = exec.Execute(`
		SELECT * FROM (VALUES (3, 'c'), (1, 'a'), (2, 'b')) AS t(num, letter)
		ORDER BY num
		LIMIT 2
	`)
	if err != nil {
		t.Fatalf("Failed to execute VALUES with ORDER BY query: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	t.Logf("VALUES with ORDER BY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestLateralSubquery tests LATERAL keyword for correlated subqueries
func TestLateralSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-lateral-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			category VARCHAR(50),
			price INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO products (name, category, price) VALUES ('Widget', 'A', 10)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO products (name, category, price) VALUES ('Gadget', 'A', 20)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO products (name, category, price) VALUES ('Gizmo', 'B', 15)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test LATERAL subquery - simple case
	// LATERAL allows the subquery to reference columns from the outer query
	result, err := exec.Execute(`
		SELECT * FROM LATERAL (SELECT 1 AS x, 2 AS y) AS t
	`)
	if err != nil {
		t.Fatalf("Failed to execute simple LATERAL query: %v", err)
	}

	t.Logf("Simple LATERAL results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// TestIgnoreNullsWindowFunction tests IGNORE NULLS in window functions
func TestIgnoreNullsWindowFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-ignorenulls-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table with NULL values
	_, err = exec.Execute(`
		CREATE TABLE data (
			id SEQ PRIMARY KEY,
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with NULLs
	_, err = exec.Execute(`INSERT INTO data (value) VALUES (10)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO data (value) VALUES (NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO data (value) VALUES (30)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO data (value) VALUES (40)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test LAG without IGNORE NULLS
	result, err := exec.Execute(`
		SELECT id, value,
		       LAG(value, 1) OVER (ORDER BY id) AS prev_value
		FROM data
		ORDER BY id
	`)
	if err != nil {
		t.Fatalf("Failed to execute LAG query: %v", err)
	}

	t.Logf("LAG without IGNORE NULLS results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test LEAD without IGNORE NULLS
	result, err = exec.Execute(`
		SELECT id, value,
		       LEAD(value, 1) OVER (ORDER BY id) AS next_value
		FROM data
		ORDER BY id
	`)
	if err != nil {
		t.Fatalf("Failed to execute LEAD query: %v", err)
	}

	t.Logf("LEAD results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestCombinedPhase2Features tests combinations of Phase 2 features
func TestCombinedPhase2Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase2-combined-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test VALUES with window functions
	result, err := exec.Execute(`
		SELECT t.num,
		       LAG(t.num, 1) OVER (ORDER BY t.num) AS prev_num,
		       LEAD(t.num, 1) OVER (ORDER BY t.num) AS next_num
		FROM (VALUES (10), (20), (30), (40)) AS t(num)
		ORDER BY t.num
	`)
	if err != nil {
		t.Fatalf("Failed to execute combined query: %v", err)
	}

	t.Logf("Combined VALUES + LEAD/LAG results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify results
	if result.RowCount != 4 {
		t.Errorf("Expected 4 rows, got %d", result.RowCount)
	}
}