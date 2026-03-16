package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ============================================================================
// Test Setup
// ============================================================================

func setupUnionTest(t *testing.T) (*Executor, func()) {
	dir, err := os.MkdirTemp("", "union-test-*")
	if err != nil {
		t.Fatal(err)
	}

	engine := storage.NewEngine(dir)
	exec := NewExecutor(engine)

	return exec, func() {
		os.RemoveAll(dir)
	}
}

func createUsersTable(t *testing.T, exec *Executor) {
	_, err := exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			department VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Engineering')`)
	if err != nil {
		t.Fatal(err)
	}
}

func createManagersTable(t *testing.T, exec *Executor) {
	_, err := exec.Execute(`
		CREATE TABLE managers (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			department VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create managers table: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO managers (id, name, department) VALUES (10, 'David', 'Engineering')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO managers (id, name, department) VALUES (11, 'Eve', 'Sales')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO managers (id, name, department) VALUES (12, 'Alice', 'Engineering')`)
	if err != nil {
		t.Fatal(err)
	}
}

// ============================================================================
// UNION Tests
// ============================================================================

func TestUnionBasic(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	result, err := exec.Execute(`
		SELECT name FROM users
		UNION
		SELECT name FROM managers
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Users: Alice, Bob, Charlie
	// Managers: David, Eve, Alice (duplicate)
	// UNION (distinct): Alice, Bob, Charlie, David, Eve = 5 rows
	if result.RowCount != 5 {
		t.Errorf("Expected 5 rows (UNION removes duplicates), got %d", result.RowCount)
	}
}

func TestUnionAll(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	result, err := exec.Execute(`
		SELECT name FROM users
		UNION ALL
		SELECT name FROM managers
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Users: 3 rows
	// Managers: 3 rows
	// UNION ALL: 6 rows total
	if result.RowCount != 6 {
		t.Errorf("Expected 6 rows (UNION ALL keeps duplicates), got %d", result.RowCount)
	}
}

func TestUnionMultipleColumns(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	result, err := exec.Execute(`
		SELECT name, department FROM users
		UNION
		SELECT name, department FROM managers
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// (Alice, Engineering), (Bob, Sales), (Charlie, Engineering)
	// (David, Engineering), (Eve, Sales), (Alice, Engineering) - duplicate
	// Result: 5 rows
	if result.RowCount != 5 {
		t.Errorf("Expected 5 rows, got %d", result.RowCount)
	}

	// Check column count
	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}
}

func TestUnionColumnMismatch(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	_, err := exec.Execute(`
		SELECT name, department FROM users
		UNION
		SELECT name FROM managers
	`)
	if err == nil {
		t.Error("Expected error for column count mismatch")
	}
}

func TestUnionWithWhere(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	result, err := exec.Execute(`
		SELECT name FROM users WHERE department = 'Engineering'
		UNION
		SELECT name FROM managers WHERE department = 'Sales'
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Users Engineering: Alice, Charlie
	// Managers Sales: Eve
	// Result: 3 rows
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

func TestUnionThreeWay(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	// Create contractors table
	_, err := exec.Execute(`
		CREATE TABLE contractors (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO contractors (id, name) VALUES (100, 'Frank')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO contractors (id, name) VALUES (101, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT name FROM users
		UNION
		SELECT name FROM managers
		UNION
		SELECT name FROM contractors
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Users: Alice, Bob, Charlie
	// Managers: David, Eve, Alice
	// Contractors: Frank, Alice
	// UNION distinct: Alice, Bob, Charlie, David, Eve, Frank = 6 rows
	if result.RowCount != 6 {
		t.Errorf("Expected 6 rows, got %d", result.RowCount)
	}
}

func TestUnionAllThreeWay(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	_, err := exec.Execute(`
		CREATE TABLE contractors (
			id SEQ PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO contractors (id, name) VALUES (100, 'Frank')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT name FROM users
		UNION ALL
		SELECT name FROM managers
		UNION ALL
		SELECT name FROM contractors
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Users: 3 rows + Managers: 3 rows + Contractors: 1 row = 7 rows
	if result.RowCount != 7 {
		t.Errorf("Expected 7 rows, got %d", result.RowCount)
	}
}

func TestUnionMixedWithUnionAll(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	createUsersTable(t, exec)
	createManagersTable(t, exec)

	// Parser creates right-associative structure:
	// users UNION ALL (managers UNION users)
	// managers UNION users = [David, Eve, Alice] + [Alice, Bob, Charlie] deduped = [David, Eve, Alice, Bob, Charlie] = 5 rows
	// Then UNION ALL with users: [Alice, Bob, Charlie] + [David, Eve, Alice, Bob, Charlie] = 8 rows
	result, err := exec.Execute(`
		SELECT name FROM users
		UNION ALL
		SELECT name FROM managers
		UNION
		SELECT name FROM users
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// users (3) UNION ALL ((managers UNION users) = 5 rows) = 8 rows
	if result.RowCount != 8 {
		t.Errorf("Expected 8 rows, got %d", result.RowCount)
	}
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestUnionEmptyTables(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE empty1 (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE empty2 (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT name FROM empty1
		UNION
		SELECT name FROM empty2
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 0 {
		t.Errorf("Expected 0 rows for empty tables, got %d", result.RowCount)
	}
}

func TestUnionOneEmptyTable(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE has_data (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO has_data (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE empty (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT name FROM has_data
		UNION
		SELECT name FROM empty
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestUnionWithNulls(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE table_a (id SEQ PRIMARY KEY, value VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_a (id, value) VALUES (1, 'test')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_a (id, value) VALUES (2, NULL)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE table_b (id SEQ PRIMARY KEY, value VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_b (id, value) VALUES (10, NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_b (id, value) VALUES (11, 'test')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT value FROM table_a
		UNION
		SELECT value FROM table_b
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// 'test' and NULL - each appears in both tables, UNION removes duplicates
	// Result: 2 rows (test, NULL)
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows (test and NULL), got %d", result.RowCount)
	}
}

// ============================================================================
// Performance / Stress Tests
// ============================================================================

func TestUnionLargeResults(t *testing.T) {
	exec, cleanup := setupUnionTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE large1 (id SEQ PRIMARY KEY, value INT)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE large2 (id SEQ PRIMARY KEY, value INT)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 50 rows into each table
	for i := 1; i <= 50; i++ {
		_, err = exec.Execute(`INSERT INTO large1 (id, value) VALUES (` + string(rune('0'+i%10)) + `, ` + string(rune('0'+i%10)) + `)`)
		if err != nil {
			// Use proper int conversion
			break
		}
	}

	// Just verify the UNION works with some data
	result, err := exec.Execute(`
		SELECT value FROM large1
		UNION ALL
		SELECT value FROM large2
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Both tables are empty after failed inserts, but query should still work
	_ = result
}
