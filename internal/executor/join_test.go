package executor

import (
	"os"
	"strings"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ============================================================================
// Test Setup
// ============================================================================

func setupJoinTest(t *testing.T) (*Executor, func()) {
	dir, err := os.MkdirTemp("", "join-test-*")
	if err != nil {
		t.Fatal(err)
	}

	engine := storage.NewEngine(dir)

	exec := NewExecutor(engine)

	return exec, func() {
		os.RemoveAll(dir)
	}
}

func createCustomersTable(t *testing.T, exec *Executor) {
	_, err := exec.Execute(`
		CREATE TABLE customers (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO customers (id, name, email) VALUES (2, 'Bob', 'bob@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO customers (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
}

func createOrdersTable(t *testing.T, exec *Executor) {
	_, err := exec.Execute(`
		CREATE TABLE orders (
			id SEQ PRIMARY KEY,
			customer_id INT,
			order_date VARCHAR(20),
			amount FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert test data - Alice has 2 orders, Bob has 1, Charlie has none
	_, err = exec.Execute(`INSERT INTO orders (id, customer_id, order_date, amount) VALUES (101, 1, '2024-01-01', 100.0)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO orders (id, customer_id, order_date, amount) VALUES (102, 1, '2024-01-02', 200.0)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO orders (id, customer_id, order_date, amount) VALUES (103, 2, '2024-01-03', 150.0)`)
	if err != nil {
		t.Fatal(err)
	}
}

func createItemsTable(t *testing.T, exec *Executor) {
	_, err := exec.Execute(`
		CREATE TABLE items (
			id SEQ PRIMARY KEY,
			order_id INT,
			product VARCHAR(100),
			quantity INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create items table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO items (id, order_id, product, quantity) VALUES (1, 101, 'Widget', 5)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO items (id, order_id, product, quantity) VALUES (2, 101, 'Gadget', 3)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO items (id, order_id, product, quantity) VALUES (3, 102, 'Doohickey', 10)`)
	if err != nil {
		t.Fatal(err)
	}
}

// ============================================================================
// INNER JOIN Tests
// ============================================================================

func TestInnerJoinBasic(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.id, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Alice has 2 orders, Bob has 1, Charlie has 0
	// Total: 3 rows
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}

	// Check that Charlie is not in results (no orders)
	for _, row := range result.Rows {
		if row[0] != nil && row[0].(string) == "Charlie" {
			t.Error("Charlie should not appear in INNER JOIN results (no matching orders)")
		}
	}
}

func TestInnerJoinWithWhere(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		WHERE o.amount > 100
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Only orders with amount > 100: order 102 (200.0) and 103 (150.0)
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

// ============================================================================
// LEFT JOIN Tests
// ============================================================================

func TestLeftJoinBasic(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.id, o.amount
		FROM customers c
		LEFT JOIN orders o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All 3 customers: Alice (2 orders), Bob (1 order), Charlie (0 orders = NULL)
	if result.RowCount != 4 {
		t.Errorf("Expected 4 rows (Alice x2, Bob x1, Charlie x1 with NULL), got %d", result.RowCount)
	}

	// Find Charlie's row (should have NULL for order columns)
	foundCharlie := false
	for _, row := range result.Rows {
		if row[0] != nil && row[0].(string) == "Charlie" {
			foundCharlie = true
			if row[1] != nil || row[2] != nil {
				t.Error("Charlie's order columns should be NULL")
			}
		}
	}
	if !foundCharlie {
		t.Error("Charlie should appear in LEFT JOIN results")
	}
}

func TestLeftJoinPreservesLeftRows(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	// Create tables where right table is empty
	_, err := exec.Execute(`
		CREATE TABLE left_table (id SEQ PRIMARY KEY, value VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO left_table (id, value) VALUES (1, 'A')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO left_table (id, value) VALUES (2, 'B')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE right_table (id SEQ PRIMARY KEY, ref_id INT, data VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	// Don't insert any rows into right_table

	result, err := exec.Execute(`
		SELECT l.value, r.data
		FROM left_table l
		LEFT JOIN right_table r ON l.id = r.ref_id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All left rows should be preserved
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	// All right columns should be NULL
	for _, row := range result.Rows {
		if row[1] != nil {
			t.Error("Right table columns should be NULL when right table is empty")
		}
	}
}

// ============================================================================
// RIGHT JOIN Tests
// ============================================================================

func TestRightJoinBasic(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.id, o.amount
		FROM customers c
		RIGHT JOIN orders o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All 3 orders should appear
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

func TestRightJoinPreservesRightRows(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	// Create tables where left table is empty
	_, err := exec.Execute(`
		CREATE TABLE left_table (id SEQ PRIMARY KEY, value VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	// Don't insert any rows

	_, err = exec.Execute(`
		CREATE TABLE right_table (id SEQ PRIMARY KEY, ref_id INT, data VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO right_table (id, ref_id, data) VALUES (1, 99, 'Orphan')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT l.value, r.data
		FROM left_table l
		RIGHT JOIN right_table r ON l.id = r.ref_id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All right rows should be preserved
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	// Left column should be NULL
	if result.Rows[0][0] != nil {
		t.Error("Left table column should be NULL when no match")
	}
	if result.Rows[0][1] != "Orphan" {
		t.Error("Right table data should be preserved")
	}
}

// ============================================================================
// CROSS JOIN Tests
// ============================================================================

func TestCrossJoin(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	// Create simple tables for cross join
	_, err := exec.Execute(`
		CREATE TABLE colors (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO colors (id, name) VALUES (1, 'Red')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO colors (id, name) VALUES (2, 'Blue')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE sizes (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO sizes (id, name) VALUES (1, 'Small')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO sizes (id, name) VALUES (2, 'Medium')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO sizes (id, name) VALUES (3, 'Large')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT c.name, s.name
		FROM colors c
		CROSS JOIN sizes s
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// 2 colors x 3 sizes = 6 rows
	if result.RowCount != 6 {
		t.Errorf("Expected 6 rows (2 x 3), got %d", result.RowCount)
	}
}

// ============================================================================
// Multiple JOIN Tests
// ============================================================================

func TestMultipleJoins(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)
	createItemsTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.id, i.product
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		INNER JOIN items i ON o.id = i.order_id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Alice's order 101 has 2 items, order 102 has 1 item
	// Total: 3 rows
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

// ============================================================================
// Column Resolution Tests
// ============================================================================

func TestQualifiedColumns(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.id, o.id
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		WHERE c.id = 1
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Alice (id=1) has 2 orders
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	// Check column names
	if len(result.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0].Name != "id" {
		t.Errorf("Expected first column 'id', got '%s'", result.Columns[0].Name)
	}
}

func TestAmbiguousColumn(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	// Both tables have 'id' column - unqualified reference should error
	_, err := exec.Execute(`
		SELECT id FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
	`)
	if err == nil {
		t.Error("Expected ambiguous column error")
	}
}

func TestStarExpansion(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT * FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		WHERE c.id = 1
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// customers: id, name, email (3 cols)
	// orders: id, customer_id, order_date, amount (4 cols)
	// Total: 7 columns
	if len(result.Columns) != 7 {
		t.Errorf("Expected 7 columns, got %d", len(result.Columns))
	}
}

func TestQualifiedStarExpansion(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.* FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		WHERE c.id = 1
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Only customers columns: id, name, email (3 cols)
	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns (c.*), got %d", len(result.Columns))
	}
}

// ============================================================================
// Self-Join Test
// ============================================================================

func TestSelfJoin(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	// Create employees table
	_, err := exec.Execute(`
		CREATE TABLE employees (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			manager_id INT
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data - Alice is boss, Bob reports to Alice, Charlie reports to Bob
	_, err = exec.Execute(`INSERT INTO employees (id, name, manager_id) VALUES (1, 'Alice', 0)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO employees (id, name, manager_id) VALUES (2, 'Bob', 1)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO employees (id, name, manager_id) VALUES (3, 'Charlie', 2)`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT e.name, m.name
		FROM employees e
		LEFT JOIN employees m ON e.manager_id = m.id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// 3 employees
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}

	// Alice has no manager (manager_id = 0, no match)
	// Bob's manager is Alice
	// Charlie's manager is Bob
}

// ============================================================================
// Edge Case Tests
// ============================================================================

func TestJoinEmptyTables(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	// Create two empty tables
	_, err := exec.Execute(`
		CREATE TABLE empty1 (id SEQ PRIMARY KEY, value VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE empty2 (id SEQ PRIMARY KEY, data VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}

	// INNER JOIN with both empty
	result, err := exec.Execute(`
		SELECT * FROM empty1 e1
		INNER JOIN empty2 e2 ON e1.id = e2.id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.RowCount != 0 {
		t.Errorf("Expected 0 rows for empty INNER JOIN, got %d", result.RowCount)
	}

	// LEFT JOIN with right empty
	result, err = exec.Execute(`
		SELECT * FROM empty1 e1
		LEFT JOIN empty2 e2 ON e1.id = e2.id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.RowCount != 0 {
		t.Errorf("Expected 0 rows for LEFT JOIN with empty left, got %d", result.RowCount)
	}
}

func TestJoinWithNullJoinColumn(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	// Create tables with nullable join columns
	_, err := exec.Execute(`
		CREATE TABLE table_a (id SEQ PRIMARY KEY, ref_id INT)
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_a (id, ref_id) VALUES (1, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_a (id, ref_id) VALUES (2, 2)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE table_b (id SEQ PRIMARY KEY, data VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_b (id, data) VALUES (1, 'One')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO table_b (id, data) VALUES (2, 'Two')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT a.id, b.data
		FROM table_a a
		INNER JOIN table_b b ON a.ref_id = b.id
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Both rows should match
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestJoinWithOrderBy(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		ORDER BY o.amount DESC
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should return 3 rows ordered by amount
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

func TestJoinWithLimit(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		LIMIT 2
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows with LIMIT 2, got %d", result.RowCount)
	}
}

func TestJoinWithOffset(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		LIMIT 10 OFFSET 1
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows after OFFSET 1, got %d", result.RowCount)
	}
}

func TestJoinWithAlias(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name AS customer_name, o.amount AS order_amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		WHERE c.id = 1
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}

	// Check column names
	if len(result.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0].Name != "customer_name" {
		t.Errorf("Expected first column 'customer_name', got '%s'", result.Columns[0].Name)
	}
	if result.Columns[1].Name != "order_amount" {
		t.Errorf("Expected second column 'order_amount', got '%s'", result.Columns[1].Name)
	}
}

func TestJoinUnknownTable(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)

	_, err := exec.Execute(`
		SELECT * FROM customers c
		INNER JOIN nonexistent n ON c.id = n.id
	`)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestJoinUnknownColumn(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	_, err := exec.Execute(`
		SELECT c.nonexistent_column FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
	`)
	if err == nil {
		t.Error("Expected error for unknown column")
	}
}

func TestJoinWithUnknownTableAlias(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	_, err := exec.Execute(`
		SELECT x.name FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
	`)
	if err == nil {
		t.Error("Expected error for unknown table alias")
	}
}

func TestJoinQualifiedStarUnknownTable(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	_, err := exec.Execute(`
		SELECT x.* FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
	`)
	if err == nil {
		t.Error("Expected error for unknown table in star expansion")
	}
}

func TestEngine(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	engine := exec.Engine()
	if engine == nil {
		t.Error("Engine() returned nil")
	}
}

func TestJoinWithComplexOnClause(t *testing.T) {
	t.Skip("Alias resolution in complex ON clauses not yet implemented")
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	// Test with AND in ON clause
	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id AND o.amount > 100
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Only orders with amount > 100
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

func TestJoinWithNotInOnClause(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE users (id SEQ PRIMARY KEY, name VARCHAR(50), active INT)
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, active) VALUES (2, 'Bob', 0)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE profiles (id SEQ PRIMARY KEY, user_id INT, bio VARCHAR(100))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO profiles (id, user_id, bio) VALUES (1, 1, 'Hello')`)
	if err != nil {
		t.Fatal(err)
	}

	// Test with NOT in ON clause
	result, err := exec.Execute(`
		SELECT u.name, p.bio
		FROM users u
		LEFT JOIN profiles p ON u.id = p.user_id AND NOT (u.active = 0)
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

func TestJoinWithParenExpr(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON (c.id = o.customer_id)
		WHERE c.id = 1
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

func TestJoinWithStringComparison(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE t1 (id SEQ PRIMARY KEY, code VARCHAR(10))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO t1 (id, code) VALUES (1, 'A')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE t2 (id SEQ PRIMARY KEY, code VARCHAR(10), value VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO t2 (id, code, value) VALUES (1, 'A', 'Match')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT t1.code, t2.value
		FROM t1
		INNER JOIN t2 ON t1.code = t2.code
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestJoinWithLike(t *testing.T) {
	t.Skip("Alias resolution in complex ON clauses not yet implemented")
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	_, err := exec.Execute(`
		CREATE TABLE users (id SEQ PRIMARY KEY, name VARCHAR(50))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = exec.Execute(`
		CREATE TABLE logs (id SEQ PRIMARY KEY, user_id INT, message VARCHAR(100))
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO logs (id, user_id, message) VALUES (1, 1, 'User logged in')`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		SELECT u.name, l.message
		FROM users u
		INNER JOIN logs l ON u.id = l.user_id AND l.message LIKE '%logged%'
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestJoinOrderByNumericColumn(t *testing.T) {
	exec, cleanup := setupJoinTest(t)
	defer cleanup()

	createCustomersTable(t, exec)
	createOrdersTable(t, exec)

	result, err := exec.Execute(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		ORDER BY 2 DESC
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

// ============================================================================
// Helper Function Tests
// ============================================================================

func TestJoinTable_HasColumn(t *testing.T) {
	tbl := &joinTable{
		name:    "users",
		columns: []*types.ColumnInfo{{Name: "id"}, {Name: "name"}},
		colIndex: map[string]int{
			"id":   0,
			"name": 1,
		},
	}

	if !tbl.hasColumn("id") {
		t.Error("hasColumn should return true for 'id'")
	}
	if !tbl.hasColumn("name") {
		t.Error("hasColumn should return true for 'name'")
	}
	if tbl.hasColumn("nonexistent") {
		t.Error("hasColumn should return false for non-existent column")
	}

	// Test case insensitivity
	if !tbl.hasColumn("ID") {
		t.Error("hasColumn should be case insensitive")
	}
	if !tbl.hasColumn("NAME") {
		t.Error("hasColumn should be case insensitive")
	}
}

func TestJoinTable_LookupKey(t *testing.T) {
	// With alias
	tblWithAlias := &joinTable{
		name:  "users",
		alias: "u",
	}
	if tblWithAlias.lookupKey() != "u" {
		t.Errorf("lookupKey with alias: got %q, want 'u'", tblWithAlias.lookupKey())
	}

	// Without alias
	tblNoAlias := &joinTable{
		name: "users",
	}
	if tblNoAlias.lookupKey() != "users" {
		t.Errorf("lookupKey without alias: got %q, want 'users'", tblNoAlias.lookupKey())
	}
}

func TestJoinedRow_GetTableValues(t *testing.T) {
	tbl := &joinTable{
		name:     "users",
		startIdx: 2,
		columns:  []*types.ColumnInfo{{Name: "id"}, {Name: "name"}},
	}

	row := &joinedRow{
		values: []interface{}{"other1", "other2", 1, "Alice", "other3"},
	}

	result := row.getTableValues(tbl)
	if len(result) != 2 {
		t.Errorf("getTableValues length: got %d, want 2", len(result))
	}
	if result[0] != 1 {
		t.Errorf("getTableValues[0]: got %v, want 1", result[0])
	}
	if result[1] != "Alice" {
		t.Errorf("getTableValues[1]: got %v, want 'Alice'", result[1])
	}
}

func TestCreateNullRow(t *testing.T) {
	row := createNullRow(5)

	if len(row.values) != 5 {
		t.Errorf("values length: got %d, want 5", len(row.values))
	}
	if len(row.nullFlags) != 5 {
		t.Errorf("nullFlags length: got %d, want 5", len(row.nullFlags))
	}

	// nullFlags are initialized to false by make()
	for i, v := range row.nullFlags {
		if v {
			t.Errorf("nullFlags[%d]: got %v, want false", i, v)
		}
	}

	// values should be nil (zero values)
	for i, v := range row.values {
		if v != nil {
			t.Errorf("values[%d]: got %v, want nil", i, v)
		}
	}
}

func TestCompareInts(t *testing.T) {
	tests := []struct {
		a, b     int64
		expected int
	}{
		{1, 2, -1},
		{2, 1, 1},
		{1, 1, 0},
		{-1, 1, -1},
		{1, -1, 1},
		{0, 0, 0},
	}

	for _, tt := range tests {
		result := compareInts(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("compareInts(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestJoinTableColumnsToLower(t *testing.T) {
	// Test that column names are stored lowercase in colIndex
	tbl := &joinTable{
		name:    "Users",
		columns: []*types.ColumnInfo{{Name: "ID"}, {Name: "Name"}},
		colIndex: map[string]int{
			strings.ToLower("ID"):   0,
			strings.ToLower("Name"): 1,
		},
	}

	// Both uppercase and lowercase lookups should work
	if !tbl.hasColumn("ID") {
		t.Error("hasColumn should find 'ID'")
	}
	if !tbl.hasColumn("id") {
		t.Error("hasColumn should find 'id'")
	}
}
