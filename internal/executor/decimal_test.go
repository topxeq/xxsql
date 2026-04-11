package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func setupDecimalTest(t *testing.T) (*Executor, func()) {
	dir, err := os.MkdirTemp("", "decimal-test-*")
	if err != nil {
		t.Fatal(err)
	}

	engine := storage.NewEngine(dir)
	exec := NewExecutor(engine)

	return exec, func() {
		os.RemoveAll(dir)
	}
}

func TestCreateTableWithDecimal(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table with DECIMAL column
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	result, err := exec.Execute(`SHOW TABLES`)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, row := range result.Rows {
		if row[0] == "products" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Table 'products' not found")
	}
}

func TestInsertDecimal(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert with decimal value
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (1, 'Widget', 99.99)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify
	result, err := exec.Execute(`SELECT * FROM products WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// Check price
	price := result.Rows[0][2]
	if price != "99.99" {
		t.Errorf("Expected price '99.99', got '%v'", price)
	}
}

func TestInsertDecimalNegative(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE transactions (
			id SEQ PRIMARY KEY,
			amount DECIMAL(15,2)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert negative decimal
	_, err = exec.Execute(`INSERT INTO transactions (id, amount) VALUES (1, -1234.56)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify
	result, err := exec.Execute(`SELECT * FROM transactions WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	amount := result.Rows[0][1]
	if amount != "-1234.56" {
		t.Errorf("Expected amount '-1234.56', got '%v'", amount)
	}
}

func TestDecimalComparison(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert multiple rows
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (1, 'Cheap', 9.99)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (2, 'Medium', 49.99)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (3, 'Expensive', 199.99)`)
	if err != nil {
		t.Fatal(err)
	}

	// Select with comparison
	result, err := exec.Execute(`SELECT * FROM products WHERE price > 50`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row with price > 50, got %d", len(result.Rows))
	}

	// Select with less than
	result, err = exec.Execute(`SELECT * FROM products WHERE price < 50`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with price < 50, got %d", len(result.Rows))
	}
}

func TestDecimalUpdate(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert
	_, err = exec.Execute(`INSERT INTO products (id, name, price) VALUES (1, 'Widget', 99.99)`)
	if err != nil {
		t.Fatal(err)
	}

	// Update
	_, err = exec.Execute(`UPDATE products SET price = 149.99 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Verify
	result, err := exec.Execute(`SELECT price FROM products WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if result.Rows[0][0] != "149.99" {
		t.Errorf("Expected price '149.99', got '%v'", result.Rows[0][0])
	}
}

func TestDecimalWithDifferentScales(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table with high precision
	_, err := exec.Execute(`
		CREATE TABLE measurements (
			id SEQ PRIMARY KEY,
			value DECIMAL(15,6)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert with different scales
	_, err = exec.Execute(`INSERT INTO measurements (id, value) VALUES (1, 3.141592)`)
	if err != nil {
		t.Fatal(err)
	}

	// Verify
	result, err := exec.Execute(`SELECT * FROM measurements`)
	if err != nil {
		t.Fatal(err)
	}

	value := result.Rows[0][1]
	if value != "3.141592" {
		t.Errorf("Expected '3.141592', got '%v'", value)
	}
}

func TestDecimalAsStringInsert(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			price DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert with string value (parser may parse it as string)
	_, err = exec.Execute(`INSERT INTO products (id, price) VALUES (1, '99.99')`)
	if err != nil {
		t.Fatalf("Failed to insert with string: %v", err)
	}

	// Verify
	result, err := exec.Execute(`SELECT price FROM products WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	if result.Rows[0][0] != "99.99" {
		t.Errorf("Expected '99.99', got '%v'", result.Rows[0][0])
	}
}

func TestDecimalShowColumns(t *testing.T) {
	exec, cleanup := setupDecimalTest(t)
	defer cleanup()

	// Create table with DECIMAL
	_, err := exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2),
			discount DECIMAL(5,3)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Show columns
	result, err := exec.Execute(`SHOW COLUMNS FROM products`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(result.Rows))
	}
}
