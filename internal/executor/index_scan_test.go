package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestIndexScanBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-indexscan-test-*")
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

	// Create a table with primary key
	_, err = exec.Execute("CREATE TABLE test_idx (id INT PRIMARY KEY, name VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test_idx VALUES (%d, 'name%d', %d)", i, i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Debug: Check total rows
	result, err := exec.Execute("SELECT COUNT(*) FROM test_idx")
	if err != nil {
		t.Fatalf("COUNT failed: %v", err)
	}
	t.Logf("Total rows in table: %v", result.Rows)

	// Create a secondary index
	_, err = exec.Execute("CREATE INDEX idx_value ON test_idx (value)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "Point lookup on primary key",
			query:    "SELECT * FROM test_idx WHERE id = 50",
			expected: 1,
		},
		{
			name:     "Range scan on primary key",
			query:    "SELECT * FROM test_idx WHERE id < 10",
			expected: 9,
		},
		{
			name:     "Greater than scan on primary key",
			query:    "SELECT * FROM test_idx WHERE id > 90",
			expected: 10,
		},
		{
			name:     "Point lookup on secondary index",
			query:    "SELECT * FROM test_idx WHERE value = 500",
			expected: 1,
		},
		{
			name:     "Full table scan",
			query:    "SELECT * FROM test_idx WHERE name = 'name50'",
			expected: 1,
		},
		{
			name:     "No match",
			query:    "SELECT * FROM test_idx WHERE id = 999",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != tt.expected {
				t.Errorf("Expected %d rows, got %d", tt.expected, len(result.Rows))
				for _, row := range result.Rows {
					t.Logf("  Row: %v", row)
				}
			}
		})
	}
}

func TestCompositeIndexScan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-compositeidx-test-*")
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

	// Create a table
	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product_id INT, amount FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	for u := 1; u <= 10; u++ {
		for p := 1; p <= 10; p++ {
			id := (u-1)*10 + p
			_, err = exec.Execute(fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d, %f)", id, u, p, float64(id)*1.5))
			if err != nil {
				t.Fatalf("INSERT failed: %v", err)
			}
		}
	}

	// Create composite index
	_, err = exec.Execute("CREATE INDEX idx_user_product ON orders (user_id, product_id)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "Filter by first column of composite index",
			query:    "SELECT * FROM orders WHERE user_id = 5",
			expected: 10,
		},
		{
			name:     "Filter by both columns of composite index",
			query:    "SELECT * FROM orders WHERE user_id = 5 AND product_id = 3",
			expected: 1,
		},
		{
			name:     "Filter by second column only (cannot use index efficiently)",
			query:    "SELECT * FROM orders WHERE product_id = 5",
			expected: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != tt.expected {
				t.Errorf("Expected %d rows, got %d", tt.expected, len(result.Rows))
				for _, row := range result.Rows {
					t.Logf("  Row: %v", row)
				}
			}
		})
	}
}

func TestIndexScanWithUpdateDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-indexupdel-test-*")
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

	// Create a table with index
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, category VARCHAR, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_category ON products (category)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert data
	_, err = exec.Execute("INSERT INTO products VALUES (1, 'electronics', 999.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (2, 'books', 29.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (3, 'electronics', 1499.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query with index
	result, err := exec.Execute("SELECT * FROM products WHERE category = 'electronics'")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Update a row
	_, err = exec.Execute("UPDATE products SET category = 'furniture' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Query again
	result, err = exec.Execute("SELECT * FROM products WHERE category = 'electronics'")
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row after update, got %d", len(result.Rows))
	}

	// Delete a row
	_, err = exec.Execute("DELETE FROM products WHERE id = 3")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Query again
	result, err = exec.Execute("SELECT * FROM products WHERE category = 'electronics'")
	if err != nil {
		t.Fatalf("SELECT after DELETE failed: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", len(result.Rows))
	}
}

func TestIndexScanExplain(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-indexexplain-test-*")
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

	// Create a table with index
	_, err = exec.Execute("CREATE TABLE test_explain (id INT PRIMARY KEY, status VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_status ON test_explain (status)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		status := "active"
		if i%2 == 0 {
			status = "inactive"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test_explain VALUES (%d, '%s')", i, status))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test EXPLAIN for primary key lookup
	result, err := exec.Execute("EXPLAIN SELECT * FROM test_explain WHERE id = 50")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	t.Logf("EXPLAIN (primary key lookup):")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}

	// Test EXPLAIN for secondary index
	result, err = exec.Execute("EXPLAIN SELECT * FROM test_explain WHERE status = 'active'")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	t.Logf("EXPLAIN (secondary index):")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}

	// Test EXPLAIN for full scan
	result, err = exec.Execute("EXPLAIN SELECT * FROM test_explain")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	t.Logf("EXPLAIN (full scan):")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}
}