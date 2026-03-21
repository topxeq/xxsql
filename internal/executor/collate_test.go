package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestCollateInComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-comp-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_collate (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO test_collate (name) VALUES ('Apple'), ('apple'), ('BANANA'), ('banana')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COLLATE NOCASE in WHERE clause (case-insensitive comparison)
	result, err := exec.Execute("SELECT * FROM test_collate WHERE name COLLATE NOCASE = 'apple'")
	if err != nil {
		t.Fatalf("SELECT with COLLATE failed: %v", err)
	}

	// Should return both 'Apple' and 'apple' (case-insensitive match)
	t.Logf("COLLATE NOCASE result: %v", result.Rows)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows for COLLATE NOCASE comparison, got %d", len(result.Rows))
	}
}

func TestCollateInOrderBy(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-order-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_order (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO test_order (name) VALUES ('apple'), ('Banana'), ('APPLE'), ('banana'), ('Cherry')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ORDER BY with COLLATE NOCASE
	result, err := exec.Execute("SELECT * FROM test_order ORDER BY name COLLATE NOCASE")
	if err != nil {
		t.Fatalf("SELECT with ORDER BY COLLATE failed: %v", err)
	}

	t.Logf("ORDER BY COLLATE NOCASE result: %v", result.Rows)
	// With NOCASE, all entries should be sorted case-insensitively
	// Expected order: apple/APPLE, Banana/banana, Cherry

	// Verify that the result has 5 rows
	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows, got %d", len(result.Rows))
	}
}

func TestCollateRTRIM(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-rtrim-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_rtrim (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data with trailing spaces
	_, err = exec.Execute("INSERT INTO test_rtrim (name) VALUES ('apple'), ('apple   '), ('banana')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COLLATE RTRIM in WHERE clause
	result, err := exec.Execute("SELECT * FROM test_rtrim WHERE name COLLATE RTRIM = 'apple'")
	if err != nil {
		t.Fatalf("SELECT with COLLATE RTRIM failed: %v", err)
	}

	t.Logf("COLLATE RTRIM result: %v", result.Rows)
	// With RTRIM, 'apple' and 'apple   ' should match
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows for COLLATE RTRIM comparison, got %d", len(result.Rows))
	}
}

func TestCollateBinary(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-binary-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_binary (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO test_binary (name) VALUES ('apple'), ('Apple'), ('APPLE')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COLLATE BINARY in WHERE clause (case-sensitive)
	result, err := exec.Execute("SELECT * FROM test_binary WHERE name COLLATE BINARY = 'apple'")
	if err != nil {
		t.Fatalf("SELECT with COLLATE BINARY failed: %v", err)
	}

	t.Logf("COLLATE BINARY result: %v", result.Rows)
	// With BINARY, only exact match should return
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row for COLLATE BINARY comparison, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "apple" {
		t.Errorf("Expected 'apple', got %v", result.Rows[0][0])
	}
}

func TestCollateComparisonFunctions(t *testing.T) {
	tests := []struct {
		name      string
		a, b      string
		collation string
		want      int // -1, 0, or 1
	}{
		// NOCASE tests
		{"NOCASE equal", "Apple", "apple", "NOCASE", 0},
		{"NOCASE less", "apple", "Banana", "NOCASE", -1},
		{"NOCASE greater", "Banana", "apple", "NOCASE", 1},

		// RTRIM tests
		{"RTRIM equal", "apple", "apple   ", "RTRIM", 0},
		{"RTRIM unequal", "apple", "banana", "RTRIM", -1},

		// BINARY tests
		{"BINARY equal", "apple", "apple", "BINARY", 0},
		{"BINARY unequal case", "Apple", "apple", "BINARY", -1}, // 'A' < 'a' in ASCII
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collationCompare(tt.a, tt.b, tt.collation)
			if got != tt.want {
				t.Errorf("collationCompare(%q, %q, %q) = %d, want %d", tt.a, tt.b, tt.collation, got, tt.want)
			}
		})
	}
}

func TestCollateWithLike(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-like-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_like (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO test_like (name) VALUES ('Apple'), ('apple'), ('BANANA')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LIKE with COLLATE NOCASE (case-insensitive)
	result, err := exec.Execute("SELECT * FROM test_like WHERE name COLLATE NOCASE LIKE 'app%'")
	if err != nil {
		t.Fatalf("SELECT with LIKE COLLATE failed: %v", err)
	}

	t.Logf("LIKE COLLATE NOCASE result: %v", result.Rows)
	// Should match both 'Apple' and 'apple'
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows for LIKE with COLLATE NOCASE, got %d", len(result.Rows))
	}
}