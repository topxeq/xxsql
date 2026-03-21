package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestCoalesce(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-coalesce-test-*")
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
	exec.SetDatabase("test")

	// Create test table
	_, err = exec.Execute(`
		CREATE TABLE test_nulls (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with NULLs
	_, err = exec.Execute(`INSERT INTO test_nulls (id, name, value) VALUES (1, 'Alice', 10)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO test_nulls (id, name, value) VALUES (2, NULL, 20)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO test_nulls (id, name, value) VALUES (3, 'Charlie', NULL)`)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "COALESCE with first non-null",
			query:    `SELECT COALESCE(name, 'Unknown') FROM test_nulls WHERE id = 1`,
			expected: "Alice",
		},
		{
			name:     "COALESCE with second value",
			query:    `SELECT COALESCE(name, 'Unknown') FROM test_nulls WHERE id = 2`,
			expected: "Unknown",
		},
		{
			name:     "COALESCE with multiple args",
			query:    `SELECT COALESCE(NULL, NULL, 'third', 'fourth')`,
			expected: "third",
		},
		{
			name:     "COALESCE with all nulls",
			query:    `SELECT COALESCE(NULL, NULL, NULL)`,
			expected: nil,
		},
		{
			name:     "COALESCE with integer",
			query:    `SELECT COALESCE(value, 0) FROM test_nulls WHERE id = 3`,
			expected: int64(0),
		},
		{
			name:     "COALESCE with value present",
			query:    `SELECT COALESCE(value, 999) FROM test_nulls WHERE id = 1`,
			expected: int64(10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query error: %v", err)
			}
			if len(result.Rows) == 0 {
				t.Fatalf("No rows returned")
			}
			got := result.Rows[0][0]
			if got != tt.expected {
				t.Errorf("Expected %v (%T), got %v (%T)", tt.expected, tt.expected, got, got)
			}
		})
	}
}

func TestNullIf(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nullif-test-*")
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

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "NULLIF equal values",
			query:    `SELECT NULLIF(10, 10)`,
			expected: nil,
		},
		{
			name:     "NULLIF different values",
			query:    `SELECT NULLIF(10, 20)`,
			expected: int64(10),
		},
		{
			name:     "NULLIF with strings equal",
			query:    `SELECT NULLIF('hello', 'hello')`,
			expected: nil,
		},
		{
			name:     "NULLIF with strings different",
			query:    `SELECT NULLIF('hello', 'world')`,
			expected: "hello",
		},
		{
			name:     "NULLIF with first NULL",
			query:    `SELECT NULLIF(NULL, 10)`,
			expected: nil,
		},
		{
			name:     "NULLIF with second NULL",
			query:    `SELECT NULLIF(10, NULL)`,
			expected: int64(10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query error: %v", err)
			}
			if len(result.Rows) == 0 {
				t.Fatalf("No rows returned")
			}
			got := result.Rows[0][0]
			if got != tt.expected {
				t.Errorf("Expected %v (%T), got %v (%T)", tt.expected, tt.expected, got, got)
			}
		})
	}
}

func TestCoalesceInWhere(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-coalesce-where-test-*")
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
	exec.SetDatabase("test")

	// Create test table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(`INSERT INTO users (id, name, email) VALUES (2, 'Bob', NULL)`)
	if err != nil {
		t.Fatal(err)
	}

	// Use COALESCE in WHERE clause
	result, err := exec.Execute(`SELECT name FROM users WHERE COALESCE(email, 'no-email') = 'no-email'`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Bob" {
		t.Errorf("Expected 'Bob', got %v", result.Rows[0][0])
	}
}