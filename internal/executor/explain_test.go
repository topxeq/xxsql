package executor

import (
	"os"
	"strings"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestExplainQueryPlan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
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

	// Create test tables
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// Create index
	_, err = exec.Execute("CREATE INDEX idx_user_id ON orders (user_id)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected []string // substrings expected in the output
	}{
		{
			name:     "Simple SELECT",
			query:    "EXPLAIN SELECT * FROM users",
			expected: []string{"SCAN", "users"},
		},
		{
			name:     "SELECT with WHERE",
			query:    "EXPLAIN SELECT * FROM users WHERE id = 1",
			expected: []string{"SCAN", "users"},
		},
		{
			name:     "EXPLAIN QUERY PLAN",
			query:    "EXPLAIN QUERY PLAN SELECT * FROM users",
			expected: []string{"SCAN", "users"},
		},
		{
			name:     "SELECT with JOIN",
			query:    "EXPLAIN SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expected: []string{"SCAN", "users", "JOIN", "orders"},
		},
		{
			name:     "INSERT",
			query:    "EXPLAIN INSERT INTO users (id, name) VALUES (100, 'Test')",
			expected: []string{"INSERT", "users"},
		},
		{
			name:     "UPDATE",
			query:    "EXPLAIN UPDATE users SET name = 'Updated' WHERE id = 1",
			expected: []string{"UPDATE", "users"},
		},
		{
			name:     "DELETE",
			query:    "EXPLAIN DELETE FROM users WHERE id = 1",
			expected: []string{"DELETE", "users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("EXPLAIN failed: %v", err)
			}

			// Check columns
			if len(result.Columns) < 3 {
				t.Errorf("Expected at least 3 columns, got %d", len(result.Columns))
			}

			// Check rows exist
			if len(result.Rows) == 0 {
				t.Error("Expected at least one row in EXPLAIN output")
			}

			// Log the output
			t.Logf("Query: %s", tt.query)
			for _, row := range result.Rows {
				t.Logf("  Row: %v", row)
			}

			// Check expected substrings
			for _, exp := range tt.expected {
				found := false
				for _, row := range result.Rows {
					for _, val := range row {
						if str, ok := val.(string); ok {
							if strings.Contains(strings.ToUpper(str), strings.ToUpper(exp)) {
								found = true
								break
							}
						}
					}
					if found {
						break
					}
				}
				if !found {
					t.Errorf("Expected substring '%s' not found in EXPLAIN output", exp)
				}
			}
		})
	}
}
