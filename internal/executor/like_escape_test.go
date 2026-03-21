package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestLikeEscape(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-likeescape-test-*")
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

	// Create a test table
	_, err = exec.Execute("CREATE TABLE test_escape (id INT PRIMARY KEY, pattern VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	testData := []struct {
		id      int
		pattern string
	}{
		{1, "test_data"},
		{2, "test%data"},
		{3, "test_data%"},
		{4, "100%"},
		{5, "a_b"},
		{6, "a%b"},
	}
	for _, td := range testData {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test_escape VALUES (%d, '%s')", td.id, td.pattern))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "Standard LIKE with % wildcard",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE 'test%'",
			expected: 3, // test_data, test%data, test_data%
		},
		{
			name:     "Standard LIKE with _ wildcard",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE 'test_data'",
			expected: 2, // test_data, test%data (because _ matches any char)
		},
		{
			name:     "ESCAPE % with # to match literal %",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE '%#%' ESCAPE '#'",
			expected: 2, // test_data%, 100% (matches strings ending with literal %)
		},
		{
			name:     "ESCAPE _ with # to match literal _",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE '%#_%' ESCAPE '#'",
			expected: 3, // test_data, test_data%, a_b
		},
		{
			name:     "Match exactly '100%'",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE '100#%' ESCAPE '#'",
			expected: 1, // 100%
		},
		{
			name:     "ESCAPE with @ for _",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE '%@_%' ESCAPE '@'",
			expected: 3, // test_data, test_data%, a_b
		},
		{
			name:     "Combined ESCAPE for literal _ with wildcard %",
			query:    "SELECT * FROM test_escape WHERE pattern LIKE 'test#_data%' ESCAPE '#'",
			expected: 2, // test_data, test_data%
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

func TestLikeEscapeSyntax(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-likeescape-syntax-test-*")
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

	// Create a test table
	_, err = exec.Execute("CREATE TABLE test_syntax (id INT PRIMARY KEY, val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test invalid escape character (must be single character)
	_, err = exec.Execute("SELECT * FROM test_syntax WHERE val LIKE 'test' ESCAPE 'ab'")
	if err == nil {
		t.Error("Expected error for multi-character escape")
	}
	t.Logf("Multi-char escape error (expected): %v", err)

	// Test valid escape syntax with #
	_, err = exec.Execute("SELECT * FROM test_syntax WHERE val LIKE 'test' ESCAPE '#'")
	if err != nil {
		t.Errorf("Unexpected error for valid ESCAPE syntax: %v", err)
	}

	// Test with different escape characters
	_, err = exec.Execute("SELECT * FROM test_syntax WHERE val LIKE 'test' ESCAPE '@'")
	if err != nil {
		t.Errorf("Unexpected error for ESCAPE '@': %v", err)
	}
}

func TestLikeEscapeNullHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-likeescape-null-test-*")
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

	// Create a test table
	_, err = exec.Execute("CREATE TABLE test_null (id INT PRIMARY KEY, val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert NULL value
	_, err = exec.Execute("INSERT INTO test_null VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LIKE with NULL
	result, err := exec.Execute("SELECT * FROM test_null WHERE val LIKE 'test%' ESCAPE '#'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows for NULL LIKE, got %d", len(result.Rows))
	}
}