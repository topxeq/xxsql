package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestGlobBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-glob-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_glob (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO test_glob (name) VALUES ('apple'), ('APPLE'), ('banana'), ('test.txt'), ('data.csv'), ('file1'), ('file2'), ('file10')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name     string
		pattern  string
		expected int
		values   []string
	}{
		{"star match", "a*", 1, []string{"apple"}},      // GLOB is case-sensitive, only lowercase
		{"star match upper", "A*", 1, []string{"APPLE"}}, // uppercase A only matches APPLE
		{"question match", "file?", 2, []string{"file1", "file2"}},
		{"extension match", "*.txt", 1, []string{"test.txt"}},
		{"case sensitive", "APPLE", 1, []string{"APPLE"}},
		{"prefix match", "ban*", 1, []string{"banana"}},
		{"exact match", "apple", 1, []string{"apple"}},
		{"case insensitive pattern", "[aA]*", 2, []string{"apple", "APPLE"}}, // use char set for case-insensitive
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute("SELECT name FROM test_glob WHERE name GLOB '" + tt.pattern + "'")
			if err != nil {
				t.Fatalf("SELECT failed: %v", err)
			}

			t.Logf("Pattern %q matched %d rows: %v", tt.pattern, len(result.Rows), result.Rows)

			if len(result.Rows) != tt.expected {
				t.Errorf("Expected %d rows for pattern %q, got %d", tt.expected, tt.pattern, len(result.Rows))
			}

			// Verify the values
			for _, expected := range tt.values {
				found := false
				for _, row := range result.Rows {
					if row[0] == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected value %q not found in results", expected)
				}
			}
		})
	}
}

func TestGlobCharSet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-glob-charset-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_charset (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = exec.Execute("INSERT INTO test_charset (name) VALUES ('a1'), ('a2'), ('b1'), ('c1'), ('aB'), ('aC')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name     string
		pattern  string
		expected int
	}{
		{"char set abc", "a[123]", 2},  // a1, a2
		{"char range", "a[1-3]", 2},    // a1, a2
		{"negated set", "a[!123]", 2},  // aB, aC (not a1, a2)
		{"negated range", "a[^1-3]", 2}, // aB, aC (not a1, a2)
		{"multiple chars", "[abc]1", 3}, // a1, b1, c1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute("SELECT name FROM test_charset WHERE name GLOB '" + tt.pattern + "'")
			if err != nil {
				t.Fatalf("SELECT failed: %v", err)
			}

			t.Logf("Pattern %q matched %d rows: %v", tt.pattern, len(result.Rows), result.Rows)

			if len(result.Rows) != tt.expected {
				t.Errorf("Expected %d rows for pattern %q, got %d", tt.expected, tt.pattern, len(result.Rows))
			}
		})
	}
}

func TestGlobToRegex(t *testing.T) {
	tests := []struct {
		pattern  string
		input    string
		expected bool
	}{
		// Basic wildcards
		{"*", "anything", true},
		{"*test", "mytest", true},
		{"test*", "test123", true},
		{"*test*", "mytestvalue", true},
		{"?", "a", true},
		{"?", "ab", false},
		{"??", "ab", true},
		{"a?c", "abc", true},
		{"a?c", "ac", false},

		// Character sets
		{"[abc]", "a", true},
		{"[abc]", "b", true},
		{"[abc]", "d", false},
		{"[a-z]", "m", true},
		{"[a-z]", "A", false}, // case sensitive
		{"[0-9]", "5", true},
		{"[0-9]", "a", false},
		{"file[0-9]", "file5", true},
		{"file[0-9]", "fileA", false},

		// Negated sets
		{"[!abc]", "d", true},
		{"[!abc]", "a", false},
		{"[^abc]", "d", true},
		{"[^abc]", "a", false},
		{"[!0-9]", "a", true},
		{"[!0-9]", "5", false},

		// Combined patterns
		{"*.txt", "file.txt", true},
		{"*.txt", "file.TXT", false}, // case sensitive
		{"file?.txt", "file1.txt", true},
		{"file[0-9].txt", "file5.txt", true},
		{"[a-z]*", "hello", true},
		{"[a-z]*", "Hello", false}, // case sensitive

		// Special characters
		{"file.name", "file.name", true},
		{"file.name", "fileXname", false},
		{"a\\b", "a\\b", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.input, func(t *testing.T) {
			regex := globToRegex(tt.pattern)
			t.Logf("Pattern %q -> Regex %q", tt.pattern, regex)

			// Test via executor
			tmpDir, _ := os.MkdirTemp("", "xxsql-glob-regex-test-*")
			defer os.RemoveAll(tmpDir)

			engine := storage.NewEngine(tmpDir)
			engine.Open()
			defer engine.Close()

			exec := NewExecutor(engine)
			_, _ = exec.Execute("CREATE TABLE t (v VARCHAR)")
			_, _ = exec.Execute("INSERT INTO t (v) VALUES ('" + tt.input + "')")

			result, err := exec.Execute("SELECT v FROM t WHERE v GLOB '" + tt.pattern + "'")
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			matched := len(result.Rows) > 0
			if matched != tt.expected {
				t.Errorf("GLOB %q with input %q: expected %v, got %v", tt.pattern, tt.input, tt.expected, matched)
			}
		})
	}
}

func TestGlobVsLike(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-glob-like-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_compare (name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data with different cases
	_, err = exec.Execute("INSERT INTO test_compare (name) VALUES ('Apple'), ('apple'), ('APPLE')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// LIKE is case-insensitive by default in SQLite (but we may differ)
	likeResult, _ := exec.Execute("SELECT name FROM test_compare WHERE name LIKE 'apple'")
	t.Logf("LIKE 'apple' matches: %d rows", len(likeResult.Rows))

	// GLOB is case-sensitive
	globResult, _ := exec.Execute("SELECT name FROM test_compare WHERE name GLOB 'apple'")
	t.Logf("GLOB 'apple' matches: %d rows", len(globResult.Rows))

	// GLOB should only match exact case
	if len(globResult.Rows) != 1 || globResult.Rows[0][0] != "apple" {
		t.Errorf("GLOB 'apple' should match exactly 1 row 'apple', got %v", globResult.Rows)
	}
}