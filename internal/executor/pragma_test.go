package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestPragmaQuery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-test-*")
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

	// Query default pragma value
	result, err := exec.Execute("PRAGMA cache_size")
	if err != nil {
		t.Fatalf("PRAGMA query failed: %v", err)
	}

	t.Logf("PRAGMA cache_size result: %v", result.Rows)
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestPragmaSetInt(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-int-test-*")
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

	// Set cache_size
	result, err := exec.Execute("PRAGMA cache_size = 5000")
	if err != nil {
		t.Fatalf("PRAGMA set failed: %v", err)
	}

	t.Logf("PRAGMA cache_size set result: %s", result.Message)

	// Query the value back
	result, err = exec.Execute("PRAGMA cache_size")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("PRAGMA cache_size query result: %v", result.Rows)
	if result.Rows[0][0] != int64(5000) {
		t.Errorf("Expected cache_size 5000, got %v", result.Rows[0][0])
	}
}

func TestPragmaSetBool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-bool-test-*")
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
		sql      string
		expected bool
	}{
		{"ON", "PRAGMA foreign_keys = ON", true},
		{"OFF", "PRAGMA foreign_keys = OFF", false},
		{"TRUE", "PRAGMA foreign_keys = TRUE", true},
		{"FALSE", "PRAGMA foreign_keys = FALSE", false},
		{"1", "PRAGMA foreign_keys = 1", true},
		{"0", "PRAGMA foreign_keys = 0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(tt.sql)
			if err != nil {
				t.Fatalf("PRAGMA set failed: %v", err)
			}

			result, err := exec.Execute("PRAGMA foreign_keys")
			if err != nil {
				t.Fatal(err)
			}

			if result.Rows[0][0] != tt.expected {
				t.Errorf("Expected foreign_keys %v, got %v", tt.expected, result.Rows[0][0])
			}
		})
	}
}

func TestPragmaSetString(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-string-test-*")
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

	// Set journal_mode
	result, err := exec.Execute("PRAGMA journal_mode = MEMORY")
	if err != nil {
		t.Fatalf("PRAGMA set failed: %v", err)
	}

	t.Logf("PRAGMA journal_mode set result: %s", result.Message)

	// Query the value back
	result, err = exec.Execute("PRAGMA journal_mode")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("PRAGMA journal_mode query result: %v", result.Rows)
	if result.Rows[0][0] != "MEMORY" {
		t.Errorf("Expected journal_mode MEMORY, got %v", result.Rows[0][0])
	}
}

func TestPragmaSynchronous(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-sync-test-*")
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
		sql      string
		value    int64
		valid    bool
	}{
		{"OFF", "PRAGMA synchronous = 0", 0, true},
		{"NORMAL", "PRAGMA synchronous = 1", 1, true},
		{"FULL", "PRAGMA synchronous = 2", 2, true},
		{"Invalid", "PRAGMA synchronous = 3", 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(tt.sql)
			if tt.valid {
				if err != nil {
					t.Fatalf("PRAGMA set failed: %v", err)
				}

				result, err := exec.Execute("PRAGMA synchronous")
				if err != nil {
					t.Fatal(err)
				}

				if result.Rows[0][0] != tt.value {
					t.Errorf("Expected synchronous %d, got %v", tt.value, result.Rows[0][0])
				}
			} else {
				if err == nil {
					t.Error("Expected error for invalid synchronous value")
				}
				t.Logf("Expected error: %v", err)
			}
		})
	}
}

func TestPragmaUnknown(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-unknown-test-*")
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

	// Query unknown pragma
	_, err = exec.Execute("PRAGMA nonexistent_pragma")
	if err == nil {
		t.Error("Expected error for unknown pragma")
	}
	t.Logf("Expected error: %v", err)
}

func TestPragmaUserVersion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-version-test-*")
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

	// Set user_version
	_, err = exec.Execute("PRAGMA user_version = 42")
	if err != nil {
		t.Fatalf("PRAGMA set failed: %v", err)
	}

	// Query user_version
	result, err := exec.Execute("PRAGMA user_version")
	if err != nil {
		t.Fatal(err)
	}

	if result.Rows[0][0] != int64(42) {
		t.Errorf("Expected user_version 42, got %v", result.Rows[0][0])
	}
}

func TestPragmaMultiple(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-multi-test-*")
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

	// Set multiple pragmas
	pragmas := []struct {
		name  string
		value interface{}
		sql   string
	}{
		{"cache_size", int64(3000), "PRAGMA cache_size = 3000"},
		{"foreign_keys", false, "PRAGMA foreign_keys = OFF"},
		{"journal_mode", "WAL", "PRAGMA journal_mode = WAL"},
		{"busy_timeout", int64(10000), "PRAGMA busy_timeout = 10000"},
	}

	for _, p := range pragmas {
		_, err := exec.Execute(p.sql)
		if err != nil {
			t.Fatalf("PRAGMA %s set failed: %v", p.name, err)
		}
	}

	// Verify all values
	for _, p := range pragmas {
		result, err := exec.Execute("PRAGMA " + p.name)
		if err != nil {
			t.Fatalf("PRAGMA %s query failed: %v", p.name, err)
		}

		t.Logf("PRAGMA %s = %v", p.name, result.Rows[0][0])
		if result.Rows[0][0] != p.value {
			t.Errorf("PRAGMA %s: expected %v, got %v", p.name, p.value, result.Rows[0][0])
		}
	}
}

func TestPragmaGetPragmaValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-getvalue-test-*")
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

	// Set a pragma
	_, err = exec.Execute("PRAGMA cache_size = 12345")
	if err != nil {
		t.Fatalf("PRAGMA set failed: %v", err)
	}

	// Get value using the internal method
	val := exec.GetPragmaValue("cache_size")
	if val == nil {
		t.Error("GetPragmaValue returned nil")
	}
	if val != int64(12345) {
		t.Errorf("Expected 12345, got %v", val)
	}
}