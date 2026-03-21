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

func TestPragmaTableInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-tableinfo-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_pragma (id INT PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR DEFAULT 'test@example.com')")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Query table_info
	result, err := exec.Execute("PRAGMA table_info(test_pragma)")
	if err != nil {
		t.Fatalf("PRAGMA table_info failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	// Check first column (id)
	if result.Rows[0][1] != "id" {
		t.Errorf("Expected column name 'id', got %v", result.Rows[0][1])
	}
	if result.Rows[0][5] != 1 { // pk = 1
		t.Errorf("Expected pk=1 for id column, got %v", result.Rows[0][5])
	}

	// Check second column (name)
	if result.Rows[1][1] != "name" {
		t.Errorf("Expected column name 'name', got %v", result.Rows[1][1])
	}
	if result.Rows[1][3] != 1 { // notnull = 1
		t.Errorf("Expected notnull=1 for name column, got %v", result.Rows[1][3])
	}

	// Check third column (email)
	if result.Rows[2][1] != "email" {
		t.Errorf("Expected column name 'email', got %v", result.Rows[2][1])
	}
	if result.Rows[2][4] == nil {
		t.Error("Expected default value for email column")
	}

	t.Logf("table_info results:")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}
}

func TestPragmaIndexList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-indexlist-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_idx (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create an index
	_, err = exec.Execute("CREATE INDEX idx_name ON test_idx (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Query index_list
	result, err := exec.Execute("PRAGMA index_list(test_idx)")
	if err != nil {
		t.Fatalf("PRAGMA index_list failed: %v", err)
	}

	// Should have PRIMARY and idx_name
	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 index, got %d", len(result.Rows))
	}

	t.Logf("index_list results:")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}

	// Find idx_name in results
	found := false
	for _, row := range result.Rows {
		if row[1] == "idx_name" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected idx_name in index_list")
	}
}

func TestPragmaDatabaseList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-dblist-test-*")
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

	// Query database_list
	result, err := exec.Execute("PRAGMA database_list")
	if err != nil {
		t.Fatalf("PRAGMA database_list failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 database, got %d", len(result.Rows))
	}

	if result.Rows[0][1] != "main" {
		t.Errorf("Expected database name 'main', got %v", result.Rows[0][1])
	}

	t.Logf("database_list results:")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}
}

func TestPragmaIntegrityCheck(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-integrity-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_integrity (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = exec.Execute("INSERT INTO test_integrity VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Run integrity check
	result, err := exec.Execute("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("PRAGMA integrity_check failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][0] != "ok" {
		t.Errorf("Expected 'ok', got %v", result.Rows[0][0])
	}

	t.Logf("integrity_check result: %v", result.Rows[0])
}

func TestPragmaCompileOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-options-test-*")
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

	// Query compile_options
	result, err := exec.Execute("PRAGMA compile_options")
	if err != nil {
		t.Fatalf("PRAGMA compile_options failed: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Error("Expected at least one compile option")
	}

	t.Logf("compile_options:")
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}
}

func TestPragmaPageCount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-pagecount-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_pages (id INT PRIMARY KEY, data VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Query page_count
	result, err := exec.Execute("PRAGMA page_count")
	if err != nil {
		t.Fatalf("PRAGMA page_count failed: %v", err)
	}

	t.Logf("page_count result: %v", result.Rows[0])

	// Query page_size
	result, err = exec.Execute("PRAGMA page_size")
	if err != nil {
		t.Fatalf("PRAGMA page_size failed: %v", err)
	}

	t.Logf("page_size result: %v", result.Rows[0])
}