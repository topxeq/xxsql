package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// Tests for DROP VIEW
func TestDropView(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-view-test-*")
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

	// Create base table
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create view
	_, err = exec.Execute("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Verify view exists
	result, err := exec.Execute("SELECT * FROM active_users")
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result from view")
	}

	// Drop view
	result, err = exec.Execute("DROP VIEW active_users")
	if err != nil {
		t.Fatalf("DROP VIEW failed: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Verify view is gone
	_, err = exec.Execute("SELECT * FROM active_users")
	if err == nil {
		t.Error("Expected error when selecting from dropped view")
	}
}

func TestDropViewIfExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-view-ifexists-test-*")
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

	// Drop non-existent view with IF EXISTS - should succeed
	result, err := exec.Execute("DROP VIEW IF EXISTS nonexistent_view")
	if err != nil {
		t.Fatalf("DROP VIEW IF EXISTS failed: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Drop non-existent view without IF EXISTS - should fail
	_, err = exec.Execute("DROP VIEW nonexistent_view")
	if err == nil {
		t.Error("Expected error when dropping non-existent view")
	}
}

// Tests for CREATE TRIGGER
func TestCreateTrigger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-trigger-test-*")
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
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, created_at VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create audit table
	_, err = exec.Execute("CREATE TABLE audit (id INT PRIMARY KEY, action VARCHAR, user_name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE audit TABLE failed: %v", err)
	}

	// Create BEFORE INSERT trigger
	result, err := exec.Execute(`CREATE TRIGGER set_timestamp
		BEFORE INSERT ON users
		FOR EACH ROW
		BEGIN
			-- This is a simple trigger test
		END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Create AFTER INSERT trigger
	result, err = exec.Execute(`CREATE TRIGGER audit_insert
		AFTER INSERT ON users
		FOR EACH ROW
		BEGIN
			INSERT INTO audit (id, action, user_name) VALUES (1, 'INSERT', NEW.name)
		END`)
	if err != nil {
		t.Fatalf("CREATE AFTER INSERT TRIGGER failed: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

func TestDropTrigger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-drop-trigger-test-*")
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
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create trigger
	_, err = exec.Execute(`CREATE TRIGGER test_trigger
		BEFORE INSERT ON users
		FOR EACH ROW
		BEGIN
		END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Drop trigger
	result, err := exec.Execute("DROP TRIGGER test_trigger")
	if err != nil {
		t.Fatalf("DROP TRIGGER failed: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

// Tests for SOUNDEX function
func TestSoundexFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-soundex-test-*")
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

	// Test SOUNDEX function
	result, err := exec.Execute("SELECT SOUNDEX('Robert')")
	if err != nil {
		t.Fatalf("SELECT SOUNDEX failed: %v", err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatal("Expected result from SOUNDEX")
	}

	// R163 is the soundex code for Robert
	soundexCode, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("Expected string, got %T", result.Rows[0][0])
	}
	if len(soundexCode) != 4 {
		t.Errorf("Expected 4-character soundex code, got %s", soundexCode)
	}

	// Test similar sounding names
	result2, err := exec.Execute("SELECT SOUNDEX('Rupert')")
	if err != nil {
		t.Fatalf("SELECT SOUNDEX failed: %v", err)
	}

	soundexCode2, _ := result2.Rows[0][0].(string)

	// Robert and Rupert should have same soundex code
	if soundexCode != soundexCode2 {
		t.Errorf("Robert (%s) and Rupert (%s) should have same soundex code", soundexCode, soundexCode2)
	}
}

// Tests for PRAGMA INDEX_INFO
func TestPragmaIndexInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-index-info-test-*")
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
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create index
	_, err = exec.Execute("CREATE INDEX idx_name ON users (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Query index info
	result, err := exec.Execute("PRAGMA INDEX_INFO('idx_name')")
	if err != nil {
		t.Fatalf("PRAGMA INDEX_INFO failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result from PRAGMA INDEX_INFO")
	}

	// Should have at least one row for the indexed column
	if len(result.Rows) == 0 {
		t.Error("Expected at least one row from PRAGMA INDEX_INFO")
	}
}

// Tests for PRAGMA FOREIGN_KEY_LIST
func TestPragmaForeignKeyList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-fk-test-*")
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

	// Create parent table
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create child table with foreign key
	_, err = exec.Execute(`CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT,
		FOREIGN KEY (user_id) REFERENCES users(id)
	)`)
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// Query foreign key list
	result, err := exec.Execute("PRAGMA FOREIGN_KEY_LIST('orders')")
	if err != nil {
		t.Fatalf("PRAGMA FOREIGN_KEY_LIST failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result from PRAGMA FOREIGN_KEY_LIST")
	}

	// Should have at least one row for the foreign key
	if len(result.Rows) == 0 {
		t.Error("Expected at least one row from PRAGMA FOREIGN_KEY_LIST")
	}
}

// Tests for PRAGMA QUICK_CHECK
func TestPragmaQuickCheck(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-pragma-quickcheck-test-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Run quick check
	result, err := exec.Execute("PRAGMA QUICK_CHECK")
	if err != nil {
		t.Fatalf("PRAGMA QUICK_CHECK failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result from PRAGMA QUICK_CHECK")
	}
}

// Tests for DIFFERENCE function (soundex)
func TestDifferenceFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-difference-test-*")
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

	// Test DIFFERENCE function - Robert and Rupert should have high similarity
	result, err := exec.Execute("SELECT DIFFERENCE('Robert', 'Rupert')")
	if err != nil {
		t.Fatalf("SELECT DIFFERENCE failed: %v", err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatal("Expected result from DIFFERENCE")
	}

	diff, ok := result.Rows[0][0].(int)
	if !ok {
		t.Fatalf("Expected int, got %T", result.Rows[0][0])
	}

	// Robert and Rupert have same soundex code, so difference should be 4
	if diff != 4 {
		t.Errorf("Expected difference 4 for Robert/Rupert, got %d", diff)
	}

	// Test with very different names
	result2, err := exec.Execute("SELECT DIFFERENCE('Robert', 'Smith')")
	if err != nil {
		t.Fatalf("SELECT DIFFERENCE failed: %v", err)
	}

	diff2, ok := result2.Rows[0][0].(int)
	if !ok {
		t.Fatalf("Expected int, got %T", result2.Rows[0][0])
	}

	// Robert and Smith should have low similarity
	if diff2 >= 3 {
		t.Errorf("Expected low difference for Robert/Smith, got %d", diff2)
	}
}

// Tests for JSON_SET function
func TestJsonSetFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-jsonset-test-*")
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

	// Test JSON_SET - add a new key
	result, err := exec.Execute(`SELECT JSON_SET('{"a": 1}', '$.b', 2)`)
	if err != nil {
		t.Fatalf("SELECT JSON_SET failed: %v", err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatal("Expected result from JSON_SET")
	}

	// The result should contain both a and b
	jsonResult, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("Expected string, got %T", result.Rows[0][0])
	}

	if !contains(jsonResult, `"a"`) || !contains(jsonResult, `"b"`) {
		t.Errorf("Expected JSON with a and b, got %s", jsonResult)
	}
}

// Tests for JSON_REPLACE function
func TestJsonReplaceFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-jsonreplace-test-*")
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

	// Test JSON_REPLACE - replace existing key
	result, err := exec.Execute(`SELECT JSON_REPLACE('{"a": 1, "b": 2}', '$.a', 100)`)
	if err != nil {
		t.Fatalf("SELECT JSON_REPLACE failed: %v", err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatal("Expected result from JSON_REPLACE")
	}

	jsonResult, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("Expected string, got %T", result.Rows[0][0])
	}

	// Should have a:100 and b:2
	if !contains(jsonResult, `100`) || !contains(jsonResult, `"b"`) {
		t.Errorf("Expected JSON with a=100 and b=2, got %s", jsonResult)
	}
}

// Tests for JSON_REMOVE function
func TestJsonRemoveFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-jsonremove-test-*")
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

	// Test JSON_REMOVE - remove a key
	result, err := exec.Execute(`SELECT JSON_REMOVE('{"a": 1, "b": 2}', '$.a')`)
	if err != nil {
		t.Fatalf("SELECT JSON_REMOVE failed: %v", err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatal("Expected result from JSON_REMOVE")
	}

	jsonResult, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("Expected string, got %T", result.Rows[0][0])
	}

	// Should only have b
	if contains(jsonResult, `"a"`) {
		t.Errorf("Expected JSON without 'a', got %s", jsonResult)
	}
	if !contains(jsonResult, `"b"`) {
		t.Errorf("Expected JSON with 'b', got %s", jsonResult)
	}
}

// Helper function for string contains check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}