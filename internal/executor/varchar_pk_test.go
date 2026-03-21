package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestVarcharPrimaryKeyConstraint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "varchar-pk-test-*")
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

	// Create table with VARCHAR primary key
	_, err = exec.Execute("CREATE TABLE scripts (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	t.Log("Table created successfully")

	// Insert first row
	_, err = exec.Execute("INSERT INTO scripts (SKEY, SCRIPT) VALUES ('hello', 'print(1)')")
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}
	t.Log("First insert succeeded")

	// Try to insert duplicate - should fail
	_, err = exec.Execute("INSERT INTO scripts (SKEY, SCRIPT) VALUES ('hello', 'print(2)')")
	if err == nil {
		t.Error("BUG: Second INSERT should have failed (duplicate primary key)")
	} else {
		t.Logf("Second INSERT correctly rejected: %v", err)
	}

	// Query the table
	result, err := exec.Execute("SELECT * FROM scripts")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	t.Logf("Total rows: %d", len(result.Rows))
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test equality query
	result2, err := exec.Execute("SELECT * FROM scripts WHERE SKEY = 'hello'")
	if err != nil {
		t.Fatalf("Equality query failed: %v", err)
	}
	t.Logf("Equality query rows: %d", len(result2.Rows))
	if len(result2.Rows) != 1 {
		t.Errorf("Expected 1 row from equality query, got %d", len(result2.Rows))
	}

	// Test LIKE query
	result3, err := exec.Execute("SELECT * FROM scripts WHERE SKEY LIKE 'hello'")
	if err != nil {
		t.Fatalf("LIKE query failed: %v", err)
	}
	t.Logf("LIKE query rows: %d", len(result3.Rows))
	if len(result3.Rows) != 1 {
		t.Errorf("Expected 1 row from LIKE query, got %d", len(result3.Rows))
	}
}

func TestVarcharPrimaryKeyEquality(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "varchar-pk-eq-test-*")
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

	// Create table with VARCHAR primary key
	_, err = exec.Execute("CREATE TABLE api (SKEY VARCHAR(50) PRIMARY KEY, SCRIPT TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert multiple rows with different keys
	testData := []struct {
		key   string
		value string
	}{
		{"hello", "http.json({\"message\": \"Hello!\"})"},
		{"users", "var users = db.query(\"SELECT * FROM users\"); http.json(users)"},
		{"status", "http.json({\"status\": \"ok\"})"},
	}

	for _, td := range testData {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO api (SKEY, SCRIPT) VALUES ('%s', '%s')", td.key, td.value))
		if err != nil {
			t.Fatalf("INSERT failed for key %s: %v", td.key, err)
		}
	}

	// Test each key with equality
	for _, td := range testData {
		query := fmt.Sprintf("SELECT * FROM api WHERE SKEY = '%s'", td.key)
		result, err := exec.Execute(query)
		if err != nil {
			t.Fatalf("Query failed for key %s: %v", td.key, err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Key '%s': expected 1 row, got %d", td.key, len(result.Rows))
		} else {
			gotKey := result.Rows[0][0].(string)
			if gotKey != td.key {
				t.Errorf("Key '%s': got wrong key '%s'", td.key, gotKey)
			}
		}
	}
}