package executor

import (
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestBeforeTriggerNewRowModification(t *testing.T) {
	tmpDir := t.TempDir()

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table
	_, err := exec.Execute("CREATE TABLE test_trigger (id INT, name VARCHAR(50), created_at VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create BEFORE INSERT trigger using XxScript to modify NEW
	_, err = exec.Execute(`CREATE TRIGGER set_timestamp
		BEFORE INSERT ON test_trigger
		FOR EACH ROW
		AS $$
			NEW.created_at = "2026-03-27"
		$$`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Check the trigger was created correctly
	triggers := engine.GetTriggersForTable("test_trigger", 0) // event 0 = INSERT
	if len(triggers) != 1 {
		t.Fatalf("Expected 1 trigger, got %d", len(triggers))
	}

	// Insert without specifying created_at
	_, err = exec.Execute("INSERT INTO test_trigger (id, name) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify the trigger modified the NEW row
	result, err := exec.Execute("SELECT * FROM test_trigger")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	t.Logf("Row: %v", row)

	// Check if created_at was set by the trigger
	// Row is []interface{}
	if len(row) < 3 {
		t.Fatalf("Expected 3 columns, got %d", len(row))
	}

	createdAt := row[2]
	t.Logf("created_at value: %v", createdAt)
	if createdAt != "2026-03-27" {
		t.Errorf("Expected created_at='2026-03-27', got '%v'", createdAt)
	}
}

func TestBeforeUpdateTriggerNewRowModification(t *testing.T) {
	tmpDir := t.TempDir()

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table
	_, err := exec.Execute("CREATE TABLE test_update (id INT, name VARCHAR(50), updated_at VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert initial row
	_, err = exec.Execute("INSERT INTO test_update (id, name) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create BEFORE UPDATE trigger using XxScript to modify NEW
	_, err = exec.Execute(`CREATE TRIGGER set_updated_at
		BEFORE UPDATE ON test_update
		FOR EACH ROW
		AS $$
			NEW.updated_at = "2026-03-27-updated"
		$$`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Update the row
	_, err = exec.Execute("UPDATE test_update SET name = 'modified' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify the trigger modified the NEW row
	result, err := exec.Execute("SELECT * FROM test_update")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	t.Logf("Row: %v", row)

	// Check if updated_at was set by the trigger
	if len(row) < 3 {
		t.Fatalf("Expected 3 columns, got %d", len(row))
	}

	updatedAt := row[2]
	t.Logf("updated_at value: %v", updatedAt)
	if updatedAt != "2026-03-27-updated" {
		t.Errorf("Expected updated_at='2026-03-27-updated', got '%v'", updatedAt)
	}
}
