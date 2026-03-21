package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestViewCheckOptionParsing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-check-test-*")
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
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, category VARCHAR, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = exec.Execute("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget', 'electronics', 19.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products (id, name, category, price) VALUES (2, 'Gadget', 'electronics', 29.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products (id, name, category, price) VALUES (3, 'Book', 'books', 9.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test 1: Create view WITHOUT check option
	_, err = exec.Execute("CREATE VIEW electronics_view AS SELECT id, name, price FROM products WHERE category = 'electronics'")
	if err != nil {
		t.Fatalf("CREATE VIEW without check option failed: %v", err)
	}

	// Verify view exists
	viewInfo, err := engine.GetView("electronics_view")
	if err != nil {
		t.Fatalf("GetView failed: %v", err)
	}
	if viewInfo.CheckOption != "" {
		t.Errorf("Expected empty check option, got '%s'", viewInfo.CheckOption)
	}

	// Test 2: Create view WITH CASCADED CHECK OPTION
	_, err = exec.Execute("CREATE OR REPLACE VIEW electronics_cascaded AS SELECT id, name, price FROM products WHERE category = 'electronics' WITH CASCADED CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW with CASCADED CHECK OPTION failed: %v", err)
	}

	viewInfo, err = engine.GetView("electronics_cascaded")
	if err != nil {
		t.Fatalf("GetView failed: %v", err)
	}
	if viewInfo.CheckOption != "CASCADED" {
		t.Errorf("Expected CASCADED check option, got '%s'", viewInfo.CheckOption)
	}

	// Test 3: Create view WITH LOCAL CHECK OPTION
	_, err = exec.Execute("CREATE OR REPLACE VIEW electronics_local AS SELECT id, name, price FROM products WHERE category = 'electronics' WITH LOCAL CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW with LOCAL CHECK OPTION failed: %v", err)
	}

	viewInfo, err = engine.GetView("electronics_local")
	if err != nil {
		t.Fatalf("GetView failed: %v", err)
	}
	if viewInfo.CheckOption != "LOCAL" {
		t.Errorf("Expected LOCAL check option, got '%s'", viewInfo.CheckOption)
	}

	// Test 4: Create view WITH CHECK OPTION (default to CASCADED)
	_, err = exec.Execute("CREATE OR REPLACE VIEW electronics_default AS SELECT id, name, price FROM products WHERE category = 'electronics' WITH CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW with default CHECK OPTION failed: %v", err)
	}

	viewInfo, err = engine.GetView("electronics_default")
	if err != nil {
		t.Fatalf("GetView failed: %v", err)
	}
	if viewInfo.CheckOption != "CASCADED" {
		t.Errorf("Expected CASCADED (default) check option, got '%s'", viewInfo.CheckOption)
	}

	// Test 5: Query the views
	result, err := exec.Execute("SELECT * FROM electronics_view")
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows from view, got %d", result.RowCount)
	}
}

func TestViewCheckOptionInsertValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-check-insert-test-*")
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
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, category VARCHAR, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create view WITH CASCADED CHECK OPTION
	_, err = exec.Execute("CREATE VIEW electronics_only AS SELECT id, name, category, price FROM products WHERE category = 'electronics' WITH CASCADED CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW with CHECK OPTION failed: %v", err)
	}

	// Test: Insert a row that matches the WHERE clause - should succeed
	_, err = exec.Execute("INSERT INTO electronics_only (id, name, category, price) VALUES (1, 'Widget', 'electronics', 19.99)")
	if err != nil {
		t.Errorf("INSERT with matching WHERE clause should succeed, got error: %v", err)
	}

	// Test: Insert a row that does NOT match the WHERE clause - should fail with check option violation
	_, err = exec.Execute("INSERT INTO electronics_only (id, name, category, price) VALUES (2, 'Book', 'books', 9.99)")
	if err == nil {
		t.Error("INSERT with non-matching WHERE clause should fail with CHECK OPTION")
	} else {
		t.Logf("Expected error received: %v", err)
	}
}

func TestViewCheckOptionUpdateValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-check-update-test-*")
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
	_, err = exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, category VARCHAR, price FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = exec.Execute("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget', 'electronics', 19.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products (id, name, category, price) VALUES (2, 'Book', 'books', 9.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create view WITH CASCADED CHECK OPTION
	_, err = exec.Execute("CREATE VIEW electronics_only AS SELECT id, name, category, price FROM products WHERE category = 'electronics' WITH CASCADED CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW with CHECK OPTION failed: %v", err)
	}

	// Test: Update a row within the view - changing price (stays in view) should succeed
	_, err = exec.Execute("UPDATE electronics_only SET price = 24.99 WHERE id = 1")
	if err != nil {
		t.Errorf("UPDATE that keeps row in view should succeed, got error: %v", err)
	}

	// Test: Update a row to move it out of the view - should fail
	_, err = exec.Execute("UPDATE electronics_only SET category = 'books' WHERE id = 1")
	if err == nil {
		t.Error("UPDATE that would move row out of view should fail with CHECK OPTION")
	} else {
		t.Logf("Expected error received: %v", err)
	}
}

func TestViewCheckOptionLocalVsCascaded(t *testing.T) {
	// Test the difference between LOCAL and CASCADED check options
	// CASCADED: Checks all underlying views
	// LOCAL: Only checks the current view's WHERE clause

	tmpDir, err := os.MkdirTemp("", "xxsql-view-check-local-test-*")
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
	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR, status VARCHAR, active INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create first view with CHECK OPTION
	_, err = exec.Execute("CREATE VIEW active_items AS SELECT id, name, status, active FROM items WHERE active = 1 WITH CASCADED CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Create second view on top of first view
	_, err = exec.Execute("CREATE VIEW pending_active AS SELECT id, name, status, active FROM active_items WHERE status = 'pending' WITH LOCAL CHECK OPTION")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Insert data through the nested view
	_, err = exec.Execute("INSERT INTO pending_active (id, name, status, active) VALUES (1, 'Item1', 'pending', 1)")
	if err != nil {
		t.Errorf("INSERT should succeed: %v", err)
	}

	// Try to insert a row that violates the outer view's condition (active != 1)
	_, err = exec.Execute("INSERT INTO pending_active (id, name, status, active) VALUES (2, 'Item2', 'pending', 0)")
	if err == nil {
		t.Error("INSERT violating CASCADED view should fail")
	}

	// Try to insert a row that violates only the inner view's condition (status != 'pending')
	_, err = exec.Execute("INSERT INTO pending_active (id, name, status, active) VALUES (3, 'Item3', 'done', 1)")
	if err == nil {
		t.Error("INSERT violating LOCAL view should fail")
	}
}