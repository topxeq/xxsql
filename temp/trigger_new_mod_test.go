package main

import (
	"fmt"
	"os"

	"github.com/topxeq/xxsql/internal/catalog"
	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

func main() {
	// Setup
	dir, err := os.MkdirTemp("", "xxsql_trigger_test")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	eng, err := storage.NewEngine(dir)
	if err != nil {
		fmt.Printf("Failed to create engine: %v\n", err)
		os.Exit(1)
	}
	cat := catalog.NewCatalog(eng)
	exec := executor.NewExecutor(eng, cat)

	// Create table
	_, err = exec.Execute("CREATE TABLE test_trigger (id INT, name VARCHAR(50), created_at VARCHAR(50))")
	if err != nil {
		fmt.Printf("CREATE TABLE failed: %v\n", err)
		os.Exit(1)
	}

	// Create BEFORE INSERT trigger using XxScript to modify NEW
	_, err = exec.Execute(`CREATE TRIGGER set_timestamp
		BEFORE INSERT ON test_trigger
		FOR EACH ROW
		AS $$
			NEW.created_at = "2026-03-27"
		$$`)
	if err != nil {
		fmt.Printf("CREATE TRIGGER failed: %v\n", err)
		os.Exit(1)
	}

	// Insert without specifying created_at
	_, err = exec.Execute("INSERT INTO test_trigger (id, name) VALUES (1, 'test')")
	if err != nil {
		fmt.Printf("INSERT failed: %v\n", err)
		os.Exit(1)
	}

	// Verify the trigger modified the NEW row
	result, err := exec.Execute("SELECT * FROM test_trigger")
	if err != nil {
		fmt.Printf("SELECT failed: %v\n", err)
		os.Exit(1)
	}

	if len(result.Rows) != 1 {
		fmt.Printf("Expected 1 row, got %d\n", len(result.Rows))
		os.Exit(1)
	}

	row := result.Rows[0]
	fmt.Printf("Row: %v\n", row)

	// Check if created_at was set by the trigger
	if len(row.Values) < 3 {
		fmt.Printf("Expected 3 columns, got %d\n", len(row.Values))
		os.Exit(1)
	}

	createdAt := row.Values[2]
	fmt.Printf("created_at value: %s\n", createdAt.String())
	if createdAt.String() != "2026-03-27" {
		fmt.Printf("ERROR: Expected created_at='2026-03-27', got '%s'\n", createdAt.String())
		os.Exit(1)
	}

	fmt.Println("SUCCESS: BEFORE trigger NEW row modification works!")
}
