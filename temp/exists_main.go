package main

import (
	"fmt"
	"os"
	
	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

func main() {
	// Create temp data dir
	tmpDir, err := os.MkdirTemp("", "xxsql-exists-test-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		fmt.Printf("Failed to open engine: %v\n", err)
		return
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)

	// Create test tables
	exec.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	exec.Execute("CREATE TABLE orders (id INT, user_id INT, amount FLOAT)")
	
	// Insert test data
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")
	
	exec.Execute("INSERT INTO orders VALUES (1, 1, 100.0)")
	exec.Execute("INSERT INTO orders VALUES (2, 1, 200.0)")
	exec.Execute("INSERT INTO orders VALUES (3, 2, 150.0)")

	// Test 1: EXISTS subquery
	fmt.Println("=== Test 1: EXISTS subquery ===")
	result, err := exec.Execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Rows: %d\n", result.RowCount)
		for _, row := range result.Rows {
			fmt.Printf("  %v\n", row)
		}
	}

	// Test 2: NOT EXISTS subquery
	fmt.Println("\n=== Test 2: NOT EXISTS subquery ===")
	result, err = exec.Execute("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Rows: %d\n", result.RowCount)
		for _, row := range result.Rows {
			fmt.Printf("  %v\n", row)
		}
	}

	// Test 3: Simple EXISTS (no correlation)
	fmt.Println("\n=== Test 3: Simple EXISTS (no correlation) ===")
	result, err = exec.Execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Rows: %d\n", result.RowCount)
		for _, row := range result.Rows {
			fmt.Printf("  %v\n", row)
		}
	}

	// Test 4: EXISTS with empty subquery
	fmt.Println("\n=== Test 4: EXISTS with empty subquery ===")
	result, err = exec.Execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 999)")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Rows: %d\n", result.RowCount)
		for _, row := range result.Rows {
			fmt.Printf("  %v\n", row)
		}
	}
}
