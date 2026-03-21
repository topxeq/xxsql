package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestAnalyzeCommand(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-analyze-test-*")
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

	// Create a table with an index
	_, err = exec.Execute("CREATE TABLE test_analyze (id INT PRIMARY KEY, name VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test_analyze VALUES (%d, 'name%d', %d)", i, i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Create a secondary index
	_, err = exec.Execute("CREATE INDEX idx_value ON test_analyze (value)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Run ANALYZE on the table
	result, err := exec.Execute("ANALYZE TABLE test_analyze")
	if err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	t.Logf("ANALYZE result: %d rows", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row from ANALYZE, got %d", len(result.Rows))
	}

	// Check that the optimizer has statistics
	if exec.optimizer == nil {
		t.Error("Optimizer should be initialized after ANALYZE")
	}

	stats := exec.optimizer.GetStatistics("test_analyze")
	if stats == nil {
		t.Error("Statistics should be available for test_analyze")
	} else {
		t.Logf("Statistics: RowCount=%d, ColumnStats=%d, IndexStats=%d",
			stats.RowCount, len(stats.ColumnStats), len(stats.IndexStats))

		if stats.RowCount != 100 {
			t.Errorf("Expected RowCount=100, got %d", stats.RowCount)
		}
	}

	// Test ANALYZE without table name (all tables)
	result, err = exec.Execute("ANALYZE")
	if err != nil {
		t.Fatalf("ANALYZE (all tables) failed: %v", err)
	}

	t.Logf("ANALYZE (all tables) result: %d rows", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  %v", row)
	}

	// Should have analyzed all tables (at least test_analyze)
	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 row from ANALYZE (all tables), got %d", len(result.Rows))
	}
}

func TestOptimizerBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-optimizer-test-*")
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
	_, err = exec.Execute("CREATE TABLE test_opt (id INT PRIMARY KEY, category VARCHAR, amount FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data with known distribution
	for i := 1; i <= 50; i++ {
		category := "A"
		if i > 25 {
			category = "B"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test_opt VALUES (%d, '%s', %f)", i, category, float64(i)*1.5))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Create index
	_, err = exec.Execute("CREATE INDEX idx_category ON test_opt (category)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Analyze
	_, err = exec.Execute("ANALYZE TABLE test_opt")
	if err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	// Query using the index
	result, err := exec.Execute("SELECT * FROM test_opt WHERE category = 'A'")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// Should return 25 rows
	if len(result.Rows) != 25 {
		t.Errorf("Expected 25 rows for category='A', got %d", len(result.Rows))
	}

	t.Logf("SELECT with index returned %d rows (expected 25)", len(result.Rows))
}