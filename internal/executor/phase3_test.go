package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestNtileWindowFunction tests NTILE window function
func TestNtileWindowFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-ntile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE scores (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			score INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data (10 rows)
	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO scores (name, score) VALUES ('Student', %d)`, i*10))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test NTILE(4) - should divide 10 rows into 4 groups
	// Groups: 3 rows, 3 rows, 2 rows, 2 rows
	result, err := exec.Execute(`
		SELECT id, name, score,
		       NTILE(4) OVER (ORDER BY score) AS quartile
		FROM scores
		ORDER BY score
	`)
	if err != nil {
		t.Fatalf("Failed to execute NTILE query: %v", err)
	}

	t.Logf("NTILE(4) results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	if result.RowCount != 10 {
		t.Errorf("Expected 10 rows, got %d", result.RowCount)
	}

	// Verify distribution
	// First 3 rows should be in quartile 1
	// Next 3 rows should be in quartile 2
	// Next 2 rows should be in quartile 3
	// Last 2 rows should be in quartile 4

	// Test NTILE(3) - should divide 10 rows into 3 groups
	result, err = exec.Execute(`
		SELECT id, name, score,
		       NTILE(3) OVER (ORDER BY score) AS tile
		FROM scores
		ORDER BY score
	`)
	if err != nil {
		t.Fatalf("Failed to execute NTILE(3) query: %v", err)
	}

	t.Logf("NTILE(3) results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test NTILE with PARTITION BY
	_, err = exec.Execute(`UPDATE scores SET name = 'GroupA' WHERE id <= 5`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	_, err = exec.Execute(`UPDATE scores SET name = 'GroupB' WHERE id > 5`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	result, err = exec.Execute(`
		SELECT name, score,
		       NTILE(2) OVER (PARTITION BY name ORDER BY score) AS tile
		FROM scores
		ORDER BY name, score
	`)
	if err != nil {
		t.Fatalf("Failed to execute NTILE with PARTITION BY query: %v", err)
	}

	t.Logf("NTILE(2) with PARTITION BY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestWindowFrameClauses tests ROWS/RANGE BETWEEN frame clauses
func TestWindowFrameClauses(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-frame-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE sales (
			id SEQ PRIMARY KEY,
			month INT,
			amount INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 6; i++ {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO sales (month, amount) VALUES (%d, %d)`, i, i*100))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running total)
	result, err := exec.Execute(`
		SELECT month, amount,
		       SUM(amount) OVER (ORDER BY month
		           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
		FROM sales
		ORDER BY month
	`)
	if err != nil {
		t.Fatalf("Failed to execute running total query: %v", err)
	}

	t.Logf("Running total results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify running totals: 100, 300, 600, 1000, 1500, 2100
	expectedTotals := []float64{100, 300, 600, 1000, 1500, 2100}
	for i, expected := range expectedTotals {
		if i < len(result.Rows) {
			actual := result.Rows[i][2]
			if actual == nil || actual.(float64) != expected {
				t.Errorf("Row %d: expected running total %v, got %v", i, expected, actual)
			}
		}
	}

	// Test ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (moving average window)
	result, err = exec.Execute(`
		SELECT month, amount,
		       AVG(amount) OVER (ORDER BY month
		           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
		FROM sales
		ORDER BY month
	`)
	if err != nil {
		t.Fatalf("Failed to execute moving average query: %v", err)
	}

	t.Logf("Moving average results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
	result, err = exec.Execute(`
		SELECT month, amount,
		       SUM(amount) OVER (ORDER BY month
		           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS remaining_total
		FROM sales
		ORDER BY month
	`)
	if err != nil {
		t.Fatalf("Failed to execute remaining total query: %v", err)
	}

	t.Logf("Remaining total results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify remaining totals: 2100, 2000, 1800, 1500, 1100, 600
	expectedRemaining := []float64{2100, 2000, 1800, 1500, 1100, 600}
	for i, expected := range expectedRemaining {
		if i < len(result.Rows) {
			actual := result.Rows[i][2]
			if actual == nil || actual.(float64) != expected {
				t.Errorf("Row %d: expected remaining total %v, got %v", i, expected, actual)
			}
		}
	}
}

// TestWindowFrameCount tests COUNT with frame clauses
func TestWindowFrameCount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-frame-count-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE events (
			id SEQ PRIMARY KEY,
			event_time INT,
			event_type VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO events (event_time, event_type) VALUES (%d, 'click')`, i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test COUNT with frame
	result, err := exec.Execute(`
		SELECT event_time, event_type,
		       COUNT(*) OVER (ORDER BY event_time
		           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS recent_count
		FROM events
		ORDER BY event_time
	`)
	if err != nil {
		t.Fatalf("Failed to execute COUNT with frame query: %v", err)
	}

	t.Logf("COUNT with frame results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify counts: first row should be 1, second 2, rest 3
	expectedCounts := []int64{1, 2, 3, 3, 3}
	for i, expected := range expectedCounts {
		if i < len(result.Rows) {
			actual := result.Rows[i][2]
			if actual != expected {
				t.Errorf("Row %d: expected count %d, got %v", i, expected, actual)
			}
		}
	}
}

// TestWindowFrameMinMax tests MIN/MAX with frame clauses
func TestWindowFrameMinMax(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-frame-minmax-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE prices (
			id SEQ PRIMARY KEY,
			day INT,
			price INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	prices := []int{100, 105, 95, 110, 90, 115, 100, 120}
	for i, p := range prices {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO prices (day, price) VALUES (%d, %d)`, i+1, p))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test MIN/MAX with frame (rolling min/max over 3 rows)
	result, err := exec.Execute(`
		SELECT day, price,
		       MIN(price) OVER (ORDER BY day
		           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_min,
		       MAX(price) OVER (ORDER BY day
		           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_max
		FROM prices
		ORDER BY day
	`)
	if err != nil {
		t.Fatalf("Failed to execute MIN/MAX with frame query: %v", err)
	}

	t.Logf("Rolling MIN/MAX results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestCombinedPhase3Features tests combinations of Phase 3 features
func TestCombinedPhase3Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase3-combined-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE metrics (
			id SEQ PRIMARY KEY,
			region VARCHAR(50),
			sales INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	data := []struct {
		region string
		sales  int
	}{
		{"East", 100},
		{"East", 150},
		{"East", 200},
		{"West", 80},
		{"West", 120},
		{"West", 180},
	}
	for _, d := range data {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO metrics (region, sales) VALUES ('%s', %d)`, d.region, d.sales))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Combine NTILE, frame clauses, and multiple window functions
	result, err := exec.Execute(`
		SELECT region, sales,
		       NTILE(2) OVER (PARTITION BY region ORDER BY sales) AS tile,
		       SUM(sales) OVER (PARTITION BY region ORDER BY sales
		           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum,
		       AVG(sales) OVER (PARTITION BY region ORDER BY sales
		           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
		FROM metrics
		ORDER BY region, sales
	`)
	if err != nil {
		t.Fatalf("Failed to execute combined query: %v", err)
	}

	t.Logf("Combined Phase 3 results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	if result.RowCount != 6 {
		t.Errorf("Expected 6 rows, got %d", result.RowCount)
	}
}