package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestNthValueWindowFunction tests NTH_VALUE window function
func TestNthValueWindowFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nthvalue-test-*")
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
	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO sales (month, amount) VALUES (%d, %d)`, i, i*100))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test NTH_VALUE(1) - should return first value
	result, err := exec.Execute(`
		SELECT month, amount,
		       NTH_VALUE(amount, 1) OVER (ORDER BY month) AS first_amount
		FROM sales
		ORDER BY month
	`)
	if err != nil {
		t.Fatalf("Failed to execute NTH_VALUE(1) query: %v", err)
	}

	t.Logf("NTH_VALUE(1) results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// First row should have first_amount = 100
	if result.Rows[0][2] == nil {
		t.Error("Expected first_amount for row 0 to be 100, got nil")
	}

	// Test NTH_VALUE(2) - should return second value
	result, err = exec.Execute(`
		SELECT month, amount,
		       NTH_VALUE(amount, 2) OVER (ORDER BY month) AS second_amount
		FROM sales
		ORDER BY month
	`)
	if err != nil {
		t.Fatalf("Failed to execute NTH_VALUE(2) query: %v", err)
	}

	t.Logf("NTH_VALUE(2) results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Test NTH_VALUE with PARTITION BY
	_, err = exec.Execute(`UPDATE sales SET month = month - (month - 1) % 2`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	result, err = exec.Execute(`
		SELECT month, amount,
		       NTH_VALUE(amount, 2) OVER (PARTITION BY month ORDER BY amount) AS second_in_group
		FROM sales
		ORDER BY month, amount
	`)
	if err != nil {
		t.Fatalf("Failed to execute NTH_VALUE with PARTITION BY query: %v", err)
	}

	t.Logf("NTH_VALUE with PARTITION BY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestPercentRankWindowFunction tests PERCENT_RANK window function
func TestPercentRankWindowFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-percentrank-test-*")
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
			player VARCHAR(100),
			score INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	scores := []int{100, 200, 300, 400, 500}
	for i, s := range scores {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO scores (player, score) VALUES ('Player%d', %d)`, i+1, s))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test PERCENT_RANK
	result, err := exec.Execute(`
		SELECT player, score,
		       PERCENT_RANK() OVER (ORDER BY score) AS pct_rank
		FROM scores
		ORDER BY score
	`)
	if err != nil {
		t.Fatalf("Failed to execute PERCENT_RANK query: %v", err)
	}

	t.Logf("PERCENT_RANK results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify: first should be 0.0, last should be 1.0
	if result.Rows[0][2].(float64) != 0.0 {
		t.Errorf("Expected first row percent rank to be 0.0, got %v", result.Rows[0][2])
	}
	if result.Rows[len(result.Rows)-1][2].(float64) != 1.0 {
		t.Errorf("Expected last row percent rank to be 1.0, got %v", result.Rows[len(result.Rows)-1][2])
	}

	// Test PERCENT_RANK with PARTITION BY
	_, err = exec.Execute(`UPDATE scores SET player = 'GroupA' WHERE score <= 300`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	_, err = exec.Execute(`UPDATE scores SET player = 'GroupB' WHERE score > 300`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	result, err = exec.Execute(`
		SELECT player, score,
		       PERCENT_RANK() OVER (PARTITION BY player ORDER BY score) AS pct_rank
		FROM scores
		ORDER BY player, score
	`)
	if err != nil {
		t.Fatalf("Failed to execute PERCENT_RANK with PARTITION BY query: %v", err)
	}

	t.Logf("PERCENT_RANK with PARTITION BY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestCumeDistWindowFunction tests CUME_DIST window function
func TestCumeDistWindowFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cumedist-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE heights (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			height INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	heights := []int{150, 160, 170, 180, 190}
	for i, h := range heights {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO heights (name, height) VALUES ('Person%d', %d)`, i+1, h))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test CUME_DIST
	result, err := exec.Execute(`
		SELECT name, height,
		       CUME_DIST() OVER (ORDER BY height) AS cume_dist
		FROM heights
		ORDER BY height
	`)
	if err != nil {
		t.Fatalf("Failed to execute CUME_DIST query: %v", err)
	}

	t.Logf("CUME_DIST results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify: last should be 1.0 (all rows <= max)
	if result.Rows[len(result.Rows)-1][2].(float64) != 1.0 {
		t.Errorf("Expected last row cume_dist to be 1.0, got %v", result.Rows[len(result.Rows)-1][2])
	}
}

// TestIntersectAll tests INTERSECT ALL set operation
func TestIntersectAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-intersectall-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE t1 (
			id SEQ PRIMARY KEY,
			val INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data (with duplicates)
	for _, v := range []int{1, 2, 2, 3, 3, 3} {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO t1 (val) VALUES (%d)`, v))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Create second table
	_, err = exec.Execute(`
		CREATE TABLE t2 (
			id SEQ PRIMARY KEY,
			val INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data (with duplicates)
	for _, v := range []int{2, 2, 3, 3, 4, 4} {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO t2 (val) VALUES (%d)`, v))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test INTERSECT ALL
	result, err := exec.Execute(`
		SELECT val FROM t1
		INTERSECT ALL
		SELECT val FROM t2
		ORDER BY val
	`)
	if err != nil {
		t.Fatalf("Failed to execute INTERSECT ALL query: %v", err)
	}

	t.Logf("INTERSECT ALL results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Should have: 2, 2, 3, 3 (min count of each value in both tables)
	// t1 has 2 twice, t2 has 2 twice -> 2 twice
	// t1 has 3 three times, t2 has 3 twice -> 3 twice
	if result.RowCount != 4 {
		t.Errorf("Expected 4 rows from INTERSECT ALL, got %d", result.RowCount)
	}
}

// TestExceptAll tests EXCEPT ALL set operation
func TestExceptAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-exceptall-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE t1 (
			id SEQ PRIMARY KEY,
			val INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data (with duplicates)
	for _, v := range []int{1, 2, 2, 3, 3, 3} {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO t1 (val) VALUES (%d)`, v))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Create second table
	_, err = exec.Execute(`
		CREATE TABLE t2 (
			id SEQ PRIMARY KEY,
			val INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data (with duplicates)
	for _, v := range []int{2, 2, 3, 3, 4} {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO t2 (val) VALUES (%d)`, v))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test EXCEPT ALL
	result, err := exec.Execute(`
		SELECT val FROM t1
		EXCEPT ALL
		SELECT val FROM t2
		ORDER BY val
	`)
	if err != nil {
		t.Fatalf("Failed to execute EXCEPT ALL query: %v", err)
	}

	t.Logf("EXCEPT ALL results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Should have: 1, 3
	// t1 has 1 once, t2 has 0 -> 1 once
	// t1 has 2 twice, t2 has 2 twice -> 0
	// t1 has 3 three times, t2 has 3 twice -> 3 once
	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows from EXCEPT ALL, got %d", result.RowCount)
	}
}

// TestStringAgg tests STRING_AGG aggregate function
func TestStringAgg(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-stringagg-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE fruits (
			id SEQ PRIMARY KEY,
			category VARCHAR(50),
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	fruits := []struct {
		cat  string
		name string
	}{
		{"citrus", "orange"},
		{"citrus", "lemon"},
		{"citrus", "lime"},
		{"berry", "strawberry"},
		{"berry", "blueberry"},
	}
	for _, f := range fruits {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO fruits (category, name) VALUES ('%s', '%s')`, f.cat, f.name))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test STRING_AGG
	result, err := exec.Execute(`
		SELECT category, STRING_AGG(name, ',') AS names
		FROM fruits
		GROUP BY category
		ORDER BY category
	`)
	if err != nil {
		t.Fatalf("Failed to execute STRING_AGG query: %v", err)
	}

	t.Logf("STRING_AGG results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount)
	}
}

// TestCombinedPhase4Features tests combinations of Phase 4 features
func TestCombinedPhase4Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase4-combined-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE results (
			id SEQ PRIMARY KEY,
			student VARCHAR(100),
			score INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	scores := []struct {
		student string
		score   int
	}{
		{"Alice", 85},
		{"Bob", 92},
		{"Charlie", 78},
		{"Diana", 88},
		{"Eve", 95},
	}
	for _, s := range scores {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO results (student, score) VALUES ('%s', %d)`, s.student, s.score))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Combine PERCENT_RANK, CUME_DIST, and NTH_VALUE
	result, err := exec.Execute(`
		SELECT student, score,
		       PERCENT_RANK() OVER (ORDER BY score) AS pct_rank,
		       CUME_DIST() OVER (ORDER BY score) AS cume_dist,
		       NTH_VALUE(score, 1) OVER (ORDER BY score) AS first_score
		FROM results
		ORDER BY score
	`)
	if err != nil {
		t.Fatalf("Failed to execute combined query: %v", err)
	}

	t.Logf("Combined Phase 4 results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	if result.RowCount != 5 {
		t.Errorf("Expected 5 rows, got %d", result.RowCount)
	}
}