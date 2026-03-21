package executor

import (
	"os"
	"strings"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestExplainSelect(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
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

	// Create test table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			age INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err = exec.Execute("INSERT INTO users (name, email, age) VALUES ('user" + string(rune('0'+i)) + "', 'user@example.com', 20)")
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:  "SimpleSelect",
			query: "EXPLAIN SELECT * FROM users",
			expected: []string{"SCAN", "users"},
		},
		{
			name:  "SelectWithWhere",
			query: "EXPLAIN SELECT * FROM users WHERE id = 1",
			expected: []string{"SCAN", "users", "FILTER"},
		},
		{
			name:  "SelectWithOrderBy",
			query: "EXPLAIN SELECT * FROM users ORDER BY name",
			expected: []string{"SCAN", "users", "ORDER BY"},
		},
		{
			name:  "SelectWithLimit",
			query: "EXPLAIN SELECT * FROM users LIMIT 5",
			expected: []string{"SCAN", "users", "LIMIT"},
		},
		{
			name:  "SelectWithGroupBy",
			query: "EXPLAIN SELECT age, COUNT(*) FROM users GROUP BY age",
			expected: []string{"SCAN", "users", "GROUP BY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query error: %v", err)
			}

			t.Logf("Query: %s", tt.query)
			t.Logf("Columns: %v", result.Columns)
			for _, row := range result.Rows {
				t.Logf("Row: %v", row)
			}

			// Check expected keywords in results
			resultStr := ""
			for _, row := range result.Rows {
				if len(row) > 3 {
					resultStr += strings.ToUpper(row[3].(string)) + " "
				}
			}

			for _, exp := range tt.expected {
				if !strings.Contains(resultStr, strings.ToUpper(exp)) {
					t.Errorf("Expected to find '%s' in plan, got: %s", exp, resultStr)
				}
			}
		})
	}
}

func TestExplainInsert(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	_, err = exec.Execute("CREATE TABLE test (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute("EXPLAIN INSERT INTO test (id, name) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	t.Logf("EXPLAIN INSERT result: %v", result.Rows)
	if len(result.Rows) == 0 {
		t.Error("Expected at least one row in EXPLAIN output")
	}
}

func TestExplainUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute("EXPLAIN UPDATE test SET name = 'updated' WHERE id = 1")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	t.Logf("EXPLAIN UPDATE result: %v", result.Rows)

	// Check for UPDATE and FILTER keywords
	foundUpdate := false
	foundFilter := false
	for _, row := range result.Rows {
		if len(row) > 3 {
			detail := strings.ToUpper(row[3].(string))
			if strings.Contains(detail, "UPDATE") {
				foundUpdate = true
			}
			if strings.Contains(detail, "FILTER") {
				foundFilter = true
			}
		}
	}

	if !foundUpdate {
		t.Error("Expected UPDATE in plan")
	}
	if !foundFilter {
		t.Error("Expected FILTER in plan for WHERE clause")
	}
}

func TestExplainDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute("EXPLAIN DELETE FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	t.Logf("EXPLAIN DELETE result: %v", result.Rows)
}

func TestExplainQueryPlan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	t.Logf("EXPLAIN QUERY PLAN result: %v", result.Rows)

	// Should have columns: id, parent, notused, detail
	if len(result.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(result.Columns))
	}
}

func TestExplainWithJoin(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		EXPLAIN SELECT u.name, o.amount
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
	`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	t.Logf("EXPLAIN JOIN result: %v", result.Rows)

	// Should mention JOIN
	foundJoin := false
	for _, row := range result.Rows {
		if len(row) > 3 {
			detail := strings.ToUpper(row[3].(string))
			if strings.Contains(detail, "JOIN") {
				foundJoin = true
				break
			}
		}
	}

	if !foundJoin {
		t.Error("Expected JOIN in query plan")
	}
}

func TestExplainSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	exec := NewExecutor(engine)

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := exec.Execute(`
		EXPLAIN SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)
	`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	t.Logf("EXPLAIN subquery result: %v", result.Rows)
}