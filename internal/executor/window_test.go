package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func TestWindowFunctionsComprehensive(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-window-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := NewExecutor(engine)

	// Create test table
	_, err = exec.Execute("CREATE TABLE employees (id INT, name VARCHAR, dept VARCHAR, salary INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO employees VALUES (1, 'Alice', 'Sales', 5000)",
		"INSERT INTO employees VALUES (2, 'Bob', 'Sales', 4500)",
		"INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 5500)",
		"INSERT INTO employees VALUES (4, 'David', 'Engineering', 7000)",
		"INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 6500)",
		"INSERT INTO employees VALUES (6, 'Frank', 'Engineering', 7500)",
		"INSERT INTO employees VALUES (7, 'Grace', 'Marketing', 4000)",
		"INSERT INTO employees VALUES (8, 'Henry', 'Marketing', 4200)",
	}
	for _, q := range testData {
		_, err = exec.Execute(q)
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
	}

	// Test ROW_NUMBER
	t.Run("RowNumber", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("ROW_NUMBER: %v", result.Rows)
		if result.RowCount != 8 {
			t.Errorf("Expected 8 rows, got %d", result.RowCount)
		}
	})

	// Test RANK
	t.Run("Rank", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, RANK() OVER (ORDER BY salary DESC) AS rank FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("RANK: columns=%v", result.Columns)
		t.Logf("RANK: rows=%v", result.Rows)
		t.Logf("RANK: rowCount=%d", result.RowCount)
		if result.RowCount != 8 {
			t.Errorf("Expected 8 rows, got %d", result.RowCount)
		}
	})

	// Test DENSE_RANK
	t.Run("DenseRank", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("DENSE_RANK: %v", result.Rows)
	})

	// Test ROW_NUMBER with PARTITION BY
	t.Run("RowNumberPartition", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("ROW_NUMBER with PARTITION BY: %v", result.Rows)
	})

	// Test SUM with PARTITION BY
	t.Run("SumPartition", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("SUM with PARTITION BY: %v", result.Rows)
	})

	// Test AVG with PARTITION BY
	t.Run("AvgPartition", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, salary, AVG(salary) OVER (PARTITION BY dept) AS dept_avg FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("AVG with PARTITION BY: %v", result.Rows)
	})

	// Test COUNT with PARTITION BY
	t.Run("CountPartition", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("COUNT with PARTITION BY: %v", result.Rows)
	})

	// Test MIN/MAX with PARTITION BY
	t.Run("MinMaxPartition", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, salary, MIN(salary) OVER (PARTITION BY dept) AS min_sal, MAX(salary) OVER (PARTITION BY dept) AS max_sal FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("MIN/MAX with PARTITION BY: %v", result.Rows)
	})

	// Test LEAD
	t.Run("Lead", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, LEAD(salary, 1, 0) OVER (ORDER BY id) AS next_salary FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("LEAD: %v", result.Rows)
	})

	// Test LAG
	t.Run("Lag", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, LAG(salary, 1, 0) OVER (ORDER BY id) AS prev_salary FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("LAG: %v", result.Rows)
	})

	// Test FIRST_VALUE
	t.Run("FirstValue", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS highest_in_dept FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("FIRST_VALUE: %v", result.Rows)
	})

	// Test LAST_VALUE
	t.Run("LastValue", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, dept, salary, LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_in_dept FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("LAST_VALUE: %v", result.Rows)
	})

	// Test NTILE
	t.Run("Ntile", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("NTILE: %v", result.Rows)
	})

	// Test PERCENT_RANK
	t.Run("PercentRank", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, PERCENT_RANK() OVER (ORDER BY salary) AS pct_rank FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("PERCENT_RANK: %v", result.Rows)
	})

	// Test CUME_DIST
	t.Run("CumeDist", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, CUME_DIST() OVER (ORDER BY salary) AS cume_dist FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("CUME_DIST: %v", result.Rows)
	})

	// Test NTH_VALUE
	t.Run("NthValue", func(t *testing.T) {
		result, err := exec.Execute("SELECT id, name, salary, NTH_VALUE(salary, 3) OVER (ORDER BY id) AS third_salary FROM employees")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("NTH_VALUE: %v", result.Rows)
	})

	// Test multiple window functions in same query
	t.Run("MultipleWindowFunctions", func(t *testing.T) {
		result, err := exec.Execute(`
			SELECT id, name, dept, salary,
				ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn,
				RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank,
				SUM(salary) OVER (PARTITION BY dept) AS dept_total
			FROM employees
		`)
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("Multiple window functions: %v", result.Rows)
		if result.RowCount != 8 {
			t.Errorf("Expected 8 rows, got %d", result.RowCount)
		}
	})

	// Test ROWS BETWEEN frame
	t.Run("RowsBetween", func(t *testing.T) {
		result, err := exec.Execute(`
			SELECT id, name, salary,
				SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS running_sum
			FROM employees
		`)
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("ROWS BETWEEN: %v", result.Rows)
	})

	// Test ROWS UNBOUNDED PRECEDING
	t.Run("RowsUnboundedPreceding", func(t *testing.T) {
		result, err := exec.Execute(`
			SELECT id, name, salary,
				SUM(salary) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running_total
			FROM employees
		`)
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		t.Logf("ROWS UNBOUNDED PRECEDING: %v", result.Rows)
	})
}