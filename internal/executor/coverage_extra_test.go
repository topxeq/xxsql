package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// ========== Tests for evaluateExpression - more function types ==========

func TestEvaluateExpressionDateFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-date-*")
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

	// Test date functions with various arguments
	testCases := []string{
		"SELECT DAYOFWEEK('2024-01-15')",
		"SELECT DAYOFMONTH('2024-01-15')",
		"SELECT DAYOFYEAR('2024-01-15')",
		"SELECT LAST_DAY('2024-02-15')",
		"SELECT WEEKDAY('2024-01-15')",
		"SELECT YEARWEEK('2024-01-15')",
		"SELECT QUARTER('2024-01-15')",
		"SELECT DAYNAME('2024-01-15')",
		"SELECT MONTHNAME('2024-01-15')",
		"SELECT TO_DAYS('2024-01-15')",
		"SELECT FROM_DAYS(739300)",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Date function failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Date function: %s -> %v", tc, result.Rows)
	}
}

func TestEvaluateExpressionStringFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-str-*")
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

	// Test string functions with various arguments
	testCases := []string{
		"SELECT LPAD('hello', 10, 'x')",
		"SELECT RPAD('hello', 10, 'x')",
		"SELECT INSTR('hello world', 'world')",
		"SELECT POSITION('world' IN 'hello world')",
		"SELECT CHAR(65, 66, 67)",
		"SELECT CONCAT_WS('-', 'a', 'b', 'c')",
		"SELECT ELT(2, 'a', 'b', 'c')",
		"SELECT FIELD('b', 'a', 'b', 'c')",
		"SELECT FIND_IN_SET('b', 'a,b,c')",
		"SELECT MAKE_SET(3, 'a', 'b', 'c')",
		"SELECT QUOTE('hello')",
		"SELECT REPEAT('ab', 3)",
		"SELECT REVERSE('hello')",
		"SELECT SPACE(5)",
		"SELECT STRCMP('abc', 'abd')",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("String function failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("String function: %s -> %v", tc, result.Rows)
	}
}

func TestEvaluateExpressionMathFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-math-*")
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

	// Test math functions with edge cases
	testCases := []string{
		"SELECT SIGN(-5)",
		"SELECT SIGN(0)",
		"SELECT SIGN(5)",
		"SELECT LOG(10)",
		"SELECT LOG10(100)",
		"SELECT LOG2(8)",
		"SELECT EXP(1)",
		"SELECT POWER(2, 10)",
		"SELECT SQRT(16)",
		"SELECT SIN(0)",
		"SELECT COS(0)",
		"SELECT TAN(0)",
		"SELECT ASIN(0)",
		"SELECT ACOS(1)",
		"SELECT ATAN(0)",
		"SELECT DEGREES(3.14159)",
		"SELECT RADIANS(180)",
		"SELECT TRUNCATE(123.456, 2)",
		"SELECT ROUND(123.456, 1)",
		"SELECT CEIL(123.456)",
		"SELECT FLOOR(123.456)",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Math function failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Math function: %s -> %v", tc, result.Rows)
	}
}

func TestEvaluateExpressionAggregateFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-agg-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test aggregate functions
	testCases := []string{
		"SELECT MIN(val) FROM test",
		"SELECT MAX(val) FROM test",
		"SELECT SUM(val) FROM test",
		"SELECT AVG(val) FROM test",
		"SELECT COUNT(*) FROM test",
		"SELECT COUNT(val) FROM test",
		"SELECT GROUP_CONCAT(val) FROM test",
		"SELECT STDDEV(val) FROM test",
		"SELECT VARIANCE(val) FROM test",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Aggregate function failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("Aggregate: %s -> %v", tc, result.Rows)
	}
}

// ========== Tests for hasAggregate - CASE expressions ==========

func TestHasAggregateCaseExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-hasagg-case-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, category VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'B', 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test CASE with aggregate in result
	result, err := exec.Execute(`
		SELECT category,
		       CASE
		           WHEN SUM(val) > 15 THEN 'high'
		           ELSE 'low'
		       END as level
		FROM test
		GROUP BY category
	`)
	if err != nil {
		t.Logf("CASE with aggregate in result failed: %v", err)
	} else {
		t.Logf("CASE with aggregate: %d rows", len(result.Rows))
	}

	// Test CASE with aggregate in condition
	result, err = exec.Execute(`
		SELECT category,
		       CASE SUM(val)
		           WHEN 10 THEN 'ten'
		           ELSE 'other'
		       END as level
		FROM test
		GROUP BY category
	`)
	if err != nil {
		t.Logf("CASE with aggregate in condition failed: %v", err)
	} else {
		t.Logf("CASE aggregate condition: %d rows", len(result.Rows))
	}

	// Test aggregate inside CASE
	result, err = exec.Execute(`
		SELECT SUM(CASE WHEN val > 15 THEN 1 ELSE 0 END) as cnt
		FROM test
	`)
	if err != nil {
		t.Logf("Aggregate inside CASE failed: %v", err)
	} else {
		t.Logf("Aggregate inside CASE: %v", result.Rows)
	}
}

// ========== Tests for evaluateWhereForRow - more expression types ==========

func TestEvaluateWhereForRowBetween(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-between-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test BETWEEN
	result, err := exec.Execute("SELECT id FROM test WHERE val BETWEEN 30 AND 70")
	if err != nil {
		t.Logf("BETWEEN failed: %v", err)
	} else {
		t.Logf("BETWEEN: %d rows -> %v", len(result.Rows), result.Rows)
	}

	// Test NOT BETWEEN
	result, err = exec.Execute("SELECT id FROM test WHERE val NOT BETWEEN 30 AND 70")
	if err != nil {
		t.Logf("NOT BETWEEN failed: %v", err)
	} else {
		t.Logf("NOT BETWEEN: %d rows -> %v", len(result.Rows), result.Rows)
	}
}

func TestEvaluateWhereForRowLikePatterns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-like-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'hello world')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'Hello World')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (3, 'goodbye')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test LIKE patterns
	testCases := []string{
		"SELECT id FROM test WHERE name LIKE 'hello%'",
		"SELECT id FROM test WHERE name LIKE '%world%'",
		"SELECT id FROM test WHERE name LIKE 'h_llo%'",
		"SELECT id FROM test WHERE name LIKE '%o%'",
		"SELECT id FROM test WHERE name NOT LIKE '%hello%'",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("LIKE pattern failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("LIKE: %s -> %d rows", tc, len(result.Rows))
	}
}

func TestEvaluateWhereForRowGlob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-glob-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'test.txt')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 'test.csv')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (3, 'data.txt')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test GLOB patterns
	testCases := []string{
		"SELECT id FROM test WHERE name GLOB '*.txt'",
		"SELECT id FROM test WHERE name GLOB 'test.*'",
		"SELECT id FROM test WHERE name GLOB '??st.*'",
		"SELECT id FROM test WHERE name GLOB '[td]*.txt'",
	}

	for _, tc := range testCases {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("GLOB pattern failed: %s, error: %v", tc, err)
			continue
		}
		t.Logf("GLOB: %s -> %d rows", tc, len(result.Rows))
	}
}

// ========== Tests for evaluateHavingExpr - more expression types ==========

func TestEvaluateHavingExprComplexExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-expr-*")
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
	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, product VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'North', 'A', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (2, 'North', 'B', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (3, 'South', 'A', 50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test HAVING with column reference
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING total > 100
	`)
	if err != nil {
		t.Logf("HAVING with column alias failed: %v", err)
	} else {
		t.Logf("HAVING alias: %d rows", len(result.Rows))
	}

	// Test HAVING with multiple aggregates
	result, err = exec.Execute(`
		SELECT region
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > AVG(amount) * 2
	`)
	if err != nil {
		t.Logf("HAVING with multiple aggregates failed: %v", err)
	} else {
		t.Logf("HAVING multiple agg: %d rows", len(result.Rows))
	}

	// Test HAVING with NOT
	result, err = exec.Execute(`
		SELECT region
		FROM sales
		GROUP BY region
		HAVING NOT (SUM(amount) < 200)
	`)
	if err != nil {
		t.Logf("HAVING with NOT failed: %v", err)
	} else {
		t.Logf("HAVING NOT: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateUnaryExpr - more cases ==========

func TestEvaluateUnaryExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-det-*")
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

	// Create table with various types
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, int_val INT, float_val FLOAT, bool_val BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, -10, -3.14, TRUE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, 20, 2.71, FALSE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test negation on column
	result, err := exec.Execute("SELECT -int_val FROM test WHERE id = 1")
	if err != nil {
		t.Logf("Negation on column failed: %v", err)
	} else {
		t.Logf("Negation: %v", result.Rows)
	}

	// Test negation on float
	result, err = exec.Execute("SELECT -float_val FROM test WHERE id = 1")
	if err != nil {
		t.Logf("Negation on float failed: %v", err)
	} else {
		t.Logf("Negation float: %v", result.Rows)
	}

	// Test NOT on bool
	result, err = exec.Execute("SELECT id FROM test WHERE NOT bool_val")
	if err != nil {
		t.Logf("NOT on bool failed: %v", err)
	} else {
		t.Logf("NOT bool: %d rows", len(result.Rows))
	}
}

// ========== Tests for executeStatementForCTE - edge cases ==========

func TestExecuteStatementForCTENested(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cte-nested-*")
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
	_, err = exec.Execute("CREATE TABLE numbers (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO numbers VALUES (%d, %d)", i, i))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test CTE with aggregation
	result, err := exec.Execute(`
		WITH stats AS (
			SELECT SUM(val) as total, AVG(val) as avg_val FROM numbers
		)
		SELECT total, avg_val FROM stats
	`)
	if err != nil {
		t.Logf("CTE with aggregation failed: %v", err)
	} else {
		t.Logf("CTE aggregation: %v", result.Rows)
	}

	// Test nested CTE
	result, err = exec.Execute(`
		WITH
			positive AS (SELECT * FROM numbers WHERE val > 0),
			large AS (SELECT * FROM positive WHERE val > 2)
		SELECT * FROM large
	`)
	if err != nil {
		t.Logf("Nested CTE failed: %v", err)
	} else {
		t.Logf("Nested CTE: %d rows", len(result.Rows))
	}

	// Test CTE with UNION
	result, err = exec.Execute(`
		WITH combined AS (
			SELECT val FROM numbers WHERE val <= 2
			UNION
			SELECT val FROM numbers WHERE val >= 4
		)
		SELECT * FROM combined ORDER BY val
	`)
	if err != nil {
		t.Logf("CTE with UNION failed: %v", err)
	} else {
		t.Logf("CTE UNION: %d rows", len(result.Rows))
	}
}

// ========== Tests for getWindowFuncArgValue - more window functions ==========

func TestGetWindowFuncArgValueDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-win-arg-det-*")
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
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		grp := (i-1)/3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d, %d)", i, grp, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test LEAD with default
	result, err := exec.Execute("SELECT id, LEAD(val, 1, -1) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Logf("LEAD with default failed: %v", err)
	} else {
		t.Logf("LEAD default: %d rows", len(result.Rows))
	}

	// Test LAG with default
	result, err = exec.Execute("SELECT id, LAG(val, 2, 0) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Logf("LAG with default failed: %v", err)
	} else {
		t.Logf("LAG default: %d rows", len(result.Rows))
	}

	// Test NTILE
	result, err = exec.Execute("SELECT id, NTILE(3) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Logf("NTILE failed: %v", err)
	} else {
		t.Logf("NTILE: %d rows", len(result.Rows))
	}

	// Test PERCENT_RANK
	result, err = exec.Execute("SELECT id, PERCENT_RANK() OVER (ORDER BY val) FROM data")
	if err != nil {
		t.Logf("PERCENT_RANK failed: %v", err)
	} else {
		t.Logf("PERCENT_RANK: %d rows", len(result.Rows))
	}

	// Test CUME_DIST
	result, err = exec.Execute("SELECT id, CUME_DIST() OVER (ORDER BY val) FROM data")
	if err != nil {
		t.Logf("CUME_DIST failed: %v", err)
	} else {
		t.Logf("CUME_DIST: %d rows", len(result.Rows))
	}
}

// ========== Tests for parseLoadDataLine - more formats ==========

func TestParseLoadDataLineFormats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-parse-line-fmt-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test CSV with quoted fields
	csvFile := tmpDir + "/quoted.csv"
	csvData := `1,"Alice Smith",TRUE
2,"Bob Jones",FALSE
3,"Charlie Brown",TRUE`
	if err := os.WriteFile(csvFile, []byte(csvData), 0644); err != nil {
		t.Fatalf("Write CSV failed: %v", err)
	}

	result, err := exec.Execute(fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE test FIELDS TERMINATED BY ','", csvFile))
	if err != nil {
		t.Logf("LOAD DATA with quotes failed: %v", err)
	} else {
		t.Logf("LOAD DATA quoted: %s, affected: %d", result.Message, result.Affected)
	}

	// Test with different line endings
	crlfFile := tmpDir + "/crlf.csv"
	crlfData := "4,David,TRUE\r\n5,Eve,FALSE\r\n"
	if err := os.WriteFile(crlfFile, []byte(crlfData), 0644); err != nil {
		t.Fatalf("Write CRLF file failed: %v", err)
	}

	result, err = exec.Execute(fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE test FIELDS TERMINATED BY ','", crlfFile))
	if err != nil {
		t.Logf("LOAD DATA CRLF failed: %v", err)
	} else {
		t.Logf("LOAD DATA CRLF: %s, affected: %d", result.Message, result.Affected)
	}
}

// ========== Tests for pragmaIndexInfo - detailed ==========

func TestPragmaIndexInfoMultiColumn(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-idx-info-multi-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a VARCHAR, b VARCHAR, c VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create multi-column index
	_, err = exec.Execute("CREATE INDEX idx_multi ON test (a, b)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Create single column index
	_, err = exec.Execute("CREATE INDEX idx_single ON test (c)")
	if err != nil {
		t.Fatalf("CREATE INDEX single failed: %v", err)
	}

	// Get index info
	result, err := exec.Execute("PRAGMA INDEX_INFO('idx_multi')")
	if err != nil {
		t.Logf("PRAGMA INDEX_INFO multi failed: %v", err)
	} else {
		t.Logf("INDEX_INFO multi: %d rows", len(result.Rows))
	}

	result, err = exec.Execute("PRAGMA INDEX_INFO('idx_single')")
	if err != nil {
		t.Logf("PRAGMA INDEX_INFO single failed: %v", err)
	} else {
		t.Logf("INDEX_INFO single: %d rows", len(result.Rows))
	}

	// Test INDEX_LIST
	result, err = exec.Execute("PRAGMA INDEX_LIST('test')")
	if err != nil {
		t.Logf("PRAGMA INDEX_LIST failed: %v", err)
	} else {
		t.Logf("INDEX_LIST: %d rows", len(result.Rows))
	}
}

// ========== Tests for executeIndexScan - more scenarios ==========

func TestExecuteIndexScanRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-idx-scan-range-*")
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

	// Create table with index
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_val ON test (val)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test range scan with >
	result, err := exec.Execute("SELECT COUNT(*) FROM test WHERE val > 50")
	if err != nil {
		t.Fatalf("Range > failed: %v", err)
	}
	t.Logf("Range >: %v", result.Rows)

	// Test range scan with <
	result, err = exec.Execute("SELECT COUNT(*) FROM test WHERE val < 50")
	if err != nil {
		t.Fatalf("Range < failed: %v", err)
	}
	t.Logf("Range <: %v", result.Rows)

	// Test range scan with >=
	result, err = exec.Execute("SELECT COUNT(*) FROM test WHERE val >= 50")
	if err != nil {
		t.Fatalf("Range >= failed: %v", err)
	}
	t.Logf("Range >=: %v", result.Rows)

	// Test range scan with <=
	result, err = exec.Execute("SELECT COUNT(*) FROM test WHERE val <= 50")
	if err != nil {
		t.Fatalf("Range <= failed: %v", err)
	}
	t.Logf("Range <=: %v", result.Rows)

	// Test range scan with BETWEEN
	result, err = exec.Execute("SELECT COUNT(*) FROM test WHERE val BETWEEN 25 AND 75")
	if err != nil {
		t.Logf("Range BETWEEN failed: %v", err)
	} else {
		t.Logf("Range BETWEEN: %v", result.Rows)
	}
}

// ========== Tests for computeFirstLastValue - edge cases ==========

func TestComputeFirstLastValueEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-firstlast-edge-*")
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

	// Create table with single row
	_, err = exec.Execute("CREATE TABLE single (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO single VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test FIRST_VALUE on single row
	result, err := exec.Execute("SELECT id, FIRST_VALUE(val) OVER () FROM single")
	if err != nil {
		t.Logf("FIRST_VALUE single row failed: %v", err)
	} else {
		t.Logf("FIRST_VALUE single: %v", result.Rows)
	}

	// Test LAST_VALUE on single row
	result, err = exec.Execute("SELECT id, LAST_VALUE(val) OVER () FROM single")
	if err != nil {
		t.Logf("LAST_VALUE single row failed: %v", err)
	} else {
		t.Logf("LAST_VALUE single: %v", result.Rows)
	}

	// Create table with NULL values
	_, err = exec.Execute("CREATE TABLE nulls (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE nulls failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nulls VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO nulls VALUES (2, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO nulls VALUES (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test with NULLs
	result, err = exec.Execute("SELECT id, FIRST_VALUE(val) OVER (ORDER BY id) FROM nulls")
	if err != nil {
		t.Logf("FIRST_VALUE with NULL failed: %v", err)
	} else {
		t.Logf("FIRST_VALUE NULL: %v", result.Rows)
	}
}

// ========== Tests for jsonSetPath, jsonReplacePath, jsonRemovePath ==========

func TestJsonPathOperationsDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-path-det-*")
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

	// Test JSON_SET with nested path
	result, err := exec.Execute(`SELECT JSON_SET('{"a": {"b": 1}}', '$.a.c', 2)`)
	if err != nil {
		t.Logf("JSON_SET nested failed: %v", err)
	} else {
		t.Logf("JSON_SET nested: %v", result.Rows)
	}

	// Test JSON_SET with array
	result, err = exec.Execute(`SELECT JSON_SET('{"arr": [1,2,3]}', '$.arr[1]', 99)`)
	if err != nil {
		t.Logf("JSON_SET array failed: %v", err)
	} else {
		t.Logf("JSON_SET array: %v", result.Rows)
	}

	// Test JSON_REPLACE with nested
	result, err = exec.Execute(`SELECT JSON_REPLACE('{"a": {"b": 1, "c": 2}}', '$.a.b', 100)`)
	if err != nil {
		t.Logf("JSON_REPLACE nested failed: %v", err)
	} else {
		t.Logf("JSON_REPLACE nested: %v", result.Rows)
	}

	// Test JSON_REMOVE with nested
	result, err = exec.Execute(`SELECT JSON_REMOVE('{"a": {"b": 1, "c": 2}}', '$.a.c')`)
	if err != nil {
		t.Logf("JSON_REMOVE nested failed: %v", err)
	} else {
		t.Logf("JSON_REMOVE nested: %v", result.Rows)
	}

	// Test JSON_REMOVE with array index
	result, err = exec.Execute(`SELECT JSON_REMOVE('{"arr": [1,2,3]}', '$.arr[1]')`)
	if err != nil {
		t.Logf("JSON_REMOVE array index failed: %v", err)
	} else {
		t.Logf("JSON_REMOVE array: %v", result.Rows)
	}
}

// ========== Tests for generateSelectPlan ==========

func TestGenerateSelectPlanExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-plan-*")
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
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_val ON test (val)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Test EXPLAIN SELECT
	result, err := exec.Execute("EXPLAIN SELECT * FROM test WHERE val = 1")
	if err != nil {
		t.Fatalf("EXPLAIN SELECT failed: %v", err)
	}
	t.Logf("EXPLAIN SELECT: %d rows", len(result.Rows))

	// Test EXPLAIN with JOIN
	_, err = exec.Execute("CREATE TABLE other (id INT PRIMARY KEY, ref_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE other failed: %v", err)
	}

	result, err = exec.Execute("EXPLAIN SELECT * FROM test JOIN other ON test.id = other.ref_id")
	if err != nil {
		t.Logf("EXPLAIN JOIN failed: %v", err)
	} else {
		t.Logf("EXPLAIN JOIN: %d rows", len(result.Rows))
	}

	// Test EXPLAIN INSERT
	result, err = exec.Execute("EXPLAIN INSERT INTO test VALUES (1, 'test', 100)")
	if err != nil {
		t.Logf("EXPLAIN INSERT failed: %v", err)
	} else {
		t.Logf("EXPLAIN INSERT: %d rows", len(result.Rows))
	}

	// Test EXPLAIN UPDATE
	result, err = exec.Execute("EXPLAIN UPDATE test SET val = 200 WHERE id = 1")
	if err != nil {
		t.Logf("EXPLAIN UPDATE failed: %v", err)
	} else {
		t.Logf("EXPLAIN UPDATE: %d rows", len(result.Rows))
	}

	// Test EXPLAIN DELETE
	result, err = exec.Execute("EXPLAIN DELETE FROM test WHERE id = 1")
	if err != nil {
		t.Logf("EXPLAIN DELETE failed: %v", err)
	} else {
		t.Logf("EXPLAIN DELETE: %d rows", len(result.Rows))
	}
}

// ========== Tests for executeUpdate and executeDelete ==========

func TestExecuteUpdateWithSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-update-subq-*")
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

	// Create tables
	_, err = exec.Execute("CREATE TABLE main (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE main failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE threshold (id INT PRIMARY KEY, min_val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE threshold failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO main VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT main failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO main VALUES (2, 20)")
	if err != nil {
		t.Fatalf("INSERT main failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO threshold VALUES (1, 15)")
	if err != nil {
		t.Fatalf("INSERT threshold failed: %v", err)
	}

	// Update with subquery
	result, err := exec.Execute("UPDATE main SET val = (SELECT min_val FROM threshold WHERE id = 1) WHERE val < 15")
	if err != nil {
		t.Fatalf("UPDATE with subquery failed: %v", err)
	}
	t.Logf("UPDATE subquery: affected %d", result.Affected)

	// Verify
	result, err = exec.Execute("SELECT * FROM main")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	t.Logf("After UPDATE: %v", result.Rows)
}

func TestExecuteDeleteWithSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-delete-subq-*")
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

	// Create tables
	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE data failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE exclude (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE exclude failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT data failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (2, 20)")
	if err != nil {
		t.Fatalf("INSERT data failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (3, 30)")
	if err != nil {
		t.Fatalf("INSERT data failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO exclude VALUES (2)")
	if err != nil {
		t.Fatalf("INSERT exclude failed: %v", err)
	}

	// Delete with subquery
	result, err := exec.Execute("DELETE FROM data WHERE id IN (SELECT id FROM exclude)")
	if err != nil {
		t.Fatalf("DELETE with subquery failed: %v", err)
	}
	t.Logf("DELETE subquery: affected %d", result.Affected)

	// Verify
	result, err = exec.Execute("SELECT * FROM data")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	t.Logf("After DELETE: %v", result.Rows)
}

// ========== Tests for executeWindowFunctions ==========

func TestExecuteWindowFunctionsFrames(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-win-frames-*")
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
	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test with ROWS frame
	result, err := exec.Execute(`
		SELECT id, amount,
		       SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as running_sum
		FROM sales
	`)
	if err != nil {
		t.Logf("Window ROWS frame failed: %v", err)
	} else {
		t.Logf("ROWS frame: %d rows", len(result.Rows))
	}

	// Test with RANGE frame
	result, err = exec.Execute(`
		SELECT id, amount,
		       SUM(amount) OVER (ORDER BY amount RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_sum
		FROM sales
	`)
	if err != nil {
		t.Logf("Window RANGE frame failed: %v", err)
	} else {
		t.Logf("RANGE frame: %d rows", len(result.Rows))
	}

	// Test multiple window functions
	result, err = exec.Execute(`
		SELECT id, amount,
		       ROW_NUMBER() OVER (ORDER BY id) as rn,
		       RANK() OVER (ORDER BY amount) as rk,
		       DENSE_RANK() OVER (ORDER BY amount) as dr
		FROM sales
	`)
	if err != nil {
		t.Logf("Multiple window functions failed: %v", err)
	} else {
		t.Logf("Multiple windows: %d rows", len(result.Rows))
	}
}

// ========== Tests for getUpdatableViewInfo ==========

func TestGetUpdatableViewInfoNested(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-view-nested-*")
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
	_, err = exec.Execute("CREATE TABLE base (id INT PRIMARY KEY, name VARCHAR, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE base failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO base VALUES (1, 'Alice', TRUE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO base VALUES (2, 'Bob', FALSE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create view on base
	_, err = exec.Execute("CREATE VIEW v1 AS SELECT id, name FROM base WHERE active = TRUE")
	if err != nil {
		t.Fatalf("CREATE VIEW v1 failed: %v", err)
	}

	// Create view on view
	_, err = exec.Execute("CREATE VIEW v2 AS SELECT id, name FROM v1")
	if err != nil {
		t.Logf("CREATE VIEW on view failed: %v", err)
	} else {
		// Select from nested view
		result, err := exec.Execute("SELECT * FROM v2")
		if err != nil {
			t.Logf("SELECT from nested view failed: %v", err)
		} else {
			t.Logf("Nested view: %d rows", len(result.Rows))
		}
	}
}

// ========== Tests for evaluateHaving - more expression types ==========

func TestEvaluateHavingWithNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-not-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category VARCHAR, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		cat := "A"
		if i > 3 {
			cat = "B"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, '%s', %d)", i, cat, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING NOT
	result, err := exec.Execute("SELECT category, SUM(price) as total FROM items GROUP BY category HAVING NOT (SUM(price) > 100)")
	if err != nil {
		t.Logf("HAVING NOT failed: %v", err)
	} else {
		t.Logf("HAVING NOT: %v rows", len(result.Rows))
	}
}

func TestEvaluateHavingWithIsNull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-isnull-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 'A', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (2, 'A', 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO data VALUES (3, 'B', 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test HAVING with IS NULL (should work with group count)
	result, err := exec.Execute("SELECT grp, COUNT(*) as cnt FROM data GROUP BY grp HAVING cnt IS NOT NULL")
	if err != nil {
		t.Logf("HAVING IS NOT NULL failed: %v", err)
	} else {
		t.Logf("HAVING IS NOT NULL: %v rows", len(result.Rows))
	}
}

func TestEvaluateHavingWithInSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, premium BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, TRUE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO customers VALUES (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test HAVING with IN subquery
	result, err := exec.Execute(`
		SELECT customer_id, SUM(amount) as total
		FROM orders
		GROUP BY customer_id
		HAVING customer_id IN (SELECT id FROM customers WHERE premium = TRUE)
	`)
	if err != nil {
		t.Logf("HAVING IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING IN subquery: %v rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow - more expression types ==========

func TestEvaluateWhereWithNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-not-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, TRUE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test WHERE NOT
	result, err := exec.Execute("SELECT * FROM test WHERE NOT active")
	if err != nil {
		t.Logf("WHERE NOT failed: %v", err)
	} else {
		t.Logf("WHERE NOT: %d rows", len(result.Rows))
	}
}

func TestEvaluateWhereWithIsNull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-isnull-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test (id) VALUES (2)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}

	// Test WHERE IS NULL
	result, err := exec.Execute("SELECT * FROM test WHERE name IS NULL")
	if err != nil {
		t.Logf("WHERE IS NULL failed: %v", err)
	} else {
		t.Logf("WHERE IS NULL: %d rows", len(result.Rows))
	}

	// Test WHERE IS NOT NULL
	result, err = exec.Execute("SELECT * FROM test WHERE name IS NOT NULL")
	if err != nil {
		t.Logf("WHERE IS NOT NULL failed: %v", err)
	} else {
		t.Logf("WHERE IS NOT NULL: %d rows", len(result.Rows))
	}
}

func TestEvaluateWhereWithInSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-in-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, TRUE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO customers VALUES (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test WHERE IN subquery
	result, err := exec.Execute("SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = TRUE)")
	if err != nil {
		t.Logf("WHERE IN subquery failed: %v", err)
	} else {
		t.Logf("WHERE IN subquery: %d rows", len(result.Rows))
	}

	// Test WHERE NOT IN subquery
	result, err = exec.Execute("SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM customers WHERE active = TRUE)")
	if err != nil {
		t.Logf("WHERE NOT IN subquery failed: %v", err)
	} else {
		t.Logf("WHERE NOT IN subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for hasAggregate ==========

func TestHasAggregateInCase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-case-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, %d)", i, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test CASE with aggregate
	result, err := exec.Execute("SELECT CASE WHEN SUM(amount) > 200 THEN 'high' ELSE 'low' END as level FROM sales")
	if err != nil {
		t.Logf("CASE with aggregate failed: %v", err)
	} else {
		t.Logf("CASE with aggregate: %v", result.Rows)
	}
}

func TestHasAggregateNested(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-nested-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		grp := (i-1)%3 + 1
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, %d, %d)", i, grp, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test aggregate in expression
	result, err := exec.Execute("SELECT grp, SUM(val) + 100 as adjusted FROM data GROUP BY grp")
	if err != nil {
		t.Logf("Aggregate in expression failed: %v", err)
	} else {
		t.Logf("Aggregate in expression: %v rows", len(result.Rows))
	}

	// Test nested aggregates (SUM of groups with MAX)
	result, err = exec.Execute("SELECT MAX(total) FROM (SELECT grp, SUM(val) as total FROM data GROUP BY grp)")
	if err != nil {
		t.Logf("Nested aggregate failed: %v", err)
	} else {
		t.Logf("Nested aggregate: %v", result.Rows)
	}
}

// ========== Tests for pragmaIntegrityCheck ==========

func TestPragmaIntegrityCheckDetailedExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-*")
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

	// Test integrity check on empty database
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("PRAGMA INTEGRITY_CHECK on empty db failed: %v", err)
	} else {
		t.Logf("Integrity check empty: %v", result.Rows)
	}

	// Create table
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Add data
	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test integrity check with data
	result, err = exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("PRAGMA INTEGRITY_CHECK failed: %v", err)
	} else {
		t.Logf("Integrity check with data: %v", result.Rows)
	}

	// Create index
	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Logf("CREATE INDEX failed: %v", err)
	}

	// Test integrity check with index
	result, err = exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("PRAGMA INTEGRITY_CHECK with index failed: %v", err)
	} else {
		t.Logf("Integrity check with index: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - more expression types ==========

func TestEvaluateExpressionCastExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, '123')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test CAST expression
	result, err := exec.Execute("SELECT CAST(val AS INT) FROM test WHERE id = 1")
	if err != nil {
		t.Logf("CAST expression failed: %v", err)
	} else {
		t.Logf("CAST result: %v", result.Rows)
	}
}

func TestEvaluateExpressionScalarSubqueryExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-subquery-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 1, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test scalar subquery in SELECT
	result, err := exec.Execute(`
		SELECT id, amount, (SELECT name FROM customers WHERE id = customer_id) as customer_name
		FROM orders
	`)
	if err != nil {
		t.Logf("Scalar subquery failed: %v", err)
	} else {
		t.Logf("Scalar subquery: %d rows", len(result.Rows))
	}

	// Test scalar subquery returning no rows
	result, err = exec.Execute("SELECT (SELECT name FROM customers WHERE id = 999) as name")
	if err != nil {
		t.Logf("Scalar subquery no rows failed: %v", err)
	} else {
		t.Logf("Scalar subquery no rows: %v", result.Rows)
	}
}

func TestEvaluateExpressionAnyAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-*")
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

	_, err = exec.Execute("CREATE TABLE numbers (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE numbers failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO numbers VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ANY
	result, err := exec.Execute("SELECT * FROM numbers WHERE val > ANY (SELECT val FROM numbers WHERE id <= 2)")
	if err != nil {
		t.Logf("ANY failed: %v", err)
	} else {
		t.Logf("ANY: %d rows", len(result.Rows))
	}

	// Test ALL
	result, err = exec.Execute("SELECT * FROM numbers WHERE val > ALL (SELECT val FROM numbers WHERE id <= 2)")
	if err != nil {
		t.Logf("ALL failed: %v", err)
	} else {
		t.Logf("ALL: %d rows", len(result.Rows))
	}
}

func TestEvaluateExpressionCollateExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'abc')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COLLATE expression
	result, err := exec.Execute("SELECT name COLLATE BINARY FROM test")
	if err != nil {
		t.Logf("COLLATE expression failed: %v", err)
	} else {
		t.Logf("COLLATE result: %v", result.Rows)
	}
}

// ========== Tests for additional expression paths ==========

func TestEvaluateExpressionParen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-paren-*")
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

	// Test parenthesized expressions
	result, err := exec.Execute("SELECT (1 + 2) * 3")
	if err != nil {
		t.Logf("Paren expression failed: %v", err)
	} else {
		t.Logf("Paren result: %v", result.Rows)
	}

	// Test nested parentheses
	result, err = exec.Execute("SELECT ((1 + 2) * (3 + 4))")
	if err != nil {
		t.Logf("Nested paren failed: %v", err)
	} else {
		t.Logf("Nested paren result: %v", result.Rows)
	}
}

func TestEvaluateExpressionUnaryNegative(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-*")
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test unary minus on column
	result, err := exec.Execute("SELECT -val FROM nums")
	if err != nil {
		t.Logf("Unary minus failed: %v", err)
	} else {
		t.Logf("Unary minus result: %v", result.Rows)
	}

	// Test unary minus on literal
	result, err = exec.Execute("SELECT -5")
	if err != nil {
		t.Logf("Unary minus literal failed: %v", err)
	} else {
		t.Logf("Unary minus literal: %v", result.Rows)
	}
}

func TestEvaluateExpressionWithNulls(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-null-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test unary minus on NULL
	result, err := exec.Execute("SELECT -val FROM test WHERE id = 1")
	if err != nil {
		t.Logf("Unary minus on NULL failed: %v", err)
	} else {
		t.Logf("Unary minus on NULL: %v", result.Rows)
	}

	// Test arithmetic with NULL
	result, err = exec.Execute("SELECT val + 1 FROM test WHERE id = 1")
	if err != nil {
		t.Logf("Arithmetic with NULL failed: %v", err)
	} else {
		t.Logf("Arithmetic with NULL: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving with EXISTS ==========

func TestEvaluateHavingWithExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-exists-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test HAVING EXISTS
	result, err := exec.Execute(`
		SELECT customer_id, SUM(amount) as total
		FROM orders
		GROUP BY customer_id
		HAVING EXISTS (SELECT 1 FROM customers WHERE customers.id = orders.customer_id)
	`)
	if err != nil {
		t.Logf("HAVING EXISTS failed: %v", err)
	} else {
		t.Logf("HAVING EXISTS: %v rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving with ScalarSubquery ==========

func TestEvaluateHavingWithScalarSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, store_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE sales failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE targets (id INT PRIMARY KEY, store_id INT, target INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE targets failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO targets VALUES (1, 1, 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO sales VALUES (2, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test HAVING with scalar subquery comparison
	result, err := exec.Execute(`
		SELECT store_id, SUM(amount) as total
		FROM sales
		GROUP BY store_id
		HAVING SUM(amount) > (SELECT target FROM targets WHERE targets.store_id = sales.store_id)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery failed: %v", err)
	} else {
		t.Logf("HAVING scalar subquery: %v rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving with IN value list ==========

func TestEvaluateHavingWithInList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-inlist-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category VARCHAR, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 6; i++ {
		cat := string(rune('A' + (i-1)%3))
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, '%s', %d)", i, cat, i*10))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING with IN value list
	result, err := exec.Execute("SELECT category, SUM(price) as total FROM items GROUP BY category HAVING category IN ('A', 'B')")
	if err != nil {
		t.Logf("HAVING IN list failed: %v", err)
	} else {
		t.Logf("HAVING IN list: %v rows", len(result.Rows))
	}

	// Test HAVING with NOT IN value list
	result, err = exec.Execute("SELECT category, SUM(price) as total FROM items GROUP BY category HAVING category NOT IN ('A')")
	if err != nil {
		t.Logf("HAVING NOT IN list failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN list: %v rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhereForRow with EXISTS ==========

func TestEvaluateWhereWithExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-exists-*")
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

	_, err = exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, active BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, TRUE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO customers VALUES (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO orders VALUES (2, 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test WHERE EXISTS
	result, err := exec.Execute(`
		SELECT * FROM orders o
		WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.active = TRUE)
	`)
	if err != nil {
		t.Logf("WHERE EXISTS failed: %v", err)
	} else {
		t.Logf("WHERE EXISTS: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression with column prefix ==========

func TestEvaluateExpressionColumnPrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-prefix-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test column with table prefix
	result, err := exec.Execute("SELECT test.name FROM test WHERE test.id = 1")
	if err != nil {
		t.Logf("Column with table prefix failed: %v", err)
	} else {
		t.Logf("Column with prefix: %v", result.Rows)
	}
}

// ========== Tests for binary operations with different types ==========

func TestEvaluateBinaryOpMixedTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-mixed-types-*")
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

	// Test int + float
	result, err := exec.Execute("SELECT 1 + 1.5")
	if err != nil {
		t.Logf("Int + float failed: %v", err)
	} else {
		t.Logf("Int + float: %v", result.Rows)
	}

	// Test string comparison
	result, err = exec.Execute("SELECT 'a' < 'b'")
	if err != nil {
		t.Logf("String comparison failed: %v", err)
	} else {
		t.Logf("String comparison: %v", result.Rows)
	}

	// Test bool in expression
	result, err = exec.Execute("SELECT TRUE AND FALSE")
	if err != nil {
		t.Logf("Bool AND failed: %v", err)
	} else {
		t.Logf("Bool AND: %v", result.Rows)
	}

	// Test OR
	result, err = exec.Execute("SELECT TRUE OR FALSE")
	if err != nil {
		t.Logf("Bool OR failed: %v", err)
	} else {
		t.Logf("Bool OR: %v", result.Rows)
	}
}

// ========== Tests for GROUP_CONCAT with DISTINCT ==========

func TestGroupConcatDistinct(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-groupcat-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, category VARCHAR, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert duplicates
	for i := 1; i <= 3; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, 'A', 'item1')", i))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test GROUP_CONCAT with DISTINCT
	result, err := exec.Execute("SELECT GROUP_CONCAT(DISTINCT name) as names FROM items")
	if err != nil {
		t.Logf("GROUP_CONCAT DISTINCT failed: %v", err)
	} else {
		t.Logf("GROUP_CONCAT DISTINCT: %v", result.Rows)
	}

	// Test GROUP_CONCAT with separator
	result, err = exec.Execute("SELECT GROUP_CONCAT(name, '|') as names FROM items")
	if err != nil {
		t.Logf("GROUP_CONCAT with separator failed: %v", err)
	} else {
		t.Logf("GROUP_CONCAT separator: %v", result.Rows)
	}
}

// ========== Tests for COUNT with DISTINCT ==========

func TestCountDistinct(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-count-distinct-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, category VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		cat := "A"
		if i > 3 {
			cat = "B"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO data VALUES (%d, '%s')", i, cat))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test COUNT(DISTINCT)
	result, err := exec.Execute("SELECT COUNT(DISTINCT category) as cnt FROM data")
	if err != nil {
		t.Logf("COUNT DISTINCT failed: %v", err)
	} else {
		t.Logf("COUNT DISTINCT: %v", result.Rows)
	}
}

// ========== Tests for LOAD DATA ==========

func TestLoadDataDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-loaddata-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create a CSV file
	csvPath := tmpDir + "/test.csv"
	csvContent := "1,Alice,100\n2,Bob,200\n3,Charlie,300\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Test LOAD DATA
	result, err := exec.Execute(fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE test FIELDS TERMINATED BY ','", csvPath))
	if err != nil {
		t.Logf("LOAD DATA failed: %v", err)
	} else {
		t.Logf("LOAD DATA: %v", result.Message)
	}

	// Verify data was loaded
	result, err = exec.Execute("SELECT COUNT(*) FROM test")
	if err != nil {
		t.Logf("SELECT COUNT failed: %v", err)
	} else {
		t.Logf("Count after load: %v", result.Rows)
	}
}

// ========== Tests for CTE with UNION ==========

func TestCTEWithUnionDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cte-union-*")
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

	_, err = exec.Execute("CREATE TABLE t1 (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE t2 (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO t1 VALUES (%d)", i))
		if err != nil {
			t.Fatalf("INSERT t1 failed: %v", err)
		}
	}
	for i := 3; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO t2 VALUES (%d)", i))
		if err != nil {
			t.Fatalf("INSERT t2 failed: %v", err)
		}
	}

	// Test CTE with UNION
	result, err := exec.Execute(`
		WITH combined AS (
			SELECT id FROM t1
			UNION
			SELECT id FROM t2
		)
		SELECT * FROM combined ORDER BY id
	`)
	if err != nil {
		t.Logf("CTE UNION failed: %v", err)
	} else {
		t.Logf("CTE UNION: %d rows", len(result.Rows))
	}

	// Test CTE with UNION ALL
	result, err = exec.Execute(`
		WITH combined AS (
			SELECT id FROM t1
			UNION ALL
			SELECT id FROM t2
		)
		SELECT * FROM combined ORDER BY id
	`)
	if err != nil {
		t.Logf("CTE UNION ALL failed: %v", err)
	} else {
		t.Logf("CTE UNION ALL: %d rows", len(result.Rows))
	}
}

// ========== Tests for window functions with frame ==========

func TestWindowFunctionWithFrameDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-window-frame-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, %d)", i, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ROWS frame
	result, err := exec.Execute(`
		SELECT id, amount,
			SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as running_sum
		FROM sales
	`)
	if err != nil {
		t.Logf("ROWS frame failed: %v", err)
	} else {
		t.Logf("ROWS frame: %d rows", len(result.Rows))
	}

	// Test RANGE frame
	result, err = exec.Execute(`
		SELECT id, amount,
			SUM(amount) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum
		FROM sales
	`)
	if err != nil {
		t.Logf("RANGE frame failed: %v", err)
	} else {
		t.Logf("RANGE frame: %d rows", len(result.Rows))
	}
}

// ========== Tests for PRAGMA INDEX_INFO ==========

func TestPragmaIndexInfoDetailedExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-idx-info-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create single-column index
	_, err = exec.Execute("CREATE INDEX idx_name ON test (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Test INDEX_INFO
	result, err := exec.Execute("PRAGMA INDEX_INFO('idx_name')")
	if err != nil {
		t.Logf("INDEX_INFO failed: %v", err)
	} else {
		t.Logf("INDEX_INFO: %v rows", len(result.Rows))
	}

	// Create composite index
	_, err = exec.Execute("CREATE INDEX idx_name_email ON test (name, email)")
	if err != nil {
		t.Logf("Composite index failed: %v", err)
	}

	result, err = exec.Execute("PRAGMA INDEX_INFO('idx_name_email')")
	if err != nil {
		t.Logf("INDEX_INFO composite failed: %v", err)
	} else {
		t.Logf("INDEX_INFO composite: %v rows", len(result.Rows))
	}
}

// ========== Tests for evaluateFunction - more functions ==========

func TestEvaluateFunctionMisc(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-func-misc-*")
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

	// Test COALESCE
	result, err := exec.Execute("SELECT COALESCE(NULL, 'default')")
	if err != nil {
		t.Logf("COALESCE failed: %v", err)
	} else {
		t.Logf("COALESCE: %v", result.Rows)
	}

	// Test NULLIF
	result, err = exec.Execute("SELECT NULLIF(1, 1)")
	if err != nil {
		t.Logf("NULLIF failed: %v", err)
	} else {
		t.Logf("NULLIF: %v", result.Rows)
	}

	// Test IFNULL
	result, err = exec.Execute("SELECT IFNULL(NULL, 'default')")
	if err != nil {
		t.Logf("IFNULL failed: %v", err)
	} else {
		t.Logf("IFNULL: %v", result.Rows)
	}

	// Test GREATEST
	result, err = exec.Execute("SELECT GREATEST(1, 5, 3, 2)")
	if err != nil {
		t.Logf("GREATEST failed: %v", err)
	} else {
		t.Logf("GREATEST: %v", result.Rows)
	}

	// Test LEAST
	result, err = exec.Execute("SELECT LEAST(1, 5, 3, 2)")
	if err != nil {
		t.Logf("LEAST failed: %v", err)
	} else {
		t.Logf("LEAST: %v", result.Rows)
	}
}

// ========== Tests for string functions with table data ==========

func TestStringFunctionsWithTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-str-table-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Hello World')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test SUBSTRING with table data
	result, err := exec.Execute("SELECT SUBSTRING(name, 1, 5) FROM test")
	if err != nil {
		t.Logf("SUBSTRING table failed: %v", err)
	} else {
		t.Logf("SUBSTRING table: %v", result.Rows)
	}

	// Test LENGTH with table data
	result, err = exec.Execute("SELECT LENGTH(name) FROM test")
	if err != nil {
		t.Logf("LENGTH table failed: %v", err)
	} else {
		t.Logf("LENGTH table: %v", result.Rows)
	}

	// Test UPPER/LOWER with table data
	result, err = exec.Execute("SELECT UPPER(name), LOWER(name) FROM test")
	if err != nil {
		t.Logf("UPPER/LOWER table failed: %v", err)
	} else {
		t.Logf("UPPER/LOWER table: %v", result.Rows)
	}
}

// ========== Tests for date functions with table ==========

func TestDateFunctionsWithTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-date-table-*")
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

	_, err = exec.Execute("CREATE TABLE events (id INT PRIMARY KEY, event_date VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO events VALUES (1, '2024-03-15')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test date extraction with table
	result, err := exec.Execute("SELECT YEAR(event_date), MONTH(event_date), DAY(event_date) FROM events")
	if err != nil {
		t.Logf("Date extraction failed: %v", err)
	} else {
		t.Logf("Date extraction: %v", result.Rows)
	}

	// Test DATE_ADD
	result, err = exec.Execute("SELECT DATE_ADD(event_date, INTERVAL 7 DAY) FROM events")
	if err != nil {
		t.Logf("DATE_ADD failed: %v", err)
	} else {
		t.Logf("DATE_ADD: %v", result.Rows)
	}
}

// ========== Tests for nested expressions ==========

func TestNestedExpressionsExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nested-expr-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, a INT, b INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test nested arithmetic
	result, err := exec.Execute("SELECT (a + b) * 2 FROM test")
	if err != nil {
		t.Logf("Nested arithmetic failed: %v", err)
	} else {
		t.Logf("Nested arithmetic: %v", result.Rows)
	}

	// Test complex expression
	result, err = exec.Execute("SELECT a * b + (a - b) FROM test")
	if err != nil {
		t.Logf("Complex expression failed: %v", err)
	} else {
		t.Logf("Complex expression: %v", result.Rows)
	}

	// Test nested function calls
	result, err = exec.Execute("SELECT UPPER(SUBSTRING(name, 1, 3)) FROM (SELECT 'Hello' as name)")
	if err != nil {
		t.Logf("Nested functions failed: %v", err)
	} else {
		t.Logf("Nested functions: %v", result.Rows)
	}
}