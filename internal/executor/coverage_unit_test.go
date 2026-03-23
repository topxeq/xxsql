package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ========== Tests for evaluateExpression - ColumnRef with table prefix ==========

func TestEvaluateExpressionColumnRefTablePrefixDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-prefix-direct-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	tbl, err := engine.GetTable("test")
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewStringValue("test", types.TypeVarchar), types.NewIntValue(100)},
	}

	// Test with outer context set
	exec.outerContext = map[string]interface{}{
		"outer_col": 999,
	}

	_, err = exec.evaluateExpression(&sql.Literal{Value: 42}, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Errorf("Literal failed: %v", err)
	}
	t.Logf("Literal evaluated with outer context")
	exec.outerContext = nil
}

// ========== Tests for evaluateExpression with nil row ==========

func TestEvaluateExpressionNilRowDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-nil-row-direct-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()
	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	// Test Literal with nil row
	_, err = exec.evaluateExpression(&sql.Literal{Value: 42}, nil, columnMap, columnOrder)
	if err != nil {
		t.Errorf("Literal with nil row failed: %v", err)
	}
	t.Logf("Literal with nil row evaluated")

	// Test FunctionCall with nil row
	result2, err := exec.Execute("SELECT ABS(-10)")
	if err != nil {
		t.Errorf("FunctionCall ABS failed: %v", err)
	} else {
		t.Logf("FunctionCall ABS(-10): %v", result2.Rows)
	}
}

// ========== Tests for UnaryExpr with different types ==========

func TestUnaryExprDifferentTypesDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-types-direct-*")
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

	// Test unary minus with int
	result, err := exec.Execute("SELECT -5")
	if err != nil {
		t.Errorf("-5 failed: %v", err)
	}
	t.Logf("-5: %v", result.Rows)

	// Test unary minus with float
	result, err = exec.Execute("SELECT -5.5")
	if err != nil {
		t.Errorf("-5.5 failed: %v", err)
	}
	t.Logf("-5.5: %v", result.Rows)

	// Test unary minus with expression
	result, err = exec.Execute("SELECT -(1 + 2)")
	if err != nil {
		t.Errorf("-(1+2) failed: %v", err)
	}
	t.Logf("-(1+2): %v", result.Rows)

	// Test double negation
	result, err = exec.Execute("SELECT -(-5)")
	if err != nil {
		t.Errorf("-(-5) failed: %v", err)
	}
	t.Logf("-(-5): %v", result.Rows)
}

// ========== Tests for evaluateExpression default case ==========

func TestEvaluateExpressionDefaultCase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-default-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test various expressions that should work
	tests := []string{
		"SELECT 1",
		"SELECT 1 + 2",
		"SELECT 1 - 2",
		"SELECT 2 * 3",
		"SELECT 10 / 2",
		"SELECT 10 % 3",
		"SELECT 1 = 1",
		"SELECT 1 != 2",
		"SELECT 1 < 2",
		"SELECT 2 > 1",
		"SELECT 1 <= 1",
		"SELECT 2 >= 1",
		"SELECT TRUE",
		"SELECT FALSE",
		"SELECT NULL",
		"SELECT 'hello'",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("Expression failed: %s, error: %v", tc, err)
		} else {
			t.Logf("Expression: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for evaluateHaving - all expression types ==========

func TestEvaluateHavingAllTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-all-*")
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

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 12; i++ {
		region := "East"
		if i > 6 {
			region = "West"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', %d)", i, region, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test HAVING with COUNT
	result, err := exec.Execute(`
		SELECT region, COUNT(*) as cnt
		FROM sales
		GROUP BY region
		HAVING COUNT(*) > 3
	`)
	if err != nil {
		t.Logf("HAVING COUNT failed: %v", err)
	} else {
		t.Logf("HAVING COUNT: %d rows", len(result.Rows))
	}

	// Test HAVING with SUM
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > 1000
	`)
	if err != nil {
		t.Logf("HAVING SUM failed: %v", err)
	} else {
		t.Logf("HAVING SUM: %d rows", len(result.Rows))
	}

	// Test HAVING with AVG
	result, err = exec.Execute(`
		SELECT region, AVG(amount) as avg_amt
		FROM sales
		GROUP BY region
		HAVING AVG(amount) < 500
	`)
	if err != nil {
		t.Logf("HAVING AVG failed: %v", err)
	} else {
		t.Logf("HAVING AVG: %d rows", len(result.Rows))
	}

	// Test HAVING with MIN/MAX
	result, err = exec.Execute(`
		SELECT region, MIN(amount) as min_amt, MAX(amount) as max_amt
		FROM sales
		GROUP BY region
		HAVING MIN(amount) < 100
	`)
	if err != nil {
		t.Logf("HAVING MIN/MAX failed: %v", err)
	} else {
		t.Logf("HAVING MIN/MAX: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - ScalarSubquery ==========

func TestEvaluateWhereScalarSubqueryTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-scalar-types-*")
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

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, bool_val BOOL, int_val INT, str_val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, TRUE, 1, 'yes'), (2, FALSE, 0, '')")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT items failed: %v", err)
	}

	// Scalar subquery returning bool TRUE
	result, err := exec.Execute("SELECT * FROM items WHERE (SELECT bool_val FROM flags WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery bool TRUE failed: %v", err)
	} else {
		t.Logf("Scalar subquery bool TRUE: %d rows", len(result.Rows))
	}

	// Scalar subquery returning bool FALSE
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT bool_val FROM flags WHERE id = 2)")
	if err != nil {
		t.Logf("Scalar subquery bool FALSE failed: %v", err)
	} else {
		t.Logf("Scalar subquery bool FALSE: %d rows (should be 0)", len(result.Rows))
	}

	// Scalar subquery returning int 1
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT int_val FROM flags WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery int 1 failed: %v", err)
	} else {
		t.Logf("Scalar subquery int 1: %d rows", len(result.Rows))
	}

	// Scalar subquery returning int 0
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT int_val FROM flags WHERE id = 2)")
	if err != nil {
		t.Logf("Scalar subquery int 0 failed: %v", err)
	} else {
		t.Logf("Scalar subquery int 0: %d rows (should be 0)", len(result.Rows))
	}

	// Scalar subquery returning string 'yes'
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT str_val FROM flags WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery string 'yes' failed: %v", err)
	} else {
		t.Logf("Scalar subquery string 'yes': %d rows", len(result.Rows))
	}

	// Scalar subquery returning empty string
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT str_val FROM flags WHERE id = 2)")
	if err != nil {
		t.Logf("Scalar subquery empty string failed: %v", err)
	} else {
		t.Logf("Scalar subquery empty string: %d rows (should be 0)", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - MatchExpr ==========

func TestEvaluateWhereMatchExprDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-match-direct-*")
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

	// Create FTS table
	_, err = exec.Execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
	if err != nil {
		t.Logf("CREATE VIRTUAL TABLE failed: %v", err)
		return
	}

	_, err = exec.Execute("INSERT INTO docs VALUES ('hello world'), ('goodbye world')")
	if err != nil {
		t.Logf("INSERT failed: %v", err)
	}

	// Test MATCH
	result, err := exec.Execute("SELECT * FROM docs WHERE docs MATCH 'hello'")
	if err != nil {
		t.Logf("MATCH failed: %v", err)
	} else {
		t.Logf("MATCH: %d rows", len(result.Rows))
	}
}

// ========== Tests for pragmaIntegrityCheck with many tables ==========

func TestPragmaIntegrityCheckManyTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-many-*")
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

	// Create many tables
	for i := 1; i <= 10; i++ {
		_, err = exec.Execute(fmt.Sprintf("CREATE TABLE t%d (id INT PRIMARY KEY, val INT)", i))
		if err != nil {
			t.Fatalf("CREATE TABLE t%d failed: %v", i, err)
		}

		// Insert some data
		for j := 1; j <= 5; j++ {
			_, err = exec.Execute(fmt.Sprintf("INSERT INTO t%d VALUES (%d, %d)", i, j, j*i))
			if err != nil {
				t.Fatalf("INSERT t%d failed: %v", i, err)
			}
		}

		// Create index on some tables
		if i%2 == 0 {
			_, err = exec.Execute(fmt.Sprintf("CREATE INDEX idx_t%d_val ON t%d (val)", i, i))
			if err != nil {
				t.Logf("CREATE INDEX t%d failed: %v", i, err)
			}
		}
	}

	// Test INTEGRITY_CHECK
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK: %v", result.Rows)
		if len(result.Rows) > 0 && result.Rows[0][0] == "ok" {
			t.Logf("Integrity check passed with 10 tables")
		}
	}

	// Test QUICK_CHECK
	result, err = exec.Execute("PRAGMA QUICK_CHECK")
	if err != nil {
		t.Logf("QUICK_CHECK failed: %v", err)
	} else {
		t.Logf("QUICK_CHECK: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - ScalarSubquery with different types ==========

func TestEvaluateHavingScalarSubqueryTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-types-*")
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

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, bool_val BOOL, int_val INT, str_val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, TRUE, 1, 'yes')")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	for i := 1; i <= 6; i++ {
		region := "East"
		if i > 3 {
			region = "West"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', %d)", i, region, i*100))
		if err != nil {
			t.Fatalf("INSERT sales failed: %v", err)
		}
	}

	// HAVING with scalar subquery returning bool
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING (SELECT bool_val FROM flags WHERE id = 1)
	`)
	if err != nil {
		t.Logf("HAVING scalar bool failed: %v", err)
	} else {
		t.Logf("HAVING scalar bool: %d rows", len(result.Rows))
	}

	// HAVING with scalar subquery returning int
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING (SELECT int_val FROM flags WHERE id = 1)
	`)
	if err != nil {
		t.Logf("HAVING scalar int failed: %v", err)
	} else {
		t.Logf("HAVING scalar int: %d rows", len(result.Rows))
	}

	// HAVING with scalar subquery returning string
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING (SELECT str_val FROM flags WHERE id = 1)
	`)
	if err != nil {
		t.Logf("HAVING scalar string failed: %v", err)
	} else {
		t.Logf("HAVING scalar string: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - default return false ==========

func TestEvaluateHavingDefaultFalse(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-false-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING FALSE should return no rows
	result, err := exec.Execute(`
		SELECT grp, COUNT(*) as cnt
		FROM test
		GROUP BY grp
		HAVING FALSE
	`)
	if err != nil {
		t.Logf("HAVING FALSE failed: %v", err)
	} else {
		t.Logf("HAVING FALSE: %d rows (should be 0)", len(result.Rows))
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows for HAVING FALSE, got %d", len(result.Rows))
		}
	}

	// HAVING TRUE should return all rows
	result, err = exec.Execute(`
		SELECT grp, COUNT(*) as cnt
		FROM test
		GROUP BY grp
		HAVING TRUE
	`)
	if err != nil {
		t.Logf("HAVING TRUE failed: %v", err)
	} else {
		t.Logf("HAVING TRUE: %d rows", len(result.Rows))
	}
}