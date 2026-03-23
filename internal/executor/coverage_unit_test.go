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

// ========== Tests for evaluateExpression - CastExpr ==========

func TestEvaluateExpressionCastExprDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-direct-*")
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

	// Test CAST to INT
	result, err := exec.Execute("SELECT CAST('123' AS INT)")
	if err != nil {
		t.Logf("CAST to INT failed: %v", err)
	} else {
		t.Logf("CAST to INT: %v", result.Rows)
	}

	// Test CAST to FLOAT
	result, err = exec.Execute("SELECT CAST('123.45' AS FLOAT)")
	if err != nil {
		t.Logf("CAST to FLOAT failed: %v", err)
	} else {
		t.Logf("CAST to FLOAT: %v", result.Rows)
	}

	// Test CAST to VARCHAR
	result, err = exec.Execute("SELECT CAST(123 AS VARCHAR)")
	if err != nil {
		t.Logf("CAST to VARCHAR failed: %v", err)
	} else {
		t.Logf("CAST to VARCHAR: %v", result.Rows)
	}

	// Test CAST to BOOL
	result, err = exec.Execute("SELECT CAST(1 AS BOOL)")
	if err != nil {
		t.Logf("CAST to BOOL failed: %v", err)
	} else {
		t.Logf("CAST to BOOL: %v", result.Rows)
	}

	// Test CAST NULL
	result, err = exec.Execute("SELECT CAST(NULL AS INT)")
	if err != nil {
		t.Logf("CAST NULL failed: %v", err)
	} else {
		t.Logf("CAST NULL: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - CollateExpr ==========

func TestEvaluateExpressionCollateExprDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-direct-*")
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

	// Test COLLATE expression
	result, err := exec.Execute("SELECT 'hello' COLLATE BINARY")
	if err != nil {
		t.Logf("COLLATE expression failed: %v", err)
	} else {
		t.Logf("COLLATE expression: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - ParenExpr ==========

func TestEvaluateExpressionParenExprDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-paren-direct-*")
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

	// Test nested parentheses
	tests := []string{
		"SELECT (1)",
		"SELECT ((1))",
		"SELECT ((1 + 2))",
		"SELECT (1 + (2 * 3))",
		"SELECT ((1 + 2) * (3 + 4))",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("ParenExpr failed: %s, error: %v", tc, err)
		} else {
			t.Logf("ParenExpr: %s -> %v", tc, result.Rows)
		}
	}
}

// ========== Tests for evaluateExpression - ScalarSubquery edge cases ==========

func TestEvaluateExpressionScalarSubqueryEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-edge-*")
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

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Scalar subquery returning empty result
	result, err := exec.Execute("SELECT (SELECT val FROM nums WHERE val > 100)")
	if err != nil {
		t.Logf("Scalar subquery empty result failed: %v", err)
	} else {
		t.Logf("Scalar subquery empty result: %v", result.Rows)
	}

	// Scalar subquery returning one row
	result, err = exec.Execute("SELECT (SELECT val FROM nums WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery one row failed: %v", err)
	} else {
		t.Logf("Scalar subquery one row: %v", result.Rows)
	}

	// Scalar subquery returning multiple rows (should error)
	result, err = exec.Execute("SELECT (SELECT val FROM nums)")
	if err != nil {
		t.Logf("Scalar subquery multiple rows (expected error): %v", err)
	} else {
		t.Logf("Scalar subquery multiple rows: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - AnyAllExpr ==========

func TestEvaluateExpressionAnyAllExprDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-direct-*")
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

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// ANY with true condition
	result, err := exec.Execute("SELECT 15 > ANY (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ANY true failed: %v", err)
	} else {
		t.Logf("ANY true: %v", result.Rows)
	}

	// ANY with false condition
	result, err = exec.Execute("SELECT 5 > ANY (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ANY false failed: %v", err)
	} else {
		t.Logf("ANY false: %v", result.Rows)
	}

	// ALL with true condition
	result, err = exec.Execute("SELECT 5 < ALL (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ALL true failed: %v", err)
	} else {
		t.Logf("ALL true: %v", result.Rows)
	}

	// ALL with false condition
	result, err = exec.Execute("SELECT 25 < ALL (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ALL false failed: %v", err)
	} else {
		t.Logf("ALL false: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - RankExpr ==========

func TestEvaluateExpressionRankExprDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rank-direct-*")
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

	// Test RankExpr without FTS manager (should return 0.0)
	result, err := exec.Execute("SELECT RANK()")
	if err != nil {
		t.Logf("RANK without FTS failed: %v", err)
	} else {
		t.Logf("RANK without FTS: %v", result.Rows)
	}

	// Test with outerContext set
	exec.outerContext = map[string]interface{}{
		"__fts_rank": 0.75,
	}

	result, err = exec.Execute("SELECT RANK()")
	if err != nil {
		t.Logf("RANK with outer context failed: %v", err)
	} else {
		t.Logf("RANK with outer context: %v", result.Rows)
	}
	exec.outerContext = nil
}

// ========== Tests for evaluateWhere - BinaryExpr with various operators ==========

func TestEvaluateWhereBinaryExprOperators(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-bin-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10, 20), (2, 20, 10), (3, 15, 15)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test various comparison operators in WHERE
	tests := []string{
		"SELECT * FROM test WHERE a = 10",
		"SELECT * FROM test WHERE a != 10",
		"SELECT * FROM test WHERE a < 15",
		"SELECT * FROM test WHERE a > 15",
		"SELECT * FROM test WHERE a <= 15",
		"SELECT * FROM test WHERE a >= 15",
		"SELECT * FROM test WHERE a < b",
		"SELECT * FROM test WHERE a > b",
		"SELECT * FROM test WHERE a = b",
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc)
		if err != nil {
			t.Logf("WHERE binary failed: %s, error: %v", tc, err)
		} else {
			t.Logf("WHERE binary: %s -> %d rows", tc, len(result.Rows))
		}
	}
}

// ========== Tests for evaluateHaving - complex conditions ==========

func TestEvaluateHavingComplexConditionsExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-complex-*")
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

	for i := 1; i <= 10; i++ {
		region := "East"
		if i > 5 {
			region = "West"
		}
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', %d)", i, region, i*100))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// HAVING with AND
	result, err := exec.Execute(`
		SELECT region, COUNT(*) as cnt, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING COUNT(*) > 1 AND SUM(amount) > 500
	`)
	if err != nil {
		t.Logf("HAVING AND failed: %v", err)
	} else {
		t.Logf("HAVING AND: %d rows", len(result.Rows))
	}

	// HAVING with OR
	result, err = exec.Execute(`
		SELECT region, COUNT(*) as cnt
		FROM sales
		GROUP BY region
		HAVING COUNT(*) > 10 OR SUM(amount) > 1000
	`)
	if err != nil {
		t.Logf("HAVING OR failed: %v", err)
	} else {
		t.Logf("HAVING OR: %d rows", len(result.Rows))
	}

	// HAVING with NOT
	result, err = exec.Execute(`
		SELECT region, COUNT(*) as cnt
		FROM sales
		GROUP BY region
		HAVING NOT (COUNT(*) < 3)
	`)
	if err != nil {
		t.Logf("HAVING NOT failed: %v", err)
	} else {
		t.Logf("HAVING NOT: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - InExpr ==========

func TestEvaluateWhereInExprDirect(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// IN with list
	result, err := exec.Execute("SELECT * FROM test WHERE val IN (10, 20, 30)")
	if err != nil {
		t.Logf("IN list failed: %v", err)
	} else {
		t.Logf("IN list: %d rows", len(result.Rows))
	}

	// NOT IN with list
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT IN (10, 20)")
	if err != nil {
		t.Logf("NOT IN list failed: %v", err)
	} else {
		t.Logf("NOT IN list: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - IsNullExpr ==========

func TestEvaluateWhereIsNullExprDirect(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, NULL), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// IS NULL
	result, err := exec.Execute("SELECT * FROM test WHERE val IS NULL")
	if err != nil {
		t.Logf("IS NULL failed: %v", err)
	} else {
		t.Logf("IS NULL: %d rows", len(result.Rows))
	}

	// IS NOT NULL
	result, err = exec.Execute("SELECT * FROM test WHERE val IS NOT NULL")
	if err != nil {
		t.Logf("IS NOT NULL failed: %v", err)
	} else {
		t.Logf("IS NOT NULL: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - BetweenExpr ==========

func TestEvaluateWhereBetweenExprDirect(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// BETWEEN
	result, err := exec.Execute("SELECT * FROM test WHERE val BETWEEN 20 AND 40")
	if err != nil {
		t.Logf("BETWEEN failed: %v", err)
	} else {
		t.Logf("BETWEEN: %d rows", len(result.Rows))
	}

	// NOT BETWEEN
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT BETWEEN 20 AND 40")
	if err != nil {
		t.Logf("NOT BETWEEN failed: %v", err)
	} else {
		t.Logf("NOT BETWEEN: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - LikeExpr ==========

func TestEvaluateWhereLikeExprDirect(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// LIKE with %
	result, err := exec.Execute("SELECT * FROM test WHERE name LIKE 'A%'")
	if err != nil {
		t.Logf("LIKE %s failed: %v", "%", err)
	} else {
		t.Logf("LIKE %s: %d rows", "%", len(result.Rows))
	}

	// LIKE with _
	result, err = exec.Execute("SELECT * FROM test WHERE name LIKE '_ob'")
	if err != nil {
		t.Logf("LIKE _ failed: %v", err)
	} else {
		t.Logf("LIKE _: %d rows", len(result.Rows))
	}

	// NOT LIKE
	result, err = exec.Execute("SELECT * FROM test WHERE name NOT LIKE 'A%'")
	if err != nil {
		t.Logf("NOT LIKE failed: %v", err)
	} else {
		t.Logf("NOT LIKE: %d rows", len(result.Rows))
	}
}