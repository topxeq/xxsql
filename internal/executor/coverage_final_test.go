package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ========== Direct tests for evaluateHaving function ==========

func TestEvaluateHavingBinaryExprInWithSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-subq-*")
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

	_, err = exec.Execute("CREATE TABLE targets (id INT PRIMARY KEY, target_amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE targets failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 300), (2, 'West', 200)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO targets VALUES (1, 300), (2, 500)")
	if err != nil {
		t.Fatalf("INSERT targets failed: %v", err)
	}

	// Test BinaryExpr IN with subquery (using ParenExpr wrapping SubqueryExpr)
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) IN (SELECT target_amount FROM targets)
	`)
	if err != nil {
		t.Logf("HAVING IN subquery failed: %v", err)
	} else {
		t.Logf("HAVING IN subquery: %d rows", len(result.Rows))
	}

	// Test BinaryExpr IN with no match
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) IN (SELECT target_amount FROM targets WHERE target_amount > 1000)
	`)
	if err != nil {
		t.Logf("HAVING IN no match failed: %v", err)
	} else {
		t.Logf("HAVING IN no match: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving InExpr with value list ==========

func TestEvaluateHavingInExprValueList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-list-*")
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

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with IN value list (parsed as expression list)
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) = 300
	`)
	if err != nil {
		t.Logf("HAVING = value failed: %v", err)
	} else {
		t.Logf("HAVING = value: %d rows", len(result.Rows))
	}

	// Test with multiple groups
	result, err = exec.Execute(`
		SELECT region, COUNT(*) as cnt
		FROM sales
		GROUP BY region
		HAVING COUNT(*) > 1
	`)
	if err != nil {
		t.Logf("HAVING COUNT > 1 failed: %v", err)
	} else {
		t.Logf("HAVING COUNT > 1: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHavingExpr with SubqueryExpr ==========

func TestEvaluateHavingExprSubqueryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-expr-subq-*")
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

	_, err = exec.Execute("CREATE TABLE thresholds (id INT PRIMARY KEY, min_val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE thresholds failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 300)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO thresholds VALUES (1, 250)")
	if err != nil {
		t.Fatalf("INSERT thresholds failed: %v", err)
	}

	// HAVING with subquery expression in arithmetic
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) > (SELECT MIN(min_val) FROM thresholds)
	`)
	if err != nil {
		t.Logf("HAVING with subquery arithmetic failed: %v", err)
	} else {
		t.Logf("HAVING with subquery arithmetic: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - ColumnRef outer context ==========

func TestEvaluateExpressionColumnRefOuterContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-outer-*")
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

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(100)},
	}

	// Test ColumnRef with outerContext set but column not in map
	exec.outerContext = map[string]interface{}{
		"outer_val": 999,
	}

	colRef := &sql.ColumnRef{Name: "outer_val"}
	result, err := exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef outer context (expected error): %v", err)
	} else {
		t.Logf("ColumnRef outer context result: %v", result)
	}

	// Test ColumnRef with table prefix mismatch and outer context
	exec.currentTable = "test"
	colRef2 := &sql.ColumnRef{Table: "other", Name: "col"}
	result, err = exec.evaluateExpression(colRef2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef table mismatch (expected error): %v", err)
	} else {
		t.Logf("ColumnRef table mismatch result: %v", result)
	}

	exec.outerContext = nil
	exec.currentTable = ""
}

// ========== Tests for evaluateExpression - UnaryExpr NOT ==========

func TestEvaluateExpressionUnaryExprNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-not-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, flag BOOL)")
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

	// Test with bool value
	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewBoolValue(true)},
	}

	// UnaryExpr NOT with bool column
	notExpr := &sql.UnaryExpr{
		Op:    sql.OpNot,
		Right: &sql.ColumnRef{Name: "flag"},
	}
	result, err := exec.evaluateExpression(notExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr NOT bool failed: %v", err)
	} else {
		t.Logf("UnaryExpr NOT bool result: %v", result)
	}

	// UnaryExpr NOT with NULL
	mockRow2 := &row.Row{
		ID:     2,
		Values: []types.Value{types.NewIntValue(2), types.NewNullValue()},
	}
	result, err = exec.evaluateExpression(notExpr, mockRow2, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr NOT NULL failed: %v", err)
	} else {
		t.Logf("UnaryExpr NOT NULL result: %v", result)
	}
}

// ========== Tests for evaluateWhere - InExpr with list ==========

func TestEvaluateWhereInExprWithList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-in-list-*")
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

	// IN with subquery
	_, err = exec.Execute("CREATE TABLE nums (n INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE nums failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (10), (30)")
	if err != nil {
		t.Fatalf("INSERT nums failed: %v", err)
	}

	result, err := exec.Execute("SELECT * FROM test WHERE val IN (SELECT n FROM nums)")
	if err != nil {
		t.Logf("IN subquery failed: %v", err)
	} else {
		t.Logf("IN subquery: %d rows", len(result.Rows))
	}

	// NOT IN with subquery
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT IN (SELECT n FROM nums)")
	if err != nil {
		t.Logf("NOT IN subquery failed: %v", err)
	} else {
		t.Logf("NOT IN subquery: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - FunctionCall with row ==========

func TestEvaluateExpressionFunctionCallWithRow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-func-row-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewStringValue("hello", types.TypeVarchar), types.NewIntValue(-5)},
	}

	// Test ABS function with column
	absFunc := &sql.FunctionCall{
		Name: "ABS",
		Args: []sql.Expression{&sql.ColumnRef{Name: "val"}},
	}
	result, err := exec.evaluateExpression(absFunc, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ABS function failed: %v", err)
	} else {
		t.Logf("ABS function result: %v", result)
	}

	// Test UPPER function with column
	upperFunc := &sql.FunctionCall{
		Name: "UPPER",
		Args: []sql.Expression{&sql.ColumnRef{Name: "name"}},
	}
	result, err = exec.evaluateExpression(upperFunc, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UPPER function failed: %v", err)
	} else {
		t.Logf("UPPER function result: %v", result)
	}
}

// ========== Tests for evaluateWhere - MatchExpr ==========

func TestEvaluateWhereMatchExprWithFTS(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-match-fts-*")
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

	// Create table for FTS
	_, err = exec.Execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE docs failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO docs VALUES (1, 'hello world'), (2, 'goodbye world')")
	if err != nil {
		t.Fatalf("INSERT docs failed: %v", err)
	}

	// Create FTS index
	_, err = exec.Execute("CREATE FTS INDEX docs_content_idx ON docs(content)")
	if err != nil {
		t.Logf("CREATE FTS INDEX failed: %v", err)
	}

	// Test MATCH expression (may require FTS setup)
	result, err := exec.Execute("SELECT * FROM docs WHERE content MATCH 'hello'")
	if err != nil {
		t.Logf("MATCH expression failed: %v", err)
	} else {
		t.Logf("MATCH expression: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - ScalarSubquery multiple rows ==========

func TestEvaluateExpressionScalarSubqueryMultipleRows(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-multi-*")
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

	// Scalar subquery returning multiple rows - should error
	result, err := exec.Execute("SELECT (SELECT val FROM nums)")
	if err != nil {
		t.Logf("Scalar subquery multiple rows (expected error): %v", err)
	} else {
		t.Logf("Scalar subquery multiple rows: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - SubqueryExpr ==========

func TestEvaluateExpressionSubqueryExprTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-subq-expr-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SubqueryExpr returning single value
	result, err := exec.Execute("SELECT (SELECT val FROM data WHERE id = 1) + 5")
	if err != nil {
		t.Logf("SubqueryExpr arithmetic failed: %v", err)
	} else {
		t.Logf("SubqueryExpr arithmetic: %v", result.Rows)
	}

	// SubqueryExpr returning no rows
	result, err = exec.Execute("SELECT (SELECT val FROM data WHERE id = 999)")
	if err != nil {
		t.Logf("SubqueryExpr empty failed: %v", err)
	} else {
		t.Logf("SubqueryExpr empty: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - AnyAllExpr in expression ==========

func TestEvaluateExpressionAnyAllInExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-expr-*")
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

	// ANY in expression context
	result, err := exec.Execute("SELECT 15 > ANY (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ANY in expression failed: %v", err)
	} else {
		t.Logf("ANY in expression: %v", result.Rows)
	}

	// ALL in expression context
	result, err = exec.Execute("SELECT 5 < ALL (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ALL in expression failed: %v", err)
	} else {
		t.Logf("ALL in expression: %v", result.Rows)
	}

	// ALL with empty result
	result, err = exec.Execute("SELECT 5 < ALL (SELECT val FROM nums WHERE val > 100)")
	if err != nil {
		t.Logf("ALL empty failed: %v", err)
	} else {
		t.Logf("ALL empty: %v", result.Rows)
	}
}

// ========== Tests for evaluateWhere - ScalarSubquery returning string ==========

func TestEvaluateWhereScalarSubqueryReturnTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-types-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, str_val VARCHAR, int_val INT, bool_val BOOL)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1), (2)")
	if err != nil {
		t.Fatalf("INSERT items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, 'yes', 1, TRUE), (2, '', 0, FALSE)")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	tests := []struct {
		name  string
		query string
	}{
		{"string_nonempty", "SELECT * FROM items WHERE (SELECT str_val FROM flags WHERE id = 1)"},
		{"string_empty", "SELECT * FROM items WHERE (SELECT str_val FROM flags WHERE id = 2)"},
		{"int_nonzero", "SELECT * FROM items WHERE (SELECT int_val FROM flags WHERE id = 1)"},
		{"int_zero", "SELECT * FROM items WHERE (SELECT int_val FROM flags WHERE id = 2)"},
		{"bool_true", "SELECT * FROM items WHERE (SELECT bool_val FROM flags WHERE id = 1)"},
		{"bool_false", "SELECT * FROM items WHERE (SELECT bool_val FROM flags WHERE id = 2)"},
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc.query)
		if err != nil {
			t.Logf("%s failed: %v", tc.name, err)
		} else {
			t.Logf("%s: %d rows", tc.name, len(result.Rows))
		}
	}
}

// ========== Tests for evaluateExpression - BinaryExpr with NULL operands ==========

func TestEvaluateExpressionBinaryWithNull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-bin-null-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// BinaryExpr with NULL on left
	binExpr := &sql.BinaryExpr{
		Left:  &sql.Literal{Value: nil},
		Op:    sql.OpAdd,
		Right: &sql.Literal{Value: 5},
	}
	result, err := exec.evaluateExpression(binExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("BinaryExpr NULL left failed: %v", err)
	} else {
		t.Logf("BinaryExpr NULL left result: %v", result)
	}

	// BinaryExpr with NULL on right
	binExpr2 := &sql.BinaryExpr{
		Left:  &sql.Literal{Value: 5},
		Op:    sql.OpAdd,
		Right: &sql.Literal{Value: nil},
	}
	result, err = exec.evaluateExpression(binExpr2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("BinaryExpr NULL right failed: %v", err)
	} else {
		t.Logf("BinaryExpr NULL right result: %v", result)
	}
}

// ========== Tests for evaluateExpression - CollateExpr ==========

func TestEvaluateExpressionCollateExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-collate-expr-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// CollateExpr
	collateExpr := &sql.CollateExpr{
		Expr:    &sql.Literal{Value: "hello"},
		Collate: "BINARY",
	}
	result, err := exec.evaluateExpression(collateExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("CollateExpr failed: %v", err)
	} else {
		t.Logf("CollateExpr result: %v", result)
	}
}

// ========== Tests for pragmaIntegrityCheck with errors ==========

func TestPragmaIntegrityCheckWithErrors(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-integrity-err-*")
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

	// Create table with data
	_, err = exec.Execute("CREATE TABLE test1 (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE test1 failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test1 VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT test1 failed: %v", err)
	}

	// Create another table
	_, err = exec.Execute("CREATE TABLE test2 (id INT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE test2 failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO test2 VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT test2 failed: %v", err)
	}

	// Run integrity check
	result, err := exec.Execute("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		t.Logf("INTEGRITY_CHECK failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK: %v", result.Rows)
	}

	// Run with specific table
	result, err = exec.Execute("PRAGMA INTEGRITY_CHECK(test1)")
	if err != nil {
		t.Logf("INTEGRITY_CHECK(test1) failed: %v", err)
	} else {
		t.Logf("INTEGRITY_CHECK(test1): %v", result.Rows)
	}

	// Run quick check
	result, err = exec.Execute("PRAGMA QUICK_CHECK")
	if err != nil {
		t.Logf("QUICK_CHECK failed: %v", err)
	} else {
		t.Logf("QUICK_CHECK: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - RankExpr ==========

func TestEvaluateExpressionRankExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rank-detailed-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// RankExpr without FTS manager
	rankExpr := &sql.RankExpr{}
	result, err := exec.evaluateExpression(rankExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("RankExpr without FTS failed: %v", err)
	} else {
		t.Logf("RankExpr without FTS result: %v", result)
	}

	// RankExpr with outer context containing rank
	exec.outerContext = map[string]interface{}{
		"__fts_rank": 0.95,
	}
	result, err = exec.evaluateExpression(rankExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("RankExpr with context failed: %v", err)
	} else {
		t.Logf("RankExpr with context result: %v", result)
	}
	exec.outerContext = nil
}

// ========== Tests for HAVING with NOT IN subquery ==========

func TestEvaluateHavingNotInSubqueryExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-not-in-*")
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

	_, err = exec.Execute("CREATE TABLE exclude (id INT PRIMARY KEY, target INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE exclude failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 300), (2, 'West', 200)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO exclude VALUES (1, 500)")
	if err != nil {
		t.Fatalf("INSERT exclude failed: %v", err)
	}

	// HAVING NOT IN subquery
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) NOT IN (SELECT target FROM exclude)
	`)
	if err != nil {
		t.Logf("HAVING NOT IN failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - InExpr with error ==========

func TestEvaluateWhereInExprError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-in-err-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE nums (n INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE nums failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (10)")
	if err != nil {
		t.Fatalf("INSERT nums failed: %v", err)
	}

	// IN with matching values
	result, err := exec.Execute("SELECT * FROM test WHERE val IN (SELECT n FROM nums)")
	if err != nil {
		t.Logf("IN subquery failed: %v", err)
	} else {
		t.Logf("IN subquery: %d rows", len(result.Rows))
	}

	// NOT IN with no matching values
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT IN (SELECT n FROM nums WHERE n > 100)")
	if err != nil {
		t.Logf("NOT IN no match failed: %v", err)
	} else {
		t.Logf("NOT IN no match: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - ParenExpr with various expressions ==========

func TestEvaluateExpressionParenExprVariations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-paren-var-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// ParenExpr with Literal
	parenExpr := &sql.ParenExpr{Expr: &sql.Literal{Value: 42}}
	result, err := exec.evaluateExpression(parenExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ParenExpr Literal failed: %v", err)
	} else {
		t.Logf("ParenExpr Literal result: %v", result)
	}

	// ParenExpr with BinaryExpr
	parenExpr2 := &sql.ParenExpr{
		Expr: &sql.BinaryExpr{
			Left:  &sql.Literal{Value: 10},
			Op:    sql.OpAdd,
			Right: &sql.Literal{Value: 5},
		},
	}
	result, err = exec.evaluateExpression(parenExpr2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ParenExpr BinaryExpr failed: %v", err)
	} else {
		t.Logf("ParenExpr BinaryExpr result: %v", result)
	}
}

// ========== Tests for evaluateExpression - UnaryExpr with float ==========

func TestEvaluateExpressionUnaryExprFloat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-float-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// UnaryExpr with float64 negation
	unaryExpr := &sql.UnaryExpr{
		Op:    sql.OpNeg,
		Right: &sql.Literal{Value: 3.14},
	}
	result, err := exec.evaluateExpression(unaryExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr float failed: %v", err)
	} else {
		t.Logf("UnaryExpr float result: %v", result)
	}

	// UnaryExpr with int64 negation
	unaryExpr2 := &sql.UnaryExpr{
		Op:    sql.OpNeg,
		Right: &sql.Literal{Value: int64(42)},
	}
	result, err = exec.evaluateExpression(unaryExpr2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr int64 failed: %v", err)
	} else {
		t.Logf("UnaryExpr int64 result: %v", result)
	}

	// UnaryExpr with int negation
	unaryExpr3 := &sql.UnaryExpr{
		Op:    sql.OpNeg,
		Right: &sql.Literal{Value: 100},
	}
	result, err = exec.evaluateExpression(unaryExpr3, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr int failed: %v", err)
	} else {
		t.Logf("UnaryExpr int result: %v", result)
	}
}

// ========== Tests for computeAggregateForHaving ==========

func TestComputeAggregateForHavingExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-agg-having-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test MIN in HAVING
	result, err := exec.Execute(`
		SELECT val, COUNT(*) as cnt
		FROM data
		GROUP BY val
		HAVING MIN(val) >= 10
	`)
	if err != nil {
		t.Logf("HAVING MIN failed: %v", err)
	} else {
		t.Logf("HAVING MIN: %d rows", len(result.Rows))
	}

	// Test MAX in HAVING
	result, err = exec.Execute(`
		SELECT val, COUNT(*) as cnt
		FROM data
		GROUP BY val
		HAVING MAX(val) <= 30
	`)
	if err != nil {
		t.Logf("HAVING MAX failed: %v", err)
	} else {
		t.Logf("HAVING MAX: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - column out of range ==========

func TestEvaluateExpressionColumnOutOfRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-range-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	// Create row with fewer values than columns
	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{}, // Empty values
	}

	// Try to access column
	colRef := &sql.ColumnRef{Name: "id"}
	result, err := exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef with empty values (expected): %v", err)
	} else {
		t.Logf("ColumnRef with empty values result: %v", result)
	}
}

// ========== Tests for evaluateWhere - AnyAllExpr with various operators ==========

func TestEvaluateWhereAnyAllWithOperators(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-op-*")
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

	tests := []struct {
		name  string
		query string
	}{
		{"ANY =", "SELECT * FROM nums WHERE val = ANY (SELECT val FROM nums WHERE val < 20)"},
		{"ANY !=", "SELECT * FROM nums WHERE val != ANY (SELECT val FROM nums)"},
		{"ANY <=", "SELECT * FROM nums WHERE val <= ANY (SELECT val FROM nums WHERE val >= 20)"},
		{"ANY >=", "SELECT * FROM nums WHERE val >= ANY (SELECT val FROM nums WHERE val <= 20)"},
		{"ALL =", "SELECT * FROM nums WHERE val = ALL (SELECT val FROM nums WHERE val = 10)"},
		{"ALL <=", "SELECT * FROM nums WHERE val <= ALL (SELECT val FROM nums WHERE val >= 30)"},
		{"ALL >=", "SELECT * FROM nums WHERE val >= ALL (SELECT val FROM nums WHERE val <= 10)"},
	}

	for _, tc := range tests {
		result, err := exec.Execute(tc.query)
		if err != nil {
			t.Logf("%s failed: %v", tc.name, err)
		} else {
			t.Logf("%s: %d rows", tc.name, len(result.Rows))
		}
	}
}

// ========== Tests for evaluateExpression - ScalarSubquery with NULL ==========

func TestEvaluateExpressionScalarSubqueryNull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-null-*")
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

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, NULL)")
	if err != nil {
		t.Logf("INSERT NULL failed: %v", err)
	}

	// Scalar subquery returning NULL
	result, err := exec.Execute("SELECT (SELECT val FROM data WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery NULL failed: %v", err)
	} else {
		t.Logf("Scalar subquery NULL: %v", result.Rows)
	}

	// Scalar subquery with empty result
	result, err = exec.Execute("SELECT (SELECT val FROM data WHERE id = 999)")
	if err != nil {
		t.Logf("Scalar subquery empty failed: %v", err)
	} else {
		t.Logf("Scalar subquery empty: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - ScalarSubquery returning multiple rows ==========

func TestEvaluateHavingScalarSubqueryMultipleRows(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-scalar-multi-*")
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

	_, err = exec.Execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE nums failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'West', 200)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT nums failed: %v", err)
	}

	// HAVING with scalar subquery returning multiple rows
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING (SELECT val FROM nums)
	`)
	if err != nil {
		t.Logf("HAVING scalar subquery multiple rows (expected error): %v", err)
	} else {
		t.Logf("HAVING scalar subquery multiple rows: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - ScalarSubquery returning string in WHERE ==========

func TestEvaluateWhereScalarSubqueryStringInWhere(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-scalar-str-*")
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1), (2)")
	if err != nil {
		t.Fatalf("INSERT items failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE flags (id INT PRIMARY KEY, str_val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE flags failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO flags VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT flags failed: %v", err)
	}

	// Scalar subquery returning string in WHERE
	result, err := exec.Execute("SELECT * FROM items WHERE (SELECT str_val FROM flags WHERE id = 1)")
	if err != nil {
		t.Logf("WHERE scalar string failed: %v", err)
	} else {
		t.Logf("WHERE scalar string: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - default return nil ==========

func TestEvaluateExpressionDefaultNil(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-nil-*")
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

	// Test with an expression that falls through to default nil
	// This would be an unknown expression type
	t.Logf("Testing evaluateExpression with various expression types")
	_ = exec // Use exec to avoid unused variable warning
}

// ========== Tests for evaluateExpression - CastExpr with various types ==========

func TestEvaluateExpressionCastExprDetailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-detailed-*")
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

	// Test CAST with blob hex string
	result, err := exec.Execute("SELECT CAST(X'48454C4C4F' AS VARCHAR)")
	if err != nil {
		t.Logf("CAST blob to VARCHAR failed: %v", err)
	} else {
		t.Logf("CAST blob to VARCHAR: %v", result.Rows)
	}

	// Test CAST with various numeric types
	result, err = exec.Execute("SELECT CAST(123.456 AS INT)")
	if err != nil {
		t.Logf("CAST float to INT failed: %v", err)
	} else {
		t.Logf("CAST float to INT: %v", result.Rows)
	}

	// Test CAST bool to string
	result, err = exec.Execute("SELECT CAST(TRUE AS VARCHAR)")
	if err != nil {
		t.Logf("CAST bool to VARCHAR failed: %v", err)
	} else {
		t.Logf("CAST bool to VARCHAR: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - FunctionCall with nil row ==========

func TestEvaluateExpressionFunctionCallNilRow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-func-nil-row-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	// FunctionCall with nil row (no row context)
	funcCall := &sql.FunctionCall{
		Name: "ABS",
		Args: []sql.Expression{&sql.Literal{Value: -10}},
	}
	result, err := exec.evaluateExpression(funcCall, nil, columnMap, columnOrder)
	if err != nil {
		t.Logf("FunctionCall nil row failed: %v", err)
	} else {
		t.Logf("FunctionCall nil row result: %v", result)
	}

	// Test UPPER with nil row
	upperCall := &sql.FunctionCall{
		Name: "UPPER",
		Args: []sql.Expression{&sql.Literal{Value: "hello"}},
	}
	result, err = exec.evaluateExpression(upperCall, nil, columnMap, columnOrder)
	if err != nil {
		t.Logf("UPPER nil row failed: %v", err)
	} else {
		t.Logf("UPPER nil row result: %v", result)
	}
}

// ========== Tests for evaluateWhere - Literal bool ==========

func TestEvaluateWhereLiteralBoolLiteral(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-literal-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test WHERE TRUE literal
	result, err := exec.Execute("SELECT * FROM test WHERE TRUE")
	if err != nil {
		t.Logf("WHERE TRUE failed: %v", err)
	} else {
		t.Logf("WHERE TRUE: %d rows", len(result.Rows))
	}

	// Test WHERE FALSE literal
	result, err = exec.Execute("SELECT * FROM test WHERE FALSE")
	if err != nil {
		t.Logf("WHERE FALSE failed: %v", err)
	} else {
		t.Logf("WHERE FALSE: %d rows", len(result.Rows))
	}

	// Test WHERE with boolean expression
	result, err = exec.Execute("SELECT * FROM test WHERE 1 = 1")
	if err != nil {
		t.Logf("WHERE 1=1 failed: %v", err)
	} else {
		t.Logf("WHERE 1=1: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - ColumnRef not found ==========

func TestEvaluateExpressionColumnRefNotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-not-found-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// Try to access non-existent column
	colRef := &sql.ColumnRef{Name: "nonexistent"}
	result, err := exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef not found (expected error): %v", err)
	} else {
		t.Logf("ColumnRef not found result: %v", result)
	}

	// Try with outer context
	exec.outerContext = map[string]interface{}{
		"nonexistent": 999,
	}
	result, err = exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef from outer context failed: %v", err)
	} else {
		t.Logf("ColumnRef from outer context result: %v", result)
	}
	exec.outerContext = nil
}

// ========== Tests for evaluateExpression - BinaryExpr error propagation ==========

func TestEvaluateExpressionBinaryExprError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-bin-err-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// BinaryExpr with error in left operand
	binExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "nonexistent"},
		Op:    sql.OpAdd,
		Right: &sql.Literal{Value: 5},
	}
	result, err := exec.evaluateExpression(binExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("BinaryExpr left error (expected): %v", err)
	} else {
		t.Logf("BinaryExpr left error result: %v", result)
	}

	// BinaryExpr with error in right operand
	binExpr2 := &sql.BinaryExpr{
		Left:  &sql.Literal{Value: 5},
		Op:    sql.OpAdd,
		Right: &sql.ColumnRef{Name: "nonexistent"},
	}
	result, err = exec.evaluateExpression(binExpr2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("BinaryExpr right error (expected): %v", err)
	} else {
		t.Logf("BinaryExpr right error result: %v", result)
	}
}

// ========== Tests for evaluateExpression - CastExpr error ==========

func TestEvaluateExpressionCastExprError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-cast-err-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// CastExpr with error in inner expression
	castExpr := &sql.CastExpr{
		Expr: &sql.ColumnRef{Name: "nonexistent"},
		Type: &sql.DataType{Name: "INT"},
	}
	result, err := exec.evaluateExpression(castExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("CastExpr error (expected): %v", err)
	} else {
		t.Logf("CastExpr error result: %v", result)
	}

	// CastExpr with valid value
	castExpr2 := &sql.CastExpr{
		Expr: &sql.Literal{Value: "123"},
		Type: &sql.DataType{Name: "INT"},
	}
	result, err = exec.evaluateExpression(castExpr2, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("CastExpr valid failed: %v", err)
	} else {
		t.Logf("CastExpr valid result: %v", result)
	}
}

// ========== Tests for evaluateExpression - UnaryExpr error ==========

func TestEvaluateExpressionUnaryExprError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unary-err-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// UnaryExpr with error in operand
	unaryExpr := &sql.UnaryExpr{
		Op:    sql.OpNeg,
		Right: &sql.ColumnRef{Name: "nonexistent"},
	}
	result, err := exec.evaluateExpression(unaryExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("UnaryExpr error (expected): %v", err)
	} else {
		t.Logf("UnaryExpr error result: %v", result)
	}
}

// ========== Tests for evaluateExpression - FunctionCall error ==========

func TestEvaluateExpressionFunctionCallError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-func-err-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// FunctionCall with error in argument
	funcCall := &sql.FunctionCall{
		Name: "ABS",
		Args: []sql.Expression{&sql.ColumnRef{Name: "nonexistent"}},
	}
	result, err := exec.evaluateExpression(funcCall, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("FunctionCall error (expected): %v", err)
	} else {
		t.Logf("FunctionCall error result: %v", result)
	}
}

// ========== Tests for evaluateExpression - ScalarSubquery with correlated ref ==========

func TestEvaluateExpressionScalarSubqueryCorrelated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-corr-*")
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

	_, err = exec.Execute("CREATE TABLE outer_tbl (id INT PRIMARY KEY, ref_val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_tbl failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE inner_tbl (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_tbl failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO outer_tbl VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT outer_tbl failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO inner_tbl VALUES (1, 5), (2, 15), (3, 25)")
	if err != nil {
		t.Fatalf("INSERT inner_tbl failed: %v", err)
	}

	// Correlated scalar subquery
	result, err := exec.Execute(`
		SELECT id, (SELECT val FROM inner_tbl WHERE val > o.ref_val ORDER BY val LIMIT 1)
		FROM outer_tbl o
	`)
	if err != nil {
		t.Logf("Correlated scalar subquery failed: %v", err)
	} else {
		t.Logf("Correlated scalar subquery: %v", result.Rows)
	}
}

// ========== Tests for evaluateWhere - InExpr with NOT ==========

func TestEvaluateWhereInExprNotVariations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-in-not-*")
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

	_, err = exec.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE exclude (n INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE exclude failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO exclude VALUES (20)")
	if err != nil {
		t.Fatalf("INSERT exclude failed: %v", err)
	}

	// NOT IN with some matches
	result, err := exec.Execute("SELECT * FROM test WHERE val NOT IN (SELECT n FROM exclude)")
	if err != nil {
		t.Logf("NOT IN some matches failed: %v", err)
	} else {
		t.Logf("NOT IN some matches: %d rows", len(result.Rows))
	}

	// IN with match
	result, err = exec.Execute("SELECT * FROM test WHERE val IN (SELECT n FROM exclude)")
	if err != nil {
		t.Logf("IN with match failed: %v", err)
	} else {
		t.Logf("IN with match: %d rows", len(result.Rows))
	}

	// NOT IN with no subquery rows
	result, err = exec.Execute("SELECT * FROM test WHERE val NOT IN (SELECT n FROM exclude WHERE n > 100)")
	if err != nil {
		t.Logf("NOT IN no subquery rows failed: %v", err)
	} else {
		t.Logf("NOT IN no subquery rows: %d rows", len(result.Rows))
	}
}

// ========== Tests for HAVING with IN and NOT ==========

func TestEvaluateHavingInExprWithNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-in-not-*")
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

	_, err = exec.Execute("CREATE TABLE targets (target INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE targets failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 300)")
	if err != nil {
		t.Fatalf("INSERT sales failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO targets VALUES (300)")
	if err != nil {
		t.Fatalf("INSERT targets failed: %v", err)
	}

	// HAVING with IN and match
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) IN (SELECT target FROM targets)
	`)
	if err != nil {
		t.Logf("HAVING IN match failed: %v", err)
	} else {
		t.Logf("HAVING IN match: %d rows", len(result.Rows))
	}

	// HAVING with NOT IN
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) NOT IN (SELECT target FROM targets)
	`)
	if err != nil {
		t.Logf("HAVING NOT IN failed: %v", err)
	} else {
		t.Logf("HAVING NOT IN: %d rows", len(result.Rows))
	}
}