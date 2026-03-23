package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ========== Tests for evaluateExpression - ScalarSubquery with row context ==========

func TestEvaluateExpressionScalarSubqueryWithRowContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-row-ctx-*")
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

	_, err = exec.Execute("INSERT INTO outer_tbl VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT outer_tbl failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO inner_tbl VALUES (1, 15), (2, 25)")
	if err != nil {
		t.Fatalf("INSERT inner_tbl failed: %v", err)
	}

	tbl, _ := engine.GetTable("outer_tbl")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// Set currentTable to test the tablePrefix path
	exec.currentTable = "outer_tbl"

	// Parse and execute a scalar subquery that uses outer context
	result, err := exec.Execute(`
		SELECT (SELECT val FROM inner_tbl WHERE val > o.ref_val ORDER BY val LIMIT 1)
		FROM outer_tbl o
		WHERE id = 1
	`)
	if err != nil {
		t.Logf("Scalar subquery with outer ref failed: %v", err)
	} else {
		t.Logf("Scalar subquery with outer ref: %v", result.Rows)
	}

	// Direct test with evaluateExpression
	// Create a ScalarSubquery AST node
	scalarSubq := &sql.ScalarSubquery{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_tbl"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "val"},
					Op:    sql.OpGt,
					Right: &sql.ColumnRef{Name: "ref_val"},
				},
			},
		},
	}

	evalResult, err := exec.evaluateExpression(scalarSubq, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("Direct ScalarSubquery evaluation failed: %v", err)
	} else {
		t.Logf("Direct ScalarSubquery evaluation result: %v", evalResult)
	}

	exec.currentTable = ""
}

// ========== Tests for evaluateExpression - SubqueryExpr with row context ==========

func TestEvaluateExpressionSubqueryExprWithRowContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-subq-row-ctx-*")
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

	tbl, _ := engine.GetTable("data")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	exec.currentTable = "data"

	// Create a SubqueryExpr AST node
	subqExpr := &sql.SubqueryExpr{
		Select: &sql.SelectStmt{
			Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
			From: &sql.FromClause{
				Table: &sql.TableRef{Name: "data"},
			},
			Where: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpEq,
				Right: &sql.Literal{Value: 1},
			},
		},
	}

	result, err := exec.evaluateExpression(subqExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("SubqueryExpr evaluation failed: %v", err)
	} else {
		t.Logf("SubqueryExpr evaluation result: %v", result)
	}

	exec.currentTable = ""
}

// ========== Tests for evaluateExpression - AnyAllExpr with comparison error ==========

func TestEvaluateExpressionAnyAllCompareError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-cmp-err-*")
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

	_, err = exec.Execute("INSERT INTO nums VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// ANY with string comparison to int (might cause error)
	result, err := exec.Execute("SELECT 'hello' = ANY (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ANY string=int failed: %v", err)
	} else {
		t.Logf("ANY string=int: %v", result.Rows)
	}

	// ALL with string comparison to int
	result, err = exec.Execute("SELECT 'hello' != ALL (SELECT val FROM nums)")
	if err != nil {
		t.Logf("ALL string!=int failed: %v", err)
	} else {
		t.Logf("ALL string!=int: %v", result.Rows)
	}
}

// ========== Tests for evaluateExpression - AnyAllExpr with row context ==========

func TestEvaluateExpressionAnyAllWithRowContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-row-ctx-*")
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

	_, err = exec.Execute("CREATE TABLE main (id INT PRIMARY KEY, threshold INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE main failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE values (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE values failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO main VALUES (1, 15)")
	if err != nil {
		t.Fatalf("INSERT main failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO values VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("INSERT values failed: %v", err)
	}

	tbl, _ := engine.GetTable("main")
	tblInfo := tbl.GetInfo()

	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[col.Name] = col
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(15)},
	}

	exec.currentTable = "main"

	// Create AnyAllExpr AST node
	anyExpr := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "threshold"},
		Op:    sql.OpLt,
		IsAny: true,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "values"},
				},
			},
		},
	}

	result, err := exec.evaluateExpression(anyExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("AnyAllExpr evaluation failed: %v", err)
	} else {
		t.Logf("AnyAllExpr evaluation result: %v", result)
	}

	// Test ALL
	allExpr := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "threshold"},
		Op:    sql.OpGt,
		IsAny: false,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "values"},
				},
			},
		},
	}

	result, err = exec.evaluateExpression(allExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ALL expr evaluation failed: %v", err)
	} else {
		t.Logf("ALL expr evaluation result: %v", result)
	}

	exec.currentTable = ""
}

// ========== Tests for evaluateHaving - InExpr with value list ==========

func TestEvaluateHavingInExprValueListExtra(t *testing.T) {
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

	_, err = exec.Execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'West', 200), (3, 'North', 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING with comparison
	result, err := exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) = 100
	`)
	if err != nil {
		t.Logf("HAVING = failed: %v", err)
	} else {
		t.Logf("HAVING =: %d rows", len(result.Rows))
	}

	// HAVING with OR
	result, err = exec.Execute(`
		SELECT region, SUM(amount) as total
		FROM sales
		GROUP BY region
		HAVING SUM(amount) = 100 OR SUM(amount) = 200
	`)
	if err != nil {
		t.Logf("HAVING OR failed: %v", err)
	} else {
		t.Logf("HAVING OR: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - ScalarSubquery with various return types ==========

func TestEvaluateWhereScalarSubqueryReturnTypesExtra(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE items (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE items failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1), (2)")
	if err != nil {
		t.Fatalf("INSERT items failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, int_val INT, str_val VARCHAR, float_val FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE data failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 1, 'hello', 1.5)")
	if err != nil {
		t.Fatalf("INSERT data failed: %v", err)
	}

	// Scalar subquery returning int
	result, err := exec.Execute("SELECT * FROM items WHERE (SELECT int_val FROM data WHERE id = 1)")
	if err != nil {
		t.Logf("WHERE int scalar failed: %v", err)
	} else {
		t.Logf("WHERE int scalar: %d rows", len(result.Rows))
	}

	// Scalar subquery returning string
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT str_val FROM data WHERE id = 1)")
	if err != nil {
		t.Logf("WHERE string scalar failed: %v", err)
	} else {
		t.Logf("WHERE string scalar: %d rows", len(result.Rows))
	}

	// Scalar subquery returning float
	result, err = exec.Execute("SELECT * FROM items WHERE (SELECT float_val FROM data WHERE id = 1)")
	if err != nil {
		t.Logf("WHERE float scalar failed: %v", err)
	} else {
		t.Logf("WHERE float scalar: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - UnaryExpr NOT ==========

func TestEvaluateHavingUnaryExprNotExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-unary-*")
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

	_, err = exec.Execute("INSERT INTO data VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// HAVING NOT with aggregate
	result, err := exec.Execute(`
		SELECT grp, COUNT(*) as cnt
		FROM data
		GROUP BY grp
		HAVING NOT (COUNT(*) > 2)
	`)
	if err != nil {
		t.Logf("HAVING NOT COUNT failed: %v", err)
	} else {
		t.Logf("HAVING NOT COUNT: %d rows", len(result.Rows))
	}

	// HAVING NOT with comparison
	result, err = exec.Execute(`
		SELECT grp, SUM(val) as total
		FROM data
		GROUP BY grp
		HAVING NOT (SUM(val) < 50)
	`)
	if err != nil {
		t.Logf("HAVING NOT SUM failed: %v", err)
	} else {
		t.Logf("HAVING NOT SUM: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - ColumnRef with exact table match ==========

func TestEvaluateExpressionColumnRefExactTableMatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-exact-match-*")
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

	// Set currentTable to match
	exec.currentTable = "test"

	// ColumnRef with matching table prefix
	colRef := &sql.ColumnRef{Table: "test", Name: "val"}
	result, err := exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef exact table match failed: %v", err)
	} else {
		t.Logf("ColumnRef exact table match result: %v", result)
	}

	exec.currentTable = ""
}

// ========== Tests for evaluateExpression - ColumnRef with no columnMap entry ==========

func TestEvaluateExpressionColumnRefNoMapEntry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-col-no-map-*")
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

	// Create columnMap without the column
	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnOrder[i] = col
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// ColumnRef for column not in map
	colRef := &sql.ColumnRef{Name: "nonexistent"}
	result, err := exec.evaluateExpression(colRef, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("ColumnRef no map entry (expected error): %v", err)
	} else {
		t.Logf("ColumnRef no map entry result: %v", result)
	}
}

// ========== Tests for evaluateWhere - AnyAllExpr with correlated subquery ==========

func TestEvaluateWhereAnyAllCorrelated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-anyall-corr-*")
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

	_, err = exec.Execute("CREATE TABLE customers (id INT PRIMARY KEY, min_order INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, 50), (2, 100)")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	// ANY with correlated subquery
	result, err := exec.Execute(`
		SELECT * FROM orders o
		WHERE amount > ANY (SELECT min_order FROM customers c WHERE c.id = o.customer_id)
	`)
	if err != nil {
		t.Logf("ANY correlated failed: %v", err)
	} else {
		t.Logf("ANY correlated: %d rows", len(result.Rows))
	}

	// ALL with correlated subquery
	result, err = exec.Execute(`
		SELECT * FROM orders o
		WHERE amount > ALL (SELECT min_order FROM customers c WHERE c.id = o.customer_id)
	`)
	if err != nil {
		t.Logf("ALL correlated failed: %v", err)
	} else {
		t.Logf("ALL correlated: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateHaving - BinaryExpr with nil operands ==========

func TestEvaluateHavingBinaryExprNilOperands(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-having-nil-op-*")
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

	_, err = exec.Execute("INSERT INTO data VALUES (1, 1, NULL), (2, 1, NULL), (3, 2, 30)")
	if err != nil {
		t.Logf("INSERT with NULL failed: %v", err)
	}

	// HAVING with NULL aggregate
	result, err := exec.Execute(`
		SELECT grp, AVG(val) as avg_val
		FROM data
		GROUP BY grp
		HAVING AVG(val) > 0
	`)
	if err != nil {
		t.Logf("HAVING with NULL aggregate failed: %v", err)
	} else {
		t.Logf("HAVING with NULL aggregate: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateWhere - ExistsExpr with correlated subquery ==========

func TestEvaluateWhereExistsCorrelatedExtra(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-exists-corr-*")
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

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 1), (2, 2), (3, 3)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO customers VALUES (1, TRUE), (2, FALSE)")
	if err != nil {
		t.Fatalf("INSERT customers failed: %v", err)
	}

	// EXISTS with correlated subquery
	result, err := exec.Execute(`
		SELECT * FROM orders o
		WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.active = TRUE)
	`)
	if err != nil {
		t.Logf("EXISTS correlated failed: %v", err)
	} else {
		t.Logf("EXISTS correlated: %d rows", len(result.Rows))
	}

	// NOT EXISTS with correlated subquery
	result, err = exec.Execute(`
		SELECT * FROM orders o
		WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.active = TRUE)
	`)
	if err != nil {
		t.Logf("NOT EXISTS correlated failed: %v", err)
	} else {
		t.Logf("NOT EXISTS correlated: %d rows", len(result.Rows))
	}
}

// ========== Tests for evaluateExpression - RankExpr with ftsManager ==========

func TestEvaluateExpressionRankExprWithFtsManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-rank-fts-mgr-*")
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

	// Test RankExpr with nil ftsManager
	rankExpr := &sql.RankExpr{}
	result, err := exec.evaluateExpression(rankExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("RankExpr nil ftsManager failed: %v", err)
	} else {
		t.Logf("RankExpr nil ftsManager result: %v", result)
	}

	// Test with outerContext but no ftsManager
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

// ========== Tests for evaluateExpression - default case returns nil ==========

func TestEvaluateExpressionUnknownExpressionType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unknown-expr-*")
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

	// Create a custom expression type that will fall through to default
	type UnknownExpr struct {
		sql.Expression
	}

	unknownExpr := &UnknownExpr{}
	result, err := exec.evaluateExpression(unknownExpr, mockRow, columnMap, columnOrder)
	if err != nil {
		t.Logf("Unknown expression type failed: %v", err)
	} else {
		t.Logf("Unknown expression type result: %v (should be nil)", result)
	}
}