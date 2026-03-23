package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ========== Tests for evaluateWhereForRow - UnaryExpr with non-NOT operator ==========

func TestEvaluateWhereForRowUnaryNonNot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-unary-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewStringValue("test", types.TypeVarchar)},
	}

	// UnaryExpr with OpNeg (negative) - should fall through to default and return false
	unaryExpr := &sql.UnaryExpr{
		Op:    sql.OpNeg,
		Right: &sql.Literal{Value: true},
	}
	result, err := exec.evaluateWhereForRow(unaryExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("UnaryExpr OpNeg returned error: %v", err)
	}
	t.Logf("UnaryExpr OpNeg result: %v (should be false for non-NOT)", result)
}

// ========== Tests for evaluateWhereForRow - Literal with LiteralBool but non-bool value ==========

func TestEvaluateWhereForRowLiteralBoolInvalid(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-literal-bool-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// Literal with LiteralBool type but non-bool value
	literal := &sql.Literal{
		Type:  sql.LiteralBool,
		Value: "not-a-bool",
	}
	result, err := exec.evaluateWhereForRow(literal, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("Literal bool with invalid value returned error: %v", err)
	}
	t.Logf("Literal bool with invalid value result: %v (should be false)", result)

	// Literal with non-LiteralBool type - should fall through
	literal2 := &sql.Literal{
		Type:  sql.LiteralString,
		Value: "test",
	}
	result2, err := exec.evaluateWhereForRow(literal2, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("Literal non-bool returned error: %v", err)
	}
	t.Logf("Literal non-bool result: %v (should be false)", result2)
}

// ========== Tests for evaluateWhereForRow - InExpr with value list ==========

func TestEvaluateWhereForRowInExprValueList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-list-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// InExpr with value list - value matches
	inExprMatch := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		List: []sql.Expression{
			&sql.Literal{Value: 5},
			&sql.Literal{Value: 10},
			&sql.Literal{Value: 15},
		},
		Not: false,
	}
	result, err := exec.evaluateWhereForRow(inExprMatch, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("InExpr with matching list failed: %v", err)
	}
	if !result {
		t.Errorf("Expected InExpr to return true for matching value, got false")
	}
	t.Logf("InExpr with matching list: %v", result)

	// InExpr with value list - value does not match
	inExprNoMatch := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		List: []sql.Expression{
			&sql.Literal{Value: 20},
			&sql.Literal{Value: 30},
		},
		Not: false,
	}
	result2, err := exec.evaluateWhereForRow(inExprNoMatch, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("InExpr with non-matching list failed: %v", err)
	}
	if result2 {
		t.Errorf("Expected InExpr to return false for non-matching value, got true")
	}
	t.Logf("InExpr with non-matching list: %v", result2)

	// InExpr with NOT and matching value
	inExprNotMatch := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		List: []sql.Expression{
			&sql.Literal{Value: 10},
		},
		Not: true,
	}
	result3, err := exec.evaluateWhereForRow(inExprNotMatch, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("InExpr NOT with matching list failed: %v", err)
	}
	if result3 {
		t.Errorf("Expected NOT IN to return false for matching value, got true")
	}
	t.Logf("NOT IN with matching value: %v", result3)

	// InExpr with NOT and non-matching value
	inExprNotNoMatch := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		List: []sql.Expression{
			&sql.Literal{Value: 99},
		},
		Not: true,
	}
	result4, err := exec.evaluateWhereForRow(inExprNotNoMatch, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("InExpr NOT with non-matching list failed: %v", err)
	}
	if !result4 {
		t.Errorf("Expected NOT IN to return true for non-matching value, got false")
	}
	t.Logf("NOT IN with non-matching value: %v", result4)
}

// ========== Tests for evaluateWhereForRow - InExpr with error in list evaluation ==========

func TestEvaluateWhereForRowInExprListError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-list-err-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// InExpr with list that contains error-causing expression (will be skipped via continue)
	inExpr := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		List: []sql.Expression{
			&sql.ColumnRef{Name: "nonexistent_column"}, // This will cause error, should continue
			&sql.Literal{Value: 10},
		},
		Not: false,
	}
	result, err := exec.evaluateWhereForRow(inExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("InExpr with error in list: %v, result: %v", err, result)
	} else {
		t.Logf("InExpr with error in list: result=%v (should still find match)", result)
	}
}

// ========== Tests for evaluateWhereForRow - ScalarSubquery truthiness checks ==========

func TestEvaluateWhereForRowScalarSubqueryString(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-scalar-str-*")
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
	_, err = exec.Execute("CREATE TABLE outer_t (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_t failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE inner_t (str_val VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_t failed: %v", err)
	}

	// Insert string values
	_, err = exec.Execute("INSERT INTO inner_t VALUES ('hello'), (''), (NULL)")
	if err != nil {
		t.Fatalf("INSERT inner_t failed: %v", err)
	}

	tbl, _ := engine.GetTable("outer_t")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// ScalarSubquery returning non-empty string (truthy)
	scalarSubq := &sql.ScalarSubquery{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "str_val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "str_val"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: "hello"},
				},
			},
		},
	}
	result, err := exec.evaluateWhereForRow(scalarSubq, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ScalarSubquery with non-empty string failed: %v", err)
	} else {
		t.Logf("ScalarSubquery with non-empty string: %v (should be true)", result)
	}

	// ScalarSubquery returning empty string (falsy)
	scalarSubqEmpty := &sql.ScalarSubquery{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "str_val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "str_val"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: ""},
				},
			},
		},
	}
	result2, err := exec.evaluateWhereForRow(scalarSubqEmpty, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ScalarSubquery with empty string failed: %v", err)
	} else {
		t.Logf("ScalarSubquery with empty string: %v (should be false)", result2)
	}

	// ScalarSubquery returning NULL (falsy)
	scalarSubqNull := &sql.ScalarSubquery{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "str_val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "str_val"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: nil},
				},
			},
		},
	}
	result3, err := exec.evaluateWhereForRow(scalarSubqNull, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ScalarSubquery with NULL failed: %v", err)
	} else {
		t.Logf("ScalarSubquery with NULL: %v (should be false)", result3)
	}
}

// ========== Tests for evaluateWhereForRow - BinaryExpr with IN and ParenExpr containing subquery ==========

func TestEvaluateWhereForRowInParenSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-paren-*")
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
		t.Fatalf("CREATE TABLE test failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE lookup (lval INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE lookup failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO lookup VALUES (10), (20)")
	if err != nil {
		t.Fatalf("INSERT lookup failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// BinaryExpr with IN and ParenExpr containing subquery - matching value
	inParen := &sql.BinaryExpr{
		Left: &sql.ColumnRef{Name: "val"},
		Op:   sql.OpIn,
		Right: &sql.ParenExpr{
			Expr: &sql.SubqueryExpr{
				Select: &sql.SelectStmt{
					Columns: []sql.Expression{&sql.ColumnRef{Name: "lval"}},
					From: &sql.FromClause{
						Table: &sql.TableRef{Name: "lookup"},
					},
				},
			},
		},
	}
	result, err := exec.evaluateWhereForRow(inParen, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("IN with parenthesized subquery failed: %v", err)
	}
	if !result {
		t.Errorf("Expected IN with matching value to return true, got false")
	}
	t.Logf("IN with parenthesized subquery (matching): %v", result)

	// Non-matching value
	mockRow2 := &row.Row{
		ID:     2,
		Values: []types.Value{types.NewIntValue(2), types.NewIntValue(99)},
	}
	result2, err := exec.evaluateWhereForRow(inParen, mockRow2, columns, colIdxMap)
	if err != nil {
		t.Fatalf("IN with parenthesized subquery failed: %v", err)
	}
	if result2 {
		t.Errorf("Expected IN with non-matching value to return false, got true")
	}
	t.Logf("IN with parenthesized subquery (non-matching): %v", result2)
}

// ========== Tests for evaluateWhereForRow - IsNullExpr ==========

func TestEvaluateWhereForRowIsNullExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-isnull-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	// Row with NULL value
	mockRowNull := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewNullValue()},
	}

	// Row with non-NULL value
	mockRowVal := &row.Row{
		ID:     2,
		Values: []types.Value{types.NewIntValue(2), types.NewIntValue(10)},
	}

	// IS NULL - should be true for NULL
	isNull := &sql.IsNullExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		Not:  false,
	}
	result, err := exec.evaluateWhereForRow(isNull, mockRowNull, columns, colIdxMap)
	if err != nil {
		t.Fatalf("IS NULL check failed: %v", err)
	}
	if !result {
		t.Errorf("Expected IS NULL to return true for NULL value, got false")
	}
	t.Logf("IS NULL (for NULL): %v", result)

	// IS NULL - should be false for non-NULL
	result2, err := exec.evaluateWhereForRow(isNull, mockRowVal, columns, colIdxMap)
	if err != nil {
		t.Fatalf("IS NULL check failed: %v", err)
	}
	if result2 {
		t.Errorf("Expected IS NULL to return false for non-NULL value, got true")
	}
	t.Logf("IS NULL (for non-NULL): %v", result2)

	// IS NOT NULL - should be false for NULL
	isNotNull := &sql.IsNullExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		Not:  true,
	}
	result3, err := exec.evaluateWhereForRow(isNotNull, mockRowNull, columns, colIdxMap)
	if err != nil {
		t.Fatalf("IS NOT NULL check failed: %v", err)
	}
	if result3 {
		t.Errorf("Expected IS NOT NULL to return false for NULL value, got true")
	}
	t.Logf("IS NOT NULL (for NULL): %v", result3)

	// IS NOT NULL - should be true for non-NULL
	result4, err := exec.evaluateWhereForRow(isNotNull, mockRowVal, columns, colIdxMap)
	if err != nil {
		t.Fatalf("IS NOT NULL check failed: %v", err)
	}
	if !result4 {
		t.Errorf("Expected IS NOT NULL to return true for non-NULL value, got false")
	}
	t.Logf("IS NOT NULL (for non-NULL): %v", result4)
}

// ========== Tests for evaluateWhereForRow - ExistsExpr ==========

func TestEvaluateWhereForRowExistsExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-exists-*")
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

	_, err = exec.Execute("CREATE TABLE outer_t (id INT PRIMARY KEY, ref_val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_t failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE inner_t (val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_t failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO inner_t VALUES (10), (20)")
	if err != nil {
		t.Fatalf("INSERT inner_t failed: %v", err)
	}

	tbl, _ := engine.GetTable("outer_t")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// Set currentTable for correlated subquery
	exec.currentTable = "outer_t"

	// EXISTS with matching rows
	existsExpr := &sql.ExistsExpr{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.Literal{Value: 1}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "val"},
					Op:    sql.OpEq,
					Right: &sql.ColumnRef{Name: "ref_val"},
				},
			},
		},
	}
	result, err := exec.evaluateWhereForRow(existsExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("EXISTS with matching rows failed: %v", err)
	} else {
		t.Logf("EXISTS with matching rows: %v", result)
	}

	// EXISTS with empty result
	existsEmpty := &sql.ExistsExpr{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.Literal{Value: 1}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "val"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: 9999}, // No match
				},
			},
		},
	}
	result2, err := exec.evaluateWhereForRow(existsEmpty, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("EXISTS with empty result failed: %v", err)
	} else {
		t.Logf("EXISTS with empty result: %v (should be false)", result2)
	}
}

// ========== Tests for evaluateWhereForRow - AnyAllExpr ==========

func TestEvaluateWhereForRowAnyAllExpr(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE outer_t (id INT PRIMARY KEY, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_t failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE inner_t (ival INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_t failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO inner_t VALUES (10), (20), (30)")
	if err != nil {
		t.Fatalf("INSERT inner_t failed: %v", err)
	}

	tbl, _ := engine.GetTable("outer_t")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(15)},
	}

	exec.currentTable = "outer_t"

	// ANY with matching values (15 > 10)
	anyExpr := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpGt,
		IsAny: true,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "ival"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
			},
		},
	}
	result, err := exec.evaluateWhereForRow(anyExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("ANY expression failed: %v", err)
	}
	if !result {
		t.Errorf("Expected ANY to return true for val=15 > some values, got false")
	}
	t.Logf("ANY (15 > ANY(10,20,30)): %v", result)

	// ANY with no matching values (15 > 50 is false for all)
	anyExprNoMatch := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpGt,
		IsAny: true,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "ival"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "ival"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: 50},
				},
			},
		},
	}
	result2, err := exec.evaluateWhereForRow(anyExprNoMatch, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("ANY expression with no match failed: %v", err)
	}
	if result2 {
		t.Errorf("Expected ANY to return false when no match, got true")
	}
	t.Logf("ANY (15 > ANY(>50) - empty): %v", result2)

	// ALL with all matching (15 < all of 20, 30)
	allExpr := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpLt,
		IsAny: false,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "ival"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "ival"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: 15},
				},
			},
		},
	}
	result3, err := exec.evaluateWhereForRow(allExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("ALL expression failed: %v", err)
	}
	if !result3 {
		t.Errorf("Expected ALL to return true when all match, got false")
	}
	t.Logf("ALL (15 < ALL(20,30)): %v", result3)

	// ALL with not all matching (15 < all of 10, 20, 30 is false)
	allExprNoMatch := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpLt,
		IsAny: false,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "ival"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
			},
		},
	}
	result4, err := exec.evaluateWhereForRow(allExprNoMatch, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("ALL expression with not all match failed: %v", err)
	}
	if result4 {
		t.Errorf("Expected ALL to return false when not all match, got true")
	}
	t.Logf("ALL (15 < ALL(10,20,30)): %v", result4)

	// ALL on empty set (should return true)
	allExprEmpty := &sql.AnyAllExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpGt,
		IsAny: false,
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "ival"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "ival"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: 100},
				},
			},
		},
	}
	result5, err := exec.evaluateWhereForRow(allExprEmpty, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("ALL expression on empty set failed: %v", err)
	}
	if !result5 {
		t.Errorf("Expected ALL on empty set to return true, got false")
	}
	t.Logf("ALL (empty set): %v", result5)
}

// ========== Tests for evaluateExprForRow - ColumnRef with outer context ==========

func TestEvaluateExprForRowColumnRefOuterContext(t *testing.T) {
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// Set outer context
	exec.outerContext = map[string]interface{}{
		"outer_col": 42,
	}

	// ColumnRef for non-existent column - should check outer context
	colRef := &sql.ColumnRef{Name: "outer_col"}
	result, err := exec.evaluateExprForRow(colRef, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ColumnRef with outer context failed: %v", err)
	} else {
		t.Logf("ColumnRef with outer context: %v (expected 42)", result)
	}

	// ColumnRef with table prefix that doesn't match current table
	exec.currentTable = "test"
	colRefOther := &sql.ColumnRef{Name: "other_col", Table: "other_table"}
	exec.outerContext = map[string]interface{}{
		"other_table.other_col": 99,
	}
	result2, err := exec.evaluateExprForRow(colRefOther, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ColumnRef with different table failed: %v", err)
	} else {
		t.Logf("ColumnRef with different table: %v (expected 99)", result2)
	}
}

// ========== Tests for evaluateExprForRow - ScalarSubquery ==========

func TestEvaluateExprForRowScalarSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-scalar-*")
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

	_, err = exec.Execute("CREATE TABLE outer_t (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_t failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE inner_t (val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_t failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO inner_t VALUES (42)")
	if err != nil {
		t.Fatalf("INSERT inner_t failed: %v", err)
	}

	tbl, _ := engine.GetTable("outer_t")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// ScalarSubquery returning single value
	scalarSubq := &sql.ScalarSubquery{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
			},
		},
	}
	result, err := exec.evaluateExprForRow(scalarSubq, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("ScalarSubquery failed: %v", err)
	}
	t.Logf("ScalarSubquery result: %v (expected 42)", result)

	// ScalarSubquery returning empty result
	scalarSubqEmpty := &sql.ScalarSubquery{
		Subquery: &sql.SubqueryExpr{
			Select: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
				From: &sql.FromClause{
					Table: &sql.TableRef{Name: "inner_t"},
				},
				Where: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "val"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: 100},
				},
			},
		},
	}
	result2, err := exec.evaluateExprForRow(scalarSubqEmpty, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("ScalarSubquery empty result failed: %v", err)
	} else {
		t.Logf("ScalarSubquery empty result: %v (expected nil)", result2)
	}
}

// ========== Tests for evaluateExprForRow - SubqueryExpr ==========

func TestEvaluateExprForRowSubqueryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-subq-*")
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

	_, err = exec.Execute("CREATE TABLE outer_t (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE outer_t failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE inner_t (val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE inner_t failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO inner_t VALUES (55)")
	if err != nil {
		t.Fatalf("INSERT inner_t failed: %v", err)
	}

	tbl, _ := engine.GetTable("outer_t")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// SubqueryExpr (treated as scalar subquery)
	subqExpr := &sql.SubqueryExpr{
		Select: &sql.SelectStmt{
			Columns: []sql.Expression{&sql.ColumnRef{Name: "val"}},
			From: &sql.FromClause{
				Table: &sql.TableRef{Name: "inner_t"},
			},
		},
	}
	result, err := exec.evaluateExprForRow(subqExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("SubqueryExpr failed: %v", err)
	}
	t.Logf("SubqueryExpr result: %v (expected 55)", result)
}

// ========== Tests for evaluateExprForRow - BinaryExpr ==========

func TestEvaluateExprForRowBinaryExpr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-binary-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(5), types.NewIntValue(3)},
	}

	// BinaryExpr with addition
	binExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "a"},
		Op:    sql.OpAdd,
		Right: &sql.ColumnRef{Name: "b"},
	}
	result, err := exec.evaluateExprForRow(binExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("BinaryExpr failed: %v", err)
	}
	t.Logf("BinaryExpr a+b: %v (expected 8)", result)
}

// ========== Tests for evaluateHaving - InExpr with value list ==========

func TestEvaluateHavingInExprListExtra(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Execute a GROUP BY with HAVING IN
	result, err := exec.Execute("SELECT grp, SUM(val) FROM test GROUP BY grp HAVING grp IN (1, 2, 3)")
	if err != nil {
		t.Logf("GROUP BY HAVING IN query failed: %v", err)
	} else {
		t.Logf("GROUP BY HAVING IN result: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - UnaryExpr NOT ==========

func TestEvaluateHavingUnaryNotExtra(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (1, 1, 10), (2, 2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Execute a GROUP BY with HAVING NOT
	result, err := exec.Execute("SELECT grp, SUM(val) FROM test GROUP BY grp HAVING NOT grp = 1")
	if err != nil {
		t.Logf("GROUP BY HAVING NOT query failed: %v", err)
	} else {
		t.Logf("GROUP BY HAVING NOT result: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - ExistsExpr ==========

func TestEvaluateHavingExists(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Execute a GROUP BY with HAVING EXISTS
	result, err := exec.Execute("SELECT grp FROM test GROUP BY grp HAVING EXISTS (SELECT 1)")
	if err != nil {
		t.Logf("GROUP BY HAVING EXISTS query failed: %v", err)
	} else {
		t.Logf("GROUP BY HAVING EXISTS result: %v", result.Rows)
	}
}

// ========== Tests for evaluateHaving - ScalarSubquery ==========

func TestEvaluateHavingScalarSubquery(t *testing.T) {
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, grp INT, val INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO test VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Execute a GROUP BY with HAVING scalar subquery
	result, err := exec.Execute("SELECT grp FROM test GROUP BY grp HAVING SUM(val) > (SELECT 15)")
	if err != nil {
		t.Logf("GROUP BY HAVING scalar subquery failed: %v", err)
	} else {
		t.Logf("GROUP BY HAVING scalar subquery result: %v", result.Rows)
	}
}

// ========== Tests for BinaryExpr IN without subquery ==========

func TestEvaluateWhereForRowInNoSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-nosub-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// BinaryExpr with IN but not a subquery - should fall through
	inExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "val"},
		Op:    sql.OpIn,
		Right: &sql.Literal{Value: "not-a-subquery"},
	}
	result, err := exec.evaluateWhereForRow(inExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("IN without subquery returned error: %v", err)
	}
	t.Logf("IN without subquery result: %v (should be false)", result)
}

// ========== Tests for Literal bool true ==========

func TestEvaluateWhereForRowLiteralBoolTrue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-literal-true-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// Literal bool true
	literalTrue := &sql.Literal{
		Type:  sql.LiteralBool,
		Value: true,
	}
	result, err := exec.evaluateWhereForRow(literalTrue, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("Literal bool true failed: %v", err)
	}
	if !result {
		t.Errorf("Expected Literal bool true to return true, got false")
	}
	t.Logf("Literal bool true: %v", result)

	// Literal bool false
	literalFalse := &sql.Literal{
		Type:  sql.LiteralBool,
		Value: false,
	}
	result2, err := exec.evaluateWhereForRow(literalFalse, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("Literal bool false failed: %v", err)
	}
	if result2 {
		t.Errorf("Expected Literal bool false to return false, got true")
	}
	t.Logf("Literal bool false: %v", result2)
}

// ========== Tests for BinaryExpr comparison ==========

func TestEvaluateWhereForRowBinaryComparison(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-binary-cmp-*")
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

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10), types.NewIntValue(5)},
	}

	// BinaryExpr with comparison - column names are lowercase in the map
	binExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "a"},
		Op:    sql.OpGt,
		Right: &sql.ColumnRef{Name: "b"},
	}
	result, err := exec.evaluateWhereForRow(binExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("BinaryExpr comparison error: %v", err)
	}
	t.Logf("BinaryExpr a > b: %v", result)
}

// ========== Tests for InExpr with subquery ==========

func TestEvaluateWhereForRowInExprSubquery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-in-subq-*")
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
		t.Fatalf("CREATE TABLE test failed: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE lookup (lval INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE lookup failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO lookup VALUES (10), (20)")
	if err != nil {
		t.Fatalf("INSERT lookup failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewIntValue(10)},
	}

	// InExpr with subquery - matching
	inExprSubq := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		Select: &sql.SelectStmt{
			Columns: []sql.Expression{&sql.ColumnRef{Name: "lval"}},
			From: &sql.FromClause{
				Table: &sql.TableRef{Name: "lookup"},
			},
		},
		Not: false,
	}
	result, err := exec.evaluateWhereForRow(inExprSubq, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("InExpr with subquery failed: %v", err)
	}
	if !result {
		t.Errorf("Expected IN with matching subquery to return true, got false")
	}
	t.Logf("InExpr with subquery (matching): %v", result)

	// InExpr with subquery - NOT matching
	inExprNotSubq := &sql.InExpr{
		Expr: &sql.ColumnRef{Name: "val"},
		Select: &sql.SelectStmt{
			Columns: []sql.Expression{&sql.ColumnRef{Name: "lval"}},
			From: &sql.FromClause{
				Table: &sql.TableRef{Name: "lookup"},
			},
		},
		Not: true,
	}
	result2, err := exec.evaluateWhereForRow(inExprNotSubq, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("InExpr NOT with subquery failed: %v", err)
	}
	if result2 {
		t.Errorf("Expected NOT IN with matching subquery to return false, got true")
	}
	t.Logf("InExpr NOT with subquery (matching): %v", result2)
}

// ========== Tests for default case (unknown expression type) ==========

func TestEvaluateWhereForRowDefaultCase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-where-default-*")
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

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1)},
	}

	// Unknown expression type - should fall through to default case
	type UnknownExpr struct {
		sql.Expression
	}
	unknownExpr := &UnknownExpr{}
	result, err := exec.evaluateWhereForRow(unknownExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("Unknown expression type returned error: %v", err)
	}
	if result {
		t.Errorf("Expected unknown expression type to return false, got true")
	}
	t.Logf("Unknown expression type: %v (should be false)", result)
}

// ========== Tests for FunctionCall in evaluateExprForRow ==========

func TestEvaluateExprForRowFunctionCall(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-expr-func-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewStringValue("hello", types.TypeVarchar)},
	}

	// FunctionCall - UPPER
	funcCall := &sql.FunctionCall{
		Name: "UPPER",
		Args: []sql.Expression{&sql.ColumnRef{Name: "name"}},
	}
	result, err := exec.evaluateExprForRow(funcCall, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("FunctionCall failed: %v", err)
	} else {
		t.Logf("FunctionCall UPPER result: %v (expected HELLO)", result)
	}
}

// ========== Tests for BinaryExpr with LIKE ==========

func TestEvaluateWhereForRowLike(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-like-*")
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

	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	tbl, _ := engine.GetTable("test")
	tblInfo := tbl.GetInfo()

	columns := tblInfo.Columns
	colIdxMap := make(map[string]int)
	for i, col := range columns {
		colIdxMap[col.Name] = i
	}

	mockRow := &row.Row{
		ID:     1,
		Values: []types.Value{types.NewIntValue(1), types.NewStringValue("hello world", types.TypeVarchar)},
	}

	// BinaryExpr with LIKE
	likeExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "name"},
		Op:    sql.OpLike,
		Right: &sql.Literal{Value: "hello%"},
	}
	result, err := exec.evaluateWhereForRow(likeExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Fatalf("LIKE expression failed: %v", err)
	}
	if !result {
		t.Errorf("Expected LIKE 'hello%%' to match 'hello world', got false")
	}
	t.Logf("LIKE 'hello%%': %v", result)

	// NOT LIKE
	notLikeExpr := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "name"},
		Op:    sql.OpNotLike,
		Right: &sql.Literal{Value: "goodbye%"},
	}
	result2, err := exec.evaluateWhereForRow(notLikeExpr, mockRow, columns, colIdxMap)
	if err != nil {
		t.Logf("NOT LIKE expression error: %v", err)
	}
	t.Logf("NOT LIKE 'goodbye%%': %v", result2)
}