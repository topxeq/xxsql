package sql

import (
	"testing"
)

func TestRankParsing(t *testing.T) {
	// Test parsing RANK as a window function
	parser := NewParser("SELECT id, name, salary, RANK() OVER (ORDER BY salary DESC) AS rank FROM employees")
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	t.Logf("SELECT columns: %d", len(selectStmt.Columns))
	for i, col := range selectStmt.Columns {
		t.Logf("Column %d: %T - %s", i, col, col.String())
		if wf, ok := col.(*WindowFuncCall); ok {
			t.Logf("  WindowFuncCall: Func=%s, Alias=%s", wf.Func.Name, wf.Alias)
		}
	}
}