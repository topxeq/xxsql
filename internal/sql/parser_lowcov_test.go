package sql

import (
	"strings"
	"testing"
)

func TestParserParseWithDirectCall(t *testing.T) {
	p := NewParser("WITH cte AS (SELECT 1) SELECT * FROM cte")

	stmt := p.parseWith()
	if stmt == nil {
		t.Fatal("parseWith returned nil")
	}

	withStmt, ok := stmt.(*WithStmt)
	if !ok {
		t.Fatalf("expected *WithStmt, got %T", stmt)
	}
	if len(withStmt.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(withStmt.CTEs))
	}
	if withStmt.CTEs[0].Name != "cte" {
		t.Fatalf("expected CTE name 'cte', got %q", withStmt.CTEs[0].Name)
	}
	if withStmt.MainQuery == nil {
		t.Fatal("expected non-nil main query")
	}
}

func TestParserCreateProcedureUnquotedBody(t *testing.T) {
	stmt, err := Parse("CREATE PROCEDURE p(INOUT arg1 INT, OUT arg2 TEXT) AS BEGIN RETURN 1; END")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	cp, ok := stmt.(*CreateProcedureStmt)
	if !ok {
		t.Fatalf("expected *CreateProcedureStmt, got %T", stmt)
	}

	if len(cp.Params) != 2 {
		t.Fatalf("expected 2 params, got %d", len(cp.Params))
	}
	if cp.Params[0].Mode != ParamModeInout {
		t.Fatalf("first param mode = %v, want INOUT", cp.Params[0].Mode)
	}
	if cp.Params[1].Mode != ParamModeOut {
		t.Fatalf("second param mode = %v, want OUT", cp.Params[1].Mode)
	}
	if cp.Body == "" {
		t.Fatal("expected non-empty procedure body")
	}
	if !strings.Contains(strings.ToUpper(cp.Body), "BEGIN") {
		t.Fatalf("expected body to contain BEGIN, got %q", cp.Body)
	}
}

func TestParserRankExprWithoutParentheses(t *testing.T) {
	stmt, err := Parse("SELECT RANK FROM t")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 select column, got %d", len(sel.Columns))
	}

	rankExpr, ok := sel.Columns[0].(*RankExpr)
	if !ok {
		t.Fatalf("expected *RankExpr, got %T", sel.Columns[0])
	}
	if rankExpr.IndexName != "" {
		t.Fatalf("expected empty index name, got %q", rankExpr.IndexName)
	}
}

func TestParserErrorAccessor(t *testing.T) {
	p := NewParser("SELECT FROM")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected parse error")
	}
	if p.Error() == nil {
		t.Fatal("Parser.Error should expose parse error")
	}
}
