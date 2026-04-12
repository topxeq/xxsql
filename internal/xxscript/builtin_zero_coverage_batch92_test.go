package xxscript

import (
	"errors"
	"strings"
	"testing"
)

type batch92UnknownStmt struct{ BaseNode }

func (s *batch92UnknownStmt) node()     {}
func (s *batch92UnknownStmt) stmtNode() {}
func (s *batch92UnknownStmt) String() string {
	return "batch92UnknownStmt"
}

func TestBuiltin_ZeroCoverage_Batch92_ExecuteStmtGuardBranches(t *testing.T) {
	t.Run("max_steps_exceeded", func(t *testing.T) {
		ctx := NewContext()
		ctx.MaxSteps = 0
		i := NewInterpreter(ctx)

		_, err := i.executeStmt(&ExprStmt{Expr: &NumberExpr{Value: 1}})
		if err == nil || !strings.Contains(err.Error(), "maximum steps") {
			t.Fatalf("expected max-steps error, got %v", err)
		}
	})

	t.Run("unknown_statement_type", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeStmt(&batch92UnknownStmt{})
		if err == nil || !strings.Contains(err.Error(), "unknown statement type") {
			t.Fatalf("expected unknown-statement error, got %v", err)
		}
	})
}

func TestBuiltin_ZeroCoverage_Batch92_ExecuteVarStmtBranches(t *testing.T) {
	t.Run("single_name_nil_value", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		got, err := i.executeVarStmt(&VarStmt{Name: "x"})
		if err != nil {
			t.Fatalf("executeVarStmt unexpected error: %v", err)
		}
		if got != nil || i.ctx.Variables["x"] != nil {
			t.Fatalf("expected nil assignment, got result=%#v var=%#v", got, i.ctx.Variables["x"])
		}
	})

	t.Run("value_evaluation_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeVarStmt(&VarStmt{Name: "x", Value: &IdentExpr{Name: "missing"}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected undefined-variable error, got %v", err)
		}
	})

	t.Run("array_destructuring_short_array", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeVarStmt(&VarStmt{
			Names: []string{"a", "b", "c"},
			Value: &ArrayExpr{Elements: []Expression{&NumberExpr{Value: 1}, &NumberExpr{Value: 2}}},
		})
		if err != nil {
			t.Fatalf("array destructuring error: %v", err)
		}
		if !valuesEqual(i.ctx.Variables["a"], float64(1)) || !valuesEqual(i.ctx.Variables["b"], float64(2)) || i.ctx.Variables["c"] != nil {
			t.Fatalf("array destructuring mismatch: a=%#v b=%#v c=%#v", i.ctx.Variables["a"], i.ctx.Variables["b"], i.ctx.Variables["c"])
		}
	})

	t.Run("map_destructuring_missing_key", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeVarStmt(&VarStmt{
			Names: []string{"a", "b"},
			Value: &MapExpr{Pairs: map[string]Expression{"a": &StringExpr{Value: "ok"}}},
		})
		if err != nil {
			t.Fatalf("map destructuring error: %v", err)
		}
		if !valuesEqual(i.ctx.Variables["a"], "ok") || i.ctx.Variables["b"] != nil {
			t.Fatalf("map destructuring mismatch: a=%#v b=%#v", i.ctx.Variables["a"], i.ctx.Variables["b"])
		}
	})

	t.Run("single_value_multi_names", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeVarStmt(&VarStmt{
			Names: []string{"a", "b"},
			Value: &NumberExpr{Value: 9},
		})
		if err != nil {
			t.Fatalf("single-value destructuring error: %v", err)
		}
		if !valuesEqual(i.ctx.Variables["a"], float64(9)) || i.ctx.Variables["b"] != nil {
			t.Fatalf("single-value destructuring mismatch: a=%#v b=%#v", i.ctx.Variables["a"], i.ctx.Variables["b"])
		}
	})
}

func TestBuiltin_ZeroCoverage_Batch92_ExecuteForTryThrowSwitch(t *testing.T) {
	t.Run("for_stmt_init_and_update_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		_, err := i.executeForStmt(&ForStmt{
			Init: &ExprStmt{Expr: &IdentExpr{Name: "missing"}},
			Body: &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected init error, got %v", err)
		}

		i = NewInterpreter(NewContext())
		i.ctx.Variables["i"] = int64(0)
		_, err = i.executeForStmt(&ForStmt{
			Condition: &BinaryExpr{Left: &IdentExpr{Name: "i"}, Op: TokLt, Right: &NumberExpr{Value: 1}},
			Update:    &ExprStmt{Expr: &IdentExpr{Name: "missing"}},
			Body:      &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected update error, got %v", err)
		}
	})

	t.Run("for_stmt_continue_and_break", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["i"] = int64(0)

		_, err := i.executeForStmt(&ForStmt{
			Condition: &BinaryExpr{Left: &IdentExpr{Name: "i"}, Op: TokLt, Right: &NumberExpr{Value: 3}},
			Update:    &ExprStmt{Expr: &PostIncDecExpr{Expr: &IdentExpr{Name: "i"}, Op: TokInc}},
			Body: &BlockStmt{Statements: []Statement{
				&ContinueStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("for continue branch error: %v", err)
		}
		if i.ctx.continueing || i.toInt(i.ctx.Variables["i"]) != 3 {
			t.Fatalf("for continue reset/increment mismatch: continue=%v i=%#v", i.ctx.continueing, i.ctx.Variables["i"])
		}

		i = NewInterpreter(NewContext())
		i.ctx.Variables["i"] = int64(0)
		_, err = i.executeForStmt(&ForStmt{
			Condition: &BinaryExpr{Left: &IdentExpr{Name: "i"}, Op: TokLt, Right: &NumberExpr{Value: 5}},
			Update:    &ExprStmt{Expr: &PostIncDecExpr{Expr: &IdentExpr{Name: "i"}, Op: TokInc}},
			Body: &BlockStmt{Statements: []Statement{
				&BreakStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("for break branch error: %v", err)
		}
		if i.ctx.breaking {
			t.Fatalf("for break flag should be cleared")
		}
	})

	t.Run("try_stmt_throw_and_propagation", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		got, err := i.executeTryStmt(&TryStmt{
			TryBlock: &BlockStmt{Statements: []Statement{
				&ThrowStmt{Error: &StringExpr{Value: "boom"}},
			}},
			CatchVar: "e",
			CatchBlock: &BlockStmt{Statements: []Statement{
				&ReturnStmt{Value: &IdentExpr{Name: "e"}},
			}},
		})
		if err != nil {
			t.Fatalf("try-catch branch error: %v", err)
		}
		if !valuesEqual(got, "boom") || !valuesEqual(i.ctx.Variables["e"], "boom") {
			t.Fatalf("try-catch mismatch: got=%#v e=%#v", got, i.ctx.Variables["e"])
		}

		i = NewInterpreter(NewContext())
		_, err = i.executeTryStmt(&TryStmt{
			TryBlock: &BlockStmt{Statements: []Statement{
				&ThrowStmt{Error: &StringExpr{Value: "boom"}},
			}},
		})
		var throwErr *ThrowError
		if !errors.As(err, &throwErr) {
			t.Fatalf("expected ThrowError propagation, got %v", err)
		}

		i = NewInterpreter(NewContext())
		_, err = i.executeTryStmt(&TryStmt{
			TryBlock: &BlockStmt{Statements: []Statement{
				&ExprStmt{Expr: &IdentExpr{Name: "missing"}},
			}},
			CatchBlock: &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected non-throw propagation, got %v", err)
		}
	})

	t.Run("throw_stmt_nil_and_eval_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeThrowStmt(&ThrowStmt{})
		var throwErr *ThrowError
		if !errors.As(err, &throwErr) || throwErr.Value != nil {
			t.Fatalf("expected nil ThrowError value, got %v", err)
		}

		i = NewInterpreter(NewContext())
		_, err = i.executeThrowStmt(&ThrowStmt{Error: &IdentExpr{Name: "missing"}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected throw eval error, got %v", err)
		}
	})

	t.Run("switch_stmt_branches", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		_, err := i.executeSwitchStmt(&SwitchStmt{Value: &IdentExpr{Name: "missing"}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected switch value error, got %v", err)
		}

		i.ctx.Variables["x"] = int64(2)
		got, err := i.executeSwitchStmt(&SwitchStmt{
			Value: &IdentExpr{Name: "x"},
			Cases: []*SwitchCase{
				{Values: []Expression{&NumberExpr{Value: 1}}, Body: &BlockStmt{Statements: []Statement{&ExprStmt{Expr: &StringExpr{Value: "one"}}}}},
				{Values: []Expression{&NumberExpr{Value: 2}}, Body: &BlockStmt{Statements: []Statement{&ExprStmt{Expr: &StringExpr{Value: "two"}}}}},
			},
		})
		if err != nil || !valuesEqual(got, "two") {
			t.Fatalf("expected matching switch case, got result=%#v err=%v", got, err)
		}

		got, err = i.executeSwitchStmt(&SwitchStmt{
			Value: &IdentExpr{Name: "x"},
			Cases: []*SwitchCase{
				{Values: []Expression{&NumberExpr{Value: 9}}, Body: &BlockStmt{Statements: []Statement{&ExprStmt{Expr: &StringExpr{Value: "no"}}}}},
				{Values: nil, Body: &BlockStmt{Statements: []Statement{&ExprStmt{Expr: &StringExpr{Value: "default"}}}}},
			},
		})
		if err != nil || !valuesEqual(got, "default") {
			t.Fatalf("expected default switch case, got result=%#v err=%v", got, err)
		}

		got, err = i.executeSwitchStmt(&SwitchStmt{
			Value: &IdentExpr{Name: "x"},
			Cases: []*SwitchCase{
				{Values: []Expression{&NumberExpr{Value: 9}}, Body: &BlockStmt{}},
			},
		})
		if err != nil || got != nil {
			t.Fatalf("expected no-match nil result, got result=%#v err=%v", got, err)
		}

		_, err = i.executeSwitchStmt(&SwitchStmt{
			Value: &IdentExpr{Name: "x"},
			Cases: []*SwitchCase{
				{Values: []Expression{&IdentExpr{Name: "missing"}}, Body: &BlockStmt{}},
			},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected switch case eval error, got %v", err)
		}
	})
}
