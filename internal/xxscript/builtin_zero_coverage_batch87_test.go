package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch87_EvalArrayAndMapBranches(t *testing.T) {
	t.Run("evalArray_spread_and_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["arr"] = []Value{int64(1), int64(2)}
		i.ctx.Variables["s"] = "ab"
		i.ctx.Variables["x"] = 7

		arrExpr := &ArrayExpr{Elements: []Expression{
			&NumberExpr{Value: 0},
			&SpreadExpr{Expr: &IdentExpr{Name: "arr"}},
			&SpreadExpr{Expr: &IdentExpr{Name: "s"}},
		}}
		got, err := i.evalArray(arrExpr)
		if err != nil {
			t.Fatalf("evalArray unexpected error: %v", err)
		}
		want := []Value{float64(0), int64(1), int64(2), "a", "b"}
		if !valuesEqual(got, want) {
			t.Fatalf("evalArray spread mismatch: got=%#v want=%#v", got, want)
		}

		_, err = i.evalArray(&ArrayExpr{Elements: []Expression{
			&SpreadExpr{Expr: &IdentExpr{Name: "x"}},
		}})
		if err == nil || !strings.Contains(err.Error(), "cannot spread type") {
			t.Fatalf("expected cannot-spread error, got %v", err)
		}

		_, err = i.evalArray(&ArrayExpr{Elements: []Expression{
			&IdentExpr{Name: "missing"},
		}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected undefined-variable error, got %v", err)
		}
	})

	t.Run("evalMap_success_and_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["x"] = "ok"

		got, err := i.evalMap(&MapExpr{Pairs: map[string]Expression{
			"a": &NumberExpr{Value: 1},
			"b": &IdentExpr{Name: "x"},
		}})
		if err != nil {
			t.Fatalf("evalMap unexpected error: %v", err)
		}
		m, ok := got.(map[string]Value)
		if !ok {
			t.Fatalf("evalMap expected map result, got %#v", got)
		}
		if !valuesEqual(m["a"], float64(1)) || !valuesEqual(m["b"], "ok") {
			t.Fatalf("evalMap values mismatch: got=%#v", m)
		}

		_, err = i.evalMap(&MapExpr{Pairs: map[string]Expression{
			"bad": &IdentExpr{Name: "missing"},
		}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected evalMap undefined-variable error, got %v", err)
		}
	})
}

func TestBuiltin_ZeroCoverage_Batch87_ExecuteWhileStmtBranches(t *testing.T) {
	t.Run("condition_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeWhileStmt(&WhileStmt{
			Condition: &IdentExpr{Name: "missing"},
			Body:      &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected while condition error, got %v", err)
		}
	})

	t.Run("body_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["cond"] = true
		_, err := i.executeWhileStmt(&WhileStmt{
			Condition: &IdentExpr{Name: "cond"},
			Body: &BlockStmt{Statements: []Statement{
				&ExprStmt{Expr: &IdentExpr{Name: "missing"}},
			}},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected while body error, got %v", err)
		}
	})

	t.Run("break_branch_clears_flag", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["cond"] = true
		_, err := i.executeWhileStmt(&WhileStmt{
			Condition: &IdentExpr{Name: "cond"},
			Body: &BlockStmt{Statements: []Statement{
				&BreakStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("while break branch error: %v", err)
		}
		if i.ctx.breaking {
			t.Fatalf("while break flag should be cleared")
		}
	})

	t.Run("continue_branch_resets_flag", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["n"] = int64(0)

		_, err := i.executeWhileStmt(&WhileStmt{
			Condition: &BinaryExpr{
				Left:  &IdentExpr{Name: "n"},
				Op:    TokLt,
				Right: &NumberExpr{Value: 2},
			},
			Body: &BlockStmt{Statements: []Statement{
				&ExprStmt{Expr: &PostIncDecExpr{Expr: &IdentExpr{Name: "n"}, Op: TokInc}},
				&ContinueStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("while continue branch error: %v", err)
		}
		if i.toInt(i.ctx.Variables["n"]) != 2 {
			t.Fatalf("expected n to reach 2, got %#v", i.ctx.Variables["n"])
		}
		if i.ctx.continueing {
			t.Fatalf("while continue flag should be reset")
		}
	})
}

func TestBuiltin_ZeroCoverage_Batch87_ExecuteForInStmtBranches(t *testing.T) {
	t.Run("iterable_eval_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "k",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "missing"},
			Body:     &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected for-in iterable error, got %v", err)
		}
	})

	t.Run("unsupported_iterable_type", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = 123
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "k",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "it"},
			Body:     &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "cannot iterate") {
			t.Fatalf("expected cannot-iterate error, got %v", err)
		}
	})

	t.Run("array_branch_break_and_assignment", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = []Value{"a", "b"}
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "k",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "it"},
			Body: &BlockStmt{Statements: []Statement{
				&BreakStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("for-in array branch error: %v", err)
		}
		if !valuesEqual(i.ctx.Variables["k"], int64(0)) || !valuesEqual(i.ctx.Variables["v"], "a") {
			t.Fatalf("for-in array assignment mismatch: k=%#v v=%#v", i.ctx.Variables["k"], i.ctx.Variables["v"])
		}
		if i.ctx.breaking {
			t.Fatalf("for-in break flag should be cleared")
		}
	})

	t.Run("map_branch_skip_key_with_underscore", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = map[string]Value{"x": int64(1)}
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "_",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "it"},
			Body:     &BlockStmt{},
		})
		if err != nil {
			t.Fatalf("for-in map branch error: %v", err)
		}
		if _, ok := i.ctx.Variables["_"]; ok {
			t.Fatalf("underscore key should not be assigned")
		}
		if !valuesEqual(i.ctx.Variables["v"], int64(1)) {
			t.Fatalf("for-in map value mismatch: got %#v", i.ctx.Variables["v"])
		}
	})

	t.Run("map_branch_break_clears_flag", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = map[string]Value{"x": int64(1)}
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "k",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "it"},
			Body: &BlockStmt{Statements: []Statement{
				&BreakStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("for-in map break error: %v", err)
		}
		if i.ctx.breaking {
			t.Fatalf("for-in map break flag should be cleared")
		}
	})

	t.Run("map_branch_returning", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = map[string]Value{"x": int64(1)}
		got, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "k",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "it"},
			Body: &BlockStmt{Statements: []Statement{
				&ReturnStmt{Value: &IdentExpr{Name: "v"}},
			}},
		})
		if err != nil {
			t.Fatalf("for-in map returning error: %v", err)
		}
		if !valuesEqual(got, int64(1)) || !i.ctx.returning {
			t.Fatalf("for-in map returning mismatch: got=%#v returning=%v", got, i.ctx.returning)
		}
	})

	t.Run("string_branch_continue_reset", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = "ab"
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "idx",
			ValueVar: "ch",
			Iterable: &IdentExpr{Name: "it"},
			Body: &BlockStmt{Statements: []Statement{
				&ContinueStmt{},
			}},
		})
		if err != nil {
			t.Fatalf("for-in string branch error: %v", err)
		}
		if !valuesEqual(i.ctx.Variables["idx"], int64(1)) || !valuesEqual(i.ctx.Variables["ch"], "b") {
			t.Fatalf("for-in string last assignment mismatch: idx=%#v ch=%#v", i.ctx.Variables["idx"], i.ctx.Variables["ch"])
		}
		if i.ctx.continueing {
			t.Fatalf("for-in continue flag should be reset")
		}
	})

	t.Run("string_branch_body_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = "a"
		_, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "idx",
			ValueVar: "ch",
			Iterable: &IdentExpr{Name: "it"},
			Body: &BlockStmt{Statements: []Statement{
				&ExprStmt{Expr: &IdentExpr{Name: "missing"}},
			}},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected for-in string body error, got %v", err)
		}
	})

	t.Run("returning_branch", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["it"] = []Value{int64(9), int64(10)}
		got, err := i.executeForInStmt(&ForInStmt{
			KeyVar:   "_",
			ValueVar: "v",
			Iterable: &IdentExpr{Name: "it"},
			Body: &BlockStmt{Statements: []Statement{
				&ReturnStmt{Value: &IdentExpr{Name: "v"}},
			}},
		})
		if err != nil {
			t.Fatalf("for-in returning branch error: %v", err)
		}
		if !valuesEqual(got, int64(9)) || !valuesEqual(i.ctx.returnValue, int64(9)) || !i.ctx.returning {
			t.Fatalf("for-in returning branch mismatch: got=%#v return=%#v returning=%v", got, i.ctx.returnValue, i.ctx.returning)
		}
	})
}
