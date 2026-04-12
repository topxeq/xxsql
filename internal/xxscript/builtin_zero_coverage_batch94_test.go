package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch94_ExecuteReturnAndForStmtEdges(t *testing.T) {
	t.Run("executeReturnStmt_nil_and_eval_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		got, err := i.executeReturnStmt(&ReturnStmt{})
		if err != nil {
			t.Fatalf("executeReturnStmt nil return error: %v", err)
		}
		if got != nil || i.ctx.returnValue != nil || !i.ctx.returning {
			t.Fatalf("executeReturnStmt nil return mismatch: got=%#v returnValue=%#v returning=%v", got, i.ctx.returnValue, i.ctx.returning)
		}

		i = NewInterpreter(NewContext())
		_, err = i.executeReturnStmt(&ReturnStmt{Value: &IdentExpr{Name: "missing"}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected executeReturnStmt eval error, got %v", err)
		}
	})

	t.Run("executeForStmt_condition_and_body_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.executeForStmt(&ForStmt{
			Condition: &IdentExpr{Name: "missing"},
			Body:      &BlockStmt{},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected condition error, got %v", err)
		}

		i = NewInterpreter(NewContext())
		i.ctx.Variables["cond"] = true
		_, err = i.executeForStmt(&ForStmt{
			Condition: &IdentExpr{Name: "cond"},
			Body: &BlockStmt{Statements: []Statement{
				&ExprStmt{Expr: &IdentExpr{Name: "missing"}},
			}},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected body error, got %v", err)
		}
	})
}

func TestBuiltin_ZeroCoverage_Batch94_BuiltinLenAndRangeBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinLen(nil); !valuesEqual(got, 0) {
		t.Fatalf("builtinLen(nil) expected 0, got %#v", got)
	}
	if got := i.builtinLen([]Value{"abc"}); !valuesEqual(got, 3) {
		t.Fatalf("builtinLen(string) expected 3, got %#v", got)
	}
	if got := i.builtinLen([]Value{[]Value{1, 2}}); !valuesEqual(got, 2) {
		t.Fatalf("builtinLen(array) expected 2, got %#v", got)
	}
	if got := i.builtinLen([]Value{map[string]Value{"a": 1}}); !valuesEqual(got, 1) {
		t.Fatalf("builtinLen(map) expected 1, got %#v", got)
	}
	if got := i.builtinLen([]Value{true}); !valuesEqual(got, 0) {
		t.Fatalf("builtinLen(default) expected 0, got %#v", got)
	}

	if got := i.builtinRange(nil); !valuesEqual(got, []Value{}) {
		t.Fatalf("builtinRange(nil) expected empty slice, got %#v", got)
	}
	if got := i.builtinRange([]Value{3}); !valuesEqual(got, []Value{0, 1, 2}) {
		t.Fatalf("builtinRange(3) expected [0 1 2], got %#v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch94_EvalMemberAndPreIncDecErrors(t *testing.T) {
	t.Run("evalMember_map_miss_and_cannot_access", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		got, err := i.evalMember(&MemberExpr{
			Object: &MapExpr{Pairs: map[string]Expression{"a": &NumberExpr{Value: 1}}},
			Member: &StringExpr{Value: "missing"},
		})
		if err != nil || got != nil {
			t.Fatalf("expected nil for missing map key, got result=%#v err=%v", got, err)
		}

		_, err = i.evalMember(&MemberExpr{
			Object: &NumberExpr{Value: 1},
			Member: &StringExpr{Value: "x"},
		})
		if err == nil || !strings.Contains(err.Error(), "cannot access member") {
			t.Fatalf("expected cannot-access-member error, got %v", err)
		}
	})

	t.Run("evalPreIncDec_member_and_index_eval_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["m"] = map[string]Value{"n": int64(1)}
		i.ctx.Variables["arr"] = []Value{int64(1)}

		_, err := i.evalPreIncDec(&PreIncDecExpr{
			Expr: &MemberExpr{Object: &IdentExpr{Name: "m"}, Member: &NumberExpr{Value: 1}},
			Op:   TokInc,
		})
		if err == nil || !strings.Contains(err.Error(), "member key must be string") {
			t.Fatalf("expected member key error, got %v", err)
		}

		_, err = i.evalPreIncDec(&PreIncDecExpr{
			Expr: &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &IdentExpr{Name: "missing"}},
			Op:   TokInc,
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected index-eval error, got %v", err)
		}
	})
}
