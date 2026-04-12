package xxscript

import (
	"strings"
	"testing"
)

type batch93ValueObject struct{}

func (o *batch93ValueObject) GetMember(name string) (Value, error) {
	return "vo:" + name, nil
}

func TestBuiltin_ZeroCoverage_Batch93_EvalCallArgsAndTernary(t *testing.T) {
	t.Run("evalCallArgs_spread_and_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["arr"] = []Value{int64(1), int64(2)}
		i.ctx.Variables["x"] = 7

		args, err := i.evalCallArgs([]Expression{
			&NumberExpr{Value: 0},
			&SpreadExpr{Expr: &IdentExpr{Name: "arr"}},
		})
		if err != nil {
			t.Fatalf("evalCallArgs unexpected error: %v", err)
		}
		want := []Value{float64(0), int64(1), int64(2)}
		if !valuesEqual(args, want) {
			t.Fatalf("evalCallArgs mismatch: got=%#v want=%#v", args, want)
		}

		_, err = i.evalCallArgs([]Expression{
			&SpreadExpr{Expr: &IdentExpr{Name: "x"}},
		})
		if err == nil || !strings.Contains(err.Error(), "cannot spread type") {
			t.Fatalf("expected cannot-spread error, got %v", err)
		}

		_, err = i.evalCallArgs([]Expression{
			&IdentExpr{Name: "missing"},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected eval arg error, got %v", err)
		}
	})

	t.Run("evalTernary_false_and_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		got, err := i.evalTernary(&TernaryExpr{
			Condition: &BoolExpr{Value: false},
			TrueExpr:  &StringExpr{Value: "yes"},
			FalseExpr: &StringExpr{Value: "no"},
		})
		if err != nil || !valuesEqual(got, "no") {
			t.Fatalf("expected false branch result, got result=%#v err=%v", got, err)
		}

		_, err = i.evalTernary(&TernaryExpr{
			Condition: &IdentExpr{Name: "missing"},
			TrueExpr:  &StringExpr{Value: "yes"},
			FalseExpr: &StringExpr{Value: "no"},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected condition eval error, got %v", err)
		}
	})
}

func TestBuiltin_ZeroCoverage_Batch93_EvalMemberCompoundAndPreIncDec(t *testing.T) {
	t.Run("evalMember_valueObject_and_key_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["obj"] = &batch93ValueObject{}

		got, err := i.evalMember(&MemberExpr{
			Object: &IdentExpr{Name: "obj"},
			Member: &StringExpr{Value: "m"},
		})
		if err != nil || !valuesEqual(got, "vo:m") {
			t.Fatalf("evalMember valueObject mismatch: result=%#v err=%v", got, err)
		}

		_, err = i.evalMember(&MemberExpr{
			Object: &MapExpr{Pairs: map[string]Expression{}},
			Member: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "member key must be string") {
			t.Fatalf("expected member key error, got %v", err)
		}
	})

	t.Run("evalCompoundAssign_invalid_paths", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		_, err := i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &NumberExpr{Value: 1},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 2},
		})
		if err == nil || !strings.Contains(err.Error(), "invalid compound assignment target") {
			t.Fatalf("expected invalid-target error, got %v", err)
		}

		i.ctx.Variables["x"] = int64(1)
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &IdentExpr{Name: "x"},
			Op:    TokenType(-1),
			Value: &NumberExpr{Value: 2},
		})
		if err == nil || !strings.Contains(err.Error(), "unknown compound assignment operator") {
			t.Fatalf("expected unknown-op error, got %v", err)
		}
	})

	t.Run("evalPreIncDec_member_index_and_invalid", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["m"] = map[string]Value{"n": int64(1)}
		i.ctx.Variables["arr"] = []Value{int64(2)}

		got, err := i.evalPreIncDec(&PreIncDecExpr{
			Expr: &MemberExpr{Object: &IdentExpr{Name: "m"}, Member: &StringExpr{Value: "n"}},
			Op:   TokInc,
		})
		if err != nil || !valuesEqual(got, int64(2)) {
			t.Fatalf("preinc member mismatch: result=%#v err=%v", got, err)
		}
		if !valuesEqual(i.ctx.Variables["m"].(map[string]Value)["n"], int64(2)) {
			t.Fatalf("preinc member assignment mismatch: %#v", i.ctx.Variables["m"])
		}

		got, err = i.evalPreIncDec(&PreIncDecExpr{
			Expr: &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 0}},
			Op:   TokDec,
		})
		if err != nil || !valuesEqual(got, int64(1)) {
			t.Fatalf("predec index mismatch: result=%#v err=%v", got, err)
		}
		if !valuesEqual(i.ctx.Variables["arr"].([]Value)[0], int64(1)) {
			t.Fatalf("predec index assignment mismatch: %#v", i.ctx.Variables["arr"])
		}

		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &StringExpr{Value: "x"}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "invalid increment/decrement target") {
			t.Fatalf("expected invalid pre-inc target error, got %v", err)
		}
	})
}
