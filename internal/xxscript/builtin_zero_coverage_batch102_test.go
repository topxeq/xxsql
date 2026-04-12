package xxscript

import (
	"errors"
	"strings"
	"testing"
)

type valueThenErrorCallable struct {
	first  Value
	errMsg string
	called bool
}

func (c *valueThenErrorCallable) Call(args []Value) (Value, error) {
	if !c.called {
		c.called = true
		return c.first, nil
	}
	return nil, errors.New(c.errMsg)
}

func TestBuiltin_ZeroCoverage_Batch102_EvalAssignAndIncDecErrorPaths(t *testing.T) {
	t.Run("evalIndex_object_eval_error", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		_, err := i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "missingObj"}, Index: &NumberExpr{Value: 0}})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected evalIndex object error, got %v", err)
		}
	})

	t.Run("evalAssign_member_and_index_eval_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		i.ctx.Variables["m"] = map[string]Value{"a": int64(1)}
		_, err := i.evalAssign(&AssignExpr{
			Left:  &MemberExpr{Object: &IdentExpr{Name: "m"}, Member: &IdentExpr{Name: "missingMember"}},
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected member eval error, got %v", err)
		}

		_, err = i.evalAssign(&AssignExpr{
			Left:  &IndexExpr{Object: &IdentExpr{Name: "missingObj"}, Index: &NumberExpr{Value: 0}},
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected index object eval error, got %v", err)
		}

		i.ctx.Variables["arr"] = []Value{int64(1)}
		_, err = i.evalAssign(&AssignExpr{
			Left:  &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &IdentExpr{Name: "missingIndex"}},
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected index eval error, got %v", err)
		}
	})

	t.Run("evalCompoundAssign_operation_and_assign_back_eval_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["x"] = "s"

		_, err := i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &IdentExpr{Name: "x"},
			Op:    TokMinusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil {
			t.Fatalf("expected operation error, got nil")
		}

		i.ctx.Variables["objErr"] = &valueThenErrorCallable{first: map[string]Value{"a": int64(1)}, errMsg: "obj second eval"}
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &MemberExpr{Object: &CallExpr{Func: &IdentExpr{Name: "objErr"}}, Member: &StringExpr{Value: "a"}},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "obj second eval") {
			t.Fatalf("expected assign-back object eval error, got %v", err)
		}

		i.ctx.Variables["objSeq"] = &sequenceCallable{values: []Value{map[string]Value{"a": int64(1)}, map[string]Value{"a": int64(1)}}}
		i.ctx.Variables["memberErr"] = &valueThenErrorCallable{first: "a", errMsg: "member second eval"}
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &MemberExpr{Object: &CallExpr{Func: &IdentExpr{Name: "objSeq"}}, Member: &CallExpr{Func: &IdentExpr{Name: "memberErr"}}},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "member second eval") {
			t.Fatalf("expected assign-back member eval error, got %v", err)
		}

		i.ctx.Variables["arrErr"] = &valueThenErrorCallable{first: []Value{int64(1)}, errMsg: "index object second eval"}
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &IndexExpr{Object: &CallExpr{Func: &IdentExpr{Name: "arrErr"}}, Index: &NumberExpr{Value: 0}},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "index object second eval") {
			t.Fatalf("expected assign-back index object eval error, got %v", err)
		}

		i.ctx.Variables["arrSeq"] = &sequenceCallable{values: []Value{[]Value{int64(1)}, []Value{int64(1)}}}
		i.ctx.Variables["idxErr"] = &valueThenErrorCallable{first: 0, errMsg: "index second eval"}
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &IndexExpr{Object: &CallExpr{Func: &IdentExpr{Name: "arrSeq"}}, Index: &CallExpr{Func: &IdentExpr{Name: "idxErr"}}},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "index second eval") {
			t.Fatalf("expected assign-back index eval error, got %v", err)
		}
	})

	t.Run("evalPreIncDec_ident_member_and_index_error_paths", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		_, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IdentExpr{Name: "missing"}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected undefined variable error, got %v", err)
		}

		i.ctx.Variables["m"] = map[string]Value{"a": "s"}
		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "m"}, Member: &StringExpr{Value: "a"}}, Op: TokDec})
		if err == nil {
			t.Fatalf("expected arithmetic error for member decrement, got nil")
		}

		i.ctx.Variables["objErr"] = &valueThenErrorCallable{first: map[string]Value{"a": int64(1)}, errMsg: "pre obj second eval"}
		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &CallExpr{Func: &IdentExpr{Name: "objErr"}}, Member: &StringExpr{Value: "a"}}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "pre obj second eval") {
			t.Fatalf("expected pre-inc member object eval error, got %v", err)
		}

		i.ctx.Variables["objSeq"] = &sequenceCallable{values: []Value{map[string]Value{"a": int64(1)}, map[string]Value{"a": int64(1)}}}
		i.ctx.Variables["memberErr"] = &valueThenErrorCallable{first: "a", errMsg: "pre member second eval"}
		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &CallExpr{Func: &IdentExpr{Name: "objSeq"}}, Member: &CallExpr{Func: &IdentExpr{Name: "memberErr"}}}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "pre member second eval") {
			t.Fatalf("expected pre-inc member eval error, got %v", err)
		}

		i.ctx.Variables["memberSeq"] = &sequenceCallable{values: []Value{"a", float64(1)}}
		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &CallExpr{Func: &IdentExpr{Name: "objSeq"}}, Member: &CallExpr{Func: &IdentExpr{Name: "memberSeq"}}}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "member key must be string") {
			t.Fatalf("expected pre-inc member key error, got %v", err)
		}

		i.ctx.Variables["arrErr"] = &valueThenErrorCallable{first: []Value{int64(1)}, errMsg: "pre index object second eval"}
		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &CallExpr{Func: &IdentExpr{Name: "arrErr"}}, Index: &NumberExpr{Value: 0}}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "pre index object second eval") {
			t.Fatalf("expected pre-inc index object eval error, got %v", err)
		}

		i.ctx.Variables["arrSeq"] = &sequenceCallable{values: []Value{[]Value{int64(1)}, []Value{int64(1)}}}
		i.ctx.Variables["idxErr"] = &valueThenErrorCallable{first: 0, errMsg: "pre index second eval"}
		_, err = i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &CallExpr{Func: &IdentExpr{Name: "arrSeq"}}, Index: &CallExpr{Func: &IdentExpr{Name: "idxErr"}}}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "pre index second eval") {
			t.Fatalf("expected pre-inc index eval error, got %v", err)
		}
	})
}
