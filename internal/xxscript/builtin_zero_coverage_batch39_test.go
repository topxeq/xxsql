package xxscript

import "testing"

func asInt64ForTest(v Value) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func TestBuiltin_ZeroCoverage_Batch39_EvalPostIncDec(t *testing.T) {
	i := NewInterpreter(NewContext())

	i.ctx.Variables["n"] = int64(2)
	old, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IdentExpr{Name: "n"}, Op: TokInc})
	if err != nil {
		t.Fatalf("unexpected post-inc ident error: %v", err)
	}
	if asInt64ForTest(old) != 2 || asInt64ForTest(i.ctx.Variables["n"]) != 3 {
		t.Fatalf("unexpected post-inc ident result old=%v new=%v", old, i.ctx.Variables["n"])
	}

	if _, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IdentExpr{Name: "missing"}, Op: TokInc}); err == nil {
		t.Fatalf("expected undefined variable error for post-inc")
	}

	i.ctx.Variables["obj"] = map[string]Value{"x": int64(5)}
	old, err = i.evalPostIncDec(&PostIncDecExpr{
		Expr: &MemberExpr{
			Object: &IdentExpr{Name: "obj"},
			Member: &StringExpr{Value: "x"},
		},
		Op: TokDec,
	})
	if err != nil {
		t.Fatalf("unexpected post-dec member error: %v", err)
	}
	obj := i.ctx.Variables["obj"].(map[string]Value)
	if asInt64ForTest(old) != 5 || asInt64ForTest(obj["x"]) != 4 {
		t.Fatalf("unexpected post-dec member result old=%v new=%v", old, obj["x"])
	}

	i.ctx.Variables["arr"] = []Value{int64(10)}
	old, err = i.evalPostIncDec(&PostIncDecExpr{
		Expr: &IndexExpr{
			Object: &IdentExpr{Name: "arr"},
			Index:  &NumberExpr{Value: 0},
		},
		Op: TokInc,
	})
	if err != nil {
		t.Fatalf("unexpected post-inc index error: %v", err)
	}
	arr := i.ctx.Variables["arr"].([]Value)
	if asInt64ForTest(old) != 10 || asInt64ForTest(arr[0]) != 11 {
		t.Fatalf("unexpected post-inc index result old=%v new=%v", old, arr[0])
	}

	if _, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &StringExpr{Value: "x"}, Op: TokInc}); err == nil {
		t.Fatalf("expected invalid target error for post-inc")
	}
}
