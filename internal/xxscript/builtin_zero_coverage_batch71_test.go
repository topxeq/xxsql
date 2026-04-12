package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch71_EvalUnaryAssignAndPreIncDec(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("evalUnary", func(t *testing.T) {
		i.ctx.Variables["b"] = false
		if got, err := i.evalUnary(&UnaryExpr{Op: TokNot, Expr: &IdentExpr{Name: "b"}}); err != nil || got != true {
			t.Fatalf("expected !false -> true, got %#v err=%v", got, err)
		}

		i.ctx.Variables["nInt"] = int(3)
		if got, err := i.evalUnary(&UnaryExpr{Op: TokMinus, Expr: &IdentExpr{Name: "nInt"}}); err != nil || got != -3 {
			t.Fatalf("expected unary minus on int, got %#v err=%v", got, err)
		}

		if _, err := i.evalUnary(&UnaryExpr{Op: TokMinus, Expr: &IdentExpr{Name: "missingVar"}}); err == nil {
			t.Fatalf("expected evalUnary to propagate evaluate error")
		}

		i.ctx.Variables["nInt64"] = int64(4)
		if got, err := i.evalUnary(&UnaryExpr{Op: TokMinus, Expr: &IdentExpr{Name: "nInt64"}}); err != nil || got != int64(-4) {
			t.Fatalf("expected unary minus on int64, got %#v err=%v", got, err)
		}

		if got, err := i.evalUnary(&UnaryExpr{Op: TokMinus, Expr: &NumberExpr{Value: 2.5}}); err != nil || got.(float64) != -2.5 {
			t.Fatalf("expected unary minus on float, got %#v err=%v", got, err)
		}

		if _, err := i.evalUnary(&UnaryExpr{Op: TokMinus, Expr: &StringExpr{Value: "x"}}); err == nil {
			t.Fatalf("expected negate string error")
		}

		if _, err := i.evalUnary(&UnaryExpr{Op: TokPlus, Expr: &NumberExpr{Value: 1}}); err == nil {
			t.Fatalf("expected unknown unary operator error")
		}
	})

	t.Run("evalAssignMultiTarget", func(t *testing.T) {
		if got, err := i.evalAssign(&AssignExpr{
			Lefts: []Expression{&IdentExpr{Name: "a"}, &IdentExpr{Name: "b"}, &IdentExpr{Name: "c"}},
			Value: &ArrayExpr{Elements: []Expression{&NumberExpr{Value: 1}, &StringExpr{Value: "x"}}},
		}); err != nil || got == nil {
			t.Fatalf("expected multi-target array assign success, got %#v err=%v", got, err)
		}
		if i.ctx.Variables["a"].(float64) != 1 || i.ctx.Variables["b"].(string) != "x" || i.ctx.Variables["c"] != nil {
			t.Fatalf("unexpected multi-target array assignment values: %#v", i.ctx.Variables)
		}

		if _, err := i.evalAssign(&AssignExpr{
			Lefts: []Expression{&IdentExpr{Name: "k1"}, &IdentExpr{Name: "missing"}},
			Value: &MapExpr{Pairs: map[string]Expression{"k1": &StringExpr{Value: "v1"}}},
		}); err != nil {
			t.Fatalf("expected multi-target map assign success, err=%v", err)
		}
		if i.ctx.Variables["k1"] != "v1" || i.ctx.Variables["missing"] != nil {
			t.Fatalf("unexpected multi-target map assignment values: %#v", i.ctx.Variables)
		}

		if _, err := i.evalAssign(&AssignExpr{
			Lefts: []Expression{&IdentExpr{Name: "one"}, &IdentExpr{Name: "two"}},
			Value: &StringExpr{Value: "single"},
		}); err != nil {
			t.Fatalf("expected multi-target scalar assign success, err=%v", err)
		}
		if i.ctx.Variables["one"] != "single" || i.ctx.Variables["two"] != nil {
			t.Fatalf("unexpected multi-target scalar assignment values: %#v", i.ctx.Variables)
		}

		if _, err := i.evalAssign(&AssignExpr{
			Lefts: []Expression{&IdentExpr{Name: "errA"}, &IdentExpr{Name: "errB"}},
			Value: &IdentExpr{Name: "does_not_exist"},
		}); err == nil {
			t.Fatalf("expected assignment to fail when rhs evaluate fails")
		}
	})

	t.Run("evalAssignSingleTarget", func(t *testing.T) {
		if got, err := i.evalAssign(&AssignExpr{Left: &IdentExpr{Name: "id"}, Value: &NumberExpr{Value: 9}}); err != nil || got.(float64) != 9 {
			t.Fatalf("expected identifier assignment success, got %#v err=%v", got, err)
		}

		i.ctx.Variables["obj"] = map[string]Value{"x": int64(1)}
		if _, err := i.evalAssign(&AssignExpr{
			Left:  &MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &StringExpr{Value: "x"}},
			Value: &NumberExpr{Value: 7},
		}); err != nil {
			t.Fatalf("expected member assignment success, err=%v", err)
		}
		if i.toInt(i.ctx.Variables["obj"].(map[string]Value)["x"]) != 7 {
			t.Fatalf("unexpected assigned member value: %#v", i.ctx.Variables["obj"])
		}

		if _, err := i.evalAssign(&AssignExpr{
			Left:  &MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &NumberExpr{Value: 1}},
			Value: &NumberExpr{Value: 7},
		}); err == nil {
			t.Fatalf("expected member key type error")
		}

		i.ctx.Variables["scalar"] = int64(1)
		if _, err := i.evalAssign(&AssignExpr{
			Left:  &MemberExpr{Object: &IdentExpr{Name: "scalar"}, Member: &StringExpr{Value: "x"}},
			Value: &NumberExpr{Value: 7},
		}); err == nil {
			t.Fatalf("expected member assignment non-map error")
		}

		i.ctx.Variables["arr"] = []Value{int64(1), int64(2)}
		if _, err := i.evalAssign(&AssignExpr{
			Left:  &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 1}},
			Value: &NumberExpr{Value: 11},
		}); err != nil {
			t.Fatalf("expected index array assignment success, err=%v", err)
		}
		if i.toInt(i.ctx.Variables["arr"].([]Value)[1]) != 11 {
			t.Fatalf("unexpected array index assignment value: %#v", i.ctx.Variables["arr"])
		}

		if _, err := i.evalAssign(&AssignExpr{
			Left:  &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 9}},
			Value: &NumberExpr{Value: 11},
		}); err != nil {
			t.Fatalf("expected out-of-range array index assignment to be ignored, err=%v", err)
		}

		i.ctx.Variables["m"] = map[string]Value{"k": "v"}
		if _, err := i.evalAssign(&AssignExpr{
			Left:  &IndexExpr{Object: &IdentExpr{Name: "m"}, Index: &StringExpr{Value: "k2"}},
			Value: &StringExpr{Value: "v2"},
		}); err != nil {
			t.Fatalf("expected map index assignment success, err=%v", err)
		}
		if i.ctx.Variables["m"].(map[string]Value)["k2"] != "v2" {
			t.Fatalf("unexpected map index assignment value: %#v", i.ctx.Variables["m"])
		}

		if _, err := i.evalAssign(&AssignExpr{
			Left:  &IndexExpr{Object: &IdentExpr{Name: "m"}, Index: &NumberExpr{Value: 1}},
			Value: &StringExpr{Value: "ignored"},
		}); err != nil {
			t.Fatalf("expected map assignment with non-string index to be ignored without error, err=%v", err)
		}

		if _, err := i.evalAssign(&AssignExpr{
			Left:  &MemberExpr{Object: &IdentExpr{Name: "missingObj"}, Member: &StringExpr{Value: "x"}},
			Value: &NumberExpr{Value: 7},
		}); err == nil {
			t.Fatalf("expected member assignment to fail when object evaluate fails")
		}

		if _, err := i.evalAssign(&AssignExpr{
			Left:  &StringExpr{Value: "bad"},
			Value: &NumberExpr{Value: 1},
		}); err == nil {
			t.Fatalf("expected invalid assignment target error")
		}
	})

	t.Run("evalPreIncDec", func(t *testing.T) {
		i.ctx.Variables["n"] = int64(2)
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IdentExpr{Name: "n"}, Op: TokInc}); err != nil || i.toInt(got) != 3 || i.toInt(i.ctx.Variables["n"]) != 3 {
			t.Fatalf("unexpected pre-inc ident result got=%#v new=%#v err=%v", got, i.ctx.Variables["n"], err)
		}
		if _, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IdentExpr{Name: "missing"}, Op: TokInc}); err == nil {
			t.Fatalf("expected undefined variable error for pre-inc")
		}

		i.ctx.Variables["obj2"] = map[string]Value{"x": int64(5)}
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "obj2"}, Member: &StringExpr{Value: "x"}}, Op: TokDec}); err != nil || i.toInt(got) != 4 {
			t.Fatalf("unexpected pre-dec member result got=%#v err=%v", got, err)
		}
		if i.toInt(i.ctx.Variables["obj2"].(map[string]Value)["x"]) != 4 {
			t.Fatalf("unexpected member value after pre-dec: %#v", i.ctx.Variables["obj2"])
		}
		if _, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "obj2"}, Member: &NumberExpr{Value: 1}}, Op: TokInc}); err == nil {
			t.Fatalf("expected member key type error for pre-inc/dec")
		}

		i.ctx.Variables["arr2"] = []Value{int64(10)}
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "arr2"}, Index: &NumberExpr{Value: 0}}, Op: TokInc}); err != nil || i.toInt(got) != 11 {
			t.Fatalf("unexpected pre-inc index result got=%#v err=%v", got, err)
		}
		if i.toInt(i.ctx.Variables["arr2"].([]Value)[0]) != 11 {
			t.Fatalf("unexpected array value after pre-inc: %#v", i.ctx.Variables["arr2"])
		}

		i.ctx.Variables["mapIdx"] = map[string]Value{"k": int64(2)}
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "mapIdx"}, Index: &StringExpr{Value: "k"}}, Op: TokInc}); err != nil || i.toInt(got) != 3 {
			t.Fatalf("unexpected pre-inc map index result got=%#v err=%v", got, err)
		}
		if i.toInt(i.ctx.Variables["mapIdx"].(map[string]Value)["k"]) != 3 {
			t.Fatalf("unexpected map value after pre-inc: %#v", i.ctx.Variables["mapIdx"])
		}

		if _, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "mapIdx"}, Index: &StringExpr{Value: "missing"}}, Op: TokInc}); err == nil {
			t.Fatalf("expected pre-inc on missing index to fail due to nil arithmetic")
		}

		if _, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &StringExpr{Value: "x"}, Op: TokInc}); err == nil {
			t.Fatalf("expected invalid target error for pre-inc/dec")
		}
	})

	t.Run("callBuiltinHandledAndUnknown", func(t *testing.T) {
		if got, handled := i.callBuiltin("len", []Value{[]Value{1, 2}}); !handled || i.toInt(got) != 2 {
			t.Fatalf("expected callBuiltin len handled with result 2, got %#v handled=%v", got, handled)
		}
		if _, handled := i.callBuiltin("__not_existing_builtin__", nil); handled {
			t.Fatalf("expected unknown builtin to be unhandled")
		}
	})
}
