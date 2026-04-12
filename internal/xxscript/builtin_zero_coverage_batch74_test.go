package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch74_BinaryAndCompoundAssign(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("fastBinaryOp", func(t *testing.T) {
		assertFast := func(op TokenType, left, right, want Value) {
			t.Helper()
			got, ok := i.fastBinaryOp(op, left, right)
			if !ok {
				t.Fatalf("expected fastBinaryOp handled op=%v left=%#v right=%#v", op, left, right)
			}
			switch w := want.(type) {
			case float64:
				if gv, ok := got.(float64); !ok || gv != w {
					t.Fatalf("unexpected float result for op=%v: got=%#v want=%#v", op, got, want)
				}
			default:
				if got != want {
					t.Fatalf("unexpected result for op=%v: got=%#v want=%#v", op, got, want)
				}
			}
		}

		assertFast(TokPlus, 7, 2, 9)
		assertFast(TokMinus, 7, 2, 5)
		assertFast(TokStar, 7, 2, 14)
		assertFast(TokSlash, 7, 2, 3.5)
		assertFast(TokPercent, 7, 2, 1)
		assertFast(TokEq, 7, 7, true)
		assertFast(TokNe, 7, 2, true)
		assertFast(TokLt, 1, 2, true)
		assertFast(TokLe, 2, 2, true)
		assertFast(TokGt, 3, 2, true)
		assertFast(TokGe, 3, 3, true)

		if _, ok := i.fastBinaryOp(TokSlash, 7, 0); ok {
			t.Fatalf("expected int division by zero to fall back")
		}
		if _, ok := i.fastBinaryOp(TokPercent, 7, 0); ok {
			t.Fatalf("expected int modulo by zero to fall back")
		}

		assertFast(TokPlus, 2, 1.5, 3.5)
		assertFast(TokMinus, 2, 1.5, 0.5)
		assertFast(TokStar, 2, 1.5, 3.0)
		assertFast(TokSlash, 3, 1.5, 2.0)
		assertFast(TokLt, 1, 1.5, true)
		assertFast(TokLe, 1, 1.5, true)
		assertFast(TokGt, 2, 1.5, true)
		assertFast(TokGe, 2, 1.5, true)
		if _, ok := i.fastBinaryOp(TokSlash, 3, 0.0); ok {
			t.Fatalf("expected int/float division by zero to fall back")
		}

		assertFast(TokPlus, int64(3), int64(4), int64(7))
		assertFast(TokMinus, int64(7), int64(4), int64(3))
		assertFast(TokStar, int64(3), int64(4), int64(12))
		assertFast(TokSlash, int64(7), int64(2), 3.5)
		assertFast(TokPercent, int64(7), int64(2), int64(1))
		assertFast(TokEq, int64(7), int64(7), true)
		assertFast(TokNe, int64(7), int64(2), true)
		assertFast(TokLt, int64(1), int64(2), true)
		assertFast(TokLe, int64(2), int64(2), true)
		assertFast(TokGt, int64(3), int64(2), true)
		assertFast(TokGe, int64(3), int64(3), true)
		if _, ok := i.fastBinaryOp(TokSlash, int64(7), int64(0)); ok {
			t.Fatalf("expected int64 division by zero to fall back")
		}

		assertFast(TokPlus, 1.25, 0.75, 2.0)
		assertFast(TokMinus, 1.25, 0.25, 1.0)
		assertFast(TokStar, 1.25, 2.0, 2.5)
		assertFast(TokSlash, 5.0, 2.0, 2.5)
		assertFast(TokEq, 1.0, 1.0, true)
		assertFast(TokNe, 1.0, 2.0, true)
		assertFast(TokLt, 1.0, 2.0, true)
		assertFast(TokLe, 2.0, 2.0, true)
		assertFast(TokGt, 3.0, 2.0, true)
		assertFast(TokGe, 3.0, 3.0, true)
		if _, ok := i.fastBinaryOp(TokSlash, 5.0, 0.0); ok {
			t.Fatalf("expected float division by zero to fall back")
		}

		assertFast(TokPlus, "a", "b", "ab")
		assertFast(TokEq, "x", "x", true)
		assertFast(TokNe, "x", "y", true)
		assertFast(TokLt, "a", "b", true)
		assertFast(TokLe, "a", "a", true)
		assertFast(TokGt, "b", "a", true)
		assertFast(TokGe, "b", "b", true)

		if _, ok := i.fastBinaryOp(TokPlus, true, false); ok {
			t.Fatalf("expected non-supported types to miss fast path")
		}
	})

	t.Run("evalBinary", func(t *testing.T) {
		if got, err := i.evalBinary(&BinaryExpr{Left: &BoolExpr{Value: false}, Op: TokAnd, Right: &IdentExpr{Name: "missing_right"}}); err != nil || got != false {
			t.Fatalf("expected && short-circuit false, got=%#v err=%v", got, err)
		}

		if _, err := i.evalBinary(&BinaryExpr{Left: &BoolExpr{Value: true}, Op: TokAnd, Right: &IdentExpr{Name: "missing_right"}}); err == nil {
			t.Fatalf("expected && to evaluate right side when left is true")
		}

		if got, err := i.evalBinary(&BinaryExpr{Left: &BoolExpr{Value: true}, Op: TokOr, Right: &IdentExpr{Name: "missing_right"}}); err != nil || got != true {
			t.Fatalf("expected || short-circuit true, got=%#v err=%v", got, err)
		}

		if got, err := i.evalBinary(&BinaryExpr{Left: &BoolExpr{Value: false}, Op: TokOr, Right: &BoolExpr{Value: true}}); err != nil || got != true {
			t.Fatalf("expected || to evaluate right side when left is false, got=%#v err=%v", got, err)
		}

		i.ctx.Variables["ia"] = 2
		i.ctx.Variables["ib"] = 5
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "ia"}, Op: TokPlus, Right: &IdentExpr{Name: "ib"}}); err != nil || got != 7 {
			t.Fatalf("expected fast path int add result, got=%#v err=%v", got, err)
		}

		if got, err := i.evalBinary(&BinaryExpr{Left: &ArrayExpr{Elements: []Expression{&NumberExpr{Value: 1}}}, Op: TokEq, Right: &ArrayExpr{Elements: []Expression{&NumberExpr{Value: 1}}}}); err != nil || got != true {
			t.Fatalf("expected fallback equality path for arrays, got=%#v err=%v", got, err)
		}

		i.ctx.Variables["i64"] = int64(9)
		i.ctx.Variables["f"] = float64(2)
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokMinus, Right: &IdentExpr{Name: "f"}}); err != nil || got.(float64) != 7 {
			t.Fatalf("expected fallback minus path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokStar, Right: &IdentExpr{Name: "f"}}); err != nil || got.(float64) != 18 {
			t.Fatalf("expected fallback star path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokSlash, Right: &IdentExpr{Name: "f"}}); err != nil || got.(float64) != 4.5 {
			t.Fatalf("expected fallback slash path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokPercent, Right: &BoolExpr{Value: true}}); err != nil || i.toInt(got) != 0 {
			t.Fatalf("expected fallback percent path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokLt, Right: &IdentExpr{Name: "f"}}); err != nil || got != false {
			t.Fatalf("expected fallback lt compare path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokLe, Right: &IdentExpr{Name: "f"}}); err != nil || got != false {
			t.Fatalf("expected fallback le compare path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokGt, Right: &IdentExpr{Name: "f"}}); err != nil || got != true {
			t.Fatalf("expected fallback gt compare path, got=%#v err=%v", got, err)
		}
		if got, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokGe, Right: &IdentExpr{Name: "f"}}); err != nil || got != true {
			t.Fatalf("expected fallback ge compare path, got=%#v err=%v", got, err)
		}

		i.ctx.Variables["z"] = int64(0)
		if _, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokSlash, Right: &IdentExpr{Name: "z"}}); err == nil {
			t.Fatalf("expected division-by-zero error from fallback path")
		}
		if _, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "i64"}, Op: TokPercent, Right: &IdentExpr{Name: "z"}}); err == nil {
			t.Fatalf("expected modulo-by-zero error from fallback path")
		}

		if _, err := i.evalBinary(&BinaryExpr{Left: &IdentExpr{Name: "missing_left"}, Op: TokPlus, Right: &NumberExpr{Value: 2}}); err == nil {
			t.Fatalf("expected binary evaluation to fail when left side evaluate fails")
		}

		if _, err := i.evalBinary(&BinaryExpr{Left: &NumberExpr{Value: 1}, Op: TokPlus, Right: &IdentExpr{Name: "missing_rhs"}}); err == nil {
			t.Fatalf("expected binary evaluation to fail when right side evaluate fails")
		}

		if _, err := i.evalBinary(&BinaryExpr{Left: &NumberExpr{Value: 1}, Op: TokNot, Right: &NumberExpr{Value: 2}}); err == nil {
			t.Fatalf("expected unknown operator error")
		}
	})

	t.Run("evalCompoundAssign", func(t *testing.T) {
		i.ctx.Variables["n"] = int64(10)
		if got, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IdentExpr{Name: "n"}, Op: TokPlusAssign, Value: &NumberExpr{Value: 5}}); err != nil || i.toInt(got) != 15 || i.toInt(i.ctx.Variables["n"]) != 15 {
			t.Fatalf("unexpected ident += result got=%#v new=%#v err=%v", got, i.ctx.Variables["n"], err)
		}

		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IdentExpr{Name: "missing"}, Op: TokPlusAssign, Value: &NumberExpr{Value: 1}}); err == nil {
			t.Fatalf("expected undefined variable error for compound assign")
		}

		i.ctx.Variables["obj"] = map[string]Value{"k": int64(3)}
		if got, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &StringExpr{Value: "k"}}, Op: TokStarAssign, Value: &NumberExpr{Value: 2}}); err != nil || i.toInt(got) != 6 {
			t.Fatalf("unexpected member *= result got=%#v err=%v", got, err)
		}
		if i.toInt(i.ctx.Variables["obj"].(map[string]Value)["k"]) != 6 {
			t.Fatalf("unexpected member value after *=: %#v", i.ctx.Variables["obj"])
		}

		i.ctx.Variables["arr"] = []Value{int64(8), int64(2)}
		if got, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 0}}, Op: TokMinusAssign, Value: &NumberExpr{Value: 3}}); err != nil || i.toInt(got) != 5 {
			t.Fatalf("unexpected index -= result got=%#v err=%v", got, err)
		}
		if i.toInt(i.ctx.Variables["arr"].([]Value)[0]) != 5 {
			t.Fatalf("unexpected array value after -=: %#v", i.ctx.Variables["arr"])
		}

		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &StringExpr{Value: "bad"}, Op: TokPlusAssign, Value: &NumberExpr{Value: 1}}); err == nil {
			t.Fatalf("expected invalid compound assignment target error")
		}

		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IdentExpr{Name: "n"}, Op: TokPlusAssign, Value: &IdentExpr{Name: "missing_rhs_for_compound"}}); err == nil {
			t.Fatalf("expected right-side evaluate error in compound assignment")
		}

		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IdentExpr{Name: "n"}, Op: TokAssign, Value: &NumberExpr{Value: 1}}); err == nil {
			t.Fatalf("expected unknown compound assignment operator error")
		}
	})
}
