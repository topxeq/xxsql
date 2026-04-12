package xxscript

import (
	"errors"
	"testing"
)

type testValueObject struct {
	value Value
	err   error
}

func (o *testValueObject) GetMember(name string) (Value, error) {
	if o.err != nil {
		return nil, o.err
	}
	return o.value, nil
}

func TestBuiltin_ZeroCoverage_Batch76_EvalMemberAndIncDecEdges(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("evalMember_objectKinds", func(t *testing.T) {
		if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "missing_object"}, Member: &StringExpr{Value: "x"}}); err == nil {
			t.Fatalf("expected object evaluation error in evalMember")
		}

		i.ctx.Variables["objForMemberErr"] = map[string]Value{"x": 1}
		if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "objForMemberErr"}, Member: &IdentExpr{Name: "missing_member_expr"}}); err == nil {
			t.Fatalf("expected member evaluation error in evalMember")
		}

		i.ctx.Variables["vo"] = &testValueObject{value: "ok"}
		if got, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "vo"}, Member: &StringExpr{Value: "x"}}); err != nil || got != "ok" {
			t.Fatalf("expected ValueObject GetMember result, got=%#v err=%v", got, err)
		}

		i.ctx.Variables["voErr"] = &testValueObject{err: errors.New("boom")}
		if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "voErr"}, Member: &StringExpr{Value: "x"}}); err == nil {
			t.Fatalf("expected ValueObject GetMember error propagation")
		}

		i.ctx.Variables["httpObj"] = NewHTTPObject(i.ctx)
		if got, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "httpObj"}, Member: &StringExpr{Value: "method"}}); err != nil || got != "" {
			t.Fatalf("expected http.method with nil request to return empty string, got=%#v err=%v", got, err)
		}
		if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "httpObj"}, Member: &StringExpr{Value: "notExists"}}); err == nil {
			t.Fatalf("expected unknown http member error")
		}

		i.ctx.Variables["dbObj"] = NewDBObject(i.ctx)
		if got, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "dbObj"}, Member: &StringExpr{Value: "query"}}); err != nil || got == nil {
			t.Fatalf("expected db.query member to resolve callable, got=%#v err=%v", got, err)
		}
		if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "dbObj"}, Member: &StringExpr{Value: "notExists"}}); err == nil {
			t.Fatalf("expected unknown db member error")
		}
	})

	t.Run("evalPreIncDec_moreOps", func(t *testing.T) {
		i.ctx.Variables["n2"] = int64(10)
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IdentExpr{Name: "n2"}, Op: TokDec}); err != nil || i.toInt(got) != 9 || i.toInt(i.ctx.Variables["n2"]) != 9 {
			t.Fatalf("unexpected pre-dec ident result got=%#v new=%#v err=%v", got, i.ctx.Variables["n2"], err)
		}

		i.ctx.Variables["arrDec"] = []Value{int64(6)}
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "arrDec"}, Index: &NumberExpr{Value: 0}}, Op: TokDec}); err != nil || i.toInt(got) != 5 {
			t.Fatalf("unexpected pre-dec index result got=%#v err=%v", got, err)
		}

		i.ctx.Variables["mapDec"] = map[string]Value{"k": int64(2)}
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "mapDec"}, Index: &StringExpr{Value: "k"}}, Op: TokDec}); err != nil || i.toInt(got) != 1 {
			t.Fatalf("unexpected pre-dec map index result got=%#v err=%v", got, err)
		}

		// Read succeeds via ValueObject.GetMember, write-back sees non-map object and is ignored.
		i.ctx.Variables["voPre"] = &testValueObject{value: int64(8)}
		if got, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "voPre"}, Member: &StringExpr{Value: "k"}}, Op: TokInc}); err != nil || i.toInt(got) != 9 {
			t.Fatalf("unexpected pre-inc member-on-valueobject result got=%#v err=%v", got, err)
		}
	})

	t.Run("evalPostIncDec_moreOps", func(t *testing.T) {
		i.ctx.Variables["pn"] = int64(10)
		if old, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IdentExpr{Name: "pn"}, Op: TokDec}); err != nil || i.toInt(old) != 10 || i.toInt(i.ctx.Variables["pn"]) != 9 {
			t.Fatalf("unexpected post-dec ident result old=%#v new=%#v err=%v", old, i.ctx.Variables["pn"], err)
		}

		i.ctx.Variables["pobj"] = map[string]Value{"k": int64(7)}
		if old, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "pobj"}, Member: &StringExpr{Value: "k"}}, Op: TokInc}); err != nil || i.toInt(old) != 7 || i.toInt(i.ctx.Variables["pobj"].(map[string]Value)["k"]) != 8 {
			t.Fatalf("unexpected post-inc member result old=%#v new=%#v err=%v", old, i.ctx.Variables["pobj"], err)
		}

		i.ctx.Variables["parr"] = []Value{int64(4)}
		if old, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "parr"}, Index: &NumberExpr{Value: 0}}, Op: TokDec}); err != nil || i.toInt(old) != 4 || i.toInt(i.ctx.Variables["parr"].([]Value)[0]) != 3 {
			t.Fatalf("unexpected post-dec index result old=%#v new=%#v err=%v", old, i.ctx.Variables["parr"], err)
		}

		// Read succeeds via ValueObject.GetMember, write-back sees non-map object and is ignored.
		i.ctx.Variables["voPost"] = &testValueObject{value: int64(5)}
		if old, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "voPost"}, Member: &StringExpr{Value: "k"}}, Op: TokInc}); err != nil || i.toInt(old) != 5 {
			t.Fatalf("unexpected post-inc member-on-valueobject old=%#v err=%v", old, err)
		}
	})

	t.Run("evalCompoundAssign_memberWritebackNonMap", func(t *testing.T) {
		i.ctx.Variables["voComp"] = &testValueObject{value: int64(4)}
		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &MemberExpr{Object: &IdentExpr{Name: "voComp"}, Member: &StringExpr{Value: "k"}}, Op: TokPlusAssign, Value: &NumberExpr{Value: 3}}); err == nil {
			t.Fatalf("expected compound member write-back error for non-map object")
		}
	})
}
