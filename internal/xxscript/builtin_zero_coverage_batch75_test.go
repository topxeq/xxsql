package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch75_CompoundAssignAndPostIncDecEdges(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("evalCompoundAssign_extraBranches", func(t *testing.T) {
		i.ctx.Variables["num"] = int64(9)
		if got, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IdentExpr{Name: "num"}, Op: TokSlashAssign, Value: &NumberExpr{Value: 3}}); err != nil || got.(float64) != 3 {
			t.Fatalf("unexpected /= result got=%#v err=%v", got, err)
		}
		i.ctx.Variables["num2"] = int64(10)
		if got, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IdentExpr{Name: "num2"}, Op: TokPercentAssign, Value: &NumberExpr{Value: 4}}); err != nil || i.toInt(got) != 2 {
			t.Fatalf("unexpected %%= result got=%#v err=%v", got, err)
		}

		i.ctx.Variables["midx"] = map[string]Value{"k": int64(5)}
		if got, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IndexExpr{Object: &IdentExpr{Name: "midx"}, Index: &StringExpr{Value: "k"}}, Op: TokPlusAssign, Value: &NumberExpr{Value: 2}}); err != nil || i.toInt(got) != 7 {
			t.Fatalf("unexpected map index += result got=%#v err=%v", got, err)
		}

		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &IndexExpr{Object: &IdentExpr{Name: "midx"}, Index: &NumberExpr{Value: 1}}, Op: TokPlusAssign, Value: &NumberExpr{Value: 2}}); err == nil {
			t.Fatalf("expected evalIndex map key type error for compound assign")
		}

		if _, err := i.evalCompoundAssign(&CompoundAssignExpr{Left: &MemberExpr{Object: &IdentExpr{Name: "midx"}, Member: &NumberExpr{Value: 1}}, Op: TokPlusAssign, Value: &NumberExpr{Value: 2}}); err == nil {
			t.Fatalf("expected member key type error while reading compound target")
		}

	})

	t.Run("evalPostIncDec_extraBranches", func(t *testing.T) {
		i.ctx.Variables["postMap"] = map[string]Value{"k": int64(2)}
		if old, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "postMap"}, Index: &StringExpr{Value: "k"}}, Op: TokInc}); err != nil || i.toInt(old) != 2 || i.toInt(i.ctx.Variables["postMap"].(map[string]Value)["k"]) != 3 {
			t.Fatalf("unexpected post-inc map index result old=%#v new=%#v err=%v", old, i.ctx.Variables["postMap"], err)
		}

		if _, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "postMap"}, Index: &StringExpr{Value: "missing"}}, Op: TokInc}); err == nil {
			t.Fatalf("expected post-inc on missing map key to fail due to nil arithmetic")
		}

		if _, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &IndexExpr{Object: &IdentExpr{Name: "postMap"}, Index: &NumberExpr{Value: 1}}, Op: TokInc}); err == nil {
			t.Fatalf("expected map index type error for post-inc")
		}

		if _, err := i.evalPostIncDec(&PostIncDecExpr{Expr: &MemberExpr{Object: &IdentExpr{Name: "postMap"}, Member: &NumberExpr{Value: 1}}, Op: TokInc}); err == nil {
			t.Fatalf("expected member key type error for post-inc")
		}
	})
}
