package xxscript

import (
	"net/http/httptest"
	"strings"
	"testing"
)

type flipMemberObjectFunc struct {
	calls int
}

func (f *flipMemberObjectFunc) Call(args []Value) (Value, error) {
	f.calls++
	if f.calls == 1 {
		return map[string]Value{"a": int64(1)}, nil
	}
	return float64(1), nil
}

func TestBuiltin_ZeroCoverage_Batch100_EvalMemberAndCompoundAssignEdges(t *testing.T) {
	t.Run("evalMember_http_object_branch", func(t *testing.T) {
		ctx := NewContext()
		ctx.HTTPRequest = httptest.NewRequest("GET", "http://example.com/p", nil)
		i := NewInterpreter(ctx)
		i.ctx.Variables["h"] = NewHTTPObject(ctx)

		got, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "h"}, Member: &StringExpr{Value: "path"}})
		if err != nil || got != "/p" {
			t.Fatalf("evalMember HTTPObject mismatch: result=%#v err=%v", got, err)
		}
	})

	t.Run("evalCompoundAssign_member_key_and_member_type_errors", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["m"] = map[string]Value{"a": int64(1)}

		_, err := i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &MemberExpr{Object: &IdentExpr{Name: "m"}, Member: &NumberExpr{Value: 1}},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "member key must be string") {
			t.Fatalf("expected member-key error, got %v", err)
		}

		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &MemberExpr{Object: &NumberExpr{Value: 1}, Member: &StringExpr{Value: "a"}},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "cannot access member of") {
			t.Fatalf("expected non-object member access error, got %v", err)
		}

		flip := &flipMemberObjectFunc{}
		i.ctx.Variables["flipMemberObj"] = flip
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left: &MemberExpr{
				Object: &CallExpr{Func: &IdentExpr{Name: "flipMemberObj"}},
				Member: &StringExpr{Value: "a"},
			},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "cannot assign to member of") {
			t.Fatalf("expected non-map member assign error, got %v", err)
		}
	})
}
