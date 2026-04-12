package xxscript

import (
	"strings"
	"testing"
)

type sequenceCallable struct {
	values []Value
	index  int
}

func (s *sequenceCallable) Call(args []Value) (Value, error) {
	if len(s.values) == 0 {
		return nil, nil
	}
	if s.index >= len(s.values) {
		return s.values[len(s.values)-1], nil
	}
	v := s.values[s.index]
	s.index++
	return v, nil
}

func TestBuiltin_ZeroCoverage_Batch101_AssignAndIncDecEdges(t *testing.T) {
	t.Run("evalCompoundAssign_invalid_and_unknown_target_paths", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		_, err := i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &NumberExpr{Value: 1},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "invalid compound assignment target") {
			t.Fatalf("expected invalid target error, got %v", err)
		}

		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &IdentExpr{Name: "missing"},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "undefined variable") {
			t.Fatalf("expected undefined variable error, got %v", err)
		}

		i.ctx.Variables["x"] = int64(1)
		_, err = i.evalCompoundAssign(&CompoundAssignExpr{
			Left:  &IdentExpr{Name: "x"},
			Op:    TokenType(-999),
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "unknown compound assignment operator") {
			t.Fatalf("expected unknown-op error, got %v", err)
		}
	})

	t.Run("evalCompoundAssign_member_key_type_changes_between_reads", func(t *testing.T) {
		i := NewInterpreter(NewContext())
		i.ctx.Variables["objSeq"] = &sequenceCallable{values: []Value{map[string]Value{"a": int64(1)}, map[string]Value{"a": int64(1)}}}
		i.ctx.Variables["memberSeq"] = &sequenceCallable{values: []Value{"a", float64(1)}}

		_, err := i.evalCompoundAssign(&CompoundAssignExpr{
			Left: &MemberExpr{
				Object: &CallExpr{Func: &IdentExpr{Name: "objSeq"}},
				Member: &CallExpr{Func: &IdentExpr{Name: "memberSeq"}},
			},
			Op:    TokPlusAssign,
			Value: &NumberExpr{Value: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "member key must be string") {
			t.Fatalf("expected member key type error, got %v", err)
		}
	})

	t.Run("evalPreIncDec_invalid_target_and_member_obj_change", func(t *testing.T) {
		i := NewInterpreter(NewContext())

		_, err := i.evalPreIncDec(&PreIncDecExpr{Expr: &NumberExpr{Value: 1}, Op: TokInc})
		if err == nil || !strings.Contains(err.Error(), "invalid increment/decrement target") {
			t.Fatalf("expected invalid inc/dec target error, got %v", err)
		}

		i.ctx.Variables["objSeq"] = &sequenceCallable{values: []Value{map[string]Value{"a": int64(1)}, float64(1)}}
		got, err := i.evalPreIncDec(&PreIncDecExpr{
			Expr: &MemberExpr{
				Object: &CallExpr{Func: &IdentExpr{Name: "objSeq"}},
				Member: &StringExpr{Value: "a"},
			},
			Op: TokInc,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valuesEqual(got, int64(2)) {
			t.Fatalf("unexpected pre-inc result: %#v", got)
		}
	})
}
