package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch33_CenterAndRandomFloat(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinCenter([]Value{}); got != "" {
		t.Fatalf("expected empty center for no args, got %v", got)
	}
	if got := i.builtinCenter([]Value{"abc"}); got != "abc" {
		t.Fatalf("expected passthrough center for short args, got %v", got)
	}
	if got := i.builtinCenter([]Value{123, 8, "-"}); got != "" {
		t.Fatalf("expected empty center for non-string input, got %v", got)
	}
	if got := i.builtinCenter([]Value{"abc", 7, "-"}); got != "--abc--" {
		t.Fatalf("expected centered value, got %v", got)
	}
	if got := i.builtinCenter([]Value{"abc", 8, "xy"}); got != "xyabcxyx" {
		t.Fatalf("expected multi-char centered value, got %v", got)
	}
	if got := i.builtinCenter([]Value{"abcdef", 3, " "}); got != "abcdef" {
		t.Fatalf("expected no-padding when width <= len, got %v", got)
	}
	if got := i.builtinCenter([]Value{"a", 3, ""}); got != " a " {
		t.Fatalf("expected default space padding, got %q", got)
	}

	if got := i.builtinRandomFloat([]Value{}).(float64); got < 0 || got >= 1 {
		t.Fatalf("expected randomFloat() in [0,1), got %v", got)
	}
	if got := i.builtinRandomFloat([]Value{5.0, 10.0}).(float64); got < 5.0 || got >= 10.0 {
		t.Fatalf("expected randomFloat(min,max) in [5,10), got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch33_PartitionFillAndByFunctions(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinPartition([]Value{}).([]Value); len(got) != 2 {
		t.Fatalf("expected two groups for partition default, got %v", got)
	}
	if got := i.builtinPartition([]Value{"not-array", "positive"}).([]Value); len(got) != 2 {
		t.Fatalf("expected two groups for partition non-array, got %v", got)
	}
	parts := i.builtinPartition([]Value{[]Value{int64(-2), int64(0), int64(3)}, "positive"}).([]Value)
	if !reflect.DeepEqual(parts[0], []Value{int64(3)}) || !reflect.DeepEqual(parts[1], []Value{int64(-2), int64(0)}) {
		t.Fatalf("unexpected partition result: %v", parts)
	}

	if got := i.builtinFill([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty fill for missing args, got %v", got)
	}
	if got := i.builtinFill([]Value{0, "x"}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty fill for non-positive size, got %v", got)
	}
	if got := i.builtinFill([]Value{3, "x"}).([]Value); !reflect.DeepEqual(got, []Value{"x", "x", "x"}) {
		t.Fatalf("unexpected fill result: %v", got)
	}

	if got := i.builtinFillRange([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty fillRange for missing args, got %v", got)
	}
	if got := i.builtinFillRange([]Value{1, 5, 0}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty fillRange for zero step, got %v", got)
	}
	if got := i.builtinFillRange([]Value{5, 1, 1}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty fillRange for invalid positive direction, got %v", got)
	}
	if got := i.builtinFillRange([]Value{1, 6, 2}).([]Value); !reflect.DeepEqual(got, []Value{int64(1), int64(3), int64(5)}) {
		t.Fatalf("unexpected fillRange positive result: %v", got)
	}
	if got := i.builtinFillRange([]Value{5, 0, -2}).([]Value); !reflect.DeepEqual(got, []Value{int64(5), int64(3), int64(1)}) {
		t.Fatalf("unexpected fillRange negative result: %v", got)
	}

	if got := i.builtinIntersectionBy([]Value{[]Value{1, 2}, []Value{2}}).([]Value); !reflect.DeepEqual(got, []Value{2}) {
		t.Fatalf("expected fallback intersection, got %v", got)
	}
	if got := i.builtinIntersectionBy([]Value{"bad", []Value{1}, "string"}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty intersectionBy for invalid args, got %v", got)
	}

	arrA := []Value{map[string]Value{"id": int64(1), "v": "a"}, map[string]Value{"id": int64(2), "v": "b"}}
	arrB := []Value{map[string]Value{"id": int64(2), "v": "x"}, map[string]Value{"id": int64(3), "v": "c"}}
	inter := i.builtinIntersectionBy([]Value{arrA, arrB, "id"}).([]Value)
	if len(inter) != 1 {
		t.Fatalf("expected one intersectionBy item, got %v", inter)
	}
	if m, ok := inter[0].(map[string]Value); !ok || m["id"] != int64(2) {
		t.Fatalf("expected id=2 in intersectionBy, got %v", inter)
	}

	if got := i.builtinUnionBy([]Value{[]Value{1}, []Value{2}}).([]Value); !reflect.DeepEqual(got, []Value{1, 2}) {
		t.Fatalf("expected fallback union, got %v", got)
	}
	if got := i.builtinUnionBy([]Value{"bad", "bad", 123}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty union for invalid fallback args, got %v", got)
	}

	uni := i.builtinUnionBy([]Value{arrA, arrB, "id"}).([]Value)
	if len(uni) != 3 {
		t.Fatalf("expected deduped unionBy length 3, got %v", uni)
	}
}
