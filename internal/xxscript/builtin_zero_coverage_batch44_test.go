package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch44_FilterMapReduce(t *testing.T) {
	i := NewInterpreter(NewContext())

	arr := []Value{int(1), int64(2), float64(3.5), "x", []Value{1}, map[string]Value{"k": 1}, nil}

	if got := i.builtinFilter([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty filter for missing args, got %v", got)
	}
	if got := i.builtinFilter([]Value{"bad", "number"}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty filter for non-array input, got %v", got)
	}
	if got := i.builtinFilter([]Value{arr, "number"}).([]Value); len(got) != 3 {
		t.Fatalf("expected 3 numeric values, got %v", got)
	}
	if got := i.builtinFilter([]Value{arr, "string"}).([]Value); len(got) != 1 {
		t.Fatalf("expected 1 string value, got %v", got)
	}
	if got := i.builtinFilter([]Value{arr, "array"}).([]Value); len(got) != 1 {
		t.Fatalf("expected 1 array value, got %v", got)
	}
	if got := i.builtinFilter([]Value{arr, "object"}).([]Value); len(got) != 1 {
		t.Fatalf("expected 1 object value, got %v", got)
	}
	if got := i.builtinFilter([]Value{arr, "nil"}).([]Value); len(got) != 1 {
		t.Fatalf("expected 1 nil value, got %v", got)
	}
	if got := i.builtinFilter([]Value{[]Value{1, "1", 1.0}, 1}).([]Value); len(got) != 3 {
		t.Fatalf("expected numeric-compare filter result length 3, got %v", got)
	}

	if got := i.builtinMap([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty map for missing args, got %v", got)
	}
	if got := i.builtinMap([]Value{"bad", "abs"}).([]Value); len(got) != 0 {
		t.Fatalf("expected empty map for non-array input, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{-1, -2.5}, "abs"}).([]Value); got[0] != 1.0 || got[1] != 2.5 {
		t.Fatalf("expected abs map results, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{1.2, 2.8}, "floor"}).([]Value); got[0] != 1.0 || got[1] != 2.0 {
		t.Fatalf("expected floor map results, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{1.2, 2.8}, "ceil"}).([]Value); got[0] != 2.0 || got[1] != 3.0 {
		t.Fatalf("expected ceil map results, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{1.2, 2.8}, "round"}).([]Value); got[0] != 1.0 || got[1] != 3.0 {
		t.Fatalf("expected round map results, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{9}, "sqrt"}).([]Value); got[0] != 3.0 {
		t.Fatalf("expected sqrt map result 3, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{2}, "square"}).([]Value); got[0] != 4.0 {
		t.Fatalf("expected square map result 4, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{2}, "double"}).([]Value); got[0] != 4.0 {
		t.Fatalf("expected double map result 4, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{2}, "half"}).([]Value); got[0] != 1.0 {
		t.Fatalf("expected half map result 1, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{" Ab "}, "trim"}).([]Value); got[0] != "Ab" {
		t.Fatalf("expected trim map result, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{"Ab"}, "lower"}).([]Value); got[0] != "ab" {
		t.Fatalf("expected lower map result, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{"ab"}, "upper"}).([]Value); got[0] != "AB" {
		t.Fatalf("expected upper map result, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{123}, "string"}).([]Value); got[0] != "123" {
		t.Fatalf("expected string map result, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{2.9}, "int"}).([]Value); got[0] != 2 {
		t.Fatalf("expected int map result 2, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{2}, "float"}).([]Value); got[0] != 2.0 {
		t.Fatalf("expected float map result 2, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{"abc", []Value{1, 2}, 10}, "len"}).([]Value); got[0] != 3 || got[1] != 2 || got[2] != 0 {
		t.Fatalf("expected len map mixed results, got %v", got)
	}
	if got := i.builtinMap([]Value{[]Value{1, "x"}, "noop"}).([]Value); got[0] != 1 || got[1] != "x" {
		t.Fatalf("expected default map passthrough, got %v", got)
	}

	if got := i.builtinReduce([]Value{}); got != 0 {
		t.Fatalf("expected reduce default 0 for missing args, got %v", got)
	}
	if got := i.builtinReduce([]Value{"bad", "sum", 9}); got != 9 {
		t.Fatalf("expected reduce initial for non-array input, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{}, "sum", 9}); got != 9 {
		t.Fatalf("expected reduce initial for empty array, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{1, 2, 3}, "sum"}); got != 6.0 {
		t.Fatalf("expected reduce sum 6, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{2, 3, 4}, "product", 1}); got != 24.0 {
		t.Fatalf("expected reduce product 24, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{10, 3, 2}, "sub"}); got != 5.0 {
		t.Fatalf("expected reduce sub 5, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{8, 2, 0}, "div"}); got != 4.0 {
		t.Fatalf("expected reduce div skip-zero result 4, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{3, 1, 2}, "min"}); got != 1 {
		t.Fatalf("expected reduce min 1, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{3, 1, 2}, "max"}); got != 3 {
		t.Fatalf("expected reduce max 3, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{"a", "b", "c"}, "concat"}); got != "abc" {
		t.Fatalf("expected reduce concat abc, got %v", got)
	}
	if got := i.builtinReduce([]Value{[]Value{1, 2, 3}, "unknown"}); got != 6.0 {
		t.Fatalf("expected reduce default sum 6, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch44_DifferenceIterateAndSmallHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinDifferenceBy([]Value{[]Value{1, 2}}).([]Value); len(got) != 0 {
		t.Fatalf("expected differenceBy fallback empty with too few args, got %v", got)
	}
	if got := i.builtinDifferenceBy([]Value{"bad", []Value{1}, "string"}).([]Value); len(got) != 0 {
		t.Fatalf("expected differenceBy empty for non-array lhs, got %v", got)
	}
	if got := i.builtinDifferenceBy([]Value{[]Value{"A", "b"}, "bad", "lower"}).([]Value); len(got) != 2 {
		t.Fatalf("expected differenceBy passthrough when rhs not array, got %v", got)
	}
	if got := i.builtinDifferenceBy([]Value{[]Value{"A", "b", "C"}, []Value{"a", "x"}, "lower"}).([]Value); len(got) != 2 || got[0] != "b" || got[1] != "C" {
		t.Fatalf("expected differenceBy lower-key filtering, got %v", got)
	}

	if got := i.builtinIterate([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected iterate empty for missing args, got %v", got)
	}
	if got := i.builtinIterate([]Value{5, 0, "inc"}).([]Value); len(got) != 1 || got[0] != 5 {
		t.Fatalf("expected iterate initial-only when n<=0, got %v", got)
	}
	if got := i.builtinIterate([]Value{5, 3, 9}).([]Value); len(got) != 1 || got[0] != 5 {
		t.Fatalf("expected iterate initial-only when op not string, got %v", got)
	}
	if got := i.builtinIterate([]Value{1, 4, "double"}).([]Value); len(got) != 4 || got[0] != 1 || got[1] != 2.0 || got[2] != 4.0 || got[3] != 8.0 {
		t.Fatalf("expected iterate doubling sequence, got %v", got)
	}

	if got := i.builtinPadBetween([]Value{}); got != "" {
		t.Fatalf("expected padBetween empty for missing args, got %v", got)
	}
	if got := i.builtinPadBetween([]Value{"L"}); got != "L" {
		t.Fatalf("expected padBetween passthrough for single arg, got %v", got)
	}
	if got := i.builtinPadBetween([]Value{1, "R", "-"}); got != "" {
		t.Fatalf("expected padBetween empty for non-string left, got %v", got)
	}
	if got := i.builtinPadBetween([]Value{"L", 2, "-"}); got != "L" {
		t.Fatalf("expected padBetween left passthrough for non-string right, got %v", got)
	}
	if got := i.builtinPadBetween([]Value{"L", "R", 1}); got != "L R" {
		t.Fatalf("expected padBetween default single-space pad, got %v", got)
	}
	if got := i.builtinPadBetween([]Value{"L", "R", "-"}); got != "L-R" {
		t.Fatalf("expected padBetween custom pad, got %v", got)
	}

	if got := i.builtinAbs([]Value{}); got != 0 {
		t.Fatalf("expected abs default 0, got %v", got)
	}
	if got := i.builtinAbs([]Value{-3}); got != 3 {
		t.Fatalf("expected abs int result 3, got %v", got)
	}
	if got := i.builtinAbs([]Value{int64(-4)}); got != int64(4) {
		t.Fatalf("expected abs int64 result 4, got %v", got)
	}
	if got := i.builtinAbs([]Value{-2.5}); got != 2.5 {
		t.Fatalf("expected abs float result 2.5, got %v", got)
	}
	if got := i.builtinAbs([]Value{"x"}); got != 0 {
		t.Fatalf("expected abs default 0 for non-number, got %v", got)
	}

	if m := i.builtinError([]Value{}).(map[string]Value); m["error"] != true || m["type"] != "error" {
		t.Fatalf("expected error default shape, got %v", m)
	}
	if m := i.builtinError([]Value{"msg"}).(map[string]Value); m["message"] != "msg" {
		t.Fatalf("expected error message from string, got %v", m)
	}
	if m := i.builtinError([]Value{map[string]Value{"message": "m", "type": "t"}}).(map[string]Value); m["message"] != "m" || m["type"] != "t" {
		t.Fatalf("expected error fields from object, got %v", m)
	}
	if m := i.builtinError([]Value{123, "custom"}).(map[string]Value); m["message"] != "123" || m["type"] != "custom" {
		t.Fatalf("expected error default fmt message with type override, got %v", m)
	}
}
