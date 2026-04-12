package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch43_CountByAndGroupBy(t *testing.T) {
	i := NewInterpreter(NewContext())

	arr := []Value{int64(1), int64(-2), int64(0), "x", []Value{1}, map[string]Value{"k": 1}, nil}

	if got := i.builtinCountBy([]Value{}).(map[string]Value); len(got) != 0 {
		t.Fatalf("expected empty countBy for missing args, got %v", got)
	}
	if got := i.builtinCountBy([]Value{"bad", "type"}).(map[string]Value); len(got) != 0 {
		t.Fatalf("expected empty countBy for non-array input, got %v", got)
	}

	byType := i.builtinCountBy([]Value{arr, "type"}).(map[string]Value)
	if asInt64ForTest(byType["number"]) != 3 || asInt64ForTest(byType["string"]) != 1 || asInt64ForTest(byType["array"]) != 1 || asInt64ForTest(byType["object"]) != 1 || asInt64ForTest(byType["null"]) != 1 {
		t.Fatalf("unexpected countBy(type) result: %v", byType)
	}

	bySign := i.builtinCountBy([]Value{[]Value{-1, 0, 2}, "sign"}).(map[string]Value)
	if asInt64ForTest(bySign["negative"]) != 1 || asInt64ForTest(bySign["zero"]) != 1 || asInt64ForTest(bySign["positive"]) != 1 {
		t.Fatalf("unexpected countBy(sign) result: %v", bySign)
	}

	byParity := i.builtinCountBy([]Value{[]Value{1, 2, 3, 4}, "parity"}).(map[string]Value)
	if asInt64ForTest(byParity["odd"]) != 2 || asInt64ForTest(byParity["even"]) != 2 {
		t.Fatalf("unexpected countBy(parity) result: %v", byParity)
	}

	byValue := i.builtinCountBy([]Value{[]Value{"a", "a", "b"}, "other"}).(map[string]Value)
	if asInt64ForTest(byValue["a"]) != 2 || asInt64ForTest(byValue["b"]) != 1 {
		t.Fatalf("unexpected countBy(default) result: %v", byValue)
	}

	if got := i.builtinGroupBy([]Value{}).(map[string]Value); len(got) != 0 {
		t.Fatalf("expected empty groupBy for missing args, got %v", got)
	}
	if got := i.builtinGroupBy([]Value{"bad", "type"}).(map[string]Value); len(got) != 0 {
		t.Fatalf("expected empty groupBy for non-array input, got %v", got)
	}

	gType := i.builtinGroupBy([]Value{arr, "type"}).(map[string]Value)
	if len(gType["number"].([]Value)) != 3 || len(gType["string"].([]Value)) != 1 || len(gType["array"].([]Value)) != 1 || len(gType["object"].([]Value)) != 1 || len(gType["null"].([]Value)) != 1 {
		t.Fatalf("unexpected groupBy(type) result: %v", gType)
	}

	gSign := i.builtinGroupBy([]Value{[]Value{-1, 0, 2}, "sign"}).(map[string]Value)
	if len(gSign["negative"].([]Value)) != 1 || len(gSign["zero"].([]Value)) != 1 || len(gSign["positive"].([]Value)) != 1 {
		t.Fatalf("unexpected groupBy(sign) result: %v", gSign)
	}

	gParity := i.builtinGroupBy([]Value{[]Value{1, 2, 3, 4}, "parity"}).(map[string]Value)
	if len(gParity["odd"].([]Value)) != 2 || len(gParity["even"].([]Value)) != 2 {
		t.Fatalf("unexpected groupBy(parity) result: %v", gParity)
	}

	gDefault := i.builtinGroupBy([]Value{[]Value{"a", "a", "b"}, "other"}).(map[string]Value)
	if len(gDefault["a"].([]Value)) != 2 || len(gDefault["b"].([]Value)) != 1 {
		t.Fatalf("unexpected groupBy(default) result: %v", gDefault)
	}
}

func TestBuiltin_ZeroCoverage_Batch43_SplitAndPredicateArrayHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	arr := []Value{1, 2, 0, 3, 0, 4}

	if got := i.builtinTakeWhile([]Value{arr}).([]Value); len(got) != len(arr) {
		t.Fatalf("expected takeWhile passthrough on missing predicate, got %v", got)
	}
	if got := i.builtinTakeWhile([]Value{"bad", "positive"}).([]Value); len(got) != 0 {
		t.Fatalf("expected takeWhile empty on non-array input, got %v", got)
	}
	if got := i.builtinTakeWhile([]Value{arr, "positive"}).([]Value); len(got) != 2 {
		t.Fatalf("expected takeWhile to stop at first non-positive, got %v", got)
	}

	if got := i.builtinDropWhile([]Value{arr}).([]Value); len(got) != len(arr) {
		t.Fatalf("expected dropWhile passthrough on missing predicate, got %v", got)
	}
	if got := i.builtinDropWhile([]Value{"bad", "positive"}).([]Value); len(got) != 0 {
		t.Fatalf("expected dropWhile empty on non-array input, got %v", got)
	}
	if got := i.builtinDropWhile([]Value{arr, "positive"}).([]Value); len(got) != 4 {
		t.Fatalf("expected dropWhile to drop leading positives only, got %v", got)
	}

	if got := i.builtinSpan([]Value{arr}).([]Value); len(got) != 2 {
		t.Fatalf("expected span pair on missing predicate, got %v", got)
	}
	if got := i.builtinSpan([]Value{"bad", "positive"}).([]Value); len(got[0].([]Value)) != 0 || len(got[1].([]Value)) != 0 {
		t.Fatalf("expected span empty pair for non-array input, got %v", got)
	}
	if got := i.builtinSpan([]Value{arr, "positive"}).([]Value); len(got[0].([]Value)) != 2 || len(got[1].([]Value)) != 4 {
		t.Fatalf("expected span split at first non-positive, got %v", got)
	}

	if got := i.builtinBreakList([]Value{arr}).([]Value); len(got) != 2 {
		t.Fatalf("expected breakList pair on missing predicate, got %v", got)
	}
	if got := i.builtinBreakList([]Value{"bad", "zero"}).([]Value); len(got[0].([]Value)) != 0 || len(got[1].([]Value)) != 0 {
		t.Fatalf("expected breakList empty pair for non-array input, got %v", got)
	}
	if got := i.builtinBreakList([]Value{arr, "zero"}).([]Value); len(got[0].([]Value)) != 2 || len(got[1].([]Value)) != 4 {
		t.Fatalf("expected breakList split at first zero, got %v", got)
	}

	if got := i.builtinSplitWhen([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected splitWhen empty for missing args, got %v", got)
	}
	if got := i.builtinSplitWhen([]Value{"bad", "zero"}).([]Value); len(got) != 0 {
		t.Fatalf("expected splitWhen empty for non-array input, got %v", got)
	}
	if got := i.builtinSplitWhen([]Value{arr, 123}).([]Value); len(got) != 1 {
		t.Fatalf("expected splitWhen single group on non-string predicate, got %v", got)
	}
	if got := i.builtinSplitWhen([]Value{arr, "zero"}).([]Value); len(got) != 3 {
		t.Fatalf("expected splitWhen to produce 3 segments, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch43_GetKeyByFunction(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.getKeyByFunction("Ab", "string"); got != "Ab" {
		t.Fatalf("expected string key function, got %q", got)
	}
	if got := i.getKeyByFunction("Ab", "lower"); got != "ab" {
		t.Fatalf("expected lower key function, got %q", got)
	}
	if got := i.getKeyByFunction("Ab", "upper"); got != "AB" {
		t.Fatalf("expected upper key function, got %q", got)
	}
	if got := i.getKeyByFunction(1, "type"); got == "" {
		t.Fatalf("expected non-empty type key function result")
	}
	if got := i.getKeyByFunction("abc", "len"); got != "3" {
		t.Fatalf("expected len key for string, got %q", got)
	}
	if got := i.getKeyByFunction([]Value{1, 2}, "len"); got != "2" {
		t.Fatalf("expected len key for array, got %q", got)
	}
	if got := i.getKeyByFunction(map[string]Value{"a": 1}, "len"); got != "1" {
		t.Fatalf("expected len key for object, got %q", got)
	}
	if got := i.getKeyByFunction(9, "len"); got != "0" {
		t.Fatalf("expected len key default 0, got %q", got)
	}
	if got := i.getKeyByFunction(map[string]Value{"k": 7}, "k"); got != "7" {
		t.Fatalf("expected property lookup key function, got %q", got)
	}
	if got := i.getKeyByFunction(12, "unknown"); got != "12" {
		t.Fatalf("expected default fmt key function, got %q", got)
	}
}
