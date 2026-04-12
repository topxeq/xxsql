package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch64_CoreStringHelpers(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("splitJoinReplace", func(t *testing.T) {
		if got := i.builtinSplit([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty split for too few args, got %#v", got)
		}
		if got := i.builtinSplit([]Value{123, ","}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty split for non-string s, got %#v", got)
		}
		if got := i.builtinSplit([]Value{"a,b", 1}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty split for non-string sep, got %#v", got)
		}
		if got := i.builtinSplit([]Value{"a,b,c", ","}); !reflect.DeepEqual(got, []Value{"a", "b", "c"}) {
			t.Fatalf("unexpected split result: %#v", got)
		}

		if got := i.builtinJoin([]Value{}); got != "" {
			t.Fatalf("expected empty join for too few args, got %v", got)
		}
		if got := i.builtinJoin([]Value{"x", ","}); got != "" {
			t.Fatalf("expected empty join for non-array arg, got %v", got)
		}
		if got := i.builtinJoin([]Value{[]Value{"a", "b"}, 1}); got != "" {
			t.Fatalf("expected empty join for non-string sep, got %v", got)
		}
		if got := i.builtinJoin([]Value{[]Value{"a", int64(2), true}, ":"}); got != "a:2:true" {
			t.Fatalf("unexpected join result: %v", got)
		}

		if got := i.builtinReplace([]Value{"abc"}); got != "" {
			t.Fatalf("expected empty replace for too few args, got %v", got)
		}
		if got := i.builtinReplace([]Value{123, "a", "b"}); got != "" {
			t.Fatalf("expected empty replace for non-string s, got %v", got)
		}
		if got := i.builtinReplace([]Value{"aaa", 1, "b"}); got != "" {
			t.Fatalf("expected empty replace for non-string old, got %v", got)
		}
		if got := i.builtinReplace([]Value{"aaa", "a", 1}); got != "" {
			t.Fatalf("expected empty replace for non-string new, got %v", got)
		}
		if got := i.builtinReplace([]Value{"aaaa", "a", "b", 2}); got != "bbaa" {
			t.Fatalf("unexpected bounded replace result: %v", got)
		}
		if got := i.builtinReplace([]Value{"aaaa", "a", "b"}); got != "bbbb" {
			t.Fatalf("unexpected replace-all result: %v", got)
		}
	})

	t.Run("trimAndCase", func(t *testing.T) {
		if got := i.builtinTrim([]Value{}); got != "" {
			t.Fatalf("expected empty trim for no args, got %v", got)
		}
		if got := i.builtinTrim([]Value{123}); got != "" {
			t.Fatalf("expected empty trim for non-string input, got %v", got)
		}
		if got := i.builtinTrim([]Value{"xxhelloxx", "x"}); got != "hello" {
			t.Fatalf("unexpected trim with cutset result: %v", got)
		}
		if got := i.builtinTrim([]Value{"  hi  ", 1}); got != "  hi  " {
			t.Fatalf("expected unchanged trim for non-string cutset, got %v", got)
		}

		if got := i.builtinTrimPrefix([]Value{"abc"}); got != "" {
			t.Fatalf("expected empty trimPrefix for too few args, got %v", got)
		}
		if got := i.builtinTrimPrefix([]Value{123, "a"}); got != "" {
			t.Fatalf("expected empty trimPrefix for non-string s, got %v", got)
		}
		if got := i.builtinTrimPrefix([]Value{"abc", 1}); got != "" {
			t.Fatalf("expected empty trimPrefix for non-string prefix, got %v", got)
		}
		if got := i.builtinTrimPrefix([]Value{"abc", "ab"}); got != "c" {
			t.Fatalf("unexpected trimPrefix result: %v", got)
		}

		if got := i.builtinTrimSuffix([]Value{"abc"}); got != "" {
			t.Fatalf("expected empty trimSuffix for too few args, got %v", got)
		}
		if got := i.builtinTrimSuffix([]Value{123, "c"}); got != "" {
			t.Fatalf("expected empty trimSuffix for non-string s, got %v", got)
		}
		if got := i.builtinTrimSuffix([]Value{"abc", 1}); got != "" {
			t.Fatalf("expected empty trimSuffix for non-string suffix, got %v", got)
		}
		if got := i.builtinTrimSuffix([]Value{"abc", "bc"}); got != "a" {
			t.Fatalf("unexpected trimSuffix result: %v", got)
		}

		if got := i.builtinLTrim([]Value{}); got != "" {
			t.Fatalf("expected empty ltrim for no args, got %v", got)
		}
		if got := i.builtinLTrim([]Value{123}); got != "" {
			t.Fatalf("expected empty ltrim for non-string input, got %v", got)
		}
		if got := i.builtinLTrim([]Value{"..abc..", "."}); got != "abc.." {
			t.Fatalf("unexpected ltrim result: %v", got)
		}

		if got := i.builtinRTrim([]Value{}); got != "" {
			t.Fatalf("expected empty rtrim for no args, got %v", got)
		}
		if got := i.builtinRTrim([]Value{123}); got != "" {
			t.Fatalf("expected empty rtrim for non-string input, got %v", got)
		}
		if got := i.builtinRTrim([]Value{"..abc..", "."}); got != "..abc" {
			t.Fatalf("unexpected rtrim result: %v", got)
		}

		if got := i.builtinUpper([]Value{}); got != "" {
			t.Fatalf("expected empty upper for no args, got %v", got)
		}
		if got := i.builtinUpper([]Value{123}); got != "" {
			t.Fatalf("expected empty upper for non-string input, got %v", got)
		}
		if got := i.builtinUpper([]Value{"abC"}); got != "ABC" {
			t.Fatalf("unexpected upper result: %v", got)
		}

		if got := i.builtinLower([]Value{}); got != "" {
			t.Fatalf("expected empty lower for no args, got %v", got)
		}
		if got := i.builtinLower([]Value{123}); got != "" {
			t.Fatalf("expected empty lower for non-string input, got %v", got)
		}
		if got := i.builtinLower([]Value{"AbC"}); got != "abc" {
			t.Fatalf("unexpected lower result: %v", got)
		}
	})

	t.Run("searchAndIndex", func(t *testing.T) {
		if got := i.builtinHasPrefix([]Value{"abc"}); got != false {
			t.Fatalf("expected false hasPrefix for too few args, got %v", got)
		}
		if got := i.builtinHasPrefix([]Value{123, "a"}); got != false {
			t.Fatalf("expected false hasPrefix for non-string s, got %v", got)
		}
		if got := i.builtinHasPrefix([]Value{"abc", 1}); got != false {
			t.Fatalf("expected false hasPrefix for non-string prefix, got %v", got)
		}
		if got := i.builtinHasPrefix([]Value{"abc", "ab"}); got != true {
			t.Fatalf("expected true hasPrefix, got %v", got)
		}

		if got := i.builtinHasSuffix([]Value{"abc"}); got != false {
			t.Fatalf("expected false hasSuffix for too few args, got %v", got)
		}
		if got := i.builtinHasSuffix([]Value{123, "c"}); got != false {
			t.Fatalf("expected false hasSuffix for non-string s, got %v", got)
		}
		if got := i.builtinHasSuffix([]Value{"abc", 1}); got != false {
			t.Fatalf("expected false hasSuffix for non-string suffix, got %v", got)
		}
		if got := i.builtinHasSuffix([]Value{"abc", "bc"}); got != true {
			t.Fatalf("expected true hasSuffix, got %v", got)
		}

		if got := i.builtinContains([]Value{"abc"}); got != false {
			t.Fatalf("expected false contains for too few args, got %v", got)
		}
		if got := i.builtinContains([]Value{123, "b"}); got != false {
			t.Fatalf("expected false contains for non-string s, got %v", got)
		}
		if got := i.builtinContains([]Value{"abc", 1}); got != false {
			t.Fatalf("expected false contains for non-string sub, got %v", got)
		}
		if got := i.builtinContains([]Value{"abc", "b"}); got != true {
			t.Fatalf("expected true contains, got %v", got)
		}

		if got := i.builtinIndexOf([]Value{"abc"}); got != -1 {
			t.Fatalf("expected -1 indexOf for too few args, got %v", got)
		}
		if got := i.builtinIndexOf([]Value{123, "a"}); got != -1 {
			t.Fatalf("expected -1 indexOf for non-string s, got %v", got)
		}
		if got := i.builtinIndexOf([]Value{"abc", 1}); got != -1 {
			t.Fatalf("expected -1 indexOf for non-string sub, got %v", got)
		}
		if got := i.builtinIndexOf([]Value{"abcabc", "bc"}); got != 1 {
			t.Fatalf("expected first index 1, got %v", got)
		}

		if got := i.builtinLastIndexOf([]Value{"abc"}); got != -1 {
			t.Fatalf("expected -1 lastIndexOf for too few args, got %v", got)
		}
		if got := i.builtinLastIndexOf([]Value{123, "a"}); got != -1 {
			t.Fatalf("expected -1 lastIndexOf for non-string s, got %v", got)
		}
		if got := i.builtinLastIndexOf([]Value{"abc", 1}); got != -1 {
			t.Fatalf("expected -1 lastIndexOf for non-string sub, got %v", got)
		}
		if got := i.builtinLastIndexOf([]Value{"abcabc", "bc"}); got != 4 {
			t.Fatalf("expected last index 4, got %v", got)
		}
	})

	t.Run("substrRepeatAndCount", func(t *testing.T) {
		if got := i.builtinSubstr([]Value{"abc"}); got != "" {
			t.Fatalf("expected empty substr for too few args, got %v", got)
		}
		if got := i.builtinSubstr([]Value{123, 0}); got != "" {
			t.Fatalf("expected empty substr for non-string s, got %v", got)
		}
		if got := i.builtinSubstr([]Value{"abc", -2}); got != "abc" {
			t.Fatalf("expected clamped start substr, got %v", got)
		}
		if got := i.builtinSubstr([]Value{"abc", 9}); got != "" {
			t.Fatalf("expected empty substr for start >= len, got %v", got)
		}
		if got := i.builtinSubstr([]Value{"abcdef", 2, 2}); got != "cd" {
			t.Fatalf("expected bounded substr cd, got %v", got)
		}
		if got := i.builtinSubstr([]Value{"abcdef", 4, 99}); got != "ef" {
			t.Fatalf("expected clipped-end substr ef, got %v", got)
		}

		if got := i.builtinRepeat([]Value{"a"}); got != "" {
			t.Fatalf("expected empty repeat for too few args, got %v", got)
		}
		if got := i.builtinRepeat([]Value{123, 2}); got != "" {
			t.Fatalf("expected empty repeat for non-string s, got %v", got)
		}
		if got := i.builtinRepeat([]Value{"a", 0}); got != "" {
			t.Fatalf("expected empty repeat for non-positive n, got %v", got)
		}
		if got := i.builtinRepeat([]Value{"ab", 3}); got != "ababab" {
			t.Fatalf("unexpected repeat result: %v", got)
		}

		if got := i.builtinCount([]Value{"abc"}); got != 0 {
			t.Fatalf("expected 0 count for too few args, got %v", got)
		}
		if got := i.builtinCount([]Value{123, "a"}); got != 0 {
			t.Fatalf("expected 0 count for non-string s, got %v", got)
		}
		if got := i.builtinCount([]Value{"abc", 1}); got != 0 {
			t.Fatalf("expected 0 count for non-string sub, got %v", got)
		}
		if got := i.builtinCount([]Value{"ababa", "ba"}); got != 2 {
			t.Fatalf("expected count 2, got %v", got)
		}
	})
}
