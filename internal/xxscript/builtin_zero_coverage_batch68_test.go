package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch68_RegexArrayAndRepeatUntil(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("repeatUntil", func(t *testing.T) {
		if got := i.builtinRepeatUntil([]Value{}); got != "" {
			t.Fatalf("expected empty repeatUntil for no args, got %v", got)
		}
		if got := i.builtinRepeatUntil([]Value{123, 5}); got != "" {
			t.Fatalf("expected empty repeatUntil for non-string input, got %v", got)
		}
		if got := i.builtinRepeatUntil([]Value{"ab", 0}); got != "" {
			t.Fatalf("expected empty repeatUntil for target<=0, got %v", got)
		}
		if got := i.builtinRepeatUntil([]Value{"", 5}); got != "" {
			t.Fatalf("expected empty repeatUntil for empty seed, got %v", got)
		}
		if got := i.builtinRepeatUntil([]Value{"ab", 5}); got != "ababa" {
			t.Fatalf("unexpected repeatUntil result: %v", got)
		}
	})

	t.Run("regexCore", func(t *testing.T) {
		if got := i.builtinRegexFindAll([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAll for no args, got %#v", got)
		}
		if got := i.builtinRegexFindAll([]Value{123, "a"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAll for non-string pattern, got %#v", got)
		}
		if got := i.builtinRegexFindAll([]Value{"[", "abc"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAll for invalid pattern, got %#v", got)
		}
		if got := i.builtinRegexFindAll([]Value{"a", "banana", 2}); !reflect.DeepEqual(got, []Value{"a", "a"}) {
			t.Fatalf("unexpected regexFindAll limited result: %#v", got)
		}

		if got := i.builtinRegexReplace([]Value{"a"}); got != "" {
			t.Fatalf("expected empty regexReplace for too few args, got %v", got)
		}
		if got := i.builtinRegexReplace([]Value{123, "x", "abc"}); got != "abc" {
			t.Fatalf("expected passthrough regexReplace for non-string pattern, got %v", got)
		}
		if got := i.builtinRegexReplace([]Value{"[", "x", "abc"}); got != "abc" {
			t.Fatalf("expected passthrough regexReplace for invalid pattern, got %v", got)
		}
		if got := i.builtinRegexReplace([]Value{"a", "x", "banana"}); got != "bxnxnx" {
			t.Fatalf("unexpected regexReplace result: %v", got)
		}

		if got := i.builtinRegexSplit([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexSplit for no args, got %#v", got)
		}
		if got := i.builtinRegexSplit([]Value{123, "a"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexSplit for non-string pattern, got %#v", got)
		}
		if got := i.builtinRegexSplit([]Value{"[", "a,b"}); !reflect.DeepEqual(got, []Value{"a,b"}) {
			t.Fatalf("expected passthrough regexSplit for invalid pattern, got %#v", got)
		}
		if got := i.builtinRegexSplit([]Value{",", "a,b,c", 2}); !reflect.DeepEqual(got, []Value{"a", "b,c"}) {
			t.Fatalf("unexpected regexSplit limited result: %#v", got)
		}

		if got := i.builtinRegexCompile([]Value{}); got.(map[string]Value)["success"] != false {
			t.Fatalf("expected regexCompile no-arg failure, got %#v", got)
		}
		if got := i.builtinRegexCompile([]Value{123}); got.(map[string]Value)["success"] != false {
			t.Fatalf("expected regexCompile type failure, got %#v", got)
		}
		if got := i.builtinRegexCompile([]Value{"["}); got.(map[string]Value)["success"] != false {
			t.Fatalf("expected regexCompile invalid-pattern failure, got %#v", got)
		}
		if got := i.builtinRegexCompile([]Value{"(a)(b)"}); got.(map[string]Value)["success"] != true {
			t.Fatalf("expected regexCompile success, got %#v", got)
		}

		if got := i.builtinRegexQuote([]Value{}); got != "" {
			t.Fatalf("expected empty regexQuote for no args, got %v", got)
		}
		if got := i.builtinRegexQuote([]Value{123}); got != "" {
			t.Fatalf("expected empty regexQuote for non-string input, got %v", got)
		}
		if got := i.builtinRegexQuote([]Value{"a+b"}); got != "a\\+b" {
			t.Fatalf("unexpected regexQuote result: %v", got)
		}

		if got := i.builtinRegexCount([]Value{"a"}); got != int64(0) {
			t.Fatalf("expected 0 regexCount too few args, got %v", got)
		}
		if got := i.builtinRegexCount([]Value{123, "abc"}); got != int64(0) {
			t.Fatalf("expected 0 regexCount for bad types, got %v", got)
		}
		if got := i.builtinRegexCount([]Value{"[", "abc"}); got != int64(0) {
			t.Fatalf("expected 0 regexCount for invalid pattern, got %v", got)
		}
		if got := i.builtinRegexCount([]Value{"a", "banana"}); got != int64(3) {
			t.Fatalf("expected regexCount 3, got %v", got)
		}
	})

	t.Run("regexGroupsAndSubmatches", func(t *testing.T) {
		if got := i.builtinRegexGroups([]Value{}); !reflect.DeepEqual(got, map[string]Value{}) {
			t.Fatalf("expected empty regexGroups for no args, got %#v", got)
		}
		if got := i.builtinRegexGroups([]Value{123, "abc"}); !reflect.DeepEqual(got, map[string]Value{}) {
			t.Fatalf("expected empty regexGroups for bad types, got %#v", got)
		}
		if got := i.builtinRegexGroups([]Value{"[", "abc"}); got.(map[string]Value)["error"] == nil {
			t.Fatalf("expected regexGroups error map for invalid pattern, got %#v", got)
		}
		if got := i.builtinRegexGroups([]Value{"(a)", "bbb"}); !reflect.DeepEqual(got, map[string]Value{}) {
			t.Fatalf("expected empty regexGroups for no match, got %#v", got)
		}
		groups := i.builtinRegexGroups([]Value{"(?P<first>[a-z]+)-(?P<num>\\d+)", "abc-42"}).(map[string]Value)
		if groups["first"] != "abc" || groups["num"] != "42" || groups["0"] != "abc-42" {
			t.Fatalf("unexpected regexGroups values: %#v", groups)
		}

		if got := i.builtinRegexFindSubmatch([]Value{"a"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindSubmatch too few args, got %#v", got)
		}
		if got := i.builtinRegexFindSubmatch([]Value{123, "abc"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindSubmatch bad types, got %#v", got)
		}
		if got := i.builtinRegexFindSubmatch([]Value{"[", "abc"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindSubmatch invalid pattern, got %#v", got)
		}
		if got := i.builtinRegexFindSubmatch([]Value{"(a)", "bbb"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindSubmatch no match, got %#v", got)
		}
		if got := i.builtinRegexFindSubmatch([]Value{"(a)(b)", "zabz"}); !reflect.DeepEqual(got, []Value{"ab", "a", "b"}) {
			t.Fatalf("unexpected regexFindSubmatch result: %#v", got)
		}

		if got := i.builtinRegexFindAllSubmatch([]Value{"a"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAllSubmatch too few args, got %#v", got)
		}
		if got := i.builtinRegexFindAllSubmatch([]Value{123, "abc"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAllSubmatch bad types, got %#v", got)
		}
		if got := i.builtinRegexFindAllSubmatch([]Value{"[", "abc"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAllSubmatch invalid pattern, got %#v", got)
		}
		if got := i.builtinRegexFindAllSubmatch([]Value{"(a)", "bbb"}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty regexFindAllSubmatch no match, got %#v", got)
		}
		if got := i.builtinRegexFindAllSubmatch([]Value{"(a)", "a_a", 1}); !reflect.DeepEqual(got, []Value{[]Value{"a", "a"}}) {
			t.Fatalf("unexpected regexFindAllSubmatch limited result: %#v", got)
		}

		if got := i.builtinRegexValid([]Value{}); got != false {
			t.Fatalf("expected false regexValid no args, got %v", got)
		}
		if got := i.builtinRegexValid([]Value{123}); got != false {
			t.Fatalf("expected false regexValid non-string, got %v", got)
		}
		if got := i.builtinRegexValid([]Value{"["}); got != false {
			t.Fatalf("expected false regexValid invalid pattern, got %v", got)
		}
		if got := i.builtinRegexValid([]Value{"a+"}); got != true {
			t.Fatalf("expected true regexValid for valid pattern, got %v", got)
		}
	})

	t.Run("arrayHelpers", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for push with no args")
			}
		}()
		_ = i.builtinPush([]Value{})
	})

	t.Run("arrayHelpersSafe", func(t *testing.T) {
		if got := i.builtinPush([]Value{[]Value{"a"}}); !reflect.DeepEqual(got, []Value{"a"}) {
			t.Fatalf("expected passthrough push for one arg, got %#v", got)
		}
		if got := i.builtinPush([]Value{"x", "y"}); !reflect.DeepEqual(got, []Value{"y"}) {
			t.Fatalf("expected singleton push for non-array target, got %#v", got)
		}
		if got := i.builtinPush([]Value{[]Value{"a"}, "b"}); !reflect.DeepEqual(got, []Value{"a", "b"}) {
			t.Fatalf("unexpected push append result: %#v", got)
		}

		if got := i.builtinPop([]Value{}); got != nil {
			t.Fatalf("expected nil pop for no args, got %#v", got)
		}
		if got := i.builtinPop([]Value{"x"}); got != nil {
			t.Fatalf("expected nil pop for non-array arg, got %#v", got)
		}
		if got := i.builtinPop([]Value{[]Value{}}); got != nil {
			t.Fatalf("expected nil pop for empty array, got %#v", got)
		}
		if got := i.builtinPop([]Value{[]Value{"a", "b"}}); got != "b" {
			t.Fatalf("expected pop to return last element, got %#v", got)
		}

		if got := i.builtinSlice([]Value{[]Value{"a"}}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty slice for too few args, got %#v", got)
		}
		if got := i.builtinSlice([]Value{"x", 0}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty slice for non-array arg, got %#v", got)
		}
		if got := i.builtinSlice([]Value{[]Value{"a", "b", "c"}, -1}); !reflect.DeepEqual(got, []Value{"a", "b", "c"}) {
			t.Fatalf("expected clamped slice from start, got %#v", got)
		}
		if got := i.builtinSlice([]Value{[]Value{"a", "b"}, 9}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty slice for start>=len, got %#v", got)
		}
		if got := i.builtinSlice([]Value{[]Value{"a", "b", "c"}, 1, 99}); !reflect.DeepEqual(got, []Value{"b", "c"}) {
			t.Fatalf("expected clipped-end slice, got %#v", got)
		}
		if got := i.builtinSlice([]Value{[]Value{"a", "b", "c"}, 1}); !reflect.DeepEqual(got, []Value{"b", "c"}) {
			t.Fatalf("expected tail slice, got %#v", got)
		}

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for slice with end < start")
			}
		}()
		_ = i.builtinSlice([]Value{[]Value{"a", "b", "c"}, 2, 1})
	})
}
