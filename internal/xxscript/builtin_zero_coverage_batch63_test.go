package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch63_StringAnalysisAndSplitVariants(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("commonPrefixAndSuffix", func(t *testing.T) {
		if got := i.builtinCommonPrefix([]Value{}); got != "" {
			t.Fatalf("expected empty prefix for no args, got %v", got)
		}
		if got := i.builtinCommonPrefix([]Value{"abc"}); got != "abc" {
			t.Fatalf("expected passthrough for one arg prefix, got %v", got)
		}
		if got := i.builtinCommonPrefix([]Value{1, "abc"}); got != "" {
			t.Fatalf("expected empty for non-string prefix input, got %v", got)
		}
		if got := i.builtinCommonPrefix([]Value{"flower", "flow"}); got != "flow" {
			t.Fatalf("expected flow prefix, got %v", got)
		}

		if got := i.builtinCommonSuffix([]Value{}); got != "" {
			t.Fatalf("expected empty suffix for no args, got %v", got)
		}
		if got := i.builtinCommonSuffix([]Value{"abc"}); got != "abc" {
			t.Fatalf("expected passthrough for one arg suffix, got %v", got)
		}
		if got := i.builtinCommonSuffix([]Value{"abc", 2}); got != "" {
			t.Fatalf("expected empty for non-string suffix input, got %v", got)
		}
		if got := i.builtinCommonSuffix([]Value{"running", "jogging"}); got != "ing" {
			t.Fatalf("expected ing suffix, got %v", got)
		}
	})

	t.Run("palindromeAndAnagram", func(t *testing.T) {
		if got := i.builtinIsPalindrome([]Value{}); got != false {
			t.Fatalf("expected false for no args palindrome, got %v", got)
		}
		if got := i.builtinIsPalindrome([]Value{123}); got != false {
			t.Fatalf("expected false for non-string palindrome input, got %v", got)
		}
		if got := i.builtinIsPalindrome([]Value{"A man, a plan, a canal: Panama"}); got != true {
			t.Fatalf("expected true palindrome after normalization, got %v", got)
		}
		if got := i.builtinIsPalindrome([]Value{"not one"}); got != false {
			t.Fatalf("expected false for non-palindrome, got %v", got)
		}

		if got := i.builtinIsAnagram([]Value{"abc"}); got != false {
			t.Fatalf("expected false for too few anagram args, got %v", got)
		}
		if got := i.builtinIsAnagram([]Value{"abc", 1}); got != false {
			t.Fatalf("expected false for non-string anagram arg, got %v", got)
		}
		if got := i.builtinIsAnagram([]Value{"listen", "silent"}); got != true {
			t.Fatalf("expected true for anagram pair, got %v", got)
		}
		if got := i.builtinIsAnagram([]Value{"foo", "bar"}); got != false {
			t.Fatalf("expected false for non-anagram pair, got %v", got)
		}
	})

	t.Run("charAndByteCount", func(t *testing.T) {
		if got := i.builtinCharCount([]Value{}); got != int64(0) {
			t.Fatalf("expected 0 char count with no args, got %v", got)
		}
		if got := i.builtinCharCount([]Value{123}); got != int64(0) {
			t.Fatalf("expected 0 char count for non-string input, got %v", got)
		}
		if got := i.builtinCharCount([]Value{"你好a"}); got != int64(3) {
			t.Fatalf("expected rune count 3, got %v", got)
		}

		if got := i.builtinByteCount([]Value{}); got != int64(0) {
			t.Fatalf("expected 0 byte count with no args, got %v", got)
		}
		if got := i.builtinByteCount([]Value{123}); got != int64(0) {
			t.Fatalf("expected 0 byte count for non-string input, got %v", got)
		}
		if got := i.builtinByteCount([]Value{"你好a"}); got != int64(len("你好a")) {
			t.Fatalf("expected byte count %d, got %v", len("你好a"), got)
		}
	})

	t.Run("splitNPartitionAndRpartition", func(t *testing.T) {
		if got := i.builtinSplitN([]Value{123, ",", 2}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty splitN result for non-string input, got %#v", got)
		}
		if got := i.builtinSplitN([]Value{"a,b,c", 1, 2}); !reflect.DeepEqual(got, []Value{"a,b,c"}) {
			t.Fatalf("expected passthrough splitN for non-string sep, got %#v", got)
		}
		if got := i.builtinSplitN([]Value{"a,b,c", ",", 2}); !reflect.DeepEqual(got, []Value{"a", "b,c"}) {
			t.Fatalf("unexpected splitN result: %#v", got)
		}

		expectedTriple := []Value{"", "", ""}
		if got := i.builtinPartitionStr([]Value{}); !reflect.DeepEqual(got, expectedTriple) {
			t.Fatalf("expected empty triple for partition no args, got %#v", got)
		}
		if got := i.builtinPartitionStr([]Value{123, ":"}); !reflect.DeepEqual(got, expectedTriple) {
			t.Fatalf("expected empty triple for partition non-string input, got %#v", got)
		}
		if got := i.builtinPartitionStr([]Value{"a:b", 1}); !reflect.DeepEqual(got, []Value{"a:b", "", ""}) {
			t.Fatalf("expected [s,empty,empty] for partition non-string sep, got %#v", got)
		}
		if got := i.builtinPartitionStr([]Value{"abc", ":"}); !reflect.DeepEqual(got, []Value{"abc", "", ""}) {
			t.Fatalf("expected not-found partition shape, got %#v", got)
		}
		if got := i.builtinPartitionStr([]Value{"a:b:c", ":"}); !reflect.DeepEqual(got, []Value{"a", ":", "b:c"}) {
			t.Fatalf("unexpected partition result: %#v", got)
		}

		if got := i.builtinRpartition([]Value{}); !reflect.DeepEqual(got, expectedTriple) {
			t.Fatalf("expected empty triple for rpartition no args, got %#v", got)
		}
		if got := i.builtinRpartition([]Value{123, ":"}); !reflect.DeepEqual(got, expectedTriple) {
			t.Fatalf("expected empty triple for rpartition non-string input, got %#v", got)
		}
		if got := i.builtinRpartition([]Value{"a:b", 1}); !reflect.DeepEqual(got, []Value{"", "", "a:b"}) {
			t.Fatalf("expected [empty,empty,s] for rpartition non-string sep, got %#v", got)
		}
		if got := i.builtinRpartition([]Value{"abc", ":"}); !reflect.DeepEqual(got, []Value{"", "", "abc"}) {
			t.Fatalf("expected not-found rpartition shape, got %#v", got)
		}
		if got := i.builtinRpartition([]Value{"a:b:c", ":"}); !reflect.DeepEqual(got, []Value{"a:b", ":", "c"}) {
			t.Fatalf("unexpected rpartition result: %#v", got)
		}
	})
}
