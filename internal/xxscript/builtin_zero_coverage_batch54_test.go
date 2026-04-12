package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch54_PadLeftRight_Insert_Quote_Replace_Rsplit(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinPadLeft([]Value{}); got != "" {
		t.Fatalf("builtinPadLeft no-args: expected empty, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{"ab"}); got != "ab" {
		t.Fatalf("builtinPadLeft short-args: expected ab, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{123}); got != "" {
		t.Fatalf("builtinPadLeft short-args non-string: expected empty, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{123, 5, "x"}); got != "" {
		t.Fatalf("builtinPadLeft invalid source type: expected empty, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{"ab", 5, 123}); got != "   ab" {
		t.Fatalf("builtinPadLeft non-string pad fallback: expected space padded, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{"ab", 5, ""}); got != "   ab" {
		t.Fatalf("builtinPadLeft empty pad fallback: expected space padded, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{"abcdef", 3, "x"}); got != "abcdef" {
		t.Fatalf("builtinPadLeft already long enough: expected unchanged, got %v", got)
	}
	if got := i.builtinPadLeft([]Value{"AB", 5, "xy"}); got != "xyxAB" {
		t.Fatalf("builtinPadLeft custom pad: expected xyxAB, got %v", got)
	}

	if got := i.builtinPadRight([]Value{}); got != "" {
		t.Fatalf("builtinPadRight no-args: expected empty, got %v", got)
	}
	if got := i.builtinPadRight([]Value{"ab"}); got != "ab" {
		t.Fatalf("builtinPadRight short-args: expected ab, got %v", got)
	}
	if got := i.builtinPadRight([]Value{123}); got != "" {
		t.Fatalf("builtinPadRight short-args non-string: expected empty, got %v", got)
	}
	if got := i.builtinPadRight([]Value{123, 5, "x"}); got != "" {
		t.Fatalf("builtinPadRight invalid source type: expected empty, got %v", got)
	}
	if got := i.builtinPadRight([]Value{"ab", 5, 123}); got != "ab   " {
		t.Fatalf("builtinPadRight non-string pad fallback: expected space padded, got %v", got)
	}
	if got := i.builtinPadRight([]Value{"ab", 5, ""}); got != "ab   " {
		t.Fatalf("builtinPadRight empty pad fallback: expected space padded, got %v", got)
	}
	if got := i.builtinPadRight([]Value{"abcdef", 3, "x"}); got != "abcdef" {
		t.Fatalf("builtinPadRight already long enough: expected unchanged, got %v", got)
	}
	if got := i.builtinPadRight([]Value{"AB", 5, "xy"}); got != "ABxyx" {
		t.Fatalf("builtinPadRight custom pad: expected ABxyx, got %v", got)
	}

	if got := i.builtinInsertStr([]Value{}); got != "" {
		t.Fatalf("builtinInsertStr no-args: expected empty, got %v", got)
	}
	if got := i.builtinInsertStr([]Value{42}); got != 42 {
		t.Fatalf("builtinInsertStr short-args passthrough: expected 42, got %v", got)
	}
	if got := i.builtinInsertStr([]Value{42, 1, "x"}); got != "" {
		t.Fatalf("builtinInsertStr invalid source type: expected empty, got %v", got)
	}
	if got := i.builtinInsertStr([]Value{"abc", 1, 999}); got != "abc" {
		t.Fatalf("builtinInsertStr invalid insert type: expected abc, got %v", got)
	}
	if got := i.builtinInsertStr([]Value{"abc", -2, "X"}); got != "Xabc" {
		t.Fatalf("builtinInsertStr negative index clamp: expected Xabc, got %v", got)
	}
	if got := i.builtinInsertStr([]Value{"abc", 99, "X"}); got != "abcX" {
		t.Fatalf("builtinInsertStr large index clamp: expected abcX, got %v", got)
	}
	if got := i.builtinInsertStr([]Value{"a界c", 2, "-"}); got != "a界-c" {
		t.Fatalf("builtinInsertStr rune index insert: expected a界-c, got %v", got)
	}

	if got := i.builtinQuote([]Value{}); got != "\"\"" {
		t.Fatalf("builtinQuote no-args: expected \"\", got %v", got)
	}
	if got := i.builtinQuote([]Value{123}); got != "\"\"" {
		t.Fatalf("builtinQuote invalid source type: expected \"\", got %v", got)
	}
	if got := i.builtinQuote([]Value{"a\"b"}); got != "\"a\\\"b\"" {
		t.Fatalf("builtinQuote default quote escape: got %v", got)
	}
	if got := i.builtinQuote([]Value{"a\"b", ""}); got != "\"a\\\"b\"" {
		t.Fatalf("builtinQuote empty quote fallback: got %v", got)
	}
	if got := i.builtinQuote([]Value{"a'b", "''"}); got != "'a\\'b'" {
		t.Fatalf("builtinQuote first-rune quote handling: got %v", got)
	}

	if got := i.builtinReplaceAll([]Value{}); got != "" {
		t.Fatalf("builtinReplaceAll no-args: expected empty, got %v", got)
	}
	if got := i.builtinReplaceAll([]Value{99}); got != 99 {
		t.Fatalf("builtinReplaceAll short-args passthrough: expected 99, got %v", got)
	}
	if got := i.builtinReplaceAll([]Value{99, "a", "b"}); got != "" {
		t.Fatalf("builtinReplaceAll invalid source type: expected empty, got %v", got)
	}
	if got := i.builtinReplaceAll([]Value{"abc", 1, "b"}); got != "abc" {
		t.Fatalf("builtinReplaceAll invalid old/new types: expected abc, got %v", got)
	}
	if got := i.builtinReplaceAll([]Value{"a_a_a", "a", "b"}); got != "b_b_b" {
		t.Fatalf("builtinReplaceAll replace all: expected b_b_b, got %v", got)
	}

	if got := i.builtinReplaceFirst([]Value{}); got != "" {
		t.Fatalf("builtinReplaceFirst no-args: expected empty, got %v", got)
	}
	if got := i.builtinReplaceFirst([]Value{88}); got != 88 {
		t.Fatalf("builtinReplaceFirst short-args passthrough: expected 88, got %v", got)
	}
	if got := i.builtinReplaceFirst([]Value{88, "a", "b"}); got != "" {
		t.Fatalf("builtinReplaceFirst invalid source type: expected empty, got %v", got)
	}
	if got := i.builtinReplaceFirst([]Value{"abc", 1, "b"}); got != "abc" {
		t.Fatalf("builtinReplaceFirst invalid old/new types: expected abc, got %v", got)
	}
	if got := i.builtinReplaceFirst([]Value{"a_a_a", "a", "b"}); got != "b_a_a" {
		t.Fatalf("builtinReplaceFirst replace first: expected b_a_a, got %v", got)
	}

	if got := i.builtinRsplit([]Value{}); !reflect.DeepEqual(got, []Value{}) {
		t.Fatalf("builtinRsplit no-args: expected empty slice, got %v", got)
	}
	if got := i.builtinRsplit([]Value{123, ","}); !reflect.DeepEqual(got, []Value{}) {
		t.Fatalf("builtinRsplit non-string source: expected empty slice, got %v", got)
	}
	if got := i.builtinRsplit([]Value{"a,b,c", 7}); !reflect.DeepEqual(got, []Value{"a,b,c"}) {
		t.Fatalf("builtinRsplit non-string sep: expected passthrough slice, got %v", got)
	}
	if got := i.builtinRsplit([]Value{"a,b,c", ""}); !reflect.DeepEqual(got, []Value{"a,b,c"}) {
		t.Fatalf("builtinRsplit empty sep: expected passthrough slice, got %v", got)
	}
	if got := i.builtinRsplit([]Value{"a,b,c,d", ","}); !reflect.DeepEqual(got, []Value{"d", "c", "b", "a"}) {
		t.Fatalf("builtinRsplit default reverse split: got %v", got)
	}
	if got := i.builtinRsplit([]Value{"a,b,c,d", ",", 99}); !reflect.DeepEqual(got, []Value{"d", "c", "b", "a"}) {
		t.Fatalf("builtinRsplit n >= parts reverse split: got %v", got)
	}
	if got := i.builtinRsplit([]Value{"a,b,c,d", ",", 3}); !reflect.DeepEqual(got, []Value{"a,b", "c", "d"}) {
		t.Fatalf("builtinRsplit limited right split: got %v", got)
	}
}
