package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch62_DeleteOverwriteAndFriends(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("deleteStr", func(t *testing.T) {
		if got := i.builtinDeleteStr([]Value{}); got != "" {
			t.Fatalf("expected empty string for no args, got %v", got)
		}
		if got := i.builtinDeleteStr([]Value{"abc"}); got != "abc" {
			t.Fatalf("expected passthrough for one arg, got %v", got)
		}
		if got := i.builtinDeleteStr([]Value{123, 1}); got != "" {
			t.Fatalf("expected empty string for non-string input, got %v", got)
		}
		if got := i.builtinDeleteStr([]Value{"abc", -2}); got != "" {
			t.Fatalf("expected full delete for negative start default length, got %v", got)
		}
		if got := i.builtinDeleteStr([]Value{"abc", 99}); got != "abc" {
			t.Fatalf("expected unchanged for start >= len, got %v", got)
		}
		if got := i.builtinDeleteStr([]Value{"abc", 1, -3}); got != "abc" {
			t.Fatalf("expected unchanged for negative length, got %v", got)
		}
		if got := i.builtinDeleteStr([]Value{"abcdef", 2, 99}); got != "ab" {
			t.Fatalf("expected clipped delete, got %v", got)
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		if got := i.builtinOverwrite([]Value{}); got != "" {
			t.Fatalf("expected empty string for no args, got %v", got)
		}
		if got := i.builtinOverwrite([]Value{"abc"}); got != "abc" {
			t.Fatalf("expected passthrough for one arg, got %v", got)
		}
		if got := i.builtinOverwrite([]Value{123, 0, "x"}); got != "" {
			t.Fatalf("expected empty string for non-string input, got %v", got)
		}
		if got := i.builtinOverwrite([]Value{"abc", 1, 77}); got != "abc" {
			t.Fatalf("expected unchanged for non-string replacement, got %v", got)
		}
		if got := i.builtinOverwrite([]Value{"abc", -2, "Z"}); got != "Zbc" {
			t.Fatalf("expected overwrite from start for negative index, got %v", got)
		}
		if got := i.builtinOverwrite([]Value{"abc", 10, "XY"}); got != "abcXY" {
			t.Fatalf("expected append when start > len, got %v", got)
		}
		if got := i.builtinOverwrite([]Value{"abc", 2, "XYZ"}); got != "abXYZ" {
			t.Fatalf("expected overwrite with extension, got %v", got)
		}
	})

	t.Run("surround", func(t *testing.T) {
		if got := i.builtinSurround([]Value{}); got != "" {
			t.Fatalf("expected empty string for no args, got %v", got)
		}
		if got := i.builtinSurround([]Value{123}); got != "" {
			t.Fatalf("expected empty string for non-string input, got %v", got)
		}
		if got := i.builtinSurround([]Value{"x"}); got != "\"x\"" {
			t.Fatalf("expected default quote wrapper, got %v", got)
		}
		if got := i.builtinSurround([]Value{"x", 99}); got != "x" {
			t.Fatalf("expected empty wrapper on non-string wrapper arg, got %v", got)
		}
		if got := i.builtinSurround([]Value{"x", "*"}); got != "*x*" {
			t.Fatalf("expected custom wrapper, got %v", got)
		}
	})

	t.Run("unquote", func(t *testing.T) {
		if got := i.builtinUnquote([]Value{}); got != "" {
			t.Fatalf("expected empty string for no args, got %v", got)
		}
		if got := i.builtinUnquote([]Value{123}); got != "" {
			t.Fatalf("expected empty string for non-string input, got %v", got)
		}
		if got := i.builtinUnquote([]Value{"\"abc\""}); got != "abc" {
			t.Fatalf("expected double-quoted unquote, got %v", got)
		}
		if got := i.builtinUnquote([]Value{"\"a\\\"b\""}); got != "a\"b" {
			t.Fatalf("expected escaped quote unquote, got %v", got)
		}
		if got := i.builtinUnquote([]Value{"'a\\'b'"}); got != "a'b" {
			t.Fatalf("expected single-quote unescape, got %v", got)
		}
		if got := i.builtinUnquote([]Value{"`abc`"}); got != "abc" {
			t.Fatalf("expected backtick unquote, got %v", got)
		}
		if got := i.builtinUnquote([]Value{"\"abc'"}); got != "\"abc'" {
			t.Fatalf("expected unchanged for mismatched quotes, got %v", got)
		}
		if got := i.builtinUnquote([]Value{"|abc|"}); got != "|abc|" {
			t.Fatalf("expected unchanged for invalid quote char, got %v", got)
		}
	})

	t.Run("replaceN", func(t *testing.T) {
		if got := i.builtinReplaceN([]Value{}); got != "" {
			t.Fatalf("expected empty string for no args, got %v", got)
		}
		if got := i.builtinReplaceN([]Value{"abc"}); got != "abc" {
			t.Fatalf("expected passthrough for too few args, got %v", got)
		}
		if got := i.builtinReplaceN([]Value{123, "a", "b", 1}); got != "" {
			t.Fatalf("expected empty string for non-string input, got %v", got)
		}
		if got := i.builtinReplaceN([]Value{"aaa", 1, "b", 2}); got != "aaa" {
			t.Fatalf("expected unchanged for invalid old/new args, got %v", got)
		}
		if got := i.builtinReplaceN([]Value{"aaaa", "a", "b", 2}); got != "bbaa" {
			t.Fatalf("expected two replacements, got %v", got)
		}
		if got := i.builtinReplaceN([]Value{"aaaa", "a", "b", -1}); got != "bbbb" {
			t.Fatalf("expected replace all for negative n, got %v", got)
		}
	})

	t.Run("replaceIgnoreCase", func(t *testing.T) {
		if got := i.builtinReplaceIgnoreCase([]Value{}); got != "" {
			t.Fatalf("expected empty string for no args, got %v", got)
		}
		if got := i.builtinReplaceIgnoreCase([]Value{"abc"}); got != "abc" {
			t.Fatalf("expected passthrough for too few args, got %v", got)
		}
		if got := i.builtinReplaceIgnoreCase([]Value{123, "a", "b"}); got != "" {
			t.Fatalf("expected empty string for non-string input, got %v", got)
		}
		if got := i.builtinReplaceIgnoreCase([]Value{"abc", 1, "b"}); got != "abc" {
			t.Fatalf("expected unchanged for invalid old/new args, got %v", got)
		}
		if got := i.builtinReplaceIgnoreCase([]Value{"abc", "", "x"}); got != "abc" {
			t.Fatalf("expected unchanged for empty old string, got %v", got)
		}
		if got := i.builtinReplaceIgnoreCase([]Value{"Hello HELLO", "hello", "hi"}); got != "hi hi" {
			t.Fatalf("expected case-insensitive replacement, got %v", got)
		}
	})
}
