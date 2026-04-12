package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch65_MoreStringUtilities(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("capitalizeTitleSwapCase", func(t *testing.T) {
		if got := i.builtinCapitalize([]Value{}); got != "" {
			t.Fatalf("expected empty capitalize for no args, got %v", got)
		}
		if got := i.builtinCapitalize([]Value{123}); got != "" {
			t.Fatalf("expected empty capitalize for non-string arg, got %v", got)
		}
		if got := i.builtinCapitalize([]Value{""}); got != "" {
			t.Fatalf("expected empty capitalize for empty string, got %v", got)
		}
		if got := i.builtinCapitalize([]Value{"hELLO"}); got != "Hello" {
			t.Fatalf("unexpected capitalize result: %v", got)
		}

		if got := i.builtinTitle([]Value{}); got != "" {
			t.Fatalf("expected empty title for no args, got %v", got)
		}
		if got := i.builtinTitle([]Value{123}); got != "" {
			t.Fatalf("expected empty title for non-string arg, got %v", got)
		}
		if got := i.builtinTitle([]Value{"hello world"}); got != "Hello World" {
			t.Fatalf("unexpected title result: %v", got)
		}

		if got := i.builtinSwapCase([]Value{}); got != "" {
			t.Fatalf("expected empty swapCase for no args, got %v", got)
		}
		if got := i.builtinSwapCase([]Value{123}); got != "" {
			t.Fatalf("expected empty swapCase for non-string arg, got %v", got)
		}
		if got := i.builtinSwapCase([]Value{"AbC!"}); got != "aBc!" {
			t.Fatalf("unexpected swapCase result: %v", got)
		}
	})

	t.Run("alphaNumericChecks", func(t *testing.T) {
		if got := i.builtinIsAlpha([]Value{}); got != false {
			t.Fatalf("expected false isAlpha for no args, got %v", got)
		}
		if got := i.builtinIsAlpha([]Value{123}); got != false {
			t.Fatalf("expected false isAlpha for non-string arg, got %v", got)
		}
		if got := i.builtinIsAlpha([]Value{""}); got != false {
			t.Fatalf("expected false isAlpha for empty string, got %v", got)
		}
		if got := i.builtinIsAlpha([]Value{"abcXYZ"}); got != true {
			t.Fatalf("expected true isAlpha for letters-only input, got %v", got)
		}
		if got := i.builtinIsAlpha([]Value{"abc1"}); got != false {
			t.Fatalf("expected false isAlpha for mixed input, got %v", got)
		}

		if got := i.builtinIsNumeric([]Value{}); got != false {
			t.Fatalf("expected false isNumeric for no args, got %v", got)
		}
		if got := i.builtinIsNumeric([]Value{123}); got != false {
			t.Fatalf("expected false isNumeric for non-string arg, got %v", got)
		}
		if got := i.builtinIsNumeric([]Value{""}); got != false {
			t.Fatalf("expected false isNumeric for empty string, got %v", got)
		}
		if got := i.builtinIsNumeric([]Value{"01234"}); got != true {
			t.Fatalf("expected true isNumeric for digits-only input, got %v", got)
		}
		if got := i.builtinIsNumeric([]Value{"12a"}); got != false {
			t.Fatalf("expected false isNumeric for mixed input, got %v", got)
		}

		if got := i.builtinIsAlphaNumeric([]Value{}); got != false {
			t.Fatalf("expected false isAlphaNumeric for no args, got %v", got)
		}
		if got := i.builtinIsAlphaNumeric([]Value{123}); got != false {
			t.Fatalf("expected false isAlphaNumeric for non-string arg, got %v", got)
		}
		if got := i.builtinIsAlphaNumeric([]Value{""}); got != false {
			t.Fatalf("expected false isAlphaNumeric for empty string, got %v", got)
		}
		if got := i.builtinIsAlphaNumeric([]Value{"abc123"}); got != true {
			t.Fatalf("expected true isAlphaNumeric for letters+digits input, got %v", got)
		}
		if got := i.builtinIsAlphaNumeric([]Value{"abc-123"}); got != false {
			t.Fatalf("expected false isAlphaNumeric for punctuation input, got %v", got)
		}
	})

	t.Run("truncateAndWordCount", func(t *testing.T) {
		if got := i.builtinTruncate([]Value{}); got != "" {
			t.Fatalf("expected empty truncate for no args, got %v", got)
		}
		if got := i.builtinTruncate([]Value{123}); got != "" {
			t.Fatalf("expected empty truncate for non-string arg, got %v", got)
		}
		if got := i.builtinTruncate([]Value{"short", 10}); got != "short" {
			t.Fatalf("expected unchanged truncate for short input, got %v", got)
		}
		if got := i.builtinTruncate([]Value{"abcdefgh", 4, "--"}); got != "abcd--" {
			t.Fatalf("unexpected truncate result: %v", got)
		}

		if got := i.builtinWordCount([]Value{}); got != 0 {
			t.Fatalf("expected 0 wordCount for no args, got %v", got)
		}
		if got := i.builtinWordCount([]Value{123}); got != 0 {
			t.Fatalf("expected 0 wordCount for non-string arg, got %v", got)
		}
		if got := i.builtinWordCount([]Value{" one  two\tthree\n"}); got != 3 {
			t.Fatalf("expected wordCount 3, got %v", got)
		}
	})

	t.Run("escapeAndUnescape", func(t *testing.T) {
		if got := i.builtinEscapeHTML([]Value{}); got != "" {
			t.Fatalf("expected empty escapeHTML for no args, got %v", got)
		}
		if got := i.builtinEscapeHTML([]Value{123}); got != "" {
			t.Fatalf("expected empty escapeHTML for non-string arg, got %v", got)
		}
		if got := i.builtinEscapeHTML([]Value{"<a&b>"}); got != "&lt;a&amp;b&gt;" {
			t.Fatalf("unexpected escapeHTML result: %v", got)
		}

		if got := i.builtinUnescapeHTML([]Value{}); got != "" {
			t.Fatalf("expected empty unescapeHTML for no args, got %v", got)
		}
		if got := i.builtinUnescapeHTML([]Value{123}); got != "" {
			t.Fatalf("expected empty unescapeHTML for non-string arg, got %v", got)
		}
		if got := i.builtinUnescapeHTML([]Value{"&lt;x&gt;"}); got != "<x>" {
			t.Fatalf("unexpected unescapeHTML result: %v", got)
		}

		if got := i.builtinEscapeURL([]Value{}); got != "" {
			t.Fatalf("expected empty escapeURL for no args, got %v", got)
		}
		if got := i.builtinEscapeURL([]Value{123}); got != "" {
			t.Fatalf("expected empty escapeURL for non-string arg, got %v", got)
		}
		if got := i.builtinEscapeURL([]Value{"a b"}); got != "a+b" {
			t.Fatalf("unexpected escapeURL result: %v", got)
		}

		if got := i.builtinUnescapeURL([]Value{}); got != "" {
			t.Fatalf("expected empty unescapeURL for no args, got %v", got)
		}
		if got := i.builtinUnescapeURL([]Value{123}); got != "" {
			t.Fatalf("expected empty unescapeURL for non-string arg, got %v", got)
		}
		if got := i.builtinUnescapeURL([]Value{"a+b"}); got != "a b" {
			t.Fatalf("unexpected unescapeURL result: %v", got)
		}
	})

	t.Run("leftRightLinesWords", func(t *testing.T) {
		if got := i.builtinLeft([]Value{"abc"}); got != "" {
			t.Fatalf("expected empty left for too few args, got %v", got)
		}
		if got := i.builtinLeft([]Value{123, 2}); got != "" {
			t.Fatalf("expected empty left for non-string input, got %v", got)
		}
		if got := i.builtinLeft([]Value{"abc", 0}); got != "" {
			t.Fatalf("expected empty left for n <= 0, got %v", got)
		}
		if got := i.builtinLeft([]Value{"abc", 99}); got != "abc" {
			t.Fatalf("expected whole string left for n >= len, got %v", got)
		}
		if got := i.builtinLeft([]Value{"abc", 2}); got != "ab" {
			t.Fatalf("unexpected left result: %v", got)
		}

		if got := i.builtinRight([]Value{"abc"}); got != "" {
			t.Fatalf("expected empty right for too few args, got %v", got)
		}
		if got := i.builtinRight([]Value{123, 2}); got != "" {
			t.Fatalf("expected empty right for non-string input, got %v", got)
		}
		if got := i.builtinRight([]Value{"abc", 0}); got != "" {
			t.Fatalf("expected empty right for n <= 0, got %v", got)
		}
		if got := i.builtinRight([]Value{"abc", 99}); got != "abc" {
			t.Fatalf("expected whole string right for n >= len, got %v", got)
		}
		if got := i.builtinRight([]Value{"abc", 2}); got != "bc" {
			t.Fatalf("unexpected right result: %v", got)
		}

		if got := i.builtinLines([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty lines for no args, got %#v", got)
		}
		if got := i.builtinLines([]Value{123}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty lines for non-string input, got %#v", got)
		}
		if got := i.builtinLines([]Value{"a\r\nb\n"}); !reflect.DeepEqual(got, []Value{"a", "b", ""}) {
			t.Fatalf("unexpected lines result: %#v", got)
		}

		if got := i.builtinWords([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty words for no args, got %#v", got)
		}
		if got := i.builtinWords([]Value{123}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty words for non-string input, got %#v", got)
		}
		if got := i.builtinWords([]Value{"  one\ttwo\nthree "}); !reflect.DeepEqual(got, []Value{"one", "two", "three"}) {
			t.Fatalf("unexpected words result: %#v", got)
		}
	})
}
