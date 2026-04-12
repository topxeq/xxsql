package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch66_NormalizeAndValidationEdges(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("normalizeAndStrip", func(t *testing.T) {
		if got := i.builtinNormalizeSpace([]Value{}); got != "" {
			t.Fatalf("expected empty normalizeSpace for no args, got %v", got)
		}
		if got := i.builtinNormalizeSpace([]Value{123}); got != "" {
			t.Fatalf("expected empty normalizeSpace for non-string arg, got %v", got)
		}
		if got := i.builtinNormalizeSpace([]Value{"  a\t\n b   c  "}); got != "a b c" {
			t.Fatalf("unexpected normalizeSpace result: %v", got)
		}

		if got := i.builtinNormalizeNewlines([]Value{}); got != "" {
			t.Fatalf("expected empty normalizeNewlines for no args, got %v", got)
		}
		if got := i.builtinNormalizeNewlines([]Value{123}); got != "" {
			t.Fatalf("expected empty normalizeNewlines for non-string arg, got %v", got)
		}
		if got := i.builtinNormalizeNewlines([]Value{"a\r\nb\rc"}); got != "a\nb\nc" {
			t.Fatalf("unexpected normalizeNewlines result: %v", got)
		}

		if got := i.builtinStripTags([]Value{}); got != "" {
			t.Fatalf("expected empty stripTags for no args, got %v", got)
		}
		if got := i.builtinStripTags([]Value{123}); got != "" {
			t.Fatalf("expected empty stripTags for non-string arg, got %v", got)
		}
		if got := i.builtinStripTags([]Value{"<b>x</b><i>y</i>"}); got != "xy" {
			t.Fatalf("unexpected stripTags result: %v", got)
		}

		if got := i.builtinStripPunctuation([]Value{}); got != "" {
			t.Fatalf("expected empty stripPunctuation for no args, got %v", got)
		}
		if got := i.builtinStripPunctuation([]Value{123}); got != "" {
			t.Fatalf("expected empty stripPunctuation for non-string arg, got %v", got)
		}
		if got := i.builtinStripPunctuation([]Value{"hi, world!"}); got != "hi world" {
			t.Fatalf("unexpected stripPunctuation result: %v", got)
		}
	})

	t.Run("emptyAnagramSplitN", func(t *testing.T) {
		if got := i.builtinIsEmpty([]Value{}); got != true {
			t.Fatalf("expected true isEmpty for no args, got %v", got)
		}
		if got := i.builtinIsEmpty([]Value{123}); got != true {
			t.Fatalf("expected true isEmpty for non-string arg, got %v", got)
		}
		if got := i.builtinIsEmpty([]Value{"   \t\n"}); got != true {
			t.Fatalf("expected true isEmpty for whitespace string, got %v", got)
		}
		if got := i.builtinIsEmpty([]Value{" x "}); got != false {
			t.Fatalf("expected false isEmpty for non-empty string, got %v", got)
		}

		if got := i.builtinIsAnagram([]Value{"aab", "abb"}); got != false {
			t.Fatalf("expected false isAnagram for unequal letter counts, got %v", got)
		}

		if got := i.builtinSplitN([]Value{"a,b,c", ","}); !reflect.DeepEqual(got, []Value{"a", "b", "c"}) {
			t.Fatalf("expected splitN fallback via split for len<3, got %#v", got)
		}
	})
}
