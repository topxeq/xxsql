package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch57_UnwrapAndSizeBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinUnwrap([]Value{}); got != "" {
		t.Fatalf("builtinUnwrap no-args: expected empty, got %v", got)
	}
	if got := i.builtinUnwrap([]Value{int64(7)}); got != "" {
		t.Fatalf("builtinUnwrap non-string branch uses empty string value, got %v", got)
	}
	if got := i.builtinUnwrap([]Value{"a"}); got != "a" {
		t.Fatalf("builtinUnwrap short string passthrough: expected a, got %v", got)
	}
	if got := i.builtinUnwrap([]Value{"[abc]", "["}); got != "[abc]" {
		t.Fatalf("builtinUnwrap invalid wrapper len<2: expected unchanged, got %v", got)
	}
	if got := i.builtinUnwrap([]Value{"[abc]", "[]"}); got != "abc" {
		t.Fatalf("builtinUnwrap matching wrapper: expected abc, got %v", got)
	}
	if got := i.builtinUnwrap([]Value{"(abc)", "[]"}); got != "(abc)" {
		t.Fatalf("builtinUnwrap non-matching wrapper: expected unchanged, got %v", got)
	}

	if got := i.builtinToSize([]Value{}); got != "" {
		t.Fatalf("builtinToSize no-args: expected empty, got %v", got)
	}
	if got := i.builtinToSize([]Value{"1024"}); got != "" {
		t.Fatalf("builtinToSize invalid type: expected empty, got %v", got)
	}
	if got := i.builtinToSize([]Value{int64(512)}); got != "512 B" {
		t.Fatalf("builtinToSize bytes: expected 512 B, got %v", got)
	}
	if got := i.builtinToSize([]Value{int(1536)}); got != "1.50 KB" {
		t.Fatalf("builtinToSize KB: expected 1.50 KB, got %v", got)
	}
	if got := i.builtinToSize([]Value{float64(2 * 1024 * 1024)}); got != "2.00 MB" {
		t.Fatalf("builtinToSize MB: expected 2.00 MB, got %v", got)
	}
	if got := i.builtinToSize([]Value{int64(3 * 1024 * 1024 * 1024)}); got != "3.00 GB" {
		t.Fatalf("builtinToSize GB: expected 3.00 GB, got %v", got)
	}
	if got := i.builtinToSize([]Value{int64(4 * 1024 * 1024 * 1024 * 1024)}); got != "4.00 TB" {
		t.Fatalf("builtinToSize TB: expected 4.00 TB, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch57_FromSizeRemainingBranch(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinFromSize([]Value{"2 TB"}); got != int64(2*1024*1024*1024*1024) {
		t.Fatalf("builtinFromSize TB conversion: expected 2TB bytes, got %v", got)
	}
}
