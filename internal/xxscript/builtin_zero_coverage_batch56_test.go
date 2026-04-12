package xxscript

import (
	"errors"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch56_IsErrorBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinIsError([]Value{}); got != false {
		t.Fatalf("builtinIsError no-args: expected false, got %v", got)
	}
	if got := i.builtinIsError([]Value{map[string]Value{"error": true}}); got != true {
		t.Fatalf("builtinIsError map with error=true: expected true, got %v", got)
	}
	if got := i.builtinIsError([]Value{map[string]Value{"error": false}}); got != false {
		t.Fatalf("builtinIsError map with error=false: expected false, got %v", got)
	}
	if got := i.builtinIsError([]Value{map[string]Value{"error": "yes"}}); got != false {
		t.Fatalf("builtinIsError map with non-bool error: expected false, got %v", got)
	}
	if got := i.builtinIsError([]Value{errors.New("boom")}); got != true {
		t.Fatalf("builtinIsError error type: expected true, got %v", got)
	}
	if got := i.builtinIsError([]Value{"error: bad"}); got != true {
		t.Fatalf("builtinIsError lowercase string prefix: expected true, got %v", got)
	}
	if got := i.builtinIsError([]Value{"Error: bad"}); got != true {
		t.Fatalf("builtinIsError uppercase string prefix: expected true, got %v", got)
	}
	if got := i.builtinIsError([]Value{"warning: not error"}); got != false {
		t.Fatalf("builtinIsError non-error string: expected false, got %v", got)
	}
	if got := i.builtinIsError([]Value{123}); got != false {
		t.Fatalf("builtinIsError default type: expected false, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch56_QuotedPrintableAdditionalBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinQuotedPrintableEncode([]Value{123}); got != "" {
		t.Fatalf("builtinQuotedPrintableEncode non-string: expected empty, got %v", got)
	}
	if got := i.builtinQuotedPrintableDecode([]Value{}); got != "" {
		t.Fatalf("builtinQuotedPrintableDecode no-args: expected empty, got %v", got)
	}
	if got := i.builtinQuotedPrintableDecode([]Value{123}); got != "" {
		t.Fatalf("builtinQuotedPrintableDecode non-string: expected empty, got %v", got)
	}

	// Try malformed inputs that should trigger decoder read error branch.
	for _, in := range []string{"=", "abc=", "=0G", "=\rX"} {
		got := i.builtinQuotedPrintableDecode([]Value{in})
		if m, ok := got.(map[string]Value); ok {
			if m["success"] != false || m["error"] == nil {
				t.Fatalf("builtinQuotedPrintableDecode malformed input map shape mismatch for %q: %v", in, m)
			}
			return
		}
	}
}
