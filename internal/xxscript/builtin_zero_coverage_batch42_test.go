package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch42_ErrorAndAssertionHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinAssertEqual([]Value{1}); got == true {
		t.Fatalf("expected assertEqual arg error, got %v", got)
	}
	if got := i.builtinAssertEqual([]Value{int64(7), float64(7)}); got != true {
		t.Fatalf("expected numeric cross-type equality, got %v", got)
	}
	if m := i.builtinAssertEqual([]Value{1, 2, "eq custom"}).(map[string]Value); m["message"] != "eq custom" {
		t.Fatalf("expected custom assertEqual message, got %v", m)
	}

	if got := i.builtinAssertNotEqual([]Value{1}); got == true {
		t.Fatalf("expected assertNotEqual arg error, got %v", got)
	}
	if got := i.builtinAssertNotEqual([]Value{1, 2}); got != true {
		t.Fatalf("expected assertNotEqual true for unequal values, got %v", got)
	}
	if m := i.builtinAssertNotEqual([]Value{"x", "x", "neq custom"}).(map[string]Value); m["message"] != "neq custom" {
		t.Fatalf("expected custom assertNotEqual message, got %v", m)
	}

	if got := i.builtinAssertNil([]Value{}); got != true {
		t.Fatalf("expected assertNil true on empty args, got %v", got)
	}
	if got := i.builtinAssertNil([]Value{nil}); got != true {
		t.Fatalf("expected assertNil true on nil value, got %v", got)
	}
	if m := i.builtinAssertNil([]Value{"x", "nil custom"}).(map[string]Value); m["message"] != "nil custom" {
		t.Fatalf("expected custom assertNil message, got %v", m)
	}

	if got := i.builtinAssertNotNil([]Value{}); got == true {
		t.Fatalf("expected assertNotNil error on empty args, got %v", got)
	}
	if got := i.builtinAssertNotNil([]Value{"x"}); got != true {
		t.Fatalf("expected assertNotNil true on non-nil value, got %v", got)
	}
	if m := i.builtinAssertNotNil([]Value{nil, "notnil custom"}).(map[string]Value); m["message"] != "notnil custom" {
		t.Fatalf("expected custom assertNotNil message, got %v", m)
	}

	if got := i.builtinAssertTrue([]Value{}); got == true {
		t.Fatalf("expected assertTrue error on empty args, got %v", got)
	}
	if got := i.builtinAssertTrue([]Value{"x"}); got != true {
		t.Fatalf("expected assertTrue true on truthy value, got %v", got)
	}
	if m := i.builtinAssertTrue([]Value{false, "true custom"}).(map[string]Value); m["message"] != "true custom" {
		t.Fatalf("expected custom assertTrue message, got %v", m)
	}

	if got := i.builtinAssertFalse([]Value{}); got != true {
		t.Fatalf("expected assertFalse true on empty args, got %v", got)
	}
	if got := i.builtinAssertFalse([]Value{0}); got != true {
		t.Fatalf("expected assertFalse true on falsy value, got %v", got)
	}
	if m := i.builtinAssertFalse([]Value{1, "false custom"}).(map[string]Value); m["message"] != "false custom" {
		t.Fatalf("expected custom assertFalse message, got %v", m)
	}

	errObj := map[string]Value{"error": true, "message": "boom", "type": "t"}
	if got := i.builtinOk([]Value{}); got != true {
		t.Fatalf("expected ok(true) for empty args, got %v", got)
	}
	if got := i.builtinOk([]Value{errObj}); got != false {
		t.Fatalf("expected ok(false) for error object, got %v", got)
	}
	if got := i.builtinFail([]Value{}); got != false {
		t.Fatalf("expected fail(false) for empty args, got %v", got)
	}
	if got := i.builtinFail([]Value{errObj}); got != true {
		t.Fatalf("expected fail(true) for error object, got %v", got)
	}

	if m := i.builtinMust([]Value{}).(map[string]Value); m["error"] != true {
		t.Fatalf("expected must error on empty args, got %v", m)
	}
	if m := i.builtinMust([]Value{errObj}).(map[string]Value); m["type"] != "must" {
		t.Fatalf("expected must error wrapper, got %v", m)
	}
	if got := i.builtinMust([]Value{"ok"}); got != "ok" {
		t.Fatalf("expected must passthrough value, got %v", got)
	}

	if got := i.builtinRecover([]Value{errObj, "fallback"}); got != "fallback" {
		t.Fatalf("expected recover fallback on error object, got %v", got)
	}
	if got := i.builtinRecover([]Value{"ok", "fallback"}); got != "ok" {
		t.Fatalf("expected recover passthrough on non-error value, got %v", got)
	}
	if got := i.builtinRecover([]Value{"solo"}); got != "solo" {
		t.Fatalf("expected recover passthrough when len<2, got %v", got)
	}

	if got := i.builtinDefaultOnError([]Value{errObj, "d"}); got != "d" {
		t.Fatalf("expected defaultOnError fallback on error object, got %v", got)
	}
	if got := i.builtinDefaultOnError([]Value{"ok", "d"}); got != "ok" {
		t.Fatalf("expected defaultOnError passthrough on non-error value, got %v", got)
	}
	if got := i.builtinDefaultOnError([]Value{"solo"}); got != "solo" {
		t.Fatalf("expected defaultOnError passthrough when len<2, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch42_TryHelpersAndSafeCall(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinTryGet([]Value{}); got != nil {
		t.Fatalf("expected tryGet nil for missing args, got %v", got)
	}
	if got := i.builtinTryGet([]Value{map[string]Value{"k": 1}, 9, "d"}); got != nil {
		t.Fatalf("expected tryGet nil for bad map key type, got %v", got)
	}
	if got := i.builtinTryGet([]Value{map[string]Value{"k": 1}, "k"}); got != 1 {
		t.Fatalf("expected tryGet map hit, got %v", got)
	}
	if got := i.builtinTryGet([]Value{[]Value{"a", "b"}, 1}); got != "b" {
		t.Fatalf("expected tryGet slice hit, got %v", got)
	}
	if got := i.builtinTryGet([]Value{[]Value{"a"}, 9, "d"}); got != "d" {
		t.Fatalf("expected tryGet default on miss, got %v", got)
	}

	if got := i.builtinTryParse([]Value{"1"}); got != nil {
		t.Fatalf("expected tryParse nil for missing parse type, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"1", 9}); got != "1" {
		t.Fatalf("expected tryParse passthrough for non-string parse type, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"7", "int"}); got != 7 {
		t.Fatalf("expected tryParse int result, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"3.25", "float"}); got != 3.25 {
		t.Fatalf("expected tryParse float result, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"true", "bool"}); got != true {
		t.Fatalf("expected tryParse bool result, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"{\"a\":1}", "json"}); got == nil {
		t.Fatalf("expected tryParse json result, got nil")
	}
	if got := i.builtinTryParse([]Value{123, "json"}); got != 123 {
		t.Fatalf("expected tryParse json passthrough for non-string input, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"x", "int", 99}); got != 99 {
		t.Fatalf("expected tryParse default on parse error, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"x", "int"}); got != nil {
		t.Fatalf("expected tryParse nil on parse error without default, got %v", got)
	}
	if got := i.builtinTryParse([]Value{"x", "unknown"}); got != "x" {
		t.Fatalf("expected tryParse passthrough for unknown parser, got %v", got)
	}

	if m := i.builtinSafeCall([]Value{}).(map[string]Value); m["error"] != true {
		t.Fatalf("expected safeCall error for missing function name, got %v", m)
	}
	if m := i.builtinSafeCall([]Value{123}).(map[string]Value); m["error"] != true {
		t.Fatalf("expected safeCall error for non-string function name, got %v", m)
	}
	if m := i.builtinSafeCall([]Value{"__missing_fn__"}).(map[string]Value); m["error"] != true {
		t.Fatalf("expected safeCall error for unknown function, got %v", m)
	}
	if got := i.builtinSafeCall([]Value{"len", []Value{1, 2, 3}}); got != 3 {
		t.Fatalf("expected safeCall success for builtin len, got %v", got)
	}
	if m := i.builtinSafeCall([]Value{"assertTrue", false}).(map[string]Value); m["error"] != true {
		t.Fatalf("expected safeCall to return builtin error result, got %v", m)
	}

	errObj := map[string]Value{"error": true, "message": "boom", "type": "x"}
	if m := i.builtinErrorFromResult([]Value{}).(map[string]Value); m["hasError"] != false {
		t.Fatalf("expected errorFromResult no-error shape on empty args, got %v", m)
	}
	if m := i.builtinErrorFromResult([]Value{errObj}).(map[string]Value); m["hasError"] != true {
		t.Fatalf("expected errorFromResult to detect error object, got %v", m)
	}
	if m := i.builtinResultOrError([]Value{}).(map[string]Value); m["ok"] != true {
		t.Fatalf("expected resultOrError ok on empty args, got %v", m)
	}
	if m := i.builtinResultOrError([]Value{errObj}).(map[string]Value); m["ok"] != false {
		t.Fatalf("expected resultOrError error shape for error object, got %v", m)
	}
	if m := i.builtinResultOrError([]Value{"ok"}).(map[string]Value); m["value"] != "ok" {
		t.Fatalf("expected resultOrError to carry value, got %v", m)
	}
}

func TestBuiltin_ZeroCoverage_Batch42_IsEqual_CompositeAndFallback(t *testing.T) {
	i := NewInterpreter(NewContext())

	if !i.isEqual(nil, nil) {
		t.Fatalf("expected isEqual(nil,nil)=true")
	}
	if i.isEqual(nil, 1) {
		t.Fatalf("expected isEqual(nil,1)=false")
	}
	if !i.isEqual([]Value{1, "x", map[string]Value{"k": int64(2)}}, []Value{int64(1), "x", map[string]Value{"k": float64(2)}}) {
		t.Fatalf("expected recursive array/map equality")
	}
	if i.isEqual([]Value{1, 2}, []Value{1}) {
		t.Fatalf("expected array length mismatch to be false")
	}
	if !i.isEqual(map[string]Value{"a": 1}, map[string]Value{"a": int64(1)}) {
		t.Fatalf("expected map numeric compatibility equality")
	}
	if i.isEqual(map[string]Value{"a": 1}, map[string]Value{"b": 1}) {
		t.Fatalf("expected map key mismatch to be false")
	}
	if !i.isEqual(struct{ A int }{A: 1}, struct{ A int }{A: 1}) {
		t.Fatalf("expected fallback string equality to match structs")
	}
}
