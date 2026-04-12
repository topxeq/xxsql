package xxscript

import (
	"errors"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch34_IntFloatAndErrorHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinInt([]Value{}); got != 0 {
		t.Fatalf("expected int() default 0, got %v", got)
	}
	if got := i.builtinInt([]Value{int64(7)}); got != 7 {
		t.Fatalf("expected int(int64) conversion, got %v", got)
	}
	if got := i.builtinInt([]Value{3.9}); got != 3 {
		t.Fatalf("expected int(float) truncation, got %v", got)
	}
	if got := i.builtinInt([]Value{"42"}); got != 42 {
		t.Fatalf("expected int(string) parse, got %v", got)
	}
	if got := i.builtinInt([]Value{map[string]Value{"x": 1}}); got != 0 {
		t.Fatalf("expected int(default) 0, got %v", got)
	}

	if got := i.builtinFloat([]Value{}); got != 0.0 {
		t.Fatalf("expected float() default 0, got %v", got)
	}
	if got := i.builtinFloat([]Value{7}); got != 7.0 {
		t.Fatalf("expected float(int) conversion, got %v", got)
	}
	if got := i.builtinFloat([]Value{int64(9)}); got != 9.0 {
		t.Fatalf("expected float(int64) conversion, got %v", got)
	}
	if got := i.builtinFloat([]Value{5.25}); got != 5.25 {
		t.Fatalf("expected float(float64) passthrough, got %v", got)
	}
	if got := i.builtinFloat([]Value{"3.5"}); got != 3.5 {
		t.Fatalf("expected float(string) parse, got %v", got)
	}
	if got := i.builtinFloat([]Value{[]Value{1}}); got != 0.0 {
		t.Fatalf("expected float(default) 0, got %v", got)
	}

	if got := i.builtinErrorMessage([]Value{}); got != "" {
		t.Fatalf("expected empty errorMessage for no args, got %v", got)
	}
	if got := i.builtinErrorMessage([]Value{map[string]Value{"message": "m"}}); got != "m" {
		t.Fatalf("expected message from map.message, got %v", got)
	}
	if got := i.builtinErrorMessage([]Value{map[string]Value{"error": "e"}}); got != "e" {
		t.Fatalf("expected message from map.error string, got %v", got)
	}
	if got := i.builtinErrorMessage([]Value{errors.New("boom")}); got != "boom" {
		t.Fatalf("expected message from error interface, got %v", got)
	}
	if got := i.builtinErrorMessage([]Value{"raw"}); got != "raw" {
		t.Fatalf("expected passthrough string, got %v", got)
	}
	if got := i.builtinErrorMessage([]Value{123}); got != "123" {
		t.Fatalf("expected formatted fallback, got %v", got)
	}

	throwDefault := i.builtinThrow([]Value{}).(map[string]Value)
	if throwDefault["message"] != "throw called" || throwDefault["type"] != "throw" {
		t.Fatalf("expected default throw object, got %v", throwDefault)
	}
	throwMap := i.builtinThrow([]Value{map[string]Value{"message": "x", "type": "custom"}}).(map[string]Value)
	if throwMap["message"] != "x" || throwMap["type"] != "custom" {
		t.Fatalf("expected mapped throw object, got %v", throwMap)
	}
	throwOther := i.builtinThrow([]Value{123}).(map[string]Value)
	if throwOther["message"] != "123" {
		t.Fatalf("expected fallback throw message, got %v", throwOther)
	}

	assertDefault := i.builtinAssert([]Value{}).(map[string]Value)
	if assertDefault["type"] != "assert" {
		t.Fatalf("expected assert type for empty args, got %v", assertDefault)
	}
	if got := i.builtinAssert([]Value{true}); got != true {
		t.Fatalf("expected assert(true) => true, got %v", got)
	}
	assertCustom := i.builtinAssert([]Value{false, "bad"}).(map[string]Value)
	if assertCustom["message"] != "bad" || assertCustom["type"] != "assert" {
		t.Fatalf("expected custom assert failure, got %v", assertCustom)
	}
}
