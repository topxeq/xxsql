package xxscript

import (
	"errors"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch82_ErrorTemplateAndFormat(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinSprintf(nil); got != "" {
		t.Fatalf("builtinSprintf nil args: got=%#v", got)
	}
	if got := i.builtinSprintf([]Value{123}); got != "123" {
		t.Fatalf("builtinSprintf non-string format: got=%#v", got)
	}
	if got := i.builtinSprintf([]Value{"x=%d y=%s", 7, "ok"}); got != "x=7 y=ok" {
		t.Fatalf("builtinSprintf formatted output: got=%#v", got)
	}

	if got := i.builtinFormat(nil); got != "" {
		t.Fatalf("builtinFormat nil args: got=%#v", got)
	}
	if got := i.builtinFormat([]Value{123}); got != "" {
		t.Fatalf("builtinFormat non-string format: got=%#v", got)
	}
	if got := i.builtinFormat([]Value{"hello"}); got != "hello" {
		t.Fatalf("builtinFormat passthrough: got=%#v", got)
	}
	if got := i.builtinFormat([]Value{"%s-%d", "x", 2}); got != "x-2" {
		t.Fatalf("builtinFormat formatted output: got=%#v", got)
	}

	if got := i.builtinTemplate(nil); got != "" {
		t.Fatalf("builtinTemplate nil args: got=%#v", got)
	}
	if got := i.builtinTemplate([]Value{1}); got != "" {
		t.Fatalf("builtinTemplate non-string template: got=%#v", got)
	}
	if got := i.builtinTemplate([]Value{"hello"}); got != "hello" {
		t.Fatalf("builtinTemplate no data: got=%#v", got)
	}
	if got := i.builtinTemplate([]Value{"hello", 1}); got != "hello" {
		t.Fatalf("builtinTemplate non-map data: got=%#v", got)
	}
	if got := i.builtinTemplate([]Value{"Hi {{name}} {{missing}}", map[string]Value{"name": "Bob"}}); got != "Hi Bob {{missing}}" {
		t.Fatalf("builtinTemplate replacement: got=%#v", got)
	}

	orig := map[string]Value{"message": "base", "type": "custom"}
	wrapped := i.builtinErrorWrap([]Value{orig, 123})
	wrappedMap, ok := wrapped.(map[string]Value)
	origField, hasOrig := wrappedMap["original"].(map[string]Value)
	if !ok || wrappedMap["message"] != "123: base" || wrappedMap["type"] != "custom" || !hasOrig || origField["message"] != "base" {
		t.Fatalf("builtinErrorWrap map input: got=%#v", wrapped)
	}
	if got := i.builtinErrorWrap([]Value{"oops", "ctx"}); got.(map[string]Value)["message"] != "ctx: oops" {
		t.Fatalf("builtinErrorWrap string input: got=%#v", got)
	}
	if got := i.builtinErrorWrap([]Value{errors.New("boom"), "ctx"}); got.(map[string]Value)["message"] != "ctx: boom" {
		t.Fatalf("builtinErrorWrap error input: got=%#v", got)
	}
	if got := i.builtinErrorWrap([]Value{99, "ctx"}); got.(map[string]Value)["message"] != "ctx: 99" {
		t.Fatalf("builtinErrorWrap default input: got=%#v", got)
	}
	if got := i.builtinErrorWrap([]Value{"only-one"}); got != "only-one" {
		t.Fatalf("builtinErrorWrap len<2 passthrough: got=%#v", got)
	}

	panicNoArg := i.builtinPanic(nil)
	panicNoArgMap, ok := panicNoArg.(map[string]Value)
	if !ok || panicNoArgMap["message"] != "panic" || panicNoArgMap["type"] != "panic" || panicNoArgMap["error"] != true {
		t.Fatalf("builtinPanic nil args: got=%#v", panicNoArg)
	}
	panicWithArg := i.builtinPanic([]Value{123})
	panicWithArgMap, ok := panicWithArg.(map[string]Value)
	if !ok || panicWithArgMap["message"] != "123" {
		t.Fatalf("builtinPanic non-string arg: got=%#v", panicWithArg)
	}
}

func TestBuiltin_ZeroCoverage_Batch82_EqualHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if !i.equal(nil, nil) || i.equal(nil, 1) {
		t.Fatalf("helper equal nil cases failed")
	}
	if !i.equal(int(1), float64(1)) || !i.equal(int64(2), int(2)) || i.equal(int64(2), float64(3)) {
		t.Fatalf("helper equal numeric coercion failed")
	}
	if !i.equal([]Value{1, "x"}, []Value{1, "x"}) || i.equal([]Value{1}, []Value{2}) {
		t.Fatalf("helper equal deep-equal slice cases failed")
	}

	if !i.isEqual(nil, nil) || i.isEqual(nil, 0) {
		t.Fatalf("isEqual nil handling failed")
	}
	if !i.isEqual(true, true) || i.isEqual(true, false) {
		t.Fatalf("isEqual bool handling failed")
	}
	if !i.isEqual([]Value{int64(1), "a"}, []Value{1, "a"}) {
		t.Fatalf("isEqual nested array coercion failed")
	}
	if i.isEqual([]Value{1}, []Value{1, 2}) || i.isEqual([]Value{1}, []Value{2}) {
		t.Fatalf("isEqual array mismatch failed")
	}
	if !i.isEqual(map[string]Value{"a": int64(1)}, map[string]Value{"a": 1}) {
		t.Fatalf("isEqual map coercion failed")
	}
	if i.isEqual(map[string]Value{"a": 1}, map[string]Value{"b": 1}) || i.isEqual(map[string]Value{"a": 1}, map[string]Value{"a": 1, "b": 2}) {
		t.Fatalf("isEqual map mismatch failed")
	}

	type sameFmt struct{ A int }
	if !i.isEqual(sameFmt{A: 1}, sameFmt{A: 1}) {
		t.Fatalf("isEqual fallback equal failed")
	}
	if i.isEqual(sameFmt{A: 1}, sameFmt{A: 2}) {
		t.Fatalf("isEqual fallback non-equal failed")
	}
}
