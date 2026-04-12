package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch97_DecodeConversionBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinBase64Decode([]Value{123}); got != "" {
		t.Fatalf("builtinBase64Decode non-string expected empty, got %#v", got)
	}
	if got := i.builtinHexDecode([]Value{123}); got != "" {
		t.Fatalf("builtinHexDecode non-string expected empty, got %#v", got)
	}
	if got := i.builtinBase64URLDecode([]Value{123}); got != "" {
		t.Fatalf("builtinBase64URLDecode non-string expected empty, got %#v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch97_BcryptHashAndByHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	need := i.builtinBcryptHash(nil).(map[string]Value)
	if need["success"] != false {
		t.Fatalf("bcrypt hash nil args expected success=false, got %#v", need)
	}

	clamped := i.builtinBcryptHash([]Value{"pw", 1}).(map[string]Value)
	if clamped["success"] != true {
		t.Fatalf("bcrypt hash min-cost clamp expected success=true, got %#v", clamped)
	}

	arr := []Value{
		map[string]Value{"n": "b"},
		map[string]Value{"n": "a"},
		map[string]Value{"n": "c"},
	}

	if got := i.builtinMinBy([]Value{arr, "n"}); !valuesEqual(got, arr[1]) {
		t.Fatalf("builtinMinBy key branch mismatch: got=%#v", got)
	}
	if got := i.builtinMaxBy([]Value{arr, "n"}); !valuesEqual(got, arr[2]) {
		t.Fatalf("builtinMaxBy key branch mismatch: got=%#v", got)
	}

	if got := i.builtinMinBy([]Value{arr, 1}); !valuesEqual(got, arr[0]) {
		t.Fatalf("builtinMinBy non-string keyFn mismatch: got=%#v", got)
	}
	if got := i.builtinMaxBy([]Value{arr, 1}); !valuesEqual(got, arr[0]) {
		t.Fatalf("builtinMaxBy non-string keyFn mismatch: got=%#v", got)
	}

	_ = i.builtinMinBy([]Value{})
	_ = i.builtinMaxBy([]Value{})
	if got := i.builtinMinBy([]Value{[]Value{}, "n"}); got != nil {
		t.Fatalf("builtinMinBy empty array expected nil, got %#v", got)
	}
	if got := i.builtinMaxBy([]Value{[]Value{}, "n"}); got != nil {
		t.Fatalf("builtinMaxBy empty array expected nil, got %#v", got)
	}
}
