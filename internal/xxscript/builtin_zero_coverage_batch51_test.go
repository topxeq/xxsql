package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch51_Validate_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinValidate([]Value{"x"}).(map[string]Value); m["valid"] != false || m["error"] == nil {
		t.Fatalf("expected validate arg error, got %v", m)
	}

	if m := i.builtinValidate([]Value{"x", "not-rules"}).(map[string]Value); m["valid"] != false || m["error"] == nil {
		t.Fatalf("expected validate rules type error, got %v", m)
	}

	rulesAll := map[string]Value{
		"required": true,
		"type":     "string",
		"min":      int64(3),
		"max":      int64(5),
		"pattern":  "^[a-z]+$",
	}

	nilRequired := i.builtinValidate([]Value{nil, rulesAll}).(map[string]Value)
	if nilRequired["valid"] != false || len(nilRequired["errors"].([]Value)) == 0 {
		t.Fatalf("expected required/type validation errors for nil value, got %v", nilRequired)
	}

	good := i.builtinValidate([]Value{"abcd", rulesAll}).(map[string]Value)
	if good["valid"] != true || len(good["errors"].([]Value)) != 0 {
		t.Fatalf("expected valid result for matching value, got %v", good)
	}

	badAll := i.builtinValidate([]Value{"A", rulesAll}).(map[string]Value)
	if badAll["valid"] != false {
		t.Fatalf("expected invalid result for min+pattern failures, got %v", badAll)
	}
	if len(badAll["errors"].([]Value)) < 2 {
		t.Fatalf("expected multiple errors for min+pattern failures, got %v", badAll)
	}

	numberRule := map[string]Value{"type": "number", "min": float64(1), "max": float64(5)}
	if m := i.builtinValidate([]Value{float64(0), numberRule}).(map[string]Value); m["valid"] != false {
		t.Fatalf("expected numeric min failure, got %v", m)
	}
	if m := i.builtinValidate([]Value{float64(10), numberRule}).(map[string]Value); m["valid"] != false {
		t.Fatalf("expected numeric max failure, got %v", m)
	}
	if m := i.builtinValidate([]Value{int64(3), numberRule}).(map[string]Value); m["valid"] != true {
		t.Fatalf("expected int64 numeric type success, got %v", m)
	}

	if m := i.builtinValidate([]Value{"x", map[string]Value{"type": "boolean"}}).(map[string]Value); m["valid"] != false {
		t.Fatalf("expected boolean type failure, got %v", m)
	}
	if m := i.builtinValidate([]Value{[]Value{1}, map[string]Value{"type": "array"}}).(map[string]Value); m["valid"] != true {
		t.Fatalf("expected array type success, got %v", m)
	}
	if m := i.builtinValidate([]Value{map[string]Value{"k": "v"}, map[string]Value{"type": "object"}}).(map[string]Value); m["valid"] != true {
		t.Fatalf("expected object type success, got %v", m)
	}

	invalidPattern := i.builtinValidate([]Value{"abc", map[string]Value{"pattern": "["}}).(map[string]Value)
	if invalidPattern["valid"] != false || len(invalidPattern["errors"].([]Value)) == 0 {
		t.Fatalf("expected invalid regex pattern to produce validation error, got %v", invalidPattern)
	}
}
