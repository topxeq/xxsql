package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch45_ConvertJSONValueAndDeepMerge(t *testing.T) {
	if got := convertJSONValue(nil); got != nil {
		t.Fatalf("expected nil conversion, got %v", got)
	}
	if got := convertJSONValue(true); got != true {
		t.Fatalf("expected bool conversion, got %v", got)
	}
	if got := convertJSONValue(1.5); got != 1.5 {
		t.Fatalf("expected float conversion, got %v", got)
	}
	if got := convertJSONValue("x"); got != "x" {
		t.Fatalf("expected string conversion, got %v", got)
	}

	arr := convertJSONValue([]interface{}{float64(1), "a", nil}).([]Value)
	if len(arr) != 3 || arr[0] != 1.0 || arr[1] != "a" || arr[2] != nil {
		t.Fatalf("unexpected array conversion: %v", arr)
	}

	m := convertJSONValue(map[string]interface{}{"a": float64(1), "b": []interface{}{float64(2)}}).(map[string]Value)
	if m["a"] != 1.0 || len(m["b"].([]Value)) != 1 {
		t.Fatalf("unexpected map conversion: %v", m)
	}

	type local struct{ A int }
	if got := convertJSONValue(local{A: 3}); got != "{3}" {
		t.Fatalf("expected fallback fmt conversion, got %v", got)
	}

	if got := deepMergeJSON(nil, map[string]interface{}{"a": 1}).(map[string]interface{}); got["a"] != 1 {
		t.Fatalf("expected deepMerge nil-base overlay result, got %v", got)
	}
	if got := deepMergeJSON(map[string]interface{}{"a": 1}, nil).(map[string]interface{}); got["a"] != 1 {
		t.Fatalf("expected deepMerge nil-overlay base result, got %v", got)
	}

	base := map[string]interface{}{"a": 1, "nest": map[string]interface{}{"x": 1, "y": 2}}
	overlay := map[string]interface{}{"b": 2, "nest": map[string]interface{}{"y": 9, "z": 3}}
	merged := deepMergeJSON(base, overlay).(map[string]interface{})
	if merged["a"] != 1 || merged["b"] != 2 {
		t.Fatalf("unexpected top-level deepMerge result: %v", merged)
	}
	n := merged["nest"].(map[string]interface{})
	if n["x"] != 1 || n["y"] != 9 || n["z"] != 3 {
		t.Fatalf("unexpected nested deepMerge result: %v", n)
	}

	if got := deepMergeJSON("base", "overlay"); got != "overlay" {
		t.Fatalf("expected overlay win for non-map merge, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch45_SetJSONByPath(t *testing.T) {
	if got := setJSONByPath(map[string]interface{}{"a": 1.0}, "", 9.0); got != 9.0 {
		t.Fatalf("expected empty path to return value directly, got %v", got)
	}

	// Non-map input should create a new object root.
	created := setJSONByPath("bad", "a", 7.0).(map[string]Value)
	if created["a"] != 7.0 {
		t.Fatalf("expected created root key path, got %v", created)
	}

	// map[string]Value input should be converted and updated.
	fromValueMap := setJSONByPath(map[string]Value{"root": map[string]Value{"x": 1.0}}, "root", 2.0).(map[string]Value)
	if fromValueMap["root"] != 2.0 {
		t.Fatalf("expected map[string]Value conversion/update, got %v", fromValueMap)
	}

	// Array extension branch while walking intermediate parts.
	arrRoot := map[string]interface{}{"items": []interface{}{}}
	arrRes := setJSONByPath(arrRoot, "items[2].name", "v").(map[string]Value)
	if _, ok := arrRes["items"]; !ok {
		t.Fatalf("expected items key to exist after nested set, got %v", arrRes)
	}

	// Final array assignment branch.
	finalArr := setJSONByPath(map[string]interface{}{"items": []interface{}{0.0, 1.0, 2.0}}, "items[1]", 9.0).(map[string]Value)
	if finalArr["items"].([]Value)[1] != 9.0 {
		t.Fatalf("expected final array index set, got %v", finalArr)
	}

	// Out-of-range final array index should leave array unchanged.
	unchanged := setJSONByPath(map[string]interface{}{"items": []interface{}{0.0, 1.0}}, "items[5]", 9.0).(map[string]Value)
	vals := unchanged["items"].([]Value)
	if len(vals) != 2 || vals[0] != 0.0 || vals[1] != 1.0 {
		t.Fatalf("expected unchanged array for out-of-range final index, got %v", unchanged)
	}
}
