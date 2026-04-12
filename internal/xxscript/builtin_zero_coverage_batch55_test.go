package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch55_JSONCollectionBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinJSONKeys([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("builtinJSONKeys no-args: expected empty, got %v", got)
	}
	if got := i.builtinJSONKeys([]Value{"{"}).([]Value); len(got) != 0 {
		t.Fatalf("builtinJSONKeys invalid json: expected empty, got %v", got)
	}
	if got := i.builtinJSONKeys([]Value{[]Value{"a", "b"}}).([]Value); len(got) != 0 {
		t.Fatalf("builtinJSONKeys non-map: expected empty, got %v", got)
	}
	keys1 := i.builtinJSONKeys([]Value{map[string]interface{}{"a": 1, "b": 2}}).([]Value)
	if len(keys1) != 2 {
		t.Fatalf("builtinJSONKeys map[string]interface{}: expected 2 keys, got %v", keys1)
	}
	keys2 := i.builtinJSONKeys([]Value{map[string]Value{"x": 1, "y": 2}}).([]Value)
	if len(keys2) != 2 {
		t.Fatalf("builtinJSONKeys map[string]Value: expected 2 keys, got %v", keys2)
	}

	if got := i.builtinJSONValues([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("builtinJSONValues no-args: expected empty, got %v", got)
	}
	if got := i.builtinJSONValues([]Value{"{"}).([]Value); len(got) != 0 {
		t.Fatalf("builtinJSONValues invalid json: expected empty, got %v", got)
	}
	if got := i.builtinJSONValues([]Value{[]Value{"a", "b"}}).([]Value); len(got) != 0 {
		t.Fatalf("builtinJSONValues non-map: expected empty, got %v", got)
	}
	vals1 := i.builtinJSONValues([]Value{map[string]interface{}{"a": float64(1), "b": "x"}}).([]Value)
	if len(vals1) != 2 {
		t.Fatalf("builtinJSONValues map[string]interface{}: expected 2 values, got %v", vals1)
	}
	vals2 := i.builtinJSONValues([]Value{map[string]Value{"x": 1, "y": "v"}}).([]Value)
	if len(vals2) != 2 {
		t.Fatalf("builtinJSONValues map[string]Value: expected 2 values, got %v", vals2)
	}

	if got := i.builtinJSONType([]Value{}); got != "null" {
		t.Fatalf("builtinJSONType no-args: expected null, got %v", got)
	}
	if got := i.builtinJSONType([]Value{"not-json"}); got != "string" {
		t.Fatalf("builtinJSONType invalid-json-string: expected string, got %v", got)
	}
	if got := i.builtinJSONType([]Value{"123"}); got != "number" {
		t.Fatalf("builtinJSONType numeric json: expected number, got %v", got)
	}
	if got := i.builtinJSONType([]Value{true}); got != "boolean" {
		t.Fatalf("builtinJSONType bool: expected boolean, got %v", got)
	}
	if got := i.builtinJSONType([]Value{nil}); got != "null" {
		t.Fatalf("builtinJSONType typed nil: expected null, got %v", got)
	}
	if got := i.builtinJSONType([]Value{"\"abc\""}); got != "string" {
		t.Fatalf("builtinJSONType JSON string literal: expected string, got %v", got)
	}
	if got := i.builtinJSONType([]Value{[]interface{}{1, 2}}); got != "array" {
		t.Fatalf("builtinJSONType []interface{}: expected array, got %v", got)
	}
	if got := i.builtinJSONType([]Value{[]Value{1, 2}}); got != "array" {
		t.Fatalf("builtinJSONType array: expected array, got %v", got)
	}
	if got := i.builtinJSONType([]Value{map[string]interface{}{"k": "v"}}); got != "object" {
		t.Fatalf("builtinJSONType map[string]interface{}: expected object, got %v", got)
	}
	if got := i.builtinJSONType([]Value{map[string]Value{"k": "v"}}); got != "object" {
		t.Fatalf("builtinJSONType object: expected object, got %v", got)
	}
	if got := i.builtinJSONType([]Value{int64(7)}); got != "int64" {
		t.Fatalf("builtinJSONType fallback type: expected int64, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch55_JSONObjectArrays_OmitPick_AppendBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinJSONObjectFromArrays([]Value{}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONObjectFromArrays short-args: expected empty, got %v", got)
	}
	if got := i.builtinJSONObjectFromArrays([]Value{"[", "[1]"}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONObjectFromArrays invalid keys json: expected empty, got %v", got)
	}
	if got := i.builtinJSONObjectFromArrays([]Value{"[\"a\"]", "["}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONObjectFromArrays invalid values json: expected empty, got %v", got)
	}

	obj1 := i.builtinJSONObjectFromArrays([]Value{[]Value{"a", "b", 3}, []Value{1, 2, 3}}).(map[string]Value)
	if len(obj1) != 2 {
		t.Fatalf("builtinJSONObjectFromArrays []Value inputs: expected 2 keys, got %v", obj1)
	}
	if _, ok := obj1["a"]; !ok {
		t.Fatalf("builtinJSONObjectFromArrays []Value inputs: missing key a, got %v", obj1)
	}
	if _, ok := obj1["b"]; !ok {
		t.Fatalf("builtinJSONObjectFromArrays []Value inputs: missing key b, got %v", obj1)
	}
	obj2 := i.builtinJSONObjectFromArrays([]Value{"[\"x\",\"y\"]", "[10,20,30]"}).(map[string]Value)
	if len(obj2) != 2 || obj2["x"] != float64(10) || obj2["y"] != float64(20) {
		t.Fatalf("builtinJSONObjectFromArrays json-string inputs: expected x/y entries, got %v", obj2)
	}
	obj3 := i.builtinJSONObjectFromArrays([]Value{[]interface{}{"m", "n"}, []interface{}{7, 8}}).(map[string]Value)
	if len(obj3) != 2 {
		t.Fatalf("builtinJSONObjectFromArrays []interface{} inputs: expected 2 keys, got %v", obj3)
	}

	if got := i.builtinJSONOmit([]Value{}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONOmit short-args: expected empty map, got %v", got)
	}
	if got := i.builtinJSONOmit([]Value{"{", "a"}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONOmit invalid json: expected empty map, got %v", got)
	}
	if got := i.builtinJSONOmit([]Value{[]interface{}{"x", "y"}, "a"}).([]Value); len(got) != 2 {
		t.Fatalf("builtinJSONOmit non-map passthrough: expected slice len 2, got %v", got)
	}
	omit := i.builtinJSONOmit([]Value{"{\"a\":1,\"b\":2,\"c\":3}", "b", 7}).(map[string]Value)
	if len(omit) != 2 {
		t.Fatalf("builtinJSONOmit expected 2 keys after omit, got %v", omit)
	}
	if _, ok := omit["b"]; ok {
		t.Fatalf("builtinJSONOmit should remove key b, got %v", omit)
	}

	if got := i.builtinJSONPick([]Value{}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONPick short-args: expected empty map, got %v", got)
	}
	if got := i.builtinJSONPick([]Value{"{", "a"}).(map[string]Value); len(got) != 0 {
		t.Fatalf("builtinJSONPick invalid json: expected empty map, got %v", got)
	}
	if got := i.builtinJSONPick([]Value{[]interface{}{"x", "y"}, "a"}).([]Value); len(got) != 2 {
		t.Fatalf("builtinJSONPick non-map passthrough: expected slice len 2, got %v", got)
	}
	pick := i.builtinJSONPick([]Value{"{\"a\":1,\"b\":2,\"c\":3}", "a", 99, "c"}).(map[string]Value)
	if len(pick) != 2 || pick["a"] != float64(1) || pick["c"] != float64(3) {
		t.Fatalf("builtinJSONPick expected keys a/c, got %v", pick)
	}

	err1 := i.builtinJSONArrayAppend([]Value{}).(map[string]Value)
	if err1["success"] != false {
		t.Fatalf("builtinJSONArrayAppend short-args should fail, got %v", err1)
	}
	err2 := i.builtinJSONArrayAppend([]Value{"[", 1}).(map[string]Value)
	if err2["success"] != false {
		t.Fatalf("builtinJSONArrayAppend invalid json should fail, got %v", err2)
	}
	err3 := i.builtinJSONArrayAppend([]Value{123, 1}).(map[string]Value)
	if err3["success"] != false {
		t.Fatalf("builtinJSONArrayAppend non-array should fail, got %v", err3)
	}
	arr1 := i.builtinJSONArrayAppend([]Value{[]Value{1, 2}, 3, "x"}).([]Value)
	if len(arr1) != 4 || arr1[3] != "x" {
		t.Fatalf("builtinJSONArrayAppend []Value input append failed, got %v", arr1)
	}
	arrIf := i.builtinJSONArrayAppend([]Value{[]interface{}{1, 2}, 3}).([]Value)
	if len(arrIf) != 3 {
		t.Fatalf("builtinJSONArrayAppend []interface{} input append failed, got %v", arrIf)
	}
	arr2 := i.builtinJSONArrayAppend([]Value{"[1,2]", 3}).([]Value)
	if len(arr2) != 3 {
		t.Fatalf("builtinJSONArrayAppend json-string input append failed, got %v", arr2)
	}
}
