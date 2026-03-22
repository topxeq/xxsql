package executor

import (
	"testing"

	"github.com/topxeq/xxsql/internal/storage/types"
)

// TestBytesCompare tests the bytesCompare method
func TestBytesCompare(t *testing.T) {
	tests := []struct {
		a, b     []byte
		expected int
	}{
		{[]byte("abc"), []byte("abc"), 0},
		{[]byte("abc"), []byte("abd"), -1},
		{[]byte("abd"), []byte("abc"), 1},
		{[]byte("ab"), []byte("abc"), -1},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte(""), []byte(""), 0},
		{[]byte(""), []byte("a"), -1},
		{[]byte("a"), []byte(""), 1},
	}

	for _, tt := range tests {
		result := (*Executor)(nil).bytesCompare(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("bytesCompare(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

// TestCollationEqual tests the collationEqual function
func TestCollationEqual(t *testing.T) {
	tests := []struct {
		a, b      string
		collation string
		expected  bool
	}{
		{"abc", "abc", "BINARY", true},
		{"abc", "ABC", "BINARY", false},
		{"abc", "ABC", "NOCASE", true},
		{"abc", "abd", "NOCASE", false},
		{"abc  ", "abc", "RTRIM", true},
		{"abc", "abc  ", "RTRIM", true},
		{"  abc", "abc", "RTRIM", false},
	}

	for _, tt := range tests {
		result := collationEqual(tt.a, tt.b, tt.collation)
		if result != tt.expected {
			t.Errorf("collationEqual(%q, %q, %q) = %v, want %v", tt.a, tt.b, tt.collation, result, tt.expected)
		}
	}
}

// TestBoolToInt tests the boolToInt function
func TestBoolToInt(t *testing.T) {
	if boolToInt(true) != 1 {
		t.Error("boolToInt(true) should be 1")
	}
	if boolToInt(false) != 0 {
		t.Error("boolToInt(false) should be 0")
	}
}

// TestToInt64 tests the toInt64 function
func TestToInt64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
		ok       bool
	}{
		{int(42), 42, true},
		{int64(42), 42, true},
		{int32(42), 42, true},
		{float64(42.5), 42, true},
		{float32(42.5), 42, true},
		{"42", 42, true},
		{"not a number", 0, false},
		{true, 1, true},
		{false, 0, true},
		{[]int{}, 0, false},
	}

	for _, tt := range tests {
		result, ok := toInt64(tt.input)
		if ok != tt.ok || (ok && result != tt.expected) {
			t.Errorf("toInt64(%v) = (%d, %v), want (%d, %v)", tt.input, result, ok, tt.expected, tt.ok)
		}
	}
}

// TestToBool tests the toBool function
func TestToBool(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected bool
		ok       bool
	}{
		{true, true, true},
		{false, false, true},
		{int(1), true, true},
		{int(0), false, true},
		{int64(1), true, true},
		{int64(0), false, true},
		{"true", true, true},
		{"false", false, true},
		{"TRUE", true, true},
		{"FALSE", false, true},
		{"on", true, true},
		{"off", false, true},
		{"yes", true, true},
		{"no", false, true},
		{"1", true, true},
		{"0", false, true},
		{"", false, false}, // empty string is not a valid boolean
		{"invalid", false, false},
		{[]int{}, false, false},
	}

	for _, tt := range tests {
		result, ok := toBool(tt.input)
		if ok != tt.ok || (ok && result != tt.expected) {
			t.Errorf("toBool(%v) = (%v, %v), want (%v, %v)", tt.input, result, ok, tt.expected, tt.ok)
		}
	}
}

// TestUDFManagerListFunctions tests UDFManager.ListFunctions
func TestUDFManagerListFunctions(t *testing.T) {
	mgr := NewUDFManager("")

	// Empty list
	if len(mgr.ListFunctions()) != 0 {
		t.Error("ListFunctions should return empty list initially")
	}
}

// TestScriptUDFManager_DropFunction tests ScriptUDFManager.DropFunction
func TestScriptUDFManager_DropFunction(t *testing.T) {
	mgr := NewScriptUDFManager("")

	// Add a function
	_ = mgr.CreateFunction(&ScriptFunction{
		Name:   "test_func",
		Script: "return arg1 * 2",
		Params: []string{"arg1"},
	}, false)

	// Drop it
	err := mgr.DropFunction("test_func")
	if err != nil {
		t.Errorf("DropFunction failed: %v", err)
	}

	// Should not exist
	if mgr.Exists("test_func") {
		t.Error("Function should not exist after drop")
	}

	// Drop non-existent
	err = mgr.DropFunction("nonexistent")
	if err == nil {
		t.Error("DropFunction should fail for non-existent function")
	}
}

// TestScriptUDFManager_ListFunctions tests ListFunctions
func TestScriptUDFManager_ListFunctions(t *testing.T) {
	mgr := NewScriptUDFManager("")

	// Empty list
	if len(mgr.ListFunctions()) != 0 {
		t.Error("ListFunctions should return empty list initially")
	}

	// Add functions
	_ = mgr.CreateFunction(&ScriptFunction{Name: "func1", Script: "return 1"}, false)
	_ = mgr.CreateFunction(&ScriptFunction{Name: "func2", Script: "return 2"}, false)

	list := mgr.ListFunctions()
	if len(list) != 2 {
		t.Errorf("ListFunctions returned %d functions, want 2", len(list))
	}
}

// TestScriptUDFManager_Exists tests Exists
func TestScriptUDFManager_Exists(t *testing.T) {
	mgr := NewScriptUDFManager("")

	if mgr.Exists("nonexistent") {
		t.Error("Exists should return false for non-existent function")
	}

	_ = mgr.CreateFunction(&ScriptFunction{Name: "test_func", Script: "return 1"}, false)
	if !mgr.Exists("test_func") {
		t.Error("Exists should return true for registered function")
	}
}

// TestScriptUDFManager_Load tests Load
func TestScriptUDFManager_Load(t *testing.T) {
	mgr := NewScriptUDFManager("")

	// Load with no data dir should succeed
	err := mgr.Load()
	if err != nil {
		t.Errorf("Load failed: %v", err)
	}
}

// TestScriptUDFManager_Save tests Save
func TestScriptUDFManager_Save(t *testing.T) {
	mgr := NewScriptUDFManager("")

	// Save with no data dir should succeed
	err := mgr.Save()
	if err != nil {
		t.Errorf("Save with empty dataDir failed: %v", err)
	}
}

// TestScriptUDFManager_GetFunction tests GetFunction
func TestScriptUDFManager_GetFunction(t *testing.T) {
	mgr := NewScriptUDFManager("")

	// Non-existent
	fn, exists := mgr.GetFunction("nonexistent")
	if exists || fn != nil {
		t.Error("GetFunction should return nil for non-existent function")
	}

	// Add a function
	_ = mgr.CreateFunction(&ScriptFunction{
		Name:   "test_func",
		Script: "return 1",
	}, false)

	fn, exists = mgr.GetFunction("test_func")
	if !exists || fn == nil {
		t.Error("GetFunction should return function")
	}
	if fn.Name != "test_func" {
		t.Errorf("Function name = %q, want 'test_func'", fn.Name)
	}
}

// TestUDFManager_DropFunctionExtra tests UDFManager.DropFunction
func TestUDFManager_DropFunctionExtra(t *testing.T) {
	mgr := NewUDFManager("")

	// Drop non-existent
	err := mgr.DropFunction("nonexistent")
	if err == nil {
		t.Error("DropFunction should fail for non-existent function")
	}
}

// TestUDFManager_Exists tests UDFManager.Exists
func TestUDFManager_Exists(t *testing.T) {
	mgr := NewUDFManager("")

	if mgr.Exists("nonexistent") {
		t.Error("Exists should return false for non-existent function")
	}
}

// TestUDFManager_GetFunction tests UDFManager.GetFunction
func TestUDFManager_GetFunction(t *testing.T) {
	mgr := NewUDFManager("")

	fn, exists := mgr.GetFunction("nonexistent")
	if exists || fn != nil {
		t.Error("GetFunction should return nil for non-existent function")
	}
}

// TestUDFManager_Save tests UDFManager.Save
func TestUDFManager_Save(t *testing.T) {
	mgr := NewUDFManager("")

	// Save with no data dir should succeed
	err := mgr.Save()
	if err != nil {
		t.Errorf("Save with empty dataDir failed: %v", err)
	}
}

// TestValuesEqual tests the valuesEqual method
func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a, b     types.Value
		expected bool
	}{
		{
			name:     "both null",
			a:        types.NewNullValue(),
			b:        types.NewNullValue(),
			expected: true,
		},
		{
			name:     "one null",
			a:        types.NewNullValue(),
			b:        types.NewIntValue(1),
			expected: false,
		},
		{
			name:     "both non-null equal int",
			a:        types.NewIntValue(42),
			b:        types.NewIntValue(42),
			expected: true,
		},
		{
			name:     "both non-null not equal int",
			a:        types.NewIntValue(1),
			b:        types.NewIntValue(2),
			expected: false,
		},
		{
			name:     "string equal",
			a:        types.NewStringValue("hello", types.TypeVarchar),
			b:        types.NewStringValue("hello", types.TypeVarchar),
			expected: true,
		},
		{
			name:     "string not equal",
			a:        types.NewStringValue("hello", types.TypeVarchar),
			b:        types.NewStringValue("world", types.TypeVarchar),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := (*Executor)(nil).valuesEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("valuesEqual() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJsonExtractExtra tests the jsonExtract function
func TestJsonExtractExtra(t *testing.T) {
	tests := []struct {
		name     string
		jsonStr  string
		path     string
		hasError bool
	}{
		{
			name:     "root path",
			jsonStr:  `{"a": 1}`,
			path:     "$",
			hasError: false,
		},
		{
			name:     "simple field",
			jsonStr:  `{"a": 1}`,
			path:     "$.a",
			hasError: false,
		},
		{
			name:     "nested field",
			jsonStr:  `{"a": {"b": 2}}`,
			path:     "$.a.b",
			hasError: false,
		},
		{
			name:     "array index",
			jsonStr:  `[1, 2, 3]`,
			path:     "$[1]",
			hasError: false,
		},
		{
			name:     "invalid path",
			jsonStr:  `{"a": 1}`,
			path:     "invalid",
			hasError: true,
		},
		{
			name:     "invalid json",
			jsonStr:  `{invalid}`,
			path:     "$",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := jsonExtract(tt.jsonStr, tt.path)
			if tt.hasError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestJsonTypeExtra tests the jsonType function
func TestJsonTypeExtra(t *testing.T) {
	tests := []struct {
		jsonStr  string
		expected string
	}{
		{`null`, "NULL"},
		{`true`, "BOOLEAN"},
		{`false`, "BOOLEAN"},
		{`42`, "INTEGER"},
		{`3.14`, "INTEGER"}, // jsonType treats all numbers as INTEGER
		{`"hello"`, "STRING"}, // jsonType returns STRING, not TEXT
		{`[]`, "ARRAY"},
		{`{}`, "OBJECT"},
		{`invalid`, "INVALID"},
	}

	for _, tt := range tests {
		result := jsonType(tt.jsonStr)
		if result != tt.expected {
			t.Errorf("jsonType(%q) = %q, want %q", tt.jsonStr, result, tt.expected)
		}
	}
}

// TestJsonContainsExtra tests the jsonContains function
func TestJsonContainsExtra(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		candidate string
		expected  bool
	}{
		{
			name:      "exact match",
			target:    `{"a": 1}`,
			candidate: `{"a": 1}`,
			expected:  true,
		},
		{
			name:      "subset keys",
			target:    `{"a": 1, "b": 2}`,
			candidate: `{"a": 1}`,
			expected:  true,
		},
		{
			name:      "array contains",
			target:    `[1, 2, 3]`,
			candidate: `2`,
			expected:  true,
		},
		{
			name:      "not contained",
			target:    `{"a": 1}`,
			candidate: `{"b": 2}`,
			expected:  false,
		},
		{
			name:      "invalid json",
			target:    `invalid`,
			candidate: `1`,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := jsonContains(tt.target, tt.candidate)
			if result != tt.expected {
				t.Errorf("jsonContains() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJsonKeysExtra tests the jsonKeys function
func TestJsonKeysExtra(t *testing.T) {
	tests := []struct {
		name     string
		jsonStr  string
		expected int // number of keys
		hasError bool
	}{
		{
			name:     "simple object",
			jsonStr:  `{"b": 2, "a": 1}`,
			expected: 2,
			hasError: false,
		},
		{
			name:     "empty object",
			jsonStr:  `{}`,
			expected: 0,
			hasError: false,
		},
		{
			name:     "not an object",
			jsonStr:  `[1, 2, 3]`,
			expected: -1, // nil result
			hasError: false,
		},
		{
			name:     "invalid json",
			jsonStr:  `invalid`,
			expected: 0,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonKeys(tt.jsonStr)
			if tt.hasError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if tt.expected < 0 {
				if result != nil {
					t.Errorf("jsonKeys() should return nil for non-object, got %v", result)
				}
			} else if len(result) != tt.expected {
				t.Errorf("jsonKeys() length = %d, want %d", len(result), tt.expected)
			}
		})
	}
}

// TestJsonLengthExtra tests the jsonLength function
func TestJsonLengthExtra(t *testing.T) {
	tests := []struct {
		jsonStr  string
		expected int64
	}{
		{`[1, 2, 3]`, 3},
		{`[]`, 0},
		{`{"a": 1, "b": 2}`, 2},
		{`{}`, 0},
		{`"string"`, 6},
		{`invalid`, 0},
	}

	for _, tt := range tests {
		result := jsonLength(tt.jsonStr)
		if result != tt.expected {
			t.Errorf("jsonLength(%q) = %d, want %d", tt.jsonStr, result, tt.expected)
		}
	}
}

// TestJsonMergePatch tests the jsonMergePatch function
func TestJsonMergePatch(t *testing.T) {
	tests := []struct {
		name        string
		target      map[string]interface{}
		patch       map[string]interface{}
		checkKey    string
		checkValue  interface{}
	}{
		{
			name:       "add field",
			target:     map[string]interface{}{"a": 1},
			patch:      map[string]interface{}{"b": 2},
			checkKey:   "b",
			checkValue: 2,
		},
		{
			name:       "replace field",
			target:     map[string]interface{}{"a": 1},
			patch:      map[string]interface{}{"a": 2},
			checkKey:   "a",
			checkValue: 2,
		},
		{
			name:       "delete field with null",
			target:     map[string]interface{}{"a": 1, "b": 2},
			patch:      map[string]interface{}{"a": nil},
			checkKey:   "a",
			checkValue: nil, // should not exist
		},
		{
			name:        "nested merge",
			target:      map[string]interface{}{"obj": map[string]interface{}{"x": 1, "y": 2}},
			patch:       map[string]interface{}{"obj": map[string]interface{}{"y": 3, "z": 4}},
			checkKey:    "obj",
			checkValue:  "map", // just check it exists and is a map
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := jsonMergePatch(tt.target, tt.patch)
			if tt.checkValue == nil {
				// Check key doesn't exist
				if _, exists := result[tt.checkKey]; exists {
					t.Errorf("jsonMergePatch() should not have key %q", tt.checkKey)
				}
				return
			}
			if tt.checkValue == "map" {
				// Check it's a nested map
				if obj, ok := result[tt.checkKey].(map[string]interface{}); !ok {
					t.Errorf("jsonMergePatch()[%q] should be a map", tt.checkKey)
				} else {
					// Verify nested values
					if obj["y"] != 3 {
						t.Errorf("nested obj['y'] = %v, want 3", obj["y"])
					}
				}
				return
			}
			if result[tt.checkKey] != tt.checkValue {
				t.Errorf("jsonMergePatch()[%q] = %v, want %v", tt.checkKey, result[tt.checkKey], tt.checkValue)
			}
		})
	}
}

// TestJsonEqual tests the jsonEqual function
func TestJsonEqual(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		expected bool
	}{
		{1, 1, true},
		{1, 2, false},
		{"hello", "hello", true},
		{[]interface{}{1, 2}, []interface{}{1, 2}, true},
		{map[string]interface{}{"a": 1}, map[string]interface{}{"a": 1}, true},
	}

	for _, tt := range tests {
		result := jsonEqual(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("jsonEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
		}
	}
}

// TestCompareEqual tests the compareEqual function
func TestCompareEqual(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		expected bool
	}{
		{nil, nil, true},
		{nil, 1, false},
		{1, nil, false},
		{1, 1, true},
		{1, 2, false},
		{"hello", "hello", true},
		{int64(42), int64(42), true},
		{float64(3.14), float64(3.14), true},
	}

	for _, tt := range tests {
		result := compareEqual(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("compareEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
		}
	}
}

// TestCollationCompare tests the collationCompare function
func TestCollationCompare(t *testing.T) {
	tests := []struct {
		a, b      string
		collation string
		expected  int
	}{
		{"abc", "abc", "BINARY", 0},
		{"abc", "abd", "BINARY", -1},
		{"abd", "abc", "BINARY", 1},
		{"abc", "ABC", "NOCASE", 0},
		{"abc  ", "abc", "RTRIM", 0},
	}

	for _, tt := range tests {
		result := collationCompare(tt.a, tt.b, tt.collation)
		if result != tt.expected {
			t.Errorf("collationCompare(%q, %q, %q) = %d, want %d", tt.a, tt.b, tt.collation, result, tt.expected)
		}
	}
}

// TestBytesEqual tests the bytesEqual function
func TestBytesEqual(t *testing.T) {
	tests := []struct {
		a, b     []byte
		expected bool
	}{
		{[]byte("abc"), []byte("abc"), true},
		{[]byte("abc"), []byte("abd"), false},
		{[]byte{}, []byte{}, true},
		{nil, nil, true},
		{[]byte("a"), nil, false},
		{nil, []byte("a"), false},
	}

	for _, tt := range tests {
		result := (*Executor)(nil).bytesEqual(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("bytesEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
		}
	}
}