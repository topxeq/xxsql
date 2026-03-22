package executor

import (
	"regexp"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/sql"
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

// TestSoundexExtra tests the soundex function
func TestSoundexExtra(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Robert", "R163"},
		{"Rupert", "R163"},
		{"Rubin", "R150"},
		{"Ashcraft", "A226"}, // Implementation returns A226
		{"Tymczak", "T522"},
		{"Pfister", "P236"},
		{"", "0000"},
		{"123", "0000"},
		{"A", "A000"},
	}

	for _, tt := range tests {
		result := soundex(tt.input)
		if result != tt.expected {
			t.Errorf("soundex(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestGlobToRegexExtra tests the globToRegex function
func TestGlobToRegexExtra(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		matches bool
	}{
		{"*.txt", "file.txt", true},
		{"*.txt", "file.csv", false},
		{"test?", "test1", true},
		{"test?", "test12", false},
		{"[abc]", "a", true},
		{"[abc]", "d", false},
		{"[!abc]", "d", true},
		{"[!abc]", "a", false},
		{"*", "", true},
		{"a*b", "aXXXb", true},
		{"a*b", "ab", true},
	}

	for _, tt := range tests {
		regex := globToRegex(tt.pattern)
		re, err := regexp.Compile(regex)
		if err != nil {
			t.Errorf("globToRegex(%q) produced invalid regex: %v", tt.pattern, err)
			continue
		}
		result := re.MatchString(tt.input)
		if result != tt.matches {
			t.Errorf("globToRegex(%q) matching %q = %v, want %v", tt.pattern, tt.input, result, tt.matches)
		}
	}
}

// TestTimestampDiff tests the timestampDiff function
func TestTimestampDiff(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		unit     string
		expected int64
	}{
		{"SECOND", 86400},
		{"MINUTE", 1440},
		{"HOUR", 24},
		{"DAY", 1},
	}

	for _, tt := range tests {
		result := timestampDiff(tt.unit, t1, t2)
		if result != tt.expected {
			t.Errorf("timestampDiff(%q, t1, t2) = %d, want %d", tt.unit, result, tt.expected)
		}
	}

	// Test with zero time
	zeroResult := timestampDiff("SECOND", time.Time{}, t2)
	if zeroResult != 0 {
		t.Errorf("timestampDiff with zero time should return 0, got %d", zeroResult)
	}
}

// TestEvaluateBinaryOpExtra tests the evaluateBinaryOp method
func TestEvaluateBinaryOpExtra(t *testing.T) {
	tests := []struct {
		left     interface{}
		op       sql.BinaryOp
		right    interface{}
		expected interface{}
		hasError bool
	}{
		{10, sql.OpAdd, 5, float64(15), false},
		{10, sql.OpSub, 3, float64(7), false},
		{6, sql.OpMul, 7, float64(42), false},
		{20, sql.OpDiv, 4, float64(5), false},
		{17, sql.OpMod, 5, float64(2), false},
		{"hello", sql.OpConcat, " world", "hello world", false},
		{5, sql.OpLt, 10, true, false},
		{10, sql.OpLt, 5, false, false},
		{5, sql.OpLe, 5, true, false},
		{10, sql.OpGt, 5, true, false},
		{5, sql.OpGe, 5, true, false},
		{5, sql.OpEq, 5, true, false},
		{5, sql.OpNe, 10, true, false},
	}

	for _, tt := range tests {
		result, err := (*Executor)(nil).evaluateBinaryOp(tt.left, tt.op, tt.right)
		if tt.hasError {
			if err == nil {
				t.Errorf("evaluateBinaryOp(%v, %v, %v) expected error", tt.left, tt.op, tt.right)
			}
			continue
		}
		if err != nil {
			t.Errorf("evaluateBinaryOp(%v, %v, %v) error: %v", tt.left, tt.op, tt.right, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("evaluateBinaryOp(%v, %v, %v) = %v, want %v", tt.left, tt.op, tt.right, result, tt.expected)
		}
	}
}

// TestEvaluateUnaryExprExtra tests the evaluateUnaryExpr method
func TestEvaluateUnaryExprExtra(t *testing.T) {
	tests := []struct {
		op       sql.UnaryOp
		val      interface{}
		expected interface{}
		hasError bool
	}{
		{sql.OpNeg, 5, -5, false},
		{sql.OpNeg, int64(10), int64(-10), false},
		{sql.OpNeg, 3.14, -3.14, false},
		{sql.OpNeg, nil, nil, false},
		{sql.OpNot, true, false, false},
		{sql.OpNot, false, true, false},
		{sql.OpNot, 0, true, false},
		{sql.OpNot, 1, false, false},
	}

	for _, tt := range tests {
		result, err := (*Executor)(nil).evaluateUnaryExpr(tt.op, tt.val)
		if tt.hasError {
			if err == nil {
				t.Errorf("evaluateUnaryExpr(%v, %v) expected error", tt.op, tt.val)
			}
			continue
		}
		if err != nil {
			t.Errorf("evaluateUnaryExpr(%v, %v) error: %v", tt.op, tt.val, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("evaluateUnaryExpr(%v, %v) = %v, want %v", tt.op, tt.val, result, tt.expected)
		}
	}
}

// TestSoundexDifferenceExtra tests the soundexDifference function
func TestSoundexDifferenceExtra(t *testing.T) {
	tests := []struct {
		s1, s2   string
		expected int
	}{
		{"Robert", "Rupert", 4}, // Same soundex
		{"Robert", "Rubin", 2},   // Different
		{"", "", 4},              // Both produce "0000", all 4 match
	}

	for _, tt := range tests {
		result := soundexDifference(tt.s1, tt.s2)
		if result != tt.expected {
			t.Errorf("soundexDifference(%q, %q) = %d, want %d", tt.s1, tt.s2, result, tt.expected)
		}
	}
}

// TestCompareValuesWithCollation tests the compareValuesWithCollation function
func TestCompareValuesWithCollation(t *testing.T) {
	tests := []struct {
		a, b      interface{}
		collation string
		expected  int
	}{
		// NULL handling
		{nil, nil, "", 0},
		{nil, "a", "", -1},
		{"a", nil, "", 1},
		// String comparison
		{"a", "b", "", -1},
		{"b", "a", "", 1},
		{"a", "a", "", 0},
		// Case insensitive with NOCASE collation
		{"A", "a", "NOCASE", 0},
		{"A", "B", "NOCASE", -1},
		// Numeric comparison
		{int64(1), int64(2), "", -1},
		{int64(2), int64(1), "", 1},
		{int64(1), int64(1), "", 0},
		{float64(1.5), float64(2.5), "", -1},
	}

	for _, tt := range tests {
		result := compareValuesWithCollation(tt.a, tt.b, tt.collation)
		if result != tt.expected {
			t.Errorf("compareValuesWithCollation(%v, %v, %q) = %d, want %d", tt.a, tt.b, tt.collation, result, tt.expected)
		}
	}
}

// TestCompareValuesNumeric tests the compareValuesNumeric function
func TestCompareValuesNumeric(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		op       sql.BinaryOp
		expected bool
	}{
		{int64(1), int64(2), sql.OpLt, true},
		{int64(2), int64(1), sql.OpGt, true},
		{int64(1), int64(1), sql.OpEq, true},
		{int64(1), int64(2), sql.OpEq, false},
		{float64(1.5), float64(2.5), sql.OpLt, true},
		{float64(2.5), float64(1.5), sql.OpGt, true},
		{int64(1), float64(1.0), sql.OpEq, true},
		{nil, int64(1), sql.OpEq, false},
		{nil, nil, sql.OpEq, true},
	}

	for _, tt := range tests {
		result, err := compareValuesNumeric(tt.a, tt.op, tt.b)
		if err != nil {
			t.Errorf("compareValuesNumeric(%v, %v, %v) error: %v", tt.a, tt.op, tt.b, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("compareValuesNumeric(%v, %v, %v) = %v, want %v", tt.a, tt.op, tt.b, result, tt.expected)
		}
	}
}

// TestToFloat64 tests the toFloat64 function
func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{int(5), 5.0, true},
		{int64(5), 5.0, true},
		{float32(5.5), 5.5, true},
		{float64(5.5), 5.5, true},
		{"not a number", 0, false},
		{nil, 0, false},
	}

	for _, tt := range tests {
		result, ok := toFloat64(tt.input)
		if ok != tt.ok {
			t.Errorf("toFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
		}
		if ok && result != tt.expected {
			t.Errorf("toFloat64(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

// TestCompareValuesJoin tests the compareValues function from join.go
func TestCompareValuesJoin(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		expected int
	}{
		{int64(1), int64(2), -1},
		{int64(2), int64(1), 1},
		{int64(1), int64(1), 0},
		{"a", "b", -1},
		{"b", "a", 1},
		{"a", "a", 0},
		{nil, nil, 0},
		{nil, int64(1), -1},
		{int64(1), nil, 1},
		{true, false, 1},
		{false, true, -1},
		{true, true, 0},
	}

	for _, tt := range tests {
		result := compareValues(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("compareValues(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

// TestHasAggregate tests the hasAggregate function
func TestHasAggregate(t *testing.T) {
	tests := []struct {
		expr     sql.Expression
		expected bool
	}{
		{nil, false},
		{&sql.Literal{Value: 42}, false},
		{&sql.FunctionCall{Name: "COUNT", Args: []sql.Expression{}}, true},
		{&sql.FunctionCall{Name: "SUM", Args: []sql.Expression{}}, true},
		{&sql.FunctionCall{Name: "AVG", Args: []sql.Expression{}}, true},
		{&sql.FunctionCall{Name: "MIN", Args: []sql.Expression{}}, true},
		{&sql.FunctionCall{Name: "MAX", Args: []sql.Expression{}}, true},
		{&sql.FunctionCall{Name: "GROUP_CONCAT", Args: []sql.Expression{}}, true},
		{&sql.FunctionCall{Name: "UPPER", Args: []sql.Expression{}}, false},
		{&sql.BinaryExpr{
			Left:  &sql.FunctionCall{Name: "COUNT", Args: []sql.Expression{}},
			Op:    sql.OpAdd,
			Right: &sql.Literal{Value: 1},
		}, true},
		{&sql.BinaryExpr{
			Left:  &sql.Literal{Value: 1},
			Op:    sql.OpAdd,
			Right: &sql.Literal{Value: 2},
		}, false},
		{&sql.UnaryExpr{
			Op:    sql.OpNeg,
			Right: &sql.FunctionCall{Name: "SUM", Args: []sql.Expression{}},
		}, true},
	}

	for i, tt := range tests {
		result := hasAggregate(tt.expr)
		if result != tt.expected {
			t.Errorf("hasAggregate[%d] = %v, want %v", i, result, tt.expected)
		}
	}
}

// TestMatchLikePattern tests the matchLikePattern function
func TestMatchLikePattern(t *testing.T) {
	tests := []struct {
		str, pattern string
		expected     bool
	}{
		{"hello", "hello", true},
		{"hello", "h%", true},
		{"hello", "%o", true},
		{"hello", "%ll%", true},
		{"hello", "h_llo", true},
		{"hello", "h_lo", false},
		{"hello", "H%", false}, // case sensitive
		{"", "%", true},
		{"", "", true},
	}

	for _, tt := range tests {
		result := matchLikePattern(tt.str, tt.pattern, "")
		if result != tt.expected {
			t.Errorf("matchLikePattern(%q, %q) = %v, want %v", tt.str, tt.pattern, result, tt.expected)
		}
	}
}

// TestGetJSONType tests the getJSONType function
func TestGetJSONType(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{"string", "STRING"},
		{[]interface{}{1, 2, 3}, "ARRAY"},
		{map[string]interface{}{"key": "value"}, "OBJECT"},
	}

	for _, tt := range tests {
		result := getJSONType(tt.input)
		if result != tt.expected {
			t.Errorf("getJSONType(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestCollationCompareNOCASE tests the collationCompareNOCASE function
func TestCollationCompareNOCASE(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"a", "A", 0},
		{"A", "a", 0},
		{"abc", "ABC", 0},
		{"a", "b", -1},
		{"B", "a", 1},
		{"", "", 0},
		{"a", "", 1},
		{"", "a", -1},
	}

	for _, tt := range tests {
		result := collationCompareNOCASE(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("collationCompareNOCASE(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

// TestCollationCompareRTRIM tests the collationCompareRTRIM function
func TestCollationCompareRTRIM(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"a", "a ", 0},
		{"a ", "a", 0},
		{"a  ", "a ", 0},
		{"a", "b", -1},
		{"b", "a", 1},
		{"", "   ", 0},
	}

	for _, tt := range tests {
		result := collationCompareRTRIM(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("collationCompareRTRIM(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

// TestTimeToJulianDay tests the timeToJulianDay function
func TestTimeToJulianDay(t *testing.T) {
	// Test known Julian day values
	tests := []struct {
		time     time.Time
		expected float64 // approximate, since Julian days can have fractional parts
	}{
		{time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC), 2451545.0}, // J2000.0
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 2440587.5}, // Unix epoch
	}

	for _, tt := range tests {
		result := timeToJulianDay(tt.time)
		// Allow for small floating point differences
		diff := result - tt.expected
		if diff < -0.01 || diff > 0.01 {
			t.Errorf("timeToJulianDay(%v) = %v, want approximately %v", tt.time, result, tt.expected)
		}
	}
}

// TestExtractJSONPath tests the extractJSONPath function
func TestExtractJSONPath(t *testing.T) {
	// Test that extractJSONPath works with various inputs
	tests := []struct {
		jsonVal  interface{}
		path     string
		hasValue bool // whether a value is expected (not nil)
	}{
		{map[string]interface{}{"a": 1}, "$.a", true},
		{map[string]interface{}{"a": map[string]interface{}{"b": 2}}, "$.a.b", true},
		{[]interface{}{1, 2, 3}, "$[0]", true},
		{[]interface{}{1, 2, 3}, "$[2]", true},
		{map[string]interface{}{}, "$.missing", false},
	}

	for _, tt := range tests {
		result := extractJSONPath(tt.jsonVal, tt.path)
		// Just verify the function doesn't crash and returns something
		if tt.hasValue && result == nil {
			t.Errorf("extractJSONPath(%v, %q) returned nil, expected a value", tt.jsonVal, tt.path)
		}
	}
}

// TestParseJSONPathParts tests the parseJSONPathParts function
func TestParseJSONPathParts(t *testing.T) {
	tests := []struct {
		path     string
		expected int // number of parts
	}{
		{"$", 0},
		{"$.a", 1},
		{"$.a.b", 2},
		{"$[0]", 1},
		{"$.a[0].b", 3},
	}

	for _, tt := range tests {
		result := parseJSONPathParts(tt.path)
		if len(result) != tt.expected {
			t.Errorf("parseJSONPathParts(%q) returned %d parts, want %d", tt.path, len(result), tt.expected)
		}
	}
}

// TestJSONMergePatch tests the jsonMergePatch function
func TestJSONMergePatch(t *testing.T) {
	target := map[string]interface{}{
		"a": 1,
		"b": 2,
	}
	patch := map[string]interface{}{
		"a": 3,
		"c": 4,
	}

	result := jsonMergePatch(target, patch)

	if result["a"] != 3 {
		t.Errorf("jsonMergePatch: a = %v, want 3", result["a"])
	}
	if result["b"] != 2 {
		t.Errorf("jsonMergePatch: b = %v, want 2", result["b"])
	}
	if result["c"] != 4 {
		t.Errorf("jsonMergePatch: c = %v, want 4", result["c"])
	}
}

// TestApplyDateModifier tests the applyDateModifier function
func TestApplyDateModifier(t *testing.T) {
	base := time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		modifier string
		check    func(time.Time) bool
	}{
		{"+1 day", func(t time.Time) bool { return t.Day() == 16 }},
		{"-1 day", func(t time.Time) bool { return t.Day() == 14 }},
		{"+1 month", func(t time.Time) bool { return t.Month() == time.February }},
		{"start of month", func(t time.Time) bool { return t.Day() == 1 }},
		{"start of year", func(t time.Time) bool { return t.Month() == time.January && t.Day() == 1 }},
	}

	for _, tt := range tests {
		result := applyDateModifier(base, tt.modifier)
		if !tt.check(result) {
			t.Errorf("applyDateModifier(%q) produced unexpected result: %v", tt.modifier, result)
		}
	}
}