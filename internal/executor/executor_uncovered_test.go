package executor

import (
	"testing"
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