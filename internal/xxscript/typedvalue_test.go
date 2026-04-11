package xxscript

import (
	"testing"
)

func TestNewTypedValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected ValueType
	}{
		{nil, TypeNull},
		{true, TypeBool},
		{int(1), TypeInt},
		{int64(1), TypeInt64},
		{float64(1.0), TypeFloat},
		{"hello", TypeString},
		{[]Value{int64(1)}, TypeArray},
		{map[string]Value{"a": int64(1)}, TypeMap},
		{&UserFunc{}, TypeFunction},
		{struct{}{}, TypeObject},
	}

	for _, tt := range tests {
		tv := NewTypedValue(tt.input)
		if tv.Type != tt.expected {
			t.Errorf("NewTypedValue(%v).Type = %v, want %v", tt.input, tv.Type, tt.expected)
		}
	}
}

func TestTypedValue_IsTruthy(t *testing.T) {
	tests := []struct {
		tv       TypedValue
		expected bool
	}{
		{TypedValue{Type: TypeNull}, false},
		{TypedValue{Type: TypeBool, Value: true}, true},
		{TypedValue{Type: TypeBool, Value: false}, false},
		{TypedValue{Type: TypeInt, Value: int(1)}, true},
		{TypedValue{Type: TypeInt, Value: int(0)}, false},
		{TypedValue{Type: TypeInt64, Value: int64(1)}, true},
		{TypedValue{Type: TypeInt64, Value: int64(0)}, false},
		{TypedValue{Type: TypeFloat, Value: float64(1.0)}, true},
		{TypedValue{Type: TypeFloat, Value: float64(0.0)}, false},
		{TypedValue{Type: TypeString, Value: "hello"}, true},
		{TypedValue{Type: TypeString, Value: ""}, false},
		{TypedValue{Type: TypeArray, Value: []Value{int64(1)}}, true},
		{TypedValue{Type: TypeArray, Value: []Value{}}, false},
		{TypedValue{Type: TypeMap, Value: map[string]Value{"a": int64(1)}}, true},
		{TypedValue{Type: TypeMap, Value: map[string]Value{}}, false},
		{TypedValue{Type: TypeObject, Value: "something"}, true},
	}

	for _, tt := range tests {
		result := tt.tv.IsTruthy()
		if result != tt.expected {
			t.Errorf("TypedValue{%v, %v}.IsTruthy() = %v, want %v", tt.tv.Type, tt.tv.Value, result, tt.expected)
		}
	}
}

func TestTypedValue_ToInt(t *testing.T) {
	tests := []struct {
		tv       TypedValue
		expected int
	}{
		{TypedValue{Type: TypeInt, Value: int(42)}, 42},
		{TypedValue{Type: TypeInt64, Value: int64(42)}, 42},
		{TypedValue{Type: TypeFloat, Value: float64(3.9)}, 3},
		{TypedValue{Type: TypeBool, Value: true}, 1},
		{TypedValue{Type: TypeBool, Value: false}, 0},
		{TypedValue{Type: TypeString, Value: "123"}, 123},
		{TypedValue{Type: TypeString, Value: "notanumber"}, 0},
		{TypedValue{Type: TypeNull}, 0},
	}

	for _, tt := range tests {
		result := tt.tv.ToInt()
		if result != tt.expected {
			t.Errorf("TypedValue{%v, %v}.ToInt() = %d, want %d", tt.tv.Type, tt.tv.Value, result, tt.expected)
		}
	}
}

func TestTypedValue_ToFloat(t *testing.T) {
	tests := []struct {
		tv       TypedValue
		expected float64
	}{
		{TypedValue{Type: TypeInt, Value: int(42)}, 42.0},
		{TypedValue{Type: TypeInt64, Value: int64(42)}, 42.0},
		{TypedValue{Type: TypeFloat, Value: float64(3.14)}, 3.14},
		{TypedValue{Type: TypeBool, Value: true}, 1.0},
		{TypedValue{Type: TypeBool, Value: false}, 0.0},
		{TypedValue{Type: TypeNull}, 0.0},
	}

	for _, tt := range tests {
		result := tt.tv.ToFloat()
		if result != tt.expected {
			t.Errorf("TypedValue{%v, %v}.ToFloat() = %f, want %f", tt.tv.Type, tt.tv.Value, result, tt.expected)
		}
	}
}

func TestTypedValue_ToString(t *testing.T) {
	tests := []struct {
		tv       TypedValue
		expected string
	}{
		{TypedValue{Type: TypeNull}, "null"},
		{TypedValue{Type: TypeString, Value: "hello"}, "hello"},
		{TypedValue{Type: TypeInt, Value: int(42)}, "42"},
		{TypedValue{Type: TypeInt64, Value: int64(42)}, "42"},
		{TypedValue{Type: TypeFloat, Value: float64(3.14)}, "3.14"},
		{TypedValue{Type: TypeBool, Value: true}, "true"},
	}

	for _, tt := range tests {
		result := tt.tv.ToString()
		if result != tt.expected {
			t.Errorf("TypedValue{%v, %v}.ToString() = %q, want %q", tt.tv.Type, tt.tv.Value, result, tt.expected)
		}
	}
}

func TestFastAdd(t *testing.T) {
	tests := []struct {
		a, b     TypedValue
		expected TypedValue
		ok       bool
	}{
		{TypedValue{Type: TypeInt, Value: int(1)}, TypedValue{Type: TypeInt, Value: int(2)}, TypedValue{Type: TypeInt, Value: int(3)}, true},
		{TypedValue{Type: TypeInt64, Value: int64(1)}, TypedValue{Type: TypeInt64, Value: int64(2)}, TypedValue{Type: TypeInt64, Value: int64(3)}, true},
		{TypedValue{Type: TypeFloat, Value: float64(1.0)}, TypedValue{Type: TypeFloat, Value: float64(2.0)}, TypedValue{Type: TypeFloat, Value: float64(3.0)}, true},
		{TypedValue{Type: TypeString, Value: "a"}, TypedValue{Type: TypeString, Value: "b"}, TypedValue{Type: TypeString, Value: "ab"}, true},
		{TypedValue{Type: TypeInt, Value: int(1)}, TypedValue{Type: TypeFloat, Value: float64(2.0)}, TypedValue{Type: TypeFloat, Value: float64(3.0)}, true},
		{TypedValue{Type: TypeBool, Value: true}, TypedValue{Type: TypeBool, Value: false}, TypedValue{}, false},
	}

	for _, tt := range tests {
		result, ok := fastAdd(tt.a, tt.b)
		if ok != tt.ok {
			t.Errorf("fastAdd(%v, %v) ok = %v, want %v", tt.a, tt.b, ok, tt.ok)
		}
		if ok && result.Type != tt.expected.Type {
			t.Errorf("fastAdd(%v, %v).Type = %v, want %v", tt.a, tt.b, result.Type, tt.expected.Type)
		}
	}
}

func TestFastSub(t *testing.T) {
	tests := []struct {
		a, b     TypedValue
		expected TypedValue
		ok       bool
	}{
		{TypedValue{Type: TypeInt, Value: int(5)}, TypedValue{Type: TypeInt, Value: int(3)}, TypedValue{Type: TypeInt, Value: int(2)}, true},
		{TypedValue{Type: TypeInt64, Value: int64(5)}, TypedValue{Type: TypeInt64, Value: int64(3)}, TypedValue{Type: TypeInt64, Value: int64(2)}, true},
		{TypedValue{Type: TypeFloat, Value: float64(5.0)}, TypedValue{Type: TypeFloat, Value: float64(3.0)}, TypedValue{Type: TypeFloat, Value: float64(2.0)}, true},
		{TypedValue{Type: TypeInt, Value: int(5)}, TypedValue{Type: TypeFloat, Value: float64(3.0)}, TypedValue{Type: TypeFloat, Value: float64(2.0)}, true},
		{TypedValue{Type: TypeBool, Value: true}, TypedValue{Type: TypeBool, Value: false}, TypedValue{}, false},
	}

	for _, tt := range tests {
		_, ok := fastSub(tt.a, tt.b)
		if ok != tt.ok {
			t.Errorf("fastSub(%v, %v) ok = %v, want %v", tt.a, tt.b, ok, tt.ok)
		}
	}
}

func TestFastMul(t *testing.T) {
	tests := []struct {
		a, b     TypedValue
		expected TypedValue
		ok       bool
	}{
		{TypedValue{Type: TypeInt, Value: int(3)}, TypedValue{Type: TypeInt, Value: int(4)}, TypedValue{Type: TypeInt, Value: int(12)}, true},
		{TypedValue{Type: TypeInt64, Value: int64(3)}, TypedValue{Type: TypeInt64, Value: int64(4)}, TypedValue{Type: TypeInt64, Value: int64(12)}, true},
		{TypedValue{Type: TypeFloat, Value: float64(3.0)}, TypedValue{Type: TypeFloat, Value: float64(4.0)}, TypedValue{Type: TypeFloat, Value: float64(12.0)}, true},
		{TypedValue{Type: TypeInt, Value: int(3)}, TypedValue{Type: TypeFloat, Value: float64(4.0)}, TypedValue{Type: TypeFloat, Value: float64(12.0)}, true},
		{TypedValue{Type: TypeBool, Value: true}, TypedValue{Type: TypeBool, Value: false}, TypedValue{}, false},
	}

	for _, tt := range tests {
		_, ok := fastMul(tt.a, tt.b)
		if ok != tt.ok {
			t.Errorf("fastMul(%v, %v) ok = %v, want %v", tt.a, tt.b, ok, tt.ok)
		}
	}
}

func TestFastDiv(t *testing.T) {
	a := TypedValue{Type: TypeInt, Value: int(10)}
	b := TypedValue{Type: TypeInt, Value: int(2)}
	result, ok := fastDiv(a, b)
	if !ok {
		t.Fatal("fastDiv expected ok")
	}
	if result.Type != TypeFloat {
		t.Errorf("Expected TypeFloat, got %v", result.Type)
	}

	zero := TypedValue{Type: TypeInt, Value: int(0)}
	_, ok = fastDiv(a, zero)
	if ok {
		t.Error("fastDiv by zero should fail")
	}
}

func TestFastCompare(t *testing.T) {
	tests := []struct {
		a, b     TypedValue
		expected int
		ok       bool
	}{
		{TypedValue{Type: TypeInt, Value: int(1)}, TypedValue{Type: TypeInt, Value: int(2)}, -1, true},
		{TypedValue{Type: TypeInt, Value: int(2)}, TypedValue{Type: TypeInt, Value: int(1)}, 1, true},
		{TypedValue{Type: TypeInt, Value: int(1)}, TypedValue{Type: TypeInt, Value: int(1)}, 0, true},
		{TypedValue{Type: TypeInt64, Value: int64(1)}, TypedValue{Type: TypeInt64, Value: int64(2)}, -1, true},
		{TypedValue{Type: TypeFloat, Value: float64(1.0)}, TypedValue{Type: TypeFloat, Value: float64(2.0)}, -1, true},
		{TypedValue{Type: TypeString, Value: "a"}, TypedValue{Type: TypeString, Value: "b"}, -1, true},
		{TypedValue{Type: TypeString, Value: "b"}, TypedValue{Type: TypeString, Value: "a"}, 1, true},
		{TypedValue{Type: TypeString, Value: "a"}, TypedValue{Type: TypeString, Value: "a"}, 0, true},
		{TypedValue{Type: TypeInt, Value: int(1)}, TypedValue{Type: TypeFloat, Value: float64(2.0)}, -1, true},
		{TypedValue{Type: TypeBool, Value: true}, TypedValue{Type: TypeBool, Value: false}, 0, false},
	}

	for _, tt := range tests {
		result, ok := fastCompare(tt.a, tt.b)
		if ok != tt.ok {
			t.Errorf("fastCompare(%v, %v) ok = %v, want %v", tt.a, tt.b, ok, tt.ok)
		}
		if ok && result != tt.expected {
			t.Errorf("fastCompare(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		t        ValueType
		expected bool
	}{
		{TypeInt, true},
		{TypeInt64, true},
		{TypeFloat, true},
		{TypeBool, false},
		{TypeString, false},
		{TypeNull, false},
		{TypeArray, false},
		{TypeMap, false},
	}

	for _, tt := range tests {
		result := isNumeric(tt.t)
		if result != tt.expected {
			t.Errorf("isNumeric(%v) = %v, want %v", tt.t, result, tt.expected)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"123", 123},
		{"-456", -456},
		{"+789", 789},
		{"0", 0},
		{"", 0},
		{"abc", 0},
		{"123abc", 123},
	}

	for _, tt := range tests {
		var n int
		err := parseInt(tt.input, &n)
		if err != nil {
			t.Errorf("parseInt(%q) error: %v", tt.input, err)
		}
		if n != tt.expected {
			t.Errorf("parseInt(%q) = %d, want %d", tt.input, n, tt.expected)
		}
	}
}
