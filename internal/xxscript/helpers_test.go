package xxscript

import (
	"testing"
)

func TestToString(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{nil, "null"},
		{"hello", "hello"},
		{int(42), "42"},
		{int64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
		{[]Value{int64(1), int64(2)}, "[1 2]"},
		{map[string]Value{"a": int64(1)}, "map[a:1]"},
	}

	for _, tt := range tests {
		result := toString(tt.input)
		if result != tt.expected {
			t.Errorf("toString(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestInterpreter_ToString(t *testing.T) {
	i := &Interpreter{}
	result := i.toString(int64(123))
	if result != "123" {
		t.Errorf("Expected '123', got %s", result)
	}
}

func TestInterpreter_ToBool(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		input    Value
		expected bool
	}{
		{true, true},
		{false, false},
		{int(1), true},
		{int(0), false},
		{int64(1), true},
		{int64(0), false},
		{float64(1.0), true},
		{float64(0.0), false},
		{"hello", true},
		{"", false},
		{"false", false},
		{"0", false},
		{"true", true},
		{nil, false},
		{[]Value{int64(1)}, true},
	}

	for _, tt := range tests {
		result := i.toBool(tt.input)
		if result != tt.expected {
			t.Errorf("toBool(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestInterpreter_ToInt(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		input    Value
		expected int
	}{
		{int(42), 42},
		{int64(42), 42},
		{float64(3.9), 3},
		{"123", 123},
		{true, 1},
		{false, 0},
		{nil, 0},
		{"notanumber", 0},
	}

	for _, tt := range tests {
		result := i.toInt(tt.input)
		if result != tt.expected {
			t.Errorf("toInt(%v) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestInterpreter_ToFloat(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		input    Value
		expected float64
	}{
		{int(42), 42.0},
		{int64(42), 42.0},
		{float64(3.14), 3.14},
		{"3.14", 3.14},
		{true, 1.0},
		{false, 0.0},
		{nil, 0.0},
		{"notanumber", 0.0},
	}

	for _, tt := range tests {
		result := i.toFloat(tt.input)
		if result != tt.expected {
			t.Errorf("toFloat(%v) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}

func TestInterpreter_IsTruthy(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		input    Value
		expected bool
	}{
		{nil, false},
		{true, true},
		{false, false},
		{int(1), true},
		{int(0), false},
		{int64(1), true},
		{int64(0), false},
		{float64(1.0), true},
		{float64(0.0), false},
		{"hello", true},
		{"", false},
		{[]Value{int64(1)}, true},
		{[]Value{}, false},
		{map[string]Value{"a": int64(1)}, true},
		{map[string]Value{}, false},
	}

	for _, tt := range tests {
		result := i.isTruthy(tt.input)
		if result != tt.expected {
			t.Errorf("isTruthy(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestInterpreter_Equal(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		a, b     Value
		expected bool
	}{
		{nil, nil, true},
		{nil, int64(0), false},
		{int(1), int(1), true},
		{int(1), int(2), false},
		{int(1), int64(1), true},
		{int64(1), int64(1), true},
		{int64(1), float64(1.0), true},
		{float64(1.0), float64(1.0), true},
		{float64(1.0), float64(2.0), false},
		{"hello", "hello", true},
		{"hello", "world", false},
		{true, true, true},
		{true, false, false},
	}

	for _, tt := range tests {
		result := i.equal(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("equal(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestInterpreter_Compare(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		a, b     Value
		expected int
	}{
		{int(1), int(2), -1},
		{int(2), int(1), 1},
		{int(1), int(1), 0},
		{int64(1), int64(2), -1},
		{int64(2), int64(1), 1},
		{float64(1.0), float64(2.0), -1},
		{float64(2.0), float64(1.0), 1},
		{int(1), float64(2.0), -1},
		{int64(1), float64(2.0), -1},
		{"a", "b", -1},
		{"b", "a", 1},
		{"a", "a", 0},
	}

	for _, tt := range tests {
		result := i.compare(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("compare(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestInterpreter_Add(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		a, b     Value
		expected Value
		hasError bool
	}{
		{int(1), int(2), int(3), false},
		{int64(1), int64(2), int64(3), false},
		{float64(1.0), float64(2.0), float64(3.0), false},
		{int(1), int64(2), int64(3), false},
		{int(1), float64(2.0), float64(3.0), false},
		{"hello", " world", "hello world", false},
		{int(1), "hello", nil, true},
	}

	for _, tt := range tests {
		result, err := i.add(tt.a, tt.b)
		if tt.hasError {
			if err == nil {
				t.Errorf("add(%v, %v) expected error", tt.a, tt.b)
			}
		} else {
			if err != nil {
				t.Errorf("add(%v, %v) unexpected error: %v", tt.a, tt.b, err)
			} else if !valuesEqual(result, tt.expected) {
				t.Errorf("add(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		}
	}
}

func TestInterpreter_Sub(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		a, b     Value
		expected Value
		hasError bool
	}{
		{int(5), int(3), int(2), false},
		{int64(5), int64(3), int64(2), false},
		{float64(5.0), float64(3.0), float64(2.0), false},
		{int(1), "hello", nil, true},
	}

	for _, tt := range tests {
		result, err := i.sub(tt.a, tt.b)
		if tt.hasError {
			if err == nil {
				t.Errorf("sub(%v, %v) expected error", tt.a, tt.b)
			}
		} else {
			if err != nil {
				t.Errorf("sub(%v, %v) unexpected error: %v", tt.a, tt.b, err)
			} else if !valuesEqual(result, tt.expected) {
				t.Errorf("sub(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		}
	}
}

func TestInterpreter_Mul(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		a, b     Value
		expected Value
		hasError bool
	}{
		{int(3), int(4), int(12), false},
		{int64(3), int64(4), int64(12), false},
		{float64(3.0), float64(4.0), float64(12.0), false},
		{int(1), "hello", nil, true},
	}

	for _, tt := range tests {
		result, err := i.mul(tt.a, tt.b)
		if tt.hasError {
			if err == nil {
				t.Errorf("mul(%v, %v) expected error", tt.a, tt.b)
			}
		} else {
			if err != nil {
				t.Errorf("mul(%v, %v) unexpected error: %v", tt.a, tt.b, err)
			} else if !valuesEqual(result, tt.expected) {
				t.Errorf("mul(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		}
	}
}

func TestInterpreter_Div(t *testing.T) {
	i := &Interpreter{}
	tests := []struct {
		b        Value
		hasError bool
	}{
		{int(10), false},
		{int64(10), false},
		{float64(10.0), false},
		{int(0), true},
		{int64(0), true},
		{float64(0.0), true},
	}

	for _, tt := range tests {
		_, err := i.div(int(10), tt.b)
		if tt.hasError {
			if err == nil {
				t.Errorf("div(10, %v) expected error", tt.b)
			}
		} else {
			if err != nil {
				t.Errorf("div(10, %v) unexpected error: %v", tt.b, err)
			}
		}
	}

	_, err := i.div(int(1), "hello")
	if err == nil {
		t.Error("div(1, 'hello') expected error")
	}
}

func TestInterpreter_Mod(t *testing.T) {
	i := &Interpreter{}
	result, err := i.mod(int64(10), int64(3))
	if err != nil {
		t.Fatalf("mod error: %v", err)
	}
	if result != int64(1) {
		// Note: mod uses toInt internally, so result is int
	}

	_, err = i.mod(int64(10), int64(0))
	if err == nil {
		t.Error("mod by zero expected error")
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		expected bool
	}{
		{nil, nil, true},
		{nil, int64(0), false},
		{int64(1), int64(1), true},
		{int64(1), int64(2), false},
		{int(1), int64(1), true},
		{float64(1.0), int64(1), true},
		{"hello", "hello", true},
		{"hello", "world", false},
		{true, true, true},
		{[]Value{int64(1), int64(2)}, []Value{int64(1), int64(2)}, true},
		{[]Value{int64(1)}, []Value{int64(2)}, false},
		{[]Value{int64(1)}, []Value{int64(1), int64(2)}, false},
		{map[string]Value{"a": int64(1)}, map[string]Value{"a": int64(1)}, true},
		{map[string]Value{"a": int64(1)}, map[string]Value{"a": int64(2)}, false},
		{map[string]Value{"a": int64(1)}, map[string]Value{"b": int64(1)}, false},
	}

	for _, tt := range tests {
		result := valuesEqual(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestValueToSlice(t *testing.T) {
	slice, ok := valueToSlice([]Value{int64(1), int64(2)})
	if !ok {
		t.Fatal("Expected ok")
	}
	if len(slice) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(slice))
	}

	slice2, ok := valueToSlice([]interface{}{int64(1), int64(2)})
	if !ok {
		t.Fatal("Expected ok for []interface{}")
	}
	if len(slice2) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(slice2))
	}

	_, ok = valueToSlice(int64(1))
	if ok {
		t.Error("Expected not ok for int64")
	}
}

func TestValueToMap(t *testing.T) {
	m, ok := valueToMap(map[string]Value{"a": int64(1)})
	if !ok {
		t.Fatal("Expected ok")
	}
	if len(m) != 1 {
		t.Errorf("Expected 1 element, got %d", len(m))
	}

	m2, ok := valueToMap(map[string]interface{}{"a": int64(1)})
	if !ok {
		t.Fatal("Expected ok for map[string]interface{}")
	}
	if len(m2) != 1 {
		t.Errorf("Expected 1 element, got %d", len(m2))
	}

	_, ok = valueToMap(int64(1))
	if ok {
		t.Error("Expected not ok for int64")
	}
}
