package types

import (
	"testing"
)

func TestDecimalTypeID(t *testing.T) {
	// Test TypeID
	if TypeDecimal != 4 {
		t.Errorf("Expected TypeDecimal to be 4, got %d", TypeDecimal)
	}

	// Test String()
	if TypeDecimal.String() != "DECIMAL" {
		t.Errorf("Expected 'DECIMAL', got '%s'", TypeDecimal.String())
	}

	// Test ParseTypeID
	tests := []struct {
		input    string
		expected TypeID
	}{
		{"DECIMAL", TypeDecimal},
		{"decimal", TypeDecimal},
		{"DEC", TypeDecimal},
		{"dec", TypeDecimal},
		{"NUMERIC", TypeDecimal},
		{"numeric", TypeDecimal},
	}

	for _, tt := range tests {
		result := ParseTypeID(tt.input)
		if result != tt.expected {
			t.Errorf("ParseTypeID(%s) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}

func TestNewDecimalValue(t *testing.T) {
	// Test creating DECIMAL from unscaled value
	v := NewDecimalValue(12345, 2) // Represents 123.45

	if v.Null {
		t.Error("Value should not be null")
	}
	if v.Type != TypeDecimal {
		t.Errorf("Expected TypeDecimal, got %d", v.Type)
	}

	unscaled, scale := v.AsDecimal()
	if unscaled != 12345 {
		t.Errorf("Expected unscaled 12345, got %d", unscaled)
	}
	if scale != 2 {
		t.Errorf("Expected scale 2, got %d", scale)
	}
}

func TestNewDecimalFromString(t *testing.T) {
	tests := []struct {
		input      string
		unscaled   int64
		scale      int
		stringRep  string
	}{
		{"123.45", 12345, 2, "123.45"},
		{"0.99", 99, 2, "0.99"},
		{"100", 100, 0, "100"},
		{"0.1", 1, 1, "0.1"},
		{"1234.5678", 12345678, 4, "1234.5678"},
		{"-123.45", -12345, 2, "-123.45"},
		{"-0.99", -99, 2, "-0.99"},
		{"1.00", 100, 2, "1"},
		{"10.50", 1050, 2, "10.5"},
	}

	for _, tt := range tests {
		v, err := NewDecimalFromString(tt.input)
		if err != nil {
			t.Errorf("NewDecimalFromString(%s) error: %v", tt.input, err)
			continue
		}

		unscaled, scale := v.AsDecimal()
		if unscaled != tt.unscaled {
			t.Errorf("NewDecimalFromString(%s) unscaled = %d, expected %d", tt.input, unscaled, tt.unscaled)
		}
		if scale != tt.scale {
			t.Errorf("NewDecimalFromString(%s) scale = %d, expected %d", tt.input, scale, tt.scale)
		}

		// Test string representation
		str := v.AsDecimalString()
		if str != tt.stringRep {
			t.Errorf("AsDecimalString() = %s, expected %s", str, tt.stringRep)
		}
	}
}

func TestDecimalCompare(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"123.45", "123.45", 0},
		{"123.45", "123.46", -1},
		{"123.46", "123.45", 1},
		{"100.00", "100", 0},
		{"99.99", "100", -1},
		{"100", "99.99", 1},
		{"-123.45", "123.45", -1},
		{"123.45", "-123.45", 1},
		{"-100", "-99", -1},
		{"-99", "-100", 1},
		{"0.01", "0.001", 1}, // Different scales
		{"0.50", "0.5", 0},   // Different scales, same value
	}

	for _, tt := range tests {
		a, _ := NewDecimalFromString(tt.a)
		b, _ := NewDecimalFromString(tt.b)

		result := a.Compare(b)
		if result != tt.expected {
			t.Errorf("Compare(%s, %s) = %d, expected %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestDecimalMarshalUnmarshal(t *testing.T) {
	tests := []string{
		"123.45",
		"0.99",
		"-1234.56",
		"100",
		"0.0001",
		"9999999999.99",
	}

	for _, tt := range tests {
		original, err := NewDecimalFromString(tt)
		if err != nil {
			t.Errorf("NewDecimalFromString(%s) error: %v", tt, err)
			continue
		}

		// Marshal
		data := original.Marshal()

		// Unmarshal
		result, bytesRead := UnmarshalValue(data, TypeDecimal)

		if result.Null {
			t.Errorf("Unmarshaled value should not be null for %s", tt)
			continue
		}

		if bytesRead != len(data) {
			t.Errorf("bytesRead = %d, expected %d", bytesRead, len(data))
		}

		// Compare values
		origUnscaled, origScale := original.AsDecimal()
		resultUnscaled, resultScale := result.AsDecimal()

		if origUnscaled != resultUnscaled || origScale != resultScale {
			t.Errorf("Roundtrip failed for %s: got unscaled=%d scale=%d, expected unscaled=%d scale=%d",
				tt, resultUnscaled, resultScale, origUnscaled, origScale)
		}
	}
}

func TestDecimalSize(t *testing.T) {
	v := NewDecimalValue(12345, 2)
	size := v.Size()

	// DECIMAL size: 1 (null flag) + 9 (scale + unscaled) = 10
	if size != 10 {
		t.Errorf("Expected size 10, got %d", size)
	}

	// Null value should have size 1
	nullVal := NewNullValue()
	nullVal.Type = TypeDecimal
	if nullVal.Size() != 1 {
		t.Errorf("Expected null size 1, got %d", nullVal.Size())
	}
}

func TestFormatDecimal(t *testing.T) {
	tests := []struct {
		unscaled  int64
		scale     int
		expected  string
	}{
		{12345, 2, "123.45"},
		{100, 2, "1"},
		{1050, 2, "10.5"},
		{99, 2, "0.99"},
		{1, 3, "0.001"},
		{-12345, 2, "-123.45"},
		{-100, 2, "-1"},
		{0, 0, "0"},
		{0, 2, "0"},
	}

	for _, tt := range tests {
		result := FormatDecimal(tt.unscaled, tt.scale)
		if result != tt.expected {
			t.Errorf("FormatDecimal(%d, %d) = %s, expected %s", tt.unscaled, tt.scale, result, tt.expected)
		}
	}
}

func TestDecimalNull(t *testing.T) {
	v := NewNullValue()
	v.Type = TypeDecimal

	if !v.Null {
		t.Error("Value should be null")
	}

	// AsDecimal on null should return 0, 0
	unscaled, scale := v.AsDecimal()
	if unscaled != 0 || scale != 0 {
		t.Errorf("Null decimal should return (0, 0), got (%d, %d)", unscaled, scale)
	}

	// AsDecimalString on null should return "NULL"
	str := v.AsDecimalString()
	if str != "NULL" {
		t.Errorf("Null decimal string should be 'NULL', got '%s'", str)
	}
}

func TestDecimalString(t *testing.T) {
	v, _ := NewDecimalFromString("123.45")
	str := v.String()
	if str != "123.45" {
		t.Errorf("String() = %s, expected 123.45", str)
	}

	// Test negative
	v, _ = NewDecimalFromString("-999.99")
	str = v.String()
	if str != "-999.99" {
		t.Errorf("String() = %s, expected -999.99", str)
	}
}
