package types

import (
	"testing"
	"time"
)

func TestTypeIDString(t *testing.T) {
	tests := []struct {
		typeID   TypeID
		expected string
	}{
		{TypeNull, "NULL"},
		{TypeSeq, "SEQ"},
		{TypeInt, "INT"},
		{TypeFloat, "FLOAT"},
		{TypeDecimal, "DECIMAL"},
		{TypeChar, "CHAR"},
		{TypeVarchar, "VARCHAR"},
		{TypeText, "TEXT"},
		{TypeDate, "DATE"},
		{TypeTime, "TIME"},
		{TypeDatetime, "DATETIME"},
		{TypeBool, "BOOL"},
		{TypeBytes, "BYTES"},
		{TypeID(99), "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.typeID.String(); got != tt.expected {
				t.Errorf("TypeID.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseTypeID(t *testing.T) {
	tests := []struct {
		input    string
		expected TypeID
	}{
		{"SEQ", TypeSeq},
		{"seq", TypeSeq},
		{"INT", TypeInt},
		{"int", TypeInt},
		{"INTEGER", TypeInt},
		{"BIGINT", TypeInt},
		{"FLOAT", TypeFloat},
		{"DOUBLE", TypeFloat},
		{"CHAR", TypeChar},
		{"VARCHAR", TypeVarchar},
		{"TEXT", TypeText},
		{"DATE", TypeDate},
		{"TIME", TypeTime},
		{"DATETIME", TypeDatetime},
		{"TIMESTAMP", TypeDatetime},
		{"BOOL", TypeBool},
		{"BOOLEAN", TypeBool},
		{"unknown", TypeNull},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseTypeID(tt.input); got != tt.expected {
				t.Errorf("ParseTypeID(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNewNullValue(t *testing.T) {
	v := NewNullValue()

	if !v.Null {
		t.Error("Value should be null")
	}
	if v.Type != TypeNull {
		t.Errorf("Type: got %v, want TypeNull", v.Type)
	}
}

func TestIntValue(t *testing.T) {
	tests := []int64{-123, -1, 0, 1, 123, 9223372036854775807}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			v := NewIntValue(tt)

			if v.Null {
				t.Error("Value should not be null")
			}
			if v.Type != TypeInt {
				t.Errorf("Type: got %v, want TypeInt", v.Type)
			}
			if got := v.AsInt(); got != tt {
				t.Errorf("AsInt(): got %d, want %d", got, tt)
			}
		})
	}
}

func TestFloatValue(t *testing.T) {
	tests := []float64{-123.45, -1.0, 0.0, 1.0, 123.45, 1e10}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			v := NewFloatValue(tt)

			if v.Null {
				t.Error("Value should not be null")
			}
			if v.Type != TypeFloat {
				t.Errorf("Type: got %v, want TypeFloat", v.Type)
			}
			if got := v.AsFloat(); got != tt {
				t.Errorf("AsFloat(): got %f, want %f", got, tt)
			}
		})
	}
}

func TestStringValue(t *testing.T) {
	tests := []struct {
		value string
		typ   TypeID
	}{
		{"hello", TypeVarchar},
		{"world", TypeChar},
		{"long text content", TypeText},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			v := NewStringValue(tt.value, tt.typ)

			if v.Null {
				t.Error("Value should not be null")
			}
			if v.Type != tt.typ {
				t.Errorf("Type: got %v, want %v", v.Type, tt.typ)
			}
			if got := v.AsString(); got != tt.value {
				t.Errorf("AsString(): got %q, want %q", got, tt.value)
			}
		})
	}
}

func TestBoolValue(t *testing.T) {
	vTrue := NewBoolValue(true)
	if vTrue.Null {
		t.Error("Value should not be null")
	}
	if !vTrue.AsBool() {
		t.Error("AsBool(): got false, want true")
	}

	vFalse := NewBoolValue(false)
	if vFalse.AsBool() {
		t.Error("AsBool(): got true, want false")
	}
}

func TestDatetimeValue(t *testing.T) {
	now := time.Now()
	v := NewDatetimeValue(now)

	if v.Null {
		t.Error("Value should not be null")
	}
	if v.Type != TypeDatetime {
		t.Errorf("Type: got %v, want TypeDatetime", v.Type)
	}

	got := v.AsDatetime()
	diff := got.Sub(now)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second {
		t.Errorf("AsDatetime(): got %v, want approximately %v", got, now)
	}
}

func TestBytesValue(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	v := NewBytesValue(data)

	if v.Null {
		t.Error("Value should not be null")
	}
	if v.Type != TypeBytes {
		t.Errorf("Type: got %v, want TypeBytes", v.Type)
	}
	if string(v.Data) != string(data) {
		t.Errorf("Data: got %v, want %v", v.Data, data)
	}
}

func TestValueCompare(t *testing.T) {
	tests := []struct {
		name     string
		a, b     Value
		expected int
	}{
		{"both null", NewNullValue(), NewNullValue(), 0},
		{"null vs int", NewNullValue(), NewIntValue(1), -1},
		{"int vs null", NewIntValue(1), NewNullValue(), 1},
		{"int equal", NewIntValue(100), NewIntValue(100), 0},
		{"int less", NewIntValue(50), NewIntValue(100), -1},
		{"int greater", NewIntValue(100), NewIntValue(50), 1},
		{"float equal", NewFloatValue(3.14), NewFloatValue(3.14), 0},
		{"float less", NewFloatValue(1.0), NewFloatValue(2.0), -1},
		{"float greater", NewFloatValue(2.0), NewFloatValue(1.0), 1},
		{"string equal", NewStringValue("abc", TypeVarchar), NewStringValue("abc", TypeVarchar), 0},
		{"string less", NewStringValue("abc", TypeVarchar), NewStringValue("def", TypeVarchar), -1},
		{"string greater", NewStringValue("def", TypeVarchar), NewStringValue("abc", TypeVarchar), 1},
		{"bool equal", NewBoolValue(true), NewBoolValue(true), 0},
		{"bool less", NewBoolValue(false), NewBoolValue(true), -1},
		{"bool greater", NewBoolValue(true), NewBoolValue(false), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.expected {
				t.Errorf("Compare(): got %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestValueSize(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		minSize int
	}{
		{"null", NewNullValue(), 1},
		{"int", NewIntValue(123), 9},
		{"float", NewFloatValue(3.14), 9},
		{"bool", NewBoolValue(true), 2},
		{"varchar", NewStringValue("hello", TypeVarchar), 8},
		{"text", NewStringValue("hello", TypeText), 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.value.Size(); got < tt.minSize {
				t.Errorf("Size(): got %d, want at least %d", got, tt.minSize)
			}
		})
	}
}

func TestValueMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Value
		typ   TypeID
	}{
		{"int", NewIntValue(12345), TypeInt},
		{"float", NewFloatValue(3.14159), TypeFloat},
		{"bool true", NewBoolValue(true), TypeBool},
		{"bool false", NewBoolValue(false), TypeBool},
		{"varchar", NewStringValue("hello world", TypeVarchar), TypeVarchar},
		{"text", NewStringValue("long text content here", TypeText), TypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.value.Marshal()

			result, bytesRead := UnmarshalValue(data, tt.typ)

			if result.Null {
				t.Error("Unmarshaled value should not be null")
			}
			if bytesRead != len(data) {
				t.Errorf("bytesRead: got %d, want %d", bytesRead, len(data))
			}

			cmp := tt.value.Compare(result)
			if cmp != 0 {
				t.Errorf("Values should be equal after roundtrip, got %d", cmp)
			}
		})
	}
}

func TestValueString(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		contains string
	}{
		{"null", NewNullValue(), "NULL"},
		{"int", NewIntValue(123), "123"},
		{"float", NewFloatValue(3.14), "3.14"},
		{"string", NewStringValue("hello", TypeVarchar), "'hello'"},
		{"bool", NewBoolValue(true), "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.value.String()
			if len(got) < len(tt.contains) || got[:len(tt.contains)] != tt.contains && got[len(got)-len(tt.contains):] != tt.contains {
				t.Errorf("String(): got %q, should contain %q", got, tt.contains)
			}
		})
	}
}

func TestNullValueOperations(t *testing.T) {
	v := NewNullValue()

	if v.AsInt() != 0 {
		t.Error("Null AsInt should return 0")
	}
	if v.AsFloat() != 0 {
		t.Error("Null AsFloat should return 0")
	}
	if v.AsString() != "" {
		t.Error("Null AsString should return empty string")
	}
	if v.AsBool() != false {
		t.Error("Null AsBool should return false")
	}
	if v.AsDatetime().IsZero() == false {
		t.Error("Null AsDatetime should return zero time")
	}
}

func TestIsLeapYear(t *testing.T) {
	tests := []struct {
		year     int
		expected bool
	}{
		{2000, true},
		{2004, true},
		{2020, true},
		{1900, false},
		{2001, false},
		{2100, false},
		{2400, true},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := isLeapYear(tt.year); got != tt.expected {
				t.Errorf("isLeapYear(%d) = %v, want %v", tt.year, got, tt.expected)
			}
		})
	}
}

func TestDaysInYear(t *testing.T) {
	tests := []struct {
		year     int
		expected int
	}{
		{2000, 366},
		{2001, 365},
		{2004, 366},
		{1900, 365},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := daysInYear(tt.year); got != tt.expected {
				t.Errorf("daysInYear(%d) = %d, want %d", tt.year, got, tt.expected)
			}
		})
	}
}

func TestUnmarshalValue_EmptyData(t *testing.T) {
	v, n := UnmarshalValue([]byte{}, TypeInt)

	if !v.Null {
		t.Error("Empty data should return null value")
	}
	if n != 0 {
		t.Errorf("bytesRead: got %d, want 0", n)
	}
}

func TestUnmarshalValue_NullFlag(t *testing.T) {
	v, n := UnmarshalValue([]byte{0x01}, TypeInt)

	if !v.Null {
		t.Error("Null flag should return null value")
	}
	if n != 1 {
		t.Errorf("bytesRead: got %d, want 1", n)
	}
}

func TestAsIntNull(t *testing.T) {
	v := Value{Type: TypeInt, Null: true}
	if v.AsInt() != 0 {
		t.Error("Null int should return 0")
	}

	v2 := Value{Type: TypeInt, Data: []byte{1, 2, 3}, Null: false}
	if v2.AsInt() != 0 {
		t.Error("Short data should return 0")
	}
}

func TestAsFloatNull(t *testing.T) {
	v := Value{Type: TypeFloat, Null: true}
	if v.AsFloat() != 0 {
		t.Error("Null float should return 0")
	}

	v2 := Value{Type: TypeFloat, Data: []byte{1, 2, 3}, Null: false}
	if v2.AsFloat() != 0 {
		t.Error("Short data should return 0")
	}
}

func TestAsBoolNull(t *testing.T) {
	v := Value{Type: TypeBool, Null: true}
	if v.AsBool() != false {
		t.Error("Null bool should return false")
	}

	v2 := Value{Type: TypeBool, Data: []byte{}, Null: false}
	if v2.AsBool() != false {
		t.Error("Short data should return false")
	}
}

func TestAsDatetimeNull(t *testing.T) {
	v := Value{Type: TypeDatetime, Null: true}
	if !v.AsDatetime().IsZero() {
		t.Error("Null datetime should return zero time")
	}

	v2 := Value{Type: TypeDatetime, Data: []byte{1, 2, 3}, Null: false}
	if !v2.AsDatetime().IsZero() {
		t.Error("Short data should return zero time")
	}
}

func TestNewDateValue(t *testing.T) {
	date := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	v := NewDateValue(date)

	if v.Null {
		t.Error("Date value should not be null")
	}
	if v.Type != TypeDate {
		t.Errorf("Type: got %v, want TypeDate", v.Type)
	}
}

func TestNewTimeValue(t *testing.T) {
	tm := time.Date(0, 1, 1, 14, 30, 45, 0, time.UTC)
	v := NewTimeValue(tm)

	if v.Null {
		t.Error("Time value should not be null")
	}
	if v.Type != TypeTime {
		t.Errorf("Type: got %v, want TypeTime", v.Type)
	}
}
