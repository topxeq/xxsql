// Package types provides data type definitions for XxSql storage engine.
package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// TypeID represents a data type identifier.
type TypeID uint8

const (
	TypeNull TypeID = iota
	TypeSeq
	TypeInt
	TypeFloat
	TypeChar
	TypeVarchar
	TypeText
	TypeDate
	TypeTime
	TypeDatetime
	TypeBool
	TypeBytes // For internal use
)

// String returns the string representation of the type.
func (t TypeID) String() string {
	switch t {
	case TypeNull:
		return "NULL"
	case TypeSeq:
		return "SEQ"
	case TypeInt:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeChar:
		return "CHAR"
	case TypeVarchar:
		return "VARCHAR"
	case TypeText:
		return "TEXT"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeDatetime:
		return "DATETIME"
	case TypeBool:
		return "BOOL"
	case TypeBytes:
		return "BYTES"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// ParseTypeID parses a type name to TypeID.
func ParseTypeID(name string) TypeID {
	switch name {
	case "SEQ", "seq":
		return TypeSeq
	case "INT", "int", "INTEGER", "integer", "BIGINT", "bigint":
		return TypeInt
	case "FLOAT", "float", "DOUBLE", "double":
		return TypeFloat
	case "CHAR", "char":
		return TypeChar
	case "VARCHAR", "varchar":
		return TypeVarchar
	case "TEXT", "text":
		return TypeText
	case "DATE", "date":
		return TypeDate
	case "TIME", "time":
		return TypeTime
	case "DATETIME", "datetime", "TIMESTAMP", "timestamp":
		return TypeDatetime
	case "BOOL", "bool", "BOOLEAN", "boolean":
		return TypeBool
	default:
		return TypeNull
	}
}

// ColumnInfo represents column metadata.
type ColumnInfo struct {
	Name       string
	Type       TypeID
	Size       int    // For CHAR/VARCHAR
	Precision  int    // For DECIMAL (not implemented yet)
	Scale      int    // For DECIMAL (not implemented yet)
	Nullable   bool
	Default    Value
	PrimaryKey bool
	AutoIncr   bool
	Unique     bool
	Comment    string
}

// Value represents a typed value.
type Value struct {
	Type  TypeID
	Data  []byte
	Null  bool
}

// NewNullValue creates a null value.
func NewNullValue() Value {
	return Value{Type: TypeNull, Null: true}
}

// NewIntValue creates an integer value.
func NewIntValue(v int64) Value {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(v))
	return Value{Type: TypeInt, Data: data, Null: false}
}

// NewFloatValue creates a float value.
func NewFloatValue(v float64) Value {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(v))
	return Value{Type: TypeFloat, Data: data, Null: false}
}

// NewStringValue creates a string value (VARCHAR/CHAR/TEXT).
func NewStringValue(v string, typ TypeID) Value {
	return Value{Type: typ, Data: []byte(v), Null: false}
}

// NewDateValue creates a date value.
func NewDateValue(v time.Time) Value {
	// Days since year 1
	days := v.YearDay()
	for y := 1; y < v.Year(); y++ {
		days += daysInYear(y)
	}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(days))
	return Value{Type: TypeDate, Data: data, Null: false}
}

// NewTimeValue creates a time value.
func NewTimeValue(v time.Time) Value {
	// Seconds since midnight
	seconds := v.Hour()*3600 + v.Minute()*60 + v.Second()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(seconds))
	return Value{Type: TypeTime, Data: data, Null: false}
}

// NewDatetimeValue creates a datetime value.
func NewDatetimeValue(v time.Time) Value {
	// Unix timestamp in microseconds
	usec := v.UnixNano() / 1000
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(usec))
	return Value{Type: TypeDatetime, Data: data, Null: false}
}

// NewBoolValue creates a boolean value.
func NewBoolValue(v bool) Value {
	data := make([]byte, 1)
	if v {
		data[0] = 1
	}
	return Value{Type: TypeBool, Data: data, Null: false}
}

// NewBytesValue creates a bytes value.
func NewBytesValue(v []byte) Value {
	return Value{Type: TypeBytes, Data: v, Null: false}
}

// AsInt returns the value as int64.
func (v Value) AsInt() int64 {
	if v.Null || len(v.Data) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(v.Data))
}

// AsFloat returns the value as float64.
func (v Value) AsFloat() float64 {
	if v.Null || len(v.Data) < 8 {
		return 0
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(v.Data))
}

// AsString returns the value as string.
func (v Value) AsString() string {
	if v.Null {
		return ""
	}
	return string(v.Data)
}

// AsBool returns the value as bool.
func (v Value) AsBool() bool {
	if v.Null || len(v.Data) < 1 {
		return false
	}
	return v.Data[0] != 0
}

// AsDatetime returns the value as time.Time.
func (v Value) AsDatetime() time.Time {
	if v.Null || len(v.Data) < 8 {
		return time.Time{}
	}
	usec := int64(binary.LittleEndian.Uint64(v.Data))
	return time.Unix(usec/1000000, (usec%1000000)*1000)
}

// Compare compares two values.
// Returns -1 if v < other, 0 if v == other, 1 if v > other.
func (v Value) Compare(other Value) int {
	// Null handling
	if v.Null && other.Null {
		return 0
	}
	if v.Null {
		return -1
	}
	if other.Null {
		return 1
	}

	// Compare based on type
	switch v.Type {
	case TypeInt, TypeSeq:
		a := v.AsInt()
		b := other.AsInt()
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0

	case TypeFloat:
		a := v.AsFloat()
		b := other.AsFloat()
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0

	case TypeChar, TypeVarchar, TypeText:
		a := v.AsString()
		b := other.AsString()
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0

	case TypeBool:
		a := v.AsBool()
		b := other.AsBool()
		if !a && b {
			return -1
		} else if a && !b {
			return 1
		}
		return 0

	case TypeDatetime:
		a := v.AsDatetime()
		b := other.AsDatetime()
		if a.Before(b) {
			return -1
		} else if a.After(b) {
			return 1
		}
		return 0

	default:
		// Byte comparison for other types
		minLen := len(v.Data)
		if len(other.Data) < minLen {
			minLen = len(other.Data)
		}
		for i := 0; i < minLen; i++ {
			if v.Data[i] < other.Data[i] {
				return -1
			} else if v.Data[i] > other.Data[i] {
				return 1
			}
		}
		if len(v.Data) < len(other.Data) {
			return -1
		} else if len(v.Data) > len(other.Data) {
			return 1
		}
		return 0
	}
}

// Size returns the storage size of the value.
func (v Value) Size() int {
	if v.Null {
		return 1 // NULL flag only
	}

	// For variable-length types, include length prefix
	switch v.Type {
	case TypeChar, TypeVarchar:
		return 1 + 2 + len(v.Data) // NULL flag + length + data
	case TypeText:
		return 1 + 4 + len(v.Data) // NULL flag + length + data
	default:
		return 1 + len(v.Data) // NULL flag + data
	}
}

// Marshal serializes the value to bytes.
func (v Value) Marshal() []byte {
	if v.Null {
		return []byte{0x01} // NULL flag
	}

	// For variable-length types, include length prefix
	switch v.Type {
	case TypeChar, TypeVarchar:
		// NULL flag (1) + length (2) + data
		buf := make([]byte, 1+2+len(v.Data))
		buf[0] = 0x00 // Not NULL
		binary.LittleEndian.PutUint16(buf[1:3], uint16(len(v.Data)))
		copy(buf[3:], v.Data)
		return buf

	case TypeText:
		// NULL flag (1) + length (4) + data
		buf := make([]byte, 1+4+len(v.Data))
		buf[0] = 0x00 // Not NULL
		binary.LittleEndian.PutUint32(buf[1:5], uint32(len(v.Data)))
		copy(buf[5:], v.Data)
		return buf

	default:
		// Fixed-length types: NULL flag + data
		buf := make([]byte, 1+len(v.Data))
		buf[0] = 0x00 // Not NULL
		copy(buf[1:], v.Data)
		return buf
	}
}

// UnmarshalValue deserializes bytes to a value.
func UnmarshalValue(data []byte, typ TypeID) (Value, int) {
	if len(data) < 1 {
		return NewNullValue(), 0
	}

	nullFlag := data[0]
	if nullFlag == 0x01 {
		return NewNullValue(), 1
	}

	// Determine data length based on type
	var dataLen int
	var offset int

	switch typ {
	case TypeInt, TypeSeq, TypeFloat, TypeDatetime:
		dataLen = 8
		offset = 1
	case TypeDate, TypeTime:
		dataLen = 4
		offset = 1
	case TypeBool:
		dataLen = 1
		offset = 1
	case TypeChar, TypeVarchar:
		if len(data) < 3 {
			return NewNullValue(), 1
		}
		dataLen = int(binary.LittleEndian.Uint16(data[1:3]))
		offset = 3
	case TypeText:
		if len(data) < 5 {
			return NewNullValue(), 1
		}
		dataLen = int(binary.LittleEndian.Uint32(data[1:5]))
		offset = 5
	default:
		dataLen = len(data) - 1
		offset = 1
	}

	if len(data) < offset+dataLen {
		dataLen = len(data) - offset
	}

	value := Value{
		Type: typ,
		Data: make([]byte, dataLen),
		Null: false,
	}
	copy(value.Data, data[offset:offset+dataLen])

	return value, offset + dataLen
}

// String returns a string representation of the value.
func (v Value) String() string {
	if v.Null {
		return "NULL"
	}

	switch v.Type {
	case TypeInt, TypeSeq:
		return fmt.Sprintf("%d", v.AsInt())
	case TypeFloat:
		return fmt.Sprintf("%f", v.AsFloat())
	case TypeChar, TypeVarchar, TypeText:
		return fmt.Sprintf("'%s'", v.AsString())
	case TypeBool:
		return fmt.Sprintf("%v", v.AsBool())
	case TypeDatetime:
		return v.AsDatetime().Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", v.Data)
	}
}

// daysInYear returns the number of days in a year.
func daysInYear(year int) int {
	if isLeapYear(year) {
		return 366
	}
	return 365
}

// isLeapYear checks if a year is a leap year.
func isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}
