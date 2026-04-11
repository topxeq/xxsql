package xxscript

// ValueType represents the type of a Value.
type ValueType int

const (
	TypeNull ValueType = iota
	TypeBool
	TypeInt
	TypeInt64
	TypeFloat
	TypeString
	TypeArray
	TypeMap
	TypeFunction
	TypeObject
)

// TypedValue is a value with an explicit type tag.
type TypedValue struct {
	Type  ValueType
	Value interface{}
}

// NewTypedValue creates a new typed value.
func NewTypedValue(v interface{}) TypedValue {
	switch v.(type) {
	case nil:
		return TypedValue{Type: TypeNull, Value: v}
	case bool:
		return TypedValue{Type: TypeBool, Value: v}
	case int:
		return TypedValue{Type: TypeInt, Value: v}
	case int64:
		return TypedValue{Type: TypeInt64, Value: v}
	case float64:
		return TypedValue{Type: TypeFloat, Value: v}
	case string:
		return TypedValue{Type: TypeString, Value: v}
	case []Value:
		return TypedValue{Type: TypeArray, Value: v}
	case map[string]Value:
		return TypedValue{Type: TypeMap, Value: v}
	case *UserFunc:
		return TypedValue{Type: TypeFunction, Value: v}
	default:
		return TypedValue{Type: TypeObject, Value: v}
	}
}

// IsTruthy returns true if the value is truthy.
func (tv TypedValue) IsTruthy() bool {
	switch tv.Type {
	case TypeNull:
		return false
	case TypeBool:
		return tv.Value.(bool)
	case TypeInt:
		return tv.Value.(int) != 0
	case TypeInt64:
		return tv.Value.(int64) != 0
	case TypeFloat:
		return tv.Value.(float64) != 0
	case TypeString:
		return tv.Value.(string) != ""
	case TypeArray:
		return len(tv.Value.([]Value)) > 0
	case TypeMap:
		return len(tv.Value.(map[string]Value)) > 0
	default:
		return tv.Value != nil
	}
}

// ToInt converts the value to int.
func (tv TypedValue) ToInt() int {
	switch tv.Type {
	case TypeInt:
		return tv.Value.(int)
	case TypeInt64:
		return int(tv.Value.(int64))
	case TypeFloat:
		return int(tv.Value.(float64))
	case TypeBool:
		if tv.Value.(bool) {
			return 1
		}
		return 0
	case TypeString:
		var n int
		_ = parseInt(tv.Value.(string), &n)
		return n
	default:
		return 0
	}
}

// ToFloat converts the value to float64.
func (tv TypedValue) ToFloat() float64 {
	switch tv.Type {
	case TypeInt:
		return float64(tv.Value.(int))
	case TypeInt64:
		return float64(tv.Value.(int64))
	case TypeFloat:
		return tv.Value.(float64)
	case TypeBool:
		if tv.Value.(bool) {
			return 1.0
		}
		return 0.0
	default:
		return 0.0
	}
}

// ToString converts the value to string.
func (tv TypedValue) ToString() string {
	return toString(tv.Value)
}

// fastAdd performs fast addition with type hints.
func fastAdd(a, b TypedValue) (TypedValue, bool) {
	// Fast path for same types
	if a.Type == b.Type {
		switch a.Type {
		case TypeInt:
			return TypedValue{Type: TypeInt, Value: a.Value.(int) + b.Value.(int)}, true
		case TypeInt64:
			return TypedValue{Type: TypeInt64, Value: a.Value.(int64) + b.Value.(int64)}, true
		case TypeFloat:
			return TypedValue{Type: TypeFloat, Value: a.Value.(float64) + b.Value.(float64)}, true
		case TypeString:
			return TypedValue{Type: TypeString, Value: a.Value.(string) + b.Value.(string)}, true
		}
	}

	// Mixed numeric types
	if isNumeric(a.Type) && isNumeric(b.Type) {
		// Promote to float for mixed types
		return TypedValue{Type: TypeFloat, Value: a.ToFloat() + b.ToFloat()}, true
	}

	return TypedValue{}, false
}

// fastSub performs fast subtraction.
func fastSub(a, b TypedValue) (TypedValue, bool) {
	if a.Type == b.Type {
		switch a.Type {
		case TypeInt:
			return TypedValue{Type: TypeInt, Value: a.Value.(int) - b.Value.(int)}, true
		case TypeInt64:
			return TypedValue{Type: TypeInt64, Value: a.Value.(int64) - b.Value.(int64)}, true
		case TypeFloat:
			return TypedValue{Type: TypeFloat, Value: a.Value.(float64) - b.Value.(float64)}, true
		}
	}

	if isNumeric(a.Type) && isNumeric(b.Type) {
		return TypedValue{Type: TypeFloat, Value: a.ToFloat() - b.ToFloat()}, true
	}

	return TypedValue{}, false
}

// fastMul performs fast multiplication.
func fastMul(a, b TypedValue) (TypedValue, bool) {
	if a.Type == b.Type {
		switch a.Type {
		case TypeInt:
			return TypedValue{Type: TypeInt, Value: a.Value.(int) * b.Value.(int)}, true
		case TypeInt64:
			return TypedValue{Type: TypeInt64, Value: a.Value.(int64) * b.Value.(int64)}, true
		case TypeFloat:
			return TypedValue{Type: TypeFloat, Value: a.Value.(float64) * b.Value.(float64)}, true
		}
	}

	if isNumeric(a.Type) && isNumeric(b.Type) {
		return TypedValue{Type: TypeFloat, Value: a.ToFloat() * b.ToFloat()}, true
	}

	return TypedValue{}, false
}

// fastDiv performs fast division.
func fastDiv(a, b TypedValue) (TypedValue, bool) {
	bf := b.ToFloat()
	if bf == 0 {
		return TypedValue{Type: TypeNull}, false
	}

	return TypedValue{Type: TypeFloat, Value: a.ToFloat() / bf}, true
}

// fastCompare performs fast comparison.
func fastCompare(a, b TypedValue) (int, bool) {
	if a.Type == b.Type {
		switch a.Type {
		case TypeInt:
			av, bv := a.Value.(int), b.Value.(int)
			if av < bv {
				return -1, true
			} else if av > bv {
				return 1, true
			}
			return 0, true
		case TypeInt64:
			av, bv := a.Value.(int64), b.Value.(int64)
			if av < bv {
				return -1, true
			} else if av > bv {
				return 1, true
			}
			return 0, true
		case TypeFloat:
			av, bv := a.Value.(float64), b.Value.(float64)
			if av < bv {
				return -1, true
			} else if av > bv {
				return 1, true
			}
			return 0, true
		case TypeString:
			if a.Value.(string) < b.Value.(string) {
				return -1, true
			} else if a.Value.(string) > b.Value.(string) {
				return 1, true
			}
			return 0, true
		}
	}

	// Mixed numeric comparison
	if isNumeric(a.Type) && isNumeric(b.Type) {
		af, bf := a.ToFloat(), b.ToFloat()
		if af < bf {
			return -1, true
		} else if af > bf {
			return 1, true
		}
		return 0, true
	}

	return 0, false
}

// isNumeric returns true if the type is numeric.
func isNumeric(t ValueType) bool {
	return t == TypeInt || t == TypeInt64 || t == TypeFloat
}

// parseInt is a fast integer parser.
func parseInt(s string, n *int) error {
	if len(s) == 0 {
		return nil
	}

	sign := 1
	i := 0
	if s[0] == '-' {
		sign = -1
		i = 1
	} else if s[0] == '+' {
		i = 1
	}

	val := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		val = val*10 + int(c-'0')
	}

	*n = sign * val
	return nil
}
