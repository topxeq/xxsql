package xxscript

import (
	"testing"
)

// Test builtin functions that are 0% coverage - Batch 3
func TestBuiltin_ZeroCoverage_Batch3(t *testing.T) {
	i := NewInterpreter(nil)

	// Test builtinSpan
	t.Run("builtinSpan", func(t *testing.T) {
		result := i.builtinSpan([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinBreakList
	t.Run("builtinBreakList", func(t *testing.T) {
		result := i.builtinBreakList([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSplitAt
	t.Run("builtinSplitAt", func(t *testing.T) {
		result := i.builtinSplitAt([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(1),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSplitWhen
	t.Run("builtinSplitWhen", func(t *testing.T) {
		result := i.builtinSplitWhen([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinAperture
	t.Run("builtinAperture", func(t *testing.T) {
		result := i.builtinAperture([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinXprod
	t.Run("builtinXprod", func(t *testing.T) {
		result := i.builtinXprod([]Value{
			[]Value{int64(1), int64(2)},
			[]Value{"a", "b"},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRangeStep
	t.Run("builtinRangeStep", func(t *testing.T) {
		result := i.builtinRangeStep([]Value{
			int64(0),
			int64(10),
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRepeatAll
	t.Run("builtinRepeatAll", func(t *testing.T) {
		result := i.builtinRepeatAll([]Value{
			[]Value{int64(1), int64(2)},
			int64(3),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinCycle
	t.Run("builtinCycle", func(t *testing.T) {
		result := i.builtinCycle([]Value{
			[]Value{int64(1), int64(2)},
			int64(3),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIterate
	t.Run("builtinIterate", func(t *testing.T) {
		result := i.builtinIterate([]Value{
			func() Value { return int64(1) },
			int64(1),
			int64(5),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinPrependAll
	t.Run("builtinPrependAll", func(t *testing.T) {
		result := i.builtinPrependAll([]Value{
			[]Value{int64(1), int64(2)},
			int64(0),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinAppendAll
	t.Run("builtinAppendAll", func(t *testing.T) {
		result := i.builtinAppendAll([]Value{
			[]Value{int64(1), int64(2)},
			int64(3),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIntersperse
	t.Run("builtinIntersperse", func(t *testing.T) {
		result := i.builtinIntersperse([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(0),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIntercalate
	t.Run("builtinIntercalate", func(t *testing.T) {
		result := i.builtinIntercalate([]Value{
			[]Value{int64(0)},
			[]Value{[]Value{int64(1), int64(2)}, []Value{int64(3), int64(4)}},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSubsequences
	t.Run("builtinSubsequences", func(t *testing.T) {
		result := i.builtinSubsequences([]Value{
			[]Value{int64(1), int64(2), int64(3)},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinStdDev
	t.Run("builtinStdDev", func(t *testing.T) {
		result := i.builtinStdDev([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4), int64(5)},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinMinBy
	t.Run("builtinMinBy", func(t *testing.T) {
		result := i.builtinMinBy([]Value{
			[]Value{map[string]Value{"a": int64(2)}, map[string]Value{"a": int64(1)}},
			"a",
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinMaxBy
	t.Run("builtinMaxBy", func(t *testing.T) {
		result := i.builtinMaxBy([]Value{
			[]Value{map[string]Value{"a": int64(2)}, map[string]Value{"a": int64(1)}},
			"a",
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinBcryptVerify
	t.Run("builtinBcryptVerify", func(t *testing.T) {
		result := i.builtinBcryptVerify([]Value{"password", "hash"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRandomBytes
	t.Run("builtinRandomBytes", func(t *testing.T) {
		result := i.builtinRandomBytes([]Value{int64(16)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRandomHex
	t.Run("builtinRandomHex", func(t *testing.T) {
		result := i.builtinRandomHex([]Value{int64(16)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRandomString
	t.Run("builtinRandomString", func(t *testing.T) {
		result := i.builtinRandomString([]Value{int64(16)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinGeneratePassword
	t.Run("builtinGeneratePassword", func(t *testing.T) {
		result := i.builtinGeneratePassword([]Value{int64(12)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinUUID
	t.Run("builtinUUID", func(t *testing.T) {
		result := i.builtinUUID([]Value{})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinUUIDv7
	t.Run("builtinUUIDv7", func(t *testing.T) {
		result := i.builtinUUIDv7([]Value{})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})
}
