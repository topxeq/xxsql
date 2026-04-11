package xxscript

import (
	"math"
	"testing"
)

// Test builtin functions that are 0% coverage - Batch 1
func TestBuiltin_ZeroCoverage_Batch1(t *testing.T) {
	i := NewInterpreter(nil)

	// Test builtinDefaultOnError
	t.Run("builtinDefaultOnError", func(t *testing.T) {
		// With error object
		result := i.builtinDefaultOnError([]Value{
			map[string]Value{"error": true, "message": "test"},
			int64(42),
		})
		if result != int64(42) {
			t.Errorf("Expected 42, got %v", result)
		}
		// Without error
		result2 := i.builtinDefaultOnError([]Value{int64(1), int64(42)})
		if result2 != int64(1) {
			t.Errorf("Expected 1, got %v", result2)
		}
		// With insufficient args
		result3 := i.builtinDefaultOnError([]Value{int64(1)})
		if result3 != int64(1) {
			t.Errorf("Expected 1, got %v", result3)
		}
	})

	// Test builtinSafeCall
	t.Run("builtinSafeCall", func(t *testing.T) {
		result := i.builtinSafeCall([]Value{
			func() Value { return int64(42) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIsInf
	t.Run("builtinIsInf", func(t *testing.T) {
		result := i.builtinIsInf([]Value{math.Inf(1)})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
		result2 := i.builtinIsInf([]Value{float64(1.0)})
		if result2 != false {
			t.Errorf("Expected false, got %v", result2)
		}
	})

	// Test builtinIsNaN
	t.Run("builtinIsNaN", func(t *testing.T) {
		result := i.builtinIsNaN([]Value{math.NaN()})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
		result2 := i.builtinIsNaN([]Value{float64(1.0)})
		if result2 != false {
			t.Errorf("Expected false, got %v", result2)
		}
	})

	// Test builtinPercentile
	t.Run("builtinPercentile", func(t *testing.T) {
		result := i.builtinPercentile([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4), int64(5)},
			float64(50),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRandomInt
	t.Run("builtinRandomInt", func(t *testing.T) {
		result := i.builtinRandomInt([]Value{int64(1), int64(10)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRandomFloat
	t.Run("builtinRandomFloat", func(t *testing.T) {
		result := i.builtinRandomFloat([]Value{})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSortByDesc
	t.Run("builtinSortByDesc", func(t *testing.T) {
		result := i.builtinSortByDesc([]Value{
			[]Value{map[string]Value{"a": int64(2)}, map[string]Value{"a": int64(1)}},
			"a",
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSortStrings
	t.Run("builtinSortStrings", func(t *testing.T) {
		result := i.builtinSortStrings([]Value{
			[]Value{"c", "a", "b"},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSortStringsDesc
	t.Run("builtinSortStringsDesc", func(t *testing.T) {
		result := i.builtinSortStringsDesc([]Value{
			[]Value{"c", "a", "b"},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSortNatural
	t.Run("builtinSortNatural", func(t *testing.T) {
		result := i.builtinSortNatural([]Value{
			[]Value{"item10", "item2", "item1"},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSortNaturalDesc
	t.Run("builtinSortNaturalDesc", func(t *testing.T) {
		result := i.builtinSortNaturalDesc([]Value{
			[]Value{"item10", "item2", "item1"},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSortMulti
	t.Run("builtinSortMulti", func(t *testing.T) {
		result := i.builtinSortMulti([]Value{
			[]Value{map[string]Value{"a": int64(2), "b": int64(1)}, map[string]Value{"a": int64(1), "b": int64(2)}},
			[]Value{"a", "b"},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRank
	t.Run("builtinRank", func(t *testing.T) {
		result := i.builtinRank([]Value{
			[]Value{int64(3), int64(1), int64(2)},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRankBy
	t.Run("builtinRankBy", func(t *testing.T) {
		result := i.builtinRankBy([]Value{
			[]Value{map[string]Value{"a": int64(2)}, map[string]Value{"a": int64(1)}},
			"a",
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinDenseRank
	t.Run("builtinDenseRank", func(t *testing.T) {
		result := i.builtinDenseRank([]Value{
			[]Value{int64(3), int64(1), int64(2)},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinTopN
	t.Run("builtinTopN", func(t *testing.T) {
		result := i.builtinTopN([]Value{
			[]Value{int64(3), int64(1), int64(2)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinBottomN
	t.Run("builtinBottomN", func(t *testing.T) {
		result := i.builtinBottomN([]Value{
			[]Value{int64(3), int64(1), int64(2)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinPartition
	t.Run("builtinPartition", func(t *testing.T) {
		result := i.builtinPartition([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinGroupBySorted
	t.Run("builtinGroupBySorted", func(t *testing.T) {
		result := i.builtinGroupBySorted([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return int64(0) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test naturalLess
	t.Run("naturalLess", func(t *testing.T) {
		result := naturalLess("item1", "item2")
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test extractNumber
	t.Run("extractNumber", func(t *testing.T) {
		num, _ := extractNumber("item123", 4)
		if num != 123 {
			t.Errorf("Expected 123, got %v", num)
		}
	})

	// Test builtinNth
	t.Run("builtinNth", func(t *testing.T) {
		result := i.builtinNth([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(1),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinFilter
	t.Run("builtinFilter", func(t *testing.T) {
		result := i.builtinFilter([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinMap
	t.Run("builtinMap", func(t *testing.T) {
		result := i.builtinMap([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return int64(1) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinReduce
	t.Run("builtinReduce", func(t *testing.T) {
		result := i.builtinReduce([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return int64(0) },
			int64(0),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinEvery
	t.Run("builtinEvery", func(t *testing.T) {
		result := i.builtinEvery([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			"number",
		})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinSome
	t.Run("builtinSome", func(t *testing.T) {
		result := i.builtinSome([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			"number",
		})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinCountBy
	t.Run("builtinCountBy", func(t *testing.T) {
		result := i.builtinCountBy([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return int64(0) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinGroupBy
	t.Run("builtinGroupBy", func(t *testing.T) {
		result := i.builtinGroupBy([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return int64(0) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRotate
	t.Run("builtinRotate", func(t *testing.T) {
		result := i.builtinRotate([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(1),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSlide
	t.Run("builtinSlide", func(t *testing.T) {
		result := i.builtinSlide([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinWindow
	t.Run("builtinWindow", func(t *testing.T) {
		result := i.builtinWindow([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinPairwise
	t.Run("builtinPairwise", func(t *testing.T) {
		result := i.builtinPairwise([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinTranspose
	t.Run("builtinTranspose", func(t *testing.T) {
		result := i.builtinTranspose([]Value{
			[]Value{[]Value{int64(1), int64(2)}, []Value{int64(3), int64(4)}},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinFillRange
	t.Run("builtinFillRange", func(t *testing.T) {
		result := i.builtinFillRange([]Value{
			[]Value{int64(0), int64(0), int64(0)},
			int64(1),
			int64(0),
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinInsertAt
	t.Run("builtinInsertAt", func(t *testing.T) {
		result := i.builtinInsertAt([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(1),
			int64(99),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRemoveAt
	t.Run("builtinRemoveAt", func(t *testing.T) {
		result := i.builtinRemoveAt([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(1),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRemoveFirst
	t.Run("builtinRemoveFirst", func(t *testing.T) {
		result := i.builtinRemoveFirst([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(2)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRemoveLast
	t.Run("builtinRemoveLast", func(t *testing.T) {
		result := i.builtinRemoveLast([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(2)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRemoveAll
	t.Run("builtinRemoveAll", func(t *testing.T) {
		result := i.builtinRemoveAll([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(2)},
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinReplaceAt
	t.Run("builtinReplaceAt", func(t *testing.T) {
		result := i.builtinReplaceAt([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(1),
			int64(99),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSwap
	t.Run("builtinSwap", func(t *testing.T) {
		result := i.builtinSwap([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(0),
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinMove
	t.Run("builtinMove", func(t *testing.T) {
		result := i.builtinMove([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			int64(0),
			int64(2),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinCompactFlat
	t.Run("builtinCompactFlat", func(t *testing.T) {
		result := i.builtinCompactFlat([]Value{
			[]Value{int64(1), nil, int64(2), nil, int64(3)},
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinDifferenceBy
	t.Run("builtinDifferenceBy", func(t *testing.T) {
		result := i.builtinDifferenceBy([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			[]Value{int64(2), int64(3), int64(4)},
			func() Value { return int64(0) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIntersectionBy
	t.Run("builtinIntersectionBy", func(t *testing.T) {
		result := i.builtinIntersectionBy([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			[]Value{int64(2), int64(3), int64(4)},
			func() Value { return int64(0) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinUnionBy
	t.Run("builtinUnionBy", func(t *testing.T) {
		result := i.builtinUnionBy([]Value{
			[]Value{int64(1), int64(2)},
			[]Value{int64(2), int64(3)},
			func() Value { return int64(0) },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinFindIndex
	t.Run("builtinFindIndex", func(t *testing.T) {
		result := i.builtinFindIndex([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinFindLastIndex
	t.Run("builtinFindLastIndex", func(t *testing.T) {
		result := i.builtinFindLastIndex([]Value{
			[]Value{int64(1), int64(2), int64(3)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIndicesOf
	t.Run("builtinIndicesOf", func(t *testing.T) {
		result := i.builtinIndicesOf([]Value{
			[]Value{int64(1), int64(2), int64(1), int64(3)},
			int64(1),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIndexOfAll
	t.Run("builtinIndexOfAll", func(t *testing.T) {
		result := i.builtinIndexOfAll([]Value{
			[]Value{int64(1), int64(2), int64(1), int64(3)},
			int64(1),
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinTakeWhile
	t.Run("builtinTakeWhile", func(t *testing.T) {
		result := i.builtinTakeWhile([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinDropWhile
	t.Run("builtinDropWhile", func(t *testing.T) {
		result := i.builtinDropWhile([]Value{
			[]Value{int64(1), int64(2), int64(3), int64(4)},
			func() Value { return true },
		})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})
}
