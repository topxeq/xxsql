package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch69_ArraySelectionAndNaturalSort(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("topAndBottomN", func(t *testing.T) {
		if got := i.builtinTopN([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty topN for no args, got %#v", got)
		}
		if got := i.builtinTopN([]Value{"not-array", 2}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty topN for non-array input, got %#v", got)
		}
		if got := i.builtinTopN([]Value{[]Value{int64(3), int64(1)}, 0}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty topN for non-positive n, got %#v", got)
		}
		if got := i.builtinTopN([]Value{[]Value{int64(3), int64(1), int64(2)}, 10}).([]Value); len(got) != 3 || i.toInt(got[0]) != 3 || i.toInt(got[1]) != 2 || i.toInt(got[2]) != 1 {
			t.Fatalf("unexpected topN full-length result: %#v", got)
		}

		if got := i.builtinBottomN([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty bottomN for no args, got %#v", got)
		}
		if got := i.builtinBottomN([]Value{"not-array", 2}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty bottomN for non-array input, got %#v", got)
		}
		if got := i.builtinBottomN([]Value{[]Value{int64(3), int64(1)}, -1}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty bottomN for non-positive n, got %#v", got)
		}
		if got := i.builtinBottomN([]Value{[]Value{int64(3), int64(1), int64(2)}, 10}).([]Value); len(got) != 3 || i.toInt(got[0]) != 1 || i.toInt(got[1]) != 2 || i.toInt(got[2]) != 3 {
			t.Fatalf("unexpected bottomN full-length result: %#v", got)
		}
	})

	t.Run("mapAndNaturalHelpers", func(t *testing.T) {
		if got := getMapValue("x", "k"); got != nil {
			t.Fatalf("expected nil getMapValue for non-map input, got %#v", got)
		}
		if got := getMapValue(map[string]Value{"k": "v"}, "missing"); got != nil {
			t.Fatalf("expected nil getMapValue for missing key, got %#v", got)
		}
		if got := getMapValue(map[string]Value{"k": "v"}, "k"); got != "v" {
			t.Fatalf("expected map value lookup success, got %#v", got)
		}

		if !naturalLess("item2", "item10") {
			t.Fatalf("expected naturalLess numeric compare to be true")
		}
		if naturalLess("item10", "item2") {
			t.Fatalf("expected naturalLess numeric compare to be false")
		}
		if !naturalLess("ab", "abc") {
			t.Fatalf("expected shorter-prefix string to be less")
		}
		if naturalLess("same", "same") {
			t.Fatalf("expected equal strings to not be less")
		}
	})

	t.Run("flattenAndChunk", func(t *testing.T) {
		if got := i.builtinFlatten([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty flatten for no args, got %#v", got)
		}
		if got := i.builtinFlatten([]Value{"x"}); !reflect.DeepEqual(got, []Value{"x"}) {
			t.Fatalf("expected singleton flatten for non-array input, got %#v", got)
		}
		nested := []Value{int64(1), []Value{int64(2), []Value{int64(3)}}}
		if got := i.builtinFlatten([]Value{nested, 0}); !reflect.DeepEqual(got, nested) {
			t.Fatalf("expected depth-0 flatten to preserve nesting, got %#v", got)
		}
		if got := i.builtinFlatten([]Value{nested, 1}); !reflect.DeepEqual(got, []Value{int64(1), int64(2), []Value{int64(3)}}) {
			t.Fatalf("expected one-level flatten, got %#v", got)
		}

		if got := i.builtinChunk([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty chunk for no args, got %#v", got)
		}
		if got := i.builtinChunk([]Value{"x", 2}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty chunk for non-array input, got %#v", got)
		}
		if got := i.builtinChunk([]Value{[]Value{1, 2}, 0}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty chunk for non-positive size, got %#v", got)
		}
		if got := i.builtinChunk([]Value{[]Value{int64(1), int64(2), int64(3)}, 2}); !reflect.DeepEqual(got, []Value{[]Value{int64(1), int64(2)}, []Value{int64(3)}}) {
			t.Fatalf("unexpected chunk result with tail remainder, got %#v", got)
		}
	})

	t.Run("takeDropAndElementAccess", func(t *testing.T) {
		if got := i.builtinTake([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty take for no args, got %#v", got)
		}
		if got := i.builtinTake([]Value{"x", 1}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty take for non-array input, got %#v", got)
		}
		if got := i.builtinTake([]Value{[]Value{1, 2}, 0}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty take for non-positive n, got %#v", got)
		}
		if got := i.builtinTake([]Value{[]Value{1, 2, 3}, 9}); !reflect.DeepEqual(got, []Value{1, 2, 3}) {
			t.Fatalf("expected take to clamp to array length, got %#v", got)
		}

		if got := i.builtinDrop([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty drop for no args, got %#v", got)
		}
		if got := i.builtinDrop([]Value{"x", 1}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty drop for non-array input, got %#v", got)
		}
		if got := i.builtinDrop([]Value{[]Value{1, 2, 3}, 0}); !reflect.DeepEqual(got, []Value{1, 2, 3}) {
			t.Fatalf("expected drop<=0 to return full array, got %#v", got)
		}
		if got := i.builtinDrop([]Value{[]Value{1, 2, 3}, 3}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty drop when n>=len, got %#v", got)
		}
		if got := i.builtinDrop([]Value{[]Value{1, 2, 3}, 1}); !reflect.DeepEqual(got, []Value{2, 3}) {
			t.Fatalf("expected proper tail from drop, got %#v", got)
		}

		if got := i.builtinFirst([]Value{}); got != nil {
			t.Fatalf("expected nil first for no args, got %#v", got)
		}
		if got := i.builtinFirst([]Value{"x"}); got != nil {
			t.Fatalf("expected nil first for non-array input, got %#v", got)
		}
		if got := i.builtinFirst([]Value{[]Value{}}); got != nil {
			t.Fatalf("expected nil first for empty array, got %#v", got)
		}
		if got := i.builtinFirst([]Value{[]Value{"a", "b"}}); got != "a" {
			t.Fatalf("expected first element, got %#v", got)
		}

		if got := i.builtinLast([]Value{}); got != nil {
			t.Fatalf("expected nil last for no args, got %#v", got)
		}
		if got := i.builtinLast([]Value{"x"}); got != nil {
			t.Fatalf("expected nil last for non-array input, got %#v", got)
		}
		if got := i.builtinLast([]Value{[]Value{}}); got != nil {
			t.Fatalf("expected nil last for empty array, got %#v", got)
		}
		if got := i.builtinLast([]Value{[]Value{"a", "b"}}); got != "b" {
			t.Fatalf("expected last element, got %#v", got)
		}

		if got := i.builtinNth([]Value{}); got != nil {
			t.Fatalf("expected nil nth for no args, got %#v", got)
		}
		if got := i.builtinNth([]Value{"x", 0}); got != nil {
			t.Fatalf("expected nil nth for non-array input, got %#v", got)
		}
		if got := i.builtinNth([]Value{[]Value{"a", "b"}, -3}); got != nil {
			t.Fatalf("expected nil nth for negative out-of-range index, got %#v", got)
		}
		if got := i.builtinNth([]Value{[]Value{"a", "b"}, 2}); got != nil {
			t.Fatalf("expected nil nth for positive out-of-range index, got %#v", got)
		}
		if got := i.builtinNth([]Value{[]Value{"a", "b"}, -1}); got != "b" {
			t.Fatalf("expected negative index nth to access from end, got %#v", got)
		}
		if got := i.builtinNth([]Value{[]Value{"a", "b"}, 0}); got != "a" {
			t.Fatalf("expected nth index 0 element, got %#v", got)
		}

		if got := i.builtinFind([]Value{}); got != nil {
			t.Fatalf("expected nil find for no args, got %#v", got)
		}
		if got := i.builtinFind([]Value{"x", "a"}); got != nil {
			t.Fatalf("expected nil find for non-array input, got %#v", got)
		}
		if got := i.builtinFind([]Value{[]Value{"a", "b"}, "z"}); got != nil {
			t.Fatalf("expected nil find when not found, got %#v", got)
		}
		if got := i.builtinFind([]Value{[]Value{"a", int64(2)}, int(2)}); got != int64(2) {
			t.Fatalf("expected find to compare by string form and match 2, got %#v", got)
		}
	})
}
