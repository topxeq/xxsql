package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch72_ArrayCombinators(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("splitAtBranches", func(t *testing.T) {
		if got := i.builtinSplitAt([]Value{[]Value{1, 2}}); !reflect.DeepEqual(got, []Value{[]Value{1, 2}, []Value{}}) {
			t.Fatalf("expected splitAt with missing index to return [arr, empty], got %#v", got)
		}
		if got := i.builtinSplitAt([]Value{"x", 1}); !reflect.DeepEqual(got, []Value{[]Value{}, []Value{}}) {
			t.Fatalf("expected splitAt non-array to return empty pair, got %#v", got)
		}
		if got := i.builtinSplitAt([]Value{[]Value{1, 2, 3}, -9}); !reflect.DeepEqual(got, []Value{[]Value{}, []Value{1, 2, 3}}) {
			t.Fatalf("expected splitAt negative index to clamp to 0, got %#v", got)
		}
		if got := i.builtinSplitAt([]Value{[]Value{1, 2, 3}, 99}); !reflect.DeepEqual(got, []Value{[]Value{1, 2, 3}, []Value{}}) {
			t.Fatalf("expected splitAt overflow index to clamp to len, got %#v", got)
		}
	})

	t.Run("rangeStepBranches", func(t *testing.T) {
		if got := i.builtinRangeStep([]Value{1, 4}); !reflect.DeepEqual(got, []Value{0}) {
			t.Fatalf("expected rangeStep fallback to builtinRange semantics, got %#v", got)
		}
		if got := i.builtinRangeStep([]Value{0, 5, 0}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty rangeStep for step 0, got %#v", got)
		}
		if got := i.builtinRangeStep([]Value{0, 6, 2}); !reflect.DeepEqual(got, []Value{int64(0), int64(2), int64(4)}) {
			t.Fatalf("unexpected positive-step rangeStep result, got %#v", got)
		}
		if got := i.builtinRangeStep([]Value{6, 0, -2}); !reflect.DeepEqual(got, []Value{int64(6), int64(4), int64(2)}) {
			t.Fatalf("unexpected negative-step rangeStep result, got %#v", got)
		}
	})

	t.Run("prependAndAppendAll", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic when prependAll called with no args")
			}
		}()
		_ = i.builtinPrependAll([]Value{})
	})

	t.Run("prependAndAppendAllSafe", func(t *testing.T) {
		if got := i.builtinPrependAll([]Value{[]Value{3}, []Value{1}, []Value{2}}); !reflect.DeepEqual(got, []Value{2, 1, 3}) {
			t.Fatalf("unexpected prependAll order result, got %#v", got)
		}
		if got := i.builtinPrependAll([]Value{"x"}); got != "x" {
			t.Fatalf("expected prependAll with single arg to passthrough, got %#v", got)
		}
		if got := i.builtinPrependAll([]Value{"x", []Value{1}, 2}); !reflect.DeepEqual(got, []Value{1}) {
			t.Fatalf("expected prependAll non-array base to use empty base, got %#v", got)
		}

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic when appendAll called with no args")
			}
		}()
		_ = i.builtinAppendAll([]Value{})
	})

	t.Run("appendAllSafe", func(t *testing.T) {
		if got := i.builtinAppendAll([]Value{[]Value{1}, []Value{2}, []Value{3}}); !reflect.DeepEqual(got, []Value{1, 2, 3}) {
			t.Fatalf("unexpected appendAll result, got %#v", got)
		}
		if got := i.builtinAppendAll([]Value{"x"}); got != "x" {
			t.Fatalf("expected appendAll with single arg to passthrough, got %#v", got)
		}
		if got := i.builtinAppendAll([]Value{"x", []Value{2}, 3}); !reflect.DeepEqual(got, []Value{2}) {
			t.Fatalf("expected appendAll non-array base to use empty base, got %#v", got)
		}
	})

	t.Run("intercalateBranches", func(t *testing.T) {
		if got := i.builtinIntercalate([]Value{[]Value{"a"}}); !reflect.DeepEqual(got, []Value{"a"}) {
			t.Fatalf("expected intercalate with one arg to passthrough, got %#v", got)
		}
		if got := i.builtinIntercalate([]Value{"x", []Value{"-"}}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected intercalate non-array input to return empty array, got %#v", got)
		}
		if got := i.builtinIntercalate([]Value{[]Value{"a", "b", "c"}, "-"}); !reflect.DeepEqual(got, []Value{"a", "-", "b", "-", "c"}) {
			t.Fatalf("expected intercalate non-array separator to fallback to intersperse, got %#v", got)
		}
		if got := i.builtinIntercalate([]Value{[]Value{"a", "b", "c"}, []Value{"-", "/"}}); !reflect.DeepEqual(got, []Value{"a", "-", "/", "b", "-", "/", "c"}) {
			t.Fatalf("unexpected intercalate flattened separator result, got %#v", got)
		}
	})

	t.Run("repeatAndCycle", func(t *testing.T) {
		if got := i.builtinRepeatAll([]Value{}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty repeatAll with too few args, got %#v", got)
		}
		if got := i.builtinRepeatAll([]Value{"x", 3}); !reflect.DeepEqual(got, []Value{"x", "x", "x"}) {
			t.Fatalf("unexpected repeatAll result, got %#v", got)
		}

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic when cycle called with no args")
			}
		}()
		_ = i.builtinCycle([]Value{})
	})

	t.Run("cycleSafe", func(t *testing.T) {
		if got := i.builtinCycle([]Value{"x", 3}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty cycle for non-array input, got %#v", got)
		}
		if got := i.builtinCycle([]Value{[]Value{}, 3}); !reflect.DeepEqual(got, []Value{}) {
			t.Fatalf("expected empty cycle for empty input array, got %#v", got)
		}
		if got := i.builtinCycle([]Value{[]Value{1, 2}, 2}); !reflect.DeepEqual(got, []Value{1, 2, 1, 2}) {
			t.Fatalf("unexpected cycle result, got %#v", got)
		}
	})
}
