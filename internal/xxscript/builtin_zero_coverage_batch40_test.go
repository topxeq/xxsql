package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch40_EveryAndSome(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinEvery([]Value{}); got != false {
		t.Fatalf("expected every() false for missing args, got %v", got)
	}
	if got := i.builtinEvery([]Value{"bad", "number"}); got != true {
		t.Fatalf("expected every(non-array) true, got %v", got)
	}
	if got := i.builtinEvery([]Value{[]Value{int64(2), 4.0, 6}, "number"}); got != true {
		t.Fatalf("expected every(number)=true, got %v", got)
	}
	if got := i.builtinEvery([]Value{[]Value{int64(2), "x"}, "number"}); got != false {
		t.Fatalf("expected every(number)=false for mixed array, got %v", got)
	}
	if got := i.builtinEvery([]Value{[]Value{int64(2), int64(4)}, "even"}); got != true {
		t.Fatalf("expected every(even)=true, got %v", got)
	}
	if got := i.builtinEvery([]Value{[]Value{int64(-1), int64(-2)}, "negative"}); got != true {
		t.Fatalf("expected every(negative)=true, got %v", got)
	}

	if got := i.builtinSome([]Value{}); got != false {
		t.Fatalf("expected some() false for missing args, got %v", got)
	}
	if got := i.builtinSome([]Value{"bad", "number"}); got != false {
		t.Fatalf("expected some(non-array) false, got %v", got)
	}
	if got := i.builtinSome([]Value{[]Value{int64(1), "x"}, "string"}); got != true {
		t.Fatalf("expected some(string)=true, got %v", got)
	}
	if got := i.builtinSome([]Value{[]Value{int64(1), int64(3)}, "even"}); got != false {
		t.Fatalf("expected some(even)=false, got %v", got)
	}
	if got := i.builtinSome([]Value{[]Value{nil, int64(3)}, "null"}); got != true {
		t.Fatalf("expected some(null)=true, got %v", got)
	}
	if got := i.builtinSome([]Value{[]Value{[]Value{1}, map[string]Value{"a": 1}}, "object"}); got != true {
		t.Fatalf("expected some(object)=true, got %v", got)
	}
}
