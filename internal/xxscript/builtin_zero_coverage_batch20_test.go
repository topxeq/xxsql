package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch20_PredicateAndOperationHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if !evaluatePredicate(1, "positive") || evaluatePredicate(-1, "positive") {
		t.Fatalf("expected evaluatePredicate positive branch behavior")
	}
	if !evaluatePredicate(-2.0, "negative") || evaluatePredicate(2.0, "negative") {
		t.Fatalf("expected evaluatePredicate negative branch behavior")
	}
	if !evaluatePredicate(0, "zero") || evaluatePredicate(1, "zero") {
		t.Fatalf("expected evaluatePredicate zero branch behavior")
	}
	if !evaluatePredicate(2, "even") || evaluatePredicate(3, "even") {
		t.Fatalf("expected evaluatePredicate even branch behavior")
	}
	if !evaluatePredicate(int64(4), "even") {
		t.Fatalf("expected evaluatePredicate even int64 branch behavior")
	}
	if !evaluatePredicate(int64(3), "odd") || evaluatePredicate(int64(4), "odd") {
		t.Fatalf("expected evaluatePredicate odd branch behavior")
	}
	if !evaluatePredicate(3, "odd") {
		t.Fatalf("expected evaluatePredicate odd int branch behavior")
	}
	if !evaluatePredicate(nil, "null") || !evaluatePredicate(nil, "nil") {
		t.Fatalf("expected evaluatePredicate null/nil behavior")
	}
	if !evaluatePredicate("", "empty") || !evaluatePredicate([]Value{}, "empty") || evaluatePredicate("x", "empty") {
		t.Fatalf("expected evaluatePredicate empty branch behavior")
	}
	if evaluatePredicate("x", "unknown") {
		t.Fatalf("expected unknown predicate to be false")
	}
	if evaluatePredicate("x", "positive") {
		t.Fatalf("expected non-numeric positive predicate to be false")
	}

	if !i.matchesPredicate("x", "string") || i.matchesPredicate(1, "string") {
		t.Fatalf("expected matchesPredicate string branch behavior")
	}
	if !i.matchesPredicate(1, "number") || !i.matchesPredicate(1.2, "numeric") || i.matchesPredicate("1", "numeric") {
		t.Fatalf("expected matchesPredicate number/numeric branch behavior")
	}
	if !i.matchesPredicate([]Value{1}, "array") || !i.matchesPredicate(map[string]Value{"a": 1}, "map") {
		t.Fatalf("expected matchesPredicate array/map branch behavior")
	}
	if !i.matchesPredicate(nil, "nil") || !i.matchesPredicate(nil, "null") {
		t.Fatalf("expected matchesPredicate nil/null behavior")
	}
	if !i.matchesPredicate(1, "positive") || !i.matchesPredicate(-1, "negative") || !i.matchesPredicate(0, "zero") {
		t.Fatalf("expected matchesPredicate sign/zero behavior")
	}
	if !i.matchesPredicate(2, "even") || !i.matchesPredicate(3, "odd") {
		t.Fatalf("expected matchesPredicate even/odd behavior")
	}
	if !i.matchesPredicate("x", "truthy") || !i.matchesPredicate(false, "falsy") {
		t.Fatalf("expected matchesPredicate truthy/falsy behavior")
	}
	if i.matchesPredicate("x", "does-not-exist") {
		t.Fatalf("expected unknown matchesPredicate to be false")
	}

	if v := i.applyOperation(1, "inc"); v != 2.0 {
		t.Fatalf("expected inc -> 2, got %v", v)
	}
	if v := i.applyOperation(2, "decrement"); v != 1.0 {
		t.Fatalf("expected decrement -> 1, got %v", v)
	}
	if v := i.applyOperation(3, "double"); v != 6.0 {
		t.Fatalf("expected double -> 6, got %v", v)
	}
	if v := i.applyOperation(8, "half"); v != 4.0 {
		t.Fatalf("expected half -> 4, got %v", v)
	}
	if v := i.applyOperation(5, "square"); v != 25.0 {
		t.Fatalf("expected square -> 25, got %v", v)
	}
	if v := i.applyOperation(7, "negate"); v != -7.0 {
		t.Fatalf("expected negate -> -7, got %v", v)
	}
	if v := i.applyOperation(10, "string"); v != "10" {
		t.Fatalf("expected string conversion -> 10, got %v", v)
	}
	if v := i.applyOperation("keep", "unknown"); v != "keep" {
		t.Fatalf("expected unknown op to return original value, got %v", v)
	}
}
