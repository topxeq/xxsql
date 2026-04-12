package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch59_IsPrimeAndJSONPretty(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinIsPrime([]Value{}); got != false {
		t.Fatalf("builtinIsPrime no-args: expected false, got %v", got)
	}
	if got := i.builtinIsPrime([]Value{int64(1)}); got != false {
		t.Fatalf("builtinIsPrime n<2: expected false, got %v", got)
	}
	if got := i.builtinIsPrime([]Value{int64(2)}); got != true {
		t.Fatalf("builtinIsPrime n==2: expected true, got %v", got)
	}
	if got := i.builtinIsPrime([]Value{int64(8)}); got != false {
		t.Fatalf("builtinIsPrime even composite: expected false, got %v", got)
	}
	if got := i.builtinIsPrime([]Value{int64(9)}); got != false {
		t.Fatalf("builtinIsPrime odd composite: expected false, got %v", got)
	}
	if got := i.builtinIsPrime([]Value{int64(13)}); got != true {
		t.Fatalf("builtinIsPrime odd prime: expected true, got %v", got)
	}

	if got := i.builtinJSONPretty([]Value{}); got != "" {
		t.Fatalf("builtinJSONPretty no-args: expected empty, got %v", got)
	}
	if got := i.builtinJSONPretty([]Value{"{"}); got != "" {
		t.Fatalf("builtinJSONPretty invalid json string: expected empty, got %v", got)
	}

	prettyFromString := i.builtinJSONPretty([]Value{"{\"a\":1,\"b\":2}"}).(string)
	if !strings.Contains(prettyFromString, "\n") || !strings.Contains(prettyFromString, "\"a\": 1") {
		t.Fatalf("builtinJSONPretty valid json string not formatted as expected: %q", prettyFromString)
	}

	prettyFromMap := i.builtinJSONPretty([]Value{map[string]Value{"x": int64(1)}}).(string)
	if !strings.Contains(prettyFromMap, "\n") || !strings.Contains(prettyFromMap, "\"x\": 1") {
		t.Fatalf("builtinJSONPretty map input not formatted as expected: %q", prettyFromMap)
	}

	if got := i.builtinJSONPretty([]Value{make(chan int)}); got != "" {
		t.Fatalf("builtinJSONPretty marshal error path: expected empty, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch59_ToWordsAdditionalBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinToWords([]Value{int64(0)}); got != "zero" {
		t.Fatalf("builtinToWords zero: expected zero, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(7)}); got != "seven" {
		t.Fatalf("builtinToWords ones: expected seven, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(42)}); got != "forty-two" {
		t.Fatalf("builtinToWords tens+ones: expected forty-two, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(300)}); got != "three hundred" {
		t.Fatalf("builtinToWords hundreds: expected three hundred, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(1205)}); got != "one thousand two hundred five" {
		t.Fatalf("builtinToWords thousands: expected one thousand two hundred five, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(2000001)}); got != "two million one" {
		t.Fatalf("builtinToWords millions: expected two million one, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(3000000004)}); got != "three billion four" {
		t.Fatalf("builtinToWords billions: expected three billion four, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(1000000000000)}); got != "one trillion" {
		t.Fatalf("builtinToWords trillions: expected one trillion, got %v", got)
	}
}
