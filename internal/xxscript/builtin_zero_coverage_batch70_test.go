package xxscript

import (
	"math"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch70_NumberTheoryAndStats(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	t.Run("naturalLessCharBranch", func(t *testing.T) {
		if !naturalLess("abc", "abd") {
			t.Fatalf("expected lexical char compare branch to return true")
		}
		if !naturalLess("a2b", "a2c") {
			t.Fatalf("expected naturalLess to continue after equal numbers")
		}
		if naturalLess("b", "a") {
			t.Fatalf("expected lexical compare false when left is greater")
		}
	})

	t.Run("numberTheory", func(t *testing.T) {
		if got := i.builtinFactorial([]Value{}); got != int64(1) {
			t.Fatalf("expected factorial no-arg to be 1, got %#v", got)
		}
		if got := i.builtinFactorial([]Value{-3}); got != int64(0) {
			t.Fatalf("expected factorial negative to be 0, got %#v", got)
		}
		if got := i.builtinFactorial([]Value{1}); got != int64(1) {
			t.Fatalf("expected factorial(1)=1, got %#v", got)
		}
		if got := i.builtinFactorial([]Value{5}); got != int64(120) {
			t.Fatalf("expected factorial(5)=120, got %#v", got)
		}

		if got := i.builtinGCD([]Value{12}); got != int64(0) {
			t.Fatalf("expected gcd with too few args to be 0, got %#v", got)
		}
		if got := i.builtinGCD([]Value{-18, 12}); got != int64(6) {
			t.Fatalf("expected gcd(-18,12)=6, got %#v", got)
		}
		if got := i.builtinGCD([]Value{18, -12}); got != int64(6) {
			t.Fatalf("expected gcd(18,-12)=6, got %#v", got)
		}
		if got := i.builtinGCD([]Value{9, 0}); got != int64(9) {
			t.Fatalf("expected gcd(9,0)=9, got %#v", got)
		}

		if got := i.builtinLCM([]Value{12}); got != int64(0) {
			t.Fatalf("expected lcm with too few args to be 0, got %#v", got)
		}
		if got := i.builtinLCM([]Value{0, 12}); got != int64(0) {
			t.Fatalf("expected lcm with zero to be 0, got %#v", got)
		}
		if got := i.builtinLCM([]Value{-6, 8}); got != int64(24) {
			t.Fatalf("expected lcm(-6,8)=24, got %#v", got)
		}
		if got := i.builtinLCM([]Value{6, -8}); got != int64(24) {
			t.Fatalf("expected lcm(6,-8)=24, got %#v", got)
		}

		if got := i.builtinFibonacci([]Value{}); got != int64(0) {
			t.Fatalf("expected fibonacci no-arg to be 0, got %#v", got)
		}
		if got := i.builtinFibonacci([]Value{-1}); got != int64(0) {
			t.Fatalf("expected fibonacci negative to be 0, got %#v", got)
		}
		if got := i.builtinFibonacci([]Value{1}); got != int64(1) {
			t.Fatalf("expected fibonacci(1)=1, got %#v", got)
		}
		if got := i.builtinFibonacci([]Value{7}); got != int64(13) {
			t.Fatalf("expected fibonacci(7)=13, got %#v", got)
		}

		if got := i.builtinBinomial([]Value{5}); got != int64(0) {
			t.Fatalf("expected binomial with too few args to be 0, got %#v", got)
		}
		if got := i.builtinBinomial([]Value{5, -1}); got != int64(0) {
			t.Fatalf("expected binomial invalid k to be 0, got %#v", got)
		}
		if got := i.builtinBinomial([]Value{5, 0}); got != int64(1) {
			t.Fatalf("expected binomial(5,0)=1, got %#v", got)
		}
		if got := i.builtinBinomial([]Value{5, 2}); got != int64(10) {
			t.Fatalf("expected binomial(5,2)=10, got %#v", got)
		}
		if got := i.builtinBinomial([]Value{6, 4}); got != int64(15) {
			t.Fatalf("expected binomial(6,4)=15 after k reduction, got %#v", got)
		}
	})

	t.Run("clampLerpAndStats", func(t *testing.T) {
		if got := i.builtinClamp([]Value{1, 0}); got != 0 {
			t.Fatalf("expected clamp too-few args to be 0, got %#v", got)
		}
		if got := i.builtinClamp([]Value{10, 0, 5}).(float64); got != 5 {
			t.Fatalf("expected clamp(10,0,5)=5, got %#v", got)
		}

		if got := i.builtinLerp([]Value{1, 2}); got != 0 {
			t.Fatalf("expected lerp too-few args to be 0, got %#v", got)
		}
		if got := i.builtinLerp([]Value{10, 20, 0.25}).(float64); got != 12.5 {
			t.Fatalf("expected lerp(10,20,0.25)=12.5, got %#v", got)
		}

		if got := i.builtinSum([]Value{}); got != 0 {
			t.Fatalf("expected sum no-arg to be 0, got %#v", got)
		}
		if got := i.builtinSum([]Value{3}).(float64); got != 3 {
			t.Fatalf("expected sum scalar passthrough 3, got %#v", got)
		}
		if got := i.builtinSum([]Value{[]Value{1, int64(2), 3.5}}).(float64); got != 6.5 {
			t.Fatalf("expected sum array to be 6.5, got %#v", got)
		}

		if got := i.builtinProduct([]Value{}); got != 1 {
			t.Fatalf("expected product no-arg to be 1, got %#v", got)
		}
		if got := i.builtinProduct([]Value{4}).(float64); got != 4 {
			t.Fatalf("expected product scalar passthrough 4, got %#v", got)
		}
		if got := i.builtinProduct([]Value{[]Value{2, int64(3), 1.5}}).(float64); got != 9 {
			t.Fatalf("expected product array to be 9, got %#v", got)
		}

		if got := i.builtinMean([]Value{}); got != 0 {
			t.Fatalf("expected mean no-arg to be 0, got %#v", got)
		}
		if got := i.builtinMean([]Value{[]Value{}}); got != 0 {
			t.Fatalf("expected mean empty array to be 0, got %#v", got)
		}
		if got := i.builtinMean([]Value{[]Value{1, 2, 3}}).(float64); got != 2 {
			t.Fatalf("expected mean([1,2,3])=2, got %#v", got)
		}

		if got := i.builtinMedian([]Value{}); got != 0 {
			t.Fatalf("expected median no-arg to be 0, got %#v", got)
		}
		if got := i.builtinMedian([]Value{[]Value{}}); got != 0 {
			t.Fatalf("expected median empty array to be 0, got %#v", got)
		}
		if got := i.builtinMedian([]Value{[]Value{3, 1, 2}}).(float64); got != 2 {
			t.Fatalf("expected median odd to be 2, got %#v", got)
		}
		if got := i.builtinMedian([]Value{[]Value{4, 1, 3, 2}}).(float64); got != 2.5 {
			t.Fatalf("expected median even to be 2.5, got %#v", got)
		}

		if got := i.builtinPercentile([]Value{[]Value{1}}); got != 0 {
			t.Fatalf("expected percentile too-few args to be 0, got %#v", got)
		}
		if got := i.builtinPercentile([]Value{[]Value{}, 50}); got != 0 {
			t.Fatalf("expected percentile empty array to be 0, got %#v", got)
		}
		if got := i.builtinPercentile([]Value{[]Value{1, 2, 3, 4}, -5}).(float64); got != 1 {
			t.Fatalf("expected percentile p<0 to clamp to min, got %#v", got)
		}
		if got := i.builtinPercentile([]Value{[]Value{1, 2, 3, 4}, 200}).(float64); got != 4 {
			t.Fatalf("expected percentile p>100 to clamp to max, got %#v", got)
		}
		if got := i.builtinPercentile([]Value{[]Value{1, 2, 3, 4}, 25}).(float64); math.Abs(got-1.75) > 1e-9 {
			t.Fatalf("expected percentile 25 to interpolate to 1.75, got %#v", got)
		}
	})
}
