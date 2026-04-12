package xxscript

import (
	"math"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch67_MathWrappersAndEdges(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	approx := func(got Value, want float64) bool {
		gf, ok := got.(float64)
		if !ok {
			return false
		}
		return math.Abs(gf-want) < 1e-9
	}

	t.Run("absMinMaxFloorCeilRound", func(t *testing.T) {
		if got := i.builtinAbs([]Value{}); got != 0 {
			t.Fatalf("expected 0 abs with no args, got %v", got)
		}
		if got := i.builtinAbs([]Value{-3}); got != 3 {
			t.Fatalf("expected abs(-3)=3, got %v", got)
		}
		if got := i.builtinAbs([]Value{int64(-7)}); got != int64(7) {
			t.Fatalf("expected abs(int64 -7)=7, got %v", got)
		}
		if got := i.builtinAbs([]Value{-2.5}); got != 2.5 {
			t.Fatalf("expected abs(-2.5)=2.5, got %v", got)
		}
		if got := i.builtinAbs([]Value{"x"}); got != 0 {
			t.Fatalf("expected 0 abs for unsupported type, got %v", got)
		}

		if got := i.builtinMin([]Value{}); got != 0 {
			t.Fatalf("expected 0 min no args, got %v", got)
		}
		if got := i.builtinMin([]Value{5, 2.5, 3}); got != 2.5 {
			t.Fatalf("expected min 2.5, got %v", got)
		}

		if got := i.builtinMax([]Value{}); got != 0 {
			t.Fatalf("expected 0 max no args, got %v", got)
		}
		if got := i.builtinMax([]Value{5, 2.5, 9}); got != 9.0 {
			t.Fatalf("expected max 9, got %v", got)
		}

		if got := i.builtinFloor([]Value{}); got != 0 {
			t.Fatalf("expected 0 floor no args, got %v", got)
		}
		if got := i.builtinFloor([]Value{3.8}); got != 3 {
			t.Fatalf("expected floor int cast 3, got %v", got)
		}

		if got := i.builtinCeil([]Value{}); got != 0 {
			t.Fatalf("expected 0 ceil no args, got %v", got)
		}
		if got := i.builtinCeil([]Value{3.1}); got != 4 {
			t.Fatalf("expected ceil impl result 4, got %v", got)
		}

		if got := i.builtinRound([]Value{}); got != 0 {
			t.Fatalf("expected 0 round no args, got %v", got)
		}
		if got := i.builtinRound([]Value{3.6}); got != 4 {
			t.Fatalf("expected round impl result 4, got %v", got)
		}
	})

	t.Run("rootsPowerTrigAndLogs", func(t *testing.T) {
		if got := i.builtinSqrt([]Value{}); got != 0 {
			t.Fatalf("expected 0 sqrt no args, got %v", got)
		}
		if got := i.builtinSqrt([]Value{-1.0}); got != 0 {
			t.Fatalf("expected 0 sqrt negative input, got %v", got)
		}
		if got := i.builtinSqrt([]Value{0.0}); got != 0.0 {
			t.Fatalf("expected 0 sqrt zero input, got %v", got)
		}
		if got := i.builtinSqrt([]Value{9.0}); !approx(got, 3.0) {
			t.Fatalf("expected sqrt(9)=3, got %v", got)
		}

		if got := i.builtinPow([]Value{2}); got != 0 {
			t.Fatalf("expected 0 pow too few args, got %v", got)
		}
		if got := i.builtinPow([]Value{2, 3}); got != 8.0 {
			t.Fatalf("expected pow(2,3)=8, got %v", got)
		}

		if got := i.builtinSin([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 sin no args, got %v", got)
		}
		if got := i.builtinCos([]Value{}); got != 1.0 {
			t.Fatalf("expected 1 cos no args, got %v", got)
		}
		if got := i.builtinTan([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 tan no args, got %v", got)
		}
		if got := i.builtinAsin([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 asin no args, got %v", got)
		}
		if got := i.builtinAcos([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 acos no args, got %v", got)
		}
		if got := i.builtinAtan([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 atan no args, got %v", got)
		}
		if got := i.builtinAtan2([]Value{1}); got != 0.0 {
			t.Fatalf("expected 0 atan2 too few args, got %v", got)
		}
		if got := i.builtinAtan2([]Value{1, 1}); !approx(got, math.Pi/4) {
			t.Fatalf("expected atan2(1,1)=pi/4, got %v", got)
		}
		if got := i.builtinSinh([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 sinh no args, got %v", got)
		}
		if got := i.builtinCosh([]Value{}); got != 1.0 {
			t.Fatalf("expected 1 cosh no args, got %v", got)
		}
		if got := i.builtinTanh([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 tanh no args, got %v", got)
		}

		if got := i.builtinLog([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 log no args, got %v", got)
		}
		if got := i.builtinLog10([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 log10 no args, got %v", got)
		}
		if got := i.builtinLog2([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 log2 no args, got %v", got)
		}
		if got := i.builtinExp([]Value{}); got != 1.0 {
			t.Fatalf("expected 1 exp no args, got %v", got)
		}
	})

	t.Run("otherMathAndPredicates", func(t *testing.T) {
		if got := i.builtinCbrt([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 cbrt no args, got %v", got)
		}
		if got := i.builtinHypot([]Value{3}); got != 0.0 {
			t.Fatalf("expected 0 hypot too few args, got %v", got)
		}
		if got := i.builtinHypot([]Value{3, 4}); got != 5.0 {
			t.Fatalf("expected hypot(3,4)=5, got %v", got)
		}

		if got := i.builtinSign([]Value{}); got != 0 {
			t.Fatalf("expected 0 sign no args, got %v", got)
		}
		if got := i.builtinSign([]Value{3}); got != 1 {
			t.Fatalf("expected sign positive 1, got %v", got)
		}
		if got := i.builtinSign([]Value{-3}); got != -1 {
			t.Fatalf("expected sign negative -1, got %v", got)
		}
		if got := i.builtinSign([]Value{0}); got != 0 {
			t.Fatalf("expected sign zero 0, got %v", got)
		}

		if got := i.builtinMod([]Value{10}); got != 0 {
			t.Fatalf("expected 0 mod too few args, got %v", got)
		}
		if got := i.builtinMod([]Value{10, 4}); got != 2.0 {
			t.Fatalf("expected mod(10,4)=2, got %v", got)
		}

		if got := i.builtinDiv([]Value{10}); got != 0 {
			t.Fatalf("expected 0 div too few args, got %v", got)
		}
		if got := i.builtinDiv([]Value{10, 0}); got != 0.0 {
			t.Fatalf("expected 0 div by zero, got %v", got)
		}
		if got := i.builtinDiv([]Value{10, 3}); got != 3.0 {
			t.Fatalf("expected trunc div 10/3=3, got %v", got)
		}

		if got := i.builtinDegrees([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 degrees no args, got %v", got)
		}
		if got := i.builtinRadians([]Value{}); got != 0.0 {
			t.Fatalf("expected 0 radians no args, got %v", got)
		}

		if got := i.builtinIsInf([]Value{}); got != false {
			t.Fatalf("expected false isInf no args, got %v", got)
		}
		if got := i.builtinIsInf([]Value{math.Inf(1)}); got != true {
			t.Fatalf("expected true isInf for +Inf, got %v", got)
		}

		if got := i.builtinIsNaN([]Value{}); got != false {
			t.Fatalf("expected false isNaN no args, got %v", got)
		}
		if got := i.builtinIsNaN([]Value{math.NaN()}); got != true {
			t.Fatalf("expected true isNaN for NaN, got %v", got)
		}
	})
}
