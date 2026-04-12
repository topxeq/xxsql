package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch77_HelperNumericMatrices(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("compare_crossTypeBranches", func(t *testing.T) {
		cases := []struct {
			a, b Value
			w    int
		}{
			{int(2), int64(1), 1},
			{int(2), int64(2), 0},
			{int64(1), int(2), -1},
			{int64(2), int(2), 0},
			{float64(2), int(1), 1},
			{float64(2), int(2), 0},
			{float64(1), int64(2), -1},
			{float64(2), int64(2), 0},
			{"a", int(1), 0},
		}
		for _, tc := range cases {
			if got := i.compare(tc.a, tc.b); got != tc.w {
				t.Fatalf("unexpected compare(%#v,%#v)=%d want=%d", tc.a, tc.b, got, tc.w)
			}
		}
	})

	t.Run("sub_mul_div_crossTypes", func(t *testing.T) {
		if got, err := i.sub(int(7), int64(2)); err != nil || got.(int64) != 5 {
			t.Fatalf("unexpected sub int-int64 result got=%#v err=%v", got, err)
		}
		if got, err := i.sub(float64(7), int64(2)); err != nil || got.(float64) != 5 {
			t.Fatalf("unexpected sub float-int64 result got=%#v err=%v", got, err)
		}

		if got, err := i.mul(int64(3), int(4)); err != nil || got.(int64) != 12 {
			t.Fatalf("unexpected mul int64-int result got=%#v err=%v", got, err)
		}
		if got, err := i.mul(float64(1.5), int64(2)); err != nil || got.(float64) != 3 {
			t.Fatalf("unexpected mul float-int64 result got=%#v err=%v", got, err)
		}

		if got, err := i.div(int64(9), int(3)); err != nil || got.(float64) != 3 {
			t.Fatalf("unexpected div int64-int result got=%#v err=%v", got, err)
		}
		if got, err := i.div(float64(9), int64(3)); err != nil || got.(float64) != 3 {
			t.Fatalf("unexpected div float-int64 result got=%#v err=%v", got, err)
		}
		if _, err := i.div(float64(9), int64(0)); err == nil {
			t.Fatalf("expected div float-int64 by zero error")
		}
	})
}
