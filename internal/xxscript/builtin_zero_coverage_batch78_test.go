package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch78_HelperArithmeticCoverage(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("sub_matrix", func(t *testing.T) {
		cases := []struct {
			a, b Value
			w    Value
		}{
			{int(8), int(3), int(5)},
			{int(8), int64(3), int64(5)},
			{int(8), float64(3), float64(5)},
			{int64(8), int(3), int64(5)},
			{int64(8), int64(3), int64(5)},
			{int64(8), float64(3), float64(5)},
			{float64(8), int(3), float64(5)},
			{float64(8), int64(3), float64(5)},
			{float64(8), float64(3), float64(5)},
		}
		for _, tc := range cases {
			got, err := i.sub(tc.a, tc.b)
			if err != nil {
				t.Fatalf("unexpected sub error for %#v-%#v: %v", tc.a, tc.b, err)
			}
			if !valuesEqual(got, tc.w) {
				t.Fatalf("unexpected sub result for %#v-%#v: got=%#v want=%#v", tc.a, tc.b, got, tc.w)
			}
		}
		if _, err := i.sub("x", 1); err == nil {
			t.Fatalf("expected sub type error")
		}
	})

	t.Run("mul_matrix", func(t *testing.T) {
		cases := []struct {
			a, b Value
			w    Value
		}{
			{int(2), int(3), int(6)},
			{int(2), int64(3), int64(6)},
			{int(2), float64(3), float64(6)},
			{int64(2), int(3), int64(6)},
			{int64(2), int64(3), int64(6)},
			{int64(2), float64(3), float64(6)},
			{float64(2), int(3), float64(6)},
			{float64(2), int64(3), float64(6)},
			{float64(2), float64(3), float64(6)},
		}
		for _, tc := range cases {
			got, err := i.mul(tc.a, tc.b)
			if err != nil {
				t.Fatalf("unexpected mul error for %#v*%#v: %v", tc.a, tc.b, err)
			}
			if !valuesEqual(got, tc.w) {
				t.Fatalf("unexpected mul result for %#v*%#v: got=%#v want=%#v", tc.a, tc.b, got, tc.w)
			}
		}
		if _, err := i.mul(true, 1); err == nil {
			t.Fatalf("expected mul type error")
		}
	})

	t.Run("div_matrix", func(t *testing.T) {
		cases := []struct {
			a, b Value
			w    float64
		}{
			{int(8), int(2), 4},
			{int(8), int64(2), 4},
			{int(8), float64(2), 4},
			{int64(8), int(2), 4},
			{int64(8), int64(2), 4},
			{int64(8), float64(2), 4},
			{float64(8), int(2), 4},
			{float64(8), int64(2), 4},
			{float64(8), float64(2), 4},
		}
		for _, tc := range cases {
			got, err := i.div(tc.a, tc.b)
			if err != nil {
				t.Fatalf("unexpected div error for %#v/%#v: %v", tc.a, tc.b, err)
			}
			if got.(float64) != tc.w {
				t.Fatalf("unexpected div result for %#v/%#v: got=%#v want=%#v", tc.a, tc.b, got, tc.w)
			}
		}
		if _, err := i.div(int64(3), int64(0)); err == nil {
			t.Fatalf("expected div zero error")
		}
		if _, err := i.div("x", 1); err == nil {
			t.Fatalf("expected div type error")
		}
	})
}
