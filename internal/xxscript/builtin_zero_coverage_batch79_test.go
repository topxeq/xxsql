package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch79_CompareMatrix(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		a, b Value
		w    int
	}{
		{int(1), int(2), -1}, {int(2), int(1), 1}, {int(2), int(2), 0},
		{int(1), int64(2), -1}, {int(2), int64(1), 1}, {int(2), int64(2), 0},
		{int(1), float64(2), -1}, {int(2), float64(1), 1}, {int(2), float64(2), 0},

		{int64(1), int(2), -1}, {int64(2), int(1), 1}, {int64(2), int(2), 0},
		{int64(1), int64(2), -1}, {int64(2), int64(1), 1}, {int64(2), int64(2), 0},
		{int64(1), float64(2), -1}, {int64(2), float64(1), 1}, {int64(2), float64(2), 0},

		{float64(1), int(2), -1}, {float64(2), int(1), 1}, {float64(2), int(2), 0},
		{float64(1), int64(2), -1}, {float64(2), int64(1), 1}, {float64(2), int64(2), 0},
		{float64(1), float64(2), -1}, {float64(2), float64(1), 1}, {float64(2), float64(2), 0},

		{"a", "b", -1}, {"b", "a", 1}, {"a", "a", 0},
		{true, false, 0},
	}

	for _, tc := range cases {
		if got := i.compare(tc.a, tc.b); got != tc.w {
			t.Fatalf("unexpected compare(%#v,%#v): got=%d want=%d", tc.a, tc.b, got, tc.w)
		}
	}
}
