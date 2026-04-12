package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch84_HelperEqualMatrix(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		a, b Value
		w    bool
	}{
		{nil, nil, true},
		{nil, 0, false},
		{int(1), int(1), true},
		{int(1), int64(1), true},
		{int(1), float64(1), true},
		{int(1), float64(2), false},
		{int64(2), int(2), true},
		{int64(2), int64(2), true},
		{int64(2), float64(2), true},
		{int64(2), float64(3), false},
		{float64(3), int(3), true},
		{float64(3), int64(3), true},
		{float64(3), float64(3), true},
		{float64(3), float64(4), false},
		{true, true, true},
		{true, false, false},
		{[]Value{1, "x"}, []Value{1, "x"}, true},
		{[]Value{1}, []Value{2}, false},
		{map[string]Value{"a": 1}, map[string]Value{"a": 1}, true},
		{map[string]Value{"a": 1}, map[string]Value{"a": 2}, false},
	}

	for _, tc := range cases {
		if got := i.equal(tc.a, tc.b); got != tc.w {
			t.Fatalf("equal(%#v,%#v): got=%v want=%v", tc.a, tc.b, got, tc.w)
		}
	}
}

func TestBuiltin_ZeroCoverage_Batch84_IsEqualEdges(t *testing.T) {
	i := NewInterpreter(NewContext())

	if !i.isEqual(true, true) || i.isEqual(true, false) {
		t.Fatalf("isEqual bool branches failed")
	}
	if !i.isEqual(int(1), int64(1)) || !i.isEqual(int(1), float64(1)) || i.isEqual(int(1), "x") {
		t.Fatalf("isEqual int branches failed")
	}
	if !i.isEqual(int64(2), int(2)) || !i.isEqual(int64(2), float64(2)) || i.isEqual(int64(2), "x") {
		t.Fatalf("isEqual int64 branches failed")
	}
	if !i.isEqual(float64(3), int(3)) || !i.isEqual(float64(3), int64(3)) || i.isEqual(float64(3), "x") {
		t.Fatalf("isEqual float branches failed")
	}
	if !i.isEqual("x", "x") || i.isEqual("x", 1) {
		t.Fatalf("isEqual string branches failed")
	}

	if i.isEqual([]Value{1}, 1) {
		t.Fatalf("isEqual slice non-slice should not match")
	}
	if i.isEqual(map[string]Value{"a": 1}, 1) {
		t.Fatalf("isEqual map non-map should not match")
	}
}
