package xxscript

import (
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch35_DeleteJSONByPath(t *testing.T) {
	obj := map[string]interface{}{
		"a":   map[string]interface{}{"b": float64(1), "c": float64(2)},
		"arr": []interface{}{float64(10), float64(20), float64(30)},
	}

	if got := deleteJSONByPath(obj, ""); got == nil {
		t.Fatalf("expected original object for empty path")
	}

	deleted := deleteJSONByPath(obj, "a.b")
	m, ok := deleted.(map[string]Value)
	if !ok {
		t.Fatalf("expected map result after delete, got %T", deleted)
	}
	a, ok := m["a"].(map[string]Value)
	if !ok {
		t.Fatalf("expected nested map at a, got %T", m["a"])
	}
	if _, exists := a["b"]; exists {
		t.Fatalf("expected key a.b to be deleted, got %v", a)
	}

	missingParent := deleteJSONByPath(map[string]interface{}{"x": 1}, "nope.key")
	if _, ok := missingParent.(map[string]Value); !ok {
		t.Fatalf("expected converted map for missing parent path, got %T", missingParent)
	}

	nonContainer := deleteJSONByPath(map[string]interface{}{"x": "text"}, "x.y")
	if _, ok := nonContainer.(map[string]Value); !ok {
		t.Fatalf("expected converted map for non-container intermediate, got %T", nonContainer)
	}

	arrayDelete := deleteJSONByPath(obj, "arr[1]")
	if _, ok := arrayDelete.(map[string]Value); !ok {
		t.Fatalf("expected converted map for array delete path, got %T", arrayDelete)
	}
}

func TestBuiltin_ZeroCoverage_Batch35_AgeAndIToInt(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinAge([]Value{}); got != int64(0) {
		t.Fatalf("expected age default 0, got %v", got)
	}
	if got := i.builtinAge([]Value{"not-a-date"}); got != int64(0) {
		t.Fatalf("expected age parse failure 0, got %v", got)
	}

	birthdate := time.Now().AddDate(-20, -1, 0)
	now := time.Now()
	expected := now.Year() - birthdate.Year()
	if now.Month() < birthdate.Month() || (now.Month() == birthdate.Month() && now.Day() < birthdate.Day()) {
		expected--
	}

	if got := i.builtinAge([]Value{birthdate}); got != int64(expected) {
		t.Fatalf("expected age %d, got %v", expected, got)
	}

	if got := iToInt(7); got != 7 {
		t.Fatalf("expected iToInt(int)=7, got %v", got)
	}
	if got := iToInt(int64(8)); got != 8 {
		t.Fatalf("expected iToInt(int64)=8, got %v", got)
	}
	if got := iToInt(9.7); got != 9 {
		t.Fatalf("expected iToInt(float64)=9, got %v", got)
	}
	if got := iToInt("x"); got != 0 {
		t.Fatalf("expected iToInt(default)=0, got %v", got)
	}
}
