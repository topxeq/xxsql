package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch73_ConversionAndTypeUtils(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Run("builtinString", func(t *testing.T) {
		if got := i.builtinString([]Value{}); got != "" {
			t.Fatalf("expected empty string for no-arg builtinString, got %#v", got)
		}
		if got := i.builtinString([]Value{123}); got != "123" {
			t.Fatalf("expected stringified integer, got %#v", got)
		}
	})

	t.Run("builtinTypeof", func(t *testing.T) {
		if got := i.builtinTypeof([]Value{}); got != "null" {
			t.Fatalf("expected typeof no-arg to be null, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{nil}); got != "null" {
			t.Fatalf("expected typeof nil to be null, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{true}); got != "bool" {
			t.Fatalf("expected typeof bool to be bool, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{int64(1)}); got != "int" {
			t.Fatalf("expected typeof int64 to be int, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{1.5}); got != "float" {
			t.Fatalf("expected typeof float64 to be float, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{"x"}); got != "string" {
			t.Fatalf("expected typeof string to be string, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{[]Value{1}}); got != "array" {
			t.Fatalf("expected typeof []Value to be array, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{map[string]Value{"k": "v"}}); got != "object" {
			t.Fatalf("expected typeof map to be object, got %#v", got)
		}
		if got := i.builtinTypeof([]Value{struct{ A int }{A: 1}}); got != "unknown" {
			t.Fatalf("expected typeof unknown type to be unknown, got %#v", got)
		}
	})

	t.Run("builtinKeysAndValues", func(t *testing.T) {
		if got := i.builtinKeys([]Value{}).([]Value); len(got) != 0 {
			t.Fatalf("expected empty keys for no args, got %#v", got)
		}
		if got := i.builtinKeys([]Value{"x"}).([]Value); len(got) != 0 {
			t.Fatalf("expected empty keys for non-map arg, got %#v", got)
		}
		if got := i.builtinKeys([]Value{map[string]Value{"a": 1, "b": 2}}).([]Value); len(got) != 2 {
			t.Fatalf("expected two keys from map, got %#v", got)
		}

		if got := i.builtinValues([]Value{}).([]Value); len(got) != 0 {
			t.Fatalf("expected empty values for no args, got %#v", got)
		}
		if got := i.builtinValues([]Value{"x"}).([]Value); len(got) != 0 {
			t.Fatalf("expected empty values for non-map arg, got %#v", got)
		}
		if got := i.builtinValues([]Value{map[string]Value{"a": 1, "b": 2}}).([]Value); len(got) != 2 {
			t.Fatalf("expected two values from map, got %#v", got)
		}
	})
}
