package builtins

import (
	"testing"

	"github.com/topxeq/xxsql/internal/xxscript"
)

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}
	if r.functions == nil {
		t.Fatal("NewRegistry did not initialize functions map")
	}
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()

	dummyFn := func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return int64(42), nil
	}

	r.Register("testFn", dummyFn)

	fn, ok := r.Get("testFn")
	if !ok {
		t.Fatal("Expected to find testFn")
	}
	if fn == nil {
		t.Fatal("Expected non-nil function")
	}

	// Verify the function works
	result, err := fn(nil, nil)
	if err != nil {
		t.Fatalf("Function execution error: %v", err)
	}
	if result != int64(42) {
		t.Errorf("Expected 42, got %v", result)
	}
}

func TestRegistry_GetNotFound(t *testing.T) {
	r := NewRegistry()

	_, ok := r.Get("nonexistent")
	if ok {
		t.Error("Expected false for nonexistent function")
	}
}

func TestRegistry_All(t *testing.T) {
	r := NewRegistry()

	r.Register("fn1", func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return nil, nil
	})
	r.Register("fn2", func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return nil, nil
	})

	all := r.All()
	if len(all) != 2 {
		t.Errorf("Expected 2 functions, got %d", len(all))
	}

	if _, ok := all["fn1"]; !ok {
		t.Error("Expected fn1 in All()")
	}
	if _, ok := all["fn2"]; !ok {
		t.Error("Expected fn2 in All()")
	}
}

func TestRegistry_AllReturnsCopy(t *testing.T) {
	r := NewRegistry()

	r.Register("fn1", func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return nil, nil
	})

	all := r.All()
	delete(all, "fn1")

	// Original registry should still have fn1
	_, ok := r.Get("fn1")
	if !ok {
		t.Error("All() should return a copy, modifications should not affect original")
	}
}

func TestGlobalRegistry(t *testing.T) {
	// Save original state
	originalRegistry := globalRegistry
	defer func() { globalRegistry = originalRegistry }()

	// Reset global registry for test
	globalRegistry = NewRegistry()

	dummyFn := func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return "global", nil
	}

	Register("globalTestFn", dummyFn)

	fn, ok := GetFunc("globalTestFn")
	if !ok {
		t.Fatal("Expected to find globalTestFn")
	}

	result, err := fn(nil, nil)
	if err != nil {
		t.Fatalf("Function execution error: %v", err)
	}
	if result != "global" {
		t.Errorf("Expected 'global', got %v", result)
	}
}

func TestAllFuncs(t *testing.T) {
	// Save original state
	originalRegistry := globalRegistry
	defer func() { globalRegistry = originalRegistry }()

	// Reset global registry for test
	globalRegistry = NewRegistry()

	Register("a", func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return nil, nil
	})
	Register("b", func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error) {
		return nil, nil
	})

	all := AllFuncs()
	if len(all) != 2 {
		t.Errorf("Expected 2 functions, got %d", len(all))
	}
}
