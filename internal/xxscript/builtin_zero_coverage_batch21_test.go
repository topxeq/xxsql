package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch21_BenchmarkAndMock(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	if m := i.builtinBenchmark([]Value{}).(map[string]Value); m["error"] != "need function and iterations" {
		t.Fatalf("expected benchmark arg error, got %v", m)
	}
	if m := i.builtinBenchmark([]Value{123, 1}).(map[string]Value); m["error"] != "function name must be string" {
		t.Fatalf("expected benchmark function-name type error, got %v", m)
	}
	if m := i.builtinBenchmark([]Value{"missingFn", 1}).(map[string]Value); m["error"] != "function not found" {
		t.Fatalf("expected benchmark missing function error, got %v", m)
	}

	ctx.Functions["benchFn"] = &UserFunc{
		Body: &BlockStmt{Statements: []Statement{
			&ReturnStmt{Value: &NumberExpr{Value: 1}},
		}},
	}
	b := i.builtinBenchmark([]Value{"benchFn", 3}).(map[string]Value)
	if b["iterations"] != 3 || b["totalMs"] == nil || b["avgMs"] == nil || b["opsPerSecond"] == nil {
		t.Fatalf("expected benchmark success payload, got %v", b)
	}

	empty := i.builtinMock([]Value{}).(map[string]Value)
	if len(empty) != 0 {
		t.Fatalf("expected mock() to return empty map, got %v", empty)
	}
	nonString := i.builtinMock([]Value{123}).(map[string]Value)
	if len(nonString) != 0 {
		t.Fatalf("expected mock(non-string) empty map, got %v", nonString)
	}

	user := i.builtinMock([]Value{"user"}).(map[string]Value)
	if user["id"] == nil || user["name"] == nil || user["email"] == nil || user["age"] == nil {
		t.Fatalf("expected mock user payload, got %v", user)
	}
	product := i.builtinMock([]Value{"product"}).(map[string]Value)
	if product["id"] == nil || product["name"] == nil || product["price"] == nil {
		t.Fatalf("expected mock product payload, got %v", product)
	}
	order := i.builtinMock([]Value{"order"}).(map[string]Value)
	if order["id"] == nil || order["userId"] == nil || order["status"] == nil || order["total"] == nil || order["quantity"] == nil {
		t.Fatalf("expected mock order payload, got %v", order)
	}
	other := i.builtinMock([]Value{"custom"}).(map[string]Value)
	if other["type"] != "custom" {
		t.Fatalf("expected default mock type echo, got %v", other)
	}
}
