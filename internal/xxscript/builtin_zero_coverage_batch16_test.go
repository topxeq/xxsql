package xxscript

import (
	"strings"
	"testing"
)

func TestInterpreter_ZeroCoverage_Batch16_CallUserFuncAndEvalAccess(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	ctx.Variables["outer"] = 99

	fn := &UserFunc{
		Params:         []string{"a", "b", "rest", "afterRest", "dflt"},
		DefaultValues:  []Expression{nil, nil, nil, nil, &StringExpr{Value: "fallback"}},
		RestParamIndex: 2,
		Body: &BlockStmt{Statements: []Statement{
			&VarStmt{Name: "outer", Value: &StringExpr{Value: "shadowed"}},
			&ReturnStmt{Value: &MapExpr{Pairs: map[string]Expression{
				"a":         &IdentExpr{Name: "a"},
				"b":         &IdentExpr{Name: "b"},
				"rest":      &IdentExpr{Name: "rest"},
				"afterRest": &IdentExpr{Name: "afterRest"},
				"dflt":      &IdentExpr{Name: "dflt"},
			}}},
		}},
	}

	result, err := i.callUserFunc(fn, []Value{1, "two", 3, 4})
	if err != nil {
		t.Fatalf("expected callUserFunc success, got err: %v", err)
	}
	resMap, ok := result.(map[string]Value)
	if !ok {
		t.Fatalf("expected map result from callUserFunc, got %T", result)
	}
	if resMap["a"] != 1 || resMap["b"] != "two" {
		t.Fatalf("expected bound params a/b, got %v", resMap)
	}
	rest, ok := resMap["rest"].([]Value)
	if !ok || len(rest) != 2 || rest[0] != 3 || rest[1] != 4 {
		t.Fatalf("expected rest args [3 4], got %v", resMap["rest"])
	}
	if resMap["afterRest"] != nil || resMap["dflt"] != nil {
		t.Fatalf("expected nil for params after rest, got %v", resMap)
	}
	if ctx.Variables["outer"] != 99 {
		t.Fatalf("expected context variables restored after call, got outer=%v", ctx.Variables["outer"])
	}
	if _, exists := ctx.Variables["a"]; exists {
		t.Fatalf("expected function-local variables to be restored, got %+v", ctx.Variables)
	}

	defaultFn := &UserFunc{
		Params:         []string{"x", "y"},
		DefaultValues:  []Expression{nil, &StringExpr{Value: "fallback"}},
		RestParamIndex: -1,
		Body: &BlockStmt{Statements: []Statement{
			&ReturnStmt{Value: &IdentExpr{Name: "y"}},
		}},
	}
	dv, err := i.callUserFunc(defaultFn, []Value{10})
	if err != nil || dv != "fallback" {
		t.Fatalf("expected default value fallback, got val=%v err=%v", dv, err)
	}

	errFn := &UserFunc{
		Params:         []string{"x"},
		DefaultValues:  []Expression{&IdentExpr{Name: "missingVar"}},
		RestParamIndex: -1,
		Body:           &BlockStmt{},
	}
	if _, err := i.callUserFunc(errFn, []Value{}); err == nil {
		t.Fatalf("expected callUserFunc default-eval error path")
	}

	ctx.Variables["obj"] = map[string]Value{"k": "v"}
	v, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &StringExpr{Value: "k"}})
	if err != nil || v != "v" {
		t.Fatalf("expected map member lookup success, got val=%v err=%v", v, err)
	}
	v, err = i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &StringExpr{Value: "missing"}})
	if err != nil || v != nil {
		t.Fatalf("expected missing member to return nil,nil, got val=%v err=%v", v, err)
	}
	if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &NumberExpr{Value: 1}}); err == nil || !strings.Contains(err.Error(), "member key must be string") {
		t.Fatalf("expected non-string member key error, got %v", err)
	}
	ctx.Variables["num"] = 1
	if _, err := i.evalMember(&MemberExpr{Object: &IdentExpr{Name: "num"}, Member: &StringExpr{Value: "x"}}); err == nil || !strings.Contains(err.Error(), "cannot access member") {
		t.Fatalf("expected cannot-access-member error, got %v", err)
	}

	ctx.Variables["arr"] = []Value{"x", "y"}
	ctx.Variables["idxInt"] = 1
	v, err = i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &IdentExpr{Name: "idxInt"}})
	if err != nil || v != "y" {
		t.Fatalf("expected int index success, got val=%v err=%v", v, err)
	}
	v, err = i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 0}})
	if err != nil || v != "x" {
		t.Fatalf("expected float index cast to int success, got val=%v err=%v", v, err)
	}
	v, err = i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 99}})
	if err != nil || v != nil {
		t.Fatalf("expected out-of-range index to return nil,nil, got val=%v err=%v", v, err)
	}
	if _, err := i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &StringExpr{Value: "bad"}}); err == nil || !strings.Contains(err.Error(), "array index must be integer") {
		t.Fatalf("expected array-index type error, got %v", err)
	}

	ctx.Variables["mp"] = map[string]Value{"a": 7}
	v, err = i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "mp"}, Index: &StringExpr{Value: "a"}})
	if err != nil || v != 7 {
		t.Fatalf("expected map index success, got val=%v err=%v", v, err)
	}
	v, err = i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "mp"}, Index: &StringExpr{Value: "missing"}})
	if err != nil || v != nil {
		t.Fatalf("expected missing map key to return nil,nil, got val=%v err=%v", v, err)
	}
	if _, err := i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "mp"}, Index: &NumberExpr{Value: 1}}); err == nil || !strings.Contains(err.Error(), "map key must be string") {
		t.Fatalf("expected map-key type error, got %v", err)
	}

	ctx.Variables["scalar"] = true
	if _, err := i.evalIndex(&IndexExpr{Object: &IdentExpr{Name: "scalar"}, Index: &NumberExpr{Value: 0}}); err == nil || !strings.Contains(err.Error(), "cannot index") {
		t.Fatalf("expected cannot-index scalar error, got %v", err)
	}
}

func TestBuiltin_ZeroCoverage_Batch16_ConcurrencyHelpers(t *testing.T) {
	ctx := NewContext()
	i := NewInterpreter(ctx)

	ctx.Functions["okFn"] = &UserFunc{
		Body: &BlockStmt{Statements: []Statement{
			&ReturnStmt{Value: &StringExpr{Value: "ok"}},
		}},
	}
	ctx.Functions["failFn"] = &UserFunc{
		Body: &BlockStmt{Statements: []Statement{
			&ThrowStmt{Error: &StringExpr{Value: "boom"}},
		}},
	}
	ctx.Functions["slowFn"] = &UserFunc{
		Body: &BlockStmt{Statements: []Statement{
			&ExprStmt{Expr: &CallExpr{Func: &IdentExpr{Name: "sleep"}, Args: []Expression{&NumberExpr{Value: 50}}}},
			&ReturnStmt{Value: &StringExpr{Value: "late"}},
		}},
	}

	if m := i.builtinRetry([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected retry arg error, got %v", m)
	}
	if m := i.builtinRetry([]Value{123, 1, 0}).(map[string]Value); m["error"] != "first arg must be function name" {
		t.Fatalf("expected retry func-name type error, got %v", m)
	}
	if m := i.builtinRetry([]Value{"missingFn", 1, 0}).(map[string]Value); m["error"] != "function not found" {
		t.Fatalf("expected retry missing function error, got %v", m)
	}

	r1 := i.builtinRetry([]Value{"okFn", 3, 0}).(map[string]Value)
	if r1["success"] != true || r1["result"] != "ok" || r1["attempts"] != 1 {
		t.Fatalf("expected retry success on first attempt, got %v", r1)
	}
	r2 := i.builtinRetry([]Value{"failFn", 2, 0}).(map[string]Value)
	if r2["success"] != false || r2["attempts"] != 2 || r2["error"] != "boom" {
		t.Fatalf("expected retry failure after max attempts, got %v", r2)
	}

	p0 := i.builtinParallel([]Value{}).([]Value)
	if len(p0) != 0 {
		t.Fatalf("expected parallel empty result, got %v", p0)
	}
	p1 := i.builtinParallel([]Value{"okFn", "missingFn", 123, "failFn"}).([]Value)
	if len(p1) != 4 || p1[0] != "ok" || p1[1] != nil || p1[2] != nil || p1[3] != nil {
		t.Fatalf("expected parallel mixed results with nil failures, got %v", p1)
	}

	if m := i.builtinTimeout([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected timeout arg error, got %v", m)
	}
	if m := i.builtinTimeout([]Value{123, 1}).(map[string]Value); m["error"] != "first arg must be function name" {
		t.Fatalf("expected timeout func-name type error, got %v", m)
	}
	if m := i.builtinTimeout([]Value{"missingFn", 1}).(map[string]Value); m["error"] != "function not found" {
		t.Fatalf("expected timeout missing function error, got %v", m)
	}

	if v := i.builtinTimeout([]Value{"okFn", 100}); v != "ok" {
		t.Fatalf("expected timeout helper to return function result, got %v", v)
	}
	tm := i.builtinTimeout([]Value{"slowFn", 1}).(map[string]Value)
	if tm["timedOut"] != true || tm["error"] != "timeout" {
		t.Fatalf("expected timeout result map, got %v", tm)
	}
}
