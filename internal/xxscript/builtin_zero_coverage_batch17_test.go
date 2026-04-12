package xxscript

import (
	"reflect"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch17_StateMachine(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinStateMachine([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected stateMachine arg error, got %v", m)
	}
	if m := i.builtinStateMachine([]Value{123}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected stateMachine name type error, got %v", m)
	}

	name := "batch17-sm"
	stateMachinesMutex.Lock()
	delete(stateMachines, name)
	delete(stateMachines, "batch17-sm-2")
	stateMachinesMutex.Unlock()

	created := i.builtinStateMachine([]Value{name, "idle"}).(map[string]Value)
	if created["created"] != true || created["state"] != "idle" {
		t.Fatalf("expected created state machine, got %v", created)
	}

	if m := i.builtinStateAdd([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected stateAdd arg error, got %v", m)
	}
	if m := i.builtinStateAdd([]Value{123, "x"}).(map[string]Value); m["error"] != "machine name must be string" {
		t.Fatalf("expected stateAdd machine type error, got %v", m)
	}
	if m := i.builtinStateAdd([]Value{name, 123}).(map[string]Value); m["error"] != "state name must be string" {
		t.Fatalf("expected stateAdd state type error, got %v", m)
	}
	if m := i.builtinStateAdd([]Value{"missing-sm", "ready"}).(map[string]Value); m["error"] != "state machine not found" {
		t.Fatalf("expected stateAdd missing machine error, got %v", m)
	}

	added := i.builtinStateAdd([]Value{name, "ready"}).(map[string]Value)
	if added["added"] != true || added["state"] != "ready" {
		t.Fatalf("expected state added, got %v", added)
	}

	if m := i.builtinStateTransition([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected stateTransition arg error, got %v", m)
	}
	if m := i.builtinStateTransition([]Value{123, "go", "ready"}).(map[string]Value); m["error"] != "machine name must be string" {
		t.Fatalf("expected stateTransition machine type error, got %v", m)
	}
	if m := i.builtinStateTransition([]Value{name, 123, "ready"}).(map[string]Value); m["error"] != "event must be string" {
		t.Fatalf("expected stateTransition event type error, got %v", m)
	}
	if m := i.builtinStateTransition([]Value{name, "go", 123}).(map[string]Value); m["error"] != "target state must be string" {
		t.Fatalf("expected stateTransition target type error, got %v", m)
	}
	if m := i.builtinStateTransition([]Value{"missing-sm", "go", "ready"}).(map[string]Value); m["error"] != "state machine not found" {
		t.Fatalf("expected stateTransition missing machine error, got %v", m)
	}
	if m := i.builtinStateTransition([]Value{name, "go", "unknown"}).(map[string]Value); m["error"] != "target state not defined" {
		t.Fatalf("expected stateTransition unknown target error, got %v", m)
	}

	tr := i.builtinStateTransition([]Value{name, "go", "ready"}).(map[string]Value)
	if tr["transitioned"] != true || tr["from"] != "idle" || tr["to"] != "ready" {
		t.Fatalf("expected successful transition idle->ready, got %v", tr)
	}

	cur := i.builtinStateCurrent([]Value{name}).(map[string]Value)
	if cur["state"] != "ready" || cur["stateCount"] != 2 {
		t.Fatalf("expected stateCurrent to report ready and 2 states, got %v", cur)
	}
	if m := i.builtinStateCurrent([]Value{}).(map[string]Value); m["error"] != "need machine name" {
		t.Fatalf("expected stateCurrent arg error, got %v", m)
	}
	if m := i.builtinStateCurrent([]Value{123}).(map[string]Value); m["error"] != "machine name must be string" {
		t.Fatalf("expected stateCurrent type error, got %v", m)
	}
	if m := i.builtinStateCurrent([]Value{"missing-sm"}).(map[string]Value); m["error"] != "state machine not found" {
		t.Fatalf("expected stateCurrent missing machine error, got %v", m)
	}

	name2 := "batch17-sm-2"
	i.builtinStateMachine([]Value{name2, "init"})
	stateMachinesMutex.Lock()
	delete(stateMachines[name2].states, "init")
	stateMachinesMutex.Unlock()
	if m := i.builtinStateTransition([]Value{name2, "go", "init"}).(map[string]Value); m["error"] != "current state not defined" {
		t.Fatalf("expected current-state-not-defined branch, got %v", m)
	}
}

func TestBuiltin_ZeroCoverage_Batch17_ExprEvalAndHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinExprEval([]Value{}).(map[string]Value); m["error"] != "need expression" {
		t.Fatalf("expected exprEval arg error, got %v", m)
	}
	if m := i.builtinExprEval([]Value{123}).(map[string]Value); m["error"] != "expression must be string" {
		t.Fatalf("expected exprEval type error, got %v", m)
	}

	ok := i.builtinExprEval([]Value{"(1 + 2) * 3 - 4 % 3"}).(map[string]Value)
	if ok["valid"] != true || ok["result"] != 8.0 {
		t.Fatalf("expected exprEval valid result 8, got %v", ok)
	}

	if m := i.builtinExprEval([]Value{"1 / 0"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected exprEval division by zero error, got %v", m)
	}

	if _, err := evalExpression(""); err == nil {
		t.Fatalf("expected empty expression error")
	}
	if _, err := evalExpression("1 +"); err == nil {
		t.Fatalf("expected unexpected-end error")
	}
	if _, err := evalExpression("(1 + 2"); err == nil {
		t.Fatalf("expected missing closing parenthesis error")
	}

	toks := tokenize("1 + 2*(3-1)")
	want := []string{"1", "+", "2", "*", "(", "3", "-", "1", ")"}
	if !reflect.DeepEqual(toks, want) {
		t.Fatalf("expected tokenization %v, got %v", want, toks)
	}

	pos := 0
	if _, err := parsePrimary([]string{}, &pos); err == nil {
		t.Fatalf("expected parsePrimary unexpected-end error")
	}
	pos = 0
	if _, err := parsePrimary([]string{"abc"}, &pos); err == nil {
		t.Fatalf("expected parsePrimary invalid-number error")
	}
	pos = 0
	v, err := parseMulDiv([]string{"5", "%", "2"}, &pos)
	if err != nil || v != 1 {
		t.Fatalf("expected parseMulDiv modulo result 1, got val=%v err=%v", v, err)
	}
	pos = 0
	v, err = parseAddSub([]string{"10", "-", "3", "+", "1"}, &pos)
	if err != nil || v != 8 {
		t.Fatalf("expected parseAddSub result 8, got val=%v err=%v", v, err)
	}
}
