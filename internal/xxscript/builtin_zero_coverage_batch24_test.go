package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch24_QueueTopicRuleEngine(t *testing.T) {
	i := NewInterpreter(NewContext())

	queueMutex.Lock()
	queues = make(map[string][]Value)
	topics = make(map[string][]func(Value))
	queueMutex.Unlock()
	rulesMutex.Lock()
	rules = make(map[string]*rule)
	rulesMutex.Unlock()

	if m := i.builtinQueueCreate([]Value{}).(map[string]Value); m["error"] != "need queue name" {
		t.Fatalf("expected queueCreate arg error, got %v", m)
	}
	qc := i.builtinQueueCreate([]Value{"q24"}).(map[string]Value)
	if qc["created"] != true || qc["name"] != "q24" {
		t.Fatalf("expected queue created, got %v", qc)
	}

	if m := i.builtinQueuePush([]Value{"q24"}).(map[string]Value); m["error"] != "need queue name and value" {
		t.Fatalf("expected queuePush arg error, got %v", m)
	}
	p1 := i.builtinQueuePush([]Value{"q24", "a"}).(map[string]Value)
	p2 := i.builtinQueuePush([]Value{"q24", "b"}).(map[string]Value)
	if p1["size"] != 1 || p2["size"] != 2 {
		t.Fatalf("expected queue size growth to 2, got p1=%v p2=%v", p1, p2)
	}

	if m := i.builtinQueuePeek([]Value{}).(map[string]Value); m["error"] != "need queue name" {
		t.Fatalf("expected queuePeek arg error, got %v", m)
	}
	peek := i.builtinQueuePeek([]Value{"q24"}).(map[string]Value)
	if peek["peeked"] != true || peek["value"] != "a" || peek["size"] != 2 {
		t.Fatalf("expected queue peek first element, got %v", peek)
	}
	if m := i.builtinQueuePeek([]Value{"missing-q"}).(map[string]Value); m["error"] != "queue is empty" {
		t.Fatalf("expected queuePeek empty error, got %v", m)
	}

	if m := i.builtinQueueSize([]Value{}).(map[string]Value); m["error"] != "need queue name" {
		t.Fatalf("expected queueSize arg error, got %v", m)
	}
	szMissing := i.builtinQueueSize([]Value{"missing-q"}).(map[string]Value)
	if szMissing["size"] != 0 {
		t.Fatalf("expected missing queue size=0, got %v", szMissing)
	}
	sz := i.builtinQueueSize([]Value{"q24"}).(map[string]Value)
	if sz["size"] != 2 {
		t.Fatalf("expected queue size=2, got %v", sz)
	}

	if m := i.builtinQueuePop([]Value{}).(map[string]Value); m["error"] != "need queue name" {
		t.Fatalf("expected queuePop arg error, got %v", m)
	}
	pop1 := i.builtinQueuePop([]Value{"q24"}).(map[string]Value)
	pop2 := i.builtinQueuePop([]Value{"q24"}).(map[string]Value)
	if pop1["value"] != "a" || pop2["value"] != "b" {
		t.Fatalf("expected FIFO pops a then b, got pop1=%v pop2=%v", pop1, pop2)
	}
	if m := i.builtinQueuePop([]Value{"q24"}).(map[string]Value); m["error"] != "queue is empty" {
		t.Fatalf("expected queuePop empty error, got %v", m)
	}

	if m := i.builtinTopicCreate([]Value{}).(map[string]Value); m["error"] != "need topic name" {
		t.Fatalf("expected topicCreate arg error, got %v", m)
	}
	tc := i.builtinTopicCreate([]Value{"topic24"}).(map[string]Value)
	if tc["created"] != true || tc["name"] != "topic24" {
		t.Fatalf("expected topic created, got %v", tc)
	}
	if m := i.builtinTopicPublish([]Value{"topic24"}).(map[string]Value); m["error"] != "need topic name and message" {
		t.Fatalf("expected topicPublish arg error, got %v", m)
	}
	tp := i.builtinTopicPublish([]Value{"topic24", "msg"}).(map[string]Value)
	if tp["published"] != true || tp["subscriberCount"] != 0 {
		t.Fatalf("expected topic publish with 0 subscribers, got %v", tp)
	}
	if m := i.builtinTopicSubscribe([]Value{"topic24"}).(map[string]Value); m["error"] != "need topic name and handler name" {
		t.Fatalf("expected topicSubscribe arg error, got %v", m)
	}
	ts := i.builtinTopicSubscribe([]Value{"topic24", "h1"}).(map[string]Value)
	if ts["subscribed"] != true || ts["handler"] != "h1" {
		t.Fatalf("expected topic subscribe payload, got %v", ts)
	}

	if m := i.builtinRuleCreate([]Value{}).(map[string]Value); m["error"] != "need rule name" {
		t.Fatalf("expected ruleCreate arg error, got %v", m)
	}
	rc := i.builtinRuleCreate([]Value{"r24", 5}).(map[string]Value)
	if rc["created"] != true || rc["name"] != "r24" {
		t.Fatalf("expected rule created, got %v", rc)
	}

	if m := i.builtinRuleAddCondition([]Value{"r24"}).(map[string]Value); m["error"] != "need rule name, field, operator, value" {
		t.Fatalf("expected ruleAddCondition arg error, got %v", m)
	}
	if m := i.builtinRuleAddCondition([]Value{"missing-r", "age", ">", 18}).(map[string]Value); m["error"] != "rule not found" {
		t.Fatalf("expected ruleAddCondition missing rule error, got %v", m)
	}
	if m := i.builtinRuleAddCondition([]Value{"r24", "age", ">", 18}).(map[string]Value); m["added"] != true {
		t.Fatalf("expected add condition success, got %v", m)
	}
	_ = i.builtinRuleAddCondition([]Value{"r24", "name", "starts_with", "A"})

	if m := i.builtinRuleEvaluate([]Value{"r24"}).(map[string]Value); m["error"] != "need rule name and data" {
		t.Fatalf("expected ruleEvaluate arg error, got %v", m)
	}
	if m := i.builtinRuleEvaluate([]Value{"r24", "bad"}).(map[string]Value); m["error"] != "data must be a map" {
		t.Fatalf("expected ruleEvaluate data type error, got %v", m)
	}
	if m := i.builtinRuleEvaluate([]Value{"missing-r", map[string]Value{}}).(map[string]Value); m["error"] != "rule not found" {
		t.Fatalf("expected ruleEvaluate missing rule error, got %v", m)
	}
	re := i.builtinRuleEvaluate([]Value{"r24", map[string]Value{"age": 22, "name": "Alice"}}).(map[string]Value)
	if re["matches"] != true {
		t.Fatalf("expected ruleEvaluate matches=true, got %v", re)
	}
	re2 := i.builtinRuleEvaluate([]Value{"r24", map[string]Value{"age": 12, "name": "Bob"}}).(map[string]Value)
	if re2["matches"] != false {
		t.Fatalf("expected ruleEvaluate matches=false, got %v", re2)
	}

	if m := i.builtinRuleChain([]Value{"bad"}).(map[string]Value); m["error"] != "need rule names array and data" {
		t.Fatalf("expected ruleChain arg error, got %v", m)
	}
	if m := i.builtinRuleChain([]Value{[]Value{"r24"}, "bad"}).(map[string]Value); m["error"] != "data must be a map" {
		t.Fatalf("expected ruleChain data type error, got %v", m)
	}
	rch := i.builtinRuleChain([]Value{[]Value{"r24", 123, "missing-r"}, map[string]Value{"age": 30, "name": "Ann"}}).(map[string]Value)
	if rch["count"] != 2 {
		t.Fatalf("expected ruleChain to evaluate two string rule names, got %v", rch)
	}

	if m := i.builtinDecisionTable([]Value{}).(map[string]Value); m["error"] != "need table and data" {
		t.Fatalf("expected decisionTable arg error, got %v", m)
	}
	if m := i.builtinDecisionTable([]Value{"bad", map[string]Value{}}).(map[string]Value); m["error"] != "table must be an array" {
		t.Fatalf("expected decisionTable table type error, got %v", m)
	}
	if m := i.builtinDecisionTable([]Value{[]Value{}, "bad"}).(map[string]Value); m["error"] != "data must be a map" {
		t.Fatalf("expected decisionTable data type error, got %v", m)
	}

	table := []Value{
		map[string]Value{"conditions": map[string]Value{"role": "admin"}, "action": "allow"},
		map[string]Value{"conditions": map[string]Value{"role": "user"}, "action": "limited"},
		"invalid-row",
	}
	dt1 := i.builtinDecisionTable([]Value{table, map[string]Value{"role": "admin"}}).(map[string]Value)
	if dt1["count"] != 1 {
		t.Fatalf("expected one matched decision row, got %v", dt1)
	}
	dt2 := i.builtinDecisionTable([]Value{table, map[string]Value{"role": "guest"}}).(map[string]Value)
	if dt2["count"] != 0 {
		t.Fatalf("expected zero matched decision rows, got %v", dt2)
	}
}

func TestBuiltin_ZeroCoverage_Batch24_EvaluateConditionBranches(t *testing.T) {
	if !evaluateCondition(nil, "not_exists", nil, false) {
		t.Fatalf("expected not_exists=true when field missing")
	}
	if evaluateCondition(nil, "exists", nil, false) {
		t.Fatalf("expected exists=false when field missing")
	}
	if !evaluateCondition("x", "eq", "x", true) || !evaluateCondition("x", "==", "x", true) {
		t.Fatalf("expected eq/== true")
	}
	if !evaluateCondition("x", "neq", "y", true) || !evaluateCondition("x", "!=", "y", true) {
		t.Fatalf("expected neq/!= true")
	}
	if !evaluateCondition(5, "gt", 3, true) || !evaluateCondition(5, ">", 3, true) {
		t.Fatalf("expected gt/> true")
	}
	if !evaluateCondition(5, "gte", 5, true) || !evaluateCondition(5, ">=", 5, true) {
		t.Fatalf("expected gte/>= true")
	}
	if !evaluateCondition(3, "lt", 5, true) || !evaluateCondition(3, "<", 5, true) {
		t.Fatalf("expected lt/< true")
	}
	if !evaluateCondition(3, "lte", 3, true) || !evaluateCondition(3, "<=", 3, true) {
		t.Fatalf("expected lte/<= true")
	}
	if !evaluateCondition("hello", "contains", "ell", true) {
		t.Fatalf("expected contains true")
	}
	if !evaluateCondition("hello", "starts_with", "he", true) {
		t.Fatalf("expected starts_with true")
	}
	if !evaluateCondition("hello", "ends_with", "lo", true) {
		t.Fatalf("expected ends_with true")
	}
	if !evaluateCondition("anything", "exists", nil, true) {
		t.Fatalf("expected exists true when field present")
	}
	if evaluateCondition("anything", "not_exists", nil, true) {
		t.Fatalf("expected not_exists false when field present")
	}
	if !evaluateCondition("b", "in", []Value{"a", "b"}, true) || evaluateCondition("c", "in", []Value{"a", "b"}, true) {
		t.Fatalf("expected in operator behavior")
	}
	if evaluateCondition("x", "in", "not-array", true) {
		t.Fatalf("expected in false for non-array condition value")
	}
	if evaluateCondition("x", "unknown-op", "x", true) {
		t.Fatalf("expected unknown operator false")
	}
}
