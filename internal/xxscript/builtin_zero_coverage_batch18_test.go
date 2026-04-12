package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch18_RateLimiterAndMetrics(t *testing.T) {
	i := NewInterpreter(NewContext())

	rateLimitersMutex.Lock()
	rateLimiters = make(map[string]*rateLimiter)
	rateLimitersMutex.Unlock()

	if m := i.builtinRateLimiter([]Value{}).(map[string]Value); m["error"] != "need name, maxTokens, refillRate" {
		t.Fatalf("expected rateLimiter arg error, got %v", m)
	}
	if m := i.builtinRateLimiter([]Value{123, 2, 1}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected rateLimiter name type error, got %v", m)
	}

	created := i.builtinRateLimiter([]Value{"rl18", 2, 0}).(map[string]Value)
	if created["created"] != true || created["name"] != "rl18" || created["maxTokens"] != 2.0 {
		t.Fatalf("expected rate limiter created with 2 tokens, got %v", created)
	}

	if m := i.builtinRateLimitCheck([]Value{}).(map[string]Value); m["error"] != "need name" {
		t.Fatalf("expected rateLimitCheck arg error, got %v", m)
	}
	if m := i.builtinRateLimitCheck([]Value{123}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected rateLimitCheck name type error, got %v", m)
	}
	if m := i.builtinRateLimitCheck([]Value{"missing-rl"}).(map[string]Value); m["error"] != "rate limiter not found" {
		t.Fatalf("expected rateLimitCheck missing limiter error, got %v", m)
	}

	c1 := i.builtinRateLimitCheck([]Value{"rl18"}).(map[string]Value)
	if c1["allowed"] != true {
		t.Fatalf("expected first check allowed, got %v", c1)
	}
	c2 := i.builtinRateLimitCheck([]Value{"rl18"}).(map[string]Value)
	if c2["allowed"] != true {
		t.Fatalf("expected second check allowed, got %v", c2)
	}
	c3 := i.builtinRateLimitCheck([]Value{"rl18"}).(map[string]Value)
	if c3["allowed"] != false {
		t.Fatalf("expected third check denied, got %v", c3)
	}

	if m := i.builtinRateLimitReset([]Value{}).(map[string]Value); m["error"] != "need name" {
		t.Fatalf("expected rateLimitReset arg error, got %v", m)
	}
	if m := i.builtinRateLimitReset([]Value{123}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected rateLimitReset name type error, got %v", m)
	}
	if m := i.builtinRateLimitReset([]Value{"missing-rl"}).(map[string]Value); m["error"] != "rate limiter not found" {
		t.Fatalf("expected rateLimitReset missing limiter error, got %v", m)
	}

	r := i.builtinRateLimitReset([]Value{"rl18"}).(map[string]Value)
	if r["reset"] != true {
		t.Fatalf("expected rate limiter reset, got %v", r)
	}
	c4 := i.builtinRateLimitCheck([]Value{"rl18", 2}).(map[string]Value)
	if c4["allowed"] != true {
		t.Fatalf("expected cost=2 allowed immediately after reset, got %v", c4)
	}

	metricsMutex.Lock()
	metricsStore = make(map[string]*metricValue)
	metricsMutex.Unlock()

	if m := i.builtinMetricsCounter([]Value{}).(map[string]Value); m["error"] != "need name and value" {
		t.Fatalf("expected metricsCounter arg error, got %v", m)
	}
	if m := i.builtinMetricsCounter([]Value{123, 1}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected metricsCounter name type error, got %v", m)
	}

	mc1 := i.builtinMetricsCounter([]Value{"hits", 2}).(map[string]Value)
	mc2 := i.builtinMetricsCounter([]Value{"hits", 3}).(map[string]Value)
	if mc1["type"] != "counter" || mc2["value"] != 5.0 {
		t.Fatalf("expected counter accumulation to 5, got mc1=%v mc2=%v", mc1, mc2)
	}

	if m := i.builtinMetricsGauge([]Value{}).(map[string]Value); m["error"] != "need name and value" {
		t.Fatalf("expected metricsGauge arg error, got %v", m)
	}
	if m := i.builtinMetricsGauge([]Value{123, 1}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected metricsGauge name type error, got %v", m)
	}
	mg := i.builtinMetricsGauge([]Value{"temp", 21.5}).(map[string]Value)
	if mg["type"] != "gauge" || mg["value"] != 21.5 {
		t.Fatalf("expected gauge value 21.5, got %v", mg)
	}

	all := i.builtinMetricsGet([]Value{}).(map[string]Value)
	if all["hits"] == nil || all["temp"] == nil {
		t.Fatalf("expected metricsGet(all) to include hits and temp, got %v", all)
	}
	if m := i.builtinMetricsGet([]Value{123}).(map[string]Value); m["error"] != "name must be string" {
		t.Fatalf("expected metricsGet name type error, got %v", m)
	}
	one := i.builtinMetricsGet([]Value{"hits"}).(map[string]Value)
	if one["name"] != "hits" || one["value"] != 5.0 || one["type"] != "counter" {
		t.Fatalf("expected metricsGet(hits)=counter 5, got %v", one)
	}
	if m := i.builtinMetricsGet([]Value{"missing-metric"}).(map[string]Value); m["error"] != "metric not found" {
		t.Fatalf("expected metricsGet missing metric error, got %v", m)
	}
}
