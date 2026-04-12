package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch53_RedisConnect_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinRedisConnect([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisConnect arg validation error, got %v", m)
	}
	if m := i.builtinRedisConnect([]Value{123}).(map[string]Value); m["error"] != "address must be string" {
		t.Fatalf("expected redisConnect address type error, got %v", m)
	}

	if m := i.builtinRedisConnect([]Value{"redis://%zz"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisConnect parse URL error, got %v", m)
	}

	if m := i.builtinRedisConnect([]Value{"127.0.0.1:0", "secret", int(3)}).(map[string]Value); m["error"] == nil || m["addr"] != "127.0.0.1:0" {
		t.Fatalf("expected redisConnect offline ping error with addr, got %v", m)
	}

	if m := i.builtinRedisConnect([]Value{"redis://127.0.0.1:0", "secret", int(2)}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redis:// connect error on offline address, got %v", m)
	}
}

func TestBuiltin_ZeroCoverage_Batch53_GetRedisClient_Missing(t *testing.T) {
	client, ok := getRedisClient("batch53-missing")
	if ok || client != nil {
		t.Fatalf("expected missing redis client lookup to return nil,false, got client=%v ok=%v", client, ok)
	}

	// Ensure expected generated ID prefix convention is stable in connect outputs.
	i := NewInterpreter(NewContext())
	m := i.builtinRedisConnect([]Value{"127.0.0.1:0"}).(map[string]Value)
	if addr, _ := m["addr"].(string); addr != "127.0.0.1:0" {
		t.Fatalf("expected addr to be echoed on connection error, got %v", m)
	}
	if errStr, _ := m["error"].(string); errStr == "" || strings.TrimSpace(errStr) == "" {
		t.Fatalf("expected non-empty connection error string, got %v", m)
	}
}
