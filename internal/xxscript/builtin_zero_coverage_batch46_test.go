package xxscript

import (
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func TestBuiltin_ZeroCoverage_Batch46_RedisSetListHashKey_GuardBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinRedisSet([]Value{"only-client"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisSet arg validation error, got %v", m)
	}
	if m := i.builtinRedisLPush([]Value{"only-client", "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisLPush arg validation error, got %v", m)
	}
	if m := i.builtinRedisRPush([]Value{"only-client", "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisRPush arg validation error, got %v", m)
	}
	if m := i.builtinRedisHDel([]Value{"only-client", "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisHDel arg validation error, got %v", m)
	}
	if m := i.builtinRedisHGetAll([]Value{"only-client"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisHGetAll arg validation error, got %v", m)
	}
	if m := i.builtinRedisKeys([]Value{"only-client"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisKeys arg validation error, got %v", m)
	}

	missingClientID := "batch46-missing"
	if m := i.builtinRedisSet([]Value{missingClientID, "k", "v"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisSet missing client error, got %v", m)
	}
	if m := i.builtinRedisLPush([]Value{missingClientID, "k", "v"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisLPush missing client error, got %v", m)
	}
	if m := i.builtinRedisRPush([]Value{missingClientID, "k", "v"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisRPush missing client error, got %v", m)
	}
	if m := i.builtinRedisHDel([]Value{missingClientID, "k", "f"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisHDel missing client error, got %v", m)
	}
	if m := i.builtinRedisHGetAll([]Value{missingClientID, "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisHGetAll missing client error, got %v", m)
	}
	if m := i.builtinRedisKeys([]Value{missingClientID, "*"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisKeys missing client error, got %v", m)
	}
}

func TestBuiltin_ZeroCoverage_Batch46_RedisSetListHashKey_OfflineErrorBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	clientID := "batch46-offline"
	client := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:0",
		DialTimeout:  20 * time.Millisecond,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
		MaxRetries:   0,
	})

	redisClientsMutex.Lock()
	redisClients[clientID] = client
	redisClientsMutex.Unlock()

	t.Cleanup(func() {
		redisClientsMutex.Lock()
		delete(redisClients, clientID)
		redisClientsMutex.Unlock()
		_ = client.Close()
	})

	if m := i.builtinRedisSet([]Value{clientID, "k-int", "v", 2}).(map[string]Value); m["error"] == nil || m["key"] != "k-int" {
		t.Fatalf("expected redisSet int expiration connection error, got %v", m)
	}
	if m := i.builtinRedisSet([]Value{clientID, "k-float", "v", float64(2)}).(map[string]Value); m["error"] == nil || m["key"] != "k-float" {
		t.Fatalf("expected redisSet float expiration connection error, got %v", m)
	}
	if m := i.builtinRedisLPush([]Value{clientID, "list", "a", 1, true}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisLPush connection error, got %v", m)
	}
	if m := i.builtinRedisRPush([]Value{clientID, "list", "a", 1, true}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisRPush connection error, got %v", m)
	}
	if m := i.builtinRedisHDel([]Value{clientID, "hash", "f1", 123, "f2"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisHDel connection error, got %v", m)
	}
	if m := i.builtinRedisHGetAll([]Value{clientID, "hash"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisHGetAll connection error, got %v", m)
	}
	if m := i.builtinRedisKeys([]Value{clientID, "user:*"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisKeys connection error, got %v", m)
	}
}
