package xxscript

import (
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func TestBuiltin_ZeroCoverage_Batch36_RedisDelExistsErrorBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	clientID := "batch36-offline"
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

	delRes := i.builtinRedisDel([]Value{clientID, "k1", 123, "k2"}).(map[string]Value)
	if delRes["error"] == nil {
		t.Fatalf("expected redisDel connection error, got %v", delRes)
	}

	existsRes := i.builtinRedisExists([]Value{clientID, "k1", 123, "k2"}).(map[string]Value)
	if existsRes["error"] == nil {
		t.Fatalf("expected redisExists connection error, got %v", existsRes)
	}
}
