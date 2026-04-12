package xxscript

import (
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func TestBuiltin_ZeroCoverage_Batch37_JSONPathAndDateParse(t *testing.T) {
	i := NewInterpreter(NewContext())

	obj := map[string]interface{}{
		"data": map[string]interface{}{
			"user": map[string]interface{}{"name": "alice"},
		},
		"items": []interface{}{
			map[string]interface{}{"id": float64(7)},
		},
	}

	if got := getJSONByPath(obj, "data.user.name"); got != "alice" {
		t.Fatalf("expected nested map json path value, got %v", got)
	}
	if got := getJSONByPath(obj, "items[0].id"); got != float64(7) {
		t.Fatalf("expected array json path value, got %v", got)
	}
	if got := getJSONByPath(obj, "items[9].id"); got != nil {
		t.Fatalf("expected nil for out-of-range json path, got %v", got)
	}
	if got := getJSONByPath(map[string]Value{"x": int64(9)}, "x"); got != int64(9) {
		t.Fatalf("expected map[string]Value direct value, got %v", got)
	}
	if got := getJSONByPath([]Value{"a", "b"}, "[1]"); got != "b" {
		t.Fatalf("expected []Value index value, got %v", got)
	}
	if got := getJSONByPath("scalar", "x"); got != nil {
		t.Fatalf("expected nil for non-container json path traversal, got %v", got)
	}

	if got := i.builtinDateParse([]Value{}); got != int64(0) {
		t.Fatalf("expected dateParse default 0, got %v", got)
	}
	if got := i.builtinDateParse([]Value{123, "Date"}); got != int64(0) {
		t.Fatalf("expected dateParse type error 0, got %v", got)
	}
	if m := i.builtinDateParse([]Value{"not-a-date", "Date"}).(map[string]Value); m["success"] != false {
		t.Fatalf("expected dateParse parse error map, got %v", m)
	}

	okMap := i.builtinDateParse([]Value{"2024-05-06", "Date"}).(map[string]Value)
	if okMap["success"] != true {
		t.Fatalf("expected dateParse success for Date format, got %v", okMap)
	}
	if _, ok := okMap["unix"].(int64); !ok {
		t.Fatalf("expected int64 unix field, got %T", okMap["unix"])
	}

	rfcMap := i.builtinDateParse([]Value{"2024-05-06T01:02:03Z", "RFC3339"}).(map[string]Value)
	if rfcMap["success"] != true {
		t.Fatalf("expected dateParse success for RFC3339, got %v", rfcMap)
	}
}

func TestBuiltin_ZeroCoverage_Batch37_RandomAvatarAndRedisGetPopErrors(t *testing.T) {
	i := NewInterpreter(NewContext())

	robo := i.builtinRandomAvatar([]Value{"robohash", 64}).(string)
	if !strings.Contains(robo, "robohash.org") || !strings.Contains(robo, "size=64x64") {
		t.Fatalf("unexpected robohash avatar url: %s", robo)
	}

	dice := i.builtinRandomAvatar([]Value{"dicebear", 80}).(string)
	if !strings.Contains(dice, "api.dicebear.com") {
		t.Fatalf("unexpected dicebear avatar url: %s", dice)
	}

	ui := i.builtinRandomAvatar([]Value{"uiavatars", 72, "Alice Bob"}).(string)
	if !strings.Contains(ui, "ui-avatars.com") || !strings.Contains(ui, "Alice+Bob") {
		t.Fatalf("unexpected uiavatars url: %s", ui)
	}

	clientID := "batch37-offline"
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

	if m := i.builtinRedisGet([]Value{clientID, "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisGet connection error, got %v", m)
	}
	if m := i.builtinRedisLPop([]Value{clientID, "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisLPop connection error, got %v", m)
	}
	if m := i.builtinRedisRPop([]Value{clientID, "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisRPop connection error, got %v", m)
	}
}
