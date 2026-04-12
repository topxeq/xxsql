package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch38_BcryptAndArgon2id(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinBcryptHash([]Value{}).(map[string]Value); m["success"] != false {
		t.Fatalf("expected bcryptHash missing-arg failure, got %v", m)
	}

	lowCost := i.builtinBcryptHash([]Value{"secret", -1}).(map[string]Value)
	if lowCost["success"] != true {
		t.Fatalf("expected bcryptHash success with clamped low cost, got %v", lowCost)
	}
	if hash, ok := lowCost["hash"].(string); !ok || !strings.HasPrefix(hash, "$2") {
		t.Fatalf("expected bcrypt hash string, got %v", lowCost)
	}

	if m := i.builtinArgon2id([]Value{}).(map[string]Value); m["success"] != false {
		t.Fatalf("expected argon2id missing-arg failure, got %v", m)
	}

	custom := i.builtinArgon2id([]Value{"password", map[string]Value{
		"time":    2.0,
		"memory":  4096.0,
		"threads": 2.0,
		"keyLen":  16.0,
	}}).(map[string]Value)

	if custom["success"] != true {
		t.Fatalf("expected argon2id success with custom options, got %v", custom)
	}
	hash, ok1 := custom["hash"].(string)
	salt, ok2 := custom["salt"].(string)
	params, ok3 := custom["params"].(map[string]Value)
	if !ok1 || !ok2 || !ok3 || len(hash) != 32 || len(salt) != 32 {
		t.Fatalf("expected argon2id hash/salt/params shapes, got %v", custom)
	}
	if params["time"] != int64(2) || params["memory"] != int64(4096) || params["threads"] != int64(2) || params["keyLen"] != int64(16) {
		t.Fatalf("expected argon2id params to match custom options, got %v", params)
	}
}
