package xxscript

import (
	"fmt"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch47_PBKDF2_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinPBKDF2([]Value{"only-password"}).(map[string]Value); m["success"] != false || m["error"] == nil {
		t.Fatalf("expected pbkdf2 arg validation failure, got %v", m)
	}

	defaultMap := i.builtinPBKDF2([]Value{"password", "salt", 10}).(map[string]Value)
	if defaultMap["success"] != true {
		t.Fatalf("expected pbkdf2 success with defaults, got %v", defaultMap)
	}
	if defaultMap["iterations"] != int64(1000) {
		t.Fatalf("expected iterations clamped to 1000, got %v", defaultMap["iterations"])
	}
	if defaultMap["keyLen"] != int64(32) {
		t.Fatalf("expected default keyLen 32, got %v", defaultMap["keyLen"])
	}
	if gotKey, ok := defaultMap["key"].(string); !ok || len(gotKey) != 64 {
		t.Fatalf("expected 64-char key hex for keyLen=32, got %T %v", defaultMap["key"], defaultMap["key"])
	}

	sha1Map := i.builtinPBKDF2([]Value{"password", "salt", 1500, 16, "sha1"}).(map[string]Value)
	if sha1Map["success"] != true || sha1Map["keyLen"] != int64(16) {
		t.Fatalf("expected pbkdf2 sha1 success with keyLen=16, got %v", sha1Map)
	}
	if gotKey, ok := sha1Map["key"].(string); !ok || len(gotKey) != 32 {
		t.Fatalf("expected 32-char key hex for keyLen=16, got %T %v", sha1Map["key"], sha1Map["key"])
	}

	sha512Map := i.builtinPBKDF2([]Value{"password", "salt", 2000, 24, "sha512"}).(map[string]Value)
	if sha512Map["success"] != true || sha512Map["iterations"] != int64(2000) || sha512Map["keyLen"] != int64(24) {
		t.Fatalf("expected pbkdf2 sha512 branch values, got %v", sha512Map)
	}
	if gotKey, ok := sha512Map["key"].(string); !ok || len(gotKey) != 48 {
		t.Fatalf("expected 48-char key hex for keyLen=24, got %T %v", sha512Map["key"], sha512Map["key"])
	}

	nonStringHashMap := i.builtinPBKDF2([]Value{"password", "salt", 2000, 8, true}).(map[string]Value)
	if nonStringHashMap["success"] != true || nonStringHashMap["keyLen"] != int64(8) {
		t.Fatalf("expected pbkdf2 success with non-string hash selector, got %v", nonStringHashMap)
	}
}

func TestBuiltin_ZeroCoverage_Batch47_Permutations_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinPermutations([]Value{123}); len(got.([]Value)) != 0 {
		t.Fatalf("expected empty permutations for non-array arg, got %v", got)
	}

	if got := i.builtinPermutations([]Value{[]Value{}}); len(got.([]Value)) != 0 {
		t.Fatalf("expected empty permutations for empty array, got %v", got)
	}

	single := i.builtinPermutations([]Value{[]Value{"x"}}).([]Value)
	if len(single) != 1 {
		t.Fatalf("expected one permutation for single-element array, got %v", single)
	}
	if perm, ok := single[0].([]Value); !ok || len(perm) != 1 || perm[0] != "x" {
		t.Fatalf("expected single permutation [x], got %v", single[0])
	}

	three := i.builtinPermutations([]Value{[]Value{1, 2, 3}}).([]Value)
	if len(three) != 6 {
		t.Fatalf("expected 6 permutations for 3 elements, got %d (%v)", len(three), three)
	}

	seen := make(map[string]struct{}, len(three))
	for _, v := range three {
		perm, ok := v.([]Value)
		if !ok || len(perm) != 3 {
			t.Fatalf("expected each permutation to be []Value length 3, got %T %v", v, v)
		}
		seen[fmt.Sprintf("%v", perm)] = struct{}{}
	}
	if len(seen) != 6 {
		t.Fatalf("expected 6 unique permutations, got %d (%v)", len(seen), seen)
	}
}
