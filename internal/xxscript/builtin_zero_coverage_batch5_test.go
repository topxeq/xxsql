package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch5_EncodingCryptoJWT(t *testing.T) {
	i := NewInterpreter(nil)

	enc := i.builtinBase64URLEncode([]Value{"hello/world"})
	if enc == "" {
		t.Fatalf("expected non-empty base64url encoding")
	}
	if got := i.builtinBase64URLDecode([]Value{enc}); got != "hello/world" {
		t.Fatalf("expected decoded value, got %v", got)
	}
	if got := i.builtinBase64URLDecode([]Value{"%%%"}); got != "" {
		t.Fatalf("expected invalid decode to return empty string, got %v", got)
	}

	cipher := i.builtinXorEncrypt([]Value{"secret", "k"})
	if cipher == "" {
		t.Fatalf("expected xor cipher text")
	}
	if got := i.builtinXorDecrypt([]Value{cipher, "k"}); got != "secret" {
		t.Fatalf("expected xor decrypt roundtrip, got %v", got)
	}
	if got := i.builtinXorDecrypt([]Value{123, "k"}); got != "" {
		t.Fatalf("expected non-string cipher input to fail, got %v", got)
	}

	token := i.builtinJWTEncode([]Value{map[string]Value{"sub": "alice"}, "s3cr3t"})
	tokenStr, ok := token.(string)
	if !ok || tokenStr == "" {
		t.Fatalf("expected jwt token string, got %T (%v)", token, token)
	}
	if strings.Count(tokenStr, ".") != 2 {
		t.Fatalf("expected three jwt sections, got %q", tokenStr)
	}

	decoded, ok := i.builtinJWTDecode([]Value{tokenStr, "s3cr3t"}).(map[string]Value)
	if !ok {
		t.Fatalf("expected jwt decode map result")
	}
	if decoded["success"] != true || decoded["valid"] != true {
		t.Fatalf("expected successful valid jwt decode, got %v", decoded)
	}

	decodedBad, ok := i.builtinJWTDecode([]Value{tokenStr, "wrong"}).(map[string]Value)
	if !ok {
		t.Fatalf("expected jwt decode map result for invalid secret")
	}
	if decodedBad["valid"] != false {
		t.Fatalf("expected invalid signature, got %v", decodedBad)
	}

	if errMap, ok := i.builtinJWTDecode([]Value{}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected missing token error, got %v", errMap)
	}
	if errMap, ok := i.builtinJWTDecode([]Value{123}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected invalid token type error, got %v", errMap)
	}
}

func TestBuiltin_ZeroCoverage_Batch5_JSON(t *testing.T) {
	i := NewInterpreter(nil)

	if got := i.builtinJSONType([]Value{}); got != "null" {
		t.Fatalf("expected null type, got %v", got)
	}
	if got := i.builtinJSONType([]Value{"[1,2]"}); got != "array" {
		t.Fatalf("expected array type, got %v", got)
	}
	if got := i.builtinJSONType([]Value{"not-json"}); got != "string" {
		t.Fatalf("expected string type fallback, got %v", got)
	}

	if got := i.builtinJSONArrayLength([]Value{"[1,2,3]"}); got != int64(3) {
		t.Fatalf("expected array len 3, got %v", got)
	}
	if got := i.builtinJSONArrayLength([]Value{[]Value{int64(1), int64(2)}}); got != int64(2) {
		t.Fatalf("expected []Value len 2, got %v", got)
	}

	appended := i.builtinJSONArrayAppend([]Value{"[1]", float64(2), "x"})
	arr, ok := appended.([]Value)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected appended array of len 3, got %T (%v)", appended, appended)
	}

	prepended := i.builtinJSONArrayPrepend([]Value{"[2]", float64(0), float64(1)})
	arr, ok = prepended.([]Value)
	if !ok || len(arr) != 3 || arr[0] != float64(0) {
		t.Fatalf("expected prepended values, got %T (%v)", prepended, prepended)
	}

	flattened := i.builtinJSONArrayFlatten([]Value{[]Value{int64(1), []Value{int64(2), int64(3)}}})
	flat, ok := flattened.([]Value)
	if !ok || len(flat) != 3 {
		t.Fatalf("expected flattened array len 3, got %T (%v)", flattened, flattened)
	}

	obj := i.builtinJSONObjectFromArrays([]Value{"[\"a\",\"b\"]", "[1,2]"})
	m, ok := obj.(map[string]Value)
	if !ok || m["a"] != float64(1) || m["b"] != float64(2) {
		t.Fatalf("expected object from arrays, got %T (%v)", obj, obj)
	}

	if errMap, ok := i.builtinJSONArrayAppend([]Value{}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected append arg error, got %v", errMap)
	}
	if errMap, ok := i.builtinJSONArrayPrepend([]Value{int64(1), int64(2)}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected prepend type error, got %v", errMap)
	}
}

func TestBuiltin_ZeroCoverage_Batch5_EnvAndProcess(t *testing.T) {
	i := NewInterpreter(nil)

	if errMap, ok := i.builtinEnvSet([]Value{"ONLY_KEY"}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected envSet arg error, got %v", errMap)
	}
	if errMap, ok := i.builtinEnvSet([]Value{123, "x"}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected envSet type error, got %v", errMap)
	}

	key := "XXSQL_TEST_BATCH5_ENV"
	set := i.builtinEnvSet([]Value{key, "value"})
	if m, ok := set.(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected envSet success, got %v", set)
	}
	if got := i.builtinEnv([]Value{key}); got != "value" {
		t.Fatalf("expected env value, got %v", got)
	}

	unset := i.builtinEnvUnset([]Value{key})
	if m, ok := unset.(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected envUnset success, got %v", unset)
	}
	if got := i.builtinEnv([]Value{key, "fallback"}); got != "fallback" {
		t.Fatalf("expected fallback env value, got %v", got)
	}

	if errMap, ok := i.builtinEnvUnset([]Value{}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected envUnset arg error, got %v", errMap)
	}

	listed, ok := i.builtinEnvList(nil).([]Value)
	if !ok || len(listed) == 0 {
		t.Fatalf("expected non-empty environment list, got %T (%v)", listed, listed)
	}

	if _, ok := i.builtinPPID(nil).(int64); !ok {
		t.Fatalf("expected int64 ppid")
	}
	if _, ok := i.builtinGID(nil).(int64); !ok {
		t.Fatalf("expected int64 gid")
	}
}
