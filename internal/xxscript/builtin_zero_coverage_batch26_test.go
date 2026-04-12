package xxscript

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch26_SecurityHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinJWTVerify([]Value{}).(map[string]Value); m["valid"] != false || m["error"] != "need token and secret" {
		t.Fatalf("expected jwtVerify arg error, got %v", m)
	}
	if m := i.builtinJWTVerify([]Value{123, "s"}).(map[string]Value); m["error"] != "token must be string" {
		t.Fatalf("expected jwtVerify token type error, got %v", m)
	}
	if m := i.builtinJWTVerify([]Value{"a.b.c", 123}).(map[string]Value); m["error"] != "secret must be string" {
		t.Fatalf("expected jwtVerify secret type error, got %v", m)
	}
	if m := i.builtinJWTVerify([]Value{"not-a-jwt", "s"}).(map[string]Value); m["error"] != "invalid token format" {
		t.Fatalf("expected jwtVerify format error, got %v", m)
	}

	payload := map[string]Value{"sub": "u1", "admin": true}
	token, ok := i.builtinJWTSign([]Value{payload, "secret1"}).(string)
	if !ok || token == "" {
		t.Fatalf("expected jwtSign string token, got %T %v", i.builtinJWTSign([]Value{payload, "secret1"}), token)
	}
	if m := i.builtinJWTVerify([]Value{token, "secret2"}).(map[string]Value); m["error"] != "invalid signature" {
		t.Fatalf("expected jwtVerify invalid signature, got %v", m)
	}

	parts := strings.Split(token, ".")
	badPayload := base64.RawURLEncoding.EncodeToString([]byte("{"))
	signingInput := parts[0] + "." + badPayload
	h := hmac.New(sha256.New, []byte("secret1"))
	h.Write([]byte(signingInput))
	badToken := signingInput + "." + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
	if m := i.builtinJWTVerify([]Value{badToken, "secret1"}).(map[string]Value); m["error"] != "invalid payload JSON" {
		t.Fatalf("expected jwtVerify payload JSON error, got %v", m)
	}

	verified := i.builtinJWTVerify([]Value{token, "secret1"}).(map[string]Value)
	if verified["valid"] != true || verified["payload"] == nil {
		t.Fatalf("expected jwtVerify success payload, got %v", verified)
	}

	if m := i.builtinDecryptAES([]Value{}).(map[string]Value); m["error"] != "need ciphertext and key" {
		t.Fatalf("expected decryptAES arg error, got %v", m)
	}
	if m := i.builtinDecryptAES([]Value{123, "k"}).(map[string]Value); m["error"] != "ciphertext must be string" {
		t.Fatalf("expected decryptAES ciphertext type error, got %v", m)
	}
	if m := i.builtinDecryptAES([]Value{"abcd", 123}).(map[string]Value); m["error"] != "key must be string" {
		t.Fatalf("expected decryptAES key type error, got %v", m)
	}
	if m := i.builtinDecryptAES([]Value{"not-base64", "k"}).(map[string]Value); m["error"] != "invalid base64" {
		t.Fatalf("expected decryptAES invalid base64 error, got %v", m)
	}
	if m := i.builtinDecryptAES([]Value{base64.StdEncoding.EncodeToString([]byte{1, 2}), "k"}).(map[string]Value); m["error"] != "ciphertext too short" {
		t.Fatalf("expected decryptAES short ciphertext error, got %v", m)
	}

	encrypted := i.builtinEncryptAES([]Value{"hello", "key1"})
	encStr, ok := encrypted.(string)
	if !ok || encStr == "" {
		t.Fatalf("expected encryptAES string ciphertext, got %T %v", encrypted, encrypted)
	}
	if m := i.builtinDecryptAES([]Value{encStr, "key2"}).(map[string]Value); m["error"] != "decryption failed" {
		t.Fatalf("expected decryptAES wrong-key error, got %v", m)
	}
	if plain := i.builtinDecryptAES([]Value{encStr, "key1"}); plain != "hello" {
		t.Fatalf("expected decryptAES success, got %v", plain)
	}

	if m := i.builtinHashPassword([]Value{}).(map[string]Value); m["error"] != "need password" {
		t.Fatalf("expected hashPassword arg error, got %v", m)
	}
	if m := i.builtinHashPassword([]Value{123}).(map[string]Value); m["error"] != "password must be string" {
		t.Fatalf("expected hashPassword type error, got %v", m)
	}
	hash, ok := i.builtinHashPassword([]Value{"pw", 4}).(string)
	if !ok || hash == "" {
		t.Fatalf("expected hashPassword output string, got %T %v", i.builtinHashPassword([]Value{"pw", 4}), hash)
	}
	if ok := i.builtinVerifyPassword([]Value{"pw", hash}).(bool); !ok {
		t.Fatalf("expected verifyPassword true for matching hash")
	}
	if ok := i.builtinVerifyPassword([]Value{"wrong", hash}).(bool); ok {
		t.Fatalf("expected verifyPassword false for non-matching hash")
	}

	if secret, ok := i.builtinGenerateSecret([]Value{8}).(string); !ok || len(secret) != 16 {
		t.Fatalf("expected hex secret length 16, got %T %v", i.builtinGenerateSecret([]Value{8}), secret)
	}
	if secret, ok := i.builtinGenerateSecret([]Value{12, "base64"}).(string); !ok || len(secret) == 0 {
		t.Fatalf("expected non-empty base64 secret, got %T %v", i.builtinGenerateSecret([]Value{12, "base64"}), secret)
	}
	if secret, ok := i.builtinGenerateSecret([]Value{12, "base64url"}).(string); !ok || strings.Contains(secret, "+") || strings.Contains(secret, "/") {
		t.Fatalf("expected base64url-safe secret, got %T %v", i.builtinGenerateSecret([]Value{12, "base64url"}), secret)
	}
}
