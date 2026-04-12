package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch96_RandomAndPasswordFunctions(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinRandomBytes(nil).(string); len(got) != 32 {
		t.Fatalf("builtinRandomBytes default hex length mismatch: %d", len(got))
	}
	if got := i.builtinRandomBytes([]Value{0}).(string); len(got) != 32 {
		t.Fatalf("builtinRandomBytes non-positive fallback mismatch: %d", len(got))
	}
	if got := i.builtinRandomBytes([]Value{2048}).(string); len(got) != 2048 {
		t.Fatalf("builtinRandomBytes max clamp mismatch: %d", len(got))
	}

	if got := i.builtinRandomHex(nil).(string); len(got) != 16 {
		t.Fatalf("builtinRandomHex default length mismatch: %d", len(got))
	}
	if got := i.builtinRandomHex([]Value{-1}).(string); len(got) != 16 {
		t.Fatalf("builtinRandomHex non-positive fallback mismatch: %d", len(got))
	}
	if got := i.builtinRandomHex([]Value{2048}).(string); len(got) != 1024 {
		t.Fatalf("builtinRandomHex max clamp mismatch: %d", len(got))
	}

	if got := i.builtinRandomString(nil).(string); len(got) != 16 {
		t.Fatalf("builtinRandomString default length mismatch: %d", len(got))
	}
	if got := i.builtinRandomString([]Value{0}).(string); len(got) != 16 {
		t.Fatalf("builtinRandomString non-positive fallback mismatch: %d", len(got))
	}
	if got := i.builtinRandomString([]Value{2048}).(string); len(got) != 1024 {
		t.Fatalf("builtinRandomString max clamp mismatch: %d", len(got))
	}
	custom := i.builtinRandomString([]Value{12, "ab"}).(string)
	if len(custom) != 12 {
		t.Fatalf("builtinRandomString custom charset length mismatch: %d", len(custom))
	}
	for _, ch := range custom {
		if ch != 'a' && ch != 'b' {
			t.Fatalf("builtinRandomString custom charset unexpected char: %q", ch)
		}
	}

	if got := i.builtinGeneratePassword([]Value{4}).(string); len(got) != 8 {
		t.Fatalf("builtinGeneratePassword min clamp mismatch: %d", len(got))
	}
	if got := i.builtinGeneratePassword([]Value{200}).(string); len(got) != 128 {
		t.Fatalf("builtinGeneratePassword max clamp mismatch: %d", len(got))
	}
	fallback := i.builtinGeneratePassword([]Value{10, map[string]Value{
		"lower":   false,
		"upper":   false,
		"digits":  false,
		"special": false,
	}}).(string)
	if len(fallback) != 10 {
		t.Fatalf("builtinGeneratePassword fallback length mismatch: %d", len(fallback))
	}

	u := i.builtinUUID(nil).(string)
	if len(u) != 36 || strings.Count(u, "-") != 4 {
		t.Fatalf("builtinUUID format mismatch: %q", u)
	}
	u7 := i.builtinUUIDv7(nil).(string)
	if len(u7) != 36 || strings.Count(u7, "-") != 4 {
		t.Fatalf("builtinUUIDv7 format mismatch: %q", u7)
	}
}

func TestBuiltin_ZeroCoverage_Batch96_EncodingAndXorFunctions(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinBase64URLEncode(nil); got != "" {
		t.Fatalf("builtinBase64URLEncode nil expected empty, got %#v", got)
	}
	encoded := i.builtinBase64URLEncode([]Value{"a+b"}).(string)
	if encoded == "" {
		t.Fatalf("builtinBase64URLEncode expected non-empty output")
	}
	if got := i.builtinBase64URLDecode(nil); got != "" {
		t.Fatalf("builtinBase64URLDecode nil expected empty, got %#v", got)
	}
	if got := i.builtinBase64URLDecode([]Value{encoded}); got != "a+b" {
		t.Fatalf("builtinBase64URLDecode mismatch: %#v", got)
	}
	if got := i.builtinBase64URLDecode([]Value{"@@@"}); got != "" {
		t.Fatalf("builtinBase64URLDecode invalid expected empty, got %#v", got)
	}

	if got := i.builtinXorEncrypt([]Value{"only-one"}); got != "" {
		t.Fatalf("builtinXorEncrypt short args expected empty, got %#v", got)
	}
	x := i.builtinXorEncrypt([]Value{"hello", "k"}).(string)
	if x == "" {
		t.Fatalf("builtinXorEncrypt expected non-empty cipher")
	}
	if got := i.builtinXorDecrypt([]Value{x, "k"}); got != "hello" {
		t.Fatalf("builtinXorDecrypt roundtrip mismatch: %#v", got)
	}
	if got := i.builtinXorDecrypt([]Value{123, "k"}); got != "" {
		t.Fatalf("builtinXorDecrypt non-string data expected empty, got %#v", got)
	}
	if got := i.builtinXorDecrypt([]Value{"zzzz", "k"}); got != "" {
		t.Fatalf("builtinXorDecrypt invalid hex expected empty, got %#v", got)
	}
}
