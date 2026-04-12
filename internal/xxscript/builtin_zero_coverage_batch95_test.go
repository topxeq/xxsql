package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch95_CryptoHashAndEncoding(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinMD5(nil); got != "" {
		t.Fatalf("builtinMD5 nil expected empty, got %#v", got)
	}
	if got := i.builtinSHA1(nil); got != "" {
		t.Fatalf("builtinSHA1 nil expected empty, got %#v", got)
	}
	if got := i.builtinSHA256(nil); got != "" {
		t.Fatalf("builtinSHA256 nil expected empty, got %#v", got)
	}
	if got := i.builtinSHA512(nil); got != "" {
		t.Fatalf("builtinSHA512 nil expected empty, got %#v", got)
	}

	if got := i.builtinMD5([]Value{"a"}); len(got.(string)) != 32 {
		t.Fatalf("builtinMD5 hash length mismatch: %#v", got)
	}
	if got := i.builtinSHA1([]Value{"a"}); len(got.(string)) != 40 {
		t.Fatalf("builtinSHA1 hash length mismatch: %#v", got)
	}
	if got := i.builtinSHA256([]Value{"a"}); len(got.(string)) != 64 {
		t.Fatalf("builtinSHA256 hash length mismatch: %#v", got)
	}
	if got := i.builtinSHA512([]Value{"a"}); len(got.(string)) != 128 {
		t.Fatalf("builtinSHA512 hash length mismatch: %#v", got)
	}

	if got := i.builtinBase64Encode(nil); got != "" {
		t.Fatalf("builtinBase64Encode nil expected empty, got %#v", got)
	}
	if got := i.builtinBase64Encode([]Value{"ab"}); got != "YWI=" {
		t.Fatalf("builtinBase64Encode mismatch: %#v", got)
	}
	if got := i.builtinBase64Decode(nil); got != "" {
		t.Fatalf("builtinBase64Decode nil expected empty, got %#v", got)
	}
	if got := i.builtinBase64Decode([]Value{"YWI="}); got != "ab" {
		t.Fatalf("builtinBase64Decode mismatch: %#v", got)
	}
	if got := i.builtinBase64Decode([]Value{"@@@"}); got != "" {
		t.Fatalf("builtinBase64Decode invalid expected empty, got %#v", got)
	}

	if got := i.builtinHexEncode(nil); got != "" {
		t.Fatalf("builtinHexEncode nil expected empty, got %#v", got)
	}
	if got := i.builtinHexEncode([]Value{"ab"}); got != "6162" {
		t.Fatalf("builtinHexEncode mismatch: %#v", got)
	}
	if got := i.builtinHexDecode(nil); got != "" {
		t.Fatalf("builtinHexDecode nil expected empty, got %#v", got)
	}
	if got := i.builtinHexDecode([]Value{"6162"}); got != "ab" {
		t.Fatalf("builtinHexDecode mismatch: %#v", got)
	}
	if got := i.builtinHexDecode([]Value{"zzz"}); got != "" {
		t.Fatalf("builtinHexDecode invalid expected empty, got %#v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch95_AdditionalHashHmacAndBcrypt(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinSHA224(nil); got != "" {
		t.Fatalf("builtinSHA224 nil expected empty, got %#v", got)
	}
	if got := i.builtinSHA384(nil); got != "" {
		t.Fatalf("builtinSHA384 nil expected empty, got %#v", got)
	}
	if got := i.builtinSHA3_256(nil); got != "" {
		t.Fatalf("builtinSHA3_256 nil expected empty, got %#v", got)
	}
	if got := i.builtinSHA3_512(nil); got != "" {
		t.Fatalf("builtinSHA3_512 nil expected empty, got %#v", got)
	}
	if got := i.builtinBlake2b256(nil); got != "" {
		t.Fatalf("builtinBlake2b256 nil expected empty, got %#v", got)
	}
	if got := i.builtinBlake2b512(nil); got != "" {
		t.Fatalf("builtinBlake2b512 nil expected empty, got %#v", got)
	}

	if got := i.builtinSHA224([]Value{"a"}); len(got.(string)) != 56 {
		t.Fatalf("builtinSHA224 hash length mismatch: %#v", got)
	}
	if got := i.builtinSHA384([]Value{"a"}); len(got.(string)) != 96 {
		t.Fatalf("builtinSHA384 hash length mismatch: %#v", got)
	}
	if got := i.builtinSHA3_256([]Value{"a"}); len(got.(string)) != 64 {
		t.Fatalf("builtinSHA3_256 hash length mismatch: %#v", got)
	}
	if got := i.builtinSHA3_512([]Value{"a"}); len(got.(string)) != 128 {
		t.Fatalf("builtinSHA3_512 hash length mismatch: %#v", got)
	}
	if got := i.builtinBlake2b256([]Value{"a"}); len(got.(string)) != 64 {
		t.Fatalf("builtinBlake2b256 hash length mismatch: %#v", got)
	}
	if got := i.builtinBlake2b512([]Value{"a"}); len(got.(string)) != 128 {
		t.Fatalf("builtinBlake2b512 hash length mismatch: %#v", got)
	}

	if got := i.builtinCRC32(nil); !valuesEqual(got, int64(0)) {
		t.Fatalf("builtinCRC32 nil expected 0, got %#v", got)
	}
	if got := i.builtinAdler32(nil); !valuesEqual(got, int64(0)) {
		t.Fatalf("builtinAdler32 nil expected 0, got %#v", got)
	}
	if got := i.builtinCRC32([]Value{"abc"}); !valuesEqual(got, int64(891568578)) {
		t.Fatalf("builtinCRC32 mismatch: %#v", got)
	}
	if got := i.builtinAdler32([]Value{"abc"}); !valuesEqual(got, int64(38600999)) {
		t.Fatalf("builtinAdler32 mismatch: %#v", got)
	}

	if got := i.builtinHmacSHA1([]Value{"a"}); got != "" {
		t.Fatalf("builtinHmacSHA1 short args expected empty, got %#v", got)
	}
	if got := i.builtinHmacSHA512([]Value{"a"}); got != "" {
		t.Fatalf("builtinHmacSHA512 short args expected empty, got %#v", got)
	}
	if got := i.builtinHmacSHA256([]Value{"a"}); got != "" {
		t.Fatalf("builtinHmacSHA256 short args expected empty, got %#v", got)
	}
	if got := i.builtinHmacSHA1([]Value{"a", "k"}); len(got.(string)) != 40 {
		t.Fatalf("builtinHmacSHA1 length mismatch: %#v", got)
	}
	if got := i.builtinHmacSHA512([]Value{"a", "k"}); len(got.(string)) != 128 {
		t.Fatalf("builtinHmacSHA512 length mismatch: %#v", got)
	}
	if got := i.builtinHmacSHA256([]Value{"a", "k"}); len(got.(string)) != 64 {
		t.Fatalf("builtinHmacSHA256 length mismatch: %#v", got)
	}

	need := i.builtinBcryptVerify([]Value{"pw"}).(map[string]Value)
	if need["success"] != false {
		t.Fatalf("bcrypt verify short-args expected success=false, got %#v", need)
	}

	hashed := i.builtinBcryptHash([]Value{"pw"}).(map[string]Value)
	if hashed["success"] != true {
		t.Fatalf("bcrypt hash expected success=true, got %#v", hashed)
	}
	hashStr, _ := hashed["hash"].(string)
	if hashStr == "" {
		t.Fatalf("bcrypt hash expected non-empty hash")
	}

	ok := i.builtinBcryptVerify([]Value{"pw", hashStr}).(map[string]Value)
	if ok["success"] != true || ok["valid"] != true {
		t.Fatalf("bcrypt verify expected valid=true, got %#v", ok)
	}

	bad := i.builtinBcryptVerify([]Value{"badpw", hashStr}).(map[string]Value)
	if bad["success"] != false || bad["valid"] != false {
		t.Fatalf("bcrypt verify expected invalid branch, got %#v", bad)
	}
}
