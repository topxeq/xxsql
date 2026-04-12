package xxscript

import (
	"encoding/base32"
	"encoding/base64"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch11_EncodingsAndValidators(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinToBinary([]Value{"Hi"}); got != "01001000 01101001" {
		t.Fatalf("expected toBinary output for Hi, got %v", got)
	}
	if got := i.builtinFromBinary([]Value{"0100100001101001"}); got != "Hi" {
		t.Fatalf("expected fromBinary decode to Hi, got %v", got)
	}
	if got := i.builtinFromBinary([]Value{"01001000 bad"}); got != "H" {
		t.Fatalf("expected fromBinary partial decode before trailing junk, got %v", got)
	}

	if got := i.builtinToOctal([]Value{"Hi"}); got != "110 151" {
		t.Fatalf("expected toOctal output for Hi, got %v", got)
	}
	if got := i.builtinFromOctal([]Value{"110151"}); got != "Hi" {
		t.Fatalf("expected fromOctal decode to Hi, got %v", got)
	}
	if got := i.builtinFromOctal([]Value{"110zzz"}); got != "" {
		t.Fatalf("expected fromOctal parse failure to return empty string, got %v", got)
	}

	morse := i.builtinMorseEncode([]Value{"SOS #"})
	morseStr, ok := morse.(string)
	if !ok || !strings.Contains(morseStr, "... --- ...") || !strings.Contains(morseStr, "#") {
		t.Fatalf("expected morse output with unknown char passthrough, got %T (%v)", morse, morse)
	}
	if got := i.builtinMorseDecode([]Value{"... --- ... / #"}); got != "SOS #" {
		t.Fatalf("expected morse decode to SOS #, got %v", got)
	}

	if got := i.builtinASCIItoHex([]Value{"Go"}); got != "476F" {
		t.Fatalf("expected ASCIItoHex 476F, got %v", got)
	}

	bytesVal := i.builtinStrToBytes([]Value{"Aé"})
	arr, ok := bytesVal.([]Value)
	if !ok || len(arr) != 3 || arr[0] != int64(65) {
		t.Fatalf("expected strToBytes []Value bytes, got %T (%v)", bytesVal, bytesVal)
	}
	if got := i.builtinBytesToStr([]Value{arr}); got != "Aé" {
		t.Fatalf("expected bytesToStr roundtrip, got %v", got)
	}
	if got := i.builtinBytesToStr([]Value{"pass"}); got != "pass" {
		t.Fatalf("expected bytesToStr string passthrough, got %v", got)
	}
	if got := i.builtinBytesToStr([]Value{true}); got != "" {
		t.Fatalf("expected bytesToStr unsupported type empty string, got %v", got)
	}

	gz := i.builtinGzipCompress([]Value{"payload"})
	gzStr, ok := gz.(string)
	if !ok || gzStr == "" {
		t.Fatalf("expected non-empty gzip base64 output, got %T (%v)", gz, gz)
	}
	if got := i.builtinGzipDecompress([]Value{gzStr}); got != "payload" {
		t.Fatalf("expected gzip roundtrip payload, got %v", got)
	}
	if m, ok := i.builtinGzipDecompress([]Value{"%%%"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected gzip invalid base64 error map, got %v", m)
	}

	zl := i.builtinZlibCompress([]Value{"payload"})
	zlStr, ok := zl.(string)
	if !ok || zlStr == "" {
		t.Fatalf("expected non-empty zlib base64 output, got %T (%v)", zl, zl)
	}
	if got := i.builtinZlibDecompress([]Value{zlStr}); got != "payload" {
		t.Fatalf("expected zlib roundtrip payload, got %v", got)
	}
	if m, ok := i.builtinZlibDecompress([]Value{"%%%"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected zlib invalid base64 error map, got %v", m)
	}

	if got := i.builtinIsBase64([]Value{base64.StdEncoding.EncodeToString([]byte("ok"))}); got != true {
		t.Fatalf("expected isBase64 true for valid data, got %v", got)
	}
	if got := i.builtinIsBase64([]Value{"abc!"}); got != false {
		t.Fatalf("expected isBase64 false for invalid chars, got %v", got)
	}

	if got := i.builtinIsHex([]Value{"A0ff"}); got != true {
		t.Fatalf("expected isHex true for valid hex, got %v", got)
	}
	if got := i.builtinIsHex([]Value{"xyz"}); got != false {
		t.Fatalf("expected isHex false for non-hex, got %v", got)
	}

	b32 := base32.StdEncoding.EncodeToString([]byte("ok"))
	if got := i.builtinIsBase32([]Value{b32}); got != true {
		t.Fatalf("expected isBase32 true for valid base32, got %v", got)
	}
	if got := i.builtinIsBase32([]Value{"abc!"}); got != false {
		t.Fatalf("expected isBase32 false for invalid chars, got %v", got)
	}
}
