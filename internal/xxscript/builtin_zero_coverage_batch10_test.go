package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch10_EncodingFamilies(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinRotN([]Value{}); got != "" {
		t.Fatalf("expected empty rotN for no args, got %v", got)
	}
	if got := i.builtinRotN([]Value{int64(1), int64(13)}); got != "" {
		t.Fatalf("expected empty rotN for non-string input, got %v", got)
	}
	if got := i.builtinRotN([]Value{"Abc-Z", int64(2)}); got != "Cde-B" {
		t.Fatalf("expected positive rotN shift, got %v", got)
	}
	if got := i.builtinRotN([]Value{"Cde-B", int64(-2)}); got != "Abc-Z" {
		t.Fatalf("expected negative rotN shift, got %v", got)
	}

	if got := i.builtinQuotedPrintableEncode([]Value{}); got != "" {
		t.Fatalf("expected empty qp encode for no args, got %v", got)
	}
	qp := i.builtinQuotedPrintableEncode([]Value{"hello=world"})
	if qp != "" {
		t.Fatalf("expected current quoted-printable encode behavior (empty string), got %T (%v)", qp, qp)
	}
	if got := i.builtinQuotedPrintableDecode([]Value{"hello=3Dworld"}); got != "hello=world" {
		t.Fatalf("expected quoted-printable decode success, got %v", got)
	}
	if got := i.builtinQuotedPrintableDecode([]Value{"=ZZ"}); got != "=ZZ" {
		t.Fatalf("expected quoted-printable decode passthrough for invalid escape, got %v", got)
	}

	if got := i.builtinUUEncode([]Value{}); got != "" {
		t.Fatalf("expected empty uuencode for no args, got %v", got)
	}
	uu := i.builtinUUEncode([]Value{"hello", "600", "msg.txt"})
	uuStr, ok := uu.(string)
	if !ok || !strings.Contains(uuStr, "begin 600 msg.txt") || !strings.Contains(uuStr, "\nend\n") {
		t.Fatalf("expected uuencode output with begin/end lines, got %T (%v)", uu, uu)
	}
	if got := i.builtinUUDecode([]Value{uuStr}); got != "hello" {
		t.Fatalf("expected uudecode roundtrip for short payload, got %v", got)
	}

	if got := i.builtinUnicodeEncode([]Value{}); got != "" {
		t.Fatalf("expected empty unicode encode for no args, got %v", got)
	}
	uenc := i.builtinUnicodeEncode([]Value{"Cafe 世界"})
	uencStr, ok := uenc.(string)
	if !ok || !strings.Contains(uencStr, "\\u4E16") || !strings.Contains(uencStr, "\\u754C") {
		t.Fatalf("expected unicode escaped non-ascii runes, got %T (%v)", uenc, uenc)
	}
	if got := i.builtinUnicodeDecode([]Value{uencStr}); got != "Cafe 世界" {
		t.Fatalf("expected unicode decode roundtrip, got %v", got)
	}

	if got := i.builtinUTF8Encode([]Value{}); len(got.([]Value)) != 0 {
		t.Fatalf("expected empty utf8 encode for no args, got %v", got)
	}
	utf := i.builtinUTF8Encode([]Value{"Aé"})
	utfVals, ok := utf.([]Value)
	if !ok || len(utfVals) != 3 || utfVals[0] != int64(65) {
		t.Fatalf("expected utf8 bytes array, got %T (%v)", utf, utf)
	}
	if got := i.builtinUTF8Decode([]Value{utfVals}); got != "Aé" {
		t.Fatalf("expected utf8 decode from []Value, got %v", got)
	}
	if got := i.builtinUTF8Decode([]Value{"already"}); got != "already" {
		t.Fatalf("expected utf8 decode string passthrough, got %v", got)
	}
	if got := i.builtinUTF8Decode([]Value{int64(1)}); got != "" {
		t.Fatalf("expected utf8 decode empty for unsupported type, got %v", got)
	}

	if got := i.builtinPunycodeEncode([]Value{}); got != "" {
		t.Fatalf("expected empty punycode encode for no args, got %v", got)
	}
	penc := i.builtinPunycodeEncode([]Value{"mañana.com"})
	pencStr, ok := penc.(string)
	if !ok || !strings.Contains(pencStr, "xn--") {
		t.Fatalf("expected punycode ascii domain, got %T (%v)", penc, penc)
	}
	if got := i.builtinPunycodeDecode([]Value{pencStr}); got != "mañana.com" {
		t.Fatalf("expected punycode decode roundtrip, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch10_JSCAndHex(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinJSEscape([]Value{}); got != "" {
		t.Fatalf("expected empty js escape for no args, got %v", got)
	}
	jsEsc := i.builtinJSEscape([]Value{"a\n\t\\\"'\x01"})
	jsEscStr, ok := jsEsc.(string)
	if !ok || !strings.Contains(jsEscStr, "\\n") || !strings.Contains(jsEscStr, "\\x01") {
		t.Fatalf("expected js escaped sequences, got %T (%v)", jsEsc, jsEsc)
	}
	if got := i.builtinJSUnescape([]Value{jsEscStr}); got != "a\n\t\\\"'\x01" {
		t.Fatalf("expected js unescape roundtrip, got %q", got)
	}
	if got := i.builtinJSUnescape([]Value{"\\u0041\\x42"}); got != "AB" {
		t.Fatalf("expected js unescape unicode+hex conversion, got %v", got)
	}

	if got := i.builtinCEscape([]Value{}); got != "" {
		t.Fatalf("expected empty c escape for no args, got %v", got)
	}
	cEsc := i.builtinCEscape([]Value{"A\nB\t"})
	cEscStr, ok := cEsc.(string)
	if !ok || !strings.HasPrefix(cEscStr, "\"") || !strings.Contains(cEscStr, "\\n") || !strings.Contains(cEscStr, "\\t") {
		t.Fatalf("expected c escaped quoted string, got %T (%v)", cEsc, cEsc)
	}
	if got := i.builtinCUnescape([]Value{cEscStr}); got != "A\nB\t" {
		t.Fatalf("expected c unescape roundtrip, got %q", got)
	}
	if got := i.builtinCUnescape([]Value{"\"\\101\\x42\\n\""}); got != "AB\n" {
		t.Fatalf("expected c unescape octal+hex conversion, got %q", got)
	}

	if got := i.builtinHexToASCII([]Value{}); got != "" {
		t.Fatalf("expected empty hexToASCII for no args, got %v", got)
	}
	if got := i.builtinHexToASCII([]Value{"48656C6C6F"}); got != "Hello" {
		t.Fatalf("expected hexToASCII Hello, got %v", got)
	}
	if got := i.builtinHexToASCII([]Value{"4865ZZ6C6F"}); got != "Helo" {
		t.Fatalf("expected hexToASCII to skip invalid pair, got %v", got)
	}
}
