package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch58_ValidationAndIPBranches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinIsEmail([]Value{}); got != false {
		t.Fatalf("builtinIsEmail no-args: expected false, got %v", got)
	}
	if got := i.builtinIsEmail([]Value{123}); got != false {
		t.Fatalf("builtinIsEmail non-string: expected false, got %v", got)
	}
	if got := i.builtinIsEmail([]Value{"bad@"}); got != false {
		t.Fatalf("builtinIsEmail invalid: expected false, got %v", got)
	}
	if got := i.builtinIsEmail([]Value{"dev@example.com"}); got != true {
		t.Fatalf("builtinIsEmail valid: expected true, got %v", got)
	}

	if got := i.builtinIsURL([]Value{}); got != false {
		t.Fatalf("builtinIsURL no-args: expected false, got %v", got)
	}
	if got := i.builtinIsURL([]Value{123}); got != false {
		t.Fatalf("builtinIsURL non-string: expected false, got %v", got)
	}
	if got := i.builtinIsURL([]Value{"example.com"}); got != false {
		t.Fatalf("builtinIsURL invalid: expected false, got %v", got)
	}
	if got := i.builtinIsURL([]Value{"https://example.com/a?q=1"}); got != true {
		t.Fatalf("builtinIsURL valid: expected true, got %v", got)
	}

	if got := i.builtinIsUUID([]Value{}); got != false {
		t.Fatalf("builtinIsUUID no-args: expected false, got %v", got)
	}
	if got := i.builtinIsUUID([]Value{123}); got != false {
		t.Fatalf("builtinIsUUID non-string: expected false, got %v", got)
	}
	if got := i.builtinIsUUID([]Value{"1234"}); got != false {
		t.Fatalf("builtinIsUUID invalid: expected false, got %v", got)
	}
	if got := i.builtinIsUUID([]Value{"550e8400-e29b-41d4-a716-446655440000"}); got != true {
		t.Fatalf("builtinIsUUID valid: expected true, got %v", got)
	}

	if got := i.builtinIsIP([]Value{}); got != false {
		t.Fatalf("builtinIsIP no-args: expected false, got %v", got)
	}
	if got := i.builtinIsIP([]Value{123}); got != false {
		t.Fatalf("builtinIsIP non-string: expected false, got %v", got)
	}
	if got := i.builtinIsIP([]Value{"999.1.2.3"}); got != false {
		t.Fatalf("builtinIsIP out-of-range IPv4: expected false, got %v", got)
	}
	if got := i.builtinIsIP([]Value{"127.0.0.1"}); got != true {
		t.Fatalf("builtinIsIP valid IPv4: expected true, got %v", got)
	}
	if got := i.builtinIsIP([]Value{"2001:0db8:85a3:0000:0000:8a2e:0370:7334"}); got != true {
		t.Fatalf("builtinIsIP valid IPv6: expected true, got %v", got)
	}
	if got := i.builtinIsIP([]Value{"not-an-ip"}); got != false {
		t.Fatalf("builtinIsIP invalid string: expected false, got %v", got)
	}

	if got := i.builtinIsCreditCard([]Value{}); got != false {
		t.Fatalf("builtinIsCreditCard no-args: expected false, got %v", got)
	}
	if got := i.builtinIsCreditCard([]Value{123}); got != false {
		t.Fatalf("builtinIsCreditCard non-string: expected false, got %v", got)
	}
	if got := i.builtinIsCreditCard([]Value{"1234"}); got != false {
		t.Fatalf("builtinIsCreditCard short length: expected false, got %v", got)
	}
	if got := i.builtinIsCreditCard([]Value{"4111-1111-1111-1111"}); got != true {
		t.Fatalf("builtinIsCreditCard valid luhn: expected true, got %v", got)
	}
	if got := i.builtinIsCreditCard([]Value{"4111-1111-1111-111X"}); got != false {
		t.Fatalf("builtinIsCreditCard invalid digit parse: expected false, got %v", got)
	}

	if got := i.builtinIsIPv4([]Value{}); got != false {
		t.Fatalf("builtinIsIPv4 no-args: expected false, got %v", got)
	}
	if got := i.builtinIsIPv4([]Value{123}); got != false {
		t.Fatalf("builtinIsIPv4 non-string: expected false, got %v", got)
	}
	if got := i.builtinIsIPv4([]Value{"bad"}); got != false {
		t.Fatalf("builtinIsIPv4 invalid parse: expected false, got %v", got)
	}
	if got := i.builtinIsIPv4([]Value{"::1"}); got != false {
		t.Fatalf("builtinIsIPv4 IPv6 input: expected false, got %v", got)
	}
	if got := i.builtinIsIPv4([]Value{"10.0.0.1"}); got != true {
		t.Fatalf("builtinIsIPv4 valid: expected true, got %v", got)
	}

	if got := i.builtinIsIPv6([]Value{}); got != false {
		t.Fatalf("builtinIsIPv6 no-args: expected false, got %v", got)
	}
	if got := i.builtinIsIPv6([]Value{123}); got != false {
		t.Fatalf("builtinIsIPv6 non-string: expected false, got %v", got)
	}
	if got := i.builtinIsIPv6([]Value{"bad"}); got != false {
		t.Fatalf("builtinIsIPv6 invalid parse: expected false, got %v", got)
	}
	if got := i.builtinIsIPv6([]Value{"10.0.0.1"}); got != false {
		t.Fatalf("builtinIsIPv6 IPv4 input: expected false, got %v", got)
	}
	if got := i.builtinIsIPv6([]Value{"2001:db8::1"}); got != true {
		t.Fatalf("builtinIsIPv6 valid: expected true, got %v", got)
	}
}
