package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch7_FormatFunctions(t *testing.T) {
	i := NewInterpreter(nil)

	if got := i.builtinFormatFloat([]Value{}); got != "" {
		t.Fatalf("expected empty formatFloat for no args, got %v", got)
	}
	if got := i.builtinFormatFloat([]Value{3.14159, int64(3)}); got != "3.142" {
		t.Fatalf("expected rounded formatFloat, got %v", got)
	}

	if got := i.builtinFormatInt([]Value{}); got != "" {
		t.Fatalf("expected empty formatInt for no args, got %v", got)
	}
	if got := i.builtinFormatInt([]Value{int64(255), int64(16)}); got != "ff" {
		t.Fatalf("expected hex formatInt ff, got %v", got)
	}
	if got := i.builtinFormatInt([]Value{int64(10), int64(2)}); got != "1010" {
		t.Fatalf("expected binary formatInt 1010, got %v", got)
	}
	if got := i.builtinFormatInt([]Value{int64(5), int64(3)}); got != "12" {
		t.Fatalf("expected base3 formatInt 12, got %v", got)
	}

	if got := i.builtinFormatCurrency([]Value{}); got != "" {
		t.Fatalf("expected empty formatCurrency for no args, got %v", got)
	}
	if got := i.builtinFormatCurrency([]Value{1234.5, "$", int64(2)}); got != "$1,234.50" {
		t.Fatalf("expected formatted currency, got %v", got)
	}
	if got := i.builtinFormatCurrency([]Value{-1234.5, "EUR", int64(1)}); got != "-EUR1,234.5" {
		t.Fatalf("expected negative currency formatting, got %v", got)
	}

	if got := i.builtinFormatPercent([]Value{}); got != "" {
		t.Fatalf("expected empty formatPercent for no args, got %v", got)
	}
	if got := i.builtinFormatPercent([]Value{0.1234, int64(1)}); got != "12.3%" {
		t.Fatalf("expected percent 12.3%%, got %v", got)
	}

	if got := i.builtinFormatBytes([]Value{}); got != "0 B" {
		t.Fatalf("expected zero bytes string, got %v", got)
	}
	if got := i.builtinFormatBytes([]Value{int64(1024)}); got != "1.00 KB" {
		t.Fatalf("expected KB bytes formatting, got %v", got)
	}
	if got := i.builtinFormatBytes([]Value{int64(-5)}); got != "5 B" {
		t.Fatalf("expected absolute value formatting, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch7_DateTextAndAlign(t *testing.T) {
	i := NewInterpreter(nil)

	if got := i.builtinFormatDate([]Value{}); got != "" {
		t.Fatalf("expected empty formatDate for no args, got %v", got)
	}
	if got := i.builtinFormatDate([]Value{int64(0), "2006-01-02"}); got != "1970-01-01" {
		t.Fatalf("expected epoch formatted date, got %v", got)
	}
	if got := i.builtinFormatDate([]Value{"not-rfc3339", "2006"}); got == "" {
		t.Fatalf("expected fallback now formatting to be non-empty")
	}

	if got := i.builtinParseDate([]Value{}); got != nil {
		t.Fatalf("expected nil parseDate for no args, got %v", got)
	}
	if got := i.builtinParseDate([]Value{123}); got != nil {
		t.Fatalf("expected nil parseDate for non-string input, got %v", got)
	}
	if m, ok := i.builtinParseDate([]Value{"2024-13-40"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected parseDate error map, got %v", m)
	}
	parsed := i.builtinParseDate([]Value{"2024-05-06"})
	pm, ok := parsed.(map[string]Value)
	if !ok || pm["success"] != true || pm["year"] != int64(2024) || pm["month"] != int64(5) || pm["day"] != int64(6) {
		t.Fatalf("expected parseDate success map, got %T (%v)", parsed, parsed)
	}

	if got := i.builtinIndent([]Value{}); got != "" {
		t.Fatalf("expected empty indent for no args, got %v", got)
	}
	if got := i.builtinIndent([]Value{"a\n\nb", "--"}); got != "--a\n\n--b" {
		t.Fatalf("expected indented multiline string, got %q", got)
	}

	if got := i.builtinAlign([]Value{"x"}); got != "" {
		t.Fatalf("expected empty align for too few args, got %v", got)
	}
	if got := i.builtinAlign([]Value{"x", int64(3), "left"}); got != "x  " {
		t.Fatalf("expected left aligned text, got %q", got)
	}
	if got := i.builtinAlign([]Value{"x", int64(3), "right"}); got != "  x" {
		t.Fatalf("expected right aligned text, got %q", got)
	}
	if got := i.builtinAlign([]Value{"x", int64(4), "center"}); got != " x  " {
		t.Fatalf("expected centered text, got %q", got)
	}

	if got := i.builtinAlignLeft([]Value{"ab", int64(5)}); got != "ab   " {
		t.Fatalf("expected alignLeft result, got %q", got)
	}
	if got := i.builtinAlignRight([]Value{"ab", int64(5)}); got != "   ab" {
		t.Fatalf("expected alignRight result, got %q", got)
	}
	if got := i.builtinAlignCenter([]Value{"ab", int64(6)}); !strings.HasPrefix(got.(string), "  ") {
		t.Fatalf("expected alignCenter to include left padding, got %q", got)
	}
}
