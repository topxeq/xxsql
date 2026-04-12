package xxscript

import (
	"strings"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch49_DateFormat_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinDateFormat([]Value{}); got != "" {
		t.Fatalf("expected empty string for insufficient args, got %v", got)
	}

	if got := i.builtinDateFormat([]Value{int64(0), 123}); got != "" {
		t.Fatalf("expected empty string for non-string format, got %v", got)
	}

	ts := int64(1700000000)
	tm := time.Unix(ts, 0)

	if got := i.builtinDateFormat([]Value{ts, "RFC3339"}); got != tm.Format(time.RFC3339) {
		t.Fatalf("unexpected RFC3339 format output: %v", got)
	}
	if got := i.builtinDateFormat([]Value{ts, "RFC3339Nano"}); got != tm.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected RFC3339Nano format output: %v", got)
	}
	if got := i.builtinDateFormat([]Value{ts, "Date"}); got != tm.Format("2006-01-02") {
		t.Fatalf("unexpected Date format output: %v", got)
	}
	if got := i.builtinDateFormat([]Value{ts, "DateTime"}); got != tm.Format("2006-01-02 15:04:05") {
		t.Fatalf("unexpected DateTime format output: %v", got)
	}
	if got := i.builtinDateFormat([]Value{ts, "Time"}); got != tm.Format("15:04:05") {
		t.Fatalf("unexpected Time format output: %v", got)
	}
	if got := i.builtinDateFormat([]Value{ts, "ISO"}); got != tm.Format("2006-01-02T15:04:05Z07:00") {
		t.Fatalf("unexpected ISO format output: %v", got)
	}
	if got := i.builtinDateFormat([]Value{ts, "2006"}); got != tm.Format("2006") {
		t.Fatalf("unexpected custom format output: %v", got)
	}

	fallback := i.builtinDateFormat([]Value{"not-a-time", "Date"}).(string)
	if len(fallback) != len("2006-01-02") {
		t.Fatalf("expected now-based fallback date string length=10, got %q", fallback)
	}
}

func TestBuiltin_ZeroCoverage_Batch49_Wrap_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinWrap([]Value{}); got != "" {
		t.Fatalf("expected empty string for no args, got %v", got)
	}

	if got := i.builtinWrap([]Value{123, 0}); got != "123" {
		t.Fatalf("expected fmt string when width<=0, got %v", got)
	}

	if got := i.builtinWrap([]Value{"   \n\t  ", 10}); got != "" {
		t.Fatalf("expected empty string for whitespace-only text, got %q", got)
	}

	if got := i.builtinWrap([]Value{"a bb ccc", 20}); got != "a bb ccc" {
		t.Fatalf("expected single-line output when width large, got %q", got)
	}

	wrapped := i.builtinWrap([]Value{"a bb ccc dddd", 6}).(string)
	if !strings.Contains(wrapped, "\n") {
		t.Fatalf("expected wrapped output to contain newline, got %q", wrapped)
	}
}

func TestBuiltin_ZeroCoverage_Batch49_GroupBySorted_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinGroupBySorted([]Value{}); len(got.(map[string]Value)) != 0 {
		t.Fatalf("expected empty map for insufficient args, got %v", got)
	}
	if got := i.builtinGroupBySorted([]Value{"not-array", "k"}); len(got.(map[string]Value)) != 0 {
		t.Fatalf("expected empty map for non-array input, got %v", got)
	}
	if got := i.builtinGroupBySorted([]Value{[]Value{}, 123}); len(got.(map[string]Value)) != 0 {
		t.Fatalf("expected empty map for non-string key, got %v", got)
	}

	arr := []Value{
		map[string]Value{"g": "b", "id": int64(2)},
		map[string]Value{"g": "a", "id": int64(1)},
		map[string]Value{"g": "b", "id": int64(3)},
	}
	grouped := i.builtinGroupBySorted([]Value{arr, "g"}).(map[string]Value)

	ga, ok := grouped["a"].([]Value)
	if !ok || len(ga) != 1 {
		t.Fatalf("expected one item in group a, got %v", grouped["a"])
	}
	gb, ok := grouped["b"].([]Value)
	if !ok || len(gb) != 2 {
		t.Fatalf("expected two items in group b, got %v", grouped["b"])
	}
}

func TestBuiltin_ZeroCoverage_Batch49_DateAdd_ZeroTimeFallback(t *testing.T) {
	i := NewInterpreter(NewContext())

	now := time.Now().Unix()
	got := i.builtinDateAdd([]Value{"not-a-time", int64(0)}).(int64)
	if got < now-2 || got > now+2 {
		t.Fatalf("expected dateAdd zero-time fallback near now, got %d now=%d", got, now)
	}
}
