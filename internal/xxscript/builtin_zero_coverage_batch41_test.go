package xxscript

import (
	"strings"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch41_StringRegexArrayAndJSONHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinCharAt([]Value{}); got != "" {
		t.Fatalf("expected charAt empty for missing args, got %v", got)
	}
	if got := i.builtinCharAt([]Value{123, 0}); got != "" {
		t.Fatalf("expected charAt empty for non-string, got %v", got)
	}
	if got := i.builtinCharAt([]Value{"abc", -1}); got != "" {
		t.Fatalf("expected charAt empty for negative index, got %v", got)
	}
	if got := i.builtinCharAt([]Value{"ab", 9}); got != "" {
		t.Fatalf("expected charAt empty for OOB index, got %v", got)
	}
	if got := i.builtinCharAt([]Value{"a你c", 1}); got != "你" {
		t.Fatalf("expected rune-aware charAt, got %v", got)
	}

	if got := i.builtinCharCodeAt([]Value{}); got != int64(-1) {
		t.Fatalf("expected charCodeAt -1 for missing args, got %v", got)
	}
	if got := i.builtinCharCodeAt([]Value{123, 0}); got != int64(-1) {
		t.Fatalf("expected charCodeAt -1 for non-string, got %v", got)
	}
	if got := i.builtinCharCodeAt([]Value{"ab", 5}); got != int64(-1) {
		t.Fatalf("expected charCodeAt -1 for OOB index, got %v", got)
	}
	if got := i.builtinCharCodeAt([]Value{"A", 0}); got != int64(65) {
		t.Fatalf("expected charCodeAt 65 for 'A', got %v", got)
	}

	if got := i.builtinFromSize([]Value{}); got != int64(0) {
		t.Fatalf("expected fromSize default 0, got %v", got)
	}
	if got := i.builtinFromSize([]Value{123}); got != int64(0) {
		t.Fatalf("expected fromSize non-string 0, got %v", got)
	}
	if got := i.builtinFromSize([]Value{"bad"}); got != int64(0) {
		t.Fatalf("expected fromSize invalid input 0, got %v", got)
	}
	if got := i.builtinFromSize([]Value{"1.5 MB"}); got != int64(1572864) {
		t.Fatalf("expected fromSize 1.5MB in bytes, got %v", got)
	}
	if got := i.builtinFromSize([]Value{"2TB"}); got != int64(2*1024*1024*1024*1024) {
		t.Fatalf("expected fromSize TB conversion, got %v", got)
	}

	if got := i.builtinRegexMatch([]Value{}); got != false {
		t.Fatalf("expected regexMatch false for missing args, got %v", got)
	}
	if got := i.builtinRegexMatch([]Value{123, "x"}); got != false {
		t.Fatalf("expected regexMatch false for bad types, got %v", got)
	}
	if got := i.builtinRegexMatch([]Value{"(", "x"}); got != false {
		t.Fatalf("expected regexMatch false for bad regex, got %v", got)
	}
	if got := i.builtinRegexMatch([]Value{"^a.+z$", "abz"}); got != true {
		t.Fatalf("expected regexMatch true for valid match, got %v", got)
	}

	if got := i.builtinRegexFind([]Value{}); got != "" {
		t.Fatalf("expected regexFind empty for missing args, got %v", got)
	}
	if got := i.builtinRegexFind([]Value{123, "x"}); got != "" {
		t.Fatalf("expected regexFind empty for bad types, got %v", got)
	}
	if got := i.builtinRegexFind([]Value{"(", "abc"}); got != "" {
		t.Fatalf("expected regexFind empty for bad regex, got %v", got)
	}
	if got := i.builtinRegexFind([]Value{"b+", "aaabbbccc"}); got != "bbb" {
		t.Fatalf("expected regexFind first match 'bbb', got %v", got)
	}

	if got := i.builtinUnion([]Value{}).([]Value); len(got) != 0 {
		t.Fatalf("expected union empty for missing args, got %v", got)
	}
	if got := i.builtinUnion([]Value{"bad", []Value{1, 1, 2}}).([]Value); len(got) != 2 {
		t.Fatalf("expected union fallback+unique from arr2, got %v", got)
	}
	if got := i.builtinUnion([]Value{[]Value{1, 2}, "bad"}).([]Value); len(got) != 2 {
		t.Fatalf("expected union fallback+unique from arr1, got %v", got)
	}
	if got := i.builtinUnion([]Value{[]Value{1, 2}, []Value{2, 3, 3}}).([]Value); len(got) != 3 {
		t.Fatalf("expected union dedupe length 3, got %v", got)
	}

	if got := i.builtinFindIndex([]Value{}); got != int64(-1) {
		t.Fatalf("expected findIndex -1 for missing args, got %v", got)
	}
	if got := i.builtinFindIndex([]Value{"bad", 1}); got != int64(-1) {
		t.Fatalf("expected findIndex -1 for non-array, got %v", got)
	}
	if got := i.builtinFindIndex([]Value{[]Value{1, 2, 3}, 9}); got != int64(-1) {
		t.Fatalf("expected findIndex -1 when not found, got %v", got)
	}
	if got := i.builtinFindIndex([]Value{[]Value{"x", "y", "x"}, "x"}); got != int64(0) {
		t.Fatalf("expected findIndex first match index 0, got %v", got)
	}

	if got := i.builtinFindLastIndex([]Value{}); got != int64(-1) {
		t.Fatalf("expected findLastIndex -1 for missing args, got %v", got)
	}
	if got := i.builtinFindLastIndex([]Value{"bad", 1}); got != int64(-1) {
		t.Fatalf("expected findLastIndex -1 for non-array, got %v", got)
	}
	if got := i.builtinFindLastIndex([]Value{[]Value{1, 2, 3}, 9}); got != int64(-1) {
		t.Fatalf("expected findLastIndex -1 when not found, got %v", got)
	}
	if got := i.builtinFindLastIndex([]Value{[]Value{"x", "y", "x"}, "x"}); got != int64(2) {
		t.Fatalf("expected findLastIndex last match index 2, got %v", got)
	}

	if m := i.builtinJSONValidate([]Value{}).(map[string]Value); m["valid"] != false {
		t.Fatalf("expected jsonValidate false for missing input, got %v", m)
	}
	if m := i.builtinJSONValidate([]Value{123}).(map[string]Value); m["valid"] != false {
		t.Fatalf("expected jsonValidate false for non-string input, got %v", m)
	}
	if m := i.builtinJSONValidate([]Value{"{"}).(map[string]Value); m["valid"] != false {
		t.Fatalf("expected jsonValidate false for invalid JSON, got %v", m)
	}
	if m := i.builtinJSONValidate([]Value{"{\"x\":1}"}).(map[string]Value); m["valid"] != true {
		t.Fatalf("expected jsonValidate true for valid JSON, got %v", m)
	}

	if got := i.builtinStrftime([]Value{}); got != "" {
		t.Fatalf("expected strftime empty for missing args, got %v", got)
	}
	if got := i.builtinStrftime([]Value{123, 456}); got != "" {
		t.Fatalf("expected strftime empty for non-string format, got %v", got)
	}
	if got := i.builtinStrftime([]Value{"2024-05-06 12:34:56", "%Y-%m-%d"}); got != "2024-05-06" {
		t.Fatalf("expected formatted date output, got %v", got)
	}
	fallback := i.builtinStrftime([]Value{"not-a-time", "%Y"}).(string)
	if strings.TrimSpace(fallback) == "" {
		t.Fatalf("expected non-empty strftime fallback output")
	}
	fixed := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := i.builtinStrftime([]Value{fixed, "%H:%M:%S"}); got != "03:04:05" {
		t.Fatalf("expected fixed-time strftime output, got %v", got)
	}
}
