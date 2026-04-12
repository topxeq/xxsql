package xxscript

import (
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch48_RegexReplaceFunc_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinRegexReplaceFunc([]Value{"only", "two"}); got != "" {
		t.Fatalf("expected empty string for insufficient args, got %v", got)
	}

	if got := i.builtinRegexReplaceFunc([]Value{123, "upper", "abc"}); got != "abc" {
		t.Fatalf("expected passthrough string for invalid arg types, got %v", got)
	}

	if got := i.builtinRegexReplaceFunc([]Value{"[", "upper", "abc"}); got != "abc" {
		t.Fatalf("expected passthrough string for invalid regex pattern, got %v", got)
	}

	cases := []struct {
		name      string
		transform string
		want      string
	}{
		{name: "upper", transform: "upper", want: "HELLO WORLD"},
		{name: "lower", transform: "lower", want: "hello world"},
		{name: "title", transform: "title", want: "Hello World"},
		{name: "reverse", transform: "reverse", want: "olleh dlrow"},
		{name: "double", transform: "double", want: "hellohello worldworld"},
		{name: "quote", transform: "quote", want: "\"hello\" \"world\""},
		{name: "default", transform: "unknown", want: "hello world"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := i.builtinRegexReplaceFunc([]Value{"\\w+", tc.transform, "hello world"})
			if got != tc.want {
				t.Fatalf("transform %s: expected %q, got %q", tc.transform, tc.want, got)
			}
		})
	}

	trimmed := i.builtinRegexReplaceFunc([]Value{"\\s+hello\\s+", "trim", " hello "})
	if trimmed != "hello" {
		t.Fatalf("transform trim: expected %q, got %q", "hello", trimmed)
	}
}

func TestBuiltin_ZeroCoverage_Batch48_CompareValues_Branches(t *testing.T) {
	if got := compareValues(nil, nil); got != 0 {
		t.Fatalf("expected compare(nil,nil)=0, got %d", got)
	}
	if got := compareValues(nil, 1); got != -1 {
		t.Fatalf("expected compare(nil,1)=-1, got %d", got)
	}
	if got := compareValues(1, nil); got != 1 {
		t.Fatalf("expected compare(1,nil)=1, got %d", got)
	}

	if got := compareValues(2, 3.0); got != -1 {
		t.Fatalf("expected numeric less-than branch, got %d", got)
	}
	if got := compareValues(3.0, int64(2)); got != 1 {
		t.Fatalf("expected numeric greater-than branch, got %d", got)
	}
	if got := compareValues(int64(7), 7.0); got != 0 {
		t.Fatalf("expected numeric equality branch, got %d", got)
	}

	if got := compareValues("abc", "abd"); got != -1 {
		t.Fatalf("expected lexical less-than branch, got %d", got)
	}
	if got := compareValues("abd", "abc"); got != 1 {
		t.Fatalf("expected lexical greater-than branch, got %d", got)
	}
	if got := compareValues("same", "same"); got != 0 {
		t.Fatalf("expected lexical equality branch, got %d", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch48_DateAdd_And_Sleep_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	nowUnix := time.Now().Unix()
	if got := i.builtinDateAdd([]Value{int64(1)}).(int64); got < nowUnix-2 || got > nowUnix+2 {
		t.Fatalf("expected dateAdd(<2 args) near now, got %d now=%d", got, nowUnix)
	}

	base := int64(1000)
	if got := i.builtinDateAdd([]Value{base, "2s"}).(int64); got != 1002 {
		t.Fatalf("expected dateAdd string duration +2s, got %d", got)
	}
	if got := i.builtinDateAdd([]Value{base, float64(1.5)}).(int64); got != 1001 {
		t.Fatalf("expected dateAdd float duration +1.5s truncated to unix +1, got %d", got)
	}
	if got := i.builtinDateAdd([]Value{base, int64(3)}).(int64); got != 1003 {
		t.Fatalf("expected dateAdd int64 duration +3s, got %d", got)
	}
	if got := i.builtinDateAdd([]Value{base, int(4)}).(int64); got != 1004 {
		t.Fatalf("expected dateAdd int duration +4s, got %d", got)
	}
	if got := i.builtinDateAdd([]Value{base, "not-duration"}).(int64); got != 1000 {
		t.Fatalf("expected dateAdd invalid duration to return base unix, got %d", got)
	}
	if got := i.builtinDateAdd([]Value{base, true}).(int64); got != 1000 {
		t.Fatalf("expected dateAdd unsupported duration type to return base unix, got %d", got)
	}

	if got := i.builtinSleep([]Value{}); got != nil {
		t.Fatalf("expected sleep with no args to return nil, got %v", got)
	}
	if got := i.builtinSleep([]Value{true}); got != nil {
		t.Fatalf("expected sleep with unsupported type to return nil, got %v", got)
	}
	if got := i.builtinSleep([]Value{"not-duration"}); got != nil {
		t.Fatalf("expected sleep with invalid duration string to return nil, got %v", got)
	}

	_ = i.builtinSleep([]Value{float64(0)})
	_ = i.builtinSleep([]Value{int(0)})
	_ = i.builtinSleep([]Value{int64(0)})

	start := time.Now()
	_ = i.builtinSleep([]Value{"1ms"})
	if elapsed := time.Since(start); elapsed < time.Millisecond {
		t.Fatalf("expected string-duration sleep branch to wait at least 1ms, got %s", elapsed)
	}
}
