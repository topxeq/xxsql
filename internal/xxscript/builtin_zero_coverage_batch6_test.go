package xxscript

import (
	"strings"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch6_DNSAndJSONTransform(t *testing.T) {
	i := NewInterpreter(nil)

	if m, ok := i.builtinDNSLookup([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookup no-arg error, got %v", m)
	}
	if m, ok := i.builtinDNSLookup([]Value{123}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookup type error, got %v", m)
	}
	if m, ok := i.builtinDNSLookup([]Value{"localhost"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected dnsLookup localhost success, got %v", m)
	}

	if m, ok := i.builtinDNSLookupHost([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookupHost no-arg error, got %v", m)
	}
	if m, ok := i.builtinDNSLookupHost([]Value{123}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookupHost type error, got %v", m)
	}
	if m, ok := i.builtinDNSLookupHost([]Value{"localhost"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected dnsLookupHost localhost success, got %v", m)
	}

	if m, ok := i.builtinDNSLookupAddr([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookupAddr no-arg error, got %v", m)
	}
	if m, ok := i.builtinDNSLookupAddr([]Value{123}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookupAddr type error, got %v", m)
	}
	if m, ok := i.builtinDNSLookupAddr([]Value{"999.999.999.999"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected dnsLookupAddr invalid IP error, got %v", m)
	}

	if m, ok := i.builtinJSONTransform([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected jsonTransform arg error, got %v", m)
	}
	if m, ok := i.builtinJSONTransform([]Value{"not-json", "upper"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected jsonTransform invalid json error, got %v", m)
	}
	if m, ok := i.builtinJSONTransform([]Value{"{}", int64(1)}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected jsonTransform type error, got %v", m)
	}

	upper := i.builtinJSONTransform([]Value{"{\"name\":\"alice\",\"tags\":[\"x\",\"y\"]}", "upper"})
	upperMap, ok := upper.(map[string]Value)
	if !ok {
		t.Fatalf("expected transformed map, got %T", upper)
	}
	if upperMap["name"] != "ALICE" {
		t.Fatalf("expected upper transformed name, got %v", upperMap["name"])
	}

	numbered := i.builtinJSONTransform([]Value{"{\"n\":\"42\"}", "number"})
	numMap, ok := numbered.(map[string]Value)
	if !ok || numMap["n"] != float64(42) {
		t.Fatalf("expected number transformed value, got %T (%v)", numbered, numbered)
	}

	asInt := i.builtinJSONTransform([]Value{"{\"f\":3.9}", "int"})
	intMap, ok := asInt.(map[string]Value)
	if !ok || intMap["f"] != int64(3) {
		t.Fatalf("expected int transformed value, got %T (%v)", asInt, asInt)
	}

	asString := i.builtinJSONTransform([]Value{"{\"f\":3.5}", "string"})
	strMap, ok := asString.(map[string]Value)
	if !ok || !strings.Contains(strMap["f"].(string), "3.5") {
		t.Fatalf("expected string transformed value, got %T (%v)", asString, asString)
	}
}

func TestBuiltin_ZeroCoverage_Batch6_DateDurationAndSystem(t *testing.T) {
	i := NewInterpreter(nil)

	if home, ok := i.builtinHome(nil).(string); !ok || home == "" {
		t.Fatalf("expected non-empty home dir, got %T (%v)", home, home)
	}
	if clk, ok := i.builtinClock(nil).(float64); !ok || clk <= 0 {
		t.Fatalf("expected positive float clock, got %T (%v)", clk, clk)
	}

	if nowUnix, ok := i.builtinDateFromUnix(nil).(int64); !ok || nowUnix <= 0 {
		t.Fatalf("expected current unix timestamp, got %T (%v)", nowUnix, nowUnix)
	}
	fromZero := i.builtinDateFromUnix([]Value{int64(0)})
	fromZeroMap, ok := fromZero.(map[string]Value)
	if !ok || fromZeroMap["unix"] != int64(0) || !strings.Contains(fromZeroMap["string"].(string), "1970-01-01") {
		t.Fatalf("expected unix epoch map, got %T (%v)", fromZero, fromZero)
	}

	if got := i.builtinDateSub([]Value{}); got != int64(0) {
		t.Fatalf("expected dateSub no-arg 0, got %v", got)
	}
	if got := i.builtinDateSub([]Value{int64(20), int64(10)}); got != int64(10) {
		t.Fatalf("expected 10 second diff, got %v", got)
	}

	if _, ok := i.builtinDateAddDays([]Value{}).(int64); !ok {
		t.Fatalf("expected int64 unix for missing dateAddDays args")
	}
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	if got := i.builtinDateAddDays([]Value{base, int64(2)}); got != time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("expected +2 day unix timestamp, got %v", got)
	}

	t1 := int64(100)
	t2 := int64(200)
	if got := i.builtinDateBefore([]Value{t1, t2}); got != true {
		t.Fatalf("expected dateBefore true, got %v", got)
	}
	if got := i.builtinDateAfter([]Value{t2, t1}); got != true {
		t.Fatalf("expected dateAfter true, got %v", got)
	}
	if got := i.builtinDateEqual([]Value{t1, t1}); got != true {
		t.Fatalf("expected dateEqual true, got %v", got)
	}
	if got := i.builtinDateBetween([]Value{int64(150), t1, t2}); got != true {
		t.Fatalf("expected dateBetween true, got %v", got)
	}
	if got := i.builtinDateBetween([]Value{}); got != false {
		t.Fatalf("expected dateBetween false for too few args, got %v", got)
	}

	if m, ok := i.builtinParseDuration([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected parseDuration no-arg error, got %v", m)
	}
	if m, ok := i.builtinParseDuration([]Value{int64(1)}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected parseDuration type error, got %v", m)
	}
	if m, ok := i.builtinParseDuration([]Value{"bad"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected parseDuration parse error, got %v", m)
	}
	if m, ok := i.builtinParseDuration([]Value{"1h30m"}).(map[string]Value); !ok || m["success"] != true || m["minutes"] != 90.0 {
		t.Fatalf("expected parseDuration success map, got %v", m)
	}

	if got := i.builtinFormatDuration([]Value{}); got != "" {
		t.Fatalf("expected empty formatDuration result for no args, got %v", got)
	}
	if got := i.builtinFormatDuration([]Value{true}); got != "" {
		t.Fatalf("expected empty formatDuration result for unsupported type, got %v", got)
	}
	if got := i.builtinFormatDuration([]Value{"bad"}); got != "" {
		t.Fatalf("expected empty formatDuration result for invalid duration string, got %v", got)
	}
	if got := i.builtinFormatDuration([]Value{int64(3661)}); got != "1h 1m 1s" {
		t.Fatalf("expected formatted duration 1h 1m 1s, got %v", got)
	}
	if got := i.builtinFormatDuration([]Value{"48h"}); got != "2d" {
		t.Fatalf("expected formatted duration 2d, got %v", got)
	}

	if got := i.builtinIsWorkday([]Value{"2024-01-06"}); got != false {
		t.Fatalf("expected saturday to be non-workday, got %v", got)
	}
	if got := i.builtinIsWorkday([]Value{"2024-01-08"}); got != true {
		t.Fatalf("expected monday to be workday, got %v", got)
	}

	if m, ok := i.builtinMemStats(nil).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected memstats map with success=true, got %v", m)
	}
	if g, ok := i.builtinGoroutines(nil).(int64); !ok || g <= 0 {
		t.Fatalf("expected positive goroutine count, got %T (%v)", g, g)
	}

	if dir, ok := i.builtinUserHome(nil).(string); !ok || dir == "" {
		t.Fatalf("expected non-empty user home dir, got %T (%v)", dir, dir)
	}
	if _, ok := i.builtinUserCache(nil).(string); !ok {
		t.Fatalf("expected user cache dir string")
	}

	t.Setenv("XDG_CONFIG_HOME", "/tmp/xxsql_cfg")
	if got := i.builtinUserConfig(nil); got != "/tmp/xxsql_cfg" {
		t.Fatalf("expected user config from XDG_CONFIG_HOME, got %v", got)
	}
}
