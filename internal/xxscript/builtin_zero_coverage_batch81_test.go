package xxscript

import (
	"strings"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch81_JSONAndTime(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinJSON(nil); got != "null" {
		t.Fatalf("builtinJSON nil args: got=%#v", got)
	}
	if got := i.builtinJSON([]Value{map[string]Value{"a": int64(1)}}); got != `{"a":1}` {
		t.Fatalf("builtinJSON object: got=%#v", got)
	}
	jsonErr := i.builtinJSON([]Value{func() {}})
	jsonErrStr, ok := jsonErr.(string)
	if !ok || !strings.Contains(jsonErrStr, `"error"`) {
		t.Fatalf("builtinJSON marshal error: got=%#v", jsonErr)
	}

	if got := i.builtinJSONParse(nil); got != nil {
		t.Fatalf("builtinJSONParse nil args: got=%#v", got)
	}
	if got := i.builtinJSONParse([]Value{1}); got != nil {
		t.Fatalf("builtinJSONParse non-string: got=%#v", got)
	}
	if got := i.builtinJSONParse([]Value{"{"}); got != nil {
		t.Fatalf("builtinJSONParse invalid json: got=%#v", got)
	}
	parsed := i.builtinJSONParse([]Value{`{"a":[1,true,"x"]}`})
	parsedMap, ok := parsed.(map[string]Value)
	if !ok {
		t.Fatalf("builtinJSONParse success type: got=%T (%#v)", parsed, parsed)
	}
	arr, ok := parsedMap["a"].([]Value)
	if !ok || len(arr) != 3 || arr[0] != float64(1) || arr[1] != true || arr[2] != "x" {
		t.Fatalf("builtinJSONParse converted value: got=%#v", parsedMap["a"])
	}

	if got := convertJSONToValue(struct{}{}); got != nil {
		t.Fatalf("convertJSONToValue default branch: got=%#v", got)
	}

	if got := i.builtinFormatTime(nil); got != "" {
		t.Fatalf("builtinFormatTime nil args: got=%#v", got)
	}
	if got := i.builtinFormatTime([]Value{int64(0), 123}); got != "" {
		t.Fatalf("builtinFormatTime non-string format: got=%#v", got)
	}
	wantFloatTS := time.Unix(123, 0).Format("2006-01-02")
	if got := i.builtinFormatTime([]Value{float64(123), "2006-01-02"}); got != wantFloatTS {
		t.Fatalf("builtinFormatTime float timestamp: got=%#v want=%#v", got, wantFloatTS)
	}
	wantIntTS := time.Unix(456, 0).Format("15:04")
	if got := i.builtinFormatTime([]Value{int64(456), "15:04"}); got != wantIntTS {
		t.Fatalf("builtinFormatTime int timestamp: got=%#v want=%#v", got, wantIntTS)
	}

	if got := i.builtinParseTime(nil); got != 0 {
		t.Fatalf("builtinParseTime nil args: got=%#v", got)
	}
	if got := i.builtinParseTime([]Value{1, time.RFC3339}); got != 0 {
		t.Fatalf("builtinParseTime non-string time: got=%#v", got)
	}
	if got := i.builtinParseTime([]Value{"1970-01-01T00:00:10Z", 1}); got != 0 {
		t.Fatalf("builtinParseTime non-string format: got=%#v", got)
	}
	if got := i.builtinParseTime([]Value{"not-a-time", time.RFC3339}); got != 0 {
		t.Fatalf("builtinParseTime parse error: got=%#v", got)
	}
	if got := i.builtinParseTime([]Value{"1970-01-01T00:00:10Z", time.RFC3339}); got != int64(10) {
		t.Fatalf("builtinParseTime success: got=%#v", got)
	}
}
