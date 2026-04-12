package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch13_HTMLRandomQRCache(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinHTMLToMarkdown([]Value{}); got != "" {
		t.Fatalf("expected empty htmlToMarkdown for no args, got %v", got)
	}
	if got := i.builtinHTMLToMarkdown([]Value{int64(1)}); got != "" {
		t.Fatalf("expected empty htmlToMarkdown for invalid type, got %v", got)
	}
	md := i.builtinHTMLToMarkdown([]Value{"<h1>T</h1><p><strong>B</strong> <a href=\"https://e.com\">L</a></p>"})
	mdStr, ok := md.(string)
	if !ok || !strings.Contains(mdStr, "# T") || !strings.Contains(mdStr, "**B**") || !strings.Contains(mdStr, "[L](https://e.com)") {
		t.Fatalf("expected markdown markers in htmlToMarkdown output, got %T (%v)", md, md)
	}

	pwd := i.builtinRandomPassword([]Value{int64(12), "d"})
	pwdStr, ok := pwd.(string)
	if !ok || len(pwdStr) != 12 {
		t.Fatalf("expected randomPassword length 12, got %T (%v)", pwd, pwd)
	}
	for _, ch := range pwdStr {
		if ch < '0' || ch > '9' {
			t.Fatalf("expected digit-only randomPassword with d flag, got %q", pwdStr)
		}
	}

	if got := i.builtinRandomColor([]Value{"rgb"}); !strings.HasPrefix(got.(string), "rgb(") {
		t.Fatalf("expected rgb randomColor format, got %v", got)
	}
	if got := i.builtinRandomColor([]Value{"rgba"}); !strings.HasPrefix(got.(string), "rgba(") {
		t.Fatalf("expected rgba randomColor format, got %v", got)
	}
	if got := i.builtinRandomColor([]Value{"hsl"}); !strings.HasPrefix(got.(string), "hsl(") {
		t.Fatalf("expected hsl randomColor format, got %v", got)
	}
	if got := i.builtinRandomColor(nil); len(got.(string)) != 7 || got.(string)[0] != '#' {
		t.Fatalf("expected hex randomColor default format, got %v", got)
	}

	if got := i.builtinRandomName([]Value{"first"}); got.(string) == "" || strings.Contains(got.(string), " ") {
		t.Fatalf("expected first name style without spaces, got %v", got)
	}
	if got := i.builtinRandomName([]Value{"last"}); strings.Contains(got.(string), " ") {
		t.Fatalf("expected last name style without spaces, got %v", got)
	}
	if got := i.builtinRandomName([]Value{"username"}); strings.Contains(got.(string), " ") {
		t.Fatalf("expected username style without spaces, got %v", got)
	}
	if got := i.builtinRandomName([]Value{"initials"}); len(got.(string)) != 2 {
		t.Fatalf("expected initials length 2, got %v", got)
	}
	if got := i.builtinRandomName(nil); !strings.Contains(got.(string), " ") {
		t.Fatalf("expected full name default with space, got %v", got)
	}

	token := i.builtinRandomToken([]Value{int64(20)})
	tokenStr, ok := token.(string)
	if !ok || len(tokenStr) != 20 {
		t.Fatalf("expected randomToken length 20, got %T (%v)", token, token)
	}

	if got := i.builtinQREncode([]Value{}); got != "" {
		t.Fatalf("expected empty qrEncode for no args, got %v", got)
	}
	qr := i.builtinQREncode([]Value{"hello"})
	if qrStr, ok := qr.(string); !ok || qrStr == "" {
		t.Fatalf("expected non-empty qr ascii output, got %T (%v)", qr, qr)
	}

	if got := i.builtinQRDataURL([]Value{}); got != "" {
		t.Fatalf("expected empty qrDataURL for no args, got %v", got)
	}
	dataURL := i.builtinQRDataURL([]Value{"hello", int64(64)})
	if s, ok := dataURL.(string); !ok || !strings.HasPrefix(s, "data:image/png;base64,") {
		t.Fatalf("expected qrDataURL prefix, got %T (%v)", dataURL, dataURL)
	}

	_ = i.builtinCacheClear(nil)
	if got := i.builtinCacheSet([]Value{}); got != false {
		t.Fatalf("expected cacheSet false for no args, got %v", got)
	}
	if got := i.builtinCacheSet([]Value{"k", "v"}); got != true {
		t.Fatalf("expected cacheSet true, got %v", got)
	}
	if got := i.builtinCacheGet([]Value{"k"}); got != "v" {
		t.Fatalf("expected cacheGet value v, got %v", got)
	}
	if got := i.builtinCacheHas([]Value{"k"}); got != true {
		t.Fatalf("expected cacheHas true for existing key, got %v", got)
	}
	keys := i.builtinCacheKeys(nil)
	if arr, ok := keys.([]Value); !ok || len(arr) == 0 {
		t.Fatalf("expected non-empty cacheKeys, got %T (%v)", keys, keys)
	}
	if got := i.builtinCacheDel([]Value{"k"}); got != true {
		t.Fatalf("expected cacheDel true, got %v", got)
	}
	if got := i.builtinCacheGet([]Value{"k"}); got != nil {
		t.Fatalf("expected cacheGet nil after delete, got %v", got)
	}

	if got := i.builtinCacheSet([]Value{"exp", "gone", int64(-1)}); got != true {
		t.Fatalf("expected cacheSet with ttl true, got %v", got)
	}
	if got := i.builtinCacheHas([]Value{"exp"}); got != false {
		t.Fatalf("expected cacheHas false for expired key, got %v", got)
	}
	if got := i.builtinCacheClear(nil); got != true {
		t.Fatalf("expected cacheClear true, got %v", got)
	}
}
