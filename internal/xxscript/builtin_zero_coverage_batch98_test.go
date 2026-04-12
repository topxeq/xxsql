package xxscript

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch98_HTTPObjectMembersAndCallables(t *testing.T) {
	ctx := NewContext()
	req := httptest.NewRequest("POST", "http://example.com/p?a=1", strings.NewReader("{\"x\":1}"))
	req.Header.Set("X-Test", "v")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "xxscript-test")
	req.RemoteAddr = "127.0.0.1:9999"
	ctx.HTTPRequest = req
	ctx.HTTPWriter = httptest.NewRecorder()

	h := NewHTTPObject(ctx)

	if v, err := h.GetMember("method"); err != nil || v != "POST" {
		t.Fatalf("http.method mismatch: v=%#v err=%v", v, err)
	}
	if v, err := h.GetMember("path"); err != nil || v != "/p" {
		t.Fatalf("http.path mismatch: v=%#v err=%v", v, err)
	}
	if v, err := h.GetMember("query"); err != nil || v != "a=1" {
		t.Fatalf("http.query mismatch: v=%#v err=%v", v, err)
	}
	if v, err := h.GetMember("remoteAddr"); err != nil || v != "127.0.0.1:9999" {
		t.Fatalf("http.remoteAddr mismatch: v=%#v err=%v", v, err)
	}
	if v, err := h.GetMember("contentType"); err != nil || v != "application/json" {
		t.Fatalf("http.contentType mismatch: v=%#v err=%v", v, err)
	}
	if v, err := h.GetMember("userAgent"); err != nil || v != "xxscript-test" {
		t.Fatalf("http.userAgent mismatch: v=%#v err=%v", v, err)
	}

	paramFn, _ := h.GetMember("param")
	if v, err := paramFn.(*HTTPParamFunc).Call([]Value{"a"}); err != nil || v != "1" {
		t.Fatalf("http.param call mismatch: v=%#v err=%v", v, err)
	}
	if v, err := paramFn.(*HTTPParamFunc).Call([]Value{1}); err != nil || v != "" {
		t.Fatalf("http.param non-string mismatch: v=%#v err=%v", v, err)
	}

	headerFn, _ := h.GetMember("header")
	if v, err := headerFn.(*HTTPHeaderFunc).Call([]Value{"X-Test"}); err != nil || v != "v" {
		t.Fatalf("http.header call mismatch: v=%#v err=%v", v, err)
	}

	bodyFn, _ := h.GetMember("body")
	if v, err := bodyFn.(*HTTPBodyFunc).Call(nil); err != nil || v != "{\"x\":1}" {
		t.Fatalf("http.body call mismatch: v=%#v err=%v", v, err)
	}

	ctx.HTTPRequest = httptest.NewRequest("POST", "http://example.com/", strings.NewReader("{\"k\":2}"))
	bodyJSONFn, _ := h.GetMember("bodyJSON")
	v, err := bodyJSONFn.(*HTTPBodyJSONFunc).Call(nil)
	if err != nil {
		t.Fatalf("http.bodyJSON call error: %v", err)
	}
	obj, ok := v.(map[string]Value)
	if !ok || !valuesEqual(obj["k"], float64(2)) {
		t.Fatalf("http.bodyJSON mismatch: %#v", v)
	}

	ctx.HTTPRequest = httptest.NewRequest("POST", "http://example.com/", strings.NewReader("{"))
	_, err = bodyJSONFn.(*HTTPBodyJSONFunc).Call(nil)
	if err == nil {
		t.Fatalf("expected http.bodyJSON invalid-json error")
	}

	statusFn, _ := h.GetMember("status")
	if _, err := statusFn.(*HTTPStatusFunc).Call([]Value{201}); err != nil {
		t.Fatalf("http.status call error: %v", err)
	}

	cookieReq := httptest.NewRequest("GET", "http://example.com/", nil)
	cookieReq.AddCookie(&http.Cookie{Name: "sid", Value: "abc"})
	ctx.HTTPRequest = cookieReq
	cookieFn, _ := h.GetMember("cookie")
	if cv, err := cookieFn.(*HTTPCookieFunc).Call([]Value{"sid"}); err != nil || cv != "abc" {
		t.Fatalf("http.cookie call mismatch: v=%#v err=%v", cv, err)
	}

	if _, err := h.GetMember("nope"); err == nil {
		t.Fatalf("expected unknown http member error")
	}
}

func TestBuiltin_ZeroCoverage_Batch98_URLFunctions(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinURLEncode(nil); got != "" {
		t.Fatalf("builtinURLEncode nil expected empty, got %#v", got)
	}
	if got := i.builtinURLEncode([]Value{"a b"}); got != "a+b" {
		t.Fatalf("builtinURLEncode mismatch: %#v", got)
	}

	if got := i.builtinURLDecode(nil); got != "" {
		t.Fatalf("builtinURLDecode nil expected empty, got %#v", got)
	}
	if got := i.builtinURLDecode([]Value{"a+b"}); got != "a b" {
		t.Fatalf("builtinURLDecode mismatch: %#v", got)
	}
	if got := i.builtinURLDecode([]Value{"%%%"}); got != "" {
		t.Fatalf("builtinURLDecode invalid expected empty, got %#v", got)
	}

	if got := i.builtinURLJoin([]Value{"http://example.com/base/", "x"}); got != "http://example.com/base/x" {
		t.Fatalf("builtinURLJoin mismatch: %#v", got)
	}
	if got := i.builtinURLJoin([]Value{"http://example.com", 1}); got != "" {
		t.Fatalf("builtinURLJoin non-string expected empty, got %#v", got)
	}
	if got := i.builtinURLJoin([]Value{"http://example.com"}); got != "" {
		t.Fatalf("builtinURLJoin short args expected empty, got %#v", got)
	}
}
