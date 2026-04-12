package xxscript

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_HTTPClientAndURL(t *testing.T) {
	i := NewInterpreter(nil)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Method", r.Method)
		w.Header().Add("X-Multi", "a")
		w.Header().Add("X-Multi", "b")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(r.Method + ":" + r.URL.Path))
	}))
	t.Cleanup(ts.Close)

	getResp, ok := i.builtinHTTPGet([]Value{ts.URL + "/get", map[string]Value{"X-Test": "1"}}).(map[string]Value)
	if !ok || getResp["success"] != true || getResp["statusCode"] != http.StatusCreated {
		t.Fatalf("expected successful GET response, got %v", getResp)
	}

	postResp, ok := i.builtinHTTPPost([]Value{ts.URL + "/post", map[string]Value{"name": "alice"}}).(map[string]Value)
	if !ok || postResp["success"] != true {
		t.Fatalf("expected successful POST response, got %v", postResp)
	}

	putResp, ok := i.builtinHTTPPut([]Value{ts.URL + "/put", "body"}).(map[string]Value)
	if !ok || putResp["success"] != true {
		t.Fatalf("expected successful PUT response, got %v", putResp)
	}

	deleteResp, ok := i.builtinHTTPDelete([]Value{ts.URL + "/delete"}).(map[string]Value)
	if !ok || deleteResp["success"] != true {
		t.Fatalf("expected successful DELETE response, got %v", deleteResp)
	}

	reqResp, ok := i.builtinHTTPRequest([]Value{map[string]Value{
		"url":     ts.URL + "/request",
		"method":  "PATCH",
		"body":    "x",
		"timeout": float64(2),
	}}).(map[string]Value)
	if !ok || reqResp["success"] != true {
		t.Fatalf("expected successful custom request response, got %v", reqResp)
	}

	if errMap, ok := i.builtinHTTPGet([]Value{}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected GET arg error, got %v", errMap)
	}
	if errMap, ok := i.builtinHTTPRequest([]Value{"bad"}).(map[string]Value); !ok || errMap["success"] != false {
		t.Fatalf("expected request options type error, got %v", errMap)
	}

	headerMap := headersToMap(http.Header{"Single": {"x"}, "Multi": {"a", "b"}})
	if headerMap["Single"] != "x" {
		t.Fatalf("expected single header mapping, got %v", headerMap)
	}
	if multi, ok := headerMap["Multi"].([]Value); !ok || len(multi) != 2 {
		t.Fatalf("expected multi header mapping, got %v", headerMap)
	}

	parsed, ok := i.builtinURLParse([]Value{"https://u:p@example.com/a/b?q=1&q=2#frag"}).(map[string]Value)
	if !ok || parsed["success"] != true || parsed["host"] != "example.com" {
		t.Fatalf("expected parsed URL map, got %v", parsed)
	}
	if q, ok := parsed["query"].(map[string]Value); !ok || q["q"] == nil {
		t.Fatalf("expected query map in parsed URL, got %v", parsed)
	}

	if got := i.builtinURLJoin([]Value{"https://example.com/base/", "child"}); got != "https://example.com/base/child" {
		t.Fatalf("expected joined URL, got %v", got)
	}

	built := i.builtinURLBuild([]Value{map[string]Value{
		"scheme":   "https",
		"host":     "example.com",
		"path":     "/x",
		"fragment": "f",
		"query":    map[string]Value{"a": "1"},
		"user":     "u",
		"password": "p",
	}})
	builtStr, ok := built.(string)
	if !ok || !strings.Contains(builtStr, "https://u:p@example.com/x") {
		t.Fatalf("expected built URL string, got %v", built)
	}

	if ipInfo, ok := i.builtinIPParse([]Value{"127.0.0.1"}).(map[string]Value); !ok || ipInfo["success"] != true {
		t.Fatalf("expected parsed IP info, got %v", ipInfo)
	}
	if got := i.builtinIsIPv4([]Value{"127.0.0.1"}); got != true {
		t.Fatalf("expected IPv4 true, got %v", got)
	}
	if got := i.builtinIsIPv6([]Value{"::1"}); got != true {
		t.Fatalf("expected IPv6 true, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_HTTPObjectMembers(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://example.com/path?q=v", strings.NewReader(`{"a":1}`))
	req.Header.Set("X-Test", "header")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ua-test")
	req.AddCookie(&http.Cookie{Name: "sid", Value: "cookie-v"})

	rr := httptest.NewRecorder()
	ctx := NewContext()
	ctx.HTTPRequest = req
	ctx.HTTPWriter = rr

	httpObj := NewHTTPObject(ctx)

	method, _ := httpObj.GetMember("method")
	if method != http.MethodPost {
		t.Fatalf("expected method member, got %v", method)
	}
	path, _ := httpObj.GetMember("path")
	if path != "/path" {
		t.Fatalf("expected path member, got %v", path)
	}

	paramFn, _ := httpObj.GetMember("param")
	if got, _ := paramFn.(Callable).Call([]Value{"q"}); got != "v" {
		t.Fatalf("expected query param value, got %v", got)
	}

	headerFn, _ := httpObj.GetMember("header")
	if got, _ := headerFn.(Callable).Call([]Value{"X-Test"}); got != "header" {
		t.Fatalf("expected header value, got %v", got)
	}

	jsonFn, _ := httpObj.GetMember("json")
	_, _ = jsonFn.(Callable).Call([]Value{map[string]Value{"ok": true}})
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected json content-type, got %q", ct)
	}

	statusFn, _ := httpObj.GetMember("status")
	_, _ = statusFn.(Callable).Call([]Value{int64(201)})

	writeFn, _ := httpObj.GetMember("write")
	_, _ = writeFn.(Callable).Call([]Value{"hello"})

	setHeaderFn, _ := httpObj.GetMember("setHeader")
	_, _ = setHeaderFn.(Callable).Call([]Value{"X-Out", "ok"})
	if rr.Header().Get("X-Out") != "ok" {
		t.Fatalf("expected response header set")
	}

	cookieFn, _ := httpObj.GetMember("cookie")
	if got, _ := cookieFn.(Callable).Call([]Value{"sid"}); got != "cookie-v" {
		t.Fatalf("expected cookie value, got %v", got)
	}

	setCookieFn, _ := httpObj.GetMember("setCookie")
	_, _ = setCookieFn.(Callable).Call([]Value{"a", "b", int64(10), "example.com", true, true})
	if setCookieHeader := rr.Header().Get("Set-Cookie"); setCookieHeader == "" {
		t.Fatalf("expected Set-Cookie header")
	}

	redirectFn, _ := httpObj.GetMember("redirect")
	_, _ = redirectFn.(Callable).Call([]Value{"/new", int64(302)})

	bodyReq := httptest.NewRequest(http.MethodPost, "http://example.com/body", strings.NewReader(`{"k":"v"}`))
	bodyCtx := NewContext()
	bodyCtx.HTTPRequest = bodyReq
	bodyObj := NewHTTPObject(bodyCtx)
	bodyFn, _ := bodyObj.GetMember("body")
	if got, _ := bodyFn.(Callable).Call(nil); got != `{"k":"v"}` {
		t.Fatalf("expected body reader content, got %v", got)
	}

	bodyJSONReq := httptest.NewRequest(http.MethodPost, "http://example.com/bodyjson", strings.NewReader(`{"k":1}`))
	bodyJSONCtx := NewContext()
	bodyJSONCtx.HTTPRequest = bodyJSONReq
	bodyJSONObj := NewHTTPObject(bodyJSONCtx)
	bodyJSONFn, _ := bodyJSONObj.GetMember("bodyJSON")
	v, err := bodyJSONFn.(Callable).Call(nil)
	if err != nil {
		t.Fatalf("expected valid JSON body parse, got error %v", err)
	}
	if m, ok := v.(map[string]Value); !ok || m["k"] != float64(1) {
		t.Fatalf("expected parsed JSON map, got %v", v)
	}

	if _, err := httpObj.GetMember("missing"); err == nil {
		t.Fatalf("expected unknown member error")
	}
}
