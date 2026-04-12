package xxscript

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

type batch99ErrReadCloser struct{}

func (b *batch99ErrReadCloser) Read(p []byte) (int, error) { return 0, errors.New("read err") }
func (b *batch99ErrReadCloser) Close() error               { return nil }

func TestBuiltin_ZeroCoverage_Batch99_HTTPCallableEdgeBranches(t *testing.T) {
	ctx := NewContext()
	ctx.HTTPWriter = httptest.NewRecorder()
	ctx.HTTPRequest = httptest.NewRequest("GET", "http://example.com/?k=v", nil)

	h := NewHTTPObject(ctx)

	headerFn, _ := h.GetMember("header")
	if v, err := headerFn.(*HTTPHeaderFunc).Call(nil); err != nil || v != "" {
		t.Fatalf("header nil-args branch mismatch: v=%#v err=%v", v, err)
	}
	if v, err := headerFn.(*HTTPHeaderFunc).Call([]Value{123}); err != nil || v != "" {
		t.Fatalf("header non-string branch mismatch: v=%#v err=%v", v, err)
	}

	bodyFn, _ := h.GetMember("body")
	ctx.HTTPRequest = httptest.NewRequest("POST", "http://example.com/", io.NopCloser(&batch99ErrReadCloser{}))
	if v, err := bodyFn.(*HTTPBodyFunc).Call(nil); err != nil || v != "" {
		t.Fatalf("body read-error branch mismatch: v=%#v err=%v", v, err)
	}

	bodyJSONFn, _ := h.GetMember("bodyJSON")
	ctx.HTTPRequest = httptest.NewRequest("POST", "http://example.com/", io.NopCloser(&batch99ErrReadCloser{}))
	if v, err := bodyJSONFn.(*HTTPBodyJSONFunc).Call(nil); err != nil || v != nil {
		t.Fatalf("bodyJSON read-error branch mismatch: v=%#v err=%v", v, err)
	}

	statusFn, _ := h.GetMember("status")
	if _, err := statusFn.(*HTTPStatusFunc).Call(nil); err != nil {
		t.Fatalf("status nil-args branch error: %v", err)
	}
	if _, err := statusFn.(*HTTPStatusFunc).Call([]Value{float64(202)}); err != nil {
		t.Fatalf("status float branch error: %v", err)
	}

	cookieFn, _ := h.GetMember("cookie")
	if v, err := cookieFn.(*HTTPCookieFunc).Call([]Value{123}); err != nil || v != "" {
		t.Fatalf("cookie non-string branch mismatch: v=%#v err=%v", v, err)
	}
	ctx.HTTPRequest = httptest.NewRequest("GET", "http://example.com/", nil)
	if v, err := cookieFn.(*HTTPCookieFunc).Call([]Value{"missing"}); err != nil || v != "" {
		t.Fatalf("cookie missing-cookie branch mismatch: v=%#v err=%v", v, err)
	}

	ctx2 := NewContext()
	h2 := NewHTTPObject(ctx2)
	if v, err := h2.GetMember("method"); err != nil || v != "" {
		t.Fatalf("GetMember method nil-request branch mismatch: v=%#v err=%v", v, err)
	}
	if v, err := h2.GetMember("path"); err != nil || v != "" {
		t.Fatalf("GetMember path nil-request branch mismatch: v=%#v err=%v", v, err)
	}
}

func TestBuiltin_ZeroCoverage_Batch99_URLJoinParseErrors(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinURLJoin([]Value{":// bad", "x"}); got != "" {
		t.Fatalf("URLJoin bad-base expected empty, got %#v", got)
	}
	if got := i.builtinURLJoin([]Value{"http://example.com", "%zz"}); got != "" {
		t.Fatalf("URLJoin bad-path expected empty, got %#v", got)
	}

	// keep one normal path check here as sanity.
	if got := i.builtinURLJoin([]Value{"http://example.com/base/", "child"}); !strings.Contains(got.(string), "/base/child") {
		t.Fatalf("URLJoin sanity mismatch: %#v", got)
	}
}
