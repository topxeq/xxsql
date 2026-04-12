package xxscript

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch61_FileServeContentTypeSwitchCases(t *testing.T) {
	ctx := NewContext()
	ctx.BaseDir = t.TempDir()
	i := NewInterpreter(ctx)

	cases := []struct {
		name     string
		file     string
		expected string
	}{
		{name: "html", file: "a.html", expected: "text/html"},
		{name: "js", file: "a.js", expected: "application/javascript"},
		{name: "json", file: "a.json", expected: "application/json"},
		{name: "xml", file: "a.xml", expected: "application/xml"},
		{name: "png", file: "a.png", expected: "image/png"},
		{name: "jpg", file: "a.jpg", expected: "image/jpeg"},
		{name: "gif", file: "a.gif", expected: "image/gif"},
		{name: "svg", file: "a.svg", expected: "image/svg+xml"},
		{name: "ico", file: "a.ico", expected: "image/x-icon"},
		{name: "woff2", file: "a.woff2", expected: "font/woff2"},
		{name: "ttf", file: "a.ttf", expected: "font/ttf"},
		{name: "pdf", file: "a.pdf", expected: "application/pdf"},
		{name: "zip", file: "a.zip", expected: "application/zip"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if m := i.builtinFileSave([]Value{tc.file, "x"}).(map[string]Value); m["success"] != true {
				t.Fatalf("setup fileSave failed for %s: %v", tc.file, m)
			}

			rec := httptest.NewRecorder()
			ctx.HTTPWriter = rec
			if m := i.builtinFileServe([]Value{tc.file}).(map[string]Value); m["success"] != true {
				t.Fatalf("fileServe failed for %s: %v", tc.file, m)
			}

			if got := rec.Header().Get("Content-Type"); !strings.Contains(got, tc.expected) {
				t.Fatalf("content-type mismatch for %s: expected contains %q, got %q", tc.file, tc.expected, got)
			}
		})
	}
}
