package xxscript

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch14_HTTPAndPortTools(t *testing.T) {
	i := NewInterpreter(NewContext())

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("download-body"))
		case http.MethodPost:
			b, _ := io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("uploaded:" + string(b)))
		case http.MethodHead:
			w.Header().Set("X-One", "1")
			w.Header().Add("X-Multi", "a")
			w.Header().Add("X-Multi", "b")
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPatch:
			b, _ := io.ReadAll(r.Body)
			_, _ = w.Write([]byte("patch:" + string(b)))
		case http.MethodOptions:
			w.Header().Set("Allow", "GET, POST, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(ts.Close)

	tmp := t.TempDir()
	dlPath := filepath.Join(tmp, "dl.txt")

	if m, ok := i.builtinHTTPDownload([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected httpDownload arg error, got %v", m)
	}
	dl := i.builtinHTTPDownload([]Value{ts.URL, dlPath})
	dlm, ok := dl.(map[string]Value)
	if !ok || dlm["success"] != true {
		t.Fatalf("expected httpDownload success, got %T (%v)", dl, dl)
	}
	if b, err := os.ReadFile(dlPath); err != nil || string(b) != "download-body" {
		t.Fatalf("expected downloaded file content, got err=%v data=%q", err, string(b))
	}

	uploadPath := filepath.Join(tmp, "up.txt")
	if err := os.WriteFile(uploadPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("failed to prepare upload file: %v", err)
	}
	if m, ok := i.builtinHTTPUpload([]Value{123, uploadPath}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected httpUpload type error, got %v", m)
	}
	up := i.builtinHTTPUpload([]Value{ts.URL, uploadPath})
	upm, ok := up.(map[string]Value)
	if !ok || upm["success"] != true || upm["status"] != http.StatusCreated || !strings.Contains(upm["responseBody"].(string), "payload") {
		t.Fatalf("expected httpUpload success, got %T (%v)", up, up)
	}

	if m, ok := i.builtinHTTPHead([]Value{}).(map[string]Value); !ok || m["error"] == nil {
		t.Fatalf("expected httpHead arg error, got %v", m)
	}
	h := i.builtinHTTPHead([]Value{ts.URL})
	hm, ok := h.(map[string]Value)
	if !ok || hm["status"] != http.StatusNoContent {
		t.Fatalf("expected httpHead success status, got %T (%v)", h, h)
	}
	headers, ok := hm["headers"].(map[string]Value)
	if !ok || headers["X-One"] != "1" {
		t.Fatalf("expected httpHead headers map, got %v", hm)
	}

	if m, ok := i.builtinHTTPPatch([]Value{}).(map[string]Value); !ok || m["error"] == nil {
		t.Fatalf("expected httpPatch arg error, got %v", m)
	}
	p := i.builtinHTTPPatch([]Value{ts.URL, map[string]Value{"a": int64(1)}})
	pm, ok := p.(map[string]Value)
	if !ok || pm["status"] != http.StatusOK || !strings.Contains(pm["responseBody"].(string), "\"a\":1") {
		t.Fatalf("expected httpPatch response to echo json body, got %T (%v)", p, p)
	}

	if m, ok := i.builtinHTTPOptions([]Value{}).(map[string]Value); !ok || m["error"] == nil {
		t.Fatalf("expected httpOptions arg error, got %v", m)
	}
	o := i.builtinHTTPOptions([]Value{ts.URL})
	om, ok := o.(map[string]Value)
	if !ok || om["status"] != http.StatusNoContent || !strings.Contains(om["allow"].(string), "GET") {
		t.Fatalf("expected httpOptions success with allow header, got %T (%v)", o, o)
	}
	if methods, ok := om["methods"].([]Value); !ok || len(methods) < 2 {
		t.Fatalf("expected parsed methods array, got %v", om)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open local listener: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	if m, ok := i.builtinPing([]Value{}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected ping arg error, got %v", m)
	}
	ping := i.builtinPing([]Value{"127.0.0.1", int64(port), int64(1)})
	pingMap, ok := ping.(map[string]Value)
	if !ok || pingMap["success"] != true || pingMap["port"] != port {
		t.Fatalf("expected ping success to local listener, got %T (%v)", ping, ping)
	}

	if m, ok := i.builtinPortCheck([]Value{}).(map[string]Value); !ok || m["error"] == nil {
		t.Fatalf("expected portCheck arg error, got %v", m)
	}
	pc := i.builtinPortCheck([]Value{"127.0.0.1", int64(port), int64(1)})
	pcm, ok := pc.(map[string]Value)
	if !ok || pcm["open"] != true || pcm["port"] != port {
		t.Fatalf("expected portCheck open true, got %T (%v)", pc, pc)
	}

	if got := i.builtinPortScan([]Value{}); len(got.([]Value)) != 0 {
		t.Fatalf("expected empty portScan for no args, got %v", got)
	}
	scan := i.builtinPortScan([]Value{"127.0.0.1", int64(port), int64(port)})
	arr, ok := scan.([]Value)
	if !ok || len(arr) != 1 || arr[0] != int64(port) {
		t.Fatalf("expected portScan to find local open port, got %T (%v)", scan, scan)
	}
}
