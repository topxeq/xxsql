package xxscript

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch27_WebhookAndImageHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinSendWebhook([]Value{}).(map[string]Value); m["success"] != false || m["error"] != "need url and payload" {
		t.Fatalf("expected sendWebhook arg error, got %v", m)
	}
	if m := i.builtinSendWebhook([]Value{123, "p"}).(map[string]Value); m["error"] != "url must be string" {
		t.Fatalf("expected sendWebhook url type error, got %v", m)
	}
	if m := i.builtinSendWebhook([]Value{"://bad-url", "p"}).(map[string]Value); m["success"] != false {
		t.Fatalf("expected sendWebhook request build error, got %v", m)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %s", r.Method)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	okResp := i.builtinSendWebhook([]Value{ts.URL, map[string]Value{"a": 1}}).(map[string]Value)
	if okResp["success"] != true || okResp["statusCode"] != 201 || okResp["response"] != "ok" {
		t.Fatalf("expected sendWebhook success payload, got %v", okResp)
	}

	if m := i.builtinImageInfo([]Value{}).(map[string]Value); m["error"] != "need image path or base64" {
		t.Fatalf("expected imageInfo arg error, got %v", m)
	}
	if m := i.builtinImageInfo([]Value{123}).(map[string]Value); m["error"] != "invalid input" {
		t.Fatalf("expected imageInfo invalid-input error, got %v", m)
	}
	if m := i.builtinImageInfo([]Value{"not_base64_@@"}).(map[string]Value); m["error"] != "invalid input" {
		t.Fatalf("expected imageInfo base64 error branch, got %v", m)
	}

	pngData := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x01, 0x02, 0x03}
	imgPath := filepath.Join(t.TempDir(), "a.png")
	if err := os.WriteFile(imgPath, pngData, 0644); err != nil {
		t.Fatalf("write temp image failed: %v", err)
	}

	infoFromPath := i.builtinImageInfo([]Value{imgPath}).(map[string]Value)
	if infoFromPath["type"] != "png" || infoFromPath["size"] != len(pngData) {
		t.Fatalf("expected imageInfo file path detection, got %v", infoFromPath)
	}

	b64Raw := base64.StdEncoding.EncodeToString(pngData)
	infoFromRaw := i.builtinImageInfo([]Value{b64Raw}).(map[string]Value)
	if infoFromRaw["type"] != "png" || infoFromRaw["size"] != len(pngData) {
		t.Fatalf("expected imageInfo raw base64 detection, got %v", infoFromRaw)
	}
	infoFromDataURL := i.builtinImageInfo([]Value{"data:image/png;base64," + b64Raw}).(map[string]Value)
	if infoFromDataURL["type"] != "png" {
		t.Fatalf("expected imageInfo data URL detection, got %v", infoFromDataURL)
	}

	if v := i.builtinImageToBase64([]Value{}); v != "" {
		t.Fatalf("expected imageToBase64 empty for no args, got %v", v)
	}
	if v := i.builtinImageToBase64([]Value{123}); v != "" {
		t.Fatalf("expected imageToBase64 empty for non-string path, got %v", v)
	}
	if v := i.builtinImageToBase64([]Value{"/no/such/file.png"}); v != "" {
		t.Fatalf("expected imageToBase64 empty for missing file, got %v", v)
	}
	uri, ok := i.builtinImageToBase64([]Value{imgPath}).(string)
	if !ok || !strings.HasPrefix(uri, "data:image/png;base64,") {
		t.Fatalf("expected imageToBase64 data URL string, got %T %v", i.builtinImageToBase64([]Value{imgPath}), uri)
	}

	if m := i.builtinBase64ToImage([]Value{}).(map[string]Value); m["error"] != "need base64 and filepath" {
		t.Fatalf("expected base64ToImage arg error, got %v", m)
	}
	if m := i.builtinBase64ToImage([]Value{123, "x"}).(map[string]Value); m["error"] != "base64 must be string" {
		t.Fatalf("expected base64ToImage type error for b64, got %v", m)
	}
	if m := i.builtinBase64ToImage([]Value{"abc", 123}).(map[string]Value); m["error"] != "filepath must be string" {
		t.Fatalf("expected base64ToImage type error for filepath, got %v", m)
	}
	if m := i.builtinBase64ToImage([]Value{"%%%", filepath.Join(t.TempDir(), "bad.bin")}).(map[string]Value); m["error"] != "invalid base64" {
		t.Fatalf("expected base64ToImage invalid base64 error, got %v", m)
	}

	outPath := filepath.Join(t.TempDir(), "out.png")
	conv := i.builtinBase64ToImage([]Value{uri, outPath}).(map[string]Value)
	if conv["success"] != true || conv["path"] != outPath || conv["size"] != len(pngData) {
		t.Fatalf("expected base64ToImage success payload, got %v", conv)
	}
	got, err := os.ReadFile(outPath)
	if err != nil || string(got) != string(pngData) {
		t.Fatalf("expected written image bytes to match input, err=%v", err)
	}
}
