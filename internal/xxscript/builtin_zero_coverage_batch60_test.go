package xxscript

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch60_UserConfigAndChmod(t *testing.T) {
	i := NewInterpreter(NewContext())

	t.Setenv("XDG_CONFIG_HOME", "/tmp/xxsql-config-home")
	if got := i.builtinUserConfig(nil); got != "/tmp/xxsql-config-home" {
		t.Fatalf("builtinUserConfig env branch: expected env value, got %v", got)
	}

	t.Setenv("XDG_CONFIG_HOME", "")
	if got := i.builtinUserConfig(nil).(string); got == "" || !strings.Contains(got, ".config") {
		t.Fatalf("builtinUserConfig home fallback branch: expected .config path, got %v", got)
	}

	ctx := NewContext()
	ctx.BaseDir = t.TempDir()
	i2 := NewInterpreter(ctx)

	if m := i2.builtinChmod([]Value{}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinChmod short-args should fail, got %v", m)
	}
	if m := i2.builtinChmod([]Value{123, "644"}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinChmod non-string path should fail, got %v", m)
	}
	if m := i2.builtinChmod([]Value{"missing.txt", "644"}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinChmod missing path should fail, got %v", m)
	}

	filePath := filepath.Join(ctx.BaseDir, "chmod.txt")
	if err := os.WriteFile(filePath, []byte("x"), 0644); err != nil {
		t.Fatalf("failed to create chmod fixture: %v", err)
	}

	if m := i2.builtinChmod([]Value{"chmod.txt", "600"}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinChmod string mode branch failed, got %v", m)
	}
	if m := i2.builtinChmod([]Value{"chmod.txt", float64(420)}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinChmod float mode branch failed, got %v", m)
	}
	if m := i2.builtinChmod([]Value{"chmod.txt", true}).(map[string]Value); m["success"] != true || m["mode"] != "0644" {
		t.Fatalf("builtinChmod default mode branch failed, got %v", m)
	}
}

func TestBuiltin_ZeroCoverage_Batch60_FileSaveAndServeExtraBranches(t *testing.T) {
	ctx := NewContext()
	ctx.BaseDir = t.TempDir()
	i := NewInterpreter(ctx)

	if m := i.builtinFileSave([]Value{"bad.bin", "%%%", "binary"}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinFileSave invalid base64 should fail, got %v", m)
	}

	if m := i.builtinFileSave([]Value{"bytes.bin", []byte{1, 2, 3}}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinFileSave []byte branch failed, got %v", m)
	}
	data1, err := os.ReadFile(filepath.Join(ctx.BaseDir, "bytes.bin"))
	if err != nil || len(data1) != 3 || data1[0] != 1 || data1[2] != 3 {
		t.Fatalf("builtinFileSave []byte branch produced unexpected file: %v %v", data1, err)
	}

	if m := i.builtinFileSave([]Value{"arr.bin", []Value{int(65), int64(66), float64(67), "x"}}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinFileSave []Value branch failed, got %v", m)
	}
	data2, err := os.ReadFile(filepath.Join(ctx.BaseDir, "arr.bin"))
	if err != nil || string(data2) != "ABC\x00" {
		t.Fatalf("builtinFileSave []Value branch produced unexpected file: %v %v", data2, err)
	}

	if m := i.builtinFileSave([]Value{"fmt.txt", map[string]Value{"k": "v"}}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinFileSave default fmt branch failed, got %v", m)
	}
	data3, err := os.ReadFile(filepath.Join(ctx.BaseDir, "fmt.txt"))
	if err != nil || !strings.Contains(string(data3), "k:v") {
		t.Fatalf("builtinFileSave default fmt branch content unexpected: %q (%v)", string(data3), err)
	}

	if m := i.builtinFileServe([]Value{}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinFileServe no-args should fail, got %v", m)
	}
	if m := i.builtinFileServe([]Value{123}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinFileServe non-string path should fail, got %v", m)
	}
	if m := i.builtinFileServe([]Value{"bytes.bin"}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinFileServe without HTTP context should fail, got %v", m)
	}

	rec := httptest.NewRecorder()
	ctx.HTTPWriter = rec
	if m := i.builtinFileServe([]Value{"missing.bin"}).(map[string]Value); m["success"] != false {
		t.Fatalf("builtinFileServe missing file should fail, got %v", m)
	}

	_ = i.builtinFileSave([]Value{"asset.css", "body{}"})
	recCSS := httptest.NewRecorder()
	ctx.HTTPWriter = recCSS
	if m := i.builtinFileServe([]Value{"asset.css"}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinFileServe css should succeed, got %v", m)
	}
	if ct := recCSS.Header().Get("Content-Type"); !strings.Contains(ct, "text/css") {
		t.Fatalf("builtinFileServe css content-type mismatch: %q", ct)
	}

	_ = i.builtinFileSave([]Value{"asset.unknown", "x"})
	recUnknown := httptest.NewRecorder()
	ctx.HTTPWriter = recUnknown
	if m := i.builtinFileServe([]Value{"asset.unknown"}).(map[string]Value); m["success"] != true {
		t.Fatalf("builtinFileServe default content type should succeed, got %v", m)
	}
	if ct := recUnknown.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("builtinFileServe default content-type mismatch: %q", ct)
	}
}
