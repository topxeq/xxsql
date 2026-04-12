package xxscript

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch23_GetSecretAndCronBuiltins(t *testing.T) {
	i := NewInterpreter(NewContext())

	if v := i.builtinGetSecret([]Value{}); v != "" {
		t.Fatalf("expected getSecret() empty string, got %v", v)
	}
	if v := i.builtinGetSecret([]Value{123}); v != "" {
		t.Fatalf("expected getSecret(non-string) empty string, got %v", v)
	}

	t.Setenv("XXSQL_BATCH23_SECRET", "env-secret")
	if v := i.builtinGetSecret([]Value{"XXSQL_BATCH23_SECRET"}); v != "env-secret" {
		t.Fatalf("expected getSecret env lookup, got %v", v)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir to temp dir failed: %v", err)
	}
	defer func() { _ = os.Chdir(wd) }()

	if err := os.MkdirAll(filepath.Join(tmpDir, "secrets"), 0755); err != nil {
		t.Fatalf("mkdir secrets failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "secrets", "BATCH23_FILE_SECRET"), []byte("  file-secret\n"), 0644); err != nil {
		t.Fatalf("write file secret failed: %v", err)
	}
	if v := i.builtinGetSecret([]Value{"BATCH23_FILE_SECRET"}); v != "file-secret" {
		t.Fatalf("expected getSecret file fallback, got %v", v)
	}
	if v := i.builtinGetSecret([]Value{"BATCH23_MISSING_SECRET"}); v != "" {
		t.Fatalf("expected getSecret missing key empty string, got %v", v)
	}

	if m := i.builtinCronParse([]Value{}).(map[string]Value); m["error"] != "need cron expression" {
		t.Fatalf("expected cronParse arg error, got %v", m)
	}
	if m := i.builtinCronParse([]Value{123}).(map[string]Value); m["error"] != "cron expression must be string" {
		t.Fatalf("expected cronParse type error, got %v", m)
	}
	if m := i.builtinCronParse([]Value{"* * * *"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected cronParse invalid-field-count error, got %v", m)
	}
	cp := i.builtinCronParse([]Value{"*/5 1 * * 1"}).(map[string]Value)
	if cp["valid"] != true || cp["minute"] != "*/5" || cp["hour"] != "1" {
		t.Fatalf("expected cronParse success payload, got %v", cp)
	}

	if m := i.builtinCronNext([]Value{}).(map[string]Value); m["error"] != "need cron expression" {
		t.Fatalf("expected cronNext arg error, got %v", m)
	}
	if m := i.builtinCronNext([]Value{123}).(map[string]Value); m["error"] != "cron expression must be string" {
		t.Fatalf("expected cronNext type error, got %v", m)
	}
	if m := i.builtinCronNext([]Value{"bad expr"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected cronNext parser error branch, got %v", m)
	}
	start := time.Date(2026, 2, 3, 4, 5, 0, 0, time.UTC).Format(time.RFC3339)
	cn := i.builtinCronNext([]Value{"*/10 * * * *", start}).(map[string]Value)
	if cn["valid"] != true || cn["next"] == nil || cn["from"] != start {
		t.Fatalf("expected cronNext success with provided start time, got %v", cn)
	}

	if m := i.builtinCronNextN([]Value{}).(map[string]Value); m["error"] != "need cron expression" {
		t.Fatalf("expected cronNextN arg error, got %v", m)
	}
	if m := i.builtinCronNextN([]Value{123}).(map[string]Value); m["error"] != "cron expression must be string" {
		t.Fatalf("expected cronNextN type error, got %v", m)
	}
	cnnInt := i.builtinCronNextN([]Value{"*/15 * * * *", 3, start}).(map[string]Value)
	if cnnInt["valid"] != true || cnnInt["count"] != 3 {
		t.Fatalf("expected cronNextN int count=3, got %v", cnnInt)
	}
	cnnFloat := i.builtinCronNextN([]Value{"*/20 * * * *", 2.0, start}).(map[string]Value)
	if cnnFloat["valid"] != true || cnnFloat["count"] != 2 {
		t.Fatalf("expected cronNextN float count=2, got %v", cnnFloat)
	}
	cnnInvalid := i.builtinCronNextN([]Value{"bad expr", 5, start}).(map[string]Value)
	if cnnInvalid["valid"] != true || cnnInvalid["count"] != 0 {
		t.Fatalf("expected cronNextN invalid expression to yield empty results, got %v", cnnInvalid)
	}
}
