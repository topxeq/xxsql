package xxscript

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch32_ChownErrorPaths(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinChown([]Value{}).(map[string]Value); m["error"] != "chown requires path, uid, gid" {
		t.Fatalf("expected chown arg error, got %v", m)
	}

	if m := i.builtinChown([]Value{123, 1, 1}).(map[string]Value); m["error"] != "path must be a string" {
		t.Fatalf("expected chown path type error, got %v", m)
	}

	baseDir := t.TempDir()
	i.ctx.BaseDir = baseDir
	m := i.builtinChown([]Value{"missing-file", 1000.0, 1000.0}).(map[string]Value)
	if m["success"] != false {
		t.Fatalf("expected chown failure for missing file, got %v", m)
	}
	if m["error"] == nil || m["error"] == "" {
		t.Fatalf("expected chown filesystem error, got %v", m)
	}

	absMissing := filepath.Join(baseDir, "missing-abs")
	m = i.builtinChown([]Value{absMissing, 1000, 1000}).(map[string]Value)
	if m["success"] != false {
		t.Fatalf("expected chown failure for missing absolute file, got %v", m)
	}
}

func TestBuiltin_ZeroCoverage_Batch32_ExitSubprocess(t *testing.T) {
	if os.Getenv("XXSCRIPT_EXIT_HELPER") == "1" {
		i := NewInterpreter(NewContext())
		if os.Getenv("XXSCRIPT_EXIT_WITH_CODE") == "1" {
			i.builtinExit([]Value{3})
			return
		}
		i.builtinExit([]Value{})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestBuiltin_ZeroCoverage_Batch32_ExitSubprocess")
	cmd.Env = append(os.Environ(), "XXSCRIPT_EXIT_HELPER=1", "XXSCRIPT_EXIT_WITH_CODE=1")
	err := cmd.Run()
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected non-zero exit error, got %v", err)
	}
	if exitErr.ExitCode() != 3 {
		t.Fatalf("expected exit code 3, got %d", exitErr.ExitCode())
	}

	cmd = exec.Command(os.Args[0], "-test.run=TestBuiltin_ZeroCoverage_Batch32_ExitSubprocess")
	cmd.Env = append(os.Environ(), "XXSCRIPT_EXIT_HELPER=1")
	if err := cmd.Run(); err != nil {
		t.Fatalf("expected zero exit for no-arg exit, got %v", err)
	}
}
