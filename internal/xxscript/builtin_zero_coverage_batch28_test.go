package xxscript

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch28_GitHelpers(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available in PATH")
	}

	i := NewInterpreter(NewContext())

	errDir := filepath.Join(t.TempDir(), "missing")
	if m := i.builtinGitStatus([]Value{errDir}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected gitStatus error for missing dir, got %v", m)
	}
	if m := i.builtinGitLog([]Value{errDir}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected gitLog error for missing dir, got %v", m)
	}
	if m := i.builtinGitBranch([]Value{errDir}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected gitBranch error for missing dir, got %v", m)
	}

	repoDir := t.TempDir()
	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = repoDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v, out=%s", args, err, string(out))
		}
	}

	run("init")
	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("hello\n"), 0644); err != nil {
		t.Fatalf("write README failed: %v", err)
	}
	run("add", "README.md")
	run("-c", "user.name=Tester", "-c", "user.email=tester@example.com", "commit", "-m", "init")
	if err := os.WriteFile(filepath.Join(repoDir, "untracked.txt"), []byte("u\n"), 0644); err != nil {
		t.Fatalf("write untracked file failed: %v", err)
	}

	status := i.builtinGitStatus([]Value{repoDir}).(map[string]Value)
	if status["clean"] != false || status["count"] == 0 {
		t.Fatalf("expected gitStatus to report untracked changes, got %v", status)
	}

	logV := i.builtinGitLog([]Value{repoDir, 5}).(map[string]Value)
	if logV["count"] == 0 || logV["commits"] == nil {
		t.Fatalf("expected gitLog commits, got %v", logV)
	}

	branch := i.builtinGitBranch([]Value{repoDir}).(map[string]Value)
	if branch["count"] == 0 || branch["branches"] == nil || branch["current"] == "" {
		t.Fatalf("expected gitBranch branch listing, got %v", branch)
	}
}
