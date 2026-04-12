package xxscript

import (
	"encoding/base64"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch9_FileAndPathOps(t *testing.T) {
	tmp := t.TempDir()
	ctx := NewContext()
	ctx.BaseDir = tmp
	i := NewInterpreter(ctx)

	if m, ok := i.builtinFileSave([]Value{"only_path"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected fileSave arg error, got %v", m)
	}
	if m, ok := i.builtinFileSave([]Value{123, "x"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected fileSave type error, got %v", m)
	}

	save := i.builtinFileSave([]Value{"sub/a.txt", "hello"})
	if m, ok := save.(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileSave success, got %v", save)
	}

	read := i.builtinFileRead([]Value{"sub/a.txt"})
	rm, ok := read.(map[string]Value)
	if !ok || rm["success"] != true || rm["data"] != "hello" {
		t.Fatalf("expected fileRead hello, got %T (%v)", read, read)
	}

	b64 := base64.StdEncoding.EncodeToString([]byte{1, 2, 3})
	if m, ok := i.builtinFileSave([]Value{"sub/b.bin", b64, "binary"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected binary fileSave success, got %v", m)
	}
	readBin := i.builtinFileRead([]Value{"sub/b.bin", "binary"})
	rb, ok := readBin.(map[string]Value)
	if !ok || rb["success"] != true || rb["data"] != b64 {
		t.Fatalf("expected binary fileRead base64 roundtrip, got %T (%v)", readBin, readBin)
	}

	if got := i.builtinFileExists([]Value{"sub/a.txt"}); got != true {
		t.Fatalf("expected fileExists true, got %v", got)
	}
	if got := i.builtinFileExists([]Value{"sub/missing.txt"}); got != false {
		t.Fatalf("expected fileExists false, got %v", got)
	}

	if m, ok := i.builtinFileAppend([]Value{"sub/a.txt", "!"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileAppend success, got %v", m)
	}
	r2 := i.builtinFileRead([]Value{"sub/a.txt"}).(map[string]Value)
	if r2["data"] != "hello!" {
		t.Fatalf("expected appended content hello!, got %v", r2["data"])
	}

	if m, ok := i.builtinFileTouch([]Value{"touched.txt"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileTouch success, got %v", m)
	}
	if m, ok := i.builtinFileDelete([]Value{"touched.txt"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileDelete success, got %v", m)
	}

	if m, ok := i.builtinDirCreate([]Value{"dir1/sub"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected dirCreate success, got %v", m)
	}
	if got := i.builtinDirExists([]Value{"dir1"}); got != true {
		t.Fatalf("expected dirExists true, got %v", got)
	}
	if got := i.builtinFileIsDir([]Value{"dir1"}); got != true {
		t.Fatalf("expected fileIsDir true for directory, got %v", got)
	}
	listed := i.builtinDirList([]Value{"dir1"})
	if arr, ok := listed.([]Value); !ok || len(arr) == 0 {
		t.Fatalf("expected non-empty dirList, got %T (%v)", listed, listed)
	}

	if m, ok := i.builtinFileSave([]Value{"dir1/sub/x.txt", "x"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileSave inside dir1 success, got %v", m)
	}
	if m, ok := i.builtinDirDelete([]Value{"dir1"}).(map[string]Value); !ok || m["success"] != false {
		t.Fatalf("expected non-recursive dirDelete failure on non-empty dir, got %v", m)
	}
	if m, ok := i.builtinDirDelete([]Value{"dir1", true}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected recursive dirDelete success, got %v", m)
	}

	if got := i.builtinPathJoin([]Value{"a", "b", "c.txt"}); !strings.Contains(got.(string), filepath.Join("a", "b", "c.txt")) {
		t.Fatalf("expected pathJoin to contain joined path, got %v", got)
	}
	if got := i.builtinPathBase([]Value{"/tmp/a.txt"}); got != "a.txt" {
		t.Fatalf("expected pathBase a.txt, got %v", got)
	}
	if got := i.builtinPathDir([]Value{"/tmp/a.txt"}); got != "/tmp" {
		t.Fatalf("expected pathDir /tmp, got %v", got)
	}
	if got := i.builtinPathExt([]Value{"a.tar.gz"}); got != ".gz" {
		t.Fatalf("expected pathExt .gz, got %v", got)
	}
	if got := i.builtinPathClean([]Value{"a/../b/./c"}); got != filepath.Clean("a/../b/./c") {
		t.Fatalf("expected cleaned path, got %v", got)
	}
	if got := i.builtinPathAbs([]Value{"sub/a.txt"}); !filepath.IsAbs(got.(string)) {
		t.Fatalf("expected absolute path, got %v", got)
	}
	split := i.builtinPathSplit([]Value{"p/q.txt"}).(map[string]Value)
	if split["file"] != "q.txt" {
		t.Fatalf("expected pathSplit file q.txt, got %v", split)
	}
	if got := i.builtinPathIsAbs([]Value{"/tmp"}); got != true {
		t.Fatalf("expected pathIsAbs true, got %v", got)
	}

	if m, ok := i.builtinFileInfo([]Value{"sub/a.txt"}).(map[string]Value); !ok || m["success"] != true || m["name"] != "a.txt" {
		t.Fatalf("expected fileInfo success, got %v", m)
	}
	if got := i.builtinFileSize([]Value{"sub/a.txt"}); got == int64(-1) {
		t.Fatalf("expected non-negative file size, got %v", got)
	}
	if got := i.builtinFileModTime([]Value{"sub/a.txt"}); got == "" {
		t.Fatalf("expected non-empty file mod time")
	}

	if m, ok := i.builtinFileCopy([]Value{"sub/a.txt", "copied/a2.txt"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileCopy success, got %v", m)
	}
	if m, ok := i.builtinFileMove([]Value{"copied/a2.txt", "moved/a3.txt"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileMove success, got %v", m)
	}
	if got := i.builtinFileExists([]Value{"moved/a3.txt"}); got != true {
		t.Fatalf("expected moved file to exist, got %v", got)
	}

	walked := i.builtinFileWalk([]Value{"."})
	if arr, ok := walked.([]Value); !ok || len(arr) == 0 {
		t.Fatalf("expected non-empty fileWalk result, got %T (%v)", walked, walked)
	}
	globbed := i.builtinFileGlob([]Value{"**/*.txt"})
	if arr, ok := globbed.([]Value); !ok || len(arr) == 0 {
		t.Fatalf("expected non-empty fileGlob result, got %T (%v)", globbed, globbed)
	}

	if m, ok := i.builtinDirCreate([]Value{"src/sub"}).(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected dirCreate for dirCopy src success, got %v", m)
	}
	_ = i.builtinFileSave([]Value{"src/sub/f.txt", "copyme"})
	dc := i.builtinDirCopy([]Value{"src", "dst"})
	dcm, ok := dc.(map[string]Value)
	if !ok || dcm["success"] != true {
		t.Fatalf("expected dirCopy success, got %T (%v)", dc, dc)
	}
	if got := i.builtinFileExists([]Value{"dst/sub/f.txt"}); got != true {
		t.Fatalf("expected copied nested file to exist, got %v", got)
	}

	rec := httptest.NewRecorder()
	i.ctx.HTTPWriter = rec
	_ = i.builtinFileSave([]Value{"served.txt", "serve"})
	serve := i.builtinFileServe([]Value{"served.txt"})
	if m, ok := serve.(map[string]Value); !ok || m["success"] != true {
		t.Fatalf("expected fileServe success, got %v", serve)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected text/plain content type, got %q", ct)
	}
}
