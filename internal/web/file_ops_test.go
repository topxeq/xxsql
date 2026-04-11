package web

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
)

func TestWeb_ExtractZip(t *testing.T) {
	// Create a temp dir
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "test.zip")
	destDir := filepath.Join(tmpDir, "dest")
	os.MkdirAll(destDir, 0755)

	// Create a test zip file
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(f)
	fw, _ := w.Create("test.txt")
	fw.Write([]byte("hello"))
	fw2, _ := w.Create("sub/test2.txt")
	fw2.Write([]byte("world"))
	w.Close()
	f.Close()

	// Test extractZip
	err = extractZip(zipPath, destDir)
	if err != nil {
		t.Fatalf("extractZip error: %v", err)
	}

	// Verify files exist
	if _, err := os.Stat(filepath.Join(destDir, "test.txt")); os.IsNotExist(err) {
		t.Error("test.txt not extracted")
	}
	if _, err := os.Stat(filepath.Join(destDir, "sub", "test2.txt")); os.IsNotExist(err) {
		t.Error("sub/test2.txt not extracted")
	}
}

func TestWeb_FindProjectRoot(t *testing.T) {
	tmpDir := t.TempDir()

	// Create project.json in tmpDir
	marker := filepath.Join(tmpDir, "project.json")
	os.WriteFile(marker, []byte(`{}`), 0644)

	// Test findProjectRoot from tmpDir
	root, err := findProjectRoot(tmpDir)
	if err != nil {
		t.Fatalf("findProjectRoot error: %v", err)
	}
	if root != tmpDir {
		t.Errorf("Expected root %s, got %s", tmpDir, root)
	}

	// Test with no marker
	os.Remove(marker)
	_, err = findProjectRoot(tmpDir)
	if err == nil {
		t.Error("Expected error when no marker found")
	}
}

func TestWeb_InstallPluginFromZIP(t *testing.T) {
	// Create a temp dir
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "plugin.zip")

	// Create a test zip file
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(f)
	fw, _ := w.Create("manifest.json")
	fw.Write([]byte(`{"name": "test-plugin", "version": "1.0.0"}`))
	w.Close()
	f.Close()

	// Test installPluginFromZIP
	err = installPluginFromZIP(zipPath)
	t.Logf("installPluginFromZIP result: %v", err)
}
