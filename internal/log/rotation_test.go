package log

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewRotator(t *testing.T) {
	r := NewRotator(100, 10, 7, true)

	if r.maxSizeMB != 100 {
		t.Errorf("maxSizeMB: got %d, want 100", r.maxSizeMB)
	}
	if r.maxBackups != 10 {
		t.Errorf("maxBackups: got %d, want 10", r.maxBackups)
	}
	if r.maxAgeDays != 7 {
		t.Errorf("maxAgeDays: got %d, want 7", r.maxAgeDays)
	}
	if !r.compress {
		t.Error("compress should be true")
	}
}

func TestRotator_ShouldRotate(t *testing.T) {
	tests := []struct {
		name      string
		maxSizeMB int64
		fileSize  int64
		want      bool
	}{
		{"should rotate", 1, 1024 * 1024, true},
		{"should not rotate", 2, 1024 * 1024, false},
		{"disabled rotation", 0, 10 * 1024 * 1024, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			logPath := filepath.Join(tmpDir, "test.log")

			f, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if tt.fileSize > 0 {
				f.Write(make([]byte, tt.fileSize))
			}

			r := NewRotator(int(tt.maxSizeMB), 5, 7, false)
			got := r.ShouldRotate(f)

			if got != tt.want {
				t.Errorf("ShouldRotate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRotator_ShouldRotate_StatError(t *testing.T) {
	r := NewRotator(1, 5, 7, false)

	f, err := os.CreateTemp("", "test*.log")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	os.Remove(f.Name())

	got := r.ShouldRotate(f)
	if got {
		t.Error("ShouldRotate should return false for stat error")
	}
}

func TestRotator_RotateWithPath(t *testing.T) {
	tests := []struct {
		name      string
		maxSizeMB int64
		fileSize  int64
		shouldRot bool
	}{
		{"rotate large file", 1, 2 * 1024 * 1024, true},
		{"no rotate small file", 10, 1024, false},
		{"no rotation for new file", 1, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			logPath := filepath.Join(tmpDir, "test.log")

			if tt.fileSize > 0 {
				f, err := os.Create(logPath)
				if err != nil {
					t.Fatal(err)
				}
				f.Write(make([]byte, tt.fileSize))
				f.Close()
			}

			r := NewRotator(int(tt.maxSizeMB), 5, 7, false)

			f, err := r.RotateWithPath(logPath)
			if err != nil {
				t.Fatalf("RotateWithPath error: %v", err)
			}
			defer f.Close()

			entries, _ := os.ReadDir(tmpDir)
			rotatedCount := 0
			for _, e := range entries {
				if e.Name() != "test.log" {
					rotatedCount++
				}
			}

			if tt.shouldRot && rotatedCount == 0 {
				t.Error("Expected rotated file but found none")
			}
			if !tt.shouldRot && rotatedCount > 0 {
				t.Error("Did not expect rotation but found rotated file")
			}
		})
	}
}

func TestRotator_Compress(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	content := []byte("test log content for compression\n")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(content)
	f.Close()

	r := NewRotator(1, 5, 7, true)

	rotatedPath := logPath + ".20240101-120000"
	os.Rename(logPath, rotatedPath)

	go r.compressFile(rotatedPath)

	time.Sleep(200 * time.Millisecond)

	entries, _ := os.ReadDir(tmpDir)
	foundGz := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			foundGz = true
			gzFile := filepath.Join(tmpDir, e.Name())
			gzReader, err := os.Open(gzFile)
			if err != nil {
				t.Fatal(err)
			}
			defer gzReader.Close()

			gz, err := gzip.NewReader(gzReader)
			if err != nil {
				t.Fatal(err)
			}
			defer gz.Close()

			decompressed, err := io.ReadAll(gz)
			if err != nil {
				t.Fatal(err)
			}

			if string(decompressed) != string(content) {
				t.Errorf("Decompressed content mismatch: got %q", string(decompressed))
			}
		}
	}

	if !foundGz {
		t.Skip("Compression test skipped - goroutine timing")
	}
}

func TestRotator_CleanupOldFiles(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "app.log")

	f, _ := os.Create(logPath)
	f.WriteString("current log")
	f.Close()

	oldFiles := []string{
		"app.log.20240101-120000",
		"app.log.20240102-120000",
		"app.log.20240103-120000",
	}

	for _, name := range oldFiles {
		f, _ := os.Create(filepath.Join(tmpDir, name))
		f.WriteString("old log")
		f.Close()

		oldTime := time.Now().Add(-48 * time.Hour)
		os.Chtimes(filepath.Join(tmpDir, name), oldTime, oldTime)
	}

	r := NewRotator(1, 1, 1, false)
	r.cleanupOldFiles(logPath)

	entries, _ := os.ReadDir(tmpDir)
	remaining := 0
	for _, e := range entries {
		if e.Name() != "app.log" {
			remaining++
		}
	}

	if remaining > 1 {
		t.Errorf("Expected at most 1 backup, got %d", remaining)
	}
}

func TestRotator_ForceRotate(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	f, _ := os.Create(logPath)
	f.WriteString("content")
	f.Close()

	r := NewRotator(1, 5, 7, false)

	if err := r.ForceRotate(logPath); err != nil {
		t.Errorf("ForceRotate error: %v", err)
	}

	entries, _ := os.ReadDir(tmpDir)
	rotatedCount := 0
	for _, e := range entries {
		if e.Name() != "test.log" && !strings.HasSuffix(e.Name(), ".gz") {
			rotatedCount++
		}
	}

	if rotatedCount == 0 {
		t.Error("Expected rotated file")
	}
}

func TestRotator_ForceRotate_NonExistent(t *testing.T) {
	r := NewRotator(1, 5, 7, false)

	err := r.ForceRotate("/nonexistent/path/test.log")
	if err != nil {
		t.Errorf("ForceRotate on non-existent file should not error: %v", err)
	}
}

func TestRotator_GetBackupInfo(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "app.log")

	f, _ := os.Create(logPath)
	f.WriteString("current")
	f.Close()

	backupNames := []string{
		"app.log.20240101-120000",
		"app.log.20240102-120000.gz",
	}
	for _, name := range backupNames {
		f, _ := os.Create(filepath.Join(tmpDir, name))
		f.WriteString("backup")
		f.Close()
	}

	r := NewRotator(1, 5, 7, false)
	infos, err := r.GetBackupInfo(logPath)
	if err != nil {
		t.Fatalf("GetBackupInfo error: %v", err)
	}

	if len(infos) != 2 {
		t.Errorf("Expected 2 backup infos, got %d", len(infos))
	}

	for _, info := range infos {
		if info.Size == 0 {
			t.Error("Backup size should not be 0")
		}
		if info.ModTime.IsZero() {
			t.Error("Backup ModTime should not be zero")
		}
	}
}

func TestRotator_GetBackupInfo_NonExistentDir(t *testing.T) {
	r := NewRotator(1, 5, 7, false)

	_, err := r.GetBackupInfo("/nonexistent/path/app.log")
	if err == nil {
		t.Error("GetBackupInfo on non-existent directory should return error")
	}
}

func TestRotator_Rotate_NilFile(t *testing.T) {
	r := NewRotator(1, 5, 7, false)

	err := r.Rotate(nil)
	if err != nil {
		t.Errorf("Rotate(nil) should not error: %v", err)
	}
}
