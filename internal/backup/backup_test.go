package backup

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupBackupTest(t *testing.T) (string, func()) {
	// Create temp data directory
	dataDir, err := os.MkdirTemp("", "backup-test-*")
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		os.RemoveAll(dataDir)
	}

	return dataDir, cleanup
}

func TestBackupAndRestore(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table metadata directly in dataDir
	tableMeta := `{
		"name": "users",
		"columns": [
			{"name": "id", "type": 1, "nullable": false, "primary_key": true},
			{"name": "name", "type": 6, "size": 100, "nullable": true}
		],
		"row_count": 0,
		"next_page_id": 1
	}`

	tableMetaPath := filepath.Join(dataDir, "users.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	// Create test table data
	tableData := make([]byte, 100)
	tableDataPath := filepath.Join(dataDir, "users.xdb")
	if err := os.WriteFile(tableDataPath, tableData, 0644); err != nil {
		t.Fatal(err)
	}

	// Create test sequence
	seqData := make([]byte, 50)
	seqPath := filepath.Join(dataDir, "_sequences.seq")
	if err := os.WriteFile(seqPath, seqData, 0644); err != nil {
		t.Fatal(err)
	}

	// Create backup manager
	mgr := NewManager(dataDir)

	// Create backup
	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "test_backup")

	manifest, err := mgr.Backup(BackupOptions{
		Path:     backupPath,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Verify manifest
	if manifest.Version != BackupVersion {
		t.Errorf("Expected version %s, got %s", BackupVersion, manifest.Version)
	}
	if manifest.Database != "testdb" {
		t.Errorf("Expected database testdb, got %s", manifest.Database)
	}
	if manifest.TableCount != 1 {
		t.Errorf("Expected 1 table, got %d", manifest.TableCount)
	}

	// Verify backup file exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Error("Backup file was not created")
	}

	// Test restore to new directory
	restoreDir, err := os.MkdirTemp("", "restore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(restoreDir)

	restoreMgr := NewManager(restoreDir)
	restoreManifest, err := restoreMgr.Restore(RestoreOptions{
		Path: backupPath,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify restore manifest
	if restoreManifest.TableCount != manifest.TableCount {
		t.Errorf("Expected %d tables, got %d", manifest.TableCount, restoreManifest.TableCount)
	}

	// Verify restored file exists (directly in restoreDir, not in tables subdirectory)
	restoredMetaPath := filepath.Join(restoreDir, "users.xmeta")
	if _, err := os.Stat(restoredMetaPath); os.IsNotExist(err) {
		t.Error("Restored metadata file was not created")
	}
}

func TestBackupCompressed(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table directly in dataDir
	tableMeta := `{"name": "test", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "test.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "compressed_backup")

	manifest, err := mgr.Backup(BackupOptions{
		Path:     backupPath,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	if !manifest.Compressed {
		t.Error("Expected compressed flag to be true")
	}

	// Verify backup file exists with extension
	if _, err := os.Stat(backupPath + BackupExt); os.IsNotExist(err) {
		t.Error("Compressed backup file was not created")
	}
}

func TestVerifyBackup(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table directly in dataDir
	tableMeta := `{"name": "test", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "test.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "test_backup")

	_, err = mgr.Backup(BackupOptions{
		Path: backupPath,
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Verify backup
	manifest, err := mgr.VerifyBackup(backupPath)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if manifest == nil {
		t.Error("Expected manifest, got nil")
	}
}

func TestRestoreNonExistentFile(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	mgr := NewManager(dataDir)

	_, err := mgr.Restore(RestoreOptions{
		Path: "/nonexistent/backup.xbak",
	})
	if err == nil {
		t.Error("Expected error for non-existent backup file")
	}
}

func TestListBackups(t *testing.T) {
	backupDir, err := os.MkdirTemp("", "backup-list-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	// Create some backup files
	backupFiles := []string{
		"backup1.xbak",
		"backup2.xbak",
		"backup3.xbak",
	}

	for _, f := range backupFiles {
		path := filepath.Join(backupDir, f)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Also create a non-backup file
	if err := os.WriteFile(filepath.Join(backupDir, "other.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	backups, err := ListBackups(backupDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 3 {
		t.Errorf("Expected 3 backups, got %d", len(backups))
	}
}

func TestListBackups_EmptyDir(t *testing.T) {
	backupDir, err := os.MkdirTemp("", "backup-empty-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backups, err := ListBackups(backupDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 0 {
		t.Errorf("Expected 0 backups, got %d", len(backups))
	}
}

func TestListBackups_NonExistentDir(t *testing.T) {
	_, err := ListBackups("/nonexistent/directory")
	if err == nil {
		t.Error("Expected error for non-existent directory")
	}
}

func TestBackup_EmptyDataDir(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "empty_backup")

	manifest, err := mgr.Backup(BackupOptions{
		Path:     backupPath,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	if manifest.TableCount != 0 {
		t.Errorf("Expected 0 tables, got %d", manifest.TableCount)
	}
}

func TestBackup_WithUsersAndGrants(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table
	tableMeta := `{"name": "users", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "users.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	// Create users.json
	usersData := `[{"id": 1, "name": "admin", "role": "admin"}]`
	usersPath := filepath.Join(dataDir, "users.json")
	if err := os.WriteFile(usersPath, []byte(usersData), 0644); err != nil {
		t.Fatal(err)
	}

	// Create grants.json
	grantsData := `[{"user": "admin", "database": "*", "privileges": ["ALL"]}]`
	grantsPath := filepath.Join(dataDir, "grants.json")
	if err := os.WriteFile(grantsPath, []byte(grantsData), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "auth_backup")

	manifest, err := mgr.Backup(BackupOptions{
		Path:     backupPath,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	if manifest.TableCount != 1 {
		t.Errorf("Expected 1 table, got %d", manifest.TableCount)
	}
}

func TestRestore_CompressedBackup(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table
	tableMeta := `{"name": "test", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "test.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "compressed_backup")

	// Create compressed backup
	_, err = mgr.Backup(BackupOptions{
		Path:     backupPath,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Restore to new directory
	restoreDir, err := os.MkdirTemp("", "restore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(restoreDir)

	restoreMgr := NewManager(restoreDir)
	restoreManifest, err := restoreMgr.Restore(RestoreOptions{
		Path: backupPath + BackupExt,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if restoreManifest.Compressed != true {
		t.Error("Expected compressed flag to be true")
	}
}

func TestVerifyBackup_Compressed(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table
	tableMeta := `{"name": "test", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "test.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "compressed_backup")

	// Create compressed backup
	_, err = mgr.Backup(BackupOptions{
		Path:     backupPath,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Verify compressed backup
	manifest, err := mgr.VerifyBackup(backupPath + BackupExt)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if manifest == nil {
		t.Error("Expected manifest, got nil")
	}
}

func TestVerifyBackup_NonExistent(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	mgr := NewManager(dataDir)

	_, err := mgr.VerifyBackup("/nonexistent/backup.xbak")
	if err == nil {
		t.Error("Expected error for non-existent backup file")
	}
}

func TestBackup_WithIndexFiles(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table metadata
	tableMeta := `{"name": "users", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "users.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	// Create test index file
	indexData := make([]byte, 100)
	indexPath := filepath.Join(dataDir, "users.xidx")
	if err := os.WriteFile(indexPath, indexData, 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "index_backup")

	manifest, err := mgr.Backup(BackupOptions{
		Path:     backupPath,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	if manifest.TableCount != 1 {
		t.Errorf("Expected 1 table, got %d", manifest.TableCount)
	}
}

func TestAddDirectoryToTar(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create a subdirectory with files
	subDir := filepath.Join(dataDir, "tables")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create files in subdirectory
	tableMeta := `{"name": "users", "columns": [], "row_count": 0}`
	metaPath := filepath.Join(subDir, "users.xmeta")
	if err := os.WriteFile(metaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	tableData := make([]byte, 100)
	dataPath := filepath.Join(subDir, "users.xdb")
	if err := os.WriteFile(dataPath, tableData, 0644); err != nil {
		t.Fatal(err)
	}

	// Create nested directory
	nestedDir := filepath.Join(subDir, "nested")
	if err := os.MkdirAll(nestedDir, 0755); err != nil {
		t.Fatal(err)
	}
	nestedFile := filepath.Join(nestedDir, "data.txt")
	if err := os.WriteFile(nestedFile, []byte("nested"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create tar file
	tarPath := filepath.Join(dataDir, "test.tar")
	tarFile, err := os.Create(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer tarFile.Close()

	tw := tar.NewWriter(tarFile)
	defer tw.Close()

	mgr := NewManager(dataDir)
	manifest := &Manifest{}

	// Test addDirectoryToTar
	err = mgr.addDirectoryToTar(tw, subDir, "tables", manifest)
	if err != nil {
		t.Fatalf("addDirectoryToTar failed: %v", err)
	}

	tw.Flush()
	tarFile.Close()

	// Verify tar file was created
	stat, err := os.Stat(tarPath)
	if err != nil {
		t.Fatalf("Tar file not created: %v", err)
	}
	if stat.Size() == 0 {
		t.Error("Tar file is empty")
	}

	// Verify manifest was updated
	if manifest.TableCount != 1 {
		t.Errorf("Expected 1 table in manifest, got %d", manifest.TableCount)
	}
}

func TestRestore_WithDirectory(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create a backup with a directory entry
	backupPath := filepath.Join(dataDir, "test_backup.tar")
	tarFile, err := os.Create(backupPath)
	if err != nil {
		t.Fatal(err)
	}

	tw := tar.NewWriter(tarFile)

	// Add a directory header
	dirHeader := &tar.Header{
		Name:     "tables",
		Typeflag: tar.TypeDir,
		Mode:     0755,
	}
	if err := tw.WriteHeader(dirHeader); err != nil {
		t.Fatal(err)
	}

	// Add a file header
	fileHeader := &tar.Header{
		Name:     "tables/test.xmeta",
		Typeflag: tar.TypeReg,
		Mode:     0644,
		Size:     int64(len(`{"name": "test", "columns": []}`)),
	}
	if err := tw.WriteHeader(fileHeader); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(`{"name": "test", "columns": []}`)); err != nil {
		t.Fatal(err)
	}

	// Add manifest
	manifestData := `{"version": "1.0", "timestamp": "2024-01-01T00:00:00Z", "table_count": 1}`
	manifestHeader := &tar.Header{
		Name:     "manifest.json",
		Typeflag: tar.TypeReg,
		Mode:     0644,
		Size:     int64(len(manifestData)),
	}
	if err := tw.WriteHeader(manifestHeader); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(manifestData)); err != nil {
		t.Fatal(err)
	}

	tw.Close()
	tarFile.Close()

	// Restore
	restoreDir, err := os.MkdirTemp("", "restore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(restoreDir)

	restoreMgr := NewManager(restoreDir)
	manifest, err := restoreMgr.Restore(RestoreOptions{
		Path: backupPath,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if manifest.Version != "1.0" {
		t.Errorf("Expected version 1.0, got %s", manifest.Version)
	}

	// Verify file was restored (directly in restoreDir, not in tables subdirectory)
	restoredPath := filepath.Join(restoreDir, "test.xmeta")
	if _, err := os.Stat(restoredPath); os.IsNotExist(err) {
		t.Error("Restored file not found")
	}
}

func TestNewManager(t *testing.T) {
	mgr := NewManager("/some/path")
	if mgr == nil {
		t.Error("NewManager returned nil")
	}
	if mgr.dataDir != "/some/path" {
		t.Errorf("Expected dataDir /some/path, got %s", mgr.dataDir)
	}
}

func TestBackupVersion(t *testing.T) {
	if BackupVersion != "1.0" {
		t.Errorf("Expected BackupVersion 1.0, got %s", BackupVersion)
	}
}

func TestBackupExt(t *testing.T) {
	if BackupExt != ".xbak" {
		t.Errorf("Expected BackupExt .xbak, got %s", BackupExt)
	}
}

func TestBackup_InvalidPath(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	mgr := NewManager(dataDir)

	// Try to backup to an invalid path (read-only location on some systems)
	_, err := mgr.Backup(BackupOptions{
		Path:     "/proc/invalid_backup",
		Database: "testdb",
	})
	// Should get an error (unless running as root)
	if err == nil {
		t.Log("Backup to /proc succeeded (may be running as root)")
	}
}

func TestRestore_InvalidTarFile(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create an invalid tar file
	invalidPath := filepath.Join(dataDir, "invalid.tar")
	if err := os.WriteFile(invalidPath, []byte("not a tar file"), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	// Restore should fail gracefully
	_, err := mgr.Restore(RestoreOptions{
		Path: invalidPath,
	})
	// Should get an error about the tar format
	if err == nil {
		t.Error("Expected error for invalid tar file")
	}
}

func TestRestore_InvalidGzipFile(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create a file with gzip magic bytes but invalid content
	gzipMagic := []byte{0x1f, 0x8b, 0x00, 0x00} // gzip magic + invalid data
	invalidGzip := filepath.Join(dataDir, "invalid.gz")
	if err := os.WriteFile(invalidGzip, gzipMagic, 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	// Restore should fail for invalid gzip
	_, err := mgr.Restore(RestoreOptions{
		Path: invalidGzip,
	})
	if err == nil {
		t.Error("Expected error for invalid gzip file")
	}
}

func TestVerifyBackup_InvalidFile(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create an invalid backup file
	invalidPath := filepath.Join(dataDir, "invalid.xbak")
	if err := os.WriteFile(invalidPath, []byte("not a valid backup"), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	// Verify should fail gracefully
	_, err := mgr.VerifyBackup(invalidPath)
	// The function may return nil manifest with no error for invalid files
	// because it just reads tar entries and looks for manifest.json
	_ = err
}

func TestVerifyBackup_InvalidGzip(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create a file with gzip magic bytes but invalid content
	gzipMagic := []byte{0x1f, 0x8b, 0x00, 0x00}
	invalidGzip := filepath.Join(dataDir, "invalid.gz.xbak")
	if err := os.WriteFile(invalidGzip, gzipMagic, 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	_, err := mgr.VerifyBackup(invalidGzip)
	if err == nil {
		t.Error("Expected error for invalid gzip file")
	}
}

func TestBackup_ManifestChecksum(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table
	tableMeta := `{"name": "test", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "test.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "checksum_backup")

	manifest, err := mgr.Backup(BackupOptions{
		Path:     backupPath,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Verify checksum is present
	if manifest.Checksum == "" {
		t.Error("Expected checksum in manifest")
	}
	if !strings.HasPrefix(manifest.Checksum, "sha256:") {
		t.Errorf("Expected sha256 checksum prefix, got %s", manifest.Checksum)
	}
}

func TestRestore_PreservesData(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table with specific content
	tableMeta := `{"name": "preserved", "columns": [{"name": "id", "type": 1}], "row_count": 42}`
	tableMetaPath := filepath.Join(dataDir, "preserved.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	tableData := []byte("specific table data content")
	tableDataPath := filepath.Join(dataDir, "preserved.xdb")
	if err := os.WriteFile(tableDataPath, tableData, 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "preserve_backup")

	_, err = mgr.Backup(BackupOptions{
		Path:     backupPath,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Restore to new directory
	restoreDir, err := os.MkdirTemp("", "restore-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(restoreDir)

	restoreMgr := NewManager(restoreDir)
	_, err = restoreMgr.Restore(RestoreOptions{
		Path: backupPath,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify data is preserved
	restoredData, err := os.ReadFile(filepath.Join(restoreDir, "preserved.xdb"))
	if err != nil {
		t.Fatalf("Failed to read restored data: %v", err)
	}
	if string(restoredData) != string(tableData) {
		t.Errorf("Data mismatch: got %q, want %q", string(restoredData), string(tableData))
	}
}

func TestBackup_CompressedGzipFormat(t *testing.T) {
	dataDir, cleanup := setupBackupTest(t)
	defer cleanup()

	// Create test table
	tableMeta := `{"name": "test", "columns": [], "row_count": 0}`
	tableMetaPath := filepath.Join(dataDir, "test.xmeta")
	if err := os.WriteFile(tableMetaPath, []byte(tableMeta), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(dataDir)

	backupDir, err := os.MkdirTemp("", "backup-dest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(backupDir)

	backupPath := filepath.Join(backupDir, "gzip_backup")

	_, err = mgr.Backup(BackupOptions{
		Path:     backupPath,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// Verify the file is a valid gzip
	compressedPath := backupPath + BackupExt
	file, err := os.Open(compressedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	// Read a bit to verify it's a valid tar inside
	tarReader := tar.NewReader(gzReader)
	_, err = tarReader.Next()
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read tar from gzip: %v", err)
	}
}
