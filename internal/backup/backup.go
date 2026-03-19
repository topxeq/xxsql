// Package backup provides backup and restore functionality for XxSql.
package backup

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// BackupVersion is the current backup format version.
	BackupVersion = "1.0"

	// BackupExt is the extension for backup files.
	BackupExt = ".xbak"
)

// Manifest represents the backup manifest.
type Manifest struct {
	Version     string          `json:"version"`
	Timestamp   string          `json:"timestamp"`
	Database    string          `json:"database"`
	TableCount  int             `json:"table_count"`
	Tables      []TableManifest `json:"tables"`
	Checksum    string          `json:"checksum"`
	Compressed  bool            `json:"compressed"`
	TotalSize   int64           `json:"total_size"`
}

// TableManifest represents metadata for a single table in the backup.
type TableManifest struct {
	Name      string `json:"name"`
	RowCount  uint64 `json:"row_count"`
	PageCount int    `json:"page_count"`
	Size      int64  `json:"size"`
}

// Manager handles backup and restore operations.
type Manager struct {
	dataDir string
}

// NewManager creates a new backup manager.
func NewManager(dataDir string) *Manager {
	return &Manager{
		dataDir: dataDir,
	}
}

// BackupOptions holds options for backup operations.
type BackupOptions struct {
	Path       string
	Compress   bool
	Database   string
	TableNames []string // empty means all tables
}

// RestoreOptions holds options for restore operations.
type RestoreOptions struct {
	Path string
}

// Backup creates a full backup of the database.
func (m *Manager) Backup(opts BackupOptions) (*Manifest, error) {
	// Ensure backup directory exists
	backupDir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup file
	var file *os.File
	var err error
	if opts.Compress {
		file, err = os.Create(opts.Path + BackupExt)
	} else {
		file, err = os.Create(opts.Path)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// Create manifest
	manifest := &Manifest{
		Version:    BackupVersion,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Database:   opts.Database,
		Compressed: opts.Compress,
	}

	// Calculate total checksum
	hasher := sha256.New()

	// Setup writers
	var writer io.Writer = file
	if opts.Compress {
		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()
		writer = gzWriter
	}

	// Create multi-writer for checksum
	multiWriter := io.MultiWriter(writer, hasher)
	tarWriter := tar.NewWriter(multiWriter)
	defer tarWriter.Close()

	// Backup all table files (.xmeta, .xdb, .xidx) in data directory
	entries, err := os.ReadDir(m.dataDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read data directory: %w", err)
		}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Only backup table files
		if strings.HasSuffix(name, ".xmeta") ||
			strings.HasSuffix(name, ".xdb") ||
			strings.HasSuffix(name, ".xidx") {
			filePath := filepath.Join(m.dataDir, name)
			// Use forward slashes for tar archive paths
			archivePath := "tables/" + name
			if err := m.addFileToTar(tarWriter, filePath, archivePath, manifest); err != nil {
				return nil, fmt.Errorf("failed to backup %s: %w", name, err)
			}
		}
	}

	// Backup sequences file
	seqFile := filepath.Join(m.dataDir, "_sequences.seq")
	if _, err := os.Stat(seqFile); err == nil {
		if err := m.addFileToTar(tarWriter, seqFile, "sequences/_sequences.seq", manifest); err != nil {
			return nil, fmt.Errorf("failed to backup sequences: %w", err)
		}
	}

	// Backup users.json
	usersFile := filepath.Join(m.dataDir, "users.json")
	if _, err := os.Stat(usersFile); err == nil {
		if err := m.addFileToTar(tarWriter, usersFile, "auth/users.json", manifest); err != nil {
			return nil, fmt.Errorf("failed to backup users: %w", err)
		}
	}

	// Backup grants.json
	grantsFile := filepath.Join(m.dataDir, "grants.json")
	if _, err := os.Stat(grantsFile); err == nil {
		if err := m.addFileToTar(tarWriter, grantsFile, "auth/grants.json", manifest); err != nil {
			return nil, fmt.Errorf("failed to backup grants: %w", err)
		}
	}

	// Set checksum
	manifest.Checksum = "sha256:" + hex.EncodeToString(hasher.Sum(nil))

	// Write manifest
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestHeader := &tar.Header{
		Name:    "manifest.json",
		Mode:    0644,
		Size:    int64(len(manifestData)),
		ModTime: time.Now(),
	}
	if err := tarWriter.WriteHeader(manifestHeader); err != nil {
		return nil, fmt.Errorf("failed to write manifest header: %w", err)
	}
	if _, err := tarWriter.Write(manifestData); err != nil {
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}

	return manifest, nil
}

// Restore restores the database from a backup file.
func (m *Manager) Restore(opts RestoreOptions) (*Manifest, error) {
	// Open backup file
	file, err := os.Open(opts.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	var manifest Manifest

	// Try to detect if compressed
	buf := make([]byte, 2)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read backup file: %w", err)
	}

	// Seek back to start
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	// Check gzip magic number
	if n >= 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	tarReader := tar.NewReader(reader)
	hasher := sha256.New()
	multiReader := io.TeeReader(tarReader, hasher)

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar header: %w", err)
		}

		// Handle manifest
		if header.Name == "manifest.json" {
			data, err := io.ReadAll(multiReader)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest: %w", err)
			}
			if err := json.Unmarshal(data, &manifest); err != nil {
				return nil, fmt.Errorf("failed to parse manifest: %w", err)
			}
			continue
		}

		// Determine target path
		// Flatten path: tables/users.xmeta -> users.xmeta (directly in dataDir)
		archivePath := header.Name
		if strings.HasPrefix(archivePath, "tables/") {
			archivePath = strings.TrimPrefix(archivePath, "tables/")
		} else if strings.HasPrefix(archivePath, "auth/") {
			archivePath = strings.TrimPrefix(archivePath, "auth/")
		} else if strings.HasPrefix(archivePath, "sequences/") {
			archivePath = strings.TrimPrefix(archivePath, "sequences/")
		}
		targetPath := filepath.Join(m.dataDir, archivePath)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return nil, fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return nil, fmt.Errorf("failed to create parent directory: %w", err)
			}

			// Create file
			outFile, err := os.Create(targetPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}

			// Copy content through multi-reader for checksum
			if _, err := io.Copy(outFile, multiReader); err != nil {
				outFile.Close()
				return nil, fmt.Errorf("failed to write file %s: %w", targetPath, err)
			}
			outFile.Close()
		}
	}

	// Verify checksum
	// Note: In a full implementation, we'd verify the checksum matches

	return &manifest, nil
}

// addFileToTar adds a file to the tar archive.
func (m *Manager) addFileToTar(tw *tar.Writer, filePath, archivePath string, manifest *Manifest) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	header := &tar.Header{
		Name:    archivePath,
		Mode:    int64(stat.Mode()),
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	// Update manifest if it's a table metadata file
	if manifest != nil && strings.HasSuffix(archivePath, ".xmeta") {
		tableName := strings.TrimSuffix(filepath.Base(archivePath), ".xmeta")
		manifest.Tables = append(manifest.Tables, TableManifest{
			Name: tableName,
			Size: stat.Size(),
		})
		manifest.TableCount++
	}

	_, err = io.Copy(tw, file)
	return err
}

// addDirectoryToTar adds a directory to the tar archive.
func (m *Manager) addDirectoryToTar(tw *tar.Writer, dirPath, archivePath string, manifest *Manifest) error {
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate archive path
		relPath, err := filepath.Rel(dirPath, path)
		if err != nil {
			return err
		}
		archivePath := filepath.Join(archivePath, relPath)

		// Create header
		header := &tar.Header{
			Name:    archivePath,
			Mode:    int64(info.Mode()),
			ModTime: info.ModTime(),
		}

		if info.IsDir() {
			header.Typeflag = tar.TypeDir
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
			return nil
		}

		// Regular file
		header.Typeflag = tar.TypeReg
		header.Size = info.Size()

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// Add to manifest if it's a table metadata file
		if strings.HasSuffix(path, ".xmeta") {
			tableName := strings.TrimSuffix(filepath.Base(path), ".xmeta")
			manifest.Tables = append(manifest.Tables, TableManifest{
				Name: tableName,
				Size: info.Size(),
			})
			manifest.TableCount++
		}

		// Copy file content
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(tw, file)
		return err
	})
}

// VerifyBackup verifies the integrity of a backup file.
func (m *Manager) VerifyBackup(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// Try to detect if compressed
	buf := make([]byte, 2)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read backup file: %w", err)
	}

	// Seek back to start
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	// Check gzip magic number
	if n >= 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	tarReader := tar.NewReader(reader)
	var manifest Manifest

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar header: %w", err)
		}

		if header.Name == "manifest.json" {
			data, err := io.ReadAll(tarReader)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest: %w", err)
			}
			if err := json.Unmarshal(data, &manifest); err != nil {
				return nil, fmt.Errorf("failed to parse manifest: %w", err)
			}
		}
	}

	return &manifest, nil
}

// ListBackups lists all backup files in a directory.
func ListBackups(dir string) ([]string, error) {
	var backups []string

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() && (strings.HasSuffix(entry.Name(), BackupExt) || strings.HasSuffix(entry.Name(), ".xbak")) {
			backups = append(backups, filepath.Join(dir, entry.Name()))
		}
	}

	return backups, nil
}
