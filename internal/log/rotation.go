// Package log provides logging facilities for XxSql.
package log

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Rotator handles log file rotation.
type Rotator struct {
	maxSizeMB  int64 // Maximum size in MB before rotation
	maxBackups int   // Maximum number of old log files to keep
	maxAgeDays int   // Maximum age in days to keep old log files
	compress   bool  // Whether to compress rotated files
}

// NewRotator creates a new log rotator.
func NewRotator(maxSizeMB, maxBackups, maxAgeDays int, compress bool) *Rotator {
	return &Rotator{
		maxSizeMB:  int64(maxSizeMB),
		maxBackups: maxBackups,
		maxAgeDays: maxAgeDays,
		compress:   compress,
	}
}

// ShouldRotate checks if the log file should be rotated.
func (r *Rotator) ShouldRotate(file *os.File) bool {
	if r.maxSizeMB <= 0 {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Size() >= r.maxSizeMB*1024*1024
}

// Rotate performs log rotation for the given file.
func (r *Rotator) Rotate(file *os.File) error {
	if file == nil {
		return nil
	}

	// Get current file path
	origPath, err := filepath.Abs(file.Name())
	if err != nil {
		return err
	}

	// Close the current file
	if err := file.Close(); err != nil {
		return err
	}

	// Rename current file with timestamp
	timestamp := time.Now().Format("20060102-150405")
	rotatedPath := origPath + "." + timestamp

	if err := os.Rename(origPath, rotatedPath); err != nil {
		return err
	}

	// Compress if enabled
	if r.compress {
		go r.compressFile(rotatedPath)
	}

	// Clean up old files
	go r.cleanupOldFiles(origPath)

	// Reopen the log file
	_, err = os.OpenFile(origPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Note: The caller will need to update their file reference
	return nil
}

// RotateWithPath rotates the log file at the given path.
// Returns the new file handle after rotation.
func (r *Rotator) RotateWithPath(path string) (*os.File, error) {
	// Get absolute path
	origPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	// Check if file exists and should be rotated
	info, err := os.Stat(origPath)
	if os.IsNotExist(err) {
		// File doesn't exist, create it
		return os.OpenFile(origPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	}
	if err != nil {
		return nil, err
	}

	if info.Size() < r.maxSizeMB*1024*1024 {
		// File exists but doesn't need rotation, open it
		return os.OpenFile(origPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	}

	// Rename current file with timestamp
	timestamp := time.Now().Format("20060102-150405")
	rotatedPath := origPath + "." + timestamp

	if err := os.Rename(origPath, rotatedPath); err != nil {
		return nil, err
	}

	// Compress if enabled
	if r.compress {
		go r.compressFile(rotatedPath)
	}

	// Clean up old files
	go r.cleanupOldFiles(origPath)

	// Create new file
	return os.OpenFile(origPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
}

// compressFile compresses a log file and removes the original.
func (r *Rotator) compressFile(path string) {
	src, err := os.Open(path)
	if err != nil {
		return
	}
	defer src.Close()

	dst, err := os.Create(path + ".gz")
	if err != nil {
		return
	}
	defer dst.Close()

	gz := gzip.NewWriter(dst)
	defer gz.Close()

	io.Copy(gz, src)

	// Remove the original file after successful compression
	os.Remove(path)
}

// cleanupOldFiles removes old log files based on maxBackups and maxAgeDays.
func (r *Rotator) cleanupOldFiles(logPath string) {
	dir := filepath.Dir(logPath)
	base := filepath.Base(logPath)

	// Find all rotated files
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	var backups []backupInfo
	now := time.Now()

	for _, entry := range entries {
		name := entry.Name()
		// Match pattern: logname.timestamp or logname.timestamp.gz
		if !strings.HasPrefix(name, base+".") {
			continue
		}
		if name == base {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		backups = append(backups, backupInfo{
			path:    filepath.Join(dir, name),
			modTime: info.ModTime(),
		})
	}

	// Sort by modification time (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].modTime.After(backups[j].modTime)
	})

	// Remove files based on age and count
	for i, backup := range backups {
		// Remove if too old
		if r.maxAgeDays > 0 {
			age := int(now.Sub(backup.modTime).Hours() / 24)
			if age > r.maxAgeDays {
				os.Remove(backup.path)
				continue
			}
		}

		// Remove if too many backups
		if r.maxBackups > 0 && i >= r.maxBackups {
			os.Remove(backup.path)
		}
	}
}

// backupInfo holds information about a backup file.
type backupInfo struct {
	path    string
	modTime time.Time
}

// ForceRotate forces a rotation of the log file.
func (r *Rotator) ForceRotate(path string) error {
	// Get absolute path
	origPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	// Check if file exists
	if _, err := os.Stat(origPath); os.IsNotExist(err) {
		return nil // Nothing to rotate
	}

	// Rename with timestamp
	timestamp := time.Now().Format("20060102-150405")
	rotatedPath := origPath + "." + timestamp

	if err := os.Rename(origPath, rotatedPath); err != nil {
		return err
	}

	// Compress if enabled
	if r.compress {
		go r.compressFile(rotatedPath)
	}

	// Clean up old files
	go r.cleanupOldFiles(origPath)

	return nil
}

// GetBackupInfo returns information about current backup files.
func (r *Rotator) GetBackupInfo(logPath string) ([]BackupInfo, error) {
	dir := filepath.Dir(logPath)
	base := filepath.Base(logPath)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var infos []BackupInfo
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, base+".") || name == base {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		infos = append(infos, BackupInfo{
			Path:    filepath.Join(dir, name),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
	}

	return infos, nil
}

// BackupInfo represents information about a backup file.
type BackupInfo struct {
	Path    string
	Size    int64
	ModTime time.Time
}