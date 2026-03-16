// Package checkpoint provides checkpoint management for XxSql storage engine.
package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/wal"
)

const (
	// CheckpointFileExt is the extension for checkpoint files.
	CheckpointFileExt = ".xckpt"

	// CheckpointMetaFile is the checkpoint metadata file.
	CheckpointMetaFile = "checkpoint.meta"
)

// Checkpoint represents a database checkpoint.
type Checkpoint struct {
	ID          uint64
	LSN         wal.LSN
	Timestamp   int64
	TableCount  int
	RowCount    uint64
	PageCount   uint64
	ActiveTxns  []uint64
	Metadata    map[string]string
}

// Manager manages checkpoint operations.
type Manager struct {
	dataDir    string
	walManager *wal.Manager
	bufPool    *buffer.BufferPool

	lastCheckpoint atomic.Value // *Checkpoint
	mu             sync.RWMutex

	// Checkpoint configuration
	interval    time.Duration
	maxWALSize  int64
	autoEnabled bool
	stopCh      chan struct{}
	stopped     atomic.Bool
}

// ManagerConfig holds checkpoint manager configuration.
type ManagerConfig struct {
	DataDir     string
	WALManager  *wal.Manager
	BufferPool  *buffer.BufferPool
	Interval    time.Duration
	MaxWALSize  int64
	AutoEnabled bool
}

// NewManager creates a new checkpoint manager.
func NewManager(config ManagerConfig) *Manager {
	if config.Interval <= 0 {
		config.Interval = 5 * time.Minute
	}
	if config.MaxWALSize <= 0 {
		config.MaxWALSize = 64 * 1024 * 1024 // 64MB
	}

	m := &Manager{
		dataDir:     config.DataDir,
		walManager:  config.WALManager,
		bufPool:     config.BufferPool,
		interval:    config.Interval,
		maxWALSize:  config.MaxWALSize,
		autoEnabled: config.AutoEnabled,
		stopCh:      make(chan struct{}),
	}

	return m
}

// Start starts automatic checkpointing.
func (m *Manager) Start() error {
	if !m.autoEnabled {
		return nil
	}

	go m.autoCheckpointLoop()
	return nil
}

// Stop stops automatic checkpointing.
func (m *Manager) Stop() {
	if m.stopped.Load() {
		return
	}

	m.stopped.Store(true)
	close(m.stopCh)
}

// autoCheckpointLoop runs periodic checkpoints.
func (m *Manager) autoCheckpointLoop() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			// Check if checkpoint needed
			needed, err := m.NeedsCheckpoint()
			if err != nil {
				continue
			}
			if needed {
				m.Create()
			}
		}
	}
}

// NeedsCheckpoint returns true if a checkpoint is needed.
func (m *Manager) NeedsCheckpoint() (bool, error) {
	// Check WAL size
	if m.walManager != nil {
		needed, err := m.walManager.NeedsCheckpoint()
		if err != nil {
			return false, err
		}
		if needed {
			return true, nil
		}
	}

	// Check time since last checkpoint
	last := m.GetLastCheckpoint()
	if last == nil {
		return true, nil
	}

	elapsed := time.Since(time.Unix(0, last.Timestamp))
	return elapsed >= m.interval, nil
}

// Create creates a new checkpoint.
func (m *Manager) Create() (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get current LSN
	var lsn wal.LSN
	if m.walManager != nil {
		lsn = m.walManager.GetCurrentLSN()
	}

	// Flush buffer pool
	if m.bufPool != nil {
		if err := m.bufPool.FlushAll(); err != nil {
			return nil, fmt.Errorf("failed to flush buffer pool: %w", err)
		}
	}

	// Create checkpoint
	ckpt := &Checkpoint{
		ID:         uint64(time.Now().UnixNano()),
		LSN:        lsn,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   make(map[string]string),
		ActiveTxns: []uint64{}, // No active txns for now (no transaction support yet)
	}

	// Save checkpoint
	if err := m.saveCheckpoint(ckpt); err != nil {
		return nil, fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Log checkpoint in WAL
	if m.walManager != nil {
		ckptData, _ := json.Marshal(ckpt)
		if _, err := m.walManager.LogCheckpoint(0, ckptData); err != nil {
			return nil, fmt.Errorf("failed to log checkpoint: %w", err)
		}
	}

	// Update last checkpoint
	m.lastCheckpoint.Store(ckpt)

	return ckpt, nil
}

// saveCheckpoint saves checkpoint to disk.
func (m *Manager) saveCheckpoint(ckpt *Checkpoint) error {
	// Create data directory if not exists
	if err := os.MkdirAll(m.dataDir, 0755); err != nil {
		return err
	}

	// Save checkpoint metadata
	metaPath := filepath.Join(m.dataDir, CheckpointMetaFile)
	data, err := json.MarshalIndent(ckpt, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metaPath, data, 0644)
}

// GetLastCheckpoint returns the last checkpoint.
func (m *Manager) GetLastCheckpoint() *Checkpoint {
	if v := m.lastCheckpoint.Load(); v != nil {
		return v.(*Checkpoint)
	}

	// Try to load from disk
	ckpt, err := m.loadLastCheckpoint()
	if err != nil {
		return nil
	}

	if ckpt != nil {
		m.lastCheckpoint.Store(ckpt)
	}

	return ckpt
}

// loadLastCheckpoint loads the last checkpoint from disk.
func (m *Manager) loadLastCheckpoint() (*Checkpoint, error) {
	metaPath := filepath.Join(m.dataDir, CheckpointMetaFile)

	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var ckpt Checkpoint
	if err := json.Unmarshal(data, &ckpt); err != nil {
		return nil, err
	}

	return &ckpt, nil
}

// RecoveryInfo holds information needed for recovery.
type RecoveryInfo struct {
	CheckpointLSN wal.LSN
	CurrentLSN    wal.LSN
	Tables        []TableRecoveryInfo
}

// TableRecoveryInfo holds recovery info for a table.
type TableRecoveryInfo struct {
	Name       string
	RootPageID page.PageID
	PageCount  uint64
}

// GetRecoveryInfo returns recovery information.
func (m *Manager) GetRecoveryInfo() (*RecoveryInfo, error) {
	info := &RecoveryInfo{}

	// Get checkpoint LSN
	ckpt := m.GetLastCheckpoint()
	if ckpt != nil {
		info.CheckpointLSN = ckpt.LSN
	}

	// Get current LSN
	if m.walManager != nil {
		info.CurrentLSN = m.walManager.GetCurrentLSN()
	}

	return info, nil
}

// Recover performs recovery from the last checkpoint.
func (m *Manager) Recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get last checkpoint
	ckpt := m.GetLastCheckpoint()
	if ckpt == nil {
		// No checkpoint, nothing to recover
		return nil
	}

	// Replay WAL from checkpoint LSN
	if m.walManager != nil {
		return m.replayWAL(ckpt.LSN)
	}

	return nil
}

// replayWAL replays WAL records from the given LSN.
func (m *Manager) replayWAL(fromLSN wal.LSN) error {
	return m.walManager.Replay(func(r *wal.Record) error {
		// Skip records before checkpoint
		if r.LSN <= fromLSN {
			return nil
		}

		// Apply record based on type
		switch r.Type {
		case wal.RecordTypeInsert:
			// TODO: Re-apply insert
		case wal.RecordTypeUpdate:
			// TODO: Re-apply update
		case wal.RecordTypeDelete:
			// TODO: Re-apply delete
		case wal.RecordTypePageWrite:
			// TODO: Re-apply page write
		}

		return nil
	})
}

// TruncateWAL truncates the WAL after a successful checkpoint.
func (m *Manager) TruncateWAL() error {
	if m.walManager == nil {
		return nil
	}

	return m.walManager.Truncate()
}

// SetWALManager sets the WAL manager.
func (m *Manager) SetWALManager(wm *wal.Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.walManager = wm
}

// SetBufferPool sets the buffer pool.
func (m *Manager) SetBufferPool(bp *buffer.BufferPool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bufPool = bp
}

// Stats returns checkpoint statistics.
func (m *Manager) Stats() CheckpointStats {
	last := m.GetLastCheckpoint()

	stats := CheckpointStats{
		AutoEnabled: m.autoEnabled,
		Interval:    m.interval.String(),
	}

	if last != nil {
		stats.LastCheckpointID = last.ID
		stats.LastCheckpointLSN = uint64(last.LSN)
		stats.LastCheckpointTime = time.Unix(0, last.Timestamp).Format(time.RFC3339)
		stats.PageCount = last.PageCount
		stats.RowCount = last.RowCount
	}

	return stats
}

// CheckpointStats holds checkpoint statistics.
type CheckpointStats struct {
	AutoEnabled       bool   `json:"auto_enabled"`
	Interval          string `json:"interval"`
	LastCheckpointID  uint64 `json:"last_checkpoint_id"`
	LastCheckpointLSN uint64 `json:"last_checkpoint_lsn"`
	LastCheckpointTime string `json:"last_checkpoint_time"`
	PageCount         uint64 `json:"page_count"`
	RowCount          uint64 `json:"row_count"`
}

// DeleteCheckpoint removes checkpoint files.
func (m *Manager) DeleteCheckpoint() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	metaPath := filepath.Join(m.dataDir, CheckpointMetaFile)
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	m.lastCheckpoint.Store((*Checkpoint)(nil))
	return nil
}
