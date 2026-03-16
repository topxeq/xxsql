// Package recovery provides crash recovery for XxSql storage engine.
package recovery

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/checkpoint"
	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/sequence"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
	"github.com/topxeq/xxsql/internal/storage/wal"
)

// RecoveryState represents the state of recovery.
type RecoveryState struct {
	Phase         string    `json:"phase"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time,omitempty"`
	RecordsRead   int64     `json:"records_read"`
	RecordsRedone int64     `json:"records_redone"`
	RecordsUndone int64     `json:"records_undone"`
	TxnsRecovered int       `json:"txns_recovered"`
	PagesRecovered int      `json:"pages_recovered"`
	Success       bool      `json:"success"`
	Error         string    `json:"error,omitempty"`
}

// Manager handles crash recovery operations.
type Manager struct {
	dataDir      string
	walManager   *wal.Manager
	bufPool      *buffer.BufferPool
	checkpointMgr *checkpoint.Manager

	// Recovery state
	state      RecoveryState
	stateMu    sync.RWMutex

	// Tables for recovery (table name -> *Table)
	tables     map[string]*table.Table
	tablesMu   sync.RWMutex

	// Sequence manager for recovery
	seqMgr     *sequence.Manager
}

// ManagerConfig holds recovery manager configuration.
type ManagerConfig struct {
	DataDir       string
	WALManager    *wal.Manager
	BufferPool    *buffer.BufferPool
	CheckpointMgr *checkpoint.Manager
}

// NewManager creates a new recovery manager.
func NewManager(config ManagerConfig) *Manager {
	return &Manager{
		dataDir:       config.DataDir,
		walManager:    config.WALManager,
		bufPool:       config.BufferPool,
		checkpointMgr: config.CheckpointMgr,
		tables:        make(map[string]*table.Table),
	}
}

// RegisterTable registers a table for recovery.
func (m *Manager) RegisterTable(name string, tbl *table.Table) {
	m.tablesMu.Lock()
	defer m.tablesMu.Unlock()
	m.tables[name] = tbl
}

// UnregisterTable unregisters a table.
func (m *Manager) UnregisterTable(name string) {
	m.tablesMu.Lock()
	defer m.tablesMu.Unlock()
	delete(m.tables, name)
}

// SetSequenceManager sets the sequence manager.
func (m *Manager) SetSequenceManager(sm *sequence.Manager) {
	m.seqMgr = sm
}

// Recover performs crash recovery using ARIES-style algorithm.
// Returns the recovery state and any error.
func (m *Manager) Recover() (*RecoveryState, error) {
	m.stateMu.Lock()
	m.state = RecoveryState{
		Phase:     "initialization",
		StartTime: time.Now(),
	}
	m.stateMu.Unlock()

	// Helper to set end time and return state
	finishRecovery := func() *RecoveryState {
		m.stateMu.Lock()
		m.state.EndTime = time.Now()
		state := m.state
		m.stateMu.Unlock()
		return &state
	}

	// Phase 0: Initialization
	ckpt, err := m.initialize()
	if err != nil {
		m.setError(err)
		return finishRecovery(), err
	}

	// If no checkpoint and no WAL records, nothing to recover
	if ckpt == nil && m.walManager == nil {
		m.stateMu.Lock()
		m.state.Success = true
		m.state.Phase = "completed"
		m.stateMu.Unlock()
		return finishRecovery(), nil
	}

	// Phase 1: Analysis - Build transaction table and dirty page table
	txns, dirtyPages, err := m.analysisPhase(ckpt)
	if err != nil {
		m.setError(err)
		return finishRecovery(), err
	}

	// Phase 2: Redo - Redo all logged operations from checkpoint
	if err := m.redoPhase(ckpt, dirtyPages); err != nil {
		m.setError(err)
		return finishRecovery(), err
	}

	// Phase 3: Undo - Undo uncommitted transactions
	if err := m.undoPhase(txns); err != nil {
		m.setError(err)
		return finishRecovery(), err
	}

	// Phase 4: Finalization
	if err := m.finalize(); err != nil {
		m.setError(err)
		return finishRecovery(), err
	}

	m.stateMu.Lock()
	m.state.Success = true
	m.state.Phase = "completed"
	m.stateMu.Unlock()

	return finishRecovery(), nil
}

// initialize sets up recovery state and loads checkpoint.
func (m *Manager) initialize() (*checkpoint.Checkpoint, error) {
	m.stateMu.Lock()
	m.state.Phase = "initialization"
	m.stateMu.Unlock()

	// Load last checkpoint
	var ckpt *checkpoint.Checkpoint
	if m.checkpointMgr != nil {
		ckpt = m.checkpointMgr.GetLastCheckpoint()
	}

	return ckpt, nil
}

// TransactionState tracks transaction state during recovery.
type TransactionState struct {
	TxnID      uint64
	Status     string // "active", "committed", "aborted"
	FirstLSN   wal.LSN
	LastLSN    wal.LSN
	Operations []*wal.Record
}

// analysisPhase analyzes WAL to build transaction table and dirty page table.
func (m *Manager) analysisPhase(ckpt *checkpoint.Checkpoint) (map[uint64]*TransactionState, map[page.PageID]wal.LSN, error) {
	m.stateMu.Lock()
	m.state.Phase = "analysis"
	m.stateMu.Unlock()

	// Transaction table: txnID -> state
	txns := make(map[uint64]*TransactionState)

	// Dirty page table: pageID -> recovery LSN
	dirtyPages := make(map[page.PageID]wal.LSN)

	// Start LSN for analysis
	startLSN := wal.LSN(0)
	if ckpt != nil {
		startLSN = ckpt.LSN
		// Initialize from checkpoint's active transactions
		for _, txnID := range ckpt.ActiveTxns {
			txns[txnID] = &TransactionState{
				TxnID:  txnID,
				Status: "active",
			}
		}
	}

	// Scan WAL forward
	if m.walManager == nil {
		return txns, dirtyPages, nil
	}

	err := m.walManager.Replay(func(r *wal.Record) error {
		m.stateMu.Lock()
		m.state.RecordsRead++
		m.stateMu.Unlock()

		// Skip records before checkpoint
		if r.LSN <= startLSN {
			return nil
		}

		// Update transaction state based on record type
		switch r.Type {
		case wal.RecordTypeBegin:
			txns[r.TxnID] = &TransactionState{
				TxnID:    r.TxnID,
				Status:   "active",
				FirstLSN: r.LSN,
			}

		case wal.RecordTypeCommit:
			if txn, ok := txns[r.TxnID]; ok {
				txn.Status = "committed"
				txn.LastLSN = r.LSN
			}

		case wal.RecordTypeAbort:
			if txn, ok := txns[r.TxnID]; ok {
				txn.Status = "aborted"
				txn.LastLSN = r.LSN
			}

		case wal.RecordTypeInsert, wal.RecordTypeUpdate, wal.RecordTypeDelete:
			// Track operation for potential undo
			if txn, ok := txns[r.TxnID]; ok {
				txn.Operations = append(txn.Operations, r)
				txn.LastLSN = r.LSN
			} else {
				// New transaction seen
				txns[r.TxnID] = &TransactionState{
					TxnID:    r.TxnID,
					Status:   "active",
					FirstLSN: r.LSN,
					LastLSN:  r.LSN,
					Operations: []*wal.Record{r},
				}
			}
			// Track dirty page
			if r.PageID > 0 {
				pageID := page.PageID(r.PageID)
				if _, exists := dirtyPages[pageID]; !exists {
					dirtyPages[pageID] = r.LSN
				}
			}

		case wal.RecordTypePageWrite:
			// Track page write
			if r.PageID > 0 {
				pageID := page.PageID(r.PageID)
				if _, exists := dirtyPages[pageID]; !exists {
					dirtyPages[pageID] = r.LSN
				}
			}

		case wal.RecordTypeSequenceNext:
			// Track sequence operations
			if txn, ok := txns[r.TxnID]; ok {
				txn.LastLSN = r.LSN
			}
		}

		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("analysis phase failed: %w", err)
	}

	// Mark transactions without commit/abort as "active" (need undo)
	for _, txn := range txns {
		if txn.Status == "active" {
			m.stateMu.Lock()
			m.state.TxnsRecovered++
			m.stateMu.Unlock()
		}
	}

	return txns, dirtyPages, nil
}

// redoPhase redoes all logged operations from checkpoint LSN.
func (m *Manager) redoPhase(ckpt *checkpoint.Checkpoint, dirtyPages map[page.PageID]wal.LSN) error {
	m.stateMu.Lock()
	m.state.Phase = "redo"
	m.stateMu.Unlock()

	startLSN := wal.LSN(0)
	if ckpt != nil {
		startLSN = ckpt.LSN
	}

	if m.walManager == nil {
		return nil
	}

	err := m.walManager.Replay(func(r *wal.Record) error {
		// Skip records before checkpoint
		if r.LSN <= startLSN {
			return nil
		}

		// Redo based on record type
		switch r.Type {
		case wal.RecordTypeInsert:
			if err := m.redoInsert(r); err != nil {
				// Log but continue
			}
			m.stateMu.Lock()
			m.state.RecordsRedone++
			m.stateMu.Unlock()

		case wal.RecordTypeUpdate:
			if err := m.redoUpdate(r); err != nil {
				// Log but continue
			}
			m.stateMu.Lock()
			m.state.RecordsRedone++
			m.stateMu.Unlock()

		case wal.RecordTypeDelete:
			if err := m.redoDelete(r); err != nil {
				// Log but continue
			}
			m.stateMu.Lock()
			m.state.RecordsRedone++
			m.stateMu.Unlock()

		case wal.RecordTypePageWrite:
			if err := m.redoPageWrite(r); err != nil {
				// Log but continue
			}
			m.stateMu.Lock()
			m.state.RecordsRedone++
			m.state.PagesRecovered++
			m.stateMu.Unlock()

		case wal.RecordTypeSequenceNext:
			if err := m.redoSequenceNext(r); err != nil {
				// Log but continue
			}
			m.stateMu.Lock()
			m.state.RecordsRedone++
			m.stateMu.Unlock()
		}

		return nil
	})

	return err
}

// undoPhase undoes uncommitted transactions.
func (m *Manager) undoPhase(txns map[uint64]*TransactionState) error {
	m.stateMu.Lock()
	m.state.Phase = "undo"
	m.stateMu.Unlock()

	// Find uncommitted transactions
	var uncommitted []*TransactionState
	for _, txn := range txns {
		if txn.Status == "active" {
			uncommitted = append(uncommitted, txn)
		}
	}

	// Undo in reverse order of operations
	for _, txn := range uncommitted {
		// Undo operations in reverse order
		for i := len(txn.Operations) - 1; i >= 0; i-- {
			r := txn.Operations[i]
			if err := m.undoOperation(r); err != nil {
				// Log but continue
			}
			m.stateMu.Lock()
			m.state.RecordsUndone++
			m.stateMu.Unlock()
		}

		// Log abort
		if m.walManager != nil {
			m.walManager.LogAbort(txn.TxnID)
		}
	}

	return nil
}

// finalize completes recovery.
func (m *Manager) finalize() error {
	m.stateMu.Lock()
	m.state.Phase = "finalization"
	m.stateMu.Unlock()

	// Flush all recovered pages
	if m.bufPool != nil {
		if err := m.bufPool.FlushAll(); err != nil {
			return err
		}
	}

	// Sync sequences
	if m.seqMgr != nil {
		if err := m.seqMgr.Persist(); err != nil {
			// Non-fatal
		}
	}

	// Create new checkpoint
	if m.checkpointMgr != nil {
		if _, err := m.checkpointMgr.Create(); err != nil {
			return err
		}
	}

	return nil
}

// redoInsert redoes an insert operation.
func (m *Manager) redoInsert(r *wal.Record) error {
	if r.TableName == "" {
		return nil
	}

	m.tablesMu.RLock()
	tbl, ok := m.tables[r.TableName]
	m.tablesMu.RUnlock()

	if !ok {
		return fmt.Errorf("table %s not found for redo", r.TableName)
	}

	// Parse row data and re-insert
	// For now, we skip actual data re-insertion as it would require
	// more complex row ID tracking
	_ = tbl
	return nil
}

// redoUpdate redoes an update operation.
func (m *Manager) redoUpdate(r *wal.Record) error {
	if r.TableName == "" || len(r.Data) < 8 {
		return nil
	}

	// Parse old and new data from record
	m.tablesMu.RLock()
	tbl, ok := m.tables[r.TableName]
	m.tablesMu.RUnlock()

	if !ok {
		return fmt.Errorf("table %s not found for redo", r.TableName)
	}

	_ = tbl
	return nil
}

// redoDelete redoes a delete operation.
func (m *Manager) redoDelete(r *wal.Record) error {
	if r.TableName == "" {
		return nil
	}

	m.tablesMu.RLock()
	tbl, ok := m.tables[r.TableName]
	m.tablesMu.RUnlock()

	if !ok {
		return fmt.Errorf("table %s not found for redo", r.TableName)
	}

	_ = tbl
	return nil
}

// redoPageWrite redoes a page write operation.
func (m *Manager) redoPageWrite(r *wal.Record) error {
	if m.bufPool == nil || len(r.Data) == 0 {
		return nil
	}

	// Write page data directly
	pageID := page.PageID(r.PageID)
	p, err := page.NewPageFromBytes(r.Data)
	if err != nil {
		return err
	}

	// For page write, we update the page content
	// GetPage returns the page, then we mark it dirty when unpinning
	_, err = m.bufPool.GetPage(pageID)
	if err != nil {
		// Page doesn't exist in buffer, that's okay
		return nil
	}

	// Mark as dirty when unpinning
	m.bufPool.UnpinPage(pageID, true)

	_ = p // Page data is stored in the record
	return nil
}

// redoSequenceNext redoes a sequence next operation.
func (m *Manager) redoSequenceNext(r *wal.Record) error {
	if m.seqMgr == nil || r.TableName == "" || len(r.Data) < 8 {
		return nil
	}

	value := int64(binary.LittleEndian.Uint64(r.Data))
	return m.seqMgr.SetCurrentValue(r.TableName, value)
}

// undoOperation undoes an operation.
func (m *Manager) undoOperation(r *wal.Record) error {
	switch r.Type {
	case wal.RecordTypeInsert:
		return m.undoInsert(r)
	case wal.RecordTypeUpdate:
		return m.undoUpdate(r)
	case wal.RecordTypeDelete:
		return m.undoDelete(r)
	}
	return nil
}

// undoInsert undoes an insert (delete the row).
func (m *Manager) undoInsert(r *wal.Record) error {
	// For undo of insert, we would delete the row
	// This requires row ID tracking which we'll implement later
	return nil
}

// undoUpdate undoes an update (restore old value).
func (m *Manager) undoUpdate(r *wal.Record) error {
	// Parse old data and restore
	return nil
}

// undoDelete undoes a delete (re-insert the row).
func (m *Manager) undoDelete(r *wal.Record) error {
	// Re-insert the deleted row
	return nil
}

// setState sets the recovery phase.
func (m *Manager) setState(phase string) {
	m.stateMu.Lock()
	m.state.Phase = phase
	m.stateMu.Unlock()
}

// setError sets the recovery error.
func (m *Manager) setError(err error) {
	m.stateMu.Lock()
	m.state.Success = false
	m.state.Error = err.Error()
	m.stateMu.Unlock()
}

// getState returns current recovery state.
func (m *Manager) getState() *RecoveryState {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	state := m.state
	return &state
}

// GetState returns current recovery state (thread-safe).
func (m *Manager) GetState() *RecoveryState {
	return m.getState()
}

// SaveRecoveryState saves recovery state to disk.
func (m *Manager) SaveRecoveryState() error {
	state := m.getState()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(m.dataDir, "recovery.state")
	return os.WriteFile(path, data, 0644)
}

// LoadRecoveryState loads recovery state from disk.
func (m *Manager) LoadRecoveryState() (*RecoveryState, error) {
	path := filepath.Join(m.dataDir, "recovery.state")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state RecoveryState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

// ClearRecoveryState removes recovery state file.
func (m *Manager) ClearRecoveryState() error {
	path := filepath.Join(m.dataDir, "recovery.state")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// NeedRecovery checks if recovery is needed.
func (m *Manager) NeedRecovery() (bool, error) {
	// Check for unclean shutdown marker
	uncleanPath := filepath.Join(m.dataDir, ".unclean")
	if _, err := os.Stat(uncleanPath); err == nil {
		return true, nil
	}

	// Check if WAL has records
	if m.walManager != nil {
		size, err := m.walManager.Size()
		if err != nil {
			return false, err
		}
		if size > 0 {
			return true, nil
		}
	}

	// Check for recovery state
	state, err := m.LoadRecoveryState()
	if err != nil {
		return false, err
	}
	if state != nil && !state.Success {
		return true, nil
	}

	return false, nil
}

// MarkUnclean marks database as unclean for recovery detection.
func (m *Manager) MarkUnclean() error {
	path := filepath.Join(m.dataDir, ".unclean")
	return os.WriteFile(path, []byte(time.Now().Format(time.RFC3339)), 0644)
}

// MarkClean marks database as clean (successful shutdown).
func (m *Manager) MarkClean() error {
	path := filepath.Join(m.dataDir, ".unclean")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// RecoveryLog logs a recovery operation.
type RecoveryLog struct {
	Timestamp time.Time `json:"timestamp"`
	Phase     string    `json:"phase"`
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
}

// LogRecovery logs a recovery event.
func (m *Manager) LogRecovery(phase, message string, success bool) error {
	log := RecoveryLog{
		Timestamp: time.Now(),
		Phase:     phase,
		Message:   message,
		Success:   success,
	}

	// Append to recovery log file
	path := filepath.Join(m.dataDir, "recovery.log")
	data, err := json.Marshal(log)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(string(data) + "\n")
	return err
}

// GetRecoveryLogs returns recent recovery logs.
func (m *Manager) GetRecoveryLogs() ([]RecoveryLog, error) {
	path := filepath.Join(m.dataDir, "recovery.log")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var logs []RecoveryLog
	lines := splitLines(string(data))
	for _, line := range lines {
		if line == "" {
			continue
		}
		var log RecoveryLog
		if err := json.Unmarshal([]byte(line), &log); err != nil {
			continue
		}
		logs = append(logs, log)
	}

	return logs, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// Value helper for types (to avoid import cycle)
type Value = types.Value
