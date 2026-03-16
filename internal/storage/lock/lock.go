// Package lock provides lock management for XxSql storage engine.
package lock

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// LockType represents the type of lock.
type LockType uint8

const (
	LockTypeShared LockType = iota
	LockTypeExclusive
)

// String returns the string representation.
func (t LockType) String() string {
	if t == LockTypeShared {
		return "SHARED"
	}
	return "EXCLUSIVE"
}

// LockLevel represents the granularity level of a lock.
type LockLevel uint8

const (
	LockLevelGlobal LockLevel = iota
	LockLevelCatalog
	LockLevelTable
	LockLevelPage
	LockLevelRow
)

// String returns the string representation.
func (t LockLevel) String() string {
	switch t {
	case LockLevelGlobal:
		return "GLOBAL"
	case LockLevelCatalog:
		return "CATALOG"
	case LockLevelTable:
		return "TABLE"
	case LockLevelPage:
		return "PAGE"
	case LockLevelRow:
		return "ROW"
	default:
		return "UNKNOWN"
	}
}

// LockID uniquely identifies a lockable resource.
type LockID struct {
	Level   LockLevel
	TableID uint64
	PageID  uint64
	RowID   uint64
}

// String returns a string representation.
func (id LockID) String() string {
	switch id.Level {
	case LockLevelGlobal:
		return "GLOBAL"
	case LockLevelCatalog:
		return "CATALOG"
	case LockLevelTable:
		return fmt.Sprintf("TABLE:%d", id.TableID)
	case LockLevelPage:
		return fmt.Sprintf("PAGE:%d:%d", id.TableID, id.PageID)
	case LockLevelRow:
		return fmt.Sprintf("ROW:%d:%d:%d", id.TableID, id.PageID, id.RowID)
	default:
		return "UNKNOWN"
	}
}

// Lock represents a lock on a resource.
type Lock struct {
	ID        LockID
	Type      LockType
	Holder    uint64 // Transaction ID
	Granted   bool
	Timestamp int64
}

// LockRequest represents a lock request.
type LockRequest struct {
	ID         LockID
	Type       LockType
	TxnID      uint64
	Granted    bool
	WaitStart  time.Time
	ResultChan chan error
}

// Manager manages locks for the database.
type Manager struct {
	// Lock tables organized by level
	globalLock   *lockEntry
	catalogLock  *lockEntry
	tableLocks   map[uint64]*lockEntry
	pageLocks    map[string]*lockEntry // key: tableID:pageID
	rowLocks     map[string]*lockEntry // key: tableID:pageID:rowID

	// Wait-for graph for deadlock detection
	waitForGraph *WaitForGraph

	// Configuration
	timeout time.Duration

	// Synchronization
	mu sync.RWMutex

	// Statistics
	waitCount   uint64
	grantCount  uint64
	timeoutCount uint64
}

// lockEntry represents a lockable resource.
type lockEntry struct {
	id          LockID
	sharedCount int32
	exclusive   uint64 // TxnID of exclusive holder, 0 if none
	waiters     []*LockRequest
	mu          sync.Mutex
}

// NewManager creates a new lock manager.
func NewManager(timeout time.Duration) *Manager {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	m := &Manager{
		timeout:       timeout,
		tableLocks:    make(map[uint64]*lockEntry),
		pageLocks:     make(map[string]*lockEntry),
		rowLocks:      make(map[string]*lockEntry),
		waitForGraph:  NewWaitForGraph(),
	}

	// Initialize global and catalog locks
	m.globalLock = &lockEntry{id: LockID{Level: LockLevelGlobal}}
	m.catalogLock = &lockEntry{id: LockID{Level: LockLevelCatalog}}

	return m
}

// Lock acquires a lock.
func (m *Manager) Lock(id LockID, lockType LockType, txnID uint64) error {
	atomic.AddUint64(&m.waitCount, 1)

	entry := m.getOrCreateEntry(id)

	resultChan := make(chan error, 1)
	req := &LockRequest{
		ID:         id,
		Type:       lockType,
		TxnID:      txnID,
		ResultChan: resultChan,
		WaitStart:  time.Now(),
	}

	// Try to acquire lock
	entry.mu.Lock()
	granted := m.tryAcquire(entry, lockType, txnID)
	if granted {
		req.Granted = true
		entry.mu.Unlock()
		atomic.AddUint64(&m.grantCount, 1)
		return nil
	}

	// Add to wait queue
	entry.waiters = append(entry.waiters, req)
	entry.mu.Unlock()

	// Add to wait-for graph
	m.waitForGraph.AddEdge(txnID, entry.exclusive)

	// Wait for lock with timeout
	select {
	case err := <-resultChan:
		return err
	case <-time.After(m.timeout):
		m.removeWaiter(entry, req)
		m.waitForGraph.RemoveEdge(txnID, entry.exclusive)
		atomic.AddUint64(&m.timeoutCount, 1)
		return fmt.Errorf("lock timeout for %s", id)
	}
}

// Unlock releases a lock.
func (m *Manager) Unlock(id LockID, txnID uint64) error {
	entry := m.getOrCreateEntry(id)
	if entry == nil {
		return nil
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// Check if we hold the lock
	if entry.exclusive == txnID {
		entry.exclusive = 0
	} else if entry.sharedCount > 0 {
		entry.sharedCount--
	} else {
		return fmt.Errorf("transaction %d does not hold lock on %s", txnID, id)
	}

	// Wake up waiters
	m.wakeWaiters(entry)

	return nil
}

// tryAcquire attempts to acquire a lock without waiting.
func (m *Manager) tryAcquire(entry *lockEntry, lockType LockType, txnID uint64) bool {
	switch lockType {
	case LockTypeShared:
		// Can acquire shared if no exclusive lock
		if entry.exclusive == 0 {
			entry.sharedCount++
			return true
		}
		// Can also acquire if we already hold exclusive
		if entry.exclusive == txnID {
			return true
		}
		return false

	case LockTypeExclusive:
		// Can acquire exclusive if no shared or exclusive locks
		if entry.exclusive == 0 && entry.sharedCount == 0 {
			entry.exclusive = txnID
			return true
		}
		// Can upgrade if we hold the only shared lock
		if entry.exclusive == 0 && entry.sharedCount == 1 {
			// Need to check if we're the shared holder - simplified for now
			entry.exclusive = txnID
			entry.sharedCount = 0
			return true
		}
		// Already hold exclusive
		if entry.exclusive == txnID {
			return true
		}
		return false
	}
	return false
}

// wakeWaiters wakes up waiting transactions.
func (m *Manager) wakeWaiters(entry *lockEntry) {
	for len(entry.waiters) > 0 {
		req := entry.waiters[0]

		if m.tryAcquire(entry, req.Type, req.TxnID) {
			req.Granted = true
			req.ResultChan <- nil
			entry.waiters = entry.waiters[1:]
			m.waitForGraph.RemoveEdge(req.TxnID, entry.exclusive)
			atomic.AddUint64(&m.grantCount, 1)
		} else {
			break
		}
	}
}

// removeWaiter removes a waiter from the queue.
func (m *Manager) removeWaiter(entry *lockEntry, req *LockRequest) {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	for i, w := range entry.waiters {
		if w == req {
			entry.waiters = append(entry.waiters[:i], entry.waiters[i+1:]...)
			break
		}
	}
}

// getOrCreateEntry gets or creates a lock entry.
func (m *Manager) getOrCreateEntry(id LockID) *lockEntry {
	switch id.Level {
	case LockLevelGlobal:
		return m.globalLock
	case LockLevelCatalog:
		return m.catalogLock
	case LockLevelTable:
		m.mu.Lock()
		if entry, ok := m.tableLocks[id.TableID]; ok {
			m.mu.Unlock()
			return entry
		}
		entry := &lockEntry{id: id}
		m.tableLocks[id.TableID] = entry
		m.mu.Unlock()
		return entry
	case LockLevelPage:
		key := fmt.Sprintf("%d:%d", id.TableID, id.PageID)
		m.mu.Lock()
		if entry, ok := m.pageLocks[key]; ok {
			m.mu.Unlock()
			return entry
		}
		entry := &lockEntry{id: id}
		m.pageLocks[key] = entry
		m.mu.Unlock()
		return entry
	case LockLevelRow:
		key := fmt.Sprintf("%d:%d:%d", id.TableID, id.PageID, id.RowID)
		m.mu.Lock()
		if entry, ok := m.rowLocks[key]; ok {
			m.mu.Unlock()
			return entry
		}
		entry := &lockEntry{id: id}
		m.rowLocks[key] = entry
		m.mu.Unlock()
		return entry
	}
	return nil
}

// TryLock attempts to acquire a lock without waiting.
func (m *Manager) TryLock(id LockID, lockType LockType, txnID uint64) bool {
	entry := m.getOrCreateEntry(id)
	if entry == nil {
		return false
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	return m.tryAcquire(entry, lockType, txnID)
}

// IsLocked checks if a resource is locked.
func (m *Manager) IsLocked(id LockID) bool {
	entry := m.getOrCreateEntry(id)
	if entry == nil {
		return false
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	return entry.exclusive != 0 || entry.sharedCount > 0
}

// GetLockInfo returns information about locks on a resource.
func (m *Manager) GetLockInfo(id LockID) (shared int, exclusive uint64) {
	entry := m.getOrCreateEntry(id)
	if entry == nil {
		return 0, 0
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	return int(entry.sharedCount), entry.exclusive
}

// Stats returns lock manager statistics.
func (m *Manager) Stats() LockStats {
	return LockStats{
		WaitCount:    atomic.LoadUint64(&m.waitCount),
		GrantCount:   atomic.LoadUint64(&m.grantCount),
		TimeoutCount: atomic.LoadUint64(&m.timeoutCount),
		TableLocks:   len(m.tableLocks),
		PageLocks:    len(m.pageLocks),
		RowLocks:     len(m.rowLocks),
	}
}

// LockStats holds lock manager statistics.
type LockStats struct {
	WaitCount    uint64 `json:"wait_count"`
	GrantCount   uint64 `json:"grant_count"`
	TimeoutCount uint64 `json:"timeout_count"`
	TableLocks   int    `json:"table_locks"`
	PageLocks    int    `json:"page_locks"`
	RowLocks     int    `json:"row_locks"`
}

// DetectDeadlock checks for deadlocks.
func (m *Manager) DetectDeadlock() []uint64 {
	return m.waitForGraph.DetectCycle()
}

// ReleaseAllLocks releases all locks held by a transaction.
func (m *Manager) ReleaseAllLocks(txnID uint64) {
	// Check and release global lock
	m.releaseLockIfHeld(m.globalLock, txnID)

	// Check and release catalog lock
	m.releaseLockIfHeld(m.catalogLock, txnID)

	// Check table locks
	m.mu.RLock()
	for _, entry := range m.tableLocks {
		m.releaseLockIfHeld(entry, txnID)
	}
	for _, entry := range m.pageLocks {
		m.releaseLockIfHeld(entry, txnID)
	}
	for _, entry := range m.rowLocks {
		m.releaseLockIfHeld(entry, txnID)
	}
	m.mu.RUnlock()
}

// releaseLockIfHeld releases a lock if held by the transaction.
func (m *Manager) releaseLockIfHeld(entry *lockEntry, txnID uint64) {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.exclusive == txnID {
		entry.exclusive = 0
		m.wakeWaiters(entry)
	}
	// Note: Shared locks are not tracked per transaction in this simplified version
}

// LockGlobal acquires the global lock.
func (m *Manager) LockGlobal(lockType LockType, txnID uint64) error {
	return m.Lock(LockID{Level: LockLevelGlobal}, lockType, txnID)
}

// UnlockGlobal releases the global lock.
func (m *Manager) UnlockGlobal(txnID uint64) error {
	return m.Unlock(LockID{Level: LockLevelGlobal}, txnID)
}

// LockCatalog acquires the catalog lock.
func (m *Manager) LockCatalog(lockType LockType, txnID uint64) error {
	return m.Lock(LockID{Level: LockLevelCatalog}, lockType, txnID)
}

// UnlockCatalog releases the catalog lock.
func (m *Manager) UnlockCatalog(txnID uint64) error {
	return m.Unlock(LockID{Level: LockLevelCatalog}, txnID)
}

// LockTable acquires a table lock.
func (m *Manager) LockTable(tableID uint64, lockType LockType, txnID uint64) error {
	return m.Lock(LockID{Level: LockLevelTable, TableID: tableID}, lockType, txnID)
}

// UnlockTable releases a table lock.
func (m *Manager) UnlockTable(tableID uint64, txnID uint64) error {
	return m.Unlock(LockID{Level: LockLevelTable, TableID: tableID}, txnID)
}

// LockPage acquires a page lock.
func (m *Manager) LockPage(tableID, pageID uint64, lockType LockType, txnID uint64) error {
	return m.Lock(LockID{Level: LockLevelPage, TableID: tableID, PageID: pageID}, lockType, txnID)
}

// UnlockPage releases a page lock.
func (m *Manager) UnlockPage(tableID, pageID uint64, txnID uint64) error {
	return m.Unlock(LockID{Level: LockLevelPage, TableID: tableID, PageID: pageID}, txnID)
}

// LockRow acquires a row lock.
func (m *Manager) LockRow(tableID, pageID, rowID uint64, lockType LockType, txnID uint64) error {
	return m.Lock(LockID{Level: LockLevelRow, TableID: tableID, PageID: pageID, RowID: rowID}, lockType, txnID)
}

// UnlockRow releases a row lock.
func (m *Manager) UnlockRow(tableID, pageID, rowID uint64, txnID uint64) error {
	return m.Unlock(LockID{Level: LockLevelRow, TableID: tableID, PageID: pageID, RowID: rowID}, txnID)
}
