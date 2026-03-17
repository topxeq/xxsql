// Package lock provides latch management for B+ tree operations.
package lock

import (
	"sync"
	"sync/atomic"
	"time"
)

// LatchType represents the type of latch.
type LatchType uint8

const (
	LatchTypeShared LatchType = iota
	LatchTypeExclusive
)

// String returns the string representation.
func (t LatchType) String() string {
	if t == LatchTypeShared {
		return "SHARED"
	}
	return "EXCLUSIVE"
}

// Latch is a lightweight lock for B+ tree nodes.
type Latch struct {
	// Use a read-write mutex for efficient shared/exclusive locking
	mu sync.RWMutex

	// Track exclusive holder for debugging (only valid when exclusively locked)
	holder uint64

	// Statistics (protected by atomic operations)
	acquireCount uint64
	waitCount    uint64
}

// Acquire acquires a latch.
func (l *Latch) Acquire(latchType LatchType, timeout time.Duration) bool {
	atomic.AddUint64(&l.acquireCount, 1)

	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	// Create a channel to signal acquisition
	done := make(chan struct{})

	go func() {
		switch latchType {
		case LatchTypeShared:
			l.mu.RLock()
		case LatchTypeExclusive:
			l.mu.Lock()
		}
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		atomic.AddUint64(&l.waitCount, 1)
		return false
	}
}

// Release releases a latch.
func (l *Latch) Release(latchType LatchType) {
	switch latchType {
	case LatchTypeShared:
		l.mu.RUnlock()
	case LatchTypeExclusive:
		l.holder = 0
		l.mu.Unlock()
	}
}

// TryAcquire attempts to acquire a latch without blocking.
func (l *Latch) TryAcquire(latchType LatchType) bool {
	switch latchType {
	case LatchTypeShared:
		return l.mu.TryRLock()
	case LatchTypeExclusive:
		return l.mu.TryLock()
	}
	return false
}

// LatchManager manages latches for B+ tree pages.
type LatchManager struct {
	latches map[uint64]*Latch
	mu      sync.RWMutex

	// Configuration
	timeout time.Duration

	// Statistics
	totalAcquires uint64
	totalWaits    uint64
}

// NewLatchManager creates a new latch manager.
func NewLatchManager(timeout time.Duration) *LatchManager {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &LatchManager{
		latches: make(map[uint64]*Latch),
		timeout: timeout,
	}
}

// getOrCreateLatch gets or creates a latch for a page.
func (m *LatchManager) getOrCreateLatch(pageID uint64) *Latch {
	m.mu.RLock()
	if latch, ok := m.latches[pageID]; ok {
		m.mu.RUnlock()
		return latch
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check
	if latch, ok := m.latches[pageID]; ok {
		return latch
	}

	latch := &Latch{}
	m.latches[pageID] = latch
	return latch
}

// Acquire acquires a latch on a page.
func (m *LatchManager) Acquire(pageID uint64, latchType LatchType) bool {
	atomic.AddUint64(&m.totalAcquires, 1)

	latch := m.getOrCreateLatch(pageID)
	return latch.Acquire(latchType, m.timeout)
}

// Release releases a latch on a page.
func (m *LatchManager) Release(pageID uint64, latchType LatchType) {
	m.mu.RLock()
	latch, ok := m.latches[pageID]
	m.mu.RUnlock()

	if ok {
		latch.Release(latchType)
	}
}

// TryAcquire attempts to acquire a latch without blocking.
func (m *LatchManager) TryAcquire(pageID uint64, latchType LatchType) bool {
	latch := m.getOrCreateLatch(pageID)
	return latch.TryAcquire(latchType)
}

// CrabbingProtocol implements the crabbing protocol for B+ tree traversal.
// Acquires latches from root to leaf, releasing parent latches when safe.
type CrabbingProtocol struct {
	manager   *LatchManager
	heldLatches []uint64
	heldTypes   []LatchType
}

// NewCrabbingProtocol creates a new crabbing protocol handler.
func NewCrabbingProtocol(manager *LatchManager) *CrabbingProtocol {
	return &CrabbingProtocol{
		manager:     manager,
		heldLatches: make([]uint64, 0),
		heldTypes:   make([]LatchType, 0),
	}
}

// AcquireRoot acquires a latch on the root page.
func (c *CrabbingProtocol) AcquireRoot(pageID uint64, latchType LatchType) bool {
	if c.manager.Acquire(pageID, latchType) {
		c.heldLatches = append(c.heldLatches, pageID)
		c.heldTypes = append(c.heldTypes, latchType)
		return true
	}
	return false
}

// CrabDown moves from parent to child, releasing parent if safe.
func (c *CrabbingProtocol) CrabDown(parentID, childID uint64, childType LatchType, isSafe bool) bool {
	// Acquire child latch first
	if !c.manager.Acquire(childID, childType) {
		return false
	}

	// Add child to held latches
	c.heldLatches = append(c.heldLatches, childID)
	c.heldTypes = append(c.heldTypes, childType)

	// If safe, release parent
	if isSafe && len(c.heldLatches) >= 2 {
		// Release the parent (second to last)
		parentIdx := len(c.heldLatches) - 2
		c.manager.Release(c.heldLatches[parentIdx], c.heldTypes[parentIdx])
		c.heldLatches = append(c.heldLatches[:parentIdx], c.heldLatches[parentIdx+1:]...)
		c.heldTypes = append(c.heldTypes[:parentIdx], c.heldTypes[parentIdx+1:]...)
	}

	return true
}

// ReleaseAll releases all held latches.
func (c *CrabbingProtocol) ReleaseAll() {
	for i, pageID := range c.heldLatches {
		c.manager.Release(pageID, c.heldTypes[i])
	}
	c.heldLatches = c.heldLatches[:0]
	c.heldTypes = c.heldTypes[:0]
}

// HeldCount returns the number of held latches.
func (c *CrabbingProtocol) HeldCount() int {
	return len(c.heldLatches)
}

// Stats returns latch manager statistics.
func (m *LatchManager) Stats() LatchStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return LatchStats{
		LatchCount:    len(m.latches),
		TotalAcquires: atomic.LoadUint64(&m.totalAcquires),
		TotalWaits:    atomic.LoadUint64(&m.totalWaits),
	}
}

// LatchStats holds latch manager statistics.
type LatchStats struct {
	LatchCount    int    `json:"latch_count"`
	TotalAcquires uint64 `json:"total_acquires"`
	TotalWaits    uint64 `json:"total_waits"`
}

// Clear removes all latches (use with caution).
func (m *LatchManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latches = make(map[uint64]*Latch)
}

// Remove removes a latch for a page.
func (m *LatchManager) Remove(pageID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.latches, pageID)
}
