// Package lock_test provides tests for lock management.
package lock_test

import (
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/storage/lock"
)

func TestLockManagerBasic(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)
	txnID := uint64(1)

	// Acquire table lock
	err := lm.LockTable(1, lock.LockTypeExclusive, txnID)
	if err != nil {
		t.Fatalf("Failed to acquire table lock: %v", err)
	}

	// Check lock info
	shared, exclusive := lm.GetLockInfo(lock.LockID{Level: lock.LockLevelTable, TableID: 1})
	if exclusive != txnID {
		t.Errorf("Expected exclusive lock by txn %d, got %d", txnID, exclusive)
	}
	if shared != 0 {
		t.Errorf("Expected 0 shared locks, got %d", shared)
	}

	// Release lock
	err = lm.UnlockTable(1, txnID)
	if err != nil {
		t.Fatalf("Failed to release table lock: %v", err)
	}

	// Check lock is released
	shared, exclusive = lm.GetLockInfo(lock.LockID{Level: lock.LockLevelTable, TableID: 1})
	if exclusive != 0 {
		t.Error("Expected no exclusive lock after release")
	}
}

func TestLockManagerShared(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)

	// Multiple transactions can acquire shared locks
	for i := 1; i <= 3; i++ {
		err := lm.LockTable(1, lock.LockTypeShared, uint64(i))
		if err != nil {
			t.Fatalf("Failed to acquire shared lock for txn %d: %v", i, err)
		}
	}

	// Check shared count
	shared, exclusive := lm.GetLockInfo(lock.LockID{Level: lock.LockLevelTable, TableID: 1})
	if shared != 3 {
		t.Errorf("Expected 3 shared locks, got %d", shared)
	}
	if exclusive != 0 {
		t.Error("Expected no exclusive lock")
	}

	// Release all
	for i := 1; i <= 3; i++ {
		lm.UnlockTable(1, uint64(i))
	}
}

func TestLockManagerExclusiveBlocks(t *testing.T) {
	lm := lock.NewManager(100 * time.Millisecond)

	// Acquire exclusive lock
	err := lm.LockTable(1, lock.LockTypeExclusive, 1)
	if err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Try to acquire another exclusive lock (should timeout)
	err = lm.LockTable(1, lock.LockTypeExclusive, 2)
	if err == nil {
		t.Error("Expected timeout error for conflicting lock")
	}

	// Release first lock
	lm.UnlockTable(1, 1)

	// Now should be able to acquire
	err = lm.LockTable(1, lock.LockTypeExclusive, 2)
	if err != nil {
		t.Fatalf("Should be able to acquire lock after release: %v", err)
	}
}

func TestLockManagerTryLock(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)

	// Acquire exclusive lock
	lm.LockTable(1, lock.LockTypeExclusive, 1)

	// TryLock should fail
	if lm.TryLock(lock.LockID{Level: lock.LockLevelTable, TableID: 1}, lock.LockTypeExclusive, 2) {
		t.Error("TryLock should fail when lock is held")
	}

	// Release and try again
	lm.UnlockTable(1, 1)

	if !lm.TryLock(lock.LockID{Level: lock.LockLevelTable, TableID: 1}, lock.LockTypeExclusive, 2) {
		t.Error("TryLock should succeed when lock is free")
	}
}

func TestLockManagerGlobalLock(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)

	// Acquire global lock
	err := lm.LockGlobal(lock.LockTypeExclusive, 1)
	if err != nil {
		t.Fatalf("Failed to acquire global lock: %v", err)
	}

	// Check it's locked
	if !lm.IsLocked(lock.LockID{Level: lock.LockLevelGlobal}) {
		t.Error("Global lock should be held")
	}

	// Try to acquire another global lock (should timeout)
	err = lm.LockGlobal(lock.LockTypeExclusive, 2)
	if err == nil {
		t.Error("Expected timeout when global lock is held by another transaction")
	}

	// Release global lock
	lm.UnlockGlobal(1)

	// Check it's unlocked
	if lm.IsLocked(lock.LockID{Level: lock.LockLevelGlobal}) {
		t.Error("Global lock should be released")
	}

	// Now should work
	err = lm.LockGlobal(lock.LockTypeExclusive, 2)
	if err != nil {
		t.Fatalf("Should be able to acquire global lock: %v", err)
	}
}

func TestLockManagerRowLock(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)

	// Acquire row lock
	err := lm.LockRow(1, 1, 100, lock.LockTypeExclusive, 1)
	if err != nil {
		t.Fatalf("Failed to acquire row lock: %v", err)
	}

	// Check it's locked
	if !lm.IsLocked(lock.LockID{Level: lock.LockLevelRow, TableID: 1, PageID: 1, RowID: 100}) {
		t.Error("Row should be locked")
	}

	// Release
	lm.UnlockRow(1, 1, 100, 1)

	// Check it's unlocked
	if lm.IsLocked(lock.LockID{Level: lock.LockLevelRow, TableID: 1, PageID: 1, RowID: 100}) {
		t.Error("Row should be unlocked after release")
	}
}

func TestLockManagerStats(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)

	// Acquire some locks
	lm.LockTable(1, lock.LockTypeExclusive, 1)
	lm.LockPage(1, 1, lock.LockTypeShared, 2)
	lm.LockRow(1, 1, 100, lock.LockTypeExclusive, 3)

	stats := lm.Stats()

	if stats.TableLocks != 1 {
		t.Errorf("Expected 1 table lock, got %d", stats.TableLocks)
	}
	if stats.PageLocks != 1 {
		t.Errorf("Expected 1 page lock, got %d", stats.PageLocks)
	}
	if stats.RowLocks != 1 {
		t.Errorf("Expected 1 row lock, got %d", stats.RowLocks)
	}
}

func TestWaitForGraph(t *testing.T) {
	wfg := lock.NewWaitForGraph()

	// Add edges: T1 -> T2 -> T3
	wfg.AddEdge(1, 2)
	wfg.AddEdge(2, 3)

	// No cycle
	cycle := wfg.DetectCycle()
	if cycle != nil {
		t.Errorf("Expected no cycle, got %v", cycle)
	}

	// Add edge: T3 -> T1 (creates cycle)
	wfg.AddEdge(3, 1)

	// Should detect cycle
	cycle = wfg.DetectCycle()
	if cycle == nil {
		t.Error("Expected to detect cycle")
	}

	// Remove edge to break cycle
	wfg.RemoveEdge(3, 1)

	// No cycle now
	cycle = wfg.DetectCycle()
	if cycle != nil {
		t.Errorf("Expected no cycle after removal, got %v", cycle)
	}
}

func TestLatchManagerBasic(t *testing.T) {
	lm := lock.NewLatchManager(1 * time.Second)

	// Acquire exclusive latch
	if !lm.Acquire(1, lock.LatchTypeExclusive) {
		t.Error("Failed to acquire exclusive latch")
	}

	// Release
	lm.Release(1, lock.LatchTypeExclusive)

	// Acquire shared latch
	if !lm.Acquire(1, lock.LatchTypeShared) {
		t.Error("Failed to acquire shared latch")
	}

	// Release
	lm.Release(1, lock.LatchTypeShared)
}

func TestLatchManagerTryAcquire(t *testing.T) {
	lm := lock.NewLatchManager(1 * time.Second)

	// Acquire exclusive latch
	lm.Acquire(1, lock.LatchTypeExclusive)

	// Try acquire should fail
	if lm.TryAcquire(1, lock.LatchTypeExclusive) {
		t.Error("TryAcquire should fail when latch is held")
	}

	// Release
	lm.Release(1, lock.LatchTypeExclusive)

	// Now try acquire should succeed
	if !lm.TryAcquire(1, lock.LatchTypeExclusive) {
		t.Error("TryAcquire should succeed when latch is free")
	}
}

func TestCrabbingProtocol(t *testing.T) {
	lm := lock.NewLatchManager(1 * time.Second)
	cp := lock.NewCrabbingProtocol(lm)

	// Acquire root
	if !cp.AcquireRoot(1, lock.LatchTypeShared) {
		t.Error("Failed to acquire root latch")
	}

	if cp.HeldCount() != 1 {
		t.Errorf("Expected 1 held latch, got %d", cp.HeldCount())
	}

	// Crab down (release parent if safe)
	if !cp.CrabDown(1, 2, lock.LatchTypeShared, true) {
		t.Error("Failed to crab down")
	}

	// Should still have 1 latch (parent released)
	if cp.HeldCount() != 1 {
		t.Errorf("Expected 1 held latch after crab down, got %d", cp.HeldCount())
	}

	// Release all
	cp.ReleaseAll()

	if cp.HeldCount() != 0 {
		t.Errorf("Expected 0 held latches after release all, got %d", cp.HeldCount())
	}
}

func TestLatchManagerStats(t *testing.T) {
	lm := lock.NewLatchManager(1 * time.Second)

	// Acquire some latches
	lm.Acquire(1, lock.LatchTypeExclusive)
	lm.Acquire(2, lock.LatchTypeShared)
	lm.Acquire(3, lock.LatchTypeExclusive)

	stats := lm.Stats()

	if stats.LatchCount != 3 {
		t.Errorf("Expected 3 latches, got %d", stats.LatchCount)
	}
}

func TestReleaseAllLocks(t *testing.T) {
	lm := lock.NewManager(5 * time.Second)

	// Acquire multiple locks for same transaction
	lm.LockTable(1, lock.LockTypeExclusive, 1)
	lm.LockTable(2, lock.LockTypeExclusive, 1)
	lm.LockPage(1, 1, lock.LockTypeExclusive, 1)

	// Release all
	lm.ReleaseAllLocks(1)

	// Check all released
	if lm.IsLocked(lock.LockID{Level: lock.LockLevelTable, TableID: 1}) {
		t.Error("Table 1 should be unlocked")
	}
	if lm.IsLocked(lock.LockID{Level: lock.LockLevelTable, TableID: 2}) {
		t.Error("Table 2 should be unlocked")
	}
	if lm.IsLocked(lock.LockID{Level: lock.LockLevelPage, TableID: 1, PageID: 1}) {
		t.Error("Page 1:1 should be unlocked")
	}
}
