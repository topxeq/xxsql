package lock

import (
	"testing"
	"time"
)

func TestLatchType_String(t *testing.T) {
	tests := []struct {
		latchType LatchType
		want      string
	}{
		{LatchTypeShared, "SHARED"},
		{LatchTypeExclusive, "EXCLUSIVE"},
		{LatchType(99), "EXCLUSIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.latchType.String(); got != tt.want {
				t.Errorf("LatchType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLatch_AcquireAndRelease(t *testing.T) {
	l := &Latch{}

	// Test shared acquire
	if !l.Acquire(LatchTypeShared, time.Second) {
		t.Error("failed to acquire shared latch")
	}
	l.Release(LatchTypeShared)

	// Test exclusive acquire
	if !l.Acquire(LatchTypeExclusive, time.Second) {
		t.Error("failed to acquire exclusive latch")
	}
	l.Release(LatchTypeExclusive)
}

func TestLatch_TryAcquire(t *testing.T) {
	l := &Latch{}

	// Test TryAcquire for shared
	if !l.TryAcquire(LatchTypeShared) {
		t.Error("TryAcquire shared failed")
	}
	l.Release(LatchTypeShared)

	// Test TryAcquire for exclusive
	if !l.TryAcquire(LatchTypeExclusive) {
		t.Error("TryAcquire exclusive failed")
	}
	l.Release(LatchTypeExclusive)
}

func TestLatch_ConcurrentShared(t *testing.T) {
	l := &Latch{}
	done := make(chan bool, 2)

	// Multiple goroutines should be able to acquire shared latch
	for i := 0; i < 2; i++ {
		go func() {
			if l.Acquire(LatchTypeShared, time.Second) {
				time.Sleep(10 * time.Millisecond)
				l.Release(LatchTypeShared)
				done <- true
			} else {
				done <- false
			}
		}()
	}

	for i := 0; i < 2; i++ {
		if !<-done {
			t.Error("concurrent shared acquire failed")
		}
	}
}

func TestLatch_Timeout(t *testing.T) {
	l := &Latch{}

	// Acquire exclusive
	if !l.Acquire(LatchTypeExclusive, time.Second) {
		t.Fatal("failed to acquire exclusive latch")
	}

	// Try to acquire again with short timeout - should fail
	done := make(chan bool)
	go func() {
		done <- l.Acquire(LatchTypeExclusive, 50*time.Millisecond)
	}()

	// Wait a bit for the goroutine to start waiting
	time.Sleep(20 * time.Millisecond)

	select {
	case success := <-done:
		if success {
			t.Error("expected timeout, but latch was acquired")
		}
	case <-time.After(time.Second):
		t.Error("test took too long")
	}

	l.Release(LatchTypeExclusive)
}

func TestLatchManager_NewLatchManager(t *testing.T) {
	lm := NewLatchManager(time.Second)
	if lm == nil {
		t.Error("expected non-nil LatchManager")
	}
}

func TestLatchManager_AcquireAndRelease(t *testing.T) {
	lm := NewLatchManager(time.Second)

	pageID := uint64(1)

	// Acquire shared
	if !lm.Acquire(pageID, LatchTypeShared) {
		t.Error("failed to acquire shared latch")
	}
	lm.Release(pageID, LatchTypeShared)

	// Acquire exclusive
	if !lm.Acquire(pageID, LatchTypeExclusive) {
		t.Error("failed to acquire exclusive latch")
	}
	lm.Release(pageID, LatchTypeExclusive)
}

func TestLatchManager_TryAcquire(t *testing.T) {
	lm := NewLatchManager(time.Second)

	pageID := uint64(1)

	// TryAcquire for shared
	if !lm.TryAcquire(pageID, LatchTypeShared) {
		t.Error("TryAcquire shared failed")
	}
	lm.Release(pageID, LatchTypeShared)

	// TryAcquire for exclusive
	if !lm.TryAcquire(pageID, LatchTypeExclusive) {
		t.Error("TryAcquire exclusive failed")
	}
	lm.Release(pageID, LatchTypeExclusive)
}

func TestCrabbingProtocol_AcquireRoot(t *testing.T) {
	lm := NewLatchManager(time.Second)
	cp := NewCrabbingProtocol(lm)

	rootID := uint64(1)
	if !cp.AcquireRoot(rootID, LatchTypeExclusive) {
		t.Error("failed to acquire root")
	}
	if cp.HeldCount() != 1 {
		t.Errorf("expected 1 held latch, got %d", cp.HeldCount())
	}
	cp.ReleaseAll()
}

func TestCrabbingProtocol_CrabDown(t *testing.T) {
	lm := NewLatchManager(time.Second)
	cp := NewCrabbingProtocol(lm)

	parentID := uint64(1)
	childID := uint64(2)

	// First acquire parent
	if !cp.AcquireRoot(parentID, LatchTypeExclusive) {
		t.Fatal("failed to acquire parent")
	}

	// Crab down to child (release parent since safe=true)
	if !cp.CrabDown(parentID, childID, LatchTypeShared, true) {
		t.Error("failed to crab down")
	}

	// Should still have 1 latch (parent released)
	if cp.HeldCount() != 1 {
		t.Errorf("expected 1 held latch after crab down, got %d", cp.HeldCount())
	}

	cp.ReleaseAll()
}

func TestCrabbingProtocol_CrabDownNotSafe(t *testing.T) {
	lm := NewLatchManager(time.Second)
	cp := NewCrabbingProtocol(lm)

	parentID := uint64(1)
	childID := uint64(2)

	// First acquire parent
	if !cp.AcquireRoot(parentID, LatchTypeExclusive) {
		t.Fatal("failed to acquire parent")
	}

	// Crab down to child (keep parent since safe=false)
	if !cp.CrabDown(parentID, childID, LatchTypeShared, false) {
		t.Error("failed to crab down")
	}

	// Should have 2 latches
	if cp.HeldCount() != 2 {
		t.Errorf("expected 2 held latches after crab down, got %d", cp.HeldCount())
	}

	cp.ReleaseAll()
}

func TestLatchManager_Clear(t *testing.T) {
	lm := NewLatchManager(time.Second)

	pageID := uint64(1)
	lm.Acquire(pageID, LatchTypeExclusive)

	lm.Clear()
}

func TestLatchManager_Remove(t *testing.T) {
	lm := NewLatchManager(time.Second)

	pageID := uint64(1)
	lm.Acquire(pageID, LatchTypeExclusive)

	lm.Remove(pageID)
}

func TestLatchManager_Stats(t *testing.T) {
	lm := NewLatchManager(time.Second)

	// Acquire some latches
	lm.Acquire(1, LatchTypeExclusive)
	lm.Acquire(2, LatchTypeShared)
	lm.Acquire(3, LatchTypeExclusive)

	stats := lm.Stats()

	if stats.LatchCount != 3 {
		t.Errorf("expected 3 latches, got %d", stats.LatchCount)
	}
}

func TestLatchManager_DefaultTimeout(t *testing.T) {
	// Test with zero timeout - should use default
	lm := NewLatchManager(0)
	if lm == nil {
		t.Error("expected non-nil LatchManager")
	}
}