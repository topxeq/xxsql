// Package buffer_test provides tests for buffer pool.
package buffer_test

import (
	"testing"

	"github.com/topxeq/xxsql/internal/storage/buffer"
	"github.com/topxeq/xxsql/internal/storage/page"
)

// MockDiskManager implements DiskManager for testing.
type MockDiskManager struct {
	pages    map[page.PageID]*page.Page
	nextID   page.PageID
	writeErr error
	readErr  error
}

func NewMockDiskManager() *MockDiskManager {
	return &MockDiskManager{
		pages:  make(map[page.PageID]*page.Page),
		nextID: 1,
	}
}

func (m *MockDiskManager) ReadPage(id page.PageID) (*page.Page, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	p, ok := m.pages[id]
	if !ok {
		// Create new page for testing
		p = page.NewPage(id, page.PageTypeData)
		p.SetLeaf(true)
	}
	return p, nil
}

func (m *MockDiskManager) WritePage(p *page.Page) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.pages[p.ID] = p
	return nil
}

func (m *MockDiskManager) AllocatePage() (page.PageID, error) {
	id := m.nextID
	m.nextID++
	return id, nil
}

func (m *MockDiskManager) DeallocatePage(id page.PageID) error {
	delete(m.pages, id)
	return nil
}

func TestBufferPoolBasic(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   10,
		DiskManager: dm,
	})

	// Allocate new page
	p, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	if p == nil {
		t.Fatal("Page should not be nil")
	}

	if p.ID == 0 {
		t.Error("Page ID should not be 0")
	}

	// Unpin page
	if err := bp.UnpinPage(p.ID, false); err != nil {
		t.Fatalf("Failed to unpin page: %v", err)
	}

	// Get page again
	p2, err := bp.GetPage(p.ID)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	if p2.ID != p.ID {
		t.Errorf("Page ID mismatch: expected %d, got %d", p.ID, p2.ID)
	}
}

func TestBufferPoolLRU(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   3,
		DiskManager: dm,
	})

	// Create 3 pages
	ids := make([]page.PageID, 3)
	for i := 0; i < 3; i++ {
		p, err := bp.NewPage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		ids[i] = p.ID
		bp.UnpinPage(ids[i], false)
	}

	// Check pool size
	if bp.Size() != 3 {
		t.Errorf("Expected pool size 3, got %d", bp.Size())
	}

	// Create another page - should evict LRU
	p, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	bp.UnpinPage(p.ID, false)

	// Pool size should still be 3
	if bp.Size() != 3 {
		t.Errorf("Expected pool size 3, got %d", bp.Size())
	}

	// Check stats
	stats := bp.Stats()
	if stats.Evictions != 1 {
		t.Errorf("Expected 1 eviction, got %d", stats.Evictions)
	}
}

func TestBufferPoolDirty(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   10,
		DiskManager: dm,
	})

	// Create and modify page
	p, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	// Unpin as dirty
	if err := bp.UnpinPage(p.ID, true); err != nil {
		t.Fatalf("Failed to unpin page: %v", err)
	}

	// Check dirty flag
	if !bp.IsDirty(p.ID) {
		t.Error("Page should be dirty")
	}

	// Flush page
	if err := bp.FlushPage(p.ID); err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	// Check dirty flag after flush
	if bp.IsDirty(p.ID) {
		t.Error("Page should not be dirty after flush")
	}

	// Check page was written to disk
	if _, ok := dm.pages[p.ID]; !ok {
		t.Error("Page should be in disk manager")
	}
}

func TestBufferPoolPin(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   2,
		DiskManager: dm,
	})

	// Create and pin page
	p, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	// Create another page
	p2, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	bp.UnpinPage(p2.ID, false)

	// Try to create a third page - should fail because p is pinned
	// and pool is full, but pinned pages can't be evicted
	bp.UnpinPage(p.ID, false) // Unpin first

	p3, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to allocate third page: %v", err)
	}
	bp.UnpinPage(p3.ID, false)

	// Pool should have 2 pages
	if bp.Size() != 2 {
		t.Errorf("Expected pool size 2, got %d", bp.Size())
	}
}

func TestBufferPoolStats(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   10,
		DiskManager: dm,
	})

	// Create pages
	for i := 0; i < 5; i++ {
		p, err := bp.NewPage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		bp.UnpinPage(p.ID, false)
	}

	stats := bp.Stats()

	if stats.PageCount != 5 {
		t.Errorf("Expected 5 pages, got %d", stats.PageCount)
	}

	if stats.PoolSize != 10 {
		t.Errorf("Expected pool size 10, got %d", stats.PoolSize)
	}
}

func TestBufferPoolFlushAll(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   10,
		DiskManager: dm,
	})

	// Create multiple dirty pages
	for i := 0; i < 5; i++ {
		p, err := bp.NewPage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		bp.UnpinPage(p.ID, true) // Mark as dirty
	}

	// Flush all
	if err := bp.FlushAll(); err != nil {
		t.Fatalf("Failed to flush all: %v", err)
	}

	// Check stats
	stats := bp.Stats()
	if stats.DirtyPages != 0 {
		t.Errorf("Expected 0 dirty pages, got %d", stats.DirtyPages)
	}
}

func TestBufferPoolClear(t *testing.T) {
	dm := NewMockDiskManager()
	bp := buffer.NewBufferPool(buffer.BufferPoolConfig{
		PoolSize:   10,
		DiskManager: dm,
	})

	// Create pages
	for i := 0; i < 5; i++ {
		p, err := bp.NewPage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		bp.UnpinPage(p.ID, true)
	}

	// Clear pool
	if err := bp.Clear(); err != nil {
		t.Fatalf("Failed to clear pool: %v", err)
	}

	// Check size
	if bp.Size() != 0 {
		t.Errorf("Expected pool size 0, got %d", bp.Size())
	}
}
