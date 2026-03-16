// Package buffer provides buffer pool management for XxSql storage engine.
package buffer

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/topxeq/xxsql/internal/storage/page"
)

// BufferPool manages a pool of cached pages.
type BufferPool struct {
	// Configuration
	poolSize   int
	pageSize   int

	// Storage
	pages    map[page.PageID]*BufferFrame
	lruList  *list.List
	lruMap   map[page.PageID]*list.Element

	// Disk manager interface
	diskManager DiskManager

	// Statistics
	hits      uint64
	misses    uint64
	evictions uint64

	// Synchronization
	mu sync.RWMutex
}

// BufferFrame represents a frame in the buffer pool.
type BufferFrame struct {
	Page      *page.Page
	PinCount  int32
	Dirty     bool
	Element   *list.Element // LRU list element
}

// DiskManager provides disk I/O operations for the buffer pool.
type DiskManager interface {
	ReadPage(id page.PageID) (*page.Page, error)
	WritePage(p *page.Page) error
	AllocatePage() (page.PageID, error)
	DeallocatePage(id page.PageID) error
}

// BufferPoolConfig holds buffer pool configuration.
type BufferPoolConfig struct {
	PoolSize   int
	PageSize   int
	DiskManager DiskManager
}

// NewBufferPool creates a new buffer pool.
func NewBufferPool(config BufferPoolConfig) *BufferPool {
	if config.PoolSize <= 0 {
		config.PoolSize = 1000 // Default pool size
	}
	if config.PageSize <= 0 {
		config.PageSize = page.PageSize
	}

	return &BufferPool{
		poolSize:    config.PoolSize,
		pageSize:    config.PageSize,
		diskManager: config.DiskManager,
		pages:       make(map[page.PageID]*BufferFrame),
		lruList:     list.New(),
		lruMap:      make(map[page.PageID]*list.Element),
	}
}

// GetPage retrieves a page from the buffer pool or loads it from disk.
func (bp *BufferPool) GetPage(id page.PageID) (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is in buffer
	if frame, ok := bp.pages[id]; ok {
		atomic.AddUint64(&bp.hits, 1)
		frame.PinCount++
		bp.moveToFront(id)
		return frame.Page, nil
	}

	atomic.AddUint64(&bp.misses, 1)

	// Need to load from disk
	if bp.diskManager == nil {
		return nil, fmt.Errorf("no disk manager configured")
	}

	p, err := bp.diskManager.ReadPage(id)
	if err != nil {
		return nil, err
	}

	// Evict if necessary
	if len(bp.pages) >= bp.poolSize {
		if err := bp.evict(); err != nil {
			return nil, err
		}
	}

	// Add to buffer
	frame := &BufferFrame{
		Page:     p,
		PinCount: 1,
		Dirty:    false,
	}
	bp.pages[id] = frame
	frame.Element = bp.lruList.PushFront(id)
	bp.lruMap[id] = frame.Element

	return p, nil
}

// NewPage creates a new page and adds it to the buffer pool.
func (bp *BufferPool) NewPage() (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.diskManager == nil {
		return nil, fmt.Errorf("no disk manager configured")
	}

	// Allocate page ID
	id, err := bp.diskManager.AllocatePage()
	if err != nil {
		return nil, err
	}

	// Create new page
	p := page.NewPage(id, page.PageTypeData)
	p.SetLeaf(true)

	// Evict if necessary
	if len(bp.pages) >= bp.poolSize {
		if err := bp.evict(); err != nil {
			return nil, err
		}
	}

	// Add to buffer
	frame := &BufferFrame{
		Page:     p,
		PinCount: 1,
		Dirty:    true, // New pages are dirty
	}
	bp.pages[id] = frame
	frame.Element = bp.lruList.PushFront(id)
	bp.lruMap[id] = frame.Element

	return p, nil
}

// UnpinPage decrements the pin count of a page.
func (bp *BufferPool) UnpinPage(id page.PageID, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, ok := bp.pages[id]
	if !ok {
		return fmt.Errorf("page %d not in buffer", id)
	}

	if frame.PinCount <= 0 {
		return fmt.Errorf("page %d is not pinned", id)
	}

	frame.PinCount--
	if isDirty {
		frame.Dirty = true
	}

	// Move to front if unpinned (becomes candidate for eviction)
	if frame.PinCount == 0 {
		bp.moveToFront(id)
	}

	return nil
}

// FlushPage flushes a specific page to disk.
func (bp *BufferPool) FlushPage(id page.PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	return bp.flushPageInternal(id)
}

// flushPageInternal flushes a page without acquiring the lock.
func (bp *BufferPool) flushPageInternal(id page.PageID) error {
	frame, ok := bp.pages[id]
	if !ok {
		return nil // Page not in buffer
	}

	if !frame.Dirty {
		return nil // Nothing to flush
	}

	if bp.diskManager == nil {
		return fmt.Errorf("no disk manager configured")
	}

	if err := bp.diskManager.WritePage(frame.Page); err != nil {
		return err
	}

	frame.Dirty = false
	return nil
}

// FlushAll flushes all dirty pages to disk.
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for id := range bp.pages {
		if err := bp.flushPageInternal(id); err != nil {
			return err
		}
	}
	return nil
}

// DeletePage removes a page from the buffer pool.
func (bp *BufferPool) DeletePage(id page.PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, ok := bp.pages[id]
	if !ok {
		return nil // Page not in buffer
	}

	if frame.PinCount > 0 {
		return fmt.Errorf("cannot delete pinned page %d", id)
	}

	// Flush if dirty
	if frame.Dirty {
		if err := bp.flushPageInternal(id); err != nil {
			return err
		}
	}

	// Remove from structures
	delete(bp.pages, id)
	if elem, ok := bp.lruMap[id]; ok {
		bp.lruList.Remove(elem)
		delete(bp.lruMap, id)
	}

	// Deallocate on disk
	if bp.diskManager != nil {
		bp.diskManager.DeallocatePage(id)
	}

	return nil
}

// evict evicts the least recently used unpinned page.
func (bp *BufferPool) evict() error {
	// Find victim (least recently used unpinned page)
	var victimID page.PageID
	var victimFrame *BufferFrame

	for elem := bp.lruList.Back(); elem != nil; elem = elem.Prev() {
		id := elem.Value.(page.PageID)
		if frame, ok := bp.pages[id]; ok && frame.PinCount == 0 {
			victimID = id
			victimFrame = frame
			break
		}
	}

	if victimFrame == nil {
		return fmt.Errorf("no evictable pages in buffer pool")
	}

	// Flush if dirty
	if victimFrame.Dirty {
		if err := bp.flushPageInternal(victimID); err != nil {
			return err
		}
	}

	// Remove from structures
	delete(bp.pages, victimID)
	if elem, ok := bp.lruMap[victimID]; ok {
		bp.lruList.Remove(elem)
		delete(bp.lruMap, victimID)
	}

	atomic.AddUint64(&bp.evictions, 1)
	return nil
}

// moveToFront moves a page to the front of the LRU list.
func (bp *BufferPool) moveToFront(id page.PageID) {
	if elem, ok := bp.lruMap[id]; ok {
		bp.lruList.MoveToFront(elem)
	}
}

// Stats returns buffer pool statistics.
func (bp *BufferPool) Stats() BufferPoolStats {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	dirtyCount := 0
	pinnedCount := 0
	for _, frame := range bp.pages {
		if frame.Dirty {
			dirtyCount++
		}
		if frame.PinCount > 0 {
			pinnedCount++
		}
	}

	return BufferPoolStats{
		PoolSize:    bp.poolSize,
		PageCount:   len(bp.pages),
		DirtyPages:  dirtyCount,
		PinnedPages: pinnedCount,
		Hits:        atomic.LoadUint64(&bp.hits),
		Misses:      atomic.LoadUint64(&bp.misses),
		Evictions:   atomic.LoadUint64(&bp.evictions),
	}
}

// BufferPoolStats holds buffer pool statistics.
type BufferPoolStats struct {
	PoolSize    int    `json:"pool_size"`
	PageCount   int    `json:"page_count"`
	DirtyPages  int    `json:"dirty_pages"`
	PinnedPages int    `json:"pinned_pages"`
	Hits        uint64 `json:"hits"`
	Misses      uint64 `json:"misses"`
	Evictions   uint64 `json:"evictions"`
}

// HitRate returns the buffer pool hit rate.
func (s BufferPoolStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Size returns the number of pages in the buffer.
func (bp *BufferPool) Size() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return len(bp.pages)
}

// Clear removes all pages from the buffer pool.
func (bp *BufferPool) Clear() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Flush all dirty pages
	for id, frame := range bp.pages {
		if frame.Dirty {
			if err := bp.flushPageInternal(id); err != nil {
				return err
			}
		}
	}

	// Clear all structures
	bp.pages = make(map[page.PageID]*BufferFrame)
	bp.lruList = list.New()
	bp.lruMap = make(map[page.PageID]*list.Element)

	return nil
}

// SetDiskManager sets the disk manager.
func (bp *BufferPool) SetDiskManager(dm DiskManager) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.diskManager = dm
}

// PinPage increments the pin count of a page.
func (bp *BufferPool) PinPage(id page.PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, ok := bp.pages[id]
	if !ok {
		return fmt.Errorf("page %d not in buffer", id)
	}

	frame.PinCount++
	return nil
}

// IsDirty returns whether a page is dirty.
func (bp *BufferPool) IsDirty(id page.PageID) bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	if frame, ok := bp.pages[id]; ok {
		return frame.Dirty
	}
	return false
}

// MarkDirty marks a page as dirty.
func (bp *BufferPool) MarkDirty(id page.PageID) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if frame, ok := bp.pages[id]; ok {
		frame.Dirty = true
	}
}
