// Package sequence provides atomic sequence counters for XxSql.
package sequence

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Sequence represents a named sequence counter.
type Sequence struct {
	Name        string
	Current     int64
	Increment   int64
	MinValue    int64
	MaxValue    int64
	StartValue  int64
	Cycle       bool
	CacheSize   int32 // Number of values to cache in memory
	CacheEnd    int64 // End of current cache range
	Created     time.Time
	LastUpdated time.Time
}

// SequenceConfig holds configuration for creating a sequence.
type SequenceConfig struct {
	Name       string
	Start      int64
	Increment  int64
	MinValue   int64
	MaxValue   int64
	Cycle      bool
	CacheSize  int32
}

// DefaultSequenceConfig returns default configuration for a sequence.
func DefaultSequenceConfig(name string) SequenceConfig {
	return SequenceConfig{
		Name:      name,
		Start:     1,
		Increment: 1,
		MinValue:  1,
		MaxValue:  9223372036854775807, // MaxInt64
		Cycle:     false,
		CacheSize: 1,
	}
}

// Manager manages multiple sequences.
type Manager struct {
	mu        sync.RWMutex
	sequences map[string]*Sequence
	path      string // Path to sequence storage file

	// Atomic counter for persistence version
	version   atomic.Uint64
	dirty     atomic.Bool
	flushChan chan struct{}
	stopChan  chan struct{}
	stopped   atomic.Bool

	// WAL integration
	walLogger  WALLogger
	syncPeriod time.Duration
}

// WALLogger defines the interface for WAL logging.
type WALLogger interface {
	LogSequenceCreate(name string, startValue int64) error
	LogSequenceDrop(name string) error
	LogSequenceNext(name string, value int64) error
}

// NewManager creates a new sequence manager.
func NewManager(path string, walLogger WALLogger) *Manager {
	m := &Manager{
		sequences:  make(map[string]*Sequence),
		path:       path,
		walLogger:  walLogger,
		flushChan:  make(chan struct{}, 1),
		stopChan:   make(chan struct{}),
		syncPeriod: 5 * time.Second,
	}

	// Start background flush goroutine
	go m.backgroundFlush()

	return m
}

// CreateSequence creates a new sequence.
func (m *Manager) CreateSequence(config SequenceConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sequences[config.Name]; exists {
		return fmt.Errorf("sequence %s already exists", config.Name)
	}

	// Validate configuration
	if config.MinValue > config.MaxValue {
		return fmt.Errorf("minvalue %d cannot be greater than maxvalue %d", config.MinValue, config.MaxValue)
	}
	if config.Start < config.MinValue {
		return fmt.Errorf("start value %d cannot be less than minvalue %d", config.Start, config.MinValue)
	}
	if config.Start > config.MaxValue {
		return fmt.Errorf("start value %d cannot be greater than maxvalue %d", config.Start, config.MaxValue)
	}
	if config.Increment == 0 {
		return fmt.Errorf("increment cannot be zero")
	}

	seq := &Sequence{
		Name:        config.Name,
		Current:     config.Start - config.Increment, // Start before first value
		Increment:   config.Increment,
		MinValue:    config.MinValue,
		MaxValue:    config.MaxValue,
		StartValue:  config.Start,
		Cycle:       config.Cycle,
		CacheSize:   config.CacheSize,
		CacheEnd:    config.Start - config.Increment,
		Created:     time.Now(),
		LastUpdated: time.Now(),
	}

	// Initialize cache
	if seq.CacheSize > 1 {
		seq.CacheEnd = seq.Current + int64(seq.CacheSize)*seq.Increment
	}

	m.sequences[config.Name] = seq
	m.markDirty()

	// Log to WAL
	if m.walLogger != nil {
		if err := m.walLogger.LogSequenceCreate(config.Name, config.Start); err != nil {
			// Log error but don't fail
		}
	}

	return nil
}

// DropSequence drops a sequence.
func (m *Manager) DropSequence(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sequences[name]; !exists {
		return fmt.Errorf("sequence %s does not exist", name)
	}

	delete(m.sequences, name)
	m.markDirty()

	// Log to WAL
	if m.walLogger != nil {
		if err := m.walLogger.LogSequenceDrop(name); err != nil {
			// Log error but don't fail
		}
	}

	return nil
}

// NextValue returns the next value from a sequence.
// This is thread-safe and uses atomic operations.
func (m *Manager) NextValue(name string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	seq, exists := m.sequences[name]
	if !exists {
		return 0, fmt.Errorf("sequence %s does not exist", name)
	}

	// Check if we need to fetch from cache or compute next
	nextValue := seq.Current + seq.Increment

	// Handle cache
	if seq.CacheSize > 1 {
		if nextValue > seq.CacheEnd {
			// Extend cache
			seq.CacheEnd = nextValue + int64(seq.CacheSize-1)*seq.Increment
		}
	}

	// Check bounds
	if nextValue < seq.MinValue {
		if seq.Cycle {
			nextValue = seq.MaxValue
		} else {
			return 0, fmt.Errorf("sequence %s has reached minimum value", name)
		}
	}
	if nextValue > seq.MaxValue {
		if seq.Cycle {
			nextValue = seq.MinValue
		} else {
			return 0, fmt.Errorf("sequence %s has reached maximum value", name)
		}
	}

	seq.Current = nextValue
	seq.LastUpdated = time.Now()
	m.markDirty()

	// Log to WAL (asynchronously for performance)
	if m.walLogger != nil {
		go func() {
			// Use goroutine to not block the NextValue call
			// In production, you might batch these
			m.walLogger.LogSequenceNext(name, nextValue)
		}()
	}

	return nextValue, nil
}

// CurrentValue returns the current value of a sequence without advancing it.
func (m *Manager) CurrentValue(name string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seq, exists := m.sequences[name]
	if !exists {
		return 0, fmt.Errorf("sequence %s does not exist", name)
	}

	return seq.Current, nil
}

// SetCurrentValue sets the current value of a sequence.
// This is used for recovery and administrative purposes.
func (m *Manager) SetCurrentValue(name string, value int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	seq, exists := m.sequences[name]
	if !exists {
		return fmt.Errorf("sequence %s does not exist", name)
	}

	if value < seq.MinValue || value > seq.MaxValue {
		return fmt.Errorf("value %d is out of sequence bounds", value)
	}

	seq.Current = value
	seq.LastUpdated = time.Now()
	m.markDirty()

	return nil
}

// Reset resets a sequence to its start value.
func (m *Manager) Reset(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	seq, exists := m.sequences[name]
	if !exists {
		return fmt.Errorf("sequence %s does not exist", name)
	}

	seq.Current = seq.StartValue - seq.Increment
	seq.CacheEnd = seq.Current
	if seq.CacheSize > 1 {
		seq.CacheEnd = seq.Current + int64(seq.CacheSize)*seq.Increment
	}
	seq.LastUpdated = time.Now()
	m.markDirty()

	return nil
}

// GetSequence returns information about a sequence.
func (m *Manager) GetSequence(name string) (*Sequence, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seq, exists := m.sequences[name]
	if !exists {
		return nil, fmt.Errorf("sequence %s does not exist", name)
	}

	// Return a copy
	copy := *seq
	return &copy, nil
}

// ListSequences returns a list of all sequence names.
func (m *Manager) ListSequences() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.sequences))
	for name := range m.sequences {
		names = append(names, name)
	}
	return names
}

// Stats returns statistics about the sequence manager.
func (m *Manager) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Stats{
		SequenceCount: len(m.sequences),
		Version:       m.version.Load(),
	}
}

// Stats holds sequence manager statistics.
type Stats struct {
	SequenceCount int    `json:"sequence_count"`
	Version       uint64 `json:"version"`
}

// markDirty marks the manager as needing persistence.
func (m *Manager) markDirty() {
	m.dirty.Store(true)
	m.version.Add(1)
}

// Persist saves all sequences to disk.
func (m *Manager) Persist() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.path == "" {
		return nil
	}

	// Create directory if needed
	dir := filepath.Dir(m.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Marshal all sequences
	data := m.marshalSequences()

	// Write to temp file first
	tempPath := m.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	// Rename to final path (atomic on most systems)
	if err := os.Rename(tempPath, m.path); err != nil {
		os.Remove(tempPath)
		return err
	}

	m.dirty.Store(false)
	return nil
}

// Load loads sequences from disk.
func (m *Manager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.path == "" {
		return nil
	}

	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet, that's ok
		}
		return err
	}

	return m.unmarshalSequences(data)
}

// marshalSequences serializes all sequences to bytes.
func (m *Manager) marshalSequences() []byte {
	// Format:
	// Header: magic (4) + version (8) + count (4)
	// For each sequence: name_len (2) + name + data

	// Calculate total size
	size := 16 // header
	for _, seq := range m.sequences {
		size += 2 + len(seq.Name) + 8 + 8 + 8 + 8 + 8 + 1 + 4 + 8 + 8 + 8
	}

	buf := make([]byte, size)
	offset := 0

	// Header
	binary.LittleEndian.PutUint32(buf[offset:4], 0x53455131) // "SEQ1"
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:offset+8], m.version.Load())
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(m.sequences)))
	offset += 4

	// Sequences
	for _, seq := range m.sequences {
		// Name length and name
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(seq.Name)))
		offset += 2
		copy(buf[offset:], seq.Name)
		offset += len(seq.Name)

		// Current
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.Current))
		offset += 8

		// Increment
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.Increment))
		offset += 8

		// MinValue
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.MinValue))
		offset += 8

		// MaxValue
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.MaxValue))
		offset += 8

		// StartValue
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.StartValue))
		offset += 8

		// Cycle
		if seq.Cycle {
			buf[offset] = 1
		}
		offset++

		// CacheSize
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(seq.CacheSize))
		offset += 4

		// CacheEnd
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.CacheEnd))
		offset += 8

		// Created (UnixNano)
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.Created.UnixNano()))
		offset += 8

		// LastUpdated (UnixNano)
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(seq.LastUpdated.UnixNano()))
		offset += 8
	}

	return buf
}

// unmarshalSequences deserializes sequences from bytes.
func (m *Manager) unmarshalSequences(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("sequence data too short")
	}

	offset := 0

	// Check magic
	magic := binary.LittleEndian.Uint32(data[offset : offset+4])
	if magic != 0x53455131 {
		return fmt.Errorf("invalid sequence file magic")
	}
	offset += 4

	// Version
	m.version.Store(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Count
	count := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read sequences
	for i := uint32(0); i < count; i++ {
		if offset+2 > len(data) {
			break
		}

		// Name
		nameLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
		if offset+int(nameLen) > len(data) {
			break
		}
		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		seq := &Sequence{Name: name}

		// Current
		seq.Current = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// Increment
		seq.Increment = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// MinValue
		seq.MinValue = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// MaxValue
		seq.MaxValue = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// StartValue
		seq.StartValue = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// Cycle
		seq.Cycle = data[offset] == 1
		offset++

		// CacheSize
		seq.CacheSize = int32(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4

		// CacheEnd
		seq.CacheEnd = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// Created
		seq.Created = time.Unix(0, int64(binary.LittleEndian.Uint64(data[offset:offset+8])))
		offset += 8

		// LastUpdated
		seq.LastUpdated = time.Unix(0, int64(binary.LittleEndian.Uint64(data[offset:offset+8])))
		offset += 8

		m.sequences[name] = seq
	}

	return nil
}

// backgroundFlush periodically persists sequences.
func (m *Manager) backgroundFlush() {
	ticker := time.NewTicker(m.syncPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			if m.dirty.Load() {
				m.Persist()
			}
		case <-m.flushChan:
			m.Persist()
		}
	}
}

// Flush triggers an immediate persist.
func (m *Manager) Flush() error {
	select {
	case m.flushChan <- struct{}{}:
	default:
		// Already a flush pending
	}
	return nil
}

// Close closes the sequence manager.
func (m *Manager) Close() error {
	if m.stopped.Swap(true) {
		return nil
	}

	close(m.stopChan)

	// Final persist
	return m.Persist()
}

// Exists checks if a sequence exists.
func (m *Manager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.sequences[name]
	return exists
}

// BatchNextValue returns multiple values from a sequence.
func (m *Manager) BatchNextValue(name string, count int) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	seq, exists := m.sequences[name]
	if !exists {
		return nil, fmt.Errorf("sequence %s does not exist", name)
	}

	values := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		nextValue := seq.Current + seq.Increment

		// Check bounds
		if nextValue < seq.MinValue || nextValue > seq.MaxValue {
			if seq.Cycle {
				if nextValue < seq.MinValue {
					nextValue = seq.MaxValue
				} else {
					nextValue = seq.MinValue
				}
			} else {
				return nil, fmt.Errorf("sequence %s exhausted after %d values", name, i)
			}
		}

		values = append(values, nextValue)
		seq.Current = nextValue
	}

	seq.LastUpdated = time.Now()
	m.markDirty()

	return values, nil
}

// GetRange returns a range of values from a sequence.
// This is more efficient than multiple NextValue calls for bulk operations.
func (m *Manager) GetRange(name string, count int) (start, end int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	seq, exists := m.sequences[name]
	if !exists {
		return 0, 0, fmt.Errorf("sequence %s does not exist", name)
	}

	start = seq.Current + seq.Increment
	end = start + int64(count-1)*seq.Increment

	// Check bounds
	if end < seq.MinValue || end > seq.MaxValue {
		return 0, 0, fmt.Errorf("sequence range out of bounds")
	}

	seq.Current = end
	seq.LastUpdated = time.Now()
	m.markDirty()

	return start, end, nil
}
