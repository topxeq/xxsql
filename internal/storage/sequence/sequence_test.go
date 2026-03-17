// Package sequence_test provides tests for sequence management.
package sequence_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/storage/sequence"
)

func TestManagerCreateSequence(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	err := m.CreateSequence(config)
	if err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Check sequence exists
	if !m.Exists("test_seq") {
		t.Error("Sequence should exist")
	}

	// Check duplicate fails
	err = m.CreateSequence(config)
	if err == nil {
		t.Error("Should fail to create duplicate sequence")
	}
}

func TestManagerNextValue(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	config.Start = 1
	config.Increment = 1

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get next values
	for i := int64(1); i <= 5; i++ {
		val, err := m.NextValue("test_seq")
		if err != nil {
			t.Fatalf("Failed to get next value: %v", err)
		}
		if val != i {
			t.Errorf("Expected %d, got %d", i, val)
		}
	}
}

func TestManagerNextValueWithIncrement(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	config.Start = 0
	config.MinValue = 0 // Allow starting at 0
	config.Increment = 5

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get next values - starting at 0 with increment 5
	// First value is START (0), then 5, 10, 15...
	expected := []int64{0, 5, 10, 15}
	for _, exp := range expected {
		val, err := m.NextValue("test_seq")
		if err != nil {
			t.Fatalf("Failed to get next value: %v", err)
		}
		if val != exp {
			t.Errorf("Expected %d, got %d", exp, val)
		}
	}
}

func TestManagerCurrentValue(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Current should be 0 before any next
	cur, err := m.CurrentValue("test_seq")
	if err != nil {
		t.Fatalf("Failed to get current value: %v", err)
	}
	if cur != 0 {
		t.Errorf("Expected current 0, got %d", cur)
	}

	// Get next
	val, _ := m.NextValue("test_seq")

	// Current should match
	cur, err = m.CurrentValue("test_seq")
	if err != nil {
		t.Fatalf("Failed to get current value: %v", err)
	}
	if cur != val {
		t.Errorf("Expected current %d, got %d", val, cur)
	}
}

func TestManagerSequenceWithCycle(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("cycle_seq")
	config.Start = 1
	config.Increment = 1
	config.MinValue = 1
	config.MaxValue = 3
	config.Cycle = true

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Cycle through: 1, 2, 3, 1, 2, 3...
	expected := []int64{1, 2, 3, 1, 2, 3}
	for i, exp := range expected {
		val, err := m.NextValue("cycle_seq")
		if err != nil {
			t.Fatalf("Failed at iteration %d: %v", i, err)
		}
		if val != exp {
			t.Errorf("At iteration %d, expected %d, got %d", i, exp, val)
		}
	}
}

func TestManagerSequenceNoCycle(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("nocycle_seq")
	config.Start = 1
	config.MinValue = 1
	config.MaxValue = 3
	config.Cycle = false

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get values until exhausted
	for i := int64(1); i <= 3; i++ {
		_, err := m.NextValue("nocycle_seq")
		if err != nil {
			t.Fatalf("Failed at value %d: %v", i, err)
		}
	}

	// Next should fail
	_, err := m.NextValue("nocycle_seq")
	if err == nil {
		t.Error("Expected error when sequence exhausted")
	}
}

func TestManagerDropSequence(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Drop it
	if err := m.DropSequence("test_seq"); err != nil {
		t.Fatalf("Failed to drop sequence: %v", err)
	}

	// Should not exist
	if m.Exists("test_seq") {
		t.Error("Sequence should not exist after drop")
	}

	// Drop non-existent should fail
	err := m.DropSequence("nonexistent")
	if err == nil {
		t.Error("Should fail to drop non-existent sequence")
	}
}

func TestManagerReset(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	config.Start = 100

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get some values
	for i := 0; i < 5; i++ {
		m.NextValue("test_seq")
	}

	// Reset
	if err := m.Reset("test_seq"); err != nil {
		t.Fatalf("Failed to reset sequence: %v", err)
	}

	// Should start from beginning
	val, err := m.NextValue("test_seq")
	if err != nil {
		t.Fatalf("Failed to get next value after reset: %v", err)
	}
	if val != 100 {
		t.Errorf("Expected 100 after reset, got %d", val)
	}
}

func TestManagerSetCurrentValue(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("test_seq")
	config.MinValue = 1
	config.MaxValue = 1000

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Set current value
	if err := m.SetCurrentValue("test_seq", 500); err != nil {
		t.Fatalf("Failed to set current value: %v", err)
	}

	// Next should be 501
	val, err := m.NextValue("test_seq")
	if err != nil {
		t.Fatalf("Failed to get next value: %v", err)
	}
	if val != 501 {
		t.Errorf("Expected 501, got %d", val)
	}

	// Set out of bounds should fail
	err = m.SetCurrentValue("test_seq", 2000)
	if err == nil {
		t.Error("Should fail to set value out of bounds")
	}
}

func TestManagerListSequences(t *testing.T) {
	m := sequence.NewManager("", nil)

	// Create multiple sequences
	for i := 1; i <= 3; i++ {
		config := sequence.DefaultSequenceConfig("seq" + string(rune('0'+i)))
		if err := m.CreateSequence(config); err != nil {
			t.Fatalf("Failed to create sequence: %v", err)
		}
	}

	// List
	list := m.ListSequences()
	if len(list) != 3 {
		t.Errorf("Expected 3 sequences, got %d", len(list))
	}
}

func TestManagerPersistAndLoad(t *testing.T) {
	// Create temp file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "sequences.seq")

	// Create manager and sequence
	m := sequence.NewManager(path, nil)

	config := sequence.DefaultSequenceConfig("persist_seq")
	config.Start = 100
	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get some values
	for i := 0; i < 5; i++ {
		m.NextValue("persist_seq")
	}

	// Persist
	if err := m.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Close
	m.Close()

	// Create new manager and load
	m2 := sequence.NewManager(path, nil)
	if err := m2.Load(); err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	// Check sequence exists
	if !m2.Exists("persist_seq") {
		t.Error("Sequence should exist after load")
	}

	// Check current value
	cur, err := m2.CurrentValue("persist_seq")
	if err != nil {
		t.Fatalf("Failed to get current value: %v", err)
	}
	if cur != 104 {
		t.Errorf("Expected current 104, got %d", cur)
	}

	// Next should continue
	val, err := m2.NextValue("persist_seq")
	if err != nil {
		t.Fatalf("Failed to get next value: %v", err)
	}
	if val != 105 {
		t.Errorf("Expected 105, got %d", val)
	}
}

func TestManagerCacheSize(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("cached_seq")
	config.CacheSize = 10

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get sequence info
	seq, err := m.GetSequence("cached_seq")
	if err != nil {
		t.Fatalf("Failed to get sequence: %v", err)
	}

	if seq.CacheSize != 10 {
		t.Errorf("Expected cache size 10, got %d", seq.CacheSize)
	}
}

func TestManagerBatchNextValue(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("batch_seq")
	config.Start = 1

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get batch of values
	values, err := m.BatchNextValue("batch_seq", 5)
	if err != nil {
		t.Fatalf("Failed to get batch values: %v", err)
	}

	if len(values) != 5 {
		t.Errorf("Expected 5 values, got %d", len(values))
	}

	expected := []int64{1, 2, 3, 4, 5}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("At index %d, expected %d, got %d", i, exp, values[i])
		}
	}

	// Current should be 5
	cur, _ := m.CurrentValue("batch_seq")
	if cur != 5 {
		t.Errorf("Expected current 5, got %d", cur)
	}
}

func TestManagerGetRange(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("range_seq")
	config.Start = 1

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get range
	start, end, err := m.GetRange("range_seq", 10)
	if err != nil {
		t.Fatalf("Failed to get range: %v", err)
	}

	if start != 1 {
		t.Errorf("Expected start 1, got %d", start)
	}
	if end != 10 {
		t.Errorf("Expected end 10, got %d", end)
	}

	// Current should be 10
	cur, _ := m.CurrentValue("range_seq")
	if cur != 10 {
		t.Errorf("Expected current 10, got %d", cur)
	}
}

func TestManagerStats(t *testing.T) {
	m := sequence.NewManager("", nil)

	// Create sequences
	for i := 0; i < 3; i++ {
		config := sequence.DefaultSequenceConfig("stat_seq")
		config.Name = string(rune('a' + i))
		m.CreateSequence(config)
	}

	stats := m.Stats()
	if stats.SequenceCount != 3 {
		t.Errorf("Expected 3 sequences in stats, got %d", stats.SequenceCount)
	}
}

func TestManagerNegativeIncrement(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("decrement_seq")
	config.Start = 10
	config.Increment = -1
	config.MinValue = 1
	config.MaxValue = 10

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Get values - first value is start (10), then decrements
	// Current = start - increment = 10 - (-1) = 11
	// First NextValue = current + increment = 11 + (-1) = 10
	expected := []int64{10, 9, 8}
	for _, exp := range expected {
		val, err := m.NextValue("decrement_seq")
		if err != nil {
			t.Fatalf("Failed to get next value: %v", err)
		}
		if val != exp {
			t.Errorf("Expected %d, got %d", exp, val)
		}
	}
}

func TestManagerValidation(t *testing.T) {
	m := sequence.NewManager("", nil)

	// Test invalid configs
	tests := []struct {
		name    string
		config  sequence.SequenceConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: sequence.SequenceConfig{
				Name:      "valid",
				Start:     1,
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
			},
			wantErr: false,
		},
		{
			name: "min > max",
			config: sequence.SequenceConfig{
				Name:      "invalid",
				Start:     50,
				Increment: 1,
				MinValue:  100,
				MaxValue:  1,
			},
			wantErr: true,
		},
		{
			name: "start < min",
			config: sequence.SequenceConfig{
				Name:      "invalid",
				Start:     0,
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
			},
			wantErr: true,
		},
		{
			name: "start > max",
			config: sequence.SequenceConfig{
				Name:      "invalid",
				Start:     200,
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
			},
			wantErr: true,
		},
		{
			name: "zero increment",
			config: sequence.SequenceConfig{
				Name:      "invalid",
				Start:     1,
				Increment: 0,
				MinValue:  1,
				MaxValue:  100,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := m.CreateSequence(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSequence() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestManagerConcurrentAccess(t *testing.T) {
	m := sequence.NewManager("", nil)

	config := sequence.DefaultSequenceConfig("concurrent_seq")
	config.Start = 1
	config.MaxValue = 10000

	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Concurrent access
	done := make(chan bool)
	count := 100

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < count; j++ {
				_, err := m.NextValue("concurrent_seq")
				if err != nil {
					t.Errorf("Failed to get next value: %v", err)
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Current should be 1000
	cur, _ := m.CurrentValue("concurrent_seq")
	if cur != 1000 {
		t.Errorf("Expected current 1000, got %d", cur)
	}
}

// MockWALLogger for testing WAL integration
type MockWALLogger struct {
	mu      sync.Mutex
	creates []string
	drops   []string
	nexts   []string
}

func (m *MockWALLogger) LogSequenceCreate(name string, startValue int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates = append(m.creates, name)
	return nil
}

func (m *MockWALLogger) LogSequenceDrop(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops = append(m.drops, name)
	return nil
}

func (m *MockWALLogger) LogSequenceNext(name string, value int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nexts = append(m.nexts, name)
	return nil
}

func (m *MockWALLogger) lenNexts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.nexts)
}

func (m *MockWALLogger) lenCreates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.creates)
}

func TestManagerWALIntegration(t *testing.T) {
	mockLogger := &MockWALLogger{}
	m := sequence.NewManager("", mockLogger)

	config := sequence.DefaultSequenceConfig("wal_seq")
	if err := m.CreateSequence(config); err != nil {
		t.Fatalf("Failed to create sequence: %v", err)
	}

	// Wait for async log
	time.Sleep(50 * time.Millisecond)

	if mockLogger.lenCreates() != 1 {
		t.Errorf("Expected 1 create log, got %d", mockLogger.lenCreates())
	}

	// Get next value
	m.NextValue("wal_seq")

	// Wait for async log
	time.Sleep(50 * time.Millisecond)

	if mockLogger.lenNexts() != 1 {
		t.Errorf("Expected 1 next log, got %d", mockLogger.lenNexts())
	}

	// Drop sequence
	m.DropSequence("wal_seq")

	if len(mockLogger.drops) != 1 {
		t.Errorf("Expected 1 drop log, got %d", len(mockLogger.drops))
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
