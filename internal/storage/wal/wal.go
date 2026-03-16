// Package wal provides Write-Ahead Logging for XxSql storage engine.
package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// WALFileExt is the extension for WAL files.
	WALFileExt = ".xwal"

	// WALHeaderSize is the size of the WAL file header.
	WALHeaderSize = 32

	// WALRecordHeaderSize is the size of a WAL record header.
	// LSN (8) + Type (1) + TxnID (8) + PageID (8) = 25 bytes
	WALRecordHeaderSize = 25

	// DefaultSyncInterval is the default sync interval.
	DefaultSyncInterval = 100 * time.Millisecond

	// DefaultMaxSize is the default max WAL file size (64MB).
	DefaultMaxSize = 64 * 1024 * 1024
)

// LSN represents a Log Sequence Number.
type LSN uint64

// InvalidLSN represents an invalid LSN.
const InvalidLSN LSN = 0

// RecordType represents the type of WAL record.
type RecordType uint8

const (
	RecordTypeBegin RecordType = iota
	RecordTypeCommit
	RecordTypeAbort
	RecordTypeInsert
	RecordTypeUpdate
	RecordTypeDelete
	RecordTypeCreateTable
	RecordTypeDropTable
	RecordTypeCreateIndex
	RecordTypeDropIndex
	RecordTypeCheckpoint
	RecordTypePageWrite
	RecordTypeSequenceCreate
	RecordTypeSequenceDrop
	RecordTypeSequenceNext
)

// String returns the string representation.
func (t RecordType) String() string {
	switch t {
	case RecordTypeBegin:
		return "BEGIN"
	case RecordTypeCommit:
		return "COMMIT"
	case RecordTypeAbort:
		return "ABORT"
	case RecordTypeInsert:
		return "INSERT"
	case RecordTypeUpdate:
		return "UPDATE"
	case RecordTypeDelete:
		return "DELETE"
	case RecordTypeCreateTable:
		return "CREATE_TABLE"
	case RecordTypeDropTable:
		return "DROP_TABLE"
	case RecordTypeCreateIndex:
		return "CREATE_INDEX"
	case RecordTypeDropIndex:
		return "DROP_INDEX"
	case RecordTypeCheckpoint:
		return "CHECKPOINT"
	case RecordTypePageWrite:
		return "PAGE_WRITE"
	case RecordTypeSequenceCreate:
		return "SEQUENCE_CREATE"
	case RecordTypeSequenceDrop:
		return "SEQUENCE_DROP"
	case RecordTypeSequenceNext:
		return "SEQUENCE_NEXT"
	default:
		return "UNKNOWN"
	}
}

// Record represents a WAL record.
type Record struct {
	LSN       LSN
	Type      RecordType
	TxnID     uint64
	PageID    uint64
	TableName string
	Data      []byte
	Timestamp int64
}

// Marshal serializes a record to bytes.
func (r *Record) Marshal() []byte {
	// Calculate size
	tableNameLen := len(r.TableName)
	dataLen := len(r.Data)
	size := WALRecordHeaderSize + 2 + tableNameLen + 4 + dataLen

	buf := make([]byte, size)
	offset := 0

	// LSN (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(r.LSN))
	offset += 8

	// Type (1 byte)
	buf[offset] = byte(r.Type)
	offset++

	// TxnID (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:offset+8], r.TxnID)
	offset += 8

	// PageID (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:offset+8], r.PageID)
	offset += 8

	// Table name length (2 bytes)
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(tableNameLen))
	offset += 2

	// Table name
	copy(buf[offset:offset+tableNameLen], r.TableName)
	offset += tableNameLen

	// Data length (4 bytes)
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(dataLen))
	offset += 4

	// Data
	copy(buf[offset:offset+dataLen], r.Data)

	return buf
}

// UnmarshalRecord deserializes bytes to a record.
func UnmarshalRecord(data []byte) (*Record, int, error) {
	if len(data) < WALRecordHeaderSize {
		return nil, 0, fmt.Errorf("data too short for WAL record")
	}

	r := &Record{}
	offset := 0

	// LSN
	r.LSN = LSN(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Type
	r.Type = RecordType(data[offset])
	offset++

	// TxnID
	r.TxnID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// PageID
	r.PageID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Table name length
	tableNameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	// Table name
	if offset+tableNameLen > len(data) {
		return nil, 0, fmt.Errorf("invalid table name length")
	}
	r.TableName = string(data[offset : offset+tableNameLen])
	offset += tableNameLen

	// Data length
	if offset+4 > len(data) {
		return nil, 0, fmt.Errorf("invalid data length field")
	}
	dataLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Data
	if offset+dataLen > len(data) {
		dataLen = len(data) - offset
	}
	r.Data = make([]byte, dataLen)
	copy(r.Data, data[offset:offset+dataLen])
	offset += dataLen

	r.Timestamp = time.Now().UnixNano()

	return r, offset, nil
}

// Manager manages WAL operations.
type Manager struct {
	path        string
	file        *os.File
	currentLSN  atomic.Uint64
	flushedLSN  atomic.Uint64
	maxSize     int64
	syncInterval time.Duration

	mu       sync.Mutex
	stopCh   chan struct{}
	stopped  atomic.Bool
}

// ManagerConfig holds WAL manager configuration.
type ManagerConfig struct {
	Path         string
	MaxSize      int64
	SyncInterval time.Duration
}

// NewManager creates a new WAL manager.
func NewManager(config ManagerConfig) *Manager {
	if config.MaxSize <= 0 {
		config.MaxSize = DefaultMaxSize
	}
	if config.SyncInterval <= 0 {
		config.SyncInterval = DefaultSyncInterval
	}

	m := &Manager{
		path:         config.Path,
		maxSize:      config.MaxSize,
		syncInterval: config.SyncInterval,
		stopCh:       make(chan struct{}),
	}

	return m
}

// Open opens the WAL file.
func (m *Manager) Open() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file != nil {
		return nil
	}

	// Create directory if not exists
	dir := filepath.Dir(m.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Open file
	f, err := os.OpenFile(m.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	m.file = f

	// Read last LSN from file
	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.Size() > 0 {
		// Read existing WAL to find last LSN
		if err := m.readLastLSN(); err != nil {
			// Log warning but continue
		}
	}

	// Start background sync goroutine
	go m.backgroundSync()

	return nil
}

// readLastLSN reads the last LSN from the WAL file.
func (m *Manager) readLastLSN() error {
	// Read from end of file to find last record
	// For simplicity, we'll read through all records
	// In production, you'd want to optimize this

	buf := make([]byte, 4096)
	var lastLSN LSN

	for {
		n, err := m.file.Read(buf)
		if err != nil {
			break
		}

		// Parse records from buffer
		offset := 0
		for offset < n {
			if offset+WALRecordHeaderSize > n {
				break
			}

			r, bytesRead, err := UnmarshalRecord(buf[offset:])
			if err != nil {
				break
			}

			if r.LSN > lastLSN {
				lastLSN = r.LSN
			}
			offset += bytesRead
		}
	}

	m.currentLSN.Store(uint64(lastLSN))
	m.flushedLSN.Store(uint64(lastLSN))

	return nil
}

// Close closes the WAL manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped.Load() {
		return nil
	}

	m.stopped.Store(true)
	close(m.stopCh)

	if m.file != nil {
		m.file.Sync()
		err := m.file.Close()
		m.file = nil
		return err
	}
	return nil
}

// nextLSN allocates the next LSN.
func (m *Manager) nextLSN() LSN {
	return LSN(m.currentLSN.Add(1))
}

// Append appends a record to the WAL.
func (m *Manager) Append(recordType RecordType, txnID, pageID uint64, tableName string, data []byte) (LSN, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return InvalidLSN, fmt.Errorf("WAL not open")
	}

	lsn := m.nextLSN()

	record := &Record{
		LSN:       lsn,
		Type:      recordType,
		TxnID:     txnID,
		PageID:    pageID,
		TableName: tableName,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}

	buf := record.Marshal()

	_, err := m.file.Write(buf)
	if err != nil {
		return InvalidLSN, err
	}

	return lsn, nil
}

// AppendRecord appends a pre-constructed record to the WAL.
func (m *Manager) AppendRecord(r *Record) (LSN, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return InvalidLSN, fmt.Errorf("WAL not open")
	}

	lsn := m.nextLSN()
	r.LSN = lsn
	r.Timestamp = time.Now().UnixNano()

	buf := r.Marshal()

	_, err := m.file.Write(buf)
	if err != nil {
		return InvalidLSN, err
	}

	return lsn, nil
}

// Sync syncs the WAL file to disk.
func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return nil
	}

	err := m.file.Sync()
	if err != nil {
		return err
	}

	m.flushedLSN.Store(m.currentLSN.Load())
	return nil
}

// backgroundSync periodically syncs the WAL.
func (m *Manager) backgroundSync() {
	ticker := time.NewTicker(m.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.Sync()
		}
	}
}

// LogBegin logs a transaction begin.
func (m *Manager) LogBegin(txnID uint64) (LSN, error) {
	return m.Append(RecordTypeBegin, txnID, 0, "", nil)
}

// LogCommit logs a transaction commit.
func (m *Manager) LogCommit(txnID uint64) (LSN, error) {
	lsn, err := m.Append(RecordTypeCommit, txnID, 0, "", nil)
	if err != nil {
		return lsn, err
	}
	// Sync on commit
	return lsn, m.Sync()
}

// LogAbort logs a transaction abort.
func (m *Manager) LogAbort(txnID uint64) (LSN, error) {
	return m.Append(RecordTypeAbort, txnID, 0, "", nil)
}

// LogInsert logs an insert operation.
func (m *Manager) LogInsert(txnID uint64, tableName string, rowData []byte) (LSN, error) {
	return m.Append(RecordTypeInsert, txnID, 0, tableName, rowData)
}

// LogUpdate logs an update operation.
func (m *Manager) LogUpdate(txnID uint64, tableName string, oldData, newData []byte) (LSN, error) {
	data := make([]byte, 4+len(oldData)+4+len(newData))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(oldData)))
	copy(data[4:4+len(oldData)], oldData)
	binary.LittleEndian.PutUint32(data[4+len(oldData):8+len(oldData)], uint32(len(newData)))
	copy(data[8+len(oldData):], newData)
	return m.Append(RecordTypeUpdate, txnID, 0, tableName, data)
}

// LogDelete logs a delete operation.
func (m *Manager) LogDelete(txnID uint64, tableName string, rowData []byte) (LSN, error) {
	return m.Append(RecordTypeDelete, txnID, 0, tableName, rowData)
}

// LogPageWrite logs a page write operation.
func (m *Manager) LogPageWrite(txnID, pageID uint64, pageData []byte) (LSN, error) {
	return m.Append(RecordTypePageWrite, txnID, pageID, "", pageData)
}

// LogCreateTable logs a create table operation.
func (m *Manager) LogCreateTable(txnID uint64, tableName string, schemaData []byte) (LSN, error) {
	return m.Append(RecordTypeCreateTable, txnID, 0, tableName, schemaData)
}

// LogDropTable logs a drop table operation.
func (m *Manager) LogDropTable(txnID uint64, tableName string) (LSN, error) {
	return m.Append(RecordTypeDropTable, txnID, 0, tableName, nil)
}

// LogCheckpoint logs a checkpoint.
func (m *Manager) LogCheckpoint(txnID uint64, checkpointData []byte) (LSN, error) {
	lsn, err := m.Append(RecordTypeCheckpoint, txnID, 0, "", checkpointData)
	if err != nil {
		return lsn, err
	}
	// Sync on checkpoint
	return lsn, m.Sync()
}

// LogSequenceCreate logs a sequence creation.
func (m *Manager) LogSequenceCreate(name string, startValue int64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(startValue))
	_, err := m.Append(RecordTypeSequenceCreate, 0, 0, name, data)
	return err
}

// LogSequenceDrop logs a sequence drop.
func (m *Manager) LogSequenceDrop(name string) error {
	_, err := m.Append(RecordTypeSequenceDrop, 0, 0, name, nil)
	return err
}

// LogSequenceNext logs a sequence next value operation.
func (m *Manager) LogSequenceNext(name string, value int64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(value))
	_, err := m.Append(RecordTypeSequenceNext, 0, 0, name, data)
	return err
}

// GetCurrentLSN returns the current LSN.
func (m *Manager) GetCurrentLSN() LSN {
	return LSN(m.currentLSN.Load())
}

// GetFlushedLSN returns the last flushed LSN.
func (m *Manager) GetFlushedLSN() LSN {
	return LSN(m.flushedLSN.Load())
}

// Replay reads all records from the WAL.
func (m *Manager) Replay(fn func(*Record) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return fmt.Errorf("WAL not open")
	}

	// Seek to beginning
	_, err := m.file.Seek(0, 0)
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	totalRead := 0

	for {
		n, err := m.file.Read(buf)
		if err != nil {
			break
		}

		offset := 0
		for offset < n {
			r, bytesRead, err := UnmarshalRecord(buf[offset:])
			if err != nil {
				break
			}

			if err := fn(r); err != nil {
				return err
			}

			offset += bytesRead
			totalRead += bytesRead
		}
	}

	return nil
}

// Truncate truncates the WAL file.
func (m *Manager) Truncate() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return nil
	}

	// Sync first
	if err := m.file.Sync(); err != nil {
		return err
	}

	// Truncate
	if err := m.file.Truncate(0); err != nil {
		return err
	}

	// Seek to beginning
	_, err := m.file.Seek(0, 0)
	if err != nil {
		return err
	}

	// Reset LSN
	m.currentLSN.Store(0)
	m.flushedLSN.Store(0)

	return nil
}

// Size returns the current WAL file size.
func (m *Manager) Size() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file == nil {
		return 0, nil
	}

	info, err := m.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

// NeedsCheckpoint returns true if a checkpoint is needed.
func (m *Manager) NeedsCheckpoint() (bool, error) {
	size, err := m.Size()
	if err != nil {
		return false, err
	}
	return size >= m.maxSize, nil
}
