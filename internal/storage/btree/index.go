// Package btree provides index management for XxSql.
package btree

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// IndexType represents the type of index.
type IndexType uint8

const (
	IndexTypePrimary IndexType = iota
	IndexTypeUnique
	IndexTypeNonUnique
)

// String returns the string representation.
func (t IndexType) String() string {
	switch t {
	case IndexTypePrimary:
		return "PRIMARY"
	case IndexTypeUnique:
		return "UNIQUE"
	case IndexTypeNonUnique:
		return "INDEX"
	default:
		return "UNKNOWN"
	}
}

// IndexInfo represents index metadata.
type IndexInfo struct {
	Name       string
	TableName  string
	Columns    []string
	Type       IndexType
	KeyType    types.TypeID
	RootPageID page.PageID
}

// Index represents an index on a table.
type Index struct {
	Info    *IndexInfo
	Tree    *BTree
	mu      sync.RWMutex
}

// NewIndex creates a new index.
func NewIndex(info *IndexInfo, pm PageManager) *Index {
	tree := NewBTree(DefaultOrder, info.KeyType, pm)
	return &Index{
		Info: info,
		Tree: tree,
	}
}

// Insert inserts a key with a row ID.
func (idx *Index) Insert(key types.Value, rowID row.RowID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Encode row ID as value
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(rowID))

	return idx.Tree.Insert(Key{Value: key}, value)
}

// Search finds a row ID by key.
func (idx *Index) Search(key types.Value) (row.RowID, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	value, found := idx.Tree.Search(Key{Value: key})
	if !found {
		return 0, false
	}

	if len(value) < 8 {
		return 0, false
	}

	return row.RowID(binary.LittleEndian.Uint64(value)), true
}

// Delete removes a key from the index.
func (idx *Index) Delete(key types.Value) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.Tree.Delete(Key{Value: key})
}

// Range returns row IDs for keys in the given range.
func (idx *Index) Range(start, end types.Value) []row.RowID {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := idx.Tree.Range(Key{Value: start}, Key{Value: end})
	result := make([]row.RowID, len(entries))
	for i, e := range entries {
		if len(e.Value) >= 8 {
			result[i] = row.RowID(binary.LittleEndian.Uint64(e.Value))
		}
	}
	return result
}

// Scan returns all row IDs in the index.
func (idx *Index) Scan() []row.RowID {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := idx.Tree.Scan()
	result := make([]row.RowID, len(entries))
	for i, e := range entries {
		if len(e.Value) >= 8 {
			result[i] = row.RowID(binary.LittleEndian.Uint64(e.Value))
		}
	}
	return result
}

// Count returns the number of entries in the index.
func (idx *Index) Count() int {
	return idx.Tree.Count()
}

// Flush persists the index.
func (idx *Index) Flush() error {
	return idx.Tree.Flush()
}

// IndexManager manages all indexes for a table.
type IndexManager struct {
	tableName string
	indexes   map[string]*Index
	primary   *Index // Primary key index
	mu        sync.RWMutex
	pageMgr   PageManager
}

// NewIndexManager creates a new index manager.
func NewIndexManager(tableName string, pm PageManager) *IndexManager {
	return &IndexManager{
		tableName: tableName,
		indexes:   make(map[string]*Index),
		pageMgr:   pm,
	}
}

// CreateIndex creates a new index.
func (m *IndexManager) CreateIndex(name string, columns []string, typ IndexType, keyType types.TypeID) (*Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; exists {
		return nil, fmt.Errorf("index already exists: %s", name)
	}

	info := &IndexInfo{
		Name:      name,
		TableName: m.tableName,
		Columns:   columns,
		Type:      typ,
		KeyType:   keyType,
	}

	idx := NewIndex(info, m.pageMgr)
	m.indexes[name] = idx

	if typ == IndexTypePrimary {
		m.primary = idx
	}

	return idx, nil
}

// DropIndex drops an index.
func (m *IndexManager) DropIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx, exists := m.indexes[name]
	if !exists {
		return fmt.Errorf("index not found: %s", name)
	}

	if idx == m.primary {
		m.primary = nil
	}

	delete(m.indexes, name)
	return nil
}

// GetIndex returns an index by name.
func (m *IndexManager) GetIndex(name string) (*Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx, exists := m.indexes[name]
	if !exists {
		return nil, fmt.Errorf("index not found: %s", name)
	}

	return idx, nil
}

// GetPrimary returns the primary key index.
func (m *IndexManager) GetPrimary() *Index {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.primary
}

// HasPrimary returns true if a primary key index exists.
func (m *IndexManager) HasPrimary() bool {
	return m.primary != nil
}

// ListIndexes returns all index names.
func (m *IndexManager) ListIndexes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.indexes))
	for name := range m.indexes {
		names = append(names, name)
	}
	return names
}

// InsertIntoIndexes inserts a row into all indexes.
func (m *IndexManager) InsertIntoIndexes(values []types.Value, rowID row.RowID, columnMap map[string]int) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, idx := range m.indexes {
		// Get key value from row
		if len(idx.Info.Columns) == 0 {
			continue
		}

		colIdx, ok := columnMap[idx.Info.Columns[0]]
		if !ok || colIdx >= len(values) {
			continue
		}

		key := values[colIdx]
		if err := idx.Insert(key, rowID); err != nil {
			return err
		}
	}

	return nil
}

// DeleteFromIndexes deletes a row from all indexes.
func (m *IndexManager) DeleteFromIndexes(values []types.Value, columnMap map[string]int) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, idx := range m.indexes {
		if len(idx.Info.Columns) == 0 {
			continue
		}

		colIdx, ok := columnMap[idx.Info.Columns[0]]
		if !ok || colIdx >= len(values) {
			continue
		}

		key := values[colIdx]
		if err := idx.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// FlushAll flushes all indexes.
func (m *IndexManager) FlushAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, idx := range m.indexes {
		if err := idx.Flush(); err != nil {
			return err
		}
	}
	return nil
}
