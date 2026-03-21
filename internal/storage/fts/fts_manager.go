// Package fts provides full-text search functionality for XxSQL.
package fts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FTSManager manages all FTS indexes for a database.
type FTSManager struct {
	mu       sync.RWMutex
	indexes  map[string]*FTSIndex // index name -> index
	dataDir  string
}

// NewFTSManager creates a new FTS manager.
func NewFTSManager(dataDir string) *FTSManager {
	return &FTSManager{
		indexes: make(map[string]*FTSIndex),
		dataDir: dataDir,
	}
}

// CreateIndex creates a new FTS index.
func (m *FTSManager) CreateIndex(name, tableName string, columns []string, tokenizer string) (*FTSIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; exists {
		return nil, fmt.Errorf("FTS index %s already exists", name)
	}

	config := FTSIndexConfig{
		Name:       name,
		TableName:  tableName,
		Columns:    columns,
		Tokenizer:  tokenizer,
		Persistent: true,
		DataDir:    m.dataDir,
	}

	idx := NewFTSIndex(config)
	m.indexes[name] = idx

	return idx, nil
}

// GetIndex returns an FTS index by name.
func (m *FTSManager) GetIndex(name string) (*FTSIndex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx, exists := m.indexes[name]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", name)
	}

	return idx, nil
}

// DropIndex removes an FTS index.
func (m *FTSManager) DropIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; !exists {
		return fmt.Errorf("FTS index %s not found", name)
	}

	// Remove persisted data
	idx := m.indexes[name]
	if idx.storagePath != "" {
		os.Remove(idx.storagePath)
	}

	delete(m.indexes, name)
	return nil
}

// ListIndexes returns all FTS index names.
func (m *FTSManager) ListIndexes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.indexes))
	for name := range m.indexes {
		names = append(names, name)
	}
	return names
}

// GetIndexesForTable returns all FTS indexes for a table.
func (m *FTSManager) GetIndexesForTable(tableName string) []*FTSIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var indexes []*FTSIndex
	for _, idx := range m.indexes {
		if idx.TableName() == tableName {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

// IndexDocument indexes a document in all relevant indexes for a table.
func (m *FTSManager) IndexDocument(tableName string, docID uint64, values map[string]interface{}) error {
	m.mu.RLock()
	indexes := m.GetIndexesForTable(tableName)
	m.mu.RUnlock()

	for _, idx := range indexes {
		if err := idx.IndexDocument(docID, values); err != nil {
			return fmt.Errorf("failed to index document in %s: %w", idx.Name(), err)
		}
	}
	return nil
}

// RemoveDocument removes a document from all relevant indexes.
func (m *FTSManager) RemoveDocument(tableName string, docID uint64) {
	m.mu.RLock()
	indexes := m.GetIndexesForTable(tableName)
	m.mu.RUnlock()

	for _, idx := range indexes {
		idx.RemoveDocument(docID)
	}
}

// UpdateDocument updates a document in all relevant indexes.
func (m *FTSManager) UpdateDocument(tableName string, docID uint64, values map[string]interface{}) error {
	m.mu.RLock()
	indexes := m.GetIndexesForTable(tableName)
	m.mu.RUnlock()

	for _, idx := range indexes {
		if err := idx.UpdateDocument(docID, values); err != nil {
			return fmt.Errorf("failed to update document in %s: %w", idx.Name(), err)
		}
	}
	return nil
}

// Search performs a search on a specific index.
func (m *FTSManager) Search(indexName, query string) ([]SearchResult, error) {
	m.mu.RLock()
	idx, exists := m.indexes[indexName]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", indexName)
	}

	return idx.Search(query)
}

// SaveAll saves all indexes to disk.
func (m *FTSManager) SaveAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, idx := range m.indexes {
		if err := idx.Save(); err != nil {
			return fmt.Errorf("failed to save index %s: %w", idx.Name(), err)
		}
	}
	return nil
}

// LoadAll loads all indexes from disk.
func (m *FTSManager) LoadAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ftsDir := filepath.Join(m.dataDir, "fts")
	entries, err := os.ReadDir(ftsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No FTS directory yet
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		// Load index metadata
		filePath := filepath.Join(ftsDir, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		var saved struct {
			Name      string
			TableName string
			Columns   []string
		}
		if err := parseJSON(data, &saved); err != nil {
			continue
		}

		// Check if index already exists (created from catalog)
		if idx, exists := m.indexes[saved.Name]; exists {
			// Load the persisted data into existing index
			if err := idx.Load(); err != nil {
				continue
			}
			continue
		}

		// Index doesn't exist, create it
		config := FTSIndexConfig{
			Name:       saved.Name,
			TableName:  saved.TableName,
			Columns:    saved.Columns,
			Tokenizer:  "simple",
			Persistent: true,
			DataDir:    m.dataDir,
		}

		idx := NewFTSIndex(config)
		if err := idx.Load(); err != nil {
			continue
		}

		m.indexes[saved.Name] = idx
	}

	return nil
}

// parseJSON is a helper to parse JSON data.
func parseJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// GetStats returns statistics for all indexes.
func (m *FTSManager) GetStats() []IndexStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]IndexStats, 0, len(m.indexes))
	for _, idx := range m.indexes {
		stats = append(stats, idx.Stats())
	}
	return stats
}

// DropIndexForTable removes all FTS indexes for a dropped table.
func (m *FTSManager) DropIndexForTable(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, idx := range m.indexes {
		if idx.TableName() == tableName {
			if idx.storagePath != "" {
				os.Remove(idx.storagePath)
			}
			delete(m.indexes, name)
		}
	}
	return nil
}

// Close closes all indexes.
func (m *FTSManager) Close() error {
	return m.SaveAll()
}