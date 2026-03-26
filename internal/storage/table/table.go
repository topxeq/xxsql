// Package table provides table management for XxSql storage engine.
package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/topxeq/xxsql/internal/storage/btree"
	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/sequence"
	"github.com/topxeq/xxsql/internal/storage/types"
)

const (
	// MetaFileExt is the extension for table metadata files.
	MetaFileExt = ".xmeta"

	// DataFileExt is the extension for table data files.
	DataFileExt = ".xdb"

	// IndexFileExt is the extension for index files.
	IndexFileExt = ".xidx"
)

// TableState represents the state of a table.
type TableState uint8

const (
	TableStateActive TableState = iota
	TableStateDeleting
	TableStateDeleted
)

// TableInfo represents table metadata.
type TableInfo struct {
	Name            string                     `json:"name"`
	Columns         []*types.ColumnInfo        `json:"columns"`
	PrimaryKey      []string                   `json:"primary_key,omitempty"`
	Indexes         []*IndexInfo               `json:"indexes,omitempty"`
	CheckConstraints []*types.CheckConstraintInfo `json:"check_constraints,omitempty"`
	ForeignKeys     []*types.ForeignKeyInfo    `json:"foreign_keys,omitempty"`
	CreatedAt       time.Time                  `json:"created_at"`
	ModifiedAt      time.Time                  `json:"modified_at"`
	RowCount        uint64                     `json:"row_count"`
	NextRowID       uint64                     `json:"next_row_id"`
	NextPageID      page.PageID                `json:"next_page_id"`
	RootPageID      page.PageID                `json:"root_page_id"`
	State           TableState                 `json:"state"`
}

// IndexInfo represents index metadata.
type IndexInfo struct {
	Name      string      `json:"name"`
	Columns   []string    `json:"columns"`
	Unique    bool        `json:"unique"`
	RootPageID page.PageID `json:"root_page_id"`
}

// Table represents an open table.
type Table struct {
	info     *TableInfo
	dataFile *os.File
	mu       sync.RWMutex
	dataDir  string

	// Page cache (simple in-memory cache for now)
	pages    map[page.PageID]*page.Page
	pageMu   sync.RWMutex

	// Sequence manager for auto-increment columns
	seqMgr *sequence.Manager

	// Index manager
	indexMgr *btree.IndexManager

	// Column name to index map
	columnMap map[string]int

	// Row ID to page mapping for efficient lookup
	rowToPage map[row.RowID]page.PageID
	rowPageMu sync.RWMutex
}

// OpenTable opens or creates a table.
func OpenTable(dataDir, name string, columns []*types.ColumnInfo) (*Table, error) {
	t := &Table{
		info: &TableInfo{
			Name:       name,
			Columns:    columns,
			CreatedAt:  time.Now(),
			ModifiedAt: time.Now(),
			NextRowID:  1,
			NextPageID: 1,
			State:      TableStateActive,
		},
		dataDir:    dataDir,
		pages:      make(map[page.PageID]*page.Page),
		indexMgr:   btree.NewIndexManager(name, nil),
		columnMap:  make(map[string]int),
		rowToPage:  make(map[row.RowID]page.PageID),
	}

	// Build column map
	for i, col := range columns {
		t.columnMap[col.Name] = i
	}

	// Initialize sequence manager
	seqPath := filepath.Join(dataDir, name+"_sequences.seq")
	t.seqMgr = sequence.NewManager(seqPath, nil)

	// Check if table exists
	metaPath := filepath.Join(dataDir, name+MetaFileExt)
	if _, err := os.Stat(metaPath); err == nil {
		// Load existing table
		if err := t.loadMeta(); err != nil {
			return nil, fmt.Errorf("failed to load table metadata: %w", err)
		}
		// Load sequences
		if err := t.seqMgr.Load(); err != nil {
			// Non-fatal, sequences will start from defaults
		}
	} else {
		// Create new table
		if err := t.create(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Open data file
	dataPath := filepath.Join(dataDir, name+DataFileExt)
	f, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	t.dataFile = f

	// Load first page if exists
	if info, _ := f.Stat(); info.Size() >= page.PageSize {
		p, err := t.readPage(1)
		if err == nil {
			t.pages[1] = p
			t.info.RootPageID = 1
		}
	}

	// Create primary key index if needed
	t.createPrimaryKeyIndex()

	// Create sequences for auto-increment columns if not exists
	t.createAutoIncrementSequences()

	return t, nil
}

// NewTempTable creates a temporary in-memory table.
// Temp tables are not persisted to disk and are cleared when the session ends.
func NewTempTable(name string, columns []*types.ColumnInfo) *Table {
	t := &Table{
		info: &TableInfo{
			Name:       name,
			Columns:    columns,
			CreatedAt:  time.Now(),
			ModifiedAt: time.Now(),
			NextRowID:  1,
			NextPageID: 2, // Start from 2 since page 1 is root
			RootPageID: 1, // Root page is page 1
			State:      TableStateActive,
		},
		dataDir:   "", // No data dir for temp tables
		pages:     make(map[page.PageID]*page.Page),
		indexMgr:  btree.NewIndexManager(name, nil),
		columnMap: make(map[string]int),
		rowToPage: make(map[row.RowID]page.PageID),
	}

	// Build column map
	for i, col := range columns {
		t.columnMap[col.Name] = i
	}

	// Initialize in-memory sequence manager (no file)
	t.seqMgr = sequence.NewManager("", nil)

	// Create the initial root page for data
	rootPage := page.NewPage(1, page.PageTypeData)
	rootPage.SetLeaf(true)
	t.pages[1] = rootPage

	// Create primary key index if needed
	t.createPrimaryKeyIndex()

	// Create sequences for auto-increment columns
	t.createAutoIncrementSequences()

	return t
}

// SetDataFile sets the data file for the table.
// This is used for temp tables to enable persistence during the session.
func (t *Table) SetDataFile(f *os.File) {
	t.dataFile = f
}

// createPrimaryKeyIndex creates a primary key index if the table has a primary key.
// For composite primary keys, we don't create a btree index (not supported yet),
// but we still track the primary key columns in info.PrimaryKey.
func (t *Table) createPrimaryKeyIndex() {
	// Collect all primary key columns
	pkColumns := make([]string, 0)
	for _, col := range t.info.Columns {
		if col.PrimaryKey {
			pkColumns = append(pkColumns, col.Name)
		}
	}

	if len(pkColumns) == 0 {
		return
	}

	// Store all primary key columns
	t.info.PrimaryKey = pkColumns

	// Only create btree index for single-column primary keys
	// Composite key indexes would require a different implementation
	if len(pkColumns) == 1 {
		for _, col := range t.info.Columns {
			if col.PrimaryKey {
				t.indexMgr.CreateIndex("PRIMARY", []string{col.Name}, btree.IndexTypePrimary, col.Type)
				break
			}
		}
	}
}

// create creates a new table.
func (t *Table) create() error {
	// Create data directory if not exists
	if err := os.MkdirAll(t.dataDir, 0755); err != nil {
		return err
	}

	// Create first data page
	p := page.NewPage(1, page.PageTypeData)
	p.SetLeaf(true)
	t.pages[1] = p
	t.info.RootPageID = 1
	t.info.NextPageID = 2

	// Save metadata
	return t.saveMeta()
}

// loadMeta loads table metadata from disk.
func (t *Table) loadMeta() error {
	metaPath := filepath.Join(t.dataDir, t.info.Name+MetaFileExt)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, t.info)
}

// saveMeta saves table metadata to disk.
func (t *Table) saveMeta() error {
	metaPath := filepath.Join(t.dataDir, t.info.Name+MetaFileExt)
	data, err := json.MarshalIndent(t.info, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metaPath, data, 0644)
}

// Close closes the table.
func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Flush all pages
	if err := t.Flush(); err != nil {
		return err
	}

	// Save metadata
	if err := t.saveMeta(); err != nil {
		return err
	}

	// Close sequence manager
	if t.seqMgr != nil {
		t.seqMgr.Close()
	}

	// Close data file
	if t.dataFile != nil {
		return t.dataFile.Close()
	}
	return nil
}

// Flush flushes all dirty pages to disk.
// For temp tables, this is a no-op since they're in-memory only.
func (t *Table) Flush() error {
	// Temp tables don't persist
	if t.dataFile == nil {
		return nil
	}

	t.pageMu.Lock()
	defer t.pageMu.Unlock()

	for _, p := range t.pages {
		if p.Modified {
			if err := t.writePage(p); err != nil {
				return err
			}
			p.Modified = false
		}
	}

	// Sync file to ensure data is written
	if t.dataFile != nil {
		return t.dataFile.Sync()
	}
	return nil
}

// readPage reads a page from disk.
func (t *Table) readPage(id page.PageID) (*page.Page, error) {
	if t.dataFile == nil {
		return nil, fmt.Errorf("data file not open")
	}

	offset := int64(id-1) * page.PageSize
	buf := make([]byte, page.PageSize)

	n, err := t.dataFile.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n != page.PageSize {
		return nil, fmt.Errorf("short read: expected %d, got %d", page.PageSize, n)
	}

	return page.NewPageFromBytes(buf)
}

// writePage writes a page to disk.
func (t *Table) writePage(p *page.Page) error {
	if t.dataFile == nil {
		return fmt.Errorf("data file not open")
	}

	offset := int64(p.ID-1) * page.PageSize
	data := p.ToBytes()

	n, err := t.dataFile.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != page.PageSize {
		return fmt.Errorf("short write: expected %d, got %d", page.PageSize, n)
	}

	return nil
}

// getPage gets a page from cache or loads it from disk.
func (t *Table) getPage(id page.PageID) (*page.Page, error) {
	t.pageMu.RLock()
	if p, ok := t.pages[id]; ok {
		t.pageMu.RUnlock()
		return p, nil
	}
	t.pageMu.RUnlock()

	// For temp tables (no dataFile), return nil
	if t.dataFile == nil {
		return nil, fmt.Errorf("page %d not found", id)
	}

	// Load from disk
	p, err := t.readPage(id)
	if err != nil {
		return nil, err
	}

	t.pageMu.Lock()
	t.pages[id] = p
	t.pageMu.Unlock()

	return p, nil
}

// newPage creates a new page.
func (t *Table) newPage() *page.Page {
	id := t.info.NextPageID
	t.info.NextPageID++

	p := page.NewPage(id, page.PageTypeData)
	p.SetLeaf(true)

	t.pageMu.Lock()
	t.pages[id] = p
	t.pageMu.Unlock()

	return p
}

// Insert inserts a row into the table.
func (t *Table) Insert(values []types.Value) (row.RowID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.info.State != TableStateActive {
		return row.InvalidRowID, fmt.Errorf("table is not active")
	}

	if len(values) != len(t.info.Columns) {
		return row.InvalidRowID, fmt.Errorf("column count mismatch: expected %d, got %d",
			len(t.info.Columns), len(values))
	}

	// Handle auto-increment columns
	for i, col := range t.info.Columns {
		if col.AutoIncr && values[i].Null {
			values[i] = types.NewIntValue(int64(t.nextSeq()))
		}
	}

	// Check primary key uniqueness (support composite keys)
	if len(t.info.PrimaryKey) > 0 {
		// Build list of primary key column indices and values
		pkIndices := make([]int, 0, len(t.info.PrimaryKey))
		pkValues := make([]types.Value, 0, len(t.info.PrimaryKey))
		validPK := true

		for _, pkCol := range t.info.PrimaryKey {
			pkIdx, ok := t.columnMap[pkCol]
			if !ok || pkIdx >= len(values) {
				validPK = false
				break
			}
			pkIndices = append(pkIndices, pkIdx)
			pkValues = append(pkValues, values[pkIdx])
		}

		if validPK {
			// For single-column primary key, use the index if available
			// For composite keys, always scan to check all columns together
			if len(pkIndices) == 1 && t.indexMgr.HasPrimary() {
				if _, found := t.indexMgr.GetPrimary().Search(pkValues[0]); found {
					return row.InvalidRowID, fmt.Errorf("duplicate key value: %v", pkValues[0])
				}
			} else {
				// For composite keys, scan pages directly (we already hold the lock)
				// Don't call Scan() as it would try to acquire a read lock -> deadlock
				for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
					p, err := t.getPage(pageID)
					if err != nil {
						continue
					}

					rowCount := p.RowCount()
					for i := 0; i < rowCount; i++ {
						rowData, err := p.GetRow(i)
						if err != nil {
							continue
						}

						r, err := row.DeserializeRow(rowData, t.info.Columns)
						if err != nil {
							continue
						}

						match := true
						for _, pkIdx := range pkIndices {
							if pkIdx >= len(r.Values) || pkIdx >= len(values) {
								match = false
								break
							}
							if r.Values[pkIdx].Compare(values[pkIdx]) != 0 {
								match = false
								break
							}
						}
						if match {
							// Build key string for error message
							keyStr := ""
							for i, v := range pkValues {
								if i > 0 {
									keyStr += ", "
								}
								keyStr += v.String()
							}
							return row.InvalidRowID, fmt.Errorf("duplicate key value: (%s)", keyStr)
						}
					}
				}
			}
		}
	}

	// Assign row ID
	rowID := row.RowID(t.info.NextRowID)
	t.info.NextRowID++

	// Serialize row
	rowData, err := row.SerializeRow(rowID, values)
	if err != nil {
		return row.InvalidRowID, err
	}

	// Find a page with enough space
	var targetPage *page.Page
	rootPage, err := t.getPage(t.info.RootPageID)
	if err != nil {
		return row.InvalidRowID, err
	}

	// B+ tree insertion strategy:
	// 1. Try to find a page with enough space starting from root
	// 2. If current page is full, check sibling pages
	// 3. If no space available, allocate new page
	targetPage = rootPage

	// Try to find a page with space using B+ tree traversal
	if targetPage.FreeSpace() < uint16(len(rowData)+4) {
		// Current page doesn't have enough space, try to find another
		found := false

		// Check existing pages for space
		t.pageMu.RLock()
		for pageID, p := range t.pages {
			if pageID != t.info.RootPageID && p.FreeSpace() >= uint16(len(rowData)+4) {
				targetPage = p
				found = true
				break
			}
		}
		t.pageMu.RUnlock()

		// If no existing page has space, create new page
		if !found {
			targetPage = t.newPage()
		}
	}

	// Insert row
	_, err = targetPage.InsertRow(rowData)
	if err != nil {
		return row.InvalidRowID, err
	}

	// Write page to disk immediately
	if err := t.writePage(targetPage); err != nil {
		return row.InvalidRowID, fmt.Errorf("failed to write page: %w", err)
	}

	// Record row ID to page mapping
	t.rowPageMu.Lock()
	t.rowToPage[rowID] = targetPage.ID
	t.rowPageMu.Unlock()

	// Update indexes
	if err := t.indexMgr.InsertIntoIndexes(values, rowID, t.columnMap); err != nil {
		return row.InvalidRowID, err
	}

	t.info.RowCount++
	t.info.ModifiedAt = time.Now()

	// Save metadata to persist row count
	_ = t.saveMeta()

	return rowID, nil
}

// nextSeq returns the next sequence value for auto-increment.
func (t *Table) nextSeq() uint64 {
	// Use sequence manager if available
	if t.seqMgr != nil {
		seqName := fmt.Sprintf("%s_auto_incr", t.info.Name)
		val, err := t.seqMgr.NextValue(seqName)
		if err == nil {
			return uint64(val)
		}
		// Fall through to default if sequence not found
	}
	return 0
}

// createAutoIncrementSequences creates sequences for auto-increment columns.
func (t *Table) createAutoIncrementSequences() {
	for _, col := range t.info.Columns {
		if col.AutoIncr {
			seqName := fmt.Sprintf("%s_auto_incr", t.info.Name)
			if !t.seqMgr.Exists(seqName) {
				config := sequence.DefaultSequenceConfig(seqName)
				config.Start = 1
				config.MinValue = 1
				t.seqMgr.CreateSequence(config)
			}
			break // Only one auto-increment column per table for now
		}
	}
}

// Scan returns all rows in the table.
func (t *Table) Scan() ([]*row.Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.info.State != TableStateActive {
		return nil, fmt.Errorf("table is not active")
	}

	var rows []*row.Row

	// Iterate through all pages
	for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
		p, err := t.getPage(pageID)
		if err != nil {
			continue
		}

		// Read all rows from page
		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}

			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}

			rows = append(rows, r)
		}
	}

	return rows, nil
}

// FindByKey finds a row by primary key.
func (t *Table) FindByKey(key types.Value) (*row.Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.indexMgr.HasPrimary() {
		return nil, fmt.Errorf("no primary key index")
	}

	rowID, found := t.indexMgr.GetPrimary().Search(key)
	if !found {
		return nil, fmt.Errorf("key not found")
	}

	// Use row ID to page mapping for efficient lookup
	t.rowPageMu.RLock()
	pageID, hasMapping := t.rowToPage[rowID]
	t.rowPageMu.RUnlock()

	if hasMapping {
		// Direct page lookup
		p, err := t.getPage(pageID)
		if err != nil {
			return nil, err
		}

		// Find the row in the page
		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}
			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}
			if r.ID == rowID {
				return r, nil
			}
		}
	}

	// Fallback: scan all rows if mapping not found
	rows, err := t.Scan()
	if err != nil {
		return nil, err
	}

	for _, r := range rows {
		if r.ID == rowID {
			return r, nil
		}
	}

	return nil, fmt.Errorf("row not found")
}

// GetRowsByRowIDs fetches rows by their row IDs.
// Returns a map of rowID to Row for efficient lookup.
func (t *Table) GetRowsByRowIDs(rowIDs []row.RowID) (map[row.RowID]*row.Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.info.State != TableStateActive {
		return nil, fmt.Errorf("table is not active")
	}

	result := make(map[row.RowID]*row.Row, len(rowIDs))
	if len(rowIDs) == 0 {
		return result, nil
	}

	// Build a set of rowIDs we need to find
	needRows := make(map[row.RowID]bool, len(rowIDs))
	for _, id := range rowIDs {
		needRows[id] = true
	}

	// Build page -> rowIDs mapping
	pageToRows := make(map[page.PageID][]row.RowID)
	t.rowPageMu.RLock()
	for _, id := range rowIDs {
		if pgID, ok := t.rowToPage[id]; ok {
			pageToRows[pgID] = append(pageToRows[pgID], id)
		}
	}
	t.rowPageMu.RUnlock()

	// Fetch rows from pages
	for pgID, pgRowIDs := range pageToRows {
		// Create a set of row IDs we need from this page
		needFromPage := make(map[row.RowID]bool, len(pgRowIDs))
		for _, id := range pgRowIDs {
			needFromPage[id] = true
		}

		p, err := t.getPage(pgID)
		if err != nil {
			continue
		}

		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}
			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}
			if needFromPage[r.ID] {
				result[r.ID] = r
				delete(needFromPage, r.ID)
				if len(needFromPage) == 0 {
					break // All rows found from this page
				}
			}
		}
	}

	// If we didn't find all rows via the mapping, scan all pages
	if len(result) < len(needRows) {
		for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
			// Skip pages we already processed
			if _, processed := pageToRows[pageID]; processed {
				continue
			}

			p, err := t.getPage(pageID)
			if err != nil {
				continue
			}

			rowCount := p.RowCount()
			for i := 0; i < rowCount; i++ {
				rowData, err := p.GetRow(i)
				if err != nil {
					continue
				}
				r, err := row.DeserializeRow(rowData, t.info.Columns)
				if err != nil {
					continue
				}
				if needRows[r.ID] && result[r.ID] == nil {
					result[r.ID] = r
					if len(result) == len(needRows) {
						return result, nil
					}
				}
			}
		}
	}

	return result, nil
}

// IndexScanResult represents the result of an index scan operation.
type IndexScanResult struct {
	IndexName string
	RowCount  int
}

// IndexRangeScan scans rows using an index for a range of values.
// Returns row IDs that match the range condition.
func (t *Table) IndexRangeScan(indexName string, startValue, endValue types.Value, includeStart, includeEnd bool) ([]row.RowID, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.info.State != TableStateActive {
		return nil, fmt.Errorf("table is not active")
	}

	var idx *btree.Index
	if indexName == "PRIMARY" || indexName == "primary" {
		idx = t.indexMgr.GetPrimary()
	} else {
		var err error
		idx, err = t.indexMgr.GetIndex(indexName)
		if err != nil {
			return nil, fmt.Errorf("index %s not found", indexName)
		}
	}

	if idx == nil {
		return nil, fmt.Errorf("index %s not found", indexName)
	}

	// Handle different scan types
	if startValue.Null && endValue.Null {
		// Full index scan
		return idx.Scan(), nil
	}

	// Range scan
	// For now, we'll use the Range method and filter by inclusion
	rowIDs := idx.Range(startValue, endValue)
	return rowIDs, nil
}

// IndexPointLookup looks up rows by an exact value in an index.
func (t *Table) IndexPointLookup(indexName string, value types.Value) ([]row.RowID, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.info.State != TableStateActive {
		return nil, fmt.Errorf("table is not active")
	}

	var idx *btree.Index
	if indexName == "PRIMARY" || indexName == "primary" {
		idx = t.indexMgr.GetPrimary()
	} else {
		var err error
		idx, err = t.indexMgr.GetIndex(indexName)
		if err != nil {
			return nil, fmt.Errorf("index %s not found", indexName)
		}
	}

	if idx == nil {
		return nil, fmt.Errorf("index %s not found", indexName)
	}

	// Point lookup
	rowID, found := idx.Search(value)
	if !found {
		return []row.RowID{}, nil
	}
	return []row.RowID{rowID}, nil
}

// IndexConditionScan scans an index based on a comparison condition.
// op can be "=", "<", "<=", ">", ">="
func (t *Table) IndexConditionScan(indexName string, op string, value types.Value) ([]row.RowID, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.info.State != TableStateActive {
		return nil, fmt.Errorf("table is not active")
	}

	var idx *btree.Index
	if indexName == "PRIMARY" || indexName == "primary" {
		idx = t.indexMgr.GetPrimary()
	} else {
		var err error
		idx, err = t.indexMgr.GetIndex(indexName)
		if err != nil {
			return nil, fmt.Errorf("index %s not found", indexName)
		}
	}

	if idx == nil {
		return nil, fmt.Errorf("index %s not found", indexName)
	}

	switch op {
	case "=":
		// Use SearchAll to handle non-unique indexes
		rowIDs := idx.SearchAll(value)
		if len(rowIDs) == 0 {
			return []row.RowID{}, nil
		}
		return rowIDs, nil

	case "<":
		return idx.ScanLessThan(value, false), nil
	case "<=":
		return idx.ScanLessThan(value, true), nil
	case ">":
		return idx.ScanGreaterThan(value, false), nil
	case ">=":
		return idx.ScanGreaterThan(value, true), nil

	default:
		// Unknown operator, fall back to full scan
		return idx.Scan(), nil
	}
}

// GetIndexForColumn returns the best index for a given column.
// Returns the index name and whether it's usable.
func (t *Table) GetIndexForColumn(columnName string) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check primary key first
	if t.indexMgr.HasPrimary() {
		pk := t.indexMgr.GetPrimary()
		if pk != nil && len(pk.Info.Columns) == 1 && pk.Info.Columns[0] == columnName {
			return "PRIMARY", true
		}
	}

	// Check other indexes
	for _, idxName := range t.indexMgr.ListIndexes() {
		idx, err := t.indexMgr.GetIndex(idxName)
		if err != nil {
			continue
		}
		if len(idx.Info.Columns) >= 1 && idx.Info.Columns[0] == columnName {
			return idxName, true
		}
	}

	return "", false
}

// GetIndexForColumns returns the best index for the given columns (for composite index support).
// The columns should be in the order they appear in the WHERE clause.
func (t *Table) GetIndexForColumns(columns []string) (string, bool, int) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(columns) == 0 {
		return "", false, 0
	}

	// Check primary key first
	if t.indexMgr.HasPrimary() {
		pk := t.indexMgr.GetPrimary()
		if pk != nil {
			matchCount := countPrefixMatch(pk.Info.Columns, columns)
			if matchCount > 0 {
				return "PRIMARY", true, matchCount
			}
		}
	}

	// Check other indexes, find the one with most matching prefix columns
	bestIndex := ""
	bestMatch := 0
	for _, idxName := range t.indexMgr.ListIndexes() {
		idx, err := t.indexMgr.GetIndex(idxName)
		if err != nil {
			continue
		}
		matchCount := countPrefixMatch(idx.Info.Columns, columns)
		if matchCount > bestMatch {
			bestIndex = idxName
			bestMatch = matchCount
		}
	}

	if bestMatch > 0 {
		return bestIndex, true, bestMatch
	}

	return "", false, 0
}

// countPrefixMatch counts how many columns in the index match the prefix of the given columns.
func countPrefixMatch(indexCols, queryCols []string) int {
	match := 0
	for i := 0; i < len(indexCols) && i < len(queryCols); i++ {
		if indexCols[i] == queryCols[i] {
			match++
		} else {
			break
		}
	}
	return match
}

// EstimateSelectivity estimates the selectivity of an index scan.
// Returns an estimated number of rows that would be scanned.
func (t *Table) EstimateSelectivity(indexName string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	totalRows := int(t.info.RowCount)
	if totalRows == 0 {
		return 0
	}

	var idx *btree.Index
	if indexName == "PRIMARY" || indexName == "primary" {
		idx = t.indexMgr.GetPrimary()
	} else {
		idx, _ = t.indexMgr.GetIndex(indexName)
	}

	if idx == nil {
		return totalRows // Fallback to full scan
	}

	// For point lookup, selectivity is 1
	// For range scan, estimate based on index type
	indexRows := idx.Count()
	if indexRows == 0 {
		return 1 // Assume at least 1 row if index exists
	}

	// Simple heuristic: for unique index, assume point lookup (1 row)
	// For non-unique, estimate based on cardinality
	if idx.Info.Type == btree.IndexTypeUnique || idx.Info.Type == btree.IndexTypePrimary {
		return 1
	}

	// Estimate average rows per distinct value
	avgRowsPerValue := totalRows / indexRows
	if avgRowsPerValue < 1 {
		avgRowsPerValue = 1
	}
	return avgRowsPerValue
}

// CreateIndex creates a new index on the table.
func (t *Table) CreateIndex(name string, columns []string, unique bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(columns) == 0 {
		return fmt.Errorf("index must have at least one column")
	}

	// Find column type
	var keyType types.TypeID
	for _, colName := range columns {
		if idx, ok := t.columnMap[colName]; ok {
			keyType = t.info.Columns[idx].Type
			break
		}
	}

	// Create index
	idxType := btree.IndexTypeNonUnique
	if unique {
		idxType = btree.IndexTypeUnique
	}

	idx, err := t.indexMgr.CreateIndex(name, columns, idxType, keyType)
	if err != nil {
		return err
	}

	// Populate index with existing data
	// We already hold the lock, so scan pages directly
	for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
		p, err := t.getPage(pageID)
		if err != nil {
			continue
		}

		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}

			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}

			// Get key value
			if len(columns) > 0 {
				colIdx, ok := t.columnMap[columns[0]]
				if ok && colIdx < len(r.Values) {
					key := r.Values[colIdx]
					if !key.Null {
						if err := idx.Insert(key, r.ID); err != nil {
							return fmt.Errorf("failed to populate index: %w", err)
						}
					}
				}
			}
		}
	}

	// Add to metadata
	t.info.Indexes = append(t.info.Indexes, &IndexInfo{
		Name:    name,
		Columns: columns,
		Unique:  unique,
	})

	return nil
}

// DropIndex drops an index.
func (t *Table) DropIndex(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.indexMgr.DropIndex(name); err != nil {
		return err
	}

	// Remove from metadata
	for i, idx := range t.info.Indexes {
		if idx.Name == name {
			t.info.Indexes = append(t.info.Indexes[:i], t.info.Indexes[i+1:]...)
			break
		}
	}

	return nil
}

// GetInfo returns the table info.
func (t *Table) GetInfo() *TableInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.info
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.info.Name
}

// Columns returns the column definitions.
func (t *Table) Columns() []*types.ColumnInfo {
	return t.info.Columns
}

// RowCount returns the number of rows.
func (t *Table) RowCount() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.info.RowCount
}

// GetIndexManager returns the index manager.
func (t *Table) GetIndexManager() *btree.IndexManager {
	return t.indexMgr
}

// Drop drops the table.
func (t *Table) Drop() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.info.State = TableStateDeleted

	// Close data file
	if t.dataFile != nil {
		t.dataFile.Close()
		t.dataFile = nil
	}

	// Delete files
	dataPath := filepath.Join(t.dataDir, t.info.Name+DataFileExt)
	metaPath := filepath.Join(t.dataDir, t.info.Name+MetaFileExt)

	os.Remove(dataPath)
	os.Remove(metaPath)

	return nil
}

// Truncate removes all rows from the table.
func (t *Table) Truncate() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Clear pages
	t.pageMu.Lock()
	t.pages = make(map[page.PageID]*page.Page)
	t.pageMu.Unlock()

	// Reset counters
	t.info.RowCount = 0
	t.info.NextRowID = 1
	t.info.NextPageID = 2
	t.info.ModifiedAt = time.Now()

	// Reset auto-increment sequence
	if t.seqMgr != nil {
		seqName := fmt.Sprintf("%s_auto_incr", t.info.Name)
		t.seqMgr.Reset(seqName)
	}

	// Clear indexes by recreating the index manager
	t.indexMgr = btree.NewIndexManager(t.info.Name, nil)
	t.createPrimaryKeyIndex()

	// Truncate data file
	if t.dataFile != nil {
		t.dataFile.Truncate(0)
		t.dataFile.Seek(0, 0)
	}

	// Create new root page
	p := page.NewPage(1, page.PageTypeData)
	p.SetLeaf(true)
	t.pages[1] = p
	t.info.RootPageID = 1

	return nil
}

// GetAllRows returns all rows in the table.
func (t *Table) GetAllRows() ([]*row.Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var rows []*row.Row

	// Iterate through all pages
	for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
		p, err := t.getPage(pageID)
		if err != nil {
			continue
		}

		// Read all rows from page
		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}

			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}

			rows = append(rows, r)
		}
	}

	return rows, nil
}

// AddColumn adds a column to the table.
func (t *Table) AddColumn(col *types.ColumnInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if column already exists
	if _, exists := t.columnMap[col.Name]; exists {
		return fmt.Errorf("column %s already exists", col.Name)
	}

	// Add column to info
	idx := len(t.info.Columns)
	t.info.Columns = append(t.info.Columns, col)
	t.columnMap[col.Name] = idx
	t.info.ModifiedAt = time.Now()

	// Save metadata
	return t.saveMeta()
}

// DropColumn drops a column from the table.
func (t *Table) DropColumn(colName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, exists := t.columnMap[colName]
	if !exists {
		return fmt.Errorf("column %s does not exist", colName)
	}

	// Check if it's a primary key
	if t.info.Columns[idx].PrimaryKey {
		return fmt.Errorf("cannot drop primary key column %s", colName)
	}

	// Remove column from info
	t.info.Columns = append(t.info.Columns[:idx], t.info.Columns[idx+1:]...)

	// Rebuild column map
	t.columnMap = make(map[string]int)
	for i, c := range t.info.Columns {
		t.columnMap[c.Name] = i
	}

	t.info.ModifiedAt = time.Now()

	// Save metadata
	return t.saveMeta()
}

// ModifyColumn modifies a column definition.
func (t *Table) ModifyColumn(col *types.ColumnInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, exists := t.columnMap[col.Name]
	if !exists {
		return fmt.Errorf("column %s does not exist", col.Name)
	}

	// Preserve some original values
	original := t.info.Columns[idx]
	if col.PrimaryKey {
		col.PrimaryKey = original.PrimaryKey // Can't change PK via MODIFY
	}
	if col.AutoIncr && !original.AutoIncr {
		// Enabling auto-increment
		if t.seqMgr != nil {
			seqName := fmt.Sprintf("%s_auto_incr", t.info.Name)
			if !t.seqMgr.Exists(seqName) {
				config := sequence.DefaultSequenceConfig(seqName)
				config.Start = 1
				config.MinValue = 1
				t.seqMgr.CreateSequence(config)
			}
		}
	}

	t.info.Columns[idx] = col
	t.info.ModifiedAt = time.Now()

	return t.saveMeta()
}

// RenameColumn renames a column.
func (t *Table) RenameColumn(oldName, newName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, exists := t.columnMap[oldName]
	if !exists {
		return fmt.Errorf("column %s does not exist", oldName)
	}

	if _, exists := t.columnMap[newName]; exists {
		return fmt.Errorf("column %s already exists", newName)
	}

	// Update column name
	t.info.Columns[idx].Name = newName

	// Rebuild column map
	t.columnMap = make(map[string]int)
	for i, c := range t.info.Columns {
		t.columnMap[c.Name] = i
	}

	t.info.ModifiedAt = time.Now()

	return t.saveMeta()
}

// Rename renames the table.
func (t *Table) Rename(newName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	oldName := t.info.Name
	oldMetaPath := filepath.Join(t.dataDir, oldName+MetaFileExt)
	oldDataPath := filepath.Join(t.dataDir, oldName+DataFileExt)

	// Update table info
	t.info.Name = newName
	t.info.ModifiedAt = time.Now()

	// Save new metadata
	if err := t.saveMeta(); err != nil {
		return err
	}

	// Rename files
	newDataPath := filepath.Join(t.dataDir, newName+DataFileExt)

	// Close file handle before rename
	if t.dataFile != nil {
		t.dataFile.Close()
	}

	// Rename data file
	if err := os.Rename(oldDataPath, newDataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to rename data file: %w", err)
	}

	// Remove old metadata file
	os.Remove(oldMetaPath)

	// Reopen data file
	f, err := os.OpenFile(newDataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen data file: %w", err)
	}
	t.dataFile = f

	return nil
}

// Update updates rows that match the predicate.
func (t *Table) Update(predicate func(*row.Row) bool, updates map[int]types.Value) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.info.State != TableStateActive {
		return 0, fmt.Errorf("table is not active")
	}

	var affected int

	// Iterate through all pages
	for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
		p, err := t.getPage(pageID)
		if err != nil {
			continue
		}

		// Read all rows from page
		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}

			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}

			// Check if row matches predicate
			if predicate(r) {
				// Apply updates
				for colIdx, val := range updates {
					if colIdx < len(r.Values) {
						r.Values[colIdx] = val
					}
				}

				// Re-serialize and update
				newRowData, err := row.SerializeRow(r.ID, r.Values)
				if err != nil {
					continue
				}

				// Try to update in place
				if err := p.UpdateRow(i, newRowData); err != nil {
					// If the new data is larger, we need to delete and reinsert
					// Mark the old row as deleted
					p.DeleteRow(i)
					// Insert the new row at the end of this page or a new page
					if p.FreeSpace() >= uint16(len(newRowData)+4) {
						p.InsertRow(newRowData)
					} else {
						// Need a new page
						newPage := t.newPage()
						newPage.InsertRow(newRowData)
						t.writePage(newPage)
					}
				}

				affected++
			}
		}

		// Write page if modified
		if p.Modified {
			t.writePage(p)
			p.Modified = false
		}
	}

	if affected > 0 {
		t.info.ModifiedAt = time.Now()
	}

	return affected, nil
}

// UpdateWithFunc updates rows using a function that computes new values based on the current row.
// This is used for expressions like "SET col = col + 1" where the new value depends on the current row.
func (t *Table) UpdateWithFunc(predicate func(*row.Row) bool, updateFunc func(*row.Row) (map[int]types.Value, error)) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.info.State != TableStateActive {
		return 0, fmt.Errorf("table is not active")
	}

	var affected int

	// Iterate through all pages
	for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
		p, err := t.getPage(pageID)
		if err != nil {
			continue
		}

		// Read all rows from page
		rowCount := p.RowCount()
		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}

			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}

			// Check if row matches predicate
			if predicate(r) {
				// Call update function to get the updates for this specific row
				updates, err := updateFunc(r)
				if err != nil {
					continue
				}

				// Apply updates
				for colIdx, val := range updates {
					if colIdx < len(r.Values) {
						r.Values[colIdx] = val
					}
				}

				// Re-serialize and update
				newRowData, err := row.SerializeRow(r.ID, r.Values)
				if err != nil {
					continue
				}

				// Try to update in place
				if err := p.UpdateRow(i, newRowData); err != nil {
					// If the new data is larger, we need to delete and reinsert
					p.DeleteRow(i)
					if p.FreeSpace() >= uint16(len(newRowData)+4) {
						p.InsertRow(newRowData)
					} else {
						newPage := t.newPage()
						newPage.InsertRow(newRowData)
						t.writePage(newPage)
					}
				}

				affected++
			}
		}

		// Write page if modified
		if p.Modified {
			t.writePage(p)
			p.Modified = false
		}
	}

	if affected > 0 {
		t.info.ModifiedAt = time.Now()
	}

	return affected, nil
}

// Delete deletes rows that match the predicate.
func (t *Table) Delete(predicate func(*row.Row) bool) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.info.State != TableStateActive {
		return 0, fmt.Errorf("table is not active")
	}

	var affected int

	// Iterate through all pages
	for pageID := page.PageID(1); pageID < t.info.NextPageID; pageID++ {
		p, err := t.getPage(pageID)
		if err != nil {
			continue
		}

		// Read all rows from page, tracking which to delete
		var toDelete []int
		rowCount := p.RowCount()

		for i := 0; i < rowCount; i++ {
			rowData, err := p.GetRow(i)
			if err != nil {
				continue
			}

			r, err := row.DeserializeRow(rowData, t.info.Columns)
			if err != nil {
				continue
			}

			if predicate(r) {
				toDelete = append(toDelete, i)
			}
		}

		// Delete rows (in reverse order to maintain indices)
		for i := len(toDelete) - 1; i >= 0; i-- {
			if err := p.DeleteRow(toDelete[i]); err == nil {
				affected++
			}
		}

		// Write page if modified
		if p.Modified {
			t.writePage(p)
			p.Modified = false
		}
	}

	if affected > 0 {
		t.info.RowCount -= uint64(affected)
		t.info.ModifiedAt = time.Now()
		// Save metadata to persist row count
		_ = t.saveMeta()
	}

	return affected, nil
}

// AddCheckConstraints adds CHECK constraints to the table.
func (t *Table) AddCheckConstraints(constraints []*types.CheckConstraintInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.info.CheckConstraints = append(t.info.CheckConstraints, constraints...)
	t.info.ModifiedAt = time.Now()

	return t.saveMeta()
}

// AddForeignKeys adds FOREIGN KEY constraints to the table.
func (t *Table) AddForeignKeys(fks []*types.ForeignKeyInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.info.ForeignKeys = append(t.info.ForeignKeys, fks...)
	t.info.ModifiedAt = time.Now()

	return t.saveMeta()
}

// GetCheckConstraints returns the CHECK constraints.
func (t *Table) GetCheckConstraints() []*types.CheckConstraintInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.info.CheckConstraints
}

// GetForeignKeys returns the FOREIGN KEY constraints.
func (t *Table) GetForeignKeys() []*types.ForeignKeyInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.info.ForeignKeys
}

// AddCheckConstraint adds a single CHECK constraint to the table.
func (t *Table) AddCheckConstraint(constraint *types.CheckConstraintInfo) error {
	return t.AddCheckConstraints([]*types.CheckConstraintInfo{constraint})
}

// AddForeignKey adds a single FOREIGN KEY constraint to the table.
func (t *Table) AddForeignKey(fk *types.ForeignKeyInfo) error {
	return t.AddForeignKeys([]*types.ForeignKeyInfo{fk})
}

// DropCheckConstraint drops a CHECK constraint by name.
func (t *Table) DropCheckConstraint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, ck := range t.info.CheckConstraints {
		if ck.Name == name {
			t.info.CheckConstraints = append(t.info.CheckConstraints[:i], t.info.CheckConstraints[i+1:]...)
			t.info.ModifiedAt = time.Now()
			return t.saveMeta()
		}
	}

	return fmt.Errorf("CHECK constraint %s not found", name)
}

// DropForeignKey drops a FOREIGN KEY constraint by name.
func (t *Table) DropForeignKey(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, fk := range t.info.ForeignKeys {
		if fk.Name == name {
			t.info.ForeignKeys = append(t.info.ForeignKeys[:i], t.info.ForeignKeys[i+1:]...)
			t.info.ModifiedAt = time.Now()
			return t.saveMeta()
		}
	}

	return fmt.Errorf("FOREIGN KEY constraint %s not found", name)
}

// SetPrimaryKey sets a column as primary key.
func (t *Table) SetPrimaryKey(colName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	found := false
	for i, col := range t.info.Columns {
		if strings.EqualFold(col.Name, colName) {
			t.info.Columns[i].PrimaryKey = true
			t.info.Columns[i].Nullable = false
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("column %s not found", colName)
	}

	// Add to primary key list if not already there
	for _, pk := range t.info.PrimaryKey {
		if strings.EqualFold(pk, colName) {
			// Already in the list
			t.info.ModifiedAt = time.Now()
			return t.saveMeta()
		}
	}

	t.info.PrimaryKey = append(t.info.PrimaryKey, colName)
	t.info.ModifiedAt = time.Now()
	return t.saveMeta()
}

// AddUniqueConstraint adds a UNIQUE constraint to a column.
func (t *Table) AddUniqueConstraint(colName, constraintName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, col := range t.info.Columns {
		if strings.EqualFold(col.Name, colName) {
			t.info.Columns[i].Unique = true
			t.info.ModifiedAt = time.Now()
			t.mu.Unlock() // Unlock before calling CreateIndex which needs its own lock

			// Create unique index using the table's CreateIndex method
			if err := t.CreateIndex(constraintName, []string{colName}, true); err != nil {
				t.mu.Lock()
				return err
			}

			t.mu.Lock()
			return t.saveMeta()
		}
	}

	return fmt.Errorf("column %s not found", colName)
}

// DropUniqueConstraint drops a UNIQUE constraint by name.
func (t *Table) DropUniqueConstraint(constraintName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Find column with this unique constraint name and remove unique flag
	found := false
	for i, col := range t.info.Columns {
		if col.Unique {
			// For simplicity, we drop unique from the first unique column
			// A more complete implementation would track constraint names
			t.info.Columns[i].Unique = false
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("UNIQUE constraint %s not found", constraintName)
	}

	// Drop the index directly (not via DropIndex to avoid deadlock)
	// We already hold the lock, so we operate directly on indexMgr
	_ = t.indexMgr.DropIndex(constraintName)

	// Remove from metadata
	for i, idx := range t.info.Indexes {
		if idx.Name == constraintName {
			t.info.Indexes = append(t.info.Indexes[:i], t.info.Indexes[i+1:]...)
			break
		}
	}

	t.info.ModifiedAt = time.Now()
	return t.saveMeta()
}
