// Package storage provides the storage engine for XxSql.
package storage

import (
	"fmt"
	"sync"

	"github.com/topxeq/xxsql/internal/storage/catalog"
	"github.com/topxeq/xxsql/internal/storage/fts"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// Engine represents the storage engine.
type Engine struct {
	catalog   *catalog.Catalog
	ftsMgr    *fts.FTSManager
	dataDir   string
	mu        sync.RWMutex

	// Temporary tables (session-scoped, not persisted)
	tempTables     map[string]*table.Table
	tempTablesMu   sync.RWMutex
}

// NewEngine creates a new storage engine.
func NewEngine(dataDir string) *Engine {
	return &Engine{
		catalog:    catalog.NewCatalog(dataDir),
		ftsMgr:     fts.NewFTSManager(dataDir),
		dataDir:    dataDir,
		tempTables: make(map[string]*table.Table),
	}
}

// Open opens the storage engine.
func (e *Engine) Open() error {
	if err := e.catalog.Open(); err != nil {
		return err
	}

	// Load existing FTS indexes from catalog
	for _, ftsInfo := range e.catalog.ListFTSIndexes() {
		info, err := e.catalog.GetFTSIndex(ftsInfo)
		if err != nil {
			continue
		}
		// Create the FTS index in memory
		e.ftsMgr.CreateIndex(info.Name, info.TableName, info.Columns, info.Tokenizer)
	}

	// Load persisted FTS data
	e.ftsMgr.LoadAll()

	return nil
}

// Close closes the storage engine.
func (e *Engine) Close() error {
	// Save FTS data before closing
	e.ftsMgr.SaveAll()
	return e.catalog.Close()
}

// CreateTable creates a new table.
func (e *Engine) CreateTable(name string, columns []*types.ColumnInfo) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.CreateTable(name, columns)
}

// DropTable drops a table.
func (e *Engine) DropTable(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.DropTable(name)
}

// TableExists checks if a table exists.
func (e *Engine) TableExists(name string) bool {
	return e.catalog.TableExists(name)
}

// GetTable returns a table by name.
func (e *Engine) GetTable(name string) (*table.Table, error) {
	return e.catalog.GetTable(name)
}

// ListTables returns all table names.
func (e *Engine) ListTables() []string {
	return e.catalog.ListTables()
}

// Insert inserts a row into a table.
func (e *Engine) Insert(tableName string, values []types.Value) (row.RowID, error) {
	t, err := e.catalog.GetTable(tableName)
	if err != nil {
		return row.InvalidRowID, err
	}

	return t.Insert(values)
}

// Scan scans all rows from a table.
func (e *Engine) Scan(tableName string) ([]*row.Row, error) {
	// Check temp tables first
	e.tempTablesMu.RLock()
	if tbl, exists := e.tempTables[tableName]; exists {
		e.tempTablesMu.RUnlock()
		return tbl.Scan()
	}
	e.tempTablesMu.RUnlock()

	// Check regular tables
	t, err := e.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	return t.Scan()
}

// Flush flushes all data to disk.
func (e *Engine) Flush() error {
	return e.catalog.Flush()
}

// RenameTable renames a table.
func (e *Engine) RenameTable(oldName, newName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.RenameTable(oldName, newName)
}

// CreateTempTable creates a temporary table.
// Temp tables are session-scoped and not persisted to disk.
// A temp table can shadow a regular table with the same name.
func (e *Engine) CreateTempTable(name string, columns []*types.ColumnInfo) error {
	e.tempTablesMu.Lock()
	defer e.tempTablesMu.Unlock()

	// Check if temp table already exists
	if _, exists := e.tempTables[name]; exists {
		return fmt.Errorf("temp table %s already exists", name)
	}

	// Create an in-memory table for temp data
	tbl := table.NewTempTable(name, columns)
	e.tempTables[name] = tbl

	return nil
}

// DropTempTable drops a temporary table.
func (e *Engine) DropTempTable(name string) error {
	e.tempTablesMu.Lock()
	defer e.tempTablesMu.Unlock()

	if _, exists := e.tempTables[name]; !exists {
		return fmt.Errorf("temp table %s does not exist", name)
	}

	delete(e.tempTables, name)
	return nil
}

// TempTableExists checks if a temporary table exists.
func (e *Engine) TempTableExists(name string) bool {
	e.tempTablesMu.RLock()
	defer e.tempTablesMu.RUnlock()

	_, exists := e.tempTables[name]
	return exists
}

// GetTempTable returns a temporary table by name.
func (e *Engine) GetTempTable(name string) (*table.Table, error) {
	e.tempTablesMu.RLock()
	defer e.tempTablesMu.RUnlock()

	tbl, exists := e.tempTables[name]
	if !exists {
		return nil, fmt.Errorf("temp table %s does not exist", name)
	}
	return tbl, nil
}

// GetTableOrTemp returns a table by name, checking both regular and temp tables.
// Temp tables have priority over regular tables with the same name.
func (e *Engine) GetTableOrTemp(name string) (*table.Table, bool, error) {
	// Check temp tables first
	e.tempTablesMu.RLock()
	if tbl, exists := e.tempTables[name]; exists {
		e.tempTablesMu.RUnlock()
		return tbl, true, nil // isTemp = true
	}
	e.tempTablesMu.RUnlock()

	// Check regular tables
	tbl, err := e.catalog.GetTable(name)
	if err != nil {
		return nil, false, err
	}
	return tbl, false, nil // isTemp = false
}

// TableOrTempExists checks if a table exists (regular or temp).
func (e *Engine) TableOrTempExists(name string) bool {
	// Check temp tables first
	e.tempTablesMu.RLock()
	if _, exists := e.tempTables[name]; exists {
		e.tempTablesMu.RUnlock()
		return true
	}
	e.tempTablesMu.RUnlock()

	// Check regular tables
	return e.catalog.TableExists(name)
}

// DropTableOrTemp drops a table (regular or temp).
func (e *Engine) DropTableOrTemp(name string) error {
	// Check if it's a temp table
	e.tempTablesMu.Lock()
	if _, exists := e.tempTables[name]; exists {
		delete(e.tempTables, name)
		e.tempTablesMu.Unlock()
		return nil
	}
	e.tempTablesMu.Unlock()

	// Drop regular table
	return e.catalog.DropTable(name)
}

// ListTempTables returns all temporary table names.
func (e *Engine) ListTempTables() []string {
	e.tempTablesMu.RLock()
	defer e.tempTablesMu.RUnlock()

	names := make([]string, 0, len(e.tempTables))
	for name := range e.tempTables {
		names = append(names, name)
	}
	return names
}

// ClearTempTables clears all temporary tables.
// Called when session ends.
func (e *Engine) ClearTempTables() {
	e.tempTablesMu.Lock()
	defer e.tempTablesMu.Unlock()

	e.tempTables = make(map[string]*table.Table)
}

// CreateIndex creates an index on a table.
func (e *Engine) CreateIndex(tableName, indexName string, columns []string, unique bool) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, err := e.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	return t.CreateIndex(indexName, columns, unique)
}

// DropIndex drops an index from a table.
func (e *Engine) DropIndex(tableName, indexName string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, err := e.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	return t.DropIndex(indexName)
}

// GetCatalog returns the catalog.
func (e *Engine) GetCatalog() *catalog.Catalog {
	return e.catalog
}

// GetFTSManager returns the FTS manager.
func (e *Engine) GetFTSManager() *fts.FTSManager {
	return e.ftsMgr
}

// ViewExists checks if a view exists.
func (e *Engine) ViewExists(name string) bool {
	return e.catalog.ViewExists(name)
}

// CreateView creates a new view.
func (e *Engine) CreateView(name, query string, columns []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.CreateView(name, query, columns)
}

// GetView returns a view by name.
func (e *Engine) GetView(name string) (*catalog.ViewInfo, error) {
	return e.catalog.GetView(name)
}

// DropView drops a view.
func (e *Engine) DropView(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.DropView(name)
}

// ListViews returns all view names.
func (e *Engine) ListViews() []string {
	return e.catalog.ListViews()
}

// TriggerExists checks if a trigger exists.
func (e *Engine) TriggerExists(name string) bool {
	return e.catalog.TriggerExists(name)
}

// CreateTrigger creates a new trigger.
func (e *Engine) CreateTrigger(name string, timing, event int, tableName string, granularity int, whenClause, body string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.CreateTrigger(name, timing, event, tableName, granularity, whenClause, body)
}

// DropTrigger drops a trigger.
func (e *Engine) DropTrigger(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.catalog.DropTrigger(name)
}

// GetTrigger returns a trigger by name.
func (e *Engine) GetTrigger(name string) (*catalog.TriggerInfo, error) {
	return e.catalog.GetTrigger(name)
}

// ListTriggers returns all trigger names.
func (e *Engine) ListTriggers() []string {
	return e.catalog.ListTriggers()
}

// GetTriggersForTable returns all triggers for a specific table and event.
func (e *Engine) GetTriggersForTable(tableName string, event int) []*catalog.TriggerInfo {
	return e.catalog.GetTriggersForTable(tableName, event)
}

// GetDataDir returns the data directory path.
func (e *Engine) GetDataDir() string {
	return e.dataDir
}

// Stats returns storage engine statistics.
func (e *Engine) Stats() *Stats {
	stats := &Stats{
		TableCount: e.catalog.TableCount(),
		Tables:     make([]TableStats, 0),
	}

	for _, name := range e.catalog.ListTables() {
		t, err := e.catalog.GetTable(name)
		if err != nil {
			continue
		}

		info := t.GetInfo()
		stats.Tables = append(stats.Tables, TableStats{
			Name:     info.Name,
			RowCount: info.RowCount,
			PageCount: int(info.NextPageID - 1),
		})
	}

	return stats
}

// Stats represents storage engine statistics.
type Stats struct {
	TableCount int          `json:"table_count"`
	Tables     []TableStats `json:"tables"`
}

// TableStats represents table statistics.
type TableStats struct {
	Name      string `json:"name"`
	RowCount  uint64 `json:"row_count"`
	PageCount int    `json:"page_count"`
}

// ParseColumnDefs parses SQL column definitions to storage types.
func ParseColumnDefs(defs []struct {
	Name     string
	Type     string
	Size     int
	Nullable bool
	Default  interface{}
	Primary  bool
	AutoIncr bool
}) []*types.ColumnInfo {
	result := make([]*types.ColumnInfo, len(defs))
	for i, d := range defs {
		result[i] = &types.ColumnInfo{
			Name:       d.Name,
			Type:       types.ParseTypeID(d.Type),
			Size:       d.Size,
			Nullable:   d.Nullable,
			PrimaryKey: d.Primary,
			AutoIncr:   d.AutoIncr,
		}

		// Set default value if specified
		if d.Default != nil {
			switch v := d.Default.(type) {
			case int:
				result[i].Default = types.NewIntValue(int64(v))
			case int64:
				result[i].Default = types.NewIntValue(v)
			case float64:
				result[i].Default = types.NewFloatValue(v)
			case string:
				result[i].Default = types.NewStringValue(v, result[i].Type)
			case bool:
				result[i].Default = types.NewBoolValue(v)
			}
		}
	}
	return result
}

// ValidateValues validates values against column definitions.
func ValidateValues(columns []*types.ColumnInfo, values []types.Value) error {
	if len(columns) != len(values) {
		return fmt.Errorf("column count mismatch: expected %d, got %d", len(columns), len(values))
	}

	for i, col := range columns {
		// Check nullable
		if !col.Nullable && values[i].Null {
			return fmt.Errorf("column %s cannot be null", col.Name)
		}

		// Check type compatibility (basic check)
		if !values[i].Null && values[i].Type != col.Type {
			// Allow some type flexibility
			switch {
			case col.Type == types.TypeVarchar && values[i].Type == types.TypeChar:
				// OK
			case col.Type == types.TypeChar && values[i].Type == types.TypeVarchar:
				// OK
			case col.Type == types.TypeText && (values[i].Type == types.TypeChar || values[i].Type == types.TypeVarchar):
				// OK
			case col.Type == types.TypeInt && values[i].Type == types.TypeSeq:
				// OK
			case col.Type == types.TypeSeq && values[i].Type == types.TypeInt:
				// OK
			default:
				return fmt.Errorf("type mismatch for column %s: expected %s, got %s",
					col.Name, col.Type, values[i].Type)
			}
		}
	}

	return nil
}

// ============================================================================
// Transaction Support
// ============================================================================

// TransactionState tracks the state of a transaction
type TransactionState struct {
	ID         uint64
	Active     bool
	Savepoints []string
}

// BeginTransaction starts a new transaction.
// For this implementation, we track transaction state in memory.
// Full rollback support would require WAL replay.
func (e *Engine) BeginTransaction() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Flush any pending data before starting transaction
	if err := e.catalog.Flush(); err != nil {
		return err
	}

	return nil
}

// CommitTransaction commits the current transaction.
func (e *Engine) CommitTransaction() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Flush all changes to disk
	return e.catalog.Flush()
}

// RollbackTransaction rolls back the current transaction.
// Note: This is a simplified implementation. Full rollback would require
// WAL replay to restore the state before the transaction started.
func (e *Engine) RollbackTransaction() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// For now, this is a no-op since we don't have full rollback support
	// In a production system, this would replay WAL records to undo changes
	return nil
}

// CreateSavepoint creates a savepoint within the current transaction.
func (e *Engine) CreateSavepoint(name string) error {
	// Savepoints are tracked in memory by the executor
	return nil
}

// ReleaseSavepoint releases a savepoint within the current transaction.
func (e *Engine) ReleaseSavepoint(name string) error {
	// Savepoints are tracked in memory by the executor
	return nil
}

// RollbackToSavepoint rolls back to a savepoint within the current transaction.
// Note: This is a simplified implementation. Full savepoint rollback would require
// tracking changes since the savepoint and undoing them.
func (e *Engine) RollbackToSavepoint(name string) error {
	// For now, this is a no-op since we don't have full rollback support
	return nil
}
