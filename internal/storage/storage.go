// Package storage provides the storage engine for XxSql.
package storage

import (
	"fmt"
	"sync"

	"github.com/topxeq/xxsql/internal/storage/catalog"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// Engine represents the storage engine.
type Engine struct {
	catalog *catalog.Catalog
	dataDir string
	mu      sync.RWMutex
}

// NewEngine creates a new storage engine.
func NewEngine(dataDir string) *Engine {
	return &Engine{
		catalog: catalog.NewCatalog(dataDir),
		dataDir: dataDir,
	}
}

// Open opens the storage engine.
func (e *Engine) Open() error {
	return e.catalog.Open()
}

// Close closes the storage engine.
func (e *Engine) Close() error {
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
