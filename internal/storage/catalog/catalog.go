// Package catalog provides catalog management for XxSql storage engine.
package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

const (
	// CatalogFileName is the name of the catalog file.
	CatalogFileName = "catalog.json"
)

// Catalog manages all table metadata.
type Catalog struct {
	dataDir string
	tables  map[string]*table.Table
	mu      sync.RWMutex
}

// NewCatalog creates a new catalog.
func NewCatalog(dataDir string) *Catalog {
	return &Catalog{
		dataDir: dataDir,
		tables:  make(map[string]*table.Table),
	}
}

// Open opens the catalog and loads all tables.
func (c *Catalog) Open() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create data directory if not exists
	if err := os.MkdirAll(c.dataDir, 0755); err != nil {
		return err
	}

	// Load catalog file
	catalogPath := filepath.Join(c.dataDir, CatalogFileName)
	data, err := os.ReadFile(catalogPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Create empty catalog
			return c.save()
		}
		return err
	}

	// Parse catalog
	var tableNames []string
	if err := json.Unmarshal(data, &tableNames); err != nil {
		return err
	}

	// Open each table
	for _, name := range tableNames {
		t, err := table.OpenTable(c.dataDir, name, nil)
		if err != nil {
			// Log warning but continue
			continue
		}
		c.tables[name] = t
	}

	return nil
}

// Close closes the catalog and all tables.
func (c *Catalog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.tables {
		t.Close()
	}
	c.tables = make(map[string]*table.Table)

	return nil
}

// save saves the catalog to disk.
func (c *Catalog) save() error {
	tableNames := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tableNames = append(tableNames, name)
	}

	data, err := json.MarshalIndent(tableNames, "", "  ")
	if err != nil {
		return err
	}

	catalogPath := filepath.Join(c.dataDir, CatalogFileName)
	return os.WriteFile(catalogPath, data, 0644)
}

// CreateTable creates a new table.
func (c *Catalog) CreateTable(name string, columns []*types.ColumnInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; exists {
		return fmt.Errorf("table already exists: %s", name)
	}

	t, err := table.OpenTable(c.dataDir, name, columns)
	if err != nil {
		return err
	}

	c.tables[name] = t
	return c.save()
}

// DropTable drops a table.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, exists := c.tables[name]
	if !exists {
		return fmt.Errorf("table not found: %s", name)
	}

	if err := t.Drop(); err != nil {
		return err
	}

	delete(c.tables, name)
	return c.save()
}

// GetTable returns a table by name.
func (c *Catalog) GetTable(name string) (*table.Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, exists := c.tables[name]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", name)
	}

	return t, nil
}

// TableExists checks if a table exists.
func (c *Catalog) TableExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.tables[name]
	return exists
}

// ListTables returns all table names.
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	return names
}

// TableCount returns the number of tables.
func (c *Catalog) TableCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.tables)
}

// Flush flushes all tables to disk.
func (c *Catalog) Flush() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, t := range c.tables {
		if err := t.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// GetTableInfos returns information about all tables.
func (c *Catalog) GetTableInfos() []*table.TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	infos := make([]*table.TableInfo, 0, len(c.tables))
	for _, t := range c.tables {
		infos = append(infos, t.GetInfo())
	}
	return infos
}

// RenameTable renames a table.
func (c *Catalog) RenameTable(oldName, newName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, exists := c.tables[oldName]
	if !exists {
		return fmt.Errorf("table not found: %s", oldName)
	}

	if _, exists := c.tables[newName]; exists {
		return fmt.Errorf("table already exists: %s", newName)
	}

	// Update table name in metadata
	if err := t.Rename(newName); err != nil {
		return err
	}

	// Update catalog map
	delete(c.tables, oldName)
	c.tables[newName] = t

	return c.save()
}
