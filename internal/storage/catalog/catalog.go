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
	dataDir  string
	tables   map[string]*table.Table
	views    map[string]*ViewInfo
	triggers map[string]*TriggerInfo
	mu       sync.RWMutex
}

// ViewInfo stores view definition.
type ViewInfo struct {
	Name    string
	Query   string // The SQL query string
	Columns []string
}

// TriggerInfo stores trigger definition.
type TriggerInfo struct {
	Name        string
	Timing      int // 0=BEFORE, 1=AFTER, 2=INSTEAD OF
	Event       int // 0=INSERT, 1=UPDATE, 2=DELETE
	TableName   string
	Granularity int // 0=FOR EACH ROW, 1=FOR EACH STATEMENT
	WhenClause  string
	Body        string
}

// NewCatalog creates a new catalog.
func NewCatalog(dataDir string) *Catalog {
	return &Catalog{
		dataDir:  dataDir,
		tables:   make(map[string]*table.Table),
		views:    make(map[string]*ViewInfo),
		triggers: make(map[string]*TriggerInfo),
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

	// Load views
	if err := c.loadViews(); err != nil {
		return err
	}

	// Load triggers
	if err := c.loadTriggers(); err != nil {
		return err
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
	c.views = make(map[string]*ViewInfo)
	c.triggers = make(map[string]*TriggerInfo)

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

// CreateView creates a new view.
func (c *Catalog) CreateView(name string, query string, columns []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; exists {
		return fmt.Errorf("table already exists: %s", name)
	}

	if _, exists := c.views[name]; exists {
		return fmt.Errorf("view already exists: %s", name)
	}

	c.views[name] = &ViewInfo{
		Name:    name,
		Query:   query,
		Columns: columns,
	}
	return c.saveViews()
}

// DropView drops a view.
func (c *Catalog) DropView(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.views[name]; !exists {
		return fmt.Errorf("view not found: %s", name)
	}

	delete(c.views, name)
	return c.saveViews()
}

// GetView returns a view by name.
func (c *Catalog) GetView(name string) (*ViewInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	v, exists := c.views[name]
	if !exists {
		return nil, fmt.Errorf("view not found: %s", name)
	}

	return v, nil
}

// ViewExists checks if a view exists.
func (c *Catalog) ViewExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.views[name]
	return exists
}

// ListViews returns all view names.
func (c *Catalog) ListViews() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.views))
	for name := range c.views {
		names = append(names, name)
	}
	return names
}

// saveViews saves views to disk.
func (c *Catalog) saveViews() error {
	viewInfos := make([]*ViewInfo, 0, len(c.views))
	for _, v := range c.views {
		viewInfos = append(viewInfos, v)
	}

	data, err := json.MarshalIndent(viewInfos, "", "  ")
	if err != nil {
		return err
	}

	viewsPath := filepath.Join(c.dataDir, "views.json")
	return os.WriteFile(viewsPath, data, 0644)
}

// loadViews loads views from disk.
func (c *Catalog) loadViews() error {
	viewsPath := filepath.Join(c.dataDir, "views.json")
	data, err := os.ReadFile(viewsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var viewInfos []*ViewInfo
	if err := json.Unmarshal(data, &viewInfos); err != nil {
		return err
	}

	for _, v := range viewInfos {
		c.views[v.Name] = v
	}
	return nil
}

// CreateTrigger creates a new trigger.
func (c *Catalog) CreateTrigger(name string, timing, event int, tableName string, granularity int, whenClause, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.triggers[name]; exists {
		return fmt.Errorf("trigger already exists: %s", name)
	}

	c.triggers[name] = &TriggerInfo{
		Name:        name,
		Timing:      timing,
		Event:       event,
		TableName:   tableName,
		Granularity: granularity,
		WhenClause:  whenClause,
		Body:        body,
	}
	return c.saveTriggers()
}

// DropTrigger drops a trigger.
func (c *Catalog) DropTrigger(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.triggers[name]; !exists {
		return fmt.Errorf("trigger not found: %s", name)
	}

	delete(c.triggers, name)
	return c.saveTriggers()
}

// GetTrigger returns a trigger by name.
func (c *Catalog) GetTrigger(name string) (*TriggerInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, exists := c.triggers[name]
	if !exists {
		return nil, fmt.Errorf("trigger not found: %s", name)
	}

	return t, nil
}

// TriggerExists checks if a trigger exists.
func (c *Catalog) TriggerExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.triggers[name]
	return exists
}

// ListTriggers returns all trigger names.
func (c *Catalog) ListTriggers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.triggers))
	for name := range c.triggers {
		names = append(names, name)
	}
	return names
}

// GetTriggersForTable returns all triggers for a specific table and event.
func (c *Catalog) GetTriggersForTable(tableName string, event int) []*TriggerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var triggers []*TriggerInfo
	for _, t := range c.triggers {
		if t.TableName == tableName && t.Event == event {
			triggers = append(triggers, t)
		}
	}
	return triggers
}

// saveTriggers saves triggers to disk.
func (c *Catalog) saveTriggers() error {
	triggerInfos := make([]*TriggerInfo, 0, len(c.triggers))
	for _, t := range c.triggers {
		triggerInfos = append(triggerInfos, t)
	}

	data, err := json.MarshalIndent(triggerInfos, "", "  ")
	if err != nil {
		return err
	}

	triggersPath := filepath.Join(c.dataDir, "triggers.json")
	return os.WriteFile(triggersPath, data, 0644)
}

// loadTriggers loads triggers from disk.
func (c *Catalog) loadTriggers() error {
	triggersPath := filepath.Join(c.dataDir, "triggers.json")
	data, err := os.ReadFile(triggersPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var triggerInfos []*TriggerInfo
	if err := json.Unmarshal(data, &triggerInfos); err != nil {
		return err
	}

	for _, t := range triggerInfos {
		c.triggers[t.Name] = t
	}
	return nil
}
