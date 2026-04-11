// Package storage_test provides tests for the storage engine.
package storage_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestEngineBasic(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "xxsql-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create engine
	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create table
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeSeq, PrimaryKey: true, AutoIncr: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100, Nullable: false},
		{Name: "age", Type: types.TypeInt, Nullable: true},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	if !engine.TableExists("users") {
		t.Error("Table should exist")
	}

	// List tables
	tables := engine.ListTables()
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	// Insert row
	values := []types.Value{
		types.NewNullValue(), // auto-increment
		types.NewStringValue("Alice", types.TypeVarchar),
		types.NewIntValue(30),
	}

	rowID, err := engine.Insert("users", values)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if rowID == 0 {
		t.Error("Row ID should not be 0")
	}

	// Insert another row
	values2 := []types.Value{
		types.NewNullValue(),
		types.NewStringValue("Bob", types.TypeVarchar),
		types.NewIntValue(25),
	}

	_, err = engine.Insert("users", values2)
	if err != nil {
		t.Fatalf("Failed to insert second row: %v", err)
	}

	// Scan rows
	rows, err := engine.Scan("users")
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// Check first row
	if rows[0].Values[1].AsString() != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", rows[0].Values[1].AsString())
	}

	if rows[0].Values[2].AsInt() != 30 {
		t.Errorf("Expected age 30, got %d", rows[0].Values[2].AsInt())
	}
}

func TestTypes(t *testing.T) {
	// Test int value
	intVal := types.NewIntValue(42)
	if intVal.AsInt() != 42 {
		t.Errorf("Expected 42, got %d", intVal.AsInt())
	}

	// Test float value
	floatVal := types.NewFloatValue(3.14)
	if floatVal.AsFloat() != 3.14 {
		t.Errorf("Expected 3.14, got %f", floatVal.AsFloat())
	}

	// Test string value
	strVal := types.NewStringValue("hello", types.TypeVarchar)
	if strVal.AsString() != "hello" {
		t.Errorf("Expected 'hello', got '%s'", strVal.AsString())
	}

	// Test bool value
	boolVal := types.NewBoolValue(true)
	if !boolVal.AsBool() {
		t.Error("Expected true")
	}

	// Test null value
	nullVal := types.NewNullValue()
	if !nullVal.Null {
		t.Error("Expected null")
	}
}

func TestValueCompare(t *testing.T) {
	tests := []struct {
		a, b   types.Value
		result int
	}{
		{types.NewIntValue(1), types.NewIntValue(2), -1},
		{types.NewIntValue(2), types.NewIntValue(1), 1},
		{types.NewIntValue(5), types.NewIntValue(5), 0},
		{types.NewStringValue("a", types.TypeVarchar), types.NewStringValue("b", types.TypeVarchar), -1},
		{types.NewStringValue("b", types.TypeVarchar), types.NewStringValue("a", types.TypeVarchar), 1},
		{types.NewStringValue("x", types.TypeVarchar), types.NewStringValue("x", types.TypeVarchar), 0},
		{types.NewNullValue(), types.NewNullValue(), 0},
		{types.NewNullValue(), types.NewIntValue(1), -1},
		{types.NewIntValue(1), types.NewNullValue(), 1},
	}

	for i, test := range tests {
		result := test.a.Compare(test.b)
		if result != test.result {
			t.Errorf("Test %d: expected %d, got %d", i, test.result, result)
		}
	}
}

func TestPageBasic(t *testing.T) {
	// Create a temp file
	tempFile := filepath.Join(os.TempDir(), "xxsql-page-test")
	defer os.Remove(tempFile)

	// Test would go here for page operations
	// For now just verify types work
	p := types.ParseTypeID("INT")
	if p != types.TypeInt {
		t.Errorf("Expected TypeInt, got %v", p)
	}

	p = types.ParseTypeID("VARCHAR")
	if p != types.TypeVarchar {
		t.Errorf("Expected TypeVarchar, got %v", p)
	}
}

func TestTableWithPrimaryKey(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "xxsql-pk-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create engine
	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create table with primary key
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true, Nullable: false},
		{Name: "name", Type: types.TypeVarchar, Size: 100, Nullable: false},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get table
	tbl, err := engine.GetTable("users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Verify primary key index was created
	idxMgr := tbl.GetIndexManager()
	if !idxMgr.HasPrimary() {
		t.Error("Expected primary key index to be created")
	}

	// Create secondary index
	if err := tbl.CreateIndex("idx_name", []string{"name"}, false); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert rows
	for i := 1; i <= 10; i++ {
		values := []types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue(fmt.Sprintf("user%d", i), types.TypeVarchar),
		}
		_, err := engine.Insert("users", values)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Verify count
	rows, err := engine.Scan("users")
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	if len(rows) != 10 {
		t.Errorf("Expected 10 rows, got %d", len(rows))
	}

	// Test duplicate key rejection
	dupValues := []types.Value{
		types.NewIntValue(1), // Duplicate
		types.NewStringValue("duplicate", types.TypeVarchar),
	}
	_, err = engine.Insert("users", dupValues)
	if err == nil {
		t.Error("Expected error for duplicate primary key")
	}
}

func TestBTreeIndex(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "xxsql-index-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeVarchar, Size: 50},
	}

	if err := engine.CreateTable("indexed", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 100; i++ {
		values := []types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue(fmt.Sprintf("value%d", i), types.TypeVarchar),
		}
		_, err := engine.Insert("indexed", values)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	tbl, err := engine.GetTable("indexed")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	idxMgr := tbl.GetIndexManager()
	if !idxMgr.HasPrimary() {
		t.Fatal("Expected primary key index")
	}

	primaryIdx := idxMgr.GetPrimary()
	if primaryIdx == nil {
		t.Fatal("Primary index is nil")
	}

	rowID, found := primaryIdx.Search(types.NewIntValue(50))
	if !found {
		t.Error("Expected to find key 50 in index")
	}
	if rowID == 0 {
		t.Error("Row ID should not be 0")
	}

	_, found = primaryIdx.Search(types.NewIntValue(200))
	if found {
		t.Error("Should not find key 200 in index")
	}

	count := primaryIdx.Count()
	if count != 100 {
		t.Errorf("Expected index count 100, got %d", count)
	}
}

func TestEngineDropTable(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	if err := engine.CreateTable("to_drop", columns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	if !engine.TableExists("to_drop") {
		t.Error("Table should exist")
	}

	if err := engine.DropTable("to_drop"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	if engine.TableExists("to_drop") {
		t.Error("Table should not exist after drop")
	}
}

func TestEngineRenameTable(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	if err := engine.CreateTable("old_name", columns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	values := []types.Value{types.NewIntValue(1)}
	engine.Insert("old_name", values)

	if err := engine.RenameTable("old_name", "new_name"); err != nil {
		t.Fatalf("RenameTable failed: %v", err)
	}

	if engine.TableExists("old_name") {
		t.Error("Old table name should not exist")
	}

	if !engine.TableExists("new_name") {
		t.Error("New table name should exist")
	}
}

func TestEngineCreateIndex(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "email", Type: types.TypeVarchar, Size: 100},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	if err := engine.CreateIndex("users", "idx_email", []string{"email"}, true); err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	if err := engine.DropIndex("users", "idx_email"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}
}

func TestEngineStats(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	if err := engine.CreateTable("stats_test", columns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		values := []types.Value{types.NewIntValue(int64(i))}
		engine.Insert("stats_test", values)
	}

	stats := engine.Stats()

	if stats.TableCount != 1 {
		t.Errorf("TableCount: got %d, want 1", stats.TableCount)
	}

	if len(stats.Tables) != 1 {
		t.Errorf("Tables: got %d, want 1", len(stats.Tables))
	}

	if stats.Tables[0].RowCount != 10 {
		t.Errorf("RowCount: got %d, want 10", stats.Tables[0].RowCount)
	}
}

func TestEngineGetDataDir(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)

	if engine.GetDataDir() != tempDir {
		t.Errorf("GetDataDir: got %q, want %q", engine.GetDataDir(), tempDir)
	}
}

func TestEngineGetCatalog(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	catalog := engine.GetCatalog()
	if catalog == nil {
		t.Error("GetCatalog should not return nil")
	}
}

func TestEngineFlush(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	engine.CreateTable("flush_test", columns)
	engine.Insert("flush_test", []types.Value{types.NewIntValue(1)})

	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestEngineNonExistentTable(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	if engine.TableExists("nonexistent") {
		t.Error("Non-existent table should not exist")
	}

	_, err := engine.GetTable("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	_, err = engine.Insert("nonexistent", nil)
	if err == nil {
		t.Error("Expected error for insert into non-existent table")
	}

	_, err = engine.Scan("nonexistent")
	if err == nil {
		t.Error("Expected error for scan on non-existent table")
	}
}

func TestParseColumnDefs(t *testing.T) {
	defs := []struct {
		Name     string
		Type     string
		Size     int
		Nullable bool
		Default  interface{}
		Primary  bool
		AutoIncr bool
	}{
		{Name: "id", Type: "INT", Primary: true},
		{Name: "name", Type: "VARCHAR", Size: 100, Nullable: true},
		{Name: "count", Type: "INT", Default: 0},
		{Name: "price", Type: "FLOAT", Default: 0.0},
		{Name: "active", Type: "BOOL", Default: true},
	}

	columns := storage.ParseColumnDefs(defs)

	if len(columns) != 5 {
		t.Fatalf("Columns: got %d, want 5", len(columns))
	}

	if columns[0].Name != "id" {
		t.Errorf("Name: got %q, want 'id'", columns[0].Name)
	}

	if columns[0].Type != types.TypeInt {
		t.Errorf("Type: got %v, want TypeInt", columns[0].Type)
	}

	if columns[0].PrimaryKey != true {
		t.Error("id should be primary key")
	}

	if columns[1].Size != 100 {
		t.Errorf("Size: got %d, want 100", columns[1].Size)
	}
}

func TestValidateValues(t *testing.T) {
	tests := []struct {
		name    string
		columns []*types.ColumnInfo
		values  []types.Value
		wantErr bool
	}{
		{
			name: "matching columns and values",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt},
				{Name: "name", Type: types.TypeVarchar},
			},
			values: []types.Value{
				types.NewIntValue(1),
				types.NewStringValue("test", types.TypeVarchar),
			},
			wantErr: false,
		},
		{
			name: "column count mismatch",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt},
				{Name: "name", Type: types.TypeVarchar},
			},
			values: []types.Value{
				types.NewIntValue(1),
			},
			wantErr: true,
		},
		{
			name: "null on non-nullable column",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt, Nullable: false},
			},
			values: []types.Value{
				types.NewNullValue(),
			},
			wantErr: true,
		},
		{
			name: "null on nullable column",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt, Nullable: true},
			},
			values: []types.Value{
				types.NewNullValue(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.ValidateValues(tt.columns, tt.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateValues(): error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEngineMultipleTables(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	for i := 1; i <= 5; i++ {
		columns := []*types.ColumnInfo{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		}
		engine.CreateTable(fmt.Sprintf("table%d", i), columns)
	}

	tables := engine.ListTables()
	if len(tables) != 5 {
		t.Errorf("ListTables: got %d, want 5", len(tables))
	}
}

func TestEngineReopen(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("First open failed: %v", err)
	}

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	engine.CreateTable("persist_test", columns)
	for i := 1; i <= 5; i++ {
		engine.Insert("persist_test", []types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		})
	}
	engine.Close()

	engine2 := storage.NewEngine(tempDir)
	if err := engine2.Open(); err != nil {
		t.Fatalf("Second open failed: %v", err)
	}
	defer engine2.Close()

	if !engine2.TableExists("persist_test") {
		t.Error("Table should persist after reopen")
	}

	rows, err := engine2.Scan("persist_test")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(rows) != 5 {
		t.Errorf("Rows: got %d, want 5", len(rows))
	}
}
