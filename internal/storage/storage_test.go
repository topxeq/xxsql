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
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "xxsql-index-test-*")
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
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeVarchar, Size: 50},
	}

	if err := engine.CreateTable("indexed", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows
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

	// Get table and check index
	tbl, err := engine.GetTable("indexed")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	idxMgr := tbl.GetIndexManager()
	if !idxMgr.HasPrimary() {
		t.Fatal("Expected primary key index")
	}

	// Test index search
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

	// Test non-existent key
	_, found = primaryIdx.Search(types.NewIntValue(200))
	if found {
		t.Error("Should not find key 200 in index")
	}

	// Verify index count
	count := primaryIdx.Count()
	if count != 100 {
		t.Errorf("Expected index count 100, got %d", count)
	}
}
