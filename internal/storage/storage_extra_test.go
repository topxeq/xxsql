// Package storage_test provides additional tests for the storage engine.
package storage_test

import (
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestEngineTempTables(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a regular table first
	regularColumns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}
	if err := engine.CreateTable("users", regularColumns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create a temp table with the same name (should shadow)
	tempColumns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "temp_name", Type: types.TypeVarchar, Size: 50},
	}

	if err := engine.CreateTempTable("users", tempColumns); err != nil {
		t.Fatalf("CreateTempTable failed: %v", err)
	}

	// Check temp table exists
	if !engine.TempTableExists("users") {
		t.Error("TempTableExists should return true")
	}

	// Get temp table
	tbl, err := engine.GetTempTable("users")
	if err != nil {
		t.Fatalf("GetTempTable failed: %v", err)
	}
	if tbl == nil {
		t.Error("Temp table should not be nil")
	}

	// GetTableOrTemp should return the temp table
	tbl2, isTemp, err := engine.GetTableOrTemp("users")
	if err != nil {
		t.Fatalf("GetTableOrTemp failed: %v", err)
	}
	if !isTemp {
		t.Error("Should indicate temp table")
	}
	if tbl2 == nil {
		t.Error("Table should not be nil")
	}

	// TableOrTempExists should return true
	if !engine.TableOrTempExists("users") {
		t.Error("TableOrTempExists should return true")
	}

	// Insert into temp table
	tbl.Insert([]types.Value{
		types.NewIntValue(1),
		types.NewStringValue("temp_value", types.TypeVarchar),
	})

	// Scan should return from temp table
	rows, err := engine.Scan("users")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Scan: got %d rows, want 1", len(rows))
	}

	// List temp tables
	tempTables := engine.ListTempTables()
	if len(tempTables) != 1 {
		t.Errorf("ListTempTables: got %d, want 1", len(tempTables))
	}

	// Drop temp table
	if err := engine.DropTempTable("users"); err != nil {
		t.Fatalf("DropTempTable failed: %v", err)
	}

	// Temp table should no longer exist
	if engine.TempTableExists("users") {
		t.Error("Temp table should be dropped")
	}

	// Regular table should still exist
	if !engine.TableExists("users") {
		t.Error("Regular table should still exist")
	}
}

func TestEngineTempTableErrors(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	// Create temp table
	if err := engine.CreateTempTable("temp1", columns); err != nil {
		t.Fatalf("CreateTempTable failed: %v", err)
	}

	// Duplicate temp table should fail
	if err := engine.CreateTempTable("temp1", columns); err == nil {
		t.Error("CreateTempTable duplicate should fail")
	}

	// Get non-existent temp table should fail
	if _, err := engine.GetTempTable("nonexistent"); err == nil {
		t.Error("GetTempTable nonexistent should fail")
	}

	// Drop non-existent temp table should fail
	if err := engine.DropTempTable("nonexistent"); err == nil {
		t.Error("DropTempTable nonexistent should fail")
	}
}

func TestEngineClearTempTables(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	// Create multiple temp tables
	engine.CreateTempTable("temp1", columns)
	engine.CreateTempTable("temp2", columns)
	engine.CreateTempTable("temp3", columns)

	if len(engine.ListTempTables()) != 3 {
		t.Error("Should have 3 temp tables")
	}

	// Clear all temp tables
	engine.ClearTempTables()

	if len(engine.ListTempTables()) != 0 {
		t.Error("Should have 0 temp tables after clear")
	}
}

func TestEngineDropTableOrTemp(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	// Create regular table
	engine.CreateTable("regular_table", columns)

	// Create temp table
	engine.CreateTempTable("temp_table", columns)

	// Drop temp table via DropTableOrTemp
	if err := engine.DropTableOrTemp("temp_table"); err != nil {
		t.Fatalf("DropTableOrTemp for temp failed: %v", err)
	}
	if engine.TempTableExists("temp_table") {
		t.Error("Temp table should be dropped")
	}

	// Drop regular table via DropTableOrTemp
	if err := engine.DropTableOrTemp("regular_table"); err != nil {
		t.Fatalf("DropTableOrTemp for regular failed: %v", err)
	}
	if engine.TableExists("regular_table") {
		t.Error("Regular table should be dropped")
	}
}

func TestEngineViews(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a table for the view
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}
	engine.CreateTable("users", columns)

	// View should not exist initially
	if engine.ViewExists("user_view") {
		t.Error("View should not exist initially")
	}

	// Create view
	if err := engine.CreateView("user_view", "SELECT id, name FROM users", []string{"id", "name"}, ""); err != nil {
		t.Fatalf("CreateView failed: %v", err)
	}

	// View should exist now
	if !engine.ViewExists("user_view") {
		t.Error("View should exist")
	}

	// Get view
	view, err := engine.GetView("user_view")
	if err != nil {
		t.Fatalf("GetView failed: %v", err)
	}
	if view == nil {
		t.Fatal("View should not be nil")
	}
	if view.Name != "user_view" {
		t.Errorf("View name: got %q, want 'user_view'", view.Name)
	}
	if view.Query != "SELECT id, name FROM users" {
		t.Errorf("View query: got %q", view.Query)
	}

	// List views
	views := engine.ListViews()
	if len(views) != 1 {
		t.Errorf("ListViews: got %d, want 1", len(views))
	}

	// Drop view
	if err := engine.DropView("user_view"); err != nil {
		t.Fatalf("DropView failed: %v", err)
	}

	// View should not exist
	if engine.ViewExists("user_view") {
		t.Error("View should be dropped")
	}
}

func TestEngineTriggers(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a table for the trigger
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}
	engine.CreateTable("users", columns)

	// Trigger should not exist initially
	if engine.TriggerExists("test_trigger") {
		t.Error("Trigger should not exist initially")
	}

	// Create trigger (timing=0=BEFORE, event=1=INSERT, granularity=0=ROW)
	if err := engine.CreateTrigger("test_trigger", 0, 1, "users", 0, "", "BEGIN END"); err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}

	// Trigger should exist
	if !engine.TriggerExists("test_trigger") {
		t.Error("Trigger should exist")
	}

	// Get trigger
	trigger, err := engine.GetTrigger("test_trigger")
	if err != nil {
		t.Fatalf("GetTrigger failed: %v", err)
	}
	if trigger == nil {
		t.Fatal("Trigger should not be nil")
	}
	if trigger.Name != "test_trigger" {
		t.Errorf("Trigger name: got %q", trigger.Name)
	}

	// List triggers
	triggers := engine.ListTriggers()
	if len(triggers) != 1 {
		t.Errorf("ListTriggers: got %d, want 1", len(triggers))
	}

	// Get triggers for table
	tableTriggers := engine.GetTriggersForTable("users", 1)
	if len(tableTriggers) != 1 {
		t.Errorf("GetTriggersForTable: got %d, want 1", len(tableTriggers))
	}

	// Drop trigger
	if err := engine.DropTrigger("test_trigger"); err != nil {
		t.Fatalf("DropTrigger failed: %v", err)
	}

	// Trigger should not exist
	if engine.TriggerExists("test_trigger") {
		t.Error("Trigger should be dropped")
	}
}

func TestEngineTransactions(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Begin transaction
	if err := engine.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create savepoint
	if err := engine.CreateSavepoint("sp1"); err != nil {
		t.Fatalf("CreateSavepoint failed: %v", err)
	}

	// Release savepoint
	if err := engine.ReleaseSavepoint("sp1"); err != nil {
		t.Fatalf("ReleaseSavepoint failed: %v", err)
	}

	// Rollback to savepoint
	if err := engine.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Commit transaction
	if err := engine.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Another transaction with rollback
	if err := engine.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	if err := engine.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
}

func TestEngineGetFTSManager(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	ftsMgr := engine.GetFTSManager()
	if ftsMgr == nil {
		t.Error("GetFTSManager should not return nil")
	}
}

func TestEngineIndexOperations(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create table
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "age", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}
	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		engine.Insert("users", []types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(20 + i)),
			types.NewStringValue("user", types.TypeVarchar),
		})
	}

	// Create secondary index on age
	if err := engine.CreateIndex("users", "idx_age", []string{"age"}, false); err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Get index for column
	idxName, hasIdx := engine.GetIndexForColumn("users", "age")
	if !hasIdx {
		t.Error("GetIndexForColumn should find index on age")
	}
	if idxName != "idx_age" {
		t.Errorf("GetIndexForColumn: got %q, want 'idx_age'", idxName)
	}

	// Get index for columns
	_, hasIdx2, matchCount := engine.GetIndexForColumns("users", []string{"age"})
	if !hasIdx2 {
		t.Error("GetIndexForColumns should find index")
	}
	if matchCount != 1 {
		t.Errorf("GetIndexForColumns matchCount: got %d, want 1", matchCount)
	}

	// Estimate selectivity
	selectivity := engine.EstimateSelectivity("users", "idx_age")
	_ = selectivity // Just verify it doesn't crash

	// Index point lookup
	rowIDs, err := engine.IndexPointLookup("users", "idx_age", types.NewIntValue(25))
	if err != nil {
		t.Fatalf("IndexPointLookup failed: %v", err)
	}
	// May or may not find rows depending on implementation

	// Index range scan
	rangeRowIDs, err := engine.IndexRangeScan("users", "idx_age", types.NewIntValue(22), types.NewIntValue(28), true, true)
	if err != nil {
		t.Fatalf("IndexRangeScan failed: %v", err)
	}
	_ = rangeRowIDs // Just verify it doesn't crash

	// Get rows by row IDs
	if len(rowIDs) > 0 {
		rows, err := engine.GetRowsByRowIDs("users", rowIDs)
		if err != nil {
			t.Fatalf("GetRowsByRowIDs failed: %v", err)
		}
		_ = rows
	}

	// Drop index
	if err := engine.DropIndex("users", "idx_age"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	// Index should no longer be found
	_, hasIdx = engine.GetIndexForColumn("users", "age")
	if hasIdx {
		t.Error("Index should be dropped")
	}
}

func TestEngineIndexNonExistentTable(t *testing.T) {
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// GetIndexForColumn on non-existent table
	_, hasIdx := engine.GetIndexForColumn("nonexistent", "col")
	if hasIdx {
		t.Error("Should not find index on non-existent table")
	}

	// GetIndexForColumns on non-existent table
	_, hasIdx, _ = engine.GetIndexForColumns("nonexistent", []string{"col"})
	if hasIdx {
		t.Error("Should not find index on non-existent table")
	}

	// EstimateSelectivity on non-existent table
	selectivity := engine.EstimateSelectivity("nonexistent", "idx")
	if selectivity != 0 {
		t.Error("Should return 0 for non-existent table")
	}

	// IndexPointLookup on non-existent table
	_, err := engine.IndexPointLookup("nonexistent", "idx", types.NewIntValue(1))
	if err == nil {
		t.Error("Should fail for non-existent table")
	}

	// IndexRangeScan on non-existent table
	_, err = engine.IndexRangeScan("nonexistent", "idx", types.NewIntValue(1), types.NewIntValue(10), true, true)
	if err == nil {
		t.Error("Should fail for non-existent table")
	}

	// GetRowsByRowIDs on non-existent table
	_, err = engine.GetRowsByRowIDs("nonexistent", []row.RowID{1})
	if err == nil {
		t.Error("Should fail for non-existent table")
	}
}

func TestValidateValuesTypeMismatch(t *testing.T) {
	tests := []struct {
		name    string
		columns []*types.ColumnInfo
		values  []types.Value
		wantErr bool
	}{
		{
			name: "varchar accepts char",
			columns: []*types.ColumnInfo{
				{Name: "col", Type: types.TypeVarchar},
			},
			values: []types.Value{
				types.NewStringValue("test", types.TypeChar),
			},
			wantErr: false,
		},
		{
			name: "char accepts varchar",
			columns: []*types.ColumnInfo{
				{Name: "col", Type: types.TypeChar},
			},
			values: []types.Value{
				types.NewStringValue("test", types.TypeVarchar),
			},
			wantErr: false,
		},
		{
			name: "text accepts varchar",
			columns: []*types.ColumnInfo{
				{Name: "col", Type: types.TypeText},
			},
			values: []types.Value{
				types.NewStringValue("test", types.TypeVarchar),
			},
			wantErr: false,
		},
		{
			name: "int accepts seq",
			columns: []*types.ColumnInfo{
				{Name: "col", Type: types.TypeInt},
			},
			values: []types.Value{
				{Type: types.TypeSeq, Data: []byte{1, 0, 0, 0, 0, 0, 0, 0}},
			},
			wantErr: false,
		},
		{
			name: "type mismatch error",
			columns: []*types.ColumnInfo{
				{Name: "col", Type: types.TypeInt},
			},
			values: []types.Value{
				types.NewStringValue("not an int", types.TypeVarchar),
			},
			wantErr: true,
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