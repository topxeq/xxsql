package catalog

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestCatalog_Views(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-views-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	// Create a table first (for view to reference)
	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	// View should not exist initially
	if cat.ViewExists("user_view") {
		t.Error("View should not exist initially")
	}

	// Create view
	err = cat.CreateView("user_view", "SELECT id FROM users", []string{"id"}, "")
	if err != nil {
		t.Fatalf("CreateView failed: %v", err)
	}

	// View should exist now
	if !cat.ViewExists("user_view") {
		t.Error("View should exist after creation")
	}

	// Get view
	view, err := cat.GetView("user_view")
	if err != nil {
		t.Fatalf("GetView failed: %v", err)
	}
	if view.Name != "user_view" {
		t.Errorf("View name = %s, want user_view", view.Name)
	}
	if view.Query != "SELECT id FROM users" {
		t.Errorf("View query = %s", view.Query)
	}

	// List views
	views := cat.ListViews()
	if len(views) != 1 {
		t.Errorf("ListViews returned %d views, want 1", len(views))
	}

	// Create view that conflicts with table should fail
	err = cat.CreateView("users", "SELECT 1", nil, "")
	if err == nil {
		t.Error("CreateView should fail when table with same name exists")
	}

	// Create duplicate view should fail
	err = cat.CreateView("user_view", "SELECT 2", nil, "")
	if err == nil {
		t.Error("CreateView should fail for duplicate view")
	}

	// Drop view
	err = cat.DropView("user_view")
	if err != nil {
		t.Fatalf("DropView failed: %v", err)
	}

	// View should not exist
	if cat.ViewExists("user_view") {
		t.Error("View should not exist after drop")
	}

	// Get non-existent view should fail
	_, err = cat.GetView("nonexistent")
	if err == nil {
		t.Error("GetView should fail for non-existent view")
	}

	// Drop non-existent view should fail
	err = cat.DropView("nonexistent")
	if err == nil {
		t.Error("DropView should fail for non-existent view")
	}
}

func TestCatalog_ViewsPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-views-persist-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create catalog and view
	cat1 := NewCatalog(tmpDir)
	if err := cat1.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat1.CreateTable("users", columns)
	cat1.CreateView("user_view", "SELECT id FROM users", []string{"id"}, "CASCADED")
	cat1.Close()

	// Reopen and verify view
	cat2 := NewCatalog(tmpDir)
	if err := cat2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat2.Close()

	if !cat2.ViewExists("user_view") {
		t.Error("View should persist after reopen")
	}

	view, _ := cat2.GetView("user_view")
	if view.CheckOption != "CASCADED" {
		t.Errorf("CheckOption = %s, want CASCADED", view.CheckOption)
	}
}

func TestCatalog_Triggers(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-triggers-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	// Create a table for the trigger
	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	// Trigger should not exist initially
	if cat.TriggerExists("test_trigger") {
		t.Error("Trigger should not exist initially")
	}

	// Create trigger (timing=0=BEFORE, event=1=INSERT, granularity=0=ROW)
	err = cat.CreateTrigger("test_trigger", 0, 1, "users", 0, "id > 0", "INSERT INTO log VALUES (NEW.id)")
	if err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}

	// Trigger should exist
	if !cat.TriggerExists("test_trigger") {
		t.Error("Trigger should exist after creation")
	}

	// Get trigger
	trigger, err := cat.GetTrigger("test_trigger")
	if err != nil {
		t.Fatalf("GetTrigger failed: %v", err)
	}
	if trigger.Name != "test_trigger" {
		t.Errorf("Trigger name = %s, want test_trigger", trigger.Name)
	}
	if trigger.TableName != "users" {
		t.Errorf("Trigger TableName = %s, want users", trigger.TableName)
	}
	if trigger.Event != 1 {
		t.Errorf("Trigger Event = %d, want 1", trigger.Event)
	}

	// List triggers
	triggers := cat.ListTriggers()
	if len(triggers) != 1 {
		t.Errorf("ListTriggers returned %d triggers, want 1", len(triggers))
	}

	// Get triggers for table
	tableTriggers := cat.GetTriggersForTable("users", 1)
	if len(tableTriggers) != 1 {
		t.Errorf("GetTriggersForTable returned %d triggers, want 1", len(tableTriggers))
	}

	// Get triggers for different event should return empty
	tableTriggers = cat.GetTriggersForTable("users", 2)
	if len(tableTriggers) != 0 {
		t.Errorf("GetTriggersForTable should return 0 for different event")
	}

	// Create duplicate trigger should fail
	err = cat.CreateTrigger("test_trigger", 0, 1, "users", 0, "", "")
	if err == nil {
		t.Error("CreateTrigger should fail for duplicate trigger")
	}

	// Drop trigger
	err = cat.DropTrigger("test_trigger")
	if err != nil {
		t.Fatalf("DropTrigger failed: %v", err)
	}

	// Trigger should not exist
	if cat.TriggerExists("test_trigger") {
		t.Error("Trigger should not exist after drop")
	}

	// Get non-existent trigger should fail
	_, err = cat.GetTrigger("nonexistent")
	if err == nil {
		t.Error("GetTrigger should fail for non-existent trigger")
	}

	// Drop non-existent trigger should fail
	err = cat.DropTrigger("nonexistent")
	if err == nil {
		t.Error("DropTrigger should fail for non-existent trigger")
	}
}

func TestCatalog_TriggersPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-triggers-persist-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create catalog and trigger
	cat1 := NewCatalog(tmpDir)
	if err := cat1.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat1.CreateTable("users", columns)
	cat1.CreateTrigger("test_trigger", 1, 2, "users", 1, "x > 0", "UPDATE log SET count = count + 1")
	cat1.Close()

	// Reopen and verify trigger
	cat2 := NewCatalog(tmpDir)
	if err := cat2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat2.Close()

	if !cat2.TriggerExists("test_trigger") {
		t.Error("Trigger should persist after reopen")
	}

	trigger, _ := cat2.GetTrigger("test_trigger")
	if trigger.Timing != 1 {
		t.Errorf("Timing = %d, want 1", trigger.Timing)
	}
	if trigger.Event != 2 {
		t.Errorf("Event = %d, want 2", trigger.Event)
	}
}

func TestCatalog_FTSIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-fts-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	// Create a table for the FTS index
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "content", Type: types.TypeText},
	}
	cat.CreateTable("documents", columns)

	// FTS index should not exist initially
	if cat.FTSIndexExists("idx_content") {
		t.Error("FTS index should not exist initially")
	}

	// Create FTS index
	err = cat.CreateFTSIndex("idx_content", "documents", []string{"content"}, "unicode")
	if err != nil {
		t.Fatalf("CreateFTSIndex failed: %v", err)
	}

	// FTS index should exist
	if !cat.FTSIndexExists("idx_content") {
		t.Error("FTS index should exist after creation")
	}

	// Get FTS index
	fts, err := cat.GetFTSIndex("idx_content")
	if err != nil {
		t.Fatalf("GetFTSIndex failed: %v", err)
	}
	if fts.Name != "idx_content" {
		t.Errorf("FTS name = %s, want idx_content", fts.Name)
	}
	if fts.TableName != "documents" {
		t.Errorf("FTS TableName = %s, want documents", fts.TableName)
	}
	if fts.Tokenizer != "unicode" {
		t.Errorf("FTS Tokenizer = %s, want unicode", fts.Tokenizer)
	}

	// List FTS indexes
	ftsIndexes := cat.ListFTSIndexes()
	if len(ftsIndexes) != 1 {
		t.Errorf("ListFTSIndexes returned %d indexes, want 1", len(ftsIndexes))
	}

	// Get FTS indexes for table
	tableFTS := cat.GetFTSIndexesForTable("documents")
	if len(tableFTS) != 1 {
		t.Errorf("GetFTSIndexesForTable returned %d indexes, want 1", len(tableFTS))
	}

	// Create duplicate FTS index should fail
	err = cat.CreateFTSIndex("idx_content", "documents", []string{"content"}, "simple")
	if err == nil {
		t.Error("CreateFTSIndex should fail for duplicate index")
	}

	// Create FTS index on non-existent table should fail
	err = cat.CreateFTSIndex("idx_other", "nonexistent", []string{"col"}, "simple")
	if err == nil {
		t.Error("CreateFTSIndex should fail for non-existent table")
	}

	// Drop FTS index
	err = cat.DropFTSIndex("idx_content")
	if err != nil {
		t.Fatalf("DropFTSIndex failed: %v", err)
	}

	// FTS index should not exist
	if cat.FTSIndexExists("idx_content") {
		t.Error("FTS index should not exist after drop")
	}

	// Get non-existent FTS index should fail
	_, err = cat.GetFTSIndex("nonexistent")
	if err == nil {
		t.Error("GetFTSIndex should fail for non-existent index")
	}

	// Drop non-existent FTS index should fail
	err = cat.DropFTSIndex("nonexistent")
	if err == nil {
		t.Error("DropFTSIndex should fail for non-existent index")
	}
}

func TestCatalog_FTSIndexPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-fts-persist-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create catalog and FTS index
	cat1 := NewCatalog(tmpDir)
	if err := cat1.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "title", Type: types.TypeText},
		{Name: "body", Type: types.TypeText},
	}
	cat1.CreateTable("articles", columns)
	cat1.CreateFTSIndex("idx_articles", "articles", []string{"title", "body"}, "porter")
	cat1.Close()

	// Reopen and verify FTS index
	cat2 := NewCatalog(tmpDir)
	if err := cat2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat2.Close()

	if !cat2.FTSIndexExists("idx_articles") {
		t.Error("FTS index should persist after reopen")
	}

	fts, _ := cat2.GetFTSIndex("idx_articles")
	if len(fts.Columns) != 2 {
		t.Errorf("FTS Columns count = %d, want 2", len(fts.Columns))
	}
}

func TestCatalog_MultipleFTSIndexes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-multi-fts-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	// Create two tables
	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt, PrimaryKey: true}, {Name: "content", Type: types.TypeText}}
	cat.CreateTable("table1", columns)
	cat.CreateTable("table2", columns)

	// Create FTS indexes
	cat.CreateFTSIndex("idx1", "table1", []string{"content"}, "simple")
	cat.CreateFTSIndex("idx2", "table2", []string{"content"}, "unicode")

	// List all FTS indexes
	allFTS := cat.ListFTSIndexes()
	if len(allFTS) != 2 {
		t.Errorf("ListFTSIndexes returned %d, want 2", len(allFTS))
	}

	// Get FTS indexes for specific table
	table1FTS := cat.GetFTSIndexesForTable("table1")
	if len(table1FTS) != 1 {
		t.Errorf("GetFTSIndexesForTable(table1) returned %d, want 1", len(table1FTS))
	}

	table2FTS := cat.GetFTSIndexesForTable("table2")
	if len(table2FTS) != 1 {
		t.Errorf("GetFTSIndexesForTable(table2) returned %d, want 1", len(table2FTS))
	}
}