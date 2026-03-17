package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestNewCatalog(t *testing.T) {
	cat := NewCatalog("/tmp/testdata")
	if cat == nil {
		t.Fatal("NewCatalog returned nil")
	}
	if cat.dataDir != "/tmp/testdata" {
		t.Errorf("dataDir = %s, want /tmp/testdata", cat.dataDir)
	}
}

func TestCatalog_CreateOpenClose(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)

	// Open (should create empty catalog)
	err = cat.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Close
	err = cat.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify catalog file was created
	catalogPath := filepath.Join(tmpDir, CatalogFileName)
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		t.Error("catalog file was not created")
	}
}

func TestCatalog_CreateTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}

	err = cat.CreateTable("users", columns)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	if !cat.TableExists("users") {
		t.Error("table 'users' should exist")
	}

	if cat.TableCount() != 1 {
		t.Errorf("TableCount = %d, want 1", cat.TableCount())
	}
}

func TestCatalog_CreateTable_AlreadyExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}

	// Create table first time
	cat.CreateTable("users", columns)

	// Try to create again
	err = cat.CreateTable("users", columns)
	if err == nil {
		t.Error("expected error for duplicate table")
	}
}

func TestCatalog_DropTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	// Drop the table
	err = cat.DropTable("users")
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	if cat.TableExists("users") {
		t.Error("table 'users' should not exist after drop")
	}
}

func TestCatalog_DropTable_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	err = cat.DropTable("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestCatalog_GetTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	table, err := cat.GetTable("users")
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	if table == nil {
		t.Fatal("GetTable returned nil")
	}
}

func TestCatalog_GetTable_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	_, err = cat.GetTable("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestCatalog_TableExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	if cat.TableExists("users") {
		t.Error("table 'users' should not exist yet")
	}

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	if !cat.TableExists("users") {
		t.Error("table 'users' should exist")
	}
}

func TestCatalog_ListTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)
	cat.CreateTable("orders", columns)
	cat.CreateTable("products", columns)

	list := cat.ListTables()
	if len(list) != 3 {
		t.Errorf("ListTables returned %d tables, want 3", len(list))
	}
}

func TestCatalog_TableCount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	if cat.TableCount() != 0 {
		t.Errorf("initial TableCount = %d, want 0", cat.TableCount())
	}

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	if cat.TableCount() != 1 {
		t.Errorf("TableCount = %d, want 1", cat.TableCount())
	}
}

func TestCatalog_Flush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	err = cat.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestCatalog_GetTableInfos(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	infos := cat.GetTableInfos()
	if len(infos) != 1 {
		t.Errorf("GetTableInfos returned %d infos, want 1", len(infos))
	}
}

func TestCatalog_RenameTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	err = cat.RenameTable("users", "customers")
	if err != nil {
		t.Fatalf("RenameTable failed: %v", err)
	}

	if cat.TableExists("users") {
		t.Error("old table name should not exist")
	}
	if !cat.TableExists("customers") {
		t.Error("new table name should exist")
	}
}

func TestCatalog_RenameTable_OldNotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	err = cat.RenameTable("nonexistent", "newname")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestCatalog_RenameTable_NewAlreadyExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)
	cat.CreateTable("customers", columns)

	err = cat.RenameTable("users", "customers")
	if err == nil {
		t.Error("expected error when new name already exists")
	}
}

func TestCatalog_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create and populate catalog
	cat1 := NewCatalog(tmpDir)
	if err := cat1.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat1.CreateTable("users", columns)
	cat1.CreateTable("orders", columns)
	cat1.Close()

	// Reopen and verify tables
	cat2 := NewCatalog(tmpDir)
	if err := cat2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat2.Close()

	if !cat2.TableExists("users") {
		t.Error("table 'users' should persist after reopen")
	}
	if !cat2.TableExists("orders") {
		t.Error("table 'orders' should persist after reopen")
	}
}

func TestCatalog_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	if err := cat.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	// Concurrent table existence checks
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			cat.TableExists("users")
			cat.TableExists("orders")
			cat.ListTables()
			cat.TableCount()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func BenchmarkCatalog_TableExists(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "catalog-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	cat.Open()
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.TableExists("users")
	}
}

func BenchmarkCatalog_GetTable(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "catalog-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cat := NewCatalog(tmpDir)
	cat.Open()
	defer cat.Close()

	columns := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}
	cat.CreateTable("users", columns)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat.GetTable("users")
	}
}
