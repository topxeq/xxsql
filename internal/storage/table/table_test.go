// Package table_test provides tests for table management.
package table_test

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestTableAutoIncrement(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Define columns with auto-increment
	columns := []*types.ColumnInfo{
		{
			Name:     "id",
			Type:     types.TypeSeq,
			AutoIncr: true,
			PrimaryKey: true,
		},
		{
			Name: "name",
			Type: types.TypeVarchar,
			Size: 100,
		},
	}

	// Open table
	tbl, err := table.OpenTable(tmpDir, "test_table", columns)
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer tbl.Close()

	// Insert rows without providing id (auto-increment should work)
	for i := 1; i <= 5; i++ {
		values := []types.Value{
			types.NewNullValue(), // id will be auto-generated
			types.NewStringValue("user"+string(rune('0'+i)), types.TypeVarchar),
		}

		rowID, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
		if rowID == 0 {
			t.Errorf("Expected non-zero row ID for row %d", i)
		}
	}

	// Verify row count
	if tbl.RowCount() != 5 {
		t.Errorf("Expected 5 rows, got %d", tbl.RowCount())
	}
}

func TestTableAutoIncrementPersist(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Define columns with auto-increment
	columns := []*types.ColumnInfo{
		{
			Name:     "id",
			Type:     types.TypeSeq,
			AutoIncr: true,
			PrimaryKey: true,
		},
		{
			Name: "value",
			Type: types.TypeInt,
		},
	}

	// Open table and insert some rows
	tbl, err := table.OpenTable(tmpDir, "persist_table", columns)
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}

	// Insert 3 rows
	for i := 1; i <= 3; i++ {
		values := []types.Value{
			types.NewNullValue(),
			types.NewIntValue(int64(i * 10)),
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Close the table
	if err := tbl.Close(); err != nil {
		t.Fatalf("Failed to close table: %v", err)
	}

	// Reopen the table
	tbl2, err := table.OpenTable(tmpDir, "persist_table", columns)
	if err != nil {
		t.Fatalf("Failed to reopen table: %v", err)
	}
	defer tbl2.Close()

	// Insert another row - auto-increment should continue
	values := []types.Value{
		types.NewNullValue(),
		types.NewIntValue(40),
	}
	_, err = tbl2.Insert(values)
	if err != nil {
		t.Fatalf("Failed to insert row after reopen: %v", err)
	}

	// Total should be 4 rows
	if tbl2.RowCount() != 4 {
		t.Errorf("Expected 4 rows, got %d", tbl2.RowCount())
	}
}

func TestTableTruncateResetSequence(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Define columns with auto-increment
	columns := []*types.ColumnInfo{
		{
			Name:     "id",
			Type:     types.TypeSeq,
			AutoIncr: true,
			PrimaryKey: true,
		},
		{
			Name: "data",
			Type: types.TypeInt,
		},
	}

	// Open table
	tbl, err := table.OpenTable(tmpDir, "trunc_table", columns)
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer tbl.Close()

	// Insert some rows
	for i := 1; i <= 5; i++ {
		values := []types.Value{
			types.NewNullValue(),
			types.NewIntValue(int64(i)),
		}
		tbl.Insert(values)
	}

	// Truncate
	if err := tbl.Truncate(); err != nil {
		t.Fatalf("Failed to truncate table: %v", err)
	}

	// Insert a new row - auto-increment should restart from 1
	values := []types.Value{
		types.NewNullValue(),
		types.NewIntValue(100),
	}
	_, err = tbl.Insert(values)
	if err != nil {
		t.Fatalf("Failed to insert after truncate: %v", err)
	}

	// Row count should be 1
	if tbl.RowCount() != 1 {
		t.Errorf("Expected 1 row after truncate, got %d", tbl.RowCount())
	}
}

func TestTableWithProvidedAutoIncrValue(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Define columns with auto-increment
	columns := []*types.ColumnInfo{
		{
			Name:     "id",
			Type:     types.TypeSeq,
			AutoIncr: true,
			PrimaryKey: true,
		},
		{
			Name: "name",
			Type: types.TypeVarchar,
			Size: 50,
		},
	}

	// Open table
	tbl, err := table.OpenTable(tmpDir, "explicit_table", columns)
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer tbl.Close()

	// Insert with explicit id value
	values := []types.Value{
		types.NewIntValue(100), // explicit id
		types.NewStringValue("explicit", types.TypeVarchar),
	}
	_, err = tbl.Insert(values)
	if err != nil {
		t.Fatalf("Failed to insert with explicit id: %v", err)
	}

	// Insert with null id (auto-increment)
	values = []types.Value{
		types.NewNullValue(), // auto-increment should provide value
		types.NewStringValue("auto", types.TypeVarchar),
	}
	_, err = tbl.Insert(values)
	if err != nil {
		t.Fatalf("Failed to insert with null id: %v", err)
	}

	// Should have 2 rows
	if tbl.RowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", tbl.RowCount())
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
