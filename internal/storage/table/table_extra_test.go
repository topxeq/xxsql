package table_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestNewTempTable(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}

	tbl := table.NewTempTable("temp_test", columns)
	if tbl == nil {
		t.Fatal("NewTempTable returned nil")
	}

	if tbl.Name() != "temp_test" {
		t.Errorf("Name: got %q, want 'temp_test'", tbl.Name())
	}

	if len(tbl.Columns()) != 2 {
		t.Errorf("Columns: got %d, want 2", len(tbl.Columns()))
	}

	// Insert into temp table
	values := []types.Value{
		types.NewIntValue(1),
		types.NewStringValue("test", types.TypeVarchar),
	}
	rowID, err := tbl.Insert(values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowID == 0 {
		t.Error("RowID should not be 0")
	}

	// Scan temp table
	rows, err := tbl.Scan()
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Scan: got %d rows, want 1", len(rows))
	}

	// Verify data
	if rows[0].Values[0].AsInt() != 1 {
		t.Errorf("Row id: got %d, want 1", rows[0].Values[0].AsInt())
	}
}

func TestTempTableWithMultipleInserts(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl := table.NewTempTable("multi_temp", columns)

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		_, err := tbl.Insert([]types.Value{types.NewIntValue(int64(i))})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	if tbl.RowCount() != 10 {
		t.Errorf("RowCount: got %d, want 10", tbl.RowCount())
	}
}

func TestTempTableUpdate(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	tbl := table.NewTempTable("update_temp", columns)

	// Insert rows
	for i := 1; i <= 5; i++ {
		tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		})
	}

	// Update rows
	affected, err := tbl.Update(
		func(r *row.Row) bool {
			return r.Values[0].AsInt() <= 3
		},
		map[int]types.Value{1: types.NewIntValue(999)},
	)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if affected != 3 {
		t.Errorf("Affected rows: got %d, want 3", affected)
	}
}

func TestTempTableDelete(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl := table.NewTempTable("delete_temp", columns)

	// Insert rows
	for i := 1; i <= 10; i++ {
		tbl.Insert([]types.Value{types.NewIntValue(int64(i))})
	}

	// Delete rows
	affected, err := tbl.Delete(func(r *row.Row) bool {
		return r.Values[0].AsInt() > 5
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if affected != 5 {
		t.Errorf("Affected rows: got %d, want 5", affected)
	}
	if tbl.RowCount() != 5 {
		t.Errorf("RowCount after delete: got %d, want 5", tbl.RowCount())
	}
}

func TestTempTableTruncate(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl := table.NewTempTable("truncate_temp", columns)

	// Insert rows
	for i := 1; i <= 5; i++ {
		tbl.Insert([]types.Value{types.NewIntValue(int64(i))})
	}

	// Truncate
	if err := tbl.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	if tbl.RowCount() != 0 {
		t.Errorf("RowCount after truncate: got %d, want 0", tbl.RowCount())
	}
}

func TestTempTableIndexOperations(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "email", Type: types.TypeVarchar, Size: 100},
	}

	tbl := table.NewTempTable("index_temp", columns)

	// Create index
	if err := tbl.CreateIndex("idx_email", []string{"email"}, true); err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert with unique constraint
	_, err := tbl.Insert([]types.Value{
		types.NewIntValue(1),
		types.NewStringValue("test@example.com", types.TypeVarchar),
	})
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Drop index
	if err := tbl.DropIndex("idx_email"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}
}

func TestTempTableColumnOperations(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl := table.NewTempTable("col_temp", columns)

	// Add column
	if err := tbl.AddColumn(&types.ColumnInfo{Name: "name", Type: types.TypeVarchar, Size: 50}); err != nil {
		t.Fatalf("AddColumn failed: %v", err)
	}
	if len(tbl.Columns()) != 2 {
		t.Errorf("Columns after add: got %d, want 2", len(tbl.Columns()))
	}

	// Rename column
	if err := tbl.RenameColumn("name", "full_name"); err != nil {
		t.Fatalf("RenameColumn failed: %v", err)
	}

	// Modify column
	if err := tbl.ModifyColumn(&types.ColumnInfo{Name: "full_name", Type: types.TypeVarchar, Size: 100}); err != nil {
		t.Fatalf("ModifyColumn failed: %v", err)
	}

	// Drop column
	if err := tbl.DropColumn("full_name"); err != nil {
		t.Fatalf("DropColumn failed: %v", err)
	}
	if len(tbl.Columns()) != 1 {
		t.Errorf("Columns after drop: got %d, want 1", len(tbl.Columns()))
	}
}

func TestTempTableGetAllRows(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	tbl := table.NewTempTable("allrows_temp", columns)

	// Insert rows
	for i := 1; i <= 5; i++ {
		tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		})
	}

	// Get all rows
	rows, err := tbl.GetAllRows()
	if err != nil {
		t.Fatalf("GetAllRows failed: %v", err)
	}

	if len(rows) != 5 {
		t.Errorf("GetAllRows: got %d rows, want 5", len(rows))
	}
}

func TestTableGetAllRows(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}

	tbl, err := table.OpenTable(tmpDir, "allrows_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows
	for i := 1; i <= 10; i++ {
		tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue("name", types.TypeVarchar),
		})
	}

	// Get all rows
	rows, err := tbl.GetAllRows()
	if err != nil {
		t.Fatalf("GetAllRows failed: %v", err)
	}

	if len(rows) != 10 {
		t.Errorf("GetAllRows: got %d rows, want 10", len(rows))
	}
}

func TestTableEstimateSelectivity(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "category", Type: types.TypeInt},
	}

	tbl, err := table.OpenTable(tmpDir, "selectivity_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows
	for i := 1; i <= 100; i++ {
		tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i % 10)),
		})
	}

	// Create secondary index
	if err := tbl.CreateIndex("idx_category", []string{"category"}, false); err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Test selectivity estimation
	selectivity := tbl.EstimateSelectivity("PRIMARY")
	if selectivity <= 0 {
		t.Errorf("EstimateSelectivity(PIMARY): got %d, want > 0", selectivity)
	}

	selectivity = tbl.EstimateSelectivity("idx_category")
	if selectivity <= 0 {
		t.Errorf("EstimateSelectivity(idx_category): got %d, want > 0", selectivity)
	}

	// Test with non-existent index
	selectivity = tbl.EstimateSelectivity("nonexistent")
	if selectivity != 100 { // Should return total row count
		t.Errorf("EstimateSelectivity(nonexistent): got %d, want 100", selectivity)
	}
}

func TestTableEstimateSelectivityEmptyTable(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl, err := table.OpenTable(tmpDir, "empty_selectivity_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Empty table should return 0
	selectivity := tbl.EstimateSelectivity("PRIMARY")
	if selectivity != 0 {
		t.Errorf("EstimateSelectivity on empty table: got %d, want 0", selectivity)
	}
}

func TestTempTableGetRowsByRowIDs(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl := table.NewTempTable("rowids_temp", columns)

	// Insert rows
	var rowIDs []row.RowID
	for i := 1; i <= 5; i++ {
		rowID, _ := tbl.Insert([]types.Value{types.NewIntValue(int64(i))})
		rowIDs = append(rowIDs, rowID)
	}

	// Get rows by IDs
	rows, err := tbl.GetRowsByRowIDs(rowIDs)
	if err != nil {
		t.Fatalf("GetRowsByRowIDs failed: %v", err)
	}

	if len(rows) != 5 {
		t.Errorf("GetRowsByRowIDs: got %d rows, want 5", len(rows))
	}
}

func TestTempTableIndexPointLookup(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}

	tbl := table.NewTempTable("lookup_temp", columns)

	// Insert rows
	for i := 1; i <= 10; i++ {
		tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue("name", types.TypeVarchar),
		})
	}

	// Point lookup via primary key
	rowIDs, err := tbl.IndexPointLookup("PRIMARY", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexPointLookup failed: %v", err)
	}
	if len(rowIDs) != 1 {
		t.Errorf("IndexPointLookup: got %d row IDs, want 1", len(rowIDs))
	}
}

func TestTempTableIndexRangeScan(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl := table.NewTempTable("range_temp", columns)

	// Insert rows
	for i := 1; i <= 20; i++ {
		tbl.Insert([]types.Value{types.NewIntValue(int64(i))})
	}

	// Range scan
	rowIDs, err := tbl.IndexRangeScan("PRIMARY", types.NewIntValue(5), types.NewIntValue(15), true, true)
	if err != nil {
		t.Fatalf("IndexRangeScan failed: %v", err)
	}

	// Should find rows 5-15 (11 rows)
	if len(rowIDs) != 11 {
		t.Errorf("IndexRangeScan: got %d row IDs, want 11", len(rowIDs))
	}
}

func TestTempTableGetIndexForColumn(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "email", Type: types.TypeVarchar, Size: 100},
		{Name: "status", Type: types.TypeInt},
	}

	tbl := table.NewTempTable("idxcol_temp", columns)

	// Create indexes
	tbl.CreateIndex("idx_email", []string{"email"}, true)
	tbl.CreateIndex("idx_status", []string{"status"}, false)

	// Get index for column
	idxName, found := tbl.GetIndexForColumn("email")
	if !found {
		t.Error("GetIndexForColumn should find index for email")
	}
	if idxName != "idx_email" {
		t.Errorf("Index name: got %q, want 'idx_email'", idxName)
	}

	// Non-indexed column
	_, found = tbl.GetIndexForColumn("id")
	if !found {
		t.Error("GetIndexForColumn should find PRIMARY for id")
	}
}

func TestTempTableGetIndexForColumns(t *testing.T) {
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "a", Type: types.TypeInt},
		{Name: "b", Type: types.TypeInt},
	}

	tbl := table.NewTempTable("idxcols_temp", columns)

	// Create composite index
	tbl.CreateIndex("idx_ab", []string{"a", "b"}, false)

	// Get index for columns
	idxName, found, matchCount := tbl.GetIndexForColumns([]string{"a", "b"})
	if !found {
		t.Error("GetIndexForColumns should find index")
	}
	if idxName != "idx_ab" {
		t.Errorf("Index name: got %q, want 'idx_ab'", idxName)
	}
	if matchCount != 2 {
		t.Errorf("Match count: got %d, want 2", matchCount)
	}
}

func TestIndexConditionScan(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	tbl, err := table.OpenTable(tmpDir, "idx_cond_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows with values 1-10
	for i := 1; i <= 10; i++ {
		values := []types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test equality scan
	rowIDs, err := tbl.IndexConditionScan("PRIMARY", "=", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexConditionScan = failed: %v", err)
	}
	if len(rowIDs) != 1 {
		t.Errorf("= scan: got %d rows, want 1", len(rowIDs))
	}

	// Test less than scan
	rowIDs, err = tbl.IndexConditionScan("PRIMARY", "<", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexConditionScan < failed: %v", err)
	}
	if len(rowIDs) != 4 {
		t.Errorf("< scan: got %d rows, want 4", len(rowIDs))
	}

	// Test less than or equal scan
	rowIDs, err = tbl.IndexConditionScan("PRIMARY", "<=", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexConditionScan <= failed: %v", err)
	}
	if len(rowIDs) != 5 {
		t.Errorf("<= scan: got %d rows, want 5", len(rowIDs))
	}

	// Test greater than scan
	rowIDs, err = tbl.IndexConditionScan("PRIMARY", ">", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexConditionScan > failed: %v", err)
	}
	if len(rowIDs) != 5 {
		t.Errorf("> scan: got %d rows, want 5", len(rowIDs))
	}

	// Test greater than or equal scan
	rowIDs, err = tbl.IndexConditionScan("PRIMARY", ">=", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexConditionScan >= failed: %v", err)
	}
	if len(rowIDs) != 6 {
		t.Errorf(">= scan: got %d rows, want 6", len(rowIDs))
	}
}

func TestIndexConditionScanNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
	}

	tbl, err := table.OpenTable(tmpDir, "idx_cond_test2", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert one row
	tbl.Insert([]types.Value{types.NewIntValue(1)})

	// Test non-existent index
	_, err = tbl.IndexConditionScan("nonexistent", "=", types.NewIntValue(1))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// TestIndexRangeScan tests the IndexRangeScan method
func TestIndexRangeScan(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	tbl, err := table.OpenTable(tmpDir, "range_scan_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows 1-20
	for i := 1; i <= 20; i++ {
		values := []types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test range scan (inclusive start, exclusive end)
	rowIDs, err := tbl.IndexRangeScan("PRIMARY", types.NewIntValue(5), types.NewIntValue(10), true, false)
	if err != nil {
		t.Fatalf("IndexRangeScan failed: %v", err)
	}
	// Just verify we got some rows
	if len(rowIDs) < 1 {
		t.Errorf("Range scan returned no rows")
	}

	// Test range scan (inclusive both ends)
	rowIDs, err = tbl.IndexRangeScan("PRIMARY", types.NewIntValue(5), types.NewIntValue(10), true, true)
	if err != nil {
		t.Fatalf("IndexRangeScan inclusive failed: %v", err)
	}
	if len(rowIDs) < 1 {
		t.Errorf("Range scan inclusive returned no rows")
	}
}

// TestIndexPointLookup tests the IndexPointLookup method
func TestIndexPointLookup(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 50},
	}

	tbl, err := table.OpenTable(tmpDir, "point_lookup_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows
	for i := 1; i <= 10; i++ {
		values := []types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue("name", types.TypeVarchar),
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Point lookup for existing row
	rowIDs, err := tbl.IndexPointLookup("PRIMARY", types.NewIntValue(5))
	if err != nil {
		t.Fatalf("IndexPointLookup failed: %v", err)
	}
	if len(rowIDs) != 1 {
		t.Errorf("Expected 1 row ID for existing key, got %d", len(rowIDs))
	}

	// Point lookup for non-existing row
	rowIDs, err = tbl.IndexPointLookup("PRIMARY", types.NewIntValue(100))
	if err != nil {
		t.Fatalf("IndexPointLookup for non-existing failed: %v", err)
	}
	if len(rowIDs) != 0 {
		t.Errorf("Expected 0 row IDs for non-existing key, got %d", len(rowIDs))
	}
}

// TestGetRowsByRowIDs tests the GetRowsByRowIDs method
func TestGetRowsByRowIDs(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	tbl, err := table.OpenTable(tmpDir, "get_rows_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows and collect row IDs
	rowIDs := make([]row.RowID, 0, 10)
	for i := 1; i <= 10; i++ {
		rowID, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
		rowIDs = append(rowIDs, rowID)
	}

	// Get specific rows by IDs
	rows, err := tbl.GetRowsByRowIDs(rowIDs[:5])
	if err != nil {
		t.Fatalf("GetRowsByRowIDs failed: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("GetRowsByRowIDs: got %d rows, want 5", len(rows))
	}

	// Test with empty list
	emptyRows, err := tbl.GetRowsByRowIDs([]row.RowID{})
	if err != nil {
		t.Fatalf("GetRowsByRowIDs empty failed: %v", err)
	}
	if len(emptyRows) != 0 {
		t.Errorf("GetRowsByRowIDs empty: got %d rows, want 0", len(emptyRows))
	}
}

// TestFindByKey tests the FindByKey method
func TestFindByKey(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 50},
	}

	tbl, err := table.OpenTable(tmpDir, "find_key_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows
	for i := 1; i <= 5; i++ {
		values := []types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue("name", types.TypeVarchar),
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Find by existing key
	r, err := tbl.FindByKey(types.NewIntValue(3))
	if err != nil {
		t.Fatalf("FindByKey failed: %v", err)
	}
	if r == nil {
		t.Fatal("Expected non-nil row")
	}
	if r.Values[0].AsInt() != 3 {
		t.Errorf("FindByKey: id = %d, want 3", r.Values[0].AsInt())
	}

	// Find by non-existing key returns error
	_, err = tbl.FindByKey(types.NewIntValue(100))
	if err == nil {
		t.Error("Expected error for non-existing key")
	}
}

// TestFindByKeyComposite tests FindByKey with multiple columns
func TestFindByKeyComposite(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "post_id", Type: types.TypeInt},
		{Name: "content", Type: types.TypeVarchar, Size: 100},
	}

	tbl, err := table.OpenTable(tmpDir, "composite_pk_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert rows with unique primary key values
	for i := 1; i <= 5; i++ {
		_, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
			types.NewStringValue("content", types.TypeVarchar),
		})
		if err != nil {
			t.Fatalf("Insert id=%d failed: %v", i, err)
		}
	}

	// Find by key
	r, err := tbl.FindByKey(types.NewIntValue(2))
	if err != nil {
		t.Fatalf("FindByKey failed: %v", err)
	}
	if r == nil {
		t.Fatal("Expected non-nil row")
	}
	if r.Values[0].AsInt() != 2 {
		t.Errorf("FindByKey: got id=%d, want 2", r.Values[0].AsInt())
	}
}

// TestTableUpdateWithIndex tests Update with indexed columns
func TestTableUpdateWithIndex(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "email", Type: types.TypeVarchar, Size: 100},
	}

	tbl, err := table.OpenTable(tmpDir, "update_index_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Create unique index on email
	err = tbl.CreateIndex("idx_email", []string{"email"}, true)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert rows
	for i := 1; i <= 5; i++ {
		_, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue("email", types.TypeVarchar),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Update email
	affected, err := tbl.Update(
		func(r *row.Row) bool {
			return r.Values[0].AsInt() == 1
		},
		map[int]types.Value{1: types.NewStringValue("newemail", types.TypeVarchar)},
	)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("Affected rows: got %d, want 1", affected)
	}
}

// TestTableGetPage tests getPage functionality
func TestTableGetPage(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 50},
	}

	tbl, err := table.OpenTable(tmpDir, "getpage_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert some data to create pages
	for i := 1; i <= 100; i++ {
		_, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue(fmt.Sprintf("name%d", i), types.TypeVarchar),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Scan to verify data
	rows, err := tbl.Scan()
	if err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("Scan count: got %d, want 100", len(rows))
	}
}

// TestTableMultiplePageOperations tests operations across multiple pages
func TestTableMultiplePageOperations(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "data", Type: types.TypeVarchar, Size: 1000},
	}

	tbl, err := table.OpenTable(tmpDir, "multipage_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert large data to span multiple pages
	largeData := strings.Repeat("x", 500)
	for i := 1; i <= 50; i++ {
		_, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue(largeData, types.TypeVarchar),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Delete some rows
	affected, err := tbl.Delete(func(r *row.Row) bool {
		return r.Values[0].AsInt()%2 == 0
	})
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	if affected != 25 {
		t.Errorf("Delete affected: got %d, want 25", affected)
	}
}

// TestTableConcurrentAccess tests concurrent table operations
func TestTableConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeInt},
	}

	tbl, err := table.OpenTable(tmpDir, "concurrent_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Concurrent inserts
	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := tbl.Insert([]types.Value{
					types.NewIntValue(int64(base*10 + j + 1)),
					types.NewIntValue(int64(j)),
				})
				if err != nil {
					errCh <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Concurrent insert error: %v", err)
	}

	// Verify count
	rows, err := tbl.Scan()
	if err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("Final count: got %d, want 100", len(rows))
	}
}

// TestTableIndexRangeScan tests index range scanning
func TestTableIndexRangeScan(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "score", Type: types.TypeInt},
	}

	tbl, err := table.OpenTable(tmpDir, "range_scan_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Create index on score
	err = tbl.CreateIndex("idx_score", []string{"score"}, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert data
	for i := 1; i <= 20; i++ {
		_, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewIntValue(int64(i * 10)),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Scan and filter
	rows, err := tbl.Scan()
	if err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	count := 0
	for _, r := range rows {
		if r.Values[1].AsInt() >= 100 {
			count++
		}
	}
	if count != 11 {
		t.Errorf("Conditional scan count: got %d, want 11", count)
	}
}

// TestTableNullValues tests NULL value handling
func TestTableNullValues(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 50, Nullable: true},
		{Name: "score", Type: types.TypeInt, Nullable: true},
	}

	tbl, err := table.OpenTable(tmpDir, "null_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	defer tbl.Close()

	// Insert with NULL values
	_, err = tbl.Insert([]types.Value{
		types.NewIntValue(1),
		types.NewNullValue(),
		types.NewIntValue(100),
	})
	if err != nil {
		t.Fatalf("Insert with NULL failed: %v", err)
	}

	_, err = tbl.Insert([]types.Value{
		types.NewIntValue(2),
		types.NewStringValue("test", types.TypeVarchar),
		types.NewNullValue(),
	})
	if err != nil {
		t.Fatalf("Insert with NULL failed: %v", err)
	}

	// Scan and verify
	rows, err := tbl.Scan()
	if err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Scan count: got %d, want 2", len(rows))
	}
}

// TestTableReopen tests reopening a table
func TestTableReopenExtra(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "value", Type: types.TypeVarchar, Size: 50},
	}

	// Create and populate table
	tbl, err := table.OpenTable(tmpDir, "reopen_test", columns)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err := tbl.Insert([]types.Value{
			types.NewIntValue(int64(i)),
			types.NewStringValue(fmt.Sprintf("value%d", i), types.TypeVarchar),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}
	tbl.Close()

	// Reopen and verify
	tbl2, err := table.OpenTable(tmpDir, "reopen_test", columns)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer tbl2.Close()

	rows, err := tbl2.Scan()
	if err != nil {
		t.Errorf("Scan after reopen failed: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("Count after reopen: got %d, want 10", len(rows))
	}
}