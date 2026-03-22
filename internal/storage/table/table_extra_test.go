package table_test

import (
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