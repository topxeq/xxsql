// Package btree_test provides tests for B+ tree implementation.
package btree_test

import (
	"testing"

	"github.com/topxeq/xxsql/internal/storage/btree"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestBTreeBasicInsert(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert single key
	err := tree.Insert(btree.Key{Value: types.NewIntValue(1)}, []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Search for key
	val, found := tree.Search(btree.Key{Value: types.NewIntValue(1)})
	if !found {
		t.Error("Key not found")
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val)
	}
}

func TestBTreeMultipleInserts(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert multiple keys
	for i := 1; i <= 10; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Search for all keys
	for i := 1; i <= 10; i++ {
		_, found := tree.Search(btree.Key{Value: types.NewIntValue(int64(i))})
		if !found {
			t.Errorf("Key %d not found", i)
		}
	}

	// Search for non-existent key
	_, found := tree.Search(btree.Key{Value: types.NewIntValue(100)})
	if found {
		t.Error("Non-existent key should not be found")
	}
}

func TestBTreeSplit(t *testing.T) {
	// Use small order to force splits
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert enough keys to cause multiple splits
	for i := 1; i <= 20; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify all keys are still accessible
	for i := 1; i <= 20; i++ {
		_, found := tree.Search(btree.Key{Value: types.NewIntValue(int64(i))})
		if !found {
			t.Errorf("Key %d not found after split", i)
		}
	}

	// Check tree height
	height := tree.Height()
	if height < 2 {
		t.Errorf("Expected height >= 2 after splits, got %d", height)
	}
}

func TestBTreeReverseInsert(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys in reverse order
	for i := 20; i >= 1; i-- {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify all keys
	for i := 1; i <= 20; i++ {
		_, found := tree.Search(btree.Key{Value: types.NewIntValue(int64(i))})
		if !found {
			t.Errorf("Key %d not found", i)
		}
	}
}

func TestBTreeScan(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys in random order
	keys := []int64{5, 2, 8, 1, 9, 3, 7, 4, 6, 10}
	for _, k := range keys {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(k)}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Scan all entries
	entries := tree.Scan()
	if len(entries) != 10 {
		t.Errorf("Expected 10 entries, got %d", len(entries))
	}

	// Verify entries are in sorted order
	for i := 1; i < len(entries); i++ {
		if entries[i].Key.Compare(entries[i-1].Key) < 0 {
			t.Error("Entries not in sorted order")
		}
	}
}

func TestBTreeRange(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys
	for i := 1; i <= 20; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Range query [5, 10]
	entries := tree.Range(
		btree.Key{Value: types.NewIntValue(5)},
		btree.Key{Value: types.NewIntValue(10)},
	)

	if len(entries) != 6 {
		t.Errorf("Expected 6 entries in range [5, 10], got %d", len(entries))
	}

	// Verify all entries are in range
	for _, e := range entries {
		k := e.Key.Value.AsInt()
		if k < 5 || k > 10 {
			t.Errorf("Entry key %d out of range [5, 10]", k)
		}
	}
}

func TestBTreeDelete(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys
	for i := 1; i <= 10; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Delete a key
	err := tree.Delete(btree.Key{Value: types.NewIntValue(5)})
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify key is deleted
	_, found := tree.Search(btree.Key{Value: types.NewIntValue(5)})
	if found {
		t.Error("Key should be deleted")
	}

	// Verify other keys still exist
	for i := 1; i <= 10; i++ {
		if i == 5 {
			continue
		}
		_, found := tree.Search(btree.Key{Value: types.NewIntValue(int64(i))})
		if !found {
			t.Errorf("Key %d should still exist", i)
		}
	}
}

func TestBTreeStringKeys(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeVarchar)

	// Insert string keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		err := tree.Insert(btree.Key{Value: types.NewStringValue(k, types.TypeVarchar)}, []byte("fruit"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Search for keys
	for _, k := range keys {
		_, found := tree.Search(btree.Key{Value: types.NewStringValue(k, types.TypeVarchar)})
		if !found {
			t.Errorf("Key '%s' not found", k)
		}
	}
}

func TestIndex(t *testing.T) {
	im := btree.NewIndexManager("test_table", nil)

	// Create primary key index
	_, err := im.CreateIndex("pk_id", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Create secondary index
	_, err = im.CreateIndex("idx_name", []string{"name"}, btree.IndexTypeNonUnique, types.TypeVarchar)
	if err != nil {
		t.Fatalf("Failed to create secondary index: %v", err)
	}

	// List indexes
	indexes := im.ListIndexes()
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(indexes))
	}

	// Check primary key
	if !im.HasPrimary() {
		t.Error("Should have primary key")
	}

	// Drop index
	err = im.DropIndex("idx_name")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	indexes = im.ListIndexes()
	if len(indexes) != 1 {
		t.Errorf("Expected 1 index after drop, got %d", len(indexes))
	}
}

func TestIndexInsertDelete(t *testing.T) {
	idx, err := btree.NewIndexManager("test_table", nil).CreateIndex("pk", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert keys
	for i := 1; i <= 10; i++ {
		err := idx.Insert(types.NewIntValue(int64(i)), row.RowID(i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Search
	rowID, found := idx.Search(types.NewIntValue(5))
	if !found {
		t.Error("Key not found")
	}
	if rowID != 5 {
		t.Errorf("Expected row ID 5, got %d", rowID)
	}

	// Delete
	err = idx.Delete(types.NewIntValue(5))
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify deletion
	_, found = idx.Search(types.NewIntValue(5))
	if found {
		t.Error("Key should be deleted")
	}
}

func TestBTreeCount(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys
	for i := 1; i <= 100; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	if tree.Count() != 100 {
		t.Errorf("Expected count 100, got %d", tree.Count())
	}
}

func TestBTreeSerialize(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys
	for i := 1; i <= 10; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Serialize
	data := tree.Serialize()
	if len(data) == 0 {
		t.Error("Serialized data should not be empty")
	}
}

func TestBTreeFlush(t *testing.T) {
	tree := btree.NewInMemoryBTree(4, types.TypeInt)

	// Insert keys
	for i := 1; i <= 5; i++ {
		err := tree.Insert(btree.Key{Value: types.NewIntValue(int64(i))}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Flush (should not error for in-memory tree)
	err := tree.Flush()
	if err != nil {
		t.Errorf("Flush error: %v", err)
	}
}

func TestIndexTypeString(t *testing.T) {
	tests := []struct {
		typ      btree.IndexType
		expected string
	}{
		{btree.IndexTypePrimary, "PRIMARY"},
		{btree.IndexTypeUnique, "UNIQUE"},
		{btree.IndexTypeNonUnique, "INDEX"},
		{btree.IndexType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.typ.String()
			if result != tt.expected {
				t.Errorf("IndexType.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestIndexRange(t *testing.T) {
	idx, err := btree.NewIndexManager("test_table", nil).CreateIndex("pk", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert keys
	for i := 1; i <= 20; i++ {
		err := idx.Insert(types.NewIntValue(int64(i)), row.RowID(i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Range query
	rowIDs := idx.Range(types.NewIntValue(5), types.NewIntValue(10))
	if len(rowIDs) != 6 {
		t.Errorf("Expected 6 row IDs in range, got %d", len(rowIDs))
	}
}

func TestIndexScan(t *testing.T) {
	idx, err := btree.NewIndexManager("test_table", nil).CreateIndex("pk", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert keys
	for i := 1; i <= 10; i++ {
		err := idx.Insert(types.NewIntValue(int64(i)), row.RowID(i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Scan all
	rowIDs := idx.Scan()
	if len(rowIDs) != 10 {
		t.Errorf("Expected 10 row IDs, got %d", len(rowIDs))
	}
}

func TestIndexCount(t *testing.T) {
	idx, err := btree.NewIndexManager("test_table", nil).CreateIndex("pk", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert keys
	for i := 1; i <= 50; i++ {
		err := idx.Insert(types.NewIntValue(int64(i)), row.RowID(i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	if idx.Count() != 50 {
		t.Errorf("Expected count 50, got %d", idx.Count())
	}
}

func TestIndexFlush(t *testing.T) {
	idx, err := btree.NewIndexManager("test_table", nil).CreateIndex("pk", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	err = idx.Flush()
	if err != nil {
		t.Errorf("Flush error: %v", err)
	}
}

func TestIndexManager_GetIndex(t *testing.T) {
	im := btree.NewIndexManager("test_table", nil)

	_, err := im.CreateIndex("idx1", []string{"id"}, btree.IndexTypeNonUnique, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Get existing index
	idx, err := im.GetIndex("idx1")
	if err != nil {
		t.Errorf("Failed to get index: %v", err)
	}
	if idx == nil {
		t.Error("Index should not be nil")
	}

	// Get non-existent index
	_, err = im.GetIndex("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestIndexManager_GetPrimary(t *testing.T) {
	im := btree.NewIndexManager("test_table", nil)

	// No primary initially
	if im.GetPrimary() != nil {
		t.Error("GetPrimary should return nil when no primary exists")
	}

	// Create primary key
	_, err := im.CreateIndex("pk", []string{"id"}, btree.IndexTypePrimary, types.TypeInt)
	if err != nil {
		t.Fatalf("Failed to create primary key: %v", err)
	}

	// Now should return primary
	pk := im.GetPrimary()
	if pk == nil {
		t.Error("GetPrimary should return the primary key index")
	}
}

func TestIndexManager_InsertIntoIndexes(t *testing.T) {
	im := btree.NewIndexManager("test_table", nil)

	// Create indexes
	im.CreateIndex("idx_id", []string{"id"}, btree.IndexTypeNonUnique, types.TypeInt)
	im.CreateIndex("idx_name", []string{"name"}, btree.IndexTypeNonUnique, types.TypeVarchar)

	// Column map
	columnMap := map[string]int{"id": 0, "name": 1}

	// Insert values
	values := []types.Value{types.NewIntValue(1), types.NewStringValue("test", types.TypeVarchar)}
	err := im.InsertIntoIndexes(values, row.RowID(1), columnMap)
	if err != nil {
		t.Errorf("InsertIntoIndexes error: %v", err)
	}
}

func TestIndexManager_DeleteFromIndexes(t *testing.T) {
	im := btree.NewIndexManager("test_table", nil)

	// Create indexes
	im.CreateIndex("idx_id", []string{"id"}, btree.IndexTypeNonUnique, types.TypeInt)

	// Column map
	columnMap := map[string]int{"id": 0}

	// Insert first
	values := []types.Value{types.NewIntValue(1)}
	im.InsertIntoIndexes(values, row.RowID(1), columnMap)

	// Delete
	err := im.DeleteFromIndexes(values, columnMap)
	if err != nil {
		t.Errorf("DeleteFromIndexes error: %v", err)
	}
}

func TestIndexManager_FlushAll(t *testing.T) {
	im := btree.NewIndexManager("test_table", nil)

	im.CreateIndex("idx1", []string{"id"}, btree.IndexTypeNonUnique, types.TypeInt)

	err := im.FlushAll()
	if err != nil {
		t.Errorf("FlushAll error: %v", err)
	}
}

func TestNodeMethods(t *testing.T) {
	node := btree.NewNode(1, true)

	// Test IsFull (empty node should not be full)
	if node.IsFull(4) {
		t.Error("Empty node should not be full")
	}

	// Test IsUnderflow (empty node is underflow for order > 2)
	if !node.IsUnderflow(4) {
		t.Error("Empty node should be underflow")
	}

	// Test CanLend (empty node cannot lend)
	if node.CanLend(4) {
		t.Error("Empty node should not be able to lend")
	}

	// Test String
	str := node.String()
	if str == "" {
		t.Error("Node.String() should not be empty")
	}
}

func TestNodeSerializeDeserialize(t *testing.T) {
	original := btree.NewNode(1, true)
	original.Keys = []btree.Key{{Value: types.NewIntValue(1)}, {Value: types.NewIntValue(2)}}
	original.Entries = []btree.Entry{
		{Key: btree.Key{Value: types.NewIntValue(1)}, Value: []byte("val1")},
		{Key: btree.Key{Value: types.NewIntValue(2)}, Value: []byte("val2")},
	}

	// Serialize
	data := original.Serialize()
	if len(data) == 0 {
		t.Error("Serialized data should not be empty")
	}

	// Deserialize
	node, err := btree.DeserializeNode(data, 1)
	if err != nil {
		t.Fatalf("DeserializeNode error: %v", err)
	}

	if len(node.Keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(node.Keys))
	}
}

func TestKeyCompare(t *testing.T) {
	tests := []struct {
		k1, k2   btree.Key
		expected int
	}{
		{btree.Key{Value: types.NewIntValue(1)}, btree.Key{Value: types.NewIntValue(2)}, -1},
		{btree.Key{Value: types.NewIntValue(2)}, btree.Key{Value: types.NewIntValue(1)}, 1},
		{btree.Key{Value: types.NewIntValue(5)}, btree.Key{Value: types.NewIntValue(5)}, 0},
	}

	for _, tt := range tests {
		result := tt.k1.Compare(tt.k2)
		if result != tt.expected {
			t.Errorf("Key.Compare() = %d, want %d", result, tt.expected)
		}
	}
}

func TestNewIndex(t *testing.T) {
	info := &btree.IndexInfo{
		Name:      "test_idx",
		TableName: "test_table",
		Columns:   []string{"id"},
		Type:      btree.IndexTypePrimary,
	}

	idx := btree.NewIndex(info, nil)
	if idx == nil {
		t.Error("NewIndex returned nil")
	}
}
