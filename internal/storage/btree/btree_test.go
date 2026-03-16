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
