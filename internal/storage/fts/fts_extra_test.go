package fts

import (
	"os"
	"testing"
)

// TestSearchWithTerms tests the SearchWithTerms method
func TestSearchWithTerms(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	idx.IndexDocument(1, map[string]interface{}{"content": "apple banana cherry"})
	idx.IndexDocument(2, map[string]interface{}{"content": "apple orange"})
	idx.IndexDocument(3, map[string]interface{}{"content": "banana orange"})

	// AND search with terms
	results, err := idx.SearchWithTerms([]string{"apple", "banana"}, false)
	if err != nil {
		t.Fatalf("SearchWithTerms failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("AND search returned %d results, want 1", len(results))
	}

	// OR search with terms
	results, err = idx.SearchWithTerms([]string{"apple", "orange"}, true)
	if err != nil {
		t.Fatalf("SearchWithTerms OR failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("OR search returned %d results, want 3", len(results))
	}

	// Empty terms
	results, err = idx.SearchWithTerms([]string{}, false)
	if err != nil {
		t.Fatalf("SearchWithTerms with empty terms failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Empty terms should return 0 results, got %d", len(results))
	}
}

// TestSetRanker tests the SetRanker method
func TestSetRanker(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	customRanker := NewBM25Ranker()
	idx.SetRanker(customRanker)

	// Verify ranker is set by checking search works
	idx.IndexDocument(1, map[string]interface{}{"content": "test document"})
	results, err := idx.Search("test")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search returned %d results, want 1", len(results))
	}
}

// TestGetDocumentIDs tests the GetDocumentIDs method
func TestGetDocumentIDs(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	idx.IndexDocument(1, map[string]interface{}{"content": "apple"})
	idx.IndexDocument(2, map[string]interface{}{"content": "banana"})
	idx.IndexDocument(3, map[string]interface{}{"content": "cherry"})

	docIDs := idx.GetDocumentIDs()
	if len(docIDs) != 3 {
		t.Errorf("GetDocumentIDs returned %d IDs, want 3", len(docIDs))
	}

	// Verify all IDs are present
	idMap := make(map[uint64]bool)
	for _, id := range docIDs {
		idMap[id] = true
	}
	for i := uint64(1); i <= 3; i++ {
		if !idMap[i] {
			t.Errorf("Document ID %d not found in GetDocumentIDs result", i)
		}
	}
}

// TestGetPostings tests the GetPostings method
func TestGetPostings(t *testing.T) {
	invIdx := NewInvertedIndex(NewSimpleTokenizer())

	invIdx.AddDocument(1, "apple banana")
	invIdx.AddDocument(2, "apple cherry")
	invIdx.AddDocument(3, "banana cherry")

	// Get postings for "apple"
	postings := invIdx.GetPostings("apple")
	if len(postings) != 2 {
		t.Errorf("GetPostings('apple') returned %d postings, want 2", len(postings))
	}

	// Get postings for non-existent term
	postings = invIdx.GetPostings("nonexistent")
	if len(postings) != 0 {
		t.Errorf("GetPostings('nonexistent') should return 0 postings, got %d", len(postings))
	}
}

// TestFTSManagerUpdateDocument tests UpdateDocument in FTSManager
func TestFTSManagerUpdateDocument(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-update-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewFTSManager(tmpDir)

	// Create index
	_, err = manager.CreateIndex("idx1", "table1", []string{"content"}, "simple")
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Index document
	manager.IndexDocument("table1", 1, map[string]interface{}{"content": "hello world"})

	// Verify initial indexing
	results, _ := manager.Search("idx1", "hello")
	if len(results) != 1 {
		t.Fatalf("Initial search returned %d results, want 1", len(results))
	}

	// Update document
	err = manager.UpdateDocument("table1", 1, map[string]interface{}{"content": "goodbye world"})
	if err != nil {
		t.Fatalf("UpdateDocument failed: %v", err)
	}

	// Verify old term is gone
	results, _ = manager.Search("idx1", "hello")
	if len(results) != 0 {
		t.Errorf("Search for 'hello' after update returned %d results, want 0", len(results))
	}

	// Verify new term is present
	results, _ = manager.Search("idx1", "goodbye")
	if len(results) != 1 {
		t.Errorf("Search for 'goodbye' after update returned %d results, want 1", len(results))
	}
}

// TestFTSManagerSaveAllLoadAll tests SaveAll and LoadAll methods
func TestFTSManagerSaveAllLoadAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-saveall-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager and indexes
	manager := NewFTSManager(tmpDir)
	manager.CreateIndex("idx1", "table1", []string{"content"}, "simple")
	manager.CreateIndex("idx2", "table2", []string{"title"}, "simple")

	// Index documents
	manager.IndexDocument("table1", 1, map[string]interface{}{"content": "hello world"})
	manager.IndexDocument("table2", 1, map[string]interface{}{"title": "test title"})

	// Save all
	err = manager.SaveAll()
	if err != nil {
		t.Fatalf("SaveAll failed: %v", err)
	}

	// Create new manager and load
	manager2 := NewFTSManager(tmpDir)
	manager2.CreateIndex("idx1", "table1", []string{"content"}, "simple")
	manager2.CreateIndex("idx2", "table2", []string{"title"}, "simple")

	err = manager2.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	// Verify data was loaded
	results, _ := manager2.Search("idx1", "hello")
	if len(results) != 1 {
		t.Errorf("Search after LoadAll returned %d results, want 1", len(results))
	}
}

// TestFTSManagerGetStats tests GetStats method
func TestFTSManagerGetStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-stats-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewFTSManager(tmpDir)
	manager.CreateIndex("idx1", "table1", []string{"content"}, "simple")

	stats := manager.GetStats()
	if len(stats) != 1 {
		t.Errorf("GetStats returned %d stats, want 1", len(stats))
	}

	if stats[0].Name != "idx1" {
		t.Errorf("Stats[0].Name = %q, want 'idx1'", stats[0].Name)
	}
}

// TestFTSManagerDropIndexForTable tests DropIndexForTable method
func TestFTSManagerDropIndexForTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-drop-table-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewFTSManager(tmpDir)
	manager.CreateIndex("idx1", "table1", []string{"content"}, "simple")
	manager.CreateIndex("idx2", "table1", []string{"title"}, "simple")
	manager.CreateIndex("idx3", "table2", []string{"content"}, "simple")

	// Drop all indexes for table1
	err = manager.DropIndexForTable("table1")
	if err != nil {
		t.Fatalf("DropIndexForTable failed: %v", err)
	}

	// Verify indexes for table1 are gone
	list := manager.ListIndexes()
	if len(list) != 1 || list[0] != "idx3" {
		t.Errorf("ListIndexes after DropIndexForTable = %v, want ['idx3']", list)
	}
}

// TestFTSManagerClose tests Close method
func TestFTSManagerClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-close-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewFTSManager(tmpDir)
	manager.CreateIndex("idx1", "table1", []string{"content"}, "simple")

	// Close should not error
	err = manager.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestParseQueryWithNot tests parseQuery with NOT operator
func TestParseQueryWithNot(t *testing.T) {
	terms, op := parseQuery("apple NOT banana")
	if op != "AND" {
		t.Errorf("parseQuery operator = %q, want 'AND'", op)
	}
	// NOT terms should be removed
	for _, term := range terms {
		if term == "banana" {
			t.Error("parseQuery should remove NOT terms")
		}
	}
}

// TestRankedResultsSort tests the ranking sort interface
func TestRankedResultsSort(t *testing.T) {
	rankings := RankedResults{
		{DocID: 1, Score: 0.5},
		{DocID: 2, Score: 0.8},
		{DocID: 3, Score: 0.3},
	}

	// Test Len
	if rankings.Len() != 3 {
		t.Errorf("Len() = %d, want 3", rankings.Len())
	}

	// Test Less (sorted by score descending)
	if rankings.Less(0, 1) {
		t.Error("Less(0, 1) should be false (0.5 < 0.8)")
	}
	if !rankings.Less(1, 2) {
		t.Error("Less(1, 2) should be true (0.8 > 0.3)")
	}

	// Test Swap
	rankings.Swap(0, 1)
	if rankings[0].Score != 0.8 || rankings[1].Score != 0.5 {
		t.Error("Swap did not work correctly")
	}
}

// TestInvertedIndexSwap tests the Swap method on PostingsList
func TestInvertedIndexSwap(t *testing.T) {
	pl := PostingsList{
		{DocID: 1, Frequency: 5},
		{DocID: 2, Frequency: 3},
	}

	pl.Swap(0, 1)

	if pl[0].DocID != 2 || pl[1].DocID != 1 {
		t.Error("PostingsList.Swap did not work correctly")
	}
}

// TestFTSManagerNonExistentIndex tests operations on non-existent indexes
func TestFTSManagerNonExistentIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-nonexist-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewFTSManager(tmpDir)

	// Get non-existent index
	_, err = manager.GetIndex("nonexistent")
	if err == nil {
		t.Error("GetIndex should fail for non-existent index")
	}

	// Search non-existent index
	_, err = manager.Search("nonexistent", "test")
	if err == nil {
		t.Error("Search should fail for non-existent index")
	}

	// Drop non-existent index
	err = manager.DropIndex("nonexistent")
	if err == nil {
		t.Error("DropIndex should fail for non-existent index")
	}
}

// TestFTSIndexIndexDocumentWithNilValues tests IndexDocument with nil values
func TestFTSIndexIndexDocumentWithNilValues(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	// Index with nil value - should not crash
	err := idx.IndexDocument(1, map[string]interface{}{"content": nil})
	if err != nil {
		t.Logf("IndexDocument with nil returned: %v", err)
	}

	// Index with missing column - should not crash
	err = idx.IndexDocument(2, map[string]interface{}{})
	if err != nil {
		t.Logf("IndexDocument with missing column returned: %v", err)
	}
}

// TestFTSIndexUpdateNonExistentDocument tests updating a document that doesn't exist
func TestFTSIndexUpdateNonExistentDocument(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	// Update non-existent document - should add it
	err := idx.UpdateDocument(1, map[string]interface{}{"content": "test document"})
	if err != nil {
		t.Errorf("UpdateDocument for non-existent doc failed: %v", err)
	}

	// Verify it was added
	results, _ := idx.Search("test")
	if len(results) != 1 {
		t.Errorf("Search after update returned %d results, want 1", len(results))
	}
}