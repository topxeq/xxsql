// Package fts provides full-text search functionality for XxSQL.
package fts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSimpleTokenizer(t *testing.T) {
	tokenizer := NewSimpleTokenizer()

	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "Hello World",
			expected: []string{"hello", "world"},
		},
		{
			input:    "The quick brown fox",
			expected: []string{"quick", "brown", "fox"}, // "the" is stop word
		},
		{
			input:    "Hello, World! How are you?",
			expected: []string{"hello", "world", "how", "you"}, // "are" is stop word, but "how"/"you" are not
		},
		{
			input:    "Testing 123 numbers",
			expected: []string{"testing", "123", "numbers"}, // numbers are kept as tokens
		},
	}

	for _, tt := range tests {
		tokens := tokenizer.Tokenize(tt.input)
		var terms []string
		for _, tok := range tokens {
			terms = append(terms, tok.Term)
		}

		if len(terms) != len(tt.expected) {
			t.Errorf("Tokenize(%q) = %v, want %v", tt.input, terms, tt.expected)
			continue
		}

		for i, term := range terms {
			if term != tt.expected[i] {
				t.Errorf("Tokenize(%q)[%d] = %q, want %q", tt.input, i, term, tt.expected[i])
			}
		}
	}
}

func TestInvertedIndex(t *testing.T) {
	idx := NewInvertedIndex(NewSimpleTokenizer())

	// Add documents
	idx.AddDocument(1, "Hello World")
	idx.AddDocument(2, "Hello Go")
	idx.AddDocument(3, "World of Programming")

	// Test total documents
	if idx.TotalDocuments() != 3 {
		t.Errorf("TotalDocuments() = %d, want 3", idx.TotalDocuments())
	}

	// Test search for "hello"
	postings := idx.Search([]string{"hello"})
	if len(postings) != 2 {
		t.Errorf("Search('hello') returned %d postings, want 2", len(postings))
	}

	// Test search for "world"
	postings = idx.Search([]string{"world"})
	if len(postings) != 2 {
		t.Errorf("Search('world') returned %d postings, want 2", len(postings))
	}

	// Test search for "go"
	postings = idx.Search([]string{"go"})
	if len(postings) != 1 {
		t.Errorf("Search('go') returned %d postings, want 1", len(postings))
	}
	if len(postings) > 0 && postings[0].DocID != 2 {
		t.Errorf("Search('go') returned docID %d, want 2", postings[0].DocID)
	}

	// Test remove document
	idx.RemoveDocument(1)
	if idx.TotalDocuments() != 2 {
		t.Errorf("TotalDocuments() after remove = %d, want 2", idx.TotalDocuments())
	}

	// Search for "hello" should now only return doc 2
	postings = idx.Search([]string{"hello"})
	if len(postings) != 1 {
		t.Errorf("Search('hello') after remove returned %d postings, want 1", len(postings))
	}
}

func TestInvertedIndexSearchAny(t *testing.T) {
	idx := NewInvertedIndex(NewSimpleTokenizer())

	idx.AddDocument(1, "apple banana")
	idx.AddDocument(2, "apple orange")
	idx.AddDocument(3, "banana orange")

	// AND search (default)
	postings := idx.Search([]string{"apple", "banana"})
	if len(postings) != 1 {
		t.Errorf("AND search returned %d postings, want 1", len(postings))
	}

	// OR search
	postings = idx.SearchAny([]string{"apple", "banana"})
	if len(postings) != 3 {
		t.Errorf("OR search returned %d postings, want 3", len(postings))
	}
}

func TestFTSIndex(t *testing.T) {
	// Create temp directory for persistence
	tmpDir, err := os.MkdirTemp("", "fts-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "documents",
		Columns:    []string{"title", "content"},
		Tokenizer:  "simple",
		Persistent: false,
		DataDir:    tmpDir,
	}

	idx := NewFTSIndex(config)

	// Test basic properties
	if idx.Name() != "test_idx" {
		t.Errorf("Name() = %q, want 'test_idx'", idx.Name())
	}
	if idx.TableName() != "documents" {
		t.Errorf("TableName() = %q, want 'documents'", idx.TableName())
	}
	if len(idx.Columns()) != 2 {
		t.Errorf("Columns() returned %d columns, want 2", len(idx.Columns()))
	}

	// Index documents
	err = idx.IndexDocument(1, map[string]interface{}{
		"title":   "Hello World",
		"content": "This is a test document",
	})
	if err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}

	err = idx.IndexDocument(2, map[string]interface{}{
		"title":   "Go Programming",
		"content": "Learning Go is fun",
	})
	if err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}

	err = idx.IndexDocument(3, map[string]interface{}{
		"title":   "World News",
		"content": "Hello from the world",
	})
	if err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}

	// Search for "hello"
	results, err := idx.Search("hello")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Search('hello') returned %d results, want 2", len(results))
	}

	// Search for "world"
	results, err = idx.Search("world")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Search('world') returned %d results, want 2", len(results))
	}

	// Search for "go"
	results, err = idx.Search("go")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search('go') returned %d results, want 1", len(results))
	}

	// Test ranking - results should be sorted by score
	if len(results) > 0 {
		if results[0].DocID != 2 {
			t.Errorf("Top result for 'go' is doc %d, want 2", results[0].DocID)
		}
		if results[0].Score <= 0 {
			t.Errorf("Score should be positive, got %f", results[0].Score)
		}
	}
}

func TestFTSIndexUpdateRemove(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	// Index a document
	idx.IndexDocument(1, map[string]interface{}{
		"content": "Hello World",
	})

	// Verify it's indexed
	results, _ := idx.Search("hello")
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	// Update the document
	idx.UpdateDocument(1, map[string]interface{}{
		"content": "Goodbye World",
	})

	// Old term should not be found
	results, _ = idx.Search("hello")
	if len(results) != 0 {
		t.Errorf("Search('hello') after update returned %d results, want 0", len(results))
	}

	// New term should be found
	results, _ = idx.Search("goodbye")
	if len(results) != 1 {
		t.Errorf("Search('goodbye') after update returned %d results, want 1", len(results))
	}

	// Remove the document
	idx.RemoveDocument(1)

	// Term should not be found
	results, _ = idx.Search("goodbye")
	if len(results) != 0 {
		t.Errorf("Search('goodbye') after remove returned %d results, want 0", len(results))
	}
}

func TestFTSIndexBooleanOperators(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "test_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	idx.IndexDocument(1, map[string]interface{}{"content": "apple banana"})
	idx.IndexDocument(2, map[string]interface{}{"content": "apple orange"})
	idx.IndexDocument(3, map[string]interface{}{"content": "banana orange"})

	// AND search (default)
	results, err := idx.Search("apple banana")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("AND search returned %d results, want 1 (doc 1)", len(results))
	}

	// OR search
	results, err = idx.Search("apple OR banana")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("OR search returned %d results, want 3", len(results))
	}
}

func TestBM25Ranker(t *testing.T) {
	ranker := NewBM25Ranker()
	idx := NewInvertedIndex(NewSimpleTokenizer())

	// Add documents - use terms that don't appear in ALL docs
	// This ensures positive IDF for meaningful scoring
	idx.AddDocument(1, "apple apple apple banana") // doc 1: 3 apples, 1 banana
	idx.AddDocument(2, "apple cherry")              // doc 2: 1 apple, 1 cherry
	idx.AddDocument(3, "banana cherry date")        // doc 3: 1 banana, 1 cherry, 1 date

	t.Logf("Total documents: %d", idx.TotalDocuments())
	t.Logf("Avg doc length: %f", idx.AverageDocumentLength())
	t.Logf("Doc frequency for 'banana': %d", idx.DocumentFrequency("banana"))
	t.Logf("Doc frequency for 'cherry': %d", idx.DocumentFrequency("cherry"))
	t.Logf("Doc 1 length: %d", idx.DocumentLength(1))
	t.Logf("Doc 3 length: %d", idx.DocumentLength(3))

	// Search for "banana" - only appears in docs 1 and 3
	postings := idx.Search([]string{"banana"})

	t.Logf("Postings for 'banana': %+v", postings)

	if len(postings) != 2 {
		t.Fatalf("Expected 2 postings for 'banana', got %d", len(postings))
	}

	// Calculate scores
	for i, p := range postings {
		score := ranker.Score(p, idx, []string{"banana"})
		t.Logf("Posting[%d]: DocID=%d, Freq=%d, Score=%f", i, p.DocID, p.Frequency, score)
	}

	// The BM25 algorithm sets IDF to 0 when negative. For "banana" appearing in 2 out of 3 docs:
	// IDF = log((3 - 2 + 0.5) / (2 + 0.5)) = log(1.5/2.5) = log(0.6) = -0.51
	// This is negative, so IDF becomes 0, making score 0.
	// This is correct behavior - terms that appear in most documents have low discriminative power.
	// For a practical test, let's verify the ranker works with a more selective term.

	// Let's test with a term that appears in fewer documents
	idx2 := NewInvertedIndex(NewSimpleTokenizer())
	idx2.AddDocument(1, "rare unique term")
	idx2.AddDocument(2, "common word")
	idx2.AddDocument(3, "another common word")

	postings2 := idx2.Search([]string{"rare"})
	if len(postings2) != 1 {
		t.Fatalf("Expected 1 posting for 'rare', got %d", len(postings2))
	}

	score := ranker.Score(postings2[0], idx2, []string{"rare"})
	t.Logf("Score for 'rare' (appears in 1/3 docs): %f", score)

	if score <= 0 {
		t.Errorf("BM25 score for rare term should be positive, got %f", score)
	}
}

func TestFTSManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-manager-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewFTSManager(tmpDir)

	// Create index
	idx, err := manager.CreateIndex("idx1", "table1", []string{"content"}, "simple")
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	if idx == nil {
		t.Fatal("CreateIndex returned nil index")
	}

	// Get index
	retrieved, err := manager.GetIndex("idx1")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if retrieved != idx {
		t.Error("GetIndex returned different index instance")
	}

	// List indexes
	list := manager.ListIndexes()
	if len(list) != 1 || list[0] != "idx1" {
		t.Errorf("ListIndexes() = %v, want ['idx1']", list)
	}

	// Get indexes for table
	tableIndexes := manager.GetIndexesForTable("table1")
	if len(tableIndexes) != 1 {
		t.Errorf("GetIndexesForTable returned %d indexes, want 1", len(tableIndexes))
	}

	// Index document
	err = manager.IndexDocument("table1", 1, map[string]interface{}{"content": "test document"})
	if err != nil {
		t.Fatalf("IndexDocument failed: %v", err)
	}

	// Search
	results, err := manager.Search("idx1", "test")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search returned %d results, want 1", len(results))
	}

	// Remove document
	manager.RemoveDocument("table1", 1)
	results, _ = manager.Search("idx1", "test")
	if len(results) != 0 {
		t.Errorf("Search after remove returned %d results, want 0", len(results))
	}

	// Drop index
	err = manager.DropIndex("idx1")
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	// Verify index is gone
	list = manager.ListIndexes()
	if len(list) != 0 {
		t.Errorf("ListIndexes after drop = %v, want []", list)
	}
}

func TestFTSIndexPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts-persist-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := FTSIndexConfig{
		Name:       "persist_idx",
		TableName:  "documents",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: true,
		DataDir:    tmpDir,
	}

	// Create and populate index
	idx := NewFTSIndex(config)
	idx.IndexDocument(1, map[string]interface{}{"content": "hello world"})
	idx.IndexDocument(2, map[string]interface{}{"content": "foo bar"})

	// Save
	if err := idx.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	storagePath := filepath.Join(tmpDir, "fts", "persist_idx.json")
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		t.Errorf("Index file not created at %s", storagePath)
	}

	// Create new index and load
	idx2 := NewFTSIndex(config)
	if err := idx2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify data was loaded
	results, err := idx2.Search("hello")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search after load returned %d results, want 1", len(results))
	}
}

func TestFTSIndexStats(t *testing.T) {
	idx := NewFTSIndex(FTSIndexConfig{
		Name:       "stats_idx",
		TableName:  "test",
		Columns:    []string{"content"},
		Tokenizer:  "simple",
		Persistent: false,
	})

	idx.IndexDocument(1, map[string]interface{}{"content": "hello world"})
	idx.IndexDocument(2, map[string]interface{}{"content": "foo bar baz"})

	stats := idx.Stats()

	if stats.Name != "stats_idx" {
		t.Errorf("Stats.Name = %q, want 'stats_idx'", stats.Name)
	}
	if stats.TableName != "test" {
		t.Errorf("Stats.TableName = %q, want 'test'", stats.TableName)
	}
	if stats.DocumentCount != 2 {
		t.Errorf("Stats.DocumentCount = %d, want 2", stats.DocumentCount)
	}
	if stats.AvgDocLength <= 0 {
		t.Errorf("Stats.AvgDocLength should be positive, got %f", stats.AvgDocLength)
	}
}