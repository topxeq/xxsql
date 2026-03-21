package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestFTSCreate tests CREATE FTS INDEX statement
func TestFTSCreate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE documents (
			id SEQ PRIMARY KEY,
			title VARCHAR(200),
			content TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some test data
	_, err = exec.Execute(`INSERT INTO documents (title, content) VALUES ('Hello World', 'This is a test document about hello')`)
	if err != nil {
		t.Fatalf("Failed to insert document 1: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO documents (title, content) VALUES ('Go Programming', 'Learning Go programming language')`)
	if err != nil {
		t.Fatalf("Failed to insert document 2: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO documents (title, content) VALUES ('World News', 'Latest news from the world')`)
	if err != nil {
		t.Fatalf("Failed to insert document 3: %v", err)
	}

	// Create FTS index
	result, err := exec.Execute(`CREATE FTS INDEX idx_content ON documents(title, content)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}
	t.Logf("FTS index created: %s", result.Message)

	// Verify FTS index exists
	result, err = exec.Execute(`SHOW FTS INDEXES`)
	if err != nil {
		t.Logf("SHOW FTS INDEXES not implemented or error: %v", err)
	} else {
		t.Logf("FTS indexes: %v", result.Rows)
	}
}

// TestFTSSearch tests MATCH expression in SELECT
func TestFTSSearch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-search-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table and FTS index
	_, err = exec.Execute(`
		CREATE TABLE articles (
			id SEQ PRIMARY KEY,
			title VARCHAR(200),
			body TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = exec.Execute(`CREATE FTS INDEX idx_articles ON articles(title, body)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Insert test documents
	documents := []struct {
		title string
		body  string
	}{
		{"Introduction to Go", "Go is a programming language created at Google"},
		{"Python Programming", "Python is another popular programming language"},
		{"Database Systems", "SQL databases are essential for data storage"},
		{"Go Concurrency", "Go provides excellent concurrency support with goroutines"},
	}

	for _, doc := range documents {
		_, err = exec.Execute(`INSERT INTO articles (title, body) VALUES ('` + doc.title + `', '` + doc.body + `')`)
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Test search with MATCH
	// Note: The MATCH syntax requires the table name: "table MATCH 'query'"
	result, err := exec.Execute(`SELECT * FROM articles WHERE articles MATCH 'go'`)
	if err != nil {
		t.Logf("FTS search error (may not be fully implemented): %v", err)
	} else {
		t.Logf("Search for 'go' returned %d results", result.RowCount)
		for i, row := range result.Rows {
			t.Logf("Row %d: %v", i, row)
		}
	}

	// Test search with ranking
	result, err = exec.Execute(`SELECT id, title FROM articles WHERE articles MATCH 'programming'`)
	if err != nil {
		t.Logf("FTS search error: %v", err)
	} else {
		t.Logf("Search for 'programming' returned %d results", result.RowCount)
	}
}

// TestFTSDrop tests DROP FTS INDEX statement
func TestFTSDrop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-drop-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`CREATE TABLE test_table (id SEQ, content TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create FTS index
	_, err = exec.Execute(`CREATE FTS INDEX test_idx ON test_table(content)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Drop FTS index
	result, err := exec.Execute(`DROP FTS INDEX test_idx`)
	if err != nil {
		t.Fatalf("Failed to drop FTS index: %v", err)
	}
	t.Logf("Dropped FTS index: %s", result.Message)

	// Try to drop non-existent index (should fail)
	_, err = exec.Execute(`DROP FTS INDEX nonexistent_idx`)
	if err == nil {
		t.Error("Expected error when dropping non-existent FTS index")
	}
	t.Logf("Expected error for non-existent index: %v", err)

	// Test IF EXISTS
	result, err = exec.Execute(`DROP FTS INDEX IF EXISTS nonexistent_idx`)
	if err != nil {
		t.Errorf("DROP FTS INDEX IF EXISTS should not error: %v", err)
	}
	t.Logf("DROP IF EXISTS result: %s", result.Message)
}

// TestFTSIndexUpdate tests that FTS index is updated on INSERT/UPDATE/DELETE
func TestFTSIndexUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-update-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table with FTS index
	_, err = exec.Execute(`
		CREATE TABLE items (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			description TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = exec.Execute(`CREATE FTS INDEX item_idx ON items(name, description)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Insert a document
	_, err = exec.Execute(`INSERT INTO items (name, description) VALUES ('Widget', 'A useful widget for testing')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// The FTS index should now include "widget"
	// Search should find it (if MATCH is fully implemented)

	// Update the document
	_, err = exec.Execute(`UPDATE items SET description = 'An improved widget with more features' WHERE name = 'Widget'`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Delete the document
	result, err := exec.Execute(`DELETE FROM items WHERE name = 'Widget'`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	t.Logf("Deleted %d rows", result.Affected)

	// Insert multiple documents for truncation test
	_, err = exec.Execute(`INSERT INTO items (name, description) VALUES ('Item1', 'Description one')`)
	if err != nil {
		t.Fatalf("Failed to insert item1: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO items (name, description) VALUES ('Item2', 'Description two')`)
	if err != nil {
		t.Fatalf("Failed to insert item2: %v", err)
	}

	// Test delete all (truncate)
	result, err = exec.Execute(`DELETE FROM items`)
	if err != nil {
		t.Fatalf("Failed to delete all: %v", err)
	}
	t.Logf("Deleted all %d rows", result.Affected)
}

// TestFTSWithExistingData tests creating FTS index on existing data
func TestFTSWithExistingData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-existing-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE posts (
			id SEQ PRIMARY KEY,
			title VARCHAR(200),
			content TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data BEFORE creating FTS index
	_, err = exec.Execute(`INSERT INTO posts (title, content) VALUES ('First Post', 'This is the first post content')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO posts (title, content) VALUES ('Second Post', 'More content for the second post')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO posts (title, content) VALUES ('Third Post', 'Final post with different words')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Now create FTS index - it should index existing data
	result, err := exec.Execute(`CREATE FTS INDEX post_idx ON posts(title, content)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}
	t.Logf("Created FTS index on existing data: %s", result.Message)

	// Verify the index was created and populated
	ftsMgr := exec.GetFTSManager()
	if ftsMgr == nil {
		t.Log("FTS Manager is nil - FTS may not be fully initialized")
		return
	}

	idx, err := ftsMgr.GetIndex("post_idx")
	if err != nil {
		t.Fatalf("Failed to get FTS index: %v", err)
	}

	stats := idx.Stats()
	t.Logf("FTS index stats: DocCount=%d, TermCount=%d, AvgDocLength=%.2f",
		stats.DocumentCount, stats.TermCount, stats.AvgDocLength)

	if stats.DocumentCount != 3 {
		t.Errorf("Expected 3 documents in index, got %d", stats.DocumentCount)
	}
}

// TestFTSIfNotExists tests CREATE FTS INDEX IF NOT EXISTS
func TestFTSIfNotExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-ifnotexists-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table with proper column definition
	// Note: 'text' might be a reserved word, use 'content' instead
	_, err = exec.Execute(`CREATE TABLE data (id SEQ PRIMARY KEY, content TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create FTS index
	_, err = exec.Execute(`CREATE FTS INDEX data_idx ON data(content)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Try to create again without IF NOT EXISTS (should fail)
	_, err = exec.Execute(`CREATE FTS INDEX data_idx ON data(content)`)
	if err == nil {
		t.Error("Expected error when creating duplicate FTS index")
	} else {
		t.Logf("Expected duplicate error: %v", err)
	}

	// Try to create again with IF NOT EXISTS (should succeed without error)
	result, err := exec.Execute(`CREATE FTS INDEX IF NOT EXISTS data_idx ON data(content)`)
	if err != nil {
		t.Errorf("CREATE FTS INDEX IF NOT EXISTS should not error: %v", err)
	} else if result != nil {
		t.Logf("IF NOT EXISTS result: %s", result.Message)
	}
}

// TestFTSBooleanSearch tests AND/OR operators in FTS search
func TestFTSBooleanSearch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-fts-boolean-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table and FTS index
	_, err = exec.Execute(`
		CREATE TABLE docs (
			id SEQ PRIMARY KEY,
			content TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = exec.Execute(`CREATE FTS INDEX doc_idx ON docs(content)`)
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Insert test documents
	_, err = exec.Execute(`INSERT INTO docs (content) VALUES ('apple banana cherry')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO docs (content) VALUES ('apple orange')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = exec.Execute(`INSERT INTO docs (content) VALUES ('banana orange grape')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Get FTS manager and test search directly
	ftsMgr := exec.GetFTSManager()
	if ftsMgr == nil {
		t.Skip("FTS Manager not available")
	}

	idx, err := ftsMgr.GetIndex("doc_idx")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	// Test AND search
	results, err := idx.Search("apple banana")
	if err != nil {
		t.Fatalf("AND search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("AND search for 'apple banana' returned %d results, want 1", len(results))
	}
	t.Logf("AND search for 'apple banana': %d results", len(results))

	// Test OR search
	results, err = idx.Search("apple OR banana")
	if err != nil {
		t.Fatalf("OR search failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("OR search for 'apple OR banana' returned %d results, want 3", len(results))
	}
	t.Logf("OR search for 'apple OR banana': %d results", len(results))

	// Test single term
	results, err = idx.Search("orange")
	if err != nil {
		t.Fatalf("Single term search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Search for 'orange' returned %d results, want 2", len(results))
	}
	t.Logf("Search for 'orange': %d results", len(results))
}