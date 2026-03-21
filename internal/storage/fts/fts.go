// Package fts provides full-text search functionality for XxSQL.
package fts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FTSIndex represents a full-text search index on a table.
type FTSIndex struct {
	name         string
	tableName    string
	columns      []string
	invertedIdx  *InvertedIndex
	ranker       Ranker
	tokenizer    Tokenizer
	mu           sync.RWMutex
	persistent   bool
	storagePath  string
}

// FTSIndexConfig holds configuration for creating an FTS index.
type FTSIndexConfig struct {
	Name       string
	TableName  string
	Columns    []string
	Tokenizer  string // "simple", "porter"
	Persistent bool
	DataDir    string
}

// NewFTSIndex creates a new full-text search index.
func NewFTSIndex(config FTSIndexConfig) *FTSIndex {
	var tokenizer Tokenizer
	switch config.Tokenizer {
	case "porter":
		tokenizer = NewPorterStemmerTokenizer(NewSimpleTokenizer())
	default:
		tokenizer = NewSimpleTokenizer()
	}

	idx := &FTSIndex{
		name:        config.Name,
		tableName:   config.TableName,
		columns:     config.Columns,
		invertedIdx: NewInvertedIndex(tokenizer),
		ranker:      NewBM25Ranker(),
		tokenizer:   tokenizer,
		persistent:  config.Persistent,
	}

	if config.Persistent && config.DataDir != "" {
		idx.storagePath = filepath.Join(config.DataDir, "fts", config.Name+".json")
	}

	return idx
}

// Name returns the index name.
func (idx *FTSIndex) Name() string {
	return idx.name
}

// TableName returns the table name.
func (idx *FTSIndex) TableName() string {
	return idx.tableName
}

// Columns returns the indexed columns.
func (idx *FTSIndex) Columns() []string {
	return idx.columns
}

// IndexDocument indexes a document (row) with the given ID.
// The values map should contain the values for each indexed column.
func (idx *FTSIndex) IndexDocument(docID uint64, values map[string]interface{}) error {
	// Combine text from all indexed columns
	var textParts []string
	for _, col := range idx.columns {
		if val, ok := values[col]; ok {
			textParts = append(textParts, fmt.Sprintf("%v", val))
		}
	}
	text := strings.Join(textParts, " ")

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.invertedIdx.AddDocument(docID, text)
	return nil
}

// RemoveDocument removes a document from the index.
func (idx *FTSIndex) RemoveDocument(docID uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.invertedIdx.RemoveDocument(docID)
}

// UpdateDocument updates a document in the index.
func (idx *FTSIndex) UpdateDocument(docID uint64, values map[string]interface{}) error {
	// Combine text from all indexed columns
	var textParts []string
	for _, col := range idx.columns {
		if val, ok := values[col]; ok {
			textParts = append(textParts, fmt.Sprintf("%v", val))
		}
	}
	text := strings.Join(textParts, " ")

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.invertedIdx.UpdateDocument(docID, text)
	return nil
}

// SearchResult represents a search result.
type SearchResult struct {
	DocID uint64
	Score float64
}

// Search performs a full-text search with the given query.
// The query supports:
// - Simple terms: "hello world"
// - AND: "hello AND world"
// - OR: "hello OR world"
// - NOT: "hello NOT world"
func (idx *FTSIndex) Search(query string) ([]SearchResult, error) {
	// Parse the query
	terms, operator := parseQuery(query)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Get matching documents
	var postings PostingsList
	if operator == "OR" {
		postings = idx.invertedIdx.SearchAny(terms)
	} else {
		// Default is AND
		postings = idx.invertedIdx.Search(terms)
	}

	// Rank the results
	results := make([]SearchResult, 0, len(postings))
	for _, p := range postings {
		score := idx.ranker.Score(p, idx.invertedIdx, terms)
		results = append(results, SearchResult{
			DocID: p.DocID,
			Score: score,
		})
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results, nil
}

// SearchWithTerms performs a search with pre-tokenized terms.
func (idx *FTSIndex) SearchWithTerms(terms []string, useOr bool) ([]SearchResult, error) {
	if len(terms) == 0 {
		return nil, nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var postings PostingsList
	if useOr {
		postings = idx.invertedIdx.SearchAny(terms)
	} else {
		postings = idx.invertedIdx.Search(terms)
	}

	results := make([]SearchResult, 0, len(postings))
	for _, p := range postings {
		score := idx.ranker.Score(p, idx.invertedIdx, terms)
		results = append(results, SearchResult{
			DocID: p.DocID,
			Score: score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results, nil
}

// parseQuery parses a search query and returns terms and operator.
func parseQuery(query string) ([]string, string) {
	query = strings.ToLower(query)
	operator := "AND"

	// Check for explicit operators
	if strings.Contains(query, " or ") {
		operator = "OR"
		query = strings.ReplaceAll(query, " or ", " ")
	}
	if strings.Contains(query, " and ") {
		query = strings.ReplaceAll(query, " and ", " ")
	}

	// Remove NOT terms for now (simplified implementation)
	if strings.Contains(query, " not ") {
		parts := strings.Split(query, " not ")
		query = parts[0]
	}

	// Tokenize
	terms := strings.Fields(query)
	var filtered []string
	for _, t := range terms {
		t = strings.TrimSpace(t)
		if t != "" && len(t) >= 2 {
			filtered = append(filtered, t)
		}
	}

	return filtered, operator
}

// Stats returns statistics about the index.
func (idx *FTSIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexStats{
		Name:            idx.name,
		TableName:       idx.tableName,
		Columns:         idx.columns,
		DocumentCount:   idx.invertedIdx.TotalDocuments(),
		TermCount:       uint64(len(idx.invertedIdx.Terms())),
		AvgDocLength:    idx.invertedIdx.AverageDocumentLength(),
	}
}

// IndexStats holds statistics about an FTS index.
type IndexStats struct {
	Name          string
	TableName     string
	Columns       []string
	DocumentCount uint64
	TermCount     uint64
	AvgDocLength  float64
}

// Save persists the index to disk.
func (idx *FTSIndex) Save() error {
	if !idx.persistent || idx.storagePath == "" {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Don't save if there's no data
	if idx.invertedIdx.docCount == 0 {
		return nil
	}

	// Create directory if needed
	dir := filepath.Dir(idx.storagePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Serialize the index
	data := struct {
		Name       string
		TableName  string
		Columns    []string
		Index      map[string]PostingsList
		DocCount   uint64
		DocLength  map[uint64]int
	}{
		Name:      idx.name,
		TableName: idx.tableName,
		Columns:   idx.columns,
		Index:     idx.invertedIdx.index,
		DocCount:  idx.invertedIdx.docCount,
		DocLength: idx.invertedIdx.docLength,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(idx.storagePath, jsonData, 0644)
}

// Load loads the index from disk.
func (idx *FTSIndex) Load() error {
	if idx.storagePath == "" {
		return nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	data, err := os.ReadFile(idx.storagePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No saved index, start fresh
		}
		return err
	}

	var saved struct {
		Name      string
		TableName string
		Columns   []string
		Index     map[string]PostingsList
		DocCount  uint64
		DocLength map[uint64]int
	}

	if err := json.Unmarshal(data, &saved); err != nil {
		return err
	}

	idx.invertedIdx.index = saved.Index
	idx.invertedIdx.docCount = saved.DocCount
	idx.invertedIdx.docLength = saved.DocLength
	return nil
}

// SetRanker sets a custom ranker for the index.
func (idx *FTSIndex) SetRanker(ranker Ranker) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.ranker = ranker
}

// GetDocumentIDs returns all document IDs in the index.
func (idx *FTSIndex) GetDocumentIDs() []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	ids := make(map[uint64]bool)
	for _, postings := range idx.invertedIdx.index {
		for _, p := range postings {
			ids[p.DocID] = true
		}
	}

	result := make([]uint64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}