// Package fts provides full-text search functionality for XxSQL.
package fts

import (
	"sort"
	"sync"
)

// Posting represents a document in the inverted index.
type Posting struct {
	DocID     uint64 // Document ID (usually row ID)
	Positions []int  // Positions where the term appears
	Frequency int    // Number of times the term appears in this document
}

// PostingsList is a list of postings for a term, sorted by DocID.
type PostingsList []Posting

// Len implements sort.Interface.
func (p PostingsList) Len() int { return len(p) }

// Less implements sort.Interface.
func (p PostingsList) Less(i, j int) bool { return p[i].DocID < p[j].DocID }

// Swap implements sort.Interface.
func (p PostingsList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// InvertedIndex is the core data structure for full-text search.
type InvertedIndex struct {
	mu        sync.RWMutex
	index     map[string]PostingsList // term -> postings list
	docCount  uint64                  // total number of documents
	docLength map[uint64]int          // doc ID -> document length (in tokens)
	tokenizer Tokenizer
}

// NewInvertedIndex creates a new inverted index.
func NewInvertedIndex(tokenizer Tokenizer) *InvertedIndex {
	return &InvertedIndex{
		index:     make(map[string]PostingsList),
		docLength: make(map[uint64]int),
		tokenizer: tokenizer,
	}
}

// AddDocument adds a document to the index.
func (idx *InvertedIndex) AddDocument(docID uint64, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Tokenize the document
	tokens := idx.tokenizer.Tokenize(text)
	if len(tokens) == 0 {
		return
	}

	// Track term positions for this document
	termPositions := make(map[string][]int)
	for _, token := range tokens {
		termPositions[token.Term] = append(termPositions[token.Term], token.Position)
	}

	// Update the index
	for term, positions := range termPositions {
		posting := Posting{
			DocID:     docID,
			Positions: positions,
			Frequency: len(positions),
		}

		// Insert into postings list, maintaining sorted order
		postings := idx.index[term]
		i := sort.Search(len(postings), func(i int) bool { return postings[i].DocID >= docID })

		if i < len(postings) && postings[i].DocID == docID {
			// Update existing posting
			postings[i] = posting
		} else {
			// Insert new posting
			postings = append(postings, Posting{})
			copy(postings[i+1:], postings[i:])
			postings[i] = posting
		}
		idx.index[term] = postings
	}

	// Update document length
	idx.docLength[docID] = len(tokens)
	idx.docCount++
}

// RemoveDocument removes a document from the index.
func (idx *InvertedIndex) RemoveDocument(docID uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for term, postings := range idx.index {
		// Find and remove the posting
		i := sort.Search(len(postings), func(i int) bool { return postings[i].DocID >= docID })
		if i < len(postings) && postings[i].DocID == docID {
			postings = append(postings[:i], postings[i+1:]...)
			if len(postings) == 0 {
				delete(idx.index, term)
			} else {
				idx.index[term] = postings
			}
		}
	}

	delete(idx.docLength, docID)
	if idx.docCount > 0 {
		idx.docCount--
	}
}

// UpdateDocument updates a document in the index.
func (idx *InvertedIndex) UpdateDocument(docID uint64, text string) {
	idx.RemoveDocument(docID)
	idx.AddDocument(docID, text)
}

// Search searches for documents containing the given terms.
// Returns postings with doc IDs that contain all terms (AND search).
func (idx *InvertedIndex) Search(terms []string) PostingsList {
	if len(terms) == 0 {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Get postings for each term
	var result PostingsList
	for i, term := range terms {
		postings, exists := idx.index[term]
		if !exists {
			return nil // Term not found, no results
		}

		if i == 0 {
			// Copy first postings list
			result = make(PostingsList, len(postings))
			copy(result, postings)
		} else {
			// Intersect with previous results
			result = intersectPostings(result, postings)
			if len(result) == 0 {
				return nil
			}
		}
	}

	return result
}

// SearchAny searches for documents containing any of the given terms (OR search).
func (idx *InvertedIndex) SearchAny(terms []string) PostingsList {
	if len(terms) == 0 {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Collect all doc IDs from all terms
	docSet := make(map[uint64]Posting)
	for _, term := range terms {
		postings, exists := idx.index[term]
		if !exists {
			continue
		}
		for _, p := range postings {
			if existing, ok := docSet[p.DocID]; !ok {
				docSet[p.DocID] = p
			} else {
				// Merge positions and frequencies
				existing.Positions = append(existing.Positions, p.Positions...)
				existing.Frequency += p.Frequency
				docSet[p.DocID] = existing
			}
		}
	}

	// Convert to sorted postings list
	result := make(PostingsList, 0, len(docSet))
	for _, p := range docSet {
		result = append(result, p)
	}
	sort.Sort(result)

	return result
}

// GetPostings returns the postings list for a single term.
func (idx *InvertedIndex) GetPostings(term string) PostingsList {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.index[term]
}

// DocumentFrequency returns the number of documents containing the term.
func (idx *InvertedIndex) DocumentFrequency(term string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.index[term])
}

// TotalDocuments returns the total number of indexed documents.
func (idx *InvertedIndex) TotalDocuments() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docCount
}

// DocumentLength returns the length of a document (in tokens).
func (idx *InvertedIndex) DocumentLength(docID uint64) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docLength[docID]
}

// AverageDocumentLength returns the average document length.
func (idx *InvertedIndex) AverageDocumentLength() float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.docCount == 0 {
		return 0
	}

	var total int
	for _, length := range idx.docLength {
		total += length
	}

	return float64(total) / float64(idx.docCount)
}

// intersectPostings returns the intersection of two postings lists.
func intersectPostings(a, b PostingsList) PostingsList {
	var result PostingsList
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i].DocID == b[j].DocID {
			// Merge postings
			result = append(result, Posting{
				DocID:     a[i].DocID,
				Positions: append(a[i].Positions, b[i].Positions...),
				Frequency: a[i].Frequency + b[i].Frequency,
			})
			i++
			j++
		} else if a[i].DocID < b[j].DocID {
			i++
		} else {
			j++
		}
	}

	return result
}

// Terms returns all terms in the index.
func (idx *InvertedIndex) Terms() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	terms := make([]string, 0, len(idx.index))
	for term := range idx.index {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}