// Package fts provides full-text search functionality for XxSQL.
package fts

import (
	"math"
)

// Ranker calculates relevance scores for search results.
type Ranker interface {
	// Score calculates a relevance score for a posting.
	Score(posting Posting, index *InvertedIndex, queryTerms []string) float64
}

// BM25Ranker implements the BM25 ranking algorithm.
// BM25 is the default ranking algorithm used by many search engines.
type BM25Ranker struct {
	// K1 controls term saturation (typically 1.2-2.0)
	K1 float64
	// B controls length normalization (typically 0.75)
	B float64
}

// NewBM25Ranker creates a new BM25 ranker with default parameters.
func NewBM25Ranker() *BM25Ranker {
	return &BM25Ranker{
		K1: 1.2,
		B:  0.75,
	}
}

// Score calculates the BM25 score for a document.
func (r *BM25Ranker) Score(posting Posting, index *InvertedIndex, queryTerms []string) float64 {
	N := float64(index.TotalDocuments())
	avgDL := index.AverageDocumentLength()
	docLen := float64(index.DocumentLength(posting.DocID))

	var score float64
	for _, term := range queryTerms {
		// Document frequency for this term
		df := float64(index.DocumentFrequency(term))
		if df == 0 {
			continue
		}

		// IDF (Inverse Document Frequency)
		idf := math.Log((N - df + 0.5) / (df + 0.5))
		if idf < 0 {
			idf = 0
		}

		// TF (Term Frequency) in this document
		tf := float64(posting.Frequency)

		// BM25 formula
		numerator := tf * (r.K1 + 1)
		denominator := tf + r.K1*(1-r.B+r.B*(docLen/avgDL))

		score += idf * (numerator / denominator)
	}

	return score
}

// RankedResult represents a search result with its score.
type RankedResult struct {
	DocID uint64
	Score float64
}

// RankedResults is a list of ranked results, sorted by score descending.
type RankedResults []RankedResult

// Len implements sort.Interface.
func (r RankedResults) Len() int { return len(r) }

// Less implements sort.Interface (descending order by score).
func (r RankedResults) Less(i, j int) bool { return r[i].Score > r[j].Score }

// Swap implements sort.Interface.
func (r RankedResults) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

// TFIDFRanker implements TF-IDF ranking.
type TFIDFRanker struct{}

// NewTFIDFRanker creates a new TF-IDF ranker.
func NewTFIDFRanker() *TFIDFRanker {
	return &TFIDFRanker{}
}

// Score calculates the TF-IDF score for a document.
func (r *TFIDFRanker) Score(posting Posting, index *InvertedIndex, queryTerms []string) float64 {
	N := float64(index.TotalDocuments())
	docLen := float64(index.DocumentLength(posting.DocID))

	var score float64
	for _, term := range queryTerms {
		df := float64(index.DocumentFrequency(term))
		if df == 0 {
			continue
		}

		// IDF
		idf := math.Log(N / df)

		// Normalized TF
		tf := float64(posting.Frequency) / docLen

		score += tf * idf
	}

	return score
}

// SimpleRanker just uses term frequency.
type SimpleRanker struct{}

// NewSimpleRanker creates a simple ranker based on term frequency.
func NewSimpleRanker() *SimpleRanker {
	return &SimpleRanker{}
}

// Score returns the sum of term frequencies.
func (r *SimpleRanker) Score(posting Posting, index *InvertedIndex, queryTerms []string) float64 {
	return float64(posting.Frequency)
}