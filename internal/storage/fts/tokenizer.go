// Package fts provides full-text search functionality for XxSQL.
package fts

import (
	"strings"
	"unicode"
)

// Token represents a token in a document.
type Token struct {
	Term     string // The token text (lowercased)
	Position int    // Position in the document (0-indexed)
}

// Tokenizer interface defines how text is tokenized for FTS.
type Tokenizer interface {
	// Tokenize breaks text into tokens.
	Tokenize(text string) []Token
}

// SimpleTokenizer is a basic tokenizer that splits on whitespace and punctuation.
type SimpleTokenizer struct {
	// MinTokenLength is the minimum length for a token to be indexed.
	MinTokenLength int
	// StopWords is a set of words to skip during tokenization.
	StopWords map[string]bool
}

// NewSimpleTokenizer creates a new SimpleTokenizer with default settings.
func NewSimpleTokenizer() *SimpleTokenizer {
	return &SimpleTokenizer{
		MinTokenLength: 1,
		StopWords:      DefaultStopWords(),
	}
}

// DefaultStopWords returns a set of common English stop words.
func DefaultStopWords() map[string]bool {
	return map[string]bool{
		"a": true, "an": true, "and": true, "are": true, "as": true,
		"at": true, "be": true, "by": true, "for": true, "from": true,
		"has": true, "he": true, "in": true, "is": true, "it": true,
		"its": true, "of": true, "on": true, "that": true, "the": true,
		"to": true, "was": true, "were": true, "will": true, "with": true,
	}
}

// Tokenize breaks text into tokens, filtering by length and stop words.
func (t *SimpleTokenizer) Tokenize(text string) []Token {
	var tokens []Token
	position := 0

	// Convert to lowercase for case-insensitive search
	text = strings.ToLower(text)

	var currentToken strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			currentToken.WriteRune(r)
		} else {
			if currentToken.Len() > 0 {
				term := currentToken.String()
				if len(term) >= t.MinTokenLength && !t.StopWords[term] {
					tokens = append(tokens, Token{
						Term:     term,
						Position: position,
					})
					position++
				}
				currentToken.Reset()
			}
		}
	}

	// Handle last token
	if currentToken.Len() > 0 {
		term := currentToken.String()
		if len(term) >= t.MinTokenLength && !t.StopWords[term] {
			tokens = append(tokens, Token{
				Term:     term,
				Position: position,
			})
		}
	}

	return tokens
}

// PorterStemmerTokenizer wraps another tokenizer and applies Porter stemming.
// This is a simplified implementation - for production, use a proper Porter stemmer.
type PorterStemmerTokenizer struct {
	base Tokenizer
}

// NewPorterStemmerTokenizer creates a tokenizer that applies Porter stemming.
func NewPorterStemmerTokenizer(base Tokenizer) *PorterStemmerTokenizer {
	return &PorterStemmerTokenizer{base: base}
}

// Tokenize tokenizes and stems the text.
func (t *PorterStemmerTokenizer) Tokenize(text string) []Token {
	tokens := t.base.Tokenize(text)
	for i := range tokens {
		tokens[i].Term = stem(tokens[i].Term)
	}
	return tokens
}

// stem applies basic Porter stemming rules (simplified version).
func stem(word string) string {
	// Simple stemming rules
	if len(word) <= 3 {
		return word
	}

	// Remove common suffixes
	suffixes := []string{"ation", "ition", "ness", "ment", "able", "ible", "ous", "ive", "ly", "ed", "ing", "es", "s"}
	for _, suffix := range suffixes {
		if strings.HasSuffix(word, suffix) && len(word)-len(suffix) >= 2 {
			word = word[:len(word)-len(suffix)]
			break
		}
	}

	return word
}