package main

import (
	"strings"
	"testing"
)

func TestParseSQLFile(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  []string
		expectErr bool
	}{
		{
			name:     "single statement",
			input:    "SELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "multiple statements",
			input:    "SELECT * FROM users; SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "statement with line comment",
			input:    "-- This is a comment\nSELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "statement with block comment",
			input:    "/* comment */ SELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "multi-line statement",
			input:    "SELECT *\nFROM users\nWHERE id = 1;",
			expected: []string{"SELECT *\nFROM users\nWHERE id = 1"},
		},
		{
			name:     "semicolon in string",
			input:    "INSERT INTO users (name) VALUES ('test;value');",
			expected: []string{"INSERT INTO users (name) VALUES ('test;value')"},
		},
		{
			name:     "semicolon in double quoted string",
			input:    `INSERT INTO users (name) VALUES ("test;value");`,
			expected: []string{`INSERT INTO users (name) VALUES ("test;value")`},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "only comments",
			input:    "-- comment 1\n-- comment 2\n",
			expected: nil,
		},
		{
			name:     "statement without semicolon",
			input:    "SELECT * FROM users",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "escaped single quote",
			input:    "INSERT INTO users (name) VALUES ('it''s test');",
			expected: []string{"INSERT INTO users (name) VALUES ('it''s test')"},
		},
		{
			name: "complex SQL file",
			input: `-- Create table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Insert data
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

/* Multi-line
   comment */
SELECT * FROM users;`,
			expected: []string{
				"CREATE TABLE users (\n    id INT PRIMARY KEY,\n    name VARCHAR(100)\n)",
				"INSERT INTO users VALUES (1, 'Alice')",
				"INSERT INTO users VALUES (2, 'Bob')",
				"SELECT * FROM users",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := parseSQLFile(reader)

			if tt.expectErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d statements, got %d", len(tt.expected), len(result))
				t.Logf("Expected: %v", tt.expected)
				t.Logf("Got: %v", result)
				return
			}

			for i, stmt := range result {
				// Normalize whitespace for comparison
				expected := strings.TrimSpace(tt.expected[i])
				got := strings.TrimSpace(stmt)
				if expected != got {
					t.Errorf("statement %d mismatch:\nexpected: %q\ngot: %q", i, expected, got)
				}
			}
		})
	}
}

func TestParseSQLFileWithSpecialChars(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "string with backslash",
			input:    `INSERT INTO t VALUES ('path\\to\\file');`,
			expected: 1,
		},
		{
			name:     "mixed quotes",
			input:    `INSERT INTO t VALUES ('single', "double");`,
			expected: 1,
		},
		{
			name:     "nested quotes",
			input:    `INSERT INTO t VALUES ('he said "hello"');`,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := parseSQLFile(reader)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(result) != tt.expected {
				t.Errorf("expected %d statements, got %d", tt.expected, len(result))
			}
		})
	}
}
