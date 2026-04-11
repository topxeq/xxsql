package main

import (
	"strings"
)

// completer provides tab completion for the CLI.
type completer struct{}

// Do implements readline.AutoCompleter.
func (c *completer) Do(line []rune, pos int) (newLine [][]rune, length int) {
	// Get the word being typed
	lineStr := string(line[:pos])
	words := strings.Fields(lineStr)
	if len(words) == 0 {
		return nil, 0
	}

	// Get the last word (partial)
	lastWord := words[len(words)-1]
	if len(words) == 1 {
		// First word - complete SQL keywords or meta commands
		return c.completeFirstWord(lastWord)
	}

	// Complete based on context
	return c.completeContext(words, lastWord)
}

// completeFirstWord completes the first word (SQL keywords or meta commands).
func (c *completer) completeFirstWord(partial string) ([][]rune, int) {
	var completions []string

	// SQL keywords
	keywords := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
		"SHOW", "DESCRIBE", "USE", "GRANT", "REVOKE", "BEGIN", "COMMIT", "ROLLBACK",
		"BACKUP", "RESTORE",
	}

	for _, kw := range keywords {
		if strings.HasPrefix(kw, strings.ToUpper(partial)) {
			completions = append(completions, kw)
		}
	}

	// Meta commands
	metaCommands := []string{
		"\\d", "\\l", "\\u", "\\h", "\\q", "\\c", "\\v", "\\conninfo",
		"\\timing", "\\g", "\\j", "\\t", "\\format",
	}

	for _, cmd := range metaCommands {
		if strings.HasPrefix(cmd, partial) {
			completions = append(completions, cmd)
		}
	}

	// Built-in commands
	builtins := []string{
		"help", "quit", "exit", "clear", "version",
	}

	for _, cmd := range builtins {
		if strings.HasPrefix(cmd, strings.ToLower(partial)) {
			completions = append(completions, cmd)
		}
	}

	if len(completions) == 0 {
		return nil, 0
	}

	// Convert to [][]rune
	result := make([][]rune, len(completions))
	for i, comp := range completions {
		result[i] = []rune(comp[len(partial):])
	}

	return result, len(partial)
}

// completeContext completes based on the context of previous words.
func (c *completer) completeContext(words []string, partial string) ([][]rune, int) {
	prevWord := strings.ToUpper(words[len(words)-2])

	switch prevWord {
	case "FROM", "JOIN", "INTO", "TABLE":
		// Table name completion - could query the database
		// For now, return empty
		return nil, 0

	case "SELECT":
		// Column name completion
		return nil, 0

	case "WHERE":
		// Column name completion
		return nil, 0
	}

	// Complete SQL keywords in context
	contextKeywords := []string{
		"WHERE", "AND", "OR", "ORDER", "BY", "GROUP", "HAVING",
		"LIMIT", "OFFSET", "ASC", "DESC", "NULL", "NOT", "IN",
		"LIKE", "BETWEEN", "IS", "AS", "ON", "LEFT", "RIGHT",
		"INNER", "OUTER", "CROSS", "FULL", "UNION", "ALL",
	}

	var completions []string
	for _, kw := range contextKeywords {
		if strings.HasPrefix(kw, strings.ToUpper(partial)) {
			completions = append(completions, kw)
		}
	}

	if len(completions) == 0 {
		return nil, 0
	}

	result := make([][]rune, len(completions))
	for i, comp := range completions {
		result[i] = []rune(comp[len(partial):])
	}

	return result, len(partial)
}
