package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// executeFile executes SQL statements from a file.
func executeFile(filename string, showProgress bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Parse SQL statements from file
	statements, err := parseSQLFile(file)
	if err != nil {
		return fmt.Errorf("failed to parse file: %w", err)
	}

	if len(statements) == 0 {
		fmt.Println("No SQL statements found in file.")
		return nil
	}

	fmt.Printf("Executing %d statement(s) from %s\n\n", len(statements), filename)

	var successCount, errorCount int
	start := time.Now()

	for i, stmt := range statements {
		if showProgress {
			// Show progress for each statement
			preview := stmt
			if len(preview) > 60 {
				preview = preview[:60] + "..."
			}
			fmt.Printf("[%d/%d] %s\n", i+1, len(statements), preview)
		}

		stmtStart := time.Now()
		err := executeSQL(stmt)
		if err != nil {
			errorCount++
			fmt.Printf("Error: %v\n", err)
			// Continue with next statement instead of stopping
		} else {
			successCount++
		}

		if timing || showProgress {
			fmt.Printf("  Time: %v\n\n", time.Since(stmtStart))
		}
	}

	// Summary
	fmt.Println("================================")
	fmt.Printf("Execution Summary:\n")
	fmt.Printf("  Total:     %d statement(s)\n", len(statements))
	fmt.Printf("  Succeeded: %d\n", successCount)
	fmt.Printf("  Failed:    %d\n", errorCount)
	fmt.Printf("  Duration:  %v\n", time.Since(start))

	if errorCount > 0 {
		return fmt.Errorf("%d statement(s) failed", errorCount)
	}

	return nil
}

// parseSQLFile parses SQL statements from a reader.
// It handles:
// - Comments (-- and /* */)
// - Semicolon-separated statements
// - String literals (preserves semicolons inside strings)
func parseSQLFile(reader io.Reader) ([]string, error) {
	var statements []string
	var currentStmt strings.Builder
	var inString bool
	var stringChar rune
	var inComment bool
	var inBlockComment bool

	scanner := bufio.NewReader(reader)
	lineNum := 0

	for {
		line, err := scanner.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read error at line %d: %w", lineNum, err)
		}

		lineNum++
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines when not in a statement
		if trimmedLine == "" && currentStmt.Len() == 0 {
			if err == io.EOF {
				break
			}
			continue
		}

		// Process character by character
		runes := []rune(line)
		i := 0

		for i < len(runes) {
			ch := runes[i]

			// Handle block comment /* */
			if inBlockComment {
				if ch == '*' && i+1 < len(runes) && runes[i+1] == '/' {
					inBlockComment = false
					i += 2
					continue
				}
				i++
				continue
			}

			// Handle line comment --
			if inComment {
				if ch == '\n' {
					inComment = false
				}
				i++
				continue
			}

			// Check for comment start
			if !inString {
				// Line comment --
				if ch == '-' && i+1 < len(runes) && runes[i+1] == '-' {
					inComment = true
					i += 2
					continue
				}

				// Block comment /*
				if ch == '/' && i+1 < len(runes) && runes[i+1] == '*' {
					inBlockComment = true
					i += 2
					continue
				}
			}

			// Handle string literals
			if !inComment && !inBlockComment {
				if !inString && (ch == '\'' || ch == '"') {
					inString = true
					stringChar = ch
					currentStmt.WriteRune(ch)
				} else if inString && ch == stringChar {
					// Check for escaped quote ('' or "")
					if i+1 < len(runes) && runes[i+1] == stringChar {
						currentStmt.WriteRune(ch)
						currentStmt.WriteRune(stringChar)
						i += 2
						continue
					}
					// End of string
					inString = false
					currentStmt.WriteRune(ch)
				} else if inString && ch == '\\' && i+1 < len(runes) {
					// Escape sequence
					currentStmt.WriteRune(ch)
					currentStmt.WriteRune(runes[i+1])
					i += 2
					continue
				} else if !inString && ch == ';' {
					// End of statement
					stmt := strings.TrimSpace(currentStmt.String())
					if stmt != "" {
						statements = append(statements, stmt)
					}
					currentStmt.Reset()
				} else {
					currentStmt.WriteRune(ch)
				}
			}

			i++
		}

		if err == io.EOF {
			break
		}
	}

	// Handle last statement without semicolon
	stmt := strings.TrimSpace(currentStmt.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements, nil
}
