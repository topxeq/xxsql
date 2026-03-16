package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
)

// startREPL starts the interactive read-eval-print loop.
func startREPL() {
	// Create readline instance
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          getPrompt(),
		HistoryFile:     getHistoryPath(),
		HistoryLimit:    1000,
		AutoComplete:    &completer{},
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Fprintf(rl, "Error creating readline: %v\n", err)
		return
	}
	defer rl.Close()

	var queryBuf bytes.Buffer
	var inMultiLine bool

	for {
		// Set prompt based on state
		if inMultiLine {
			rl.SetPrompt("    -> ")
		} else {
			rl.SetPrompt(getPrompt())
		}

		// Read input
		line, err := rl.Readline()
		if err != nil {
			// Handle EOF or interrupt
			break
		}

		// Trim trailing whitespace
		line = strings.TrimSpace(line)

		// Skip empty lines
		if line == "" {
			if !inMultiLine {
				continue
			}
			queryBuf.WriteByte('\n')
			continue
		}

		// Handle clear command in multi-line mode
		if inMultiLine && line == "\\c" {
			queryBuf.Reset()
			inMultiLine = false
			fmt.Println("Query cleared.")
			continue
		}

		// Handle meta commands (only when not in multi-line)
		if !inMultiLine && strings.HasPrefix(line, "\\") {
			if err := handleMetaCommand(line); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
			continue
		}

		// Handle built-in commands (only when not in multi-line)
		if !inMultiLine {
			switch strings.ToLower(line) {
			case "quit", "exit", "\\q":
				fmt.Println("Bye!")
				return
			case "help", "\\h", "\\?":
				printHelp()
				continue
			case "clear", "\\c":
				clearScreen()
				continue
			case "version", "\\v":
				fmt.Printf("XxSql Client v%s\n", Version)
				continue
			}
		}

		// Add to query buffer
		if queryBuf.Len() > 0 {
			queryBuf.WriteByte(' ')
		}
		queryBuf.WriteString(line)

		// Check if query is complete (ends with ;)
		if strings.HasSuffix(line, ";") {
			// Execute the query
			query := queryBuf.String()
			queryBuf.Reset()
			inMultiLine = false

			// Remove trailing semicolon for cleaner display
			displayQuery := strings.TrimSuffix(query, ";")

			start := time.Now()
			if err := executeSQL(displayQuery); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
			if timing {
				elapsed := time.Since(start)
				fmt.Printf("Time: %v\n", elapsed)
			}
			fmt.Println()
		} else {
			// Multi-line mode
			inMultiLine = true
		}
	}

	fmt.Println("Bye!")
}

// handleMetaCommand handles backslash meta commands.
func handleMetaCommand(cmd string) error {
	cmd = strings.ToLower(cmd)
	args := strings.Fields(cmd)

	switch {
	case cmd == "\\d" || strings.HasPrefix(cmd, "\\d "):
		// Describe tables
		if len(args) == 1 {
			// List all tables
			return listTables()
		}
		// Describe specific table
		return describeTable(args[1])

	case cmd == "\\l":
		// List databases - not applicable for single-db system
		fmt.Println("Databases: (current instance)")
		fmt.Printf("  %s\n", dbName)

	case cmd == "\\u" || strings.HasPrefix(cmd, "\\u "):
		// Use database
		if len(args) < 2 {
			return fmt.Errorf("usage: \\u <database>")
		}
		return useDatabase(args[1])

	case cmd == "\\conninfo":
		// Show connection info
		fmt.Printf("Host: %s\n", *flagHost)
		fmt.Printf("Port: %d\n", *flagPort)
		fmt.Printf("User: %s\n", *flagUser)
		fmt.Printf("Database: %s\n", dbName)

	case cmd == "\\timing":
		// Toggle timing
		timing = !timing
		if timing {
			fmt.Println("Timing is on.")
		} else {
			fmt.Println("Timing is off.")
		}

	case cmd == "\\g", cmd == "\\vertical":
		// Vertical format for next query
		outFmt = FormatVertical
		fmt.Println("Output format: vertical")

	case cmd == "\\j", cmd == "\\json":
		// JSON format
		outFmt = FormatJSON
		fmt.Println("Output format: json")

	case cmd == "\\t", cmd == "\\tsv":
		// TSV format
		outFmt = FormatTSV
		fmt.Println("Output format: tsv")

	case cmd == "\\table":
		// Table format (default)
		outFmt = FormatTable
		fmt.Println("Output format: table")

	case cmd == "\\format":
		// Show current format
		fmt.Printf("Output format: %s\n", outFmt)

	case cmd == "\\h", cmd == "\\?":
		printHelp()

	case cmd == "\\q":
		fmt.Println("Bye!")
		os.Exit(0)

	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}

	return nil
}

// getPrompt returns the current prompt string.
func getPrompt() string {
	if dbName != "" {
		return fmt.Sprintf("%s> ", dbName)
	}
	return "xxsql> "
}

// printHelp prints the help message.
func printHelp() {
	fmt.Println()
	fmt.Println("  SQL Commands:")
	fmt.Println("    Enter SQL queries ending with ';' to execute")
	fmt.Println("    Multi-line queries are supported")
	fmt.Println()
	fmt.Println("  Meta Commands:")
	fmt.Println("    \\h, \\?      Show this help")
	fmt.Println("    \\q          Quit")
	fmt.Println("    \\c          Clear screen / Clear current query")
	fmt.Println("    \\v          Show version")
	fmt.Println("    \\l          List databases")
	fmt.Println("    \\d [table]  Describe table or list tables")
	fmt.Println("    \\u <db>     Use database")
	fmt.Println("    \\conninfo   Show connection info")
	fmt.Println("    \\timing     Toggle query timing")
	fmt.Println()
	fmt.Println("  Output Format:")
	fmt.Println("    \\table      Table format (default)")
	fmt.Println("    \\g, \\vertical  Vertical format")
	fmt.Println("    \\j, \\json   JSON format")
	fmt.Println("    \\t, \\tsv    Tab-separated values")
	fmt.Println()
	fmt.Println("  Built-in Commands:")
	fmt.Println("    help        Show this help")
	fmt.Println("    quit        Quit")
	fmt.Println("    clear       Clear screen")
	fmt.Println("    version     Show version")
	fmt.Println()
}

// clearScreen clears the terminal screen.
func clearScreen() {
	fmt.Print("\033[2J\033[H")
}
