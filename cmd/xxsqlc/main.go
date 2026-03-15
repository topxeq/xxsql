// Package main provides the XxSql client entry point.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.1"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// Command-line flags
var (
	flagHost     = flag.String("host", "localhost", "Server host")
	flagPort     = flag.Int("port", 9527, "Server port")
	flagUser     = flag.String("user", "", "Username")
	flagPassword = flag.String("password", "", "Password")
	flagDatabase = flag.String("database", "", "Database name")
	flagVersion  = flag.Bool("version", false, "Print version information")
	flagCommand  = flag.String("command", "", "Execute command and exit")
	flagQuiet    = flag.Bool("quiet", false, "Suppress welcome message")
)

func main() {
	flag.Parse()

	// Handle version flag
	if *flagVersion {
		fmt.Printf("XxSql Client v%s\n", Version)
		fmt.Printf("Git Commit: %s\n", GitCommit)
		fmt.Printf("Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	// Handle single command mode
	if *flagCommand != "" {
		if err := executeCommand(*flagCommand); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Start interactive REPL
	if !*flagQuiet {
		printWelcome()
	}
	startREPL()
}

// printWelcome prints the welcome message.
func printWelcome() {
	fmt.Println()
	fmt.Println("  ╔═══════════════════════════════════════╗")
	fmt.Println("  ║         XxSql Interactive Client      ║")
	fmt.Println("  ╚═══════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Version: %s\n", Version)
	fmt.Printf("  Connected to: %s:%d\n", *flagHost, *flagPort)
	if *flagDatabase != "" {
		fmt.Printf("  Database: %s\n", *flagDatabase)
	}
	fmt.Println()
	fmt.Println("  Type 'help' for commands, 'quit' to exit.")
	fmt.Println()
}

// startREPL starts the interactive read-eval-print loop.
func startREPL() {
	reader := bufio.NewReader(os.Stdin)

	for {
		// Print prompt
		prompt := getPrompt()
		fmt.Print(prompt)

		// Read input
		input, err := reader.ReadString('\n')
		if err != nil {
			// Handle EOF (Ctrl+D)
			fmt.Println("\nBye!")
			return
		}

		// Trim whitespace
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Handle meta commands
		if strings.HasPrefix(input, "\\") {
			if err := handleMetaCommand(input); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
			continue
		}

		// Handle built-in commands
		switch strings.ToLower(input) {
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

		// Execute SQL command (will be implemented in later phases)
		if err := executeCommand(input); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}

// getPrompt returns the current prompt string.
func getPrompt() string {
	if *flagDatabase != "" {
		return fmt.Sprintf("%s> ", *flagDatabase)
	}
	return "xxsql> "
}

// handleMetaCommand handles backslash meta commands.
func handleMetaCommand(cmd string) error {
	cmd = strings.ToLower(cmd)

	switch {
	case cmd == "\\d" || strings.HasPrefix(cmd, "\\d "):
		// Describe tables (Phase 3+)
		fmt.Println("Table listing not yet implemented")

	case cmd == "\\l":
		// List databases (Phase 3+)
		fmt.Println("Database listing not yet implemented")

	case cmd == "\\u" || strings.HasPrefix(cmd, "\\u "):
		// Use database (Phase 3+)
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			return fmt.Errorf("usage: \\u <database>")
		}
		*flagDatabase = parts[1]
		fmt.Printf("Using database: %s\n", *flagDatabase)

	case cmd == "\\conninfo":
		// Show connection info
		fmt.Printf("Host: %s\n", *flagHost)
		fmt.Printf("Port: %d\n", *flagPort)
		fmt.Printf("User: %s\n", *flagUser)
		fmt.Printf("Database: %s\n", *flagDatabase)

	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}

	return nil
}

// executeCommand executes a SQL command.
func executeCommand(cmd string) error {
	// Connection and execution will be implemented in Phase 2+
	// For now, just acknowledge the command
	fmt.Printf("Executing: %s\n", cmd)
	fmt.Println("(Command execution not yet implemented)")
	return nil
}

// printHelp prints the help message.
func printHelp() {
	fmt.Println()
	fmt.Println("  Available commands:")
	fmt.Println()
	fmt.Println("  SQL Commands:")
	fmt.Println("    SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.")
	fmt.Println()
	fmt.Println("  Meta Commands:")
	fmt.Println("    \\h, \\?      Show this help")
	fmt.Println("    \\q          Quit")
	fmt.Println("    \\c          Clear screen")
	fmt.Println("    \\v          Show version")
	fmt.Println("    \\l          List databases")
	fmt.Println("    \\d [table]  Describe table or list tables")
	fmt.Println("    \\u <db>     Use database")
	fmt.Println("    \\conninfo   Show connection info")
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