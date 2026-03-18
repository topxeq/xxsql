// Package main provides the XxSql client entry point.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	_ "github.com/topxeq/xxsql/pkg/xxsql"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.2"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// CLI flags
var (
	flagHost     = flag.String("host", "localhost", "Server host")
	flagPort     = flag.Int("port", 3306, "Server port")
	flagUser     = flag.String("u", "", "Username")
	flagPassword = flag.String("p", "", "Password")
	flagDatabase = flag.String("d", "", "Database name")
	flagVersion  = flag.Bool("version", false, "Print version information")
	flagCommand  = flag.String("e", "", "Execute command and exit")
	flagQuiet    = flag.Bool("q", false, "Suppress welcome message")
	flagFormat   = flag.String("format", "table", "Output format: table, vertical, json, tsv")
)

// Global state
var (
	db       *sql.DB
	dbName   string
	timing   bool
	outFmt   OutputFormat
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

	// Set output format
	outFmt = parseOutputFormat(*flagFormat)
	dbName = *flagDatabase

	// Connect to database
	dsn := buildDSN()
	var err error
	db, err = sql.Open("xxsql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Connection failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "Connection failed: %v\n", err)
		os.Exit(1)
	}

	// Handle single command mode
	if *flagCommand != "" {
		if err := executeSQL(*flagCommand); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Setup signal handling
	setupSignals()

	// Start interactive REPL
	if !*flagQuiet {
		printWelcome()
	}
	startREPL()
}

// buildDSN constructs the DSN from flags.
func buildDSN() string {
	var dsn strings.Builder

	// User
	if *flagUser != "" {
		dsn.WriteString(*flagUser)
	}

	// Password
	if *flagPassword != "" {
		dsn.WriteString(":")
		dsn.WriteString(*flagPassword)
	}

	// Host and port
	if *flagUser != "" || *flagPassword != "" {
		dsn.WriteString("@")
	}
	dsn.WriteString("tcp(")
	dsn.WriteString(*flagHost)
	dsn.WriteString(":")
	dsn.WriteString(fmt.Sprintf("%d", *flagPort))
	dsn.WriteString(")")

	// Database
	dsn.WriteString("/")
	if *flagDatabase != "" {
		dsn.WriteString(*flagDatabase)
	}

	return dsn.String()
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
	if dbName != "" {
		fmt.Printf("  Database: %s\n", dbName)
	}
	fmt.Println()
	fmt.Println("  Type 'help' for commands, 'quit' to exit.")
	fmt.Println("  Type SQL queries ending with ';' to execute.")
	fmt.Println()
}

// setupSignals handles OS signals.
func setupSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\nBye!")
		os.Exit(0)
	}()
}

// getHistoryPath returns the path to the history file.
func getHistoryPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".xxsql_history")
}
