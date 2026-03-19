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

	"github.com/topxeq/xxsql/pkg/xxsql"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.3"
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
	flagFile     = flag.String("f", "", "Execute SQL from file and exit")
	flagQuiet    = flag.Bool("q", false, "Suppress welcome message")
	flagFormat   = flag.String("format", "table", "Output format: table, vertical, json, tsv")
	flagDSN      = flag.String("dsn", "", "Connection string (URL format: xxsql://user:pass@host:port/db)")
	flagProgress = flag.Bool("progress", false, "Show progress when executing SQL file")
)

// Global state
var (
	db     *sql.DB
	dbName string
	timing bool
	outFmt OutputFormat
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

	// Connect to database
	var dsn string
	if *flagDSN != "" {
		// Use DSN directly if provided
		dsn = *flagDSN
	} else {
		dsn = buildDSN()
	}

	// Parse DSN to get connection info
	cfg, err := xxsql.ParseDSN(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid DSN: %v\n", err)
		os.Exit(1)
	}
	dbName = cfg.DBName

	var dbErr error
	db, dbErr = sql.Open("xxsql", dsn)
	if dbErr != nil {
		fmt.Fprintf(os.Stderr, "Connection failed: %v\n", dbErr)
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

	// Handle file execution mode
	if *flagFile != "" {
		if err := executeFile(*flagFile, *flagProgress); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Setup signal handling
	setupSignals()

	// Start interactive REPL
	if !*flagQuiet {
		printWelcomeWithConfig(cfg)
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

// printWelcomeWithConfig prints the welcome message using parsed config.
func printWelcomeWithConfig(cfg *xxsql.Config) {
	fmt.Println()
	fmt.Println("  ╔═══════════════════════════════════════╗")
	fmt.Println("  ║         XxSql Interactive Client      ║")
	fmt.Println("  ╚═══════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Version: %s\n", Version)
	fmt.Printf("  Connected to: %s\n", cfg.Addr)
	if cfg.DBName != "" {
		fmt.Printf("  Database: %s\n", cfg.DBName)
	}
	if cfg.User != "" {
		fmt.Printf("  User: %s\n", cfg.User)
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
