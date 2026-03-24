// Package main provides the XxSql client entry point.
package main

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/topxeq/xxsql/pkg/xxsql"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.4"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// CLI flags
var (
	flagHost     = flag.String("host", "localhost", "Server host")
	flagPort     = flag.Int("port", 3306, "Server port")
	flagHTTPPort = flag.Int("http-port", 8080, "HTTP API port (for project deployment)")
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
	flagProject  = flag.String("project", "", "Deploy project from specified directory")
	flagProtocol = flag.String("protocol", "", "Protocol to use: mysql or private (auto-detected from port if not specified)")
	flagReset    = flag.Bool("reset", false, "Reset server to initial state (admin only)")
	flagResetForce = flag.Bool("force", false, "Force reset without confirmation prompt")
	flagResetFull = flag.Bool("full", false, "Full reset including user accounts (keeps admin)")
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

	// Handle project deployment mode
	if *flagProject != "" {
		if err := deployProject(*flagProject); err != nil {
			fmt.Fprintf(os.Stderr, "Project deployment failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Handle reset mode
	if *flagReset {
		if err := resetServer(); err != nil {
			fmt.Fprintf(os.Stderr, "Reset failed: %v\n", err)
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

	// Protocol
	protocol := *flagProtocol
	if protocol == "" {
		// Auto-detect: port 3306 -> mysql, port 9527 -> private
		if *flagPort == 3306 {
			protocol = "mysql"
		} else if *flagPort == 9527 {
			protocol = "private"
		}
	}
	if protocol != "" {
		dsn.WriteString("?protocol=")
		dsn.WriteString(protocol)
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

// deployProject deploys a project to the XxSql server
func deployProject(projectPath string) error {
	fmt.Printf("Deploying project from: %s\n", projectPath)

	// Read project.json
	projectFile := filepath.Join(projectPath, "project.json")
	projectData, err := os.ReadFile(projectFile)
	if err != nil {
		return fmt.Errorf("failed to read project.json: %w", err)
	}

	var project struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		Tables  string `json:"tables"`
	}
	if err := json.Unmarshal(projectData, &project); err != nil {
		return fmt.Errorf("failed to parse project.json: %w", err)
	}

	fmt.Printf("Project: %s v%s\n", project.Name, project.Version)

	// Execute setup.sql
	setupFile := filepath.Join(projectPath, "setup.sql")
	if _, err := os.Stat(setupFile); err == nil {
		fmt.Println("Executing setup.sql...")
		if err := executeFile(setupFile, true); err != nil {
			return fmt.Errorf("failed to execute setup.sql: %w", err)
		}
		fmt.Println("setup.sql executed successfully")
	}

	// Upload static files
	staticDir := filepath.Join(projectPath, "static")
	if _, err := os.Stat(staticDir); err == nil {
		fmt.Println("Uploading static files...")
		if err := uploadStaticFiles(staticDir, project.Name); err != nil {
			return fmt.Errorf("failed to upload static files: %w", err)
		}
		fmt.Println("Static files uploaded successfully")
	}

	// Check for index.html in project root (backward compatibility)
	indexFile := filepath.Join(projectPath, "index.html")
	if _, err := os.Stat(indexFile); err == nil {
		fmt.Println("Uploading index.html...")
		if err := uploadFileViaMicroservice(indexFile, project.Name, "index.html"); err != nil {
			return fmt.Errorf("failed to upload index.html: %w", err)
		}
	}

	// Register project
	fmt.Println("Registering project...")
	if err := registerProject(project.Name, project.Version, project.Tables); err != nil {
		fmt.Printf("Warning: failed to register project: %v\n", err)
	}

	fmt.Printf("Project '%s' deployed successfully!\n", project.Name)
	return nil
}

// uploadStaticFiles uploads all files from a directory recursively
func uploadStaticFiles(dir, projectName string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Calculate relative path
		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		return uploadFileViaMicroservice(path, projectName, relPath)
	})
}

// uploadFileViaMicroservice uploads a single file via microservice HTTP endpoint
func uploadFileViaMicroservice(filePath, projectName, relPath string) error {
	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// Encode to base64
	encoded := base64.StdEncoding.EncodeToString(content)

	// Determine file path on server
	serverPath := fmt.Sprintf("projects/%s/%s", projectName, filepath.ToSlash(relPath))

	// Create directory first via microservice
	dirPath := filepath.Dir(serverPath)
	if dirPath != "." && dirPath != "" {
		mkdirPayload := map[string]interface{}{
			"path": dirPath,
		}
		mkdirJSON, _ := json.Marshal(mkdirPayload)
		mkdirURL := fmt.Sprintf("http://%s:%d/ms/_sys_ms/dir/create", *flagHost, httpPort())
		req, _ := http.NewRequest("POST", mkdirURL, bytes.NewReader(mkdirJSON))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("  Warning: failed to create directory %s: %v\n", dirPath, err)
		} else {
			resp.Body.Close()
		}
	}

	// Upload file via microservice
	uploadPayload := map[string]interface{}{
		"path":    serverPath,
		"content": encoded,
		"mode":    "binary",
	}
	uploadJSON, _ := json.Marshal(uploadPayload)
	uploadURL := fmt.Sprintf("http://%s:%d/ms/_sys_ms/file/uploadBinary", *flagHost, httpPort())
	req, _ := http.NewRequest("POST", uploadURL, bytes.NewReader(uploadJSON))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload %s: %w", relPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to upload %s: %s", relPath, string(body))
	}

	fmt.Printf("  Uploaded: %s\n", relPath)
	return nil
}

// httpPort returns the HTTP port based on server configuration
func httpPort() int {
	return *flagHTTPPort
}

// registerProject registers the project in _sys_projects table
func registerProject(name, version, tables string) error {
	sql := fmt.Sprintf("INSERT INTO _sys_projects (name, version, installed_at, tables) VALUES ('%s', '%s', datetime('now'), '%s')",
		name, version, tables)
	_, err := db.Exec(sql)
	return err
}

// resetServer resets the server to its initial state via HTTP API
func resetServer() error {
	// Check for required credentials
	if *flagUser == "" || *flagPassword == "" {
		return fmt.Errorf("reset requires admin credentials: use -u admin -p <password>")
	}

	// Show warning and get confirmation unless --force is specified
	if !*flagResetForce {
		fmt.Println("⚠️  WARNING: This will DELETE ALL DATA and reset the server to initial state!")
		fmt.Println("    - All user tables will be dropped")
		fmt.Println("    - All user data will be lost")
		fmt.Println("    - All projects will be unregistered")
		fmt.Println("    - All uploaded files will be deleted")
		if *flagResetFull {
			fmt.Println("    - All user accounts will be deleted (except admin)")
		}
		fmt.Println()
		fmt.Print("Type 'RESET' to confirm: ")

		var confirmation string
		fmt.Scanln(&confirmation)
		if confirmation != "RESET" {
			fmt.Println("Reset cancelled.")
			return nil
		}
	}

	// Call the reset API
	resetURL := fmt.Sprintf("http://%s:%d/api/admin/reset", *flagHost, *flagHTTPPort)

	payload := map[string]interface{}{
		"confirm": "RESET",
		"full":    *flagResetFull,
	}
	payloadJSON, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", resetURL, bytes.NewReader(payloadJSON))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(*flagUser, *flagPassword)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("authentication failed: invalid credentials")
	}
	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("access denied: admin privileges required")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server error: %s", string(body))
	}

	// Parse and display result
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	fmt.Println("\n✓ Server reset complete!")
	if tables, ok := result["tables_dropped"].([]interface{}); ok {
		fmt.Printf("  Tables dropped: %d\n", len(tables))
	}
	if projects, ok := result["projects_cleared"].(float64); ok {
		fmt.Printf("  Projects cleared: %d\n", int(projects))
	}
	if files, ok := result["files_deleted"].(float64); ok {
		fmt.Printf("  Files deleted: %d\n", int(files))
	}
	if users, ok := result["users_deleted"].(float64); ok {
		fmt.Printf("  Users deleted: %d\n", int(users))
	}
	fmt.Println("\nServer has been reset to initial state.")

	return nil
}
