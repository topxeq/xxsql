package integration

import (
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/server"
	"github.com/topxeq/xxsql/internal/storage"
	_ "github.com/topxeq/xxsql/pkg/xxsql"
)

// TestClientServerIntegration tests the full client-server flow
func TestClientServerIntegration_BasicFlow(t *testing.T) {
	tmpDir := t.TempDir()

	// Find available ports
	privatePort := findAvailablePort(t)
	mysqlPort := findAvailablePort(t)

	t.Logf("Using private port: %d, MySQL port: %d", privatePort, mysqlPort)

	// Create and start server
	cfg := &config.Config{
		Server: config.ServerConfig{
			DataDir: tmpDir,
		},
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: privatePort,
			MySQLPort:   mysqlPort,
			HTTPPort:    0,
		},
		Connection: config.ConnectionConfig{
			MaxConnections: 10,
			WaitTimeout:    30,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
		Log: config.LogConfig{
			Level: "DEBUG",
		},
	}

	logger := log.NewLogger(log.WithLevel(log.DEBUG))
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	srv := server.New(cfg, logger, engine)
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	// Wait for server to be ready
	time.Sleep(500 * time.Millisecond)

	// Connect using the driver
	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb", mysqlPort)
	t.Logf("Connecting with DSN: %s", dsn)

	db, err := sql.Open("xxsql", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set connection timeout
	db.SetConnMaxLifetime(10 * time.Second)

	// Test connection with timeout
	done := make(chan error, 1)
	go func() {
		done <- db.Ping()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Failed to ping database: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Ping timed out after 10 seconds")
	}

	t.Log("Successfully connected to server")

	// Create table
	_, err = db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query data
	rows, err := db.Query("SELECT id, name, age FROM users")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, age int
		var name string
		if err := rows.Scan(&id, &name, &age); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		count++
		t.Logf("Row: id=%d, name=%s, age=%d", id, name, age)
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}
}

func TestClientServerIntegration_CRUD(t *testing.T) {
	tmpDir := t.TempDir()
	privatePort := findAvailablePort(t)
	mysqlPort := findAvailablePort(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			DataDir: tmpDir,
		},
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: privatePort,
			MySQLPort:   mysqlPort,
			HTTPPort:    0,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
		Log: config.LogConfig{
			Level: "ERROR",
		},
	}

	logger := log.NewLogger(log.WithLevel(log.ERROR))
	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	srv := server.New(cfg, logger, engine)
	srv.Start()
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create
	db.Exec("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price INT)")

	// Insert
	db.Exec("INSERT INTO products VALUES (1, 'Widget', 100)")
	db.Exec("INSERT INTO products VALUES (2, 'Gadget', 200)")

	// Read
	row := db.QueryRow("SELECT name, price FROM products WHERE id = 1")
	var name string
	var price int
	if err := row.Scan(&name, &price); err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	if name != "Widget" || price != 100 {
		t.Errorf("Expected Widget/100, got %s/%d", name, price)
	}

	// Update
	_, err := db.Exec("UPDATE products SET price = 150 WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	row = db.QueryRow("SELECT price FROM products WHERE id = 1")
	var newPrice int
	if err := row.Scan(&newPrice); err != nil {
		t.Fatalf("Failed to scan after update: %v", err)
	}
	if newPrice != 150 {
		t.Errorf("Expected price 150, got %d", newPrice)
	}

	// Delete
	_, err = db.Exec("DELETE FROM products WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify delete
	rows, _ := db.Query("SELECT id FROM products")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("Expected 1 row after delete, got %d", count)
	}
}

func TestClientServerIntegration_Aggregates(t *testing.T) {
	tmpDir := t.TempDir()
	privatePort := findAvailablePort(t)
	mysqlPort := findAvailablePort(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			DataDir: tmpDir,
		},
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: privatePort,
			MySQLPort:   mysqlPort,
			HTTPPort:    0,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
		Log: config.LogConfig{
			Level: "ERROR",
		},
	}

	logger := log.NewLogger(log.WithLevel(log.ERROR))
	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	srv := server.New(cfg, logger, engine)
	srv.Start()
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Setup
	_, err := db.Exec("CREATE TABLE sales (id INT PRIMARY KEY, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES (3, 300)")
	if err != nil {
		t.Fatalf("INSERT 3 failed: %v", err)
	}

	// Verify data was inserted
	t.Log("Verifying data with SELECT *")
	rows2, err := db.Query("SELECT * FROM sales")
	if err != nil {
		t.Fatalf("SELECT * failed: %v", err)
	}
	defer rows2.Close()
	rowCount := 0
	for rows2.Next() {
		var id, amount int
		if err := rows2.Scan(&id, &amount); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		t.Logf("Row: id=%d, amount=%d", id, amount)
		rowCount++
	}
	t.Logf("Found %d rows in sales table", rowCount)

	// COUNT
	t.Log("Executing COUNT query")
	rows, err := db.Query("SELECT COUNT(*) FROM sales")
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		if err := rows.Scan(&count); err != nil {
			t.Errorf("COUNT scan failed: %v", err)
		}
		t.Logf("COUNT result: %d", count)
		if count != 3 {
			t.Errorf("COUNT: expected 3, got %d", count)
		}
	} else {
		t.Error("COUNT: no rows returned")
	}

	// SUM
	t.Log("Executing SUM query")
	row := db.QueryRow("SELECT SUM(amount) FROM sales")
	var sum int
	if err := row.Scan(&sum); err != nil {
		t.Errorf("SUM failed: %v", err)
	}
	if sum != 600 {
		t.Errorf("SUM: expected 600, got %d", sum)
	}

	// AVG
	row = db.QueryRow("SELECT AVG(amount) FROM sales")
	var avg float64
	if err := row.Scan(&avg); err != nil {
		t.Errorf("AVG failed: %v", err)
	}
	if avg != 200 {
		t.Errorf("AVG: expected 200, got %f", avg)
	}

	// MAX
	row = db.QueryRow("SELECT MAX(amount) FROM sales")
	var max int
	if err := row.Scan(&max); err != nil {
		t.Errorf("MAX failed: %v", err)
	}
	if max != 300 {
		t.Errorf("MAX: expected 300, got %d", max)
	}

	// MIN
	row = db.QueryRow("SELECT MIN(amount) FROM sales")
	var min int
	if err := row.Scan(&min); err != nil {
		t.Errorf("MIN failed: %v", err)
	}
	if min != 100 {
		t.Errorf("MIN: expected 100, got %d", min)
	}
}

func TestClientServerIntegration_WhereClause(t *testing.T) {
	tmpDir := t.TempDir()
	privatePort := findAvailablePort(t)
	mysqlPort := findAvailablePort(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			DataDir: tmpDir,
		},
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: privatePort,
			MySQLPort:   mysqlPort,
			HTTPPort:    0,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
		Log: config.LogConfig{
			Level: "ERROR",
		},
	}

	logger := log.NewLogger(log.WithLevel(log.ERROR))
	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	srv := server.New(cfg, logger, engine)
	srv.Start()
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Setup
	db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
	db.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
	db.Exec("INSERT INTO users VALUES (2, 'Bob', 25)")
	db.Exec("INSERT INTO users VALUES (3, 'Charlie', 35)")

	// Test various WHERE conditions
	tests := []struct {
		query    string
		expected int
	}{
		{"SELECT * FROM users WHERE id = 1", 1},
		{"SELECT * FROM users WHERE age > 30", 1},
		{"SELECT * FROM users WHERE age >= 30", 2},
		{"SELECT * FROM users WHERE age < 30", 1},
		{"SELECT * FROM users WHERE age <= 30", 2},
		{"SELECT * FROM users WHERE name = 'Bob'", 1},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			if count != tt.expected {
				t.Errorf("Expected %d rows, got %d", tt.expected, count)
			}
		})
	}
}

func TestClientServerIntegration_ShowTables(t *testing.T) {
	tmpDir := t.TempDir()
	privatePort := findAvailablePort(t)
	mysqlPort := findAvailablePort(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			DataDir: tmpDir,
		},
		Network: config.NetworkConfig{
			Bind:        "127.0.0.1",
			PrivatePort: privatePort,
			MySQLPort:   mysqlPort,
			HTTPPort:    0,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
		Log: config.LogConfig{
			Level: "ERROR",
		},
	}

	logger := log.NewLogger(log.WithLevel(log.ERROR))
	engine := storage.NewEngine(tmpDir)
	engine.Open()
	defer engine.Close()

	srv := server.New(cfg, logger, engine)
	srv.Start()
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create tables
	db.Exec("CREATE TABLE table1 (id INT)")
	db.Exec("CREATE TABLE table2 (id INT)")

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Errorf("SHOW TABLES failed: %v", err)
		return
	}
	defer rows.Close()

	tableCount := 0
	for rows.Next() {
		tableCount++
	}
	if tableCount != 2 {
		t.Errorf("Expected 2 tables, got %d", tableCount)
	}
}

// Helper function to find an available port
func findAvailablePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to resolve addr: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}