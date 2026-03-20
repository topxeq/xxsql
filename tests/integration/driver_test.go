package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/server"
	"github.com/topxeq/xxsql/internal/storage"
	_ "github.com/topxeq/xxsql/pkg/xxsql"
)

// TestIntegration_PreparedStatement tests prepared statements
func TestIntegration_PreparedStatement(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table
	db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)")

	// Prepare insert statement
	stmt, err := db.Prepare("INSERT INTO users VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute prepared statement
	_, err = stmt.Exec(1, "Alice", 30)
	if err != nil {
		t.Errorf("Failed to execute prepared statement: %v", err)
	}

	_, err = stmt.Exec(2, "Bob", 25)
	if err != nil {
		t.Errorf("Failed to execute prepared statement: %v", err)
	}

	// Verify data
	rows, _ := db.Query("SELECT COUNT(*) FROM users")
	defer rows.Close()
	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}

	// Prepare select statement
	stmt2, err := db.Prepare("SELECT name, age FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare select: %v", err)
	}
	defer stmt2.Close()

	rows2, err := stmt2.Query(1)
	if err != nil {
		t.Errorf("Failed to query with prepared statement: %v", err)
	} else {
		defer rows2.Close()
		if rows2.Next() {
			var name string
			var age int
			rows2.Scan(&name, &age)
			if name != "Alice" || age != 30 {
				t.Errorf("Expected Alice/30, got %s/%d", name, age)
			}
		}
	}
}

// TestIntegration_DataTypes tests various data types
func TestIntegration_DataTypes(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table with various types
	db.Exec("CREATE TABLE types_test (id INT PRIMARY KEY, v_int INT, v_str VARCHAR(100), v_float FLOAT)")

	// Insert with different types
	db.Exec("INSERT INTO types_test VALUES (1, 42, 'hello', 3.14)")
	db.Exec("INSERT INTO types_test VALUES (2, -10, 'world', 2.71)")

	// Query and verify
	rows, err := db.Query("SELECT v_int, v_str, v_float FROM types_test WHERE id = 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var vInt int
		var vStr string
		var vFloat float64
		rows.Scan(&vInt, &vStr, &vFloat)

		if vInt != 42 {
			t.Errorf("Expected v_int=42, got %d", vInt)
		}
		if vStr != "hello" {
			t.Errorf("Expected v_str='hello', got '%s'", vStr)
		}
	}
}

// TestIntegration_ContextTimeout tests context timeout
func TestIntegration_ContextTimeout(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Test with context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := db.PingContext(ctx)
	if err != nil {
		t.Errorf("PingContext failed: %v", err)
	}

	// Test query with context
	db.ExecContext(ctx, "CREATE TABLE test (id INT)")

	_, err = db.ExecContext(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("ExecContext failed: %v", err)
	}

	// Test query with context
	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	if err != nil {
		t.Errorf("QueryContext failed: %v", err)
	}
	rows.Close()
}

// TestIntegration_ConnectionClose tests connection close and reuse
func TestIntegration_ConnectionClose(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)

	// Open and close connection
	db1, _ := sql.Open("xxsql", dsn)
	db1.Ping()
	db1.Close()

	// Open new connection
	db2, _ := sql.Open("xxsql", dsn)
	defer db2.Close()

	err := db2.Ping()
	if err != nil {
		t.Errorf("Failed to ping new connection: %v", err)
	}
}

// TestIntegration_ConcurrentConnections tests concurrent connections
func TestIntegration_ConcurrentConnections(t *testing.T) {
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
		Connection: config.ConnectionConfig{
			MaxConnections: 20,
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)

	// Open multiple connections
	numConns := 10
	dbs := make([]*sql.DB, numConns)

	for i := 0; i < numConns; i++ {
		db, err := sql.Open("xxsql", dsn)
		if err != nil {
			t.Fatalf("Failed to open connection %d: %v", i, err)
		}
		dbs[i] = db
	}

	// Create table first (before concurrent operations)
	dbs[0].Exec("CREATE TABLE test_concurrent (id INT PRIMARY KEY)")

	// Execute queries concurrently
	errCh := make(chan error, numConns)
	for i := 0; i < numConns; i++ {
		go func(idx int) {
			db := dbs[idx]
			_, err := db.Exec(fmt.Sprintf("INSERT INTO test_concurrent VALUES (%d)", idx))
			errCh <- err
		}(i)
	}

	// Collect errors
	for i := 0; i < numConns; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	}

	// Close all connections
	for _, db := range dbs {
		db.Close()
	}
}

// TestIntegration_ErrorHandling tests error handling
func TestIntegration_ErrorHandling(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Test syntax error
	_, err := db.Exec("INVALID SQL SYNTAX")
	if err == nil {
		t.Error("Expected error for invalid SQL syntax")
	}

	// Test table not found
	_, err = db.Query("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	// Test duplicate table
	db.Exec("CREATE TABLE test_dup (id INT)")
	_, err = db.Exec("CREATE TABLE test_dup (id INT)")
	if err == nil {
		t.Error("Expected error for duplicate table")
	}
}

// TestIntegration_MultipleQueries tests multiple sequential queries
func TestIntegration_MultipleQueries(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Execute multiple queries in sequence
	db.Exec("CREATE TABLE test1 (id INT)")
	db.Exec("CREATE TABLE test2 (id INT)")
	db.Exec("CREATE TABLE test3 (id INT)")

	db.Exec("INSERT INTO test1 VALUES (1)")
	db.Exec("INSERT INTO test2 VALUES (2)")
	db.Exec("INSERT INTO test3 VALUES (3)")

	// Query each table
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("test%d", i)
		expectedVal := i

		rows, err := db.Query(fmt.Sprintf("SELECT id FROM %s", tableName))
		if err != nil {
			t.Errorf("Query %s failed: %v", tableName, err)
			continue
		}

		if rows.Next() {
			var val int
			rows.Scan(&val)
			if val != expectedVal {
				t.Errorf("Expected %d from %s, got %d", expectedVal, tableName, val)
			}
		}
		rows.Close()
	}
}

// TestIntegration_LargeResult tests large result sets
func TestIntegration_LargeResult(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table and insert many rows
	db.Exec("CREATE TABLE large_table (id INT PRIMARY KEY, value INT)")

	numRows := 100
	for i := 0; i < numRows; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO large_table VALUES (%d, %d)", i, i*10))
	}

	// Query all rows
	rows, err := db.Query("SELECT id, value FROM large_table ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, value int
		rows.Scan(&id, &value)
		if value != id*10 {
			t.Errorf("Expected value=%d for id=%d, got %d", id*10, id, value)
		}
		count++
	}

	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}
}

// TestIntegration_NullValues tests NULL value handling
func TestIntegration_NullValues(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table with nullable columns
	db.Exec("CREATE TABLE nullable_test (id INT PRIMARY KEY, name VARCHAR(100), age INT)")

	// Insert with NULL values
	db.Exec("INSERT INTO nullable_test VALUES (1, NULL, NULL)")
	db.Exec("INSERT INTO nullable_test VALUES (2, 'Alice', 30)")

	// Query and check NULL handling
	rows, err := db.Query("SELECT id, name, age FROM nullable_test ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// First row should have NULL values
	if rows.Next() {
		var id int
		var name, age sql.NullString
		rows.Scan(&id, &name, &age)

		if id != 1 {
			t.Errorf("Expected id=1, got %d", id)
		}
		if name.Valid {
			t.Errorf("Expected NULL name, got %s", name.String)
		}
	}

	// Second row should have valid values
	if rows.Next() {
		var id int
		var name sql.NullString
		var age sql.NullInt64
		rows.Scan(&id, &name, &age)

		if id != 2 {
			t.Errorf("Expected id=2, got %d", id)
		}
		if !name.Valid || name.String != "Alice" {
			t.Errorf("Expected name='Alice', got %v", name)
		}
	}
}

// TestIntegration_ConnectionString tests various connection string formats
func TestIntegration_ConnectionString(t *testing.T) {
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

	// Test DSN with database
	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, err := sql.Open("xxsql", dsn)
	if err != nil {
		t.Errorf("Failed to open with DSN %s: %v", dsn, err)
	} else {
		err = db.Ping()
		if err != nil {
			t.Errorf("Failed to ping with DSN %s: %v", dsn, err)
		}
		db.Close()
	}
}

// TestIntegration_OrderByLimit tests ORDER BY
func TestIntegration_OrderByLimit(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table and insert data
	db.Exec("CREATE TABLE sort_test (id INT PRIMARY KEY, value INT)")
	for i := 10; i > 0; i-- {
		db.Exec(fmt.Sprintf("INSERT INTO sort_test VALUES (%d, %d)", i, i*10))
	}

	// Test ORDER BY ASC - verify query runs
	rows, err := db.Query("SELECT id FROM sort_test ORDER BY id ASC")
	if err != nil {
		t.Fatalf("ORDER BY ASC query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}

	// Test ORDER BY DESC - verify query runs
	rows2, err := db.Query("SELECT id FROM sort_test ORDER BY id DESC")
	if err != nil {
		t.Fatalf("ORDER BY DESC query failed: %v", err)
	}
	defer rows2.Close()

	count = 0
	for rows2.Next() {
		count++
	}
	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}
}

// TestIntegration_OrderByAlias tests ORDER BY with column aliases
func TestIntegration_OrderByAlias(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table and insert data
	db.Exec("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50), price INT)")
	db.Exec("INSERT INTO items VALUES (1, 'apple', 100)")
	db.Exec("INSERT INTO items VALUES (2, 'banana', 50)")
	db.Exec("INSERT INTO items VALUES (3, 'cherry', 200)")

	// Test ORDER BY with alias
	rows, err := db.Query("SELECT id AS item_id, name AS item_name FROM items ORDER BY item_id DESC")
	if err != nil {
		t.Fatalf("ORDER BY alias query failed: %v", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		ids = append(ids, id)
	}

	// Should be ordered DESC: 3, 2, 1
	expected := []int{3, 2, 1}
	if len(ids) != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), len(ids))
	}
	for i, v := range expected {
		if i < len(ids) && ids[i] != v {
			t.Errorf("Expected id[%d]=%d, got %d", i, v, ids[i])
		}
	}

	// Test ORDER BY with alias ASC
	rows2, err := db.Query("SELECT id AS item_id FROM items ORDER BY item_id ASC")
	if err != nil {
		t.Fatalf("ORDER BY alias ASC query failed: %v", err)
	}
	defer rows2.Close()

	ids = nil
	for rows2.Next() {
		var id int
		if err := rows2.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		ids = append(ids, id)
	}

	// Should be ordered ASC: 1, 2, 3
	expected = []int{1, 2, 3}
	for i, v := range expected {
		if i < len(ids) && ids[i] != v {
			t.Errorf("Expected id[%d]=%d, got %d", i, v, ids[i])
		}
	}
}

// TestIntegration_OrderByExpression tests ORDER BY with expressions
func TestIntegration_OrderByExpression(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table and insert data
	db.Exec("CREATE TABLE products (id INT PRIMARY KEY, price INT, quantity INT)")
	db.Exec("INSERT INTO products VALUES (1, 100, 2)")  // total = 200
	db.Exec("INSERT INTO products VALUES (2, 50, 3)")   // total = 150
	db.Exec("INSERT INTO products VALUES (3, 200, 1)")  // total = 200

	// Test ORDER BY with multiplication expression (columns in SELECT list)
	// Note: For expressions to work, columns must be in SELECT list
	rows, err := db.Query("SELECT id, price, quantity FROM products ORDER BY price * quantity DESC")
	if err != nil {
		t.Fatalf("ORDER BY expression query failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		id, price, quantity int
	}
	for rows.Next() {
		var id, price, quantity int
		if err := rows.Scan(&id, &price, &quantity); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("id=%d, price=%d, quantity=%d, total=%d", id, price, quantity, price*quantity)
		results = append(results, struct{ id, price, quantity int }{id, price, quantity})
	}

	// Expected order by price*quantity DESC: (1, 100, 2)=200, (3, 200, 1)=200, (2, 50, 3)=150
	// IDs 1 and 3 both have total=200, so order between them might vary
	if len(results) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(results))
	}
	// Last item should be id=2 (lowest total = 150)
	if len(results) > 0 && results[len(results)-1].id != 2 {
		t.Errorf("Expected last item to be id=2 (total=150), got id=%d", results[len(results)-1].id)
	}

	// Test ORDER BY with addition expression (columns in SELECT list)
	rows2, err := db.Query("SELECT id, price, quantity FROM products ORDER BY price + quantity ASC")
	if err != nil {
		t.Fatalf("ORDER BY addition expression query failed: %v", err)
	}
	defer rows2.Close()

	type result struct {
		id, price, quantity int
	}
	results2 := []result{}
	for rows2.Next() {
		var id, price, quantity int
		if err := rows2.Scan(&id, &price, &quantity); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("id=%d, price=%d, quantity=%d, sum=%d", id, price, quantity, price+quantity)
		results2 = append(results2, result{id, price, quantity})
	}

	// price+quantity: id=1: 102, id=2: 53, id=3: 201
	// Expected order ASC: 2 (sum=53), 1 (sum=102), 3 (sum=201)
	if len(results2) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(results2))
	}
	if len(results2) >= 1 && results2[0].id != 2 {
		t.Errorf("Expected first id=2 (sum=53), got id=%d", results2[0].id)
	}
	if len(results2) >= 2 && results2[1].id != 1 {
		t.Errorf("Expected second id=1 (sum=102), got id=%d", results2[1].id)
	}
	if len(results2) >= 3 && results2[2].id != 3 {
		t.Errorf("Expected third id=3 (sum=201), got id=%d", results2[2].id)
	}
}

// TestIntegration_GroupBy tests GROUP BY
func TestIntegration_GroupBy(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table and insert data
	db.Exec("CREATE TABLE sales (id INT PRIMARY KEY, category VARCHAR(50), amount INT)")
	db.Exec("INSERT INTO sales VALUES (1, 'A', 100)")
	db.Exec("INSERT INTO sales VALUES (2, 'A', 200)")
	db.Exec("INSERT INTO sales VALUES (3, 'B', 150)")
	db.Exec("INSERT INTO sales VALUES (4, 'B', 250)")

	// Test GROUP BY with aggregate - verify query runs without error
	rows, err := db.Query("SELECT category, SUM(amount) FROM sales GROUP BY category")
	if err != nil {
		t.Fatalf("GROUP BY query failed: %v", err)
	}
	defer rows.Close()

	// Collect results
	results := make(map[string]int)
	for rows.Next() {
		var category string
		var total int
		if err := rows.Scan(&category, &total); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("category=%s, sum=%d", category, total)
		results[category] = total
	}

	// Verify we got exactly 2 groups (A and B)
	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}

	// Verify aggregate values: A = 100+200 = 300, B = 150+250 = 400
	if sum, ok := results["A"]; !ok || sum != 300 {
		t.Errorf("Expected category A sum=300, got %d", sum)
	}
	if sum, ok := results["B"]; !ok || sum != 400 {
		t.Errorf("Expected category B sum=400, got %d", sum)
	}

	// Test GROUP BY with COUNT
	rows2, err := db.Query("SELECT category, COUNT(*) FROM sales GROUP BY category")
	if err != nil {
		t.Fatalf("GROUP BY COUNT query failed: %v", err)
	}
	defer rows2.Close()

	countResults := make(map[string]int)
	for rows2.Next() {
		var category string
		var count int
		if err := rows2.Scan(&category, &count); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("category=%s, count=%d", category, count)
		countResults[category] = count
	}

	// Each category should have count = 2
	if cnt, ok := countResults["A"]; !ok || cnt != 2 {
		t.Errorf("Expected category A count=2, got %d", cnt)
	}
	if cnt, ok := countResults["B"]; !ok || cnt != 2 {
		t.Errorf("Expected category B count=2, got %d", cnt)
	}
}

// TestIntegration_Subquery tests subquery support (IN clause)
func TestIntegration_Subquery(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create tables
	db.Exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	db.Exec("INSERT INTO orders VALUES (1, 1, 100)")
	db.Exec("INSERT INTO orders VALUES (2, 1, 200)")
	db.Exec("INSERT INTO orders VALUES (3, 2, 150)")
	db.Exec("INSERT INTO orders VALUES (4, 3, 300)")

	db.Exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
	db.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	db.Exec("INSERT INTO customers VALUES (2, 'Bob')")
	db.Exec("INSERT INTO customers VALUES (3, 'Charlie')")

	// Test IN with subquery
	rows, err := db.Query("SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)")
	if err != nil {
		t.Fatalf("Subquery IN query failed: %v", err)
	}
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("name=%s", name)
		names = append(names, name)
	}

	// Should return Alice, Bob, Charlie (all customers have orders)
	if len(names) != 3 {
		t.Errorf("Expected 3 names, got %d", len(names))
	}

	// Test NOT IN with subquery
	// First insert David before testing
	db.Exec("INSERT INTO customers VALUES (4, 'David')") // David has no orders

	// Verify all customers are there
	rowsAll, err := db.Query("SELECT id, name FROM customers")
	if err != nil {
		t.Fatalf("SELECT all customers failed: %v", err)
	}
	defer rowsAll.Close()
	t.Log("All customers:")
	for rowsAll.Next() {
		var id int
		var name string
		rowsAll.Scan(&id, &name)
		t.Logf("  id=%d, name=%s", id, name)
	}

	// Verify subquery returns expected results
	rowsSub, err := db.Query("SELECT customer_id FROM orders")
	if err != nil {
		t.Fatalf("Subquery SELECT failed: %v", err)
	}
	defer rowsSub.Close()
	t.Log("Order customer_ids:")
	for rowsSub.Next() {
		var cid int
		rowsSub.Scan(&cid)
		t.Logf("  customer_id=%d", cid)
	}

	rows2, err := db.Query("SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)")
	if err != nil {
		t.Fatalf("Subquery NOT IN query failed: %v", err)
	}
	defer rows2.Close()

	notInNames := []string{}
	for rows2.Next() {
		var name string
		if err := rows2.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("NOT IN name=%s", name)
		notInNames = append(notInNames, name)
	}

	// Should return only David
	if len(notInNames) != 1 {
		t.Errorf("Expected 1 name, got %d", len(notInNames))
	}
	if len(notInNames) > 0 && notInNames[0] != "David" {
		t.Errorf("Expected David, got %s", notInNames[0])
	}
}

// TestIntegration_DatabaseOperations tests database-level operations
func TestIntegration_DatabaseOperations(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// CREATE DATABASE
	_, err := db.Exec("CREATE DATABASE mydb")
	if err != nil {
		t.Logf("CREATE DATABASE: %v", err)
	}

	// USE database
	_, err = db.Exec("USE mydb")
	if err != nil {
		t.Logf("USE database: %v", err)
	}
}

// TestIntegration_IndexOperations tests index operations
func TestIntegration_IndexOperations(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create index
	_, err = db.Exec("CREATE INDEX idx_name ON users (name)")
	if err != nil {
		t.Errorf("CREATE INDEX failed: %v", err)
	}

	// Create unique index
	_, err = db.Exec("CREATE UNIQUE INDEX idx_email ON users (email)")
	if err != nil {
		t.Errorf("CREATE UNIQUE INDEX failed: %v", err)
	}

	// Drop index
	_, err = db.Exec("DROP INDEX idx_name ON users")
	if err != nil {
		t.Errorf("DROP INDEX failed: %v", err)
	}
}

// TestIntegration_AlterTable tests ALTER TABLE operations
func TestIntegration_AlterTable(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Add column
	_, err = db.Exec("ALTER TABLE test ADD COLUMN age INT")
	if err != nil {
		t.Errorf("ADD COLUMN failed: %v", err)
	}

	// Drop column
	_, err = db.Exec("ALTER TABLE test DROP COLUMN age")
	if err != nil {
		t.Errorf("DROP COLUMN failed: %v", err)
	}
}

// TestIntegration_UnionQuery tests UNION queries
func TestIntegration_UnionQuery(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table and insert data
	db.Exec("CREATE TABLE t1 (id INT, value VARCHAR(50))")
	db.Exec("CREATE TABLE t2 (id INT, value VARCHAR(50))")
	db.Exec("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
	db.Exec("INSERT INTO t2 VALUES (2, 'b'), (3, 'c')")

	// UNION query
	rows, err := db.Query("SELECT id FROM t1 UNION SELECT id FROM t2")
	if err != nil {
		t.Fatalf("UNION query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	// UNION should return distinct values
	if count < 1 {
		t.Errorf("Expected at least 1 row from UNION, got %d", count)
	}
}

// TestIntegration_Joins tests various join operations
func TestIntegration_Joins(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create tables
	db.Exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100))")
	db.Exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")

	// Insert data
	db.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	db.Exec("INSERT INTO customers VALUES (2, 'Bob')")
	db.Exec("INSERT INTO orders VALUES (1, 1, 100)")
	db.Exec("INSERT INTO orders VALUES (2, 1, 200)")

	// INNER JOIN
	rows, err := db.Query(`
		SELECT c.name, o.amount
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Fatalf("INNER JOIN failed: %v", err)
	}
	rows.Close()

	// LEFT JOIN
	rows, err = db.Query(`
		SELECT c.name, o.amount
		FROM customers c
		LEFT JOIN orders o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Fatalf("LEFT JOIN failed: %v", err)
	}
	rows.Close()
}

// TestIntegration_ShowCreateTable tests SHOW CREATE TABLE
func TestIntegration_ShowCreateTable(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// SHOW CREATE TABLE
	rows, err := db.Query("SHOW CREATE TABLE test")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("Expected 1 row from SHOW CREATE TABLE")
	}
}

// TestIntegration_BatchInsert tests multiple inserts
func TestIntegration_BatchInsert(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)
	db, _ := sql.Open("xxsql", dsn)
	defer db.Close()

	// Create table
	db.Exec("CREATE TABLE batch_test (id INT PRIMARY KEY, value VARCHAR(100))")

	// Insert multiple rows
	for i := 0; i < 50; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO batch_test VALUES (%d, 'value%d')", i, i))
		if err != nil {
			t.Errorf("Insert %d failed: %v", i, err)
		}
	}

	// Verify count
	rows, _ := db.Query("SELECT COUNT(*) FROM batch_test")
	defer rows.Close()
	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 50 {
			t.Errorf("Expected 50 rows, got %d", count)
		}
	}
}

// TestIntegration_ConnectionResilience tests connection resilience
func TestIntegration_ConnectionResilience(t *testing.T) {
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

	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/testdb?protocol=mysql", mysqlPort)

	// Test connection pooling
	db, _ := sql.Open("xxsql", dsn)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	defer db.Close()

	// Execute multiple queries on the same connection
	for i := 0; i < 10; i++ {
		err := db.Ping()
		if err != nil {
			t.Errorf("Ping %d failed: %v", i, err)
		}
	}

	// Create table and query multiple times
	db.Exec("CREATE TABLE test (id INT PRIMARY KEY)")
	db.Exec("INSERT INTO test VALUES (1)")

	for i := 0; i < 5; i++ {
		rows, err := db.Query("SELECT * FROM test")
		if err != nil {
			t.Errorf("Query %d failed: %v", i, err)
		}
		rows.Close()
	}
}