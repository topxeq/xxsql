package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestCopyFrom tests COPY table FROM 'file.csv'
func TestCopyFrom(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-copyfrom-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table without auto-increment id
	_, err = exec.Execute(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			age INT,
			active BOOL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create test CSV file
	csvContent := `id,name,age,active
1,Alice,30,true
2,Bob,25,false
3,Charlie,35,true
`
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Execute COPY FROM
	result, err := exec.Execute(fmt.Sprintf(
		"COPY users FROM '%s' WITH (FORMAT csv, HEADER true, DELIMITER ',')", csvPath))
	if err != nil {
		t.Fatalf("Failed to execute COPY FROM: %v", err)
	}

	t.Logf("COPY FROM result: %v", result.Rows[0])

	// Verify data was imported
	result, err = exec.Execute("SELECT * FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}

	t.Logf("Imported data:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify specific values
	if result.Rows[0][1].(string) != "Alice" {
		t.Errorf("Expected first row name to be Alice, got %v", result.Rows[0][1])
	}
}

// TestCopyTo tests COPY (SELECT ...) TO 'file.csv'
func TestCopyTo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-copyto-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table and insert data
	_, err = exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			price FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 5; i++ {
		_, err = exec.Execute(fmt.Sprintf(
			"INSERT INTO products (name, price) VALUES ('Product%d', %d.99)", i, i*10))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Export to CSV
	csvPath := filepath.Join(tmpDir, "export.csv")
	result, err := exec.Execute(fmt.Sprintf(
		"COPY (SELECT * FROM products ORDER BY id) TO '%s' WITH (FORMAT csv, HEADER true)", csvPath))
	if err != nil {
		t.Fatalf("Failed to execute COPY TO: %v", err)
	}

	t.Logf("COPY TO result: %v", result.Rows[0])

	// Verify file was created
	content, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	t.Logf("Exported CSV content:\n%s", string(content))

	// Verify header
	if len(content) > 0 {
		lines := splitLines(string(content))
		if lines[0] != "id,name,price" {
			t.Errorf("Expected header 'id,name,price', got '%s'", lines[0])
		}
		if len(lines) != 6 { // header + 5 rows
			t.Errorf("Expected 6 lines (header + 5 rows), got %d", len(lines))
		}
	}
}

// TestCopyToFromTable tests COPY table TO 'file.csv'
func TestCopyToFromTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-copyto-table-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table and insert data
	_, err = exec.Execute(`
		CREATE TABLE items (
			id SEQ PRIMARY KEY,
			item_name VARCHAR(100),
			quantity INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items (item_name, quantity) VALUES ('Widget', 100)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Export table directly
	csvPath := filepath.Join(tmpDir, "items_export.csv")
	result, err := exec.Execute(fmt.Sprintf("COPY items TO '%s' WITH (HEADER true)", csvPath))
	if err != nil {
		t.Fatalf("Failed to execute COPY table TO: %v", err)
	}

	t.Logf("COPY table TO result: %v", result.Rows[0])

	// Verify file exists
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

// TestLoadDataInfile tests LOAD DATA INFILE
func TestLoadDataInfile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-loaddata-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE customers (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			country VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create test data file (tab-separated)
	dataContent := `1	John	john@example.com	USA
2	Jane	jane@example.com	UK
3	Bob	bob@example.com	Canada
`
	dataPath := filepath.Join(tmpDir, "customers.tsv")
	if err := os.WriteFile(dataPath, []byte(dataContent), 0644); err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}

	// Execute LOAD DATA INFILE
	result, err := exec.Execute(fmt.Sprintf(
		"LOAD DATA INFILE '%s' INTO TABLE customers FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'",
		dataPath))
	if err != nil {
		t.Fatalf("Failed to execute LOAD DATA: %v", err)
	}

	t.Logf("LOAD DATA result: %v", result.Rows[0])

	// Verify data
	result, err = exec.Execute("SELECT * FROM customers ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}

	t.Logf("Loaded data:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

// TestLoadDataWithIgnore tests LOAD DATA INFILE with IGNORE n ROWS
func TestLoadDataWithIgnore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-loaddata-ignore-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE data (
			id INT PRIMARY KEY,
			value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create test data with header (comma-separated)
	dataContent := `header_row
100
200
300
`
	dataPath := filepath.Join(tmpDir, "data.csv")
	if err := os.WriteFile(dataPath, []byte(dataContent), 0644); err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}

	// Execute LOAD DATA with IGNORE 1 ROWS (skip header)
	sql := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE data IGNORE 1 ROWS", dataPath)
	t.Logf("SQL: %s", sql)
	result, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("Failed to execute LOAD DATA: %v", err)
	}

	t.Logf("LOAD DATA with IGNORE result: %v", result.Rows[0])

	// Verify only 3 rows were inserted (skipping header)
	result, err = exec.Execute("SELECT COUNT(*) FROM data")
	if err != nil {
		t.Fatalf("Failed to SELECT COUNT: %v", err)
	}

	var count int
	switch v := result.Rows[0][0].(type) {
	case int:
		count = v
	case int64:
		count = int(v)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows (after skipping header), got %d", count)
	}
}

// TestCopyRoundTrip tests export and import round trip
func TestCopyRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-roundtrip-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create source table
	_, err = exec.Execute(`
		CREATE TABLE source (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			score INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Insert data
	testData := []struct {
		name  string
		score int
	}{
		{"Alpha", 95},
		{"Beta", 87},
		{"Gamma", 92},
	}
	for _, d := range testData {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO source (name, score) VALUES ('%s', %d)", d.name, d.score))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Export to CSV
	exportPath := filepath.Join(tmpDir, "export.csv")
	_, err = exec.Execute(fmt.Sprintf(
		"COPY source TO '%s' WITH (FORMAT csv, HEADER true)", exportPath))
	if err != nil {
		t.Fatalf("Failed to export: %v", err)
	}

	// Create destination table
	_, err = exec.Execute(`
		CREATE TABLE destination (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			score INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create destination table: %v", err)
	}

	// Import from CSV
	_, err = exec.Execute(fmt.Sprintf(
		"COPY destination FROM '%s' WITH (FORMAT csv, HEADER true)", exportPath))
	if err != nil {
		t.Fatalf("Failed to import: %v", err)
	}

	// Verify data matches
	result, err := exec.Execute("SELECT COUNT(*) FROM destination")
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	var count int
	switch v := result.Rows[0][0].(type) {
	case int:
		count = v
	case int64:
		count = int(v)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows in destination, got %d", count)
	}

	t.Logf("Round trip successful: exported and imported %d rows", count)
}

// TestCopyWithNullValues tests handling of NULL values
func TestCopyWithNullValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-copy-null-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE nullable_data (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			optional_value INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create CSV with NULL markers
	csvContent := `id,name,optional_value
1,Alice,100
2,Bob,\N
3,Charlie,200
`
	csvPath := filepath.Join(tmpDir, "null_data.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Import with NULL string marker
	result, err := exec.Execute(fmt.Sprintf(
		"COPY nullable_data FROM '%s' WITH (FORMAT csv, HEADER true, NULL '\\\\N')", csvPath))
	if err != nil {
		t.Fatalf("Failed to execute COPY FROM: %v", err)
	}

	t.Logf("COPY FROM with NULL result: %v", result.Rows[0])

	// Verify Bob has NULL optional_value
	result, err = exec.Execute("SELECT name, optional_value FROM nullable_data ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}

	t.Logf("Data with NULLs:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Bob's optional_value should be nil
	if result.Rows[1][1] != nil {
		t.Errorf("Expected Bob's optional_value to be nil, got %v", result.Rows[1][1])
	}
}

// TestCombinedPhase7Features tests combinations of Phase 7 features
func TestCombinedPhase7Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase7-combined-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE sales (
			id INT PRIMARY KEY,
			product VARCHAR(100),
			quantity INT,
			price FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create CSV with sales data
	csvContent := `id,product,quantity,price
1,Widget,10,9.99
2,Gadget,5,19.99
3,Gizmo,20,4.99
`
	csvPath := filepath.Join(tmpDir, "sales.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Import data
	result, err := exec.Execute(fmt.Sprintf(
		"COPY sales FROM '%s' WITH (FORMAT csv, HEADER true)", csvPath))
	if err != nil {
		t.Fatalf("Failed to import: %v", err)
	}
	t.Logf("Imported %d rows", result.Rows[0][0])

	// Calculate totals
	result, err = exec.Execute(`
		SELECT product, quantity, price, quantity * price AS total
		FROM sales
		ORDER BY total DESC
	`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	t.Logf("Sales with totals:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Export summary
	exportPath := filepath.Join(tmpDir, "sales_summary.csv")
	result, err = exec.Execute(fmt.Sprintf(
		"COPY (SELECT product, SUM(quantity) as total_qty, SUM(quantity * price) as total_revenue FROM sales GROUP BY product ORDER BY total_revenue DESC) TO '%s' WITH (HEADER true)",
		exportPath))
	if err != nil {
		t.Fatalf("Failed to export summary: %v", err)
	}

	t.Logf("Exported summary: %v", result.Rows[0])

	// Verify exported file
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}
	t.Logf("Summary CSV:\n%s", string(content))
}

// Helper function to split lines
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		line := s[start:]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		lines = append(lines, line)
	}
	return lines
}