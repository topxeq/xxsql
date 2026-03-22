package main

import (
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestParseSQLFile(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  []string
		expectErr bool
	}{
		{
			name:     "single statement",
			input:    "SELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "multiple statements",
			input:    "SELECT * FROM users; SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "statement with line comment",
			input:    "-- This is a comment\nSELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "statement with block comment",
			input:    "/* comment */ SELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "multi-line statement",
			input:    "SELECT *\nFROM users\nWHERE id = 1;",
			expected: []string{"SELECT *\nFROM users\nWHERE id = 1"},
		},
		{
			name:     "semicolon in string",
			input:    "INSERT INTO users (name) VALUES ('test;value');",
			expected: []string{"INSERT INTO users (name) VALUES ('test;value')"},
		},
		{
			name:     "semicolon in double quoted string",
			input:    `INSERT INTO users (name) VALUES ("test;value");`,
			expected: []string{`INSERT INTO users (name) VALUES ("test;value")`},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "only comments",
			input:    "-- comment 1\n-- comment 2\n",
			expected: nil,
		},
		{
			name:     "statement without semicolon",
			input:    "SELECT * FROM users",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "escaped single quote",
			input:    "INSERT INTO users (name) VALUES ('it''s test');",
			expected: []string{"INSERT INTO users (name) VALUES ('it''s test')"},
		},
		{
			name: "complex SQL file",
			input: `-- Create table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Insert data
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

/* Multi-line
   comment */
SELECT * FROM users;`,
			expected: []string{
				"CREATE TABLE users (\n    id INT PRIMARY KEY,\n    name VARCHAR(100)\n)",
				"INSERT INTO users VALUES (1, 'Alice')",
				"INSERT INTO users VALUES (2, 'Bob')",
				"SELECT * FROM users",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := parseSQLFile(reader)

			if tt.expectErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d statements, got %d", len(tt.expected), len(result))
				t.Logf("Expected: %v", tt.expected)
				t.Logf("Got: %v", result)
				return
			}

			for i, stmt := range result {
				// Normalize whitespace for comparison
				expected := strings.TrimSpace(tt.expected[i])
				got := strings.TrimSpace(stmt)
				if expected != got {
					t.Errorf("statement %d mismatch:\nexpected: %q\ngot: %q", i, expected, got)
				}
			}
		})
	}
}

func TestParseSQLFileWithSpecialChars(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "string with backslash",
			input:    `INSERT INTO t VALUES ('path\\to\\file');`,
			expected: 1,
		},
		{
			name:     "mixed quotes",
			input:    `INSERT INTO t VALUES ('single', "double");`,
			expected: 1,
		},
		{
			name:     "nested quotes",
			input:    `INSERT INTO t VALUES ('he said "hello"');`,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := parseSQLFile(reader)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(result) != tt.expected {
				t.Errorf("expected %d statements, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestExecuteFile tests the executeFile function
func TestExecuteFile(t *testing.T) {
	// Create a temp SQL file
	content := `CREATE TABLE test (id INT);
INSERT INTO test VALUES (1);
SELECT * FROM test;
`
	tmpFile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Setup mock database
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	// Expect CREATE TABLE
	mock.ExpectExec("CREATE TABLE test").WillReturnResult(sqlmock.NewResult(0, 0))
	// Expect INSERT
	mock.ExpectExec("INSERT INTO test").WillReturnResult(sqlmock.NewResult(1, 1))
	// Expect SELECT
	rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// Execute the file
	err = executeFile(tmpFile.Name(), false)
	if err != nil {
		t.Errorf("executeFile failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestExecuteFileWithProgress tests executeFile with progress display
func TestExecuteFileWithProgress(t *testing.T) {
	content := `SELECT 1; SELECT 2;`
	tmpFile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Setup mock database
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	// Expect two SELECT queries
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery("SELECT 2").WillReturnRows(sqlmock.NewRows([]string{"2"}).AddRow(2))

	// Execute with progress
	err = executeFile(tmpFile.Name(), true)
	if err != nil {
		t.Errorf("executeFile failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestExecuteFileNotFound tests executeFile with non-existent file
func TestExecuteFileNotFound(t *testing.T) {
	err := executeFile("/nonexistent/file.sql", false)
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// TestExecuteFileEmpty tests executeFile with empty file
func TestExecuteFileEmpty(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = executeFile(tmpFile.Name(), false)
	if err != nil {
		t.Errorf("executeFile with empty file failed: %v", err)
	}
}

// TestExecuteFileWithError tests executeFile when some statements fail
func TestExecuteFileWithError(t *testing.T) {
	content := `SELECT 1; INSERT INTO nonexistent VALUES (1); SELECT 2;`
	tmpFile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Setup mock database
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	// First SELECT succeeds
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	// INSERT fails
	mock.ExpectExec("INSERT").WillReturnError(os.ErrNotExist)
	// Second SELECT succeeds
	mock.ExpectQuery("SELECT 2").WillReturnRows(sqlmock.NewRows([]string{"2"}).AddRow(2))

	// Execute should return error due to one failed statement
	err = executeFile(tmpFile.Name(), false)
	if err == nil {
		t.Error("Expected error due to failed statement")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestExecuteSQL tests the executeSQL function
func TestExecuteSQL(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	// Test SELECT
	rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	err = executeSQL("SELECT * FROM test")
	if err != nil {
		t.Errorf("executeSQL SELECT failed: %v", err)
	}

	// Test INSERT
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	err = executeSQL("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("executeSQL INSERT failed: %v", err)
	}

	// Test empty query
	err = executeSQL("")
	if err != nil {
		t.Errorf("executeSQL empty failed: %v", err)
	}

	// Test whitespace query
	err = executeSQL("   ")
	if err != nil {
		t.Errorf("executeSQL whitespace failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestListTables tests the listTables function
func TestListTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	rows := sqlmock.NewRows([]string{"Tables_in_test"}).
		AddRow("users").
		AddRow("orders")
	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	err = listTables()
	if err != nil {
		t.Errorf("listTables failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestDescribeTable tests the describeTable function
func TestDescribeTable(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	rows := sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
		AddRow("id", "int", "NO", "PRI", nil, "")
	mock.ExpectQuery("DESCRIBE users").WillReturnRows(rows)

	err = describeTable("users")
	if err != nil {
		t.Errorf("describeTable failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestUseDatabase tests the useDatabase function
func TestUseDatabase(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	origDbName := dbName
	defer func() {
		db = origDB
		dbName = origDbName
		mockDB.Close()
	}()

	mock.ExpectExec("USE testdb").WillReturnResult(sqlmock.NewResult(0, 0))

	err = useDatabase("testdb")
	if err != nil {
		t.Errorf("useDatabase failed: %v", err)
	}

	if dbName != "testdb" {
		t.Errorf("dbName = %q, want 'testdb'", dbName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestExecuteExec tests the executeExec function
func TestExecuteExec(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	origDB := db
	db = mockDB
	defer func() {
		db = origDB
		mockDB.Close()
	}()

	// Test with RowsAffected and LastInsertId
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(5, 3))
	err = executeExec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	// Test with no rows affected
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	err = executeExec("UPDATE test SET a = 1 WHERE false")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}
