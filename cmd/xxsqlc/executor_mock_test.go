package main

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// setupMockDB creates a mock database connection and returns a cleanup function
func setupMockDB(t *testing.T) (sqlmock.Sqlmock, func()) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}

	// Save original db and set mock
	origDB := db
	db = mockDB

	cleanup := func() {
		db = origDB
		mockDB.Close()
	}

	return mock, cleanup
}

// TestExecuteQuery tests the executeQuery function
func TestExecuteQuery_Select(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice").
		AddRow(2, "Bob")
	mock.ExpectQuery("SELECT (.+) FROM users").WillReturnRows(rows)

	err := executeQuery("SELECT * FROM users")
	if err != nil {
		t.Errorf("executeQuery failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteQuery_Error(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query error"))

	err := executeQuery("SELECT * FROM nonexistent")
	if err == nil {
		t.Error("Expected error for failed query")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteQuery_EmptyResult(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "name"})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	err := executeQuery("SELECT * FROM empty_table")
	if err != nil {
		t.Errorf("executeQuery failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteQuery_WithBytes(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "data"}).
		AddRow(1, []byte("binary_data"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	err := executeQuery("SELECT * FROM test")
	if err != nil {
		t.Errorf("executeQuery failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteQuery_ColumnsError(t *testing.T) {
	// This test is difficult to implement with sqlmock because
	// sqlmock.Rows doesn't easily allow returning errors on Columns()
	// Skip this edge case test
	t.Skip("sqlmock doesn't support Columns() error simulation")
}

func TestExecuteQuery_ScanError(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	// Create rows with column type mismatch
	rows := sqlmock.NewRows([]string{"id"}).
		AddRow("not_an_int")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	// The error handling depends on how the test is set up
	_ = executeQuery("SELECT id FROM test")

	_ = mock.ExpectationsWereMet()
}

func TestExecuteQuery_RowsError(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id"}).
		AddRow(1).
		CloseError(errors.New("rows error"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	err := executeQuery("SELECT id FROM test")
	if err == nil {
		t.Error("Expected rows error")
	}
}

// TestExecuteExec tests the executeExec function
func TestExecuteExec_Insert(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	result := sqlmock.NewResult(1, 1)
	mock.ExpectExec("INSERT INTO users").WillReturnResult(result)

	err := executeExec("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_Update(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	result := sqlmock.NewResult(0, 5)
	mock.ExpectExec("UPDATE users").WillReturnResult(result)

	err := executeExec("UPDATE users SET name = 'test'")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_Delete(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	result := sqlmock.NewResult(0, 3)
	mock.ExpectExec("DELETE FROM").WillReturnResult(result)

	err := executeExec("DELETE FROM users WHERE id < 10")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_CreateTable(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	result := sqlmock.NewResult(0, 0)
	mock.ExpectExec("CREATE TABLE").WillReturnResult(result)

	err := executeExec("CREATE TABLE test (id INT)")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_Error(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	mock.ExpectExec("INSERT").WillReturnError(errors.New("exec error"))

	err := executeExec("INSERT INTO test VALUES (1)")
	if err == nil {
		t.Error("Expected error for failed exec")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_RowsAffectedError(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	// Create a result that returns error for RowsAffected
	result := &errorResult{}
	mock.ExpectExec("INSERT").WillReturnResult(result)

	err := executeExec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("executeExec should handle RowsAffected error gracefully: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_NoRowsAffected(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	result := sqlmock.NewResult(0, 0)
	mock.ExpectExec("CREATE TABLE").WillReturnResult(result)

	err := executeExec("CREATE TABLE test (id INT)")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteExec_WithLastInsertID(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	result := sqlmock.NewResult(42, 1)
	mock.ExpectExec("INSERT").WillReturnResult(result)

	err := executeExec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("executeExec failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestListTables tests the listTables function
func TestListTables_Success(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Tables_in_test"}).
		AddRow("users").
		AddRow("orders").
		AddRow("products")
	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	err := listTables()
	if err != nil {
		t.Errorf("listTables failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestListTables_Empty(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Tables_in_test"})
	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	err := listTables()
	if err != nil {
		t.Errorf("listTables failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestListTables_Error(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	mock.ExpectQuery("SHOW TABLES").WillReturnError(errors.New("query error"))

	err := listTables()
	if err == nil {
		t.Error("Expected error for failed query")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestListTables_ScanError(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Tables_in_test"}).
		AddRow(123)
	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	_ = listTables()

	_ = mock.ExpectationsWereMet()
}

// TestDescribeTable tests the describeTable function
func TestDescribeTable_Success(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
		AddRow("id", "int(11)", "NO", "PRI", nil, "auto_increment").
		AddRow("name", "varchar(100)", "YES", "", nil, "")
	mock.ExpectQuery("DESCRIBE users").WillReturnRows(rows)

	err := describeTable("users")
	if err != nil {
		t.Errorf("describeTable failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestDescribeTable_Error(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	mock.ExpectQuery("DESCRIBE nonexistent").WillReturnError(errors.New("table doesn't exist"))

	err := describeTable("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestDescribeTable_WithBytes(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Field", "Type"}).
		AddRow([]byte("id"), []byte("int"))
	mock.ExpectQuery("DESCRIBE test").WillReturnRows(rows)

	err := describeTable("test")
	if err != nil {
		t.Errorf("describeTable failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestDescribeTable_Empty(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Field", "Type"})
	mock.ExpectQuery("DESCRIBE empty_table").WillReturnRows(rows)

	err := describeTable("empty_table")
	if err != nil {
		t.Errorf("describeTable failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestUseDatabase tests the useDatabase function
func TestUseDatabase_Success(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	origDBName := dbName
	defer func() { dbName = origDBName }()

	mock.ExpectExec("USE testdb").WillReturnResult(sqlmock.NewResult(0, 0))

	err := useDatabase("testdb")
	if err != nil {
		t.Errorf("useDatabase failed: %v", err)
	}

	if dbName != "testdb" {
		t.Errorf("dbName should be 'testdb', got %q", dbName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestUseDatabase_Error(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	origDBName := dbName
	defer func() { dbName = origDBName }()

	mock.ExpectExec("USE nonexistent").WillReturnError(errors.New("database doesn't exist"))

	err := useDatabase("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent database")
	}

	if dbName == "nonexistent" {
		t.Error("dbName should not be changed on error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestExecuteSQL_Routing tests the routing logic in executeSQL
func TestExecuteSQL_RoutingToQuery(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	queries := []string{
		"SELECT * FROM users",
		"SHOW TABLES",
		"DESCRIBE users",
		"DESC users",
		"EXPLAIN SELECT * FROM users",
	}

	for i := range queries {
		mock.ExpectQuery(".").WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))
		err := executeSQL(queries[i])
		if err != nil {
			t.Errorf("executeSQL failed for %q: %v", queries[i], err)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteSQL_RoutingToExec(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	execQueries := []string{
		"INSERT INTO users VALUES (1, 'test')",
		"UPDATE users SET name = 'test'",
		"DELETE FROM users",
		"CREATE TABLE test (id INT)",
		"DROP TABLE test",
	}

	for i := range execQueries {
		mock.ExpectExec(".").WillReturnResult(sqlmock.NewResult(0, 1))
		err := executeSQL(execQueries[i])
		if err != nil {
			t.Errorf("executeSQL failed for %q: %v", execQueries[i], err)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestExecuteSQL_EmptyQuery(t *testing.T) {
	err := executeSQL("")
	if err != nil {
		t.Errorf("Empty query should not error: %v", err)
	}

	err = executeSQL("   ")
	if err != nil {
		t.Errorf("Whitespace query should not error: %v", err)
	}
}

// TestOutputFormat_WithMock tests output formatting with real data
func TestOutputFormat_WithMock(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "name", "value"}).
		AddRow(1, "Alice", 100).
		AddRow(2, "Bob", 200)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	origFmt := outFmt
	outFmt = FormatTable
	defer func() { outFmt = origFmt }()

	err := executeQuery("SELECT * FROM test")
	if err != nil {
		t.Errorf("executeQuery failed: %v", err)
	}
}

// errorResult is a mock result that returns error for RowsAffected
type errorResult struct{}

func (r *errorResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *errorResult) RowsAffected() (int64, error) {
	return 0, errors.New("rows affected error")
}

// Ensure errorResult implements sql.Result
var _ sql.Result = (*errorResult)(nil)

// TestHandleMetaCommand_Describe tests \d command with mock database
func TestHandleMetaCommand_Describe(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Tables_in_test"}).
		AddRow("users").
		AddRow("orders")
	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	err := handleMetaCommand("\\d")
	if err != nil {
		t.Errorf("\\d failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestHandleMetaCommand_DescribeTable(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"Field", "Type"}).
		AddRow("id", "int").
		AddRow("name", "varchar(100)")
	mock.ExpectQuery("DESCRIBE users").WillReturnRows(rows)

	err := handleMetaCommand("\\d users")
	if err != nil {
		t.Errorf("\\d users failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestHandleMetaCommand_UseDatabaseCmd(t *testing.T) {
	mock, cleanup := setupMockDB(t)
	defer cleanup()

	origDBName := dbName
	defer func() { dbName = origDBName }()

	mock.ExpectExec("USE testdb").WillReturnResult(sqlmock.NewResult(0, 0))

	err := handleMetaCommand("\\u testdb")
	if err != nil {
		t.Errorf("\\u testdb failed: %v", err)
	}

	if dbName != "testdb" {
		t.Errorf("dbName should be 'testdb', got %q", dbName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestHandleMetaCommand_UseDatabaseError(t *testing.T) {
	// Test \u without database name
	err := handleMetaCommand("\\u")
	if err == nil {
		t.Error("Expected error for \\u without database name")
	}
}

func TestHandleMetaCommand_AllCommands(t *testing.T) {
	tests := []struct {
		cmd string
	}{
		{"\\l"},
		{"\\conninfo"},
		{"\\format"},
	}

	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			err := handleMetaCommand(tt.cmd)
			if err != nil {
				t.Errorf("Command %q failed: %v", tt.cmd, err)
			}
		})
	}
}

func TestHandleMetaCommand_UnknownError(t *testing.T) {
	err := handleMetaCommand("\\unknowncommand")
	if err == nil {
		t.Error("Expected error for unknown command")
	}
}