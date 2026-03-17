package executor_test

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestExecutorCreateTable(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Test CREATE TABLE
	result, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Verify table exists
	if !engine.TableExists("users") {
		t.Error("Table 'users' should exist")
	}
}

func TestExecutorShowTables(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create a table first
	_, err = exec.Execute("CREATE TABLE test1 (id INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW TABLES
	result, err := exec.Execute("SHOW TABLES")
	if err != nil {
		t.Fatalf("Failed to show tables: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 table, got %d", result.RowCount)
	}
}

func TestExecutorSelectLiteral(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Test SELECT 1
	result, err := exec.Execute("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to select 1: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestExecutorDropTable(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Create a table
	_, err = exec.Execute("CREATE TABLE todrop (id INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Drop the table
	result, err := exec.Execute("DROP TABLE todrop")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}

	// Try to drop non-existent table (should fail)
	_, err = exec.Execute("DROP TABLE nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent table")
	}

	// Drop with IF EXISTS (should succeed)
	result, err = exec.Execute("DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("Failed to drop table with IF EXISTS: %v", err)
	}
	if result.Message != "OK" {
		t.Errorf("Expected OK, got %s", result.Message)
	}
}

func TestExecutorUseDatabase(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Test USE DATABASE
	result, err := exec.Execute("USE mydb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}
	if result.Message != "Database changed" {
		t.Errorf("Expected 'Database changed', got %s", result.Message)
	}
}

func TestExecutorShowColumns(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Create a table with multiple columns
	_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100), active BOOL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SHOW COLUMNS
	result, err := exec.Execute("SHOW COLUMNS FROM test")
	if err != nil {
		t.Fatalf("Failed to show columns: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 columns, got %d", result.RowCount)
	}
}

func TestExecutorSetPermissionChecker(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Create a permission checker that denies all permissions
	denyAll := &mockPermissionChecker{allow: false}
	exec.SetPermissionChecker(denyAll)

	// Try to execute SELECT - should be denied
	_, err = exec.Execute("SELECT 1")
	if err == nil {
		t.Error("Expected permission denied error")
	}

	// Create a permission checker that allows all permissions
	allowAll := &mockPermissionChecker{allow: true}
	exec.SetPermissionChecker(allowAll)

	// Try to execute SELECT - should succeed
	result, err := exec.Execute("SELECT 1")
	if err != nil {
		t.Fatalf("Expected success with allow all, got: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestExecutorSetAuthManager(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Set a mock auth manager - this tests that SetAuthManager doesn't panic
	mockAuth := &mockAuthManager{}
	exec.SetAuthManager(mockAuth)

	// The test passes if we get here without panic
}

func TestExecutorPermissionDenied(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor without permission checker first to create table
	exec := executor.NewExecutor(engine)

	// Create table first (no permission check)
	_, err = exec.Execute("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Now set a permission checker that denies INSERT but allows SELECT
	checker := &selectivePermissionChecker{
		allowed: map[executor.Permission]bool{
			executor.PermSelect:      true,
			executor.PermInsert:      false,
			executor.PermCreateTable: true,
		},
	}
	exec.SetPermissionChecker(checker)

	// SELECT should succeed
	_, err = exec.Execute("SELECT * FROM test")
	if err != nil {
		t.Errorf("SELECT should succeed: %v", err)
	}

	// INSERT should be denied
	_, err = exec.Execute("INSERT INTO test (id) VALUES (1)")
	if err == nil {
		t.Error("INSERT should be denied")
	}
}

func TestSessionPermissionAdapter(t *testing.T) {
	t.Run("with nil function", func(t *testing.T) {
		adapter := executor.NewSessionPermissionAdapter(nil)
		if !adapter.HasPermission(executor.PermSelect) {
			t.Error("Should allow all permissions when function is nil")
		}
	})

	t.Run("with allow function", func(t *testing.T) {
		adapter := executor.NewSessionPermissionAdapter(func(perm uint32) bool {
			return perm == uint32(executor.PermSelect)
		})

		if !adapter.HasPermission(executor.PermSelect) {
			t.Error("Should allow PermSelect")
		}
		if adapter.HasPermission(executor.PermInsert) {
			t.Error("Should deny PermInsert")
		}
	})
}

func TestExecuteWithPerms(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-executor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage engine
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	exec := executor.NewExecutor(engine)

	// Create a permission checker that denies all
	denyAll := &mockPermissionChecker{allow: false}

	// ExecuteWithPerms should use the provided checker
	_, err = exec.ExecuteWithPerms("SELECT 1", denyAll)
	if err == nil {
		t.Error("Expected permission denied error")
	}

	// Create a permission checker that allows all
	allowAll := &mockPermissionChecker{allow: true}

	// ExecuteWithPerms should use the provided checker
	result, err := exec.ExecuteWithPerms("SELECT 1", allowAll)
	if err != nil {
		t.Errorf("Expected success: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// mockPermissionChecker is a mock implementation for testing
type mockPermissionChecker struct {
	allow bool
}

func (m *mockPermissionChecker) HasPermission(perm executor.Permission) bool {
	return m.allow
}

// selectivePermissionChecker allows specific permissions
type selectivePermissionChecker struct {
	allowed map[executor.Permission]bool
}

func (s *selectivePermissionChecker) HasPermission(perm executor.Permission) bool {
	return s.allowed[perm]
}

// mockAuthManager is a mock implementation for testing
type mockAuthManager struct{}

func (m *mockAuthManager) CreateUser(username, password string, role int) (interface{}, error) {
	return nil, nil
}

func (m *mockAuthManager) DeleteUser(username string) error {
	return nil
}

func (m *mockAuthManager) GetUser(username string) (interface{}, error) {
	return nil, nil
}

func (m *mockAuthManager) ChangePassword(username, oldPassword, newPassword string) error {
	return nil
}

func (m *mockAuthManager) GrantGlobal(username string, priv interface{}) error {
	return nil
}

func (m *mockAuthManager) GrantDatabase(username, database string, priv interface{}) error {
	return nil
}

func (m *mockAuthManager) GrantTable(username, database, table string, priv interface{}) error {
	return nil
}

func (m *mockAuthManager) RevokeGlobal(username string, priv interface{}) error {
	return nil
}

func (m *mockAuthManager) RevokeDatabase(username, database string, priv interface{}) error {
	return nil
}

func (m *mockAuthManager) RevokeTable(username, database, table string, priv interface{}) error {
	return nil
}

func (m *mockAuthManager) GetGrants(username string) ([]string, error) {
	return nil, nil
}
