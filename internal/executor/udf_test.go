package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
)

func TestUDFManager_CreateFunction(t *testing.T) {
	mgr := NewUDFManager("")

	// Create a simple function
	fn := &sql.UserFunction{
		Name:       "double",
		ReturnType: &sql.DataType{Name: "INT"},
		Body:       &sql.BinaryExpr{Left: &sql.ColumnRef{Name: "x"}, Op: sql.OpMul, Right: &sql.Literal{Value: 2}},
	}

	err := mgr.CreateFunction(fn, false)
	if err != nil {
		t.Fatalf("CreateFunction failed: %v", err)
	}

	// Check function exists
	if !mgr.Exists("double") {
		t.Error("function should exist")
	}

	// Try to create again without replace
	err = mgr.CreateFunction(fn, false)
	if err == nil {
		t.Error("should fail when creating duplicate without replace")
	}

	// Try with replace
	err = mgr.CreateFunction(fn, true)
	if err != nil {
		t.Fatalf("CreateFunction with replace failed: %v", err)
	}
}

func TestUDFManager_DropFunction(t *testing.T) {
	mgr := NewUDFManager("")

	fn := &sql.UserFunction{
		Name:       "test",
		ReturnType: &sql.DataType{Name: "INT"},
		Body:       &sql.Literal{Value: 1},
	}
	mgr.CreateFunction(fn, false)

	err := mgr.DropFunction("test")
	if err != nil {
		t.Fatalf("DropFunction failed: %v", err)
	}

	if mgr.Exists("test") {
		t.Error("function should not exist after drop")
	}

	// Drop non-existent
	err = mgr.DropFunction("nonexistent")
	if err == nil {
		t.Error("should fail when dropping non-existent function")
	}
}

func TestUDFManager_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "udf_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager and function
	mgr1 := NewUDFManager(tmpDir)
	fn := &sql.UserFunction{
		Name:       "triple",
		ReturnType: &sql.DataType{Name: "INT"},
		Body:       &sql.BinaryExpr{Left: &sql.ColumnRef{Name: "x"}, Op: sql.OpMul, Right: &sql.Literal{Value: 3}},
	}
	mgr1.CreateFunction(fn, false)

	// Save
	if err := mgr1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Create new manager and load
	mgr2 := NewUDFManager(tmpDir)
	if err := mgr2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !mgr2.Exists("triple") {
		t.Error("function should exist after load")
	}
}

func TestExecutor_CreateFunction(t *testing.T) {
	// Create temp data dir
	tmpDir, err := os.MkdirTemp("", "udf_exec_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create engine
	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	// Create executor with UDF manager
	exec := NewExecutor(engine)
	udfMgr := NewUDFManager(tmpDir)
	exec.SetUDFManager(udfMgr)

	// Parse and execute CREATE FUNCTION
	_, err = sql.Parse("CREATE FUNCTION double(x INT) RETURNS INT RETURN x * 2")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	result, err := exec.Execute("CREATE FUNCTION double(x INT) RETURNS INT RETURN x * 2")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("result should not be nil")
	}

	// Check function exists
	if !udfMgr.Exists("DOUBLE") {
		t.Error("function DOUBLE should exist")
	}
}

func TestExecutor_DropFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "udf_exec_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	exec := NewExecutor(engine)
	udfMgr := NewUDFManager(tmpDir)
	exec.SetUDFManager(udfMgr)

	// Create function first
	exec.Execute("CREATE FUNCTION test(x INT) RETURNS INT RETURN x + 1")

	// Drop function
	result, err := exec.Execute("DROP FUNCTION test")
	if err != nil {
		t.Fatalf("DROP FUNCTION failed: %v", err)
	}

	if result == nil {
		t.Fatal("result should not be nil")
	}

	// Check function is gone
	if udfMgr.Exists("TEST") {
		t.Error("function should not exist after drop")
	}
}

func TestExecutor_UDFCall(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "udf_exec_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	exec := NewExecutor(engine)
	udfMgr := NewUDFManager(tmpDir)
	exec.SetUDFManager(udfMgr)

	// Create a simple function
	_, err = exec.Execute("CREATE FUNCTION double(x INT) RETURNS INT RETURN x * 2")
	if err != nil {
		t.Fatalf("CREATE FUNCTION failed: %v", err)
	}

	// Test calling the UDF in a SELECT without FROM
	result, err := exec.Execute("SELECT double(5)")
	if err != nil {
		t.Fatalf("SELECT with UDF failed: %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Fatal("result should have rows")
	}

	// The result should be 10
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		val := result.Rows[0][0]
		// Check various numeric types
		var got int
		switch v := val.(type) {
		case int:
			got = v
		case int64:
			got = int(v)
		case float64:
			got = int(v)
		default:
			t.Fatalf("double(5) returned unexpected type %T: %v", val, val)
		}
		if got != 10 {
			t.Errorf("double(5) = %d, want 10", got)
		}
	}
}

func TestExecutor_UDFWithConcat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "udf_exec_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	defer engine.Close()

	exec := NewExecutor(engine)
	udfMgr := NewUDFManager(tmpDir)
	exec.SetUDFManager(udfMgr)

	// Create a function that uses CONCAT
	_, err = exec.Execute("CREATE FUNCTION greeting(name VARCHAR) RETURNS VARCHAR RETURN CONCAT('Hello, ', name)")
	if err != nil {
		t.Fatalf("CREATE FUNCTION failed: %v", err)
	}

	// Test calling the UDF
	result, err := exec.Execute("SELECT greeting('World')")
	if err != nil {
		t.Fatalf("SELECT with UDF failed: %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Fatal("result should have rows")
	}

	// The result should be "Hello, World"
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		val := result.Rows[0][0]
		if val != "Hello, World" {
			t.Errorf("greeting('World') = %v, want 'Hello, World'", val)
		}
	}
}

func TestParseCreateFunction(t *testing.T) {
	tests := []struct {
		input    string
		name     string
		params   int
		returns  string
	}{
		{
			input:   "CREATE FUNCTION foo(x INT) RETURNS INT RETURN x + 1",
			name:    "foo",
			params:  1,
			returns: "INT",
		},
		{
			input:   "CREATE FUNCTION bar(a VARCHAR, b INT) RETURNS VARCHAR RETURN a",
			name:    "bar",
			params:  2,
			returns: "VARCHAR",
		},
		{
			input:   "CREATE OR REPLACE FUNCTION baz() RETURNS INT RETURN 42",
			name:    "baz",
			params:  0,
			returns: "INT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sql.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			cf, ok := stmt.(*sql.CreateFunctionStmt)
			if !ok {
				t.Fatalf("Expected *CreateFunctionStmt, got %T", stmt)
			}

			if cf.Name != tt.name {
				t.Errorf("Name = %q, want %q", cf.Name, tt.name)
			}

			if len(cf.Parameters) != tt.params {
				t.Errorf("Parameters count = %d, want %d", len(cf.Parameters), tt.params)
			}

			if cf.ReturnType == nil || cf.ReturnType.Name != tt.returns {
				t.Errorf("ReturnType = %v, want %s", cf.ReturnType, tt.returns)
			}

			if cf.Body == nil {
				t.Error("Body should not be nil")
			}
		})
	}
}

func TestParseDropFunction(t *testing.T) {
	tests := []struct {
		input    string
		name     string
		ifExists bool
	}{
		{
			input: "DROP FUNCTION foo",
			name:  "foo",
		},
		{
			input:    "DROP FUNCTION IF EXISTS bar",
			name:     "bar",
			ifExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := sql.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			df, ok := stmt.(*sql.DropFunctionStmt)
			if !ok {
				t.Fatalf("Expected *DropFunctionStmt, got %T", stmt)
			}

			if df.Name != tt.name {
				t.Errorf("Name = %q, want %q", df.Name, tt.name)
			}

			if df.IfExists != tt.ifExists {
				t.Errorf("IfExists = %v, want %v", df.IfExists, tt.ifExists)
			}
		})
	}
}