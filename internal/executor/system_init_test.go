package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

func openSystemInitTestEngine(t *testing.T) (*storage.Engine, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "xxsql-system-init-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to open engine: %v", err)
	}

	return engine, tmpDir
}

func closeSystemInitTestEngine(t *testing.T, engine *storage.Engine, dir string) {
	t.Helper()

	if engine != nil {
		_ = engine.Close()
	}
	_ = os.RemoveAll(dir)
}

func TestInitSystemTables(t *testing.T) {
	err := InitSystemTables(nil)
	if err == nil {
		t.Fatal("InitSystemTables should fail for nil engine")
	}

	engine, tmpDir := openSystemInitTestEngine(t)
	defer closeSystemInitTestEngine(t, engine, tmpDir)

	err = InitSystemTables(engine)
	if err != nil {
		t.Fatalf("InitSystemTables failed: %v", err)
	}

	if !engine.TableExists(SysTableMicroservices) {
		t.Fatalf("%s table should exist", SysTableMicroservices)
	}
	if !engine.TableExists(SysTableProjects) {
		t.Fatalf("%s table should exist", SysTableProjects)
	}
	if !engine.TableExists(SysTablePlugins) {
		t.Fatalf("%s table should exist", SysTablePlugins)
	}

	// Should be idempotent when tables already exist.
	err = InitSystemTables(engine)
	if err != nil {
		t.Fatalf("InitSystemTables second call failed: %v", err)
	}
}

func TestInsertSystemMicroserviceAndInit(t *testing.T) {
	engine, tmpDir := openSystemInitTestEngine(t)
	defer closeSystemInitTestEngine(t, engine, tmpDir)

	if err := InitSystemTables(engine); err != nil {
		t.Fatalf("InitSystemTables failed: %v", err)
	}

	if err := InsertSystemMicroservice(engine, "svc/test", "return 'ok';", "desc one"); err != nil {
		t.Fatalf("InsertSystemMicroservice insert failed: %v", err)
	}

	exec := NewExecutor(engine)
	result, err := exec.Execute("SELECT SCRIPT, description FROM _sys_ms WHERE SKEY = 'svc/test'")
	if err != nil {
		t.Fatalf("SELECT inserted microservice failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for inserted microservice, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "return 'ok';" {
		t.Fatalf("unexpected inserted script: %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "desc one" {
		t.Fatalf("unexpected inserted description: %v", result.Rows[0][1])
	}

	if err := InsertSystemMicroservice(engine, "svc/test", "line 'two'", "desc 'two'"); err != nil {
		t.Fatalf("InsertSystemMicroservice update failed: %v", err)
	}

	result, err = exec.Execute("SELECT SCRIPT, description FROM _sys_ms WHERE SKEY = 'svc/test'")
	if err != nil {
		t.Fatalf("SELECT updated microservice failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row for updated microservice, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "line 'two'" {
		t.Fatalf("unexpected updated script: %v", result.Rows[0][0])
	}
	if result.Rows[0][1] != "desc 'two'" {
		t.Fatalf("unexpected updated description: %v", result.Rows[0][1])
	}

	if err := InitSystemMicroservices(engine, tmpDir); err != nil {
		t.Fatalf("InitSystemMicroservices failed: %v", err)
	}

	countRes, err := exec.Execute("SELECT COUNT(*) FROM _sys_ms")
	if err != nil {
		t.Fatalf("SELECT COUNT(*) failed: %v", err)
	}
	if len(countRes.Rows) != 1 || len(countRes.Rows[0]) != 1 {
		t.Fatalf("unexpected COUNT(*) result shape: %+v", countRes.Rows)
	}

	switch v := countRes.Rows[0][0].(type) {
	case int:
		if v < 10 {
			t.Fatalf("expected at least 10 microservices, got %d", v)
		}
	case int64:
		if v < 10 {
			t.Fatalf("expected at least 10 microservices, got %d", v)
		}
	default:
		t.Fatalf("unexpected COUNT(*) type: %T", countRes.Rows[0][0])
	}

	healthRes, err := exec.Execute("SELECT SKEY FROM _sys_ms WHERE SKEY = 'health'")
	if err != nil {
		t.Fatalf("SELECT health microservice failed: %v", err)
	}
	if len(healthRes.Rows) != 1 {
		t.Fatalf("expected health microservice row, got %d", len(healthRes.Rows))
	}
}

func TestEscapeSQLString(t *testing.T) {
	if got := escapeSQLString("abc"); got != "abc" {
		t.Fatalf("escapeSQLString plain value = %q, want %q", got, "abc")
	}

	input := "a'b''c"
	want := "a''b''''c"
	if got := escapeSQLString(input); got != want {
		t.Fatalf("escapeSQLString quoted value = %q, want %q", got, want)
	}
}
