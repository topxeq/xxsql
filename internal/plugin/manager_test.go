package plugin

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

func setupTestEngine(t *testing.T, tmpDir string) (*storage.Engine, *executor.Executor) {
	t.Helper()
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Initialize system tables
	if err := executor.InitSystemTables(engine); err != nil {
		engine.Close()
		t.Fatalf("Failed to init system tables: %v", err)
	}

	exec := executor.NewExecutor(engine)
	return engine, exec
}

func TestManager_ListInstalled(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	plugins, err := mgr.ListInstalled()
	if err != nil {
		t.Fatalf("ListInstalled failed: %v", err)
	}

	// Should be empty initially
	if len(plugins) != 0 {
		t.Errorf("Expected 0 plugins, got %d", len(plugins))
	}
}

func TestManager_InstallFromZIP(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	// Create a test plugin ZIP
	pluginDir := filepath.Join(tmpDir, "test_plugin")
	os.MkdirAll(filepath.Join(pluginDir, "scripts"), 0755)

	// Write plugin.json
	pluginJSON := `{
		"name": "test",
		"version": "1.0.0",
		"author": "Test Author",
		"description": "Test plugin",
		"category": "test",
		"tables": "_plugin_test_data",
		"endpoints": [
			{"skey": "test/hello", "script": "scripts/hello.xxscript", "description": "Hello endpoint"}
		]
	}`
	os.WriteFile(filepath.Join(pluginDir, "plugin.json"), []byte(pluginJSON), 0644)

	// Write setup.sql
	setupSQL := "CREATE TABLE _plugin_test_data (id INT PRIMARY KEY, value VARCHAR(100));"
	os.WriteFile(filepath.Join(pluginDir, "setup.sql"), []byte(setupSQL), 0644)

	// Write script
	script := `http.json({"message": "Hello from test plugin"})`
	os.WriteFile(filepath.Join(pluginDir, "scripts", "hello.xxscript"), []byte(script), 0644)

	// Create ZIP file
	zipPath := filepath.Join(tmpDir, "test.zip")
	err := createZIP(pluginDir, zipPath)
	if err != nil {
		t.Fatalf("Failed to create ZIP: %v", err)
	}

	// Install from ZIP
	err = mgr.InstallFromZIP(zipPath)
	if err != nil {
		t.Fatalf("InstallFromZIP failed: %v", err)
	}

	// Verify plugin is installed
	plugins, err := mgr.ListInstalled()
	if err != nil {
		t.Fatalf("ListInstalled failed: %v", err)
	}

	if len(plugins) != 1 {
		t.Fatalf("Expected 1 plugin, got %d", len(plugins))
	}

	if plugins[0].Name != "test" {
		t.Errorf("Expected plugin name 'test', got %q", plugins[0].Name)
	}

	// Verify table was created
	tables := engine.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "_plugin_test_data" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected table _plugin_test_data to be created")
	}

	// Verify script was inserted
	result, err := exec.Execute("SELECT SCRIPT FROM _sys_ms WHERE SKEY LIKE 'test/hello'")
	if err != nil {
		t.Fatalf("Failed to query script: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Error("Expected script to be inserted")
	}
}

func TestManager_Uninstall(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	// Create and install a test plugin
	pluginDir := filepath.Join(tmpDir, "test_plugin")
	os.MkdirAll(filepath.Join(pluginDir, "scripts"), 0755)

	pluginJSON := `{
		"name": "uninstall_test",
		"version": "1.0.0",
		"author": "Test",
		"description": "Test",
		"category": "test",
		"tables": "_plugin_uninstall_test",
		"endpoints": []
	}`
	os.WriteFile(filepath.Join(pluginDir, "plugin.json"), []byte(pluginJSON), 0644)
	os.WriteFile(filepath.Join(pluginDir, "setup.sql"), []byte("CREATE TABLE _plugin_uninstall_test (id INT);"), 0644)

	zipPath := filepath.Join(tmpDir, "test.zip")
	createZIP(pluginDir, zipPath)

	if err := mgr.InstallFromZIP(zipPath); err != nil {
		t.Fatalf("Install failed: %v", err)
	}

	// Uninstall
	if err := mgr.Uninstall("uninstall_test"); err != nil {
		t.Fatalf("Uninstall failed: %v", err)
	}

	// Verify plugin is removed
	plugins, _ := mgr.ListInstalled()
	if len(plugins) != 0 {
		t.Error("Expected plugin to be uninstalled")
	}

	// Verify table is dropped
	tables := engine.ListTables()
	for _, tbl := range tables {
		if tbl == "_plugin_uninstall_test" {
			t.Error("Expected table to be dropped")
		}
	}
}

func TestManager_EnableDisable(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	// Create and install a test plugin
	pluginDir := filepath.Join(tmpDir, "test_plugin")
	os.MkdirAll(pluginDir, 0755)

	pluginJSON := `{
		"name": "enable_test",
		"version": "1.0.0",
		"author": "Test",
		"description": "Test",
		"category": "test",
		"endpoints": []
	}`
	os.WriteFile(filepath.Join(pluginDir, "plugin.json"), []byte(pluginJSON), 0644)
	os.WriteFile(filepath.Join(pluginDir, "setup.sql"), []byte(""), 0644)

	zipPath := filepath.Join(tmpDir, "test.zip")
	createZIP(pluginDir, zipPath)
	mgr.InstallFromZIP(zipPath)

	// Disable
	if err := mgr.Disable("enable_test"); err != nil {
		t.Fatalf("Disable failed: %v", err)
	}

	plugin, _ := mgr.GetPlugin("enable_test")
	if plugin.Enabled {
		t.Error("Expected plugin to be disabled")
	}

	// Enable
	if err := mgr.Enable("enable_test"); err != nil {
		t.Fatalf("Enable failed: %v", err)
	}

	plugin, _ = mgr.GetPlugin("enable_test")
	if !plugin.Enabled {
		t.Error("Expected plugin to be enabled")
	}
}

func TestGetAvailablePlugins(t *testing.T) {
	plugins := GetAvailablePlugins()
	if len(plugins) == 0 {
		t.Error("Expected at least one available plugin")
	}

	// Check for auth plugin
	found := false
	for _, p := range plugins {
		if p.Name == "auth" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected auth plugin to be available")
	}
}

// Helper function to create a ZIP file
func createZIP(srcDir, zipPath string) error {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	w := zip.NewWriter(zipFile)
	defer w.Close()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		writer, err := w.Create(relPath)
		if err != nil {
			return err
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		_, err = writer.Write(data)
		return err
	})
}

func TestManager_IsInstalled_AndGetPluginNotFound(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	if mgr.IsInstalled("missing") {
		t.Fatal("IsInstalled should be false for missing plugin")
	}

	if _, err := mgr.GetPlugin("missing"); err == nil {
		t.Fatal("GetPlugin should fail for missing plugin")
	}

	pluginDir := filepath.Join(tmpDir, "is_installed_plugin")
	if err := os.MkdirAll(pluginDir, 0755); err != nil {
		t.Fatalf("mkdir plugin dir: %v", err)
	}
	pluginJSON := `{"name":"is_installed","version":"1.0.0","author":"t","description":"d","category":"test","endpoints":[]}`
	if err := os.WriteFile(filepath.Join(pluginDir, "plugin.json"), []byte(pluginJSON), 0644); err != nil {
		t.Fatalf("write plugin.json: %v", err)
	}

	zipPath := filepath.Join(tmpDir, "is_installed.zip")
	if err := createZIP(pluginDir, zipPath); err != nil {
		t.Fatalf("create zip: %v", err)
	}
	if err := mgr.InstallFromZIP(zipPath); err != nil {
		t.Fatalf("InstallFromZIP failed: %v", err)
	}

	if !mgr.IsInstalled("is_installed") {
		t.Fatal("IsInstalled should be true after install")
	}
}

func TestRegistryHelpers(t *testing.T) {
	if GetRegistryPlugin("auth") == nil {
		t.Fatal("expected auth in registry")
	}
	if GetRegistryPlugin("no-such-plugin") != nil {
		t.Fatal("expected nil for unknown registry plugin")
	}
}

func TestSplitSQLStatements_QuotesAndTail(t *testing.T) {
	sql := "INSERT INTO t VALUES('a;b');INSERT INTO t VALUES(\"x;y\");UPDATE t SET v='z'"
	stmts := splitSQLStatements(sql)
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d (%v)", len(stmts), stmts)
	}
	if !strings.Contains(stmts[0], "'a;b'") {
		t.Fatalf("expected first statement to retain quoted semicolon: %q", stmts[0])
	}
	if !strings.Contains(stmts[1], "\"x;y\"") {
		t.Fatalf("expected second statement to retain quoted semicolon: %q", stmts[1])
	}
}

func TestManager_ListInstalled_NilFields(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	_, err := exec.Execute("INSERT INTO _sys_plugins (name, version, latest_version, author, description, category, enabled, installed_at, tables, has_update, source) VALUES ('nil_fields', NULL, NULL, NULL, NULL, 'cat', true, NOW(), '', true, 'local')")
	if err != nil {
		t.Fatalf("insert plugin row: %v", err)
	}

	plugins, err := mgr.ListInstalled()
	if err != nil {
		t.Fatalf("ListInstalled failed: %v", err)
	}
	if len(plugins) != 1 {
		t.Fatalf("expected one plugin, got %d", len(plugins))
	}

	if plugins[0].Version != "" {
		t.Fatalf("expected empty version for NULL, got %q", plugins[0].Version)
	}
	if plugins[0].Author != "" {
		t.Fatalf("expected empty author for NULL, got %q", plugins[0].Author)
	}
	if plugins[0].Description != "" {
		t.Fatalf("expected empty description for NULL, got %q", plugins[0].Description)
	}
	if !plugins[0].Enabled || !plugins[0].HasUpdate {
		t.Fatalf("expected enabled and has_update to be true: %+v", plugins[0])
	}
}

func TestManager_InstallFromZIP_ErrorBranches(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	t.Run("missing zip file", func(t *testing.T) {
		err := mgr.InstallFromZIP(filepath.Join(tmpDir, "missing.zip"))
		if err == nil || !strings.Contains(err.Error(), "failed to open zip") {
			t.Fatalf("expected open zip error, got %v", err)
		}
	})

	t.Run("missing plugin json", func(t *testing.T) {
		d := filepath.Join(tmpDir, "no_plugin_json")
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "setup.sql"), []byte(""), 0644); err != nil {
			t.Fatalf("write setup.sql: %v", err)
		}

		zipPath := filepath.Join(tmpDir, "no_plugin_json.zip")
		if err := createZIP(d, zipPath); err != nil {
			t.Fatalf("create zip: %v", err)
		}

		err := mgr.InstallFromZIP(zipPath)
		if err == nil || !strings.Contains(err.Error(), "failed to read plugin.json") {
			t.Fatalf("expected missing plugin.json error, got %v", err)
		}
	})

	t.Run("invalid plugin json", func(t *testing.T) {
		d := filepath.Join(tmpDir, "invalid_plugin_json")
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "plugin.json"), []byte("{"), 0644); err != nil {
			t.Fatalf("write plugin.json: %v", err)
		}

		zipPath := filepath.Join(tmpDir, "invalid_plugin_json.zip")
		if err := createZIP(d, zipPath); err != nil {
			t.Fatalf("create zip: %v", err)
		}

		err := mgr.InstallFromZIP(zipPath)
		if err == nil || !strings.Contains(err.Error(), "invalid plugin.json") {
			t.Fatalf("expected invalid plugin.json error, got %v", err)
		}
	})

	t.Run("empty plugin name", func(t *testing.T) {
		d := filepath.Join(tmpDir, "empty_plugin_name")
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "plugin.json"), []byte(`{"name":""}`), 0644); err != nil {
			t.Fatalf("write plugin.json: %v", err)
		}

		zipPath := filepath.Join(tmpDir, "empty_plugin_name.zip")
		if err := createZIP(d, zipPath); err != nil {
			t.Fatalf("create zip: %v", err)
		}

		err := mgr.InstallFromZIP(zipPath)
		if err == nil || !strings.Contains(err.Error(), "plugin name is required") {
			t.Fatalf("expected required name error, got %v", err)
		}
	})

	t.Run("setup sql execute failure", func(t *testing.T) {
		d := filepath.Join(tmpDir, "bad_setup_sql")
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "plugin.json"), []byte(`{"name":"bad_sql","version":"1.0.0","author":"a","description":"d","category":"test","endpoints":[]}`), 0644); err != nil {
			t.Fatalf("write plugin.json: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "setup.sql"), []byte("THIS IS NOT SQL;"), 0644); err != nil {
			t.Fatalf("write setup.sql: %v", err)
		}

		zipPath := filepath.Join(tmpDir, "bad_setup_sql.zip")
		if err := createZIP(d, zipPath); err != nil {
			t.Fatalf("create zip: %v", err)
		}

		err := mgr.InstallFromZIP(zipPath)
		if err == nil || !strings.Contains(err.Error(), "failed to execute setup.sql") {
			t.Fatalf("expected setup.sql execute error, got %v", err)
		}
	})
}

func TestManager_Uninstall_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	engine, exec := setupTestEngine(t, tmpDir)
	defer engine.Close()

	mgr := NewManager(engine, exec, tmpDir)

	if err := mgr.Uninstall("missing"); err == nil {
		t.Fatal("Uninstall should fail for missing plugin")
	}
}
