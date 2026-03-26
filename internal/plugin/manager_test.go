package plugin

import (
	"archive/zip"
	"os"
	"path/filepath"
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