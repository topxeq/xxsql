package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ScriptFunction represents a user-defined function implemented in XxScript.
type ScriptFunction struct {
	Name       string   `json:"name"`
	Params     []string `json:"params"`
	ReturnType string   `json:"return_type"`
	Script     string   `json:"script"`
}

// ScriptUDFManager manages user-defined functions implemented in XxScript.
// Note: The actual script execution is done in the Executor to avoid import cycles.
type ScriptUDFManager struct {
	mu        sync.RWMutex
	functions map[string]*ScriptFunction
	dataDir   string
}

// NewScriptUDFManager creates a new script-based UDF manager.
func NewScriptUDFManager(dataDir string) *ScriptUDFManager {
	return &ScriptUDFManager{
		functions: make(map[string]*ScriptFunction),
		dataDir:   dataDir,
	}
}

// CreateFunction creates a new script-based function.
func (m *ScriptUDFManager) CreateFunction(fn *ScriptFunction, replace bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := fn.Name
	if _, exists := m.functions[name]; exists && !replace {
		return fmt.Errorf("function %s already exists", name)
	}

	m.functions[name] = fn
	return nil
}

// DropFunction drops a user-defined function.
func (m *ScriptUDFManager) DropFunction(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.functions[name]; !exists {
		return fmt.Errorf("function %s does not exist", name)
	}

	delete(m.functions, name)
	return nil
}

// GetFunction retrieves a function by name.
func (m *ScriptUDFManager) GetFunction(name string) (*ScriptFunction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fn, exists := m.functions[name]
	return fn, exists
}

// ListFunctions returns all function names.
func (m *ScriptUDFManager) ListFunctions() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.functions))
	for name := range m.functions {
		names = append(names, name)
	}
	return names
}

// Exists checks if a function exists.
func (m *ScriptUDFManager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.functions[name]
	return exists
}

// Save saves all functions to disk.
func (m *ScriptUDFManager) Save() error {
	if m.dataDir == "" {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	fns := make([]*ScriptFunction, 0, len(m.functions))
	for _, fn := range m.functions {
		fns = append(fns, fn)
	}

	data, err := json.MarshalIndent(fns, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal functions: %w", err)
	}

	path := filepath.Join(m.dataDir, "script_udf.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write functions file: %w", err)
	}

	return nil
}

// Load loads functions from disk.
func (m *ScriptUDFManager) Load() error {
	if m.dataDir == "" {
		return nil
	}

	path := filepath.Join(m.dataDir, "script_udf.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read functions file: %w", err)
	}

	var fns []*ScriptFunction
	if err := json.Unmarshal(data, &fns); err != nil {
		return fmt.Errorf("failed to unmarshal functions: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, fn := range fns {
		m.functions[fn.Name] = fn
	}

	return nil
}