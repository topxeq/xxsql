package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/topxeq/xxsql/internal/sql"
)

// UDFManager manages user-defined functions.
type UDFManager struct {
	mu       sync.RWMutex
	functions map[string]*sql.UserFunction
	dataDir  string
}

// NewUDFManager creates a new UDF manager.
func NewUDFManager(dataDir string) *UDFManager {
	return &UDFManager{
		functions: make(map[string]*sql.UserFunction),
		dataDir:   dataDir,
	}
}

// CreateFunction creates a new user-defined function.
func (m *UDFManager) CreateFunction(fn *sql.UserFunction, replace bool) error {
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
func (m *UDFManager) DropFunction(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.functions[name]; !exists {
		return fmt.Errorf("function %s does not exist", name)
	}

	delete(m.functions, name)
	return nil
}

// GetFunction retrieves a user-defined function by name.
func (m *UDFManager) GetFunction(name string) (*sql.UserFunction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fn, exists := m.functions[name]
	return fn, exists
}

// ListFunctions returns all user-defined function names.
func (m *UDFManager) ListFunctions() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.functions))
	for name := range m.functions {
		names = append(names, name)
	}
	return names
}

// Exists checks if a function exists.
func (m *UDFManager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.functions[name]
	return exists
}

// functionJSON is used for JSON serialization.
type functionJSON struct {
	Name       string          `json:"name"`
	Parameters []paramJSON     `json:"parameters"`
	ReturnType string          `json:"return_type"`
	Body       string          `json:"body"`
}

type paramJSON struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Save saves all functions to disk.
func (m *UDFManager) Save() error {
	if m.dataDir == "" {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Convert to JSON-serializable format
	fns := make([]functionJSON, 0, len(m.functions))
	for _, fn := range m.functions {
		fns = append(fns, functionJSON{
			Name:       fn.Name,
			ReturnType: fn.ReturnType.String(),
			Body:       fn.Body.String(),
		})
	}

	data, err := json.MarshalIndent(fns, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal functions: %w", err)
	}

	path := filepath.Join(m.dataDir, "udf.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write functions file: %w", err)
	}

	return nil
}

// Load loads functions from disk.
func (m *UDFManager) Load() error {
	if m.dataDir == "" {
		return nil
	}

	path := filepath.Join(m.dataDir, "udf.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file, nothing to load
		}
		return fmt.Errorf("failed to read functions file: %w", err)
	}

	var fns []functionJSON
	if err := json.Unmarshal(data, &fns); err != nil {
		return fmt.Errorf("failed to unmarshal functions: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, fn := range fns {
		// Parse return type
		returnType := parseTypeFromString(fn.ReturnType)

		// Parse body expression - wrap in SELECT to make it parseable
		bodySQL := "SELECT " + fn.Body
		body, err := sql.Parse(bodySQL)
		if err != nil {
			continue // Skip invalid functions
		}

		// Try to convert to expression
		var bodyExpr sql.Expression
		if stmt, ok := body.(*sql.SelectStmt); ok && len(stmt.Columns) == 1 {
			bodyExpr = stmt.Columns[0]
		}

		if bodyExpr == nil {
			continue
		}

		m.functions[fn.Name] = &sql.UserFunction{
			Name:       fn.Name,
			ReturnType: returnType,
			Body:       bodyExpr,
		}
	}

	return nil
}

// parseTypeFromString parses a type from string representation.
func parseTypeFromString(s string) *sql.DataType {
	switch s {
	case "INT", "INTEGER":
		return &sql.DataType{Name: "INT"}
	case "BIGINT":
		return &sql.DataType{Name: "BIGINT"}
	case "FLOAT":
		return &sql.DataType{Name: "FLOAT"}
	case "DOUBLE":
		return &sql.DataType{Name: "DOUBLE"}
	case "DECIMAL":
		return &sql.DataType{Name: "DECIMAL"}
	case "CHAR":
		return &sql.DataType{Name: "CHAR"}
	case "VARCHAR":
		return &sql.DataType{Name: "VARCHAR"}
	case "TEXT":
		return &sql.DataType{Name: "TEXT"}
	case "DATE":
		return &sql.DataType{Name: "DATE"}
	case "TIME":
		return &sql.DataType{Name: "TIME"}
	case "DATETIME":
		return &sql.DataType{Name: "DATETIME"}
	case "BOOL", "BOOLEAN":
		return &sql.DataType{Name: "BOOL"}
	case "BLOB":
		return &sql.DataType{Name: "BLOB"}
	default:
		return &sql.DataType{Name: "TEXT"} // Default to TEXT
	}
}