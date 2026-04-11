// Package builtins provides built-in functions for xxscript.
package builtins

import "github.com/topxeq/xxsql/internal/xxscript"

// BuiltinFunc is the type for built-in functions.
type BuiltinFunc func(ctx *xxscript.Context, args []xxscript.Value) (xxscript.Value, error)

// Registry manages built-in functions.
type Registry struct {
	functions map[string]BuiltinFunc
}

// NewRegistry creates a new function registry.
func NewRegistry() *Registry {
	return &Registry{
		functions: make(map[string]BuiltinFunc),
	}
}

// Register adds a built-in function.
func (r *Registry) Register(name string, fn BuiltinFunc) {
	r.functions[name] = fn
}

// Get retrieves a function by name.
func (r *Registry) Get(name string) (BuiltinFunc, bool) {
	fn, ok := r.functions[name]
	return fn, ok
}

// All returns all registered functions.
func (r *Registry) All() map[string]BuiltinFunc {
	result := make(map[string]BuiltinFunc)
	for k, v := range r.functions {
		result[k] = v
	}
	return result
}

// globalRegistry is the global function registry.
var globalRegistry = NewRegistry()

// Register adds a function to the global registry.
func Register(name string, fn BuiltinFunc) {
	globalRegistry.Register(name, fn)
}

// GetFunc retrieves a function from the global registry.
func GetFunc(name string) (BuiltinFunc, bool) {
	return globalRegistry.Get(name)
}

// AllFuncs returns all functions from the global registry.
func AllFuncs() map[string]BuiltinFunc {
	return globalRegistry.All()
}
