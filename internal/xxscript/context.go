// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"net/http"
	"sync"
	"time"

	"github.com/topxeq/xxsql/internal/storage"
)

// ErrMaxStepsExceeded is returned when script exceeds maximum steps.
var ErrMaxStepsExceeded = NewScriptError("script exceeded maximum steps")

// ScriptError represents a script execution error.
type ScriptError struct {
	Message string
}

func (e *ScriptError) Error() string {
	return e.Message
}

// NewScriptError creates a new script error.
func NewScriptError(msg string) *ScriptError {
	return &ScriptError{Message: msg}
}

// SQLExecutor is an interface for executing SQL queries.
type SQLExecutor interface {
	ExecuteForScript(query string) (interface{}, error)
}

// Value represents a runtime value.
type Value interface{}

// ThrowError represents an error thrown by the throw statement.
type ThrowError struct {
	Value Value
}

func (e *ThrowError) Error() string {
	return e.String()
}

func (e *ThrowError) String() string {
	return e.Value.(string)
}

// Context provides the execution context.
type Context struct {
	Variables   map[string]Value
	Functions   map[string]*UserFunc
	Executor    SQLExecutor
	Engine      *storage.Engine
	HTTPWriter  http.ResponseWriter
	HTTPRequest *http.Request
	MaxSteps    int
	BaseDir     string
	Timezone    *time.Location
	steps       int
	returning   bool
	breaking    bool
	continueing bool
	returnValue Value
	cache       map[string]cacheEntry
	cacheMu     sync.RWMutex
}

type cacheEntry struct {
	value     Value
	expiresAt time.Time
}

// UserFunc represents a user-defined function.
type UserFunc struct {
	Params         []string
	DefaultValues  []Expression // Default values for parameters (nil if no default)
	RestParamIndex int          // Index of rest parameter, -1 if none
	Body           *BlockStmt
	Bytecode       *Bytecode // Compiled bytecode for the function
}

// NewContext creates a new execution context.
func NewContext() *Context {
	return &Context{
		Variables: make(map[string]Value),
		Functions: make(map[string]*UserFunc),
		MaxSteps:  10000000,
		cache:     make(map[string]cacheEntry),
	}
}

// GetVariable gets a variable value.
func (c *Context) GetVariable(name string) (Value, bool) {
	val, ok := c.Variables[name]
	return val, ok
}

// SetVariable sets a variable value.
func (c *Context) SetVariable(name string, value Value) {
	c.Variables[name] = value
}

// DeleteVariable deletes a variable from the context.
func (c *Context) DeleteVariable(name string) {
	delete(c.Variables, name)
}

// DefineFunction defines a user function.
func (c *Context) DefineFunction(name string, params []string, body *BlockStmt) {
	c.Functions[name] = &UserFunc{
		Params: params,
		Body:   body,
	}
}

// GetFunction gets a user-defined function.
func (c *Context) GetFunction(name string) (*UserFunc, bool) {
	fn, ok := c.Functions[name]
	return fn, ok
}

// IncrementSteps increments the step counter and checks for limits.
func (c *Context) IncrementSteps() error {
	c.steps++
	if c.steps > c.MaxSteps {
		return ErrMaxStepsExceeded
	}
	return nil
}

// IsReturning returns true if a return statement was executed.
func (c *Context) IsReturning() bool {
	return c.returning
}

// SetReturning sets the returning flag.
func (c *Context) SetReturning(val Value) {
	c.returnValue = val
	c.returning = true
}

// GetReturnValue returns the return value.
func (c *Context) GetReturnValue() Value {
	return c.returnValue
}

// IsBreaking returns true if a break statement was executed.
func (c *Context) IsBreaking() bool {
	return c.breaking
}

// SetBreaking sets the breaking flag.
func (c *Context) SetBreaking() {
	c.breaking = true
}

// ClearBreaking clears the breaking flag.
func (c *Context) ClearBreaking() {
	c.breaking = false
}

// IsContinuing returns true if a continue statement was executed.
func (c *Context) IsContinuing() bool {
	return c.continueing
}

// SetContinuing sets the continuing flag.
func (c *Context) SetContinuing() {
	c.continueing = true
}

// ClearContinuing clears the continuing flag.
func (c *Context) ClearContinuing() {
	c.continueing = false
}

// ResetFlowControl resets all flow control flags.
func (c *Context) ResetFlowControl() {
	c.returning = false
	c.breaking = false
	c.continueing = false
	c.returnValue = nil
}

// CacheSet stores a value in cache with optional TTL.
func (c *Context) CacheSet(key string, value Value, ttl time.Duration) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	entry := cacheEntry{value: value}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}
	c.cache[key] = entry
}

// CacheGet retrieves a value from cache.
func (c *Context) CacheGet(key string) (Value, bool) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	entry, ok := c.cache[key]
	if !ok {
		return nil, false
	}

	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.value, true
}

// CacheDelete removes a value from cache.
func (c *Context) CacheDelete(key string) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	delete(c.cache, key)
}

// CacheHas checks if a key exists in cache.
func (c *Context) CacheHas(key string) bool {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	entry, ok := c.cache[key]
	if !ok {
		return false
	}

	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return false
	}

	return true
}

// CacheClear clears all cache entries.
func (c *Context) CacheClear() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.cache = make(map[string]cacheEntry)
}

// CacheKeys returns all cache keys.
func (c *Context) CacheKeys() []string {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	keys := make([]string, 0, len(c.cache))
	now := time.Now()
	for k, entry := range c.cache {
		if entry.expiresAt.IsZero() || now.Before(entry.expiresAt) {
			keys = append(keys, k)
		}
	}
	return keys
}
