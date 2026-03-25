// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/topxeq/xxsql/internal/storage"
)

// runtimeOS and runtimeArch are constants for the current OS and architecture
const (
	runtimeOS   = runtime.GOOS
	runtimeArch = runtime.GOARCH
)

// SQLExecutor is an interface for executing SQL queries.
// This interface breaks the circular dependency between xxscript and executor packages.
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
	return fmt.Sprintf("%v", e.Value)
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
	BaseDir     string // Base directory for file operations
	steps       int
	returning   bool
	breaking    bool
	continueing bool
	returnValue Value
}

// UserFunc represents a user-defined function.
type UserFunc struct {
	Params []string
	Body   *BlockStmt
}

// NewContext creates a new execution context.
func NewContext() *Context {
	return &Context{
		Variables: make(map[string]Value),
		Functions: make(map[string]*UserFunc),
		MaxSteps:  10000000, // 10 million steps max
	}
}

// Interpreter interprets XxScript AST.
type Interpreter struct {
	ctx *Context
}

// NewInterpreter creates a new interpreter.
func NewInterpreter(ctx *Context) *Interpreter {
	return &Interpreter{ctx: ctx}
}

// Run executes a script.
func Run(script string, ctx *Context) (Value, error) {
	prog, err := Parse(script)
	if err != nil {
		return nil, err
	}

	if ctx == nil {
		ctx = NewContext()
		ctx.SetupBuiltins()
	}

	interp := NewInterpreter(ctx)
	return interp.Run(prog)
}

// Run executes a program.
func (i *Interpreter) Run(prog *Program) (Value, error) {
	var result Value

	for _, stmt := range prog.Statements {
		val, err := i.executeStmt(stmt)
		if err != nil {
			return nil, err
		}
		result = val

		if i.ctx.returning {
			return i.ctx.returnValue, nil
		}
	}

	return result, nil
}

func (i *Interpreter) executeStmt(stmt Statement) (Value, error) {
	i.ctx.steps++
	if i.ctx.steps > i.ctx.MaxSteps {
		return nil, fmt.Errorf("script exceeded maximum steps (%d)", i.ctx.MaxSteps)
	}

	switch s := stmt.(type) {
	case *VarStmt:
		return i.executeVarStmt(s)
	case *ExprStmt:
		return i.evaluate(s.Expr)
	case *BlockStmt:
		return i.executeBlockStmt(s)
	case *IfStmt:
		return i.executeIfStmt(s)
	case *ForStmt:
		return i.executeForStmt(s)
	case *WhileStmt:
		return i.executeWhileStmt(s)
	case *FuncStmt:
		return i.executeFuncStmt(s)
	case *ReturnStmt:
		return i.executeReturnStmt(s)
	case *BreakStmt:
		i.ctx.breaking = true
		return nil, nil
	case *ContinueStmt:
		i.ctx.continueing = true
		return nil, nil
	case *TryStmt:
		return i.executeTryStmt(s)
	case *ThrowStmt:
		return i.executeThrowStmt(s)
	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}
}

func (i *Interpreter) executeVarStmt(stmt *VarStmt) (Value, error) {
	var val Value
	if stmt.Value != nil {
		var err error
		val, err = i.evaluate(stmt.Value)
		if err != nil {
			return nil, err
		}
	}
	i.ctx.Variables[stmt.Name] = val
	return val, nil
}

func (i *Interpreter) executeBlockStmt(block *BlockStmt) (Value, error) {
	var result Value

	for _, stmt := range block.Statements {
		val, err := i.executeStmt(stmt)
		if err != nil {
			return nil, err
		}
		result = val

		if i.ctx.returning || i.ctx.breaking || i.ctx.continueing {
			break
		}
	}

	return result, nil
}

func (i *Interpreter) executeIfStmt(stmt *IfStmt) (Value, error) {
	cond, err := i.evaluate(stmt.Condition)
	if err != nil {
		return nil, err
	}

	if i.isTruthy(cond) {
		return i.executeBlockStmt(stmt.Then)
	} else if stmt.Else != nil {
		switch e := stmt.Else.(type) {
		case *BlockStmt:
			return i.executeBlockStmt(e)
		case *IfStmt:
			return i.executeIfStmt(e)
		}
	}

	return nil, nil
}

func (i *Interpreter) executeForStmt(stmt *ForStmt) (Value, error) {
	// Init
	if stmt.Init != nil {
		_, err := i.executeStmt(stmt.Init)
		if err != nil {
			return nil, err
		}
	}

	var result Value

	for {
		// Condition
		if stmt.Condition != nil {
			cond, err := i.evaluate(stmt.Condition)
			if err != nil {
				return nil, err
			}
			if !i.isTruthy(cond) {
				break
			}
		}

		// Body
		val, err := i.executeBlockStmt(stmt.Body)
		if err != nil {
			return nil, err
		}
		result = val

		if i.ctx.returning {
			break
		}

		if i.ctx.breaking {
			i.ctx.breaking = false
			break
		}

		i.ctx.continueing = false

		// Update
		if stmt.Update != nil {
			_, err = i.executeStmt(stmt.Update)
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func (i *Interpreter) executeWhileStmt(stmt *WhileStmt) (Value, error) {
	var result Value

	for {
		cond, err := i.evaluate(stmt.Condition)
		if err != nil {
			return nil, err
		}
		if !i.isTruthy(cond) {
			break
		}

		val, err := i.executeBlockStmt(stmt.Body)
		if err != nil {
			return nil, err
		}
		result = val

		if i.ctx.returning {
			break
		}

		if i.ctx.breaking {
			i.ctx.breaking = false
			break
		}

		i.ctx.continueing = false
	}

	return result, nil
}

func (i *Interpreter) executeFuncStmt(stmt *FuncStmt) (Value, error) {
	i.ctx.Functions[stmt.Name] = &UserFunc{
		Params: stmt.Params,
		Body:   stmt.Body,
	}
	return nil, nil
}

func (i *Interpreter) executeReturnStmt(stmt *ReturnStmt) (Value, error) {
	var val Value
	var err error
	if stmt.Value != nil {
		val, err = i.evaluate(stmt.Value)
		if err != nil {
			return nil, err
		}
	}
	i.ctx.returnValue = val
	i.ctx.returning = true
	return val, nil
}

func (i *Interpreter) executeTryStmt(stmt *TryStmt) (Value, error) {
	// Execute try block
	result, err := i.executeBlockStmt(stmt.TryBlock)
	if err != nil {
		// Check if it's a thrown error
		var throwErr *ThrowError
		if errors.As(err, &throwErr) {
			// We have a catch block
			if stmt.CatchBlock != nil {
				// Set the catch variable
				if stmt.CatchVar != "" {
					i.ctx.Variables[stmt.CatchVar] = throwErr.Value
				}
				// Execute catch block
				catchResult, catchErr := i.executeBlockStmt(stmt.CatchBlock)
				if catchErr != nil {
					return nil, catchErr
				}
				return catchResult, nil
			}
		}
		// No catch block or not a thrown error - propagate
		return nil, err
	}

	return result, nil
}

func (i *Interpreter) executeThrowStmt(stmt *ThrowStmt) (Value, error) {
	var errValue Value
	if stmt.Error != nil {
		val, err := i.evaluate(stmt.Error)
		if err != nil {
			return nil, err
		}
		errValue = val
	}
	return nil, &ThrowError{Value: errValue}
}

func (i *Interpreter) evaluate(expr Expression) (Value, error) {
	switch e := expr.(type) {
	case *IdentExpr:
		return i.evalIdent(e)
	case *NumberExpr:
		return e.Value, nil
	case *StringExpr:
		return e.Value, nil
	case *BoolExpr:
		return e.Value, nil
	case *NullExpr:
		return nil, nil
	case *ArrayExpr:
		return i.evalArray(e)
	case *MapExpr:
		return i.evalMap(e)
	case *BinaryExpr:
		return i.evalBinary(e)
	case *UnaryExpr:
		return i.evalUnary(e)
	case *CallExpr:
		return i.evalCall(e)
	case *MemberExpr:
		return i.evalMember(e)
	case *IndexExpr:
		return i.evalIndex(e)
	case *AssignExpr:
		return i.evalAssign(e)
	default:
		return nil, fmt.Errorf("unknown expression type: %T", expr)
	}
}

func (i *Interpreter) evalIdent(expr *IdentExpr) (Value, error) {
	if val, ok := i.ctx.Variables[expr.Name]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("undefined variable: %s", expr.Name)
}

func (i *Interpreter) evalArray(expr *ArrayExpr) (Value, error) {
	elements := make([]Value, len(expr.Elements))
	for idx, elem := range expr.Elements {
		val, err := i.evaluate(elem)
		if err != nil {
			return nil, err
		}
		elements[idx] = val
	}
	return elements, nil
}

func (i *Interpreter) evalMap(expr *MapExpr) (Value, error) {
	pairs := make(map[string]Value)
	for key, val := range expr.Pairs {
		v, err := i.evaluate(val)
		if err != nil {
			return nil, err
		}
		pairs[key] = v
	}
	return pairs, nil
}

func (i *Interpreter) evalBinary(expr *BinaryExpr) (Value, error) {
	left, err := i.evaluate(expr.Left)
	if err != nil {
		return nil, err
	}

	// Short-circuit for && and ||
	if expr.Op == TokAnd {
		if !i.isTruthy(left) {
			return false, nil
		}
		right, err := i.evaluate(expr.Right)
		if err != nil {
			return nil, err
		}
		return i.isTruthy(right), nil
	}

	if expr.Op == TokOr {
		if i.isTruthy(left) {
			return true, nil
		}
		right, err := i.evaluate(expr.Right)
		if err != nil {
			return nil, err
		}
		return i.isTruthy(right), nil
	}

	right, err := i.evaluate(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case TokPlus:
		return i.add(left, right)
	case TokMinus:
		return i.sub(left, right)
	case TokStar:
		return i.mul(left, right)
	case TokSlash:
		return i.div(left, right)
	case TokPercent:
		return i.mod(left, right)
	case TokEq:
		return i.equal(left, right), nil
	case TokNe:
		return !i.equal(left, right), nil
	case TokLt:
		return i.compare(left, right) < 0, nil
	case TokLe:
		return i.compare(left, right) <= 0, nil
	case TokGt:
		return i.compare(left, right) > 0, nil
	case TokGe:
		return i.compare(left, right) >= 0, nil
	default:
		return nil, fmt.Errorf("unknown operator: %s", expr.Op)
	}
}

func (i *Interpreter) evalUnary(expr *UnaryExpr) (Value, error) {
	val, err := i.evaluate(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case TokNot:
		return !i.isTruthy(val), nil
	case TokMinus:
		switch v := val.(type) {
		case int:
			return -v, nil
		case int64:
			return -v, nil
		case float64:
			return -v, nil
		default:
			return nil, fmt.Errorf("cannot negate %T", val)
		}
	default:
		return nil, fmt.Errorf("unknown unary operator: %s", expr.Op)
	}
}

func (i *Interpreter) evalCall(expr *CallExpr) (Value, error) {
	// Check if it's an identifier (function name) - check builtins and user funcs first
	if ident, ok := expr.Func.(*IdentExpr); ok {
		// Evaluate arguments
		args := make([]Value, len(expr.Args))
		for idx, arg := range expr.Args {
			val, err := i.evaluate(arg)
			if err != nil {
				return nil, err
			}
			args[idx] = val
		}

		// Check built-in function first
		if result, handled := i.callBuiltin(ident.Name, args); handled {
			return result, nil
		}

		// Check user-defined function
		if userFunc, ok := i.ctx.Functions[ident.Name]; ok {
			return i.callUserFunc(userFunc, args)
		}
	}

	// Evaluate function expression
	funcVal, err := i.evaluate(expr.Func)
	if err != nil {
		return nil, err
	}

	// Evaluate arguments
	args := make([]Value, len(expr.Args))
	for idx, arg := range expr.Args {
		val, err := i.evaluate(arg)
		if err != nil {
			return nil, err
		}
		args[idx] = val
	}

	// Check if it's a callable value
	if callable, ok := funcVal.(Callable); ok {
		return callable.Call(args)
	}

	return nil, fmt.Errorf("not a function: %T", funcVal)
}

func (i *Interpreter) evalMember(expr *MemberExpr) (Value, error) {
	obj, err := i.evaluate(expr.Object)
	if err != nil {
		return nil, err
	}

	member, err := i.evaluate(expr.Member)
	if err != nil {
		return nil, err
	}

	key, ok := member.(string)
	if !ok {
		return nil, fmt.Errorf("member key must be string, got %T", member)
	}

	// Handle map
	if m, ok := obj.(map[string]Value); ok {
		if val, ok := m[key]; ok {
			return val, nil
		}
		return nil, nil
	}

	// Handle object methods
	if objMap, ok := obj.(ValueObject); ok {
		return objMap.GetMember(key)
	}

	// Handle builtin objects (http, db, etc.)
	switch o := obj.(type) {
	case *HTTPObject:
		return o.GetMember(key)
	case *DBObject:
		return o.GetMember(key)
	}

	return nil, fmt.Errorf("cannot access member of %T", obj)
}

func (i *Interpreter) evalIndex(expr *IndexExpr) (Value, error) {
	obj, err := i.evaluate(expr.Object)
	if err != nil {
		return nil, err
	}

	index, err := i.evaluate(expr.Index)
	if err != nil {
		return nil, err
	}

	switch o := obj.(type) {
	case []Value:
		idx, ok := index.(int)
		if !ok {
			if f, ok := index.(float64); ok {
				idx = int(f)
			} else {
				return nil, fmt.Errorf("array index must be integer, got %T", index)
			}
		}
		if idx < 0 || idx >= len(o) {
			return nil, nil
		}
		return o[idx], nil
	case map[string]Value:
		key, ok := index.(string)
		if !ok {
			return nil, fmt.Errorf("map key must be string, got %T", index)
		}
		if val, ok := o[key]; ok {
			return val, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot index %T", obj)
	}
}

func (i *Interpreter) evalAssign(expr *AssignExpr) (Value, error) {
	val, err := i.evaluate(expr.Value)
	if err != nil {
		return nil, err
	}

	switch left := expr.Left.(type) {
	case *IdentExpr:
		i.ctx.Variables[left.Name] = val
	case *MemberExpr:
		obj, err := i.evaluate(left.Object)
		if err != nil {
			return nil, err
		}
		member, err := i.evaluate(left.Member)
		if err != nil {
			return nil, err
		}
		key, ok := member.(string)
		if !ok {
			return nil, fmt.Errorf("member key must be string")
		}
		if m, ok := obj.(map[string]Value); ok {
			m[key] = val
		} else {
			return nil, fmt.Errorf("cannot assign to member of %T", obj)
		}
	case *IndexExpr:
		obj, err := i.evaluate(left.Object)
		if err != nil {
			return nil, err
		}
		index, err := i.evaluate(left.Index)
		if err != nil {
			return nil, err
		}
		switch o := obj.(type) {
		case []Value:
			idx := i.toInt(index)
			if idx >= 0 && idx < len(o) {
				o[idx] = val
			}
		case map[string]Value:
			key, ok := index.(string)
			if ok {
				o[key] = val
			}
		}
	default:
		return nil, fmt.Errorf("invalid assignment target")
	}

	return val, nil
}

// Helper methods

func (i *Interpreter) isTruthy(val Value) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int, int64, float64:
		return v != 0
	case string:
		return v != ""
	case []Value:
		return len(v) > 0
	case map[string]Value:
		return len(v) > 0
	default:
		return true
	}
}

func (i *Interpreter) equal(a, b Value) bool {
	return reflect.DeepEqual(a, b)
}

func (i *Interpreter) compare(a, b Value) int {
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		case int64:
			if int64(av) < bv {
				return -1
			} else if int64(av) > bv {
				return 1
			}
			return 0
		case float64:
			if float64(av) < bv {
				return -1
			} else if float64(av) > bv {
				return 1
			}
			return 0
		}
	case int64:
		switch bv := b.(type) {
		case int:
			if av < int64(bv) {
				return -1
			} else if av > int64(bv) {
				return 1
			}
			return 0
		case int64:
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		case float64:
			if float64(av) < bv {
				return -1
			} else if float64(av) > bv {
				return 1
			}
			return 0
		}
	case float64:
		switch bv := b.(type) {
		case int:
			if av < float64(bv) {
				return -1
			} else if av > float64(bv) {
				return 1
			}
			return 0
		case int64:
			if av < float64(bv) {
				return -1
			} else if av > float64(bv) {
				return 1
			}
			return 0
		case float64:
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case string:
		switch bv := b.(type) {
		case string:
			return strings.Compare(av, bv)
		}
	}
	return 0
}

func (i *Interpreter) add(a, b Value) (Value, error) {
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av + bv, nil
		case int64:
			return int64(av) + bv, nil
		case float64:
			return float64(av) + bv, nil
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av + int64(bv), nil
		case int64:
			return av + bv, nil
		case float64:
			return float64(av) + bv, nil
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av + float64(bv), nil
		case int64:
			return av + float64(bv), nil
		case float64:
			return av + bv, nil
		}
	case string:
		switch bv := b.(type) {
		case string:
			return av + bv, nil
		}
	}
	return nil, fmt.Errorf("cannot add %T and %T", a, b)
}

func (i *Interpreter) sub(a, b Value) (Value, error) {
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av - bv, nil
		case int64:
			return int64(av) - bv, nil
		case float64:
			return float64(av) - bv, nil
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av - int64(bv), nil
		case int64:
			return av - bv, nil
		case float64:
			return float64(av) - bv, nil
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av - float64(bv), nil
		case int64:
			return av - float64(bv), nil
		case float64:
			return av - bv, nil
		}
	}
	return nil, fmt.Errorf("cannot subtract %T and %T", a, b)
}

func (i *Interpreter) mul(a, b Value) (Value, error) {
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av * bv, nil
		case int64:
			return int64(av) * bv, nil
		case float64:
			return float64(av) * bv, nil
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av * int64(bv), nil
		case int64:
			return av * bv, nil
		case float64:
			return float64(av) * bv, nil
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av * float64(bv), nil
		case int64:
			return av * float64(bv), nil
		case float64:
			return av * bv, nil
		}
	}
	return nil, fmt.Errorf("cannot multiply %T and %T", a, b)
}

func (i *Interpreter) div(a, b Value) (Value, error) {
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(av) / float64(bv), nil
		case int64:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(av) / float64(bv), nil
		case float64:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(av) / bv, nil
		}
	case int64:
		switch bv := b.(type) {
		case int:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(av) / float64(bv), nil
		case int64:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(av) / float64(bv), nil
		case float64:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(av) / bv, nil
		}
	case float64:
		switch bv := b.(type) {
		case int:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return av / float64(bv), nil
		case int64:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return av / float64(bv), nil
		case float64:
			if bv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return av / bv, nil
		}
	}
	return nil, fmt.Errorf("cannot divide %T and %T", a, b)
}

func (i *Interpreter) mod(a, b Value) (Value, error) {
	av := i.toInt(a)
	bv := i.toInt(b)
	if bv == 0 {
		return nil, fmt.Errorf("modulo by zero")
	}
	return av % bv, nil
}

func (i *Interpreter) toInt(val Value) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		var n int
		fmt.Sscanf(v, "%d", &n)
		return n
	default:
		return 0
	}
}

func (i *Interpreter) callUserFunc(fn *UserFunc, args []Value) (Value, error) {
	// Save old variables
	oldVars := make(map[string]Value)
	for k, v := range i.ctx.Variables {
		oldVars[k] = v
	}

	// Bind parameters
	for idx, param := range fn.Params {
		if idx < len(args) {
			i.ctx.Variables[param] = args[idx]
		} else {
			i.ctx.Variables[param] = nil
		}
	}

	// Execute body
	i.ctx.returning = false
	i.ctx.returnValue = nil
	_, err := i.executeBlockStmt(fn.Body)

	// Restore variables
	i.ctx.Variables = oldVars

	if err != nil {
		return nil, err
	}

	return i.ctx.returnValue, nil
}

// Callable interface for callable values
type Callable interface {
	Call(args []Value) (Value, error)
}

// ValueObject interface for objects with members
type ValueObject interface {
	GetMember(name string) (Value, error)
}

// ============================================================================
// Built-in Functions and Objects
// ============================================================================

func (i *Interpreter) callBuiltin(name string, args []Value) (Value, bool) {
	switch name {
	case "len":
		return i.builtinLen(args), true
	case "print", "println":
		return i.builtinPrint(name == "println", args), true
	case "sprintf":
		return i.builtinSprintf(args), true
	case "json":
		return i.builtinJSON(args), true
	case "jsonParse":
		return i.builtinJSONParse(args), true
	case "now":
		return i.builtinNow(args), true
	case "rand":
		return i.builtinRand(args), true
	case "formatTime":
		return i.builtinFormatTime(args), true
	case "parseTime":
		return i.builtinParseTime(args), true
	case "int":
		return i.builtinInt(args), true
	case "float":
		return i.builtinFloat(args), true
	case "string":
		return i.builtinString(args), true
	case "typeof":
		return i.builtinTypeof(args), true
	case "keys":
		return i.builtinKeys(args), true
	case "values":
		return i.builtinValues(args), true
	case "range":
		return i.builtinRange(args), true
	// String manipulation
	case "split":
		return i.builtinSplit(args), true
	case "join":
		return i.builtinJoin(args), true
	case "replace":
		return i.builtinReplace(args), true
	case "trim":
		return i.builtinTrim(args), true
	case "trimPrefix":
		return i.builtinTrimPrefix(args), true
	case "trimSuffix":
		return i.builtinTrimSuffix(args), true
	case "upper":
		return i.builtinUpper(args), true
	case "lower":
		return i.builtinLower(args), true
	case "hasPrefix":
		return i.builtinHasPrefix(args), true
	case "hasSuffix":
		return i.builtinHasSuffix(args), true
	case "contains":
		return i.builtinContains(args), true
	case "indexOf":
		return i.builtinIndexOf(args), true
	case "substr":
		return i.builtinSubstr(args), true
	// Extended string functions
	case "repeat":
		return i.builtinRepeat(args), true
	case "reverse":
		return i.builtinReverse(args), true
	case "padLeft":
		return i.builtinPadLeft(args), true
	case "padRight":
		return i.builtinPadRight(args), true
	case "ltrim":
		return i.builtinLTrim(args), true
	case "rtrim":
		return i.builtinRTrim(args), true
	case "count":
		return i.builtinCount(args), true
	case "lastIndexOf":
		return i.builtinLastIndexOf(args), true
	case "capitalize":
		return i.builtinCapitalize(args), true
	case "title":
		return i.builtinTitle(args), true
	case "swapCase":
		return i.builtinSwapCase(args), true
	case "isAlpha":
		return i.builtinIsAlpha(args), true
	case "isNumeric":
		return i.builtinIsNumeric(args), true
	case "isAlphaNumeric":
		return i.builtinIsAlphaNumeric(args), true
	case "isEmpty":
		return i.builtinIsEmpty(args), true
	case "truncate":
		return i.builtinTruncate(args), true
	case "wordCount":
		return i.builtinWordCount(args), true
	case "escapeHTML":
		return i.builtinEscapeHTML(args), true
	case "unescapeHTML":
		return i.builtinUnescapeHTML(args), true
	case "escapeURL":
		return i.builtinEscapeURL(args), true
	case "unescapeURL":
		return i.builtinUnescapeURL(args), true
	case "left":
		return i.builtinLeft(args), true
	case "right":
		return i.builtinRight(args), true
	case "center":
		return i.builtinCenter(args), true
	case "lines":
		return i.builtinLines(args), true
	case "words":
		return i.builtinWords(args), true
	case "startsWith":
		return i.builtinHasPrefix(args), true
	case "endsWith":
		return i.builtinHasSuffix(args), true
	// Array functions
	case "push":
		return i.builtinPush(args), true
	case "pop":
		return i.builtinPop(args), true
	case "slice":
		return i.builtinSlice(args), true
	// Math functions
	case "abs":
		return i.builtinAbs(args), true
	case "min":
		return i.builtinMin(args), true
	case "max":
		return i.builtinMax(args), true
	case "floor":
		return i.builtinFloor(args), true
	case "ceil":
		return i.builtinCeil(args), true
	case "round":
		return i.builtinRound(args), true
	case "sqrt":
		return i.builtinSqrt(args), true
	case "pow":
		return i.builtinPow(args), true
	// Trigonometric functions
	case "sin":
		return i.builtinSin(args), true
	case "cos":
		return i.builtinCos(args), true
	case "tan":
		return i.builtinTan(args), true
	case "asin":
		return i.builtinAsin(args), true
	case "acos":
		return i.builtinAcos(args), true
	case "atan":
		return i.builtinAtan(args), true
	case "atan2":
		return i.builtinAtan2(args), true
	case "sinh":
		return i.builtinSinh(args), true
	case "cosh":
		return i.builtinCosh(args), true
	case "tanh":
		return i.builtinTanh(args), true
	// Logarithm and exponential
	case "log":
		return i.builtinLog(args), true
	case "log10":
		return i.builtinLog10(args), true
	case "log2":
		return i.builtinLog2(args), true
	case "exp":
		return i.builtinExp(args), true
	// Other math
	case "cbrt":
		return i.builtinCbrt(args), true
	case "hypot":
		return i.builtinHypot(args), true
	case "sign":
		return i.builtinSign(args), true
	case "mod":
		return i.builtinMod(args), true
	case "div":
		return i.builtinDiv(args), true
	case "clamp":
		return i.builtinClamp(args), true
	case "lerp":
		return i.builtinLerp(args), true
	case "degrees":
		return i.builtinDegrees(args), true
	case "radians":
		return i.builtinRadians(args), true
	case "isInf":
		return i.builtinIsInf(args), true
	case "isNaN":
		return i.builtinIsNaN(args), true
	// Number theory
	case "factorial":
		return i.builtinFactorial(args), true
	case "gcd":
		return i.builtinGCD(args), true
	case "lcm":
		return i.builtinLCM(args), true
	case "isPrime":
		return i.builtinIsPrime(args), true
	case "fibonacci":
		return i.builtinFibonacci(args), true
	case "binomial":
		return i.builtinBinomial(args), true
	// Statistics
	case "sum":
		return i.builtinSum(args), true
	case "product":
		return i.builtinProduct(args), true
	case "mean":
		return i.builtinMean(args), true
	case "median":
		return i.builtinMedian(args), true
	case "variance":
		return i.builtinVariance(args), true
	case "stddev":
		return i.builtinStddev(args), true
	case "percentile":
		return i.builtinPercentile(args), true
	// Random
	case "random":
		return i.builtinRandom(args), true
	case "randomInt":
		return i.builtinRandomInt(args), true
	case "randomFloat":
		return i.builtinRandomFloat(args), true
	case "shuffle":
		return i.builtinShuffle(args), true
	case "sample":
		return i.builtinSample(args), true
	// Data processing - array functions
	case "sort":
		return i.builtinSort(args), true
	case "sortDesc":
		return i.builtinSortDesc(args), true
	case "arrayReverse":
		return i.builtinArrayReverse(args), true
	case "unique":
		return i.builtinUnique(args), true
	case "flatten":
		return i.builtinFlatten(args), true
	case "chunk":
		return i.builtinChunk(args), true
	case "take":
		return i.builtinTake(args), true
	case "drop":
		return i.builtinDrop(args), true
	case "first":
		return i.builtinFirst(args), true
	case "last":
		return i.builtinLast(args), true
	case "nth":
		return i.builtinNth(args), true
	case "find":
		return i.builtinFind(args), true
	case "filter":
		return i.builtinFilter(args), true
	case "map":
		return i.builtinMap(args), true
	case "reduce":
		return i.builtinReduce(args), true
	case "every":
		return i.builtinEvery(args), true
	case "some":
		return i.builtinSome(args), true
	case "countBy":
		return i.builtinCountBy(args), true
	case "groupBy":
		return i.builtinGroupBy(args), true
	case "zip":
		return i.builtinZip(args), true
	case "unzip":
		return i.builtinUnzip(args), true
	case "intersection":
		return i.builtinIntersection(args), true
	case "union":
		return i.builtinUnion(args), true
	case "difference":
		return i.builtinDifference(args), true
	// Crypto/Hash functions
	case "md5":
		return i.builtinMD5(args), true
	case "sha1":
		return i.builtinSHA1(args), true
	case "sha256":
		return i.builtinSHA256(args), true
	case "sha512":
		return i.builtinSHA512(args), true
	case "base64Encode":
		return i.builtinBase64Encode(args), true
	case "base64Decode":
		return i.builtinBase64Decode(args), true
	case "hexEncode":
		return i.builtinHexEncode(args), true
	case "hexDecode":
		return i.builtinHexDecode(args), true
	case "hmacSHA256":
		return i.builtinHmacSHA256(args), true
	// File operations
	case "fileSave":
		return i.builtinFileSave(args), true
	case "fileRead":
		return i.builtinFileRead(args), true
	case "fileDelete":
		return i.builtinFileDelete(args), true
	case "fileExists":
		return i.builtinFileExists(args), true
	case "dirList":
		return i.builtinDirList(args), true
	case "dirCreate":
		return i.builtinDirCreate(args), true
	case "dirDelete":
		return i.builtinDirDelete(args), true
	case "fileServe":
		return i.builtinFileServe(args), true
	// Path operations
	case "pathJoin":
		return i.builtinPathJoin(args), true
	case "pathBase":
		return i.builtinPathBase(args), true
	case "pathDir":
		return i.builtinPathDir(args), true
	case "pathExt":
		return i.builtinPathExt(args), true
	case "pathAbs":
		return i.builtinPathAbs(args), true
	case "pathClean":
		return i.builtinPathClean(args), true
	case "pathSplit":
		return i.builtinPathSplit(args), true
	case "pathIsAbs":
		return i.builtinPathIsAbs(args), true
	// Extended file operations
	case "fileInfo":
		return i.builtinFileInfo(args), true
	case "fileCopy":
		return i.builtinFileCopy(args), true
	case "fileMove":
		return i.builtinFileMove(args), true
	case "fileRename":
		return i.builtinFileMove(args), true // alias
	case "fileSize":
		return i.builtinFileSize(args), true
	case "fileModTime":
		return i.builtinFileModTime(args), true
	case "fileIsDir":
		return i.builtinFileIsDir(args), true
	case "fileWalk":
		return i.builtinFileWalk(args), true
	case "fileAppend":
		return i.builtinFileAppend(args), true
	case "fileGlob":
		return i.builtinFileGlob(args), true
	case "fileTouch":
		return i.builtinFileTouch(args), true
	case "dirExists":
		return i.builtinDirExists(args), true
	case "dirCopy":
		return i.builtinDirCopy(args), true
	case "dirWalk":
		return i.builtinFileWalk(args), true // alias
	// HTTP Client functions
	case "httpGet":
		return i.builtinHTTPGet(args), true
	case "httpPost":
		return i.builtinHTTPPost(args), true
	case "httpPut":
		return i.builtinHTTPPut(args), true
	case "httpDelete":
		return i.builtinHTTPDelete(args), true
	case "httpRequest":
		return i.builtinHTTPRequest(args), true
	// URL functions
	case "urlParse":
		return i.builtinURLParse(args), true
	case "urlEncode":
		return i.builtinURLEncode(args), true
	case "urlDecode":
		return i.builtinURLDecode(args), true
	case "urlJoin":
		return i.builtinURLJoin(args), true
	case "urlBuild":
		return i.builtinURLBuild(args), true
	// DNS and IP functions
	case "dnsLookup":
		return i.builtinDNSLookup(args), true
	case "dnsLookupHost":
		return i.builtinDNSLookupHost(args), true
	case "dnsLookupAddr":
		return i.builtinDNSLookupAddr(args), true
	case "ipParse":
		return i.builtinIPParse(args), true
	case "isIPv4":
		return i.builtinIsIPv4(args), true
	case "isIPv6":
		return i.builtinIsIPv6(args), true
	// JSON functions
	case "jsonEncode":
		return i.builtinJSONEncode(args), true
	case "jsonDecode":
		return i.builtinJSONDecode(args), true
	case "jsonPretty":
		return i.builtinJSONPretty(args), true
	// OS - Environment variables
	case "env":
		return i.builtinEnv(args), true
	case "envSet":
		return i.builtinEnvSet(args), true
	case "envUnset":
		return i.builtinEnvUnset(args), true
	case "envList":
		return i.builtinEnvList(args), true
	// OS - Process info
	case "pid":
		return i.builtinPID(args), true
	case "ppid":
		return i.builtinPPID(args), true
	case "uid":
		return i.builtinUID(args), true
	case "gid":
		return i.builtinGID(args), true
	// OS - System info
	case "hostname":
		return i.builtinHostname(args), true
	case "osInfo":
		return i.builtinOSInfo(args), true
	case "arch":
		return i.builtinArch(args), true
	case "cwd":
		return i.builtinCwd(args), true
	case "home":
		return i.builtinHome(args), true
	case "tempDir":
		return i.builtinTempDir(args), true
	// OS - Time functions
	case "sleep":
		return i.builtinSleep(args), true
	case "clock":
		return i.builtinClock(args), true
	case "timestamp":
		return i.builtinTimestamp(args), true
	case "dateParts":
		return i.builtinDateParts(args), true
	// OS - Command execution
	case "exec":
		return i.builtinExec(args), true
	case "execOutput":
		return i.builtinExecOutput(args), true
	// OS - Memory and CPU
	case "memStats":
		return i.builtinMemStats(args), true
	case "goroutines":
		return i.builtinGoroutines(args), true
	// OS - User
	case "userHome":
		return i.builtinUserHome(args), true
	case "userCache":
		return i.builtinUserCache(args), true
	case "userConfig":
		return i.builtinUserConfig(args), true
	// OS - File permissions
	case "chmod":
		return i.builtinChmod(args), true
	case "chown":
		return i.builtinChown(args), true
	// OS - Exit
	case "exit":
		return i.builtinExit(args), true
	// Format - Number formatting
	case "formatNumber":
		return i.builtinFormatNumber(args), true
	case "formatFloat":
		return i.builtinFormatFloat(args), true
	case "formatInt":
		return i.builtinFormatInt(args), true
	case "formatCurrency":
		return i.builtinFormatCurrency(args), true
	case "formatPercent":
		return i.builtinFormatPercent(args), true
	case "formatBytes":
		return i.builtinFormatBytes(args), true
	// Format - Date/Time
	case "formatDate":
		return i.builtinFormatDate(args), true
	case "parseDate":
		return i.builtinParseDate(args), true
	case "formatDuration":
		return i.builtinFormatDuration(args), true
	case "parseDuration":
		return i.builtinParseDuration(args), true
	// Format - Text
	case "indent":
		return i.builtinIndent(args), true
	case "wrap":
		return i.builtinWrap(args), true
	case "align":
		return i.builtinAlign(args), true
	case "alignLeft":
		return i.builtinAlignLeft(args), true
	case "alignRight":
		return i.builtinAlignRight(args), true
	case "alignCenter":
		return i.builtinAlignCenter(args), true
	// Format - Table/CSV
	case "table":
		return i.builtinTable(args), true
	case "csv":
		return i.builtinCSV(args), true
	case "csvParse":
		return i.builtinCSVParse(args), true
	// Format - Other
	case "printf":
		return i.builtinPrintf(args), true
	case "padNumber":
		return i.builtinPadNumber(args), true
	case "toRoman":
		return i.builtinToRoman(args), true
	case "fromRoman":
		return i.builtinFromRoman(args), true
	case "toWords":
		return i.builtinToWords(args), true
	case "toOrdinal":
		return i.builtinToOrdinal(args), true
	default:
		return nil, false
	}
}

func (i *Interpreter) builtinLen(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	switch v := args[0].(type) {
	case string:
		return len(v)
	case []Value:
		return len(v)
	case map[string]Value:
		return len(v)
	default:
		return 0
	}
}

func (i *Interpreter) builtinPrint(newline bool, args []Value) Value {
	var parts []string
	for _, arg := range args {
		parts = append(parts, fmt.Sprintf("%v", arg))
	}
	output := strings.Join(parts, " ")
	if newline {
		output += "\n"
	}
	// Write to HTTP response if available
	if i.ctx.HTTPWriter != nil {
		i.ctx.HTTPWriter.Write([]byte(output))
	} else {
		fmt.Print(output)
	}
	return nil
}

func (i *Interpreter) builtinSprintf(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	format, ok := args[0].(string)
	if !ok {
		return fmt.Sprintf("%v", args[0])
	}

	var formatArgs []interface{}
	for _, arg := range args[1:] {
		formatArgs = append(formatArgs, arg)
	}
	return fmt.Sprintf(format, formatArgs...)
}

func (i *Interpreter) builtinJSON(args []Value) Value {
	if len(args) == 0 {
		return "null"
	}
	data, err := json.Marshal(args[0])
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return string(data)
}

func (i *Interpreter) builtinNow(args []Value) Value {
	return time.Now().Unix()
}

func (i *Interpreter) builtinRand(args []Value) Value {
	return time.Now().UnixNano() % 1000000000
}

func (i *Interpreter) builtinInt(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	switch v := args[0].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		var n int
		fmt.Sscanf(v, "%d", &n)
		return n
	default:
		return 0
	}
}

func (i *Interpreter) builtinFloat(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	switch v := args[0].(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f
	default:
		return 0.0
	}
}

func (i *Interpreter) builtinString(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	return fmt.Sprintf("%v", args[0])
}

func (i *Interpreter) builtinTypeof(args []Value) Value {
	if len(args) == 0 {
		return "null"
	}
	switch args[0].(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int, int64:
		return "int"
	case float64:
		return "float"
	case string:
		return "string"
	case []Value:
		return "array"
	case map[string]Value:
		return "object"
	default:
		return "unknown"
	}
}

func (i *Interpreter) builtinKeys(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	switch v := args[0].(type) {
	case map[string]Value:
		keys := make([]Value, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		return keys
	default:
		return []Value{}
	}
}

func (i *Interpreter) builtinValues(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	switch v := args[0].(type) {
	case map[string]Value:
		values := make([]Value, 0, len(v))
		for _, val := range v {
			values = append(values, val)
		}
		return values
	default:
		return []Value{}
	}
}

func (i *Interpreter) builtinRange(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	n := i.toInt(args[0])
	result := make([]Value, n)
	for i := 0; i < n; i++ {
		result[i] = i
	}
	return result
}

// ============================================================================
// Additional Built-in Functions
// ============================================================================

func (i *Interpreter) builtinJSONParse(args []Value) Value {
	if len(args) == 0 {
		return nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil
	}
	var result interface{}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil
	}
	return convertJSONToValue(result)
}

func convertJSONToValue(v interface{}) Value {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case float64:
		return val
	case string:
		return val
	case []interface{}:
		arr := make([]Value, len(val))
		for i, item := range val {
			arr[i] = convertJSONToValue(item)
		}
		return arr
	case map[string]interface{}:
		m := make(map[string]Value)
		for k, v := range val {
			m[k] = convertJSONToValue(v)
		}
		return m
	default:
		return nil
	}
}

func (i *Interpreter) builtinFormatTime(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	timestamp, ok := args[0].(float64)
	if !ok {
		timestamp = float64(i.toInt(args[0]))
	}
	format, ok := args[1].(string)
	if !ok {
		return ""
	}
	t := time.Unix(int64(timestamp), 0)
	return t.Format(format)
}

func (i *Interpreter) builtinParseTime(args []Value) Value {
	if len(args) < 2 {
		return 0
	}
	timeStr, ok := args[0].(string)
	if !ok {
		return 0
	}
	format, ok := args[1].(string)
	if !ok {
		return 0
	}
	t, err := time.Parse(format, timeStr)
	if err != nil {
		return 0
	}
	return t.Unix()
}

// String manipulation functions

func (i *Interpreter) builtinSplit(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}
	sep, ok := args[1].(string)
	if !ok {
		return []Value{}
	}
	parts := strings.Split(s, sep)
	result := make([]Value, len(parts))
	for i, p := range parts {
		result[i] = p
	}
	return result
}

func (i *Interpreter) builtinJoin(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return ""
	}
	sep, ok := args[1].(string)
	if !ok {
		return ""
	}
	strs := make([]string, len(arr))
	for i, v := range arr {
		strs[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(strs, sep)
}

func (i *Interpreter) builtinReplace(args []Value) Value {
	if len(args) < 3 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	old, ok := args[1].(string)
	if !ok {
		return ""
	}
	newStr, ok := args[2].(string)
	if !ok {
		return ""
	}
	n := -1
	if len(args) > 3 {
		n = i.toInt(args[3])
	}
	return strings.Replace(s, old, newStr, n)
}

func (i *Interpreter) builtinTrim(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	cutset := " \t\n\r"
	if len(args) > 1 {
		cutset, _ = args[1].(string)
	}
	return strings.Trim(s, cutset)
}

func (i *Interpreter) builtinTrimPrefix(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	prefix, ok := args[1].(string)
	if !ok {
		return ""
	}
	return strings.TrimPrefix(s, prefix)
}

func (i *Interpreter) builtinTrimSuffix(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	suffix, ok := args[1].(string)
	if !ok {
		return ""
	}
	return strings.TrimSuffix(s, suffix)
}

func (i *Interpreter) builtinUpper(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return strings.ToUpper(s)
}

func (i *Interpreter) builtinLower(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return strings.ToLower(s)
}

func (i *Interpreter) builtinHasPrefix(args []Value) Value {
	if len(args) < 2 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}
	prefix, ok := args[1].(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(s, prefix)
}

func (i *Interpreter) builtinHasSuffix(args []Value) Value {
	if len(args) < 2 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}
	suffix, ok := args[1].(string)
	if !ok {
		return false
	}
	return strings.HasSuffix(s, suffix)
}

func (i *Interpreter) builtinContains(args []Value) Value {
	if len(args) < 2 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}
	substr, ok := args[1].(string)
	if !ok {
		return false
	}
	return strings.Contains(s, substr)
}

func (i *Interpreter) builtinIndexOf(args []Value) Value {
	if len(args) < 2 {
		return -1
	}
	s, ok := args[0].(string)
	if !ok {
		return -1
	}
	substr, ok := args[1].(string)
	if !ok {
		return -1
	}
	return strings.Index(s, substr)
}

func (i *Interpreter) builtinSubstr(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	start := i.toInt(args[1])
	if start < 0 {
		start = 0
	}
	if start >= len(s) {
		return ""
	}
	if len(args) > 2 {
		length := i.toInt(args[2])
		end := start + length
		if end > len(s) {
			end = len(s)
		}
		return s[start:end]
	}
	return s[start:]
}

// Additional string manipulation functions

func (i *Interpreter) builtinRepeat(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	n := i.toInt(args[1])
	if n <= 0 {
		return ""
	}
	return strings.Repeat(s, n)
}

func (i *Interpreter) builtinReverse(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func (i *Interpreter) builtinPadLeft(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		s, _ := args[0].(string)
		return s
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	length := i.toInt(args[1])
	pad, ok := args[2].(string)
	if !ok || pad == "" {
		pad = " "
	}
	if len(s) >= length {
		return s
	}
	padLen := length - len(s)
	return strings.Repeat(pad, (padLen+len(pad)-1)/len(pad))[:padLen] + s
}

func (i *Interpreter) builtinPadRight(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		s, _ := args[0].(string)
		return s
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	length := i.toInt(args[1])
	pad, ok := args[2].(string)
	if !ok || pad == "" {
		pad = " "
	}
	if len(s) >= length {
		return s
	}
	padLen := length - len(s)
	return s + strings.Repeat(pad, (padLen+len(pad)-1)/len(pad))[:padLen]
}

func (i *Interpreter) builtinLTrim(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	cutset := " \t\n\r"
	if len(args) > 1 {
		cutset, _ = args[1].(string)
	}
	return strings.TrimLeft(s, cutset)
}

func (i *Interpreter) builtinRTrim(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	cutset := " \t\n\r"
	if len(args) > 1 {
		cutset, _ = args[1].(string)
	}
	return strings.TrimRight(s, cutset)
}

func (i *Interpreter) builtinCount(args []Value) Value {
	if len(args) < 2 {
		return 0
	}
	s, ok := args[0].(string)
	if !ok {
		return 0
	}
	substr, ok := args[1].(string)
	if !ok {
		return 0
	}
	return strings.Count(s, substr)
}

func (i *Interpreter) builtinLastIndexOf(args []Value) Value {
	if len(args) < 2 {
		return -1
	}
	s, ok := args[0].(string)
	if !ok {
		return -1
	}
	substr, ok := args[1].(string)
	if !ok {
		return -1
	}
	return strings.LastIndex(s, substr)
}

func (i *Interpreter) builtinCapitalize(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}

func (i *Interpreter) builtinTitle(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return strings.Title(s)
}

func (i *Interpreter) builtinSwapCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	var result strings.Builder
	for _, r := range s {
		if unicode.IsUpper(r) {
			result.WriteRune(unicode.ToLower(r))
		} else if unicode.IsLower(r) {
			result.WriteRune(unicode.ToUpper(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func (i *Interpreter) builtinIsAlpha(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsNumeric(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsAlphaNumeric(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsEmpty(args []Value) Value {
	if len(args) == 0 {
		return true
	}
	s, ok := args[0].(string)
	if !ok {
		return true
	}
	return strings.TrimSpace(s) == ""
}

func (i *Interpreter) builtinTruncate(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	maxLen := 100
	suffix := "..."
	if len(args) > 1 {
		maxLen = i.toInt(args[1])
	}
	if len(args) > 2 {
		suffix, _ = args[2].(string)
	}
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + suffix
}

func (i *Interpreter) builtinWordCount(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	s, ok := args[0].(string)
	if !ok {
		return 0
	}
	words := strings.Fields(s)
	return len(words)
}

func (i *Interpreter) builtinEscapeHTML(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return html.EscapeString(s)
}

func (i *Interpreter) builtinUnescapeHTML(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return html.UnescapeString(s)
}

func (i *Interpreter) builtinEscapeURL(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return url.QueryEscape(s)
}

func (i *Interpreter) builtinUnescapeURL(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	result, _ := url.QueryUnescape(s)
	return result
}

func (i *Interpreter) builtinLeft(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	n := i.toInt(args[1])
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	return s[:n]
}

func (i *Interpreter) builtinRight(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	n := i.toInt(args[1])
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	return s[len(s)-n:]
}

func (i *Interpreter) builtinCenter(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		s, _ := args[0].(string)
		return s
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	width := i.toInt(args[1])
	pad, ok := args[2].(string)
	if !ok || pad == "" {
		pad = " "
	}
	if len(s) >= width {
		return s
	}
	padLen := width - len(s)
	leftPad := padLen / 2
	rightPad := padLen - leftPad

	leftStr := strings.Repeat(pad, (leftPad+len(pad)-1)/len(pad))[:leftPad]
	rightStr := strings.Repeat(pad, (rightPad+len(pad)-1)/len(pad))[:rightPad]
	return leftStr + s + rightStr
}

func (i *Interpreter) builtinLines(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}
	lines := strings.Split(s, "\n")
	result := make([]Value, len(lines))
	for i, line := range lines {
		result[i] = strings.TrimRight(line, "\r")
	}
	return result
}

func (i *Interpreter) builtinWords(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}
	words := strings.Fields(s)
	result := make([]Value, len(words))
	for i, word := range words {
		result[i] = word
	}
	return result
}

func (i *Interpreter) builtinStartsWith(args []Value) Value {
	return i.builtinHasPrefix(args)
}

func (i *Interpreter) builtinEndsWith(args []Value) Value {
	return i.builtinHasSuffix(args)
}

// Array functions

func (i *Interpreter) builtinPush(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{args[1]}
	}
	return append(arr, args[1])
}

func (i *Interpreter) builtinPop(args []Value) Value {
	if len(args) == 0 {
		return nil
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return nil
	}
	return arr[len(arr)-1]
}

func (i *Interpreter) builtinSlice(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	start := i.toInt(args[1])
	if start < 0 {
		start = 0
	}
	if start >= len(arr) {
		return []Value{}
	}
	if len(args) > 2 {
		end := i.toInt(args[2])
		if end > len(arr) {
			end = len(arr)
		}
		return arr[start:end]
	}
	return arr[start:]
}

// Math functions

func (i *Interpreter) builtinAbs(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	switch v := args[0].(type) {
	case int:
		if v < 0 {
			return -v
		}
		return v
	case int64:
		if v < 0 {
			return -v
		}
		return v
	case float64:
		if v < 0 {
			return -v
		}
		return v
	default:
		return 0
	}
}

func (i *Interpreter) builtinMin(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	minVal := i.toFloat(args[0])
	for _, v := range args[1:] {
		f := i.toFloat(v)
		if f < minVal {
			minVal = f
		}
	}
	return minVal
}

func (i *Interpreter) builtinMax(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	maxVal := i.toFloat(args[0])
	for _, v := range args[1:] {
		f := i.toFloat(v)
		if f > maxVal {
			maxVal = f
		}
	}
	return maxVal
}

func (i *Interpreter) toFloat(v Value) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}

func (i *Interpreter) builtinFloor(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	f := i.toFloat(args[0])
	return int(f)
}

func (i *Interpreter) builtinCeil(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	f := i.toFloat(args[0])
	return int(f) + 1
}

func (i *Interpreter) builtinRound(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	f := i.toFloat(args[0])
	return int(f + 0.5)
}

func (i *Interpreter) builtinSqrt(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	f := i.toFloat(args[0])
	if f < 0 {
		return 0
	}
	// Simple square root using Newton's method
	if f == 0 {
		return 0.0
	}
	x := f
	for i := 0; i < 20; i++ {
		x = 0.5 * (x + f/x)
	}
	return x
}

func (i *Interpreter) builtinPow(args []Value) Value {
	if len(args) < 2 {
		return 0
	}
	base := i.toFloat(args[0])
	exp := i.toFloat(args[1])
	return math.Pow(base, exp)
}

// ============================================================================
// Trigonometric Functions
// ============================================================================

func (i *Interpreter) builtinSin(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Sin(i.toFloat(args[0]))
}

func (i *Interpreter) builtinCos(args []Value) Value {
	if len(args) == 0 {
		return 1.0
	}
	return math.Cos(i.toFloat(args[0]))
}

func (i *Interpreter) builtinTan(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Tan(i.toFloat(args[0]))
}

func (i *Interpreter) builtinAsin(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Asin(i.toFloat(args[0]))
}

func (i *Interpreter) builtinAcos(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Acos(i.toFloat(args[0]))
}

func (i *Interpreter) builtinAtan(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Atan(i.toFloat(args[0]))
}

func (i *Interpreter) builtinAtan2(args []Value) Value {
	if len(args) < 2 {
		return 0.0
	}
	return math.Atan2(i.toFloat(args[0]), i.toFloat(args[1]))
}

func (i *Interpreter) builtinSinh(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Sinh(i.toFloat(args[0]))
}

func (i *Interpreter) builtinCosh(args []Value) Value {
	if len(args) == 0 {
		return 1.0
	}
	return math.Cosh(i.toFloat(args[0]))
}

func (i *Interpreter) builtinTanh(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Tanh(i.toFloat(args[0]))
}

// ============================================================================
// Logarithm and Exponential Functions
// ============================================================================

func (i *Interpreter) builtinLog(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Log(i.toFloat(args[0]))
}

func (i *Interpreter) builtinLog10(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Log10(i.toFloat(args[0]))
}

func (i *Interpreter) builtinLog2(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Log2(i.toFloat(args[0]))
}

func (i *Interpreter) builtinExp(args []Value) Value {
	if len(args) == 0 {
		return 1.0
	}
	return math.Exp(i.toFloat(args[0]))
}

// ============================================================================
// Other Math Functions
// ============================================================================

func (i *Interpreter) builtinCbrt(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return math.Cbrt(i.toFloat(args[0]))
}

func (i *Interpreter) builtinHypot(args []Value) Value {
	if len(args) < 2 {
		return 0.0
	}
	return math.Hypot(i.toFloat(args[0]), i.toFloat(args[1]))
}

func (i *Interpreter) builtinSign(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	v := i.toFloat(args[0])
	if v > 0 {
		return 1
	} else if v < 0 {
		return -1
	}
	return 0
}

func (i *Interpreter) builtinMod(args []Value) Value {
	if len(args) < 2 {
		return 0
	}
	return math.Mod(i.toFloat(args[0]), i.toFloat(args[1]))
}

func (i *Interpreter) builtinDiv(args []Value) Value {
	if len(args) < 2 {
		return 0
	}
	a := i.toFloat(args[0])
	b := i.toFloat(args[1])
	if b == 0 {
		return 0.0
	}
	return math.Trunc(a / b)
}

func (i *Interpreter) builtinClamp(args []Value) Value {
	if len(args) < 3 {
		return 0
	}
	val := i.toFloat(args[0])
	minVal := i.toFloat(args[1])
	maxVal := i.toFloat(args[2])
	return math.Max(minVal, math.Min(maxVal, val))
}

func (i *Interpreter) builtinLerp(args []Value) Value {
	if len(args) < 3 {
		return 0
	}
	start := i.toFloat(args[0])
	end := i.toFloat(args[1])
	t := i.toFloat(args[2])
	return start + (end-start)*t
}

func (i *Interpreter) builtinDegrees(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return i.toFloat(args[0]) * 180 / math.Pi
}

func (i *Interpreter) builtinRadians(args []Value) Value {
	if len(args) == 0 {
		return 0.0
	}
	return i.toFloat(args[0]) * math.Pi / 180
}

func (i *Interpreter) builtinIsInf(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	return math.IsInf(i.toFloat(args[0]), 0)
}

func (i *Interpreter) builtinIsNaN(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	return math.IsNaN(i.toFloat(args[0]))
}

// ============================================================================
// Number Theory Functions
// ============================================================================

func (i *Interpreter) builtinFactorial(args []Value) Value {
	if len(args) == 0 {
		return int64(1)
	}
	n := int64(i.toInt(args[0]))
	if n < 0 {
		return int64(0)
	}
	if n <= 1 {
		return int64(1)
	}
	result := int64(1)
	for j := int64(2); j <= n; j++ {
		result *= j
	}
	return result
}

func (i *Interpreter) builtinGCD(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	a := int64(i.toInt(args[0]))
	b := int64(i.toInt(args[1]))
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (i *Interpreter) builtinLCM(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	a := int64(i.toInt(args[0]))
	b := int64(i.toInt(args[1]))
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	if a == 0 || b == 0 {
		return int64(0)
	}
	gcd := func(x, y int64) int64 {
		for y != 0 {
			x, y = y, x%y
		}
		return x
	}
	return a / gcd(a, b) * b
}

func (i *Interpreter) builtinIsPrime(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	n := int64(i.toInt(args[0]))
	if n < 2 {
		return false
	}
	if n == 2 {
		return true
	}
	if n%2 == 0 {
		return false
	}
	for j := int64(3); j*j <= n; j += 2 {
		if n%j == 0 {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinFibonacci(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}
	n := int64(i.toInt(args[0]))
	if n <= 0 {
		return int64(0)
	}
	if n == 1 {
		return int64(1)
	}
	a, b := int64(0), int64(1)
	for j := int64(2); j <= n; j++ {
		a, b = b, a+b
	}
	return b
}

func (i *Interpreter) builtinBinomial(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	n := int64(i.toInt(args[0]))
	k := int64(i.toInt(args[1]))
	if k < 0 || k > n {
		return int64(0)
	}
	if k == 0 || k == n {
		return int64(1)
	}
	if k > n-k {
		k = n - k
	}
	result := int64(1)
	for j := int64(0); j < k; j++ {
		result = result * (n - j) / (j + 1)
	}
	return result
}

// ============================================================================
// Statistical Functions
// ============================================================================

func (i *Interpreter) builtinSum(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return i.toFloat(args[0])
	}
	var sum float64
	for _, v := range arr {
		sum += i.toFloat(v)
	}
	return sum
}

func (i *Interpreter) builtinProduct(args []Value) Value {
	if len(args) == 0 {
		return 1
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return i.toFloat(args[0])
	}
	prod := 1.0
	for _, v := range arr {
		prod *= i.toFloat(v)
	}
	return prod
}

func (i *Interpreter) builtinMean(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return 0
	}
	var sum float64
	for _, v := range arr {
		sum += i.toFloat(v)
	}
	return sum / float64(len(arr))
}

func (i *Interpreter) builtinMedian(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return 0
	}
	// Copy and sort
	vals := make([]float64, len(arr))
	for j, v := range arr {
		vals[j] = i.toFloat(v)
	}
	sort.Float64s(vals)
	n := len(vals)
	if n%2 == 0 {
		return (vals[n/2-1] + vals[n/2]) / 2
	}
	return vals[n/2]
}

func (i *Interpreter) builtinVariance(args []Value) Value {
	if len(args) == 0 {
		return 0
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return 0
	}
	// Calculate mean
	var sum float64
	for _, v := range arr {
		sum += i.toFloat(v)
	}
	mean := sum / float64(len(arr))
	// Calculate variance
	var variance float64
	for _, v := range arr {
		diff := i.toFloat(v) - mean
		variance += diff * diff
	}
	return variance / float64(len(arr))
}

func (i *Interpreter) builtinStddev(args []Value) Value {
	variance := i.builtinVariance(args)
	return math.Sqrt(variance.(float64))
}

func (i *Interpreter) builtinPercentile(args []Value) Value {
	if len(args) < 2 {
		return 0
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return 0
	}
	p := i.toFloat(args[1])
	if p < 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	// Copy and sort
	vals := make([]float64, len(arr))
	for j, v := range arr {
		vals[j] = i.toFloat(v)
	}
	sort.Float64s(vals)
	// Calculate index
	index := (p / 100) * float64(len(vals)-1)
	lower := int(index)
	upper := lower + 1
	if upper >= len(vals) {
		return vals[len(vals)-1]
	}
	frac := index - float64(lower)
	return vals[lower] + frac*(vals[upper]-vals[lower])
}

// ============================================================================
// Random Functions
// ============================================================================

func (i *Interpreter) builtinRandom(args []Value) Value {
	return rand.Float64()
}

func (i *Interpreter) builtinRandomInt(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	min := int64(i.toInt(args[0]))
	max := int64(i.toInt(args[1]))
	return min + rand.Int63n(max-min+1)
}

func (i *Interpreter) builtinRandomFloat(args []Value) Value {
	if len(args) < 2 {
		return rand.Float64()
	}
	min := i.toFloat(args[0])
	max := i.toFloat(args[1])
	return min + rand.Float64()*(max-min)
}

func (i *Interpreter) builtinShuffle(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	// Copy array
	result := make([]Value, len(arr))
	copy(result, arr)
	// Fisher-Yates shuffle
	for j := len(result) - 1; j > 0; j-- {
		k := rand.Intn(j + 1)
		result[j], result[k] = result[k], result[j]
	}
	return result
}

func (i *Interpreter) builtinSample(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return []Value{}
	}
	n := 1
	if len(args) > 1 {
		n = int(i.toInt(args[1]))
	}
	if n >= len(arr) {
		// Return shuffled copy
		return i.builtinShuffle(args)
	}
	// Reservoir sampling
	result := make([]Value, n)
	copy(result, arr[:n])
	for j := n; j < len(arr); j++ {
		k := rand.Intn(j + 1)
		if k < n {
			result[k] = arr[j]
		}
	}
	return result
}

// ============================================================================
// Data Processing - Array Functions
// ============================================================================

func (i *Interpreter) builtinSort(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	// Convert to float64s for sorting
	vals := make([]float64, len(arr))
	for j, v := range arr {
		vals[j] = i.toFloat(v)
	}
	sort.Float64s(vals)
	// Convert back
	result := make([]Value, len(vals))
	for j, v := range vals {
		result[j] = v
	}
	return result
}

func (i *Interpreter) builtinSortDesc(args []Value) Value {
	result := i.builtinSort(args)
	arr := result.([]Value)
	// Reverse
	for j, k := 0, len(arr)-1; j < k; j, k = j+1, k-1 {
		arr[j], arr[k] = arr[k], arr[j]
	}
	return arr
}

func (i *Interpreter) builtinArrayReverse(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	result := make([]Value, len(arr))
	for j, v := range arr {
		result[len(arr)-1-j] = v
	}
	return result
}

func (i *Interpreter) builtinUnique(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	seen := make(map[string]bool)
	result := []Value{}
	for _, v := range arr {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	return result
}

func (i *Interpreter) builtinFlatten(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{args[0]}
	}
	depth := -1 // infinite depth
	if len(args) > 1 {
		depth = int(i.toInt(args[1]))
	}
	return i.flattenArray(arr, depth)
}

func (i *Interpreter) flattenArray(arr []Value, depth int) []Value {
	result := []Value{}
	for _, v := range arr {
		if subArr, ok := v.([]Value); ok && depth != 0 {
			newDepth := depth
			if depth > 0 {
				newDepth--
			}
			result = append(result, i.flattenArray(subArr, newDepth)...)
		} else {
			result = append(result, v)
		}
	}
	return result
}

func (i *Interpreter) builtinChunk(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	size := int(i.toInt(args[1]))
	if size <= 0 {
		return []Value{}
	}
	result := []Value{}
	for j := 0; j < len(arr); j += size {
		end := j + size
		if end > len(arr) {
			end = len(arr)
		}
		result = append(result, arr[j:end])
	}
	return result
}

func (i *Interpreter) builtinTake(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	n := int(i.toInt(args[1]))
	if n <= 0 {
		return []Value{}
	}
	if n > len(arr) {
		n = len(arr)
	}
	return arr[:n]
}

func (i *Interpreter) builtinDrop(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	n := int(i.toInt(args[1]))
	if n <= 0 {
		return arr
	}
	if n >= len(arr) {
		return []Value{}
	}
	return arr[n:]
}

func (i *Interpreter) builtinFirst(args []Value) Value {
	if len(args) == 0 {
		return nil
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return nil
	}
	return arr[0]
}

func (i *Interpreter) builtinLast(args []Value) Value {
	if len(args) == 0 {
		return nil
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return nil
	}
	return arr[len(arr)-1]
}

func (i *Interpreter) builtinNth(args []Value) Value {
	if len(args) < 2 {
		return nil
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return nil
	}
	n := int(i.toInt(args[1]))
	if n < 0 {
		n = len(arr) + n
	}
	if n < 0 || n >= len(arr) {
		return nil
	}
	return arr[n]
}

func (i *Interpreter) builtinFind(args []Value) Value {
	// find(arr, predicate) - simplified: find(arr, value)
	if len(args) < 2 {
		return nil
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return nil
	}
	for _, v := range arr {
		if fmt.Sprintf("%v", v) == fmt.Sprintf("%v", args[1]) {
			return v
		}
	}
	return nil
}

func (i *Interpreter) builtinFilter(args []Value) Value {
	// filter(arr, type) - filter by type or condition
	if len(args) < 2 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	typeStr, _ := args[1].(string)
	result := []Value{}
	for _, v := range arr {
		switch typeStr {
		case "number", "numeric":
			if _, ok := v.(float64); ok {
				result = append(result, v)
			} else if _, ok := v.(int); ok {
				result = append(result, v)
			} else if _, ok := v.(int64); ok {
				result = append(result, v)
			}
		case "string":
			if _, ok := v.(string); ok {
				result = append(result, v)
			}
		case "array":
			if _, ok := v.([]Value); ok {
				result = append(result, v)
			}
		case "map", "object":
			if _, ok := v.(map[string]Value); ok {
				result = append(result, v)
			}
		case "null", "nil":
			if v == nil {
				result = append(result, v)
			}
		default:
			// Try numeric comparison
			if i.toFloat(v) == i.toFloat(args[1]) {
				result = append(result, v)
			}
		}
	}
	return result
}

func (i *Interpreter) builtinMap(args []Value) Value {
	// map(arr, operation) - apply operation to each element
	if len(args) < 2 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}
	op, _ := args[1].(string)
	result := make([]Value, len(arr))
	for j, v := range arr {
		switch op {
		case "abs":
			result[j] = math.Abs(i.toFloat(v))
		case "floor":
			result[j] = math.Floor(i.toFloat(v))
		case "ceil":
			result[j] = math.Ceil(i.toFloat(v))
		case "round":
			result[j] = math.Round(i.toFloat(v))
		case "sqrt":
			result[j] = math.Sqrt(i.toFloat(v))
		case "neg", "negate":
			result[j] = -i.toFloat(v)
		case "square":
			f := i.toFloat(v)
			result[j] = f * f
		case "double":
			result[j] = i.toFloat(v) * 2
		case "half":
			result[j] = i.toFloat(v) / 2
		case "upper":
			result[j] = strings.ToUpper(fmt.Sprintf("%v", v))
		case "lower":
			result[j] = strings.ToLower(fmt.Sprintf("%v", v))
		case "trim":
			result[j] = strings.TrimSpace(fmt.Sprintf("%v", v))
		case "string":
			result[j] = fmt.Sprintf("%v", v)
		case "int":
			result[j] = int(i.toFloat(v))
		case "float":
			result[j] = i.toFloat(v)
		case "len":
			if s, ok := v.(string); ok {
				result[j] = len(s)
			} else if a, ok := v.([]Value); ok {
				result[j] = len(a)
			} else {
				result[j] = 0
			}
		default:
			result[j] = v
		}
	}
	return result
}

func (i *Interpreter) builtinReduce(args []Value) Value {
	// reduce(arr, operation, initial)
	if len(args) < 2 {
		return 0
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		if len(args) > 2 {
			return args[2]
		}
		return 0
	}
	op, _ := args[1].(string)
	var acc Value
	if len(args) > 2 {
		acc = args[2]
	} else {
		acc = arr[0]
		arr = arr[1:]
	}

	for _, v := range arr {
		switch op {
		case "sum", "+":
			acc = i.toFloat(acc) + i.toFloat(v)
		case "product", "*":
			acc = i.toFloat(acc) * i.toFloat(v)
		case "sub", "-":
			acc = i.toFloat(acc) - i.toFloat(v)
		case "div", "/":
			if i.toFloat(v) != 0 {
				acc = i.toFloat(acc) / i.toFloat(v)
			}
		case "min":
			if i.toFloat(v) < i.toFloat(acc) {
				acc = v
			}
		case "max":
			if i.toFloat(v) > i.toFloat(acc) {
				acc = v
			}
		case "concat":
			acc = fmt.Sprintf("%v%v", acc, v)
		default:
			acc = i.toFloat(acc) + i.toFloat(v)
		}
	}
	return acc
}

func (i *Interpreter) builtinEvery(args []Value) Value {
	// every(arr, type) - check if all elements match
	if len(args) < 2 {
		return false
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return true
	}
	typeStr, _ := args[1].(string)
	for _, v := range arr {
		match := false
		switch typeStr {
		case "number", "numeric":
			_, match = v.(float64)
			if !match {
				_, match = v.(int)
			}
			if !match {
				_, match = v.(int64)
			}
		case "string":
			_, match = v.(string)
		case "array":
			_, match = v.([]Value)
		case "map", "object":
			_, match = v.(map[string]Value)
		case "null", "nil":
			match = v == nil
		case "positive":
			match = i.toFloat(v) > 0
		case "negative":
			match = i.toFloat(v) < 0
		case "even":
			match = int(i.toFloat(v))%2 == 0
		case "odd":
			match = int(i.toFloat(v))%2 != 0
		}
		if !match {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinSome(args []Value) Value {
	// some(arr, type) - check if any element matches
	if len(args) < 2 {
		return false
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return false
	}
	typeStr, _ := args[1].(string)
	for _, v := range arr {
		match := false
		switch typeStr {
		case "number", "numeric":
			_, match = v.(float64)
			if !match {
				_, match = v.(int)
			}
			if !match {
				_, match = v.(int64)
			}
		case "string":
			_, match = v.(string)
		case "array":
			_, match = v.([]Value)
		case "map", "object":
			_, match = v.(map[string]Value)
		case "null", "nil":
			match = v == nil
		case "positive":
			match = i.toFloat(v) > 0
		case "negative":
			match = i.toFloat(v) < 0
		case "even":
			match = int(i.toFloat(v))%2 == 0
		case "odd":
			match = int(i.toFloat(v))%2 != 0
		}
		if match {
			return true
		}
	}
	return false
}

func (i *Interpreter) builtinCountBy(args []Value) Value {
	// countBy(arr, keyFn) - count occurrences by key
	if len(args) < 2 {
		return map[string]Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return map[string]Value{}
	}
	keyType, _ := args[1].(string)
	result := map[string]Value{}
	for _, v := range arr {
		var key string
		switch keyType {
		case "type":
			switch v.(type) {
			case float64:
				key = "number"
			case int, int64:
				key = "number"
			case string:
				key = "string"
			case []Value:
				key = "array"
			case map[string]Value:
				key = "object"
			case nil:
				key = "null"
			default:
				key = "unknown"
			}
		case "sign":
			f := i.toFloat(v)
			if f > 0 {
				key = "positive"
			} else if f < 0 {
				key = "negative"
			} else {
				key = "zero"
			}
		case "parity":
			if int(i.toFloat(v))%2 == 0 {
				key = "even"
			} else {
				key = "odd"
			}
		default:
			key = fmt.Sprintf("%v", v)
		}
		if count, ok := result[key]; ok {
			result[key] = i.toInt(count) + 1
		} else {
			result[key] = int64(1)
		}
	}
	return result
}

func (i *Interpreter) builtinGroupBy(args []Value) Value {
	// groupBy(arr, keyFn) - group elements by key
	if len(args) < 2 {
		return map[string]Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok {
		return map[string]Value{}
	}
	keyType, _ := args[1].(string)
	result := map[string]Value{}
	for _, v := range arr {
		var key string
		switch keyType {
		case "type":
			switch v.(type) {
			case float64, int, int64:
				key = "number"
			case string:
				key = "string"
			case []Value:
				key = "array"
			case map[string]Value:
				key = "object"
			case nil:
				key = "null"
			default:
				key = "unknown"
			}
		case "sign":
			f := i.toFloat(v)
			if f > 0 {
				key = "positive"
			} else if f < 0 {
				key = "negative"
			} else {
				key = "zero"
			}
		case "parity":
			if int(i.toFloat(v))%2 == 0 {
				key = "even"
			} else {
				key = "odd"
			}
		default:
			key = fmt.Sprintf("%v", v)
		}
		if group, ok := result[key]; ok {
			result[key] = append(group.([]Value), v)
		} else {
			result[key] = []Value{v}
		}
	}
	return result
}

func (i *Interpreter) builtinZip(args []Value) Value {
	// zip(arr1, arr2) - combine arrays element-wise
	if len(args) < 2 {
		return []Value{}
	}
	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	if !ok1 || !ok2 {
		return []Value{}
	}
	minLen := len(arr1)
	if len(arr2) < minLen {
		minLen = len(arr2)
	}
	result := make([]Value, minLen)
	for j := 0; j < minLen; j++ {
		result[j] = []Value{arr1[j], arr2[j]}
	}
	return result
}

func (i *Interpreter) builtinUnzip(args []Value) Value {
	// unzip(arr) - separate pairs
	if len(args) == 0 {
		return []Value{}
	}
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return []Value{}
	}
	arr1, arr2 := []Value{}, []Value{}
	for _, v := range arr {
		if pair, ok := v.([]Value); ok && len(pair) >= 2 {
			arr1 = append(arr1, pair[0])
			arr2 = append(arr2, pair[1])
		}
	}
	return []Value{arr1, arr2}
}

func (i *Interpreter) builtinIntersection(args []Value) Value {
	// intersection(arr1, arr2) - common elements
	if len(args) < 2 {
		return []Value{}
	}
	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	if !ok1 || !ok2 {
		return []Value{}
	}
	set := make(map[string]bool)
	for _, v := range arr1 {
		set[fmt.Sprintf("%v", v)] = true
	}
	result := []Value{}
	seen := make(map[string]bool)
	for _, v := range arr2 {
		key := fmt.Sprintf("%v", v)
		if set[key] && !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	return result
}

func (i *Interpreter) builtinUnion(args []Value) Value {
	// union(arr1, arr2) - all unique elements
	if len(args) < 2 {
		return []Value{}
	}
	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	if !ok1 {
		arr1 = []Value{}
	}
	if !ok2 {
		arr2 = []Value{}
	}
	combined := append(arr1, arr2...)
	return i.builtinUnique([]Value{combined})
}

func (i *Interpreter) builtinDifference(args []Value) Value {
	// difference(arr1, arr2) - elements in arr1 not in arr2
	if len(args) < 2 {
		return []Value{}
	}
	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	if !ok1 {
		return []Value{}
	}
	if !ok2 {
		return arr1
	}
	set := make(map[string]bool)
	for _, v := range arr2 {
		set[fmt.Sprintf("%v", v)] = true
	}
	result := []Value{}
	for _, v := range arr1 {
		if !set[fmt.Sprintf("%v", v)] {
			result = append(result, v)
		}
	}
	return result
}

// ============================================================================
// Crypto/Hash Functions
// ============================================================================

// builtinMD5 computes MD5 hash of a string
func (i *Interpreter) builtinMD5(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

// builtinSHA1 computes SHA1 hash of a string
func (i *Interpreter) builtinSHA1(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	hash := sha1.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

// builtinSHA256 computes SHA256 hash of a string
func (i *Interpreter) builtinSHA256(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// builtinSHA512 computes SHA512 hash of a string
func (i *Interpreter) builtinSHA512(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	hash := sha512.Sum512([]byte(s))
	return hex.EncodeToString(hash[:])
}

// builtinBase64Encode encodes a string to base64
func (i *Interpreter) builtinBase64Encode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// builtinBase64Decode decodes a base64 string
func (i *Interpreter) builtinBase64Decode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		s = fmt.Sprintf("%v", args[0])
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return ""
	}
	return string(decoded)
}

// builtinHexEncode encodes a string to hex
func (i *Interpreter) builtinHexEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	return hex.EncodeToString([]byte(s))
}

// builtinHexDecode decodes a hex string
func (i *Interpreter) builtinHexDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		s = fmt.Sprintf("%v", args[0])
	}
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return ""
	}
	return string(decoded)
}

// builtinHmacSHA256 computes HMAC-SHA256
func (i *Interpreter) builtinHmacSHA256(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	data := fmt.Sprintf("%v", args[0])
	key := fmt.Sprintf("%v", args[1])

	// Simple HMAC implementation
	// HMAC = H(K XOR opad || H(K XOR ipad || text))
	blockSize := 64 // SHA256 block size

	// Pad key
	keyPad := make([]byte, blockSize)
	copy(keyPad, []byte(key))

	// XOR with ipad (0x36)
	ipad := make([]byte, blockSize)
	for j := 0; j < blockSize; j++ {
		ipad[j] = keyPad[j] ^ 0x36
	}

	// XOR with opad (0x5c)
	opad := make([]byte, blockSize)
	for j := 0; j < blockSize; j++ {
		opad[j] = keyPad[j] ^ 0x5c
	}

	// Inner hash
	inner := sha256.New()
	inner.Write(ipad)
	inner.Write([]byte(data))
	innerHash := inner.Sum(nil)

	// Outer hash
	outer := sha256.New()
	outer.Write(opad)
	outer.Write(innerHash)

	return hex.EncodeToString(outer.Sum(nil))
}

// ============================================================================
// HTTP Object
// ============================================================================

// HTTPObject provides HTTP-related methods.
type HTTPObject struct {
	ctx *Context
}

// NewHTTPObject creates a new HTTP object.
func NewHTTPObject(ctx *Context) *HTTPObject {
	return &HTTPObject{ctx: ctx}
}

// GetMember returns a member value.
func (h *HTTPObject) GetMember(name string) (Value, error) {
	switch name {
	case "param":
		return &HTTPParamFunc{ctx: h.ctx}, nil
	case "header":
		return &HTTPHeaderFunc{ctx: h.ctx}, nil
	case "method":
		if h.ctx.HTTPRequest == nil {
			return "", nil
		}
		return h.ctx.HTTPRequest.Method, nil
	case "path":
		if h.ctx.HTTPRequest == nil {
			return "", nil
		}
		return h.ctx.HTTPRequest.URL.Path, nil
	case "query":
		if h.ctx.HTTPRequest == nil {
			return "", nil
		}
		return h.ctx.HTTPRequest.URL.RawQuery, nil
	case "body":
		return &HTTPBodyFunc{ctx: h.ctx}, nil
	case "bodyJSON":
		return &HTTPBodyJSONFunc{ctx: h.ctx}, nil
	case "json":
		return &HTTPJSONFunc{ctx: h.ctx}, nil
	case "status":
		return &HTTPStatusFunc{ctx: h.ctx}, nil
	case "setHeader":
		return &HTTPSetHeaderFunc{ctx: h.ctx}, nil
	case "write":
		return &HTTPWriteFunc{ctx: h.ctx}, nil
	case "redirect":
		return &HTTPRedirectFunc{ctx: h.ctx}, nil
	case "cookie":
		return &HTTPCookieFunc{ctx: h.ctx}, nil
	case "setCookie":
		return &HTTPSetCookieFunc{ctx: h.ctx}, nil
	case "remoteAddr":
		if h.ctx.HTTPRequest == nil {
			return "", nil
		}
		return h.ctx.HTTPRequest.RemoteAddr, nil
	case "contentType":
		if h.ctx.HTTPRequest == nil {
			return "", nil
		}
		return h.ctx.HTTPRequest.Header.Get("Content-Type"), nil
	case "userAgent":
		if h.ctx.HTTPRequest == nil {
			return "", nil
		}
		return h.ctx.HTTPRequest.UserAgent(), nil
	default:
		return nil, fmt.Errorf("unknown http method: %s", name)
	}
}

// HTTPParamFunc gets a query parameter.
type HTTPParamFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPParamFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.HTTPRequest == nil {
		return "", nil
	}
	name, ok := args[0].(string)
	if !ok {
		return "", nil
	}
	return f.ctx.HTTPRequest.URL.Query().Get(name), nil
}

// HTTPHeaderFunc gets a header value.
type HTTPHeaderFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPHeaderFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.HTTPRequest == nil {
		return "", nil
	}
	name, ok := args[0].(string)
	if !ok {
		return "", nil
	}
	return f.ctx.HTTPRequest.Header.Get(name), nil
}

// HTTPBodyFunc reads the request body.
type HTTPBodyFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPBodyFunc) Call(args []Value) (Value, error) {
	if f.ctx.HTTPRequest == nil {
		return "", nil
	}
	data, err := io.ReadAll(f.ctx.HTTPRequest.Body)
	if err != nil {
		return "", nil
	}
	return string(data), nil
}

// HTTPBodyJSONFunc parses the request body as JSON.
type HTTPBodyJSONFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPBodyJSONFunc) Call(args []Value) (Value, error) {
	if f.ctx.HTTPRequest == nil {
		return nil, nil
	}
	data, err := io.ReadAll(f.ctx.HTTPRequest.Body)
	if err != nil {
		return nil, nil
	}
	var result interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON: %v", err)
	}
	return convertJSONToValue(result), nil
}

// HTTPJSONFunc writes JSON response.
type HTTPJSONFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPJSONFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 {
		return nil, nil
	}
	if f.ctx.HTTPWriter != nil {
		f.ctx.HTTPWriter.Header().Set("Content-Type", "application/json")
		data, _ := json.Marshal(args[0])
		f.ctx.HTTPWriter.Write(data)
	}
	return nil, nil
}

// HTTPStatusFunc sets the HTTP status code.
type HTTPStatusFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPStatusFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.HTTPWriter == nil {
		return nil, nil
	}
	code := 200
	switch v := args[0].(type) {
	case int:
		code = v
	case int64:
		code = int(v)
	case float64:
		code = int(v)
	}
	f.ctx.HTTPWriter.WriteHeader(code)
	return nil, nil
}

// HTTPSetHeaderFunc sets a response header.
type HTTPSetHeaderFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPSetHeaderFunc) Call(args []Value) (Value, error) {
	if len(args) < 2 || f.ctx.HTTPWriter == nil {
		return nil, nil
	}
	name, ok1 := args[0].(string)
	value, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return nil, nil
	}
	f.ctx.HTTPWriter.Header().Set(name, value)
	return nil, nil
}

// HTTPWriteFunc writes to the response.
type HTTPWriteFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPWriteFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.HTTPWriter == nil {
		return nil, nil
	}
	data := fmt.Sprintf("%v", args[0])
	f.ctx.HTTPWriter.Write([]byte(data))
	return nil, nil
}

// HTTPRedirectFunc redirects the client.
type HTTPRedirectFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPRedirectFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.HTTPWriter == nil {
		return nil, nil
	}
	url, ok := args[0].(string)
	if !ok {
		return nil, nil
	}
	code := 302
	if len(args) > 1 {
		code = int(iToInt(args[1]))
	}
	http.Redirect(f.ctx.HTTPWriter, f.ctx.HTTPRequest, url, code)
	return nil, nil
}

// HTTPCookieFunc gets a cookie value.
type HTTPCookieFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPCookieFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.HTTPRequest == nil {
		return "", nil
	}
	name, ok := args[0].(string)
	if !ok {
		return "", nil
	}
	cookie, err := f.ctx.HTTPRequest.Cookie(name)
	if err != nil {
		return "", nil
	}
	return cookie.Value, nil
}

// HTTPSetCookieFunc sets a cookie.
type HTTPSetCookieFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *HTTPSetCookieFunc) Call(args []Value) (Value, error) {
	if len(args) < 2 || f.ctx.HTTPWriter == nil {
		return nil, nil
	}
	name, ok1 := args[0].(string)
	value, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return nil, nil
	}

	cookie := &http.Cookie{
		Name:  name,
		Value: value,
		Path:  "/",
	}

	// Optional: max age
	if len(args) > 2 {
		cookie.MaxAge = int(iToInt(args[2]))
	}

	// Optional: domain
	if len(args) > 3 {
		if domain, ok := args[3].(string); ok {
			cookie.Domain = domain
		}
	}

	// Optional: secure
	if len(args) > 4 {
		if secure, ok := args[4].(bool); ok {
			cookie.Secure = secure
		}
	}

	// Optional: httpOnly
	if len(args) > 5 {
		if httpOnly, ok := args[5].(bool); ok {
			cookie.HttpOnly = httpOnly
		}
	}

	http.SetCookie(f.ctx.HTTPWriter, cookie)
	return nil, nil
}

// ============================================================================
// HTTP Client Functions
// ============================================================================

// HTTPClient is a shared HTTP client
var httpClient = &http.Client{Timeout: 30 * time.Second}

// builtinHTTPGet performs an HTTP GET request
func (i *Interpreter) builtinHTTPGet(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "URL required"}
	}

	urlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "URL must be a string"}
	}

	// Optional headers
	headers := map[string]string{}
	if len(args) > 1 {
		if h, ok := args[1].(map[string]Value); ok {
			for k, v := range h {
				headers[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", "XxScript/1.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":     true,
		"statusCode":  resp.StatusCode,
		"status":      resp.Status,
		"headers":     headersToMap(resp.Header),
		"body":        string(body),
		"contentType": resp.Header.Get("Content-Type"),
	}
}

// builtinHTTPPost performs an HTTP POST request
func (i *Interpreter) builtinHTTPPost(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "URL required"}
	}

	urlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "URL must be a string"}
	}

	// Body
	var bodyReader io.Reader
	contentType := "application/json"
	if len(args) > 1 {
		switch v := args[1].(type) {
		case string:
			bodyReader = strings.NewReader(v)
		case map[string]Value:
			data, _ := json.Marshal(v)
			bodyReader = bytes.NewReader(data)
		case []Value:
			data, _ := json.Marshal(v)
			bodyReader = bytes.NewReader(data)
		}
	}

	// Optional headers
	headers := map[string]string{}
	if len(args) > 2 {
		if h, ok := args[2].(map[string]Value); ok {
			for k, v := range h {
				headers[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	req, err := http.NewRequest("POST", urlStr, bodyReader)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "XxScript/1.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":     true,
		"statusCode":  resp.StatusCode,
		"status":      resp.Status,
		"headers":     headersToMap(resp.Header),
		"body":        string(body),
		"contentType": resp.Header.Get("Content-Type"),
	}
}

// builtinHTTPPut performs an HTTP PUT request
func (i *Interpreter) builtinHTTPPut(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "URL required"}
	}

	urlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "URL must be a string"}
	}

	var bodyReader io.Reader
	if len(args) > 1 {
		switch v := args[1].(type) {
		case string:
			bodyReader = strings.NewReader(v)
		case map[string]Value:
			data, _ := json.Marshal(v)
			bodyReader = bytes.NewReader(data)
		}
	}

	headers := map[string]string{}
	if len(args) > 2 {
		if h, ok := args[2].(map[string]Value); ok {
			for k, v := range h {
				headers[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	req, err := http.NewRequest("PUT", urlStr, bodyReader)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "XxScript/1.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":     true,
		"statusCode":  resp.StatusCode,
		"status":      resp.Status,
		"headers":     headersToMap(resp.Header),
		"body":        string(body),
	}
}

// builtinHTTPDelete performs an HTTP DELETE request
func (i *Interpreter) builtinHTTPDelete(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "URL required"}
	}

	urlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "URL must be a string"}
	}

	headers := map[string]string{}
	if len(args) > 1 {
		if h, ok := args[1].(map[string]Value); ok {
			for k, v := range h {
				headers[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	req, err := http.NewRequest("DELETE", urlStr, nil)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	req.Header.Set("User-Agent", "XxScript/1.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":    true,
		"statusCode": resp.StatusCode,
		"status":     resp.Status,
		"headers":    headersToMap(resp.Header),
		"body":       string(body),
	}
}

// builtinHTTPRequest performs a custom HTTP request
func (i *Interpreter) builtinHTTPRequest(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "options required"}
	}

	opts, ok := args[0].(map[string]Value)
	if !ok {
		return map[string]Value{"success": false, "error": "options must be a map"}
	}

	// Get URL
	urlStr := ""
	if u, ok := opts["url"].(string); ok {
		urlStr = u
	}
	if urlStr == "" {
		return map[string]Value{"success": false, "error": "url is required"}
	}

	// Get method
	method := "GET"
	if m, ok := opts["method"].(string); ok {
		method = m
	}

	// Get body
	var bodyReader io.Reader
	if b, ok := opts["body"]; ok {
		switch v := b.(type) {
		case string:
			bodyReader = strings.NewReader(v)
		case map[string]Value:
			data, _ := json.Marshal(v)
			bodyReader = bytes.NewReader(data)
		}
	}

	// Create request
	req, err := http.NewRequest(method, urlStr, bodyReader)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	// Set headers
	if h, ok := opts["headers"].(map[string]Value); ok {
		for k, v := range h {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}
	req.Header.Set("User-Agent", "XxScript/1.0")

	// Set Content-Type if body exists
	if bodyReader != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Timeout
	timeout := 30 * time.Second
	if t, ok := opts["timeout"].(float64); ok {
		timeout = time.Duration(t) * time.Second
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":    true,
		"statusCode": resp.StatusCode,
		"status":     resp.Status,
		"headers":    headersToMap(resp.Header),
		"body":       string(body),
	}
}

func headersToMap(h http.Header) map[string]Value {
	result := make(map[string]Value)
	for k, v := range h {
		if len(v) == 1 {
			result[k] = v[0]
		} else {
			arr := make([]Value, len(v))
			for i, s := range v {
				arr[i] = s
			}
			result[k] = arr
		}
	}
	return result
}

// ============================================================================
// URL Functions
// ============================================================================

// builtinURLParse parses a URL into its components
func (i *Interpreter) builtinURLParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "URL required"}
	}

	urlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "URL must be a string"}
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	result := map[string]Value{
		"success":    true,
		"scheme":     u.Scheme,
		"host":       u.Host,
		"path":       u.Path,
		"rawQuery":   u.RawQuery,
		"fragment":   u.Fragment,
		"rawPath":    u.RawPath,
		"opaque":     u.Opaque,
		"requestURI": u.RequestURI(),
	}

	// Parse query params
	query := make(map[string]Value)
	for k, v := range u.Query() {
		if len(v) == 1 {
			query[k] = v[0]
		} else {
			arr := make([]Value, len(v))
			for i, s := range v {
				arr[i] = s
			}
			query[k] = arr
		}
	}
	result["query"] = query

	// User info
	if u.User != nil {
		result["user"] = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			result["password"] = pass
		}
	}

	return result
}

// builtinURLEncode encodes a string for use in a URL
func (i *Interpreter) builtinURLEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	return url.QueryEscape(fmt.Sprintf("%v", args[0]))
}

// builtinURLDecode decodes a URL-encoded string
func (i *Interpreter) builtinURLDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, err := url.QueryUnescape(fmt.Sprintf("%v", args[0]))
	if err != nil {
		return ""
	}
	return s
}

// builtinURLJoin joins a base URL with a path
func (i *Interpreter) builtinURLJoin(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	base, ok1 := args[0].(string)
	path, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return ""
	}

	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}

	pathURL, err := url.Parse(path)
	if err != nil {
		return ""
	}

	return baseURL.ResolveReference(pathURL).String()
}

// builtinURLBuild builds a URL from components
func (i *Interpreter) builtinURLBuild(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	opts, ok := args[0].(map[string]Value)
	if !ok {
		return ""
	}

	u := &url.URL{}

	if scheme, ok := opts["scheme"].(string); ok {
		u.Scheme = scheme
	}
	if host, ok := opts["host"].(string); ok {
		u.Host = host
	}
	if path, ok := opts["path"].(string); ok {
		u.Path = path
	}
	if fragment, ok := opts["fragment"].(string); ok {
		u.Fragment = fragment
	}

	// Build query string
	if query, ok := opts["query"].(map[string]Value); ok {
		q := u.Query()
		for k, v := range query {
			q.Set(k, fmt.Sprintf("%v", v))
		}
		u.RawQuery = q.Encode()
	}

	// User info
	if user, ok := opts["user"].(string); ok {
		if pass, ok := opts["password"].(string); ok {
			u.User = url.UserPassword(user, pass)
		} else {
			u.User = url.User(user)
		}
	}

	return u.String()
}

// ============================================================================
// DNS and IP Functions
// ============================================================================

// builtinDNSLookup looks up hostnames for a domain
func (i *Interpreter) builtinDNSLookup(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "hostname required"}
	}

	hostname, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "hostname must be a string"}
	}

	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	result := make([]Value, len(addrs))
	for i, addr := range addrs {
		result[i] = addr
	}

	return map[string]Value{
		"success":  true,
		"hostname": hostname,
		"addrs":    result,
	}
}

// builtinDNSLookupHost looks up hostnames (alias with more info)
func (i *Interpreter) builtinDNSLookupHost(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "hostname required"}
	}

	hostname, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "hostname must be a string"}
	}

	// LookupHost returns both IPv4 and IPv6
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	// CNAME lookup
	cname, _ := net.LookupCNAME(hostname)

	// Separate IPv4 and IPv6
	ipv4 := []Value{}
	ipv6 := []Value{}
	for _, addr := range addrs {
		if strings.Contains(addr, ":") {
			ipv6 = append(ipv6, addr)
		} else {
			ipv4 = append(ipv4, addr)
		}
	}

	return map[string]Value{
		"success":  true,
		"hostname": hostname,
		"cname":    cname,
		"addrs":    addrs,
		"ipv4":     ipv4,
		"ipv6":     ipv6,
	}
}

// builtinDNSLookupAddr performs reverse DNS lookup
func (i *Interpreter) builtinDNSLookupAddr(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "IP address required"}
	}

	ipAddr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "IP address must be a string"}
	}

	names, err := net.LookupAddr(ipAddr)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	result := make([]Value, len(names))
	for i, name := range names {
		result[i] = name
	}

	return map[string]Value{
		"success": true,
		"ip":      ipAddr,
		"names":   result,
	}
}

// builtinIPParse parses an IP address
func (i *Interpreter) builtinIPParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "IP address required"}
	}

	ipStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "IP address must be a string"}
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return map[string]Value{"success": false, "error": "invalid IP address"}
	}

	return map[string]Value{
		"success":  true,
		"ip":       ip.String(),
		"isIPv4":   ip.To4() != nil,
		"isIPv6":   ip.To4() == nil,
		"isLoopback": ip.IsLoopback(),
		"isPrivate":  ip.IsPrivate(),
		"isGlobalUnicast": ip.IsGlobalUnicast(),
	}
}

// builtinIsIPv4 checks if a string is a valid IPv4 address
func (i *Interpreter) builtinIsIPv4(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	ipStr, ok := args[0].(string)
	if !ok {
		return false
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	return ip.To4() != nil
}

// builtinIsIPv6 checks if a string is a valid IPv6 address
func (i *Interpreter) builtinIsIPv6(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	ipStr, ok := args[0].(string)
	if !ok {
		return false
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	return ip.To4() == nil
}

// ============================================================================
// JSON Functions
// ============================================================================

// builtinJSONEncode encodes a value to JSON string
func (i *Interpreter) builtinJSONEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	data, err := json.Marshal(args[0])
	if err != nil {
		return ""
	}
	return string(data)
}

// builtinJSONDecode decodes a JSON string to a value
func (i *Interpreter) builtinJSONDecode(args []Value) Value {
	if len(args) == 0 {
		return nil
	}

	jsonStr, ok := args[0].(string)
	if !ok {
		return nil
	}

	var result interface{}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	// Convert to XxScript values
	return convertJSONValue(result)
}

// builtinJSONPretty pretty-prints JSON
func (i *Interpreter) builtinJSONPretty(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var data []byte
	var err error

	switch v := args[0].(type) {
	case string:
		var obj interface{}
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return ""
		}
		data, err = json.MarshalIndent(obj, "", "  ")
	default:
		data, err = json.MarshalIndent(v, "", "  ")
	}

	if err != nil {
		return ""
	}
	return string(data)
}

func convertJSONValue(v interface{}) Value {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case float64:
		return val
	case string:
		return val
	case []interface{}:
		arr := make([]Value, len(val))
		for i, item := range val {
			arr[i] = convertJSONValue(item)
		}
		return arr
	case map[string]interface{}:
		m := make(map[string]Value)
		for k, v := range val {
			m[k] = convertJSONValue(v)
		}
		return m
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ============================================================================
// OS Functions - Environment Variables
// ============================================================================

func (i *Interpreter) builtinEnv(args []Value) Value {
	if len(args) == 0 {
		// Return all environment variables
		result := make(map[string]Value)
		for _, env := range os.Environ() {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 {
				result[parts[0]] = parts[1]
			}
		}
		return result
	}

	key, ok := args[0].(string)
	if !ok {
		return ""
	}

	defaultVal := ""
	if len(args) > 1 {
		if d, ok := args[1].(string); ok {
			defaultVal = d
		}
	}

	val := os.Getenv(key)
	if val == "" && defaultVal != "" {
		return defaultVal
	}
	return val
}

func (i *Interpreter) builtinEnvSet(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "envSet requires key and value"}
	}

	key, ok1 := args[0].(string)
	value, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return map[string]Value{"success": false, "error": "key and value must be strings"}
	}

	if err := os.Setenv(key, value); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true, "key": key, "value": value}
}

func (i *Interpreter) builtinEnvUnset(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "key required"}
	}

	key, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "key must be a string"}
	}

	if err := os.Unsetenv(key); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true}
}

func (i *Interpreter) builtinEnvList(args []Value) Value {
	envVars := os.Environ()
	result := make([]Value, len(envVars))
	for i, env := range envVars {
		result[i] = env
	}
	return result
}

// ============================================================================
// OS Functions - Process Info
// ============================================================================

func (i *Interpreter) builtinPID(args []Value) Value {
	return int64(os.Getpid())
}

func (i *Interpreter) builtinPPID(args []Value) Value {
	return int64(os.Getppid())
}

func (i *Interpreter) builtinUID(args []Value) Value {
	return int64(os.Getuid())
}

func (i *Interpreter) builtinGID(args []Value) Value {
	return int64(os.Getgid())
}

// ============================================================================
// OS Functions - System Info
// ============================================================================

func (i *Interpreter) builtinHostname(args []Value) Value {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return hostname
}

func (i *Interpreter) builtinOSInfo(args []Value) Value {
	return map[string]Value{
		"success":  true,
		"os":       runtimeOS,
		"arch":     runtimeArch,
		"hostname": i.builtinHostname(nil),
		"pid":      int64(os.Getpid()),
		"ppid":     int64(os.Getppid()),
		"uid":      int64(os.Getuid()),
		"gid":      int64(os.Getgid()),
		"cwd":      i.builtinCwd(nil).(string),
		"home":     os.Getenv("HOME"),
		"tempDir":  os.TempDir(),
	}
}

func (i *Interpreter) builtinArch(args []Value) Value {
	return runtimeArch
}

func (i *Interpreter) builtinCwd(args []Value) Value {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	return dir
}

func (i *Interpreter) builtinHome(args []Value) Value {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return homeDir
}

func (i *Interpreter) builtinTempDir(args []Value) Value {
	return os.TempDir()
}

// ============================================================================
// OS Functions - Time
// ============================================================================

func (i *Interpreter) builtinSleep(args []Value) Value {
	if len(args) == 0 {
		return nil
	}

	var d time.Duration
	switch v := args[0].(type) {
	case float64:
		d = time.Duration(v * float64(time.Second))
	case int:
		d = time.Duration(v) * time.Second
	case int64:
		d = time.Duration(v) * time.Second
	case string:
		// Parse duration string like "1s", "100ms", "1h30m"
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil
		}
		d = parsed
	default:
		return nil
	}

	time.Sleep(d)
	return nil
}

func (i *Interpreter) builtinClock(args []Value) Value {
	// Return Unix timestamp in seconds
	return float64(time.Now().UnixNano()) / 1e9
}

func (i *Interpreter) builtinTimestamp(args []Value) Value {
	// Return Unix timestamp in milliseconds
	return time.Now().UnixMilli()
}

func (i *Interpreter) builtinDateParts(args []Value) Value {
	var t time.Time
	if len(args) == 0 {
		t = time.Now()
	} else if ts, ok := args[0].(float64); ok {
		t = time.Unix(int64(ts), 0)
	} else if ts, ok := args[0].(int64); ok {
		t = time.Unix(ts, 0)
	} else {
		t = time.Now()
	}

	return map[string]Value{
		"year":    int64(t.Year()),
		"month":   int64(t.Month()),
		"day":     int64(t.Day()),
		"hour":    int64(t.Hour()),
		"minute":  int64(t.Minute()),
		"second":  int64(t.Second()),
		"weekday": int64(t.Weekday()),
		"yday":    int64(t.YearDay()),
		"unix":    t.Unix(),
	}
}

// ============================================================================
// OS Functions - Command Execution
// ============================================================================

func (i *Interpreter) builtinExec(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "command required"}
	}

	cmd, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "command must be a string"}
	}

	// Split command into name and args
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return map[string]Value{"success": false, "error": "empty command"}
	}

	name := parts[0]
	cmdArgs := parts[1:]

	// Additional args from second argument
	if len(args) > 1 {
		if arr, ok := args[1].([]Value); ok {
			for _, a := range arr {
				cmdArgs = append(cmdArgs, fmt.Sprintf("%v", a))
			}
		}
	}

	command := exec.Command(name, cmdArgs...)
	output, err := command.CombinedOutput()

	result := map[string]Value{
		"success":    err == nil,
		"output":     string(output),
		"command":    cmd,
		"exitCode":   0,
	}

	if err != nil {
		result["error"] = err.Error()
		if exitErr, ok := err.(*exec.ExitError); ok {
			result["exitCode"] = int64(exitErr.ExitCode())
		}
	}

	return result
}

func (i *Interpreter) builtinExecOutput(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	cmd, ok := args[0].(string)
	if !ok {
		return ""
	}

	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return ""
	}

	name := parts[0]
	cmdArgs := parts[1:]

	if len(args) > 1 {
		if arr, ok := args[1].([]Value); ok {
			for _, a := range arr {
				cmdArgs = append(cmdArgs, fmt.Sprintf("%v", a))
			}
		}
	}

	command := exec.Command(name, cmdArgs...)
	output, err := command.Output()
	if err != nil {
		return ""
	}
	return string(output)
}

// ============================================================================
// OS Functions - Memory and CPU
// ============================================================================

func (i *Interpreter) builtinMemStats(args []Value) Value {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]Value{
		"success":         true,
		"alloc":           int64(m.Alloc),
		"totalAlloc":      int64(m.TotalAlloc),
		"sys":             int64(m.Sys),
		"lookups":         int64(m.Lookups),
		"mallocs":         int64(m.Mallocs),
		"frees":           int64(m.Frees),
		"heapAlloc":       int64(m.HeapAlloc),
		"heapSys":         int64(m.HeapSys),
		"heapIdle":        int64(m.HeapIdle),
		"heapInuse":       int64(m.HeapInuse),
		"heapReleased":    int64(m.HeapReleased),
		"heapObjects":     int64(m.HeapObjects),
		"stackInuse":      int64(m.StackInuse),
		"stackSys":        int64(m.StackSys),
		"mspanInuse":      int64(m.MSpanInuse),
		"mspanSys":        int64(m.MSpanSys),
		"mcacheInuse":     int64(m.MCacheInuse),
		"mcacheSys":       int64(m.MCacheSys),
		"buckHashSys":     int64(m.BuckHashSys),
		"gcsys":           int64(m.GCSys),
		"otherSys":        int64(m.OtherSys),
		"nextGC":          int64(m.NextGC),
		"lastGC":          int64(m.LastGC),
		"numGC":           int64(m.NumGC),
		"numForcedGC":     int64(m.NumForcedGC),
		"gcCPUFraction":   m.GCCPUFraction,
	}
}

func (i *Interpreter) builtinGoroutines(args []Value) Value {
	return int64(runtime.NumGoroutine())
}

// ============================================================================
// OS Functions - User Directories
// ============================================================================

func (i *Interpreter) builtinUserHome(args []Value) Value {
	dir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return dir
}

func (i *Interpreter) builtinUserCache(args []Value) Value {
	dir, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return dir
}

func (i *Interpreter) builtinUserConfig(args []Value) Value {
	// Go 1.13+ has os.UserConfigDir
	configDir := os.Getenv("XDG_CONFIG_HOME")
	if configDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		configDir = filepath.Join(homeDir, ".config")
	}
	return configDir
}

// ============================================================================
// OS Functions - File Permissions
// ============================================================================

func (i *Interpreter) builtinChmod(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "chmod requires path and mode"}
	}

	path, ok1 := args[0].(string)
	if !ok1 {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	var mode os.FileMode
	switch m := args[1].(type) {
	case float64:
		mode = os.FileMode(int(m))
	case int:
		mode = os.FileMode(m)
	case int64:
		mode = os.FileMode(m)
	case string:
		// Parse octal string like "755"
		var n uint32
		fmt.Sscanf(m, "%o", &n)
		mode = os.FileMode(n)
	default:
		mode = 0644
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	if err := os.Chmod(path, mode); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true, "path": path, "mode": fmt.Sprintf("%04o", mode)}
}

func (i *Interpreter) builtinChown(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"success": false, "error": "chown requires path, uid, gid"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	uid := int(i.toInt(args[1]))
	gid := int(i.toInt(args[2]))

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	if err := os.Chown(path, uid, gid); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true, "path": path}
}

// ============================================================================
// OS Functions - Exit
// ============================================================================

func (i *Interpreter) builtinExit(args []Value) Value {
	code := 0
	if len(args) > 0 {
		code = int(i.toInt(args[0]))
	}
	os.Exit(code)
	return nil
}

// ============================================================================
// Format Functions - Number Formatting
// ============================================================================

func (i *Interpreter) builtinFormatNumber(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toFloat(args[0])
	decimals := 2
	if len(args) > 1 {
		decimals = int(i.toInt(args[1]))
	}

	// Format with thousand separators
	format := fmt.Sprintf("%%.%df", decimals)
	str := fmt.Sprintf(format, num)

	// Add thousand separators
	parts := strings.Split(str, ".")
	intPart := parts[0]
	decPart := ""
	if len(parts) > 1 {
		decPart = "." + parts[1]
	}

	// Add commas for thousands
	var result []rune
	for j, c := range intPart {
		if j > 0 && (len(intPart)-j)%3 == 0 && intPart[j-1] != '-' {
			result = append(result, ',')
		}
		result = append(result, c)
	}

	return string(result) + decPart
}

func (i *Interpreter) builtinFormatFloat(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toFloat(args[0])
	decimals := 2
	if len(args) > 1 {
		decimals = int(i.toInt(args[1]))
	}

	format := fmt.Sprintf("%%.%df", decimals)
	return fmt.Sprintf(format, num)
}

func (i *Interpreter) builtinFormatInt(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toInt(args[0])

	// Optional base
	base := 10
	if len(args) > 1 {
		base = int(i.toInt(args[1]))
		if base < 2 || base > 36 {
			base = 10
		}
	}

	// Format in specified base
	if base == 10 {
		return fmt.Sprintf("%d", num)
	} else if base == 16 {
		return fmt.Sprintf("%x", num)
	} else if base == 8 {
		return fmt.Sprintf("%o", num)
	} else if base == 2 {
		return fmt.Sprintf("%b", num)
	}

	// Custom base conversion
	digits := "0123456789abcdefghijklmnopqrstuvwxyz"
	if num < 0 {
		num = -num
	}
	if num == 0 {
		return "0"
	}

	var result []byte
	num64 := int64(num)
	for num64 > 0 {
		result = append([]byte{digits[num64%int64(base)]}, result...)
		num64 = num64 / int64(base)
	}

	return string(result)
}

func (i *Interpreter) builtinFormatCurrency(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toFloat(args[0])
	symbol := "$"
	if len(args) > 1 {
		if s, ok := args[1].(string); ok {
			symbol = s
		}
	}

	decimals := 2
	if len(args) > 2 {
		decimals = int(i.toInt(args[2]))
	}

	format := fmt.Sprintf("%%.%df", decimals)
	formatted := fmt.Sprintf(format, num)

	// Add thousand separators
	parts := strings.Split(formatted, ".")
	intPart := parts[0]
	decPart := ""
	if len(parts) > 1 {
		decPart = "." + parts[1]
	}

	var result []rune
	for j, c := range intPart {
		if j > 0 && (len(intPart)-j)%3 == 0 && intPart[j-1] != '-' {
			result = append(result, ',')
		}
		result = append(result, c)
	}

	if num < 0 {
		return "-" + symbol + string(result[1:]) + decPart
	}
	return symbol + string(result) + decPart
}

func (i *Interpreter) builtinFormatPercent(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toFloat(args[0])
	decimals := 1
	if len(args) > 1 {
		decimals = int(i.toInt(args[1]))
	}

	format := fmt.Sprintf("%%.%df", decimals)
	return fmt.Sprintf(format+"%%", num*100)
}

func (i *Interpreter) builtinFormatBytes(args []Value) Value {
	if len(args) == 0 {
		return "0 B"
	}

	bytes := i.toInt(args[0])
	if bytes < 0 {
		bytes = -bytes
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	unit := 0
	value := float64(bytes)

	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}

	if unit == 0 {
		return fmt.Sprintf("%d %s", bytes, units[unit])
	}
	return fmt.Sprintf("%.2f %s", value, units[unit])
}

// ============================================================================
// Format Functions - Date/Time
// ============================================================================

func (i *Interpreter) builtinFormatDate(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var t time.Time
	switch v := args[0].(type) {
	case float64:
		t = time.Unix(int64(v), 0)
	case int64:
		t = time.Unix(v, 0)
	case int:
		t = time.Unix(int64(v), 0)
	case string:
		// Try to parse as RFC3339 first
		parsed, err := time.Parse(time.RFC3339, v)
		if err == nil {
			t = parsed
		} else {
			t = time.Now()
		}
	default:
		t = time.Now()
	}

	layout := "2006-01-02 15:04:05"
	if len(args) > 1 {
		if l, ok := args[1].(string); ok {
			layout = l
		}
	}

	return t.Format(layout)
}

func (i *Interpreter) builtinParseDate(args []Value) Value {
	if len(args) == 0 {
		return nil
	}

	dateStr, ok := args[0].(string)
	if !ok {
		return nil
	}

	layout := "2006-01-02"
	if len(args) > 1 {
		if l, ok := args[1].(string); ok {
			layout = l
		}
	}

	t, err := time.Parse(layout, dateStr)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success": true,
		"unix":    t.Unix(),
		"year":    int64(t.Year()),
		"month":   int64(t.Month()),
		"day":     int64(t.Day()),
		"hour":    int64(t.Hour()),
		"minute":  int64(t.Minute()),
		"second":  int64(t.Second()),
	}
}

func (i *Interpreter) builtinFormatDuration(args []Value) Value {
	if len(args) == 0 {
		return "0s"
	}

	var d time.Duration
	switch v := args[0].(type) {
	case float64:
		d = time.Duration(v * float64(time.Second))
	case int64:
		d = time.Duration(v) * time.Second
	case int:
		d = time.Duration(v) * time.Second
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return "0s"
		}
		d = parsed
	default:
		return "0s"
	}

	// Format as human readable
	seconds := int(d.Seconds())
	minutes := seconds / 60
	hours := minutes / 60
	days := hours / 24

	seconds %= 60
	minutes %= 60
	hours %= 24

	parts := []string{}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", seconds))
	}

	return strings.Join(parts, " ")
}

func (i *Interpreter) builtinParseDuration(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}

	durStr, ok := args[0].(string)
	if !ok {
		return int64(0)
	}

	d, err := time.ParseDuration(durStr)
	if err != nil {
		return int64(0)
	}

	return int64(d.Seconds())
}

// ============================================================================
// Format Functions - Text
// ============================================================================

func (i *Interpreter) builtinIndent(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	text, ok := args[0].(string)
	if !ok {
		text = fmt.Sprintf("%v", args[0])
	}

	indent := "  "
	if len(args) > 1 {
		if ind, ok := args[1].(string); ok {
			indent = ind
		}
	}

	lines := strings.Split(text, "\n")
	for j, line := range lines {
		if line != "" {
			lines[j] = indent + line
		}
	}
	return strings.Join(lines, "\n")
}

func (i *Interpreter) builtinWrap(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	text, ok := args[0].(string)
	if !ok {
		text = fmt.Sprintf("%v", args[0])
	}

	width := 80
	if len(args) > 1 {
		width = int(i.toInt(args[1]))
	}

	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}

	var lines []string
	currentLine := words[0]

	for _, word := range words[1:] {
		if len(currentLine)+1+len(word) <= width {
			currentLine += " " + word
		} else {
			lines = append(lines, currentLine)
			currentLine = word
		}
	}
	lines = append(lines, currentLine)

	return strings.Join(lines, "\n")
}

func (i *Interpreter) builtinAlign(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	text, ok := args[0].(string)
	if !ok {
		text = fmt.Sprintf("%v", args[0])
	}

	width := int(i.toInt(args[1]))
	alignType := "left"
	if len(args) > 2 {
		if t, ok := args[2].(string); ok {
			alignType = t
		}
	}

	textLen := len(text)
	if textLen >= width {
		return text
	}

	padding := width - textLen

	switch alignType {
	case "right":
		return strings.Repeat(" ", padding) + text
	case "center":
		leftPad := padding / 2
		rightPad := padding - leftPad
		return strings.Repeat(" ", leftPad) + text + strings.Repeat(" ", rightPad)
	default: // left
		return text + strings.Repeat(" ", padding)
	}
}

func (i *Interpreter) builtinAlignLeft(args []Value) Value {
	return i.builtinAlign(append(args, "left"))
}

func (i *Interpreter) builtinAlignRight(args []Value) Value {
	return i.builtinAlign(append(args, "right"))
}

func (i *Interpreter) builtinAlignCenter(args []Value) Value {
	return i.builtinAlign(append(args, "center"))
}

// ============================================================================
// Format Functions - Table/CSV
// ============================================================================

func (i *Interpreter) builtinTable(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var data [][]Value
	switch v := args[0].(type) {
	case []Value:
		// Check if it's 2D array
		if len(v) > 0 {
			if inner, ok := v[0].([]Value); ok {
				data = make([][]Value, len(v))
				data[0] = inner
				for j := 1; j < len(v); j++ {
					if inner, ok := v[j].([]Value); ok {
						data[j] = inner
					}
				}
			} else {
				// Single row
				data = [][]Value{v}
			}
		}
	case map[string]Value:
		// Convert map to key-value table
		data = [][]Value{}
		for k, val := range v {
			data = append(data, []Value{k, val})
		}
	default:
		return ""
	}

	if len(data) == 0 {
		return ""
	}

	// Calculate column widths
	numCols := len(data[0])
	colWidths := make([]int, numCols)
	for _, row := range data {
		for j, cell := range row {
			cellStr := fmt.Sprintf("%v", cell)
			if j < len(colWidths) && len(cellStr) > colWidths[j] {
				colWidths[j] = len(cellStr)
			}
		}
	}

	// Build table
	var lines []string

	// Top border
	var border strings.Builder
	border.WriteString("+")
	for _, w := range colWidths {
		border.WriteString(strings.Repeat("-", w+2))
		border.WriteString("+")
	}
	lines = append(lines, border.String())

	// Rows
	for _, row := range data {
		var line strings.Builder
		line.WriteString("|")
		for j, cell := range row {
			cellStr := fmt.Sprintf("%v", cell)
			width := colWidths[j]
			line.WriteString(" ")
			line.WriteString(cellStr)
			line.WriteString(strings.Repeat(" ", width-len(cellStr)+1))
			line.WriteString("|")
		}
		lines = append(lines, line.String())
		lines = append(lines, border.String())
	}

	return strings.Join(lines, "\n")
}

func (i *Interpreter) builtinCSV(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var data [][]Value
	switch v := args[0].(type) {
	case []Value:
		if len(v) > 0 {
			if inner, ok := v[0].([]Value); ok {
				data = make([][]Value, len(v))
				data[0] = inner
				for j := 1; j < len(v); j++ {
					if inner, ok := v[j].([]Value); ok {
						data[j] = inner
					}
				}
			} else {
				data = [][]Value{v}
			}
		}
	case map[string]Value:
		data = [][]Value{}
		for k, val := range v {
			data = append(data, []Value{k, val})
		}
	default:
		return ""
	}

	separator := ","
	if len(args) > 1 {
		if s, ok := args[1].(string); ok && len(s) > 0 {
			separator = s
		}
	}

	var lines []string
	for _, row := range data {
		cells := make([]string, len(row))
		for j, cell := range row {
			cellStr := fmt.Sprintf("%v", cell)
			// Quote if contains separator or newline
			if strings.Contains(cellStr, separator) || strings.Contains(cellStr, "\n") || strings.Contains(cellStr, "\"") {
				cellStr = "\"" + strings.ReplaceAll(cellStr, "\"", "\"\"") + "\""
			}
			cells[j] = cellStr
		}
		lines = append(lines, strings.Join(cells, separator))
	}

	return strings.Join(lines, "\n")
}

func (i *Interpreter) builtinCSVParse(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	csvStr, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	separator := ","
	if len(args) > 1 {
		if s, ok := args[1].(string); ok && len(s) > 0 {
			separator = s
		}
	}

	var result []Value
	var currentCell strings.Builder
	var inQuotes bool
	var row []Value

	for j := 0; j < len(csvStr); j++ {
		ch := csvStr[j]

		if inQuotes {
			if ch == '"' {
				if j+1 < len(csvStr) && csvStr[j+1] == '"' {
					currentCell.WriteByte('"')
					j++
				} else {
					inQuotes = false
				}
			} else {
				currentCell.WriteByte(ch)
			}
		} else {
			if ch == '"' {
				inQuotes = true
			} else if string(ch) == separator {
				row = append(row, currentCell.String())
				currentCell.Reset()
			} else if ch == '\n' {
				row = append(row, currentCell.String())
				result = append(result, row)
				row = nil
				currentCell.Reset()
			} else if ch != '\r' {
				currentCell.WriteByte(ch)
			}
		}
	}

	if currentCell.Len() > 0 || len(row) > 0 {
		row = append(row, currentCell.String())
		result = append(result, row)
	}

	return result
}

// ============================================================================
// Format Functions - Other
// ============================================================================

func (i *Interpreter) builtinPrintf(args []Value) Value {
	if len(args) == 0 {
		return nil
	}

	format, ok := args[0].(string)
	if !ok {
		return nil
	}

	var formatArgs []interface{}
	for _, arg := range args[1:] {
		formatArgs = append(formatArgs, arg)
	}

	output := fmt.Sprintf(format, formatArgs...)

	// Write to HTTP response if available, otherwise print
	if i.ctx.HTTPWriter != nil {
		i.ctx.HTTPWriter.Write([]byte(output))
	}

	return nil
}

func (i *Interpreter) builtinPadNumber(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toInt(args[0])
	width := 2
	if len(args) > 1 {
		width = int(i.toInt(args[1]))
	}

	padChar := "0"
	if len(args) > 2 {
		if p, ok := args[2].(string); ok && len(p) > 0 {
			padChar = string(p[0])
		}
	}

	str := fmt.Sprintf("%d", num)
	for len(str) < width {
		str = padChar + str
	}
	return str
}

func (i *Interpreter) builtinToRoman(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toInt(args[0])
	if num < 1 || num > 3999 {
		return ""
	}

	vals := []int{1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1}
	romans := []string{"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"}

	var result strings.Builder
	for j, val := range vals {
		for num >= val {
			result.WriteString(romans[j])
			num -= val
		}
	}

	return result.String()
}

func (i *Interpreter) builtinFromRoman(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}

	roman, ok := args[0].(string)
	if !ok {
		return int64(0)
	}

	roman = strings.ToUpper(roman)
	values := map[byte]int{'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}

	result := 0
	for j := 0; j < len(roman); j++ {
		val := values[roman[j]]
		if j+1 < len(roman) && values[roman[j+1]] > val {
			result -= val
		} else {
			result += val
		}
	}

	return int64(result)
}

func (i *Interpreter) builtinToWords(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toInt(args[0])
	if num < 0 {
		return "negative " + i.builtinToWords([]Value{-num}).(string)
	}
	if num == 0 {
		return "zero"
	}

	ones := []string{"", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
		"eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"}
	tens := []string{"", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"}

	var parts []string

	if num >= 1000000000000 {
		parts = append(parts, i.builtinToWords([]Value{num/1000000000000}).(string), "trillion")
		num %= 1000000000000
	}
	if num >= 1000000000 {
		parts = append(parts, i.builtinToWords([]Value{num/1000000000}).(string), "billion")
		num %= 1000000000
	}
	if num >= 1000000 {
		parts = append(parts, i.builtinToWords([]Value{num/1000000}).(string), "million")
		num %= 1000000
	}
	if num >= 1000 {
		parts = append(parts, i.builtinToWords([]Value{num/1000}).(string), "thousand")
		num %= 1000
	}
	if num >= 100 {
		parts = append(parts, ones[num/100], "hundred")
		num %= 100
	}
	if num >= 20 {
		parts = append(parts, tens[num/10])
		if num%10 > 0 {
			parts[len(parts)-1] += "-" + ones[num%10]
		}
	} else if num > 0 {
		parts = append(parts, ones[num])
	}

	return strings.Join(parts, " ")
}

func (i *Interpreter) builtinToOrdinal(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	num := i.toInt(args[0])
	words := i.builtinToWords(args)

	// Add ordinal suffix
	lastDigit := int(num % 10)
	lastTwoDigits := int(num % 100)

	var suffix string
	if lastTwoDigits >= 11 && lastTwoDigits <= 13 {
		suffix = "th"
	} else {
		switch lastDigit {
		case 1:
			suffix = "st"
		case 2:
			suffix = "nd"
		case 3:
			suffix = "rd"
		default:
			suffix = "th"
		}
	}

	return words.(string) + suffix
}

func iToInt(v Value) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}

// ============================================================================
// DB Object
// ============================================================================

// DBObject provides database-related methods.
type DBObject struct {
	ctx *Context
}

// NewDBObject creates a new DB object.
func NewDBObject(ctx *Context) *DBObject {
	return &DBObject{ctx: ctx}
}

// GetMember returns a member value.
func (d *DBObject) GetMember(name string) (Value, error) {
	switch name {
	case "query":
		return &DBQueryFunc{ctx: d.ctx}, nil
	case "exec":
		return &DBExecFunc{ctx: d.ctx}, nil
	case "queryRow":
		return &DBQueryRowFunc{ctx: d.ctx}, nil
	default:
		return nil, fmt.Errorf("unknown db method: %s", name)
	}
}

// DBQueryFunc executes a query and returns results.
type DBQueryFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *DBQueryFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.Executor == nil {
		return nil, fmt.Errorf("no query or executor")
	}

	query, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("query must be string")
	}

	result, err := f.ctx.Executor.ExecuteForScript(query)
	if err != nil {
		return nil, err
	}

	// Convert result to script values
	rows, columns, err := extractResult(result)
	if err != nil {
		return nil, err
	}

	resultRows := make([]Value, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]Value)
		for j, col := range columns {
			if j < len(row) {
				rowMap[col] = row[j]
			}
		}
		resultRows[i] = rowMap
	}

	return resultRows, nil
}

// DBExecFunc executes a statement.
type DBExecFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *DBExecFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.Executor == nil {
		return nil, fmt.Errorf("no query or executor")
	}

	query, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("query must be string")
	}

	result, err := f.ctx.Executor.ExecuteForScript(query)
	if err != nil {
		return nil, err
	}

	affected, insertID := extractExecResult(result)
	return map[string]Value{
		"affected":  affected,
		"insert_id": insertID,
	}, nil
}

// DBQueryRowFunc executes a query and returns a single row.
type DBQueryRowFunc struct {
	ctx *Context
}

// Call implements Callable.
func (f *DBQueryRowFunc) Call(args []Value) (Value, error) {
	if len(args) == 0 || f.ctx.Executor == nil {
		return nil, fmt.Errorf("no query or executor")
	}

	query, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("query must be string")
	}

	result, err := f.ctx.Executor.ExecuteForScript(query)
	if err != nil {
		return nil, err
	}

	rows, columns, err := extractResult(result)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	rowMap := make(map[string]Value)
	row := rows[0]
	for j, col := range columns {
		if j < len(row) {
			rowMap[col] = row[j]
		}
	}

	return rowMap, nil
}

// extractResult extracts rows and columns from a result.
// It handles both map[string]interface{} and struct types via reflection.
func extractResult(result interface{}) ([][]interface{}, []string, error) {
	if result == nil {
		return nil, nil, nil
	}

	// Try map[string]interface{} first (our adapter returns this)
	if m, ok := result.(map[string]interface{}); ok {
		var rows [][]interface{}
		var columns []string

		if r, ok := m["rows"].([][]interface{}); ok {
			rows = r
		}
		if c, ok := m["columns"].([]string); ok {
			columns = c
		}

		return rows, columns, nil
	}

	// Try using reflection for struct types
	rv := reflect.ValueOf(result)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Struct {
		var rows [][]interface{}
		var columns []string

		// Try to get Rows field
		rowsField := rv.FieldByName("Rows")
		if rowsField.IsValid() {
			for i := 0; i < rowsField.Len(); i++ {
				rowField := rowsField.Index(i)
				var row []interface{}
				if rowField.Kind() == reflect.Slice {
					for j := 0; j < rowField.Len(); j++ {
						row = append(row, rowField.Index(j).Interface())
					}
				}
				rows = append(rows, row)
			}
		}

		// Try to get Columns field
		colsField := rv.FieldByName("Columns")
		if colsField.IsValid() && colsField.Kind() == reflect.Slice {
			for i := 0; i < colsField.Len(); i++ {
				col := colsField.Index(i)
				if col.Kind() == reflect.Struct {
					// Check for Alias first, then fall back to Name
					aliasField := col.FieldByName("Alias")
					nameField := col.FieldByName("Name")
					if aliasField.IsValid() && aliasField.String() != "" {
						columns = append(columns, aliasField.String())
					} else if nameField.IsValid() {
						columns = append(columns, nameField.String())
					}
				} else if col.Kind() == reflect.String {
					columns = append(columns, col.String())
				}
			}
		}

		return rows, columns, nil
	}

	return nil, nil, fmt.Errorf("unsupported result type: %T", result)
}

// extractExecResult extracts affected rows and insert ID from an exec result.
func extractExecResult(result interface{}) (int64, int64) {
	var affected, insertID int64

	if result == nil {
		return affected, insertID
	}

	// Try map
	if m, ok := result.(map[string]interface{}); ok {
		if a, ok := m["affected"].(int64); ok {
			affected = a
		}
		if i, ok := m["insert_id"].(int64); ok {
			insertID = i
		}
		return affected, insertID
	}

	// Try reflection
	rv := reflect.ValueOf(result)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Struct {
		if f := rv.FieldByName("RowCount"); f.IsValid() {
			switch f.Kind() {
			case reflect.Int, reflect.Int64:
				affected = f.Int()
			}
		}
		if f := rv.FieldByName("LastInsert"); f.IsValid() {
			switch f.Kind() {
			case reflect.Int, reflect.Int64:
				insertID = f.Int()
			}
		}
	}

	return affected, insertID
}

// ============================================================================
// File Operation Functions
// ============================================================================

// builtinFileSave saves content to a file
// fileSave(path, content) - saves string content to file
// fileSave(path, content, "binary") - saves binary content (base64 encoded string)
func (i *Interpreter) builtinFileSave(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "fileSave requires at least 2 arguments (path, content)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to create directory: %v", err)}
	}

	var data []byte
	switch v := args[1].(type) {
	case string:
		// Check if binary mode
		if len(args) > 2 {
			mode, _ := args[2].(string)
			if mode == "binary" {
				// Decode base64
				decoded, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return map[string]Value{"success": false, "error": fmt.Sprintf("invalid base64: %v", err)}
				}
				data = decoded
			} else {
				data = []byte(v)
			}
		} else {
			data = []byte(v)
		}
	case []byte:
		data = v
	case []Value:
		// Array of bytes
		data = make([]byte, len(v))
		for idx, b := range v {
			if n, ok := b.(int); ok {
				data[idx] = byte(n)
			} else if n, ok := b.(int64); ok {
				data[idx] = byte(n)
			} else if n, ok := b.(float64); ok {
				data[idx] = byte(n)
			}
		}
	default:
		data = []byte(fmt.Sprintf("%v", v))
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to write file: %v", err)}
	}

	return map[string]Value{"success": true, "path": path}
}

// builtinFileRead reads content from a file
// fileRead(path) - returns string content
// fileRead(path, "binary") - returns base64 encoded string
func (i *Interpreter) builtinFileRead(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "fileRead requires at least 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to read file: %v", err)}
	}

	// Check if binary mode
	if len(args) > 1 {
		mode, _ := args[1].(string)
		if mode == "binary" {
			return map[string]Value{"success": true, "data": base64.StdEncoding.EncodeToString(data)}
		}
	}

	return map[string]Value{"success": true, "data": string(data)}
}

// builtinFileDelete deletes a file
func (i *Interpreter) builtinFileDelete(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "fileDelete requires 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	if err := os.Remove(path); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to delete file: %v", err)}
	}

	return map[string]Value{"success": true}
}

// builtinFileExists checks if a file exists
func (i *Interpreter) builtinFileExists(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	path, ok := args[0].(string)
	if !ok {
		return false
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	_, err := os.Stat(path)
	return err == nil
}

// builtinDirList lists files in a directory
// Returns array of {name, is_dir, size}
func (i *Interpreter) builtinDirList(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	path, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return []Value{}
	}

	result := make([]Value, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		size := int64(0)
		if err == nil {
			size = info.Size()
		}
		result = append(result, map[string]Value{
			"name":  entry.Name(),
			"isDir": entry.IsDir(),
			"size":  size,
		})
	}

	return result
}

// builtinDirCreate creates a directory
func (i *Interpreter) builtinDirCreate(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "dirCreate requires 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to create directory: %v", err)}
	}

	return map[string]Value{"success": true, "path": path}
}

// builtinDirDelete deletes a directory
func (i *Interpreter) builtinDirDelete(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "dirDelete requires 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	// Check if recursive
	recursive := false
	if len(args) > 1 {
		if r, ok := args[1].(bool); ok {
			recursive = r
		}
	}

	var err error
	if recursive {
		err = os.RemoveAll(path)
	} else {
		err = os.Remove(path)
	}

	if err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to delete directory: %v", err)}
	}

	return map[string]Value{"success": true}
}

// builtinFileServe serves a static file with proper Content-Type
// fileServe(path) - serves file with auto-detected content type
func (i *Interpreter) builtinFileServe(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "fileServe requires 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	// Check if HTTP writer is available
	if i.ctx.HTTPWriter == nil {
		return map[string]Value{"success": false, "error": "no HTTP context available"}
	}

	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to read file: %v", err)}
	}

	// Determine content type based on extension
	ext := strings.ToLower(filepath.Ext(path))
	contentType := "application/octet-stream"
	switch ext {
	case ".html", ".htm":
		contentType = "text/html; charset=utf-8"
	case ".css":
		contentType = "text/css; charset=utf-8"
	case ".js":
		contentType = "application/javascript"
	case ".json":
		contentType = "application/json"
	case ".txt":
		contentType = "text/plain; charset=utf-8"
	case ".xml":
		contentType = "application/xml"
	case ".png":
		contentType = "image/png"
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".gif":
		contentType = "image/gif"
	case ".svg":
		contentType = "image/svg+xml"
	case ".ico":
		contentType = "image/x-icon"
	case ".woff", ".woff2":
		contentType = "font/woff2"
	case ".ttf":
		contentType = "font/ttf"
	case ".pdf":
		contentType = "application/pdf"
	case ".zip":
		contentType = "application/zip"
	}

	// Set headers and write response
	i.ctx.HTTPWriter.Header().Set("Content-Type", contentType)
	i.ctx.HTTPWriter.Write(data)

	return map[string]Value{"success": true}
}

// ============================================================================
// Path Operations
// ============================================================================

// builtinPathJoin joins path components
func (i *Interpreter) builtinPathJoin(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		if s, ok := arg.(string); ok {
			parts = append(parts, s)
		}
	}
	result := filepath.Join(parts...)
	// Make relative to BaseDir if not absolute
	if i.ctx.BaseDir != "" && !filepath.IsAbs(result) {
		result = filepath.Join(i.ctx.BaseDir, result)
	}
	return result
}

// builtinPathBase returns the last element of the path
func (i *Interpreter) builtinPathBase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	path, ok := args[0].(string)
	if !ok {
		return ""
	}
	return filepath.Base(path)
}

// builtinPathDir returns the directory part of the path
func (i *Interpreter) builtinPathDir(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	path, ok := args[0].(string)
	if !ok {
		return ""
	}
	return filepath.Dir(path)
}

// builtinPathExt returns the file extension
func (i *Interpreter) builtinPathExt(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	path, ok := args[0].(string)
	if !ok {
		return ""
	}
	return filepath.Ext(path)
}

// builtinPathAbs returns the absolute path
func (i *Interpreter) builtinPathAbs(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	path, ok := args[0].(string)
	if !ok {
		return ""
	}
	// Resolve relative to BaseDir if not absolute
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

// builtinPathClean cleans the path (removes . and ..)
func (i *Interpreter) builtinPathClean(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	path, ok := args[0].(string)
	if !ok {
		return ""
	}
	return filepath.Clean(path)
}

// builtinPathSplit splits path into directory and file
func (i *Interpreter) builtinPathSplit(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"dir": "", "file": ""}
	}
	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"dir": "", "file": ""}
	}
	dir, file := filepath.Split(path)
	return map[string]Value{"dir": dir, "file": file}
}

// builtinPathIsAbs checks if the path is absolute
func (i *Interpreter) builtinPathIsAbs(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	path, ok := args[0].(string)
	if !ok {
		return false
	}
	return filepath.IsAbs(path)
}

// ============================================================================
// Extended File Operations
// ============================================================================

// builtinFileInfo returns detailed file information
func (i *Interpreter) builtinFileInfo(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "fileInfo requires 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	info, err := os.Stat(path)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":  true,
		"name":     info.Name(),
		"size":     info.Size(),
		"isDir":    info.IsDir(),
		"modTime":  info.ModTime().Format(time.RFC3339),
		"mode":     info.Mode().String(),
	}
}

// builtinFileCopy copies a file
func (i *Interpreter) builtinFileCopy(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "fileCopy requires 2 arguments (src, dst)"}
	}

	src, ok1 := args[0].(string)
	dst, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return map[string]Value{"success": false, "error": "paths must be strings"}
	}

	// Resolve paths relative to BaseDir
	if i.ctx.BaseDir != "" {
		if !filepath.IsAbs(src) {
			src = filepath.Join(i.ctx.BaseDir, src)
		}
		if !filepath.IsAbs(dst) {
			dst = filepath.Join(i.ctx.BaseDir, dst)
		}
	}

	// Read source file
	data, err := os.ReadFile(src)
	if err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to read source: %v", err)}
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to create directory: %v", err)}
	}

	// Write destination file
	if err := os.WriteFile(dst, data, 0644); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to write destination: %v", err)}
	}

	return map[string]Value{"success": true, "src": src, "dst": dst}
}

// builtinFileMove moves/renames a file
func (i *Interpreter) builtinFileMove(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "fileMove requires 2 arguments (src, dst)"}
	}

	src, ok1 := args[0].(string)
	dst, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return map[string]Value{"success": false, "error": "paths must be strings"}
	}

	// Resolve paths relative to BaseDir
	if i.ctx.BaseDir != "" {
		if !filepath.IsAbs(src) {
			src = filepath.Join(i.ctx.BaseDir, src)
		}
		if !filepath.IsAbs(dst) {
			dst = filepath.Join(i.ctx.BaseDir, dst)
		}
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to create directory: %v", err)}
	}

	if err := os.Rename(src, dst); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to move: %v", err)}
	}

	return map[string]Value{"success": true, "src": src, "dst": dst}
}

// builtinFileSize returns file size
func (i *Interpreter) builtinFileSize(args []Value) Value {
	if len(args) == 0 {
		return int64(-1)
	}

	path, ok := args[0].(string)
	if !ok {
		return int64(-1)
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	info, err := os.Stat(path)
	if err != nil {
		return int64(-1)
	}
	return info.Size()
}

// builtinFileModTime returns file modification time
func (i *Interpreter) builtinFileModTime(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	path, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	info, err := os.Stat(path)
	if err != nil {
		return ""
	}
	return info.ModTime().Format(time.RFC3339)
}

// builtinFileIsDir checks if path is a directory
func (i *Interpreter) builtinFileIsDir(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	path, ok := args[0].(string)
	if !ok {
		return false
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// builtinFileWalk walks directory tree recursively
func (i *Interpreter) builtinFileWalk(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	root, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(root) {
		root = filepath.Join(i.ctx.BaseDir, root)
	}

	result := []Value{}

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// Make path relative to root
		relPath, _ := filepath.Rel(root, path)
		result = append(result, map[string]Value{
			"path":    path,
			"relPath": relPath,
			"name":    info.Name(),
			"size":    info.Size(),
			"isDir":   info.IsDir(),
			"modTime": info.ModTime().Format(time.RFC3339),
		})
		return nil
	})

	return result
}

// builtinFileAppend appends content to a file
func (i *Interpreter) builtinFileAppend(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "fileAppend requires 2 arguments (path, content)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	// Get content
	var data []byte
	switch v := args[1].(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		data = []byte(fmt.Sprintf("%v", v))
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to create directory: %v", err)}
	}

	// Open file in append mode
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to open file: %v", err)}
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to append: %v", err)}
	}

	return map[string]Value{"success": true, "path": path}
}

// builtinFileGlob matches files by pattern
func (i *Interpreter) builtinFileGlob(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	pattern, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	// Resolve pattern relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(pattern) {
		pattern = filepath.Join(i.ctx.BaseDir, pattern)
	}

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return []Value{}
	}

	result := make([]Value, len(matches))
	for i, m := range matches {
		result[i] = m
	}
	return result
}

// builtinFileTouch creates an empty file or updates modification time
func (i *Interpreter) builtinFileTouch(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "fileTouch requires 1 argument (path)"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to create directory: %v", err)}
	}

	// Create or touch file
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to touch file: %v", err)}
	}
	f.Close()

	// Update modification time
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		return map[string]Value{"success": false, "error": fmt.Sprintf("failed to update time: %v", err)}
	}

	return map[string]Value{"success": true, "path": path}
}

// builtinDirExists checks if a directory exists
func (i *Interpreter) builtinDirExists(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	path, ok := args[0].(string)
	if !ok {
		return false
	}

	// Resolve path relative to BaseDir
	if i.ctx.BaseDir != "" && !filepath.IsAbs(path) {
		path = filepath.Join(i.ctx.BaseDir, path)
	}

	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// builtinDirCopy copies a directory recursively
func (i *Interpreter) builtinDirCopy(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "dirCopy requires 2 arguments (src, dst)"}
	}

	src, ok1 := args[0].(string)
	dst, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return map[string]Value{"success": false, "error": "paths must be strings"}
	}

	// Resolve paths relative to BaseDir
	if i.ctx.BaseDir != "" {
		if !filepath.IsAbs(src) {
			src = filepath.Join(i.ctx.BaseDir, src)
		}
		if !filepath.IsAbs(dst) {
			dst = filepath.Join(i.ctx.BaseDir, dst)
		}
	}

	// Walk and copy
	copied := 0
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate destination path
		relPath, _ := filepath.Rel(src, path)
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dstPath, data, info.Mode()); err != nil {
			return err
		}
		copied++
		return nil
	})

	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true, "src": src, "dst": dst, "filesCopied": copied}
}

// SetupBuiltins sets up built-in objects.
func (ctx *Context) SetupBuiltins() {
	ctx.Variables["http"] = NewHTTPObject(ctx)
	ctx.Variables["db"] = NewDBObject(ctx)
	ctx.Variables["true"] = true
	ctx.Variables["false"] = false
	ctx.Variables["null"] = nil
}