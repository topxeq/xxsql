// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/adler32"
	"hash/crc32"
	"html"
	"io"
	"math"
	mathrand "math/rand"
	"mime/quotedprintable"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/sha3"
	"golang.org/x/net/idna"

	"github.com/BurntSushi/toml"
	"github.com/skip2/go-qrcode"
	"gopkg.in/yaml.v3"

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
	Timezone    *time.Location
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
	// Error handling
	case "error":
		return i.builtinError(args), true
	case "isError":
		return i.builtinIsError(args), true
	case "errorMessage":
		return i.builtinErrorMessage(args), true
	case "errorWrap":
		return i.builtinErrorWrap(args), true
	case "throw":
		return i.builtinThrow(args), true
	case "assert":
		return i.builtinAssert(args), true
	case "assertEqual":
		return i.builtinAssertEqual(args), true
	case "assertNotEqual":
		return i.builtinAssertNotEqual(args), true
	case "assertNil":
		return i.builtinAssertNil(args), true
	case "assertNotNil":
		return i.builtinAssertNotNil(args), true
	case "assertTrue":
		return i.builtinAssertTrue(args), true
	case "assertFalse":
		return i.builtinAssertFalse(args), true
	case "ok":
		return i.builtinOk(args), true
	case "fail":
		return i.builtinFail(args), true
	case "must":
		return i.builtinMust(args), true
	case "recover":
		return i.builtinRecover(args), true
	case "panic":
		return i.builtinPanic(args), true
	case "defaultOnError":
		return i.builtinDefaultOnError(args), true
	case "tryGet":
		return i.builtinTryGet(args), true
	case "tryParse":
		return i.builtinTryParse(args), true
	case "safeCall":
		return i.builtinSafeCall(args), true
	case "errorFromResult":
		return i.builtinErrorFromResult(args), true
	case "resultOrError":
		return i.builtinResultOrError(args), true
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
	// Encoding/Decoding Functions
	case "base64Encode":
		return i.builtinBase64Encode(args), true
	case "base64Decode":
		return i.builtinBase64Decode(args), true
	case "base64URLEncode":
		return i.builtinBase64URLEncode(args), true
	case "base64URLDecode":
		return i.builtinBase64URLDecode(args), true
	case "base32Encode":
		return i.builtinBase32Encode(args), true
	case "base32Decode":
		return i.builtinBase32Decode(args), true
	case "hexEncode":
		return i.builtinHexEncode(args), true
	case "hexDecode":
		return i.builtinHexDecode(args), true
	case "rot13":
		return i.builtinRot13(args), true
	case "rotN":
		return i.builtinRotN(args), true
	case "caesarEncode":
		return i.builtinCaesarEncode(args), true
	case "caesarDecode":
		return i.builtinCaesarDecode(args), true
	case "atob":
		return i.builtinBase64Decode(args), true // alias
	case "btoa":
		return i.builtinBase64Encode(args), true // alias
	case "quotedPrintableEncode":
		return i.builtinQuotedPrintableEncode(args), true
	case "quotedPrintableDecode":
		return i.builtinQuotedPrintableDecode(args), true
	case "uuencode":
		return i.builtinUUEncode(args), true
	case "uudecode":
		return i.builtinUUDecode(args), true
	case "htmlEntityEncode":
		return i.builtinHTMLEntityEncode(args), true
	case "htmlEntityDecode":
		return i.builtinHTMLEntityDecode(args), true
	case "unicodeEncode":
		return i.builtinUnicodeEncode(args), true
	case "unicodeDecode":
		return i.builtinUnicodeDecode(args), true
	case "utf8Encode":
		return i.builtinUTF8Encode(args), true
	case "utf8Decode":
		return i.builtinUTF8Decode(args), true
	case "punycodeEncode":
		return i.builtinPunycodeEncode(args), true
	case "punycodeDecode":
		return i.builtinPunycodeDecode(args), true
	case "jsEscape":
		return i.builtinJSEscape(args), true
	case "jsUnescape":
		return i.builtinJSUnescape(args), true
	case "cEscape":
		return i.builtinCEscape(args), true
	case "cUnescape":
		return i.builtinCUnescape(args), true
	case "toBinary":
		return i.builtinToBinary(args), true
	case "fromBinary":
		return i.builtinFromBinary(args), true
	case "toOctal":
		return i.builtinToOctal(args), true
	case "fromOctal":
		return i.builtinFromOctal(args), true
	case "morseEncode":
		return i.builtinMorseEncode(args), true
	case "morseDecode":
		return i.builtinMorseDecode(args), true
	case "asciiToHex":
		return i.builtinASCIItoHex(args), true
	case "hexToAscii":
		return i.builtinHexToASCII(args), true
	case "strToBytes":
		return i.builtinStrToBytes(args), true
	case "bytesToStr":
		return i.builtinBytesToStr(args), true
	case "gzipCompress":
		return i.builtinGzipCompress(args), true
	case "gzipDecompress":
		return i.builtinGzipDecompress(args), true
	case "zlibCompress":
		return i.builtinZlibCompress(args), true
	case "zlibDecompress":
		return i.builtinZlibDecompress(args), true
	case "isBase64":
		return i.builtinIsBase64(args), true
	case "isHex":
		return i.builtinIsHex(args), true
	case "isBase32":
		return i.builtinIsBase32(args), true
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
	// Regular expression functions
	case "regexMatch":
		return i.builtinRegexMatch(args), true
	case "regexFind":
		return i.builtinRegexFind(args), true
	case "regexFindAll":
		return i.builtinRegexFindAll(args), true
	case "regexReplace":
		return i.builtinRegexReplace(args), true
	case "regexSplit":
		return i.builtinRegexSplit(args), true
	case "regexCompile":
		return i.builtinRegexCompile(args), true
	case "regexQuote":
		return i.builtinRegexQuote(args), true
	case "regexCount":
		return i.builtinRegexCount(args), true
	case "regexGroups":
		return i.builtinRegexGroups(args), true
	case "regexFindSubmatch":
		return i.builtinRegexFindSubmatch(args), true
	case "regexFindAllSubmatch":
		return i.builtinRegexFindAllSubmatch(args), true
	case "regexReplaceFunc":
		return i.builtinRegexReplaceFunc(args), true
	case "regexValid":
		return i.builtinRegexValid(args), true
	case "regexEscape":
		return i.builtinRegexQuote(args), true // alias
	// Advanced String Functions
	// Case Conversion
	case "camelCase":
		return i.builtinCamelCase(args), true
	case "snakeCase":
		return i.builtinSnakeCase(args), true
	case "kebabCase":
		return i.builtinKebabCase(args), true
	case "pascalCase":
		return i.builtinPascalCase(args), true
	case "sentenceCase":
		return i.builtinSentenceCase(args), true
	case "constantCase":
		return i.builtinConstantCase(args), true
	case "dotCase":
		return i.builtinDotCase(args), true
	case "pathCase":
		return i.builtinPathCase(args), true
	// Character Operations
	case "charAt":
		return i.builtinCharAt(args), true
	case "charCodeAt":
		return i.builtinCharCodeAt(args), true
	case "fromCharCode":
		return i.builtinFromCharCode(args), true
	case "isLower":
		return i.builtinIsLowerStr(args), true
	case "isUpper":
		return i.builtinIsUpperStr(args), true
	case "isSpace":
		return i.builtinIsSpaceStr(args), true
	case "isPrintable":
		return i.builtinIsPrintable(args), true
	case "isASCII":
		return i.builtinIsASCII(args), true
	// String Manipulation
	case "insert":
		return i.builtinInsertStr(args), true
	case "deleteStr":
		return i.builtinDeleteStr(args), true
	case "overwrite":
		return i.builtinOverwrite(args), true
	case "surround":
		return i.builtinSurround(args), true
	case "quote":
		return i.builtinQuote(args), true
	case "unquote":
		return i.builtinUnquote(args), true
	case "stripTags":
		return i.builtinStripTags(args), true
	case "stripPunctuation":
		return i.builtinStripPunctuation(args), true
	case "normalizeSpace":
		return i.builtinNormalizeSpace(args), true
	case "normalizeNewlines":
		return i.builtinNormalizeNewlines(args), true
	// Advanced Search/Replace
	case "replaceAll":
		return i.builtinReplaceAll(args), true
	case "replaceFirst":
		return i.builtinReplaceFirst(args), true
	case "replaceN":
		return i.builtinReplaceN(args), true
	case "replaceIgnoreCase":
		return i.builtinReplaceIgnoreCase(args), true
	// String Analysis
	case "levenshtein":
		return i.builtinLevenshtein(args), true
	case "commonPrefix":
		return i.builtinCommonPrefix(args), true
	case "commonSuffix":
		return i.builtinCommonSuffix(args), true
	case "isPalindrome":
		return i.builtinIsPalindrome(args), true
	case "isAnagram":
		return i.builtinIsAnagram(args), true
	case "charCount":
		return i.builtinCharCount(args), true
	case "byteCount":
		return i.builtinByteCount(args), true
	// String Splitting
	case "splitN":
		return i.builtinSplitN(args), true
	case "rsplit":
		return i.builtinRsplit(args), true
	case "strPartition":
		return i.builtinPartitionStr(args), true
	case "rpartition":
		return i.builtinRpartition(args), true
	// Unicode Support
	case "slugify":
		return i.builtinSlugify(args), true
	case "truncateWords":
		return i.builtinTruncateWords(args), true
	case "wordWrap":
		return i.builtinWordWrap(args), true
	case "dedent":
		return i.builtinDedent(args), true
	// Validation
	case "isEmail":
		return i.builtinIsEmail(args), true
	case "isURL":
		return i.builtinIsURL(args), true
	case "isUUID":
		return i.builtinIsUUID(args), true
	case "isIP":
		return i.builtinIsIP(args), true
	case "isCreditCard":
		return i.builtinIsCreditCard(args), true
	case "isHexColor":
		return i.builtinIsHexColor(args), true
	case "isJSON":
		return i.builtinIsJSONStr(args), true
	// String Utilities
	case "format":
		return i.builtinFormat(args), true
	case "template":
		return i.builtinTemplate(args), true
	case "repeatUntil":
		return i.builtinRepeatUntil(args), true
	case "padBetween":
		return i.builtinPadBetween(args), true
	case "unwrap":
		return i.builtinUnwrap(args), true
	case "toSize":
		return i.builtinToSize(args), true
	case "fromSize":
		return i.builtinFromSize(args), true
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
	case "sortBy":
		return i.builtinSortBy(args), true
	case "sortByDesc":
		return i.builtinSortByDesc(args), true
	case "sortStrings":
		return i.builtinSortStrings(args), true
	case "sortStringsDesc":
		return i.builtinSortStringsDesc(args), true
	case "sortNatural":
		return i.builtinSortNatural(args), true
	case "sortNaturalDesc":
		return i.builtinSortNaturalDesc(args), true
	case "sortMulti":
		return i.builtinSortMulti(args), true
	case "rank":
		return i.builtinRank(args), true
	case "rankBy":
		return i.builtinRankBy(args), true
	case "denseRank":
		return i.builtinDenseRank(args), true
	case "topN":
		return i.builtinTopN(args), true
	case "bottomN":
		return i.builtinBottomN(args), true
	case "partition":
		return i.builtinPartition(args), true
	case "groupBySorted":
		return i.builtinGroupBySorted(args), true
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
	// Advanced array functions
	case "rotate":
		return i.builtinRotate(args), true
	case "slide":
		return i.builtinSlide(args), true
	case "window":
		return i.builtinWindow(args), true
	case "pairwise":
		return i.builtinPairwise(args), true
	case "transpose":
		return i.builtinTranspose(args), true
	case "fill":
		return i.builtinFill(args), true
	case "fillRange":
		return i.builtinFillRange(args), true
	case "insertAt":
		return i.builtinInsertAt(args), true
	case "removeAt":
		return i.builtinRemoveAt(args), true
	case "removeFirst":
		return i.builtinRemoveFirst(args), true
	case "removeLast":
		return i.builtinRemoveLast(args), true
	case "removeAll":
		return i.builtinRemoveAll(args), true
	case "replaceAt":
		return i.builtinReplaceAt(args), true
	case "swap":
		return i.builtinSwap(args), true
	case "move":
		return i.builtinMove(args), true
	case "compact":
		return i.builtinCompact(args), true
	case "compactFlat":
		return i.builtinCompactFlat(args), true
	case "uniqBy":
		return i.builtinUniqBy(args), true
	case "differenceBy":
		return i.builtinDifferenceBy(args), true
	case "intersectionBy":
		return i.builtinIntersectionBy(args), true
	case "unionBy":
		return i.builtinUnionBy(args), true
	case "findIndex":
		return i.builtinFindIndex(args), true
	case "findLastIndex":
		return i.builtinFindLastIndex(args), true
	case "indicesOf":
		return i.builtinIndicesOf(args), true
	case "indexOfAll":
		return i.builtinIndexOfAll(args), true
	case "takeWhile":
		return i.builtinTakeWhile(args), true
	case "dropWhile":
		return i.builtinDropWhile(args), true
	case "span":
		return i.builtinSpan(args), true
	case "breakList":
		return i.builtinBreakList(args), true
	case "splitAt":
		return i.builtinSplitAt(args), true
	case "splitWhen":
		return i.builtinSplitWhen(args), true
	case "aperture":
		return i.builtinAperture(args), true
	case "xprod":
		return i.builtinXprod(args), true
	case "fromPairs":
		return i.builtinFromPairs(args), true
	case "toPairs":
		return i.builtinToPairs(args), true
	case "rangeStep":
		return i.builtinRangeStep(args), true
	case "repeatAll":
		return i.builtinRepeatAll(args), true
	case "cycle":
		return i.builtinCycle(args), true
	case "iterate":
		return i.builtinIterate(args), true
	case "prependAll":
		return i.builtinPrependAll(args), true
	case "appendAll":
		return i.builtinAppendAll(args), true
	case "intersperse":
		return i.builtinIntersperse(args), true
	case "intercalate":
		return i.builtinIntercalate(args), true
	case "subsequences":
		return i.builtinSubsequences(args), true
	case "permutations":
		return i.builtinPermutations(args), true
	case "mode":
		return i.builtinMode(args), true
	case "stdDev":
		return i.builtinStdDev(args), true
	case "minBy":
		return i.builtinMinBy(args), true
	case "maxBy":
		return i.builtinMaxBy(args), true
	// Crypto/Hash functions
	case "md5":
		return i.builtinMD5(args), true
	case "sha1":
		return i.builtinSHA1(args), true
	case "sha256":
		return i.builtinSHA256(args), true
	case "sha512":
		return i.builtinSHA512(args), true
	case "sha224":
		return i.builtinSHA224(args), true
	case "sha384":
		return i.builtinSHA384(args), true
	case "sha3_256":
		return i.builtinSHA3_256(args), true
	case "sha3_512":
		return i.builtinSHA3_512(args), true
	case "blake2b256":
		return i.builtinBlake2b256(args), true
	case "blake2b512":
		return i.builtinBlake2b512(args), true
	case "crc32":
		return i.builtinCRC32(args), true
	case "adler32":
		return i.builtinAdler32(args), true
	// HMAC functions
	case "hmacSHA1":
		return i.builtinHmacSHA1(args), true
	case "hmacSHA256":
		return i.builtinHmacSHA256(args), true
	case "hmacSHA512":
		return i.builtinHmacSHA512(args), true
	// Password hashing
	case "bcryptHash":
		return i.builtinBcryptHash(args), true
	case "bcryptVerify":
		return i.builtinBcryptVerify(args), true
	case "argon2id":
		return i.builtinArgon2id(args), true
	// Key derivation
	case "pbkdf2":
		return i.builtinPBKDF2(args), true
	case "hkdf":
		return i.builtinHKDF(args), true
	// Random generation
	case "randomBytes":
		return i.builtinRandomBytes(args), true
	case "randomHex":
		return i.builtinRandomHex(args), true
	case "randomString":
		return i.builtinRandomString(args), true
	case "generatePassword":
		return i.builtinGeneratePassword(args), true
	case "uuid":
		return i.builtinUUID(args), true
	case "uuidv4":
		return i.builtinUUID(args), true // alias
	case "uuidv7":
		return i.builtinUUIDv7(args), true
	// Simple crypto
	case "xorEncrypt":
		return i.builtinXorEncrypt(args), true
	case "xorDecrypt":
		return i.builtinXorDecrypt(args), true
	// JWT (simple)
	case "jwtEncode":
		return i.builtinJWTEncode(args), true
	case "jwtDecode":
		return i.builtinJWTDecode(args), true
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
	case "jsonMinify":
		return i.builtinJSONMinify(args), true
	case "jsonGet":
		return i.builtinJSONGet(args), true
	case "jsonSet":
		return i.builtinJSONSet(args), true
	case "jsonDelete":
		return i.builtinJSONDelete(args), true
	case "jsonHas":
		return i.builtinJSONHas(args), true
	case "jsonKeys":
		return i.builtinJSONKeys(args), true
	case "jsonValues":
		return i.builtinJSONValues(args), true
	case "jsonType":
		return i.builtinJSONType(args), true
	case "jsonMerge":
		return i.builtinJSONMerge(args), true
	case "jsonDeepMerge":
		return i.builtinJSONDeepMerge(args), true
	case "jsonArrayLength":
		return i.builtinJSONArrayLength(args), true
	case "jsonArrayAppend":
		return i.builtinJSONArrayAppend(args), true
	case "jsonArrayPrepend":
		return i.builtinJSONArrayPrepend(args), true
	case "jsonArrayFlatten":
		return i.builtinJSONArrayFlatten(args), true
	case "jsonObjectFromArrays":
		return i.builtinJSONObjectFromArrays(args), true
	case "jsonValidate":
		return i.builtinJSONValidate(args), true
	case "jsonOmit":
		return i.builtinJSONOmit(args), true
	case "jsonPick":
		return i.builtinJSONPick(args), true
	case "jsonTransform":
		return i.builtinJSONTransform(args), true
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
	// Date/Time functions - formatting
	case "dateFormat":
		return i.builtinDateFormat(args), true
	case "dateParse":
		return i.builtinDateParse(args), true
	case "strftime":
		return i.builtinStrftime(args), true
	case "strptime":
		return i.builtinStrptime(args), true
	// Date/Time functions - creation
	case "date":
		return i.builtinDate(args), true
	case "dateFromUnix":
		return i.builtinDateFromUnix(args), true
	case "dateNow":
		return i.builtinDateNow(args), true
	case "today":
		return i.builtinToday(args), true
	// Date/Time functions - extraction
	case "year":
		return i.builtinYear(args), true
	case "month":
		return i.builtinMonth(args), true
	case "day":
		return i.builtinDay(args), true
	case "hour":
		return i.builtinHour(args), true
	case "minute":
		return i.builtinMinute(args), true
	case "second":
		return i.builtinSecond(args), true
	case "weekday":
		return i.builtinWeekday(args), true
	case "yearday":
		return i.builtinYearday(args), true
	case "week":
		return i.builtinWeek(args), true
	case "quarter":
		return i.builtinQuarter(args), true
	// Date/Time functions - manipulation
	case "dateAdd":
		return i.builtinDateAdd(args), true
	case "dateSub":
		return i.builtinDateSub(args), true
	case "dateAddDays":
		return i.builtinDateAddDays(args), true
	case "dateAddMonths":
		return i.builtinDateAddMonths(args), true
	case "dateAddYears":
		return i.builtinDateAddYears(args), true
	case "startOfDay":
		return i.builtinStartOfDay(args), true
	case "endOfDay":
		return i.builtinEndOfDay(args), true
	case "startOfWeek":
		return i.builtinStartOfWeek(args), true
	case "endOfWeek":
		return i.builtinEndOfWeek(args), true
	case "startOfMonth":
		return i.builtinStartOfMonth(args), true
	case "endOfMonth":
		return i.builtinEndOfMonth(args), true
	case "startOfYear":
		return i.builtinStartOfYear(args), true
	case "endOfYear":
		return i.builtinEndOfYear(args), true
	// Date/Time functions - comparison
	case "dateDiff":
		return i.builtinDateDiff(args), true
	case "dateCompare":
		return i.builtinDateCompare(args), true
	case "dateBefore":
		return i.builtinDateBefore(args), true
	case "dateAfter":
		return i.builtinDateAfter(args), true
	case "dateEqual":
		return i.builtinDateEqual(args), true
	case "dateBetween":
		return i.builtinDateBetween(args), true
	// Date/Time functions - utilities
	case "isLeapYear":
		return i.builtinIsLeapYear(args), true
	case "daysInMonth":
		return i.builtinDaysInMonth(args), true
	case "daysInYear":
		return i.builtinDaysInYear(args), true
	case "parseDuration":
		return i.builtinParseDuration(args), true
	case "formatDuration":
		return i.builtinFormatDuration(args), true
	case "age":
		return i.builtinAge(args), true
	case "isWeekend":
		return i.builtinIsWeekend(args), true
	case "isWorkday":
		return i.builtinIsWorkday(args), true
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
	case "csvStringify":
		return i.builtinCSVStringify(args), true
	case "csvEncode":
		return i.builtinCSVStringify(args), true // alias
	// Format - XML
	case "xmlParse":
		return i.builtinXMLParse(args), true
	case "xmlStringify":
		return i.builtinXMLStringify(args), true
	case "xmlEncode":
		return i.builtinXMLStringify(args), true // alias
	case "xmlGet":
		return i.builtinXMLGet(args), true
	// Format - YAML
	case "yamlParse":
		return i.builtinYAMLParse(args), true
	case "yamlStringify":
		return i.builtinYAMLStringify(args), true
	case "yamlEncode":
		return i.builtinYAMLStringify(args), true // alias
	// Format - TOML
	case "tomlParse":
		return i.builtinTOMLParse(args), true
	case "tomlStringify":
		return i.builtinTOMLStringify(args), true
	case "tomlEncode":
		return i.builtinTOMLStringify(args), true // alias
	// Format - Markdown
	case "markdownToHTML":
		return i.builtinMarkdownToHTML(args), true
	case "htmlToMarkdown":
		return i.builtinHTMLToMarkdown(args), true
	// Network - HTTP Extended
	case "httpDownload":
		return i.builtinHTTPDownload(args), true
	case "httpUpload":
		return i.builtinHTTPUpload(args), true
	case "httpHead":
		return i.builtinHTTPHead(args), true
	case "httpPatch":
		return i.builtinHTTPPatch(args), true
	case "httpOptions":
		return i.builtinHTTPOptions(args), true
	// Network - Connectivity
	case "ping":
		return i.builtinPing(args), true
	case "portCheck":
		return i.builtinPortCheck(args), true
	case "portScan":
		return i.builtinPortScan(args), true
	// Concurrency
	case "retry":
		return i.builtinRetry(args), true
	case "parallel":
		return i.builtinParallel(args), true
	case "timeout":
		return i.builtinTimeout(args), true
	// Random Generation
	case "randomPassword":
		return i.builtinRandomPassword(args), true
	case "randomColor":
		return i.builtinRandomColor(args), true
	case "randomName":
		return i.builtinRandomName(args), true
	case "randomUUID":
		return i.builtinUUID(args), true // alias
	case "randomToken":
		return i.builtinRandomToken(args), true
	// QR Code
	case "qrEncode":
		return i.builtinQREncode(args), true
	case "qrDataURL":
		return i.builtinQRDataURL(args), true
	// Cache functions
	case "cacheSet":
		return i.builtinCacheSet(args), true
	case "cacheGet":
		return i.builtinCacheGet(args), true
	case "cacheDel":
		return i.builtinCacheDel(args), true
	case "cacheHas":
		return i.builtinCacheHas(args), true
	case "cacheClear":
		return i.builtinCacheClear(args), true
	case "cacheKeys":
		return i.builtinCacheKeys(args), true
	// Security Enhancement
	case "jwtSign":
		return i.builtinJWTSign(args), true
	case "jwtVerify":
		return i.builtinJWTVerify(args), true
	case "hashPassword":
		return i.builtinHashPassword(args), true
	case "verifyPassword":
		return i.builtinVerifyPassword(args), true
	case "generateSecret":
		return i.builtinGenerateSecret(args), true
	case "encryptAES":
		return i.builtinEncryptAES(args), true
	case "decryptAES":
		return i.builtinDecryptAES(args), true
	// Validation and Sanitization
	case "validate":
		return i.builtinValidate(args), true
	case "sanitize":
		return i.builtinSanitize(args), true
	case "normalizeEmail":
		return i.builtinNormalizeEmail(args), true
	case "normalizePhone":
		return i.builtinNormalizePhone(args), true
	case "validatePassword":
		return i.builtinValidatePassword(args), true
	case "isStrongPassword":
		return i.builtinValidatePassword(args), true // alias
	// Template and Rendering
	case "renderTemplate":
		return i.builtinRenderTemplate(args), true
	case "minify":
		return i.builtinMinify(args), true
	case "beautify":
		return i.builtinBeautify(args), true
	// Email and Notification
	case "sendEmail":
		return i.builtinSendEmail(args), true
	case "sendWebhook":
		return i.builtinSendWebhook(args), true
	// Random Generation Extended
	case "randomAvatar":
		return i.builtinRandomAvatar(args), true
	case "generateLorem":
		return i.builtinGenerateLorem(args), true
	case "faker":
		return i.builtinFaker(args), true
	// Internationalization
	case "getTimezone":
		return i.builtinGetTimezone(args), true
	case "setTimezone":
		return i.builtinSetTimezone(args), true
	case "listTimezones":
		return i.builtinListTimezones(args), true
	// Image Processing
	case "imageInfo":
		return i.builtinImageInfo(args), true
	case "imageToBase64":
		return i.builtinImageToBase64(args), true
	case "base64ToImage":
		return i.builtinBase64ToImage(args), true
	case "barcodeEncode":
		return i.builtinBarcodeEncode(args), true
	// Data Structures
	case "newStack":
		return i.builtinNewStack(args), true
	case "newQueue":
		return i.builtinNewQueue(args), true
	case "newSet":
		return i.builtinNewSet(args), true
	// Debug and Testing
	case "debug":
		return i.builtinDebug(args), true
	case "benchmark":
		return i.builtinBenchmark(args), true
	case "mock":
		return i.builtinMock(args), true
	// Configuration Management
	case "loadConfig":
		return i.builtinLoadConfig(args), true
	case "saveConfig":
		return i.builtinSaveConfig(args), true
	case "getSecret":
		return i.builtinGetSecret(args), true
	// System Extended
	case "getMemory":
		return i.builtinGetMemory(args), true
	case "getCPU":
		return i.builtinGetCPU(args), true
	case "killProcess":
		return i.builtinKillProcess(args), true
	// Network Extended
	case "ipLookup":
		return i.builtinIPLookup(args), true
	case "whois":
		return i.builtinWhois(args), true
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
	// Phase 2: WebSocket Support
	case "wsConnect":
		return i.builtinWSConnect(args), true
	case "wsSend":
		return i.builtinWSSend(args), true
	case "wsReceive":
		return i.builtinWSReceive(args), true
	case "wsClose":
		return i.builtinWSClose(args), true
	case "wsIsConnected":
		return i.builtinWSIsConnected(args), true
	// Phase 2: Redis Client
	case "redisConnect":
		return i.builtinRedisConnect(args), true
	case "redisGet":
		return i.builtinRedisGet(args), true
	case "redisSet":
		return i.builtinRedisSet(args), true
	case "redisDel":
		return i.builtinRedisDel(args), true
	case "redisExists":
		return i.builtinRedisExists(args), true
	case "redisExpire":
		return i.builtinRedisExpire(args), true
	case "redisIncr":
		return i.builtinRedisIncr(args), true
	case "redisDecr":
		return i.builtinRedisDecr(args), true
	case "redisLPush":
		return i.builtinRedisLPush(args), true
	case "redisRPush":
		return i.builtinRedisRPush(args), true
	case "redisLPop":
		return i.builtinRedisLPop(args), true
	case "redisRPop":
		return i.builtinRedisRPop(args), true
	case "redisHSet":
		return i.builtinRedisHSet(args), true
	case "redisHGet":
		return i.builtinRedisHGet(args), true
	case "redisHDel":
		return i.builtinRedisHDel(args), true
	case "redisHGetAll":
		return i.builtinRedisHGetAll(args), true
	case "redisKeys":
		return i.builtinRedisKeys(args), true
	case "redisTTL":
		return i.builtinRedisTTL(args), true
	// Phase 2: PDF Processing
	case "pdfCreate":
		return i.builtinPDFCreate(args), true
	case "pdfAddPage":
		return i.builtinPDFAddPage(args), true
	case "pdfAddText":
		return i.builtinPDFAddText(args), true
	case "pdfSetFont":
		return i.builtinPDFSetFont(args), true
	case "pdfSave":
		return i.builtinPDFSave(args), true
	case "pdfCell":
		return i.builtinPDFCell(args), true
	// Phase 2: Excel Processing
	case "excelCreate":
		return i.builtinExcelCreate(args), true
	case "excelOpen":
		return i.builtinExcelOpen(args), true
	case "excelSetCell":
		return i.builtinExcelSetCell(args), true
	case "excelGetCell":
		return i.builtinExcelGetCell(args), true
	case "excelNewSheet":
		return i.builtinExcelNewSheet(args), true
	case "excelSave":
		return i.builtinExcelSave(args), true
	case "excelClose":
		return i.builtinExcelClose(args), true
	// Phase 2: Charts
	case "chartLine":
		return i.builtinChartLine(args), true
	case "chartBar":
		return i.builtinChartBar(args), true
	case "chartPie":
		return i.builtinChartPie(args), true
	// Phase 2: Job Scheduling
	case "cronParse":
		return i.builtinCronParse(args), true
	case "cronNext":
		return i.builtinCronNext(args), true
	case "cronNextN":
		return i.builtinCronNextN(args), true
	// Phase 2: Geo Location
	case "geoDistance":
		return i.builtinGeoDistance(args), true
	case "geoEncode":
		return i.builtinGeoEncode(args), true
	case "geoDecode":
		return i.builtinGeoDecode(args), true
	case "geoIP":
		return i.builtinGeoIP(args), true
	case "geoBoundingBox":
		return i.builtinGeoBoundingBox(args), true
	// Phase 2: HTML Parsing
	case "htmlParse":
		return i.builtinHTMLParse(args), true
	case "htmlSelect":
		return i.builtinHTMLSelect(args), true
	case "htmlSelectAll":
		return i.builtinHTMLSelectAll(args), true
	case "htmlAttr":
		return i.builtinHTMLAttr(args), true
	case "htmlText":
		return i.builtinHTMLText(args), true
	case "htmlLinks":
		return i.builtinHTMLLinks(args), true
	case "rssParse":
		return i.builtinRSSParse(args), true
	// Phase 2: ML Simplified
	case "mlTokenize":
		return i.builtinMLTokenize(args), true
	case "mlSentiment":
		return i.builtinMLSentiment(args), true
	case "mlSimilarity":
		return i.builtinMLSimilarity(args), true
	case "mlKeywords":
		return i.builtinMLKeywords(args), true
	case "mlNgrams":
		return i.builtinMLNgrams(args), true
	case "mlWordFreq":
		return i.builtinMLWordFreq(args), true
	// Phase 2: State Machine
	case "stateMachine":
		return i.builtinStateMachine(args), true
	case "stateTransition":
		return i.builtinStateTransition(args), true
	// Phase 2: Rate Limiting
	case "rateLimiter":
		return i.builtinRateLimiter(args), true
	case "rateLimitCheck":
		return i.builtinRateLimitCheck(args), true
	// Phase 2: Expression Evaluation
	case "exprEval":
		return i.builtinExprEval(args), true
	// Phase 2: Git Operations
	case "gitStatus":
		return i.builtinGitStatus(args), true
	case "gitLog":
		return i.builtinGitLog(args), true
	case "gitBranch":
		return i.builtinGitBranch(args), true
	// Phase 2: Metrics
	case "metricsCounter":
		return i.builtinMetricsCounter(args), true
	case "metricsGauge":
		return i.builtinMetricsGauge(args), true
	case "metricsGet":
		return i.builtinMetricsGet(args), true
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
// Error Handling Functions
// ============================================================================

// ErrorValue is a special type for error values
type ErrorValue struct {
	Message string
	Type    string
	Cause   error
}

// builtinError creates an error value
func (i *Interpreter) builtinError(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":  true,
			"message": "",
			"type":   "error",
		}
	}

	msg := ""
	errType := "error"

	switch v := args[0].(type) {
	case string:
		msg = v
	case map[string]Value:
		if m, ok := v["message"].(string); ok {
			msg = m
		}
		if t, ok := v["type"].(string); ok {
			errType = t
		}
	default:
		msg = fmt.Sprintf("%v", args[0])
	}

	// Check for additional type argument
	if len(args) > 1 {
		if t, ok := args[1].(string); ok {
			errType = t
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    errType,
	}
}

// builtinIsError checks if a value is an error
func (i *Interpreter) builtinIsError(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	switch v := args[0].(type) {
	case map[string]Value:
		if err, ok := v["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return true
			}
		}
	case error:
		return true
	case string:
		// Check if it looks like an error message
		return strings.HasPrefix(v, "error:") || strings.HasPrefix(v, "Error:")
	}
	return false
}

// builtinErrorMessage extracts the error message
func (i *Interpreter) builtinErrorMessage(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	switch v := args[0].(type) {
	case map[string]Value:
		if msg, ok := v["message"].(string); ok {
			return msg
		}
		if err, ok := v["error"].(string); ok {
			return err
		}
	case error:
		return v.Error()
	case string:
		return v
	}
	return fmt.Sprintf("%v", args[0])
}

// builtinErrorWrap wraps an error with additional context
func (i *Interpreter) builtinErrorWrap(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	origMsg := ""
	errType := "error"

	switch v := args[0].(type) {
	case map[string]Value:
		if msg, ok := v["message"].(string); ok {
			origMsg = msg
		}
		if t, ok := v["type"].(string); ok {
			errType = t
		}
	case string:
		origMsg = v
	case error:
		origMsg = v.Error()
	default:
		origMsg = fmt.Sprintf("%v", args[0])
	}

	wrapMsg := ""
	switch v := args[1].(type) {
	case string:
		wrapMsg = v
	default:
		wrapMsg = fmt.Sprintf("%v", v)
	}

	return map[string]Value{
		"error":    true,
		"message":  wrapMsg + ": " + origMsg,
		"type":     errType,
		"original": args[0],
	}
}

// builtinThrow throws an error (returns error object)
func (i *Interpreter) builtinThrow(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "throw called",
			"type":    "throw",
		}
	}

	msg := ""
	errType := "throw"

	switch v := args[0].(type) {
	case string:
		msg = v
	case map[string]Value:
		if m, ok := v["message"].(string); ok {
			msg = m
		}
		if t, ok := v["type"].(string); ok {
			errType = t
		}
	default:
		msg = fmt.Sprintf("%v", args[0])
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    errType,
	}
}

// builtinAssert asserts a condition, throws if false
func (i *Interpreter) builtinAssert(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "assertion failed: no condition provided",
			"type":    "assert",
		}
	}

	condition := i.toBool(args[0])
	if condition {
		return true
	}

	msg := "assertion failed"
	if len(args) > 1 {
		if m, ok := args[1].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinAssertEqual asserts two values are equal
func (i *Interpreter) builtinAssertEqual(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{
			"error":   true,
			"message": "assertEqual requires two arguments",
			"type":    "assert",
		}
	}

	if i.isEqual(args[0], args[1]) {
		return true
	}

	msg := fmt.Sprintf("assertEqual failed: %v != %v", args[0], args[1])
	if len(args) > 2 {
		if m, ok := args[2].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinAssertNotEqual asserts two values are not equal
func (i *Interpreter) builtinAssertNotEqual(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{
			"error":   true,
			"message": "assertNotEqual requires two arguments",
			"type":    "assert",
		}
	}

	if !i.isEqual(args[0], args[1]) {
		return true
	}

	msg := fmt.Sprintf("assertNotEqual failed: %v == %v", args[0], args[1])
	if len(args) > 2 {
		if m, ok := args[2].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinAssertNil asserts value is nil
func (i *Interpreter) builtinAssertNil(args []Value) Value {
	if len(args) == 0 {
		return true
	}

	if args[0] == nil {
		return true
	}

	msg := fmt.Sprintf("assertNil failed: %v is not nil", args[0])
	if len(args) > 1 {
		if m, ok := args[1].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinAssertNotNil asserts value is not nil
func (i *Interpreter) builtinAssertNotNil(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "assertNotNil failed: value is nil",
			"type":    "assert",
		}
	}

	if args[0] != nil {
		return true
	}

	msg := "assertNotNil failed: value is nil"
	if len(args) > 1 {
		if m, ok := args[1].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinAssertTrue asserts value is true
func (i *Interpreter) builtinAssertTrue(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "assertTrue failed: no value provided",
			"type":    "assert",
		}
	}

	if i.toBool(args[0]) {
		return true
	}

	msg := fmt.Sprintf("assertTrue failed: %v is not true", args[0])
	if len(args) > 1 {
		if m, ok := args[1].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinAssertFalse asserts value is false
func (i *Interpreter) builtinAssertFalse(args []Value) Value {
	if len(args) == 0 {
		return true
	}

	if !i.toBool(args[0]) {
		return true
	}

	msg := fmt.Sprintf("assertFalse failed: %v is not false", args[0])
	if len(args) > 1 {
		if m, ok := args[1].(string); ok {
			msg = m
		}
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "assert",
	}
}

// builtinOk checks if a result is ok (not an error)
func (i *Interpreter) builtinOk(args []Value) Value {
	if len(args) == 0 {
		return true
	}

	// Check if it's an error object
	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return false
			}
		}
	}

	return true
}

// builtinFail checks if a result is an error
func (i *Interpreter) builtinFail(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	// Check if it's an error object
	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return true
			}
		}
	}

	return false
}

// builtinMust returns the value or throws if it's an error
func (i *Interpreter) builtinMust(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "must: no value provided",
			"type":    "must",
		}
	}

	// Check if it's an error object
	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return map[string]Value{
					"error":   true,
					"message": "must: " + i.builtinErrorMessage(args).([]interface{})[0].(string),
					"type":    "must",
				}
			}
		}
	}

	return args[0]
}

// builtinRecover recovers from an error, returning a default value
func (i *Interpreter) builtinRecover(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	// Check if first arg is an error
	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return args[1] // Return default value
			}
		}
	}

	return args[0] // Return original value if not an error
}

// builtinPanic creates a panic error
func (i *Interpreter) builtinPanic(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "panic",
			"type":    "panic",
		}
	}

	msg := ""
	switch v := args[0].(type) {
	case string:
		msg = v
	default:
		msg = fmt.Sprintf("%v", v)
	}

	return map[string]Value{
		"error":   true,
		"message": msg,
		"type":    "panic",
	}
}

// builtinDefaultOnError returns a default value if the result is an error
func (i *Interpreter) builtinDefaultOnError(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	// Check if first arg is an error
	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return args[1]
			}
		}
	}

	return args[0]
}

// builtinTryGet safely gets a value from an object/array
func (i *Interpreter) builtinTryGet(args []Value) Value {
	if len(args) < 2 {
		return nil
	}

	switch obj := args[0].(type) {
	case map[string]Value:
		key, ok := args[1].(string)
		if !ok {
			return nil
		}
		if val, ok := obj[key]; ok {
			return val
		}
	case []Value:
		idx := i.toInt(args[1])
		if idx >= 0 && idx < len(obj) {
			return obj[idx]
		}
	}

	// Return default if provided
	if len(args) > 2 {
		return args[2]
	}
	return nil
}

// builtinTryParse tries to parse a value, returns default on failure
func (i *Interpreter) builtinTryParse(args []Value) Value {
	if len(args) < 2 {
		return nil
	}

	parseType, ok := args[1].(string)
	if !ok {
		return args[0]
	}

	var result Value = args[0]
	var err error

	switch parseType {
	case "int":
		result, err = strconv.Atoi(fmt.Sprintf("%v", args[0]))
	case "float":
		result, err = strconv.ParseFloat(fmt.Sprintf("%v", args[0]), 64)
	case "bool":
		result, err = strconv.ParseBool(fmt.Sprintf("%v", args[0]))
	case "json":
		var parsed interface{}
		if s, ok := args[0].(string); ok {
			err = json.Unmarshal([]byte(s), &parsed)
			if err == nil {
				result = convertJSONToValue(parsed)
			}
		} else {
			return args[0]
		}
	default:
		return args[0]
	}

	if err != nil {
		// Return default if provided
		if len(args) > 2 {
			return args[2]
		}
		return nil
	}

	return result
}

// builtinSafeCall safely calls a function, catching errors
func (i *Interpreter) builtinSafeCall(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"error":   true,
			"message": "safeCall: no function name provided",
			"type":    "safeCall",
		}
	}

	funcName, ok := args[0].(string)
	if !ok {
		return map[string]Value{
			"error":   true,
			"message": "safeCall: function name must be a string",
			"type":    "safeCall",
		}
	}

	// Get call arguments
	callArgs := []Value{}
	if len(args) > 1 {
		callArgs = args[1:]
	}

	// Try to call the function
	defer func() {
		if r := recover(); r != nil {
			// Handle panic - but we can't modify the return value here
		}
	}()

	result, handled := i.callBuiltin(funcName, callArgs)
	if !handled {
		return map[string]Value{
			"error":   true,
			"message": fmt.Sprintf("safeCall: unknown function '%s'", funcName),
			"type":    "safeCall",
		}
	}

	// Check if result is an error
	if m, ok := result.(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				// Return default if provided (already in callArgs as last arg)
				return result
			}
		}
	}

	return result
}

// builtinErrorFromResult extracts error info from a result
func (i *Interpreter) builtinErrorFromResult(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"hasError": false,
			"message":  "",
			"type":     "",
		}
	}

	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				msg := ""
				errType := ""
				if m, ok := m["message"].(string); ok {
					msg = m
				}
				if t, ok := m["type"].(string); ok {
					errType = t
				}
				return map[string]Value{
					"hasError": true,
					"message":  msg,
					"type":     errType,
				}
			}
		}
	}

	return map[string]Value{
		"hasError": false,
		"message":  "",
		"type":     "",
	}
}

// builtinResultOrError returns a result object with value or error
func (i *Interpreter) builtinResultOrError(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{
			"ok":    true,
			"value": nil,
			"error": nil,
		}
	}

	// Check if it's an error
	if m, ok := args[0].(map[string]Value); ok {
		if err, ok := m["error"]; ok {
			if b, ok := err.(bool); ok && b {
				return map[string]Value{
					"ok":    false,
					"value": nil,
					"error": m,
				}
			}
		}
	}

	return map[string]Value{
		"ok":    true,
		"value": args[0],
		"error": nil,
	}
}

// Helper functions for error handling

func (i *Interpreter) toBool(v Value) bool {
	switch val := v.(type) {
	case bool:
		return val
	case int:
		return val != 0
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "false" && val != "0"
	case nil:
		return false
	default:
		return true
	}
}

func (i *Interpreter) isEqual(a, b Value) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try type-specific comparisons
	switch aVal := a.(type) {
	case bool:
		if bVal, ok := b.(bool); ok {
			return aVal == bVal
		}
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal == bVal
		case int64:
			return int64(aVal) == bVal
		case float64:
			return float64(aVal) == bVal
		}
	case int64:
		switch bVal := b.(type) {
		case int:
			return aVal == int64(bVal)
		case int64:
			return aVal == bVal
		case float64:
			return float64(aVal) == bVal
		}
	case float64:
		switch bVal := b.(type) {
		case int:
			return aVal == float64(bVal)
		case int64:
			return aVal == float64(bVal)
		case float64:
			return aVal == bVal
		}
	case string:
		if bVal, ok := b.(string); ok {
			return aVal == bVal
		}
	case []Value:
		if bVal, ok := b.([]Value); ok {
			if len(aVal) != len(bVal) {
				return false
			}
			for idx := range aVal {
				if !i.isEqual(aVal[idx], bVal[idx]) {
					return false
				}
			}
			return true
		}
	case map[string]Value:
		if bVal, ok := b.(map[string]Value); ok {
			if len(aVal) != len(bVal) {
				return false
			}
			for k, v := range aVal {
				if bV, ok := bVal[k]; !ok || !i.isEqual(v, bV) {
					return false
				}
			}
			return true
		}
	}

	// Fallback to string comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
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

// ============================================================================
// Advanced String Processing Functions
// ============================================================================

// Case Conversion Functions

func (i *Interpreter) builtinCamelCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Split by spaces, underscores, hyphens, dots
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '_' || r == '-' || r == '.'
	})

	if len(words) == 0 {
		return ""
	}

	var result strings.Builder
	for idx, word := range words {
		if idx == 0 {
			result.WriteString(strings.ToLower(word))
		} else {
			if len(word) > 0 {
				result.WriteString(strings.ToUpper(word[:1]))
				if len(word) > 1 {
					result.WriteString(strings.ToLower(word[1:]))
				}
			}
		}
	}
	return result.String()
}

func (i *Interpreter) builtinSnakeCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 {
				result.WriteRune('_')
			}
			result.WriteRune(unicode.ToLower(r))
		} else if r == ' ' || r == '-' || r == '.' {
			result.WriteRune('_')
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func (i *Interpreter) builtinKebabCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 {
				result.WriteRune('-')
			}
			result.WriteRune(unicode.ToLower(r))
		} else if r == ' ' || r == '_' || r == '.' {
			result.WriteRune('-')
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func (i *Interpreter) builtinPascalCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '_' || r == '-' || r == '.'
	})

	var result strings.Builder
	for _, word := range words {
		if len(word) > 0 {
			result.WriteString(strings.ToUpper(word[:1]))
			if len(word) > 1 {
				result.WriteString(strings.ToLower(word[1:]))
			}
		}
	}
	return result.String()
}

func (i *Interpreter) builtinSentenceCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return ""
	}

	s = strings.ToLower(s)
	runes := []rune(s)
	if len(runes) > 0 {
		runes[0] = unicode.ToUpper(runes[0])
	}
	return string(runes)
}

func (i *Interpreter) builtinConstantCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 {
				result.WriteRune('_')
			}
			result.WriteRune(r)
		} else if r == ' ' || r == '-' || r == '.' {
			result.WriteRune('_')
		} else {
			result.WriteRune(unicode.ToUpper(r))
		}
	}
	return result.String()
}

func (i *Interpreter) builtinDotCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 {
				result.WriteRune('.')
			}
			result.WriteRune(unicode.ToLower(r))
		} else if r == ' ' || r == '_' || r == '-' {
			result.WriteRune('.')
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func (i *Interpreter) builtinPathCase(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 {
				result.WriteRune('/')
			}
			result.WriteRune(unicode.ToLower(r))
		} else if r == ' ' || r == '_' || r == '-' || r == '.' {
			result.WriteRune('/')
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Character Operations

func (i *Interpreter) builtinCharAt(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	idx := i.toInt(args[1])
	runes := []rune(s)
	if idx < 0 || idx >= len(runes) {
		return ""
	}
	return string(runes[idx])
}

func (i *Interpreter) builtinCharCodeAt(args []Value) Value {
	if len(args) < 2 {
		return int64(-1)
	}
	s, ok := args[0].(string)
	if !ok {
		return int64(-1)
	}
	idx := i.toInt(args[1])
	runes := []rune(s)
	if idx < 0 || idx >= len(runes) {
		return int64(-1)
	}
	return int64(runes[idx])
}

func (i *Interpreter) builtinFromCharCode(args []Value) Value {
	var result strings.Builder
	for _, arg := range args {
		code := i.toInt(arg)
		if code >= 0 && code <= 0x10FFFF {
			result.WriteRune(rune(code))
		}
	}
	return result.String()
}

func (i *Interpreter) builtinIsLowerStr(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsUpperStr(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsSpaceStr(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsPrintable(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsASCII(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] > 127 {
			return false
		}
	}
	return true
}

// String Manipulation Functions

func (i *Interpreter) builtinInsertStr(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	idx := i.toInt(args[1])
	toInsert, ok := args[2].(string)
	if !ok {
		return s
	}

	runes := []rune(s)
	if idx < 0 {
		idx = 0
	}
	if idx > len(runes) {
		idx = len(runes)
	}

	return string(runes[:idx]) + toInsert + string(runes[idx:])
}

func (i *Interpreter) builtinDeleteStr(args []Value) Value {
	if len(args) < 2 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	start := i.toInt(args[1])

	runes := []rune(s)
	if start < 0 {
		start = 0
	}
	if start >= len(runes) {
		return s
	}

	length := len(runes) - start
	if len(args) > 2 {
		length = i.toInt(args[2])
	}
	if length < 0 {
		length = 0
	}
	end := start + length
	if end > len(runes) {
		end = len(runes)
	}

	return string(runes[:start]) + string(runes[end:])
}

func (i *Interpreter) builtinOverwrite(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	start := i.toInt(args[1])
	newStr, ok := args[2].(string)
	if !ok {
		return s
	}

	runes := []rune(s)
	newRunes := []rune(newStr)
	if start < 0 {
		start = 0
	}
	if start > len(runes) {
		return s + newStr
	}

	for idx, r := range newRunes {
		pos := start + idx
		if pos >= len(runes) {
			runes = append(runes, r)
		} else {
			runes[pos] = r
		}
	}
	return string(runes)
}

func (i *Interpreter) builtinSurround(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	wrapper := "\""
	if len(args) > 1 {
		wrapper, _ = args[1].(string)
	}
	return wrapper + s + wrapper
}

func (i *Interpreter) builtinQuote(args []Value) Value {
	if len(args) == 0 {
		return "\"\""
	}
	s, ok := args[0].(string)
	if !ok {
		return "\"\""
	}
	quoteChar := "\""
	if len(args) > 1 {
		quoteChar, _ = args[1].(string)
		if len(quoteChar) == 0 {
			quoteChar = "\""
		} else {
			quoteChar = string([]rune(quoteChar)[0])
		}
	}
	// Escape the quote character if present
	escaped := strings.ReplaceAll(s, quoteChar, "\\"+quoteChar)
	return quoteChar + escaped + quoteChar
}

func (i *Interpreter) builtinUnquote(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok || len(s) < 2 {
		return s
	}

	// Detect quote character
	quoteChar := s[0]
	if s[len(s)-1] != quoteChar {
		return s
	}

	// Valid quotes
	if quoteChar != '"' && quoteChar != '\'' && quoteChar != '`' {
		return s
	}

	inner := s[1 : len(s)-1]
	// Unescape
	result := strings.ReplaceAll(inner, string([]byte{'\\', quoteChar}), string([]byte{quoteChar}))
	return result
}

func (i *Interpreter) builtinStripTags(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Simple tag stripping using regex
	re := regexp.MustCompile(`<[^>]*>`)
	return re.ReplaceAllString(s, "")
}

func (i *Interpreter) builtinStripPunctuation(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, r := range s {
		if !unicode.IsPunct(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func (i *Interpreter) builtinNormalizeSpace(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Replace multiple whitespace with single space
	re := regexp.MustCompile(`\s+`)
	return strings.TrimSpace(re.ReplaceAllString(s, " "))
}

func (i *Interpreter) builtinNormalizeNewlines(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Normalize to \n
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

// Advanced Search/Replace Functions

func (i *Interpreter) builtinReplaceAll(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	old, ok1 := args[1].(string)
	new, ok2 := args[2].(string)
	if !ok1 || !ok2 {
		return s
	}
	return strings.ReplaceAll(s, old, new)
}

func (i *Interpreter) builtinReplaceFirst(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	old, ok1 := args[1].(string)
	new, ok2 := args[2].(string)
	if !ok1 || !ok2 {
		return s
	}
	return strings.Replace(s, old, new, 1)
}

func (i *Interpreter) builtinReplaceN(args []Value) Value {
	if len(args) < 4 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	old, ok1 := args[1].(string)
	new, ok2 := args[2].(string)
	n := i.toInt(args[3])
	if !ok1 || !ok2 {
		return s
	}
	return strings.Replace(s, old, new, int(n))
}

func (i *Interpreter) builtinReplaceIgnoreCase(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	old, ok1 := args[1].(string)
	new, ok2 := args[2].(string)
	if !ok1 || !ok2 {
		return s
	}

	if old == "" {
		return s
	}

	// Case-insensitive replace
	result := s
	lowerS := strings.ToLower(s)
	lowerOld := strings.ToLower(old)
	start := 0

	for {
		idx := strings.Index(lowerS[start:], lowerOld)
		if idx == -1 {
			break
		}
		idx += start
		result = result[:idx] + new + result[idx+len(old):]
		lowerS = strings.ToLower(result)
		start = idx + len(new)
	}
	return result
}

// String Analysis Functions

func (i *Interpreter) builtinLevenshtein(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	s1, ok1 := args[0].(string)
	s2, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return int64(0)
	}

	r1 := []rune(s1)
	r2 := []rune(s2)
	len1 := len(r1)
	len2 := len(r2)

	if len1 == 0 {
		return int64(len2)
	}
	if len2 == 0 {
		return int64(len1)
	}

	// Create matrix
	matrix := make([][]int, len1+1)
	for idx := range matrix {
		matrix[idx] = make([]int, len2+1)
		matrix[idx][0] = idx
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j] = j
	}

	for idx := 1; idx <= len1; idx++ {
		for j := 1; j <= len2; j++ {
			cost := 1
			if r1[idx-1] == r2[j-1] {
				cost = 0
			}
			matrix[idx][j] = min(matrix[idx-1][j]+1, matrix[idx][j-1]+1, matrix[idx-1][j-1]+cost)
		}
	}

	return int64(matrix[len1][len2])
}

func min(vals ...int) int {
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func (i *Interpreter) builtinCommonPrefix(args []Value) Value {
	if len(args) < 2 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}

	s1, ok1 := args[0].(string)
	s2, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return ""
	}

	r1 := []rune(s1)
	r2 := []rune(s2)

	var result strings.Builder
	for idx := 0; idx < len(r1) && idx < len(r2); idx++ {
		if r1[idx] == r2[idx] {
			result.WriteRune(r1[idx])
		} else {
			break
		}
	}
	return result.String()
}

func (i *Interpreter) builtinCommonSuffix(args []Value) Value {
	if len(args) < 2 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}

	s1, ok1 := args[0].(string)
	s2, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return ""
	}

	r1 := []rune(s1)
	r2 := []rune(s2)

	var result strings.Builder
	i1, i2 := len(r1)-1, len(r2)-1
	for i1 >= 0 && i2 >= 0 {
		if r1[i1] == r2[i2] {
			result.WriteRune(r1[i1])
			i1--
			i2--
		} else {
			break
		}
	}

	// Reverse result
	runes := []rune(result.String())
	for idx, j := 0, len(runes)-1; idx < j; idx, j = idx+1, j-1 {
		runes[idx], runes[j] = runes[j], runes[idx]
	}
	return string(runes)
}

func (i *Interpreter) builtinIsPalindrome(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	// Remove non-alphanumeric and convert to lower
	var cleaned strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cleaned.WriteRune(unicode.ToLower(r))
		}
	}

	runes := []rune(cleaned.String())
	for idx, j := 0, len(runes)-1; idx < j; idx, j = idx+1, j-1 {
		if runes[idx] != runes[j] {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsAnagram(args []Value) Value {
	if len(args) < 2 {
		return false
	}
	s1, ok1 := args[0].(string)
	s2, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return false
	}

	// Normalize and count characters
	countChars := func(s string) map[rune]int {
		counts := make(map[rune]int)
		for _, r := range s {
			if unicode.IsLetter(r) {
				counts[unicode.ToLower(r)]++
			}
		}
		return counts
	}

	c1 := countChars(s1)
	c2 := countChars(s2)

	if len(c1) != len(c2) {
		return false
	}
	for r, c := range c1 {
		if c2[r] != c {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinCharCount(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}
	s, ok := args[0].(string)
	if !ok {
		return int64(0)
	}
	return int64(len([]rune(s)))
}

func (i *Interpreter) builtinByteCount(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}
	s, ok := args[0].(string)
	if !ok {
		return int64(0)
	}
	return int64(len(s))
}

// String Splitting Functions

func (i *Interpreter) builtinSplitN(args []Value) Value {
	if len(args) < 3 {
		return i.builtinSplit(args)
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}
	sep, ok := args[1].(string)
	if !ok {
		return []Value{s}
	}
	n := i.toInt(args[2])

	parts := strings.SplitN(s, sep, int(n))
	result := make([]Value, len(parts))
	for idx, p := range parts {
		result[idx] = p
	}
	return result
}

func (i *Interpreter) builtinRsplit(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}
	sep, ok := args[1].(string)
	if !ok || sep == "" {
		return []Value{s}
	}
	n := -1
	if len(args) > 2 {
		n = int(i.toInt(args[2]))
	}

	// Split from right
	parts := strings.Split(s, sep)
	if n <= 0 || n >= len(parts) {
		// Reverse the parts
		result := make([]Value, len(parts))
		for idx, p := range parts {
			result[len(parts)-1-idx] = p
		}
		return result
	}

	// Keep first part intact
	result := make([]Value, n)
	result[0] = strings.Join(parts[:len(parts)-n+1], sep)
	for idx := 1; idx < n; idx++ {
		result[idx] = parts[len(parts)-n+idx]
	}
	return result
}

func (i *Interpreter) builtinPartitionStr(args []Value) Value {
	if len(args) < 2 {
		return []Value{"", "", ""}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{"", "", ""}
	}
	sep, ok := args[1].(string)
	if !ok {
		return []Value{s, "", ""}
	}

	idx := strings.Index(s, sep)
	if idx == -1 {
		return []Value{s, "", ""}
	}
	return []Value{s[:idx], sep, s[idx+len(sep):]}
}

func (i *Interpreter) builtinRpartition(args []Value) Value {
	if len(args) < 2 {
		return []Value{"", "", ""}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{"", "", ""}
	}
	sep, ok := args[1].(string)
	if !ok {
		return []Value{"", "", s}
	}

	idx := strings.LastIndex(s, sep)
	if idx == -1 {
		return []Value{"", "", s}
	}
	return []Value{s[:idx], sep, s[idx+len(sep):]}
}

// Unicode Support Functions

func (i *Interpreter) builtinSlugify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	lastWasDash := false

	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			result.WriteRune(unicode.ToLower(r))
			lastWasDash = false
		} else if r == ' ' || r == '-' || r == '_' {
			if !lastWasDash {
				result.WriteRune('-')
				lastWasDash = true
			}
		}
	}

	slug := result.String()
	slug = strings.Trim(slug, "-")
	return slug
}

func (i *Interpreter) builtinTruncateWords(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	numWords := 10
	if len(args) > 1 {
		numWords = int(i.toInt(args[1]))
	}
	suffix := "..."
	if len(args) > 2 {
		suffix, _ = args[2].(string)
	}

	words := strings.Fields(s)
	if len(words) <= numWords {
		return s
	}

	return strings.Join(words[:numWords], " ") + suffix
}

func (i *Interpreter) builtinWordWrap(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	width := 80
	if len(args) > 1 {
		width = int(i.toInt(args[1]))
	}
	if width <= 0 {
		return s
	}

	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}

	var result strings.Builder
	lineLen := 0

	for _, word := range words {
		wordLen := len(word)
		if lineLen > 0 && lineLen+1+wordLen > width {
			result.WriteString("\n")
			result.WriteString(word)
			lineLen = wordLen
		} else {
			if lineLen > 0 {
				result.WriteString(" ")
				lineLen++
			}
			result.WriteString(word)
			lineLen += wordLen
		}
	}
	return result.String()
}

func (i *Interpreter) builtinDedent(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	lines := strings.Split(s, "\n")
	if len(lines) == 0 {
		return ""
	}

	// Find minimum indentation
	minIndent := -1
	for _, line := range lines {
		trimmed := strings.TrimLeft(line, " \t")
		if trimmed == "" {
			continue
		}
		indent := len(line) - len(trimmed)
		if minIndent == -1 || indent < minIndent {
			minIndent = indent
		}
	}

	if minIndent <= 0 {
		return s
	}

	// Remove indentation
	for idx, line := range lines {
		if len(line) >= minIndent && strings.TrimLeft(line[:minIndent], " \t") == "" {
			lines[idx] = line[minIndent:]
		}
	}
	return strings.Join(lines, "\n")
}

// Validation Functions

func (i *Interpreter) builtinIsEmail(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	// Simple email validation
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(s)
}

func (i *Interpreter) builtinIsURL(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	urlRegex := regexp.MustCompile(`^(https?|ftp)://[^\s/$.?#].[^\s]*$`)
	return urlRegex.MatchString(s)
}

func (i *Interpreter) builtinIsUUID(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	uuidRegex := regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	return uuidRegex.MatchString(s)
}

func (i *Interpreter) builtinIsIP(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	// IPv4
	ipv4Regex := regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	if ipv4Regex.MatchString(s) {
		parts := strings.Split(s, ".")
		for _, p := range parts {
			n, err := strconv.Atoi(p)
			if err != nil || n < 0 || n > 255 {
				return false
			}
		}
		return true
	}

	// Simple IPv6 check
	ipv6Regex := regexp.MustCompile(`^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$`)
	if ipv6Regex.MatchString(s) {
		return true
	}

	return false
}

func (i *Interpreter) builtinIsCreditCard(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	// Remove spaces and dashes
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "-", "")

	// Check length and digits
	if len(s) < 13 || len(s) > 19 {
		return false
	}

	// Luhn algorithm
	sum := 0
	alt := false
	for idx := len(s) - 1; idx >= 0; idx-- {
		digit, err := strconv.Atoi(string(s[idx]))
		if err != nil {
			return false
		}

		if alt {
			digit *= 2
			if digit > 9 {
				digit -= 9
			}
		}
		sum += digit
		alt = !alt
	}

	return sum%10 == 0
}

func (i *Interpreter) builtinIsHexColor(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	hexRegex := regexp.MustCompile(`^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$`)
	return hexRegex.MatchString(s)
}

func (i *Interpreter) builtinIsJSONStr(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok {
		return false
	}

	var js interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

// String Utility Functions

func (i *Interpreter) builtinFormat(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	format, ok := args[0].(string)
	if !ok {
		return ""
	}

	if len(args) == 1 {
		return format
	}

	// Convert args to interface slice
	formatArgs := make([]interface{}, len(args)-1)
	for idx, arg := range args[1:] {
		formatArgs[idx] = arg
	}

	return fmt.Sprintf(format, formatArgs...)
}

func (i *Interpreter) builtinTemplate(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	tmpl, ok := args[0].(string)
	if !ok {
		return ""
	}

	if len(args) < 2 {
		return tmpl
	}

	// Template with map or object
	data, ok := args[1].(map[string]Value)
	if !ok {
		return tmpl
	}

	// Simple template replacement: {{key}}
	re := regexp.MustCompile(`\{\{(\w+)\}\}`)
	result := re.ReplaceAllStringFunc(tmpl, func(match string) string {
		key := match[2 : len(match)-2]
		if val, exists := data[key]; exists {
			return fmt.Sprintf("%v", val)
		}
		return match
	})

	return result
}

func (i *Interpreter) builtinRepeatUntil(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	targetLen := i.toInt(args[1])
	if targetLen <= 0 {
		return ""
	}

	if len(s) == 0 {
		return ""
	}

	for len(s) < int(targetLen) {
		s += s
	}
	return s[:targetLen]
}

func (i *Interpreter) builtinPadBetween(args []Value) Value {
	if len(args) < 3 {
		if len(args) == 0 {
			return ""
		}
		return args[0]
	}
	left, ok := args[0].(string)
	if !ok {
		return ""
	}
	right, ok := args[1].(string)
	if !ok {
		return left
	}
	pad, ok := args[2].(string)
	if !ok || pad == "" {
		pad = " "
	}

	return left + pad + right
}

func (i *Interpreter) builtinUnwrap(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok || len(s) < 2 {
		return s
	}

	wrapper := "\"\""
	if len(args) > 1 {
		wrapper, _ = args[1].(string)
	}
	if len(wrapper) < 2 {
		return s
	}

	runes := []rune(wrapper)
	left := runes[0]
	right := runes[len(runes)-1]

	sRunes := []rune(s)
	if sRunes[0] == left && sRunes[len(sRunes)-1] == right {
		return string(sRunes[1 : len(sRunes)-1])
	}
	return s
}

func (i *Interpreter) builtinToSize(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var bytes int64
	switch v := args[0].(type) {
	case int64:
		bytes = v
	case int:
		bytes = int64(v)
	case float64:
		bytes = int64(v)
	default:
		return ""
	}

	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func (i *Interpreter) builtinFromSize(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}
	s, ok := args[0].(string)
	if !ok {
		return int64(0)
	}

	s = strings.TrimSpace(s)
	re := regexp.MustCompile(`(?i)^([\d.]+)\s*(B|KB|MB|GB|TB)?$`)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return int64(0)
	}

	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return int64(0)
	}

	unit := strings.ToUpper(matches[2])
	switch unit {
	case "TB":
		value *= 1024 * 1024 * 1024 * 1024
	case "GB":
		value *= 1024 * 1024 * 1024
	case "MB":
		value *= 1024 * 1024
	case "KB":
		value *= 1024
	}

	return int64(value)
}

// ============================================================================
// Regular Expression Functions
// ============================================================================

// builtinRegexMatch checks if a string matches a regex pattern
func (i *Interpreter) builtinRegexMatch(args []Value) Value {
	if len(args) < 2 {
		return false
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return false
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(str)
}

// builtinRegexFind finds the first match of a regex pattern in a string
func (i *Interpreter) builtinRegexFind(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return ""
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return ""
	}
	return re.FindString(str)
}

// builtinRegexFindAll finds all matches of a regex pattern in a string
func (i *Interpreter) builtinRegexFindAll(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return []Value{}
	}

	n := -1
	if len(args) > 2 {
		n = int(i.toInt(args[2]))
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return []Value{}
	}

	matches := re.FindAllString(str, n)
	result := make([]Value, len(matches))
	for i, m := range matches {
		result[i] = m
	}
	return result
}

// builtinRegexReplace replaces all matches of a regex pattern with a replacement string
func (i *Interpreter) builtinRegexReplace(args []Value) Value {
	if len(args) < 3 {
		return ""
	}
	pattern, ok1 := args[0].(string)
	replacement, ok2 := args[1].(string)
	str, ok3 := args[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return str
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return str
	}
	return re.ReplaceAllString(str, replacement)
}

// builtinRegexSplit splits a string by a regex pattern
func (i *Interpreter) builtinRegexSplit(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return []Value{}
	}

	n := -1
	if len(args) > 2 {
		n = int(i.toInt(args[2]))
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return []Value{str}
	}

	parts := re.Split(str, n)
	result := make([]Value, len(parts))
	for i, p := range parts {
		result[i] = p
	}
	return result
}

// builtinRegexCompile compiles a regex pattern and returns a map with pattern info
func (i *Interpreter) builtinRegexCompile(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "no pattern provided"}
	}

	pattern, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "pattern must be a string"}
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":   true,
		"pattern":   pattern,
		"numSubexp": int64(re.NumSubexp()),
	}
}

// builtinRegexQuote escapes all regular expression metacharacters in a string
func (i *Interpreter) builtinRegexQuote(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return regexp.QuoteMeta(s)
}

// builtinRegexCount counts the number of matches of a regex pattern in a string
func (i *Interpreter) builtinRegexCount(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return int64(0)
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return int64(0)
	}

	matches := re.FindAllString(str, -1)
	return int64(len(matches))
}

// builtinRegexGroups extracts named groups from a regex match
func (i *Interpreter) builtinRegexGroups(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{}
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return map[string]Value{}
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	match := re.FindStringSubmatch(str)
	if match == nil {
		return map[string]Value{}
	}

	result := make(map[string]Value)
	subexpNames := re.SubexpNames()

	for i, name := range subexpNames {
		if i > 0 && name != "" { // Skip the whole match (index 0)
			if i < len(match) {
				result[name] = match[i]
			}
		}
	}

	// Also include numbered groups
	for i, m := range match {
		result[fmt.Sprintf("%d", i)] = m
	}

	return result
}

// builtinRegexFindSubmatch finds the first match including submatches
func (i *Interpreter) builtinRegexFindSubmatch(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return []Value{}
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return []Value{}
	}

	match := re.FindStringSubmatch(str)
	if match == nil {
		return []Value{}
	}

	result := make([]Value, len(match))
	for i, m := range match {
		result[i] = m
	}
	return result
}

// builtinRegexFindAllSubmatch finds all matches including submatches
func (i *Interpreter) builtinRegexFindAllSubmatch(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}
	pattern, ok1 := args[0].(string)
	str, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return []Value{}
	}

	n := -1
	if len(args) > 2 {
		n = int(i.toInt(args[2]))
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return []Value{}
	}

	matches := re.FindAllStringSubmatch(str, n)
	result := make([]Value, len(matches))
	for i, match := range matches {
		submatch := make([]Value, len(match))
		for j, m := range match {
			submatch[j] = m
		}
		result[i] = submatch
	}
	return result
}

// builtinRegexReplaceFunc replaces matches using a transformation function
func (i *Interpreter) builtinRegexReplaceFunc(args []Value) Value {
	if len(args) < 3 {
		return ""
	}
	pattern, ok1 := args[0].(string)
	transformType, ok2 := args[1].(string)
	str, ok3 := args[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return str
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return str
	}

	result := re.ReplaceAllStringFunc(str, func(s string) string {
		switch transformType {
		case "upper", "uppercase":
			return strings.ToUpper(s)
		case "lower", "lowercase":
			return strings.ToLower(s)
		case "title":
			return strings.Title(s)
		case "reverse":
			runes := []rune(s)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		case "trim":
			return strings.TrimSpace(s)
		case "double":
			return s + s
		case "quote":
			return fmt.Sprintf("%q", s)
		default:
			return s
		}
	})

	return result
}

// builtinRegexValid checks if a regex pattern is valid
func (i *Interpreter) builtinRegexValid(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	pattern, ok := args[0].(string)
	if !ok {
		return false
	}

	_, err := regexp.Compile(pattern)
	return err == nil
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
	return mathrand.Float64()
}

func (i *Interpreter) builtinRandomInt(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}
	min := int64(i.toInt(args[0]))
	max := int64(i.toInt(args[1]))
	return min + mathrand.Int63n(max-min+1)
}

func (i *Interpreter) builtinRandomFloat(args []Value) Value {
	if len(args) < 2 {
		return mathrand.Float64()
	}
	min := i.toFloat(args[0])
	max := i.toFloat(args[1])
	return min + mathrand.Float64()*(max-min)
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
		k := mathrand.Intn(j + 1)
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
		k := mathrand.Intn(j + 1)
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

// ============================================================================
// Advanced Sorting Functions
// ============================================================================

// sortBy sorts an array of objects by a specified key
func (i *Interpreter) builtinSortBy(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	key, ok := args[1].(string)
	if !ok {
		return []Value{}
	}

	// Copy array to avoid modifying original
	result := make([]Value, len(arr))
	copy(result, arr)

	// Sort by key
	sort.Slice(result, func(a, b int) bool {
		valA := getMapValue(result[a], key)
		valB := getMapValue(result[b], key)
		return compareValues(valA, valB) < 0
	})

	return result
}

// sortByDesc sorts an array of objects by a specified key in descending order
func (i *Interpreter) builtinSortByDesc(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	key, ok := args[1].(string)
	if !ok {
		return []Value{}
	}

	result := make([]Value, len(arr))
	copy(result, arr)

	sort.Slice(result, func(a, b int) bool {
		valA := getMapValue(result[a], key)
		valB := getMapValue(result[b], key)
		return compareValues(valA, valB) > 0
	})

	return result
}

// sortStrings sorts an array of strings alphabetically
func (i *Interpreter) builtinSortStrings(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	// Extract strings
	strs := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			strs = append(strs, s)
		} else {
			strs = append(strs, fmt.Sprintf("%v", v))
		}
	}

	sort.Strings(strs)

	result := make([]Value, len(strs))
	for j, s := range strs {
		result[j] = s
	}
	return result
}

// sortStringsDesc sorts an array of strings in descending alphabetical order
func (i *Interpreter) builtinSortStringsDesc(args []Value) Value {
	result := i.builtinSortStrings(args)
	arr := result.([]Value)
	for j, k := 0, len(arr)-1; j < k; j, k = j+1, k-1 {
		arr[j], arr[k] = arr[k], arr[j]
	}
	return arr
}

// sortNatural sorts strings naturally (file1, file2, file10 instead of file1, file10, file2)
func (i *Interpreter) builtinSortNatural(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	result := make([]Value, len(arr))
	copy(result, arr)

	sort.Slice(result, func(a, b int) bool {
		strA := fmt.Sprintf("%v", result[a])
		strB := fmt.Sprintf("%v", result[b])
		return naturalLess(strA, strB)
	})

	return result
}

// sortNaturalDesc sorts strings naturally in descending order
func (i *Interpreter) builtinSortNaturalDesc(args []Value) Value {
	result := i.builtinSortNatural(args)
	arr := result.([]Value)
	for j, k := 0, len(arr)-1; j < k; j, k = j+1, k-1 {
		arr[j], arr[k] = arr[k], arr[j]
	}
	return arr
}

// sortMulti sorts by multiple keys
func (i *Interpreter) builtinSortMulti(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	keys, ok := args[1].([]Value)
	if !ok {
		return arr
	}

	result := make([]Value, len(arr))
	copy(result, arr)

	sort.Slice(result, func(a, b int) bool {
		for _, k := range keys {
			key, ok := k.(string)
			if !ok {
				continue
			}
			valA := getMapValue(result[a], key)
			valB := getMapValue(result[b], key)
			cmp := compareValues(valA, valB)
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})

	return result
}

// rank returns the ranking of elements (1-based, with ties)
func (i *Interpreter) builtinRank(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	// Create indexed pairs for sorting
	type indexed struct {
		val   Value
		index int
	}
	indexedArr := make([]indexed, len(arr))
	for j, v := range arr {
		indexedArr[j] = indexed{val: v, index: j}
	}

	// Sort by value
	sort.Slice(indexedArr, func(a, b int) bool {
		return compareValues(indexedArr[a].val, indexedArr[b].val) < 0
	})

	// Assign ranks
	ranks := make([]Value, len(arr))
	for j := 0; j < len(indexedArr); j++ {
		rank := j + 1
		// Check for ties
		if j > 0 && compareValues(indexedArr[j].val, indexedArr[j-1].val) == 0 {
			// Same rank as previous
			ranks[indexedArr[j].index] = ranks[indexedArr[j-1].index]
		} else {
			ranks[indexedArr[j].index] = int64(rank)
		}
	}

	return ranks
}

// rankBy returns the ranking of objects by a key
func (i *Interpreter) builtinRankBy(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	key, ok := args[1].(string)
	if !ok {
		return []Value{}
	}

	// Create indexed pairs
	type indexed struct {
		val   Value
		index int
	}
	indexedArr := make([]indexed, len(arr))
	for j, v := range arr {
		indexedArr[j] = indexed{val: getMapValue(v, key), index: j}
	}

	// Sort by value
	sort.Slice(indexedArr, func(a, b int) bool {
		return compareValues(indexedArr[a].val, indexedArr[b].val) < 0
	})

	// Assign ranks
	ranks := make([]Value, len(arr))
	for j := 0; j < len(indexedArr); j++ {
		rank := j + 1
		if j > 0 && compareValues(indexedArr[j].val, indexedArr[j-1].val) == 0 {
			ranks[indexedArr[j].index] = ranks[indexedArr[j-1].index]
		} else {
			ranks[indexedArr[j].index] = int64(rank)
		}
	}

	return ranks
}

// denseRank returns dense ranking (no gaps for ties)
func (i *Interpreter) builtinDenseRank(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	// Sort first
	sorted := i.builtinSort(args).([]Value)

	// Build value-to-rank map
	rankMap := make(map[string]int64)
	denseRank := int64(0)
	for _, v := range sorted {
		key := fmt.Sprintf("%v", v)
		if _, exists := rankMap[key]; !exists {
			denseRank++
			rankMap[key] = denseRank
		}
	}

	// Assign ranks
	ranks := make([]Value, len(arr))
	for j, v := range arr {
		key := fmt.Sprintf("%v", v)
		ranks[j] = rankMap[key]
	}

	return ranks
}

// topN returns the top N elements
func (i *Interpreter) builtinTopN(args []Value) Value {
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

	// Sort and take top N
	sorted := i.builtinSortDesc([]Value{arr}).([]Value)
	return sorted[:n]
}

// bottomN returns the bottom N elements
func (i *Interpreter) builtinBottomN(args []Value) Value {
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

	sorted := i.builtinSort([]Value{arr}).([]Value)
	return sorted[:n]
}

// partition splits array into two groups based on predicate
func (i *Interpreter) builtinPartition(args []Value) Value {
	if len(args) < 2 {
		return []Value{[]Value{}, []Value{}}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{[]Value{}, []Value{}}
	}

	predicate, ok := args[1].(string)
	if !ok {
		return []Value{[]Value{}, []Value{}}
	}

	trueGroup := []Value{}
	falseGroup := []Value{}

	for _, v := range arr {
		if evaluatePredicate(v, predicate) {
			trueGroup = append(trueGroup, v)
		} else {
			falseGroup = append(falseGroup, v)
		}
	}

	return []Value{trueGroup, falseGroup}
}

// groupBySorted groups consecutive elements by key after sorting
func (i *Interpreter) builtinGroupBySorted(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return map[string]Value{}
	}

	key, ok := args[1].(string)
	if !ok {
		return map[string]Value{}
	}

	// Sort by key first
	sorted := i.builtinSortBy([]Value{arr, key}).([]Value)

	result := map[string]Value{}
	for _, v := range sorted {
		keyVal := fmt.Sprintf("%v", getMapValue(v, key))
		if group, ok := result[keyVal]; ok {
			result[keyVal] = append(group.([]Value), v)
		} else {
			result[keyVal] = []Value{v}
		}
	}

	return result
}

// Helper functions for sorting

func getMapValue(v Value, key string) Value {
	if m, ok := v.(map[string]Value); ok {
		return m[key]
	}
	return nil
}

func compareValues(a, b Value) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison first
	aFloat, aIsNum := toFloatValue(a)
	bFloat, bIsNum := toFloatValue(b)
	if aIsNum && bIsNum {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func toFloatValue(v Value) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

func toFloat(v Value) float64 {
	val, _ := toFloatValue(v)
	return val
}

func naturalLess(a, b string) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		ca, cb := a[i], b[j]

		// Both digits - compare numbers
		if ca >= '0' && ca <= '9' && cb >= '0' && cb <= '9' {
			// Extract numbers
			na, ia := extractNumber(a, i)
			nb, ib := extractNumber(b, j)
			if na != nb {
				return na < nb
			}
			i, j = ia, ib
			continue
		}

		// Compare characters
		if ca != cb {
			return ca < cb
		}
		i++
		j++
	}

	// Shorter string comes first
	return len(a) < len(b)
}

func extractNumber(s string, start int) (int, int) {
	num := 0
	i := start
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		num = num*10 + int(s[i]-'0')
		i++
	}
	return num, i
}

func evaluatePredicate(v Value, predicate string) bool {
	switch predicate {
	case "positive":
		if f, ok := toFloatValue(v); ok {
			return f > 0
		}
	case "negative":
		if f, ok := toFloatValue(v); ok {
			return f < 0
		}
	case "zero":
		if f, ok := toFloatValue(v); ok {
			return f == 0
		}
	case "even":
		if n, ok := v.(int); ok {
			return n%2 == 0
		}
		if n, ok := v.(int64); ok {
			return n%2 == 0
		}
	case "odd":
		if n, ok := v.(int); ok {
			return n%2 != 0
		}
		if n, ok := v.(int64); ok {
			return n%2 != 0
		}
	case "null", "nil":
		return v == nil
	case "empty":
		if s, ok := v.(string); ok {
			return s == ""
		}
		if arr, ok := v.([]Value); ok {
			return len(arr) == 0
		}
	}
	return false
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
// Advanced Array Functions
// ============================================================================

// builtinRotate rotates array by n positions (positive = right, negative = left)
func (i *Interpreter) builtinRotate(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return arr
	}

	n := int(i.toInt(args[1]))
	n = n % len(arr)
	if n < 0 {
		n += len(arr)
	}

	// Rotate right by n
	result := make([]Value, len(arr))
	for i, v := range arr {
		newIdx := (i + n) % len(arr)
		result[newIdx] = v
	}
	return result
}

// builtinSlide slides a window over array
func (i *Interpreter) builtinSlide(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	windowSize := int(i.toInt(args[1]))
	if windowSize <= 0 || windowSize > len(arr) {
		return []Value{}
	}

	result := []Value{}
	for i := 0; i <= len(arr)-windowSize; i++ {
		result = append(result, arr[i:i+windowSize])
	}
	return result
}

// builtinWindow returns sliding windows (alias for slide)
func (i *Interpreter) builtinWindow(args []Value) Value {
	return i.builtinSlide(args)
}

// builtinPairwise returns pairs of adjacent elements
func (i *Interpreter) builtinPairwise(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok || len(arr) < 2 {
		return []Value{}
	}

	result := []Value{}
	for i := 0; i < len(arr)-1; i++ {
		result = append(result, []Value{arr[i], arr[i+1]})
	}
	return result
}

// builtinTranspose transposes 2D array (rows to columns)
func (i *Interpreter) builtinTranspose(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return []Value{}
	}

	// Find max row length
	maxLen := 0
	for _, row := range arr {
		if r, ok := row.([]Value); ok && len(r) > maxLen {
			maxLen = len(r)
		}
	}

	if maxLen == 0 {
		return []Value{}
	}

	// Transpose
	result := make([]Value, maxLen)
	for i := 0; i < maxLen; i++ {
		col := []Value{}
		for _, row := range arr {
			if r, ok := row.([]Value); ok && i < len(r) {
				col = append(col, r[i])
			}
		}
		result[i] = col
	}
	return result
}

// builtinFill fills array with value
func (i *Interpreter) builtinFill(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	n := int(i.toInt(args[0]))
	if n <= 0 {
		return []Value{}
	}

	value := args[1]
	result := make([]Value, n)
	for i := range result {
		result[i] = value
	}
	return result
}

// builtinFillRange fills array with values in range
func (i *Interpreter) builtinFillRange(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	start := int(i.toInt(args[0]))
	end := int(i.toInt(args[1]))
	step := 1
	if len(args) > 2 {
		step = int(i.toInt(args[2]))
	}

	if step == 0 || (step > 0 && start > end) || (step < 0 && start < end) {
		return []Value{}
	}

	result := []Value{}
	if step > 0 {
		for i := start; i < end; i += step {
			result = append(result, int64(i))
		}
	} else {
		for i := start; i > end; i += step {
			result = append(result, int64(i))
		}
	}
	return result
}

// builtinInsertAt inserts value at index
func (i *Interpreter) builtinInsertAt(args []Value) Value {
	if len(args) < 3 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	idx := int(i.toInt(args[1]))
	if idx < 0 {
		idx = 0
	}
	if idx > len(arr) {
		idx = len(arr)
	}

	value := args[2]
	result := make([]Value, 0, len(arr)+1)
	result = append(result, arr[:idx]...)
	result = append(result, value)
	result = append(result, arr[idx:]...)
	return result
}

// builtinRemoveAt removes element at index
func (i *Interpreter) builtinRemoveAt(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return arr
	}

	idx := int(i.toInt(args[1]))
	if idx < 0 || idx >= len(arr) {
		return arr
	}

	result := make([]Value, 0, len(arr)-1)
	result = append(result, arr[:idx]...)
	result = append(result, arr[idx+1:]...)
	return result
}

// builtinRemoveFirst removes first occurrence of value
func (i *Interpreter) builtinRemoveFirst(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return arr
	}

	target := args[1]
	for idx, v := range arr {
		if i.isEqual(v, target) {
			result := make([]Value, 0, len(arr)-1)
			result = append(result, arr[:idx]...)
			result = append(result, arr[idx+1:]...)
			return result
		}
	}
	return arr
}

// builtinRemoveLast removes last occurrence of value
func (i *Interpreter) builtinRemoveLast(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return arr
	}

	target := args[1]
	for idx := len(arr) - 1; idx >= 0; idx-- {
		if i.isEqual(arr[idx], target) {
			result := make([]Value, 0, len(arr)-1)
			result = append(result, arr[:idx]...)
			result = append(result, arr[idx+1:]...)
			return result
		}
	}
	return arr
}

// builtinRemoveAll removes all occurrences of value
func (i *Interpreter) builtinRemoveAll(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return arr
	}

	target := args[1]
	result := []Value{}
	for _, v := range arr {
		if !i.isEqual(v, target) {
			result = append(result, v)
		}
	}
	return result
}

// builtinReplaceAt replaces element at index
func (i *Interpreter) builtinReplaceAt(args []Value) Value {
	if len(args) < 3 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return arr
	}

	idx := int(i.toInt(args[1]))
	if idx < 0 || idx >= len(arr) {
		return arr
	}

	result := make([]Value, len(arr))
	copy(result, arr)
	result[idx] = args[2]
	return result
}

// builtinSwap swaps two elements
func (i *Interpreter) builtinSwap(args []Value) Value {
	if len(args) < 3 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) < 2 {
		return arr
	}

	i1 := int(i.toInt(args[1]))
	i2 := int(i.toInt(args[2]))

	if i1 < 0 || i1 >= len(arr) || i2 < 0 || i2 >= len(arr) {
		return arr
	}

	result := make([]Value, len(arr))
	copy(result, arr)
	result[i1], result[i2] = result[i2], result[i1]
	return result
}

// builtinMove moves element from one index to another
func (i *Interpreter) builtinMove(args []Value) Value {
	if len(args) < 3 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) < 2 {
		return arr
	}

	from := int(i.toInt(args[1]))
	to := int(i.toInt(args[2]))

	if from < 0 || from >= len(arr) || to < 0 || to >= len(arr) {
		return arr
	}

	result := make([]Value, len(arr))
	copy(result, arr)

	value := result[from]
	// Remove from source
	result = append(result[:from], result[from+1:]...)
	// Insert at destination
	result = append(result[:to], append([]Value{value}, result[to:]...)...)

	return result[:len(arr)]
}

// builtinCompact removes nil/null values
func (i *Interpreter) builtinCompact(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	result := []Value{}
	for _, v := range arr {
		if v != nil {
			result = append(result, v)
		}
	}
	return result
}

// builtinCompactFlat removes nil values and flattens one level
func (i *Interpreter) builtinCompactFlat(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	result := []Value{}
	for _, v := range arr {
		if v == nil {
			continue
		}
		if sub, ok := v.([]Value); ok {
			for _, sv := range sub {
				if sv != nil {
					result = append(result, sv)
				}
			}
		} else {
			result = append(result, v)
		}
	}
	return result
}

// builtinUniqBy returns unique elements by key function
func (i *Interpreter) builtinUniqBy(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	keyFn, ok := args[1].(string)
	if !ok {
		return i.builtinUnique(args)
	}

	seen := make(map[string]bool)
	result := []Value{}

	for _, v := range arr {
		key := i.getKeyByFunction(v, keyFn)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	return result
}

// builtinDifferenceBy difference with key function
func (i *Interpreter) builtinDifferenceBy(args []Value) Value {
	if len(args) < 3 {
		return i.builtinDifference(args)
	}

	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	keyFn, ok3 := args[2].(string)

	if !ok1 || !ok3 {
		return []Value{}
	}
	if !ok2 {
		return arr1
	}

	set := make(map[string]bool)
	for _, v := range arr2 {
		set[i.getKeyByFunction(v, keyFn)] = true
	}

	result := []Value{}
	for _, v := range arr1 {
		if !set[i.getKeyByFunction(v, keyFn)] {
			result = append(result, v)
		}
	}
	return result
}

// builtinIntersectionBy intersection with key function
func (i *Interpreter) builtinIntersectionBy(args []Value) Value {
	if len(args) < 3 {
		return i.builtinIntersection(args)
	}

	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	keyFn, ok3 := args[2].(string)

	if !ok1 || !ok2 || !ok3 {
		return []Value{}
	}

	set := make(map[string]bool)
	for _, v := range arr2 {
		set[i.getKeyByFunction(v, keyFn)] = true
	}

	result := []Value{}
	for _, v := range arr1 {
		if set[i.getKeyByFunction(v, keyFn)] {
			result = append(result, v)
		}
	}
	return result
}

// builtinUnionBy union with key function
func (i *Interpreter) builtinUnionBy(args []Value) Value {
	if len(args) < 3 {
		return i.builtinUnion(args)
	}

	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)
	keyFn, ok3 := args[2].(string)

	if !ok1 {
		arr1 = []Value{}
	}
	if !ok2 {
		arr2 = []Value{}
	}
	if !ok3 {
		return i.builtinUnion([]Value{arr1, arr2})
	}

	seen := make(map[string]bool)
	result := []Value{}

	for _, arr := range [][]Value{arr1, arr2} {
		for _, v := range arr {
			key := i.getKeyByFunction(v, keyFn)
			if !seen[key] {
				seen[key] = true
				result = append(result, v)
			}
		}
	}
	return result
}

// builtinFindIndex finds index of first matching element
func (i *Interpreter) builtinFindIndex(args []Value) Value {
	if len(args) < 2 {
		return int64(-1)
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return int64(-1)
	}

	target := args[1]
	for idx, v := range arr {
		if i.isEqual(v, target) {
			return int64(idx)
		}
	}
	return int64(-1)
}

// builtinFindLastIndex finds index of last matching element
func (i *Interpreter) builtinFindLastIndex(args []Value) Value {
	if len(args) < 2 {
		return int64(-1)
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return int64(-1)
	}

	target := args[1]
	for idx := len(arr) - 1; idx >= 0; idx-- {
		if i.isEqual(arr[idx], target) {
			return int64(idx)
		}
	}
	return int64(-1)
}

// builtinIndicesOf finds all indices of value
func (i *Interpreter) builtinIndicesOf(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	target := args[1]
	result := []Value{}
	for idx, v := range arr {
		if i.isEqual(v, target) {
			result = append(result, int64(idx))
		}
	}
	return result
}

// builtinIndexOfAll alias for indicesOf
func (i *Interpreter) builtinIndexOfAll(args []Value) Value {
	return i.builtinIndicesOf(args)
}

// builtinTakeWhile takes elements while predicate is true
func (i *Interpreter) builtinTakeWhile(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	predicate, ok := args[1].(string)
	if !ok {
		return arr
	}

	result := []Value{}
	for _, v := range arr {
		if !i.matchesPredicate(v, predicate) {
			break
		}
		result = append(result, v)
	}
	return result
}

// builtinDropWhile drops elements while predicate is true
func (i *Interpreter) builtinDropWhile(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	predicate, ok := args[1].(string)
	if !ok {
		return arr
	}

	result := []Value{}
	dropping := true
	for _, v := range arr {
		if dropping && i.matchesPredicate(v, predicate) {
			continue
		}
		dropping = false
		result = append(result, v)
	}
	return result
}

// builtinSpan splits array at first point predicate fails
func (i *Interpreter) builtinSpan(args []Value) Value {
	if len(args) < 2 {
		return []Value{args[0], []Value{}}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{[]Value{}, []Value{}}
	}

	predicate, ok := args[1].(string)
	if !ok {
		return []Value{arr, []Value{}}
	}

	splitIdx := len(arr)
	for idx, v := range arr {
		if !i.matchesPredicate(v, predicate) {
			splitIdx = idx
			break
		}
	}

	return []Value{arr[:splitIdx], arr[splitIdx:]}
}

// builtinBreakList splits array at first point predicate succeeds
func (i *Interpreter) builtinBreakList(args []Value) Value {
	if len(args) < 2 {
		return []Value{args[0], []Value{}}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{[]Value{}, []Value{}}
	}

	predicate, ok := args[1].(string)
	if !ok {
		return []Value{arr, []Value{}}
	}

	splitIdx := len(arr)
	for idx, v := range arr {
		if i.matchesPredicate(v, predicate) {
			splitIdx = idx
			break
		}
	}

	return []Value{arr[:splitIdx], arr[splitIdx:]}
}

// builtinSplitAt splits array at index
func (i *Interpreter) builtinSplitAt(args []Value) Value {
	if len(args) < 2 {
		return []Value{args[0], []Value{}}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{[]Value{}, []Value{}}
	}

	idx := int(i.toInt(args[1]))
	if idx < 0 {
		idx = 0
	}
	if idx > len(arr) {
		idx = len(arr)
	}

	return []Value{arr[:idx], arr[idx:]}
}

// builtinSplitWhen splits array when predicate is true
func (i *Interpreter) builtinSplitWhen(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	predicate, ok := args[1].(string)
	if !ok {
		return []Value{arr}
	}

	result := [][]Value{}
	current := []Value{}

	for _, v := range arr {
		if i.matchesPredicate(v, predicate) {
			if len(current) > 0 {
				result = append(result, current)
				current = []Value{}
			}
		} else {
			current = append(current, v)
		}
	}
	if len(current) > 0 {
		result = append(result, current)
	}

	// Convert to []Value
	ret := make([]Value, len(result))
	for i, r := range result {
		ret[i] = r
	}
	return ret
}

// builtinAperture returns consecutive n-tuples
func (i *Interpreter) builtinAperture(args []Value) Value {
	return i.builtinSlide(args)
}

// builtinXprod cartesian product of two arrays
func (i *Interpreter) builtinXprod(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	arr1, ok1 := args[0].([]Value)
	arr2, ok2 := args[1].([]Value)

	if !ok1 || !ok2 {
		return []Value{}
	}

	result := []Value{}
	for _, a := range arr1 {
		for _, b := range arr2 {
			result = append(result, []Value{a, b})
		}
	}
	return result
}

// builtinFromPairs converts pairs to object
func (i *Interpreter) builtinFromPairs(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok {
		return map[string]Value{}
	}

	result := make(map[string]Value)
	for _, pair := range arr {
		if p, ok := pair.([]Value); ok && len(p) >= 2 {
			if key, ok := p[0].(string); ok {
				result[key] = p[1]
			}
		}
	}
	return result
}

// builtinToPairs converts object to pairs
func (i *Interpreter) builtinToPairs(args []Value) Value {
	obj, ok := args[0].(map[string]Value)
	if !ok {
		return []Value{}
	}

	result := []Value{}
	for k, v := range obj {
		result = append(result, []Value{k, v})
	}
	return result
}

// builtinRangeStep creates range with step
func (i *Interpreter) builtinRangeStep(args []Value) Value {
	if len(args) < 3 {
		return i.builtinRange(args)
	}

	start := int(i.toInt(args[0]))
	end := int(i.toInt(args[1]))
	step := int(i.toInt(args[2]))

	if step == 0 {
		return []Value{}
	}

	result := []Value{}
	if step > 0 {
		for i := start; i < end; i += step {
			result = append(result, int64(i))
		}
	} else {
		for i := start; i > end; i += step {
			result = append(result, int64(i))
		}
	}
	return result
}

// builtinRepeatAll creates array by repeating value
func (i *Interpreter) builtinRepeatAll(args []Value) Value {
	if len(args) < 2 {
		return []Value{}
	}

	value := args[0]
	n := int(i.toInt(args[1]))

	result := make([]Value, n)
	for i := range result {
		result[i] = value
	}
	return result
}

// builtinCycle cycles through array n times
func (i *Interpreter) builtinCycle(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return []Value{}
	}

	n := int(i.toInt(args[1]))
	result := []Value{}

	for i := 0; i < n; i++ {
		result = append(result, arr...)
	}
	return result
}

// builtinIterate creates array by iterating function
func (i *Interpreter) builtinIterate(args []Value) Value {
	if len(args) < 3 {
		return []Value{}
	}

	initial := args[0]
	n := int(i.toInt(args[1]))
	operation, ok := args[2].(string)

	if !ok || n <= 0 {
		return []Value{initial}
	}

	result := []Value{initial}
	current := initial

	for idx := 1; idx < n; idx++ {
		current = i.applyOperation(current, operation)
		result = append(result, current)
	}
	return result
}

// builtinPrependAll prepends all elements from arrays
func (i *Interpreter) builtinPrependAll(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok {
		arr = []Value{}
	}

	result := []Value{}
	for i := len(args) - 1; i >= 1; i-- {
		if toPrepend, ok := args[i].([]Value); ok {
			result = append(result, toPrepend...)
		}
	}
	result = append(result, arr...)
	return result
}

// builtinAppendAll appends all elements from arrays
func (i *Interpreter) builtinAppendAll(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	result, ok := args[0].([]Value)
	if !ok {
		result = []Value{}
	}

	for i := 1; i < len(args); i++ {
		if toAppend, ok := args[i].([]Value); ok {
			result = append(result, toAppend...)
		}
	}
	return result
}

// builtinIntersperse inserts value between elements
func (i *Interpreter) builtinIntersperse(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) <= 1 {
		return arr
	}

	separator := args[1]
	result := []Value{}

	for i, v := range arr {
		if i > 0 {
			result = append(result, separator)
		}
		result = append(result, v)
	}
	return result
}

// builtinIntercalate inserts array between elements and flattens
func (i *Interpreter) builtinIntercalate(args []Value) Value {
	if len(args) < 2 {
		return args[0]
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return []Value{}
	}

	separator, ok := args[1].([]Value)
	if !ok {
		return i.builtinIntersperse(args)
	}

	result := []Value{}
	for i, v := range arr {
		if i > 0 {
			result = append(result, separator...)
		}
		result = append(result, v)
	}
	return result
}

// builtinSubsequences returns all subsequences
func (i *Interpreter) builtinSubsequences(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	n := len(arr)
	total := 1 << n // 2^n subsequences

	result := []Value{}
	for i := 0; i < total; i++ {
		subseq := []Value{}
		for j := 0; j < n; j++ {
			if i&(1<<j) != 0 {
				subseq = append(subseq, arr[j])
			}
		}
		if len(subseq) > 0 {
			result = append(result, subseq)
		}
	}
	return append([]Value{}, result...) // Ensure we include empty list at start
}

// builtinPermutations returns all permutations
func (i *Interpreter) builtinPermutations(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok {
		return []Value{}
	}

	if len(arr) == 0 {
		return []Value{}
	}

	// Heap's algorithm
	result := [][]Value{}
	n := len(arr)

	// Helper to generate permutations
	var generate func([]Value, int)
	generate = func(a []Value, k int) {
		if k == 1 {
			perm := make([]Value, len(a))
			copy(perm, a)
			result = append(result, perm)
			return
		}
		generate(a, k-1)
		for i := 0; i < k-1; i++ {
			if k%2 == 0 {
				a[i], a[k-1] = a[k-1], a[i]
			} else {
				a[0], a[k-1] = a[k-1], a[0]
			}
			generate(a, k-1)
		}
	}

	arrCopy := make([]Value, len(arr))
	copy(arrCopy, arr)
	generate(arrCopy, n)

	ret := make([]Value, len(result))
	for i, r := range result {
		ret[i] = r
	}
	return ret
}

// builtinMode calculates mode (most frequent value)
func (i *Interpreter) builtinMode(args []Value) Value {
	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return nil
	}

	counts := make(map[string]int)
	for _, v := range arr {
		key := fmt.Sprintf("%v", v)
		counts[key]++
	}

	maxCount := 0
	var mode Value
	for k, c := range counts {
		if c > maxCount {
			maxCount = c
			// Find original value
			for _, v := range arr {
				if fmt.Sprintf("%v", v) == k {
					mode = v
					break
				}
			}
		}
	}
	return mode
}

// builtinStdDev calculates standard deviation
func (i *Interpreter) builtinStdDev(args []Value) Value {
	variance := i.builtinVariance(args)
	return math.Sqrt(i.toFloat(variance))
}

// builtinMinBy finds minimum by key function
func (i *Interpreter) builtinMinBy(args []Value) Value {
	if len(args) < 2 {
		return i.builtinMin(args)
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return nil
	}

	keyFn, ok := args[1].(string)
	if !ok {
		return arr[0]
	}

	minIdx := 0
	minKey := i.getKeyByFunction(arr[0], keyFn)

	for idx, v := range arr[1:] {
		key := i.getKeyByFunction(v, keyFn)
		if key < minKey {
			minKey = key
			minIdx = idx + 1
		}
	}
	return arr[minIdx]
}

// builtinMaxBy finds maximum by key function
func (i *Interpreter) builtinMaxBy(args []Value) Value {
	if len(args) < 2 {
		return i.builtinMax(args)
	}

	arr, ok := args[0].([]Value)
	if !ok || len(arr) == 0 {
		return nil
	}

	keyFn, ok := args[1].(string)
	if !ok {
		return arr[0]
	}

	maxIdx := 0
	maxKey := i.getKeyByFunction(arr[0], keyFn)

	for idx, v := range arr[1:] {
		key := i.getKeyByFunction(v, keyFn)
		if key > maxKey {
			maxKey = key
			maxIdx = idx + 1
		}
	}
	return arr[maxIdx]
}

// Helper functions for array operations

func (i *Interpreter) matchesPredicate(v Value, predicate string) bool {
	switch predicate {
	case "truthy", "true":
		return i.toBool(v)
	case "falsy", "false":
		return !i.toBool(v)
	case "string":
		_, ok := v.(string)
		return ok
	case "number", "numeric":
		switch v.(type) {
		case int, int64, float64:
			return true
		default:
			return false
		}
	case "array":
		_, ok := v.([]Value)
		return ok
	case "object", "map":
		_, ok := v.(map[string]Value)
		return ok
	case "nil", "null":
		return v == nil
	case "positive":
		return i.toFloat(v) > 0
	case "negative":
		return i.toFloat(v) < 0
	case "even":
		return int(i.toFloat(v))%2 == 0
	case "odd":
		return int(i.toFloat(v))%2 != 0
	case "zero":
		return i.toFloat(v) == 0
	default:
		return false
	}
}

func (i *Interpreter) getKeyByFunction(v Value, keyFn string) string {
	switch keyFn {
	case "string":
		return fmt.Sprintf("%v", v)
	case "lower":
		return strings.ToLower(fmt.Sprintf("%v", v))
	case "upper":
		return strings.ToUpper(fmt.Sprintf("%v", v))
	case "type":
		return fmt.Sprintf("%T", v)
	case "len":
		switch val := v.(type) {
		case string:
			return fmt.Sprintf("%d", len(val))
		case []Value:
			return fmt.Sprintf("%d", len(val))
		case map[string]Value:
			return fmt.Sprintf("%d", len(val))
		default:
			return "0"
		}
	default:
		// Check if it's an object property
		if obj, ok := v.(map[string]Value); ok {
			if val, exists := obj[keyFn]; exists {
				return fmt.Sprintf("%v", val)
			}
		}
		return fmt.Sprintf("%v", v)
	}
}

func (i *Interpreter) applyOperation(v Value, operation string) Value {
	switch operation {
	case "inc", "increment":
		return i.toFloat(v) + 1
	case "dec", "decrement":
		return i.toFloat(v) - 1
	case "double":
		return i.toFloat(v) * 2
	case "half":
		return i.toFloat(v) / 2
	case "square":
		f := i.toFloat(v)
		return f * f
	case "negate":
		return -i.toFloat(v)
	case "string":
		return fmt.Sprintf("%v", v)
	default:
		return v
	}
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
// Additional Hash Functions
// ============================================================================

func (i *Interpreter) builtinSHA224(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	hash := sha256.Sum224([]byte(s))
	return hex.EncodeToString(hash[:])
}

func (i *Interpreter) builtinSHA384(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	hash := sha512.Sum384([]byte(s))
	return hex.EncodeToString(hash[:])
}

func (i *Interpreter) builtinSHA3_256(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	h := sha3.New256()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func (i *Interpreter) builtinSHA3_512(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	h := sha3.New512()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func (i *Interpreter) builtinBlake2b256(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	h, err := blake2b.New(32, nil)
	if err != nil {
		return ""
	}
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func (i *Interpreter) builtinBlake2b512(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	h, err := blake2b.New(64, nil)
	if err != nil {
		return ""
	}
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func (i *Interpreter) builtinCRC32(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}
	s := fmt.Sprintf("%v", args[0])
	crc := crc32.ChecksumIEEE([]byte(s))
	return int64(crc)
}

func (i *Interpreter) builtinAdler32(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}
	s := fmt.Sprintf("%v", args[0])
	adler := adler32.Checksum([]byte(s))
	return int64(adler)
}

// ============================================================================
// HMAC Functions
// ============================================================================

func (i *Interpreter) builtinHmacSHA1(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	data := fmt.Sprintf("%v", args[0])
	key := fmt.Sprintf("%v", args[1])
	h := hmac.New(sha1.New, []byte(key))
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func (i *Interpreter) builtinHmacSHA512(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	data := fmt.Sprintf("%v", args[0])
	key := fmt.Sprintf("%v", args[1])
	h := hmac.New(sha512.New, []byte(key))
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

// ============================================================================
// Password Hashing Functions
// ============================================================================

func (i *Interpreter) builtinBcryptHash(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "password required"}
	}

	password := fmt.Sprintf("%v", args[0])
	cost := bcrypt.DefaultCost
	if len(args) > 1 {
		cost = int(i.toInt(args[1]))
		if cost < bcrypt.MinCost {
			cost = bcrypt.MinCost
		}
		if cost > bcrypt.MaxCost {
			cost = bcrypt.MaxCost
		}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success": true,
		"hash":    string(hash),
	}
}

func (i *Interpreter) builtinBcryptVerify(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "password and hash required"}
	}

	password := fmt.Sprintf("%v", args[0])
	hash := fmt.Sprintf("%v", args[1])

	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	if err != nil {
		return map[string]Value{"success": false, "valid": false, "error": "invalid password"}
	}

	return map[string]Value{"success": true, "valid": true}
}

func (i *Interpreter) builtinArgon2id(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "password required"}
	}

	password := fmt.Sprintf("%v", args[0])

	// Generate random salt
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	// Default parameters
	timeCost := uint32(3)
	memory := uint32(64 * 1024) // 64 MB
	threads := uint8(4)
	keyLen := uint32(32)

	if len(args) > 1 {
		if opts, ok := args[1].(map[string]Value); ok {
			if t, ok := opts["time"].(float64); ok {
				timeCost = uint32(t)
			}
			if m, ok := opts["memory"].(float64); ok {
				memory = uint32(m)
			}
			if th, ok := opts["threads"].(float64); ok {
				threads = uint8(th)
			}
			if k, ok := opts["keyLen"].(float64); ok {
				keyLen = uint32(k)
			}
		}
	}

	hash := argon2.IDKey([]byte(password), salt, timeCost, memory, threads, keyLen)

	return map[string]Value{
		"success": true,
		"hash":    hex.EncodeToString(hash),
		"salt":    hex.EncodeToString(salt),
		"params": map[string]Value{
			"time":    int64(timeCost),
			"memory":  int64(memory),
			"threads": int64(threads),
			"keyLen":  int64(keyLen),
		},
	}
}

// ============================================================================
// Key Derivation Functions
// ============================================================================

func (i *Interpreter) builtinPBKDF2(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"success": false, "error": "password, salt, and iterations required"}
	}

	password := fmt.Sprintf("%v", args[0])
	salt := fmt.Sprintf("%v", args[1])
	iterations := int(i.toInt(args[2]))
	if iterations < 1000 {
		iterations = 1000
	}

	keyLen := 32
	if len(args) > 3 {
		keyLen = int(i.toInt(args[3]))
	}

	hashFunc := sha256.New
	if len(args) > 4 {
		if h, ok := args[4].(string); ok {
			switch h {
			case "sha1":
				hashFunc = sha1.New
			case "sha512":
				hashFunc = sha512.New
			}
		}
	}

	key := pbkdf2.Key([]byte(password), []byte(salt), iterations, keyLen, hashFunc)

	return map[string]Value{
		"success":     true,
		"key":         hex.EncodeToString(key),
		"iterations":  int64(iterations),
		"keyLen":      int64(keyLen),
	}
}

func (i *Interpreter) builtinHKDF(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "secret and salt required"}
	}

	secret := fmt.Sprintf("%v", args[0])
	salt := fmt.Sprintf("%v", args[1])
	info := ""
	if len(args) > 2 {
		info = fmt.Sprintf("%v", args[2])
	}

	keyLen := 32
	if len(args) > 3 {
		keyLen = int(i.toInt(args[3]))
	}

	// Simple HKDF implementation using HMAC-SHA256
	// Extract
	prk := hmac.New(sha256.New, []byte(salt))
	prk.Write([]byte(secret))
	prkBytes := prk.Sum(nil)

	// Expand
	var okm []byte
	var t []byte
	counter := byte(1)
	hashLen := 32

	for len(okm) < keyLen {
		h := hmac.New(sha256.New, prkBytes)
		h.Write(t)
		h.Write([]byte(info))
		h.Write([]byte{counter})
		t = h.Sum(nil)
		okm = append(okm, t...)
		counter++
		if int(counter) > hashLen*255 {
			break
		}
	}

	return map[string]Value{
		"success": true,
		"key":     hex.EncodeToString(okm[:keyLen]),
	}
}

// ============================================================================
// Random Generation Functions
// ============================================================================

func (i *Interpreter) builtinRandomBytes(args []Value) Value {
	n := 16
	if len(args) > 0 {
		n = int(i.toInt(args[0]))
	}
	if n <= 0 {
		n = 16
	}
	if n > 1024 {
		n = 1024
	}

	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes)
}

func (i *Interpreter) builtinRandomHex(args []Value) Value {
	n := 16
	if len(args) > 0 {
		n = int(i.toInt(args[0]))
	}
	if n <= 0 {
		n = 16
	}
	if n > 1024 {
		n = 1024
	}

	bytes := make([]byte, (n+1)/2)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes)[:n]
}

func (i *Interpreter) builtinRandomString(args []Value) Value {
	n := 16
	if len(args) > 0 {
		n = int(i.toInt(args[0]))
	}
	if n <= 0 {
		n = 16
	}
	if n > 1024 {
		n = 1024
	}

	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if len(args) > 1 {
		if c, ok := args[1].(string); ok {
			chars = c
		}
	}

	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}

	result := make([]byte, n)
	for j := 0; j < n; j++ {
		result[j] = chars[int(bytes[j])%len(chars)]
	}
	return string(result)
}

func (i *Interpreter) builtinGeneratePassword(args []Value) Value {
	length := 16
	if len(args) > 0 {
		length = int(i.toInt(args[0]))
	}
	if length < 8 {
		length = 8
	}
	if length > 128 {
		length = 128
	}

	// Character sets
	lower := "abcdefghijklmnopqrstuvwxyz"
	upper := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digits := "0123456789"
	special := "!@#$%^&*()_+-=[]{}|;:,.<>?"

	// Parse options
	includeLower := true
	includeUpper := true
	includeDigits := true
	includeSpecial := false

	if len(args) > 1 {
		if opts, ok := args[1].(map[string]Value); ok {
			if v, ok := opts["lower"].(bool); ok {
				includeLower = v
			}
			if v, ok := opts["upper"].(bool); ok {
				includeUpper = v
			}
			if v, ok := opts["digits"].(bool); ok {
				includeDigits = v
			}
			if v, ok := opts["special"].(bool); ok {
				includeSpecial = v
			}
		}
	}

	// Build character set
	var charset string
	if includeLower {
		charset += lower
	}
	if includeUpper {
		charset += upper
	}
	if includeDigits {
		charset += digits
	}
	if includeSpecial {
		charset += special
	}

	if len(charset) == 0 {
		charset = lower + upper + digits
	}

	// Generate password
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}

	password := make([]byte, length)
	for j := 0; j < length; j++ {
		password[j] = charset[int(bytes[j])%len(charset)]
	}

	return string(password)
}

func (i *Interpreter) builtinUUID(args []Value) Value {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return ""
	}

	// Version 4
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Variant
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

func (i *Interpreter) builtinUUIDv7(args []Value) Value {
	now := time.Now()
	ts := uint64(now.UnixMilli())

	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return ""
	}

	// Set timestamp (48 bits, big-endian)
	uuid[0] = byte(ts >> 40)
	uuid[1] = byte(ts >> 32)
	uuid[2] = byte(ts >> 24)
	uuid[3] = byte(ts >> 16)
	uuid[4] = byte(ts >> 8)
	uuid[5] = byte(ts)

	// Version 7
	uuid[6] = (uuid[6] & 0x0f) | 0x70
	// Variant
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// ============================================================================
// Encoding Functions
// ============================================================================

func (i *Interpreter) builtinBase64URLEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", args[0])
	return base64.URLEncoding.EncodeToString([]byte(s))
}

func (i *Interpreter) builtinBase64URLDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		s = fmt.Sprintf("%v", args[0])
	}
	decoded, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return ""
	}
	return string(decoded)
}

// ============================================================================
// Simple Crypto Functions
// ============================================================================

func (i *Interpreter) builtinXorEncrypt(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	data := fmt.Sprintf("%v", args[0])
	key := fmt.Sprintf("%v", args[1])

	result := make([]byte, len(data))
	for j := 0; j < len(data); j++ {
		result[j] = data[j] ^ key[j%len(key)]
	}

	return hex.EncodeToString(result)
}

func (i *Interpreter) builtinXorDecrypt(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	dataHex, ok := args[0].(string)
	if !ok {
		return ""
	}

	data, err := hex.DecodeString(dataHex)
	if err != nil {
		return ""
	}

	key := fmt.Sprintf("%v", args[1])

	result := make([]byte, len(data))
	for j := 0; j < len(data); j++ {
		result[j] = data[j] ^ key[j%len(key)]
	}

	return string(result)
}

// ============================================================================
// JWT Functions (Simple)
// ============================================================================

func (i *Interpreter) builtinJWTEncode(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	// Get header
	header := `{"alg":"HS256","typ":"JWT"}`
	if len(args) > 2 {
		if h, ok := args[2].(string); ok {
			header = h
		}
	}

	// Get payload
	var payload string
	switch v := args[0].(type) {
	case string:
		payload = v
	case map[string]Value:
		data, _ := json.Marshal(v)
		payload = string(data)
	default:
		payload = fmt.Sprintf("%v", v)
	}

	// Get secret
	secret := fmt.Sprintf("%v", args[1])

	// Encode header and payload
	headerB64 := base64.RawURLEncoding.EncodeToString([]byte(header))
	payloadB64 := base64.RawURLEncoding.EncodeToString([]byte(payload))

	// Sign
	signingInput := headerB64 + "." + payloadB64
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(signingInput))
	signature := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

	return signingInput + "." + signature
}

func (i *Interpreter) builtinJWTDecode(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "token required"}
	}

	token, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "invalid token"}
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return map[string]Value{"success": false, "error": "invalid token format"}
	}

	// Decode header
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return map[string]Value{"success": false, "error": "invalid header encoding"}
	}

	// Decode payload
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return map[string]Value{"success": false, "error": "invalid payload encoding"}
	}

	result := map[string]Value{
		"success": true,
		"header":  string(headerBytes),
		"payload": string(payloadBytes),
	}

	// Verify signature if secret provided
	if len(args) > 1 {
		secret := fmt.Sprintf("%v", args[1])
		signingInput := parts[0] + "." + parts[1]
		h := hmac.New(sha256.New, []byte(secret))
		h.Write([]byte(signingInput))
		expectedSig := base64.RawURLEncoding.EncodeToString(h.Sum(nil))
		result["valid"] = expectedSig == parts[2]
	}

	return result
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
// JSON Processing Functions
// ============================================================================

// builtinJSONMinify minifies a JSON string
func (i *Interpreter) builtinJSONMinify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return map[string]Value{"success": false, "error": err.Error()}
		}
	default:
		obj = v
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	return string(data)
}

// builtinJSONGet gets a value from JSON by path (e.g., "data.user.name" or "items[0].id")
func (i *Interpreter) builtinJSONGet(args []Value) Value {
	if len(args) < 2 {
		return nil
	}

	// Parse the JSON or use the object directly
	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return nil
		}
	default:
		obj = v
	}

	path, ok := args[1].(string)
	if !ok {
		return nil
	}

	result := getJSONByPath(obj, path)
	return result
}

// getJSONByPath navigates through JSON using a path like "data.user.name" or "items[0].id"
func getJSONByPath(obj interface{}, path string) Value {
	parts := parseJSONPath(path)
	current := obj

	for _, part := range parts {
		if current == nil {
			return nil
		}

		switch v := current.(type) {
		case map[string]interface{}:
			if val, ok := v[part.key]; ok {
				current = val
			} else {
				return nil
			}
		case []interface{}:
			if part.index >= 0 && part.index < len(v) {
				current = v[part.index]
			} else {
				return nil
			}
		case map[string]Value:
			if val, ok := v[part.key]; ok {
				return val
			}
			return nil
		case []Value:
			if part.index >= 0 && part.index < len(v) {
				return v[part.index]
			}
			return nil
		default:
			return nil
		}
	}

	return convertJSONToValue(current)
}

type pathPart struct {
	key   string
	index int
}

func parseJSONPath(path string) []pathPart {
	var parts []pathPart
	current := ""
	inBracket := false
	bracketContent := ""

	for _, ch := range path {
		switch ch {
		case '.':
			if inBracket {
				bracketContent += string(ch)
			} else if current != "" {
				parts = append(parts, pathPart{key: current})
				current = ""
			}
		case '[':
			if current != "" {
				parts = append(parts, pathPart{key: current})
				current = ""
			}
			inBracket = true
			bracketContent = ""
		case ']':
			if inBracket {
				// Parse index
				idx := 0
				for _, c := range bracketContent {
					if c >= '0' && c <= '9' {
						idx = idx*10 + int(c-'0')
					}
				}
				parts = append(parts, pathPart{index: idx})
				inBracket = false
			}
		default:
			if inBracket {
				bracketContent += string(ch)
			} else {
				current += string(ch)
			}
		}
	}

	if current != "" {
		parts = append(parts, pathPart{key: current})
	}

	return parts
}

// builtinJSONSet sets a value in JSON by path
func (i *Interpreter) builtinJSONSet(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"success": false, "error": "requires json, path, and value"}
	}

	// Parse the JSON or use the object directly
	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return map[string]Value{"success": false, "error": err.Error()}
		}
	default:
		obj = v
	}

	path, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	value := args[2]

	// Set the value
	result := setJSONByPath(obj, path, value)
	return result
}

func setJSONByPath(obj interface{}, path string, value Value) Value {
	parts := parseJSONPath(path)
	if len(parts) == 0 {
		return value
	}

	// If obj is nil or not a map/array, create a new map
	var root interface{}
	switch v := obj.(type) {
	case map[string]interface{}:
		root = v
	case map[string]Value:
		// Convert to map[string]interface{}
		newMap := make(map[string]interface{})
		for k, val := range v {
			newMap[k] = val
		}
		root = newMap
	default:
		root = make(map[string]interface{})
	}

	// Navigate and set
	current := root
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		var next interface{}

		switch v := current.(type) {
		case map[string]interface{}:
			if val, ok := v[part.key]; ok {
				next = val
			} else {
				// Create next level based on next part
				if parts[i+1].index >= 0 {
					next = make([]interface{}, 0)
				} else {
					next = make(map[string]interface{})
				}
				v[part.key] = next
			}
		case []interface{}:
			if part.index >= 0 && part.index < len(v) {
				next = v[part.index]
			} else if part.index >= len(v) {
				// Extend array
				for len(v) <= part.index {
					v = append(v, nil)
				}
				current = v
				if parts[i+1].index >= 0 {
					next = make([]interface{}, 0)
				} else {
					next = make(map[string]interface{})
				}
				v[part.index] = next
			} else {
				return map[string]Value{"success": false, "error": "invalid array index"}
			}
		}
		current = next
	}

	// Set the final value
	lastPart := parts[len(parts)-1]
	switch v := current.(type) {
	case map[string]interface{}:
		v[lastPart.key] = value
	case []interface{}:
		if lastPart.index >= 0 && lastPart.index < len(v) {
			v[lastPart.index] = value
		}
	}

	return convertJSONToValue(root)
}

// builtinJSONDelete deletes a value from JSON by path
func (i *Interpreter) builtinJSONDelete(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "requires json and path"}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return map[string]Value{"success": false, "error": err.Error()}
		}
	default:
		obj = v
	}

	path, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "path must be a string"}
	}

	result := deleteJSONByPath(obj, path)
	return result
}

func deleteJSONByPath(obj interface{}, path string) Value {
	parts := parseJSONPath(path)
	if len(parts) == 0 {
		return obj
	}

	// Navigate to parent
	current := obj
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		switch v := current.(type) {
		case map[string]interface{}:
			if val, ok := v[part.key]; ok {
				current = val
			} else {
				return convertJSONToValue(obj)
			}
		case []interface{}:
			if part.index >= 0 && part.index < len(v) {
				current = v[part.index]
			} else {
				return convertJSONToValue(obj)
			}
		default:
			return convertJSONToValue(obj)
		}
	}

	// Delete the key/index
	lastPart := parts[len(parts)-1]
	switch v := current.(type) {
	case map[string]interface{}:
		delete(v, lastPart.key)
	case []interface{}:
		if lastPart.index >= 0 && lastPart.index < len(v) {
			v = append(v[:lastPart.index], v[lastPart.index+1:]...)
			// Update parent - this is tricky, so we return as is
		}
	}

	return convertJSONToValue(obj)
}

// builtinJSONHas checks if a path exists in JSON
func (i *Interpreter) builtinJSONHas(args []Value) Value {
	if len(args) < 2 {
		return false
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return false
		}
	default:
		obj = v
	}

	path, ok := args[1].(string)
	if !ok {
		return false
	}

	result := getJSONByPath(obj, path)
	return result != nil
}

// builtinJSONKeys gets all keys from a JSON object
func (i *Interpreter) builtinJSONKeys(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return []Value{}
		}
	default:
		obj = v
	}

	switch v := obj.(type) {
	case map[string]interface{}:
		keys := make([]Value, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		return keys
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

// builtinJSONValues gets all values from a JSON object
func (i *Interpreter) builtinJSONValues(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return []Value{}
		}
	default:
		obj = v
	}

	switch v := obj.(type) {
	case map[string]interface{}:
		values := make([]Value, 0, len(v))
		for _, val := range v {
			values = append(values, convertJSONToValue(val))
		}
		return values
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

// builtinJSONType returns the type of a JSON value
func (i *Interpreter) builtinJSONType(args []Value) Value {
	if len(args) == 0 {
		return "null"
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		// Try to parse as JSON
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return "string"
		}
	default:
		obj = v
	}

	switch v := obj.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}, []Value:
		return "array"
	case map[string]interface{}, map[string]Value:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// builtinJSONMerge merges multiple JSON objects
func (i *Interpreter) builtinJSONMerge(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{}
	}

	result := make(map[string]interface{})

	for _, arg := range args {
		var obj interface{}
		switch v := arg.(type) {
		case string:
			if err := json.Unmarshal([]byte(v), &obj); err != nil {
				continue
			}
		default:
			obj = v
		}

		switch v := obj.(type) {
		case map[string]interface{}:
			for k, val := range v {
				result[k] = val
			}
		case map[string]Value:
			for k, val := range v {
				result[k] = val
			}
		}
	}

	return convertJSONToValue(result)
}

// builtinJSONDeepMerge deeply merges multiple JSON objects
func (i *Interpreter) builtinJSONDeepMerge(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{}
	}

	var result interface{}

	for _, arg := range args {
		var obj interface{}
		switch v := arg.(type) {
		case string:
			if err := json.Unmarshal([]byte(v), &obj); err != nil {
				continue
			}
		default:
			obj = v
		}

		result = deepMergeJSON(result, obj)
	}

	return convertJSONToValue(result)
}

func deepMergeJSON(base, overlay interface{}) interface{} {
	if base == nil {
		return overlay
	}
	if overlay == nil {
		return base
	}

	baseMap, baseIsMap := base.(map[string]interface{})
	overlayMap, overlayIsMap := overlay.(map[string]interface{})

	if baseIsMap && overlayIsMap {
		result := make(map[string]interface{})
		for k, v := range baseMap {
			result[k] = v
		}
		for k, v := range overlayMap {
			if existing, ok := result[k]; ok {
				result[k] = deepMergeJSON(existing, v)
			} else {
				result[k] = v
			}
		}
		return result
	}

	// If not both maps, overlay wins
	return overlay
}

// builtinJSONArrayLength returns the length of a JSON array
func (i *Interpreter) builtinJSONArrayLength(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return int64(0)
		}
	default:
		obj = v
	}

	switch v := obj.(type) {
	case []interface{}:
		return int64(len(v))
	case []Value:
		return int64(len(v))
	default:
		return int64(0)
	}
}

// builtinJSONArrayAppend appends values to a JSON array
func (i *Interpreter) builtinJSONArrayAppend(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "requires array and value"}
	}

	var arr []interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &arr); err != nil {
			return map[string]Value{"success": false, "error": err.Error()}
		}
	case []interface{}:
		arr = v
	case []Value:
		arr = make([]interface{}, len(v))
		for i, val := range v {
			arr[i] = val
		}
	default:
		return map[string]Value{"success": false, "error": "first argument must be an array"}
	}

	// Append all additional arguments
	for _, val := range args[1:] {
		arr = append(arr, val)
	}

	return convertJSONToValue(arr)
}

// builtinJSONArrayPrepend prepends values to a JSON array
func (i *Interpreter) builtinJSONArrayPrepend(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "requires array and value"}
	}

	var arr []interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &arr); err != nil {
			return map[string]Value{"success": false, "error": err.Error()}
		}
	case []interface{}:
		arr = v
	case []Value:
		arr = make([]interface{}, len(v))
		for i, val := range v {
			arr[i] = val
		}
	default:
		return map[string]Value{"success": false, "error": "first argument must be an array"}
	}

	// Prepend all additional arguments
	newArr := make([]interface{}, 0, len(arr)+len(args)-1)
	for _, val := range args[1:] {
		newArr = append(newArr, val)
	}
	newArr = append(newArr, arr...)

	return convertJSONToValue(newArr)
}

// builtinJSONArrayFlatten flattens nested arrays
func (i *Interpreter) builtinJSONArrayFlatten(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return []Value{}
		}
	default:
		obj = v
	}

	result := flattenJSON(obj)
	return result
}

func flattenJSON(obj interface{}) []Value {
	var result []Value

	switch v := obj.(type) {
	case []interface{}:
		for _, item := range v {
			result = append(result, flattenJSON(item)...)
		}
	case []Value:
		for _, item := range v {
			result = append(result, flattenJSON(item)...)
		}
	default:
		result = append(result, convertJSONToValue(obj))
	}

	return result
}

// builtinJSONObjectFromArrays creates an object from keys and values arrays
func (i *Interpreter) builtinJSONObjectFromArrays(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{}
	}

	var keys []interface{}
	var values []interface{}

	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &keys); err != nil {
			return map[string]Value{}
		}
	case []interface{}:
		keys = v
	case []Value:
		keys = make([]interface{}, len(v))
		for i, val := range v {
			keys[i] = val
		}
	}

	switch v := args[1].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &values); err != nil {
			return map[string]Value{}
		}
	case []interface{}:
		values = v
	case []Value:
		values = make([]interface{}, len(v))
		for i, val := range v {
			values[i] = val
		}
	}

	result := make(map[string]interface{})
	for i := 0; i < len(keys) && i < len(values); i++ {
		if key, ok := keys[i].(string); ok {
			result[key] = values[i]
		}
	}

	return convertJSONToValue(result)
}

// builtinJSONValidate validates a JSON string
func (i *Interpreter) builtinJSONValidate(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"valid": false, "error": "no input"}
	}

	jsonStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"valid": false, "error": "input must be a string"}
	}

	var obj interface{}
	err := json.Unmarshal([]byte(jsonStr), &obj)
	if err != nil {
		return map[string]Value{"valid": false, "error": err.Error()}
	}

	return map[string]Value{"valid": true}
}

// builtinJSONOmit creates a new object omitting specified keys
func (i *Interpreter) builtinJSONOmit(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return map[string]Value{}
		}
	default:
		obj = v
	}

	// Get keys to omit
	omitKeys := make(map[string]bool)
	for _, arg := range args[1:] {
		if key, ok := arg.(string); ok {
			omitKeys[key] = true
		}
	}

	objMap, ok := obj.(map[string]interface{})
	if !ok {
		return convertJSONToValue(obj)
	}

	result := make(map[string]interface{})
	for k, v := range objMap {
		if !omitKeys[k] {
			result[k] = v
		}
	}

	return convertJSONToValue(result)
}

// builtinJSONPick creates a new object with only specified keys
func (i *Interpreter) builtinJSONPick(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return map[string]Value{}
		}
	default:
		obj = v
	}

	// Get keys to pick
	pickKeys := make(map[string]bool)
	for _, arg := range args[1:] {
		if key, ok := arg.(string); ok {
			pickKeys[key] = true
		}
	}

	objMap, ok := obj.(map[string]interface{})
	if !ok {
		return convertJSONToValue(obj)
	}

	result := make(map[string]interface{})
	for k, v := range objMap {
		if pickKeys[k] {
			result[k] = v
		}
	}

	return convertJSONToValue(result)
}

// builtinJSONTransform transforms values in a JSON object using a function
func (i *Interpreter) builtinJSONTransform(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "requires json and transform type"}
	}

	var obj interface{}
	switch v := args[0].(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return map[string]Value{"success": false, "error": err.Error()}
		}
	default:
		obj = v
	}

	transformType, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "transform type must be a string"}
	}

	result := transformJSON(obj, transformType)
	return result
}

func transformJSON(obj interface{}, transformType string) Value {
	switch v := obj.(type) {
	case map[string]interface{}:
		result := make(map[string]Value)
		for k, val := range v {
			result[k] = transformJSON(val, transformType)
		}
		return result
	case []interface{}:
		result := make([]Value, len(v))
		for i, val := range v {
			result[i] = transformJSON(val, transformType)
		}
		return result
	case string:
		switch transformType {
		case "upper", "uppercase":
			return strings.ToUpper(v)
		case "lower", "lowercase":
			return strings.ToLower(v)
		case "trim":
			return strings.TrimSpace(v)
		case "number":
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
			return v
		default:
			return v
		}
	case float64:
		switch transformType {
		case "int":
			return int64(v)
		case "string":
			return fmt.Sprintf("%v", v)
		default:
			return v
		}
	default:
		return convertJSONToValue(obj)
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
// Date/Time Functions
// ============================================================================

// Helper to parse time from various inputs
func parseTimeArg(arg Value) time.Time {
	switch v := arg.(type) {
	case time.Time:
		return v
	case float64:
		return time.Unix(int64(v), 0)
	case int64:
		return time.Unix(v, 0)
	case int:
		return time.Unix(int64(v), 0)
	case string:
		// Try common formats
		formats := []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"2006/01/02",
			"01/02/2006",
			"02-01-2006",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// builtinDateFormat formats a time using Go's reference time format
func (i *Interpreter) builtinDateFormat(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	format, ok := args[1].(string)
	if !ok {
		return ""
	}

	// Convert common format placeholders to Go format
	// Support both Go format and common strftime-like format
	switch format {
	case "RFC3339":
		return t.Format(time.RFC3339)
	case "RFC3339Nano":
		return t.Format(time.RFC3339Nano)
	case "Date":
		return t.Format("2006-01-02")
	case "DateTime":
		return t.Format("2006-01-02 15:04:05")
	case "Time":
		return t.Format("15:04:05")
	case "ISO":
		return t.Format("2006-01-02T15:04:05Z07:00")
	default:
		return t.Format(format)
	}
}

// builtinDateParse parses a string into a timestamp
func (i *Interpreter) builtinDateParse(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}

	dateStr, ok1 := args[0].(string)
	format, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return int64(0)
	}

	// Handle named formats
	switch format {
	case "RFC3339":
		format = time.RFC3339
	case "RFC3339Nano":
		format = time.RFC3339Nano
	case "Date":
		format = "2006-01-02"
	case "DateTime":
		format = "2006-01-02 15:04:05"
	case "ISO":
		format = "2006-01-02T15:04:05Z07:00"
	}

	t, err := time.Parse(format, dateStr)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success": true,
		"unix":    t.Unix(),
		"string":  t.Format(time.RFC3339),
	}
}

// builtinStrftime formats time using strftime-like format
func (i *Interpreter) builtinStrftime(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	format, ok := args[1].(string)
	if !ok {
		return ""
	}

	// Convert strftime format to Go format
	goFormat := convertStrftimeToGo(format)
	return t.Format(goFormat)
}

func convertStrftimeToGo(format string) string {
	// Common strftime conversions
	replacements := []struct {
		strftime string
		goFormat string
	}{
		{"%Y", "2006"},
		{"%y", "06"},
		{"%m", "01"},
		{"%d", "02"},
		{"%H", "15"},
		{"%M", "04"},
		{"%S", "05"},
		{"%I", "03"}, // 12-hour
		{"%p", "PM"},
		{"%A", "Monday"},
		{"%a", "Mon"},
		{"%B", "January"},
		{"%b", "Jan"},
		{"%w", "Monday"}, // weekday
		{"%j", "002"},    // day of year
		{"%U", "00"},     // week number (Sunday start)
		{"%W", "00"},     // week number (Monday start)
		{"%Z", "MST"},
		{"%z", "-0700"},
		{"%%", "%"},
	}

	result := format
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.strftime, r.goFormat)
	}
	return result
}

// builtinStrptime parses a string using strftime-like format
func (i *Interpreter) builtinStrptime(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false}
	}

	dateStr, ok1 := args[0].(string)
	format, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return map[string]Value{"success": false}
	}

	goFormat := convertStrftimeToGo(format)
	t, err := time.Parse(goFormat, dateStr)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success": true,
		"unix":    t.Unix(),
	}
}

// builtinDate creates a date from components
func (i *Interpreter) builtinDate(args []Value) Value {
	if len(args) < 3 {
		return time.Now().Unix()
	}

	year := i.toInt(args[0])
	month := i.toInt(args[1])
	day := i.toInt(args[2])

	hour, min, sec := 0, 0, 0
	if len(args) > 3 {
		hour = i.toInt(args[3])
	}
	if len(args) > 4 {
		min = i.toInt(args[4])
	}
	if len(args) > 5 {
		sec = i.toInt(args[5])
	}

	t := time.Date(year, time.Month(month), day, hour, min, sec, 0, time.Local)
	return t.Unix()
}

// builtinDateFromUnix creates a date from Unix timestamp
func (i *Interpreter) builtinDateFromUnix(args []Value) Value {
	if len(args) == 0 {
		return time.Now().Unix()
	}

	ts := i.toInt(args[0])
	t := time.Unix(int64(ts), 0)
	return map[string]Value{
		"unix":   t.Unix(),
		"string": t.Format(time.RFC3339),
	}
}

// builtinDateNow returns current time info
func (i *Interpreter) builtinDateNow(args []Value) Value {
	t := time.Now()
	return map[string]Value{
		"unix":       t.Unix(),
		"unixMilli":  t.UnixMilli(),
		"unixMicro":  t.UnixMicro(),
		"unixNano":   t.UnixNano(),
		"string":     t.Format(time.RFC3339),
		"date":       t.Format("2006-01-02"),
		"time":       t.Format("15:04:05"),
		"datetime":   t.Format("2006-01-02 15:04:05"),
		"year":       int64(t.Year()),
		"month":      int64(t.Month()),
		"day":        int64(t.Day()),
		"hour":       int64(t.Hour()),
		"minute":     int64(t.Minute()),
		"second":     int64(t.Second()),
		"weekday":    t.Weekday().String(),
		"weekdayNum": int64(t.Weekday()),
	}
}

// builtinToday returns today's date info
func (i *Interpreter) builtinToday(args []Value) Value {
	now := time.Now()
	t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	return map[string]Value{
		"date":     t.Format("2006-01-02"),
		"unix":     t.Unix(),
		"year":     int64(t.Year()),
		"month":    int64(t.Month()),
		"day":      int64(t.Day()),
		"weekday":  t.Weekday().String(),
		"start":    t.Unix(),
		"end":      t.Add(24*time.Hour - time.Second).Unix(),
	}
}

// Extraction functions
func (i *Interpreter) builtinYear(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.Year())
}

func (i *Interpreter) builtinMonth(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.Month())
}

func (i *Interpreter) builtinDay(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.Day())
}

func (i *Interpreter) builtinHour(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.Hour())
}

func (i *Interpreter) builtinMinute(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.Minute())
}

func (i *Interpreter) builtinSecond(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.Second())
}

func (i *Interpreter) builtinWeekday(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return t.Weekday().String()
}

func (i *Interpreter) builtinYearday(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	return int64(t.YearDay())
}

func (i *Interpreter) builtinWeek(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	_, week := t.ISOWeek()
	return int64(week)
}

func (i *Interpreter) builtinQuarter(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	month := t.Month()
	quarter := (month-1)/3 + 1
	return int64(quarter)
}

// Manipulation functions
func (i *Interpreter) builtinDateAdd(args []Value) Value {
	if len(args) < 2 {
		return time.Now().Unix()
	}

	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	// Parse duration
	switch d := args[1].(type) {
	case string:
		if dur, err := time.ParseDuration(d); err == nil {
			return t.Add(dur).Unix()
		}
	case float64:
		return t.Add(time.Duration(d * float64(time.Second))).Unix()
	case int64:
		return t.Add(time.Duration(d) * time.Second).Unix()
	case int:
		return t.Add(time.Duration(d) * time.Second).Unix()
	}

	return t.Unix()
}

func (i *Interpreter) builtinDateSub(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}

	t := parseTimeArg(args[0])
	other := parseTimeArg(args[1])

	return int64(t.Sub(other).Seconds())
}

func (i *Interpreter) builtinDateAddDays(args []Value) Value {
	if len(args) < 2 {
		return time.Now().Unix()
	}

	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	days := i.toInt(args[1])
	return t.AddDate(0, 0, int(days)).Unix()
}

func (i *Interpreter) builtinDateAddMonths(args []Value) Value {
	if len(args) < 2 {
		return time.Now().Unix()
	}

	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	months := i.toInt(args[1])
	return t.AddDate(0, int(months), 0).Unix()
}

func (i *Interpreter) builtinDateAddYears(args []Value) Value {
	if len(args) < 2 {
		return time.Now().Unix()
	}

	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	years := i.toInt(args[1])
	return t.AddDate(int(years), 0, 0).Unix()
}

func (i *Interpreter) builtinStartOfDay(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	return start.Unix()
}

func (i *Interpreter) builtinEndOfDay(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	end := time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 0, t.Location())
	return end.Unix()
}

func (i *Interpreter) builtinStartOfWeek(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	weekday := t.Weekday()
	daysToSubtract := int(weekday)
	if daysToSubtract == 0 {
		daysToSubtract = 6 // Sunday = end of week
	} else {
		daysToSubtract--
	}
	start := t.AddDate(0, 0, -daysToSubtract)
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
	return start.Unix()
}

func (i *Interpreter) builtinEndOfWeek(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	weekday := t.Weekday()
	daysToAdd := 6 - int(weekday)
	if weekday == time.Sunday {
		daysToAdd = 0
	}
	end := t.AddDate(0, 0, daysToAdd)
	end = time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 0, end.Location())
	return end.Unix()
}

func (i *Interpreter) builtinStartOfMonth(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	return start.Unix()
}

func (i *Interpreter) builtinEndOfMonth(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	// First day of next month minus 1 day
	nextMonth := t.AddDate(0, 1, -t.Day()+1)
	end := time.Date(nextMonth.Year(), nextMonth.Month(), nextMonth.Day()-1, 23, 59, 59, 0, nextMonth.Location())
	return end.Unix()
}

func (i *Interpreter) builtinStartOfYear(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	start := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	return start.Unix()
}

func (i *Interpreter) builtinEndOfYear(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}
	end := time.Date(t.Year(), 12, 31, 23, 59, 59, 0, t.Location())
	return end.Unix()
}

// Comparison functions
func (i *Interpreter) builtinDateDiff(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{}
	}

	t1 := parseTimeArg(args[0])
	t2 := parseTimeArg(args[1])

	if t1.IsZero() || t2.IsZero() {
		return map[string]Value{"error": "invalid date"}
	}

	dur := t2.Sub(t1)
	seconds := int64(dur.Seconds())
	minutes := seconds / 60
	hours := minutes / 60
	days := hours / 24

	return map[string]Value{
		"seconds":    seconds,
		"minutes":    minutes,
		"hours":      hours,
		"days":       days,
		"weeks":      days / 7,
		"months":     days / 30,
		"years":      days / 365,
		"absSeconds": int64(math.Abs(float64(seconds))),
		"absDays":    int64(math.Abs(float64(days))),
	}
}

func (i *Interpreter) builtinDateCompare(args []Value) Value {
	if len(args) < 2 {
		return int64(0)
	}

	t1 := parseTimeArg(args[0])
	t2 := parseTimeArg(args[1])

	if t1.Before(t2) {
		return int64(-1)
	} else if t1.After(t2) {
		return int64(1)
	}
	return int64(0)
}

func (i *Interpreter) builtinDateBefore(args []Value) Value {
	if len(args) < 2 {
		return false
	}

	t1 := parseTimeArg(args[0])
	t2 := parseTimeArg(args[1])

	return t1.Before(t2)
}

func (i *Interpreter) builtinDateAfter(args []Value) Value {
	if len(args) < 2 {
		return false
	}

	t1 := parseTimeArg(args[0])
	t2 := parseTimeArg(args[1])

	return t1.After(t2)
}

func (i *Interpreter) builtinDateEqual(args []Value) Value {
	if len(args) < 2 {
		return false
	}

	t1 := parseTimeArg(args[0])
	t2 := parseTimeArg(args[1])

	return t1.Equal(t2)
}

func (i *Interpreter) builtinDateBetween(args []Value) Value {
	if len(args) < 3 {
		return false
	}

	t := parseTimeArg(args[0])
	start := parseTimeArg(args[1])
	end := parseTimeArg(args[2])

	return (t.Equal(start) || t.After(start)) && (t.Equal(end) || t.Before(end))
}

// Utility functions
func (i *Interpreter) builtinIsLeapYear(args []Value) Value {
	year := time.Now().Year()
	if len(args) > 0 {
		year = int(i.toInt(args[0]))
	}

	isLeap := (year%4 == 0 && year%100 != 0) || (year%400 == 0)
	return isLeap
}

func (i *Interpreter) builtinDaysInMonth(args []Value) Value {
	year := time.Now().Year()
	month := int(time.Now().Month())

	if len(args) >= 2 {
		year = int(i.toInt(args[0]))
		month = int(i.toInt(args[1]))
	} else if len(args) == 1 {
		// Assume it's a timestamp
		t := parseTimeArg(args[0])
		year = t.Year()
		month = int(t.Month())
	}

	// First day of next month minus 1 day gives last day of current month
	firstOfNextMonth := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, time.UTC)
	lastDay := firstOfNextMonth.AddDate(0, 0, -1).Day()
	return int64(lastDay)
}

func (i *Interpreter) builtinDaysInYear(args []Value) Value {
	year := time.Now().Year()
	if len(args) > 0 {
		year = int(i.toInt(args[0]))
	}

	if (year%4 == 0 && year%100 != 0) || (year%400 == 0) {
		return int64(366)
	}
	return int64(365)
}

func (i *Interpreter) builtinParseDuration(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "no duration provided"}
	}

	durStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "duration must be a string"}
	}

	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":     true,
		"nanoseconds": int64(dur),
		"seconds":     dur.Seconds(),
		"minutes":     dur.Minutes(),
		"hours":       dur.Hours(),
		"string":      dur.String(),
	}
}

func (i *Interpreter) builtinFormatDuration(args []Value) Value {
	if len(args) == 0 {
		return ""
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
			return ""
		}
		d = parsed
	default:
		return ""
	}

	// Format in human-readable form
	seconds := int64(d.Seconds())
	minutes := seconds / 60
	hours := minutes / 60
	days := hours / 24

	parts := []string{}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours%24 > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours%24))
	}
	if minutes%60 > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes%60))
	}
	if seconds%60 > 0 {
		parts = append(parts, fmt.Sprintf("%ds", seconds%60))
	}

	if len(parts) == 0 {
		return "0s"
	}
	return strings.Join(parts, " ")
}

func (i *Interpreter) builtinAge(args []Value) Value {
	if len(args) == 0 {
		return int64(0)
	}

	birthdate := parseTimeArg(args[0])
	if birthdate.IsZero() {
		return int64(0)
	}

	now := time.Now()
	years := now.Year() - birthdate.Year()

	// Adjust if birthday hasn't occurred this year
	if now.Month() < birthdate.Month() ||
		(now.Month() == birthdate.Month() && now.Day() < birthdate.Day()) {
		years--
	}

	return int64(years)
}

func (i *Interpreter) builtinIsWeekend(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	weekday := t.Weekday()
	return weekday == time.Saturday || weekday == time.Sunday
}

func (i *Interpreter) builtinIsWorkday(args []Value) Value {
	t := parseTimeArg(args[0])
	if t.IsZero() {
		t = time.Now()
	}

	weekday := t.Weekday()
	return weekday != time.Saturday && weekday != time.Sunday
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

// ============================================================================
// Encoding/Decoding Functions
// ============================================================================

// Base32 Encoding/Decoding
func (i *Interpreter) builtinBase32Encode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	return base32.StdEncoding.EncodeToString([]byte(s))
}

func (i *Interpreter) builtinBase32Decode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	data, err := base32.StdEncoding.DecodeString(s)
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	return string(data)
}

// ROT13 Encoding/Decoding (self-inverse)
func (i *Interpreter) builtinRot13(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			result.WriteRune('a' + (r-'a'+13)%26)
		} else if r >= 'A' && r <= 'Z' {
			result.WriteRune('A' + (r-'A'+13)%26)
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// ROT-N Encoding/Decoding
func (i *Interpreter) builtinRotN(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	n := i.toInt(args[1])

	var result strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			// Handle negative n
			shift := ((int(r-'a') + int(n)) % 26 + 26) % 26
			result.WriteRune(rune('a' + shift))
		} else if r >= 'A' && r <= 'Z' {
			shift := ((int(r-'A') + int(n)) % 26 + 26) % 26
			result.WriteRune(rune('A' + shift))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Caesar Cipher
func (i *Interpreter) builtinCaesarEncode(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}
	shift := i.toInt(args[1])

	var result strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			result.WriteRune('a' + (r-'a'+rune(shift)+26)%26)
		} else if r >= 'A' && r <= 'Z' {
			result.WriteRune('A' + (r-'A'+rune(shift)+26)%26)
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func (i *Interpreter) builtinCaesarDecode(args []Value) Value {
	if len(args) < 2 {
		return ""
	}
	// Decode is encode with negative shift
	shift := i.toInt(args[1])
	newArgs := []Value{args[0], -shift}
	return i.builtinCaesarEncode(newArgs)
}

// Quoted-Printable Encoding/Decoding
func (i *Interpreter) builtinQuotedPrintableEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	reader := strings.NewReader(s)
	writer := quotedprintable.NewWriter(&result)
	writer.Write([]byte(s))
	reader.Read(make([]byte, len(s)))
	return result.String()
}

func (i *Interpreter) builtinQuotedPrintableDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	reader := quotedprintable.NewReader(strings.NewReader(s))
	data, err := io.ReadAll(reader)
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	return string(data)
}

// UUencode/UUdecode (simplified implementation)
func (i *Interpreter) builtinUUEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	data := []byte(s)

	// Write begin line
	mode := "644"
	if len(args) > 1 {
		mode, _ = args[1].(string)
	}
	filename := "file"
	if len(args) > 2 {
		filename, _ = args[2].(string)
	}
	result.WriteString(fmt.Sprintf("begin %s %s\n", mode, filename))

	// Encode data
	for len(data) > 0 {
		chunk := data
		if len(chunk) > 45 {
			chunk = data[:45]
			data = data[45:]
		} else {
			data = nil
		}

		// Write length byte
		result.WriteByte(' ' + byte(len(chunk)))

		// Encode chunk
		for j := 0; j < len(chunk); j += 3 {
			var b1, b2, b3 byte
			b1 = chunk[j]
			if j+1 < len(chunk) {
				b2 = chunk[j+1]
			}
			if j+2 < len(chunk) {
				b3 = chunk[j+2]
			}

			result.WriteByte(' ' + (b1 >> 2))
			result.WriteByte(' ' + ((b1&0x03)<<4 | b2>>4))
			result.WriteByte(' ' + ((b2&0x0f)<<2 | b3>>6))
			result.WriteByte(' ' + (b3 & 0x3f))
		}
		result.WriteByte('\n')
	}

	result.WriteString("`\nend\n")
	return result.String()
}

func (i *Interpreter) builtinUUDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	lines := strings.Split(s, "\n")
	var result []byte
	inData := false

	for _, line := range lines {
		if strings.HasPrefix(line, "begin ") {
			inData = true
			continue
		}
		if line == "end" {
			break
		}
		if !inData || len(line) == 0 {
			continue
		}

		// Get length
		length := int(line[0] - ' ')
		if length <= 0 {
			continue
		}

		// Decode line
		data := line[1:]
		for j := 0; j < len(data) && len(result) < length; j += 4 {
			if j+3 >= len(data) {
				break
			}

			b1 := data[j] - ' '
			b2 := data[j+1] - ' '
			b3 := data[j+2] - ' '
			b4 := data[j+3] - ' '

			result = append(result, (b1<<2)|(b2>>4))
			if len(result) < length {
				result = append(result, ((b2&0x0f)<<4)|(b3>>2))
			}
			if len(result) < length {
				result = append(result, ((b3&0x03)<<6)|b4)
			}
		}
	}

	return string(result)
}

// HTML Entity Encoding/Decoding (numeric entities)
func (i *Interpreter) builtinHTMLEntityEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, r := range s {
		if r < 128 && (unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r)) {
			result.WriteRune(r)
		} else {
			result.WriteString(fmt.Sprintf("&#%d;", r))
		}
	}
	return result.String()
}

func (i *Interpreter) builtinHTMLEntityDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Decode numeric entities
	re := regexp.MustCompile(`&#(\d+);|&#x([0-9a-fA-F]+);`)
	result := re.ReplaceAllStringFunc(s, func(match string) string {
		if strings.HasPrefix(match, "&#x") || strings.HasPrefix(match, "&#X") {
			// Hex entity
			hexStr := match[3 : len(match)-1]
			n, err := strconv.ParseInt(hexStr, 16, 32)
			if err != nil {
				return match
			}
			return string(rune(n))
		} else {
			// Decimal entity
			numStr := match[2 : len(match)-1]
			n, err := strconv.ParseInt(numStr, 10, 32)
			if err != nil {
				return match
			}
			return string(rune(n))
		}
	})
	return result
}

// Unicode Escape/Unescape
func (i *Interpreter) builtinUnicodeEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, r := range s {
		if r < 128 {
			result.WriteRune(r)
		} else {
			result.WriteString(fmt.Sprintf("\\u%04X", r))
		}
	}
	return result.String()
}

func (i *Interpreter) builtinUnicodeDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Decode \uXXXX format
	re := regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)
	result := re.ReplaceAllStringFunc(s, func(match string) string {
		hexStr := match[2:]
		n, err := strconv.ParseInt(hexStr, 16, 32)
		if err != nil {
			return match
		}
		return string(rune(n))
	})
	return result
}

// UTF-8 Encode/Decode (converts to/from byte array)
func (i *Interpreter) builtinUTF8Encode(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	data := []byte(s)
	result := make([]Value, len(data))
	for idx, b := range data {
		result[idx] = int64(b)
	}
	return result
}

func (i *Interpreter) builtinUTF8Decode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var data []byte
	switch v := args[0].(type) {
	case []Value:
		data = make([]byte, len(v))
		for idx, val := range v {
			data[idx] = byte(i.toInt(val))
		}
	case string:
		return v
	default:
		return ""
	}
	return string(data)
}

// Punycode Encoding/Decoding
func (i *Interpreter) builtinPunycodeEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Use idna for punycode
	result, err := idna.ToASCII(s)
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	return result
}

func (i *Interpreter) builtinPunycodeDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	result, err := idna.ToUnicode(s)
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	return result
}

// JavaScript Escape/Unescape
func (i *Interpreter) builtinJSEscape(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			result.WriteString("\\\\")
		case '"':
			result.WriteString("\\\"")
		case '\'':
			result.WriteString("\\'")
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\t':
			result.WriteString("\\t")
		case '\b':
			result.WriteString("\\b")
		case '\f':
			result.WriteString("\\f")
		default:
			if r < 32 {
				result.WriteString(fmt.Sprintf("\\x%02X", r))
			} else {
				result.WriteRune(r)
			}
		}
	}
	return result.String()
}

func (i *Interpreter) builtinJSUnescape(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Unescape JS escape sequences
	result := s
	replacements := []struct {
		from, to string
	}{
		{"\\\\", "\x00"},
		{"\\\"", "\""},
		{"\\'", "'"},
		{"\\n", "\n"},
		{"\\r", "\r"},
		{"\\t", "\t"},
		{"\\b", "\b"},
		{"\\f", "\f"},
		{"\x00", "\\"},
	}

	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.from, r.to)
	}

	// Handle \xNN
	hexRe := regexp.MustCompile(`\\x([0-9a-fA-F]{2})`)
	result = hexRe.ReplaceAllStringFunc(result, func(match string) string {
		n, err := strconv.ParseInt(match[2:], 16, 32)
		if err != nil {
			return match
		}
		return string(rune(n))
	})

	// Handle \uXXXX
	unicodeRe := regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)
	result = unicodeRe.ReplaceAllStringFunc(result, func(match string) string {
		n, err := strconv.ParseInt(match[2:], 16, 32)
		if err != nil {
			return match
		}
		return string(rune(n))
	})

	return result
}

// C String Escape/Unescape
func (i *Interpreter) builtinCEscape(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	result.WriteString("\"")
	for _, r := range s {
		switch r {
		case '\\':
			result.WriteString("\\\\")
		case '"':
			result.WriteString("\\\"")
		case '\a':
			result.WriteString("\\a")
		case '\b':
			result.WriteString("\\b")
		case '\f':
			result.WriteString("\\f")
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\t':
			result.WriteString("\\t")
		case '\v':
			result.WriteString("\\v")
		default:
			if r < 32 || r > 126 {
				result.WriteString(fmt.Sprintf("\\%03o", r))
			} else {
				result.WriteRune(r)
			}
		}
	}
	result.WriteString("\"")
	return result.String()
}

func (i *Interpreter) builtinCUnescape(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Remove surrounding quotes if present
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}

	var result strings.Builder
	idx := 0
	for idx < len(s) {
		if s[idx] == '\\' && idx+1 < len(s) {
			idx++
			switch s[idx] {
			case 'a':
				result.WriteRune('\a')
			case 'b':
				result.WriteRune('\b')
			case 'f':
				result.WriteRune('\f')
			case 'n':
				result.WriteRune('\n')
			case 'r':
				result.WriteRune('\r')
			case 't':
				result.WriteRune('\t')
			case 'v':
				result.WriteRune('\v')
			case '\\':
				result.WriteRune('\\')
			case '"':
				result.WriteRune('"')
			case '\'':
				result.WriteRune('\'')
			case '0', '1', '2', '3', '4', '5', '6', '7':
				// Octal escape
				n := 0
				for j := 0; j < 3 && idx < len(s) && s[idx] >= '0' && s[idx] <= '7'; j++ {
					n = n*8 + int(s[idx]-'0')
					idx++
				}
				result.WriteRune(rune(n))
				idx-- // Adjust for loop increment
			case 'x':
				// Hex escape
				if idx+2 < len(s) {
					hexStr := s[idx+1 : idx+3]
					n, err := strconv.ParseInt(hexStr, 16, 32)
					if err == nil {
						result.WriteRune(rune(n))
						idx += 2
					}
				}
			default:
				result.WriteByte(s[idx])
			}
		} else {
			result.WriteByte(s[idx])
		}
		idx++
	}
	return result.String()
}

// Binary Encoding/Decoding
func (i *Interpreter) builtinToBinary(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, b := range []byte(s) {
		if result.Len() > 0 {
			result.WriteString(" ")
		}
		result.WriteString(fmt.Sprintf("%08b", b))
	}
	return result.String()
}

func (i *Interpreter) builtinFromBinary(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Remove spaces
	s = strings.ReplaceAll(s, " ", "")

	var result []byte
	for i := 0; i+8 <= len(s); i += 8 {
		b, err := strconv.ParseUint(s[i:i+8], 2, 8)
		if err != nil {
			return ""
		}
		result = append(result, byte(b))
	}
	return string(result)
}

// Octal Encoding/Decoding
func (i *Interpreter) builtinToOctal(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, b := range []byte(s) {
		if result.Len() > 0 {
			result.WriteString(" ")
		}
		result.WriteString(fmt.Sprintf("%03o", b))
	}
	return result.String()
}

func (i *Interpreter) builtinFromOctal(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Remove spaces
	s = strings.ReplaceAll(s, " ", "")

	var result []byte
	for i := 0; i+3 <= len(s); i += 3 {
		b, err := strconv.ParseUint(s[i:i+3], 8, 8)
		if err != nil {
			return ""
		}
		result = append(result, byte(b))
	}
	return string(result)
}

// Morse Code
var morseCodeMap = map[rune]string{
	'A': ".-", 'B': "-...", 'C': "-.-.", 'D': "-..", 'E': ".",
	'F': "..-.", 'G': "--.", 'H': "....", 'I': "..", 'J': ".---",
	'K': "-.-", 'L': ".-..", 'M': "--", 'N': "-.", 'O': "---",
	'P': ".--.", 'Q': "--.-", 'R': ".-.", 'S': "...", 'T': "-",
	'U': "..-", 'V': "...-", 'W': ".--", 'X': "-..-", 'Y': "-.--",
	'Z': "--..", '0': "-----", '1': ".----", '2': "..---",
	'3': "...--", '4': "....-", '5': ".....", '6': "-....",
	'7': "--...", '8': "---..", '9': "----.", '.': ".-.-.-",
	',': "--..--", '?': "..--..", '\'': ".----.", '!': "-.-.--",
	'/': "-..-.", '(': "-.--.", ')': "-.--.-", '&': ".-...",
	':': "---...", ';': "-.-.-.", '=': "-...-", '+': ".-.-.",
	'-': "-....-", '_': "..--.-", '"': ".-..-.", '$': "...-..-",
	'@': ".--.-.", ' ': "/",
}

var reverseMorseMap map[string]rune

func init() {
	reverseMorseMap = make(map[string]rune)
	for k, v := range morseCodeMap {
		reverseMorseMap[v] = k
	}
}

func (i *Interpreter) builtinMorseEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for idx, r := range s {
		if idx > 0 {
			result.WriteString(" ")
		}
		upperR := unicode.ToUpper(r)
		if code, exists := morseCodeMap[upperR]; exists {
			result.WriteString(code)
		} else {
			result.WriteString(string(r))
		}
	}
	return result.String()
}

func (i *Interpreter) builtinMorseDecode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	codes := strings.Split(s, " ")
	var result strings.Builder
	for _, code := range codes {
		if r, exists := reverseMorseMap[code]; exists {
			result.WriteRune(r)
		} else if code == "" {
			// Skip empty codes
		} else {
			result.WriteString(code)
		}
	}
	return result.String()
}

// ASCII to Hex and vice versa
func (i *Interpreter) builtinASCIItoHex(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for _, r := range s {
		result.WriteString(fmt.Sprintf("%02X", r))
	}
	return result.String()
}

func (i *Interpreter) builtinHexToASCII(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var result strings.Builder
	for i := 0; i+2 <= len(s); i += 2 {
		hexStr := s[i : i+2]
		n, err := strconv.ParseInt(hexStr, 16, 32)
		if err != nil {
			continue
		}
		result.WriteRune(rune(n))
	}
	return result.String()
}

// String to Bytes and vice versa
func (i *Interpreter) builtinStrToBytes(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	data := []byte(s)
	result := make([]Value, len(data))
	for idx, b := range data {
		result[idx] = int64(b)
	}
	return result
}

func (i *Interpreter) builtinBytesToStr(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var data []byte
	switch v := args[0].(type) {
	case []Value:
		data = make([]byte, len(v))
		for idx, val := range v {
			data[idx] = byte(i.toInt(val))
		}
	case string:
		return v
	default:
		return ""
	}
	return string(data)
}

// Gzip Compression/Decompression
func (i *Interpreter) builtinGzipCompress(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	writer.Write([]byte(s))
	writer.Close()

	// Return as base64
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func (i *Interpreter) builtinGzipDecompress(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Decode base64
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return map[string]Value{"error": "invalid base64: " + err.Error(), "success": false}
	}

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	return string(result)
}

// Zlib Compression/Decompression
func (i *Interpreter) builtinZlibCompress(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	writer.Write([]byte(s))
	writer.Close()

	// Return as base64
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func (i *Interpreter) builtinZlibDecompress(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Decode base64
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return map[string]Value{"error": "invalid base64: " + err.Error(), "success": false}
	}

	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return map[string]Value{"error": err.Error(), "success": false}
	}
	return string(result)
}

// Validation functions
func (i *Interpreter) builtinIsBase64(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}

	// Check valid base64 characters
	for _, r := range s {
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '+' || r == '/' || r == '=') {
			return false
		}
	}

	// Try to decode
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

func (i *Interpreter) builtinIsHex(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}

	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

func (i *Interpreter) builtinIsBase32(args []Value) Value {
	if len(args) == 0 {
		return false
	}
	s, ok := args[0].(string)
	if !ok || s == "" {
		return false
	}

	// Check valid base32 characters
	for _, r := range s {
		if !((r >= 'A' && r <= 'Z') || (r >= '2' && r <= '7') || r == '=') {
			return false
		}
	}

	_, err := base32.StdEncoding.DecodeString(s)
	return err == nil
}

// ============================================================================
// Data Format Functions
// ============================================================================

// CSV Stringify
func (i *Interpreter) builtinCSVStringify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	var buf strings.Builder
	writer := csv.NewWriter(&buf)

	// Handle different input types
	switch v := args[0].(type) {
	case []Value:
		// Check if it's array of arrays (rows) or array of values (single row)
		if len(v) > 0 {
			if _, ok := v[0].([]Value); ok {
				// Array of rows
				for _, row := range v {
					if rowArr, ok := row.([]Value); ok {
						record := make([]string, len(rowArr))
						for idx, cell := range rowArr {
							record[idx] = fmt.Sprintf("%v", cell)
						}
						writer.Write(record)
					}
				}
			} else {
				// Single row
				record := make([]string, len(v))
				for idx, cell := range v {
					record[idx] = fmt.Sprintf("%v", cell)
				}
				writer.Write(record)
			}
		}
	case map[string]Value:
		// Single object - write header and values
		keys := make([]string, 0, len(v))
		values := make([]string, 0, len(v))
		for k, val := range v {
			keys = append(keys, k)
			values = append(values, fmt.Sprintf("%v", val))
		}
		writer.Write(keys)
		writer.Write(values)
	}

	writer.Flush()
	return buf.String()
}

// XML Parse - simple XML to map
func (i *Interpreter) builtinXMLParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "input must be string"}
	}

	decoder := xml.NewDecoder(strings.NewReader(s))
	var result map[string]Value = make(map[string]Value)

	// Simple parsing - extract root element content
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}

		switch t := token.(type) {
		case xml.StartElement:
			// Parse element content
			var content string
			if err := decoder.DecodeElement(&content, &t); err == nil {
				result[t.Name.Local] = content
			}
		}
	}

	return result
}

// XML Stringify - map to XML
func (i *Interpreter) builtinXMLStringify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	data, ok := args[0].(map[string]Value)
	if !ok {
		return ""
	}

	root := "root"
	if len(args) > 1 {
		if r, ok := args[1].(string); ok {
			root = r
		}
	}

	var buf strings.Builder
	buf.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	buf.WriteString("<" + root + ">\n")

	for k, v := range data {
		buf.WriteString(fmt.Sprintf("  <%s>%v</%s>\n", k, escapeXML(fmt.Sprintf("%v", v)), k))
	}

	buf.WriteString("</" + root + ">")
	return buf.String()
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

// XML Get - extract value from XML path
func (i *Interpreter) builtinXMLGet(args []Value) Value {
	if len(args) < 2 {
		return nil
	}

	xmlStr, ok := args[0].(string)
	if !ok {
		return nil
	}

	path, ok := args[1].(string)
	if !ok {
		return nil
	}

	// Simple regex-based extraction
	pattern := fmt.Sprintf(`<%s[^>]*>(.*?)</%s>`, path, path)
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(xmlStr)
	if len(matches) > 1 {
		return matches[1]
	}
	return nil
}

// YAML Parse
func (i *Interpreter) builtinYAMLParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "input must be string"}
	}

	var result interface{}
	if err := yaml.Unmarshal([]byte(s), &result); err != nil {
		return map[string]Value{"error": err.Error()}
	}

	return i.convertToValue(result)
}

// YAML Stringify
func (i *Interpreter) builtinYAMLStringify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	data := args[0]
	goData := i.valueToGo(data)

	result, err := yaml.Marshal(goData)
	if err != nil {
		return ""
	}
	return string(result)
}

// TOML Parse
func (i *Interpreter) builtinTOMLParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{}
	}
	s, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "input must be string"}
	}

	var result map[string]interface{}
	if _, err := toml.Decode(s, &result); err != nil {
		return map[string]Value{"error": err.Error()}
	}

	return i.convertToValue(result).(map[string]Value)
}

// TOML Stringify
func (i *Interpreter) builtinTOMLStringify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	data, ok := args[0].(map[string]Value)
	if !ok {
		return ""
	}

	goData := i.valueToGo(data).(map[string]interface{})

	var buf strings.Builder
	if err := toml.NewEncoder(&buf).Encode(goData); err != nil {
		return ""
	}
	return buf.String()
}

// Markdown to HTML
func (i *Interpreter) builtinMarkdownToHTML(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Simple markdown to HTML conversion
	result := s

	// Headers
	result = regexp.MustCompile(`(?m)^### (.+)$`).ReplaceAllString(result, "<h3>$1</h3>")
	result = regexp.MustCompile(`(?m)^## (.+)$`).ReplaceAllString(result, "<h2>$1</h2>")
	result = regexp.MustCompile(`(?m)^# (.+)$`).ReplaceAllString(result, "<h1>$1</h1>")

	// Bold and Italic
	result = regexp.MustCompile(`\*\*\*(.+?)\*\*\*`).ReplaceAllString(result, "<strong><em>$1</em></strong>")
	result = regexp.MustCompile(`\*\*(.+?)\*\*`).ReplaceAllString(result, "<strong>$1</strong>")
	result = regexp.MustCompile(`\*(.+?)\*`).ReplaceAllString(result, "<em>$1</em>")
	result = regexp.MustCompile(`__(.+?)__`).ReplaceAllString(result, "<strong>$1</strong>")
	result = regexp.MustCompile(`_(.+?)_`).ReplaceAllString(result, "<em>$1</em>")

	// Code
	result = regexp.MustCompile("```(.+?)```").ReplaceAllString(result, "<pre><code>$1</code></pre>")
	result = regexp.MustCompile("`(.+?)`").ReplaceAllString(result, "<code>$1</code>")

	// Links
	result = regexp.MustCompile(`\[(.+?)\]\((.+?)\)`).ReplaceAllString(result, `<a href="$2">$1</a>`)

	// Line breaks
	result = regexp.MustCompile(`\n\n`).ReplaceAllString(result, "</p><p>")
	result = regexp.MustCompile(`\n`).ReplaceAllString(result, "<br>")

	return "<p>" + result + "</p>"
}

// HTML to Markdown
func (i *Interpreter) builtinHTMLToMarkdown(args []Value) Value {
	if len(args) == 0 {
		return ""
	}
	s, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Simple HTML to Markdown conversion
	result := s

	// Remove HTML tags and convert to markdown
	result = regexp.MustCompile(`<h1[^>]*>(.+?)</h1>`).ReplaceAllString(result, "# $1\n")
	result = regexp.MustCompile(`<h2[^>]*>(.+?)</h2>`).ReplaceAllString(result, "## $1\n")
	result = regexp.MustCompile(`<h3[^>]*>(.+?)</h3>`).ReplaceAllString(result, "### $1\n")
	result = regexp.MustCompile(`<strong[^>]*>(.+?)</strong>`).ReplaceAllString(result, "**$1**")
	result = regexp.MustCompile(`<b[^>]*>(.+?)</b>`).ReplaceAllString(result, "**$1**")
	result = regexp.MustCompile(`<em[^>]*>(.+?)</em>`).ReplaceAllString(result, "*$1*")
	result = regexp.MustCompile(`<i[^>]*>(.+?)</i>`).ReplaceAllString(result, "*$1*")
	result = regexp.MustCompile(`<code[^>]*>(.+?)</code>`).ReplaceAllString(result, "`$1`")
	result = regexp.MustCompile(`<a[^>]*href="([^"]+)"[^>]*>(.+?)</a>`).ReplaceAllString(result, "[$2]($1)")
	result = regexp.MustCompile(`<br\s*/?>`).ReplaceAllString(result, "\n")
	result = regexp.MustCompile(`<p[^>]*>(.+?)</p>`).ReplaceAllString(result, "$1\n\n")
	result = regexp.MustCompile(`<[^>]+>`).ReplaceAllString(result, "")

	return result
}

// ============================================================================
// Network Extended Functions
// ============================================================================

// HTTP Download
func (i *Interpreter) builtinHTTPDownload(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "need url and filepath"}
	}

	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "url must be string"}
	}

	filepath, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "filepath must be string"}
	}

	// Create HTTP client with timeout
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return map[string]Value{"success": false, "error": fmt.Sprintf("HTTP %d", resp.StatusCode)}
	}

	// Create file
	out, err := os.Create(filepath)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer out.Close()

	// Write body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{
		"success":  true,
		"url":      url,
		"filepath": filepath,
		"size":     resp.ContentLength,
	}
}

// HTTP Upload
func (i *Interpreter) builtinHTTPUpload(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "need url and filepath"}
	}

	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "url must be string"}
	}

	filepath, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "filepath must be string"}
	}

	// Read file
	file, err := os.Open(filepath)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer file.Close()

	// Create request
	req, err := http.NewRequest("POST", url, file)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	// Set content type
	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	return map[string]Value{
		"success":      true,
		"status":       resp.StatusCode,
		"responseBody": string(body),
	}
}

// HTTP Head
func (i *Interpreter) builtinHTTPHead(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need url"}
	}

	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "url must be string"}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Head(url)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}
	defer resp.Body.Close()

	headers := make(map[string]Value)
	for k, v := range resp.Header {
		if len(v) == 1 {
			headers[k] = v[0]
		} else {
			arr := make([]Value, len(v))
			for idx, s := range v {
				arr[idx] = s
			}
			headers[k] = arr
		}
	}

	return map[string]Value{
		"status":      resp.StatusCode,
		"headers":     headers,
		"contentSize": resp.ContentLength,
	}
}

// HTTP Patch
func (i *Interpreter) builtinHTTPPatch(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need url and body"}
	}

	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "url must be string"}
	}

	bodyData := args[1]
	var bodyReader io.Reader

	switch v := bodyData.(type) {
	case string:
		bodyReader = strings.NewReader(v)
	case map[string]Value:
		jsonData, _ := json.Marshal(v)
		bodyReader = bytes.NewReader(jsonData)
	default:
		bodyReader = strings.NewReader(fmt.Sprintf("%v", v))
	}

	req, err := http.NewRequest("PATCH", url, bodyReader)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	return map[string]Value{
		"status":       resp.StatusCode,
		"responseBody": string(body),
	}
}

// HTTP Options
func (i *Interpreter) builtinHTTPOptions(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need url"}
	}

	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "url must be string"}
	}

	req, err := http.NewRequest("OPTIONS", url, nil)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}
	defer resp.Body.Close()

	allow := resp.Header.Get("Allow")
	methods := strings.Split(allow, ",")
	result := make([]Value, len(methods))
	for idx, m := range methods {
		result[idx] = strings.TrimSpace(m)
	}

	return map[string]Value{
		"status":  resp.StatusCode,
		"methods": result,
		"allow":   allow,
	}
}

// Ping - TCP connectivity test
func (i *Interpreter) builtinPing(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "need host"}
	}

	host, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "host must be string"}
	}

	port := 80
	if len(args) > 1 {
		port = int(i.toInt(args[1]))
	}

	timeout := 5 * time.Second
	if len(args) > 2 {
		timeout = time.Duration(i.toInt(args[2])) * time.Second
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), timeout)
	elapsed := time.Since(start)

	if err != nil {
		return map[string]Value{
			"success": false,
			"error":   err.Error(),
			"host":    host,
			"port":    port,
		}
	}
	conn.Close()

	return map[string]Value{
		"success":  true,
		"host":     host,
		"port":     port,
		"latency":  elapsed.Milliseconds(),
		"latencyMs": elapsed.Milliseconds(),
	}
}

// Port Check
func (i *Interpreter) builtinPortCheck(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need host and port"}
	}

	host, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "host must be string"}
	}

	port := int(i.toInt(args[1]))
	timeout := 3 * time.Second
	if len(args) > 2 {
		timeout = time.Duration(i.toInt(args[2])) * time.Second
	}

	address := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return map[string]Value{
			"open":  false,
			"host":  host,
			"port":  port,
			"error": err.Error(),
		}
	}
	conn.Close()

	return map[string]Value{
		"open": true,
		"host": host,
		"port": port,
	}
}

// Port Scan
func (i *Interpreter) builtinPortScan(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	host, ok := args[0].(string)
	if !ok {
		return []Value{}
	}

	startPort := 1
	endPort := 1024
	if len(args) > 1 {
		startPort = int(i.toInt(args[1]))
	}
	if len(args) > 2 {
		endPort = int(i.toInt(args[2]))
	}

	var openPorts []Value
	timeout := 500 * time.Millisecond

	for port := startPort; port <= endPort && port <= 65535; port++ {
		address := fmt.Sprintf("%s:%d", host, port)
		conn, err := net.DialTimeout("tcp", address, timeout)
		if err == nil {
			conn.Close()
			openPorts = append(openPorts, int64(port))
		}
	}

	return openPorts
}

// ============================================================================
// Concurrency Functions
// ============================================================================

// Retry
func (i *Interpreter) builtinRetry(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"error": "need function, maxAttempts, delay"}
	}

	// Get function name
	funcName, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "first arg must be function name"}
	}

	maxAttempts := 3
	if len(args) > 1 {
		maxAttempts = int(i.toInt(args[1]))
	}

	delay := 1000 // ms
	if len(args) > 2 {
		delay = int(i.toInt(args[2]))
	}

	// Get user function
	userFunc, ok := i.ctx.Functions[funcName]
	if !ok {
		return map[string]Value{"error": "function not found"}
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Call the function
		result, err := i.callUserFunc(userFunc, []Value{})
		if err == nil {
			return map[string]Value{
				"success":  true,
				"result":   result,
				"attempts": attempt,
			}
		}
		lastErr = err

		if attempt < maxAttempts {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}

	return map[string]Value{
		"success": false,
		"error":   lastErr.Error(),
		"attempts": maxAttempts,
	}
}

// Parallel - execute multiple operations (simplified)
func (i *Interpreter) builtinParallel(args []Value) Value {
	if len(args) == 0 {
		return []Value{}
	}

	// For simplicity, just execute sequentially with goroutine-like behavior
	// Real parallel execution would need more complex handling
	results := make([]Value, len(args))

	for idx, arg := range args {
		if funcName, ok := arg.(string); ok {
			if userFunc, exists := i.ctx.Functions[funcName]; exists {
				result, _ := i.callUserFunc(userFunc, []Value{})
				results[idx] = result
			}
		}
	}

	return results
}

// Timeout
func (i *Interpreter) builtinTimeout(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need function and timeout"}
	}

	funcName, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "first arg must be function name"}
	}

	timeoutMs := i.toInt(args[1])

	userFunc, ok := i.ctx.Functions[funcName]
	if !ok {
		return map[string]Value{"error": "function not found"}
	}

	done := make(chan Value, 1)
	go func() {
		result, _ := i.callUserFunc(userFunc, []Value{})
		done <- result
	}()

	select {
	case result := <-done:
		return result
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		return map[string]Value{"error": "timeout", "timedOut": true}
	}
}

// ============================================================================
// Random Generation Functions
// ============================================================================

// Random Password
func (i *Interpreter) builtinRandomPassword(args []Value) Value {
	length := 16
	if len(args) > 0 {
		length = int(i.toInt(args[0]))
	}

	// Character sets
	lower := "abcdefghijklmnopqrstuvwxyz"
	upper := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digits := "0123456789"
	special := "!@#$%^&*()_+-=[]{}|;:,.<>?"

	// Default: use all character sets
	chars := lower + upper + digits + special
	if len(args) > 1 {
		if flags, ok := args[1].(string); ok {
			chars = ""
			if strings.Contains(flags, "l") {
				chars += lower
			}
			if strings.Contains(flags, "u") {
				chars += upper
			}
			if strings.Contains(flags, "d") {
				chars += digits
			}
			if strings.Contains(flags, "s") {
				chars += special
			}
			if chars == "" {
				chars = lower + upper + digits
			}
		}
	}

	result := make([]byte, length)
	rand.Read(result)
	for idx := range result {
		result[idx] = chars[int(result[idx])%len(chars)]
	}

	return string(result)
}

// Random Color
func (i *Interpreter) builtinRandomColor(args []Value) Value {
	format := "hex"
	if len(args) > 0 {
		if f, ok := args[0].(string); ok {
			format = f
		}
	}

	r := make([]byte, 3)
	rand.Read(r)

	switch format {
	case "rgb":
		return fmt.Sprintf("rgb(%d, %d, %d)", r[0], r[1], r[2])
	case "rgba":
		alpha := float64(r[0]) / 255.0
		return fmt.Sprintf("rgba(%d, %d, %d, %.2f)", r[0], r[1], r[2], alpha)
	case "hsl":
		h := int(r[0]) * 360 / 256
		s := 50 + int(r[1])%50
		l := 40 + int(r[2])%30
		return fmt.Sprintf("hsl(%d, %d%%, %d%%)", h, s, l)
	default: // hex
		return fmt.Sprintf("#%02X%02X%02X", r[0], r[1], r[2])
	}
}

// Random Name
func (i *Interpreter) builtinRandomName(args []Value) Value {
	firstNames := []string{"James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles",
		"Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen"}

	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
		"Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"}

	style := "full"
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			style = s
		}
	}

	r := make([]byte, 4)
	rand.Read(r)

	first := firstNames[int(r[0])%len(firstNames)]
	last := lastNames[int(r[1])%len(lastNames)]

	switch style {
	case "first":
		return first
	case "last":
		return last
	case "username":
		return strings.ToLower(first + strings.ReplaceAll(last, " ", ""))
	case "initials":
		return string(first[0]) + string(last[0])
	default: // full
		return first + " " + last
	}
}

// Random Token
func (i *Interpreter) builtinRandomToken(args []Value) Value {
	length := 32
	if len(args) > 0 {
		length = int(i.toInt(args[0]))
	}

	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	r := make([]byte, length)
	rand.Read(r)

	for idx := range result {
		result[idx] = chars[int(r[idx])%len(chars)]
	}

	return string(result)
}

// ============================================================================
// QR Code Functions
// ============================================================================

// QR Encode - returns QR code as string representation
func (i *Interpreter) builtinQREncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	content, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Create QR code
	qr, err := qrcode.New(content, qrcode.Medium)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	// Return as string (ASCII art)
	return qr.ToString(false)
}

// QR Data URL - returns QR code as data URL
func (i *Interpreter) builtinQRDataURL(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	content, ok := args[0].(string)
	if !ok {
		return ""
	}

	size := 256
	if len(args) > 1 {
		size = int(i.toInt(args[1]))
	}

	// Create QR code
	qr, err := qrcode.New(content, qrcode.Medium)
	if err != nil {
		return ""
	}

	// Generate PNG
	var buf bytes.Buffer
	if err := qr.Write(size, &buf); err != nil {
		return ""
	}

	// Convert to base64 data URL
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes())
}

// ============================================================================
// Cache Functions
// ============================================================================

// Simple in-memory cache
var globalCache = make(map[string]cachedItem)

type cachedItem struct {
	value      Value
	expiration time.Time
}

// Cache Set
func (i *Interpreter) builtinCacheSet(args []Value) Value {
	if len(args) < 2 {
		return false
	}

	key, ok := args[0].(string)
	if !ok {
		return false
	}

	value := args[1]
	expiration := time.Time{}

	if len(args) > 2 {
		ttl := i.toInt(args[2])
		expiration = time.Now().Add(time.Duration(ttl) * time.Second)
	}

	globalCache[key] = cachedItem{
		value:      value,
		expiration: expiration,
	}

	return true
}

// Cache Get
func (i *Interpreter) builtinCacheGet(args []Value) Value {
	if len(args) == 0 {
		return nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil
	}

	item, exists := globalCache[key]
	if !exists {
		return nil
	}

	// Check expiration
	if !item.expiration.IsZero() && time.Now().After(item.expiration) {
		delete(globalCache, key)
		return nil
	}

	return item.value
}

// Cache Delete
func (i *Interpreter) builtinCacheDel(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	key, ok := args[0].(string)
	if !ok {
		return false
	}

	delete(globalCache, key)
	return true
}

// Cache Has
func (i *Interpreter) builtinCacheHas(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	key, ok := args[0].(string)
	if !ok {
		return false
	}

	item, exists := globalCache[key]
	if !exists {
		return false
	}

	// Check expiration
	if !item.expiration.IsZero() && time.Now().After(item.expiration) {
		delete(globalCache, key)
		return false
	}

	return true
}

// Cache Clear
func (i *Interpreter) builtinCacheClear(args []Value) Value {
	globalCache = make(map[string]cachedItem)
	return true
}

// Cache Keys
func (i *Interpreter) builtinCacheKeys(args []Value) Value {
	now := time.Now()
	result := []Value{}

	for k, item := range globalCache {
		// Skip expired items
		if !item.expiration.IsZero() && now.After(item.expiration) {
			continue
		}
		result = append(result, k)
	}

	return result
}

// ============================================================================
// Helper Functions
// ============================================================================

func (i *Interpreter) convertToValue(v interface{}) Value {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case int:
		return int64(val)
	case int64:
		return val
	case float64:
		return val
	case string:
		return val
	case []interface{}:
		result := make([]Value, len(val))
		for idx, item := range val {
			result[idx] = i.convertToValue(item)
		}
		return result
	case map[string]interface{}:
		result := make(map[string]Value)
		for k, v := range val {
			result[k] = i.convertToValue(v)
		}
		return result
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (i *Interpreter) valueToGo(v Value) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case int64:
		return val
	case float64:
		return val
	case string:
		return val
	case []Value:
		result := make([]interface{}, len(val))
		for idx, item := range val {
			result[idx] = i.valueToGo(item)
		}
		return result
	case map[string]Value:
		result := make(map[string]interface{})
		for k, v := range val {
			result[k] = i.valueToGo(v)
		}
		return result
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ============================================================================
// Security Enhancement Functions
// ============================================================================

// JWT Sign - create a simple JWT token
func (i *Interpreter) builtinJWTSign(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need payload and secret"}
	}

	payload, ok := args[0].(map[string]Value)
	if !ok {
		return map[string]Value{"error": "payload must be an object"}
	}

	secret, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "secret must be string"}
	}

	// Create header
	header := `{"alg":"HS256","typ":"JWT"}`
	headerB64 := base64.RawURLEncoding.EncodeToString([]byte(header))

	// Create payload
	payloadJSON, _ := json.Marshal(i.valueToGo(payload))
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	// Create signature
	signingInput := headerB64 + "." + payloadB64
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(signingInput))
	signature := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

	return signingInput + "." + signature
}

// JWT Verify - verify a JWT token
func (i *Interpreter) builtinJWTVerify(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"valid": false, "error": "need token and secret"}
	}

	token, ok := args[0].(string)
	if !ok {
		return map[string]Value{"valid": false, "error": "token must be string"}
	}

	secret, ok := args[1].(string)
	if !ok {
		return map[string]Value{"valid": false, "error": "secret must be string"}
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return map[string]Value{"valid": false, "error": "invalid token format"}
	}

	// Verify signature
	signingInput := parts[0] + "." + parts[1]
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(signingInput))
	expectedSig := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

	if parts[2] != expectedSig {
		return map[string]Value{"valid": false, "error": "invalid signature"}
	}

	// Decode payload
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return map[string]Value{"valid": false, "error": "invalid payload encoding"}
	}

	var payload interface{}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return map[string]Value{"valid": false, "error": "invalid payload JSON"}
	}

	return map[string]Value{
		"valid":   true,
		"header":  parts[0],
		"payload": i.convertToValue(payload),
	}
}

// Hash Password - using bcrypt
func (i *Interpreter) builtinHashPassword(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need password"}
	}

	password, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "password must be string"}
	}

	cost := bcrypt.DefaultCost
	if len(args) > 1 {
		cost = int(i.toInt(args[1]))
		if cost < bcrypt.MinCost {
			cost = bcrypt.MinCost
		}
		if cost > bcrypt.MaxCost {
			cost = bcrypt.MaxCost
		}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	return string(hash)
}

// Verify Password - using bcrypt
func (i *Interpreter) builtinVerifyPassword(args []Value) Value {
	if len(args) < 2 {
		return false
	}

	password, ok := args[0].(string)
	if !ok {
		return false
	}

	hash, ok := args[1].(string)
	if !ok {
		return false
	}

	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// Generate Secret
func (i *Interpreter) builtinGenerateSecret(args []Value) Value {
	length := 32
	if len(args) > 0 {
		length = int(i.toInt(args[0]))
	}

	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}

	encoding := "hex"
	if len(args) > 1 {
		if e, ok := args[1].(string); ok {
			encoding = e
		}
	}

	switch encoding {
	case "base64":
		return base64.StdEncoding.EncodeToString(bytes)
	case "base64url":
		return base64.RawURLEncoding.EncodeToString(bytes)
	default:
		return hex.EncodeToString(bytes)
	}
}

// Encrypt AES
func (i *Interpreter) builtinEncryptAES(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need plaintext and key"}
	}

	plaintext, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "plaintext must be string"}
	}

	key, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "key must be string"}
	}

	// Derive 32-byte key from input
	keyHash := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	// Generate nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return map[string]Value{"error": err.Error()}
	}

	// Encrypt
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(ciphertext)
}

// Decrypt AES
func (i *Interpreter) builtinDecryptAES(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need ciphertext and key"}
	}

	ciphertextB64, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "ciphertext must be string"}
	}

	key, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "key must be string"}
	}

	// Decode base64
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return map[string]Value{"error": "invalid base64"}
	}

	// Derive 32-byte key
	keyHash := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return map[string]Value{"error": "ciphertext too short"}
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return map[string]Value{"error": "decryption failed"}
	}

	return string(plaintext)
}

// ============================================================================
// Validation and Sanitization Functions
// ============================================================================

// Validate - generic data validation
func (i *Interpreter) builtinValidate(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"valid": false, "error": "need value and rules"}
	}

	value := args[0]
	rules, ok := args[1].(map[string]Value)
	if !ok {
		return map[string]Value{"valid": false, "error": "rules must be an object"}
	}

	errors := []Value{}

	// Check required
	if req, exists := rules["required"]; exists {
		if i.toBool(req) && value == nil {
			errors = append(errors, "value is required")
		}
	}

	// Check type
	if typeRule, exists := rules["type"]; exists {
		typeStr, _ := typeRule.(string)
		switch typeStr {
		case "string":
			if _, ok := value.(string); !ok {
				errors = append(errors, "value must be string")
			}
		case "number":
			if _, ok := value.(float64); !ok {
				if _, ok := value.(int64); !ok {
					errors = append(errors, "value must be number")
				}
			}
		case "boolean", "bool":
			if _, ok := value.(bool); !ok {
				errors = append(errors, "value must be boolean")
			}
		case "array":
			if _, ok := value.([]Value); !ok {
				errors = append(errors, "value must be array")
			}
		case "object", "map":
			if _, ok := value.(map[string]Value); !ok {
				errors = append(errors, "value must be object")
			}
		}
	}

	// Check min
	if min, exists := rules["min"]; exists {
		if num, ok := value.(float64); ok {
			if num < i.toFloat(min) {
				errors = append(errors, fmt.Sprintf("value must be >= %v", min))
			}
		} else if str, ok := value.(string); ok {
			if len(str) < int(i.toInt(min)) {
				errors = append(errors, fmt.Sprintf("string length must be >= %v", min))
			}
		}
	}

	// Check max
	if max, exists := rules["max"]; exists {
		if num, ok := value.(float64); ok {
			if num > i.toFloat(max) {
				errors = append(errors, fmt.Sprintf("value must be <= %v", max))
			}
		} else if str, ok := value.(string); ok {
			if len(str) > int(i.toInt(max)) {
				errors = append(errors, fmt.Sprintf("string length must be <= %v", max))
			}
		}
	}

	// Check pattern (regex)
	if pattern, exists := rules["pattern"]; exists {
		if patternStr, ok := pattern.(string); ok {
			if str, ok := value.(string); ok {
				matched, err := regexp.MatchString(patternStr, str)
				if err != nil || !matched {
					errors = append(errors, fmt.Sprintf("value does not match pattern: %s", patternStr))
				}
			}
		}
	}

	return map[string]Value{
		"valid":  len(errors) == 0,
		"errors": errors,
	}
}

// Sanitize - clean data
func (i *Interpreter) builtinSanitize(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	s, ok := args[0].(string)
	if !ok {
		return args[0]
	}

	// HTML escape
	s = html.EscapeString(s)

	// Remove control characters
	var result strings.Builder
	for _, r := range s {
		if r >= 32 || r == '\n' || r == '\r' || r == '\t' {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// Normalize Email
func (i *Interpreter) builtinNormalizeEmail(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	email, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Trim and lowercase
	email = strings.TrimSpace(strings.ToLower(email))

	// Remove dots in Gmail addresses
	parts := strings.Split(email, "@")
	if len(parts) == 2 {
		if strings.HasSuffix(parts[1], "gmail.com") {
			parts[0] = strings.ReplaceAll(parts[0], ".", "")
			email = parts[0] + "@" + parts[1]
		}
	}

	return email
}

// Normalize Phone
func (i *Interpreter) builtinNormalizePhone(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	phone, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Remove non-digits
	var result strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			result.WriteRune(r)
		}
	}

	digits := result.String()

	// Default country code
	countryCode := "1"
	if len(args) > 1 {
		countryCode, _ = args[1].(string)
	}

	// Add country code if missing
	if len(digits) == 10 {
		digits = countryCode + digits
	}

	return "+" + digits
}

// Validate Password Strength
func (i *Interpreter) builtinValidatePassword(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"valid": false, "score": 0}
	}

	password, ok := args[0].(string)
	if !ok {
		return map[string]Value{"valid": false, "score": 0}
	}

	minLength := 8
	if len(args) > 1 {
		minLength = int(i.toInt(args[1]))
	}

	score := 0
	feedback := []Value{}

	// Length check
	if len(password) >= minLength {
		score++
	} else {
		feedback = append(feedback, fmt.Sprintf("Password must be at least %d characters", minLength))
	}

	// Uppercase check
	if regexp.MustCompile(`[A-Z]`).MatchString(password) {
		score++
	} else {
		feedback = append(feedback, "Add uppercase letter")
	}

	// Lowercase check
	if regexp.MustCompile(`[a-z]`).MatchString(password) {
		score++
	} else {
		feedback = append(feedback, "Add lowercase letter")
	}

	// Number check
	if regexp.MustCompile(`[0-9]`).MatchString(password) {
		score++
	} else {
		feedback = append(feedback, "Add number")
	}

	// Special character check
	if regexp.MustCompile(`[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>/?]`).MatchString(password) {
		score++
	} else {
		feedback = append(feedback, "Add special character")
	}

	// Strength rating
	var strength string
	switch score {
	case 5:
		strength = "very strong"
	case 4:
		strength = "strong"
	case 3:
		strength = "medium"
	case 2:
		strength = "weak"
	default:
		strength = "very weak"
	}

	return map[string]Value{
		"valid":    score >= 3 && len(password) >= minLength,
		"score":    score,
		"strength": strength,
		"feedback": feedback,
	}
}

// ============================================================================
// Template and Rendering Functions
// ============================================================================

// Render Template - simple template rendering
func (i *Interpreter) builtinRenderTemplate(args []Value) Value {
	if len(args) < 2 {
		return ""
	}

	tmpl, ok := args[0].(string)
	if !ok {
		return ""
	}

	data, ok := args[1].(map[string]Value)
	if !ok {
		return tmpl
	}

	// Simple {{variable}} replacement
	re := regexp.MustCompile(`\{\{(\w+)\}\}`)
	result := re.ReplaceAllStringFunc(tmpl, func(match string) string {
		key := match[2 : len(match)-2]
		if val, exists := data[key]; exists {
			return fmt.Sprintf("%v", val)
		}
		return match
	})

	return result
}

// Minify - minify code
func (i *Interpreter) builtinMinify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	code, ok := args[0].(string)
	if !ok {
		return ""
	}

	lang := "auto"
	if len(args) > 1 {
		lang, _ = args[1].(string)
	}

	// Simple minification: remove extra whitespace
	switch lang {
	case "json":
		// Compact JSON
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(code)); err == nil {
			return compact.String()
		}
	case "html":
		// Remove comments and extra whitespace
		code = regexp.MustCompile(`<!--.*?-->`).ReplaceAllString(code, "")
		code = regexp.MustCompile(`\s+`).ReplaceAllString(code, " ")
		code = regexp.MustCompile(`>\s+<`).ReplaceAllString(code, "><")
		return strings.TrimSpace(code)
	default:
		// Generic minification
		code = regexp.MustCompile(`\s+`).ReplaceAllString(code, " ")
		return strings.TrimSpace(code)
	}

	return code
}

// Beautify - format code
func (i *Interpreter) builtinBeautify(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	code, ok := args[0].(string)
	if !ok {
		return ""
	}

	lang := "auto"
	if len(args) > 1 {
		lang, _ = args[1].(string)
	}

	switch lang {
	case "json":
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, []byte(code), "", "  "); err == nil {
			return pretty.String()
		}
	case "xml":
		// Simple XML formatting
		code = regexp.MustCompile(`>`).ReplaceAllString(code, ">\n")
		code = regexp.MustCompile(`<`).ReplaceAllString(code, "\n<")
		code = regexp.MustCompile(`\n+`).ReplaceAllString(code, "\n")
		return code
	}

	return code
}

// ============================================================================
// Email and Notification Functions
// ============================================================================

// Send Email - requires SMTP config
func (i *Interpreter) builtinSendEmail(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"success": false, "error": "need to, subject, body"}
	}

	to, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "to must be string"}
	}

	subject, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "subject must be string"}
	}

	body, ok := args[2].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "body must be string"}
	}

	// This is a placeholder - actual email sending would require SMTP config
	// For now, just return success with the email details
	return map[string]Value{
		"success": true,
		"message": "Email would be sent (SMTP not configured)",
		"to":      to,
		"subject": subject,
		"length":  len(body),
	}
}

// Send Webhook
func (i *Interpreter) builtinSendWebhook(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "need url and payload"}
	}

	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "url must be string"}
	}

	payload := args[1]
	var bodyReader io.Reader

	switch v := payload.(type) {
	case string:
		bodyReader = strings.NewReader(v)
	case map[string]Value:
		jsonData, _ := json.Marshal(i.valueToGo(v))
		bodyReader = bytes.NewReader(jsonData)
	default:
		jsonData, _ := json.Marshal(payload)
		bodyReader = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	return map[string]Value{
		"success":    resp.StatusCode >= 200 && resp.StatusCode < 300,
		"statusCode": resp.StatusCode,
		"response":   string(respBody),
	}
}

// ============================================================================
// Random Generation Extended Functions
// ============================================================================

// Random Avatar URL
func (i *Interpreter) builtinRandomAvatar(args []Value) Value {
	style := "identicon"
	size := 200

	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			style = s
		}
	}
	if len(args) > 1 {
		size = int(i.toInt(args[1]))
	}

	// Generate random seed
	seed := make([]byte, 8)
	rand.Read(seed)
	seedHex := hex.EncodeToString(seed)

	switch style {
	case "robohash":
		return fmt.Sprintf("https://robohash.org/%s?size=%dx%d", seedHex, size, size)
	case "dicebear":
		return fmt.Sprintf("https://api.dicebear.com/7.x/avataaars/svg?seed=%s", seedHex)
	case "uiavatars":
		name := "User"
		if len(args) > 2 {
			name, _ = args[2].(string)
		}
		return fmt.Sprintf("https://ui-avatars.com/api/?name=%s&size=%d&background=random", url.QueryEscape(name), size)
	default:
		return fmt.Sprintf("https://www.gravatar.com/avatar/%s?d=identicon&s=%d", seedHex, size)
	}
}

// Generate Lorem Ipsum
func (i *Interpreter) builtinGenerateLorem(args []Value) Value {
	count := 5
	units := "paragraphs"

	if len(args) > 0 {
		count = int(i.toInt(args[0]))
	}
	if len(args) > 1 {
		units, _ = args[1].(string)
	}

	words := []string{
		"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
		"sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore",
		"magna", "aliqua", "enim", "ad", "minim", "veniam", "quis", "nostrud",
		"exercitation", "ullamco", "laboris", "nisi", "aliquip", "ex", "ea", "commodo",
	}

	generateSentence := func() string {
		n := 10 + int(mathrand.Intn(10))
		sentence := make([]string, n)
		for i := 0; i < n; i++ {
			sentence[i] = words[mathrand.Intn(len(words))]
		}
		sentence[0] = strings.Title(sentence[0])
		return strings.Join(sentence, " ") + "."
	}

	switch units {
	case "words":
		result := make([]string, count)
		for i := 0; i < count; i++ {
			result[i] = words[mathrand.Intn(len(words))]
		}
		return strings.Join(result, " ")
	case "sentences":
		result := make([]string, count)
		for i := 0; i < count; i++ {
			result[i] = generateSentence()
		}
		return strings.Join(result, " ")
	default: // paragraphs
		result := make([]string, count)
		for i := 0; i < count; i++ {
			sentences := 3 + mathrand.Intn(3)
			para := make([]string, sentences)
			for j := 0; j < sentences; j++ {
				para[j] = generateSentence()
			}
			result[i] = strings.Join(para, " ")
		}
		return strings.Join(result, "\n\n")
	}
}

// Faker - generate mock data
func (i *Interpreter) builtinFaker(args []Value) Value {
	fakerType := "name"
	if len(args) > 0 {
		fakerType, _ = args[0].(string)
	}

	r := make([]byte, 4)
	rand.Read(r)

	switch fakerType {
	case "name":
		names := []string{"John Smith", "Jane Doe", "Bob Wilson", "Alice Brown", "Charlie Davis", "Emma Johnson", "Michael Lee", "Sarah Miller"}
		return names[int(r[0])%len(names)]
	case "firstName":
		names := []string{"John", "Jane", "Bob", "Alice", "Charlie", "Emma", "Michael", "Sarah"}
		return names[int(r[0])%len(names)]
	case "lastName":
		names := []string{"Smith", "Doe", "Wilson", "Brown", "Davis", "Johnson", "Lee", "Miller"}
		return names[int(r[0])%len(names)]
	case "email":
		names := []string{"john", "jane", "bob", "alice", "charlie"}
		domains := []string{"example.com", "test.com", "demo.org", "sample.net"}
		return names[int(r[0])%len(names)] + "@" + domains[int(r[1])%len(domains)]
	case "phone":
		return fmt.Sprintf("+1-555-%03d-%04d", int(r[0])%1000, int(r[1])*256+int(r[2])%10000)
	case "address":
		num := int(r[0])%900 + 100
		streets := []string{"Main St", "Oak Ave", "Park Rd", "First St", "Second Ave"}
		cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"}
		return fmt.Sprintf("%d %s, %s", num, streets[int(r[1])%len(streets)], cities[int(r[2])%len(cities)])
	case "company":
		companies := []string{"Acme Corp", "Tech Solutions", "Global Industries", "Innovative Labs", "Future Systems"}
		return companies[int(r[0])%len(companies)]
	case "jobTitle":
		titles := []string{"Software Engineer", "Product Manager", "Data Analyst", "UX Designer", "Marketing Director"}
		return titles[int(r[0])%len(titles)]
	case "country":
		countries := []string{"United States", "Canada", "United Kingdom", "Germany", "France", "Japan", "Australia"}
		return countries[int(r[0])%len(countries)]
	case "city":
		cities := []string{"New York", "London", "Tokyo", "Paris", "Sydney", "Berlin", "Toronto"}
		return cities[int(r[0])%len(cities)]
	case "price":
		return float64(int(r[0])*100+int(r[1])) / 100
	case "product":
		products := []string{"Laptop", "Smartphone", "Tablet", "Headphones", "Camera", "Smart Watch"}
		return products[int(r[0])%len(products)]
	case "date":
		year := 2020 + int(r[0])%6
		month := 1 + int(r[1])%12
		day := 1 + int(r[2])%28
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	case "url":
		domains := []string{"example.com", "test.org", "demo.net"}
		paths := []string{"/home", "/about", "/products", "/contact"}
		return "https://" + domains[int(r[0])%len(domains)] + paths[int(r[1])%len(paths)]
	case "ipv4":
		return fmt.Sprintf("%d.%d.%d.%d", int(r[0])%256, int(r[1])%256, int(r[2])%256, int(r[3])%256)
	case "uuid":
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			uint32(r[0])<<24|uint32(r[1])<<16|uint32(r[2])<<8|uint32(r[3]),
			uint16(r[0])<<8|uint16(r[1]),
			0x4000|uint16(r[2])<<8|uint16(r[3]),
			0x8000|uint16(r[0])<<8|uint16(r[1]),
			uint64(r[0])<<40|uint64(r[1])<<32|uint64(r[2])<<24|uint64(r[3])<<16|uint64(r[0])<<8|uint64(r[1]))
	case "creditCard":
		return fmt.Sprintf("4%03d-%04d-%04d-%04d", int(r[0])%1000, int(r[1])*256+int(r[2]), int(r[2])*256+int(r[3]), int(r[3])*256+int(r[0]))
	default:
		return "unknown faker type"
	}
}

// ============================================================================
// Internationalization Functions
// ============================================================================

// Get Timezone
func (i *Interpreter) builtinGetTimezone(args []Value) Value {
	_, offset := time.Now().Zone()
	return map[string]Value{
		"name":   time.Now().Location().String(),
		"offset": offset / 3600,
	}
}

// Set Timezone (affects current context)
func (i *Interpreter) builtinSetTimezone(args []Value) Value {
	if len(args) == 0 {
		return false
	}

	tz, ok := args[0].(string)
	if !ok {
		return false
	}

	loc, err := time.LoadLocation(tz)
	if err != nil {
		return false
	}

	// This affects the interpreter context
	if i.ctx != nil {
		i.ctx.Timezone = loc
	}

	return true
}

// List Timezones
func (i *Interpreter) builtinListTimezones(args []Value) Value {
	// Common timezones
	timezones := []Value{
		"UTC",
		"America/New_York",
		"America/Chicago",
		"America/Denver",
		"America/Los_Angeles",
		"America/Toronto",
		"America/Vancouver",
		"Europe/London",
		"Europe/Paris",
		"Europe/Berlin",
		"Europe/Moscow",
		"Asia/Tokyo",
		"Asia/Shanghai",
		"Asia/Hong_Kong",
		"Asia/Singapore",
		"Asia/Dubai",
		"Australia/Sydney",
		"Australia/Melbourne",
		"Pacific/Auckland",
	}
	return timezones
}

// ============================================================================
// Image Processing Functions
// ============================================================================

// Image Info
func (i *Interpreter) builtinImageInfo(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need image path or base64"}
	}

	var data []byte

	switch v := args[0].(type) {
	case string:
		// Check if it's a file path or base64
		if strings.HasPrefix(v, "data:image") {
			// Base64 data URL
			parts := strings.SplitN(v, ",", 2)
			if len(parts) == 2 {
				decoded, err := base64.StdEncoding.DecodeString(parts[1])
				if err != nil {
					return map[string]Value{"error": "invalid base64"}
				}
				data = decoded
			}
		} else if _, err := os.Stat(v); err == nil {
			// File path
			fileData, err := os.ReadFile(v)
			if err != nil {
				return map[string]Value{"error": err.Error()}
			}
			data = fileData
		} else {
			// Assume base64 string
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return map[string]Value{"error": "invalid input"}
			}
			data = decoded
		}
	default:
		return map[string]Value{"error": "invalid input"}
	}

	// Detect image type
	imageType := "unknown"
	if len(data) > 4 {
		if data[0] == 0xFF && data[1] == 0xD8 {
			imageType = "jpeg"
		} else if data[0] == 0x89 && string(data[1:4]) == "PNG" {
			imageType = "png"
		} else if data[0] == 'G' && data[1] == 'I' && data[2] == 'F' {
			imageType = "gif"
		} else if data[0] == 'B' && data[1] == 'M' {
			imageType = "bmp"
		}
	}

	return map[string]Value{
		"type": imageType,
		"size": len(data),
	}
}

// Image to Base64
func (i *Interpreter) builtinImageToBase64(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	path, ok := args[0].(string)
	if !ok {
		return ""
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	// Detect type
	imageType := "png"
	if len(data) > 4 {
		if data[0] == 0xFF && data[1] == 0xD8 {
			imageType = "jpeg"
		} else if data[0] == 'G' && data[1] == 'I' && data[2] == 'F' {
			imageType = "gif"
		}
	}

	return fmt.Sprintf("data:image/%s;base64,%s", imageType, base64.StdEncoding.EncodeToString(data))
}

// Base64 to Image
func (i *Interpreter) builtinBase64ToImage(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need base64 and filepath"}
	}

	b64, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "base64 must be string"}
	}

	filepath, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "filepath must be string"}
	}

	// Remove data URL prefix if present
	if strings.Contains(b64, ",") {
		parts := strings.SplitN(b64, ",", 2)
		b64 = parts[1]
	}

	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return map[string]Value{"error": "invalid base64"}
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return map[string]Value{"error": err.Error()}
	}

	return map[string]Value{
		"success": true,
		"path":    filepath,
		"size":    len(data),
	}
}

// Barcode Encode
func (i *Interpreter) builtinBarcodeEncode(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	content, ok := args[0].(string)
	if !ok {
		return ""
	}

	format := "code128"
	if len(args) > 1 {
		format, _ = args[1].(string)
	}

	// Simple barcode representation (text-based)
	// Real implementation would use a barcode library
	var bars strings.Builder

	switch format {
	case "code39":
		// Code 39 pattern simulation
		bars.WriteString("*")
		for _, c := range content {
			bars.WriteRune(c)
			bars.WriteString(" ")
		}
		bars.WriteString("*")
	default: // code128
		// Simple representation
		for _, c := range content {
			bars.WriteString(fmt.Sprintf("|%d|", int(c)))
		}
	}

	return bars.String()
}

// ============================================================================
// Data Structures Functions
// ============================================================================

// New Stack
func (i *Interpreter) builtinNewStack(args []Value) Value {
	return map[string]Value{
		"type":  "stack",
		"items": []Value{},
	}
}

// New Queue
func (i *Interpreter) builtinNewQueue(args []Value) Value {
	return map[string]Value{
		"type":  "queue",
		"items": []Value{},
	}
}

// New Set
func (i *Interpreter) builtinNewSet(args []Value) Value {
	return map[string]Value{
		"type":    "set",
		"members": map[string]Value{},
	}
}

// ============================================================================
// Debug and Testing Functions
// ============================================================================

// Debug
func (i *Interpreter) builtinDebug(args []Value) Value {
	if len(args) == 0 {
		return nil
	}

	// Return debug info about the value
	v := args[0]
	return map[string]Value{
		"value": v,
		"type":  fmt.Sprintf("%T", v),
		"repr":  fmt.Sprintf("%#v", v),
	}
}

// Benchmark
func (i *Interpreter) builtinBenchmark(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need function and iterations"}
	}

	funcName, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "function name must be string"}
	}

	iterations := int(i.toInt(args[1]))

	userFunc, ok := i.ctx.Functions[funcName]
	if !ok {
		return map[string]Value{"error": "function not found"}
	}

	start := time.Now()
	for j := 0; j < iterations; j++ {
		i.callUserFunc(userFunc, []Value{})
	}
	elapsed := time.Since(start)

	return map[string]Value{
		"iterations":   iterations,
		"totalMs":      elapsed.Milliseconds(),
		"avgMs":        float64(elapsed.Milliseconds()) / float64(iterations),
		"opsPerSecond": float64(iterations) / elapsed.Seconds(),
	}
}

// Mock
func (i *Interpreter) builtinMock(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{}
	}

	mockType, ok := args[0].(string)
	if !ok {
		return map[string]Value{}
	}

	// Generate mock data based on type
	switch mockType {
	case "user":
		return map[string]Value{
			"id":    int64(1 + mathrand.Intn(1000)),
			"name":  i.builtinFaker([]Value{"name"}),
			"email": i.builtinFaker([]Value{"email"}),
			"age":   int64(18 + mathrand.Intn(60)),
		}
	case "product":
		return map[string]Value{
			"id":    int64(1 + mathrand.Intn(100)),
			"name":  i.builtinFaker([]Value{"product"}),
			"price": i.builtinFaker([]Value{"price"}),
		}
	case "order":
		return map[string]Value{
			"id":       i.builtinFaker([]Value{"uuid"}),
			"userId":   int64(1 + mathrand.Intn(100)),
			"status":   []string{"pending", "processing", "shipped", "delivered"}[mathrand.Intn(4)],
			"total":    float64(10 + mathrand.Intn(500)),
			"quantity": int64(1 + mathrand.Intn(10)),
		}
	default:
		return map[string]Value{"type": mockType}
	}
}

// ============================================================================
// Configuration Management Functions
// ============================================================================

// Load Config
func (i *Interpreter) builtinLoadConfig(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need filepath"}
	}

	path, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "filepath must be string"}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	// Detect format from extension
	ext := strings.ToLower(filepath.Ext(path))
	var result map[string]interface{}

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &result); err != nil {
			return map[string]Value{"error": err.Error()}
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &result); err != nil {
			return map[string]Value{"error": err.Error()}
		}
	case ".toml":
		if _, err := toml.Decode(string(data), &result); err != nil {
			return map[string]Value{"error": err.Error()}
		}
	default:
		// Try JSON first
		if err := json.Unmarshal(data, &result); err != nil {
			return map[string]Value{"error": "unknown format"}
		}
	}

	return i.convertToValue(result).(map[string]Value)
}

// Save Config
func (i *Interpreter) builtinSaveConfig(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"success": false, "error": "need config and filepath"}
	}

	config, ok := args[0].(map[string]Value)
	if !ok {
		return map[string]Value{"success": false, "error": "config must be object"}
	}

	path, ok := args[1].(string)
	if !ok {
		return map[string]Value{"success": false, "error": "filepath must be string"}
	}

	// Detect format from extension
	ext := strings.ToLower(filepath.Ext(path))
	var data []byte
	var err error

	goConfig := i.valueToGo(config).(map[string]interface{})

	switch ext {
	case ".json":
		data, err = json.MarshalIndent(goConfig, "", "  ")
	case ".yaml", ".yml":
		data, err = yaml.Marshal(goConfig)
	case ".toml":
		var buf strings.Builder
		err = toml.NewEncoder(&buf).Encode(goConfig)
		data = []byte(buf.String())
	default:
		data, err = json.MarshalIndent(goConfig, "", "  ")
	}

	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true, "path": path}
}

// Get Secret
func (i *Interpreter) builtinGetSecret(args []Value) Value {
	if len(args) == 0 {
		return ""
	}

	key, ok := args[0].(string)
	if !ok {
		return ""
	}

	// Check environment variable first
	if val := os.Getenv(key); val != "" {
		return val
	}

	// Check common secret locations
	secretPaths := []string{
		"/run/secrets/" + key,
		"/etc/secrets/" + key,
		"./secrets/" + key,
	}

	for _, path := range secretPaths {
		if data, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(data))
		}
	}

	return ""
}

// ============================================================================
// System Extended Functions
// ============================================================================

// Get Memory
func (i *Interpreter) builtinGetMemory(args []Value) Value {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]Value{
		"allocMB":      m.Alloc / 1024 / 1024,
		"totalAllocMB": m.TotalAlloc / 1024 / 1024,
		"sysMB":        m.Sys / 1024 / 1024,
		"numGC":        m.NumGC,
		"heapObjects":  m.HeapObjects,
	}
}

// Get CPU
func (i *Interpreter) builtinGetCPU(args []Value) Value {
	return map[string]Value{
		"numCPU":     runtime.NumCPU(),
		"goroutines": runtime.NumGoroutine(),
		"goVersion":  runtime.Version(),
		"compiler":   runtime.Compiler,
	}
}

// Kill Process
func (i *Interpreter) builtinKillProcess(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"success": false, "error": "need PID"}
	}

	pid := int(i.toInt(args[0]))

	// Find process
	process, err := os.FindProcess(pid)
	if err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	// Kill process
	if err := process.Kill(); err != nil {
		return map[string]Value{"success": false, "error": err.Error()}
	}

	return map[string]Value{"success": true, "pid": int64(pid)}
}

// ============================================================================
// Network Extended Functions
// ============================================================================

// IP Lookup
func (i *Interpreter) builtinIPLookup(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need IP or hostname"}
	}

	host, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "input must be string"}
	}

	// Parse as IP
	ip := net.ParseIP(host)
	if ip != nil {
		// Reverse lookup
		names, err := net.LookupAddr(host)
		if err != nil {
			return map[string]Value{
				"ip":     host,
				"names":  []Value{},
				"valid":  true,
				"version": map[string]Value{"v4": ip.To4() != nil, "v6": ip.To4() == nil},
			}
		}
		result := make([]Value, len(names))
		for idx, n := range names {
			result[idx] = n
		}
		return map[string]Value{
			"ip":      host,
			"names":   result,
			"valid":   true,
			"version": map[string]Value{"v4": ip.To4() != nil, "v6": ip.To4() == nil},
		}
	}

	// Forward lookup
	addrs, err := net.LookupHost(host)
	if err != nil {
		return map[string]Value{"error": err.Error(), "host": host}
	}

	ips := make([]Value, len(addrs))
	for idx, addr := range addrs {
		ips[idx] = addr
	}

	return map[string]Value{
		"host": host,
		"ips":  ips,
	}
}

// Whois (simplified)
func (i *Interpreter) builtinWhois(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need domain"}
	}

	domain, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "domain must be string"}
	}

	// This is a placeholder - actual whois requires network access to whois servers
	return map[string]Value{
		"domain":  domain,
		"message": "Whois lookup requires external whois server connection",
		"note":    "Use dnsLookup for DNS information",
	}
}

// Cron parsing and scheduling
func (i *Interpreter) builtinCronParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need cron expression"}
	}
	expr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "cron expression must be string"}
	}

	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return map[string]Value{"error": "invalid cron expression, need 5 fields (minute hour day month weekday)"}
	}

	return map[string]Value{
		"valid":    true,
		"minute":   parts[0],
		"hour":     parts[1],
		"day":      parts[2],
		"month":    parts[3],
		"weekday":  parts[4],
		"original": expr,
	}
}

func (i *Interpreter) builtinCronNext(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need cron expression"}
	}
	expr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "cron expression must be string"}
	}

	// Get optional start time
	startTime := time.Now()
	if len(args) > 1 {
		if t, ok := args[1].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, t); err == nil {
				startTime = parsed
			}
		}
	}

	// Simple cron parser - supports basic patterns
	next, err := parseCronAndFindNext(expr, startTime)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	return map[string]Value{
		"valid":   true,
		"next":    next.Format(time.RFC3339),
		"unix":    next.Unix(),
		"from":    startTime.Format(time.RFC3339),
	}
}

func (i *Interpreter) builtinCronNextN(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need cron expression"}
	}
	expr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "cron expression must be string"}
	}

	count := 5
	if len(args) > 1 {
		if c, ok := args[1].(int); ok {
			count = c
		} else if c, ok := args[1].(float64); ok {
			count = int(c)
		}
	}

	startTime := time.Now()
	if len(args) > 2 {
		if t, ok := args[2].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, t); err == nil {
				startTime = parsed
			}
		}
	}

	results := make([]Value, 0, count)
	current := startTime
	for len(results) < count {
		next, err := parseCronAndFindNext(expr, current)
		if err != nil {
			break
		}
		results = append(results, map[string]Value{
			"time":  next.Format(time.RFC3339),
			"unix":  next.Unix(),
		})
		current = next
	}

	return map[string]Value{
		"valid":  true,
		"count":  len(results),
		"times":  results,
	}
}

// Helper function for cron parsing
func parseCronAndFindNext(expr string, start time.Time) (time.Time, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return time.Time{}, fmt.Errorf("invalid cron expression")
	}

	// Simple implementation - only handles exact values and *
	// For more complex patterns, a proper cron parser library is recommended
	minute, hour, day, month, weekday := parts[0], parts[1], parts[2], parts[3], parts[4]

	// Start from the next minute
	current := start.Add(time.Minute).Truncate(time.Minute)

	// Search for next match (limit iterations to prevent infinite loop)
	for i := 0; i < 366*24*60; i++ {
		if matchesCron(current, minute, hour, day, month, weekday) {
			return current, nil
		}
		current = current.Add(time.Minute)
	}

	return time.Time{}, fmt.Errorf("no matching time found within 1 year")
}

func matchesCron(t time.Time, minute, hour, day, month, weekday string) bool {
	return matchField(t.Minute(), minute, 0, 59) &&
		matchField(t.Hour(), hour, 0, 23) &&
		matchField(t.Day(), day, 1, 31) &&
		matchField(int(t.Month()), month, 1, 12) &&
		matchField(int(t.Weekday()), weekday, 0, 6)
}

func matchField(value int, field string, min, max int) bool {
	if field == "*" {
		return true
	}

	// Handle comma-separated values
	if strings.Contains(field, ",") {
		for _, part := range strings.Split(field, ",") {
			if matchField(value, part, min, max) {
				return true
			}
		}
		return false
	}

	// Handle ranges
	if strings.Contains(field, "-") {
		parts := strings.Split(field, "-")
		if len(parts) == 2 {
			start, _ := strconv.Atoi(parts[0])
			end, _ := strconv.Atoi(parts[1])
			return value >= start && value <= end
		}
	}

	// Handle step values (*/n)
	if strings.HasPrefix(field, "*/") {
		step, _ := strconv.Atoi(strings.TrimPrefix(field, "*/"))
		if step > 0 {
			return value%step == 0
		}
	}

	// Exact match
	intVal, _ := strconv.Atoi(field)
	return value == intVal
}

// Geo location functions
func (i *Interpreter) builtinGeoDistance(args []Value) Value {
	if len(args) < 4 {
		return map[string]Value{"error": "need lat1, lon1, lat2, lon2"}
	}

	lat1 := toFloat(args[0])
	lon1 := toFloat(args[1])
	lat2 := toFloat(args[2])
	lon2 := toFloat(args[3])

	// Haversine formula
	const earthRadius = 6371.0 // km

	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	deltaLat := (lat2 - lat1) * math.Pi / 180
	deltaLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	distanceKm := earthRadius * c
	distanceMiles := distanceKm * 0.621371

	return map[string]Value{
		"km":     math.Round(distanceKm*100) / 100,
		"miles":  math.Round(distanceMiles*100) / 100,
		"meters": math.Round(distanceKm * 1000),
		"valid":  true,
	}
}

func (i *Interpreter) builtinGeoEncode(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need latitude and longitude"}
	}

	lat := toFloat(args[0])
	lon := toFloat(args[1])

	// Simple geohash encoding
	geohash := encodeGeohash(lat, lon, 12)

	return map[string]Value{
		"geohash": geohash,
		"lat":     lat,
		"lon":     lon,
		"valid":   true,
	}
}

func (i *Interpreter) builtinGeoDecode(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need geohash"}
	}

	geohash, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "geohash must be string"}
	}

	lat, lon := decodeGeohash(geohash)

	return map[string]Value{
		"lat":     lat,
		"lon":     lon,
		"geohash": geohash,
		"valid":   true,
	}
}

func (i *Interpreter) builtinGeoBoundingBox(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"error": "need lat, lon, radius_km"}
	}

	lat := toFloat(args[0])
	lon := toFloat(args[1])
	radiusKm := toFloat(args[2])

	// Calculate bounding box
	latDelta := radiusKm / 111.0 // 1 degree latitude ≈ 111 km
	lonDelta := radiusKm / (111.0 * math.Cos(lat*math.Pi/180))

	return map[string]Value{
		"minLat": lat - latDelta,
		"maxLat": lat + latDelta,
		"minLon": lon - lonDelta,
		"maxLon": lon + lonDelta,
		"center": map[string]Value{
			"lat": lat,
			"lon": lon,
		},
		"radiusKm": radiusKm,
		"valid":    true,
	}
}

func (i *Interpreter) builtinGeoIP(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need IP address"}
	}

	ip, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "IP must be string"}
	}

	// Basic IP validation
	netIP := net.ParseIP(ip)
	if netIP == nil {
		return map[string]Value{"error": "invalid IP address"}
	}

	// Return IP info (actual geo lookup requires external service)
	return map[string]Value{
		"ip":      ip,
		"version": map[string]Value{"v4": netIP.To4() != nil, "v6": netIP.To4() == nil},
		"note":    "Geo location requires external IP service",
		"valid":   true,
	}
}

func (i *Interpreter) builtinGeoWithin(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"error": "need lat, lon, and bounding box or center+radius"}
	}

	lat := toFloat(args[0])
	lon := toFloat(args[1])

	// Check if point is within bounding box
	if bbox, ok := args[2].(map[string]Value); ok {
		minLat := toFloat(bbox["minLat"])
		maxLat := toFloat(bbox["maxLat"])
		minLon := toFloat(bbox["minLon"])
		maxLon := toFloat(bbox["maxLon"])

		within := lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon
		return map[string]Value{
			"within": within,
			"point":  map[string]Value{"lat": lat, "lon": lon},
			"valid":  true,
		}
	}

	return map[string]Value{"error": "third argument must be bounding box map"}
}

// Geohash encoding/decoding helpers
func encodeGeohash(lat, lon float64, precision int) string {
	const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

	latRange := [2]float64{-90, 90}
	lonRange := [2]float64{-180, 180}

	var geohash strings.Builder
	var bits uint
	var mid float64
	currentBit := 0

	for geohash.Len() < precision {
		if currentBit%2 == 0 {
			// Even bit: longitude
			mid = (lonRange[0] + lonRange[1]) / 2
			if lon > mid {
				bits = bits*2 + 1
				lonRange[0] = mid
			} else {
				bits = bits * 2
				lonRange[1] = mid
			}
		} else {
			// Odd bit: latitude
			mid = (latRange[0] + latRange[1]) / 2
			if lat > mid {
				bits = bits*2 + 1
				latRange[0] = mid
			} else {
				bits = bits * 2
				latRange[1] = mid
			}
		}

		currentBit++
		if currentBit%5 == 0 {
			geohash.WriteByte(base32[bits])
			bits = 0
		}
	}

	return geohash.String()
}

func decodeGeohash(geohash string) (lat, lon float64) {
	const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

	latRange := [2]float64{-90, 90}
	lonRange := [2]float64{-180, 180}

	for _, c := range geohash {
		idx := strings.Index(base32, string(c))
		if idx < 0 {
			continue
		}

		for i := 4; i >= 0; i-- {
			bit := (idx >> i) & 1

			if (len(geohash)-strings.Index(geohash, string(c)))*5+(4-i) == 0 {
				continue
			}

			// Determine if this is a lon or lat bit
			pos := (strings.Index(geohash, string(c)))*5 + (4 - i)
			if pos%2 == 0 {
				// Even position: longitude
				mid := (lonRange[0] + lonRange[1]) / 2
				if bit == 1 {
					lonRange[0] = mid
				} else {
					lonRange[1] = mid
				}
			} else {
				// Odd position: latitude
				mid := (latRange[0] + latRange[1]) / 2
				if bit == 1 {
					latRange[0] = mid
				} else {
					latRange[1] = mid
				}
			}
		}
	}

	return (latRange[0] + latRange[1]) / 2, (lonRange[0] + lonRange[1]) / 2
}

// Rate limiting functions
var rateLimiters = make(map[string]*rateLimiter)
var rateLimitersMutex sync.RWMutex

type rateLimiter struct {
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func (i *Interpreter) builtinRateLimiter(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"error": "need name, maxTokens, refillRate"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}

	maxTokens := toFloat(args[1])
	refillRate := toFloat(args[2])

	rateLimitersMutex.Lock()
	defer rateLimitersMutex.Unlock()

	rateLimiters[name] = &rateLimiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}

	return map[string]Value{
		"name":       name,
		"maxTokens":  maxTokens,
		"refillRate": refillRate,
		"created":    true,
	}
}

func (i *Interpreter) builtinRateLimitCheck(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need name"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}

	cost := 1.0
	if len(args) > 1 {
		cost = toFloat(args[1])
	}

	rateLimitersMutex.Lock()
	defer rateLimitersMutex.Unlock()

	rl, exists := rateLimiters[name]
	if !exists {
		return map[string]Value{"error": "rate limiter not found", "name": name}
	}

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens = math.Min(rl.maxTokens, rl.tokens+elapsed*rl.refillRate)
	rl.lastRefill = now

	if rl.tokens >= cost {
		rl.tokens -= cost
		return map[string]Value{
			"allowed":     true,
			"tokensLeft":  rl.tokens,
			"name":        name,
		}
	}

	return map[string]Value{
		"allowed":     false,
		"tokensLeft":  rl.tokens,
		"retryAfter":  (cost - rl.tokens) / rl.refillRate,
		"name":        name,
	}
}

func (i *Interpreter) builtinRateLimitReset(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need name"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}

	rateLimitersMutex.Lock()
	defer rateLimitersMutex.Unlock()

	if rl, exists := rateLimiters[name]; exists {
		rl.tokens = rl.maxTokens
		rl.lastRefill = time.Now()
		return map[string]Value{"reset": true, "name": name}
	}

	return map[string]Value{"error": "rate limiter not found", "name": name}
}

// Metrics functions
var metricsStore = make(map[string]*metricValue)
var metricsMutex sync.RWMutex

type metricValue struct {
	metricType string
	value      float64
	labels     map[string]string
	updatedAt  time.Time
}

func (i *Interpreter) builtinMetricsCounter(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need name and value"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}
	value := toFloat(args[1])

	metricsMutex.Lock()
	defer metricsMutex.Unlock()

	if existing, exists := metricsStore[name]; exists {
		existing.value += value
		existing.updatedAt = time.Now()
	} else {
		metricsStore[name] = &metricValue{
			metricType: "counter",
			value:      value,
			updatedAt:  time.Now(),
		}
	}

	return map[string]Value{
		"name":  name,
		"value": metricsStore[name].value,
		"type":  "counter",
	}
}

func (i *Interpreter) builtinMetricsGauge(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need name and value"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}
	value := toFloat(args[1])

	metricsMutex.Lock()
	defer metricsMutex.Unlock()

	metricsStore[name] = &metricValue{
		metricType: "gauge",
		value:      value,
		updatedAt:  time.Now(),
	}

	return map[string]Value{
		"name":  name,
		"value": value,
		"type":  "gauge",
	}
}

func (i *Interpreter) builtinMetricsGet(args []Value) Value {
	if len(args) == 0 {
		// Return all metrics
		metricsMutex.RLock()
		defer metricsMutex.RUnlock()

		result := make(map[string]Value)
		for name, m := range metricsStore {
			result[name] = map[string]Value{
				"type":  m.metricType,
				"value": m.value,
				"updatedAt": m.updatedAt.Format(time.RFC3339),
			}
		}
		return result
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}

	metricsMutex.RLock()
	defer metricsMutex.RUnlock()

	if m, exists := metricsStore[name]; exists {
		return map[string]Value{
			"name":     name,
			"type":     m.metricType,
			"value":    m.value,
			"updatedAt": m.updatedAt.Format(time.RFC3339),
		}
	}

	return map[string]Value{"error": "metric not found", "name": name}
}

// State machine functions
type stateMachineState struct {
	name        string
	transitions map[string]string // event -> target state
	onEnter     string
	onExit      string
}

var stateMachines = make(map[string]*stateMachine)
var stateMachinesMutex sync.RWMutex

type stateMachine struct {
	currentState string
	states       map[string]*stateMachineState
	initialState string
}

func (i *Interpreter) builtinStateMachine(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need name"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "name must be string"}
	}

	initial := "initial"
	if len(args) > 1 {
		if init, ok := args[1].(string); ok {
			initial = init
		}
	}

	sm := &stateMachine{
		currentState: initial,
		states:       make(map[string]*stateMachineState),
		initialState: initial,
	}

	sm.states[initial] = &stateMachineState{
		name:        initial,
		transitions: make(map[string]string),
	}

	stateMachinesMutex.Lock()
	stateMachines[name] = sm
	stateMachinesMutex.Unlock()

	return map[string]Value{
		"name":       name,
		"state":      initial,
		"created":    true,
	}
}

func (i *Interpreter) builtinStateAdd(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need machine name and state name"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "machine name must be string"}
	}

	stateName, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "state name must be string"}
	}

	stateMachinesMutex.Lock()
	defer stateMachinesMutex.Unlock()

	sm, exists := stateMachines[name]
	if !exists {
		return map[string]Value{"error": "state machine not found"}
	}

	sm.states[stateName] = &stateMachineState{
		name:        stateName,
		transitions: make(map[string]string),
	}

	return map[string]Value{
		"added":    true,
		"machine":  name,
		"state":    stateName,
	}
}

func (i *Interpreter) builtinStateTransition(args []Value) Value {
	if len(args) < 3 {
		return map[string]Value{"error": "need machine name, event, and target state"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "machine name must be string"}
	}

	event, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "event must be string"}
	}

	targetState, ok := args[2].(string)
	if !ok {
		return map[string]Value{"error": "target state must be string"}
	}

	stateMachinesMutex.Lock()
	defer stateMachinesMutex.Unlock()

	sm, exists := stateMachines[name]
	if !exists {
		return map[string]Value{"error": "state machine not found"}
	}

	// Check if current state exists
	current, exists := sm.states[sm.currentState]
	if !exists {
		return map[string]Value{"error": "current state not defined"}
	}

	// Check if target state exists
	if _, exists := sm.states[targetState]; !exists {
		return map[string]Value{"error": "target state not defined"}
	}

	// Add transition
	current.transitions[event] = targetState

	// Check if this event is valid for current state
	if target, valid := current.transitions[event]; valid {
		sm.currentState = target
		return map[string]Value{
			"transitioned": true,
			"from":         current.name,
			"to":           target,
			"event":        event,
		}
	}

	return map[string]Value{
		"transitioned": false,
		"currentState": sm.currentState,
		"error":        "invalid event for current state",
	}
}

func (i *Interpreter) builtinStateCurrent(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need machine name"}
	}

	name, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "machine name must be string"}
	}

	stateMachinesMutex.RLock()
	defer stateMachinesMutex.RUnlock()

	sm, exists := stateMachines[name]
	if !exists {
		return map[string]Value{"error": "state machine not found"}
	}

	return map[string]Value{
		"name":       name,
		"state":      sm.currentState,
		"initial":    sm.initialState,
		"stateCount": len(sm.states),
	}
}

// Expression evaluation
func (i *Interpreter) builtinExprEval(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need expression"}
	}

	expr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "expression must be string"}
	}

	// Simple expression evaluator
	result, err := evalExpression(expr)
	if err != nil {
		return map[string]Value{"error": err.Error()}
	}

	return map[string]Value{
		"result": result,
		"expr":   expr,
		"valid":  true,
	}
}

func evalExpression(expr string) (interface{}, error) {
	// Simple expression parser supporting +, -, *, /, %, parentheses
	// This is a basic implementation - for complex expressions, use a proper parser

	// Tokenize
	tokens := tokenize(expr)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty expression")
	}

	// Parse and evaluate
	pos := 0
	return parseAddSub(tokens, &pos)
}

func tokenize(expr string) []string {
	var tokens []string
	var current strings.Builder

	for _, c := range expr {
		switch c {
		case ' ', '\t', '\n':
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		case '+', '-', '*', '/', '%', '(', ')':
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			tokens = append(tokens, string(c))
		default:
			current.WriteRune(c)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func parseAddSub(tokens []string, pos *int) (float64, error) {
	left, err := parseMulDiv(tokens, pos)
	if err != nil {
		return 0, err
	}

	for *pos < len(tokens) {
		op := tokens[*pos]
		if op != "+" && op != "-" {
			break
		}
		*pos++
		right, err := parseMulDiv(tokens, pos)
		if err != nil {
			return 0, err
		}
		if op == "+" {
			left += right
		} else {
			left -= right
		}
	}
	return left, nil
}

func parseMulDiv(tokens []string, pos *int) (float64, error) {
	left, err := parsePrimary(tokens, pos)
	if err != nil {
		return 0, err
	}

	for *pos < len(tokens) {
		op := tokens[*pos]
		if op != "*" && op != "/" && op != "%" {
			break
		}
		*pos++
		right, err := parsePrimary(tokens, pos)
		if err != nil {
			return 0, err
		}
		switch op {
		case "*":
			left *= right
		case "/":
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			left /= right
		case "%":
			left = math.Mod(left, right)
		}
	}
	return left, nil
}

func parsePrimary(tokens []string, pos *int) (float64, error) {
	if *pos >= len(tokens) {
		return 0, fmt.Errorf("unexpected end of expression")
	}

	token := tokens[*pos]
	*pos++

	if token == "(" {
		result, err := parseAddSub(tokens, pos)
		if err != nil {
			return 0, err
		}
		if *pos >= len(tokens) || tokens[*pos] != ")" {
			return 0, fmt.Errorf("missing closing parenthesis")
		}
		*pos++
		return result, nil
	}

	// Number
	val, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %s", token)
	}
	return val, nil
}

// HTML parsing functions (basic implementation)
func (i *Interpreter) builtinHTMLParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need HTML string"}
	}

	htmlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "HTML must be string"}
	}

	// Basic HTML parsing - just extract text content
	text := extractTextFromHTML(htmlStr)

	return map[string]Value{
		"html":    htmlStr,
		"text":    text,
		"length":  len(htmlStr),
		"valid":   true,
	}
}

func (i *Interpreter) builtinHTMLSelect(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need HTML and selector"}
	}

	htmlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "HTML must be string"}
	}

	selector, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "selector must be string"}
	}

	// Basic selector implementation
	elements := findElementsBySelector(htmlStr, selector)

	return map[string]Value{
		"found":    len(elements) > 0,
		"count":    len(elements),
		"elements": elements,
		"selector": selector,
	}
}

func (i *Interpreter) builtinHTMLSelectAll(args []Value) Value {
	return i.builtinHTMLSelect(args) // Same implementation
}

func (i *Interpreter) builtinHTMLAttr(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need HTML element and attribute name"}
	}

	htmlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "HTML must be string"}
	}

	attr, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "attribute must be string"}
	}

	value := extractAttribute(htmlStr, attr)

	return map[string]Value{
		"attribute": attr,
		"value":     value,
		"found":     value != "",
	}
}

func (i *Interpreter) builtinHTMLText(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need HTML string"}
	}

	htmlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "HTML must be string"}
	}

	text := extractTextFromHTML(htmlStr)

	return map[string]Value{
		"text": text,
	}
}

func (i *Interpreter) builtinHTMLLinks(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need HTML string"}
	}

	htmlStr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "HTML must be string"}
	}

	links := extractLinks(htmlStr)

	return map[string]Value{
		"links": links,
		"count": len(links),
	}
}

// Helper functions for HTML processing
func extractTextFromHTML(htmlStr string) string {
	// Remove HTML tags
	re := regexp.MustCompile(`<[^>]*>`)
	text := re.ReplaceAllString(htmlStr, " ")
	// Decode HTML entities
	text = html.UnescapeString(text)
	// Clean up whitespace
	text = strings.Join(strings.Fields(text), " ")
	return text
}

func findElementsBySelector(htmlStr, selector string) []Value {
	var elements []Value

	// Basic tag selector (e.g., "div", "p", "a")
	re := regexp.MustCompile(`<` + selector + `[^>]*>(.*?)</` + selector + `>`)
	matches := re.FindAllStringSubmatch(htmlStr, -1)

	for _, match := range matches {
		if len(match) > 1 {
			elements = append(elements, match[0])
		}
	}

	return elements
}

func extractAttribute(htmlStr, attr string) string {
	re := regexp.MustCompile(attr + `=["']([^"']*)["']`)
	match := re.FindStringSubmatch(htmlStr)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func extractLinks(htmlStr string) []Value {
	var links []Value

	// Extract href attributes
	hrefRe := regexp.MustCompile(`href=["']([^"']*)["']`)
	matches := hrefRe.FindAllStringSubmatch(htmlStr, -1)
	for _, match := range matches {
		if len(match) > 1 {
			links = append(links, match[1])
		}
	}

	return links
}

// RSS parsing
func (i *Interpreter) builtinRSSParse(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need RSS XML string"}
	}

	rssXML, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "RSS must be string"}
	}

	// Basic RSS parsing
	items := parseRSSItems(rssXML)

	return map[string]Value{
		"valid": true,
		"items": items,
		"count": len(items),
	}
}

func parseRSSItems(rssXML string) []Value {
	var items []Value

	// Simple regex-based parsing
	itemRe := regexp.MustCompile(`<item>(.*?)</item>`)
	matches := itemRe.FindAllStringSubmatch(rssXML, -1)

	for _, match := range matches {
		if len(match) > 1 {
			itemXML := match[1]
			title := extractXMLContent(itemXML, "title")
			link := extractXMLContent(itemXML, "link")
			desc := extractXMLContent(itemXML, "description")

			items = append(items, map[string]Value{
				"title":       title,
				"link":        link,
				"description": desc,
			})
		}
	}

	return items
}

func extractXMLContent(xmlStr, tag string) string {
	re := regexp.MustCompile(`<` + tag + `><!\[CDATA\[(.*?)\]\]></` + tag + `>`)
	match := re.FindStringSubmatch(xmlStr)
	if len(match) > 1 {
		return match[1]
	}

	re = regexp.MustCompile(`<` + tag + `>(.*?)</` + tag + `>`)
	match = re.FindStringSubmatch(xmlStr)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

// ML functions (simplified implementations)
func (i *Interpreter) builtinMLTokenize(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need text"}
	}

	text, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "text must be string"}
	}

	// Simple whitespace tokenization
	words := strings.Fields(text)
	tokens := make([]Value, len(words))
	for idx, w := range words {
		// Clean punctuation
		w = strings.Trim(w, ".,!?;:\"'()[]{}")
		tokens[idx] = w
	}

	return map[string]Value{
		"tokens": tokens,
		"count":  len(tokens),
		"valid":  true,
	}
}

func (i *Interpreter) builtinMLSentiment(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need text"}
	}

	text, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "text must be string"}
	}

	// Simple sentiment analysis based on keyword matching
	positive := []string{"good", "great", "excellent", "amazing", "wonderful", "fantastic", "happy", "love", "best", "beautiful"}
	negative := []string{"bad", "terrible", "awful", "horrible", "worst", "hate", "poor", "sad", "angry", "disappointing"}

	textLower := strings.ToLower(text)
	posScore := 0
	negScore := 0

	for _, word := range positive {
		if strings.Contains(textLower, word) {
			posScore++
		}
	}
	for _, word := range negative {
		if strings.Contains(textLower, word) {
			negScore++
		}
	}

	total := posScore + negScore
	if total == 0 {
		return map[string]Value{
			"sentiment": "neutral",
			"score":     0.0,
			"positive":  0,
			"negative":  0,
		}
	}

	score := float64(posScore-negScore) / float64(total)
	sentiment := "neutral"
	if score > 0.2 {
		sentiment = "positive"
	} else if score < -0.2 {
		sentiment = "negative"
	}

	return map[string]Value{
		"sentiment": sentiment,
		"score":     math.Round(score*100) / 100,
		"positive":  posScore,
		"negative":  negScore,
	}
}

func (i *Interpreter) builtinMLSimilarity(args []Value) Value {
	if len(args) < 2 {
		return map[string]Value{"error": "need two texts"}
	}

	text1, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "text1 must be string"}
	}

	text2, ok := args[1].(string)
	if !ok {
		return map[string]Value{"error": "text2 must be string"}
	}

	// Jaccard similarity based on words
	words1 := make(map[string]bool)
	for _, w := range strings.Fields(strings.ToLower(text1)) {
		words1[strings.Trim(w, ".,!?;:\"'")] = true
	}

	words2 := make(map[string]bool)
	for _, w := range strings.Fields(strings.ToLower(text2)) {
		words2[strings.Trim(w, ".,!?;:\"'")] = true
	}

	intersection := 0
	for w := range words1 {
		if words2[w] {
			intersection++
		}
	}

	union := len(words1) + len(words2) - intersection
	if union == 0 {
		return map[string]Value{"similarity": 0.0}
	}

	similarity := float64(intersection) / float64(union)

	return map[string]Value{
		"similarity": math.Round(similarity*100) / 100,
		"intersection": intersection,
		"union":      union,
	}
}

func (i *Interpreter) builtinMLKeywords(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need text"}
	}

	text, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "text must be string"}
	}

	count := 5
	if len(args) > 1 {
		if c, ok := args[1].(int); ok {
			count = c
		}
	}

	// Simple keyword extraction based on word frequency
	wordFreq := make(map[string]int)
	stopWords := map[string]bool{
		"the": true, "a": true, "an": true, "is": true, "are": true,
		"was": true, "were": true, "be": true, "been": true, "being": true,
		"have": true, "has": true, "had": true, "do": true, "does": true,
		"did": true, "will": true, "would": true, "could": true, "should": true,
		"may": true, "might": true, "must": true, "shall": true, "can": true,
		"to": true, "of": true, "in": true, "for": true, "on": true,
		"with": true, "at": true, "by": true, "from": true, "as": true,
		"into": true, "through": true, "during": true, "before": true, "after": true,
		"and": true, "but": true, "or": true, "nor": true, "so": true, "yet": true,
		"this": true, "that": true, "these": true, "those": true,
		"it": true, "its": true, "they": true, "them": true, "their": true,
		"he": true, "she": true, "him": true, "her": true, "his": true,
		"we": true, "us": true, "our": true, "you": true, "your": true, "i": true, "my": true,
	}

	for _, w := range strings.Fields(strings.ToLower(text)) {
		w = strings.Trim(w, ".,!?;:\"'()[]{}")
		if len(w) > 2 && !stopWords[w] {
			wordFreq[w]++
		}
	}

	// Sort by frequency
	type wordCount struct {
		word  string
		count int
	}
	var sorted []wordCount
	for w, c := range wordFreq {
		sorted = append(sorted, wordCount{w, c})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	keywords := make([]Value, 0, count)
	for i := 0; i < count && i < len(sorted); i++ {
		keywords = append(keywords, map[string]Value{
			"word":  sorted[i].word,
			"count": sorted[i].count,
		})
	}

	return map[string]Value{
		"keywords": keywords,
		"count":    len(keywords),
	}
}

func (i *Interpreter) builtinMLNgrams(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need text"}
	}

	text, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "text must be string"}
	}

	n := 2
	if len(args) > 1 {
		if val, ok := args[1].(int); ok {
			n = val
		}
	}

	words := strings.Fields(strings.ToLower(text))
	for idx, w := range words {
		words[idx] = strings.Trim(w, ".,!?;:\"'")
	}

	if len(words) < n {
		return map[string]Value{
			"ngrams": []Value{},
			"n":      n,
		}
	}

	var ngrams []Value
	for j := 0; j <= len(words)-n; j++ {
		ngram := strings.Join(words[j:j+n], " ")
		ngrams = append(ngrams, ngram)
	}

	return map[string]Value{
		"ngrams": ngrams,
		"n":      n,
		"count":  len(ngrams),
	}
}

func (i *Interpreter) builtinMLWordFreq(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need text"}
	}

	text, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "text must be string"}
	}

	wordFreq := make(map[string]int)
	for _, w := range strings.Fields(strings.ToLower(text)) {
		w = strings.Trim(w, ".,!?;:\"'()[]{}")
		if len(w) > 0 {
			wordFreq[w]++
		}
	}

	freq := make(map[string]Value)
	for w, c := range wordFreq {
		freq[w] = c
	}

	return map[string]Value{
		"frequencies": freq,
		"uniqueWords": len(wordFreq),
	}
}

// WebSocket functions (require gorilla/websocket - stub implementation)
func (i *Interpreter) builtinWSConnect(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need URL"}
	}
	url, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "URL must be string"}
	}
	return map[string]Value{
		"error":   "WebSocket support requires github.com/gorilla/websocket",
		"hint":    "Add dependency and rebuild",
		"url":     url,
	}
}

func (i *Interpreter) builtinWSSend(args []Value) Value {
	return map[string]Value{"error": "WebSocket not available - requires gorilla/websocket dependency"}
}

func (i *Interpreter) builtinWSReceive(args []Value) Value {
	return map[string]Value{"error": "WebSocket not available - requires gorilla/websocket dependency"}
}

func (i *Interpreter) builtinWSClose(args []Value) Value {
	return map[string]Value{"error": "WebSocket not available - requires gorilla/websocket dependency"}
}

func (i *Interpreter) builtinWSIsConnected(args []Value) Value {
	return map[string]Value{"error": "WebSocket not available - requires gorilla/websocket dependency", "connected": false}
}

// Redis functions (require go-redis - stub implementation)
func (i *Interpreter) builtinRedisConnect(args []Value) Value {
	if len(args) == 0 {
		return map[string]Value{"error": "need connection string"}
	}
	addr, ok := args[0].(string)
	if !ok {
		return map[string]Value{"error": "address must be string"}
	}
	return map[string]Value{
		"error": "Redis support requires github.com/redis/go-redis/v9",
		"hint":  "Add dependency and rebuild",
		"addr":  addr,
	}
}

func (i *Interpreter) builtinRedisGet(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisSet(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisDel(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisExists(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisExpire(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisIncr(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisDecr(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisLPush(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisRPush(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisLPop(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisRPop(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisHSet(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisHGet(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisHDel(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisHGetAll(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisKeys(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

func (i *Interpreter) builtinRedisTTL(args []Value) Value {
	return map[string]Value{"error": "Redis not available - requires go-redis dependency"}
}

// PDF functions (require gofpdf - stub implementation)
func (i *Interpreter) builtinPDFCreate(args []Value) Value {
	return map[string]Value{
		"error": "PDF support requires github.com/jung-kurt/gofpdf",
		"hint":  "Add dependency and rebuild",
	}
}

func (i *Interpreter) builtinPDFAddPage(args []Value) Value {
	return map[string]Value{"error": "PDF not available - requires gofpdf dependency"}
}

func (i *Interpreter) builtinPDFAddText(args []Value) Value {
	return map[string]Value{"error": "PDF not available - requires gofpdf dependency"}
}

func (i *Interpreter) builtinPDFSetFont(args []Value) Value {
	return map[string]Value{"error": "PDF not available - requires gofpdf dependency"}
}

func (i *Interpreter) builtinPDFSave(args []Value) Value {
	return map[string]Value{"error": "PDF not available - requires gofpdf dependency"}
}

func (i *Interpreter) builtinPDFCell(args []Value) Value {
	return map[string]Value{"error": "PDF not available - requires gofpdf dependency"}
}

// Excel functions (require excelize - stub implementation)
func (i *Interpreter) builtinExcelCreate(args []Value) Value {
	return map[string]Value{
		"error": "Excel support requires github.com/xuri/excelize/v2",
		"hint":  "Add dependency and rebuild",
	}
}

func (i *Interpreter) builtinExcelOpen(args []Value) Value {
	return map[string]Value{"error": "Excel not available - requires excelize dependency"}
}

func (i *Interpreter) builtinExcelSetCell(args []Value) Value {
	return map[string]Value{"error": "Excel not available - requires excelize dependency"}
}

func (i *Interpreter) builtinExcelGetCell(args []Value) Value {
	return map[string]Value{"error": "Excel not available - requires excelize dependency"}
}

func (i *Interpreter) builtinExcelNewSheet(args []Value) Value {
	return map[string]Value{"error": "Excel not available - requires excelize dependency"}
}

func (i *Interpreter) builtinExcelSave(args []Value) Value {
	return map[string]Value{"error": "Excel not available - requires excelize dependency"}
}

func (i *Interpreter) builtinExcelClose(args []Value) Value {
	return map[string]Value{"error": "Excel not available - requires excelize dependency"}
}

// Chart functions (require go-chart - stub implementation)
func (i *Interpreter) builtinChartLine(args []Value) Value {
	return map[string]Value{
		"error": "Chart support requires github.com/wcharczuk/go-chart/v2",
		"hint":  "Add dependency and rebuild",
	}
}

func (i *Interpreter) builtinChartBar(args []Value) Value {
	return map[string]Value{"error": "Chart not available - requires go-chart dependency"}
}

func (i *Interpreter) builtinChartPie(args []Value) Value {
	return map[string]Value{"error": "Chart not available - requires go-chart dependency"}
}

// Git functions (using exec)
func (i *Interpreter) builtinGitStatus(args []Value) Value {
	dir := ""
	if len(args) > 0 {
		if d, ok := args[0].(string); ok {
			dir = d
		}
	}

	cmd := exec.Command("git", "status", "--porcelain")
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return map[string]Value{"error": err.Error(), "output": string(output)}
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var changes []Value
	for _, line := range lines {
		if line == "" {
			continue
		}
		if len(line) >= 3 {
			status := strings.TrimSpace(line[:2])
			file := line[3:]
			changes = append(changes, map[string]Value{
				"status": status,
				"file":   file,
			})
		}
	}

	return map[string]Value{
		"clean":   len(changes) == 0,
		"changes": changes,
		"count":   len(changes),
	}
}

func (i *Interpreter) builtinGitLog(args []Value) Value {
	dir := ""
	if len(args) > 0 {
		if d, ok := args[0].(string); ok {
			dir = d
		}
	}

	count := 10
	if len(args) > 1 {
		if c, ok := args[1].(int); ok {
			count = c
		}
	}

	cmd := exec.Command("git", "log", fmt.Sprintf("-%d", count), "--pretty=format:%H|%s|%an|%ad", "--date=short")
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return map[string]Value{"error": err.Error(), "output": string(output)}
	}

	var commits []Value
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) >= 4 {
			commits = append(commits, map[string]Value{
				"hash":    parts[0],
				"message": parts[1],
				"author":  parts[2],
				"date":    parts[3],
			})
		}
	}

	return map[string]Value{
		"commits": commits,
		"count":   len(commits),
	}
}

func (i *Interpreter) builtinGitBranch(args []Value) Value {
	dir := ""
	if len(args) > 0 {
		if d, ok := args[0].(string); ok {
			dir = d
		}
	}

	// Get current branch
	cmd := exec.Command("git", "branch", "--show-current")
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return map[string]Value{"error": err.Error(), "output": string(output)}
	}
	currentBranch := strings.TrimSpace(string(output))

	// Get all branches
	cmd = exec.Command("git", "branch", "-a")
	if dir != "" {
		cmd.Dir = dir
	}
	output, err = cmd.CombinedOutput()
	if err != nil {
		return map[string]Value{"error": err.Error(), "output": string(output)}
	}

	var branches []Value
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		current := strings.HasPrefix(line, "* ")
		name := strings.TrimPrefix(line, "* ")
		name = strings.TrimSpace(name)
		branches = append(branches, map[string]Value{
			"name":    name,
			"current": current,
		})
	}

	return map[string]Value{
		"current":  currentBranch,
		"branches": branches,
		"count":    len(branches),
	}
}

// SetupBuiltins sets up built-in objects.
func (ctx *Context) SetupBuiltins() {
	ctx.Variables["http"] = NewHTTPObject(ctx)
	ctx.Variables["db"] = NewDBObject(ctx)
	ctx.Variables["true"] = true
	ctx.Variables["false"] = false
	ctx.Variables["null"] = nil
}