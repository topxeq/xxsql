// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/storage"
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
	result := 1.0
	for exp > 0 {
		if int(exp)%2 == 1 {
			result *= base
		}
		base *= base
		exp = float64(int(exp) / 2)
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
					nameField := col.FieldByName("Name")
					if nameField.IsValid() {
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

// SetupBuiltins sets up built-in objects.
func (ctx *Context) SetupBuiltins() {
	ctx.Variables["http"] = NewHTTPObject(ctx)
	ctx.Variables["db"] = NewDBObject(ctx)
	ctx.Variables["true"] = true
	ctx.Variables["false"] = false
	ctx.Variables["null"] = nil
}