// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

// Value represents a runtime value.
type Value interface{}

// Context provides the execution context.
type Context struct {
	Variables  map[string]Value
	Functions  map[string]*UserFunc
	Executor   *executor.Executor
	Engine     *storage.Engine
	HTTPWriter http.ResponseWriter
	HTTPRequest *http.Request
	MaxSteps   int
	steps      int
	returning  bool
	breaking   bool
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
	case "now":
		return i.builtinNow(args), true
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
	return i.toInt(args[0])
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
	body, err := f.ctx.HTTPRequest.GetBody()
	if err != nil {
		return "", nil
	}
	data := make([]byte, 0)
	body.Read(data)
	return string(data), nil
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

	result, err := f.ctx.Executor.Execute(query)
	if err != nil {
		return nil, err
	}

	// Convert result to script values
	rows := make([]Value, len(result.Rows))
	for i, row := range result.Rows {
		rowMap := make(map[string]Value)
		for j, col := range result.Columns {
			key := col.Name
			if col.Alias != "" {
				key = col.Alias
			}
			if j < len(row) {
				rowMap[key] = row[j]
			}
		}
		rows[i] = rowMap
	}

	return rows, nil
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

	result, err := f.ctx.Executor.Execute(query)
	if err != nil {
		return nil, err
	}

	return map[string]Value{
		"affected":  result.RowCount,
		"insert_id": result.LastInsert,
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

	result, err := f.ctx.Executor.Execute(query)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, nil
	}

	rowMap := make(map[string]Value)
	row := result.Rows[0]
	for j, col := range result.Columns {
		key := col.Name
		if col.Alias != "" {
			key = col.Alias
		}
		if j < len(row) {
			rowMap[key] = row[j]
		}
	}

	return rowMap, nil
}

// SetupBuiltins sets up built-in objects.
func (ctx *Context) SetupBuiltins() {
	ctx.Variables["http"] = NewHTTPObject(ctx)
	ctx.Variables["db"] = NewDBObject(ctx)
	ctx.Variables["true"] = true
	ctx.Variables["false"] = false
	ctx.Variables["null"] = nil
}