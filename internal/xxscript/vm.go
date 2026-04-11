package xxscript

import (
	"fmt"
)

// VM is a virtual machine for executing bytecode.
type VM struct {
	bytecode    *Bytecode
	ip          int              // Instruction pointer
	stack       []Value          // Value stack
	vars        map[string]Value // Variables
	globals     map[string]Value // Global variables
	ctx         *Context
	returning   bool
	breaking    bool
	continueing bool
	lastValue   Value // Last popped value (for expression results)
}

// NewVM creates a new virtual machine.
func NewVM(bytecode *Bytecode, globals map[string]Value) *VM {
	return &VM{
		bytecode: bytecode,
		ip:       0,
		stack:    make([]Value, 0, 256),
		vars:     make(map[string]Value),
		globals:  globals,
		ctx:      NewContext(),
	}
}

// currentLine returns the line number for the current instruction.
func (vm *VM) currentLine() int {
	if vm.ip > 0 && vm.ip <= len(vm.bytecode.Lines) {
		return vm.bytecode.Lines[vm.ip-1]
	}
	return 0
}

// Run executes the bytecode.
func (vm *VM) Run() (Value, error) {
	for vm.ip < len(vm.bytecode.Instructions) {
		instr := vm.bytecode.Instructions[vm.ip]

		vm.ip++

		if err := vm.execute(instr); err != nil {
			// Add line info to error
			line := vm.currentLine()
			if line > 0 {
				return nil, fmt.Errorf("line %d: %w", line, err)
			}
			return nil, err
		}

		if vm.returning {
			if len(vm.stack) > 0 {
				return vm.stack[len(vm.stack)-1], nil
			}
			return vm.lastValue, nil
		}
	}

	if len(vm.stack) > 0 {
		return vm.stack[len(vm.stack)-1], nil
	}
	return vm.lastValue, nil
}

func (vm *VM) execute(instr Instruction) error {
	switch instr.Op {
	case OpPush:
		vm.push(vm.bytecode.Constants[instr.Arg])

	case OpPop:
		vm.lastValue = vm.pop()

	case OpDup:
		if len(vm.stack) > 0 {
			vm.push(vm.stack[len(vm.stack)-1])
		}

	case OpNull:
		vm.push(nil)

	case OpTrue:
		vm.push(true)

	case OpFalse:
		vm.push(false)

	case OpLoadVar:
		if val, ok := vm.vars[instr.ArgS]; ok {
			vm.push(val)
		} else if val, ok := vm.globals[instr.ArgS]; ok {
			vm.push(val)
		} else {
			vm.push(nil)
		}

	case OpStoreVar:
		val := vm.pop()
		vm.vars[instr.ArgS] = val
		vm.push(val)

	case OpAdd:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.add(a, b))

	case OpSub:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.sub(a, b))

	case OpMul:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.mul(a, b))

	case OpDiv:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.div(a, b))

	case OpMod:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.mod(a, b))

	case OpNeg:
		a := vm.pop()
		vm.push(vm.neg(a))

	case OpEq:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.equal(a, b))

	case OpNe:
		b, a := vm.pop(), vm.pop()
		vm.push(!vm.equal(a, b))

	case OpLt:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.compare(a, b) < 0)

	case OpLe:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.compare(a, b) <= 0)

	case OpGt:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.compare(a, b) > 0)

	case OpGe:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.compare(a, b) >= 0)

	case OpAnd:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.isTruthy(a) && vm.isTruthy(b))

	case OpOr:
		b, a := vm.pop(), vm.pop()
		vm.push(vm.isTruthy(a) || vm.isTruthy(b))

	case OpNot:
		a := vm.pop()
		vm.push(!vm.isTruthy(a))

	case OpJump:
		vm.ip = instr.Arg

	case OpJumpIfTrue:
		val := vm.pop()
		if vm.isTruthy(val) {
			vm.ip = instr.Arg
		}

	case OpJumpIfFalse:
		val := vm.pop()
		if !vm.isTruthy(val) {
			vm.ip = instr.Arg
		}

	case OpCall:
		args := make([]Value, instr.Arg)
		for i := instr.Arg - 1; i >= 0; i-- {
			args[i] = vm.pop()
		}
		fn := vm.pop()
		result, err := vm.callFunction(fn, args)
		if err != nil {
			return err
		}
		vm.push(result)

	case OpReturn:
		vm.returning = true

	case OpArray:
		elements := make([]Value, instr.Arg)
		for i := instr.Arg - 1; i >= 0; i-- {
			elements[i] = vm.pop()
		}
		vm.push(elements)

	case OpMap:
		pairs := make(map[string]Value)
		for i := 0; i < instr.Arg; i++ {
			v := vm.pop()
			k := vm.pop()
			pairs[toString(k)] = v
		}
		vm.push(pairs)

	case OpIndex:
		index := vm.pop()
		obj := vm.pop()
		val, err := vm.getIndex(obj, index)
		if err != nil {
			return err
		}
		vm.push(val)

	case OpSetIndex:
		// For index assignment, we need the value, object, and index
		// But the order depends on how compileAssignExpr sets them
		// Let's assume: value, object are on stack, index follows
		// This needs to be adjusted based on the compiler
		vm.pop() // Placeholder

	case OpMember:
		prop := vm.pop()
		obj := vm.pop()
		val, err := vm.getMember(obj, toString(prop))
		if err != nil {
			return err
		}
		vm.push(val)

	case OpTernary:
		// Handled by jumps in compiler

	case OpBreak:
		vm.breaking = true

	case OpContinue:
		vm.continueing = true

	case OpThrow:
		val := vm.pop()
		return &ThrowError{Value: val}

	case OpUnpack:
		// Unpack array/map into multiple values on the stack
		val := vm.pop()
		count := instr.Arg

		switch v := val.(type) {
		case []Value:
			// Push elements in reverse order so they can be popped in order
			for i := count - 1; i >= 0; i-- {
				if i < len(v) {
					vm.push(v[i])
				} else {
					vm.push(nil)
				}
			}
		case map[string]Value:
			// For maps, we can't unpack in order without knowing the keys
			// The compiler will handle this differently
			// For now, push nil values
			for i := 0; i < count; i++ {
				vm.push(nil)
			}
		default:
			// Single value - push it for first variable, nil for rest
			vm.push(val)
			for i := 1; i < count; i++ {
				vm.push(nil)
			}
		}

	case OpHalt:
		// Stop execution

	default:
		return fmt.Errorf("unknown opcode: %v", instr.Op)
	}

	return nil
}

func (vm *VM) push(val Value) {
	vm.stack = append(vm.stack, val)
}

func (vm *VM) pop() Value {
	if len(vm.stack) == 0 {
		return nil
	}
	val := vm.stack[len(vm.stack)-1]
	vm.stack = vm.stack[:len(vm.stack)-1]
	return val
}

func (vm *VM) isTruthy(val Value) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	case []Value:
		return len(v) > 0
	case map[string]Value:
		return len(v) > 0
	}
	return true
}

func (vm *VM) equal(a, b Value) bool {
	return valuesEqual(a, b)
}

func (vm *VM) compare(a, b Value) int {
	af, bf := vm.toFloat(a), vm.toFloat(b)
	if af < bf {
		return -1
	} else if af > bf {
		return 1
	}
	return 0
}

func (vm *VM) toFloat(val Value) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	}
	return 0
}

func (vm *VM) add(a, b Value) Value {
	// String concatenation
	if sa, ok := a.(string); ok {
		if sb, ok := b.(string); ok {
			return sa + sb
		}
		return sa + toString(b)
	}

	// Numeric addition
	af, bf := vm.toFloat(a), vm.toFloat(b)

	// Try to preserve int type
	if ai, ok := a.(int); ok {
		if bi, ok := b.(int); ok {
			return ai + bi
		}
	}

	return af + bf
}

func (vm *VM) sub(a, b Value) Value {
	if ai, ok := a.(int); ok {
		if bi, ok := b.(int); ok {
			return ai - bi
		}
	}
	return vm.toFloat(a) - vm.toFloat(b)
}

func (vm *VM) mul(a, b Value) Value {
	if ai, ok := a.(int); ok {
		if bi, ok := b.(int); ok {
			return ai * bi
		}
	}
	return vm.toFloat(a) * vm.toFloat(b)
}

func (vm *VM) div(a, b Value) Value {
	bf := vm.toFloat(b)
	if bf == 0 {
		return nil
	}
	return vm.toFloat(a) / bf
}

func (vm *VM) mod(a, b Value) Value {
	ai, aok := a.(int)
	bi, bok := b.(int)
	if aok && bok {
		if bi == 0 {
			return nil
		}
		return ai % bi
	}
	af, bf := vm.toFloat(a), vm.toFloat(b)
	if bf == 0 {
		return nil
	}
	return float64(int(af) % int(bf))
}

func (vm *VM) neg(a Value) Value {
	switch v := a.(type) {
	case int:
		return -v
	case int64:
		return -v
	case float64:
		return -v
	}
	return -vm.toFloat(a)
}

func (vm *VM) getIndex(obj, index Value) (Value, error) {
	switch o := obj.(type) {
	case []Value:
		idx := vm.toInt(index)
		if idx < 0 || idx >= len(o) {
			return nil, nil
		}
		return o[idx], nil
	case map[string]Value:
		key := toString(index)
		return o[key], nil
	}
	return nil, fmt.Errorf("cannot index type %T", obj)
}

func (vm *VM) getMember(obj Value, name string) (Value, error) {
	switch o := obj.(type) {
	case map[string]Value:
		return o[name], nil
	case []Value:
		switch name {
		case "length":
			return len(o), nil
		}
	}
	return nil, fmt.Errorf("cannot access member %s of type %T", name, obj)
}

func (vm *VM) toInt(val Value) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func (vm *VM) callFunction(fn Value, args []Value) (Value, error) {
	switch f := fn.(type) {
	case *UserFunc:
		return vm.callUserFunc(f, args)
	case Callable:
		return f.Call(args)
	case map[string]Value:
		// Could be an object with __call
		if call, ok := f["__call"]; ok {
			if callable, ok := call.(Callable); ok {
				return callable.Call(args)
			}
		}
	}

	return nil, fmt.Errorf("not callable: %T", fn)
}

func (vm *VM) callUserFunc(fn *UserFunc, args []Value) (Value, error) {
	// Create new scope
	oldVars := vm.vars
	vm.vars = make(map[string]Value)

	// Copy parent scope
	for k, v := range oldVars {
		vm.vars[k] = v
	}

	// Bind parameters
	for i, param := range fn.Params {
		if i < len(args) {
			vm.vars[param] = args[i]
		} else {
			vm.vars[param] = nil
		}
	}

	// Execute function bytecode if available
	if fn.Bytecode != nil {
		funcVM := NewVM(fn.Bytecode, vm.globals)
		result, err := funcVM.Run()
		vm.vars = oldVars
		return result, err
	}

	// Otherwise execute the body statements directly
	var result Value
	for _, stmt := range fn.Body.Statements {
		// Use interpreter for function body
		interp := &Interpreter{ctx: vm.ctx}
		val, err := interp.executeStmt(stmt)
		if err != nil {
			vm.vars = oldVars
			return nil, err
		}
		result = val
	}

	vm.vars = oldVars
	return result, nil
}

// RunBytecode compiles and runs source code using the bytecode VM.
func RunBytecode(source string, globals map[string]Value) (Value, error) {
	bytecode, err := CompileString(source)
	if err != nil {
		return nil, err
	}

	vm := NewVM(bytecode, globals)
	return vm.Run()
}
