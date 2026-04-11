package xxscript

// Opcode represents a bytecode instruction opcode.
type Opcode byte

const (
	// Stack operations
	OpPush Opcode = iota // Push constant
	OpPop                // Pop top of stack
	OpDup                // Duplicate top of stack

	// Variable operations
	OpLoadVar     // Load variable onto stack
	OpStoreVar    // Store top of stack to variable
	OpLoadGlobal  // Load global variable
	OpStoreGlobal // Store to global variable

	// Literals
	OpNull  // Push null
	OpTrue  // Push true
	OpFalse // Push false

	// Arithmetic
	OpAdd // Add top two values
	OpSub // Subtract top two values
	OpMul // Multiply top two values
	OpDiv // Divide top two values
	OpMod // Modulo top two values
	OpNeg // Negate top of stack

	// Comparison
	OpEq // Equal
	OpNe // Not equal
	OpLt // Less than
	OpLe // Less than or equal
	OpGt // Greater than
	OpGe // Greater than or equal

	// Logical
	OpAnd // Logical and
	OpOr  // Logical or
	OpNot // Logical not

	// Control flow
	OpJump        // Unconditional jump
	OpJumpIfTrue  // Jump if top is true
	OpJumpIfFalse // Jump if top is false

	// Functions
	OpCall    // Call function
	OpReturn  // Return from function
	OpClosure // Create closure

	// Data structures
	OpArray     // Create array
	OpMap       // Create map
	OpIndex     // Index access
	OpSetIndex  // Index assignment
	OpMember    // Member access
	OpSetMember // Member assignment

	// Compound assignment
	OpAddAssign
	OpSubAssign
	OpMulAssign
	OpDivAssign
	OpModAssign

	// Increment/Decrement
	OpInc
	OpDec

	// Ternary
	OpTernary

	// For loops
	OpBreak
	OpContinue

	// Throw/Try
	OpThrow
	OpTry
	OpEndTry
	OpCatch

	// Switch
	OpSwitch
	OpCase
	OpDefault
	OpEndSwitch

	// Multi-return / Destructuring
	OpUnpack // Unpack array into multiple variables (Arg = count)

	// Misc
	OpHalt // Stop execution
)

// Instruction represents a bytecode instruction.
type Instruction struct {
	Op   Opcode
	Arg  int    // Integer argument (e.g., constant index, jump target)
	ArgS string // String argument (e.g., variable name)
}

// Bytecode represents compiled bytecode.
type Bytecode struct {
	Instructions []Instruction
	Constants    []Value
	Lines        []int // Line numbers for debugging
}

// NewBytecode creates a new bytecode container.
func NewBytecode() *Bytecode {
	return &Bytecode{
		Instructions: make([]Instruction, 0),
		Constants:    make([]Value, 0),
		Lines:        make([]int, 0),
	}
}

// Emit adds an instruction to the bytecode.
func (b *Bytecode) Emit(op Opcode, arg int, argS string) int {
	pos := len(b.Instructions)
	b.Instructions = append(b.Instructions, Instruction{Op: op, Arg: arg, ArgS: argS})
	b.Lines = append(b.Lines, 0) // Will be set by compiler
	return pos
}

// EmitWithLine adds an instruction with a line number.
func (b *Bytecode) EmitWithLine(op Opcode, arg int, argS string, line int) int {
	pos := len(b.Instructions)
	b.Instructions = append(b.Instructions, Instruction{Op: op, Arg: arg, ArgS: argS})
	b.Lines = append(b.Lines, line)
	return pos
}

// SetLine sets the line number for the last instruction.
func (b *Bytecode) SetLine(line int) {
	if len(b.Lines) > 0 {
		b.Lines[len(b.Lines)-1] = line
	}
}

// SetLineAt sets the line number for an instruction at a specific position.
func (b *Bytecode) SetLineAt(pos int, line int) {
	if pos >= 0 && pos < len(b.Lines) {
		b.Lines[pos] = line
	}
}

// AddConstant adds a constant value and returns its index.
func (b *Bytecode) AddConstant(v Value) int {
	for i, c := range b.Constants {
		if valuesEqual(c, v) {
			return i
		}
	}
	idx := len(b.Constants)
	b.Constants = append(b.Constants, v)
	return idx
}

// PatchJump updates a jump instruction's target.
func (b *Bytecode) PatchJump(pos int, target int) {
	b.Instructions[pos].Arg = target
}
