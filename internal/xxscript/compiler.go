package xxscript

import (
	"fmt"
)

// Compiler compiles AST to bytecode.
type Compiler struct {
	bytecode    *Bytecode
	loopBreaks  []int // Positions to patch break jumps
	loopConts   []int // Positions to patch continue jumps
	currentLine int
	errors      []string
}

// NewCompiler creates a new compiler.
func NewCompiler() *Compiler {
	return &Compiler{
		bytecode: NewBytecode(),
	}
}

// Compile compiles a program to bytecode.
func (c *Compiler) Compile(program *Program) (*Bytecode, error) {
	for _, stmt := range program.Statements {
		if err := c.compileStatement(stmt); err != nil {
			return nil, err
		}
	}
	c.emit(OpHalt, 0, "")

	if len(c.errors) > 0 {
		return nil, fmt.Errorf("compile errors: %v", c.errors)
	}

	return c.bytecode, nil
}

// emit emits an instruction with the current line number.
func (c *Compiler) emit(op Opcode, arg int, argS string) int {
	return c.bytecode.EmitWithLine(op, arg, argS, c.currentLine)
}

// compileStatement compiles a statement.
func (c *Compiler) compileStatement(stmt Statement) error {
	c.currentLine = stmt.Line()

	switch s := stmt.(type) {
	case *VarStmt:
		return c.compileVarStmt(s)
	case *ExprStmt:
		return c.compileExprStmt(s)
	case *BlockStmt:
		return c.compileBlockStmt(s)
	case *IfStmt:
		return c.compileIfStmt(s)
	case *ForStmt:
		return c.compileForStmt(s)
	case *WhileStmt:
		return c.compileWhileStmt(s)
	case *FuncStmt:
		return c.compileFuncStmt(s)
	case *ReturnStmt:
		return c.compileReturnStmt(s)
	case *BreakStmt:
		return c.compileBreakStmt(s)
	case *ContinueStmt:
		return c.compileContinueStmt(s)
	case *TryStmt:
		return c.compileTryStmt(s)
	case *ThrowStmt:
		return c.compileThrowStmt(s)
	case *SwitchStmt:
		return c.compileSwitchStmt(s)
	default:
		return fmt.Errorf("unknown statement type: %T", stmt)
	}
}

func (c *Compiler) compileVarStmt(stmt *VarStmt) error {
	if stmt.Value != nil {
		if err := c.compileExpression(stmt.Value); err != nil {
			return err
		}
	} else {
		c.emit(OpNull, 0, "")
	}

	// Handle multi-variable destructuring
	if len(stmt.Names) > 1 {
		// Emit OpUnpack with the count
		c.emit(OpUnpack, len(stmt.Names), "")
		// Now store each value
		for _, name := range stmt.Names {
			c.emit(OpStoreVar, 0, name)
			c.emit(OpPop, 0, "")
		}
		return nil
	}

	c.emit(OpStoreVar, 0, stmt.Name)
	// Pop the stored value from the stack (var statements don't leave values)
	c.emit(OpPop, 0, "")
	return nil
}

func (c *Compiler) compileExprStmt(stmt *ExprStmt) error {
	if err := c.compileExpression(stmt.Expr); err != nil {
		return err
	}
	c.emit(OpPop, 0, "")
	return nil
}

func (c *Compiler) compileBlockStmt(stmt *BlockStmt) error {
	for _, s := range stmt.Statements {
		if err := c.compileStatement(s); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compiler) compileIfStmt(stmt *IfStmt) error {
	// Compile condition
	if err := c.compileExpression(stmt.Condition); err != nil {
		return err
	}

	// Jump to else/end if condition is false
	elseJump := c.emit(OpJumpIfFalse, 0, "")

	// Compile then block
	if err := c.compileBlockStmt(stmt.Then); err != nil {
		return err
	}

	// Jump over else block
	endJump := c.emit(OpJump, 0, "")

	// Patch else jump
	c.bytecode.PatchJump(elseJump, len(c.bytecode.Instructions))

	// Compile else block if exists
	if stmt.Else != nil {
		if err := c.compileStatement(stmt.Else); err != nil {
			return err
		}
	}

	// Patch end jump
	c.bytecode.PatchJump(endJump, len(c.bytecode.Instructions))

	return nil
}

func (c *Compiler) compileForStmt(stmt *ForStmt) error {
	// Compile init
	if stmt.Init != nil {
		if err := c.compileStatement(stmt.Init); err != nil {
			return err
		}
	}

	// Start of loop (condition check)
	loopStart := len(c.bytecode.Instructions)

	// Compile condition
	if stmt.Condition != nil {
		if err := c.compileExpression(stmt.Condition); err != nil {
			return err
		}
		// Jump out of loop if condition is false
		breakJump := c.emit(OpJumpIfFalse, 0, "")
		c.loopBreaks = append(c.loopBreaks, breakJump)
	}

	// Save break/continue positions
	oldBreaks := c.loopBreaks
	oldConts := c.loopConts
	c.loopBreaks = make([]int, 0)
	c.loopConts = make([]int, 0)

	// Compile body
	if err := c.compileBlockStmt(stmt.Body); err != nil {
		return err
	}

	// Continue target (for update)
	continueTarget := len(c.bytecode.Instructions)

	// Compile update
	if stmt.Update != nil {
		if err := c.compileStatement(stmt.Update); err != nil {
			return err
		}
	}

	// Jump back to condition
	c.emit(OpJump, loopStart, "")

	// Patch break jumps
	endPos := len(c.bytecode.Instructions)
	for _, pos := range c.loopBreaks {
		c.bytecode.PatchJump(pos, endPos)
	}

	// Patch continue jumps
	for _, pos := range c.loopConts {
		c.bytecode.PatchJump(pos, continueTarget)
	}

	// Restore outer break/continue
	c.loopBreaks = oldBreaks
	c.loopConts = oldConts

	// Patch condition break jump
	if stmt.Condition != nil {
		c.bytecode.PatchJump(c.loopBreaks[len(c.loopBreaks)-1], endPos)
	}

	return nil
}

func (c *Compiler) compileWhileStmt(stmt *WhileStmt) error {
	// Start of loop
	loopStart := len(c.bytecode.Instructions)

	// Compile condition
	if err := c.compileExpression(stmt.Condition); err != nil {
		return err
	}

	// Jump out if false
	breakJump := c.emit(OpJumpIfFalse, 0, "")

	// Save break/continue positions
	oldBreaks := c.loopBreaks
	oldConts := c.loopConts
	c.loopBreaks = []int{breakJump}
	c.loopConts = make([]int, 0)

	// Compile body
	if err := c.compileBlockStmt(stmt.Body); err != nil {
		return err
	}

	// Jump back to start
	c.emit(OpJump, loopStart, "")

	// Patch break jumps
	endPos := len(c.bytecode.Instructions)
	for _, pos := range c.loopBreaks {
		c.bytecode.PatchJump(pos, endPos)
	}
	for _, pos := range c.loopConts {
		c.bytecode.PatchJump(pos, loopStart)
	}

	// Restore
	c.loopBreaks = oldBreaks
	c.loopConts = oldConts

	return nil
}

func (c *Compiler) compileFuncStmt(stmt *FuncStmt) error {
	// Extract parameter names, default values, and rest parameter index
	paramNames := make([]string, len(stmt.Params))
	defaultValues := make([]Expression, len(stmt.Params))
	restParamIndex := -1
	for idx, p := range stmt.Params {
		paramNames[idx] = p.Name
		defaultValues[idx] = p.DefaultValue
		if p.IsRest {
			restParamIndex = idx
		}
	}

	// Create function constant
	fn := &UserFunc{
		Params:         paramNames,
		DefaultValues:  defaultValues,
		RestParamIndex: restParamIndex,
		Body:           stmt.Body,
	}

	// Compile function body into separate bytecode
	fnCompiler := NewCompiler()
	fnBytecode, err := fnCompiler.Compile(&Program{Statements: stmt.Body.Statements})
	if err != nil {
		return err
	}
	fn.Bytecode = fnBytecode

	// Add function as constant
	idx := c.bytecode.AddConstant(fn)
	c.emit(OpClosure, idx, stmt.Name)

	return nil
}

func (c *Compiler) compileReturnStmt(stmt *ReturnStmt) error {
	if stmt.Value != nil {
		if err := c.compileExpression(stmt.Value); err != nil {
			return err
		}
	} else {
		c.emit(OpNull, 0, "")
	}
	c.emit(OpReturn, 0, "")
	return nil
}

func (c *Compiler) compileBreakStmt(stmt *BreakStmt) error {
	pos := c.emit(OpJump, 0, "")
	c.loopBreaks = append(c.loopBreaks, pos)
	return nil
}

func (c *Compiler) compileContinueStmt(stmt *ContinueStmt) error {
	pos := c.emit(OpJump, 0, "")
	c.loopConts = append(c.loopConts, pos)
	return nil
}

func (c *Compiler) compileTryStmt(stmt *TryStmt) error {
	// Mark try start
	tryStart := c.emit(OpTry, 0, "")

	// Compile try block
	if err := c.compileBlockStmt(stmt.TryBlock); err != nil {
		return err
	}

	// Jump over catch
	catchJump := c.emit(OpJump, 0, "")

	// Mark catch start
	catchStart := len(c.bytecode.Instructions)
	c.bytecode.PatchJump(tryStart, catchStart)

	// Compile catch block
	if stmt.CatchBlock != nil {
		// Store exception in catch variable
		c.emit(OpStoreVar, 0, stmt.CatchVar)
		if err := c.compileBlockStmt(stmt.CatchBlock); err != nil {
			return err
		}
	}

	// Patch jump over catch
	c.bytecode.PatchJump(catchJump, len(c.bytecode.Instructions))

	return nil
}

func (c *Compiler) compileThrowStmt(stmt *ThrowStmt) error {
	if stmt.Error != nil {
		if err := c.compileExpression(stmt.Error); err != nil {
			return err
		}
	} else {
		c.emit(OpNull, 0, "")
	}
	c.emit(OpThrow, 0, "")
	return nil
}

func (c *Compiler) compileSwitchStmt(stmt *SwitchStmt) error {
	// Compile switch value
	if err := c.compileExpression(stmt.Value); err != nil {
		return err
	}

	// Collect case jumps
	caseJumps := make([]int, 0)
	var defaultJump int
	hasDefault := false

	// Compile each case
	for _, cse := range stmt.Cases {
		if cse.Values == nil {
			// Default case
			hasDefault = true
			defaultJump = c.emit(OpJump, 0, "")
			continue
		}

		// Duplicate switch value for comparison
		c.emit(OpDup, 0, "")

		// Compile case values (use first one for now, multiple values need special handling)
		for i, val := range cse.Values {
			if i > 0 {
				c.emit(OpDup, 0, "")
			}
			if err := c.compileExpression(val); err != nil {
				return err
			}
			c.emit(OpEq, 0, "")
			if i == 0 && len(cse.Values) > 1 {
				// First match, start or chain
			}
		}

		// Jump to case body if matched
		caseJump := c.emit(OpJumpIfTrue, 0, "")
		caseJumps = append(caseJumps, caseJump)
	}

	// Jump to end if no match
	endJump := c.emit(OpJump, 0, "")

	// Compile case bodies
	bodyStarts := make([]int, len(caseJumps))
	for i, cse := range stmt.Cases {
		if cse.Values == nil {
			continue // Skip default, handled separately
		}
		bodyStarts[i] = len(c.bytecode.Instructions)
		c.bytecode.PatchJump(caseJumps[i], bodyStarts[i])
		if err := c.compileBlockStmt(cse.Body); err != nil {
			return err
		}
		// Jump to end after body
		c.emit(OpJump, 0, "") // Will be patched later
	}

	// Default case
	if hasDefault {
		c.bytecode.PatchJump(defaultJump, len(c.bytecode.Instructions))
		for _, cse := range stmt.Cases {
			if cse.Values == nil {
				if err := c.compileBlockStmt(cse.Body); err != nil {
					return err
				}
				break
			}
		}
	}

	// End of switch
	endPos := len(c.bytecode.Instructions)
	c.bytecode.PatchJump(endJump, endPos)

	return nil
}

// compileExpression compiles an expression.
func (c *Compiler) compileExpression(expr Expression) error {
	switch e := expr.(type) {
	case *IdentExpr:
		c.emit(OpLoadVar, 0, e.Name)
	case *NumberExpr:
		idx := c.bytecode.AddConstant(e.Value)
		c.emit(OpPush, idx, "")
	case *StringExpr:
		idx := c.bytecode.AddConstant(e.Value)
		c.emit(OpPush, idx, "")
	case *BoolExpr:
		if e.Value {
			c.emit(OpTrue, 0, "")
		} else {
			c.emit(OpFalse, 0, "")
		}
	case *NullExpr:
		c.emit(OpNull, 0, "")
	case *ArrayExpr:
		for _, el := range e.Elements {
			if err := c.compileExpression(el); err != nil {
				return err
			}
		}
		c.emit(OpArray, len(e.Elements), "")
	case *MapExpr:
		for k, v := range e.Pairs {
			c.emit(OpPush, c.bytecode.AddConstant(k), "")
			if err := c.compileExpression(v); err != nil {
				return err
			}
		}
		c.emit(OpMap, len(e.Pairs), "")
	case *BinaryExpr:
		if err := c.compileBinaryExpr(e); err != nil {
			return err
		}
	case *UnaryExpr:
		if err := c.compileUnaryExpr(e); err != nil {
			return err
		}
	case *CallExpr:
		if err := c.compileCallExpr(e); err != nil {
			return err
		}
	case *MemberExpr:
		if err := c.compileExpression(e.Object); err != nil {
			return err
		}
		c.emit(OpPush, c.bytecode.AddConstant(e.Member), "")
		c.emit(OpMember, 0, "")
	case *IndexExpr:
		if err := c.compileExpression(e.Object); err != nil {
			return err
		}
		if err := c.compileExpression(e.Index); err != nil {
			return err
		}
		c.emit(OpIndex, 0, "")
	case *AssignExpr:
		if err := c.compileAssignExpr(e); err != nil {
			return err
		}
	case *TernaryExpr:
		if err := c.compileTernaryExpr(e); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown expression type: %T", expr)
	}
	return nil
}

func (c *Compiler) compileBinaryExpr(expr *BinaryExpr) error {
	// Short-circuit evaluation for && and ||
	if expr.Op == TokAnd {
		return c.compileAndExpr(expr)
	}

	if expr.Op == TokOr {
		return c.compileOrExpr(expr)
	}

	// Regular binary operators
	if err := c.compileExpression(expr.Left); err != nil {
		return err
	}
	if err := c.compileExpression(expr.Right); err != nil {
		return err
	}

	switch expr.Op {
	case TokPlus:
		c.emit(OpAdd, 0, "")
	case TokMinus:
		c.emit(OpSub, 0, "")
	case TokStar:
		c.emit(OpMul, 0, "")
	case TokSlash:
		c.emit(OpDiv, 0, "")
	case TokPercent:
		c.emit(OpMod, 0, "")
	case TokEq:
		c.emit(OpEq, 0, "")
	case TokNe:
		c.emit(OpNe, 0, "")
	case TokLt:
		c.emit(OpLt, 0, "")
	case TokLe:
		c.emit(OpLe, 0, "")
	case TokGt:
		c.emit(OpGt, 0, "")
	case TokGe:
		c.emit(OpGe, 0, "")
	default:
		return fmt.Errorf("unknown operator: %v", expr.Op)
	}

	return nil
}

func (c *Compiler) compileUnaryExpr(expr *UnaryExpr) error {
	if err := c.compileExpression(expr.Expr); err != nil {
		return err
	}

	switch expr.Op {
	case TokMinus:
		c.emit(OpNeg, 0, "")
	case TokNot:
		c.emit(OpNot, 0, "")
	default:
		return fmt.Errorf("unknown unary operator: %v", expr.Op)
	}

	return nil
}

func (c *Compiler) compileCallExpr(expr *CallExpr) error {
	// Compile arguments
	for _, arg := range expr.Args {
		if err := c.compileExpression(arg); err != nil {
			return err
		}
	}

	// Compile function
	if err := c.compileExpression(expr.Func); err != nil {
		return err
	}

	c.emit(OpCall, len(expr.Args), "")

	return nil
}

func (c *Compiler) compileAssignExpr(expr *AssignExpr) error {
	// Compile right side
	if err := c.compileExpression(expr.Value); err != nil {
		return err
	}

	// Handle multi-target assignment
	if len(expr.Lefts) > 1 {
		// Emit OpUnpack with the count
		c.emit(OpUnpack, len(expr.Lefts), "")
		// Now store each value
		for _, left := range expr.Lefts {
			if ident, ok := left.(*IdentExpr); ok {
				c.emit(OpStoreVar, 0, ident.Name)
				c.emit(OpPop, 0, "")
			}
		}
		return nil
	}

	// Handle assignment target
	switch t := expr.Left.(type) {
	case *IdentExpr:
		c.emit(OpStoreVar, 0, t.Name)
	case *IndexExpr:
		if err := c.compileExpression(t.Object); err != nil {
			return err
		}
		c.emit(OpSetIndex, 0, "")
	case *MemberExpr:
		if err := c.compileExpression(t.Object); err != nil {
			return err
		}
		c.emit(OpPush, c.bytecode.AddConstant(t.Member), "")
		c.emit(OpSetMember, 0, "")
	default:
		return fmt.Errorf("invalid assignment target: %T", t)
	}

	return nil
}

func (c *Compiler) compileTernaryExpr(expr *TernaryExpr) error {
	// Compile condition
	if err := c.compileExpression(expr.Condition); err != nil {
		return err
	}

	// Jump to else branch if false
	elseJump := c.emit(OpJumpIfFalse, 0, "")

	// Compile then branch
	if err := c.compileExpression(expr.TrueExpr); err != nil {
		return err
	}

	// Jump over else
	endJump := c.emit(OpJump, 0, "")

	// Else branch
	c.bytecode.PatchJump(elseJump, len(c.bytecode.Instructions))
	if err := c.compileExpression(expr.FalseExpr); err != nil {
		return err
	}

	// End
	c.bytecode.PatchJump(endJump, len(c.bytecode.Instructions))

	return nil
}

// CompileString compiles source code to bytecode.
func CompileString(source string) (*Bytecode, error) {
	program, err := Parse(source)
	if err != nil {
		return nil, err
	}

	compiler := NewCompiler()
	return compiler.Compile(program)
}
