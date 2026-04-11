package xxscript

// This file contains the short-circuit compilation logic for && and || operators.

// compileAndExpr compiles a && b with proper short-circuit evaluation.
// The result is always left on the stack.
func (c *Compiler) compileAndExpr(expr *BinaryExpr) error {
	// a && b:
	// - Evaluate a
	// - If a is falsy, result is false (skip b)
	// - If a is truthy, result is b
	if err := c.compileExpression(expr.Left); err != nil {
		return err
	}
	// Jump to "push false" if left is falsy
	falseJump := c.emit(OpJumpIfFalse, 0, "")
	// Left was truthy, pop it and evaluate right
	c.emit(OpPop, 0, "")
	if err := c.compileExpression(expr.Right); err != nil {
		return err
	}
	// Jump over the "push false" code
	endJump := c.emit(OpJump, 0, "")
	// Patch: here we push false (left was falsy)
	c.bytecode.PatchJump(falseJump, len(c.bytecode.Instructions))
	c.emit(OpPop, 0, "")   // Pop the left value
	c.emit(OpFalse, 0, "") // Push false as result
	// End
	c.bytecode.PatchJump(endJump, len(c.bytecode.Instructions))
	return nil
}

// compileOrExpr compiles a || b with proper short-circuit evaluation.
// The result is always left on the stack.
func (c *Compiler) compileOrExpr(expr *BinaryExpr) error {
	// a || b:
	// - Evaluate a
	// - If a is truthy, result is true (skip b)
	// - If a is falsy, result is b
	if err := c.compileExpression(expr.Left); err != nil {
		return err
	}
	// Jump to "push true" if left is truthy
	trueJump := c.emit(OpJumpIfTrue, 0, "")
	// Left was falsy, pop it and evaluate right
	c.emit(OpPop, 0, "")
	if err := c.compileExpression(expr.Right); err != nil {
		return err
	}
	// Jump over the "push true" code
	endJump := c.emit(OpJump, 0, "")
	// Patch: here we push true (left was truthy)
	c.bytecode.PatchJump(trueJump, len(c.bytecode.Instructions))
	c.emit(OpPop, 0, "")  // Pop the left value
	c.emit(OpTrue, 0, "") // Push true as result
	// End
	c.bytecode.PatchJump(endJump, len(c.bytecode.Instructions))
	return nil
}
