package xxscript

import (
	"testing"
)

func TestCompiler_CompileForStmt(t *testing.T) {
	prog, err := Parse("for (var i = 0; i < 5; i = i + 1) { sum = sum + i }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileWhileStmt(t *testing.T) {
	prog, err := Parse("while (x < 10) { x = x + 1 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileFuncStmt(t *testing.T) {
	prog, err := Parse("func add(a, b) { return a + b }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileBreakStmt(t *testing.T) {
	prog, err := Parse("for (var i = 0; i < 10; i = i + 1) { if (i == 5) { break } }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileContinueStmt(t *testing.T) {
	prog, err := Parse("for (var i = 0; i < 10; i = i + 1) { if (i == 5) { continue } }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileTryStmt(t *testing.T) {
	prog, err := Parse("try { x = 1 } catch (e) { print(e) }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileSwitchStmt(t *testing.T) {
	prog, err := Parse("switch x { case 1: { break } default: { break } }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileCallExpr(t *testing.T) {
	prog, err := Parse("foo(1, 2, 3)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileAssignExpr(t *testing.T) {
	prog, err := Parse("x = 42")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileMemberAccess(t *testing.T) {
	prog, err := Parse("obj.field")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileIndexAccess(t *testing.T) {
	prog, err := Parse("arr[0]")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileThrowStmt(t *testing.T) {
	prog, err := Parse("throw \"error\"")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileReturnStmt(t *testing.T) {
	prog, err := Parse("return 42")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	c := NewCompiler()
	bc, err := c.Compile(prog)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}

func TestCompiler_CompileString(t *testing.T) {
	source := "var x = 10; x + 5"
	bc, err := CompileString(source)
	if err != nil {
		t.Fatalf("CompileString error: %v", err)
	}
	if bc == nil {
		t.Fatal("Expected non-nil bytecode")
	}
}
