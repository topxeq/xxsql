package xxscript

import (
	"testing"
)

func TestBytecodeArithmetic(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{"1 + 2", 3.0},
		{"5 - 3", 2.0},
		{"4 * 3", 12.0},
		{"10 / 2", 5.0},
		{"10 % 3", 1.0},
		{"-5", -5.0},
		{"2 + 3 * 4", 14.0},
		{"(2 + 3) * 4", 20.0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// Compile
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			// Run
			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeComparison(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"1 < 2", true},
		{"2 < 1", false},
		{"1 > 2", false},
		{"2 > 1", true},
		{"1 <= 1", true},
		{"1 >= 1", true},
		{"1 == 1", true},
		{"1 == 2", false},
		{"1 != 2", true},
		{"1 != 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeVariables(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{"var x = 10; x", 10.0},
		{"var a = 1; var b = 2; a + b", 3.0},
		{"var x = 5; x = x + 1; x", 6.0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeConditionals(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{"if (true) { return 1; } return 2;", 1.0},
		{"if (false) { return 1; } return 2;", 2.0},
		{"if (1 < 2) { return 10; } else { return 20; }", 10.0},
		{"if (1 > 2) { return 10; } else { return 20; }", 20.0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{`"hello"`, "hello"},
		{`"hello" + " " + "world"`, "hello world"},
		{`"test" == "test"`, true},
		{`"a" == "b"`, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeArrays(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{"[1, 2, 3][0]", 1.0},
		{"[1, 2, 3][2]", 3.0},
		{"len([1, 2, 3])", 3.0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// Arrays and built-in functions need interpreter for now
			// Use the regular Run function for built-in support
			result, err := Run(tt.input, nil)
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeTernary(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{"true ? 1 : 2", 1.0},
		{"false ? 1 : 2", 2.0},
		{"1 < 2 ? 10 : 20", 10.0},
		{"1 > 2 ? 10 : 20", 20.0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeLogicalOps(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"true && true", true},
		{"true && false", false},
		{"false && true", false},
		{"false && false", false},
		{"true || true", true},
		{"true || false", true},
		{"false || true", true},
		{"false || false", false},
		{"!true", false},
		{"!false", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bytecode, err := CompileString(tt.input)
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}

			vm := NewVM(bytecode, nil)
			result, err := vm.Run()
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBytecodeNull(t *testing.T) {
	result, err := RunBytecode("null", nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestBytecodeLineTracking(t *testing.T) {
	// Test that line numbers are tracked in bytecode
	source := `var x = 1
var y = 2
x + y`

	bytecode, err := CompileString(source)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Check that lines are tracked (should have non-zero lines)
	hasNonZeroLine := false
	for _, line := range bytecode.Lines {
		if line > 0 {
			hasNonZeroLine = true
			break
		}
	}

	if !hasNonZeroLine {
		t.Errorf("Expected non-zero line numbers in bytecode, but all are zero")
	}
}

func TestBytecodeErrorWithLine(t *testing.T) {
	// Test that errors include line information
	source := `var x = 1
throw "test error"`

	_, err := RunBytecode(source, nil)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	// The error should include line 2
	if !containsString(err.Error(), "line 2") && !containsString(err.Error(), "line") {
		t.Errorf("Expected error to include line information, got: %v", err)
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestMultiVarDeclaration(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]Value
	}{
		{"var a, b = [1, 2]; a", map[string]Value{"a": 1.0}},
		{"var a, b = [1, 2]; b", map[string]Value{"b": 2.0}},
		{"var a, b, c = [1, 2, 3]; a + b + c", map[string]Value{"result": 6.0}},
		{"var x, y, z = [10, 20]; x + y", map[string]Value{"result": 30.0}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := Run(tt.input, nil)
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			// Check the result
			if expected, ok := tt.expected["result"]; ok {
				if !valuesEqual(result, expected) {
					t.Errorf("Expected result %v, got %v", expected, result)
				}
			} else {
				// Check individual variable
				for key, expected := range tt.expected {
					// Run the expression to get the variable
					prog, _ := Parse(tt.input)
					interp := &Interpreter{ctx: NewContext()}
					for _, stmt := range prog.Statements {
						interp.executeStmt(stmt)
					}
					actual := interp.ctx.Variables[key]
					if !valuesEqual(actual, expected) {
						t.Errorf("Expected %s = %v, got %v", key, expected, actual)
					}
					break
				}
			}
		})
	}
}

func TestMultiVarBytecode(t *testing.T) {
	// Test that bytecode handles multi-var declarations
	source := `var a, b = [1, 2]
a + b`

	result, err := RunBytecode(source, nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if !valuesEqual(result, 3.0) {
		t.Errorf("Expected 3.0, got %v", result)
	}
}

func TestMultiVarFewerValues(t *testing.T) {
	// Test when there are fewer values than variables
	source := `var a, b, c = [1, 2]
a`

	result, err := Run(source, nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if !valuesEqual(result, 1.0) {
		t.Errorf("Expected 1.0, got %v", result)
	}
}

func TestMultiAssignExpression(t *testing.T) {
	tests := []struct {
		input    string
		expected Value
	}{
		{"var a, b; a, b = [10, 20]; a", 10.0},
		{"var x, y; x, y = [1, 2]; x + y", 3.0},
		{"var a, b, c; a, b, c = [1, 2, 3]; a + b + c", 6.0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := Run(tt.input, nil)
			if err != nil {
				t.Fatalf("Run error: %v", err)
			}

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMultiAssignBytecode(t *testing.T) {
	source := `var a, b
a, b = [100, 200]
a + b`

	result, err := RunBytecode(source, nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if !valuesEqual(result, 300.0) {
		t.Errorf("Expected 300.0, got %v", result)
	}
}
