package xxscript

import (
	"testing"
)

// Test print function
func TestInterpreter_PrintFunction(t *testing.T) {
	input := `print("hello")`
	_, err := Run(input, nil)
	if err != nil {
		t.Errorf("Execution error: %v", err)
	}
}

// Test sprintf function
func TestInterpreter_SprintfFunction(t *testing.T) {
	input := `
		var s = sprintf("Hello %s", "Alice")
		s
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	expected := "Hello Alice"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// Test jsonParse function
func TestInterpreter_JSONParseFunction(t *testing.T) {
	input := `
		var obj = jsonParse("{\"name\": \"Alice\", \"age\": 30}")
		obj.name
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "Alice" {
		t.Errorf("Expected 'Alice', got %v", result)
	}
}

// Test jsonParse with array
func TestInterpreter_JSONParseArray(t *testing.T) {
	input := `
		var arr = jsonParse("[1, 2, 3]")
		arr[1]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(2), result) {
		t.Errorf("Expected 2, got %v", result)
	}
}

// Test null handling
func TestInterpreter_NullHandling(t *testing.T) {
	input := `
		var x = null
		x == null
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != true {
		t.Errorf("Expected true, got %v", result)
	}
}

// Test nested objects
func TestInterpreter_NestedObjects(t *testing.T) {
	input := `
		var obj = {"person": {"name": "Alice", "age": 30}}
		obj.person.name
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "Alice" {
		t.Errorf("Expected 'Alice', got %v", result)
	}
}

// Test object property assignment
func TestInterpreter_ObjectPropertyAssignment(t *testing.T) {
	input := `
		var obj = {}
		obj.name = "Bob"
		obj.name
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "Bob" {
		t.Errorf("Expected 'Bob', got %v", result)
	}
}

// Test array assignment
func TestInterpreter_ArrayAssignment(t *testing.T) {
	input := `
		var arr = [1, 2, 3]
		arr[1] = 20
		arr[1]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(20), result) {
		t.Errorf("Expected 20, got %v", result)
	}
}

// Test unary minus
func TestInterpreter_UnaryMinus(t *testing.T) {
	input := `
		var x = -5
		x
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(-5), result) {
		t.Errorf("Expected -5, got %v", result)
	}
}

// Test error propagation
func TestInterpreter_ErrorPropagation(t *testing.T) {
	input := `
		func foo() {
			throw "error from foo"
		}
		try {
			foo()
		} catch (e) {
			e
		}
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "error from foo" {
		t.Errorf("Expected 'error from foo', got %v", result)
	}
}

// Test string concatenation
func TestInterpreter_StringConcatenation(t *testing.T) {
	input := `"hello" + " " + "world"`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "hello world" {
		t.Errorf("Expected 'hello world', got %v", result)
	}
}

// Test logical short-circuit
func TestInterpreter_LogicalShortCircuit(t *testing.T) {
	input := `false && (1 / 0)`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != false {
		t.Errorf("Expected false, got %v", result)
	}
}

// Test nested function calls
func TestInterpreter_NestedFunctionCalls(t *testing.T) {
	input := `
		func double(x) {
			return x * 2
		}
		func addOne(x) {
			return x + 1
		}
		double(addOne(5))
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(12), result) {
		t.Errorf("Expected 12, got %v", result)
	}
}