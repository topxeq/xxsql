package xxscript

import (
	"testing"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input    string
		expected []TokenType
	}{
		{
			input:    "var x = 10",
			expected: []TokenType{TokVar, TokIdent, TokAssign, TokNumber, TokEOF},
		},
		{
			input:    "if (x > 5) { }",
			expected: []TokenType{TokIf, TokLParen, TokIdent, TokGt, TokNumber, TokRParen, TokLBrace, TokRBrace, TokEOF},
		},
		{
			input:    `"hello world"`,
			expected: []TokenType{TokString, TokEOF},
		},
		{
			input:    "true && false",
			expected: []TokenType{TokBool, TokAnd, TokBool, TokEOF},
		},
		{
			input:    "x == y",
			expected: []TokenType{TokIdent, TokEq, TokIdent, TokEOF},
		},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		for i, expected := range tt.expected {
			tok := lexer.NextToken()
			if tok.Type != expected {
				t.Errorf("Test %d: expected %v, got %v (value: %q)", i, expected, tok.Type, tok.Value)
			}
		}
	}
}

func TestParser(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "var statement",
			input: "var x = 10",
		},
		{
			name:  "if statement",
			input: "if (x > 5) { var y = 1 }",
		},
		{
			name:  "for loop",
			input: "for (var i = 0; i < 10; i = i + 1) { }",
		},
		{
			name:  "function call",
			input: "print(\"hello\")",
		},
		{
			name:  "member access",
			input: "http.json({\"status\": \"ok\"})",
		},
		{
			name:  "array literal",
			input: "var arr = [1, 2, 3]",
		},
		{
			name:  "map literal",
			input: "var obj = {\"name\": \"test\", \"value\": 42}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog, err := Parse(tt.input)
			if err != nil {
				t.Errorf("Parse error: %v", err)
				return
			}
			if len(prog.Statements) == 0 {
				t.Errorf("Expected statements, got empty program")
			}
		})
	}
}

func TestInterpreter(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
	}{
		{
			name:     "number",
			input:    "42",
			expected: 42.0,
		},
		{
			name:     "string",
			input:    `"hello"`,
			expected: "hello",
		},
		{
			name:     "boolean",
			input:    "true",
			expected: true,
		},
		{
			name:     "arithmetic",
			input:    "10 + 5 * 2",
			expected: 20.0,
		},
		{
			name:     "comparison",
			input:    "5 > 3",
			expected: true,
		},
		{
			name:     "string concat",
			input:    `"hello" + " world"`,
			expected: "hello world",
		},
		{
			name:     "array literal",
			input:    "[1, 2, 3]",
			expected: []Value{1.0, 2.0, 3.0},
		},
		{
			name:     "map literal",
			input:    `{"x": 1}`,
			expected: map[string]Value{"x": 1.0},
		},
		{
			name:     "var and reference",
			input:    "var x = 10\n x + 5",
			expected: 15.0,
		},
		{
			name:     "if true",
			input:    "if (true) { 1 } else { 2 }",
			expected: 1.0,
		},
		{
			name:     "if false",
			input:    "if (false) { 1 } else { 2 }",
			expected: 2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Run(tt.input, nil)
			if err != nil {
				t.Errorf("Run error: %v", err)
				return
			}
			if !compareValues(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestInterpreterBuiltin(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "len string",
			input: `len("hello")`,
		},
		{
			name:  "len array",
			input: "len([1, 2, 3])",
		},
		{
			name:  "typeof",
			input: `typeof("hello")`,
		},
		{
			name:  "int",
			input: "int(3.14)",
		},
		{
			name:  "string",
			input: "string(42)",
		},
		{
			name:  "keys",
			input: `keys({"a": 1, "b": 2})`,
		},
		{
			name:  "range",
			input: "range(5)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Errorf("Run error: %v", err)
			}
		})
	}
}

func TestInterpreterFunctions(t *testing.T) {
	input := `
func add(a, b) {
	return a + b
}
add(3, 4)
`

	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if result != 7.0 {
		t.Errorf("Expected 7.0, got %v", result)
	}
}

func TestInterpreterLoops(t *testing.T) {
	// Test for loop
	input := `
var sum = 0
for (var i = 1; i <= 5; i = i + 1) {
	sum = sum + i
}
sum
`

	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if result != 15.0 {
		t.Errorf("Expected 15.0, got %v", result)
	}

	// Test while loop
	input2 := `
var count = 0
var i = 0
while (i < 5) {
	count = count + 1
	i = i + 1
}
count
`

	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if result2 != 5.0 {
		t.Errorf("Expected 5.0, got %v", result2)
	}
}

func compareValues(a, b Value) bool {
	// Handle nil
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare types
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av == bv
		case int64:
			return int64(av) == bv
		case float64:
			return float64(av) == bv
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av == int64(bv)
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av == float64(bv)
		case int64:
			return av == float64(bv)
		case float64:
			return av == bv
		}
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case []Value:
		bv, ok := b.([]Value)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !compareValues(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]Value:
		bv, ok := b.(map[string]Value)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !compareValues(v, bv[k]) {
				return false
			}
		}
		return true
	}

	return false
}