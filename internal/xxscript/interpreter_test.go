package xxscript

import (
	"testing"
)

// Test basic expression evaluation
func TestInterpreter_EvalBasic(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"1 + 2", int64(3)},
		{"10 - 3", int64(7)},
		{"4 * 5", int64(20)},
		{"15 / 3", int64(5)},
		{"10 % 3", int64(1)},
		{"1 + 2 * 3", int64(7)},
		{"(1 + 2) * 3", int64(9)},
		{"\"hello\" + \" world\"", "hello world"},
		{"true", true},
		{"false", false},
		{"!true", false},
		{"!false", true},
		{"1 < 2", true},
		{"2 < 1", false},
		{"1 <= 1", true},
		{"1 > 2", false},
		{"2 > 1", true},
		{"2 >= 2", true},
		{"1 == 1", true},
		{"1 == 2", false},
		{"1 != 2", true},
		{"true && true", true},
		{"true && false", false},
		{"false || true", true},
		{"false || false", false},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !compareValues(tt.expected, result) {
			t.Errorf("For %s: expected %v (%T), got %v (%T)", tt.input, tt.expected, tt.expected, result, result)
		}
	}
}

// Test variable declarations and assignments
func TestInterpreter_Variables(t *testing.T) {
	input := `
		var x = 10
		var y = 20
		x + y
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(30), result) {
		t.Errorf("Expected 30, got %v", result)
	}
}

// Test if statements
func TestInterpreter_IfStatement(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{
			`
			var x = 10
			if (x > 5) {
				x = 100
			}
			x
			`,
			int64(100),
		},
		{
			`
			var x = 10
			if (x < 5) {
				x = 100
			} else {
				x = 200
			}
			x
			`,
			int64(200),
		},
		{
			`
			var x = 10
			if (x > 20) {
				x = 1
			} else if (x > 5) {
				x = 2
			} else {
				x = 3
			}
			x
			`,
			int64(2),
		},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Fatalf("Execution error: %v", err)
		}

		if !compareValues(tt.expected, result) {
			t.Errorf("Expected %v, got %v", tt.expected, result)
		}
	}
}

// Test for loops
func TestInterpreter_ForLoop(t *testing.T) {
	input := `
		var sum = 0
		for (var i = 1; i <= 5; i = i + 1) {
			sum = sum + i
		}
		sum
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(15), result) {
		t.Errorf("Expected 15, got %v", result)
	}
}

// Test while loops
func TestInterpreter_WhileLoop(t *testing.T) {
	input := `
		var count = 0
		var i = 0
		while (i < 5) {
			count = count + 1
			i = i + 1
		}
		count
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(5), result) {
		t.Errorf("Expected 5, got %v", result)
	}
}

// Test functions
func TestInterpreter_Functions(t *testing.T) {
	input := `
		func add(a, b) {
			return a + b
		}
		add(3, 4)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(7), result) {
		t.Errorf("Expected 7, got %v", result)
	}
}

// Test recursive function
func TestInterpreter_RecursiveFunction(t *testing.T) {
	input := `
		func factorial(n) {
			if (n <= 1) {
				return 1
			}
			return n * factorial(n - 1)
		}
		factorial(5)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(120), result) {
		t.Errorf("Expected 120, got %v", result)
	}
}

// Test arrays
func TestInterpreter_Arrays(t *testing.T) {
	input := `
		var arr = [1, 2, 3, 4, 5]
		arr[2]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(3), result) {
		t.Errorf("Expected 3, got %v", result)
	}
}

// Test objects
func TestInterpreter_Objects(t *testing.T) {
	input := `
		var obj = {"name": "Alice", "age": 30}
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

// Test built-in functions
func TestInterpreter_Builtins(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"len(\"hello\")", int64(5)},
		{"len([1, 2, 3])", int64(3)},
		{"upper(\"hello\")", "HELLO"},
		{"lower(\"HELLO\")", "hello"},
		{"trim(\"  hi  \")", "hi"},
		{"int(\"42\")", int64(42)},
		{"string(42)", "42"},
		{"abs(-5)", int64(5)},
		{"min(3, 1, 2)", int64(1)},
		{"max(3, 1, 2)", int64(3)},
		{"floor(3.7)", int64(3)},
		{"ceil(3.2)", int64(4)},
		{"round(3.5)", int64(4)},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !compareValues(tt.expected, result) {
			t.Errorf("For %s: expected %v (%T), got %v (%T)", tt.input, tt.expected, tt.expected, result, result)
		}
	}
}

// Test typeof
func TestInterpreter_Typeof(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"typeof(42)", "float"},
		{"typeof(\"hello\")", "string"},
		{"typeof(true)", "bool"},
		{"typeof([1, 2])", "array"},
		{"typeof(null)", "null"},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if result != tt.expected {
			t.Errorf("For %s: expected %s, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test try/catch
func TestInterpreter_TryCatch(t *testing.T) {
	input := `
		var result = "no error"
		try {
			throw "test error"
		} catch (err) {
			result = err
		}
		result
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "test error" {
		t.Errorf("Expected 'test error', got %v", result)
	}
}

// Test break and continue
func TestInterpreter_BreakContinue(t *testing.T) {
	input := `
		var sum = 0
		for (var i = 0; i < 10; i = i + 1) {
			if (i == 3) {
				continue
			}
			if (i == 7) {
				break
			}
			sum = sum + i
		}
		sum
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	// 0+1+2+4+5+6 = 18 (skips 3, breaks at 7)
	if !compareValues(int64(18), result) {
		t.Errorf("Expected 18, got %v", result)
	}
}

// Test JSON functions
func TestInterpreter_JSONFunctions(t *testing.T) {
	input := `
		var obj = {"name": "Alice"}
		var jsonStr = json(obj)
		typeof(jsonStr)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "string" {
		t.Errorf("Expected 'string', got %v", result)
	}
}

// Test now function
func TestInterpreter_NowFunction(t *testing.T) {
	input := `
		var t = now()
		typeof(t)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "int" {
		t.Errorf("Expected 'int', got %v", result)
	}
}

// Test string functions
func TestInterpreter_StringFunctions(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{`join([1, 2, 3], "-")`, "1-2-3"},
		{`replace("aaa", "a", "b")`, "bbb"},
		{`hasPrefix("hello", "he")`, true},
		{`hasSuffix("hello", "lo")`, true},
		{`contains("hello", "ell")`, true},
		{`indexOf("hello", "l")`, int64(2)},
		{`substr("hello", 1, 3)`, "ell"},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !compareValues(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test split function
func TestInterpreter_SplitFunction(t *testing.T) {
	input := `
		var arr = split("a,b,c", ",")
		len(arr)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(3), result) {
		t.Errorf("Expected 3, got %v", result)
	}
}

// Test math functions
func TestInterpreter_MathFunctions(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"sqrt(16)", int64(4)},
		{"pow(2, 3)", int64(8)},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !compareValues(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test push function
func TestInterpreter_PushFunction(t *testing.T) {
	input := `
		var arr = [1, 2]
		var newArr = push(arr, 3)
		newArr[2]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(3), result) {
		t.Errorf("Expected 3, got %v", result)
	}
}

// Test pop function
func TestInterpreter_PopFunction(t *testing.T) {
	input := `
		var arr = [1, 2, 3]
		var last = pop(arr)
		last
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(3), result) {
		t.Errorf("Expected 3, got %v", result)
	}
}

// Test slice function
func TestInterpreter_SliceFunction(t *testing.T) {
	input := `
		var arr = [1, 2, 3, 4, 5]
		var sub = slice(arr, 1, 3)
		sub[0]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(2), result) {
		t.Errorf("Expected 2, got %v", result)
	}
}

// Test range function
func TestInterpreter_RangeFunction(t *testing.T) {
	input := `
		var arr = range(5)
		arr[4]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(4), result) {
		t.Errorf("Expected 4, got %v", result)
	}
}

// Test object functions
func TestInterpreter_ObjectFunctions(t *testing.T) {
	input := `
		var obj = {"a": 1, "b": 2}
		var k = keys(obj)
		len(k)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(2), result) {
		t.Errorf("Expected 2, got %v", result)
	}
}

// Test values function
func TestInterpreter_ValuesFunction(t *testing.T) {
	input := `
		var obj = {"a": 1, "b": 2}
		var v = values(obj)
		len(v)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !compareValues(int64(2), result) {
		t.Errorf("Expected 2, got %v", result)
	}
}

// Test float function
func TestInterpreter_FloatFunction(t *testing.T) {
	input := `
		var f = float("3.14")
		typeof(f)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "float" {
		t.Errorf("Expected 'float', got %v", result)
	}
}

// Test trim prefix/suffix
func TestInterpreter_TrimPrefixSuffix(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{`trimPrefix("hello", "he")`, "llo"},
		{`trimSuffix("hello", "lo")`, "hel"},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !compareValues(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test formatTime function
func TestInterpreter_FormatTimeFunction(t *testing.T) {
	input := `
		var ts = now()
		var s = formatTime(ts, "2006-01-02")
		typeof(s)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "string" {
		t.Errorf("Expected 'string', got %v", result)
	}
}

// Test parseTime function
func TestInterpreter_ParseTimeFunction(t *testing.T) {
	input := `
		var ts = parseTime("2022-01-01", "2006-01-02")
		typeof(ts)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "int" {
		t.Errorf("Expected 'int', got %v", result)
	}
}