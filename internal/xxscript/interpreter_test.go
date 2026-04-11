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

		if !valuesEqual(tt.expected, result) {
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

	if !valuesEqual(int64(30), result) {
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

		if !valuesEqual(tt.expected, result) {
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

	if !valuesEqual(int64(15), result) {
		t.Errorf("Expected 15, got %v", result)
	}
}

// Test for-in loop with arrays
func TestInterpreter_ForInLoop_Array(t *testing.T) {
	// Test with key and value
	input := `
		var arr = [10, 20, 30, 40, 50]
		var sum = 0
		var indexSum = 0
		for i, v in arr {
			sum = sum + v
			indexSum = indexSum + i
		}
		[sum, indexSum]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	arr, ok := result.([]Value)
	if !ok {
		t.Fatalf("Expected array, got %T", result)
	}
	if !valuesEqual(int64(150), arr[0]) {
		t.Errorf("Expected sum 150, got %v", arr[0])
	}
	if !valuesEqual(int64(10), arr[1]) { // 0+1+2+3+4 = 10
		t.Errorf("Expected indexSum 10, got %v", arr[1])
	}

	// Test with value only
	input2 := `
		var arr = [1, 2, 3]
		var product = 1
		for v in arr {
			product = product * v
		}
		product
	`
	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(6), result2) {
		t.Errorf("Expected product 6, got %v", result2)
	}
}

// Test for-in loop with maps
func TestInterpreter_ForInLoop_Map(t *testing.T) {
	input := `
		var m = {"a": 1, "b": 2, "c": 3}
		var keys = ""
		var sum = 0
		for k, v in m {
			keys = keys + k
			sum = sum + v
		}
		[sum, keys]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	arr, ok := result.([]Value)
	if !ok {
		t.Fatalf("Expected array, got %T", result)
	}
	if !valuesEqual(int64(6), arr[0]) { // 1+2+3 = 6
		t.Errorf("Expected sum 6, got %v", arr[0])
	}
	// keys should contain "abc" in some order (map iteration is unordered)
	keysStr, ok := arr[1].(string)
	if !ok {
		t.Fatalf("Expected string for keys, got %T", arr[1])
	}
	if len(keysStr) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keysStr))
	}
}

// Test for-in loop with strings
func TestInterpreter_ForInLoop_String(t *testing.T) {
	input := `
		var s = "hello"
		var chars = ""
		var indices = ""
		for i, c in s {
			chars = chars + c
			indices = indices + string(i)
		}
		[chars, indices]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	arr, ok := result.([]Value)
	if !ok {
		t.Fatalf("Expected array, got %T", result)
	}
	if !valuesEqual("hello", arr[0]) {
		t.Errorf("Expected chars 'hello', got %v", arr[0])
	}
	if !valuesEqual("01234", arr[1]) {
		t.Errorf("Expected indices '01234', got %v", arr[1])
	}
}

// Test for-in loop with break
func TestInterpreter_ForInLoop_Break(t *testing.T) {
	input := `
		var arr = [1, 2, 3, 4, 5]
		var sum = 0
		for i, v in arr {
			if i == 3 {
				break
			}
			sum = sum + v
		}
		sum
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	// Should sum [1, 2, 3] = 6 (break when i=3)
	if !valuesEqual(int64(6), result) {
		t.Errorf("Expected 6, got %v", result)
	}
}

// Test for-in loop with continue
func TestInterpreter_ForInLoop_Continue(t *testing.T) {
	input := `
		var arr = [1, 2, 3, 4, 5]
		var sum = 0
		for i, v in arr {
			if i == 2 {
				continue
			}
			sum = sum + v
		}
		sum
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	// Should sum [1, 2, 4, 5] = 12 (skip index 2)
	if !valuesEqual(int64(12), result) {
		t.Errorf("Expected 12, got %v", result)
	}
}

// Test spread operator in arrays
func TestInterpreter_SpreadOperator_Array(t *testing.T) {
	// Basic spread in array literal
	input1 := `
		var arr1 = [1, 2, 3]
		var arr2 = [...arr1, 4, 5]
		arr2
	`
	result1, err := Run(input1, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	arr1, ok := result1.([]Value)
	if !ok {
		t.Fatalf("Expected array, got %T", result1)
	}
	if len(arr1) != 5 {
		t.Errorf("Expected 5 elements, got %d", len(arr1))
	}
	if !valuesEqual(int64(1), arr1[0]) || !valuesEqual(int64(5), arr1[4]) {
		t.Errorf("Expected [1, 2, 3, 4, 5], got %v", arr1)
	}

	// Multiple spreads
	input2 := `
		var a = [1, 2]
		var b = [3, 4]
		var c = [...a, ...b, 5]
		c
	`
	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	arr2, _ := result2.([]Value)
	if len(arr2) != 5 {
		t.Errorf("Expected 5 elements, got %d", len(arr2))
	}

	// Spread string
	input3 := `
		var s = "ab"
		var arr = [...s, "c"]
		arr
	`
	result3, err := Run(input3, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	arr3, _ := result3.([]Value)
	if len(arr3) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(arr3))
	}
	if arr3[0] != "a" || arr3[1] != "b" || arr3[2] != "c" {
		t.Errorf("Expected ['a', 'b', 'c'], got %v", arr3)
	}
}

// Test spread operator in function calls
func TestInterpreter_SpreadOperator_FunctionCall(t *testing.T) {
	// Spread in function call (use a non-builtin name)
	input := `
		func add3(a, b, c) {
			return a + b + c
		}
		var args = [1, 2, 3]
		add3(...args)
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(6), result) {
		t.Errorf("Expected 6, got %v", result)
	}

	// Mixed spread and regular args
	input2 := `
		func add4(a, b, c, d) {
			return a + b + c + d
		}
		var args = [1, 2]
		add4(...args, 3, 4)
	`
	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(10), result2) {
		t.Errorf("Expected 10, got %v", result2)
	}

	// Spread with builtin function max
	input3 := `
		var nums = [3, 1, 2]
		max(...nums)
	`
	result3, err := Run(input3, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(3), result3) {
		t.Errorf("Expected 3, got %v", result3)
	}
}

// Test default function parameters
func TestInterpreter_DefaultParameters(t *testing.T) {
	// Basic default parameter
	input1 := `
		func greet(name, greeting = "Hello") {
			return greeting + " " + name
		}
		greet("World")
	`
	result1, err := Run(input1, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if result1 != "Hello World" {
		t.Errorf("Expected 'Hello World', got %v", result1)
	}

	// Override default parameter
	input2 := `
		func greet(name, greeting = "Hello") {
			return greeting + " " + name
		}
		greet("World", "Hi")
	`
	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if result2 != "Hi World" {
		t.Errorf("Expected 'Hi World', got %v", result2)
	}

	// Multiple default parameters
	input3 := `
		func add3(a, b = 10, c = 20) {
			return a + b + c
		}
		add3(5)
	`
	result3, err := Run(input3, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(35), result3) {
		t.Errorf("Expected 35, got %v", result3)
	}

	// Partial override
	input4 := `
		func add3(a, b = 10, c = 20) {
			return a + b + c
		}
		add3(5, 15)
	`
	result4, err := Run(input4, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(40), result4) {
		t.Errorf("Expected 40, got %v", result4)
	}

	// Default with expression
	input5 := `
		func compute(a, b = a * 2) {
			return a + b
		}
		compute(5)
	`
	result5, err := Run(input5, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(15), result5) {
		t.Errorf("Expected 15, got %v", result5)
	}
}

// Test rest parameters
func TestInterpreter_RestParameters(t *testing.T) {
	// Basic rest parameter
	input1 := `
		func sumAll(...nums) {
			var total = 0
			for n in nums {
				total = total + n
			}
			return total
		}
		sumAll(1, 2, 3, 4, 5)
	`
	result1, err := Run(input1, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if !valuesEqual(int64(15), result1) {
		t.Errorf("Expected 15, got %v", result1)
	}

	// Mixed regular and rest parameters
	input2 := `
		func greetAll(greeting, ...names) {
			var result = ""
			for n in names {
				result = result + greeting + " " + n + ", "
			}
			return result
		}
		greetAll("Hello", "Alice", "Bob", "Carol")
	`
	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	if result2 != "Hello Alice, Hello Bob, Hello Carol, " {
		t.Errorf("Expected 'Hello Alice, Hello Bob, Hello Carol, ', got %v", result2)
	}

	// Rest with no extra arguments
	input3 := `
		func firstAndRest(first, ...rest) {
			return [first, rest]
		}
		firstAndRest(1)
	`
	result3, err := Run(input3, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	arr, ok := result3.([]Value)
	if !ok {
		t.Fatalf("Expected array, got %T", result3)
	}
	if !valuesEqual(int64(1), arr[0]) {
		t.Errorf("Expected first=1, got %v", arr[0])
	}
	restArr, ok := arr[1].([]Value)
	if !ok {
		t.Fatalf("Expected rest to be array, got %T", arr[1])
	}
	if len(restArr) != 0 {
		t.Errorf("Expected empty rest array, got %v", restArr)
	}

	// Rest with multiple arguments
	input4 := `
		func multiplyAll(factor, ...nums) {
			var result = []
			for n in nums {
				result = [...result, n * factor]
			}
			return result
		}
		multiplyAll(2, 1, 2, 3)
	`
	result4, err := Run(input4, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}
	arr4, _ := result4.([]Value)
	if len(arr4) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(arr4))
	}
	if !valuesEqual(int64(2), arr4[0]) || !valuesEqual(int64(4), arr4[1]) || !valuesEqual(int64(6), arr4[2]) {
		t.Errorf("Expected [2, 4, 6], got %v", arr4)
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

	if !valuesEqual(int64(5), result) {
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

	if !valuesEqual(int64(7), result) {
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

	if !valuesEqual(int64(120), result) {
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

	if !valuesEqual(int64(3), result) {
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

		if !valuesEqual(tt.expected, result) {
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
	if !valuesEqual(int64(18), result) {
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

		if !valuesEqual(tt.expected, result) {
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

	if !valuesEqual(int64(3), result) {
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

		if !valuesEqual(tt.expected, result) {
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

	if !valuesEqual(int64(3), result) {
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

	if !valuesEqual(int64(3), result) {
		t.Errorf("Expected 3, got %v", result)
	}
}

// Test slice function
func TestInterpreter_SliceFunction(t *testing.T) {
	// First test that arrays work
	input1 := `var arr = [1, 2, 3, 4, 5]; arr[0]`
	result1, err := Run(input1, nil)
	if err != nil {
		t.Fatalf("Array test error: %v", err)
	}
	if !valuesEqual(int64(1), result1) {
		t.Errorf("Array test: Expected 1, got %v", result1)
	}

	// Test slice function call
	input2 := `var arr = [1, 2, 3, 4, 5]; slice(arr, 1, 3)`
	result2, err := Run(input2, nil)
	if err != nil {
		t.Fatalf("Slice test error: %v", err)
	}
	arr, ok := result2.([]Value)
	if !ok {
		t.Errorf("Expected array, got %T", result2)
		return
	}
	if len(arr) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(arr))
		return
	}

	// Full test
	input := `
		var arr = [1, 2, 3, 4, 5]
		var sub = slice(arr, 1, 3)
		sub[0]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !valuesEqual(int64(2), result) {
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

	if !valuesEqual(int64(4), result) {
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

	if !valuesEqual(int64(2), result) {
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

	if !valuesEqual(int64(2), result) {
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

		if !valuesEqual(tt.expected, result) {
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

// Test compound assignment operators (+=, -=, *=, /=, %=)
func TestInterpreter_CompoundAssign(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{`var x = 10; x += 5; x`, int64(15)},
		{`var x = 10; x -= 3; x`, int64(7)},
		{`var x = 4; x *= 3; x`, int64(12)},
		{`var x = 15.0; x /= 3; x`, 5.0},
		{`var x = 10; x %= 3; x`, int64(1)},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !valuesEqual(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test compound assignment on map members
func TestInterpreter_CompoundAssignMap(t *testing.T) {
	input := `
		var obj = {"count": 10}
		obj.count += 5
		obj.count
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !valuesEqual(int64(15), result) {
		t.Errorf("Expected 15, got %v", result)
	}
}

// Test compound assignment on array index
func TestInterpreter_CompoundAssignArray(t *testing.T) {
	input := `
		var arr = [10, 20, 30]
		arr[1] += 5
		arr[1]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !valuesEqual(int64(25), result) {
		t.Errorf("Expected 25, got %v", result)
	}
}

// Test prefix increment/decrement (++x, --x)
func TestInterpreter_PreIncDec(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{`var x = 5; ++x`, int64(6)},
		{`var x = 5; --x`, int64(4)},
		{`var x = 5; ++x; x`, int64(6)},
		{`var x = 5; --x; x`, int64(4)},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !valuesEqual(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test postfix increment/decrement (x++, x--)
func TestInterpreter_PostIncDec(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{`var x = 5; x++`, int64(5)},    // returns old value
		{`var x = 5; x--`, int64(5)},    // returns old value
		{`var x = 5; x++; x`, int64(6)}, // x is now 6
		{`var x = 5; x--; x`, int64(4)}, // x is now 4
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !valuesEqual(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test ternary conditional operator (condition ? true_expr : false_expr)
func TestInterpreter_TernaryOperator(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"true ? 1 : 2", int64(1)},
		{"false ? 1 : 2", int64(2)},
		{"1 < 2 ? \"yes\" : \"no\"", "yes"},
		{"1 > 2 ? \"yes\" : \"no\"", "no"},
		{"var x = 5; x > 3 ? x * 2 : x / 2", int64(10)},
	}

	for _, tt := range tests {
		result, err := Run(tt.input, nil)
		if err != nil {
			t.Errorf("Execution error for %s: %v", tt.input, err)
			continue
		}

		if !valuesEqual(tt.expected, result) {
			t.Errorf("For %s: expected %v, got %v", tt.input, tt.expected, result)
		}
	}
}

// Test nested ternary
func TestInterpreter_NestedTernary(t *testing.T) {
	input := `
		var x = 5
		x < 3 ? "small" : (x < 7 ? "medium" : "large")
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if result != "medium" {
		t.Errorf("Expected 'medium', got %v", result)
	}
}

// Test pre-increment on map member
func TestInterpreter_PreIncDecMap(t *testing.T) {
	input := `var obj = {"count": 10}; ++obj.count`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !valuesEqual(int64(11), result) {
		t.Errorf("Expected 11, got %v", result)
	}
}

// Test post-increment on array element
func TestInterpreter_PostIncDecArray(t *testing.T) {
	input := `
		var arr = [10, 20, 30]
		arr[1]++
		arr[1]
	`
	result, err := Run(input, nil)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if !valuesEqual(int64(21), result) {
		t.Errorf("Expected 21, got %v", result)
	}
}
