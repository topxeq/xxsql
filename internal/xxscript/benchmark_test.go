package xxscript

import (
	"testing"
)

// BenchmarkArithmetic benchmarks arithmetic operations.
func BenchmarkArithmetic(b *testing.B) {
	script := `1 + 2 * 3 - 4 / 2`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkVariableAccess benchmarks variable access.
func BenchmarkVariableAccess(b *testing.B) {
	script := `
		var x = 10
		var y = 20
		x + y
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkFunctionCall benchmarks function calls.
func BenchmarkFunctionCall(b *testing.B) {
	script := `
		func add(a, b) {
			return a + b
		}
		add(1, 2)
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkLoop benchmarks loop performance.
func BenchmarkLoop(b *testing.B) {
	script := `
		var sum = 0
		for (var i = 0; i < 100; i = i + 1) {
			sum = sum + i
		}
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkStringConcat benchmarks string concatenation.
func BenchmarkStringConcat(b *testing.B) {
	script := `"hello" + " " + "world"`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkArrayOperations benchmarks array operations.
func BenchmarkArrayOperations(b *testing.B) {
	script := `
		var arr = [1, 2, 3, 4, 5]
		push(arr, 6)
		pop(arr)
		len(arr)
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkMapOperations benchmarks map operations.
func BenchmarkMapOperations(b *testing.B) {
	script := `
		var obj = {"a": 1, "b": 2, "c": 3}
		obj.a
		obj.b = 10
		keys(obj)
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkJSON benchmarks JSON operations.
func BenchmarkJSON(b *testing.B) {
	script := `
		var obj = {"name": "test", "value": 123}
		json(obj)
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkConditionals benchmarks conditional statements.
func BenchmarkConditionals(b *testing.B) {
	script := `
		var x = 10
		if (x > 5) {
			x = x * 2
		} else {
			x = x / 2
		}
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkTernary benchmarks ternary operator.
func BenchmarkTernary(b *testing.B) {
	script := `10 > 5 ? "yes" : "no"`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkComplexScript benchmarks a complex script.
func BenchmarkComplexScript(b *testing.B) {
	script := `
		func fibonacci(n) {
			if (n <= 1) {
				return n
			}
			return fibonacci(n - 1) + fibonacci(n - 2)
		}
		fibonacci(10)
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkBuiltinCall benchmarks builtin function calls.
func BenchmarkBuiltinCall(b *testing.B) {
	script := `len("hello world")`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Run(script, nil)
	}
}

// BenchmarkParse benchmarks parsing performance.
func BenchmarkParse(b *testing.B) {
	script := `
		var x = 10
		for (var i = 0; i < 10; i = i + 1) {
			x = x + i
		}
		if (x > 50) {
			x = x / 2
		}
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(script)
	}
}

// BenchmarkLexer benchmarks lexer performance.
func BenchmarkLexer(b *testing.B) {
	script := `
		var x = 10
		for (var i = 0; i < 10; i = i + 1) {
			x = x + i
		}
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Tokenize(script)
	}
}
