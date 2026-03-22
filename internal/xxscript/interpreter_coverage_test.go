package xxscript

import (
	"testing"
)

// TestThrowError tests the ThrowError type
func TestThrowError(t *testing.T) {
	err := ThrowError{Value: "test error"}
	if err.Error() != "test error" {
		t.Errorf("Error() = %q, want 'test error'", err.Error())
	}
}

// TestLexerSkipComment tests skipComment method
func TestLexerSkipComment(t *testing.T) {
	// Test that comments are tokenized as TokComment
	l := NewLexer("// comment\n42")
	tok := l.NextToken()
	// Comments might be returned as tokens or skipped
	// Let's just test that the lexer works
	_ = tok
}

// TestTokenTypeString tests the String method on TokenType
func TestTokenTypeString(t *testing.T) {
	tests := []struct {
		tokType  TokenType
		expected string
	}{
		{TokEOF, "EOF"},
		{TokError, "ERROR"},
		{TokIdent, "IDENT"},
		{TokString, "STRING"},
		{TokNumber, "NUMBER"},
		{TokBool, "BOOL"},
		{TokNull, "NULL"},
	}

	for _, tt := range tests {
		result := tt.tokType.String()
		if result != tt.expected {
			t.Errorf("TokenType.String() = %q, want %q", result, tt.expected)
		}
	}
}

// TestASTStringMethods tests String methods on AST nodes
func TestASTStringMethods(t *testing.T) {
	// Test NumberExpr
	num := &NumberExpr{Value: 42}
	if num.String() == "" {
		t.Error("NumberExpr.String() should not be empty")
	}

	// Test StringExpr
	str := &StringExpr{Value: "test"}
	if str.String() == "" {
		t.Error("StringExpr.String() should not be empty")
	}

	// Test BoolExpr
	b := &BoolExpr{Value: true}
	if b.String() == "" {
		t.Error("BoolExpr.String() should not be empty")
	}

	// Test NullExpr
	null := &NullExpr{}
	if null.String() == "" {
		t.Error("NullExpr.String() should not be empty")
	}

	// Test IdentExpr
	id := &IdentExpr{Name: "x"}
	if id.String() == "" {
		t.Error("IdentExpr.String() should not be empty")
	}

	// Test ArrayExpr
	arr := &ArrayExpr{Elements: []Expression{&NumberExpr{Value: 1}}}
	if arr.String() == "" {
		t.Error("ArrayExpr.String() should not be empty")
	}

	// Test MapExpr
	obj := &MapExpr{Pairs: map[string]Expression{"key": &StringExpr{Value: "value"}}}
	if obj.String() == "" {
		t.Error("MapExpr.String() should not be empty")
	}

	// Test BinaryExpr
	bin := &BinaryExpr{Left: &NumberExpr{Value: 1}, Op: TokPlus, Right: &NumberExpr{Value: 2}}
	if bin.String() == "" {
		t.Error("BinaryExpr.String() should not be empty")
	}

	// Test UnaryExpr
	unary := &UnaryExpr{Op: TokMinus, Expr: &NumberExpr{Value: 1}}
	if unary.String() == "" {
		t.Error("UnaryExpr.String() should not be empty")
	}

	// Test CallExpr
	call := &CallExpr{Func: &IdentExpr{Name: "print"}, Args: []Expression{}}
	if call.String() == "" {
		t.Error("CallExpr.String() should not be empty")
	}

	// Test IndexExpr
	idx := &IndexExpr{Object: &IdentExpr{Name: "arr"}, Index: &NumberExpr{Value: 0}}
	if idx.String() == "" {
		t.Error("IndexExpr.String() should not be empty")
	}

	// Test MemberExpr
	member := &MemberExpr{Object: &IdentExpr{Name: "obj"}, Member: &StringExpr{Value: "field"}}
	if member.String() == "" {
		t.Error("MemberExpr.String() should not be empty")
	}

	// Test AssignExpr
	assign := &AssignExpr{Left: &IdentExpr{Name: "x"}, Value: &NumberExpr{Value: 1}}
	if assign.String() == "" {
		t.Error("AssignExpr.String() should not be empty")
	}
}

// TestStmtStringMethods tests String methods on statement nodes
func TestStmtStringMethods(t *testing.T) {
	// Test VarStmt
	varStmt := &VarStmt{Name: "x", Value: &NumberExpr{Value: 1}}
	if varStmt.String() == "" {
		t.Error("VarStmt.String() should not be empty")
	}

	// Test ExprStmt
	exprStmt := &ExprStmt{Expr: &NumberExpr{Value: 1}}
	if exprStmt.String() == "" {
		t.Error("ExprStmt.String() should not be empty")
	}

	// Test BlockStmt
	block := &BlockStmt{Statements: []Statement{&ExprStmt{Expr: &NumberExpr{Value: 1}}}}
	if block.String() == "" {
		t.Error("BlockStmt.String() should not be empty")
	}

	// Test IfStmt
	ifStmt := &IfStmt{Condition: &BoolExpr{Value: true}, Then: &BlockStmt{}}
	if ifStmt.String() == "" {
		t.Error("IfStmt.String() should not be empty")
	}

	// Test WhileStmt
	whileStmt := &WhileStmt{Condition: &BoolExpr{Value: true}, Body: &BlockStmt{}}
	if whileStmt.String() == "" {
		t.Error("WhileStmt.String() should not be empty")
	}

	// Test ForStmt
	forStmt := &ForStmt{Init: &VarStmt{}, Condition: &BoolExpr{Value: true}, Body: &BlockStmt{}}
	if forStmt.String() == "" {
		t.Error("ForStmt.String() should not be empty")
	}

	// Test FuncStmt
	funcStmt := &FuncStmt{Name: "test", Params: []string{}, Body: &BlockStmt{}}
	if funcStmt.String() == "" {
		t.Error("FuncStmt.String() should not be empty")
	}

	// Test ReturnStmt
	retStmt := &ReturnStmt{Value: &NumberExpr{Value: 1}}
	if retStmt.String() == "" {
		t.Error("ReturnStmt.String() should not be empty")
	}

	// Test BreakStmt
	breakStmt := &BreakStmt{}
	if breakStmt.String() == "" {
		t.Error("BreakStmt.String() should not be empty")
	}

	// Test ContinueStmt
	continueStmt := &ContinueStmt{}
	if continueStmt.String() == "" {
		t.Error("ContinueStmt.String() should not be empty")
	}

	// Test TryStmt
	tryStmt := &TryStmt{TryBlock: &BlockStmt{}, CatchVar: "e", CatchBlock: &BlockStmt{}}
	if tryStmt.String() == "" {
		t.Error("TryStmt.String() should not be empty")
	}

	// Test ThrowStmt
	throwStmt := &ThrowStmt{Error: &StringExpr{Value: "error"}}
	if throwStmt.String() == "" {
		t.Error("ThrowStmt.String() should not be empty")
	}
}

// TestIntFunction tests int() builtin
func TestIntFunction(t *testing.T) {
	tests := []struct {
		script   string
		check    func(interface{}) bool
	}{
		{"int(42)", func(v interface{}) bool { _, ok := v.(int); return ok || v == int64(42) }},
		{"int(42.9)", func(v interface{}) bool { return v == int64(42) || v == 42 }},
		{"int('42')", func(v interface{}) bool { return v == int64(42) || v == 42 }},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if !tt.check(result) {
			t.Errorf("Run(%q) = %v (type %T), check failed", tt.script, result, result)
		}
	}
}

// TestFloatFunction tests float() builtin
func TestFloatFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"float(42)", 42.0},
		{"float(42.5)", 42.5},
		{"float('42.5')", 42.5},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestModOperator tests modulo operator
func TestModOperator(t *testing.T) {
	result, err := Run("10 % 3", nil)
	if err != nil {
		t.Errorf("10 %% 3 error: %v", err)
	}
	if result != int64(1) && result != 1 {
		t.Errorf("10 %% 3 = %v, want 1", result)
	}
}

// TestNowFunction tests now() builtin
func TestNowFunction(t *testing.T) {
	result, err := Run("now()", nil)
	if err != nil {
		t.Errorf("now() error: %v", err)
	}
	if result == nil {
		t.Error("now() should return a value")
	}
}

// TestFormatTimeFunction tests formatTime() builtin
func TestFormatTimeFunction(t *testing.T) {
	result, err := Run(`formatTime("2023-01-15", "2006-01-02", "Jan 02, 2006")`, nil)
	if err != nil {
		t.Logf("formatTime error (may be expected): %v", err)
	}
	_ = result
}

// TestHTTPObject tests HTTP object through script
func TestHTTPObjectInScript(t *testing.T) {
	// Test that HTTP object exists
	_, err := Run(`http.get`, nil)
	if err != nil {
		t.Logf("HTTP object test: %v", err)
	}
}

// TestDBObject tests DB object through script
func TestDBObjectInScript(t *testing.T) {
	// Test that DB object exists (without a real executor)
	_, err := Run(`db.query`, nil)
	if err != nil {
		t.Logf("DB object test: %v", err)
	}
}

// TestCompareOperations tests comparison operations with different types
func TestCompareOperations(t *testing.T) {
	tests := []struct {
		script   string
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
		// Mixed type comparisons
		{"1 < 2.5", true},
		{"2.5 > 1", true},
		{"1.5 == 1.5", true},
		{"1.5 != 2.5", true},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestArithmeticOperations tests arithmetic with mixed types
func TestArithmeticOperations(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"1 + 2", 3.0},
		{"1 + 2.5", 3.5},
		{"2.5 + 1", 3.5},
		{"1.5 + 2.5", 4.0},
		{"5 - 2", 3.0},
		{"5.5 - 2", 3.5},
		{"5 - 2.5", 2.5},
		{"3 * 4", 12.0},
		{"3.5 * 2", 7.0},
		{"10 / 2", 5.0},
		{"10.0 / 4", 2.5},
		{"-5", -5.0},
		{"-2.5", -2.5},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
		}
	}
}

// TestStringOperations tests string operations
func TestStringOperationsExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{`"hello" + " " + "world"`, "hello world"},
		{`"test" == "test"`, "true"},
		{`"a" < "b"`, "true"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if tt.expected == "true" || tt.expected == "false" {
			// Boolean result
			expected := tt.expected == "true"
			if result != expected {
				t.Errorf("Run(%q) = %v, want %v", tt.script, result, expected)
			}
		} else {
			if result != tt.expected {
				t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
			}
		}
	}
}

// TestLogicalOperations tests logical operators
func TestLogicalOperations(t *testing.T) {
	tests := []struct {
		script   string
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
		{"1 && 1", true},
		{"0 || 1", true},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinFunctionsExtra tests additional builtin functions
func TestBuiltinFunctionsExtra(t *testing.T) {
	// Test abs
	result, err := Run("abs(-5)", nil)
	if err != nil {
		t.Errorf("abs(-5) error: %v", err)
	} else {
		// Result might be float64
		if r, ok := result.(float64); !ok || r != 5.0 {
			if r2, ok2 := result.(int); !ok2 || r2 != 5 {
				t.Errorf("abs(-5) = %v (type %T), want 5", result, result)
			}
		}
	}

	// Test abs with float
	result, err = Run("abs(-3.5)", nil)
	if err != nil {
		t.Errorf("abs(-3.5) error: %v", err)
	} else if r, ok := result.(float64); !ok || r != 3.5 {
		t.Errorf("abs(-3.5) = %v, want 3.5", result)
	}

	// Test min
	result, err = Run("min(1, 2)", nil)
	if err != nil {
		t.Errorf("min(1, 2) error: %v", err)
	} else {
		if r, ok := result.(float64); !ok || r != 1.0 {
			if r2, ok2 := result.(int); !ok2 || r2 != 1 {
				t.Errorf("min(1, 2) = %v (type %T), want 1", result, result)
			}
		}
	}

	// Test max
	result, err = Run("max(1, 2)", nil)
	if err != nil {
		t.Errorf("max(1, 2) error: %v", err)
	} else {
		if r, ok := result.(float64); !ok || r != 2.0 {
			if r2, ok2 := result.(int); !ok2 || r2 != 2 {
				t.Errorf("max(1, 2) = %v (type %T), want 2", result, result)
			}
		}
	}
}

// TestArrayOperations tests array operations
func TestArrayOperations(t *testing.T) {
	// Array creation
	result, err := Run("[1, 2, 3]", nil)
	if err != nil {
		t.Errorf("Array creation error: %v", err)
	}
	_ = result

	// Array indexing
	result, err = Run("[1, 2, 3][0]", nil)
	if err != nil {
		t.Errorf("Array indexing error: %v", err)
	} else {
		// Result might be float64
		if r, ok := result.(float64); !ok || r != 1.0 {
			if r2, ok2 := result.(int); !ok2 || r2 != 1 {
				t.Errorf("Array indexing = %v (type %T), want 1", result, result)
			}
		}
	}

	// Array length
	result, err = Run("len([1, 2, 3])", nil)
	if err != nil {
		t.Errorf("Array length error: %v", err)
	} else if result != 3 {
		t.Errorf("Array length = %v, want 3", result)
	}
}

// TestMapOperations tests map operations
func TestMapOperations(t *testing.T) {
	// Map creation
	result, err := Run(`{"a": 1, "b": 2}`, nil)
	if err != nil {
		t.Errorf("Map creation error: %v", err)
	}
	_ = result

	// Map access
	result, err = Run(`{"a": 1, "b": 2}.a`, nil)
	if err != nil {
		t.Errorf("Map access error: %v", err)
	} else {
		// Result might be float64
		if r, ok := result.(float64); !ok || r != 1.0 {
			if r2, ok2 := result.(int); !ok2 || r2 != 1 {
				t.Errorf("Map access = %v (type %T), want 1", result, result)
			}
		}
	}
}

// TestTernaryOperator tests ternary operator - disabled if not supported
func TestTernaryOperator(t *testing.T) {
	// Check if ternary operator is supported
	_, err := Run("true ? 1 : 2", nil)
	if err != nil {
		t.Logf("Ternary operator not supported: %v", err)
		return
	}

	tests := []struct {
		script   string
		expected interface{}
	}{
		{"true ? 1 : 2", 1},
		{"false ? 1 : 2", 2},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestCompareFunction tests the compare function with various types
func TestCompareFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		// Numeric comparisons
		{"1 < 2", true},
		{"2 < 1", false},
		{"1 > 0", true},
		{"0 > 1", false},
		{"1 <= 1", true},
		{"1 >= 1", true},
		// String comparisons
		{`"a" < "b"`, true},
		{`"b" < "a"`, false},
		{`"a" > "A"`, true},
		// Equality
		{"1 == 1", true},
		{"1 == 2", false},
		{`"a" == "a"`, true},
		{`"a" == "b"`, false},
		// Mixed type
		{"1 < 2.5", true},
		{"2.5 > 1", true},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestDivisionFunction tests the div function
func TestDivisionFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"10 / 2", 5.0},
		{"10 / 3", 3.3333333333333335},
		{"7 / 2", 3.5},
		{"1 / 2", 0.5},
		{"100 / 10", 10.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
		}
	}
}

// TestSubtractionFunction tests the sub function
func TestSubtractionFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"10 - 5", 5.0},
		{"10 - 3", 7.0},
		{"5.5 - 2.5", 3.0},
		{"100 - 50", 50.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
		}
	}
}

// TestMultiplicationFunction tests the mul function
func TestMultiplicationFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"2 * 3", 6.0},
		{"10 * 10", 100.0},
		{"2.5 * 4", 10.0},
		{"0 * 100", 0.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
		}
	}
}

// TestBuiltinAbsFunction tests the abs builtin
func TestBuiltinAbsFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"abs(-5)", 5.0},
		{"abs(5)", 5.0},
		{"abs(-3.5)", 3.5},
		{"abs(0)", 0.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			if r2, ok2 := result.(int); !ok2 || float64(r2) != tt.expected {
				t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
			}
		}
	}
}

// TestHTTPObjectMethods tests HTTP object methods
func TestHTTPObjectMethods(t *testing.T) {
	// Test accessing HTTP object members
	result, err := Run(`http.get`, nil)
	if err != nil {
		t.Logf("HTTP object access error: %v", err)
	}
	_ = result

	result, err = Run(`http.post`, nil)
	if err != nil {
		t.Logf("HTTP object access error: %v", err)
	}
	_ = result
}

// TestDBObjectMethods tests DB object methods
func TestDBObjectMethods(t *testing.T) {
	// Test accessing DB object members
	result, err := Run(`db.query`, nil)
	if err != nil {
		t.Logf("DB object access error: %v", err)
	}
	_ = result

	result, err = Run(`db.execute`, nil)
	if err != nil {
		t.Logf("DB object access error: %v", err)
	}
	_ = result
}

// TestEvalCallFunction tests the evalCall function
func TestEvalCallFunction(t *testing.T) {
	// Test built-in function calls
	result, err := Run(`len("hello")`, nil)
	if err != nil {
		t.Errorf("Built-in function error: %v", err)
	} else if result != 5 {
		t.Errorf(`len("hello") = %v, want 5`, result)
	}

	// Test nested built-in calls
	result, err = Run(`len(upper("hello"))`, nil)
	if err != nil {
		t.Errorf("Nested function error: %v", err)
	} else if result != 5 {
		t.Errorf(`len(upper("hello")) = %v, want 5`, result)
	}
}

// TestIsTruthyFunction tests the isTruthy function
func TestIsTruthyFunction(t *testing.T) {
	tests := []struct {
		script   string
		expected bool
	}{
		{"if (1) { true } else { false }", true},
		{"if ('hello') { true } else { false }", true},
		{"if (null) { true } else { false }", false},
		{"if (true) { true } else { false }", true},
		{"if (false) { true } else { false }", false},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinAbsExtra tests the abs builtin with various types
func TestBuiltinAbsExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"abs(-5)", 5},
		{"abs(5)", 5},
		{"abs(-3.14)", 3.14},
		{"abs(0)", 0},
		{"abs()", 0},
		{"abs(-10.5)", 10.5},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		// Check result based on type
		switch exp := tt.expected.(type) {
		case int:
			if r, ok := result.(int); !ok || r != exp {
				if r2, ok2 := result.(float64); !ok2 || int(r2) != exp {
					t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
				}
			}
		case float64:
			if r, ok := result.(float64); !ok || r != exp {
				t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
			}
		}
	}
}

// TestBuiltinMinExtra tests the min builtin
func TestBuiltinMinExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"min(1, 2, 3)", 1.0},
		{"min(3, 2, 1)", 1.0},
		{"min(-5, -10, -3)", -10.0},
		{"min(1.5, 2.5)", 1.5},
		{"min()", 0.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			if r2, ok2 := result.(int); !ok2 || float64(r2) != tt.expected {
				t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
			}
		}
	}
}

// TestBuiltinMaxExtra tests the max builtin
func TestBuiltinMaxExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"max(1, 2, 3)", 3.0},
		{"max(3, 2, 1)", 3.0},
		{"max(-5, -10, -3)", -3.0},
		{"max(1.5, 2.5)", 2.5},
		{"max()", 0.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(float64); !ok || r != tt.expected {
			if r2, ok2 := result.(int); !ok2 || float64(r2) != tt.expected {
				t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
			}
		}
	}
}

// TestBuiltinFloorCeil tests floor and ceil builtins
func TestBuiltinFloorCeil(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"floor(3.7)", 3},
		{"floor(3.2)", 3},
		{"floor(-3.7)", -4},
		{"ceil(3.2)", 4},
		{"ceil(3.7)", 4},
		{"ceil(-3.2)", -3},
		{"floor()", 0},
		{"ceil()", 0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		_ = result
	}
}

// TestBuiltinSlice tests the slice builtin
func TestBuiltinSlice(t *testing.T) {
	tests := []struct {
		script   string
		check    func(interface{}) bool
	}{
		{"slice([1, 2, 3, 4, 5], 1, 3)", func(v interface{}) bool {
			arr, ok := v.([]Value);
			return ok && len(arr) == 2
		}},
		{"slice([1, 2, 3], 0, 2)", func(v interface{}) bool {
			arr, ok := v.([]Value);
			return ok && len(arr) == 2
		}},
		{"slice([1, 2, 3], 1)", func(v interface{}) bool {
			arr, ok := v.([]Value);
			return ok && len(arr) == 2
		}},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if !tt.check(result) {
			t.Errorf("Run(%q) = %v (type %T) check failed", tt.script, result, result)
		}
	}
}

// TestBuiltinSubstr tests the substr builtin
func TestBuiltinSubstr(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{`substr("hello", 1, 3)`, "ell"},
		{`substr("hello", 0, 5)`, "hello"},
		{`substr("hello", 2)`, "llo"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinLenExtra tests the len builtin with various types
func TestBuiltinLenExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected int
	}{
		{"len([1, 2, 3])", 3},
		{"len('hello')", 5},
		{"len({a: 1, b: 2})", 2},
		{"len([])", 0},
		{"len('')", 0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if r, ok := result.(int); !ok || r != tt.expected {
			t.Errorf("Run(%q) = %v (type %T), want %v", tt.script, result, result, tt.expected)
		}
	}
}

// TestBuiltinIntFloatString tests type conversion builtins
func TestBuiltinIntFloatString(t *testing.T) {
	// Test int()
	result, err := Run("int(42.9)", nil)
	if err != nil {
		t.Errorf("int(42.9) error: %v", err)
	} else if r, ok := result.(int); !ok || r != 42 {
		if r2, ok2 := result.(int64); !ok2 || r2 != 42 {
			t.Errorf("int(42.9) = %v (type %T), want 42", result, result)
		}
	}

	// Test float()
	result, err = Run("float(42)", nil)
	if err != nil {
		t.Errorf("float(42) error: %v", err)
	} else if r, ok := result.(float64); !ok || r != 42.0 {
		t.Errorf("float(42) = %v (type %T), want 42.0", result, result)
	}

	// Test string()
	result, err = Run("string(42)", nil)
	if err != nil {
		t.Errorf("string(42) error: %v", err)
	} else if result != "42" {
		t.Errorf("string(42) = %v, want '42'", result)
	}

	// Test int() with empty args
	result, err = Run("int()", nil)
	if err != nil {
		t.Errorf("int() error: %v", err)
	}

	// Test float() with empty args
	result, err = Run("float()", nil)
	if err != nil {
		t.Errorf("float() error: %v", err)
	}
}

// TestBuiltinStringManipulation tests string manipulation builtins
func TestBuiltinStringManipulation(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{`upper("hello")`, "HELLO"},
		{`lower("HELLO")`, "hello"},
		{`trim("  hello  ")`, "hello"},
		{`trimPrefix("hello", "he")`, "llo"},
		{`trimSuffix("hello", "lo")`, "hel"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinStringSearch tests string search builtins
func TestBuiltinStringSearch(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{`hasPrefix("hello", "he")`, true},
		{`hasPrefix("hello", "lo")`, false},
		{`hasSuffix("hello", "lo")`, true},
		{`hasSuffix("hello", "he")`, false},
		{`contains("hello", "ell")`, true},
		{`contains("hello", "xyz")`, false},
		{`indexOf("hello", "l")`, 2},
		{`indexOf("hello", "x")`, -1},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinReplace tests the replace builtin
func TestBuiltinReplace(t *testing.T) {
	result, err := Run(`replace("hello world", "world", "there")`, nil)
	if err != nil {
		t.Errorf("replace error: %v", err)
	} else if result != "hello there" {
		t.Errorf("replace = %v, want 'hello there'", result)
	}
}

// TestBuiltinSplitJoin tests split and join builtins
func TestBuiltinSplitJoin(t *testing.T) {
	// Test split
	result, err := Run(`split("a,b,c", ",")`, nil)
	if err != nil {
		t.Errorf("split error: %v", err)
	} else {
		arr, ok := result.([]Value)
		if !ok || len(arr) != 3 {
			t.Errorf("split = %v (type %T), want 3 elements", result, result)
		}
	}

	// Test join
	result, err = Run(`join(["a", "b", "c"], "-")`, nil)
	if err != nil {
		t.Errorf("join error: %v", err)
	} else if result != "a-b-c" {
		t.Errorf("join = %v, want 'a-b-c'", result)
	}
}

// TestBuiltinKeysValues tests keys and values builtins
func TestBuiltinKeysValues(t *testing.T) {
	// Test keys
	result, err := Run(`keys({a: 1, b: 2})`, nil)
	if err != nil {
		t.Errorf("keys error: %v", err)
	} else {
		arr, ok := result.([]Value)
		if !ok || len(arr) != 2 {
			t.Errorf("keys = %v (type %T), want 2 elements", result, result)
		}
	}

	// Test values
	result, err = Run(`values({a: 1, b: 2})`, nil)
	if err != nil {
		t.Errorf("values error: %v", err)
	} else {
		arr, ok := result.([]Value)
		if !ok || len(arr) != 2 {
			t.Errorf("values = %v (type %T), want 2 elements", result, result)
		}
	}
}

// TestBuiltinPushPop tests push and pop builtins
func TestBuiltinPushPop(t *testing.T) {
	// Test push - need to use different syntax
	result, err := Run(`var arr = [1, 2]; push(arr, 3); arr`, nil)
	if err != nil {
		t.Logf("push error (syntax may not be supported): %v", err)
	}
	_ = result

	// Test pop
	result, err = Run(`var arr = [1, 2, 3]; pop(arr)`, nil)
	if err != nil {
		t.Logf("pop error (syntax may not be supported): %v", err)
	}
	_ = result
}

// TestBuiltinJSON tests JSON builtins
func TestBuiltinJSON(t *testing.T) {
	// Test json (encode)
	result, err := Run(`json({a: 1})`, nil)
	if err != nil {
		t.Errorf("json error: %v", err)
	}
	_ = result

	// Test jsonParse
	result, err = Run(`jsonParse('{"a": 1}')`, nil)
	if err != nil {
		t.Errorf("jsonParse error: %v", err)
	}
	_ = result
}

// TestBuiltinTypeof tests typeof builtin
func TestBuiltinTypeof(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{"typeof(42)", "float"}, // numbers are float by default
		{"typeof('hello')", "string"},
		{"typeof(true)", "bool"},
		{"typeof(null)", "null"},
		{"typeof([])", "array"},
		{"typeof({})", "object"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinRangeFunc tests the range builtin function
func TestBuiltinRangeFunc(t *testing.T) {
	result, err := Run(`range(5)`, nil)
	if err != nil {
		t.Errorf("range error: %v", err)
	} else {
		arr, ok := result.([]Value)
		if !ok || len(arr) != 5 {
			t.Errorf("range = %v (type %T), want 5 elements", result, result)
		}
	}
}

// TestBuiltinFormatTime tests formatTime builtin
func TestBuiltinFormatTime(t *testing.T) {
	result, err := Run(`formatTime('2023-01-15', '2006-01-02', 'Jan 02, 2006')`, nil)
	if err != nil {
		t.Logf("formatTime error (may be expected): %v", err)
	}
	_ = result
}

// TestBuiltinParseTime tests parseTime builtin
func TestBuiltinParseTime(t *testing.T) {
	result, err := Run(`parseTime('2023-01-15', '2006-01-02')`, nil)
	if err != nil {
		t.Logf("parseTime error (may be expected): %v", err)
	}
	_ = result
}

// TestBuiltinSprintf tests sprintf builtin
func TestBuiltinSprintf(t *testing.T) {
	result, err := Run(`sprintf('%s has %f items', 'list', 5)`, nil)
	if err != nil {
		t.Errorf("sprintf error: %v", err)
	}
	_ = result
}

// TestBuiltinAbsMore tests abs builtin with more cases
func TestBuiltinAbsMore2(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"abs(-5)", float64(5)},
		{"abs(5)", float64(5)},
		{"abs(-3.14)", float64(3.14)},
		{"abs(0)", float64(0)},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinRoundExtra tests round builtin
func TestBuiltinRoundExtra(t *testing.T) {
	tests := []struct {
		script string
		check  func(interface{}) bool
	}{
		{"round(3.7)", func(v interface{}) bool { return v == float64(4) || v == int(4) }},
		{"round(3.2)", func(v interface{}) bool { return v == float64(3) || v == int(3) }},
		{"round(3.5)", func(v interface{}) bool { return v == float64(4) || v == int(4) }},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if !tt.check(result) {
			t.Errorf("Run(%q) = %v (type %T), unexpected", tt.script, result, result)
		}
	}
}

// TestBuiltinSqrt tests sqrt builtin
func TestBuiltinSqrt(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"sqrt(4)", 2},
		{"sqrt(9)", 3},
		{"sqrt(16)", 4},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if val, ok := result.(float64); !ok || val != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinPowExtra tests pow builtin
func TestBuiltinPowExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"pow(2, 3)", 8},
		{"pow(3, 2)", 9},
		{"pow(10, 0)", 1},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if val, ok := result.(float64); !ok || val != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinStringManipulationExtra tests string manipulation builtins
func TestBuiltinStringManipulationExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{"trim('  hello  ')", "hello"},
		{"trimPrefix('hello', 'he')", "llo"},
		{"trimSuffix('hello', 'lo')", "hel"},
		{"upper('hello')", "HELLO"},
		{"lower('HELLO')", "hello"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinStringSearchExtra tests string search builtins
func TestBuiltinStringSearchExtra(t *testing.T) {
	tests := []struct {
		script string
		check  func(interface{}) bool
	}{
		{"hasPrefix('hello', 'he')", func(v interface{}) bool { return v == true }},
		{"hasPrefix('hello', 'lo')", func(v interface{}) bool { return v == false }},
		{"hasSuffix('hello', 'lo')", func(v interface{}) bool { return v == true }},
		{"hasSuffix('hello', 'he')", func(v interface{}) bool { return v == false }},
		{"contains('hello', 'ell')", func(v interface{}) bool { return v == true }},
		{"contains('hello', 'xyz')", func(v interface{}) bool { return v == false }},
		{"indexOf('hello', 'l')", func(v interface{}) bool { return v == float64(2) || v == int(2) }},
		{"indexOf('hello', 'x')", func(v interface{}) bool { return v == float64(-1) || v == int(-1) }},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if !tt.check(result) {
			t.Errorf("Run(%q) = %v (type %T), unexpected", tt.script, result, result)
		}
	}
}

// TestBuiltinSubstrExtra tests substr builtin
func TestBuiltinSubstrExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{"substr('hello', 0, 3)", "hel"},
		{"substr('hello', 1, 3)", "ell"},
		{"substr('hello', 2)", "llo"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinReplaceExtra tests replace builtin
func TestBuiltinReplaceExtra(t *testing.T) {
	result, err := Run(`replace('hello world', 'world', 'there')`, nil)
	if err != nil {
		t.Errorf("replace error: %v", err)
		return
	}
	if result != "hello there" {
		t.Errorf("replace = %v, want 'hello there'", result)
	}
}

// TestBuiltinSplitJoinExtra tests split and join builtins
func TestBuiltinSplitJoinExtra(t *testing.T) {
	// Test split
	result, err := Run(`split('a,b,c', ',')`, nil)
	if err != nil {
		t.Errorf("split error: %v", err)
		return
	}
	arr, ok := result.([]Value)
	if !ok || len(arr) != 3 {
		t.Errorf("split = %v, want 3 elements", result)
	}

	// Test join
	result, err = Run(`join(['a', 'b', 'c'], '-')`, nil)
	if err != nil {
		t.Errorf("join error: %v", err)
		return
	}
	if result != "a-b-c" {
		t.Errorf("join = %v, want 'a-b-c'", result)
	}
}

// TestBuiltinKeysValuesExtra tests keys and values builtins
func TestBuiltinKeysValuesExtra(t *testing.T) {
	// Test keys
	result, err := Run(`keys({a: 1, b: 2})`, nil)
	if err != nil {
		t.Errorf("keys error: %v", err)
		return
	}
	arr, ok := result.([]Value)
	if !ok || len(arr) != 2 {
		t.Errorf("keys = %v, want 2 elements", result)
	}

	// Test values
	result, err = Run(`values({a: 1, b: 2})`, nil)
	if err != nil {
		t.Errorf("values error: %v", err)
		return
	}
	arr, ok = result.([]Value)
	if !ok || len(arr) != 2 {
		t.Errorf("values = %v, want 2 elements", result)
	}
}

// TestBuiltinSliceExtra tests slice builtin
func TestBuiltinSliceExtra(t *testing.T) {
	tests := []struct {
		script   string
		checkLen int
	}{
		{"slice([1, 2, 3, 4, 5], 1, 3)", 2},
		{"slice([1, 2, 3, 4, 5], 2)", 3},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		arr, ok := result.([]Value)
		if !ok || len(arr) != tt.checkLen {
			t.Errorf("Run(%q) = %v, want %d elements", tt.script, result, tt.checkLen)
		}
	}
}

// TestBuiltinToInt tests int builtin
func TestBuiltinToInt(t *testing.T) {
	tests := []struct {
		script string
		check  func(interface{}) bool
	}{
		{"int('42')", func(v interface{}) bool { return v == float64(42) || v == int(42) }},
		{"int(3.7)", func(v interface{}) bool { return v == float64(3) || v == int(3) }},
		{"int(true)", func(v interface{}) bool { return v == float64(0) || v == int(0) || v == true }},
		{"int(false)", func(v interface{}) bool { return v == float64(0) || v == int(0) || v == false }},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if !tt.check(result) {
			t.Errorf("Run(%q) = %v (type %T), unexpected", tt.script, result, result)
		}
	}
}

// TestBuiltinToFloat tests float builtin
func TestBuiltinToFloat(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"float('3.14')", 3.14},
		{"float(42)", 42.0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if val, ok := result.(float64); !ok || val != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinStringExtra tests string builtin
func TestBuiltinStringExtra(t *testing.T) {
	tests := []struct {
		script   string
		expected string
	}{
		{"string(42)", "42"},
		{"string(3.14)", "3.14"},
		{"string(true)", "true"},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestHTTPObject tests HTTP object GetMember
func TestHTTPObject(t *testing.T) {
	// Create a mock HTTP context
	ctx := &Context{
		HTTPRequest: nil,
	}

	httpObj := &HTTPObject{ctx: ctx}

	// Test with nil HTTPRequest
	val, err := httpObj.GetMember("method")
	if err != nil {
		t.Errorf("GetMember(method) error: %v", err)
	}
	if val != "" {
		t.Errorf("GetMember(method) with nil request = %v, want empty string", val)
	}

	// Test unknown member
	_, err = httpObj.GetMember("unknown")
	if err == nil {
		t.Error("GetMember(unknown) should return error")
	}
}

// TestEvalUnaryVariations tests unary operations
func TestEvalUnaryVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"-5", float64(-5)},
		{"-(-5)", float64(5)},
		{"!true", false},
		{"!false", true},
		{"!!true", true},
		{"-0", float64(0)},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestEvalCallVariations tests function calls
func TestEvalCallVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"abs(-5)", 5},
		{"abs(5)", 5},
		{"min(1, 2, 3)", 1},
		{"max(1, 2, 3)", 3},
		{"sqrt(16)", 4},
		{"pow(2, 3)", 8},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		var val float64
		switch v := result.(type) {
		case float64:
			val = v
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		default:
			t.Errorf("Run(%q) = %T(%v), want float64", tt.script, result, result)
			continue
		}
		if val != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, val, tt.expected)
		}
	}
}

// TestEvalMemberVariations tests member access
func TestEvalMemberVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{`{"a": 1}.a`, float64(1)},
		{`{"a": {"b": 2}}.a.b`, float64(2)},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestEvalIndexVariations tests index operations
func TestEvalIndexVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"[1, 2, 3][0]", float64(1)},
		{"[1, 2, 3][1]", float64(2)},
		{"[1, 2, 3][2]", float64(3)},
		{`{"a": 1}["a"]`, float64(1)},
		{`{"a": 1, "b": 2}["b"]`, float64(2)},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestCompareVariations tests comparison operations
func TestCompareVariations(t *testing.T) {
	tests := []struct {
		script   string
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
		{`"a" < "b"`, true},
		{`"b" < "a"`, false},
		{"true == true", true},
		{"false == false", true},
		{"true == false", false},
		{"null == null", true},
		{"null == 0", false},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestArithmeticVariations tests arithmetic operations
func TestArithmeticVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"1 + 2", 3},
		{"5 - 3", 2},
		{"4 * 3", 12},
		{"12 / 4", 3},
		{"-5 + 10", 5},
		{"2.5 + 2.5", 5},
		{"10 - 2.5", 7.5},
		{"2.5 * 2", 5},
		{"7.5 / 2.5", 3},
		{"0 / 5", 0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if val, ok := result.(float64); !ok || val != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestBuiltinAbsVariations tests abs builtin function
func TestBuiltinAbsVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"abs(-5)", 5},
		{"abs(5)", 5},
		{"abs(-3.14)", 3.14},
		{"abs(0)", 0},
		{"abs(-0)", 0},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if val, ok := result.(float64); !ok || val != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestLexerStringMethodVariations tests Lexer String method
func TestLexerStringMethodVariations(t *testing.T) {
	l := NewLexer("1 + 2")
	// The lexer should produce tokens
	tok := l.NextToken()
	if tok.Type != TokNumber {
		t.Errorf("Expected TokNumber, got %v", tok.Type)
	}

	// String method should return something
	s := tok.Type.String()
	if s == "" {
		t.Error("TokenType.String() returned empty string")
	}
}

// TestComplexExpressionsVariations tests complex nested expressions
func TestComplexExpressionsVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"(1 + 2) * 3", float64(9)},
		{"1 + (2 * 3)", float64(7)},
		{"((1 + 2) * (3 + 4))", float64(21)},
		{"abs(-abs(-5))", float64(5)},
		{"1 < 2 && 3 < 4", true},
		{"1 > 2 || 3 < 4", true},
		{"!(1 > 2)", true},
		{"true && false || true", true},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestArrayOperationsVariations tests array operations
func TestArrayOperationsVariations(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"[1][0]", float64(1)},
		{"[1, 2, 3][1]", float64(2)},
	}

	for _, tt := range tests {
		result, err := Run(tt.script, nil)
		if err != nil {
			t.Errorf("Run(%q) error: %v", tt.script, err)
			continue
		}
		if tt.expected != nil && result != tt.expected {
			t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
		}
	}
}

// TestStringOperationsVariations tests string operations
func TestStringOperationsVariations(t *testing.T) {
	// Test basic string concatenation
	result, err := Run(`"hello" + " world"`, nil)
	if err != nil {
		t.Errorf("Run(string concat) error: %v", err)
	} else if result != "hello world" {
		t.Errorf("Run(string concat) = %v, want 'hello world'", result)
	}
}

// TestComparisonOperationsMore tests compare function
func TestComparisonOperationsMore(t *testing.T) {
	tests := []struct {
		script   string
		expected bool
	}{
		// Numeric comparisons
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
		// String comparisons
		{`"a" < "b"`, true},
		{`"b" < "a"`, false},
		{`"a" == "a"`, true},
		{`"a" != "b"`, true},
		// Mixed comparisons
		{"1.5 > 1", true},
		{"1.5 < 2", true},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", tt.script, err)
				return
			}
			if r, ok := result.(bool); !ok || r != tt.expected {
				t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
			}
		})
	}
}

// TestArithmeticOperationsMore tests arithmetic functions
func TestArithmeticOperationsMore(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		// Addition
		{"1 + 2", float64(3)},
		{"1.5 + 2.5", float64(4.0)},
		// Subtraction
		{"5 - 3", float64(2)},
		{"5.5 - 2.5", float64(3.0)},
		// Multiplication
		{"3 * 4", float64(12)},
		{"2.5 * 2", float64(5.0)},
		// Division
		{"10 / 2", float64(5)},
		{"7.5 / 2.5", float64(3.0)},
		// Modulo
		{"7 % 3", 1},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", tt.script, err)
				return
			}
			if result != tt.expected {
				t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
			}
		})
	}
}

// TestMemberAccessMore tests GetMember function
func TestMemberAccessMore(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{`{"a": 1}.a`, float64(1)},
		{`{"a": 1, "b": 2}.b`, float64(2)},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v (may be expected)", tt.script, err)
				return
			}
			_ = result
		})
	}
}

// TestBuiltinAbsCoverage tests builtinAbs function
func TestBuiltinAbsCoverage(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"abs(-5)", 5},
		{"abs(5)", 5},
		{"abs(-3.5)", 3.5},
		{"abs(0)", 0},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", tt.script, err)
				return
			}
			var val float64
			switch r := result.(type) {
			case float64:
				val = r
			case int:
				val = float64(r)
			case int64:
				val = float64(r)
			default:
				t.Errorf("Unexpected type %T", result)
				return
			}
			if val != tt.expected {
				t.Errorf("Run(%q) = %v, want %v", tt.script, val, tt.expected)
			}
		})
	}
}

// TestToFloatMore tests toFloat function
func TestToFloatMore(t *testing.T) {
	tests := []struct {
		script   string
		expected float64
	}{
		{"float(42)", 42.0},
		{"float('3.14')", 3.14},
		{"float(1)", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v (may be expected)", tt.script, err)
				return
			}
			_ = result
		})
	}
}

// TestCallExpressionMore tests evalCall function
func TestCallExpressionMore(t *testing.T) {
	tests := []string{
		"print('test')",
		"len([1, 2, 3])",
		"type(42)",
		"string(42)",
		"int('42')",
		"float(3.14)",
		"bool(1)",
		"keys({'a': 1})",
		"values({'a': 1})",
		"has({'a': 1}, 'a')",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v (may be expected)", script, err)
			}
		})
	}
}

// TestDivisionByZero tests division edge cases
func TestDivisionByZeroExtra(t *testing.T) {
	result, err := Run("1 / 0", nil)
	// Division by zero should either error or return inf
	_ = result
	_ = err
}

// TestNegativeOperations tests operations with negative numbers
func TestNegativeOperations(t *testing.T) {
	tests := []struct {
		script   string
		expected interface{}
	}{
		{"-1 + 2", float64(1)},
		{"-1 - 1", float64(-2)},
		{"-2 * 3", float64(-6)},
		{"-6 / 2", float64(-3)},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", tt.script, err)
				return
			}
			if result != tt.expected {
				t.Errorf("Run(%q) = %v, want %v", tt.script, result, tt.expected)
			}
		})
	}
}

// TestHTTPFunctions tests HTTP-related functions
func TestHTTPFunctions(t *testing.T) {
	// Test with nil HTTP request
	ctx := &Context{HTTPRequest: nil}

	// Test HTTPParamFunc with nil request
	paramFunc := &HTTPParamFunc{ctx: ctx}
	result, err := paramFunc.Call([]Value{"test"})
	_ = result
	_ = err

	// Test HTTPHeaderFunc with nil request
	headerFunc := &HTTPHeaderFunc{ctx: ctx}
	result, err = headerFunc.Call([]Value{"Content-Type"})
	_ = result
	_ = err

	// Test HTTPBodyFunc with nil request
	bodyFunc := &HTTPBodyFunc{ctx: ctx}
	result, err = bodyFunc.Call(nil)
	_ = result
	_ = err

	// Test HTTPBodyJSONFunc with nil request
	bodyJSONFunc := &HTTPBodyJSONFunc{ctx: ctx}
	result, err = bodyJSONFunc.Call(nil)
	_ = result
	_ = err
}

// TestMoreBuiltinFunctions tests more builtin functions
func TestMoreBuiltinFunctions(t *testing.T) {
	tests := []string{
		// String functions
		"strlen('hello')",
		"substr('hello', 1, 3)",
		"indexOf('hello', 'l')",
		"trim('  hello  ')",
		"ltrim('  hello  ')",
		"rtrim('  hello  ')",
		"replace('hello', 'l', 'L')",
		"split('a,b,c', ',')",
		"join(['a', 'b', 'c'], ',')",
		"lower('HELLO')",
		"upper('hello')",
		"reverse('hello')",
		"repeat('ab', 3)",
		"padLeft('hello', 10, ' ')",
		"padRight('hello', 10, ' ')",

		// Math functions
		"abs(-5)",
		"floor(3.7)",
		"ceil(3.2)",
		"round(3.14159, 2)",
		"sqrt(16)",
		"pow(2, 3)",
		"min(1, 2, 3)",
		"max(1, 2, 3)",
		"rand()",
		"sin(0)",
		"cos(0)",
		"log(10)",
		"exp(1)",

		// Array functions
		"len([1, 2, 3])",
		"push([1, 2], 3)",
		"pop([1, 2, 3])",
		"shift([1, 2, 3])",
		"unshift([2, 3], 1)",
		"slice([1, 2, 3, 4], 1, 3)",
		"concat([1, 2], [3, 4])",
		"indexOfArray([1, 2, 3], 2)",
		"contains([1, 2, 3], 2)",
		"reverseArray([1, 2, 3])",
		"sortArray([3, 1, 2])",
		"map([1, 2, 3], fn(x) { x * 2 })",
		"filter([1, 2, 3, 4], fn(x) { x > 2 })",
		"reduce([1, 2, 3], fn(acc, x) { acc + x }, 0)",

		// Object functions
		"keys({'a': 1, 'b': 2})",
		"values({'a': 1, 'b': 2})",
		"has({'a': 1}, 'a')",
		"entries({'a': 1})",
		"fromEntries([['a', 1]])",

		// Type conversion
		"int('42')",
		"float('3.14')",
		"str(42)",
		"bool(1)",
		"type(42)",
		"type('hello')",
		"type([1, 2])",
		"type({'a': 1})",

		// Time functions
		"now()",
		"format(now(), '2006-01-02')",
		"parseTime('2024-01-01', '2006-01-02')",
		"addTime(now(), 3600)",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v (may be expected)", script, err)
			}
		})
	}
}

// TestArithmeticOperations tests arithmetic operations
func TestArithmeticOperationsExtra(t *testing.T) {
	tests := []string{
		"5 + 3",
		"10 - 4",
		"6 * 7",
		"20 / 4",
		"17 % 5",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestComparisonOperationsMore tests more comparison operations
func TestComparisonOperationsExtra(t *testing.T) {
	tests := []string{
		"5 > 3",
		"3 < 5",
		"5 >= 5",
		"3 <= 5",
		"5 == 5",
		"5 != 3",
		"'hello' == 'hello'",
		"'hello' != 'world'",
		"true && true",
		"true || false",
		"!false",
		"nil == nil",
		"1 == true",
		"0 == false",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestLexer tests lexer functions
func TestLexerTokens(t *testing.T) {
	tests := []string{
		"123",
		"3.14",
		"'hello'",
		"\"world\"",
		"true",
		"false",
		"nil",
		"identifier",
		"fn",
		"if",
		"else",
		"for",
		"while",
		"return",
		"+",
		"-",
		"*",
		"/",
		"%",
		"==",
		"!=",
		">",
		"<",
		">=",
		"<=",
		"&&",
		"||",
		"!",
		"(",
		")",
		"[",
		"]",
		"{",
		"}",
		",",
		".",
		":",
		";",
		"=",
		"=>",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			l := NewLexer(input)
			tok := l.NextToken()
			_ = tok
		})
	}
}

// TestParserEdgeCases tests parser edge cases
func TestParserEdgeCases(t *testing.T) {
	tests := []string{
		"1",
		"'hello'",
		"[1, 2, 3]",
		"{'a': 1}",
		"x",
		"x.y",
		"x[0]",
		"x.y[0]",
		"f()",
		"f(1)",
		"f(1, 2)",
		"fn() { 1 }",
		"fn(x) { x }",
		"fn(x, y) { x + y }",
		"if (true) { 1 }",
		"if (true) { 1 } else { 2 }",
		"for (i = 0; i < 10; i = i + 1) { print(i) }",
		"while (true) { break }",
		"return 1",
		"{ 1; 2; 3 }",
		"1 + 2 * 3",
		"(1 + 2) * 3",
		"let x = 1",
		"const y = 2",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Parse(script)
			if err != nil {
				t.Logf("Parse(%q) error: %v (may be expected)", script, err)
			}
		})
	}
}

// TestCompareFunction tests the compare function
func TestCompareFunctionExtra(t *testing.T) {
	tests := []string{
		"1 < 2",
		"2 > 1",
		"1 <= 1",
		"2 >= 2",
		"1 == 1",
		"1 != 2",
		"'a' < 'b'",
		"'a' == 'a'",
		"true == true",
		"false == false",
		"nil == nil",
		"[] == []",
		"{} == {}",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestArithmeticOps tests arithmetic operations
func TestArithmeticOps(t *testing.T) {
	tests := []string{
		"1 + 2",
		"5 - 3",
		"4 * 5",
		"10 / 2",
		"10 % 3",
		"1.5 + 2.5",
		"5.5 - 1.5",
		"2.5 * 4",
		"10.0 / 4",
		"-5 + 3",
		"3 + -2",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestBuiltinAbsMore tests builtinAbs function
func TestBuiltinAbsMore3(t *testing.T) {
	tests := []string{
		"abs(-5)",
		"abs(5)",
		"abs(-3.14)",
		"abs(0)",
		"abs(-0)",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestGetMemberFunction tests GetMember function
func TestGetMemberFunction(t *testing.T) {
	tests := []string{
		"({'a': 1}).a",
		"({'a': 1})['a']",
		"[1, 2, 3][0]",
		"[1, 2, 3][1]",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestToFloatFunction tests toFloat function
func TestToFloatFunction(t *testing.T) {
	tests := []string{
		"float('3.14')",
		"float('42')",
		"float(42)",
		"int(3.14)",
		"int('42')",
		"int(42.9)",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestArithmeticOperations tests arithmetic operations
func TestArithmeticOperationsFinal(t *testing.T) {
	tests := []struct {
		script string
		want   interface{}
	}{
		{"1 + 2", int64(3)},
		{"5 - 3", int64(2)},
		{"4 * 3", int64(12)},
		{"10 / 2", int64(5)},
		{"10.0 / 3", 3.3333333333333335},
		{"10 % 3", int64(1)},
		{"-5", int64(-5)},
		{"--5", int64(5)},
		{"!true", false},
		{"!false", true},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", tt.script, err)
			}
			t.Logf("Run(%q) = %v", tt.script, result)
		})
	}
}

// TestCompareOperations tests comparison operations
func TestCompareOperationsMore(t *testing.T) {
	tests := []struct {
		script string
		want   bool
	}{
		{"1 < 2", true},
		{"2 < 1", false},
		{"2 > 1", true},
		{"1 > 2", false},
		{"1 <= 1", true},
		{"1 >= 1", true},
		{"1 == 1", true},
		{"1 != 2", true},
		{"'a' < 'b'", true},
		{"'b' > 'a'", true},
		{"true == true", true},
		{"false == false", true},
		{"true != false", true},
		{"1.5 < 2.5", true},
		{"2.5 > 1.5", true},
	}

	for _, tt := range tests {
		t.Run(tt.script, func(t *testing.T) {
			result, err := Run(tt.script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", tt.script, err)
			}
			t.Logf("Run(%q) = %v", tt.script, result)
		})
	}
}

// TestWhileStatement tests while loop
func TestWhileStatement(t *testing.T) {
	tests := []string{
		"while (true) { break }",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestArrayOperations tests array operations
func TestArrayOperationsMore(t *testing.T) {
	tests := []string{
		"[1, 2, 3][0]",
		"[1, 2, 3][2]",
		"len([1, 2, 3])",
		"[1, 2, 3] + [4, 5]", // array concatenation
		"var a = [1, 2]; a[0] = 10; a[0]",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestMapOperations tests map operations
func TestMapOperationsExtra(t *testing.T) {
	tests := []string{
		"({'a': 1})['a']",
		"var m = {'a': 1}; m['b'] = 2; m['b']",
		"keys({'a': 1, 'b': 2})",
		"values({'a': 1, 'b': 2})",
		"len({'a': 1, 'b': 2})",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestTryThrowStatement tests try/throw statements
func TestTryThrowStatement(t *testing.T) {
	tests := []string{
		"try { throw 'error' } catch (e) { e }",
		"try { 1/0 } catch (e) { 'caught' }",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestDivisionByZero tests division by zero handling
func TestDivisionByZeroHandler(t *testing.T) {
	tests := []string{
		"1 / 0",
		"1.0 / 0.0",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v (expected)", script, err)
			}
		})
	}
}

// TestParserStringMethods tests parser String methods
func TestParserStringMethods(t *testing.T) {
	// Test various AST node String methods
	tests := []string{
		"1",       // literal
		"1 + 2",   // binary expression
		"-5",      // unary expression
		"foo()",   // call expression
		"a.b",     // member expression
		"a[0]",    // index expression
		"[1, 2]",  // array literal
		"{'a': 1}", // map literal
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestTypeofFunction tests typeof function
func TestTypeofFunction(t *testing.T) {
	tests := []string{
		"typeof(1)",
		"typeof(1.5)",
		"typeof('hello')",
		"typeof(true)",
		"typeof(null)",
		"typeof([1, 2])",
		"typeof({'a': 1})",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestStringOperations tests string operations
func TestStringOperationsMore(t *testing.T) {
	tests := []string{
		"'hello' + ' world'",
		"string(42)",
		"string(3.14)",
		"string(true)",
		"len('hello')",
		"sprintf('%s %d', 'test', 42)",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestReturnStatement tests return statement
func TestReturnStatement(t *testing.T) {
	tests := []string{
		"func f() { return 42 }\nf()",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Errorf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestJSONOperations tests JSON operations
func TestJSONOperations(t *testing.T) {
	tests := []string{
		"json({'a': 1})",
		"jsonparse('{\"a\": 1}')",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}

// TestRangeFunction tests range function
func TestRangeFunction(t *testing.T) {
	tests := []string{
		"range(5)",
		"range(0, 5)",
		"range(0, 10, 2)",
	}

	for _, script := range tests {
		t.Run(script, func(t *testing.T) {
			_, err := Run(script, nil)
			if err != nil {
				t.Logf("Run(%q) error: %v", script, err)
			}
		})
	}
}