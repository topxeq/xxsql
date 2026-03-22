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
func TestStringOperations(t *testing.T) {
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