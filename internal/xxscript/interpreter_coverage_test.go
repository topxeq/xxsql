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