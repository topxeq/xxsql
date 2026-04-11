package xxscript

import (
	"testing"
)

func TestParse_SimpleExpression(t *testing.T) {
	prog, err := Parse("1 + 2")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(prog.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(prog.Statements))
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	bin, ok := exprStmt.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected BinaryExpr, got %T", exprStmt.Expr)
	}
	if bin.Op != TokPlus {
		t.Errorf("Expected +, got %s", bin.Op)
	}
}

func TestParse_VarStmt(t *testing.T) {
	prog, err := Parse("var x = 10")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	varStmt, ok := prog.Statements[0].(*VarStmt)
	if !ok {
		t.Fatalf("Expected VarStmt, got %T", prog.Statements[0])
	}
	if varStmt.Name != "x" {
		t.Errorf("Expected name 'x', got %s", varStmt.Name)
	}
}

func TestParse_VarStmtNoValue(t *testing.T) {
	prog, err := Parse("var x")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	varStmt, ok := prog.Statements[0].(*VarStmt)
	if !ok {
		t.Fatalf("Expected VarStmt, got %T", prog.Statements[0])
	}
	if varStmt.Value != nil {
		t.Errorf("Expected nil value, got %v", varStmt.Value)
	}
}

func TestParse_MultiVarDestructuring(t *testing.T) {
	prog, err := Parse("var a, b = [1, 2]")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	varStmt, ok := prog.Statements[0].(*VarStmt)
	if !ok {
		t.Fatalf("Expected VarStmt, got %T", prog.Statements[0])
	}
	if len(varStmt.Names) != 2 {
		t.Errorf("Expected 2 names, got %d", len(varStmt.Names))
	}
}

func TestParse_IfStmt(t *testing.T) {
	prog, err := Parse("if (x > 0) { x = 1 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	ifStmt, ok := prog.Statements[0].(*IfStmt)
	if !ok {
		t.Fatalf("Expected IfStmt, got %T", prog.Statements[0])
	}
	if ifStmt.Else != nil {
		t.Errorf("Expected nil else")
	}
}

func TestParse_IfElseStmt(t *testing.T) {
	prog, err := Parse("if (x > 0) { x = 1 } else { x = 2 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	ifStmt, ok := prog.Statements[0].(*IfStmt)
	if !ok {
		t.Fatalf("Expected IfStmt, got %T", prog.Statements[0])
	}
	if ifStmt.Else == nil {
		t.Errorf("Expected else block")
	}
}

func TestParse_IfElseIfStmt(t *testing.T) {
	prog, err := Parse("if (x > 0) { x = 1 } else if (x < 0) { x = -1 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	ifStmt, ok := prog.Statements[0].(*IfStmt)
	if !ok {
		t.Fatalf("Expected IfStmt, got %T", prog.Statements[0])
	}
	elseIf, ok := ifStmt.Else.(*IfStmt)
	if !ok {
		t.Fatalf("Expected IfStmt in else, got %T", ifStmt.Else)
	}
	_ = elseIf
}

func TestParse_ForStmt(t *testing.T) {
	prog, err := Parse("for (var i = 0; i < 10; i = i + 1) { sum = sum + i }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	forStmt, ok := prog.Statements[0].(*ForStmt)
	if !ok {
		t.Fatalf("Expected ForStmt, got %T", prog.Statements[0])
	}
	if forStmt.Init == nil || forStmt.Condition == nil || forStmt.Update == nil {
		t.Errorf("ForStmt missing parts")
	}
}

func TestParse_ForInStmt(t *testing.T) {
	tests := []struct {
		input       string
		expectKey   string
		expectValue string
	}{
		{"for v in arr { }", "_", "v"},
		{"for k, v in arr { }", "k", "v"},
	}

	for _, tt := range tests {
		prog, err := Parse(tt.input)
		if err != nil {
			t.Fatalf("Parse error for %s: %v", tt.input, err)
		}
		forIn, ok := prog.Statements[0].(*ForInStmt)
		if !ok {
			t.Fatalf("Expected ForInStmt, got %T", prog.Statements[0])
		}
		if forIn.KeyVar != tt.expectKey {
			t.Errorf("Expected key %s, got %s", tt.expectKey, forIn.KeyVar)
		}
		if forIn.ValueVar != tt.expectValue {
			t.Errorf("Expected value %s, got %s", tt.expectValue, forIn.ValueVar)
		}
	}
}

func TestParse_WhileStmt(t *testing.T) {
	prog, err := Parse("while (x < 10) { x = x + 1 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	whileStmt, ok := prog.Statements[0].(*WhileStmt)
	if !ok {
		t.Fatalf("Expected WhileStmt, got %T", prog.Statements[0])
	}
	if whileStmt.Body == nil {
		t.Errorf("WhileStmt missing body")
	}
}

func TestParse_FuncStmt(t *testing.T) {
	prog, err := Parse("func add(a, b) { return a + b }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	funcStmt, ok := prog.Statements[0].(*FuncStmt)
	if !ok {
		t.Fatalf("Expected FuncStmt, got %T", prog.Statements[0])
	}
	if funcStmt.Name != "add" {
		t.Errorf("Expected name 'add', got %s", funcStmt.Name)
	}
	if len(funcStmt.Params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(funcStmt.Params))
	}
}

func TestParse_FuncStmtWithDefaults(t *testing.T) {
	prog, err := Parse("func greet(name, greeting = \"Hello\") { return greeting + \" \" + name }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	funcStmt, ok := prog.Statements[0].(*FuncStmt)
	if !ok {
		t.Fatalf("Expected FuncStmt, got %T", prog.Statements[0])
	}
	if funcStmt.Params[1].DefaultValue == nil {
		t.Errorf("Expected default value for greeting")
	}
}

func TestParse_FuncStmtWithRestParam(t *testing.T) {
	prog, err := Parse("func sumAll(...nums) { return nums }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	funcStmt, ok := prog.Statements[0].(*FuncStmt)
	if !ok {
		t.Fatalf("Expected FuncStmt, got %T", prog.Statements[0])
	}
	if !funcStmt.Params[0].IsRest {
		t.Errorf("Expected rest parameter")
	}
}

func TestParse_ReturnStmt(t *testing.T) {
	prog, err := Parse("return 42")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	retStmt, ok := prog.Statements[0].(*ReturnStmt)
	if !ok {
		t.Fatalf("Expected ReturnStmt, got %T", prog.Statements[0])
	}
	if retStmt.Value == nil {
		t.Errorf("Expected return value")
	}
}

func TestParse_ReturnStmtNoValue(t *testing.T) {
	prog, err := Parse("return")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	retStmt, ok := prog.Statements[0].(*ReturnStmt)
	if !ok {
		t.Fatalf("Expected ReturnStmt, got %T", prog.Statements[0])
	}
	if retStmt.Value != nil {
		t.Errorf("Expected nil return value")
	}
}

func TestParse_BreakStmt(t *testing.T) {
	prog, err := Parse("break")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	_, ok := prog.Statements[0].(*BreakStmt)
	if !ok {
		t.Fatalf("Expected BreakStmt, got %T", prog.Statements[0])
	}
}

func TestParse_ContinueStmt(t *testing.T) {
	prog, err := Parse("continue")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	_, ok := prog.Statements[0].(*ContinueStmt)
	if !ok {
		t.Fatalf("Expected ContinueStmt, got %T", prog.Statements[0])
	}
}

func TestParse_TryCatchStmt(t *testing.T) {
	prog, err := Parse("try { throw \"error\" } catch (e) { print(e) }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	tryStmt, ok := prog.Statements[0].(*TryStmt)
	if !ok {
		t.Fatalf("Expected TryStmt, got %T", prog.Statements[0])
	}
	if tryStmt.CatchVar != "e" {
		t.Errorf("Expected catch var 'e', got %s", tryStmt.CatchVar)
	}
}

func TestParse_TryStmtNoCatch(t *testing.T) {
	prog, err := Parse("try { x = 1 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	tryStmt, ok := prog.Statements[0].(*TryStmt)
	if !ok {
		t.Fatalf("Expected TryStmt, got %T", prog.Statements[0])
	}
	if tryStmt.CatchBlock != nil {
		t.Errorf("Expected nil catch block")
	}
}

func TestParse_ThrowStmt(t *testing.T) {
	prog, err := Parse("throw \"error\"")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	throwStmt, ok := prog.Statements[0].(*ThrowStmt)
	if !ok {
		t.Fatalf("Expected ThrowStmt, got %T", prog.Statements[0])
	}
	if throwStmt.Error == nil {
		t.Errorf("Expected throw expression")
	}
}

func TestParse_SwitchStmt(t *testing.T) {
	prog, err := Parse("switch x { case 1: { break } default: { break } }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	switchStmt, ok := prog.Statements[0].(*SwitchStmt)
	if !ok {
		t.Fatalf("Expected SwitchStmt, got %T", prog.Statements[0])
	}
	if len(switchStmt.Cases) != 2 {
		t.Errorf("Expected 2 cases, got %d", len(switchStmt.Cases))
	}
	if switchStmt.Cases[1].Values != nil {
		t.Errorf("Expected nil values for default case")
	}
}

func TestParse_SwitchMultipleCaseValues(t *testing.T) {
	prog, err := Parse("switch x { case 1, 2, 3: { break } }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	switchStmt, ok := prog.Statements[0].(*SwitchStmt)
	if !ok {
		t.Fatalf("Expected SwitchStmt, got %T", prog.Statements[0])
	}
	if len(switchStmt.Cases[0].Values) != 3 {
		t.Errorf("Expected 3 case values, got %d", len(switchStmt.Cases[0].Values))
	}
}

func TestParse_ArrayLiteral(t *testing.T) {
	prog, err := Parse("[1, 2, 3]")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	arr, ok := exprStmt.Expr.(*ArrayExpr)
	if !ok {
		t.Fatalf("Expected ArrayExpr, got %T", exprStmt.Expr)
	}
	if len(arr.Elements) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(arr.Elements))
	}
}

func TestParse_ArrayLiteralWithSpread(t *testing.T) {
	prog, err := Parse("[...arr, 4, 5]")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	arr, ok := exprStmt.Expr.(*ArrayExpr)
	if !ok {
		t.Fatalf("Expected ArrayExpr, got %T", exprStmt.Expr)
	}
	_, ok = arr.Elements[0].(*SpreadExpr)
	if !ok {
		t.Errorf("Expected SpreadExpr as first element")
	}
}

func TestParse_MapLiteral(t *testing.T) {
	prog, err := Parse("{\"name\": \"Alice\", \"age\": 30}")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	mapExpr, ok := exprStmt.Expr.(*MapExpr)
	if !ok {
		t.Fatalf("Expected MapExpr, got %T", exprStmt.Expr)
	}
	if len(mapExpr.Pairs) != 2 {
		t.Errorf("Expected 2 pairs, got %d", len(mapExpr.Pairs))
	}
}

func TestParse_MapLiteralWithIdentifierKeys(t *testing.T) {
	prog, err := Parse("{name: \"Alice\", age: 30}")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	mapExpr, ok := exprStmt.Expr.(*MapExpr)
	if !ok {
		t.Fatalf("Expected MapExpr, got %T", exprStmt.Expr)
	}
	if _, ok := mapExpr.Pairs["name"]; !ok {
		t.Errorf("Expected 'name' key")
	}
}

func TestParse_BlockStmt(t *testing.T) {
	prog, err := Parse("{ var x = 1; var y = 2 }")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	block, ok := prog.Statements[0].(*BlockStmt)
	if !ok {
		t.Fatalf("Expected BlockStmt, got %T", prog.Statements[0])
	}
	if len(block.Statements) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(block.Statements))
	}
}

func TestParse_CallExpr(t *testing.T) {
	prog, err := Parse("foo(1, 2, 3)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	call, ok := exprStmt.Expr.(*CallExpr)
	if !ok {
		t.Fatalf("Expected CallExpr, got %T", exprStmt.Expr)
	}
	if len(call.Args) != 3 {
		t.Errorf("Expected 3 args, got %d", len(call.Args))
	}
}

func TestParse_CallExprWithSpread(t *testing.T) {
	prog, err := Parse("foo(...args)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	call, ok := exprStmt.Expr.(*CallExpr)
	if !ok {
		t.Fatalf("Expected CallExpr, got %T", exprStmt.Expr)
	}
	_, ok = call.Args[0].(*SpreadExpr)
	if !ok {
		t.Errorf("Expected SpreadExpr in args")
	}
}

func TestParse_MemberExpr(t *testing.T) {
	prog, err := Parse("obj.field")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	member, ok := exprStmt.Expr.(*MemberExpr)
	if !ok {
		t.Fatalf("Expected MemberExpr, got %T", exprStmt.Expr)
	}
	strExpr, ok := member.Member.(*StringExpr)
	if !ok {
		t.Fatalf("Expected StringExpr for member, got %T", member.Member)
	}
	if strExpr.Value != "field" {
		t.Errorf("Expected 'field', got %s", strExpr.Value)
	}
}

func TestParse_IndexExpr(t *testing.T) {
	prog, err := Parse("arr[0]")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	index, ok := exprStmt.Expr.(*IndexExpr)
	if !ok {
		t.Fatalf("Expected IndexExpr, got %T", exprStmt.Expr)
	}
	_ = index
}

func TestParse_TernaryExpr(t *testing.T) {
	prog, err := Parse("x > 0 ? \"positive\" : \"negative\"")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	ternary, ok := exprStmt.Expr.(*TernaryExpr)
	if !ok {
		t.Fatalf("Expected TernaryExpr, got %T", exprStmt.Expr)
	}
	if ternary.TrueExpr == nil || ternary.FalseExpr == nil {
		t.Errorf("TernaryExpr missing expressions")
	}
}

func TestParse_CompoundAssign(t *testing.T) {
	ops := []struct {
		input string
		op    TokenType
	}{
		{"x += 1", TokPlusAssign},
		{"x -= 1", TokMinusAssign},
		{"x *= 2", TokStarAssign},
		{"x /= 2", TokSlashAssign},
		{"x %= 2", TokPercentAssign},
	}

	for _, tt := range ops {
		prog, err := Parse(tt.input)
		if err != nil {
			t.Fatalf("Parse error for %s: %v", tt.input, err)
		}
		exprStmt, ok := prog.Statements[0].(*ExprStmt)
		if !ok {
			t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
		}
		compound, ok := exprStmt.Expr.(*CompoundAssignExpr)
		if !ok {
			t.Fatalf("Expected CompoundAssignExpr, got %T", exprStmt.Expr)
		}
		if compound.Op != tt.op {
			t.Errorf("Expected %s, got %s", tt.op, compound.Op)
		}
	}
}

func TestParse_PreIncDec(t *testing.T) {
	prog, err := Parse("++x")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	pre, ok := exprStmt.Expr.(*PreIncDecExpr)
	if !ok {
		t.Fatalf("Expected PreIncDecExpr, got %T", exprStmt.Expr)
	}
	if pre.Op != TokInc {
		t.Errorf("Expected TokInc, got %s", pre.Op)
	}
}

func TestParse_PostIncDec(t *testing.T) {
	prog, err := Parse("x++")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	post, ok := exprStmt.Expr.(*PostIncDecExpr)
	if !ok {
		t.Fatalf("Expected PostIncDecExpr, got %T", exprStmt.Expr)
	}
	if post.Op != TokInc {
		t.Errorf("Expected TokInc, got %s", post.Op)
	}
}

func TestParse_Unary(t *testing.T) {
	tests := []struct {
		input string
		op    TokenType
	}{
		{"!x", TokNot},
		{"-x", TokMinus},
	}

	for _, tt := range tests {
		prog, err := Parse(tt.input)
		if err != nil {
			t.Fatalf("Parse error for %s: %v", tt.input, err)
		}
		exprStmt, ok := prog.Statements[0].(*ExprStmt)
		if !ok {
			t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
		}
		unary, ok := exprStmt.Expr.(*UnaryExpr)
		if !ok {
			t.Fatalf("Expected UnaryExpr, got %T", exprStmt.Expr)
		}
		if unary.Op != tt.op {
			t.Errorf("Expected %s, got %s", tt.op, unary.Op)
		}
	}
}

func TestParse_Literals(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"42", float64(42)},
		{"3.14", 3.14},
		{"true", true},
		{"false", false},
		{"null", nil},
		{`"hello"`, "hello"},
	}

	for _, tt := range tests {
		prog, err := Parse(tt.input)
		if err != nil {
			t.Fatalf("Parse error for %s: %v", tt.input, err)
		}
		exprStmt, ok := prog.Statements[0].(*ExprStmt)
		if !ok {
			t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
		}
		_ = exprStmt
	}
}

func TestParse_Error(t *testing.T) {
	_, err := Parse("@@@")
	if err == nil {
		t.Errorf("Expected parse error")
	}
}

func TestParse_EmptyProgram(t *testing.T) {
	prog, err := Parse("")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(prog.Statements) != 0 {
		t.Errorf("Expected empty program")
	}
}

func TestParse_Semicolons(t *testing.T) {
	prog, err := Parse("var x = 1; var y = 2;")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(prog.Statements) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(prog.Statements))
	}
}

func TestParse_MultiTargetAssign(t *testing.T) {
	prog, err := Parse("a, b = [1, 2]")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	assign, ok := exprStmt.Expr.(*AssignExpr)
	if !ok {
		t.Fatalf("Expected AssignExpr, got %T", exprStmt.Expr)
	}
	if len(assign.Lefts) != 2 {
		t.Errorf("Expected 2 lefts, got %d", len(assign.Lefts))
	}
}

func TestParse_SpreadExpr(t *testing.T) {
	prog, err := Parse("foo(...args, 1)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	exprStmt, ok := prog.Statements[0].(*ExprStmt)
	if !ok {
		t.Fatalf("Expected ExprStmt, got %T", prog.Statements[0])
	}
	call, ok := exprStmt.Expr.(*CallExpr)
	if !ok {
		t.Fatalf("Expected CallExpr, got %T", exprStmt.Expr)
	}
	_, ok = call.Args[0].(*SpreadExpr)
	if !ok {
		t.Errorf("Expected SpreadExpr")
	}
}

func TestNode_String(t *testing.T) {
	tests := []struct {
		node Node
	}{
		{&IdentExpr{Name: "x"}},
		{&NumberExpr{Value: 42}},
		{&StringExpr{Value: "hello"}},
		{&BoolExpr{Value: true}},
		{&NullExpr{}},
		{&ArrayExpr{Elements: []Expression{&NumberExpr{Value: 1}}}},
		{&MapExpr{Pairs: map[string]Expression{"a": &NumberExpr{Value: 1}}}},
		{&BinaryExpr{Left: &NumberExpr{Value: 1}, Op: TokPlus, Right: &NumberExpr{Value: 2}}},
		{&UnaryExpr{Op: TokNot, Expr: &IdentExpr{Name: "x"}}},
		{&CallExpr{Func: &IdentExpr{Name: "foo"}, Args: []Expression{&NumberExpr{Value: 1}}}},
		{&TernaryExpr{Condition: &BoolExpr{Value: true}, TrueExpr: &NumberExpr{Value: 1}, FalseExpr: &NumberExpr{Value: 2}}},
		{&VarStmt{Name: "x", Value: &NumberExpr{Value: 1}}},
		{&BreakStmt{}},
		{&ContinueStmt{}},
		{&ReturnStmt{Value: &NumberExpr{Value: 42}}},
		{&ThrowStmt{Error: &StringExpr{Value: "error"}}},
	}

	for _, tt := range tests {
		s := tt.node.String()
		if s == "" {
			t.Errorf("String() returned empty for %T", tt.node)
		}
	}
}

func TestBaseNode_Line(t *testing.T) {
	n := &BaseNode{}
	n.SetLine(42)
	if n.Line() != 42 {
		t.Errorf("Expected line 42, got %d", n.Line())
	}
}

func TestSwitchCase_String(t *testing.T) {
	defaultCase := &SwitchCase{Values: nil, Body: &BlockStmt{}}
	if defaultCase.String() != "default" {
		t.Errorf("Expected 'default', got %s", defaultCase.String())
	}

	caseExpr := &SwitchCase{Values: []Expression{&NumberExpr{Value: 1}}, Body: &BlockStmt{}}
	s := caseExpr.String()
	if s == "" {
		t.Errorf("String() returned empty")
	}
}
