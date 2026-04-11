// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"fmt"
	"strconv"
)

// Node represents an AST node with line tracking.
type Node interface {
	node()
	String() string
	Line() int
	SetLine(int)
}

// BaseNode provides common fields for all AST nodes.
type BaseNode struct {
	line int
}

func (n *BaseNode) Line() int     { return n.line }
func (n *BaseNode) SetLine(l int) { n.line = l }

// Expression represents an expression.
type Expression interface {
	Node
	exprNode()
}

// Statement represents a statement.
type Statement interface {
	Node
	stmtNode()
}

// ============================================================================
// Expressions
// ============================================================================

// IdentExpr represents an identifier.
type IdentExpr struct {
	BaseNode
	Name string
}

func (e *IdentExpr) node()          {}
func (e *IdentExpr) exprNode()      {}
func (e *IdentExpr) String() string { return e.Name }

// NumberExpr represents a number literal.
type NumberExpr struct {
	BaseNode
	Value float64
}

func (e *NumberExpr) node()          {}
func (e *NumberExpr) exprNode()      {}
func (e *NumberExpr) String() string { return fmt.Sprintf("%v", e.Value) }

// StringExpr represents a string literal.
type StringExpr struct {
	BaseNode
	Value string
}

func (e *StringExpr) node()          {}
func (e *StringExpr) exprNode()      {}
func (e *StringExpr) String() string { return fmt.Sprintf("%q", e.Value) }

// BoolExpr represents a boolean literal.
type BoolExpr struct {
	BaseNode
	Value bool
}

func (e *BoolExpr) node()          {}
func (e *BoolExpr) exprNode()      {}
func (e *BoolExpr) String() string { return fmt.Sprintf("%v", e.Value) }

// NullExpr represents null.
type NullExpr struct {
	BaseNode
}

func (e *NullExpr) node()          {}
func (e *NullExpr) exprNode()      {}
func (e *NullExpr) String() string { return "null" }

// ArrayExpr represents an array literal.
type ArrayExpr struct {
	BaseNode
	Elements []Expression
}

func (e *ArrayExpr) node()     {}
func (e *ArrayExpr) exprNode() {}
func (e *ArrayExpr) String() string {
	return fmt.Sprintf("%v", e.Elements)
}

// MapExpr represents a map/object literal.
type MapExpr struct {
	BaseNode
	Pairs map[string]Expression
}

func (e *MapExpr) node()          {}
func (e *MapExpr) exprNode()      {}
func (e *MapExpr) String() string { return fmt.Sprintf("%v", e.Pairs) }

// SpreadExpr represents a spread expression: ...expr
type SpreadExpr struct {
	BaseNode
	Expr Expression
}

func (e *SpreadExpr) node()          {}
func (e *SpreadExpr) exprNode()      {}
func (e *SpreadExpr) String() string { return fmt.Sprintf("...%s", e.Expr) }

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	BaseNode
	Left  Expression
	Op    TokenType
	Right Expression
}

func (e *BinaryExpr) node()     {}
func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	BaseNode
	Op   TokenType
	Expr Expression
}

func (e *UnaryExpr) node()     {}
func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) String() string {
	return fmt.Sprintf("(%s %s)", e.Op, e.Expr)
}

// CallExpr represents a function call.
type CallExpr struct {
	BaseNode
	Func Expression
	Args []Expression
}

func (e *CallExpr) node()     {}
func (e *CallExpr) exprNode() {}
func (e *CallExpr) String() string {
	return fmt.Sprintf("%s(%v)", e.Func, e.Args)
}

// MemberExpr represents a member access (obj.field or obj["field"]).
type MemberExpr struct {
	BaseNode
	Object Expression
	Member Expression
}

func (e *MemberExpr) node()     {}
func (e *MemberExpr) exprNode() {}
func (e *MemberExpr) String() string {
	return fmt.Sprintf("%s.%s", e.Object, e.Member)
}

// IndexExpr represents an index access (arr[index]).
type IndexExpr struct {
	BaseNode
	Object Expression
	Index  Expression
}

func (e *IndexExpr) node()     {}
func (e *IndexExpr) exprNode() {}
func (e *IndexExpr) String() string {
	return fmt.Sprintf("%s[%s]", e.Object, e.Index)
}

// AssignExpr represents an assignment expression.
type AssignExpr struct {
	BaseNode
	Left  Expression   // Single target
	Lefts []Expression // Multiple targets (for destructuring)
	Value Expression
}

func (e *AssignExpr) node()     {}
func (e *AssignExpr) exprNode() {}
func (e *AssignExpr) String() string {
	if len(e.Lefts) > 1 {
		return fmt.Sprintf("%s = %s", e.Lefts, e.Value)
	}
	return fmt.Sprintf("%s = %s", e.Left, e.Value)
}

// CompoundAssignExpr represents a compound assignment expression (+=, -=, *=, /=, %=).
type CompoundAssignExpr struct {
	BaseNode
	Left  Expression
	Op    TokenType // TokPlusAssign, TokMinusAssign, etc.
	Value Expression
}

func (e *CompoundAssignExpr) node()     {}
func (e *CompoundAssignExpr) exprNode() {}
func (e *CompoundAssignExpr) String() string {
	return fmt.Sprintf("%s %s= %s", e.Left, e.Op, e.Value)
}

// PreIncDecExpr represents a prefix increment/decrement expression (++i, --i).
type PreIncDecExpr struct {
	BaseNode
	Op   TokenType // TokInc or TokDec
	Expr Expression
}

func (e *PreIncDecExpr) node()     {}
func (e *PreIncDecExpr) exprNode() {}
func (e *PreIncDecExpr) String() string {
	return fmt.Sprintf("%s%s", e.Op, e.Expr)
}

// PostIncDecExpr represents a postfix increment/decrement expression (i++, i--).
type PostIncDecExpr struct {
	BaseNode
	Expr Expression
	Op   TokenType // TokInc or TokDec
}

func (e *PostIncDecExpr) node()     {}
func (e *PostIncDecExpr) exprNode() {}
func (e *PostIncDecExpr) String() string {
	return fmt.Sprintf("%s%s", e.Expr, e.Op)
}

// TernaryExpr represents a ternary conditional expression (condition ? true_expr : false_expr).
type TernaryExpr struct {
	BaseNode
	Condition Expression
	TrueExpr  Expression
	FalseExpr Expression
}

func (e *TernaryExpr) node()     {}
func (e *TernaryExpr) exprNode() {}
func (e *TernaryExpr) String() string {
	return fmt.Sprintf("%s ? %s : %s", e.Condition, e.TrueExpr, e.FalseExpr)
}

// MultiReturnExpr represents multiple return values from a function.
// Used internally when a function returns multiple values.
type MultiReturnExpr struct {
	BaseNode
	Values []Expression
}

func (e *MultiReturnExpr) node()     {}
func (e *MultiReturnExpr) exprNode() {}
func (e *MultiReturnExpr) String() string {
	return fmt.Sprintf("(%v)", e.Values)
}

// ============================================================================
// Statements
// ============================================================================

// VarStmt represents a variable declaration.
type VarStmt struct {
	BaseNode
	Name  string   // Single variable name (for backward compatibility)
	Names []string // Multiple variable names (for destructuring)
	Value Expression
}

func (s *VarStmt) node()     {}
func (s *VarStmt) stmtNode() {}
func (s *VarStmt) String() string {
	if len(s.Names) > 1 {
		return fmt.Sprintf("var %s = %s", s.Names, s.Value)
	}
	if s.Value != nil {
		return fmt.Sprintf("var %s = %s", s.Name, s.Value)
	}
	return fmt.Sprintf("var %s", s.Name)
}

// ExprStmt represents an expression statement.
type ExprStmt struct {
	BaseNode
	Expr Expression
}

func (s *ExprStmt) node()          {}
func (s *ExprStmt) stmtNode()      {}
func (s *ExprStmt) String() string { return s.Expr.String() }

// BlockStmt represents a block of statements.
type BlockStmt struct {
	BaseNode
	Statements []Statement
}

func (s *BlockStmt) node()          {}
func (s *BlockStmt) stmtNode()      {}
func (s *BlockStmt) String() string { return fmt.Sprintf("{ %v }", s.Statements) }

// IfStmt represents an if statement.
type IfStmt struct {
	BaseNode
	Condition Expression
	Then      *BlockStmt
	Else      Statement // can be *BlockStmt or *IfStmt
}

func (s *IfStmt) node()     {}
func (s *IfStmt) stmtNode() {}
func (s *IfStmt) String() string {
	if s.Else != nil {
		return fmt.Sprintf("if %s %s else %s", s.Condition, s.Then, s.Else)
	}
	return fmt.Sprintf("if %s %s", s.Condition, s.Then)
}

// ForStmt represents a for loop.
type ForStmt struct {
	BaseNode
	Init      Statement
	Condition Expression
	Update    Statement
	Body      *BlockStmt
}

func (s *ForStmt) node()     {}
func (s *ForStmt) stmtNode() {}
func (s *ForStmt) String() string {
	return fmt.Sprintf("for (%s; %s; %s) %s", s.Init, s.Condition, s.Update, s.Body)
}

// ForInStmt represents a for-in loop: for k, v in expr { ... }
type ForInStmt struct {
	BaseNode
	KeyVar   string     // Key variable name (index for arrays, key for maps)
	ValueVar string     // Value variable name (optional)
	Iterable Expression // Expression to iterate over
	Body     *BlockStmt
}

func (s *ForInStmt) node()     {}
func (s *ForInStmt) stmtNode() {}
func (s *ForInStmt) String() string {
	if s.ValueVar != "" {
		return fmt.Sprintf("for %s, %s in %s %s", s.KeyVar, s.ValueVar, s.Iterable, s.Body)
	}
	return fmt.Sprintf("for %s in %s %s", s.KeyVar, s.Iterable, s.Body)
}

// WhileStmt represents a while loop.
type WhileStmt struct {
	BaseNode
	Condition Expression
	Body      *BlockStmt
}

func (s *WhileStmt) node()     {}
func (s *WhileStmt) stmtNode() {}
func (s *WhileStmt) String() string {
	return fmt.Sprintf("while %s %s", s.Condition, s.Body)
}

// ReturnStmt represents a return statement.
type ReturnStmt struct {
	BaseNode
	Value Expression
}

func (s *ReturnStmt) node()     {}
func (s *ReturnStmt) stmtNode() {}
func (s *ReturnStmt) String() string {
	if s.Value != nil {
		return fmt.Sprintf("return %s", s.Value)
	}
	return "return"
}

// BreakStmt represents a break statement.
type BreakStmt struct {
	BaseNode
}

func (s *BreakStmt) node()          {}
func (s *BreakStmt) stmtNode()      {}
func (s *BreakStmt) String() string { return "break" }

// ContinueStmt represents a continue statement.
type ContinueStmt struct {
	BaseNode
}

func (s *ContinueStmt) node()          {}
func (s *ContinueStmt) stmtNode()      {}
func (s *ContinueStmt) String() string { return "continue" }

// TryStmt represents a try-catch statement.
type TryStmt struct {
	BaseNode
	TryBlock   *BlockStmt
	CatchVar   string // variable name for caught error
	CatchBlock *BlockStmt
}

func (s *TryStmt) node()     {}
func (s *TryStmt) stmtNode() {}
func (s *TryStmt) String() string {
	return fmt.Sprintf("try %s catch (%s) %s", s.TryBlock, s.CatchVar, s.CatchBlock)
}

// ThrowStmt represents a throw statement.
type ThrowStmt struct {
	BaseNode
	Error Expression
}

func (s *ThrowStmt) node()     {}
func (s *ThrowStmt) stmtNode() {}
func (s *ThrowStmt) String() string {
	return fmt.Sprintf("throw %s", s.Error)
}

// SwitchStmt represents a switch statement.
type SwitchStmt struct {
	BaseNode
	Value Expression
	Cases []*SwitchCase
}

// SwitchCase represents a case in a switch statement.
type SwitchCase struct {
	Values []Expression // nil for default case
	Body   *BlockStmt
}

func (s *SwitchStmt) node()     {}
func (s *SwitchStmt) stmtNode() {}
func (s *SwitchStmt) String() string {
	return fmt.Sprintf("switch %s { ... }", s.Value)
}

func (s *SwitchCase) String() string {
	if s.Values == nil {
		return "default"
	}
	return fmt.Sprintf("case %v", s.Values)
}

// FuncStmt represents a function declaration.
// Param represents a function parameter with optional default value.
type Param struct {
	Name         string
	DefaultValue Expression // nil if no default
	IsRest       bool       // true if this is a rest parameter (...name)
}

type FuncStmt struct {
	BaseNode
	Name   string
	Params []Param // Changed from []string to support default values
	Body   *BlockStmt
}

func (s *FuncStmt) node()     {}
func (s *FuncStmt) stmtNode() {}
func (s *FuncStmt) String() string {
	paramStrs := make([]string, len(s.Params))
	for i, p := range s.Params {
		if p.DefaultValue != nil {
			paramStrs[i] = fmt.Sprintf("%s=%s", p.Name, p.DefaultValue)
		} else {
			paramStrs[i] = p.Name
		}
	}
	return fmt.Sprintf("func %s(%v) %s", s.Name, paramStrs, s.Body)
}

// Program represents a complete script.
type Program struct {
	BaseNode
	Statements []Statement
}

func (p *Program) node()          {}
func (p *Program) stmtNode()      {}
func (p *Program) String() string { return fmt.Sprintf("%v", p.Statements) }

// ============================================================================
// Parser
// ============================================================================

// Parser parses XxScript source code.
type Parser struct {
	tokens []Token
	pos    int
	errors []string
}

// NewParser creates a new parser.
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
	}
}

// Parse parses the tokens and returns an AST.
func Parse(source string) (*Program, error) {
	tokens := Tokenize(source)
	p := NewParser(tokens)
	prog := p.parseProgram()
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", p.errors)
	}
	return prog, nil
}

func (p *Parser) parseProgram() *Program {
	prog := &Program{}

	for !p.isAtEnd() {
		stmt := p.parseStatement()
		if stmt != nil {
			prog.Statements = append(prog.Statements, stmt)
		}
		// Consume optional semicolon between statements
		if p.current().Type == TokSemicolon {
			p.advance()
		}
	}

	return prog
}

// line returns the current token's line number.
func (p *Parser) line() int {
	return p.current().Line
}

func (p *Parser) parseStatement() Statement {
	switch p.current().Type {
	case TokVar:
		return p.parseVarStmt()
	case TokIf:
		return p.parseIfStmt()
	case TokFor:
		return p.parseForOrForInStmt()
	case TokWhile:
		return p.parseWhileStmt()
	case TokFunc:
		return p.parseFuncStmt()
	case TokReturn:
		return p.parseReturnStmt()
	case TokBreak:
		line := p.line()
		p.advance()
		stmt := &BreakStmt{}
		stmt.SetLine(line)
		return stmt
	case TokContinue:
		line := p.line()
		p.advance()
		stmt := &ContinueStmt{}
		stmt.SetLine(line)
		return stmt
	case TokTry:
		return p.parseTryStmt()
	case TokThrow:
		return p.parseThrowStmt()
	case TokSwitch:
		return p.parseSwitchStmt()
	case TokLBrace:
		// Peek ahead to determine if this is a block or map literal
		if p.isMapLiteral() {
			return p.parseExprStmt()
		}
		return p.parseBlockStmt()
	default:
		return p.parseExprStmt()
	}
}

func (p *Parser) parseVarStmt() *VarStmt {
	line := p.line()
	p.advance() // consume 'var'

	if p.current().Type != TokIdent {
		p.error("expected identifier after 'var'")
		return nil
	}

	// Collect variable names (support multiple for destructuring)
	var names []string
	name := p.current().Value
	names = append(names, name)
	p.advance()

	// Check for comma-separated variable names: var a, b, c = ...
	for p.current().Type == TokComma {
		p.advance() // consume comma
		if p.current().Type != TokIdent {
			p.error("expected identifier in variable list")
			return nil
		}
		names = append(names, p.current().Value)
		p.advance()
	}

	var value Expression
	if p.current().Type == TokAssign {
		p.advance()
		value = p.parseExpression()
	}

	stmt := &VarStmt{Name: names[0], Names: names, Value: value}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseIfStmt() *IfStmt {
	line := p.line()
	p.advance() // consume 'if'

	condition := p.parseExpression()
	thenBlock := p.parseBlockStmt()

	var elseStmt Statement
	if p.current().Type == TokElse {
		p.advance()
		if p.current().Type == TokIf {
			elseStmt = p.parseIfStmt()
		} else {
			elseStmt = p.parseBlockStmt()
		}
	}

	stmt := &IfStmt{
		Condition: condition,
		Then:      thenBlock,
		Else:      elseStmt,
	}
	stmt.SetLine(line)
	return stmt
}

// parseForOrForInStmt parses both regular for loops and for-in loops.
// Regular for: for (init; cond; update) { body }
// For-in: for key, value in expr { body } or for value in expr { body }
func (p *Parser) parseForOrForInStmt() Statement {
	line := p.line()
	p.advance() // consume 'for'

	// Check if this is a regular for loop (starts with '(')
	if p.current().Type == TokLParen {
		return p.parseForStmtFromToken(line)
	}

	// Parse for-in loop: for [key,] value in iterable { body }
	// First variable (key or value)
	if p.current().Type != TokIdent {
		p.error("expected identifier after 'for'")
		return nil
	}

	firstVar := p.current().Value
	p.advance()

	var keyVar, valueVar string

	// Check if we have a comma (meaning firstVar is key, next is value)
	if p.current().Type == TokComma {
		p.advance() // consume ','
		if p.current().Type != TokIdent {
			p.error("expected identifier after ',' in for-in")
			return nil
		}
		keyVar = firstVar
		valueVar = p.current().Value
		p.advance()
	} else {
		// Only one variable - it's the value, key is implicit
		keyVar = "_"
		valueVar = firstVar
	}

	// Expect 'in' keyword
	if p.current().Type != TokIn {
		p.error("expected 'in' in for-in loop")
		return nil
	}
	p.advance() // consume 'in'

	// Parse iterable expression
	iterable := p.parseExpression()

	// Parse body
	body := p.parseBlockStmt()

	stmt := &ForInStmt{
		KeyVar:   keyVar,
		ValueVar: valueVar,
		Iterable: iterable,
		Body:     body,
	}
	stmt.SetLine(line)
	return stmt
}

// parseForStmtFromToken parses a regular for loop after 'for' has been consumed.
func (p *Parser) parseForStmtFromToken(line int) *ForStmt {
	p.expect(TokLParen)

	// Init
	var init Statement
	if p.current().Type != TokSemicolon {
		init = p.parseVarStmt()
	}
	p.expect(TokSemicolon)

	// Condition
	var condition Expression
	if p.current().Type != TokSemicolon {
		condition = p.parseExpression()
	}
	p.expect(TokSemicolon)

	// Update
	var update Statement
	if p.current().Type != TokRParen {
		update = p.parseExprStmt()
	}
	p.expect(TokRParen)

	body := p.parseBlockStmt()

	stmt := &ForStmt{
		Init:      init,
		Condition: condition,
		Update:    update,
		Body:      body,
	}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseWhileStmt() *WhileStmt {
	line := p.line()
	p.advance() // consume 'while'

	condition := p.parseExpression()
	body := p.parseBlockStmt()

	stmt := &WhileStmt{
		Condition: condition,
		Body:      body,
	}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseFuncStmt() *FuncStmt {
	line := p.line()
	p.advance() // consume 'func'

	name := p.current().Value
	p.expect(TokIdent)
	p.expect(TokLParen)

	var params []Param
	for p.current().Type != TokRParen {
		var isRest bool
		// Check for rest parameter: ...name
		if p.current().Type == TokSpread {
			p.advance() // consume '...'
			isRest = true
		}

		paramName := p.current().Value
		p.expect(TokIdent)

		var defaultValue Expression
		// Check for default value: param = value (not allowed for rest params)
		if !isRest && p.current().Type == TokAssign {
			p.advance() // consume '='
			defaultValue = p.parseExpression()
		}

		params = append(params, Param{Name: paramName, DefaultValue: defaultValue, IsRest: isRest})

		if p.current().Type == TokComma {
			p.advance()
		}
	}
	p.expect(TokRParen)

	body := p.parseBlockStmt()

	stmt := &FuncStmt{
		Name:   name,
		Params: params,
		Body:   body,
	}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseReturnStmt() *ReturnStmt {
	line := p.line()
	p.advance() // consume 'return'

	var value Expression
	if p.current().Type != TokSemicolon && p.current().Type != TokRBrace && p.current().Type != TokEOF {
		value = p.parseExpression()
	}

	stmt := &ReturnStmt{Value: value}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseTryStmt() *TryStmt {
	line := p.line()
	p.advance() // consume 'try'

	tryBlock := p.parseBlockStmt()

	var catchVar string
	var catchBlock *BlockStmt

	if p.current().Type == TokCatch {
		p.advance() // consume 'catch'

		// Optional catch variable: catch (e) { ... }
		if p.current().Type == TokLParen {
			p.advance()
			if p.current().Type == TokIdent {
				catchVar = p.current().Value
				p.advance()
			}
			p.expect(TokRParen)
		}

		catchBlock = p.parseBlockStmt()
	}

	stmt := &TryStmt{
		TryBlock:   tryBlock,
		CatchVar:   catchVar,
		CatchBlock: catchBlock,
	}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseThrowStmt() *ThrowStmt {
	line := p.line()
	p.advance() // consume 'throw'

	var errExpr Expression
	if p.current().Type != TokSemicolon && p.current().Type != TokRBrace && p.current().Type != TokEOF {
		errExpr = p.parseExpression()
	}

	stmt := &ThrowStmt{Error: errExpr}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseSwitchStmt() *SwitchStmt {
	line := p.line()
	p.advance() // consume 'switch'

	// Parse the value expression
	value := p.parseExpression()

	p.expect(TokLBrace)

	stmt := &SwitchStmt{
		Value: value,
		Cases: make([]*SwitchCase, 0),
	}
	stmt.SetLine(line)

	for p.current().Type != TokRBrace && !p.isAtEnd() {
		switch p.current().Type {
		case TokCase:
			p.advance() // consume 'case'

			// Parse case values (comma-separated)
			values := make([]Expression, 0)
			for {
				values = append(values, p.parseExpression())
				if p.current().Type != TokComma {
					break
				}
				p.advance() // consume ','
			}

			p.expect(TokColon)
			body := p.parseBlockStmt()

			stmt.Cases = append(stmt.Cases, &SwitchCase{
				Values: values,
				Body:   body,
			})

		case TokDefault:
			p.advance() // consume 'default'
			p.expect(TokColon)
			body := p.parseBlockStmt()

			stmt.Cases = append(stmt.Cases, &SwitchCase{
				Values: nil, // nil indicates default case
				Body:   body,
			})

		default:
			p.error("unexpected token in switch: %v", p.current().Type)
			p.advance()
		}
	}

	p.expect(TokRBrace)
	return stmt
}

func (p *Parser) parseBlockStmt() *BlockStmt {
	line := p.line()
	p.expect(TokLBrace)

	block := &BlockStmt{}
	for p.current().Type != TokRBrace && !p.isAtEnd() {
		stmt := p.parseStatement()
		if stmt != nil {
			block.Statements = append(block.Statements, stmt)
		}
		// Consume optional semicolon between statements
		if p.current().Type == TokSemicolon {
			p.advance()
		}
	}

	p.expect(TokRBrace)
	block.SetLine(line)
	return block
}

func (p *Parser) parseExprStmt() *ExprStmt {
	line := p.line()
	expr := p.parseExpression()
	stmt := &ExprStmt{Expr: expr}
	stmt.SetLine(line)
	return stmt
}

func (p *Parser) parseExpression() Expression {
	return p.parseTernary()
}

func (p *Parser) parseTernary() Expression {
	expr := p.parseAssignment()

	// Handle ternary operator: condition ? true_expr : false_expr
	if p.current().Type == TokQuestion {
		line := p.line()
		p.advance() // consume '?'
		trueExpr := p.parseTernary()

		if p.current().Type != TokColon {
			p.error("expected ':' in ternary expression")
			return expr
		}
		p.advance() // consume ':'

		falseExpr := p.parseTernary()
		ternary := &TernaryExpr{Condition: expr, TrueExpr: trueExpr, FalseExpr: falseExpr}
		ternary.SetLine(line)
		return ternary
	}

	return expr
}

func (p *Parser) parseAssignment() Expression {
	expr := p.parseOr()

	// Check for multi-target assignment: a, b = value
	// Only process if we see a comma AND it's followed by an identifier AND then an assignment
	// This avoids interfering with function calls: func(a, b)
	if _, ok := expr.(*IdentExpr); ok && p.current().Type == TokComma {
		// Peek ahead to see if this is a multi-target assignment
		// Save position
		savePos := p.pos
		lefts := []Expression{expr}

		// Try to collect targets
		for p.current().Type == TokComma {
			p.advance() // consume comma
			if p.current().Type != TokIdent {
				// Not a multi-target assignment, restore position
				p.pos = savePos
				break
			}
			next := &IdentExpr{Name: p.current().Value}
			lefts = append(lefts, next)
			p.advance()
		}

		// Check for assignment
		if p.current().Type == TokAssign && len(lefts) > 1 {
			line := p.line()
			p.advance()
			value := p.parseAssignment()
			assign := &AssignExpr{Lefts: lefts, Value: value}
			assign.SetLine(line)
			return assign
		}

		// Not a multi-target assignment, restore position
		p.pos = savePos
	}

	// Handle regular assignment
	if p.current().Type == TokAssign {
		line := p.line()
		p.advance()
		value := p.parseAssignment()
		assign := &AssignExpr{Left: expr, Value: value}
		assign.SetLine(line)
		return assign
	}

	// Handle compound assignment (+=, -=, *=, /=, %=)
	if p.current().Type == TokPlusAssign || p.current().Type == TokMinusAssign ||
		p.current().Type == TokStarAssign || p.current().Type == TokSlashAssign ||
		p.current().Type == TokPercentAssign {
		line := p.line()
		op := p.current().Type
		p.advance()
		value := p.parseAssignment()
		compound := &CompoundAssignExpr{Left: expr, Op: op, Value: value}
		compound.SetLine(line)
		return compound
	}

	return expr
}

func (p *Parser) parseOr() Expression {
	left := p.parseAnd()

	for p.current().Type == TokOr {
		line := p.line()
		op := p.current().Type
		p.advance()
		right := p.parseAnd()
		bin := &BinaryExpr{Left: left, Op: op, Right: right}
		bin.SetLine(line)
		left = bin
	}

	return left
}

func (p *Parser) parseAnd() Expression {
	left := p.parseEquality()

	for p.current().Type == TokAnd {
		line := p.line()
		op := p.current().Type
		p.advance()
		right := p.parseEquality()
		bin := &BinaryExpr{Left: left, Op: op, Right: right}
		bin.SetLine(line)
		left = bin
	}

	return left
}

func (p *Parser) parseEquality() Expression {
	left := p.parseComparison()

	for p.current().Type == TokEq || p.current().Type == TokNe {
		line := p.line()
		op := p.current().Type
		p.advance()
		right := p.parseComparison()
		bin := &BinaryExpr{Left: left, Op: op, Right: right}
		bin.SetLine(line)
		left = bin
	}

	return left
}

func (p *Parser) parseComparison() Expression {
	left := p.parseAdditive()

	for p.current().Type == TokLt || p.current().Type == TokLe ||
		p.current().Type == TokGt || p.current().Type == TokGe {
		line := p.line()
		op := p.current().Type
		p.advance()
		right := p.parseAdditive()
		bin := &BinaryExpr{Left: left, Op: op, Right: right}
		bin.SetLine(line)
		left = bin
	}

	return left
}

func (p *Parser) parseAdditive() Expression {
	left := p.parseMultiplicative()

	for p.current().Type == TokPlus || p.current().Type == TokMinus {
		line := p.line()
		op := p.current().Type
		p.advance()
		right := p.parseMultiplicative()
		bin := &BinaryExpr{Left: left, Op: op, Right: right}
		bin.SetLine(line)
		left = bin
	}

	return left
}

func (p *Parser) parseMultiplicative() Expression {
	left := p.parseUnary()

	for p.current().Type == TokStar || p.current().Type == TokSlash || p.current().Type == TokPercent {
		line := p.line()
		op := p.current().Type
		p.advance()
		right := p.parseUnary()
		bin := &BinaryExpr{Left: left, Op: op, Right: right}
		bin.SetLine(line)
		left = bin
	}

	return left
}

func (p *Parser) parseUnary() Expression {
	// Handle prefix increment/decrement: ++x, --x
	if p.current().Type == TokInc || p.current().Type == TokDec {
		line := p.line()
		op := p.current().Type
		p.advance()
		expr := p.parseUnary()
		pre := &PreIncDecExpr{Op: op, Expr: expr}
		pre.SetLine(line)
		return pre
	}

	// Handle logical NOT and unary minus
	if p.current().Type == TokNot || p.current().Type == TokMinus {
		line := p.line()
		op := p.current().Type
		p.advance()
		expr := p.parseUnary()
		unary := &UnaryExpr{Op: op, Expr: expr}
		unary.SetLine(line)
		return unary
	}

	return p.parsePostfix()
}

func (p *Parser) parsePostfix() Expression {
	expr := p.parsePrimary()

	for {
		switch p.current().Type {
		case TokLParen:
			// Function call
			line := p.line()
			p.advance()
			var args []Expression
			for p.current().Type != TokRParen {
				// Check for spread operator
				if p.current().Type == TokSpread {
					p.advance() // consume '...'
					arg := p.parseExpression()
					spread := &SpreadExpr{Expr: arg}
					spread.SetLine(line)
					args = append(args, spread)
				} else {
					args = append(args, p.parseExpression())
				}
				if p.current().Type == TokComma {
					p.advance()
				}
			}
			p.expect(TokRParen)
			call := &CallExpr{Func: expr, Args: args}
			call.SetLine(line)
			expr = call
		case TokDot:
			// Member access
			line := p.line()
			p.advance()
			member := &StringExpr{Value: p.current().Value}
			p.expect(TokIdent)
			mem := &MemberExpr{Object: expr, Member: member}
			mem.SetLine(line)
			expr = mem
		case TokLBracket:
			// Index access
			line := p.line()
			p.advance()
			index := p.parseExpression()
			p.expect(TokRBracket)
			idx := &IndexExpr{Object: expr, Index: index}
			idx.SetLine(line)
			expr = idx
		case TokInc, TokDec:
			// Postfix increment/decrement: x++, x--
			line := p.line()
			op := p.current().Type
			p.advance()
			post := &PostIncDecExpr{Expr: expr, Op: op}
			post.SetLine(line)
			expr = post
		default:
			return expr
		}
	}
}

func (p *Parser) parsePrimary() Expression {
	tok := p.current()
	line := tok.Line

	switch tok.Type {
	case TokIdent:
		p.advance()
		expr := &IdentExpr{Name: tok.Value}
		expr.SetLine(line)
		return expr

	case TokNumber:
		p.advance()
		val, _ := strconv.ParseFloat(tok.Value, 64)
		expr := &NumberExpr{Value: val}
		expr.SetLine(line)
		return expr

	case TokString:
		p.advance()
		expr := &StringExpr{Value: tok.Value}
		expr.SetLine(line)
		return expr

	case TokBool:
		p.advance()
		expr := &BoolExpr{Value: tok.Value == "true"}
		expr.SetLine(line)
		return expr

	case TokNull:
		p.advance()
		expr := &NullExpr{}
		expr.SetLine(line)
		return expr

	case TokLParen:
		p.advance()
		expr := p.parseExpression()
		p.expect(TokRParen)
		return expr

	case TokLBracket:
		return p.parseArrayLiteral()

	case TokLBrace:
		return p.parseMapLiteral()

	default:
		p.error("unexpected token: %s", tok.Type)
		p.advance()
		expr := &NullExpr{}
		expr.SetLine(line)
		return expr
	}
}

func (p *Parser) parseArrayLiteral() *ArrayExpr {
	p.expect(TokLBracket)
	line := p.line()

	var elements []Expression
	for p.current().Type != TokRBracket {
		// Check for spread operator
		if p.current().Type == TokSpread {
			p.advance() // consume '...'
			expr := p.parseExpression()
			spread := &SpreadExpr{Expr: expr}
			spread.SetLine(line)
			elements = append(elements, spread)
		} else {
			elements = append(elements, p.parseExpression())
		}
		if p.current().Type == TokComma {
			p.advance()
		}
	}

	p.expect(TokRBracket)
	expr := &ArrayExpr{Elements: elements}
	expr.SetLine(line)
	return expr
}

func (p *Parser) parseMapLiteral() *MapExpr {
	line := p.line()
	p.expect(TokLBrace)

	pairs := make(map[string]Expression)
	for p.current().Type != TokRBrace {
		// Key can be string or identifier
		var key string
		if p.current().Type == TokString {
			key = p.current().Value
			p.advance()
		} else if p.current().Type == TokIdent {
			key = p.current().Value
			p.advance()
		} else {
			p.error("expected string or identifier as map key")
			expr := &MapExpr{}
			expr.SetLine(line)
			return expr
		}

		p.expect(TokColon)
		value := p.parseExpression()
		pairs[key] = value

		if p.current().Type == TokComma {
			p.advance()
		}
	}

	p.expect(TokRBrace)
	expr := &MapExpr{Pairs: pairs}
	expr.SetLine(line)
	return expr
}

// Helper methods

func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokEOF}
	}
	return p.tokens[p.pos]
}

// isMapLiteral checks if the current { starts a map literal (not a block).
// A map literal has the form { "key": value } or { key: value }
func (p *Parser) isMapLiteral() bool {
	// We're at TokLBrace, check next tokens
	if p.pos+1 >= len(p.tokens) {
		return false
	}
	next := p.tokens[p.pos+1]
	// Map literal keys can be string or identifier
	if next.Type != TokString && next.Type != TokIdent {
		return false
	}
	// Check if followed by colon
	if p.pos+2 >= len(p.tokens) {
		return false
	}
	return p.tokens[p.pos+2].Type == TokColon
}

func (p *Parser) advance() Token {
	tok := p.current()
	p.pos++
	return tok
}

func (p *Parser) expect(typ TokenType) bool {
	if p.current().Type != typ {
		p.error("expected %s, got %s", typ, p.current().Type)
		return false
	}
	p.advance()
	return true
}

func (p *Parser) isAtEnd() bool {
	return p.current().Type == TokEOF
}

func (p *Parser) error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.errors = append(p.errors, msg)
}
