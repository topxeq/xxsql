// Package xxscript provides a simple scripting language for XxSql.
package xxscript

import (
	"fmt"
	"strconv"
)

// Node represents an AST node.
type Node interface {
	node()
	String() string
}

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
	Name string
}

func (e *IdentExpr) node()      {}
func (e *IdentExpr) exprNode()  {}
func (e *IdentExpr) String() string { return e.Name }

// NumberExpr represents a number literal.
type NumberExpr struct {
	Value float64
}

func (e *NumberExpr) node()      {}
func (e *NumberExpr) exprNode()  {}
func (e *NumberExpr) String() string { return fmt.Sprintf("%v", e.Value) }

// StringExpr represents a string literal.
type StringExpr struct {
	Value string
}

func (e *StringExpr) node()      {}
func (e *StringExpr) exprNode()  {}
func (e *StringExpr) String() string { return fmt.Sprintf("%q", e.Value) }

// BoolExpr represents a boolean literal.
type BoolExpr struct {
	Value bool
}

func (e *BoolExpr) node()      {}
func (e *BoolExpr) exprNode()  {}
func (e *BoolExpr) String() string { return fmt.Sprintf("%v", e.Value) }

// NullExpr represents null.
type NullExpr struct{}

func (e *NullExpr) node()      {}
func (e *NullExpr) exprNode()  {}
func (e *NullExpr) String() string { return "null" }

// ArrayExpr represents an array literal.
type ArrayExpr struct {
	Elements []Expression
}

func (e *ArrayExpr) node()      {}
func (e *ArrayExpr) exprNode()  {}
func (e *ArrayExpr) String() string {
	return fmt.Sprintf("%v", e.Elements)
}

// MapExpr represents a map/object literal.
type MapExpr struct {
	Pairs map[string]Expression
}

func (e *MapExpr) node()      {}
func (e *MapExpr) exprNode()  {}
func (e *MapExpr) String() string { return fmt.Sprintf("%v", e.Pairs) }

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Left  Expression
	Op    TokenType
	Right Expression
}

func (e *BinaryExpr) node()      {}
func (e *BinaryExpr) exprNode()  {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Op   TokenType
	Expr Expression
}

func (e *UnaryExpr) node()      {}
func (e *UnaryExpr) exprNode()  {}
func (e *UnaryExpr) String() string {
	return fmt.Sprintf("(%s %s)", e.Op, e.Expr)
}

// CallExpr represents a function call.
type CallExpr struct {
	Func Expression
	Args []Expression
}

func (e *CallExpr) node()      {}
func (e *CallExpr) exprNode()  {}
func (e *CallExpr) String() string {
	return fmt.Sprintf("%s(%v)", e.Func, e.Args)
}

// MemberExpr represents a member access (obj.field or obj["field"]).
type MemberExpr struct {
	Object Expression
	Member Expression
}

func (e *MemberExpr) node()      {}
func (e *MemberExpr) exprNode()  {}
func (e *MemberExpr) String() string {
	return fmt.Sprintf("%s.%s", e.Object, e.Member)
}

// IndexExpr represents an index access (arr[index]).
type IndexExpr struct {
	Object Expression
	Index  Expression
}

func (e *IndexExpr) node()      {}
func (e *IndexExpr) exprNode()  {}
func (e *IndexExpr) String() string {
	return fmt.Sprintf("%s[%s]", e.Object, e.Index)
}

// AssignExpr represents an assignment expression.
type AssignExpr struct {
	Left  Expression
	Value Expression
}

func (e *AssignExpr) node()      {}
func (e *AssignExpr) exprNode()  {}
func (e *AssignExpr) String() string {
	return fmt.Sprintf("%s = %s", e.Left, e.Value)
}

// ============================================================================
// Statements
// ============================================================================

// VarStmt represents a variable declaration.
type VarStmt struct {
	Name  string
	Value Expression
}

func (s *VarStmt) node()       {}
func (s *VarStmt) stmtNode()   {}
func (s *VarStmt) String() string {
	if s.Value != nil {
		return fmt.Sprintf("var %s = %s", s.Name, s.Value)
	}
	return fmt.Sprintf("var %s", s.Name)
}

// ExprStmt represents an expression statement.
type ExprStmt struct {
	Expr Expression
}

func (s *ExprStmt) node()       {}
func (s *ExprStmt) stmtNode()   {}
func (s *ExprStmt) String() string { return s.Expr.String() }

// BlockStmt represents a block of statements.
type BlockStmt struct {
	Statements []Statement
}

func (s *BlockStmt) node()       {}
func (s *BlockStmt) stmtNode()   {}
func (s *BlockStmt) String() string { return fmt.Sprintf("{ %v }", s.Statements) }

// IfStmt represents an if statement.
type IfStmt struct {
	Condition Expression
	Then      *BlockStmt
	Else      Statement // can be *BlockStmt or *IfStmt
}

func (s *IfStmt) node()       {}
func (s *IfStmt) stmtNode()   {}
func (s *IfStmt) String() string {
	if s.Else != nil {
		return fmt.Sprintf("if %s %s else %s", s.Condition, s.Then, s.Else)
	}
	return fmt.Sprintf("if %s %s", s.Condition, s.Then)
}

// ForStmt represents a for loop.
type ForStmt struct {
	Init   Statement
	Condition Expression
	Update Statement
	Body   *BlockStmt
}

func (s *ForStmt) node()       {}
func (s *ForStmt) stmtNode()   {}
func (s *ForStmt) String() string {
	return fmt.Sprintf("for (%s; %s; %s) %s", s.Init, s.Condition, s.Update, s.Body)
}

// WhileStmt represents a while loop.
type WhileStmt struct {
	Condition Expression
	Body      *BlockStmt
}

func (s *WhileStmt) node()       {}
func (s *WhileStmt) stmtNode()   {}
func (s *WhileStmt) String() string {
	return fmt.Sprintf("while %s %s", s.Condition, s.Body)
}

// ReturnStmt represents a return statement.
type ReturnStmt struct {
	Value Expression
}

func (s *ReturnStmt) node()       {}
func (s *ReturnStmt) stmtNode()   {}
func (s *ReturnStmt) String() string {
	if s.Value != nil {
		return fmt.Sprintf("return %s", s.Value)
	}
	return "return"
}

// BreakStmt represents a break statement.
type BreakStmt struct{}

func (s *BreakStmt) node()       {}
func (s *BreakStmt) stmtNode()   {}
func (s *BreakStmt) String() string { return "break" }

// ContinueStmt represents a continue statement.
type ContinueStmt struct{}

func (s *ContinueStmt) node()       {}
func (s *ContinueStmt) stmtNode()   {}
func (s *ContinueStmt) String() string { return "continue" }

// TryStmt represents a try-catch statement.
type TryStmt struct {
	TryBlock  *BlockStmt
	CatchVar  string // variable name for caught error
	CatchBlock *BlockStmt
}

func (s *TryStmt) node()       {}
func (s *TryStmt) stmtNode()   {}
func (s *TryStmt) String() string {
	return fmt.Sprintf("try %s catch (%s) %s", s.TryBlock, s.CatchVar, s.CatchBlock)
}

// ThrowStmt represents a throw statement.
type ThrowStmt struct {
	Error Expression
}

func (s *ThrowStmt) node()       {}
func (s *ThrowStmt) stmtNode()   {}
func (s *ThrowStmt) String() string {
	return fmt.Sprintf("throw %s", s.Error)
}

// FuncStmt represents a function declaration.
type FuncStmt struct {
	Name   string
	Params []string
	Body   *BlockStmt
}

func (s *FuncStmt) node()       {}
func (s *FuncStmt) stmtNode()   {}
func (s *FuncStmt) String() string {
	return fmt.Sprintf("func %s(%v) %s", s.Name, s.Params, s.Body)
}

// Program represents a complete script.
type Program struct {
	Statements []Statement
}

func (p *Program) node()       {}
func (p *Program) stmtNode()   {}
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
	}

	return prog
}

func (p *Parser) parseStatement() Statement {
	switch p.current().Type {
	case TokVar:
		return p.parseVarStmt()
	case TokIf:
		return p.parseIfStmt()
	case TokFor:
		return p.parseForStmt()
	case TokWhile:
		return p.parseWhileStmt()
	case TokFunc:
		return p.parseFuncStmt()
	case TokReturn:
		return p.parseReturnStmt()
	case TokBreak:
		p.advance()
		return &BreakStmt{}
	case TokContinue:
		p.advance()
		return &ContinueStmt{}
	case TokTry:
		return p.parseTryStmt()
	case TokThrow:
		return p.parseThrowStmt()
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
	p.advance() // consume 'var'

	if p.current().Type != TokIdent {
		p.error("expected identifier after 'var'")
		return nil
	}

	name := p.current().Value
	p.advance()

	var value Expression
	if p.current().Type == TokAssign {
		p.advance()
		value = p.parseExpression()
	}

	return &VarStmt{Name: name, Value: value}
}

func (p *Parser) parseIfStmt() *IfStmt {
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

	return &IfStmt{
		Condition: condition,
		Then:      thenBlock,
		Else:      elseStmt,
	}
}

func (p *Parser) parseForStmt() *ForStmt {
	p.advance() // consume 'for'

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

	return &ForStmt{
		Init:      init,
		Condition: condition,
		Update:    update,
		Body:      body,
	}
}

func (p *Parser) parseWhileStmt() *WhileStmt {
	p.advance() // consume 'while'

	condition := p.parseExpression()
	body := p.parseBlockStmt()

	return &WhileStmt{
		Condition: condition,
		Body:      body,
	}
}

func (p *Parser) parseFuncStmt() *FuncStmt {
	p.advance() // consume 'func'

	name := p.current().Value
	p.expect(TokIdent)
	p.expect(TokLParen)

	var params []string
	for p.current().Type != TokRParen {
		params = append(params, p.current().Value)
		p.expect(TokIdent)
		if p.current().Type == TokComma {
			p.advance()
		}
	}
	p.expect(TokRParen)

	body := p.parseBlockStmt()

	return &FuncStmt{
		Name:   name,
		Params: params,
		Body:   body,
	}
}

func (p *Parser) parseReturnStmt() *ReturnStmt {
	p.advance() // consume 'return'

	var value Expression
	if p.current().Type != TokSemicolon && p.current().Type != TokRBrace && p.current().Type != TokEOF {
		value = p.parseExpression()
	}

	return &ReturnStmt{Value: value}
}

func (p *Parser) parseTryStmt() *TryStmt {
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

	return &TryStmt{
		TryBlock:   tryBlock,
		CatchVar:   catchVar,
		CatchBlock: catchBlock,
	}
}

func (p *Parser) parseThrowStmt() *ThrowStmt {
	p.advance() // consume 'throw'

	var errExpr Expression
	if p.current().Type != TokSemicolon && p.current().Type != TokRBrace && p.current().Type != TokEOF {
		errExpr = p.parseExpression()
	}

	return &ThrowStmt{Error: errExpr}
}

func (p *Parser) parseBlockStmt() *BlockStmt {
	p.expect(TokLBrace)

	block := &BlockStmt{}
	for p.current().Type != TokRBrace && !p.isAtEnd() {
		stmt := p.parseStatement()
		if stmt != nil {
			block.Statements = append(block.Statements, stmt)
		}
	}

	p.expect(TokRBrace)
	return block
}

func (p *Parser) parseExprStmt() *ExprStmt {
	expr := p.parseExpression()
	return &ExprStmt{Expr: expr}
}

func (p *Parser) parseExpression() Expression {
	return p.parseAssignment()
}

func (p *Parser) parseAssignment() Expression {
	expr := p.parseOr()

	if p.current().Type == TokAssign {
		p.advance()
		value := p.parseAssignment()
		return &AssignExpr{Left: expr, Value: value}
	}

	return expr
}

func (p *Parser) parseOr() Expression {
	left := p.parseAnd()

	for p.current().Type == TokOr {
		op := p.current().Type
		p.advance()
		right := p.parseAnd()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseAnd() Expression {
	left := p.parseEquality()

	for p.current().Type == TokAnd {
		op := p.current().Type
		p.advance()
		right := p.parseEquality()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseEquality() Expression {
	left := p.parseComparison()

	for p.current().Type == TokEq || p.current().Type == TokNe {
		op := p.current().Type
		p.advance()
		right := p.parseComparison()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseComparison() Expression {
	left := p.parseAdditive()

	for p.current().Type == TokLt || p.current().Type == TokLe ||
		p.current().Type == TokGt || p.current().Type == TokGe {
		op := p.current().Type
		p.advance()
		right := p.parseAdditive()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseAdditive() Expression {
	left := p.parseMultiplicative()

	for p.current().Type == TokPlus || p.current().Type == TokMinus {
		op := p.current().Type
		p.advance()
		right := p.parseMultiplicative()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseMultiplicative() Expression {
	left := p.parseUnary()

	for p.current().Type == TokStar || p.current().Type == TokSlash || p.current().Type == TokPercent {
		op := p.current().Type
		p.advance()
		right := p.parseUnary()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseUnary() Expression {
	if p.current().Type == TokNot || p.current().Type == TokMinus {
		op := p.current().Type
		p.advance()
		expr := p.parseUnary()
		return &UnaryExpr{Op: op, Expr: expr}
	}

	return p.parsePostfix()
}

func (p *Parser) parsePostfix() Expression {
	expr := p.parsePrimary()

	for {
		switch p.current().Type {
		case TokLParen:
			// Function call
			p.advance()
			var args []Expression
			for p.current().Type != TokRParen {
				args = append(args, p.parseExpression())
				if p.current().Type == TokComma {
					p.advance()
				}
			}
			p.expect(TokRParen)
			expr = &CallExpr{Func: expr, Args: args}
		case TokDot:
			// Member access
			p.advance()
			member := &StringExpr{Value: p.current().Value}
			p.expect(TokIdent)
			expr = &MemberExpr{Object: expr, Member: member}
		case TokLBracket:
			// Index access
			p.advance()
			index := p.parseExpression()
			p.expect(TokRBracket)
			expr = &IndexExpr{Object: expr, Index: index}
		default:
			return expr
		}
	}
}

func (p *Parser) parsePrimary() Expression {
	tok := p.current()

	switch tok.Type {
	case TokIdent:
		p.advance()
		return &IdentExpr{Name: tok.Value}

	case TokNumber:
		p.advance()
		val, _ := strconv.ParseFloat(tok.Value, 64)
		return &NumberExpr{Value: val}

	case TokString:
		p.advance()
		return &StringExpr{Value: tok.Value}

	case TokBool:
		p.advance()
		return &BoolExpr{Value: tok.Value == "true"}

	case TokNull:
		p.advance()
		return &NullExpr{}

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
		return &NullExpr{}
	}
}

func (p *Parser) parseArrayLiteral() *ArrayExpr {
	p.expect(TokLBracket)

	var elements []Expression
	for p.current().Type != TokRBracket {
		elements = append(elements, p.parseExpression())
		if p.current().Type == TokComma {
			p.advance()
		}
	}

	p.expect(TokRBracket)
	return &ArrayExpr{Elements: elements}
}

func (p *Parser) parseMapLiteral() *MapExpr {
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
			return &MapExpr{}
		}

		p.expect(TokColon)
		value := p.parseExpression()
		pairs[key] = value

		if p.current().Type == TokComma {
			p.advance()
		}
	}

	p.expect(TokRBrace)
	return &MapExpr{Pairs: pairs}
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