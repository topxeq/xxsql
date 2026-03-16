package sql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Lexer tokenizes SQL input.
type Lexer struct {
	input  string    // input string
	pos    int       // current position in input
	line   int       // current line number (1-based)
	column int       // current column number (1-based)
	width  int       // width of last rune read
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		pos:    0,
		line:   1,
		column: 1,
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokEOF, Line: l.line, Column: l.column}
	}

	line, col := l.line, l.column
	r := l.peek()

	// Identifiers and keywords
	if isAlpha(r) || r == '_' || r == '`' {
		return l.scanIdentOrKeyword()
	}

	// Numbers
	if isDigit(r) || (r == '.' && l.isNextDigit()) {
		return l.scanNumber()
	}

	// Strings
	if r == '\'' || r == '"' {
		return l.scanString(r)
	}

	// Operators and punctuation
	switch r {
	case '(':
		l.next()
		return Token{Type: TokLParen, Value: "(", Line: line, Column: col}
	case ')':
		l.next()
		return Token{Type: TokRParen, Value: ")", Line: line, Column: col}
	case ',':
		l.next()
		return Token{Type: TokComma, Value: ",", Line: line, Column: col}
	case ';':
		l.next()
		return Token{Type: TokSemi, Value: ";", Line: line, Column: col}
	case '.':
		l.next()
		return Token{Type: TokDot, Value: ".", Line: line, Column: col}
	case '+':
		l.next()
		return Token{Type: TokPlus, Value: "+", Line: line, Column: col}
	case '-':
		// Check for comment
		if l.peekAt(1) == '-' {
			l.skipLineComment()
			return l.NextToken()
		}
		l.next()
		return Token{Type: TokMinus, Value: "-", Line: line, Column: col}
	case '*':
		l.next()
		return Token{Type: TokStar, Value: "*", Line: line, Column: col}
	case '/':
		// Check for comment
		if l.peekAt(1) == '*' {
			l.skipBlockComment()
			return l.NextToken()
		}
		l.next()
		return Token{Type: TokSlash, Value: "/", Line: line, Column: col}
	case '%':
		l.next()
		return Token{Type: TokPercent, Value: "%", Line: line, Column: col}
	case '^':
		l.next()
		return Token{Type: TokCaret, Value: "^", Line: line, Column: col}
	case '=':
		l.next()
		return Token{Type: TokEq, Value: "=", Line: line, Column: col}
	case '<':
		l.next()
		switch l.peek() {
		case '=':
			l.next()
			return Token{Type: TokLe, Value: "<=", Line: line, Column: col}
		case '>':
			l.next()
			return Token{Type: TokNe, Value: "<>", Line: line, Column: col}
		default:
			return Token{Type: TokLt, Value: "<", Line: line, Column: col}
		}
	case '>':
		l.next()
		if l.peek() == '=' {
			l.next()
			return Token{Type: TokGe, Value: ">=", Line: line, Column: col}
		}
		return Token{Type: TokGt, Value: ">", Line: line, Column: col}
	case '!':
		l.next()
		if l.peek() == '=' {
			l.next()
			return Token{Type: TokNe, Value: "!=", Line: line, Column: col}
		}
		return Token{Type: TokNot, Value: "!", Line: line, Column: col}
	case '@':
		l.next()
		return Token{Type: TokAt, Value: "@", Line: line, Column: col}
	case '|':
		l.next()
		if l.peek() == '|' {
			l.next()
			return Token{Type: TokConcat, Value: "||", Line: line, Column: col}
		}
		return Token{Type: TokError, Value: "|", Line: line, Column: col}
	case ':':
		l.next()
		if l.peek() == '=' {
			l.next()
			return Token{Type: TokAssign, Value: ":=", Line: line, Column: col}
		}
		if l.peek() == ':' {
			l.next()
			return Token{Type: TokDoubleCol, Value: "::", Line: line, Column: col}
		}
		return Token{Type: TokColon, Value: ":", Line: line, Column: col}
	case '?':
		l.next()
		return Token{Type: TokParameter, Value: "?", Line: line, Column: col}
	case '$':
		return l.scanParameter()
	}

	// Unknown character
	l.next()
	return Token{Type: TokError, Value: string(r), Line: line, Column: col}
}

// scanIdentOrKeyword scans an identifier or keyword.
func (l *Lexer) scanIdentOrKeyword() Token {
	line, col := l.line, l.column

	// Handle backtick quoted identifiers
	if l.peek() == '`' {
		l.next() // consume opening backtick
		var sb strings.Builder
		for {
			r := l.peek()
			if r == 0 {
				return Token{Type: TokError, Value: sb.String(), Line: line, Column: col}
			}
			if r == '`' {
				l.next()
				// Check for escaped backtick
				if l.peek() == '`' {
					sb.WriteRune('`')
					l.next()
					continue
				}
				break
			}
			sb.WriteRune(r)
			l.next()
		}
		return Token{Type: TokIdent, Value: sb.String(), Line: line, Column: col}
	}

	// Regular identifier or keyword
	var sb strings.Builder
	for {
		r := l.peek()
		if !isAlphaNum(r) && r != '_' {
			break
		}
		sb.WriteRune(r)
		l.next()
	}

	value := sb.String()
	upper := strings.ToUpper(value)
	tokenType := LookupKeyword(upper)

	// For boolean literals, keep the original case value
	if tokenType == TokBoolLit {
		return Token{Type: TokBoolLit, Value: upper, Line: line, Column: col}
	}

	return Token{Type: tokenType, Value: value, Line: line, Column: col}
}

// scanNumber scans a numeric literal.
func (l *Lexer) scanNumber() Token {
	line, col := l.line, l.column
	var sb strings.Builder

	// Integer part
	for isDigit(l.peek()) {
		sb.WriteRune(l.peek())
		l.next()
	}

	// Check for hexadecimal: 0x or 0X followed by hex digits
	if sb.String() == "0" && (l.peek() == 'x' || l.peek() == 'X') {
		sb.WriteRune(l.peek())
		l.next()
		for isHexDigit(l.peek()) {
			sb.WriteRune(l.peek())
			l.next()
		}
		return Token{Type: TokNumber, Value: sb.String(), Line: line, Column: col}
	}

	// Decimal part
	if l.peek() == '.' {
		sb.WriteRune('.')
		l.next()
		for isDigit(l.peek()) {
			sb.WriteRune(l.peek())
			l.next()
		}
	}

	// Exponent
	if r := l.peek(); r == 'e' || r == 'E' {
		sb.WriteRune(r)
		l.next()
		if r := l.peek(); r == '+' || r == '-' {
			sb.WriteRune(r)
			l.next()
		}
		for isDigit(l.peek()) {
			sb.WriteRune(l.peek())
			l.next()
		}
	}

	return Token{Type: TokNumber, Value: sb.String(), Line: line, Column: col}
}

// scanString scans a string literal.
func (l *Lexer) scanString(quote rune) Token {
	line, col := l.line, l.column
	l.next() // consume opening quote

	var sb strings.Builder
	for {
		r := l.peek()
		if r == 0 {
			return Token{Type: TokError, Value: "unterminated string", Line: line, Column: col}
		}
		if r == quote {
			l.next()
			// Check for escaped quote
			if l.peek() == quote {
				sb.WriteRune(quote)
				l.next()
				continue
			}
			break
		}
		if r == '\\' {
			l.next()
			esc := l.peek()
			switch esc {
			case 'n':
				sb.WriteRune('\n')
			case 't':
				sb.WriteRune('\t')
			case 'r':
				sb.WriteRune('\r')
			case '\\':
				sb.WriteRune('\\')
			case quote:
				sb.WriteRune(quote)
			case '0':
				sb.WriteRune(0)
			default:
				sb.WriteRune(esc)
			}
			l.next()
			continue
		}
		sb.WriteRune(r)
		l.next()
	}

	return Token{Type: TokString, Value: sb.String(), Line: line, Column: col}
}

// scanParameter scans a parameter placeholder ($1, $2, etc.)
func (l *Lexer) scanParameter() Token {
	line, col := l.line, l.column
	l.next() // consume $

	var sb strings.Builder
	sb.WriteRune('$')
	for isDigit(l.peek()) {
		sb.WriteRune(l.peek())
		l.next()
	}

	return Token{Type: TokParameter, Value: sb.String(), Line: line, Column: col}
}

// skipWhitespace skips whitespace characters.
func (l *Lexer) skipWhitespace() {
	for {
		r := l.peek()
		if !unicode.IsSpace(r) {
			break
		}
		l.next()
	}
}

// skipLineComment skips a line comment (-- ...).
func (l *Lexer) skipLineComment() {
	l.next() // consume first -
	l.next() // consume second -
	for {
		r := l.peek()
		if r == 0 || r == '\n' {
			break
		}
		l.next()
	}
}

// skipBlockComment skips a block comment (/* ... */).
func (l *Lexer) skipBlockComment() {
	l.next() // consume /
	l.next() // consume *
	for {
		r := l.peek()
		if r == 0 {
			return
		}
		if r == '*' && l.peekAt(1) == '/' {
			l.next() // consume *
			l.next() // consume /
			return
		}
		l.next()
	}
}

// next reads the next rune from input.
func (l *Lexer) next() rune {
	if l.pos >= len(l.input) {
		return 0
	}

	r, width := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = width
	l.pos += width

	// Update line/column
	if r == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}

	return r
}

// peek returns the next rune without consuming it.
func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.pos:])
	return r
}

// peekAt returns the rune at the given offset from current position.
func (l *Lexer) peekAt(offset int) rune {
	pos := l.pos
	for i := 0; i < offset && pos < len(l.input); i++ {
		_, width := utf8.DecodeRuneInString(l.input[pos:])
		pos += width
	}
	if pos >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[pos:])
	return r
}

// isNextDigit checks if the next character is a digit.
func (l *Lexer) isNextDigit() bool {
	return isDigit(l.peekAt(1))
}

// Pos returns the current position.
func (l *Lexer) Pos() int {
	return l.pos
}

// Line returns the current line number.
func (l *Lexer) Line() int {
	return l.line
}

// Column returns the current column number.
func (l *Lexer) Column() int {
	return l.column
}

// Helper functions

func isAlpha(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isAlphaNum(r rune) bool {
	return isAlpha(r) || isDigit(r)
}

func isHexDigit(r rune) bool {
	return isDigit(r) || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

// Tokenize returns all tokens from the input.
func Tokenize(input string) ([]Token, error) {
	l := NewLexer(input)
	var tokens []Token

	for {
		tok := l.NextToken()
		if tok.Type == TokEOF {
			break
		}
		if tok.Type == TokError {
			return nil, fmt.Errorf("lexer error at line %d, column %d: %s", tok.Line, tok.Column, tok.Value)
		}
		tokens = append(tokens, tok)
	}

	return tokens, nil
}