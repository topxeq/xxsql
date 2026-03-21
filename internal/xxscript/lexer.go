// Package xxscript provides a simple scripting language for XxSql.
package xxscript

// TokenType represents a token type.
type TokenType int

const (
	TokEOF TokenType = iota
	TokError
	TokComment

	// Literals
	TokIdent    // identifier
	TokString   // "string" or 'string'
	TokNumber   // 123 or 123.45
	TokBool     // true or false
	TokNull     // null

	// Keywords
	TokVar      // var
	TokIf       // if
	TokElse     // else
	TokFor      // for
	TokWhile    // while
	TokFunc     // func
	TokReturn   // return
	TokBreak    // break
	TokContinue // continue
	TokTry      // try
	TokCatch    // catch
	TokThrow    // throw

	// Operators
	TokPlus     // +
	TokMinus    // -
	TokStar     // *
	TokSlash    // /
	TokPercent  // %
	TokEq       // ==
	TokNe       // !=
	TokLt       // <
	TokLe       // <=
	TokGt       // >
	TokGe       // >=
	TokAnd      // &&
	TokOr       // ||
	TokNot      // !
	TokAssign   // =

	// Delimiters
	TokLParen   // (
	TokRParen   // )
	TokLBrace   // {
	TokRBrace   // }
	TokLBracket // [
	TokRBracket // ]
	TokComma    // ,
	TokSemicolon // ;
	TokDot      // .
	TokColon    // :
)

// Token represents a token.
type Token struct {
	Type  TokenType
	Value string
	Line  int
	Col   int
}

// String returns the string representation of a token type.
func (t TokenType) String() string {
	switch t {
	case TokEOF:
		return "EOF"
	case TokError:
		return "ERROR"
	case TokComment:
		return "COMMENT"
	case TokIdent:
		return "IDENT"
	case TokString:
		return "STRING"
	case TokNumber:
		return "NUMBER"
	case TokBool:
		return "BOOL"
	case TokNull:
		return "NULL"
	case TokVar:
		return "VAR"
	case TokIf:
		return "IF"
	case TokElse:
		return "ELSE"
	case TokFor:
		return "FOR"
	case TokWhile:
		return "WHILE"
	case TokFunc:
		return "FUNC"
	case TokReturn:
		return "RETURN"
	case TokBreak:
		return "BREAK"
	case TokContinue:
		return "CONTINUE"
	case TokTry:
		return "TRY"
	case TokCatch:
		return "CATCH"
	case TokThrow:
		return "THROW"
	case TokPlus:
		return "+"
	case TokMinus:
		return "-"
	case TokStar:
		return "*"
	case TokSlash:
		return "/"
	case TokPercent:
		return "%"
	case TokEq:
		return "=="
	case TokNe:
		return "!="
	case TokLt:
		return "<"
	case TokLe:
		return "<="
	case TokGt:
		return ">"
	case TokGe:
		return ">="
	case TokAnd:
		return "&&"
	case TokOr:
		return "||"
	case TokNot:
		return "!"
	case TokAssign:
		return "="
	case TokLParen:
		return "("
	case TokRParen:
		return ")"
	case TokLBrace:
		return "{"
	case TokRBrace:
		return "}"
	case TokLBracket:
		return "["
	case TokRBracket:
		return "]"
	case TokComma:
		return ","
	case TokSemicolon:
		return ";"
	case TokDot:
		return "."
	case TokColon:
		return ":"
	default:
		return "UNKNOWN"
	}
}

// Lexer tokenizes XxScript source code.
type Lexer struct {
	input string
	pos   int
	line  int
	col   int
}

// NewLexer creates a new lexer.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		line:  1,
		col:   1,
	}
}

// NextToken returns the next token.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokEOF, Line: l.line, Col: l.col}
	}

	ch := l.input[l.pos]

	// Single character tokens
	switch ch {
	case '+':
		l.advance()
		return Token{Type: TokPlus, Line: l.line, Col: l.col}
	case '-':
		l.advance()
		return Token{Type: TokMinus, Line: l.line, Col: l.col}
	case '*':
		l.advance()
		return Token{Type: TokStar, Line: l.line, Col: l.col}
	case '/':
		if l.peek(1) == '/' {
			// Single line comment
			l.skipComment()
			return l.NextToken()
		}
		l.advance()
		return Token{Type: TokSlash, Line: l.line, Col: l.col}
	case '%':
		l.advance()
		return Token{Type: TokPercent, Line: l.line, Col: l.col}
	case '(':
		l.advance()
		return Token{Type: TokLParen, Line: l.line, Col: l.col}
	case ')':
		l.advance()
		return Token{Type: TokRParen, Line: l.line, Col: l.col}
	case '{':
		l.advance()
		return Token{Type: TokLBrace, Line: l.line, Col: l.col}
	case '}':
		l.advance()
		return Token{Type: TokRBrace, Line: l.line, Col: l.col}
	case '[':
		l.advance()
		return Token{Type: TokLBracket, Line: l.line, Col: l.col}
	case ']':
		l.advance()
		return Token{Type: TokRBracket, Line: l.line, Col: l.col}
	case ',':
		l.advance()
		return Token{Type: TokComma, Line: l.line, Col: l.col}
	case ';':
		l.advance()
		return Token{Type: TokSemicolon, Line: l.line, Col: l.col}
	case '.':
		l.advance()
		return Token{Type: TokDot, Line: l.line, Col: l.col}
	case ':':
		l.advance()
		return Token{Type: TokColon, Line: l.line, Col: l.col}
	case '=':
		if l.peek(1) == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokEq, Line: l.line, Col: l.col}
		}
		l.advance()
		return Token{Type: TokAssign, Line: l.line, Col: l.col}
	case '!':
		if l.peek(1) == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokNe, Line: l.line, Col: l.col}
		}
		l.advance()
		return Token{Type: TokNot, Line: l.line, Col: l.col}
	case '<':
		if l.peek(1) == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokLe, Line: l.line, Col: l.col}
		}
		l.advance()
		return Token{Type: TokLt, Line: l.line, Col: l.col}
	case '>':
		if l.peek(1) == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokGe, Line: l.line, Col: l.col}
		}
		l.advance()
		return Token{Type: TokGt, Line: l.line, Col: l.col}
	case '&':
		if l.peek(1) == '&' {
			l.advance()
			l.advance()
			return Token{Type: TokAnd, Line: l.line, Col: l.col}
		}
		return Token{Type: TokError, Value: "unexpected character '&'", Line: l.line, Col: l.col}
	case '|':
		if l.peek(1) == '|' {
			l.advance()
			l.advance()
			return Token{Type: TokOr, Line: l.line, Col: l.col}
		}
		return Token{Type: TokError, Value: "unexpected character '|'", Line: l.line, Col: l.col}
	case '"', '\'':
		return l.readString(ch)
	}

	// Numbers
	if isDigit(ch) {
		return l.readNumber()
	}

	// Identifiers and keywords
	if isIdentStart(ch) {
		return l.readIdent()
	}

	l.advance()
	return Token{Type: TokError, Value: string(ch), Line: l.line, Col: l.col}
}

func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

func (l *Lexer) peek(offset int) byte {
	pos := l.pos + offset
	if pos >= len(l.input) {
		return 0
	}
	return l.input[pos]
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.advance()
		} else {
			break
		}
	}
}

func (l *Lexer) skipComment() {
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.advance()
	}
}

func (l *Lexer) readString(quote byte) Token {
	l.advance() // skip opening quote

	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != quote {
		if l.input[l.pos] == '\\' {
			l.advance() // skip escape character
		}
		l.advance()
	}

	value := l.input[start:l.pos]
	if l.pos >= len(l.input) {
		return Token{Type: TokError, Value: "unterminated string", Line: l.line, Col: l.col}
	}

	l.advance() // skip closing quote

	// Process escape sequences
	processed := processEscapes(value)

	return Token{Type: TokString, Value: processed, Line: l.line, Col: l.col}
}

func (l *Lexer) readNumber() Token {
	start := l.pos

	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.advance()
	}

	// Check for decimal point
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.advance()
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.advance()
		}
	}

	return Token{Type: TokNumber, Value: l.input[start:l.pos], Line: l.line, Col: l.col}
}

func (l *Lexer) readIdent() Token {
	start := l.pos

	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.advance()
	}

	value := l.input[start:l.pos]

	// Check for keywords
	switch value {
	case "var":
		return Token{Type: TokVar, Value: value, Line: l.line, Col: l.col}
	case "if":
		return Token{Type: TokIf, Value: value, Line: l.line, Col: l.col}
	case "else":
		return Token{Type: TokElse, Value: value, Line: l.line, Col: l.col}
	case "for":
		return Token{Type: TokFor, Value: value, Line: l.line, Col: l.col}
	case "while":
		return Token{Type: TokWhile, Value: value, Line: l.line, Col: l.col}
	case "func":
		return Token{Type: TokFunc, Value: value, Line: l.line, Col: l.col}
	case "return":
		return Token{Type: TokReturn, Value: value, Line: l.line, Col: l.col}
	case "break":
		return Token{Type: TokBreak, Value: value, Line: l.line, Col: l.col}
	case "continue":
		return Token{Type: TokContinue, Value: value, Line: l.line, Col: l.col}
	case "try":
		return Token{Type: TokTry, Value: value, Line: l.line, Col: l.col}
	case "catch":
		return Token{Type: TokCatch, Value: value, Line: l.line, Col: l.col}
	case "throw":
		return Token{Type: TokThrow, Value: value, Line: l.line, Col: l.col}
	case "true", "false":
		return Token{Type: TokBool, Value: value, Line: l.line, Col: l.col}
	case "null":
		return Token{Type: TokNull, Value: value, Line: l.line, Col: l.col}
	default:
		return Token{Type: TokIdent, Value: value, Line: l.line, Col: l.col}
	}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}

func processEscapes(s string) string {
	result := make([]byte, 0, len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				result = append(result, '\n')
			case 't':
				result = append(result, '\t')
			case 'r':
				result = append(result, '\r')
			case '\\':
				result = append(result, '\\')
			case '"':
				result = append(result, '"')
			case '\'':
				result = append(result, '\'')
			default:
				result = append(result, s[i+1])
			}
			i += 2
		} else {
			result = append(result, s[i])
			i++
		}
	}
	return string(result)
}

// Tokenize returns all tokens from the input.
func Tokenize(input string) []Token {
	lexer := NewLexer(input)
	var tokens []Token

	for {
		tok := lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokEOF || tok.Type == TokError {
			break
		}
	}

	return tokens
}