package xxscript

import (
	"testing"
)

func TestNewLexer(t *testing.T) {
	l := NewLexer("1 + 2")
	if l == nil {
		t.Fatal("NewLexer returned nil")
	}
	if l.line != 1 || l.col != 1 {
		t.Errorf("Expected line=1, col=1, got line=%d, col=%d", l.line, l.col)
	}
}

func TestLexer_Identifiers(t *testing.T) {
	tokens := Tokenize("foo bar123 _test")
	if len(tokens) != 4 {
		t.Fatalf("Expected 4 tokens, got %d", len(tokens))
	}
	expected := []TokenType{TokIdent, TokIdent, TokIdent, TokEOF}
	for i, tt := range expected {
		if tokens[i].Type != tt {
			t.Errorf("Token %d: expected %s, got %s", i, tt, tokens[i].Type)
		}
	}
}

func TestLexer_Numbers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"123", "123"},
		{"3.14", "3.14"},
		{"0", "0"},
		{"100.0", "100.0"},
	}

	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != TokNumber {
			t.Errorf("Input %s: expected NUMBER, got %s", tt.input, tokens[0].Type)
		}
		if tokens[0].Value != tt.expected {
			t.Errorf("Input %s: expected value %s, got %s", tt.input, tt.expected, tokens[0].Value)
		}
	}
}

func TestLexer_Strings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"hello"`, "hello"},
		{`'world'`, "world"},
		{`"hello\nworld"`, "hello\nworld"},
		{`"tab\there"`, "tab\there"},
		{`"quote\"here"`, "quote\"here"},
		{`'single\'quote'`, "single'quote"},
		{`"back\\slash"`, "back\\slash"},
	}

	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != TokString {
			t.Errorf("Input %s: expected STRING, got %s", tt.input, tokens[0].Type)
		}
		if tokens[0].Value != tt.expected {
			t.Errorf("Input %s: expected value %q, got %q", tt.input, tt.expected, tokens[0].Value)
		}
	}
}

func TestLexer_UnterminatedString(t *testing.T) {
	tokens := Tokenize(`"unterminated`)
	if tokens[0].Type != TokError {
		t.Errorf("Expected ERROR, got %s", tokens[0].Type)
	}
}

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"var", TokVar},
		{"if", TokIf},
		{"else", TokElse},
		{"for", TokFor},
		{"while", TokWhile},
		{"func", TokFunc},
		{"return", TokReturn},
		{"break", TokBreak},
		{"continue", TokContinue},
		{"try", TokTry},
		{"catch", TokCatch},
		{"throw", TokThrow},
		{"switch", TokSwitch},
		{"case", TokCase},
		{"default", TokDefault},
		{"in", TokIn},
		{"true", TokBool},
		{"false", TokBool},
		{"null", TokNull},
	}

	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != tt.expected {
			t.Errorf("Input %s: expected %s, got %s", tt.input, tt.expected, tokens[0].Type)
		}
	}
}

func TestLexer_Operators(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"+", TokPlus},
		{"-", TokMinus},
		{"*", TokStar},
		{"/", TokSlash},
		{"%", TokPercent},
		{"==", TokEq},
		{"!=", TokNe},
		{"<", TokLt},
		{"<=", TokLe},
		{">", TokGt},
		{">=", TokGe},
		{"&&", TokAnd},
		{"||", TokOr},
		{"!", TokNot},
		{"=", TokAssign},
		{"+=", TokPlusAssign},
		{"-=", TokMinusAssign},
		{"*=", TokStarAssign},
		{"/=", TokSlashAssign},
		{"%=", TokPercentAssign},
		{"++", TokInc},
		{"--", TokDec},
		{"?", TokQuestion},
	}

	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != tt.expected {
			t.Errorf("Input %s: expected %s, got %s", tt.input, tt.expected, tokens[0].Type)
		}
	}
}

func TestLexer_Delimiters(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"(", TokLParen},
		{")", TokRParen},
		{"{", TokLBrace},
		{"}", TokRBrace},
		{"[", TokLBracket},
		{"]", TokRBracket},
		{",", TokComma},
		{";", TokSemicolon},
		{".", TokDot},
		{":", TokColon},
		{"...", TokSpread},
	}

	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != tt.expected {
			t.Errorf("Input %s: expected %s, got %s", tt.input, tt.expected, tokens[0].Type)
		}
	}
}

func TestLexer_SingleCharError(t *testing.T) {
	tests := []struct {
		input       string
		expectedErr string
	}{
		{"&", "unexpected character '&'"},
		{"|", "unexpected character '|'"},
		{"@", "@"},
		{"#", "#"},
	}

	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != TokError {
			t.Errorf("Input %s: expected ERROR, got %s", tt.input, tokens[0].Type)
		}
	}
}

func TestLexer_Comments(t *testing.T) {
	tokens := Tokenize("// this is a comment\nvar x = 1")
	if tokens[0].Type != TokVar {
		t.Errorf("Expected VAR after comment, got %s", tokens[0].Type)
	}
}

func TestLexer_Whitespace(t *testing.T) {
	tokens := Tokenize("  \t\n  var  \t  x")
	if tokens[0].Type != TokVar {
		t.Errorf("Expected VAR, got %s", tokens[0].Type)
	}
}

func TestLexer_EOF(t *testing.T) {
	tokens := Tokenize("")
	if len(tokens) != 1 || tokens[0].Type != TokEOF {
		t.Errorf("Expected EOF, got %v", tokens)
	}
}

func TestLexer_MultiLine(t *testing.T) {
	input := `var x = 10
var y = 20
x + y`
	tokens := Tokenize(input)
	varTypes := []TokenType{TokVar, TokIdent, TokAssign, TokNumber, TokVar, TokIdent, TokAssign, TokNumber, TokIdent, TokPlus, TokIdent, TokEOF}
	if len(tokens) != len(varTypes) {
		t.Fatalf("Expected %d tokens, got %d", len(varTypes), len(tokens))
	}
	for i, tt := range varTypes {
		if tokens[i].Type != tt {
			t.Errorf("Token %d: expected %s, got %s (%s)", i, tt, tokens[i].Type, tokens[i].Value)
		}
	}
}

func TestTokenType_String(t *testing.T) {
	tests := []struct {
		tok      TokenType
		expected string
	}{
		{TokEOF, "EOF"},
		{TokError, "ERROR"},
		{TokComment, "COMMENT"},
		{TokIdent, "IDENT"},
		{TokString, "STRING"},
		{TokNumber, "NUMBER"},
		{TokBool, "BOOL"},
		{TokNull, "NULL"},
		{TokVar, "VAR"},
		{TokIf, "IF"},
		{TokElse, "ELSE"},
		{TokFor, "FOR"},
		{TokWhile, "WHILE"},
		{TokFunc, "FUNC"},
		{TokReturn, "RETURN"},
		{TokBreak, "BREAK"},
		{TokContinue, "CONTINUE"},
		{TokTry, "TRY"},
		{TokCatch, "CATCH"},
		{TokThrow, "THROW"},
		{TokSwitch, "SWITCH"},
		{TokCase, "CASE"},
		{TokDefault, "DEFAULT"},
		{TokIn, "IN"},
		{TokPlus, "+"},
		{TokMinus, "-"},
		{TokStar, "*"},
		{TokSlash, "/"},
		{TokPercent, "%"},
		{TokEq, "=="},
		{TokNe, "!="},
		{TokLt, "<"},
		{TokLe, "<="},
		{TokGt, ">"},
		{TokGe, ">="},
		{TokAnd, "&&"},
		{TokOr, "||"},
		{TokNot, "!"},
		{TokAssign, "="},
		{TokPlusAssign, "+="},
		{TokMinusAssign, "-="},
		{TokStarAssign, "*="},
		{TokSlashAssign, "/="},
		{TokPercentAssign, "%="},
		{TokInc, "++"},
		{TokDec, "--"},
		{TokQuestion, "?"},
		{TokColonColon, "::"},
		{TokLParen, "("},
		{TokRParen, ")"},
		{TokLBrace, "{"},
		{TokRBrace, "}"},
		{TokLBracket, "["},
		{TokRBracket, "]"},
		{TokComma, ","},
		{TokSemicolon, ";"},
		{TokDot, "."},
		{TokColon, ":"},
		{TokSpread, "..."},
		{TokenType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		if tt.tok.String() != tt.expected {
			t.Errorf("TokenType(%d).String() = %s, want %s", tt.tok, tt.tok.String(), tt.expected)
		}
	}
}
