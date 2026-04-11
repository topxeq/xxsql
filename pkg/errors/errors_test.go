package errors

import (
	"errors"
	"testing"
)

func TestXxSqlError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *XxSqlError
		contains []string
	}{
		{
			name: "basic error",
			err: &XxSqlError{
				Code:    ErrSyntaxError,
				Message: "syntax error",
			},
			contains: []string{"[3001]", "syntax error"},
		},
		{
			name: "error with SQL state",
			err: &XxSqlError{
				Code:     ErrSyntaxError,
				Message:  "syntax error",
				SQLState: SQLStateSyntaxError,
			},
			contains: []string{"[3001]", "syntax error", "SQLSTATE: 42000"},
		},
		{
			name: "error with position",
			err: &XxSqlError{
				Code:     ErrSyntaxError,
				Message:  "unexpected token",
				Position: &Position{Line: 5, Column: 10},
			},
			contains: []string{"[3001]", "unexpected token", "line 5", "column 10"},
		},
		{
			name: "error with detail",
			err: &XxSqlError{
				Code:    ErrInternalError,
				Message: "internal error",
				Detail:  "something went wrong",
			},
			contains: []string{"Detail: something went wrong"},
		},
		{
			name: "error with hint",
			err: &XxSqlError{
				Code:    ErrUndefinedTable,
				Message: "table not found",
				Hint:    "Check the table name",
			},
			contains: []string{"Hint: Check the table name"},
		},
		{
			name: "complete error",
			err: &XxSqlError{
				Code:     ErrSyntaxError,
				SubCode:  1,
				Message:  "syntax error",
				SQLState: SQLStateSyntaxError,
				Detail:   "near SELECT",
				Hint:     "Check your query",
				Position: &Position{Line: 1, Column: 5},
			},
			contains: []string{"[3001]", "syntax error", "SQLSTATE: 42000", "line 1", "column 5", "Detail: near SELECT", "Hint: Check your query"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			for _, s := range tt.contains {
				if !containsString(got, s) {
					t.Errorf("Error() = %q, should contain %q", got, s)
				}
			}
		})
	}
}

func TestXxSqlError_WithMethods(t *testing.T) {
	err := NewError(ErrSyntaxError, "test error")

	err.WithSubCode(42)
	if err.SubCode != 42 {
		t.Errorf("WithSubCode: got %d, want 42", err.SubCode)
	}

	err.WithSQLState(SQLStateSyntaxError)
	if err.SQLState != SQLStateSyntaxError {
		t.Errorf("WithSQLState: got %s, want %s", err.SQLState, SQLStateSyntaxError)
	}

	err.WithDetail("detail text")
	if err.Detail != "detail text" {
		t.Errorf("WithDetail: got %s, want 'detail text'", err.Detail)
	}

	err.WithHint("hint text")
	if err.Hint != "hint text" {
		t.Errorf("WithHint: got %s, want 'hint text'", err.Hint)
	}

	err.WithPosition(10, 20)
	if err.Position == nil || err.Position.Line != 10 || err.Position.Column != 20 {
		t.Errorf("WithPosition: got %+v, want Line=10, Column=20", err.Position)
	}

	err.WithContext("key", "value")
	if err.Context["key"] != "value" {
		t.Errorf("WithContext: got %v, want 'value'", err.Context["key"])
	}
}

func TestXxSqlError_Is(t *testing.T) {
	err1 := NewError(ErrSyntaxError, "error 1")
	err2 := NewError(ErrSyntaxError, "error 2")
	err3 := NewError(ErrInternalError, "error 3")

	if !errors.Is(err1, err2) {
		t.Error("errors with same code should match")
	}

	if errors.Is(err1, err3) {
		t.Error("errors with different codes should not match")
	}

	var stdErr error = errors.New("standard error")
	if errors.Is(err1, stdErr) {
		t.Error("XxSqlError should not match standard error")
	}
}

func TestXxSqlError_Unwrap(t *testing.T) {
	err := NewError(ErrInternalError, "test")
	if unwrapped := err.Unwrap(); unwrapped != nil {
		t.Errorf("Unwrap should return nil, got %v", unwrapped)
	}
}

func TestXxSqlError_CodeRange(t *testing.T) {
	tests := []struct {
		code   int
		range_ string
	}{
		{ErrConnectionFailed, "Connection"},
		{ErrProtocolViolation, "Protocol"},
		{ErrSyntaxError, "Parsing"},
		{ErrUndefinedTable, "Semantic"},
		{ErrQueryFailed, "Execution"},
		{ErrInternalError, "Internal"},
		{999, "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.range_, func(t *testing.T) {
			err := &XxSqlError{Code: tt.code}
			if got := err.CodeRange(); got != tt.range_ {
				t.Errorf("CodeRange() = %q, want %q", got, tt.range_)
			}
		})
	}
}

func TestNewError(t *testing.T) {
	err := NewError(ErrSyntaxError, "test message")

	if err.Code != ErrSyntaxError {
		t.Errorf("Code: got %d, want %d", err.Code, ErrSyntaxError)
	}
	if err.Message != "test message" {
		t.Errorf("Message: got %q, want 'test message'", err.Message)
	}
	if err.Context == nil {
		t.Error("Context should be initialized")
	}
}

func TestConvenienceConstructors(t *testing.T) {
	tests := []struct {
		name     string
		err      *XxSqlError
		wantCode int
	}{
		{"ErrConnection", ErrConnection("connection failed"), ErrConnectionFailed},
		{"ErrSyntax", ErrSyntax("bad syntax", 1, 5), ErrSyntaxError},
		{"ErrTableNotFound", ErrTableNotFound("users"), ErrUndefinedTable},
		{"ErrColumnNotFound", ErrColumnNotFound("id", "users"), ErrUndefinedColumn},
		{"ErrTableAlreadyExists", ErrTableAlreadyExists("users"), ErrTableExists},
		{"ErrInternal", ErrInternal("internal error"), ErrInternalError},
		{"ErrStorage", ErrStorage("storage error"), ErrStorageError},
		{"ErrConfig", ErrConfig("config error"), ErrConfigError},
		{"ErrDeadlock", ErrDeadlock(), ErrDeadlockDetected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Code != tt.wantCode {
				t.Errorf("Code: got %d, want %d", tt.err.Code, tt.wantCode)
			}
			if tt.err.Message == "" {
				t.Error("Message should not be empty")
			}
		})
	}
}

func TestErrSyntax_Position(t *testing.T) {
	err := ErrSyntax("unexpected token", 10, 20)

	if err.Position == nil {
		t.Fatal("Position should not be nil")
	}
	if err.Position.Line != 10 {
		t.Errorf("Line: got %d, want 10", err.Position.Line)
	}
	if err.Position.Column != 20 {
		t.Errorf("Column: got %d, want 20", err.Position.Column)
	}
}

func TestErrTableNotFound_Message(t *testing.T) {
	err := ErrTableNotFound("mytable")

	if !containsString(err.Message, "mytable") {
		t.Errorf("Message should contain table name: %s", err.Message)
	}
	if err.Hint == "" {
		t.Error("Hint should not be empty")
	}
}

func TestErrColumnNotFound_Message(t *testing.T) {
	err := ErrColumnNotFound("mycol", "mytable")

	if !containsString(err.Message, "mycol") {
		t.Errorf("Message should contain column name: %s", err.Message)
	}
	if !containsString(err.Message, "mytable") {
		t.Errorf("Message should contain table name: %s", err.Message)
	}

	errNoTable := ErrColumnNotFound("mycol", "")
	if containsString(errNoTable.Message, "in table") {
		t.Errorf("Message without table should not contain 'in table': %s", errNoTable.Message)
	}
}

func TestErrorCodes(t *testing.T) {
	ranges := []struct {
		name string
		min  int
		max  int
	}{
		{"Connection", 1000, 1999},
		{"Protocol", 2000, 2999},
		{"Parsing", 3000, 3999},
		{"Semantic", 4000, 4999},
		{"Execution", 5000, 5999},
		{"Internal", 6000, 6999},
	}

	codeTests := []struct {
		code int
		min  int
		max  int
	}{
		{ErrConnectionFailed, 1000, 1999},
		{ErrProtocolViolation, 2000, 2999},
		{ErrSyntaxError, 3000, 3999},
		{ErrUndefinedTable, 4000, 4999},
		{ErrQueryFailed, 5000, 5999},
		{ErrInternalError, 6000, 6999},
	}

	for _, tt := range codeTests {
		if tt.code < tt.min || tt.code > tt.max {
			t.Errorf("Code %d out of range [%d, %d]", tt.code, tt.min, tt.max)
		}
	}

	_ = ranges
}

func TestSQLStateCodes(t *testing.T) {
	states := []string{
		SQLStateConnectionException,
		SQLStateSyntaxError,
		SQLStateUniqueViolation,
		SQLStateForeignKeyViolation,
		SQLStateCheckViolation,
		SQLStateNullViolation,
		SQLStateInternalError,
	}

	for _, state := range states {
		if len(state) != 5 {
			t.Errorf("SQL state %q should be 5 characters", state)
		}
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
