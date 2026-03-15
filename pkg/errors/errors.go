// Package errors provides error types and codes for XxSql database.
package errors

import (
	"fmt"
	"strings"
)

// Position represents a position in SQL text for error reporting.
type Position struct {
	Line   int // 1-based line number
	Column int // 1-based column number
}

// XxSqlError represents a structured error with comprehensive information.
type XxSqlError struct {
	Code     int               // Primary error code (1xxx-6xxx ranges)
	SubCode  int               // Secondary code for detail
	SQLState string            // SQL standard state code (5 chars)
	Message  string            // User-facing message
	Detail   string            // Technical details
	Hint     string            // Suggested fix
	Position *Position         // Line/column in SQL
	Context  map[string]any    // Additional context
}

// Error codes organized by category (1xxx-6xxx ranges)
const (
	// Connection errors (1xxx)
	ErrConnectionFailed     = 1001
	ErrConnectionTimeout    = 1002
	ErrConnectionRefused    = 1003
	ErrConnectionReset      = 1004
	ErrTooManyConnections   = 1005
	ErrConnectionClosed     = 1006
	ErrAuthFailed           = 1007
	ErrAuthTimeout          = 1008
	ErrPermissionDenied     = 1009

	// Protocol errors (2xxx)
	ErrProtocolViolation    = 2001
	ErrInvalidMessage       = 2002
	ErrMessageTooLarge      = 2003
	ErrInvalidEncoding      = 2004
	ErrHandshakeFailed      = 2005

	// SQL parsing errors (3xxx)
	ErrSyntaxError          = 3001
	ErrInvalidToken         = 3002
	ErrUnexpectedToken      = 3003
	ErrUnterminatedString   = 3004
	ErrUnterminatedIdent    = 3005
	ErrInvalidNumber        = 3006
	ErrInvalidIdentifier    = 3007
	ErrReservedWord         = 3008

	// Semantic errors (4xxx)
	ErrUndefinedTable       = 4001
	ErrUndefinedColumn      = 4002
	ErrUndefinedFunction    = 4003
	ErrAmbiguousColumn      = 4004
	ErrDuplicateTable       = 4005
	ErrDuplicateColumn      = 4006
	ErrDuplicateIndex       = 4007
	ErrTypeMismatch         = 4008
	ErrInvalidCast          = 4009
	ErrDivisionByZero       = 4010
	ErrNullViolation        = 4011
	ErrUniqueViolation      = 4012
	ErrForeignKeyViolation  = 4013
	ErrCheckViolation       = 4014
	ErrInvalidConstraint    = 4015
	ErrTableExists          = 4016
	ErrIndexExists          = 4017
	ErrDatabaseExists       = 4018
	ErrDatabaseNotFound     = 4019

	// Execution errors (5xxx)
	ErrQueryFailed          = 5001
	ErrInsertFailed         = 5002
	ErrUpdateFailed         = 5003
	ErrDeleteFailed         = 5004
	ErrTransactionFailed    = 5005
	ErrDeadlockDetected     = 5006
	ErrLockTimeout          = 5007
	ErrRollbackFailed       = 5008
	ErrSavepointFailed      = 5009
	ErrPreparedStatementFailed = 5010
	ErrInvalidCursor        = 5011
	ErrBufferOverflow       = 5012
	ErrMemoryLimitExceeded  = 5013

	// Internal errors (6xxx)
	ErrInternalError        = 6001
	ErrStorageError         = 6002
	ErrIndexError           = 6003
	ErrCacheError           = 6004
	ErrWALError             = 6005
	ErrCheckpointError      = 6006
	ErrRecoveryError        = 6007
	ErrCorruptionDetected   = 6008
	ErrBackupError          = 6009
	ErrConfigError          = 6010
	ErrLogFileError         = 6011
	ErrPIDFileError         = 6012
)

// SQL state codes (SQL standard)
const (
	SQLStateConnectionException    = "08000"
	SQLStateConnectionDoesNotExist = "08003"
	SQLStateConnectionFailure      = "08006"
	SQLStateSyntaxError            = "42000"
	SQLStateInvalidTableDefinition = "42P01"
	SQLStateInvalidColumnDefinition = "42701"
	SQLStateDuplicateObject        = "42710"
	SQLStateUndefinedObject        = "42704"
	SQLStateAmbiguousColumn        = "42702"
	SQLStateIntegrityConstraintViolation = "23000"
	SQLStateUniqueViolation        = "23505"
	SQLStateForeignKeyViolation    = "23503"
	SQLStateCheckViolation         = "23514"
	SQLStateNullViolation          = "23502"
	SQLStateDivisionByZero         = "22012"
	SQLStateInternalError          = "XX000"
	SQLStateDataCorruption         = "XX001"
	SQLStateDiskFull               = "53100"
	SQLStateOutOfMemory            = "53000"
)

// Error implements the error interface.
func (e *XxSqlError) Error() string {
	var sb strings.Builder

	// Format: [CODE] Message
	sb.WriteString(fmt.Sprintf("[%d] %s", e.Code, e.Message))

	// Add SQL state if present
	if e.SQLState != "" {
		sb.WriteString(fmt.Sprintf(" (SQLSTATE: %s)", e.SQLState))
	}

	// Add position if present
	if e.Position != nil {
		sb.WriteString(fmt.Sprintf(" at line %d, column %d", e.Position.Line, e.Position.Column))
	}

	// Add detail if present
	if e.Detail != "" {
		sb.WriteString(fmt.Sprintf("\n  Detail: %s", e.Detail))
	}

	// Add hint if present
	if e.Hint != "" {
		sb.WriteString(fmt.Sprintf("\n  Hint: %s", e.Hint))
	}

	return sb.String()
}

// NewError creates a new XxSqlError with the given code and message.
func NewError(code int, message string) *XxSqlError {
	return &XxSqlError{
		Code:    code,
		Message: message,
		Context: make(map[string]any),
	}
}

// WithSubCode sets the subcode for the error.
func (e *XxSqlError) WithSubCode(subCode int) *XxSqlError {
	e.SubCode = subCode
	return e
}

// WithSQLState sets the SQL state for the error.
func (e *XxSqlError) WithSQLState(state string) *XxSqlError {
	e.SQLState = state
	return e
}

// WithDetail sets the detail for the error.
func (e *XxSqlError) WithDetail(detail string) *XxSqlError {
	e.Detail = detail
	return e
}

// WithHint sets the hint for the error.
func (e *XxSqlError) WithHint(hint string) *XxSqlError {
	e.Hint = hint
	return e
}

// WithPosition sets the position for the error.
func (e *XxSqlError) WithPosition(line, column int) *XxSqlError {
	e.Position = &Position{Line: line, Column: column}
	return e
}

// WithContext adds context key-value pairs to the error.
func (e *XxSqlError) WithContext(key string, value any) *XxSqlError {
	if e.Context == nil {
		e.Context = make(map[string]any)
	}
	e.Context[key] = value
	return e
}

// Is checks if the error matches the target code.
func (e *XxSqlError) Is(target error) bool {
	if t, ok := target.(*XxSqlError); ok {
		return e.Code == t.Code
	}
	return false
}

// Unwrap returns nil (no wrapped error).
func (e *XxSqlError) Unwrap() error {
	return nil
}

// CodeRange returns the category of the error based on its code.
func (e *XxSqlError) CodeRange() string {
	switch {
	case e.Code >= 1000 && e.Code < 2000:
		return "Connection"
	case e.Code >= 2000 && e.Code < 3000:
		return "Protocol"
	case e.Code >= 3000 && e.Code < 4000:
		return "Parsing"
	case e.Code >= 4000 && e.Code < 5000:
		return "Semantic"
	case e.Code >= 5000 && e.Code < 6000:
		return "Execution"
	case e.Code >= 6000 && e.Code < 7000:
		return "Internal"
	default:
		return "Unknown"
	}
}

// Convenience constructors for common errors

// ErrConnection creates a connection error.
func ErrConnection(msg string) *XxSqlError {
	return NewError(ErrConnectionFailed, msg).
		WithSQLState(SQLStateConnectionFailure)
}

// ErrSyntax creates a syntax error.
func ErrSyntax(msg string, line, col int) *XxSqlError {
	return NewError(ErrSyntaxError, msg).
		WithSQLState(SQLStateSyntaxError).
		WithPosition(line, col)
}

// ErrTableNotFound creates a table not found error.
func ErrTableNotFound(tableName string) *XxSqlError {
	return NewError(ErrUndefinedTable, fmt.Sprintf("table %q does not exist", tableName)).
		WithSQLState(SQLStateUndefinedObject).
		WithHint("Check the table name or create the table first.")
}

// ErrColumnNotFound creates a column not found error.
func ErrColumnNotFound(colName, tableName string) *XxSqlError {
	msg := fmt.Sprintf("column %q does not exist", colName)
	if tableName != "" {
		msg = fmt.Sprintf("column %q does not exist in table %q", colName, tableName)
	}
	return NewError(ErrUndefinedColumn, msg).
		WithSQLState(SQLStateUndefinedObject)
}

// ErrTableAlreadyExists creates a duplicate table error.
func ErrTableAlreadyExists(tableName string) *XxSqlError {
	return NewError(ErrTableExists, fmt.Sprintf("table %q already exists", tableName)).
		WithSQLState(SQLStateDuplicateObject)
}

// ErrInternal creates an internal error.
func ErrInternal(msg string) *XxSqlError {
	return NewError(ErrInternalError, msg).
		WithSQLState(SQLStateInternalError)
}

// ErrStorage creates a storage error.
func ErrStorage(msg string) *XxSqlError {
	return NewError(ErrStorageError, msg).
		WithSQLState(SQLStateInternalError)
}

// ErrConfig creates a configuration error.
func ErrConfig(msg string) *XxSqlError {
	return NewError(ErrConfigError, msg)
}

// ErrDeadlock creates a deadlock error.
func ErrDeadlock() *XxSqlError {
	return NewError(ErrDeadlockDetected, "deadlock detected").
		WithSQLState("40P01").
		WithHint("Try restarting the transaction.")
}