package xxsql

import (
	"errors"
	"fmt"
)

// Sentinel errors
var (
	// ErrBadConn indicates the connection is in a bad state.
	ErrBadConn = errors.New("bad connection")

	// ErrTxDone indicates the transaction has already been committed or rolled back.
	ErrTxDone = errors.New("transaction has already been committed or rolled back")

	// ErrDuplicateEntry indicates a unique constraint violation.
	ErrDuplicateEntry = errors.New("duplicate entry")

	// ErrTableNotExist indicates the table does not exist.
	ErrTableNotExist = errors.New("table does not exist")

	// ErrDeadlock indicates a deadlock was detected.
	ErrDeadlock = errors.New("deadlock detected")

	// ErrAccessDenied indicates authentication failed.
	ErrAccessDenied = errors.New("access denied")

	// ErrSyntax indicates a SQL syntax error.
	ErrSyntax = errors.New("syntax error")
)

// MySQL error codes
const (
	ErrCodeAccessDenied     = 1045
	ErrCodeDuplicateEntry   = 1062
	ErrCodeSyntax           = 1064
	ErrCodeTableNotExist    = 1146
	ErrCodeDeadlock         = 1213
	ErrCodeServerGone       = 2006
	ErrCodeConnectionLost   = 2013
)

// mysqlError represents a MySQL error.
type mysqlError struct {
	code    uint16
	state   string
	message string
}

// Error implements the error interface.
func (e *mysqlError) Error() string {
	return e.message
}

// Number returns the MySQL error code.
func (e *mysqlError) Number() uint16 {
	return e.code
}

// SQLState returns the SQL state code.
func (e *mysqlError) SQLState() string {
	return e.state
}

// Is implements errors.Is for error comparison.
func (e *mysqlError) Is(target error) bool {
	switch target {
	case ErrDuplicateEntry:
		return e.code == ErrCodeDuplicateEntry
	case ErrTableNotExist:
		return e.code == ErrCodeTableNotExist
	case ErrDeadlock:
		return e.code == ErrCodeDeadlock
	case ErrAccessDenied:
		return e.code == ErrCodeAccessDenied
	case ErrSyntax:
		return e.code == ErrCodeSyntax
	}
	return false
}

// newMySQLError creates a new MySQL error.
func newMySQLError(code uint16, state, message string) *mysqlError {
	return &mysqlError{
		code:    code,
		state:   state,
		message: message,
	}
}

// wrapError wraps an error with additional context.
func wrapError(err error, message string) error {
	return fmt.Errorf("%s: %w", message, err)
}
