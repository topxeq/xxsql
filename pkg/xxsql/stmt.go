package xxsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// stmt implements driver.Stmt and extended interfaces.
type stmt struct {
	conn     *conn
	query    string
	paramLen int
}

// newStmt creates a new prepared statement.
func newStmt(c *conn, query string) *stmt {
	// Count placeholders
	paramLen := strings.Count(query, "?")

	return &stmt{
		conn:     c,
		query:    query,
		paramLen: paramLen,
	}
}

// Close closes the statement.
func (s *stmt) Close() error {
	return nil
}

// NumInput returns the number of parameters.
func (s *stmt) NumInput() int {
	return s.paramLen
}

// Exec executes a prepared statement with the given arguments.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

// Query executes a prepared statement and returns rows.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(args))
}

// ExecContext executes with context.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	sqlStr, err := s.interpolate(args)
	if err != nil {
		return nil, err
	}

	return s.conn.ExecContext(ctx, sqlStr, nil)
}

// QueryContext queries with context.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	sqlStr, err := s.interpolate(args)
	if err != nil {
		return nil, err
	}

	return s.conn.QueryContext(ctx, sqlStr, nil)
}

// interpolate builds the final SQL string from the query template and arguments.
func (s *stmt) interpolate(args []driver.NamedValue) (string, error) {
	return interpolateQuery(s.query, args)
}

// interpolateQuery interpolates parameters into a query string.
func interpolateQuery(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	// Convert NamedValue to Value slice
	values := make([]driver.Value, len(args))
	for _, arg := range args {
		if arg.Ordinal < 1 || arg.Ordinal > len(args) {
			return "", fmt.Errorf("invalid parameter ordinal: %d", arg.Ordinal)
		}
		values[arg.Ordinal-1] = arg.Value
	}

	return interpolateValues(query, values)
}

// interpolateValues interpolates values into a query string.
func interpolateValues(query string, values []driver.Value) (string, error) {
	result := query
	paramIndex := 0

	for i := 0; i < len(result); {
		if result[i] == '?' {
			if paramIndex >= len(values) {
				return "", fmt.Errorf("not enough parameters")
			}

			replacement := formatValue(values[paramIndex])
			result = result[:i] + replacement + result[i+1:]
			i += len(replacement)
			paramIndex++
		} else {
			i++
		}
	}

	if paramIndex < len(values) {
		return "", fmt.Errorf("too many parameters")
	}

	return result, nil
}

// formatValue formats a Go value as a SQL literal.
func formatValue(v driver.Value) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case string:
		return "'" + escapeString(val) + "'"
	case []byte:
		return "0x" + fmt.Sprintf("%x", val)
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05.999999") + "'"
	default:
		return "'" + escapeString(fmt.Sprintf("%v", val)) + "'"
	}
}

// escapeString escapes special characters for SQL strings.
func escapeString(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	for _, r := range s {
		switch r {
		case '\x00':
			result.WriteString("\\0")
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\\':
			result.WriteString("\\\\")
		case '\'':
			result.WriteString("\\'")
		case '"':
			result.WriteString("\\\"")
		case '\x1a':
			result.WriteString("\\Z")
		default:
			result.WriteRune(r)
		}
	}

	return result.String()
}

// namedValues converts a slice of driver.Value to []driver.NamedValue.
func namedValues(args []driver.Value) []driver.NamedValue {
	nv := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nv[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return nv
}
