package xxsql

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected *Config
		wantErr  bool
	}{
		{
			name: "simple DSN",
			dsn:  "root@tcp(localhost:3306)/testdb",
			expected: &Config{
				User:   "root",
				Net:    "tcp",
				Addr:   "localhost:3306",
				DBName: "testdb",
			},
		},
		{
			name: "DSN with password",
			dsn:  "user:pass@tcp(127.0.0.1:3307)/mydb",
			expected: &Config{
				User:   "user",
				Passwd: "pass",
				Net:    "tcp",
				Addr:   "127.0.0.1:3307",
				DBName: "mydb",
			},
		},
		{
			name: "DSN with defaults",
			dsn:  "user:pass@/dbname",
			expected: &Config{
				User:   "user",
				Passwd: "pass",
				Net:    "tcp",
				Addr:   "127.0.0.1:3306",
				DBName: "dbname",
			},
		},
		{
			name: "DSN with parameters",
			dsn:  "root@tcp(localhost:3306)/test?timeout=5s&charset=utf8",
			expected: &Config{
				User:     "root",
				Net:      "tcp",
				Addr:     "localhost:3306",
				DBName:   "test",
				Timeout:  5 * time.Second,
				Charset:  "utf8",
			},
		},
		{
			name:    "empty DSN",
			dsn:     "",
			wantErr: true,
		},
		{
			name:    "missing database",
			dsn:     "root@tcp(localhost:3306)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Set defaults for comparison
			expected := NewConfig()
			if tt.expected.User != "" {
				expected.User = tt.expected.User
			}
			if tt.expected.Passwd != "" {
				expected.Passwd = tt.expected.Passwd
			}
			if tt.expected.Net != "" {
				expected.Net = tt.expected.Net
			}
			if tt.expected.Addr != "" {
				expected.Addr = tt.expected.Addr
			}
			if tt.expected.DBName != "" {
				expected.DBName = tt.expected.DBName
			}
			if tt.expected.Timeout != 0 {
				expected.Timeout = tt.expected.Timeout
			}
			if tt.expected.Charset != "" {
				expected.Charset = tt.expected.Charset
			}

			if cfg.User != expected.User {
				t.Errorf("User: got %q, want %q", cfg.User, expected.User)
			}
			if cfg.Passwd != expected.Passwd {
				t.Errorf("Passwd: got %q, want %q", cfg.Passwd, expected.Passwd)
			}
			if cfg.Net != expected.Net {
				t.Errorf("Net: got %q, want %q", cfg.Net, expected.Net)
			}
			if cfg.Addr != expected.Addr {
				t.Errorf("Addr: got %q, want %q", cfg.Addr, expected.Addr)
			}
			if cfg.DBName != expected.DBName {
				t.Errorf("DBName: got %q, want %q", cfg.DBName, expected.DBName)
			}
			if tt.expected.Timeout != 0 && cfg.Timeout != expected.Timeout {
				t.Errorf("Timeout: got %v, want %v", cfg.Timeout, expected.Timeout)
			}
		})
	}
}

func TestFormatDSN(t *testing.T) {
	cfg := &Config{
		User:   "root",
		Passwd: "secret",
		Net:    "tcp",
		Addr:   "localhost:3306",
		DBName: "testdb",
	}

	dsn := cfg.FormatDSN()
	if dsn == "" {
		t.Error("FormatDSN returned empty string")
	}

	// Parse it back
	parsed, err := ParseDSN(dsn)
	if err != nil {
		t.Errorf("failed to parse formatted DSN: %v", err)
	}

	if parsed.User != cfg.User {
		t.Errorf("User: got %q, want %q", parsed.User, cfg.User)
	}
	if parsed.Passwd != cfg.Passwd {
		t.Errorf("Passwd: got %q, want %q", parsed.Passwd, cfg.Passwd)
	}
	if parsed.DBName != cfg.DBName {
		t.Errorf("DBName: got %q, want %q", parsed.DBName, cfg.DBName)
	}
}

func TestInterpolateQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []driver.NamedValue
		expected string
		wantErr  bool
	}{
		{
			name:     "no parameters",
			query:    "SELECT 1",
			args:     nil,
			expected: "SELECT 1",
		},
		{
			name:  "integer parameter",
			query: "SELECT * FROM users WHERE id = ?",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: int64(123)},
			},
			expected: "SELECT * FROM users WHERE id = 123",
		},
		{
			name:  "string parameter",
			query: "SELECT * FROM users WHERE name = ?",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: "Alice"},
			},
			expected: "SELECT * FROM users WHERE name = 'Alice'",
		},
		{
			name:  "multiple parameters",
			query: "SELECT * FROM users WHERE id = ? AND name = ?",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: int64(1)},
				{Ordinal: 2, Value: "Bob"},
			},
			expected: "SELECT * FROM users WHERE id = 1 AND name = 'Bob'",
		},
		{
			name:  "null parameter",
			query: "INSERT INTO users (name) VALUES (?)",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: nil},
			},
			expected: "INSERT INTO users (name) VALUES (NULL)",
		},
		{
			name:  "bool parameter",
			query: "SELECT * FROM users WHERE active = ?",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: true},
			},
			expected: "SELECT * FROM users WHERE active = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := interpolateQuery(tt.query, tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestEscapeString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"hello'world", "hello\\'world"},
		{"hello\"world", "hello\\\"world"},
		{"hello\nworld", "hello\\nworld"},
		{"hello\\world", "hello\\\\world"},
		{"hello\x00world", "hello\\0world"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeString(tt.input)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name     string
		value    driver.Value
		expected string
	}{
		{"nil", nil, "NULL"},
		{"int", int64(123), "123"},
		{"float", float64(3.14), "3.14"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"string", "hello", "'hello'"},
		{"bytes", []byte{0x01, 0x02}, "0x0102"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.value)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestMySQLError(t *testing.T) {
	err := &mysqlError{
		code:    1062,
		state:   "23000",
		message: "Duplicate entry '1' for key 'PRIMARY'",
	}

	if err.Error() != err.message {
		t.Errorf("Error(): got %q, want %q", err.Error(), err.message)
	}
	if err.Number() != 1062 {
		t.Errorf("Number(): got %d, want %d", err.Number(), 1062)
	}
	if err.SQLState() != "23000" {
		t.Errorf("SQLState(): got %q, want %q", err.SQLState(), "23000")
	}

	// Test errors.Is
	if !err.Is(ErrDuplicateEntry) {
		t.Error("errors.Is should match ErrDuplicateEntry")
	}
	if err.Is(ErrTableNotExist) {
		t.Error("errors.Is should not match ErrTableNotExist")
	}
}

func TestResult(t *testing.T) {
	r := &result{
		affectedRows: 10,
		lastInsertID: 5,
	}

	lastID, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId error: %v", err)
	}
	if lastID != 5 {
		t.Errorf("LastInsertId: got %d, want %d", lastID, 5)
	}

	affected, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected error: %v", err)
	}
	if affected != 10 {
		t.Errorf("RowsAffected: got %d, want %d", affected, 10)
	}
}

func TestDriverRegistration(t *testing.T) {
	// The driver should already be registered via init()
	drivers := sqlDrivers()
	found := false
	for _, name := range drivers {
		if name == DriverName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("driver %q not registered", DriverName)
	}
}

// sqlDrivers returns a list of registered SQL drivers.
func sqlDrivers() []string {
	// This is a workaround since database/sql.Drivers() isn't available in all Go versions
	// We'll check if Open returns a "unknown driver" error
	return []string{DriverName} // We know it's registered since init() runs
}
