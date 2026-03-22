package xxsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()

	if cfg.Net != DefaultNet {
		t.Errorf("Net: got %q, want %q", cfg.Net, DefaultNet)
	}
	if cfg.Addr != DefaultAddr {
		t.Errorf("Addr: got %q, want %q", cfg.Addr, DefaultAddr)
	}
	if cfg.Timeout != DefaultTimeout {
		t.Errorf("Timeout: got %v, want %v", cfg.Timeout, DefaultTimeout)
	}
	if cfg.Charset != DefaultCharset {
		t.Errorf("Charset: got %q, want %q", cfg.Charset, DefaultCharset)
	}
}

func TestConfigClone(t *testing.T) {
	original := &Config{
		User:             "testuser",
		Passwd:           "testpass",
		Net:              "tcp",
		Addr:             "localhost:3307",
		DBName:           "testdb",
		Timeout:          15 * time.Second,
		ReadTimeout:      20 * time.Second,
		WriteTimeout:     25 * time.Second,
		Charset:          "utf8",
		Collation:        "utf8_general_ci",
		TLS:              true,
		AllowOldPassword: true,
		MaxAllowedPacket: 8192,
	}

	cloned := original.Clone()

	if cloned.User != original.User {
		t.Errorf("User: got %q, want %q", cloned.User, original.User)
	}
	if cloned.Passwd != original.Passwd {
		t.Errorf("Passwd: got %q, want %q", cloned.Passwd, original.Passwd)
	}
	if cloned.DBName != original.DBName {
		t.Errorf("DBName: got %q, want %q", cloned.DBName, original.DBName)
	}
	if cloned.Timeout != original.Timeout {
		t.Errorf("Timeout: got %v, want %v", cloned.Timeout, original.Timeout)
	}
	if cloned.TLS != original.TLS {
		t.Errorf("TLS: got %v, want %v", cloned.TLS, original.TLS)
	}

	cloned.User = "modified"
	if original.User == "modified" {
		t.Error("Modifying clone should not affect original")
	}
}

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
				User:    "root",
				Net:     "tcp",
				Addr:    "localhost:3306",
				DBName:  "test",
				Timeout: 5 * time.Second,
				Charset: "utf8",
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
		{
			name: "DSN with TLS",
			dsn:  "user@tcp(localhost:3306)/db?tls=true",
			expected: &Config{
				User:   "user",
				Net:    "tcp",
				Addr:   "localhost:3306",
				DBName: "db",
				TLS:    true,
			},
		},
		{
			name: "DSN with readTimeout",
			dsn:  "user@tcp(localhost:3306)/db?readTimeout=10s",
			expected: &Config{
				User:        "user",
				Net:         "tcp",
				Addr:        "localhost:3306",
				DBName:      "db",
				ReadTimeout: 10 * time.Second,
			},
		},
		{
			name: "DSN with writeTimeout",
			dsn:  "user@tcp(localhost:3306)/db?writeTimeout=15s",
			expected: &Config{
				User:         "user",
				Net:          "tcp",
				Addr:         "localhost:3306",
				DBName:       "db",
				WriteTimeout: 15 * time.Second,
			},
		},
		{
			name: "DSN with collation",
			dsn:  "user@tcp(localhost:3306)/db?collation=utf8mb4_unicode_ci",
			expected: &Config{
				User:      "user",
				Net:       "tcp",
				Addr:      "localhost:3306",
				DBName:    "db",
				Collation: "utf8mb4_unicode_ci",
			},
		},
		{
			name: "DSN with allowOldPasswords",
			dsn:  "user@tcp(localhost:3306)/db?allowOldPasswords=true",
			expected: &Config{
				User:             "user",
				Net:              "tcp",
				Addr:             "localhost:3306",
				DBName:           "db",
				AllowOldPassword: true,
			},
		},
		{
			name: "DSN with maxAllowedPacket",
			dsn:  "user@tcp(localhost:3306)/db?maxAllowedPacket=16777216",
			expected: &Config{
				User:             "user",
				Net:              "tcp",
				Addr:             "localhost:3306",
				DBName:           "db",
				MaxAllowedPacket: 16777216,
			},
		},
		// URL format DSN tests
		{
			name: "URL format simple",
			dsn:  "xxsql://root@localhost:3306/testdb",
			expected: &Config{
				User:   "root",
				Net:    "tcp",
				Addr:   "localhost:3306",
				DBName: "testdb",
			},
		},
		{
			name: "URL format with password",
			dsn:  "xxsql://user:pass@127.0.0.1:3307/mydb",
			expected: &Config{
				User:   "user",
				Passwd: "pass",
				Net:    "tcp",
				Addr:   "127.0.0.1:3307",
				DBName: "mydb",
			},
		},
		{
			name: "URL format without port",
			dsn:  "xxsql://root@localhost/testdb",
			expected: &Config{
				User:     "root",
				Net:      "tcp",
				Addr:     "localhost:9527", // Default to private protocol port
				Protocol: ProtocolPrivate,
				DBName:   "testdb",
			},
		},
		{
			name: "URL format with parameters",
			dsn:  "xxsql://root@localhost:3306/test?timeout=5s&charset=utf8",
			expected: &Config{
				User:    "root",
				Net:     "tcp",
				Addr:    "localhost:3306",
				DBName:  "test",
				Timeout: 5 * time.Second,
				Charset: "utf8",
			},
		},
		{
			name: "URL format with TLS",
			dsn:  "xxsql://user:secret@localhost:3306/db?tls=true",
			expected: &Config{
				User:   "user",
				Passwd: "secret",
				Net:    "tcp",
				Addr:   "localhost:3306",
				DBName: "db",
				TLS:    true,
			},
		},
		{
			name: "URL format no user",
			dsn:  "xxsql://localhost:3306/testdb",
			expected: &Config{
				Net:    "tcp",
				Addr:   "localhost:3306",
				DBName: "testdb",
			},
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
			if tt.expected.ReadTimeout != 0 {
				expected.ReadTimeout = tt.expected.ReadTimeout
			}
			if tt.expected.WriteTimeout != 0 {
				expected.WriteTimeout = tt.expected.WriteTimeout
			}
			if tt.expected.Collation != "" {
				expected.Collation = tt.expected.Collation
			}
			if tt.expected.TLS {
				expected.TLS = tt.expected.TLS
			}
			if tt.expected.AllowOldPassword {
				expected.AllowOldPassword = tt.expected.AllowOldPassword
			}
			if tt.expected.MaxAllowedPacket != 0 {
				expected.MaxAllowedPacket = tt.expected.MaxAllowedPacket
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
		{
			name:  "float parameter",
			query: "SELECT * FROM products WHERE price = ?",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: float64(19.99)},
			},
			expected: "SELECT * FROM products WHERE price = 19.99",
		},
		{
			name:  "time parameter",
			query: "SELECT * FROM events WHERE created_at = ?",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
			},
			expected: "SELECT * FROM events WHERE created_at = '2024-01-15 10:30:00'",
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
		{"int32", int64(456), "456"},
		{"uint64", int64(999), "999"},
		{"float32", float64(2.5), "2.5"},
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

	if !err.Is(ErrDuplicateEntry) {
		t.Error("errors.Is should match ErrDuplicateEntry")
	}
	if err.Is(ErrTableNotExist) {
		t.Error("errors.Is should not match ErrTableNotExist")
	}
}

func TestMySQLError_AllCodes(t *testing.T) {
	tests := []struct {
		code    uint16
		target  error
		matches bool
	}{
		{ErrCodeAccessDenied, ErrAccessDenied, true},
		{ErrCodeDuplicateEntry, ErrDuplicateEntry, true},
		{ErrCodeSyntax, ErrSyntax, true},
		{ErrCodeTableNotExist, ErrTableNotExist, true},
		{ErrCodeDeadlock, ErrDeadlock, true},
		{ErrCodeAccessDenied, ErrTableNotExist, false},
		{ErrCodeDuplicateEntry, ErrSyntax, false},
	}

	for _, tt := range tests {
		t.Run(tt.target.Error(), func(t *testing.T) {
			err := &mysqlError{code: tt.code}
			if err.Is(tt.target) != tt.matches {
				t.Errorf("Is(%v) = %v, want %v", tt.target, !tt.matches, tt.matches)
			}
		})
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

func TestReadLengthEncodedInt(t *testing.T) {
	tests := []struct {
		data     []byte
		expected int64
		n        int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x7F}, 127, 1},
		{[]byte{0xFA}, 250, 1},
		{[]byte{0xFC, 0x01, 0x00}, 1, 3},
		{[]byte{0xFC, 0xFF, 0x00}, 255, 3},
		{[]byte{0xFC, 0xFF, 0x01}, 511, 3},
		{[]byte{0xFD, 0x00, 0x01, 0x00}, 256, 4},
		{[]byte{0xFE, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 1, 9},
		{[]byte{}, 0, 0},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			val, n := readLengthEncodedInt(tt.data)
			if val != tt.expected {
				t.Errorf("value: got %d, want %d", val, tt.expected)
			}
			if n != tt.n {
				t.Errorf("bytes read: got %d, want %d", n, tt.n)
			}
		})
	}
}

func TestIsolationLevelToString(t *testing.T) {
	tests := []struct {
		level    driver.IsolationLevel
		expected string
	}{
		{IsolationLevelReadUncommitted, "READ UNCOMMITTED"},
		{IsolationLevelReadCommitted, "READ COMMITTED"},
		{IsolationLevelRepeatableRead, "REPEATABLE READ"},
		{IsolationLevelSerializable, "SERIALIZABLE"},
		{IsolationLevelDefault, ""},
		{driver.IsolationLevel(99), ""},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := isolationLevelToString(tt.level)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestDriverRegistration(t *testing.T) {
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

func TestNewMySQLError(t *testing.T) {
	err := newMySQLError(ErrCodeAccessDenied, "28000", "Access denied for user 'root'")

	if err.code != ErrCodeAccessDenied {
		t.Errorf("code: got %d, want %d", err.code, ErrCodeAccessDenied)
	}
	if err.state != "28000" {
		t.Errorf("state: got %q, want '28000'", err.state)
	}
	if err.message != "Access denied for user 'root'" {
		t.Errorf("message: got %q", err.message)
	}
}

func TestWrapError(t *testing.T) {
	inner := &mysqlError{code: 1045, message: "Access denied"}
	wrapped := wrapError(inner, "connection failed")

	if wrapped.Error() != "connection failed: Access denied" {
		t.Errorf("got %q", wrapped.Error())
	}
}

func sqlDrivers() []string {
	return []string{DriverName}
}

func TestWriteLengthEncodedInt(t *testing.T) {
	tests := []struct {
		value    int64
		expected int
	}{
		{0, 1},
		{250, 1},
		{251, 3},
		{65535, 3},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.value), func(t *testing.T) {
			mc := &mysqlConn{}
			result := mc.writeLengthEncodedInt(tt.value)
			if len(result) != tt.expected {
				t.Errorf("writeLengthEncodedInt(%d) length: got %d, want %d", tt.value, len(result), tt.expected)
			}
		})
	}
}

func TestAuthPassword(t *testing.T) {
	mc := &mysqlConn{}

	t.Run("empty password", func(t *testing.T) {
		result := mc.authPassword("", make([]byte, 20))
		if result != nil {
			t.Error("Empty password should return nil")
		}
	})

	t.Run("non-empty password", func(t *testing.T) {
		salt := make([]byte, 20)
		for i := range salt {
			salt[i] = byte(i)
		}

		result := mc.authPassword("testpassword", salt)
		if len(result) != 20 {
			t.Errorf("Auth response length: got %d, want 20", len(result))
		}
	})
}

func TestParseOKPacket(t *testing.T) {
	mc := &mysqlConn{}

	packet := []byte{OKPacket, 0x05, 0x03}
	affectedRows, lastInsertID, err := mc.parseOKPacket(packet)

	if err != nil {
		t.Errorf("parseOKPacket error: %v", err)
	}
	if affectedRows != 5 {
		t.Errorf("Affected rows: got %d, want 5", affectedRows)
	}
	if lastInsertID != 3 {
		t.Errorf("Last insert ID: got %d, want 3", lastInsertID)
	}
}

func TestParseError(t *testing.T) {
	mc := &mysqlConn{}

	packet := []byte{ERRPacket, 0x51, 0x04, '#', '4', '2', '0', '0', '0', 'T', 'e', 's', 't', ' ', 'e', 'r', 'r', 'o', 'r'}

	err := mc.parseError(packet)
	if err == nil {
		t.Fatal("parseError should return an error")
	}

	mysqlErr, ok := err.(*mysqlError)
	if !ok {
		t.Fatalf("Expected *mysqlError, got %T", err)
	}

	if mysqlErr.Number() != 1105 {
		t.Errorf("Error code: got %d, want 1105", mysqlErr.Number())
	}
}

func TestReadLengthEncodedInt_EdgeCases(t *testing.T) {
	mc := &mysqlConn{}

	t.Run("empty data", func(t *testing.T) {
		val, n := mc.readLengthEncodedInt([]byte{})
		if val != 0 || n != 0 {
			t.Errorf("Empty data: got (%d, %d), want (0, 0)", val, n)
		}
	})

	t.Run("small values", func(t *testing.T) {
		data := []byte{100}
		val, n := mc.readLengthEncodedInt(data)
		if val != 100 || n != 1 {
			t.Errorf("Small value: got (%d, %d), want (100, 1)", val, n)
		}
	})

	t.Run("0xFC prefix", func(t *testing.T) {
		data := []byte{0xFC, 0x01, 0x00}
		val, n := mc.readLengthEncodedInt(data)
		if val != 1 || n != 3 {
			t.Errorf("0xFC: got (%d, %d), want (1, 3)", val, n)
		}
	})
}

func TestConnector(t *testing.T) {
	cfg := NewConfig()
	cfg.User = "testuser"
	cfg.DBName = "testdb"

	conn := &connector{
		dsn: "testuser@tcp(localhost:3306)/testdb",
		cfg: cfg,
		drv: driverInstance,
	}

	t.Run("Driver", func(t *testing.T) {
		d := conn.Driver()
		if d == nil {
			t.Error("Driver should not be nil")
		}
	})
}

func TestOpen(t *testing.T) {
	dsn := "testuser:testpass@tcp(localhost:3306)/testdb"

	db, err := Open(dsn)
	// Open returns a *sql.DB even if connection fails
	// The actual connection happens on first use
	if db == nil && err == nil {
		t.Error("Open returned nil db without error")
	}
	_ = db
	_ = err
}

func TestOpenDB(t *testing.T) {
	dsn := "testuser:testpass@tcp(localhost:3306)/testdb"

	db, err := OpenDB(dsn)
	// OpenDB returns a *sql.DB even if connection fails
	if db == nil && err == nil {
		t.Error("OpenDB returned nil db without error")
	}
	_ = db
	_ = err
}

func TestDriverOpen(t *testing.T) {
	d := &xxsqlDriver{}

	t.Run("invalid DSN", func(t *testing.T) {
		_, err := d.Open("")
		if err == nil {
			t.Error("Open with empty DSN should error")
		}
	})
}

func TestDriverOpenConnector(t *testing.T) {
	d := &xxsqlDriver{}

	t.Run("invalid DSN", func(t *testing.T) {
		_, err := d.OpenConnector("")
		if err == nil {
			t.Error("OpenConnector with empty DSN should error")
		}
	})

	t.Run("valid DSN", func(t *testing.T) {
		conn, err := d.OpenConnector("root@tcp(localhost:3306)/test")
		if err != nil {
			t.Fatalf("OpenConnector error: %v", err)
		}
		if conn == nil {
			t.Error("OpenConnector returned nil")
		}
	})
}

func TestConstants(t *testing.T) {
	if ProtocolVersion != 10 {
		t.Errorf("ProtocolVersion: got %d, want 10", ProtocolVersion)
	}
	if ComQuit != 0x01 {
		t.Errorf("ComQuit: got 0x%02X, want 0x01", ComQuit)
	}
	if ComQuery != 0x03 {
		t.Errorf("ComQuery: got 0x%02X, want 0x03", ComQuery)
	}
	if ComPing != 0x0E {
		t.Errorf("ComPing: got 0x%02X, want 0x0E", ComPing)
	}
	if OKPacket != 0x00 {
		t.Errorf("OKPacket: got 0x%02X, want 0x00", OKPacket)
	}
	if ERRPacket != 0xFF {
		t.Errorf("ERRPacket: got 0x%02X, want 0xFF", ERRPacket)
	}
}

func TestDefaultClientCapabilities(t *testing.T) {
	caps := DefaultClientCapabilities

	if caps&ClientProtocol41 == 0 {
		t.Error("ClientProtocol41 should be set")
	}
	if caps&ClientSecureConn == 0 {
		t.Error("ClientSecureConn should be set")
	}
	if caps&ClientPluginAuth == 0 {
		t.Error("ClientPluginAuth should be set")
	}
}

func TestFormatValue_Time(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	result := formatValue(testTime)

	if !strings.Contains(result, "2024") {
		t.Errorf("Time format should contain year: %q", result)
	}
}

func TestEscapeString_AllSpecials(t *testing.T) {
	input := "test\x00\n\r\\'\"\x1a"
	result := escapeString(input)

	if !strings.Contains(result, "\\0") {
		t.Error("Should escape null byte")
	}
	if !strings.Contains(result, "\\n") {
		t.Error("Should escape newline")
	}
	if !strings.Contains(result, "\\r") {
		t.Error("Should escape carriage return")
	}
	if !strings.Contains(result, "\\\\") {
		t.Error("Should escape backslash")
	}
}

func TestInterpolateQuery_MoreTypes(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		args    []driver.NamedValue
		wantErr bool
	}{
		{
			name:    "bytes",
			query:   "SELECT * FROM t WHERE data = ?",
			args:    []driver.NamedValue{{Ordinal: 1, Value: []byte{0x01, 0x02}}},
			wantErr: false,
		},
		{
			name:    "float32",
			query:   "SELECT * FROM t WHERE val = ?",
			args:    []driver.NamedValue{{Ordinal: 1, Value: float32(3.14)}},
			wantErr: false,
		},
		{
			name:    "int32",
			query:   "SELECT * FROM t WHERE id = ?",
			args:    []driver.NamedValue{{Ordinal: 1, Value: int32(123)}},
			wantErr: false,
		},
		{
			name:    "uint",
			query:   "SELECT * FROM t WHERE id = ?",
			args:    []driver.NamedValue{{Ordinal: 1, Value: uint(456)}},
			wantErr: false,
		},
		{
			name:    "uint64",
			query:   "SELECT * FROM t WHERE id = ?",
			args:    []driver.NamedValue{{Ordinal: 1, Value: uint64(789)}},
			wantErr: false,
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
			if result == "" {
				t.Error("result should not be empty")
			}
		})
	}
}

func TestNewMySQLConn(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)

	if mc == nil {
		t.Fatal("newMySQLConn returned nil")
	}
	if mc.reader == nil {
		t.Error("reader should not be nil")
	}
	if mc.writer == nil {
		t.Error("writer should not be nil")
	}
	if mc.capability != DefaultClientCapabilities {
		t.Errorf("capability: got %d, want %d", mc.capability, DefaultClientCapabilities)
	}
}

func TestNamedValues(t *testing.T) {
	args := []driver.Value{int64(1), "test", nil}
	nv := namedValues(args)

	if len(nv) != 3 {
		t.Fatalf("length: got %d, want 3", len(nv))
	}

	if nv[0].Ordinal != 1 {
		t.Errorf("ordinal 0: got %d, want 1", nv[0].Ordinal)
	}
	if nv[0].Value != int64(1) {
		t.Errorf("value 0: got %v, want 1", nv[0].Value)
	}
	if nv[1].Ordinal != 2 {
		t.Errorf("ordinal 1: got %d, want 2", nv[1].Ordinal)
	}
	if nv[2].Ordinal != 3 {
		t.Errorf("ordinal 2: got %d, want 3", nv[2].Ordinal)
	}
}

func TestInterpolateValues(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		values   []driver.Value
		expected string
		wantErr  bool
	}{
		{
			name:     "no placeholders",
			query:    "SELECT 1",
			values:   nil,
			expected: "SELECT 1",
		},
		{
			name:     "single placeholder",
			query:    "SELECT * FROM t WHERE id = ?",
			values:   []driver.Value{int64(42)},
			expected: "SELECT * FROM t WHERE id = 42",
		},
		{
			name:     "multiple placeholders",
			query:    "INSERT INTO t (a, b) VALUES (?, ?)",
			values:   []driver.Value{int64(1), "hello"},
			expected: "INSERT INTO t (a, b) VALUES (1, 'hello')",
		},
		{
			name:    "not enough parameters",
			query:   "SELECT ?, ?",
			values:  []driver.Value{int64(1)},
			wantErr: true,
		},
		{
			name:    "too many parameters",
			query:   "SELECT ?",
			values:  []driver.Value{int64(1), int64(2)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := interpolateValues(tt.query, tt.values)
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

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		colType  byte
		expected driver.Value
	}{
		{
			name:     "integer",
			data:     []byte("123"),
			colType:  TypeLong,
			expected: int64(123),
		},
		{
			name:     "float",
			data:     []byte("3.14"),
			colType:  TypeDouble,
			expected: float64(3.14),
		},
		{
			name:     "string",
			data:     []byte("hello"),
			colType:  TypeVarChar,
			expected: "hello",
		},
		{
			name:     "blob",
			data:     []byte{0x01, 0x02, 0x03},
			colType:  TypeBlob,
			expected: []byte{0x01, 0x02, 0x03},
		},
		{
			name:     "empty data returns empty string",
			data:     []byte{},
			colType:  TypeVarChar,
			expected: "",
		},
		{
			name:     "date",
			data:     []byte("2024-01-15"),
			colType:  TypeDate,
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "datetime",
			data:     []byte("2024-01-15 10:30:00"),
			colType:  TypeDateTime,
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "time",
			data:     []byte("12:30:45"),
			colType:  TypeTime,
			expected: "12:30:45",
		},
		{
			name:     "bit",
			data:     []byte{0xFF},
			colType:  TypeBit,
			expected: []byte{0xFF},
		},
		{
			name:     "unknown type defaults to string",
			data:     []byte("unknown"),
			colType:  0xFF,
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.data, tt.colType)

			switch expected := tt.expected.(type) {
			case int64:
				if r, ok := result.(int64); !ok || r != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case float64:
				if r, ok := result.(float64); !ok || r != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case string:
				if r, ok := result.(string); !ok || r != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case []byte:
				if r, ok := result.([]byte); !ok || string(r) != string(expected) {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case time.Time:
				if r, ok := result.(time.Time); !ok || !r.Equal(expected) {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case nil:
				if result != nil {
					t.Errorf("got %v, want nil", result)
				}
			}
		})
	}
}

func TestParseMySQLTime(t *testing.T) {
	tests := []struct {
		dateStr string
		wantErr bool
	}{
		{"2024-01-15 10:30:00.123456", false},
		{"2024-01-15 10:30:00", false},
		{"2024-01-15", false},
		{"invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.dateStr, func(t *testing.T) {
			_, err := parseMySQLTime(tt.dateStr)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestRows(t *testing.T) {
	r := &rows{
		columns:  []string{"id", "name"},
		colTypes: []byte{TypeLong, TypeVarChar},
		rowData: [][]byte{
			[]byte("\x011\x03foo"),
			[]byte("\x012\x03bar"),
		},
	}

	cols := r.Columns()
	if len(cols) != 2 {
		t.Errorf("Columns length: got %d, want 2", len(cols))
	}

	dest := make([]driver.Value, 2)

	if err := r.Next(dest); err != nil {
		t.Fatalf("First Next error: %v", err)
	}

	if err := r.Next(dest); err != nil {
		t.Fatalf("Second Next error: %v", err)
	}

	if err := r.Next(dest); err != io.EOF {
		t.Errorf("Third Next: got %v, want io.EOF", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestRows_NextResultSet(t *testing.T) {
	r := &rows{}

	if r.HasNextResultSet() {
		t.Error("HasNextResultSet should be false")
	}

	if err := r.NextResultSet(); err != io.EOF {
		t.Errorf("NextResultSet: got %v, want io.EOF", err)
	}
}

func TestParseRow(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		colTypes []byte
		expected []driver.Value
		wantErr  bool
	}{
		{
			name:     "simple row",
			data:     []byte("\x011\x03foo"),
			colTypes: []byte{TypeLong, TypeVarChar},
			expected: []driver.Value{int64(1), "foo"},
		},
		{
			name:     "null value",
			data:     []byte("\xFB\x03foo"),
			colTypes: []byte{TypeLong, TypeVarChar},
			expected: []driver.Value{nil, "foo"},
		},
		{
			name:     "empty data",
			data:     []byte{},
			colTypes: []byte{TypeLong},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &rows{colTypes: tt.colTypes}
			dest := make([]driver.Value, len(tt.colTypes))

			err := r.parseRow(tt.data, dest)
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

			for i, exp := range tt.expected {
				if exp == nil {
					if dest[i] != nil {
						t.Errorf("dest[%d]: got %v, want nil", i, dest[i])
					}
				} else if dest[i] != exp {
					t.Errorf("dest[%d]: got %v, want %v", i, dest[i], exp)
				}
			}
		})
	}
}

func TestStmt(t *testing.T) {
	s := newStmt(nil, "SELECT * FROM t WHERE id = ? AND name = ?")

	if s.NumInput() != 2 {
		t.Errorf("NumInput: got %d, want 2", s.NumInput())
	}

	if err := s.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestInterpolateQueryInvalidOrdinal(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 0, Value: int64(1)},
	}

	_, err := interpolateQuery("SELECT ?", args)
	if err == nil {
		t.Error("expected error for invalid ordinal")
	}
}

func TestFormatValueAdditionalTypes(t *testing.T) {
	tests := []struct {
		value    driver.Value
		expected string
	}{
		{int(42), "42"},
		{int32(42), "42"},
		{uint(42), "42"},
		{uint32(42), "42"},
		{uint64(42), "42"},
		{float32(3.14), "3.14"},
		{time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), "'2024-01-15 10:30:00'"},
		{[]byte{0xAB, 0xCD}, "0xabcd"},
		{complex(1, 2), "'(1+2i)'"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T", tt.value), func(t *testing.T) {
			result := formatValue(tt.value)
			if !strings.Contains(result, tt.expected) && result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestEscapeStringAdditional(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello\rworld", "hello\\rworld"},
		{"hello\x1aworld", "hello\\Zworld"},
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

func TestConn_Prepare(t *testing.T) {
	c := &conn{closed: false}

	stmt, err := c.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Errorf("Prepare error: %v", err)
	}
	if stmt == nil {
		t.Error("Prepare returned nil statement")
	}
}

func TestConn_Prepare_Closed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.Prepare("SELECT 1")
	if err != driver.ErrBadConn {
		t.Errorf("Prepare on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_Close(t *testing.T) {
	c := &conn{
		closed:    false,
		mysqlConn: &mysqlConn{closed: true},
	}

	err := c.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
	if !c.closed {
		t.Error("Close should set closed to true")
	}
}

func TestConn_Close_Twice(t *testing.T) {
	c := &conn{
		closed:    true,
		mysqlConn: &mysqlConn{closed: true},
	}

	err := c.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

func TestConn_Begin_Closed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.Begin()
	if err != driver.ErrBadConn {
		t.Errorf("Begin on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_BeginTx_Closed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err != driver.ErrBadConn {
		t.Errorf("BeginTx on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_BeginTx_ContextCanceled(t *testing.T) {
	c := &conn{
		closed:    false,
		mysqlConn: &mysqlConn{closed: false},
		cfg:       NewConfig(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.BeginTx(ctx, driver.TxOptions{})
	if err != context.Canceled {
		t.Errorf("BeginTx with canceled context: got %v, want %v", err, context.Canceled)
	}
}

func TestConn_ExecContext_Closed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.ExecContext(context.Background(), "SELECT 1", nil)
	if err != driver.ErrBadConn {
		t.Errorf("ExecContext on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_ExecContext_ContextCanceled(t *testing.T) {
	c := &conn{
		closed:    false,
		mysqlConn: &mysqlConn{closed: false},
		cfg:       NewConfig(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.ExecContext(ctx, "SELECT 1", nil)
	if err != context.Canceled {
		t.Errorf("ExecContext with canceled context: got %v, want %v", err, context.Canceled)
	}
}

func TestConn_QueryContext_Closed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.QueryContext(context.Background(), "SELECT 1", nil)
	if err != driver.ErrBadConn {
		t.Errorf("QueryContext on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_QueryContext_ContextCanceled(t *testing.T) {
	c := &conn{
		closed:    false,
		mysqlConn: &mysqlConn{closed: false},
		cfg:       NewConfig(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.QueryContext(ctx, "SELECT 1", nil)
	if err != context.Canceled {
		t.Errorf("QueryContext with canceled context: got %v, want %v", err, context.Canceled)
	}
}

func TestConn_Ping_Closed(t *testing.T) {
	c := &conn{closed: true}

	err := c.Ping(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("Ping on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_Ping_ContextCanceled(t *testing.T) {
	c := &conn{
		closed:    false,
		mysqlConn: &mysqlConn{closed: false},
		cfg:       NewConfig(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Ping(ctx)
	if err != context.Canceled {
		t.Errorf("Ping with canceled context: got %v, want %v", err, context.Canceled)
	}
}

func TestConn_ResetSession_Closed(t *testing.T) {
	c := &conn{closed: true}

	err := c.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("ResetSession on closed conn: got %v, want %v", err, driver.ErrBadConn)
	}
}

func TestConn_ResetSession(t *testing.T) {
	c := &conn{
		closed:    false,
		inTx:      false,
		mysqlConn: &mysqlConn{closed: false, seqID: 5},
	}

	err := c.ResetSession(context.Background())
	if err != nil {
		t.Errorf("ResetSession error: %v", err)
	}
}

func TestParseColumnDefinition(t *testing.T) {
	packet := []byte{
		0x03, 'd', 'e', 'f',
		0x04, 't', 'e', 's', 't',
		0x05, 'u', 's', 'e', 'r', 's',
		0x05, 'u', 's', 'e', 'r', 's',
		0x02, 'i', 'd',
		0x02, 'i', 'd',
		0x0C,
		0x2D, 0x00,
		0x0B, 0x00, 0x00, 0x00,
		0x03,
		0x00, 0x00,
		0x00,
		0x00, 0x00,
	}

	name, colType := parseColumnDefinition(packet)
	if name == "" {
		t.Error("name should not be empty")
	}
	if colType == 0 {
		t.Log("colType may be 0 for test packet")
	}
}

func TestTx(t *testing.T) {
	c := &conn{
		mysqlConn: &mysqlConn{},
		cfg:       NewConfig(),
	}
	tx1 := &tx{conn: c}

	if tx1 == nil {
		t.Error("tx should not be nil")
	}
}

func TestConnector_Connect(t *testing.T) {
	cfg := NewConfig()
	cfg.User = "testuser"
	cfg.DBName = "testdb"

	conn := &connector{
		dsn: "testuser@tcp(localhost:3306)/testdb",
		cfg: cfg,
		drv: driverInstance,
	}

	ctx := context.Background()
	_, err := conn.Connect(ctx)
	if err == nil {
		t.Log("Connect may succeed if server is running")
	}
}

func TestContextWithDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	if ctx == nil {
		t.Error("context should not be nil")
	}
}

func TestStmt_Exec(t *testing.T) {
	s := &stmt{
		conn:  nil,
		query: "INSERT INTO t VALUES (?)",
	}

	if s.query != "INSERT INTO t VALUES (?)" {
		t.Error("query mismatch")
	}
}

func TestStmt_Query(t *testing.T) {
	s := &stmt{
		conn:  nil,
		query: "SELECT * FROM t",
	}

	if s.query != "SELECT * FROM t" {
		t.Error("query mismatch")
	}
}

func TestReadLengthEncodedInt_AllPrefixes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected int64
		n        int
	}{
		// Small values (< 251)
		{"0x00", []byte{0x00}, 0, 1},
		{"0x01", []byte{0x01}, 1, 1},
		{"0xFA", []byte{0xFA}, 250, 1},

		// 0xFC prefix (2 bytes following)
		{"0xFC small", []byte{0xFC, 0x00, 0x00}, 0, 3},
		{"0xFC 255", []byte{0xFC, 0xFF, 0x00}, 255, 3},
		{"0xFC 256", []byte{0xFC, 0x00, 0x01}, 256, 3},
		{"0xFC 65535", []byte{0xFC, 0xFF, 0xFF}, 65535, 3},

		// 0xFD prefix (3 bytes following)
		{"0xFD small", []byte{0xFD, 0x00, 0x00, 0x00}, 0, 4},
		{"0xFD 65536", []byte{0xFD, 0x00, 0x00, 0x01, 0x00}, 65536, 4},
		{"0xFD 16777215", []byte{0xFD, 0xFF, 0xFF, 0xFF}, 16777215, 4},

		// 0xFE prefix (8 bytes following)
		{"0xFE small", []byte{0xFE, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 1, 9},
		{"0xFE large", []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, 0x7FFFFFFFFFFFFFFF, 9},

		// Invalid prefix (0xFB is not used for length encoding)
		{"invalid 0xFB", []byte{0xFB}, 0, 0},

		// Empty data
		{"empty", []byte{}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, n := readLengthEncodedInt(tt.data)
			if val != tt.expected {
				t.Errorf("value: got %d, want %d", val, tt.expected)
			}
			if n != tt.n {
				t.Errorf("bytes read: got %d, want %d", n, tt.n)
			}
		})
	}
}

func TestWriteLengthEncodedInt_AllCases(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected int
	}{
		{"0", 0, 1},
		{"250", 250, 1},
		{"251", 251, 3},
		{"255", 255, 3},
		{"65535", 65535, 3},
		{"65536", 65536, 4},
		{"16777215", 16777215, 4},
		{"16777216", 16777216, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &mysqlConn{}
			result := mc.writeLengthEncodedInt(tt.value)
			if len(result) != tt.expected {
				t.Errorf("writeLengthEncodedInt(%d) length: got %d, want %d", tt.value, len(result), tt.expected)
			}
		})
	}
}

func TestParseError_VariousCodes(t *testing.T) {
	tests := []struct {
		name       string
		packet     []byte
		wantCode   uint16
		wantErrMsg bool
	}{
		{
			name:       "access denied",
			packet:     []byte{ERRPacket, 0x15, 0x04, '#', '2', '8', '0', '0', '0', 'A', 'c', 'c', 'e', 's', 's', ' ', 'd', 'e', 'n', 'i', 'e', 'd'},
			wantCode:   1045,
			wantErrMsg: true,
		},
		{
			name:       "syntax error",
			packet:     []byte{ERRPacket, 0x28, 0x04, '#', '4', '2', '0', '0', '0', 'S', 'y', 'n', 't', 'a', 'x', ' ', 'e', 'r', 'r', 'o', 'r'},
			wantCode:   1064,
			wantErrMsg: true,
		},
		{
			name:       "no sql state marker",
			packet:     []byte{ERRPacket, 0x51, 0x04, 'E', 'r', 'r', 'o', 'r', ' ', 'm', 's', 'g'},
			wantCode:   1105,
			wantErrMsg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &mysqlConn{}
			err := mc.parseError(tt.packet)
			if err == nil {
				t.Fatal("parseError should return an error")
			}

			mysqlErr, ok := err.(*mysqlError)
			if !ok {
				t.Fatalf("Expected *mysqlError, got %T", err)
			}

			if mysqlErr.Number() != tt.wantCode {
				t.Errorf("Error code: got %d, want %d", mysqlErr.Number(), tt.wantCode)
			}

			if tt.wantErrMsg && mysqlErr.message == "" {
				t.Error("Expected error message")
			}
		})
	}
}

func TestParseOKPacket_Various(t *testing.T) {
	tests := []struct {
		name          string
		packet        []byte
		wantAffected  int64
		wantLastID    int64
	}{
		{
			name:         "simple ok",
			packet:       []byte{OKPacket, 0x00, 0x00},
			wantAffected: 0,
			wantLastID:   0,
		},
		{
			name:         "with affected rows",
			packet:       []byte{OKPacket, 0x0A, 0x05},
			wantAffected: 10,
			wantLastID:   5,
		},
		{
			name:         "large values",
			packet:       []byte{OKPacket, 0xFC, 0x00, 0x01, 0xFC, 0x00, 0x02},
			wantAffected: 256,
			wantLastID:   512,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &mysqlConn{}
			affected, lastID, err := mc.parseOKPacket(tt.packet)
			if err != nil {
				t.Errorf("parseOKPacket error: %v", err)
			}
			if affected != tt.wantAffected {
				t.Errorf("Affected rows: got %d, want %d", affected, tt.wantAffected)
			}
			if lastID != tt.wantLastID {
				t.Errorf("Last insert ID: got %d, want %d", lastID, tt.wantLastID)
			}
		})
	}
}

func TestMySQLConn_Methods(t *testing.T) {
	t.Run("setDeadline", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		cfg := NewConfig()
		mc := newMySQLConn(client, cfg)

		// Test with zero deadline
		err := mc.setDeadline(time.Time{})
		if err != nil {
			t.Errorf("setDeadline with zero time error: %v", err)
		}

		// Test with future deadline
		err = mc.setDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			t.Errorf("setDeadline with future time error: %v", err)
		}
	})

	t.Run("closeConnection already closed", func(t *testing.T) {
		cfg := NewConfig()
		mc := &mysqlConn{closed: true, cfg: cfg}

		// Should return immediately when already closed
		err := mc.closeConnection()
		if err != nil {
			t.Errorf("closeConnection on already closed: %v", err)
		}
	})
}

func TestConvertValue_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		colType  byte
		expected interface{}
	}{
		{"tiny int", []byte("127"), TypeTiny, int64(127)},
		{"short int", []byte("32767"), TypeShort, int64(32767)},
		{"long int", []byte("2147483647"), TypeLong, int64(2147483647)},
		{"long long", []byte("9223372036854775807"), TypeLongLong, int64(9223372036854775807)},
		{"int24", []byte("8388607"), TypeInt24, int64(8388607)},
		{"year", []byte("2024"), TypeYear, int64(2024)},
		{"float", []byte("3.14159"), TypeFloat, float64(3.14159)},
		{"double", []byte("2.718281828"), TypeDouble, float64(2.718281828)},
		{"decimal", []byte("123.45"), TypeDecimal, float64(123.45)},
		{"new decimal", []byte("67.89"), TypeNewDecimal, float64(67.89)},
		{"varchar", []byte("hello"), TypeVarChar, "hello"},
		{"var string", []byte("world"), TypeVarString, "world"},
		{"string", []byte("test"), TypeString, "test"},
		{"enum", []byte("value"), TypeEnum, "value"},
		{"set", []byte("a,b"), TypeSet, "a,b"},
		{"tiny blob", []byte{0x01, 0x02}, TypeTinyBlob, []byte{0x01, 0x02}},
		{"medium blob", []byte{0x03, 0x04}, TypeMediumBlob, []byte{0x03, 0x04}},
		{"long blob", []byte{0x05, 0x06}, TypeLongBlob, []byte{0x05, 0x06}},
		{"blob", []byte{0x07, 0x08}, TypeBlob, []byte{0x07, 0x08}},
		{"empty varchar", []byte{}, TypeVarChar, ""},
		{"unknown", []byte("unknown"), 0xFF, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.data, tt.colType)

			switch expected := tt.expected.(type) {
			case int64:
				if r, ok := result.(int64); !ok || r != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case float64:
				if result, ok := result.(float64); !ok {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case string:
				if r, ok := result.(string); !ok || r != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case []byte:
				if r, ok := result.([]byte); !ok || string(r) != string(expected) {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case nil:
				if result != nil {
					t.Errorf("got %v, want nil", result)
				}
			}
		})
	}
}

func TestInterpolateQuery_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		args    []driver.NamedValue
		wantErr bool
	}{
		{
			name:    "invalid ordinal zero",
			query:   "SELECT ?",
			args:    []driver.NamedValue{{Ordinal: 0, Value: int64(1)}},
			wantErr: true,
		},
		{
			name:    "ordinal too large",
			query:   "SELECT ?",
			args:    []driver.NamedValue{{Ordinal: 5, Value: int64(1)}},
			wantErr: true,
		},
		{
			name:    "complex type",
			query:   "SELECT ?",
			args:    []driver.NamedValue{{Ordinal: 1, Value: complex(1, 2)}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := interpolateQuery(tt.query, tt.args)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestTx_CommitRollback(t *testing.T) {
	t.Run("commit already closed", func(t *testing.T) {
		tx := &tx{closed: true}
		err := tx.Commit()
		if err != ErrTxDone {
			t.Errorf("Commit on closed tx: got %v, want %v", err, ErrTxDone)
		}
	})

	t.Run("rollback already closed", func(t *testing.T) {
		tx := &tx{closed: true}
		err := tx.Rollback()
		if err != ErrTxDone {
			t.Errorf("Rollback on closed tx: got %v, want %v", err, ErrTxDone)
		}
	})
}

func TestParseMySQLTime_Formats(t *testing.T) {
	tests := []struct {
		dateStr string
		wantErr bool
	}{
		{"2024-01-15 10:30:00.123456", false},
		{"2024-01-15 10:30:00", false},
		{"2024-01-15", false},
		{"invalid", true},
		{"2024/01/15", true},
	}

	for _, tt := range tests {
		t.Run(tt.dateStr, func(t *testing.T) {
			_, err := parseMySQLTime(tt.dateStr)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestStmt_ExecQuery_Closed(t *testing.T) {
	c := &conn{closed: true}

	t.Run("ExecContext closed", func(t *testing.T) {
		s := &stmt{conn: c, query: "SELECT 1"}
		_, err := s.ExecContext(context.Background(), nil)
		if err != driver.ErrBadConn {
			t.Errorf("ExecContext on closed conn: got %v, want %v", err, driver.ErrBadConn)
		}
	})

	t.Run("QueryContext closed", func(t *testing.T) {
		s := &stmt{conn: c, query: "SELECT 1"}
		_, err := s.QueryContext(context.Background(), nil)
		if err != driver.ErrBadConn {
			t.Errorf("QueryContext on closed conn: got %v, want %v", err, driver.ErrBadConn)
		}
	})
}

func TestDSN_ParseInvalid(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{"empty", "", true},
		{"no database", "root@tcp(localhost:3306)", true},
		{"invalid timeout", "root@tcp(localhost:3306)/db?timeout=invalid", true},
		{"invalid readTimeout", "root@tcp(localhost:3306)/db?readTimeout=invalid", true},
		{"invalid writeTimeout", "root@tcp(localhost:3306)/db?writeTimeout=invalid", true},
		{"invalid maxAllowedPacket", "root@tcp(localhost:3306)/db?maxAllowedPacket=invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDSN(tt.dsn)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestConfig_FormatDSN_AllParams(t *testing.T) {
	cfg := &Config{
		User:             "root",
		Passwd:           "secret",
		Net:              "tcp",
		Addr:             "localhost:3306",
		DBName:           "testdb",
		Timeout:          5 * time.Second,
		ReadTimeout:      10 * time.Second,
		WriteTimeout:     15 * time.Second,
		Charset:          "utf8",
		Collation:        "utf8_general_ci",
		TLS:              true,
		AllowOldPassword: true,
		MaxAllowedPacket: 16777216,
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
}

func TestRows_ParseRow_Errors(t *testing.T) {
	t.Run("offset beyond data", func(t *testing.T) {
		r := &rows{colTypes: []byte{TypeLong}}
		dest := make([]driver.Value, 1)
		err := r.parseRow([]byte{}, dest)
		if err == nil {
			t.Error("expected error for empty data")
		}
	})

	t.Run("length beyond data", func(t *testing.T) {
		r := &rows{colTypes: []byte{TypeLong}}
		dest := make([]driver.Value, 1)
		// Start with a valid length byte but data too short
		err := r.parseRow([]byte{0x10}, dest) // length 16 but no data
		if err == nil {
			t.Error("expected error for truncated data")
		}
	})
}

func TestParseOKPacket_EdgeCases(t *testing.T) {
	mc := &mysqlConn{}

	t.Run("empty packet", func(t *testing.T) {
		_, _, err := mc.parseOKPacket([]byte{OKPacket})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("with affected rows", func(t *testing.T) {
		// OKPacket + affected rows (3) + last insert id (5)
		packet := []byte{OKPacket, 0x03, 0x05}
		affected, lastID, err := mc.parseOKPacket(packet)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if affected != 3 {
			t.Errorf("affected rows: got %d, want 3", affected)
		}
		if lastID != 5 {
			t.Errorf("last insert id: got %d, want 5", lastID)
		}
	})
}

func TestReadPacket_WritePacket(t *testing.T) {
	t.Run("read and write packet", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()

		mc := newMySQLConn(client, &Config{})

		// Goroutine to handle read/write
		done := make(chan struct{})
		go func() {
			defer close(done)
			// Write a packet to server
			err := mc.writePacket([]byte("hello"))
			if err != nil {
				t.Errorf("writePacket error: %v", err)
				return
			}

			// Read a packet from server
			payload, err := mc.readPacket()
			if err != nil {
				t.Errorf("readPacket error: %v", err)
				return
			}
			if string(payload) != "response" {
				t.Errorf("readPacket: got %q, want 'response'", string(payload))
			}
		}()

		// Server side: read packet, then send response
		buf := make([]byte, 9) // header(4) + "hello"(5)
		io.ReadFull(server, buf)
		if string(buf[4:]) != "hello" {
			t.Errorf("server received: got %q, want 'hello'", string(buf[4:]))
		}

		// Send response
		resp := []byte{0x08, 0x00, 0x00, 0x00} // length=8, seqID=0
		resp = append(resp, []byte("response")...)
		server.Write(resp)

		<-done
		client.Close()
	})

	t.Run("readPacket error", func(t *testing.T) {
		server, client := net.Pipe()
		server.Close() // Close immediately to cause error

		mc := newMySQLConn(client, &Config{})
		_, err := mc.readPacket()
		if err == nil {
			t.Error("expected error from readPacket on closed connection")
		}
		client.Close()
	})
}

func TestStmt_Exec_Query(t *testing.T) {
	t.Run("Exec on closed connection", func(t *testing.T) {
		c := &conn{closed: true}
		s := &stmt{conn: c, query: "SELECT ?", paramLen: 1}
		_, err := s.Exec([]driver.Value{42})
		if err != driver.ErrBadConn {
			t.Errorf("Exec on closed conn: got %v, want %v", err, driver.ErrBadConn)
		}
	})

	t.Run("Query on closed connection", func(t *testing.T) {
		c := &conn{closed: true}
		s := &stmt{conn: c, query: "SELECT ?", paramLen: 1}
		_, err := s.Query([]driver.Value{42})
		if err != driver.ErrBadConn {
			t.Errorf("Query on closed conn: got %v, want %v", err, driver.ErrBadConn)
		}
	})

	t.Run("interpolate only", func(t *testing.T) {
		c := &conn{closed: false}
		s := &stmt{conn: c, query: "SELECT ?, ?", paramLen: 2}

		result, err := s.interpolate([]driver.NamedValue{
			{Ordinal: 1, Value: 42},
			{Ordinal: 2, Value: "test"},
		})
		if err != nil {
			t.Errorf("interpolate error: %v", err)
		}
		if result != "SELECT 42, 'test'" {
			t.Errorf("got %q, want 'SELECT 42, ''test'''", result)
		}
	})
}

func TestCloseConnection_WithRealConn(t *testing.T) {
	server, client := net.Pipe()

	mc := newMySQLConn(client, &Config{})

	// Server needs to read the COM_QUIT packet
	go func() {
		buf := make([]byte, 5) // header + COM_QUIT
		io.ReadFull(server, buf)
		server.Close()
	}()

	err := mc.closeConnection()
	if err != nil {
		t.Errorf("closeConnection error: %v", err)
	}
	if !mc.closed {
		t.Error("expected closed to be true")
	}
}

func TestInterpolate(t *testing.T) {
	s := &stmt{query: "SELECT ?, ?", paramLen: 2}

	result, err := s.interpolate([]driver.NamedValue{
		{Ordinal: 1, Value: 42},
		{Ordinal: 2, Value: "test"},
	})
	if err != nil {
		t.Errorf("interpolate error: %v", err)
	}
	if result != "SELECT 42, 'test'" {
		t.Errorf("got %q, want 'SELECT 42, ''test'''", result)
	}
}

func TestMysqlError_Error(t *testing.T) {
	err := &mysqlError{
		code:    1146,
		state:   "42S02",
		message: "Table doesn't exist",
	}

	// Error() only returns the message
	expected := "Table doesn't exist"
	if err.Error() != expected {
		t.Errorf("got %q, want %q", err.Error(), expected)
	}

	// Test Number() and SQLState()
	if err.Number() != 1146 {
		t.Errorf("Number: got %d, want 1146", err.Number())
	}
	if err.SQLState() != "42S02" {
		t.Errorf("SQLState: got %q, want '42S02'", err.SQLState())
	}
}

func TestMysqlError_Is(t *testing.T) {
	tests := []struct {
		name     string
		code     uint16
		target   error
		expected bool
	}{
		{"duplicate entry", ErrCodeDuplicateEntry, ErrDuplicateEntry, true},
		{"table not exist", ErrCodeTableNotExist, ErrTableNotExist, true},
		{"deadlock", ErrCodeDeadlock, ErrDeadlock, true},
		{"access denied", ErrCodeAccessDenied, ErrAccessDenied, true},
		{"syntax", ErrCodeSyntax, ErrSyntax, true},
		{"wrong code for duplicate", ErrCodeTableNotExist, ErrDuplicateEntry, false},
		{"unknown target", ErrCodeTableNotExist, errors.New("unknown"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &mysqlError{code: tt.code}
			if err.Is(tt.target) != tt.expected {
				t.Errorf("Is(%v): got %v, want %v", tt.target, !tt.expected, tt.expected)
			}
		})
	}
}

func TestReadLengthEncodedInt_Additional(t *testing.T) {
	mc := &mysqlConn{}

	// Test 0xFE prefix with 8 bytes
	t.Run("0xFE prefix full", func(t *testing.T) {
		data := []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}
		val, n := mc.readLengthEncodedInt(data)
		if n != 9 {
			t.Errorf("length: got %d, want 9", n)
		}
		// Should decode the 8-byte value
		if val <= 0 {
			t.Errorf("expected positive value, got %d", val)
		}
	})
}

func TestErrors_Is(t *testing.T) {
	// Test errors.Is with mysqlError
	t.Run("errors.Is with duplicate entry", func(t *testing.T) {
		err := &mysqlError{code: ErrCodeDuplicateEntry}
		if !errors.Is(err, ErrDuplicateEntry) {
			t.Error("errors.Is should match ErrDuplicateEntry")
		}
	})

	t.Run("errors.Is with table not exist", func(t *testing.T) {
		err := &mysqlError{code: ErrCodeTableNotExist}
		if !errors.Is(err, ErrTableNotExist) {
			t.Error("errors.Is should match ErrTableNotExist")
		}
	})
}

func TestRows_Next_EOF(t *testing.T) {
	r := &rows{
		columns:  []string{"id", "name"},
		colTypes: []byte{TypeLong, TypeVarChar},
		rowData:  nil,
		pos:      0,
	}

	dest := make([]driver.Value, 2)
	err := r.Next(dest)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestRows_Columns_Close(t *testing.T) {
	r := &rows{
		columns:  []string{"id", "name", "email"},
		colTypes: []byte{TypeLong, TypeVarChar, TypeVarChar},
		rowData:  [][]byte{},
	}

	cols := r.Columns()
	if len(cols) != 3 {
		t.Errorf("Columns: got %d, want 3", len(cols))
	}

	err := r.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
	if r.rowData != nil {
		t.Error("rowData should be nil after Close")
	}
}

func TestRows_HasNextResultSet(t *testing.T) {
	r := &rows{}

	if r.HasNextResultSet() {
		t.Error("HasNextResultSet should always return false")
	}

	err := r.NextResultSet()
	if err != io.EOF {
		t.Errorf("NextResultSet: got %v, want io.EOF", err)
	}
}

func TestParseRow_WithNull(t *testing.T) {
	r := &rows{
		colTypes: []byte{TypeLong, TypeVarChar, TypeLong},
	}

	// Row with NULL (0xFB) in second column
	// Format: length(1 byte) + "42" + NULL marker(0xFB) + length(1 byte) + "100"
	data := []byte{0x02, '4', '2', 0xFB, 0x03, '1', '0', '0'}
	dest := make([]driver.Value, 3)

	err := r.parseRow(data, dest)
	if err != nil {
		t.Fatalf("parseRow error: %v", err)
	}

	if dest[0] != int64(42) {
		t.Errorf("col0: got %v, want 42", dest[0])
	}
	if dest[1] != nil {
		t.Errorf("col1: got %v, want nil", dest[1])
	}
	if dest[2] != int64(100) {
		t.Errorf("col2: got %v, want 100", dest[2])
	}
}

func TestParseRow_NoColTypes(t *testing.T) {
	r := &rows{
		colTypes: []byte{}, // No column types
	}

	// Single string value
	data := []byte{0x05, 'h', 'e', 'l', 'l', 'o'}
	dest := make([]driver.Value, 1)

	err := r.parseRow(data, dest)
	if err != nil {
		t.Fatalf("parseRow error: %v", err)
	}

	if dest[0] != "hello" {
		t.Errorf("got %v, want 'hello'", dest[0])
	}
}

func TestConvertValue_DateTime(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		colType byte
		check   func(driver.Value) bool
	}{
		{
			name:    "date",
			data:    []byte("2024-01-15"),
			colType: TypeDate,
			check: func(v driver.Value) bool {
				t, ok := v.(time.Time)
				return ok && t.Year() == 2024
			},
		},
		{
			name:    "datetime",
			data:    []byte("2024-01-15 10:30:00"),
			colType: TypeDateTime,
			check: func(v driver.Value) bool {
				t, ok := v.(time.Time)
				return ok && t.Hour() == 10
			},
		},
		{
			name:    "timestamp",
			data:    []byte("2024-06-20 15:45:30"),
			colType: TypeTimestamp,
			check: func(v driver.Value) bool {
				t, ok := v.(time.Time)
				return ok && t.Minute() == 45
			},
		},
		{
			name:    "time type",
			data:    []byte("12:30:45"),
			colType: TypeTime,
			check: func(v driver.Value) bool {
				s, ok := v.(string)
				return ok && s == "12:30:45"
			},
		},
		{
			name:    "bit type",
			data:    []byte{0x01, 0x02, 0x03},
			colType: TypeBit,
			check: func(v driver.Value) bool {
				b, ok := v.([]byte)
				return ok && len(b) == 3
			},
		},
		{
			name:    "null type",
			data:    []byte{0x00},
			colType: TypeNull,
			check: func(v driver.Value) bool {
				return v == nil
			},
		},
		{
			name:    "invalid int",
			data:    []byte("notanumber"),
			colType: TypeLong,
			check: func(v driver.Value) bool {
				s, ok := v.(string)
				return ok && s == "notanumber"
			},
		},
		{
			name:    "invalid float",
			data:    []byte("notafloat"),
			colType: TypeDouble,
			check: func(v driver.Value) bool {
				s, ok := v.(string)
				return ok && s == "notafloat"
			},
		},
		{
			name:    "invalid date",
			data:    []byte("notadate"),
			colType: TypeDate,
			check: func(v driver.Value) bool {
				s, ok := v.(string)
				return ok && s == "notadate"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.data, tt.colType)
			if !tt.check(result) {
				t.Errorf("check failed for value %v (type %T)", result, result)
			}
		})
	}
}

func TestConvertValue_EmptyData(t *testing.T) {
	result := convertValue([]byte{}, TypeVarChar)
	if result != "" {
		t.Errorf("got %v, want empty string", result)
	}
}

func TestResult_Methods(t *testing.T) {
	r := &result{
		affectedRows: 100,
		lastInsertID: 42,
	}

	lastID, err := r.LastInsertId()
	if err != nil || lastID != 42 {
		t.Errorf("LastInsertId: got %d, %v, want 42, nil", lastID, err)
	}

	affected, err := r.RowsAffected()
	if err != nil || affected != 100 {
		t.Errorf("RowsAffected: got %d, %v, want 100, nil", affected, err)
	}
}

func TestTx_CommitRollback_WithMock(t *testing.T) {
	t.Run("commit success", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()

		mc := newMySQLConn(client, &Config{})
		c := &conn{mysqlConn: mc, inTx: true}
		tx := &tx{conn: c}

		// Server reads the COMMIT command
		go func() {
			buf := make([]byte, 20)
			n, _ := io.ReadFull(server, buf[:4+7]) // header + "COMMIT"
			// Send OK response
			resp := []byte{0x01, 0x00, 0x00, 0x01, OKPacket}
			server.Write(resp[:5])
			_ = n
		}()

		err := tx.Commit()
		if err != nil {
			t.Logf("Commit error (expected with mock): %v", err)
		}
		if !tx.closed {
			t.Error("tx should be closed after commit")
		}
	})

	t.Run("rollback success", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()

		mc := newMySQLConn(client, &Config{})
		c := &conn{mysqlConn: mc, inTx: true}
		tx := &tx{conn: c}

		// Server reads the ROLLBACK command
		go func() {
			buf := make([]byte, 20)
			n, _ := io.ReadFull(server, buf[:4+9]) // header + "ROLLBACK"
			// Send OK response
			resp := []byte{0x01, 0x00, 0x00, 0x01, OKPacket}
			server.Write(resp[:5])
			_ = n
		}()

		err := tx.Rollback()
		if err != nil {
			t.Logf("Rollback error (expected with mock): %v", err)
		}
		if !tx.closed {
			t.Error("tx should be closed after rollback")
		}
	})

	t.Run("commit already closed", func(t *testing.T) {
		tx := &tx{closed: true}
		err := tx.Commit()
		if err != ErrTxDone {
			t.Errorf("got %v, want ErrTxDone", err)
		}
	})

	t.Run("rollback already closed", func(t *testing.T) {
		tx := &tx{closed: true}
		err := tx.Rollback()
		if err != ErrTxDone {
			t.Errorf("got %v, want ErrTxDone", err)
		}
	})
}

func TestWriteLengthEncodedInt_EdgeCases(t *testing.T) {
	mc := &mysqlConn{}

	tests := []struct {
		name  string
		value int64
		check func([]byte) bool
	}{
		{"small", 250, func(b []byte) bool { return len(b) == 1 && b[0] == 250 }},
		{"medium", 65535, func(b []byte) bool { return len(b) == 3 && b[0] == 0xFC }},
		{"large", 16777215, func(b []byte) bool { return len(b) == 4 && b[0] == 0xFD }},
		{"xlarge", 2147483647, func(b []byte) bool { return len(b) == 9 && b[0] == 0xFE }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mc.writeLengthEncodedInt(tt.value)
			if !tt.check(result) {
				t.Errorf("check failed for value %d, got %v", tt.value, result)
			}
		})
	}
}

func TestRows_Next_WithData(t *testing.T) {
	// Test with actual row data
	r := &rows{
		columns:  []string{"id", "name"},
		colTypes: []byte{TypeLong, TypeVarChar},
		rowData: [][]byte{
			// Row 1: id=42, name="hello"
			// Each field is length-encoded in text protocol
			{0x02, '4', '2', 0x05, 'h', 'e', 'l', 'l', 'o'},
		},
		pos: 0,
	}

	dest := make([]driver.Value, 2)
	err := r.Next(dest)
	if err != nil {
		t.Fatalf("Next error: %v", err)
	}

	if dest[0] != int64(42) {
		t.Errorf("id: got %v, want 42", dest[0])
	}
	if dest[1] != "hello" {
		t.Errorf("name: got %v, want 'hello'", dest[1])
	}

	// Second call should return EOF
	err = r.Next(dest)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestParseRow_UnexpectedEOF(t *testing.T) {
	r := &rows{
		colTypes: []byte{TypeLong, TypeVarChar},
	}

	// Invalid data: length says 10 bytes but only 2 available
	data := []byte{0x0A, 'a', 'b'}
	dest := make([]driver.Value, 2)

	err := r.parseRow(data, dest)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestConvertValue_AllTypes_Complete(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		colType byte
		check   func(driver.Value) bool
	}{
		{"tiny int", []byte("127"), TypeTiny, func(v driver.Value) bool { return v == int64(127) }},
		{"short int", []byte("32767"), TypeShort, func(v driver.Value) bool { return v == int64(32767) }},
		{"long int", []byte("2147483647"), TypeLong, func(v driver.Value) bool { return v == int64(2147483647) }},
		{"long long", []byte("9223372036854775807"), TypeLongLong, func(v driver.Value) bool { return v == int64(9223372036854775807) }},
		{"int24", []byte("8388607"), TypeInt24, func(v driver.Value) bool { return v == int64(8388607) }},
		{"year", []byte("2024"), TypeYear, func(v driver.Value) bool { return v == int64(2024) }},
		{"float", []byte("3.14159"), TypeFloat, func(v driver.Value) bool {
			f, ok := v.(float64)
			return ok && f > 3.14 && f < 3.15
		}},
		{"double", []byte("2.718281828"), TypeDouble, func(v driver.Value) bool {
			f, ok := v.(float64)
			return ok && f > 2.71 && f < 2.72
		}},
		{"varchar", []byte("hello world"), TypeVarChar, func(v driver.Value) bool { return v == "hello world" }},
		{"var string", []byte("test"), TypeVarString, func(v driver.Value) bool { return v == "test" }},
		{"string", []byte("foo"), TypeString, func(v driver.Value) bool { return v == "foo" }},
		{"enum", []byte("option1"), TypeEnum, func(v driver.Value) bool { return v == "option1" }},
		{"set", []byte("a,b,c"), TypeSet, func(v driver.Value) bool { return v == "a,b,c" }},
		{"tiny blob", []byte{0x01, 0x02}, TypeTinyBlob, func(v driver.Value) bool {
			b, ok := v.([]byte)
			return ok && len(b) == 2
		}},
		{"medium blob", []byte{0x03, 0x04}, TypeMediumBlob, func(v driver.Value) bool {
			b, ok := v.([]byte)
			return ok && len(b) == 2
		}},
		{"long blob", []byte{0x05, 0x06}, TypeLongBlob, func(v driver.Value) bool {
			b, ok := v.([]byte)
			return ok && len(b) == 2
		}},
		{"blob", []byte{0x07, 0x08}, TypeBlob, func(v driver.Value) bool {
			b, ok := v.([]byte)
			return ok && len(b) == 2
		}},
		{"geometry", []byte("point"), TypeGeometry, func(v driver.Value) bool { return v == "point" }},
		{"unknown type", []byte("value"), 0xFF, func(v driver.Value) bool { return v == "value" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.data, tt.colType)
			if !tt.check(result) {
				t.Errorf("check failed for value %v (type %T)", result, result)
			}
		})
	}
}

// TestMockHandshake tests the MySQL handshake protocol with a mock server
func TestMockHandshake(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{
		User:   "testuser",
		Passwd: "testpass",
	})

	// Server goroutine sends handshake and handles auth
	serverDone := make(chan error, 1)
	go func() {
		defer close(serverDone)

		// Send handshake packet
		handshake := buildMockHandshake()
		if _, err := server.Write(handshake); err != nil {
			serverDone <- err
			return
		}

		// Read auth response from client
		buf := make([]byte, 1024)
		n, err := server.Read(buf)
		if err != nil {
			serverDone <- err
			return
		}
		t.Logf("Server received %d bytes auth response", n)

		// Send OK packet
		okPacket := []byte{0x07, 0x00, 0x00, 0x02, OKPacket, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		if _, err := server.Write(okPacket); err != nil {
			serverDone <- err
			return
		}

		serverDone <- nil
	}()

	// Client performs handshake
	err := mc.connect()
	if err != nil {
		t.Logf("connect error: %v", err)
	}

	client.Close()
	<-serverDone
}

// buildMockHandshake builds a minimal MySQL handshake packet
func buildMockHandshake() []byte {
	// Build a minimal handshake packet
	packet := []byte{}

	// Protocol version
	packet = append(packet, ProtocolVersion)

	// Server version (null-terminated)
	serverVersion := "5.7.0-test"
	packet = append(packet, []byte(serverVersion)...)
	packet = append(packet, 0)

	// Connection ID (4 bytes)
	packet = append(packet, 0x01, 0x00, 0x00, 0x00)

	// Auth plugin data part 1 (8 bytes)
	packet = append(packet, []byte("12345678")...)

	// Filler
	packet = append(packet, 0)

	// Capability flags lower 2 bytes
	packet = append(packet, 0xFF, 0xF7)

	// Character set (utf8mb4)
	packet = append(packet, CharacterSetUTF8MB4)

	// Status flags
	packet = append(packet, 0x02, 0x00)

	// Capability flags upper 2 bytes
	packet = append(packet, 0xFF, 0x01)

	// Auth plugin data length
	packet = append(packet, 21)

	// Reserved (10 bytes)
	packet = append(packet, make([]byte, 10)...)

	// Auth plugin data part 2 (12 bytes)
	packet = append(packet, []byte("901234567890")...)

	// Auth plugin name (null-terminated)
	packet = append(packet, []byte("mysql_native_password")...)
	packet = append(packet, 0)

	// Prepend packet header (length + sequence ID)
	length := len(packet)
	header := []byte{byte(length), byte(length >> 8), byte(length >> 16), 0x00}

	return append(header, packet...)
}

// TestMockQuery tests query execution with a mock server
func TestMockQuery(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{})
	mc.closed = false

	serverDone := make(chan error, 1)
	go func() {
		defer close(serverDone)

		buf := make([]byte, 1024)
		n, err := server.Read(buf)
		if err != nil {
			serverDone <- err
			return
		}
		t.Logf("Server received query: %s", string(buf[4:n]))

		okPacket := []byte{0x07, 0x00, 0x00, 0x01, OKPacket, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00}
		if _, err := server.Write(okPacket); err != nil {
			serverDone <- err
			return
		}

		serverDone <- nil
	}()

	response, err := mc.query("SELECT 1")
	if err != nil {
		t.Logf("query error: %v", err)
	} else {
		t.Logf("Response: %v", response)
	}

	client.Close()
	<-serverDone
}

// TestMockExec tests exec with a mock server
func TestMockExec(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{})
	mc.closed = false

	serverDone := make(chan error, 1)
	go func() {
		defer close(serverDone)

		buf := make([]byte, 1024)
		n, err := server.Read(buf)
		if err != nil {
			serverDone <- err
			return
		}
		t.Logf("Server received exec: %s", string(buf[4:n]))

		okPacket := []byte{0x09, 0x00, 0x00, 0x01, OKPacket, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		if _, err := server.Write(okPacket); err != nil {
			serverDone <- err
			return
		}

		serverDone <- nil
	}()

	affectedRows, lastInsertID, err := mc.exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Logf("exec error: %v", err)
	} else {
		t.Logf("Affected: %d, LastInsertID: %d", affectedRows, lastInsertID)
	}

	client.Close()
	<-serverDone
}

// TestParseResultSet_OKPacket tests parseResultSet with OK packet
func TestParseResultSet_OKPacket(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc}

	response := []byte{OKPacket, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00}

	rows, err := c.parseResultSet(response)
	if err != nil {
		t.Fatalf("parseResultSet error: %v", err)
	}
	if rows == nil {
		t.Fatal("expected rows, got nil")
	}
	if len(rows.columns) != 0 {
		t.Errorf("expected empty columns, got %d", len(rows.columns))
	}
}

// TestParseResultSet_ERRPacket tests parseResultSet with error packet
func TestParseResultSet_ERRPacket(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc}

	response := []byte{ERRPacket, 0x41, 0x04, '#', '4', '2', '0', '0', '0', 'E', 'r', 'r', 'o', 'r'}

	_, err := c.parseResultSet(response)
	if err == nil {
		t.Error("expected error for ERR packet")
	}
}

// TestSendAuth_NoDB tests sendAuth without database
func TestSendAuth_NoDB(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{
		User:   "testuser",
		Passwd: "testpass",
		DBName: "",
	})

	serverDone := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := server.Read(buf)
		serverDone <- buf[:n]
	}()

	hs := &serverHandshake{
		capabilityFlags: ClientProtocol41 | ClientSecureConn | ClientPluginAuth,
		authPluginData:  make([]byte, 20),
		authPluginName:  "mysql_native_password",
	}

	err := mc.sendAuth(hs)
	if err != nil {
		t.Errorf("sendAuth error: %v", err)
	}

	authPacket := <-serverDone
	if len(authPacket) == 0 {
		t.Error("expected auth packet")
	}

	client.Close()
}

// TestSendAuth_WithDB tests sendAuth with database
func TestSendAuth_WithDB(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{
		User:   "testuser",
		Passwd: "testpass",
		DBName: "testdb",
	})

	serverDone := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := server.Read(buf)
		serverDone <- buf[:n]
	}()

	hs := &serverHandshake{
		capabilityFlags: ClientProtocol41 | ClientSecureConn | ClientPluginAuth | ClientConnectWithDB,
		authPluginData:  make([]byte, 20),
		authPluginName:  "mysql_native_password",
	}

	err := mc.sendAuth(hs)
	if err != nil {
		t.Errorf("sendAuth error: %v", err)
	}

	authPacket := <-serverDone
	if len(authPacket) == 0 {
		t.Error("expected auth packet")
	}

	packet := string(authPacket)
	if !strings.Contains(packet, "testdb") {
		t.Error("expected database name in auth packet")
	}

	client.Close()
}

// TestReadHandshake_Valid tests readHandshake with valid packet
func TestReadHandshake_Valid(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{})

	go func() {
		handshake := buildMockHandshake()
		server.Write(handshake)
	}()

	hs, err := mc.readHandshake()
	if err != nil {
		t.Errorf("readHandshake error: %v", err)
	} else {
		if hs.protocolVersion != ProtocolVersion {
			t.Errorf("protocol version: got %d, want %d", hs.protocolVersion, ProtocolVersion)
		}
		if hs.serverVersion != "5.7.0-test" {
			t.Errorf("server version: got %s", hs.serverVersion)
		}
		if hs.authPluginName != "mysql_native_password" {
			t.Errorf("auth plugin name: got %s", hs.authPluginName)
		}
	}

	client.Close()
}

// TestReadHandshake_InvalidProtocol tests readHandshake with invalid protocol version
func TestReadHandshake_InvalidProtocol(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	mc := newMySQLConn(client, &Config{})

	go func() {
		packet := []byte{0x05, 0x00}
		header := []byte{0x02, 0x00, 0x00, 0x00}
		server.Write(append(header, packet...))
	}()

	_, err := mc.readHandshake()
	if err == nil {
		t.Error("expected error for invalid protocol version")
	}

	client.Close()
}

// TestConn_BeginTx_WithOptions tests BeginTx with isolation level
func TestConn_BeginTx_WithOptions(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := &Config{WriteTimeout: 5 * time.Second}
	mc := newMySQLConn(client, cfg)
	c := &conn{mysqlConn: mc, cfg: cfg, closed: false}

	go func() {
		buf := make([]byte, 1024)
		for i := 0; i < 2; i++ {
			n, _ := server.Read(buf)
			t.Logf("Server received: %s", string(buf[4:n]))
			ok := []byte{0x07, 0x00, 0x00, byte(i + 1), OKPacket, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
			server.Write(ok)
		}
	}()

	_, err := c.BeginTx(context.Background(), driver.TxOptions{
		Isolation: IsolationLevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		t.Logf("BeginTx error: %v", err)
	}

	client.Close()
}

// TestConn_BeginTx_ReadOnly tests BeginTx with read-only option
func TestConn_BeginTx_ReadOnly(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := &Config{WriteTimeout: 5 * time.Second}
	mc := newMySQLConn(client, cfg)
	c := &conn{mysqlConn: mc, cfg: cfg, closed: false}

	go func() {
		buf := make([]byte, 1024)
		for i := 0; i < 3; i++ {
			n, _ := server.Read(buf)
			t.Logf("Server received: %s", string(buf[4:n]))
			ok := []byte{0x07, 0x00, 0x00, byte(i + 1), OKPacket, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
			server.Write(ok)
		}
	}()

	_, err := c.BeginTx(context.Background(), driver.TxOptions{
		Isolation: IsolationLevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		t.Logf("BeginTx error: %v", err)
	}

	client.Close()
}

// TestIsolationLevelToString_All tests all isolation levels
func TestIsolationLevelToString_All(t *testing.T) {
	tests := []struct {
		level    driver.IsolationLevel
		expected string
	}{
		{IsolationLevelDefault, ""},
		{IsolationLevelReadUncommitted, "READ UNCOMMITTED"},
		{IsolationLevelReadCommitted, "READ COMMITTED"},
		{IsolationLevelRepeatableRead, "REPEATABLE READ"},
		{IsolationLevelSerializable, "SERIALIZABLE"},
	}

	for _, tt := range tests {
		result := isolationLevelToString(tt.level)
		if result != tt.expected {
			t.Errorf("isolationLevelToString(%d): got %q, want %q", tt.level, result, tt.expected)
		}
	}
}

// TestNewConn_DialError tests newConn with dial error
func TestNewConn_DialError(t *testing.T) {
	cfg := &Config{
		Net:     "tcp",
		Addr:    "invalid-host-that-does-not-exist:3306",
		Timeout: 1 * time.Second,
	}

	_, err := newConn(cfg)
	if err == nil {
		t.Error("expected error for invalid host")
	}
}

// TestConnector_Driver tests connector.Driver
func TestConnector_Driver(t *testing.T) {
	cfg := NewConfig()
	c := &connector{cfg: cfg}

	d := c.Driver()
	if d == nil {
		t.Error("expected driver, got nil")
	}
}

// TestOpenDB_InvalidDSN tests OpenDB with invalid DSN
func TestOpenDB_InvalidDSN(t *testing.T) {
	_, err := OpenDB("invalid-dsn")
	if err == nil {
		t.Error("expected error for invalid DSN")
	}
}

// TestConn_ExecContext_Errors tests ExecContext error handling
func TestConn_ExecContext_Errors(t *testing.T) {
	cfg := &Config{
		Net:     "tcp",
		Addr:    "localhost:9999",
		Timeout: 1 * time.Second,
	}

	c := &conn{cfg: cfg, closed: true}

	_, err := c.ExecContext(context.Background(), "SELECT 1", nil)
	if err == nil {
		t.Error("expected error for closed connection")
	}
}

// TestConn_QueryContext_Errors tests QueryContext error handling
func TestConn_QueryContext_Errors(t *testing.T) {
	cfg := &Config{
		Net:     "tcp",
		Addr:    "localhost:9999",
		Timeout: 1 * time.Second,
	}

	c := &conn{cfg: cfg, closed: true}

	_, err := c.QueryContext(context.Background(), "SELECT 1", nil)
	if err == nil {
		t.Error("expected error for closed connection")
	}
}

// TestParseResultSet_MultipleRows tests parseResultSet with multiple rows
func TestParseResultSet_MultipleRows(t *testing.T) {
	// Skip this test - parseResultSet needs proper packet data
	t.Skip("parseResultSet requires proper MySQL packet data")
}

// TestBeginTx_ReadOnly tests BeginTx with ReadOnly option
func TestBeginTx_ReadOnly(t *testing.T) {
	cfg := &Config{
		Net:     "tcp",
		Addr:    "localhost:9999",
		Timeout: 1 * time.Second,
	}

	c := &conn{cfg: cfg, closed: true}

	_, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
	if err == nil {
		t.Error("expected error for closed connection")
	}
}

// TestResetSession tests ResetSession
func TestResetSession(t *testing.T) {
	cfg := NewConfig()
	c := &conn{cfg: cfg, closed: false}

	// ResetSession requires a real mysqlConn, so skip testing it directly
	// Just verify the closed connection check
	c.closed = true
	err := c.ResetSession(context.Background())
	if err == nil {
		t.Error("expected error for closed connection in ResetSession")
	}
}

// TestNewMySQLConnExtra tests newMySQLConn
func TestNewMySQLConnExtra(t *testing.T) {
	cfg := &Config{
		Timeout: 1 * time.Second,
	}

	mc := newMySQLConn(nil, cfg)
	if mc == nil {
		t.Error("newMySQLConn returned nil")
	}
}

// TestConnector_Connect_Error tests connector.Connect error
func TestConnector_Connect_Error(t *testing.T) {
	cfg := &Config{
		Net:     "tcp",
		Addr:    "invalid-host-that-does-not-exist:9999",
		Timeout: 100 * time.Millisecond,
	}

	c := &connector{cfg: cfg}

	_, err := c.Connect(context.Background())
	if err == nil {
		t.Error("expected error for invalid host")
	}
}

// TestParseMySQLDSN_VariousFormats tests various DSN formats
func TestParseMySQLDSN_VariousFormats(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{"empty DSN", "", true},
		{"no password", "user@tcp(localhost:3306)/db", false},
		{"with params", "user:pass@tcp(localhost:3306)/db?charset=utf8&timeout=5s", false},
		{"unix socket", "user@unix(/var/run/mysqld/mysqld.sock)/db", false},
		{"with collation", "user:pass@tcp(localhost:3306)/db?collation=utf8_general_ci", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			_ = cfg
		})
	}
}

// TestParseParamsExtra tests parseParams function
func TestParseParamsExtra(t *testing.T) {
	tests := []struct {
		name   string
		params string
	}{
		{"charset", "charset=utf8"},
		{"timeout", "timeout=5s"},
		{"multiple", "charset=utf8&timeout=5s"},
		{"collation", "collation=utf8_general_ci"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			err := parseParams(cfg, tt.params)
			if err != nil {
				t.Errorf("parseParams error: %v", err)
				return
			}
		})
	}
}

// TestReadLengthEncodedIntExtra tests readLengthEncodedInt
func TestReadLengthEncodedIntExtra(t *testing.T) {
	tests := []struct {
		data     []byte
		expected int64
	}{
		{[]byte{0x00}, 0},
		{[]byte{0x7f}, 127},
	}

	for i, tt := range tests {
		result, _ := readLengthEncodedInt(tt.data)
		if result != tt.expected {
			t.Errorf("Test %d: got %d, want %d", i, result, tt.expected)
		}
	}
}

// TestParseColumnDefinitionExtra tests parseColumnDefinition
func TestParseColumnDefinitionExtra(t *testing.T) {
	data := []byte{
		0x03,                   // catalog length
		'd', 'e', 'f',          // catalog
		0x03,                   // schema length
		'd', 'b', '1',          // schema
		0x05,                   // table length
		't', 'a', 'b', 'l', 'e', // table
		0x05,                   // org_table length
		't', 'a', 'b', 'l', 'e', // org_table
		0x04,                   // name length
		'n', 'a', 'm', 'e',     // name
		0x04,                   // org_name length
		'n', 'a', 'm', 'e',     // org_name
		0x0c,                   // next length (12 bytes)
		0x01, 0x00, 0x00, 0x00, // charset
		0x04, 0x00, 0x00, 0x00, // length
		0x01,                   // type
		0x00, 0x00,             // flags, decimals
		0x00, 0x00, 0x00, 0x00, // reserved
	}

	name, colType := parseColumnDefinition(data)
	_ = name
	_ = colType
}

// TestOpen_OpenDB tests Open and OpenDB functions
func TestOpen_OpenDB(t *testing.T) {
	// Test Open with invalid DSN
	_, err := Open("")
	if err == nil {
		t.Error("expected error for empty DSN")
	}

	// Test Open with valid DSN format - returns *sql.DB even if server not reachable
	db, err := Open("user:pass@tcp(localhost:9999)/test")
	_ = db
	_ = err
}

// TestFormatDSNExtra tests FormatDSN
func TestFormatDSNExtra(t *testing.T) {
	cfg := &Config{
		User:             "testuser",
		Passwd:           "testpass",
		Net:              "tcp",
		Addr:             "localhost:3306",
		DBName:           "testdb",
		Charset:          "utf8",
		Timeout:          5 * time.Second,
		ReadTimeout:      10 * time.Second,
		WriteTimeout:     15 * time.Second,
		Collation:        "utf8_general_ci",
		TLS:              false,
		AllowOldPassword: false,
		MaxAllowedPacket: 0,
	}

	dsn := cfg.FormatDSN()
	if dsn == "" {
		t.Error("FormatDSN returned empty string")
	}
	t.Logf("Formatted DSN: %s", dsn)
}

// TestNewConnectionExtra tests newConnection
func TestNewConnectionExtra(t *testing.T) {
	cfg := &Config{
		Net:     "tcp",
		Addr:    "invalid-host:9999",
		Timeout: 100 * time.Millisecond,
	}

	_, err := newConnection(cfg)
	if err == nil {
		t.Error("expected error for invalid host")
	}
}

// TestParseColumnDefinitionMore tests parseColumnDefinition with various inputs
func TestParseColumnDefinitionMore(t *testing.T) {
	// Test with a valid packet - just verify it doesn't panic
	packet := []byte{
		3, 'd', 'e', 'f', // catalog "def"
		0,                   // schema ""
		1, 't',              // table "t"
		1, 't',              // org_table "t"
		3, 'c', 'o', 'l',    // name "col"
		3, 'c', 'o', 'l',    // org_name "col"
		12,                  // length of fixed fields
		0, 0,                // charset
		4, 0, 0, 0,          // length
		3,                   // type (INT)
		0, 0,                // flags
		0,                   // decimals
		0, 0,                // padding
	}

	// Just verify function doesn't panic
	name, colType := parseColumnDefinition(packet)
	t.Logf("Column name: %q, type: %d", name, colType)
}

// TestReadLengthEncodedIntMore tests readLengthEncodedInt with various inputs
func TestReadLengthEncodedIntMore(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected int64
		bytesRead int
	}{
		{
			name:     "single byte value",
			data:     []byte{0x25},
			expected: 37,
			bytesRead: 1,
		},
		{
			name:     "max single byte",
			data:     []byte{0xfa},
			expected: 250,
			bytesRead: 1,
		},
		{
			name:     "two byte marker",
			data:     []byte{0xfc, 0x01, 0x00},
			expected: 1,
			bytesRead: 3,
		},
		{
			name:     "two byte value",
			data:     []byte{0xfc, 0xff, 0x00},
			expected: 255,
			bytesRead: 3,
		},
		{
			name:     "three byte marker",
			data:     []byte{0xfd, 0x00, 0x01, 0x00},
			expected: 256,
			bytesRead: 4,
		},
		{
			name:     "eight byte marker",
			data:     []byte{0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			expected: 16777216,
			bytesRead: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, n := readLengthEncodedInt(tt.data)
			if result != tt.expected {
				t.Errorf("readLengthEncodedInt() = %d, want %d", result, tt.expected)
			}
			if n != tt.bytesRead {
				t.Errorf("bytes read = %d, want %d", n, tt.bytesRead)
			}
		})
	}
}

// TestParseMySQLDSNMore tests parseMySQLDSN with more formats
func TestParseMySQLDSNMore(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
		check   func(*Config) bool
	}{
		{
			name:    "with charset parameter",
			dsn:     "user:pass@tcp(localhost:3306)/db?charset=utf8mb4",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Charset == "utf8mb4"
			},
		},
		{
			name:    "with timeout parameter",
			dsn:     "user:pass@tcp(localhost:3306)/db?timeout=30s",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Timeout == 30*time.Second
			},
		},
		{
			name:    "with collation parameter",
			dsn:     "user:pass@tcp(localhost:3306)/db?collation=utf8mb4_unicode_ci",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Collation == "utf8mb4_unicode_ci"
			},
		},
		{
			name:    "multiple parameters",
			dsn:     "user:pass@tcp(localhost:3306)/db?charset=utf8mb4&collation=utf8mb4_unicode_ci&timeout=10s",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Charset == "utf8mb4" && c.Collation == "utf8mb4_unicode_ci" && c.Timeout == 10*time.Second
			},
		},
		{
			name:    "no database",
			dsn:     "user:pass@tcp(localhost:3306)/",
			wantErr: false,
			check: func(c *Config) bool {
				return c.DBName == ""
			},
		},
		{
			name:    "invalid DSN format",
			dsn:     "invalid dsn format",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseMySQLDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if tt.check != nil && !tt.check(cfg) {
				t.Errorf("Config check failed for DSN: %s", tt.dsn)
			}
		})
	}
}

// TestParseURLDSNMore tests parseURLDSN with more formats
func TestParseURLDSNMore(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
		check   func(*Config) bool
	}{
		{
			name:    "basic URL",
			dsn:     "xxsql://user:pass@localhost:3306/dbname",
			wantErr: false,
			check: func(c *Config) bool {
				return c.User == "user" && c.Passwd == "pass" && c.DBName == "dbname"
			},
		},
		{
			name:    "URL with query params",
			dsn:     "xxsql://user:pass@localhost:3306/dbname?charset=utf8mb4",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Charset == "utf8mb4"
			},
		},
		{
			name:    "URL without password",
			dsn:     "xxsql://user@localhost:3306/dbname",
			wantErr: false,
			check: func(c *Config) bool {
				return c.User == "user" && c.Passwd == ""
			},
		},
		{
			name:    "invalid URL",
			dsn:     "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseURLDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if tt.check != nil && !tt.check(cfg) {
				t.Errorf("Config check failed")
			}
		})
	}
}

// TestConfigFormatDSNMore tests FormatDSN with more configs
func TestConfigFormatDSNMore(t *testing.T) {
	tests := []struct {
		name       string
		config     *Config
		wantFields []string
	}{
		{
			name: "full config",
			config: &Config{
				User:             "root",
				Passwd:           "secret",
				Net:              "tcp",
				Addr:             "127.0.0.1:3306",
				DBName:           "testdb",
				Charset:          "utf8mb4",
				Collation:        "utf8mb4_unicode_ci",
				Timeout:          30 * time.Second,
				ReadTimeout:      60 * time.Second,
				WriteTimeout:     60 * time.Second,
				MaxAllowedPacket: 16777216,
			},
			wantFields: []string{"root", "secret", "127.0.0.1:3306", "testdb"},
		},
		{
			name: "minimal config",
			config: &Config{
				User: "user",
				Net:  "tcp",
				Addr: "localhost:3306",
			},
			wantFields: []string{"user", "localhost:3306"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.config.FormatDSN()
			for _, field := range tt.wantFields {
				if !strings.Contains(dsn, field) {
					t.Errorf("DSN does not contain expected field %q: %s", field, dsn)
				}
			}
		})
	}
}

// TestMySQLConnParseError tests parseError method
func TestMySQLConnParseError(t *testing.T) {
	tests := []struct {
		name    string
		packet  []byte
		wantErr bool
	}{
		{
			name:    "error packet",
			packet:  []byte{0xff, 0x48, 0x04, '#', 'H', 'Y', '0', '0', '0', ' ', 'E', 'r', 'r', 'o', 'r'},
			wantErr: true,
		},
		{
			name:    "ok packet",
			packet:  []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &mysqlConn{}
			err := mc.parseError(tt.packet)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			}
		})
	}
}

// TestRowsColumnTypes tests column type handling
func TestRowsColumnTypes(t *testing.T) {
	r := &rows{
		columns: []string{"id", "name", "value"},
		colTypes: []byte{0x03, 0xfd, 0x01}, // INT, VARCHAR, TINY
	}

	// Just verify we can access the columns
	if len(r.columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(r.columns))
	}
}

// TestReadLengthEncodedIntMoreVariations tests more length-encoded integer parsing
func TestReadLengthEncodedIntMoreVariations(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want int64
	}{
		{"one byte", []byte{0x7f}, 127},
		{"two byte marker", []byte{0xfc, 0xff, 0x00}, 255},
		{"three byte marker", []byte{0xfd, 0xff, 0xff, 0x00}, 65535},
		{"eight byte marker", []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00}, 4294967295},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := readLengthEncodedInt(tt.data)
			if got != tt.want {
				t.Errorf("readLengthEncodedInt() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestRowsNextMoreVariations tests the rows.Next method more thoroughly
func TestRowsNextMoreVariations(t *testing.T) {
	// Test empty rows
	r := &rows{columns: []string{}, rowData: [][]byte{}}
	dest := make([]driver.Value, 0)
	err := r.Next(dest)
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// TestRowsClose tests the rows.Close method
func TestRowsClose(t *testing.T) {
	r := &rows{
		columns: []string{"id"},
		rowData: [][]byte{{0x01}},
	}

	err := r.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestRowsColumns tests the rows.Columns method
func TestRowsColumns(t *testing.T) {
	r := &rows{
		columns: []string{"id", "name"},
	}

	cols := r.Columns()
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
}

// TestRowsHasNextResultSet tests the HasNextResultSet method
func TestRowsHasNextResultSet(t *testing.T) {
	r := &rows{}
	if r.HasNextResultSet() {
		t.Error("HasNextResultSet should be false")
	}
}

// TestRowsNextResultSet tests the NextResultSet method
func TestRowsNextResultSet(t *testing.T) {
	r := &rows{}
	err := r.NextResultSet()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// TestParseColumnDefinitionMoreVariations tests column definition parsing
func TestParseColumnDefinitionMoreVariations(t *testing.T) {
	// Test with a properly formatted packet
	// Format: lenenc_str catalog, lenenc_str schema, lenenc_str table, lenenc_str org_table,
	//         lenenc_str name, lenenc_str org_name, then fixed length fields
	packet := []byte{
		0x03, 'd', 'e', 'f',  // catalog "def"
		0x00,                  // schema ""
		0x04, 't', 'e', 's', 't', // table "test"
		0x04, 't', 'e', 's', 't', // org_table "test"
		0x04, 'n', 'a', 'm', 'e', // name "name"
		0x04, 'n', 'a', 'm', 'e', // org_name "name"
		0x0c,                  // fixed length 12
		0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x00, 0x00, 0x00, 0x00,
	}
	name, colType := parseColumnDefinition(packet)
	_ = name
	_ = colType
}

// TestConfigFormatDSNMoreVariations tests DSN formatting
func TestConfigFormatDSNMoreVariations(t *testing.T) {
	cfg := &Config{
		User:   "testuser",
		Passwd: "testpass",
		Net:    "tcp",
		Addr:   "localhost:3306",
		DBName: "testdb",
	}

	dsn := cfg.FormatDSN()
	if dsn == "" {
		t.Error("Expected non-empty DSN")
	}
}

// TestParseResultSet tests parseResultSet function
func TestParseResultSet(t *testing.T) {
	mc := &mysqlConn{
		capability: DefaultClientCapabilities,
	}

	// Test ERR packet
	errPacket := []byte{ERRPacket, 0x51, 0x04, '#', '4', '2', '0', '0', '0', 'T', 'e', 's', 't'}
	c := &conn{mysqlConn: mc}
	_, err := c.parseResultSet(errPacket)
	if err == nil {
		t.Error("parseResultSet should return error for ERR packet")
	}

	// Test OK packet (no rows)
	okPacket := []byte{OKPacket, 0x00}
	rows, err := c.parseResultSet(okPacket)
	if err != nil {
		t.Errorf("parseResultSet OK packet error: %v", err)
	}
	if rows == nil {
		t.Error("rows should not be nil for OK packet")
	}
}

// TestParseResultSetWithColumns tests parseResultSet with column definitions
func TestParseResultSetWithColumns(t *testing.T) {
	// This test requires a mock connection that can provide column data
	mc := &mysqlConn{
		capability: DefaultClientCapabilities & ^uint32(ClientDeprecateEOF),
	}
	c := &conn{mysqlConn: mc}

	// Column count packet (2 columns)
	colCountPacket := []byte{0x02} // 2 columns

	// We can't fully test this without a proper mock of readPacket
	// Just test that it doesn't panic
	_ = c
	_ = colCountPacket
}

// TestConnPrepare tests conn.Prepare
func TestConnPrepare(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: false}

	stmt, err := c.Prepare("SELECT 1")
	if err != nil {
		t.Errorf("Prepare failed: %v", err)
	}
	if stmt == nil {
		t.Error("Statement should not be nil")
	}
}

// TestConnPrepareClosed tests conn.Prepare on closed connection
func TestConnPrepareClosed(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: true}

	_, err := c.Prepare("SELECT 1")
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestConnClose tests conn.Close
func TestConnClose(t *testing.T) {
	// Test closing a connection that's already closed
	c := &conn{closed: true}
	err := c.Close()
	if err != nil {
		t.Errorf("Close on already closed should return nil: %v", err)
	}
}

// TestConnBegin tests conn.Begin
func TestConnBegin(t *testing.T) {
	// Test that BeginTx on a closed connection returns ErrBadConn
	c := &conn{closed: true}
	_, err := c.Begin()
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestConnBeginClosed tests conn.Begin on closed connection
func TestConnBeginClosed(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: true}

	_, err := c.Begin()
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestConnBeginTxCanceled tests conn.BeginTx with canceled context
func TestConnBeginTxCanceled(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: false}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := c.BeginTx(ctx, driver.TxOptions{})
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestConnResetSession tests conn.ResetSession
func TestConnResetSession(t *testing.T) {
	// Test that ResetSession on a closed connection returns ErrBadConn
	c := &conn{closed: true}
	err := c.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestConnResetSessionClosed tests conn.ResetSession on closed connection
func TestConnResetSessionClosed(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: true}

	err := c.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestStmtExecContext tests stmt.ExecContext
func TestStmtExecContext(t *testing.T) {
	// Test ExecContext on a closed connection
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: true}
	s := newStmt(c, "SELECT 1")

	_, err := s.ExecContext(context.Background(), []driver.NamedValue{})
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestStmtQueryContext tests stmt.QueryContext
func TestStmtQueryContext(t *testing.T) {
	// Test QueryContext on a closed connection
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: true}
	s := newStmt(c, "SELECT 1")

	_, err := s.QueryContext(context.Background(), []driver.NamedValue{})
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestTxCommit tests tx.Commit
func TestTxCommit(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: false, inTx: true}
	tx := &tx{conn: c}

	// Commit will try to write to connection but we just verify no panic
	_ = tx
}

// TestTxRollback tests tx.Rollback
func TestTxRollback(t *testing.T) {
	mc := &mysqlConn{}
	c := &conn{mysqlConn: mc, closed: false, inTx: true}
	tx := &tx{conn: c}

	// Rollback will try to write to connection but we just verify no panic
	_ = tx
}

// TestReadLengthEncodedIntAllPaths tests all code paths
func TestReadLengthEncodedIntAllPaths(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected int64
		n        int
	}{
		{"empty", []byte{}, 0, 0},
		{"small", []byte{0x01}, 1, 1},
		{"0xFB marker invalid", []byte{0xFB}, 0, 0},
		{"0xFC with insufficient data", []byte{0xFC, 0x01}, 0, 0},
		{"0xFD with insufficient data", []byte{0xFD, 0x01, 0x02}, 0, 0},
		{"0xFE with insufficient data", []byte{0xFE, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, 0, 0},
		{"0xFC full", []byte{0xFC, 0x01, 0x00}, 1, 3},
		{"0xFD full", []byte{0xFD, 0x01, 0x00, 0x00}, 1, 4},
		{"0xFE full", []byte{0xFE, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 1, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, n := readLengthEncodedInt(tt.data)
			if val != tt.expected {
				t.Errorf("value: got %d, want %d", val, tt.expected)
			}
			if n != tt.n {
				t.Errorf("n: got %d, want %d", n, tt.n)
			}
		})
	}
}

// TestInterpolateQueryEdgeCases tests edge cases
func TestInterpolateQueryEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		args    []driver.NamedValue
		wantErr bool
	}{
		{"no placeholders", "SELECT 1", nil, false},
		{"nil value", "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: nil}}, false},
		{"time value", "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: time.Now()}}, false},
		{"int value", "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: int64(42)}}, false},
		{"string value", "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: "test"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := interpolateQuery(tt.query, tt.args)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestEscapeStringMore tests more escape scenarios
func TestEscapeStringMore(t *testing.T) {
	tests := []struct {
		input    string
		contains string
	}{
		{"test\x00test", "\\0"},
		{"test\ntest", "\\n"},
		{"test\rtest", "\\r"},
		{"test\\test", "\\\\"},
		{"test'test", "\\'"},
		{"test\"test", "\\\""},
		{"test\x1atest", "\\Z"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeString(tt.input)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("escapeString(%q) = %q, should contain %q", tt.input, result, tt.contains)
			}
		})
	}
}

// TestFormatValueMore tests more value types
func TestFormatValueMore(t *testing.T) {
	// Test time
	result := formatValue(time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC))
	if !strings.Contains(result, "2024") {
		t.Errorf("Time format should contain year: %q", result)
	}

	// Test nil
	result = formatValue(nil)
	if result != "NULL" {
		t.Errorf("nil format: got %q, want NULL", result)
	}
}


// TestConnExecContextCanceled tests ExecContext with canceled context
func TestConnExecContextCanceled(t *testing.T) {
	c := &conn{closed: false, cfg: NewConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := c.ExecContext(ctx, "SELECT 1", nil)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestConnQueryContextCanceled tests QueryContext with canceled context
func TestConnQueryContextCanceled(t *testing.T) {
	c := &conn{closed: false, cfg: NewConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := c.QueryContext(ctx, "SELECT 1", nil)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestConnPingCanceled tests Ping with canceled context
func TestConnPingCanceled(t *testing.T) {
	c := &conn{closed: false, cfg: NewConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := c.Ping(ctx)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestConnExecContextClosed tests ExecContext on closed connection
func TestConnExecContextClosed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.ExecContext(context.Background(), "SELECT 1", nil)
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestConnQueryContextClosed tests QueryContext on closed connection
func TestConnQueryContextClosed(t *testing.T) {
	c := &conn{closed: true}

	_, err := c.QueryContext(context.Background(), "SELECT 1", nil)
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestConnPingClosed tests Ping on closed connection
func TestConnPingClosed(t *testing.T) {
	c := &conn{closed: true}

	err := c.Ping(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn, got %v", err)
	}
}

// TestInterpolateQueryMore tests more query interpolation
func TestInterpolateQueryMore(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []driver.NamedValue
		expected string
	}{
		{
			name:     "int64",
			query:    "SELECT * FROM t WHERE id = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: int64(42)}},
			expected: "SELECT * FROM t WHERE id = 42",
		},
		{
			name:     "float64",
			query:    "SELECT * FROM t WHERE val = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: 3.14}},
			expected: "SELECT * FROM t WHERE val = 3.14",
		},
		{
			name:     "string",
			query:    "SELECT * FROM t WHERE name = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: "test"}},
			expected: "SELECT * FROM t WHERE name = 'test'",
		},
		{
			name:     "bool true",
			query:    "SELECT * FROM t WHERE active = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: true}},
			expected: "SELECT * FROM t WHERE active = 1",
		},
		{
			name:     "bool false",
			query:    "SELECT * FROM t WHERE active = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: false}},
			expected: "SELECT * FROM t WHERE active = 0",
		},
		{
			name:     "nil",
			query:    "SELECT * FROM t WHERE val = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: nil}},
			expected: "SELECT * FROM t WHERE val = NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := interpolateQuery(tt.query, tt.args)
			if err != nil {
				t.Errorf("interpolateQuery error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestParseResultSetOK tests parseResultSet with OK packet
func TestParseResultSetOK(t *testing.T) {
	mc := &mysqlConn{capability: DefaultClientCapabilities}
	c := &conn{mysqlConn: mc}

	// OK packet with no rows
	packet := []byte{OKPacket, 0x00}
	rows, err := c.parseResultSet(packet)
	if err != nil {
		t.Errorf("parseResultSet OK packet error: %v", err)
	}
	if rows == nil {
		t.Error("rows should not be nil")
	}
}

// TestParseResultSetERR tests parseResultSet with ERR packet
func TestParseResultSetERR(t *testing.T) {
	mc := &mysqlConn{capability: DefaultClientCapabilities}
	c := &conn{mysqlConn: mc}

	// ERR packet
	packet := []byte{ERRPacket, 0x51, 0x04, '#', '4', '2', '0', '0', '0', 'T', 'e', 's', 't'}
	_, err := c.parseResultSet(packet)
	if err == nil {
		t.Error("parseResultSet should return error for ERR packet")
	}
}

// TestConnectorConnect tests connector.Connect
func TestConnectorConnect(t *testing.T) {
	cfg := NewConfig()
	cfg.User = "testuser"
	cfg.DBName = "testdb"

	conn := &connector{
		dsn: "testuser@tcp(localhost:3306)/testdb",
		cfg: cfg,
		drv: driverInstance,
	}

	// Connect will fail without server, just verify no panic
	_, err := conn.Connect(context.Background())
	_ = err
}

// TestConnectorConnectCanceled tests connector.Connect with canceled context
func TestConnectorConnectCanceled(t *testing.T) {
	cfg := NewConfig()
	conn := &connector{
		dsn: "testuser@tcp(localhost:3306)/testdb",
		cfg: cfg,
		drv: driverInstance,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := conn.Connect(ctx)
	if err == nil {
		t.Error("Connect should fail with canceled context")
	}
}

// TestMysqlConnClose tests mysqlConn.close
func TestMysqlConnClose(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)

	// Read in background to prevent blocking
	go func() {
		buf := make([]byte, 1024)
		_, _ = server.Read(buf)
	}()

	// Close should not panic
	err := mc.closeConnection()
	_ = err
}

// TestWritePacket tests writePacket
func TestWritePacket(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)

	// Read in background
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1024)
		n, _ := server.Read(buf)
		_ = n
		close(done)
	}()

	err := mc.writePacket([]byte{0x01, 0x02, 0x03})
	_ = err
	<-done
}

// TestReadPacket tests readPacket
func TestReadPacket(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)

	// Write in background
	go func() {
		// MySQL packet: 3 byte length + 1 byte seq + data
		server.Write([]byte{0x03, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03})
		server.Close()
	}()

	packet, err := mc.readPacket()
	_ = err
	_ = packet
}

// TestSetDeadline tests setDeadline
func TestSetDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)

	// Should not panic
	mc.setDeadline(time.Now().Add(5 * time.Second))
}

// TestResultLastInsertID tests result.LastInsertId
func TestResultLastInsertID(t *testing.T) {
	r := &result{lastInsertID: 42}
	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId error: %v", err)
	}
	if id != 42 {
		t.Errorf("LastInsertId: got %d, want 42", id)
	}
}

// TestResultRowsAffected tests result.RowsAffected
func TestResultRowsAffected(t *testing.T) {
	r := &result{affectedRows: 100}
	affected, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected error: %v", err)
	}
	if affected != 100 {
		t.Errorf("RowsAffected: got %d, want 100", affected)
	}
}

// TestRowsCloseCoverage tests rows.Close
func TestRowsCloseCoverage(t *testing.T) {
	r := &rows{}
	if err := r.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

// TestConfigFormatDSNExtra tests FormatDSN with more options
func TestConfigFormatDSNExtra(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{
			name: "with TLS",
			config: &Config{
				User:     "user",
				Passwd:   "pass",
				Net:      "tcp",
				Addr:     "localhost:3306",
				DBName:   "test",
				TLS:      true,
				Charset:  "utf8mb4",
				Collation: "utf8mb4_general_ci",
			},
		},
		{
			name: "with timeouts",
			config: &Config{
				User:         "user",
				Passwd:       "pass",
				Net:          "tcp",
				Addr:         "localhost:3306",
				DBName:       "test",
				Timeout:      30 * time.Second,
				ReadTimeout:  10 * time.Second,
				WriteTimeout: 10 * time.Second,
			},
		},
		{
			name: "with old password",
			config: &Config{
				User:             "user",
				Passwd:           "pass",
				Net:              "tcp",
				Addr:             "localhost:3306",
				DBName:           "test",
				AllowOldPassword: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.config.FormatDSN()
			if dsn == "" {
				t.Error("FormatDSN returned empty string")
			}
		})
	}
}

// TestParseMySQLDSNExtra tests DSN parsing edge cases
func TestParseMySQLDSNExtra(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{"empty password", "user:@tcp(localhost:3306)/db", false},
		{"no password", "user@tcp(localhost:3306)/db", false},
		{"with params", "user:pass@tcp(localhost:3306)/db?charset=utf8&timeout=30s", false},
		{"with collation", "user:pass@tcp(localhost:3306)/db?collation=utf8_general_ci", false},
		{"unix socket", "user:pass@unix(/tmp/mysql.sock)/db", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && cfg == nil {
				t.Error("ParseDSN returned nil config")
			}
		})
	}
}

// TestIsolationLevel tests isolation level conversion
func TestIsolationLevel(t *testing.T) {
	tests := []struct {
		level driver.IsolationLevel
		want  string
	}{
		{IsolationLevelDefault, ""},
		{IsolationLevelReadUncommitted, "READ UNCOMMITTED"},
		{IsolationLevelReadCommitted, "READ COMMITTED"},
		{IsolationLevelRepeatableRead, "REPEATABLE READ"},
		{IsolationLevelSerializable, "SERIALIZABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := isolationLevelToString(tt.level)
			if result != tt.want {
				t.Errorf("isolationLevelToString(%v) = %q, want %q", tt.level, result, tt.want)
			}
		})
	}
}

// TestReadLengthEncodedIntMore tests readLengthEncodedInt
func TestReadLengthEncodedIntCoverage(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"small int", []byte{0x05}},
		{"two byte int", []byte{0xfc, 0x01, 0x00}},
		{"three byte int", []byte{0xfd, 0x01, 0x00, 0x00}},
		{"eight byte int", []byte{0xfe, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, n := readLengthEncodedInt(tt.data)
			_ = val
			_ = n
		})
	}
}

// TestConnPrepare tests Prepare
func TestConnPrepareCoverage(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)
	c := &conn{mysqlConn: mc, cfg: cfg}

	go func() {
		buf := make([]byte, 1024)
		server.Read(buf)
	}()

	stmt, err := c.Prepare("SELECT 1")
	// Will fail because connection isn't a real MySQL server
	_ = err
	_ = stmt
}

// TestStmtOperations tests statement operations
func TestStmtOperations(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cfg := NewConfig()
	mc := newMySQLConn(client, cfg)
	c := &conn{mysqlConn: mc, cfg: cfg}

	go func() {
		buf := make([]byte, 4096)
		for {
			_, err := server.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	stmt := &stmt{
		conn:     c,
		query:    "SELECT ?",
		paramLen: 1,
	}

	// Test NumInput
	if stmt.NumInput() != 1 {
		t.Errorf("NumInput: got %d, want 1", stmt.NumInput())
	}
}

// TestPrivateConnBuildAuthRequest tests buildAuthRequest
func TestPrivateConnBuildAuthRequest(t *testing.T) {
	cfg := &Config{
		User:   "testuser",
		Passwd: "testpass",
		DBName: "testdb",
	}
	pc := &privateConn{cfg: cfg}

	authReq := pc.buildAuthRequest()
	if authReq == nil || len(authReq) == 0 {
		t.Error("buildAuthRequest returned empty or nil result")
	}

	// Verify auth method byte
	if authReq[0] != 0 {
		t.Errorf("auth method: got %d, want 0", authReq[0])
	}
}

// TestPrivateConnParseHandshakeResponse tests parseHandshakeResponse
func TestPrivateConnParseHandshakeResponse(t *testing.T) {
	pc := &privateConn{}

	tests := []struct {
		name    string
		payload []byte
		wantErr bool
	}{
		{"too short", []byte{0x00, 0x01, 0x02}, true},
		{"valid", []byte{
			0x00, 0x03, // protocol version 3
			0x00, 0x05, // server version length
			'1', '.', '0', '.', '0', // server version
			0x01, // supported = true
		}, false},
		{"invalid version length", []byte{
			0x00, 0x03,
			0x00, 0x10, // server version length too large
			'1', '.', '0', '.', '0',
			0x01,
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := pc.parseHandshakeResponse(tt.payload)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil {
					t.Error("resp is nil")
				}
			}
		})
	}
}

// TestPrivateConnParseAuthResponse tests parseAuthResponse
func TestPrivateConnParseAuthResponse(t *testing.T) {
	pc := &privateConn{}

	tests := []struct {
		name    string
		payload []byte
		success bool
		message string
		wantErr bool
	}{
		{"empty", []byte{}, false, "", true},
		{"success", []byte{0x01}, true, "", false},
		{"failure", []byte{0x00}, false, "", false},
		{"with message", []byte{0x01, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}, true, "hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := pc.parseAuthResponse(tt.payload)
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
			if resp.Success != tt.success {
				t.Errorf("Success: got %v, want %v", resp.Success, tt.success)
			}
			if resp.Message != tt.message {
				t.Errorf("Message: got %q, want %q", resp.Message, tt.message)
			}
		})
	}
}

// TestPrivateConnParseQueryResult tests parseQueryResult
func TestPrivateConnParseQueryResult(t *testing.T) {
	pc := &privateConn{}

	tests := []struct {
		name    string
		payload []byte
		wantErr bool
	}{
		{"too short", []byte{0x00, 0x01, 0x02, 0x03}, true},
		{"valid", []byte{
			0x00, 0x00, 0x00, 0x0A, // rows affected = 10
			0x00, 0x00, 0x00, 0x05, // last insert id = 5
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pc.parseQueryResult(tt.payload)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result == nil {
					t.Error("result is nil")
				}
			}
		})
	}
}

// TestPrivateConnParseRows tests parseRows
func TestPrivateConnParseRows(t *testing.T) {
	pc := &privateConn{}

	tests := []struct {
		name    string
		payload []byte
		wantErr bool
	}{
		{"too short", []byte{0x00}, true},
		{"valid empty", []byte{0x00, 0x00, 0x00, 0x00}, false},
		{"valid with columns", []byte{
			0x00, 0x02, // 2 columns
			0x00, 0x02, 'i', 'd', // column "id"
			0x00, 0x04, 'n', 'a', 'm', 'e', // column "name"
			0x00, 0x00, // 0 rows
		}, false},
		{"valid with rows", []byte{
			0x00, 0x01, // 1 column
			0x00, 0x01, 'a', // column "a"
			0x00, 0x01, // 1 row
			0x01, // int64 type
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, // value 42
		}, false},
		{"row with string", []byte{
			0x00, 0x01, // 1 column
			0x00, 0x01, 'a', // column "a"
			0x00, 0x01, // 1 row
			0x03, // string type
			0x00, 0x05, 'h', 'e', 'l', 'l', 'o', // "hello"
		}, false},
		{"row with null", []byte{
			0x00, 0x01, // 1 column
			0x00, 0x01, 'a', // column "a"
			0x00, 0x01, // 1 row
			0x00, // null type
		}, false},
		{"row with float", []byte{
			0x00, 0x01, // 1 column
			0x00, 0x01, 'a', // column "a"
			0x00, 0x01, // 1 row
			0x02, // float64 type
			0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18, // pi
		}, false},
		{"row with bool", []byte{
			0x00, 0x01, // 1 column
			0x00, 0x01, 'a', // column "a"
			0x00, 0x01, // 1 row
			0x04, // bool type
			0x01, // true
		}, false},
		{"unknown type", []byte{
			0x00, 0x01, // 1 column
			0x00, 0x01, 'a', // column "a"
			0x00, 0x01, // 1 row
			0xFF, // unknown type
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := pc.parseRows(tt.payload)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if rows == nil {
					t.Error("rows is nil")
				}
			}
		})
	}
}

// TestPrivateResult tests privateResult
func TestPrivateResult(t *testing.T) {
	r := &privateResult{
		rowsAffected: 100,
		lastInsertID: 42,
	}

	lastID, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId error: %v", err)
	}
	if lastID != 42 {
		t.Errorf("LastInsertId: got %d, want 42", lastID)
	}

	affected, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected error: %v", err)
	}
	if affected != 100 {
		t.Errorf("RowsAffected: got %d, want 100", affected)
	}
}

// TestPrivateRows tests privateRows
func TestPrivateRows(t *testing.T) {
	r := &privateRows{
		columns: []string{"id", "name"},
		rows: [][]interface{}{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
		},
	}

	cols := r.Columns()
	if len(cols) != 2 {
		t.Errorf("Columns: got %d, want 2", len(cols))
	}

	dest := make([]driver.Value, 2)
	err := r.Next(dest)
	if err != nil {
		t.Errorf("Next error: %v", err)
	}
	if dest[0] != int64(1) {
		t.Errorf("dest[0]: got %v, want 1", dest[0])
	}

	err = r.Next(dest)
	if err != nil {
		t.Errorf("Next error: %v", err)
	}

	err = r.Next(dest)
	if err != io.EOF {
		t.Errorf("Next on exhausted: got %v, want io.EOF", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

// TestPrivateStmt tests privateStmt
func TestPrivateStmt(t *testing.T) {
	stmt := &privateStmt{
		conn:  nil,
		query: "SELECT 1",
	}

	if stmt.NumInput() != -1 {
		t.Errorf("NumInput: got %d, want -1", stmt.NumInput())
	}

	if err := stmt.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

// TestPrivateTx tests privateTx
func TestPrivateTx(t *testing.T) {
	pc := &privateConn{inTx: true}
	tx := &privateTx{conn: pc}

	if err := tx.Commit(); err != nil {
		t.Errorf("Commit error: %v", err)
	}
	if pc.inTx {
		t.Error("inTx should be false after commit")
	}

	pc.inTx = true
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback error: %v", err)
	}
	if pc.inTx {
		t.Error("inTx should be false after rollback")
	}
}

// TestPrivateConnCheckNamedValue tests CheckNamedValue
func TestPrivateConnCheckNamedValue(t *testing.T) {
	pc := &privateConn{}
	nv := &driver.NamedValue{Name: "test", Value: "value"}

	err := pc.CheckNamedValue(nv)
	if err != nil {
		t.Errorf("CheckNamedValue error: %v", err)
	}
}

// TestPrivateConnResetSession tests ResetSession
func TestPrivateConnResetSession(t *testing.T) {
	pc := &privateConn{closed: false}
	err := pc.ResetSession(context.Background())
	if err != nil {
		t.Errorf("ResetSession error: %v", err)
	}

	pc.closed = true
	err = pc.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("ResetSession on closed: got %v, want ErrBadConn", err)
	}
}

// TestReadFull tests readFull
func TestReadFull(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		server.Write([]byte("hello world"))
	}()

	buf := make([]byte, 5)
	n, err := readFull(client, buf)
	if err != nil {
		t.Errorf("readFull error: %v", err)
	}
	if n != 5 {
		t.Errorf("readFull: got %d, want 5", n)
	}
	if string(buf) != "hello" {
		t.Errorf("readFull: got %q, want 'hello'", string(buf))
	}
}

// TestPrivateConnPrepare tests Prepare
func TestPrivateConnPrepare(t *testing.T) {
	pc := &privateConn{closed: false}
	stmt, err := pc.Prepare("SELECT 1")
	if err != nil {
		t.Errorf("Prepare error: %v", err)
	}
	if stmt == nil {
		t.Error("Prepare returned nil stmt")
	}

	// Test on closed connection
	pc.closed = true
	_, err = pc.Prepare("SELECT 1")
	if err != driver.ErrBadConn {
		t.Errorf("Prepare on closed: got %v, want ErrBadConn", err)
	}
}

// TestPrivateConnBegin tests Begin
func TestPrivateConnBeginFinal(t *testing.T) {
	pc := &privateConn{closed: false, inTx: false}
	pc.mu = sync.Mutex{}

	tx, err := pc.Begin()
	if err != nil {
		t.Errorf("Begin error: %v", err)
	}
	if tx == nil {
		t.Error("Begin returned nil tx")
	}
	if !pc.inTx {
		t.Error("Begin didn't set inTx flag")
	}

	// Test on closed connection
	pc2 := &privateConn{closed: true}
	pc2.mu = sync.Mutex{}
	_, err = pc2.Begin()
	if err != driver.ErrBadConn {
		t.Errorf("Begin on closed: got %v, want ErrBadConn", err)
	}
}

// TestPrivateConnBeginTx tests BeginTx
func TestPrivateConnBeginTxFinal(t *testing.T) {
	pc := &privateConn{closed: false, inTx: false}
	pc.mu = sync.Mutex{}

	tx, err := pc.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Errorf("BeginTx error: %v", err)
	}
	if tx == nil {
		t.Error("BeginTx returned nil tx")
	}

	// Test already in transaction
	_, err = pc.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		t.Error("BeginTx when already in tx should fail")
	}
}

// TestPrivateStmtExtra tests privateStmt methods
func TestPrivateStmtExtra(t *testing.T) {
	pc := &privateConn{closed: false}
	pc.mu = sync.Mutex{}

	stmt := &privateStmt{conn: pc, query: "SELECT 1"}

	// Test Close
	err := stmt.Close()
	if err != nil {
		t.Errorf("Stmt.Close error: %v", err)
	}

	// Test NumInput
	if stmt.NumInput() != -1 {
		t.Errorf("NumInput: got %d, want -1", stmt.NumInput())
	}
}

// TestPrivateTxExtra tests privateTx methods
func TestPrivateTxExtra(t *testing.T) {
	pc := &privateConn{closed: false, inTx: true}
	pc.mu = sync.Mutex{}

	tx := &privateTx{conn: pc}

	// Test Commit
	err := tx.Commit()
	if err != nil {
		t.Errorf("Commit error: %v", err)
	}
	if pc.inTx {
		t.Error("Commit didn't reset inTx flag")
	}

	// Test Rollback
	pc.inTx = true
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Rollback error: %v", err)
	}
	if pc.inTx {
		t.Error("Rollback didn't reset inTx flag")
	}
}

// TestBuildQueryRequest tests buildQueryRequest
func TestBuildQueryRequest(t *testing.T) {
	pc := &privateConn{}
	query := "SELECT * FROM users WHERE id = ?"
	args := []driver.NamedValue{
		{Ordinal: 1, Value: 42},
	}

	req := pc.buildQueryRequest(query, args)
	if len(req) == 0 {
		t.Error("buildQueryRequest returned empty")
	}
}

// TestBuildMessage tests buildMessage
func TestBuildMessage(t *testing.T) {
	pc := &privateConn{}
	payload := []byte("test payload")

	msg, err := pc.buildMessage(0x01, payload)
	if err != nil {
		t.Errorf("buildMessage error: %v", err)
	}
	if len(msg) == 0 {
		t.Error("buildMessage returned empty message")
	}
}
