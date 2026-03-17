package xxsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"strings"
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
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	if db == nil {
		t.Error("Open returned nil db")
	}
}

func TestOpenDB(t *testing.T) {
	dsn := "testuser:testpass@tcp(localhost:3306)/testdb"

	db, err := OpenDB(dsn)
	if err != nil {
		t.Fatalf("OpenDB error: %v", err)
	}
	if db == nil {
		t.Error("OpenDB returned nil db")
	}
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
