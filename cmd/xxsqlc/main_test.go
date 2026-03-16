package main

import (
	"testing"
)

func TestParseOutputFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected OutputFormat
	}{
		{"table", FormatTable},
		{"vertical", FormatVertical},
		{"v", FormatVertical},
		{"json", FormatJSON},
		{"j", FormatJSON},
		{"tsv", FormatTSV},
		{"t", FormatTSV},
		{"unknown", FormatTable}, // default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseOutputFormat(tt.input)
			if result != tt.expected {
				t.Errorf("parseOutputFormat(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestOutputFormatString(t *testing.T) {
	tests := []struct {
		format   OutputFormat
		expected string
	}{
		{FormatTable, "table"},
		{FormatVertical, "vertical"},
		{FormatJSON, "json"},
		{FormatTSV, "tsv"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.format.String()
			if result != tt.expected {
				t.Errorf("OutputFormat(%d).String() = %q, want %q", tt.format, result, tt.expected)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"nil", nil, "NULL"},
		{"string", "hello", "hello"},
		{"int", int64(123), "123"},
		{"float", float64(3.14), "3.14"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"bytes", []byte("test"), "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.value)
			if result != tt.expected {
				t.Errorf("formatValue(%v) = %q, want %q", tt.value, result, tt.expected)
			}
		})
	}
}

func TestPadRight(t *testing.T) {
	tests := []struct {
		input    string
		width    int
		expected string
	}{
		{"hello", 10, "hello     "},
		{"hello", 5, "hello"},
		{"hello", 3, "hello"}, // wider than width
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := padRight(tt.input, tt.width)
			if result != tt.expected {
				t.Errorf("padRight(%q, %d) = %q, want %q", tt.input, tt.width, result, tt.expected)
			}
		})
	}
}

func TestBuildDSN(t *testing.T) {
	// Save original flag values
	origHost := *flagHost
	origPort := *flagPort
	origUser := *flagUser
	origPassword := *flagPassword
	origDatabase := *flagDatabase
	defer func() {
		*flagHost = origHost
		*flagPort = origPort
		*flagUser = origUser
		*flagPassword = origPassword
		*flagDatabase = origDatabase
	}()

	tests := []struct {
		name     string
		host     string
		port     int
		user     string
		password string
		database string
		expected string
	}{
		{
			name:     "full DSN",
			host:     "localhost",
			port:     3306,
			user:     "root",
			password: "secret",
			database: "test",
			expected: "root:secret@tcp(localhost:3306)/test",
		},
		{
			name:     "no password",
			host:     "localhost",
			port:     3306,
			user:     "root",
			password: "",
			database: "test",
			expected: "root@tcp(localhost:3306)/test",
		},
		{
			name:     "no user",
			host:     "localhost",
			port:     3306,
			user:     "",
			password: "",
			database: "test",
			expected: "tcp(localhost:3306)/test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			*flagHost = tt.host
			*flagPort = tt.port
			*flagUser = tt.user
			*flagPassword = tt.password
			*flagDatabase = tt.database

			result := buildDSN()
			if result != tt.expected {
				t.Errorf("buildDSN() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestGetPrompt(t *testing.T) {
	// Test with no database
	dbName = ""
	result := getPrompt()
	expected := "xxsql> "
	if result != expected {
		t.Errorf("getPrompt() with no db = %q, want %q", result, expected)
	}

	// Test with database
	dbName = "testdb"
	result = getPrompt()
	expected = "testdb> "
	if result != expected {
		t.Errorf("getPrompt() with db = %q, want %q", result, expected)
	}
}
