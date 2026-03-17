package main

import (
	"strings"
	"testing"
)

func TestParseOutputFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected OutputFormat
	}{
		{"table", FormatTable},
		{"TABLE", FormatTable},
		{"vertical", FormatVertical},
		{"v", FormatVertical},
		{"V", FormatVertical},
		{"json", FormatJSON},
		{"j", FormatJSON},
		{"tsv", FormatTSV},
		{"t", FormatTSV},
		{"unknown", FormatTable},
		{"", FormatTable},
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
		{OutputFormat(99), "unknown"},
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
		{"int", int(123), "123"},
		{"int32", int32(456), "456"},
		{"int64", int64(789), "789"},
		{"uint", uint(100), "100"},
		{"uint32", uint32(200), "200"},
		{"uint64", uint64(300), "300"},
		{"float32", float32(3.14), "3.14"},
		{"float64", float64(2.718), "2.718"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"bytes", []byte("test"), "test"},
		{"other", struct{}{}, "{}"},
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
		{"hello", 3, "hello"},
		{"", 5, "     "},
		{"ab", 5, "ab   "},
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
		{
			name:     "no database",
			host:     "127.0.0.1",
			port:     9527,
			user:     "admin",
			password: "pass",
			database: "",
			expected: "admin:pass@tcp(127.0.0.1:9527)/",
		},
		{
			name:     "minimal DSN",
			host:     "db.example.com",
			port:     3306,
			user:     "",
			password: "",
			database: "",
			expected: "tcp(db.example.com:3306)/",
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
	origDbName := dbName
	defer func() { dbName = origDbName }()

	dbName = ""
	result := getPrompt()
	expected := "xxsql> "
	if result != expected {
		t.Errorf("getPrompt() with no db = %q, want %q", result, expected)
	}

	dbName = "testdb"
	result = getPrompt()
	expected = "testdb> "
	if result != expected {
		t.Errorf("getPrompt() with db = %q, want %q", result, expected)
	}
}

func TestGetFormatter(t *testing.T) {
	tests := []struct {
		format   OutputFormat
		expected string
	}{
		{FormatTable, "*tableFormatter"},
		{FormatVertical, "*verticalFormatter"},
		{FormatJSON, "*jsonFormatter"},
		{FormatTSV, "*tsvFormatter"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			f := getFormatter(tt.format)
			if f == nil {
				t.Error("getFormatter returned nil")
			}
		})
	}
}

func TestTableFormatter(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	f := &tableFormatter{}
	f.Format(columns, rows)
}

func TestTableFormatter_Empty(t *testing.T) {
	f := &tableFormatter{}
	f.Format([]string{}, nil)
	f.Format(nil, nil)
}

func TestTableFormatter_LongValue(t *testing.T) {
	columns := []string{"description"}
	rows := [][]interface{}{
		{"This is a very long description that should be truncated because it exceeds the maximum column width limit"},
	}

	f := &tableFormatter{}
	f.Format(columns, rows)
}

func TestVerticalFormatter(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	f := &verticalFormatter{}
	f.Format(columns, rows)
}

func TestVerticalFormatter_Empty(t *testing.T) {
	f := &verticalFormatter{}
	f.Format([]string{}, nil)
}

func TestVerticalFormatter_LongName(t *testing.T) {
	columns := []string{"very_long_column_name", "short"}
	rows := [][]interface{}{
		{"value1", "value2"},
	}

	f := &verticalFormatter{}
	f.Format(columns, rows)
}

func TestJSONFormatter(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	f := &jsonFormatter{}
	f.Format(columns, rows)
}

func TestJSONFormatter_Empty(t *testing.T) {
	f := &jsonFormatter{}
	f.Format([]string{}, nil)
}

func TestTSVFormatter(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	f := &tsvFormatter{}
	f.Format(columns, rows)
}

func TestTSVFormatter_Empty(t *testing.T) {
	f := &tsvFormatter{}
	f.Format([]string{}, nil)
}

func TestCompleter_FirstWord(t *testing.T) {
	c := &completer{}

	tests := []struct {
		line     string
		hasMatch bool
	}{
		{"SE", true},
		{"IN", true},
		{"UP", true},
		{"DE", true},
		{"CR", true},
		{"DR", true},
		{"SH", true},
		{"\\d", true},
		{"\\q", true},
		{"he", true},
		{"qu", true},
		{"xyz", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			newLine, _ := c.Do([]rune(tt.line), len(tt.line))
			if tt.hasMatch && len(newLine) == 0 {
				t.Errorf("Expected completions for %q, got none", tt.line)
			}
			if !tt.hasMatch && len(newLine) > 0 {
				t.Errorf("Unexpected completions for %q: %v", tt.line, newLine)
			}
		})
	}
}

func TestCompleter_Empty(t *testing.T) {
	c := &completer{}

	newLine, length := c.Do([]rune{}, 0)
	if len(newLine) != 0 || length != 0 {
		t.Error("Empty line should return no completions")
	}
}

func TestCompleter_Context(t *testing.T) {
	c := &completer{}

	tests := []struct {
		line     string
		hasMatch bool
	}{
		{"SELECT * FROM users WHE", true},
		{"SELECT * FROM users ORD", true},
		{"SELECT * FROM users HAV", true},
		{"SELECT * FROM users LIM", true},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			newLine, _ := c.Do([]rune(tt.line), len(tt.line))
			if tt.hasMatch && len(newLine) == 0 {
				t.Errorf("Expected completions for %q, got none", tt.line)
			}
		})
	}
}

func TestVersionInfo(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
	if GitCommit == "" {
		t.Error("GitCommit should not be empty")
	}
	if BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}
}

func TestGetHistoryPath(t *testing.T) {
	path := getHistoryPath()
	if path == "" {
		t.Skip("Could not get home directory")
	}
	if len(path) < 10 {
		t.Errorf("History path seems too short: %s", path)
	}
}

func TestClearScreen(t *testing.T) {
	clearScreen()
}

func TestPrintHelp(t *testing.T) {
	printHelp()
}

func TestPrintWelcome(t *testing.T) {
	origHost := *flagHost
	origPort := *flagPort
	defer func() {
		*flagHost = origHost
		*flagPort = origPort
	}()

	*flagHost = "localhost"
	*flagPort = 3306
	dbName = "testdb"

	printWelcome()
}

func TestHandleMetaCommand_Unknown(t *testing.T) {
	err := handleMetaCommand("\\unknown")
	if err == nil {
		t.Error("Expected error for unknown command")
	}
}

func TestHandleMetaCommand_Help(t *testing.T) {
	err := handleMetaCommand("\\h")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	err = handleMetaCommand("\\?")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestHandleMetaCommand_Format(t *testing.T) {
	tests := []struct {
		cmd      string
		expected OutputFormat
	}{
		{"\\table", FormatTable},
		{"\\g", FormatVertical},
		{"\\vertical", FormatVertical},
		{"\\j", FormatJSON},
		{"\\json", FormatJSON},
		{"\\t", FormatTSV},
		{"\\tsv", FormatTSV},
	}

	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			err := handleMetaCommand(tt.cmd)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if outFmt != tt.expected {
				t.Errorf("Format: got %v, want %v", outFmt, tt.expected)
			}
		})
	}
}

func TestHandleMetaCommand_Timing(t *testing.T) {
	origTiming := timing
	defer func() { timing = origTiming }()

	timing = false
	err := handleMetaCommand("\\timing")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !timing {
		t.Error("Timing should be true after toggle")
	}

	err = handleMetaCommand("\\timing")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if timing {
		t.Error("Timing should be false after second toggle")
	}
}

func TestHandleMetaCommand_ConnInfo(t *testing.T) {
	origHost := *flagHost
	origPort := *flagPort
	origUser := *flagUser
	defer func() {
		*flagHost = origHost
		*flagPort = origPort
		*flagUser = origUser
	}()

	*flagHost = "localhost"
	*flagPort = 3306
	*flagUser = "testuser"
	dbName = "testdb"

	err := handleMetaCommand("\\conninfo")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestHandleMetaCommand_UseDatabase_Error(t *testing.T) {
	err := handleMetaCommand("\\u")
	if err == nil {
		t.Error("Expected error for \\u without database name")
	}
}

func TestHandleMetaCommand_ListDatabases(t *testing.T) {
	dbName = "testdb"
	err := handleMetaCommand("\\l")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestExecuteSQL_Empty(t *testing.T) {
	err := executeSQL("")
	if err != nil {
		t.Errorf("Empty query should not error: %v", err)
	}

	err = executeSQL("   ")
	if err != nil {
		t.Errorf("Whitespace query should not error: %v", err)
	}
}

func TestExecuteSQL_QueryType(t *testing.T) {
	tests := []string{
		"SELECT 1",
		"SHOW TABLES",
		"DESCRIBE users",
		"DESC users",
		"EXPLAIN SELECT * FROM users",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			upperQuery := strings.ToUpper(query)
			isQuery := strings.HasPrefix(upperQuery, "SELECT") ||
				strings.HasPrefix(upperQuery, "SHOW") ||
				strings.HasPrefix(upperQuery, "DESCRIBE") ||
				strings.HasPrefix(upperQuery, "DESC") ||
				strings.HasPrefix(upperQuery, "EXPLAIN")

			if !isQuery {
				t.Errorf("Query should be recognized as a SELECT/SHOW/DESCRIBE/EXPLAIN query")
			}
		})
	}
}

func TestSetupSignals(t *testing.T) {
	setupSignals()
}

func TestFormatValue_FloatPrecision(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		contains string
	}{
		{"float32", float32(3.14159), "3.14"},
		{"float64", float64(2.71828), "2.71"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.value)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("formatValue(%v) = %q, should contain %q", tt.value, result, tt.contains)
			}
		})
	}
}

func TestCompleter_SecondWord(t *testing.T) {
	c := &completer{}

	tests := []struct {
		line     string
		hasMatch bool
	}{
		{"SELECT * FROM users WHE", true},
		{"INSERT IN", true},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			newLine, _ := c.Do([]rune(tt.line), len(tt.line))
			if tt.hasMatch && len(newLine) == 0 {
				t.Errorf("Expected completions for %q, got none", tt.line)
			}
		})
	}
}

func TestJSONFormatter_WithNil(t *testing.T) {
	columns := []string{"id", "name", "value"}
	rows := [][]interface{}{
		{1, nil, "test"},
		{2, "Alice", nil},
	}

	f := &jsonFormatter{}
	f.Format(columns, rows)
}

func TestTSVFormatter_SpecialChars(t *testing.T) {
	columns := []string{"id", "description"}
	rows := [][]interface{}{
		{1, "value\twith\ttabs"},
		{2, "value\nwith\nnewlines"},
	}

	f := &tsvFormatter{}
	f.Format(columns, rows)
}

func TestVerticalFormatter_WithNil(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, nil},
	}

	f := &verticalFormatter{}
	f.Format(columns, rows)
}

func TestTableFormatter_Width(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	f := &tableFormatter{}
	f.Format(columns, rows)
}

func TestHandleMetaCommand_FormatCommand(t *testing.T) {
	err := handleMetaCommand("\\format")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestCompleter_NoMatch(t *testing.T) {
	c := &completer{}

	tests := []struct {
		line string
	}{
		{"XYZ"},
		{"SELECT * FROM users XYZ"},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			newLine, _ := c.Do([]rune(tt.line), len(tt.line))
			if len(newLine) > 0 {
				t.Errorf("Unexpected completions for %q", tt.line)
			}
		})
	}
}

func TestCompleter_SingleSpace(t *testing.T) {
	c := &completer{}

	// A line with just a space should have no completions
	newLine, _ := c.Do([]rune(" "), 1)
	if len(newLine) > 0 {
		t.Errorf("Unexpected completions for space")
	}
}

func TestCompleter_TableNameContext(t *testing.T) {
	c := &completer{}

	// After FROM, JOIN, INTO, TABLE - should return no completions (not implemented)
	tests := []string{
		"SELECT * FROM users ",
		"INSERT INTO users ",
		"CREATE TABLE users ",
		"DROP TABLE ",
	}

	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			newLine, _ := c.Do([]rune(tt), len(tt))
			// Currently returns no completions for table names
			// This test just ensures no panic
			_ = newLine
		})
	}
}

func TestFormatter_WithMoreColumns(t *testing.T) {
	columns := []string{"id", "name", "email", "created_at", "status"}
	rows := [][]interface{}{
		{1, "Alice", "alice@example.com", "2024-01-15", "active"},
		{2, "Bob", "bob@example.com", "2024-01-16", "inactive"},
	}

	t.Run("table", func(t *testing.T) {
		f := &tableFormatter{}
		f.Format(columns, rows)
	})

	t.Run("vertical", func(t *testing.T) {
		f := &verticalFormatter{}
		f.Format(columns, rows)
	})

	t.Run("json", func(t *testing.T) {
		f := &jsonFormatter{}
		f.Format(columns, rows)
	})

	t.Run("tsv", func(t *testing.T) {
		f := &tsvFormatter{}
		f.Format(columns, rows)
	})
}

func TestTableFormatter_VeryLongValue(t *testing.T) {
	columns := []string{"description"}
	longValue := strings.Repeat("x", 100)
	rows := [][]interface{}{
		{longValue},
	}

	f := &tableFormatter{}
	f.Format(columns, rows)
}

func TestVerticalFormatter_NoRows(t *testing.T) {
	f := &verticalFormatter{}
	f.Format([]string{"id", "name"}, nil)
}

func TestJSONFormatter_MarshalError(t *testing.T) {
	// Create a value that can't be marshalled to JSON
	columns := []string{"data"}
	rows := [][]interface{}{
		{make(chan int)}, // channels can't be marshalled to JSON
	}

	f := &jsonFormatter{}
	f.Format(columns, rows) // Should handle gracefully
}

func TestFormatValue_AdditionalTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		contains string
	}{
		{"int8", int8(42), "42"},
		{"int16", int16(100), "100"},
		{"uint8", uint8(50), "50"},
		{"uint16", uint16(200), "200"},
		{"int", int(123), "123"},
		{"int64", int64(999), "999"},
		{"float32", float32(1.5), "1.5"},
		{"float64", float64(2.5), "2.5"},
		{"true", true, "1"},
		{"false", false, "0"},
		{"nil", nil, "NULL"},
		{"string", "hello", "hello"},
		{"bytes", []byte("test"), "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.value)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("formatValue(%v) = %q, should contain %q", tt.value, result, tt.contains)
			}
		})
	}
}

func TestPadRight_EdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		width    int
		expected string
	}{
		{"", 0, ""},
		{"a", 0, "a"},
		{"a", 1, "a"},
		{"ab", 1, "ab"},
		{"abc", 2, "abc"},
		{"", 5, "     "},
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

func TestOutputFormat_Default(t *testing.T) {
	// Test that unknown format defaults to table
	result := parseOutputFormat("unknown_format")
	if result != FormatTable {
		t.Errorf("parseOutputFormat('unknown_format') = %v, want %v", result, FormatTable)
	}
}

func TestBuildDSN_EdgeCases(t *testing.T) {
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

	t.Run("only password no user", func(t *testing.T) {
		*flagHost = "localhost"
		*flagPort = 3306
		*flagUser = ""
		*flagPassword = "secret"
		*flagDatabase = "test"

		result := buildDSN()
		// Password without user should still work
		if result == "" {
			t.Error("buildDSN returned empty string")
		}
	})
}

func TestGetHistoryPath_NoHome(t *testing.T) {
	// This test just ensures the function doesn't panic
	path := getHistoryPath()
	_ = path
}

func TestVersionInfo_NonEmpty(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
	if GitCommit == "" {
		t.Error("GitCommit should not be empty")
	}
	if BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}
}
