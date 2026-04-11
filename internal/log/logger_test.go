package log

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestLevel_String(t *testing.T) {
	tests := []struct {
		level Level
		want  string
	}{
		{DEBUG, "DEBUG"},
		{INFO, "INFO"},
		{WARN, "WARN"},
		{ERROR, "ERROR"},
		{FATAL, "FATAL"},
		{Level(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.level.String(); got != tt.want {
				t.Errorf("Level.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  Level
	}{
		{"DEBUG", DEBUG},
		{"debug", DEBUG},
		{"INFO", INFO},
		{"info", INFO},
		{"WARN", WARN},
		{"warn", WARN},
		{"WARNING", WARN},
		{"warning", WARN},
		{"ERROR", ERROR},
		{"error", ERROR},
		{"FATAL", FATAL},
		{"fatal", FATAL},
		{"unknown", INFO},
		{"", INFO},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseLevel(tt.input); got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	logger := NewLogger()
	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}
	if logger.level != INFO {
		t.Errorf("default level: got %v, want INFO", logger.level)
	}
}

func TestNewLogger_WithOptions(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(DEBUG),
		WithOutput(&buf),
		WithCaller(true),
		WithTimeFormat("2006-01-02"),
		WithPrefix("test"),
	)

	if logger.level != DEBUG {
		t.Errorf("level: got %v, want DEBUG", logger.level)
	}
	if logger.output != &buf {
		t.Error("output not set")
	}
	if !logger.showCaller {
		t.Error("showCaller should be true")
	}
	if logger.timeFormat != "2006-01-02" {
		t.Errorf("timeFormat: got %q, want '2006-01-02'", logger.timeFormat)
	}
	if logger.prefix != "test" {
		t.Errorf("prefix: got %q, want 'test'", logger.prefix)
	}
}

func TestLogger_SetLevel(t *testing.T) {
	logger := NewLogger()

	logger.SetLevel(DEBUG)
	if logger.GetLevel() != DEBUG {
		t.Errorf("GetLevel: got %v, want DEBUG", logger.GetLevel())
	}

	logger.SetLevelString("ERROR")
	if logger.GetLevel() != ERROR {
		t.Errorf("GetLevel after SetLevelString: got %v, want ERROR", logger.GetLevel())
	}
}

func TestLogger_Output(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(DEBUG),
		WithOutput(&buf),
	)

	tests := []struct {
		logFunc func(string, ...any)
		level   string
	}{
		{logger.Debug, "DEBUG"},
		{logger.Info, "INFO"},
		{logger.Warn, "WARN"},
		{logger.Error, "ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			buf.Reset()
			tt.logFunc("test message")
			output := buf.String()
			if !strings.Contains(output, tt.level) {
				t.Errorf("output should contain %q: %s", tt.level, output)
			}
			if !strings.Contains(output, "test message") {
				t.Errorf("output should contain message: %s", output)
			}
		})
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(WARN),
		WithOutput(&buf),
	)

	logger.Debug("debug message")
	if buf.Len() > 0 {
		t.Error("DEBUG should be filtered when level is WARN")
	}

	buf.Reset()
	logger.Info("info message")
	if buf.Len() > 0 {
		t.Error("INFO should be filtered when level is WARN")
	}

	buf.Reset()
	logger.Warn("warn message")
	if buf.Len() == 0 {
		t.Error("WARN should not be filtered")
	}

	buf.Reset()
	logger.Error("error message")
	if buf.Len() == 0 {
		t.Error("ERROR should not be filtered")
	}
}

func TestLogger_WithFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger := NewLogger(WithFile(logFile))
	defer logger.Close()

	logger.Info("test message")

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !strings.Contains(string(data), "test message") {
		t.Errorf("Log file should contain message: %s", string(data))
	}
}

func TestLogger_WithCaller(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(INFO),
		WithOutput(&buf),
		WithCaller(true),
	)

	logger.Info("test")

	output := buf.String()
	if !strings.Contains(output, "logger_test.go:") {
		t.Errorf("Output should contain caller info: %s", output)
	}
}

func TestLogger_WithPrefix(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(INFO),
		WithOutput(&buf),
		WithPrefix("MYPREFIX"),
	)

	logger.Info("test")

	output := buf.String()
	if !strings.Contains(output, "[MYPREFIX]") {
		t.Errorf("Output should contain prefix: %s", output)
	}
}

func TestLogger_Aliases(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(DEBUG),
		WithOutput(&buf),
	)

	logger.Debugf("debug %s", "test")
	if !strings.Contains(buf.String(), "debug test") {
		t.Error("Debugf failed")
	}

	buf.Reset()
	logger.Infof("info %s", "test")
	if !strings.Contains(buf.String(), "info test") {
		t.Error("Infof failed")
	}

	buf.Reset()
	logger.Warnf("warn %s", "test")
	if !strings.Contains(buf.String(), "warn test") {
		t.Error("Warnf failed")
	}

	buf.Reset()
	logger.Errorf("error %s", "test")
	if !strings.Contains(buf.String(), "error test") {
		t.Error("Errorf failed")
	}
}

func TestLogger_Concurrent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(
		WithLevel(INFO),
		WithOutput(&buf),
	)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			logger.Info("concurrent message %d", n)
		}(i)
	}
	wg.Wait()

	output := buf.String()
	count := strings.Count(output, "concurrent message")
	if count != 100 {
		t.Errorf("Expected 100 messages, got %d", count)
	}
}

func TestLogger_Close(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger := NewLogger(WithFile(logFile))

	if err := logger.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	logger2 := NewLogger()
	if err := logger2.Close(); err != nil {
		t.Errorf("Close on logger without file should not error: %v", err)
	}
}

func TestLogger_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger := NewLogger(WithFile(logFile))
	logger.Info("test")

	if err := logger.Sync(); err != nil {
		t.Errorf("Sync returned error: %v", err)
	}

	var buf bytes.Buffer
	logger2 := NewLogger(WithOutput(&buf))
	if err := logger2.Sync(); err != nil {
		t.Errorf("Sync on logger with buffer should not error: %v", err)
	}
}

func TestGlobalLogger(t *testing.T) {
	original := GetGlobal()
	defer SetGlobal(original)

	var buf bytes.Buffer
	testLogger := NewLogger(
		WithLevel(DEBUG),
		WithOutput(&buf),
	)
	SetGlobal(testLogger)

	Debug("global debug")
	if !strings.Contains(buf.String(), "global debug") {
		t.Error("Global Debug failed")
	}

	buf.Reset()
	Info("global info")
	if !strings.Contains(buf.String(), "global info") {
		t.Error("Global Info failed")
	}

	buf.Reset()
	Warn("global warn")
	if !strings.Contains(buf.String(), "global warn") {
		t.Error("Global Warn failed")
	}

	buf.Reset()
	Error("global error")
	if !strings.Contains(buf.String(), "global error") {
		t.Error("Global Error failed")
	}
}

func TestWithRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger := NewLogger(
		WithFile(logFile),
		WithRotation(1, 5, 7, false),
	)
	defer logger.Close()

	if logger.rotate == nil {
		t.Error("Rotation should be enabled")
	}

	logger.Info("test message")
}
