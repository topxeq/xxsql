// Package log provides logging facilities for XxSql.
package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Level represents a log level.
type Level int

const (
	// DEBUG level for detailed debugging information.
	DEBUG Level = iota
	// INFO level for general operational information.
	INFO
	// WARN level for warning messages.
	WARN
	// ERROR level for error messages.
	ERROR
	// FATAL level for fatal errors (exits the program).
	FATAL
)

// String returns the string representation of the log level.
func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a string into a log level.
func ParseLevel(s string) Level {
	switch s {
	case "DEBUG", "debug":
		return DEBUG
	case "INFO", "info":
		return INFO
	case "WARN", "warn", "WARNING", "warning":
		return WARN
	case "ERROR", "error":
		return ERROR
	case "FATAL", "fatal":
		return FATAL
	default:
		return INFO
	}
}

// Logger is a thread-safe logger with configurable output and rotation.
type Logger struct {
	mu          sync.Mutex
	level       Level
	output      io.Writer
	file        *os.File
	rotate      *Rotator
	showCaller  bool
	timeFormat  string
	prefix      string
}

// Option is a functional option for Logger configuration.
type Option func(*Logger)

// WithLevel sets the log level.
func WithLevel(level Level) Option {
	return func(l *Logger) {
		l.level = level
	}
}

// WithOutput sets the output writer.
func WithOutput(w io.Writer) Option {
	return func(l *Logger) {
		l.output = w
	}
}

// WithFile sets the log file path.
func WithFile(path string) Option {
	return func(l *Logger) {
		if path == "" {
			return
		}
		// Ensure directory exists
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err == nil {
			if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
				l.file = f
				l.output = f
			}
		}
	}
}

// WithRotation sets up log rotation.
func WithRotation(maxSizeMB, maxBackups, maxAgeDays int, compress bool) Option {
	return func(l *Logger) {
		if l.file != nil {
			l.rotate = NewRotator(maxSizeMB, maxBackups, maxAgeDays, compress)
		}
	}
}

// WithCaller enables/disables caller information.
func WithCaller(show bool) Option {
	return func(l *Logger) {
		l.showCaller = show
	}
}

// WithTimeFormat sets the time format for log messages.
func WithTimeFormat(format string) Option {
	return func(l *Logger) {
		l.timeFormat = format
	}
}

// WithPrefix sets a prefix for all log messages.
func WithPrefix(prefix string) Option {
	return func(l *Logger) {
		l.prefix = prefix
	}
}

// NewLogger creates a new Logger with the given options.
func NewLogger(opts ...Option) *Logger {
	l := &Logger{
		level:      INFO,
		output:     os.Stdout,
		timeFormat: "2006-01-02 15:04:05.000",
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// SetLevel sets the log level at runtime.
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetLevelString sets the log level from a string.
func (l *Logger) SetLevelString(level string) {
	l.SetLevel(ParseLevel(level))
}

// GetLevel returns the current log level.
func (l *Logger) GetLevel() Level {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// log writes a log message at the given level.
func (l *Logger) log(level Level, format string, args ...any) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Check for rotation
	if l.rotate != nil && l.file != nil {
		if l.rotate.ShouldRotate(l.file) {
			l.rotate.Rotate(l.file)
		}
	}

	// Build the log message
	now := time.Now().Format(l.timeFormat)
	msg := fmt.Sprintf(format, args...)

	var caller string
	if l.showCaller {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			caller = fmt.Sprintf(" %s:%d", filepath.Base(file), line)
		}
	}

	// Format: [TIME] [LEVEL] [PREFIX] MESSAGE CALLER
	var output string
	if l.prefix != "" {
		output = fmt.Sprintf("[%s] [%s] [%s] %s%s\n", now, level, l.prefix, msg, caller)
	} else {
		output = fmt.Sprintf("[%s] [%s] %s%s\n", now, level, msg, caller)
	}

	// Write to output
	l.output.Write([]byte(output))

	// Handle fatal
	if level == FATAL {
		os.Exit(1)
	}
}

// Debug logs a debug message.
func (l *Logger) Debug(format string, args ...any) {
	l.log(DEBUG, format, args...)
}

// Info logs an info message.
func (l *Logger) Info(format string, args ...any) {
	l.log(INFO, format, args...)
}

// Warn logs a warning message.
func (l *Logger) Warn(format string, args ...any) {
	l.log(WARN, format, args...)
}

// Error logs an error message.
func (l *Logger) Error(format string, args ...any) {
	l.log(ERROR, format, args...)
}

// Fatal logs a fatal message and exits.
func (l *Logger) Fatal(format string, args ...any) {
	l.log(FATAL, format, args...)
}

// Debugf logs a formatted debug message (alias for Debug).
func (l *Logger) Debugf(format string, args ...any) {
	l.Debug(format, args...)
}

// Infof logs a formatted info message (alias for Info).
func (l *Logger) Infof(format string, args ...any) {
	l.Info(format, args...)
}

// Warnf logs a formatted warning message (alias for Warn).
func (l *Logger) Warnf(format string, args ...any) {
	l.Warn(format, args...)
}

// Errorf logs a formatted error message (alias for Error).
func (l *Logger) Errorf(format string, args ...any) {
	l.Error(format, args...)
}

// Fatalf logs a formatted fatal message and exits (alias for Fatal).
func (l *Logger) Fatalf(format string, args ...any) {
	l.Fatal(format, args...)
}

// Close closes the log file if opened.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// Sync flushes any buffered log output.
func (l *Logger) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Sync()
	}
	if s, ok := l.output.(interface{ Sync() error }); ok {
		return s.Sync()
	}
	return nil
}

// Global logger instance
var globalLogger = NewLogger()

// SetGlobal sets the global logger instance.
func SetGlobal(l *Logger) {
	globalLogger = l
}

// GetGlobal returns the global logger instance.
func GetGlobal() *Logger {
	return globalLogger
}

// Debug logs a debug message using the global logger.
func Debug(format string, args ...any) {
	globalLogger.Debug(format, args...)
}

// Info logs an info message using the global logger.
func Info(format string, args ...any) {
	globalLogger.Info(format, args...)
}

// Warn logs a warning message using the global logger.
func Warn(format string, args ...any) {
	globalLogger.Warn(format, args...)
}

// Error logs an error message using the global logger.
func Error(format string, args ...any) {
	globalLogger.Error(format, args...)
}

// Fatal logs a fatal message using the global logger.
func Fatal(format string, args ...any) {
	globalLogger.Fatal(format, args...)
}