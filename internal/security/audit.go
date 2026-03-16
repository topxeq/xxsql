// Package security provides security features for XxSql.
package security

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// EventType represents an audit event type.
type EventType int

const (
	EventLoginSuccess EventType = iota
	EventLoginFailed
	EventLogout
	EventPasswordChange
	EventUserCreated
	EventUserDeleted
	EventPermissionGranted
	EventPermissionRevoked
	EventConnectionRejected
	EventRateLimitExceeded
	EventIPBlocked
	EventTLSHandshake
	EventTLSHandshakeFailed
)

// String returns the string representation of the event type.
func (e EventType) String() string {
	switch e {
	case EventLoginSuccess:
		return "LOGIN_SUCCESS"
	case EventLoginFailed:
		return "LOGIN_FAILED"
	case EventLogout:
		return "LOGOUT"
	case EventPasswordChange:
		return "PASSWORD_CHANGE"
	case EventUserCreated:
		return "USER_CREATED"
	case EventUserDeleted:
		return "USER_DELETED"
	case EventPermissionGranted:
		return "PERMISSION_GRANTED"
	case EventPermissionRevoked:
		return "PERMISSION_REVOKED"
	case EventConnectionRejected:
		return "CONNECTION_REJECTED"
	case EventRateLimitExceeded:
		return "RATE_LIMIT_EXCEEDED"
	case EventIPBlocked:
		return "IP_BLOCKED"
	case EventTLSHandshake:
		return "TLS_HANDSHAKE"
	case EventTLSHandshakeFailed:
		return "TLS_HANDSHAKE_FAILED"
	default:
		return "UNKNOWN"
	}
}

// Severity represents the severity level of an audit event.
type Severity int

const (
	SeverityInfo Severity = iota
	SeverityWarning
	SeverityCritical
)

// String returns the string representation of the severity.
func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// Event represents an audit event.
type Event struct {
	Timestamp   time.Time              `json:"timestamp"`
	EventType   EventType              `json:"event_type"`
	Severity    Severity               `json:"severity"`
	User        string                 `json:"user,omitempty"`
	SourceIP    string                 `json:"source_ip,omitempty"`
	ConnectionID uint64                `json:"connection_id,omitempty"`
	Database    string                 `json:"database,omitempty"`
	Table       string                 `json:"table,omitempty"`
	Permission  string                 `json:"permission,omitempty"`
	Message     string                 `json:"message,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// AuditLogger provides audit logging functionality.
type AuditLogger struct {
	mu         sync.Mutex
	file       *os.File
	filePath   string
	maxSize    int64  // Max file size in bytes
	maxBackups int
	buffer     []Event
	flushInt   time.Duration
	stopCh     chan struct{}
}

// AuditConfig contains configuration for audit logging.
type AuditConfig struct {
	Enabled    bool
	FilePath   string
	MaxSizeMB  int
	MaxBackups int
	FlushIntMs int
}

// DefaultAuditConfig returns default audit configuration.
func DefaultAuditConfig() *AuditConfig {
	return &AuditConfig{
		Enabled:    true,
		FilePath:   "audit.log",
		MaxSizeMB:  100,
		MaxBackups: 10,
		FlushIntMs: 100,
	}
}

// NewAuditLogger creates a new audit logger.
func NewAuditLogger(cfg *AuditConfig) (*AuditLogger, error) {
	if cfg == nil {
		cfg = DefaultAuditConfig()
	}

	// Ensure flush interval is positive
	flushInt := time.Duration(cfg.FlushIntMs) * time.Millisecond
	if flushInt <= 0 {
		flushInt = 100 * time.Millisecond
	}

	al := &AuditLogger{
		filePath:   cfg.FilePath,
		maxSize:    int64(cfg.MaxSizeMB) * 1024 * 1024,
		maxBackups: cfg.MaxBackups,
		buffer:     make([]Event, 0, 100),
		flushInt:   flushInt,
		stopCh:     make(chan struct{}),
	}

	// Ensure directory exists
	dir := filepath.Dir(cfg.FilePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create audit log directory: %w", err)
		}
	}

	// Open file
	if err := al.openFile(); err != nil {
		return nil, err
	}

	// Start background flush
	go al.backgroundFlush()

	return al, nil
}

// openFile opens the audit log file.
func (al *AuditLogger) openFile() error {
	f, err := os.OpenFile(al.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}
	al.file = f
	return nil
}

// Log records an audit event.
func (al *AuditLogger) Log(event *Event) {
	if event == nil {
		return
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	al.mu.Lock()
	al.buffer = append(al.buffer, *event)
	shouldFlush := len(al.buffer) >= 100
	al.mu.Unlock()

	if shouldFlush {
		al.Flush()
	}
}

// LogSimple records a simple audit event.
func (al *AuditLogger) LogSimple(eventType EventType, severity Severity, user, sourceIP, message string) {
	al.Log(&Event{
		EventType: eventType,
		Severity:  severity,
		User:      user,
		SourceIP:  sourceIP,
		Message:   message,
	})
}

// Flush writes all buffered events to the file.
func (al *AuditLogger) Flush() error {
	al.mu.Lock()
	defer al.mu.Unlock()

	if len(al.buffer) == 0 {
		return nil
	}

	// Check if rotation is needed
	if err := al.checkRotation(); err != nil {
		return err
	}

	// Write events
	for _, event := range al.buffer {
		data, err := json.Marshal(event)
		if err != nil {
			continue
		}
		if _, err := al.file.Write(append(data, '\n')); err != nil {
			return err
		}
	}

	// Clear buffer
	al.buffer = al.buffer[:0]

	return al.file.Sync()
}

// checkRotation checks if log rotation is needed.
func (al *AuditLogger) checkRotation() error {
	if al.maxSize <= 0 {
		return nil
	}

	info, err := al.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() < al.maxSize {
		return nil
	}

	// Close current file
	al.file.Close()

	// Rotate files
	for i := al.maxBackups - 1; i > 0; i-- {
		oldPath := fmt.Sprintf("%s.%d", al.filePath, i)
		newPath := fmt.Sprintf("%s.%d", al.filePath, i+1)
		os.Rename(oldPath, newPath)
	}

	// Rename current file to .1
	os.Rename(al.filePath, al.filePath+".1")

	// Open new file
	return al.openFile()
}

// backgroundFlush periodically flushes the buffer.
func (al *AuditLogger) backgroundFlush() {
	ticker := time.NewTicker(al.flushInt)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			al.Flush()
		case <-al.stopCh:
			al.Flush()
			return
		}
	}
}

// Close closes the audit logger.
func (al *AuditLogger) Close() error {
	close(al.stopCh)
	al.mu.Lock()
	defer al.mu.Unlock()
	if al.file != nil {
		return al.file.Close()
	}
	return nil
}

// Query retrieves audit events matching the given criteria.
func (al *AuditLogger) Query(filter *AuditFilter) ([]Event, error) {
	if filter == nil {
		filter = &AuditFilter{}
	}

	al.mu.Lock()
	defer al.mu.Unlock()

	// Read the file
	data, err := os.ReadFile(al.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var events []Event
	lines := splitLines(string(data))

	for _, line := range lines {
		if line == "" {
			continue
		}

		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		// Apply filters
		if !filter.Match(&event) {
			continue
		}

		events = append(events, event)
	}

	return events, nil
}

// AuditFilter provides filtering for audit queries.
type AuditFilter struct {
	EventType   *EventType
	Severity    *Severity
	User        string
	SourceIP    string
	StartTime   *time.Time
	EndTime     *time.Time
	Limit       int
}

// Match checks if an event matches the filter.
func (f *AuditFilter) Match(event *Event) bool {
	if f == nil {
		return true
	}

	if f.EventType != nil && event.EventType != *f.EventType {
		return false
	}

	if f.Severity != nil && event.Severity != *f.Severity {
		return false
	}

	if f.User != "" && event.User != f.User {
		return false
	}

	if f.SourceIP != "" && event.SourceIP != f.SourceIP {
		return false
	}

	if f.StartTime != nil && event.Timestamp.Before(*f.StartTime) {
		return false
	}

	if f.EndTime != nil && event.Timestamp.After(*f.EndTime) {
		return false
	}

	return true
}

// splitLines splits a string into lines.
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
