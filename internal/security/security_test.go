package security

import (
	"os"
	"testing"
	"time"
)

// ============================================================================
// Audit Logger Tests
// ============================================================================

func TestAuditLogger(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := &AuditConfig{
		Enabled:    true,
		FilePath:   tmpFile.Name(),
		MaxSizeMB:  10,
		MaxBackups: 2,
		FlushIntMs: 100,
	}

	audit, err := NewAuditLogger(cfg)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer audit.Close()

	// Test logging
	audit.Log(&Event{
		EventType: EventLoginSuccess,
		Severity:  SeverityInfo,
		User:      "testuser",
		SourceIP:  "127.0.0.1",
		Message:   "Test login",
	})

	// Flush and verify
	if err := audit.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Read file and verify
	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to read audit file: %v", err)
	}

	if len(data) == 0 {
		t.Error("Audit file is empty")
	}
}

func TestAuditLogSimple(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := &AuditConfig{
		Enabled:  true,
		FilePath: tmpFile.Name(),
	}
	audit, _ := NewAuditLogger(cfg)
	defer audit.Close()

	audit.LogSimple(EventLoginFailed, SeverityWarning, "admin", "192.168.1.1", "Invalid password")
	audit.Flush()

	events, err := audit.Query(&AuditFilter{User: "admin"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	if events[0].EventType != EventLoginFailed {
		t.Errorf("Expected EventLoginFailed, got %v", events[0].EventType)
	}
}

func TestAuditFilter(t *testing.T) {
	now := time.Now()
	events := []Event{
		{EventType: EventLoginSuccess, User: "alice", Severity: SeverityInfo, Timestamp: now},
		{EventType: EventLoginFailed, User: "bob", Severity: SeverityWarning, Timestamp: now},
		{EventType: EventLoginSuccess, User: "alice", Severity: SeverityInfo, Timestamp: now.Add(-time.Hour)},
	}

	tests := []struct {
		name   string
		filter *AuditFilter
		count  int
	}{
		{"no filter", nil, 3},
		{"filter by user", &AuditFilter{User: "alice"}, 2},
		{"filter by event", &AuditFilter{EventType: eventTypePtr(EventLoginFailed)}, 1},
		{"filter by severity", &AuditFilter{Severity: severityPtr(SeverityWarning)}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := 0
			for _, event := range events {
				if tt.filter.Match(&event) {
					matched++
				}
			}
			if matched != tt.count {
				t.Errorf("Expected %d matches, got %d", tt.count, matched)
			}
		})
	}
}

func eventTypePtr(e EventType) *EventType {
	return &e
}

func severityPtr(s Severity) *Severity {
	return &s
}

// ============================================================================
// Rate Limiter Tests
// ============================================================================

func TestRateLimiterBasic(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:       true,
		MaxAttempts:   3,
		WindowSize:    time.Minute,
		BlockDuration: 5 * time.Minute,
		CleanupInt:    time.Minute,
	}

	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	ip := "192.168.1.100"

	// Should be allowed initially
	if !rl.CheckAllowed(ip) {
		t.Error("IP should be allowed initially")
	}

	// Record attempts
	rl.RecordAttempt(ip, "user1")
	rl.RecordAttempt(ip, "user1")

	// Should still be allowed
	if !rl.CheckAllowed(ip) {
		t.Error("IP should be allowed after 2 attempts")
	}

	// Record third attempt - should trigger block
	blocked := rl.RecordAttempt(ip, "user1")
	if !blocked {
		t.Error("IP should be blocked after 3 attempts")
	}

	// Should be blocked now
	if rl.CheckAllowed(ip) {
		t.Error("IP should be blocked")
	}

	// Record success should clear
	rl.RecordSuccess(ip)
	if !rl.CheckAllowed(ip) {
		t.Error("IP should be allowed after success")
	}
}

func TestRateLimiterRemainingAttempts(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:     true,
		MaxAttempts: 5,
		WindowSize:  time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	ip := "10.0.0.1"

	if rl.GetRemainingAttempts(ip) != 5 {
		t.Errorf("Expected 5 remaining, got %d", rl.GetRemainingAttempts(ip))
	}

	rl.RecordAttempt(ip, "user")
	if rl.GetRemainingAttempts(ip) != 4 {
		t.Errorf("Expected 4 remaining, got %d", rl.GetRemainingAttempts(ip))
	}
}

// ============================================================================
// IP Access Tests
// ============================================================================

func TestIPAccessAllowAll(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode: AccessModeAllowAll,
	}

	ipa, err := NewIPAccess(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to create IP access: %v", err)
	}

	tests := []string{
		"192.168.1.1",
		"10.0.0.1",
		"172.16.0.1",
		"8.8.8.8",
	}

	for _, ip := range tests {
		if !ipa.IsAllowed(ip) {
			t.Errorf("IP %s should be allowed in allow_all mode", ip)
		}
	}
}

func TestIPAccessWhitelist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeWhitelist,
		Whitelist: []string{"192.168.1.100", "10.0.0.0/8"},
	}

	ipa, err := NewIPAccess(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to create IP access: %v", err)
	}

	// Should allow whitelisted IP
	if !ipa.IsAllowed("192.168.1.100") {
		t.Error("Whitelisted IP should be allowed")
	}

	// Should allow IP in whitelisted CIDR
	if !ipa.IsAllowed("10.0.0.50") {
		t.Error("IP in whitelisted CIDR should be allowed")
	}

	// Should block non-whitelisted IP
	if ipa.IsAllowed("8.8.8.8") {
		t.Error("Non-whitelisted IP should be blocked")
	}
}

func TestIPAccessBlacklist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeBlacklist,
		Blacklist: []string{"192.168.1.100", "10.0.0.0/8"},
	}

	ipa, err := NewIPAccess(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to create IP access: %v", err)
	}

	// Should block blacklisted IP
	if ipa.IsAllowed("192.168.1.100") {
		t.Error("Blacklisted IP should be blocked")
	}

	// Should block IP in blacklisted CIDR
	if ipa.IsAllowed("10.0.0.50") {
		t.Error("IP in blacklisted CIDR should be blocked")
	}

	// Should allow non-blacklisted IP
	if !ipa.IsAllowed("8.8.8.8") {
		t.Error("Non-blacklisted IP should be allowed")
	}
}

func TestIPAccessDynamicBlacklist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode: AccessModeAllowAll,
	}

	ipa, _ := NewIPAccess(cfg, nil)

	// Add to blacklist
	ipa.AddToBlacklist("192.168.1.200")

	if ipa.IsAllowed("192.168.1.200") {
		t.Error("Blacklisted IP should be blocked")
	}

	// Remove from blacklist
	ipa.RemoveFromBlacklist("192.168.1.200")

	if !ipa.IsAllowed("192.168.1.200") {
		t.Error("IP should be allowed after removing from blacklist")
	}
}

// ============================================================================
// Password Policy Tests
// ============================================================================

func TestPasswordValidation(t *testing.T) {
	cfg := &PasswordPolicy{
		MinLength:        8,
		MaxLength:        128,
		RequireUppercase: true,
		RequireLowercase: true,
		RequireDigit:     true,
		RequireSpecial:   false,
	}

	validator := NewPasswordValidator(cfg, nil)

	tests := []struct {
		password string
		valid    bool
	}{
		{"short", false},                    // Too short
		{"alllowercase1", false},             // No uppercase
		{"ALLUPPERCASE1", false},             // No lowercase
		{"NoDigitsHere", false},              // No digits
		{"ValidPass1", true},                 // Valid (no special required)
		{"AnotherValid2", true},              // Valid
	}

	for _, tt := range tests {
		err := validator.Validate(tt.password)
		if (err == nil) != tt.valid {
			t.Errorf("Password %q: expected valid=%v, got err=%v", tt.password, tt.valid, err)
		}
	}
}

func TestPasswordStrength(t *testing.T) {
	tests := []struct {
		password    string
		minStrength PasswordStrength
	}{
		{"password", StrengthVeryWeak},
		{"Password1", StrengthVeryWeak},
		{"Password1!", StrengthWeak},
		{"MySecure@Pass123", StrengthMedium},
	}

	for _, tt := range tests {
		strength := CheckStrength(tt.password)
		if strength < tt.minStrength {
			t.Errorf("Password %q: expected at least %v, got %v", tt.password, tt.minStrength, strength)
		}
	}
}

func TestPasswordExpiration(t *testing.T) {
	cfg := &PasswordPolicy{
		ExpireDays: 30,
	}

	validator := NewPasswordValidator(cfg, nil)

	// Record password change
	validator.RecordPasswordChange("testuser", "hash123")

	// Should not be expired
	if validator.IsPasswordExpired("testuser") {
		t.Error("Password should not be expired immediately after change")
	}

	// Check expiry is set
	expiry := validator.GetPasswordExpiry("testuser")
	if expiry.IsZero() {
		t.Error("Expiry should be set")
	}

	// Days until expiry should be around 30
	days := validator.DaysUntilExpiry("testuser")
	if days < 29 || days > 30 {
		t.Errorf("Expected ~30 days until expiry, got %d", days)
	}
}

func TestPasswordHistory(t *testing.T) {
	cfg := &PasswordPolicy{
		MinLength:    4,
		HistoryCount: 3,
	}

	validator := NewPasswordValidator(cfg, nil)

	// Record password changes
	validator.RecordPasswordChange("user1", "pass1")
	validator.RecordPasswordChange("user1", "pass2")
	validator.RecordPasswordChange("user1", "pass3")

	// These should be rejected (in history)
	if err := validator.ValidateForUser("user1", "pass1"); err == nil {
		t.Error("Should reject password in history")
	}
	if err := validator.ValidateForUser("user1", "pass2"); err == nil {
		t.Error("Should reject password in history")
	}

	// This should be accepted (not in history)
	if err := validator.ValidateForUser("user1", "pass4"); err != nil {
		t.Errorf("Should accept new password: %v", err)
	}
}

// ============================================================================
// TLS Tests
// ============================================================================

func TestTLSMode(t *testing.T) {
	tests := []struct {
		mode    TLSMode
		str     string
		enabled bool
	}{
		{TLSModeDisabled, "DISABLED", false},
		{TLSModeOptional, "OPTIONAL", true},
		{TLSModeRequired, "REQUIRED", true},
		{TLSModeVerifyCA, "VERIFY_CA", true},
	}

	for _, tt := range tests {
		if tt.mode.String() != tt.str {
			t.Errorf("Expected %q, got %q", tt.str, tt.mode.String())
		}
	}
}

func TestTLSConfigDisabled(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
	}

	tm, err := NewTLSManager(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to create TLS manager: %v", err)
	}

	if tm.IsEnabled() {
		t.Error("TLS should be disabled")
	}

	if tm.IsTLSRequired() {
		t.Error("TLS should not be required when disabled")
	}
}

// ============================================================================
// Security Manager Tests
// ============================================================================

func TestSecurityManager(t *testing.T) {
	cfg := &SecurityConfig{
		Audit: &AuditConfig{
			Enabled:  true,
			FilePath: os.TempDir() + "/test-audit.log",
		},
		RateLimit: &RateLimitConfig{
			Enabled:     true,
			MaxAttempts: 5,
			WindowSize:  time.Minute,
		},
		Password: &PasswordPolicy{
			MinLength: 8,
		},
	}

	sm, err := NewSecurityManager(cfg)
	if err != nil {
		t.Fatalf("Failed to create security manager: %v", err)
	}
	defer sm.Close()

	// Test connection check (should pass)
	if err := sm.CheckConnection("192.168.1.1"); err != nil {
		t.Errorf("Connection check should pass: %v", err)
	}

	// Test password validation
	if err := sm.ValidatePassword("short"); err == nil {
		t.Error("Short password should be rejected")
	}

	if err := sm.ValidatePassword("validpassword123"); err != nil {
		t.Errorf("Valid password should be accepted: %v", err)
	}
}

func TestSecurityManagerAuditEvents(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := &SecurityConfig{
		Audit: &AuditConfig{
			Enabled:  true,
			FilePath: tmpFile.Name(),
		},
	}

	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	// Test auth events
	sm.RecordAuthFailure("10.0.0.1", "testuser")
	sm.RecordAuthSuccess("10.0.0.1", "testuser")
	sm.RecordLogout("10.0.0.1", "testuser")

	// Test user events
	sm.LogUserCreated("admin", "newuser")
	sm.LogUserDeleted("admin", "olduser")

	// Test permission events
	sm.LogPermissionGranted("admin", "user1", "SELECT")
	sm.LogPermissionRevoked("admin", "user1", "DELETE")

	sm.GetAuditLogger().Flush()

	// Verify events were logged
	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit log should contain events")
	}
}
