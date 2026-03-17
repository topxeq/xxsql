package security

import (
	"net"
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
		{"short", false},         // Too short
		{"alllowercase1", false}, // No uppercase
		{"ALLUPPERCASE1", false}, // No lowercase
		{"NoDigitsHere", false},  // No digits
		{"ValidPass1", true},     // Valid (no special required)
		{"AnotherValid2", true},  // Valid
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

func TestDefaultSecurityConfig(t *testing.T) {
	cfg := DefaultSecurityConfig()

	if cfg == nil {
		t.Fatal("DefaultSecurityConfig returned nil")
	}
	if cfg.Audit == nil {
		t.Error("Audit config should not be nil")
	}
	if cfg.RateLimit == nil {
		t.Error("RateLimit config should not be nil")
	}
	if cfg.IPAccess == nil {
		t.Error("IPAccess config should not be nil")
	}
	if cfg.Password == nil {
		t.Error("Password config should not be nil")
	}
	if cfg.TLS == nil {
		t.Error("TLS config should not be nil")
	}
}

func TestNewSecurityManager_NilConfig(t *testing.T) {
	sm, err := NewSecurityManager(nil)
	if err != nil {
		t.Fatalf("NewSecurityManager with nil config should succeed: %v", err)
	}
	if sm == nil {
		t.Fatal("SecurityManager should not be nil")
	}
	defer sm.Close()
}

func TestSecurityManager_WithoutComponents(t *testing.T) {
	sm, _ := NewSecurityManager(&SecurityConfig{})
	defer sm.Close()

	// Validate password should return nil when no validator
	if err := sm.ValidatePassword("anypassword"); err != nil {
		t.Errorf("ValidatePassword should return nil without validator: %v", err)
	}

	// ValidatePasswordForUser should return nil when no validator
	if err := sm.ValidatePasswordForUser("user", "pass"); err != nil {
		t.Errorf("ValidatePasswordForUser should return nil without validator: %v", err)
	}

	// IsPasswordExpired should return false without validator
	if sm.IsPasswordExpired("user") {
		t.Error("IsPasswordExpired should return false without validator")
	}

	// DaysUntilPasswordExpiry should return -1 without validator
	if sm.DaysUntilPasswordExpiry("user") != -1 {
		t.Errorf("DaysUntilPasswordExpiry should return -1 without validator, got %d", sm.DaysUntilPasswordExpiry("user"))
	}

	// GetTLSConfig should return nil without TLS
	if sm.GetTLSConfig() != nil {
		t.Error("GetTLSConfig should return nil without TLS")
	}

	// IsTLSEnabled should return false without TLS
	if sm.IsTLSEnabled() {
		t.Error("IsTLSEnabled should return false without TLS")
	}

	// IsTLSRequired should return false without TLS
	if sm.IsTLSRequired() {
		t.Error("IsTLSRequired should return false without TLS")
	}
}

func TestSecurityManager_IPBlacklistWhitelist(t *testing.T) {
	cfg := &SecurityConfig{
		IPAccess: &IPAccessConfig{
			Mode: AccessModeAllowAll,
		},
	}

	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	// Add to blacklist
	if err := sm.AddIPToBlacklist("10.0.0.1"); err != nil {
		t.Errorf("AddIPToBlacklist failed: %v", err)
	}

	// Remove from blacklist
	sm.RemoveIPFromBlacklist("10.0.0.1")

	// Add to whitelist
	if err := sm.AddIPToWhitelist("192.168.1.1"); err != nil {
		t.Errorf("AddIPToWhitelist failed: %v", err)
	}

	// Remove from whitelist
	sm.RemoveIPFromWhitelist("192.168.1.1")
}

func TestSecurityManager_IPBlacklistWhitelist_WithoutIPAccess(t *testing.T) {
	sm, _ := NewSecurityManager(&SecurityConfig{})
	defer sm.Close()

	// Add to blacklist should fail without IPAccess
	if err := sm.AddIPToBlacklist("10.0.0.1"); err == nil {
		t.Error("AddIPToBlacklist should fail without IPAccess")
	}

	// Add to whitelist should fail without IPAccess
	if err := sm.AddIPToWhitelist("192.168.1.1"); err == nil {
		t.Error("AddIPToWhitelist should fail without IPAccess")
	}
}

func TestSecurityManager_Getters(t *testing.T) {
	cfg := &SecurityConfig{
		IPAccess:  &IPAccessConfig{Mode: AccessModeAllowAll},
		RateLimit: &RateLimitConfig{Enabled: true, MaxAttempts: 5, WindowSize: time.Minute},
		Password:  &PasswordPolicy{MinLength: 8},
	}

	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	if sm.GetRateLimiter() == nil {
		t.Error("GetRateLimiter should not return nil")
	}
	if sm.GetPasswordValidator() == nil {
		t.Error("GetPasswordValidator should not return nil")
	}
	if sm.GetIPAccess() == nil {
		t.Error("GetIPAccess should not return nil")
	}
}

func TestSecurityManager_RecordPasswordChange(t *testing.T) {
	cfg := &SecurityConfig{
		Password: &PasswordPolicy{MinLength: 8},
	}

	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	sm.RecordPasswordChange("testuser", "hash123")

	if sm.IsPasswordExpired("testuser") {
		t.Error("Password should not be expired after just being set")
	}
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		name     string
		addr     net.Addr
		expected string
	}{
		{
			name:     "TCPAddr",
			addr:     &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 3306},
			expected: "192.168.1.1",
		},
		{
			name:     "UDPAddr",
			addr:     &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53},
			expected: "10.0.0.1",
		},
		{
			name:     "IPAddr",
			addr:     &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			expected: "127.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractIP(tt.addr)
			if result != tt.expected {
				t.Errorf("ExtractIP: got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestSecurityManager_CheckConnection_RateLimited(t *testing.T) {
	cfg := &SecurityConfig{
		RateLimit: &RateLimitConfig{
			Enabled:       true,
			MaxAttempts:   2,
			WindowSize:    time.Minute,
			BlockDuration: time.Minute,
		},
	}

	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	ip := "192.168.1.100"

	// First connection should pass
	if err := sm.CheckConnection(ip); err != nil {
		t.Errorf("First connection should pass: %v", err)
	}

	// Record failures
	sm.RecordAuthFailure(ip, "user1")
	sm.RecordAuthFailure(ip, "user1")

	// Now should be blocked
	if err := sm.CheckConnection(ip); err == nil {
		t.Error("Connection should be blocked after rate limit")
	}
}

func TestSecurityManager_CheckConnection_IPBlocked(t *testing.T) {
	cfg := &SecurityConfig{
		IPAccess: &IPAccessConfig{
			Mode:      AccessModeBlacklist,
			Blacklist: []string{"10.0.0.1"},
		},
	}

	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	// Blocked IP should fail
	if err := sm.CheckConnection("10.0.0.1"); err == nil {
		t.Error("Connection from blacklisted IP should fail")
	}

	// Non-blocked IP should pass
	if err := sm.CheckConnection("10.0.0.2"); err != nil {
		t.Errorf("Connection from non-blacklisted IP should pass: %v", err)
	}
}

// ============================================================================
// Additional IPAccess Tests
// ============================================================================

func TestIPAccess_SetMode(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode: AccessModeAllowAll,
	}
	ipa, _ := NewIPAccess(cfg, nil)

	// Change to whitelist mode
	ipa.SetMode(AccessModeWhitelist)
	if ipa.GetMode() != AccessModeWhitelist {
		t.Errorf("Mode should be whitelist, got %v", ipa.GetMode())
	}

	// Change to blacklist mode
	ipa.SetMode(AccessModeBlacklist)
	if ipa.GetMode() != AccessModeBlacklist {
		t.Errorf("Mode should be blacklist, got %v", ipa.GetMode())
	}
}

func TestIPAccess_GetWhitelistBlacklist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeWhitelist,
		Whitelist: []string{"192.168.1.1"},
		Blacklist: []string{"10.0.0.1"},
	}
	ipa, _ := NewIPAccess(cfg, nil)

	whitelist := ipa.GetWhitelist()
	if len(whitelist) != 1 || whitelist[0] != "192.168.1.1" {
		t.Errorf("Whitelist should contain 192.168.1.1, got %v", whitelist)
	}

	blacklist := ipa.GetBlacklist()
	if len(blacklist) != 1 || blacklist[0] != "10.0.0.1" {
		t.Errorf("Blacklist should contain 10.0.0.1, got %v", blacklist)
	}
}

func TestIPAccess_ClearWhitelistBlacklist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeWhitelist,
		Whitelist: []string{"192.168.1.1"},
		Blacklist: []string{"10.0.0.1"},
	}
	ipa, _ := NewIPAccess(cfg, nil)

	ipa.ClearWhitelist()
	if len(ipa.GetWhitelist()) != 0 {
		t.Error("Whitelist should be empty after clear")
	}

	ipa.ClearBlacklist()
	if len(ipa.GetBlacklist()) != 0 {
		t.Error("Blacklist should be empty after clear")
	}
}

func TestIPAccess_Stats(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeBlacklist,
		Blacklist: []string{"10.0.0.1"},
	}
	ipa, _ := NewIPAccess(cfg, nil)

	stats := ipa.Stats()
	if stats.Mode != AccessModeBlacklist {
		t.Errorf("Stats mode should be blacklist, got %v", stats.Mode)
	}
}

// ============================================================================
// Additional Password Tests
// ============================================================================

func TestPasswordValidator_ClearHistory(t *testing.T) {
	cfg := &PasswordPolicy{
		MinLength:    4,
		HistoryCount: 3,
	}
	validator := NewPasswordValidator(cfg, nil)

	validator.RecordPasswordChange("user1", "pass1")
	validator.RecordPasswordChange("user1", "pass2")

	// Clear history
	validator.ClearHistory("user1")

	// Now pass1 should be allowed
	if err := validator.ValidateForUser("user1", "pass1"); err != nil {
		t.Errorf("pass1 should be allowed after clearing history: %v", err)
	}
}

func TestPasswordValidator_GetSetPolicy(t *testing.T) {
	cfg := &PasswordPolicy{
		MinLength: 8,
	}
	validator := NewPasswordValidator(cfg, nil)

	// Get policy
	policy := validator.GetPolicy()
	if policy.MinLength != 8 {
		t.Errorf("MinLength should be 8, got %d", policy.MinLength)
	}

	// Set new policy
	newPolicy := &PasswordPolicy{
		MinLength: 10,
	}
	validator.SetPolicy(newPolicy)

	policy = validator.GetPolicy()
	if policy.MinLength != 10 {
		t.Errorf("MinLength should be 10, got %d", policy.MinLength)
	}
}

func TestPasswordStrength_String(t *testing.T) {
	tests := []struct {
		strength PasswordStrength
		expected string
	}{
		{StrengthVeryWeak, "Very Weak"},
		{StrengthWeak, "Weak"},
		{StrengthMedium, "Medium"},
		{StrengthStrong, "Strong"},
		{StrengthVeryStrong, "Very Strong"},
	}

	for _, tt := range tests {
		if tt.strength.String() != tt.expected {
			t.Errorf("Strength.String(): got %q, want %q", tt.strength.String(), tt.expected)
		}
	}
}

func TestPasswordValidator_GenerateRandomPassword(t *testing.T) {
	password, err := GenerateRandomPassword(&PasswordPolicy{MinLength: 16})
	if err != nil {
		t.Fatalf("GenerateRandomPassword failed: %v", err)
	}
	if len(password) != 16 {
		t.Errorf("Password length should be 16, got %d", len(password))
	}
}

func TestPasswordValidator_GenerateResetToken(t *testing.T) {
	token, err := GenerateResetToken()
	if err != nil {
		t.Fatalf("GenerateResetToken failed: %v", err)
	}
	if len(token) == 0 {
		t.Error("Token should not be empty")
	}
}

// ============================================================================
// Additional Event/Severity Tests
// ============================================================================

func TestEventType_String(t *testing.T) {
	tests := []struct {
		event    EventType
		expected string
	}{
		{EventLoginSuccess, "LOGIN_SUCCESS"},
		{EventLoginFailed, "LOGIN_FAILED"},
		{EventLogout, "LOGOUT"},
		{EventUserCreated, "USER_CREATED"},
		{EventUserDeleted, "USER_DELETED"},
		{EventPermissionGranted, "PERMISSION_GRANTED"},
		{EventPermissionRevoked, "PERMISSION_REVOKED"},
	}

	for _, tt := range tests {
		if tt.event.String() != tt.expected {
			t.Errorf("EventType.String(): got %q, want %q", tt.event.String(), tt.expected)
		}
	}
}

func TestSeverity_String(t *testing.T) {
	tests := []struct {
		severity Severity
		expected string
	}{
		{SeverityInfo, "INFO"},
		{SeverityWarning, "WARNING"},
		{SeverityCritical, "CRITICAL"},
		{SeverityCritical, "CRITICAL"},
	}

	for _, tt := range tests {
		if tt.severity.String() != tt.expected {
			t.Errorf("Severity.String(): got %q, want %q", tt.severity.String(), tt.expected)
		}
	}
}

// ============================================================================
// Rate Limiter Additional Tests
// ============================================================================

func TestRateLimiter_Unblock(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:       true,
		MaxAttempts:   2,
		WindowSize:    time.Minute,
		BlockDuration: time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	ip := "192.168.1.100"

	// Block the IP
	rl.RecordAttempt(ip, "user1")
	rl.RecordAttempt(ip, "user1")

	if rl.CheckAllowed(ip) {
		t.Error("IP should be blocked")
	}

	// Unblock
	rl.Unblock(ip)

	if !rl.CheckAllowed(ip) {
		t.Error("IP should be allowed after unblock")
	}
}

func TestRateLimiter_Clear(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:     true,
		MaxAttempts: 3,
		WindowSize:  time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	// Record some attempts
	rl.RecordAttempt("192.168.1.1", "user1")
	rl.RecordAttempt("192.168.1.2", "user2")

	// Clear all
	rl.Clear("192.168.1.1")

	// All IPs should be allowed
	if rl.GetRemainingAttempts("192.168.1.1") != 3 {
		t.Error("All attempts should be cleared")
	}
}

func TestRateLimiter_Stats(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:     true,
		MaxAttempts: 3,
		WindowSize:  time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	rl.RecordAttempt("192.168.1.1", "user1")
	rl.RecordAttempt("192.168.1.2", "user2")

	stats := rl.Stats()
	if stats.TotalTracked != 2 {
		t.Errorf("TotalIPs should be 2, got %d", stats.TotalTracked)
	}
}

func TestRateLimiter_GetBlockTimeRemaining(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:       true,
		MaxAttempts:   2,
		WindowSize:    time.Minute,
		BlockDuration: 5 * time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	ip := "192.168.1.100"

	// Not blocked yet
	if rl.GetBlockTimeRemaining(ip) != 0 {
		t.Error("Block time should be 0 for non-blocked IP")
	}

	// Block the IP
	rl.RecordAttempt(ip, "user1")
	rl.RecordAttempt(ip, "user1")

	// Should have remaining time
	remaining := rl.GetBlockTimeRemaining(ip)
	if remaining <= 0 {
		t.Error("Block time should be positive for blocked IP")
	}
}
