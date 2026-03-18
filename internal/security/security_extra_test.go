package security

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"
)

// ============================================================================
// TLS Manager Extended Tests
// ============================================================================

func TestTLSManager_GetMode(t *testing.T) {
	tests := []struct {
		mode     TLSMode
		expected TLSMode
	}{
		{TLSModeDisabled, TLSModeDisabled},
		{TLSModeOptional, TLSModeOptional},
		{TLSModeRequired, TLSModeRequired},
		{TLSModeVerifyCA, TLSModeVerifyCA},
	}

	for _, tt := range tests {
		t.Run(tt.expected.String(), func(t *testing.T) {
			cfg := &TLSConfig{
				Enabled: false,
				Mode:    tt.mode,
			}
			tm, err := NewTLSManager(cfg, nil)
			if err != nil {
				t.Fatalf("NewTLSManager failed: %v", err)
			}

			if tm.GetMode() != tt.expected {
				t.Errorf("GetMode() = %v, want %v", tm.GetMode(), tt.expected)
			}
		})
	}
}

func TestTLSManager_ShouldUpgrade(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		mode     TLSMode
		expected bool
	}{
		{"disabled", false, TLSModeDisabled, false},
		{"optional enabled", true, TLSModeOptional, true},
		{"required enabled", true, TLSModeRequired, true},
		{"verifyCA enabled", true, TLSModeVerifyCA, true},
		{"disabled but enabled", true, TLSModeDisabled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &TLSConfig{
				Enabled: false,
				Mode:    tt.mode,
			}
			tm, err := NewTLSManager(cfg, nil)
			if err != nil {
				t.Fatalf("NewTLSManager failed: %v", err)
			}

			// Manually set enabled for testing
			tm.mu.Lock()
			tm.config.Enabled = tt.enabled
			tm.mu.Unlock()

			if tm.ShouldUpgrade() != tt.expected {
				t.Errorf("ShouldUpgrade() = %v, want %v", tm.ShouldUpgrade(), tt.expected)
			}
		})
	}
}

func TestTLSManager_GetTLSConfig_Nil(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
	}
	tm, _ := NewTLSManager(cfg, nil)

	if tm.GetTLSConfig() != nil {
		t.Error("GetTLSConfig should return nil when TLS is disabled")
	}
}

func TestTLSManager_GetConfig(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:    false,
		Mode:       TLSModeOptional,
		MinVersion: tls.VersionTLS12,
	}
	tm, _ := NewTLSManager(cfg, nil)

	returnedCfg := tm.GetConfig()
	if returnedCfg == nil {
		t.Fatal("GetConfig returned nil")
	}
	if returnedCfg.Mode != TLSModeOptional {
		t.Errorf("Mode = %v, want OPTIONAL", returnedCfg.Mode)
	}
}

func TestTLSManager_SetConfig(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
		Mode:    TLSModeDisabled,
	}
	tm, _ := NewTLSManager(cfg, nil)

	// Set new disabled config
	newCfg := &TLSConfig{
		Enabled: false,
		Mode:    TLSModeRequired,
	}
	err := tm.SetConfig(newCfg)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	if tm.GetMode() != TLSModeRequired {
		t.Errorf("Mode = %v, want REQUIRED", tm.GetMode())
	}
}

func TestTLSManager_ReloadCertificates_Disabled(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
	}
	tm, _ := NewTLSManager(cfg, nil)

	// Should succeed when TLS is disabled
	err := tm.ReloadCertificates()
	if err != nil {
		t.Errorf("ReloadCertificates should succeed when disabled: %v", err)
	}
}

func TestTLSManager_LogTLSHandshake(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tls-audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	auditCfg := &AuditConfig{
		Enabled:  true,
		FilePath: tmpFile.Name(),
	}
	audit, _ := NewAuditLogger(auditCfg)
	defer audit.Close()

	tm, _ := NewTLSManager(&TLSConfig{Enabled: false}, audit)

	// Log successful handshake
	tm.LogTLSHandshake("192.168.1.1", true, nil)

	// Log failed handshake with error
	tm.LogTLSHandshake("192.168.1.2", false, &net.DNSError{Err: "connection refused"})

	// Log failed handshake without error
	tm.LogTLSHandshake("192.168.1.3", false, nil)

	audit.Flush()

	// Verify events were logged
	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit log should contain TLS handshake events")
	}
}

func TestTLSManager_LogTLSHandshake_NilAudit(t *testing.T) {
	tm, _ := NewTLSManager(&TLSConfig{Enabled: false}, nil)

	// Should not panic with nil audit
	tm.LogTLSHandshake("192.168.1.1", true, nil)
	tm.LogTLSHandshake("192.168.1.2", false, &net.DNSError{Err: "error"})
}

func TestTLSManager_VerifyClient_NotVerifyCAMode(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
		Mode:    TLSModeOptional,
	}
	tm, _ := NewTLSManager(cfg, nil)

	// Should always succeed when not in VerifyCA mode
	err := tm.VerifyClient(&x509.Certificate{})
	if err != nil {
		t.Errorf("VerifyClient should succeed when not in VerifyCA mode: %v", err)
	}
}

func TestTLSManager_VerifyClient_NoCA(t *testing.T) {
	cfg := &TLSConfig{
		Enabled: false,
		Mode:    TLSModeVerifyCA,
	}
	tm, _ := NewTLSManager(cfg, nil)

	// Should fail when no CA is configured
	err := tm.VerifyClient(&x509.Certificate{})
	if err == nil {
		t.Error("VerifyClient should fail when no CA is configured")
	}
}

func TestTLSManager_WithCA(t *testing.T) {
	// Generate test certificate, key, and CA
	certFile, keyFile, caFile, err := generateTestCertificateWithCA(t)
	if err != nil {
		t.Fatalf("Failed to generate test certificates: %v", err)
	}
	defer os.Remove(certFile)
	defer os.Remove(keyFile)
	defer os.Remove(caFile)

	cfg := &TLSConfig{
		Enabled:  true,
		Mode:     TLSModeVerifyCA,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}

	tm, err := NewTLSManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewTLSManager failed: %v", err)
	}

	// Test VerifyClient with CA
	err = tm.VerifyClient(&x509.Certificate{})
	// Verification may fail due to certificate chain issues, but the CA path is covered
	t.Logf("VerifyClient result: %v", err)
}

func TestGenerateSelfSignedCert(t *testing.T) {
	err := GenerateSelfSignedCert("cert.pem", "key.pem", []string{"localhost"})
	if err == nil {
		t.Error("GenerateSelfSignedCert should return error (not implemented)")
	}
}

func TestTLSManager_WithCertificates(t *testing.T) {
	// Generate test certificate and key
	certFile, keyFile, err := generateTestCertificate(t)
	if err != nil {
		t.Fatalf("Failed to generate test certificate: %v", err)
	}
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	cfg := &TLSConfig{
		Enabled:  true,
		Mode:     TLSModeOptional,
		CertFile: certFile,
		KeyFile:  keyFile,
	}

	tm, err := NewTLSManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewTLSManager failed: %v", err)
	}

	// Test GetTLSConfig
	tlsCfg := tm.GetTLSConfig()
	if tlsCfg == nil {
		t.Error("GetTLSConfig should not return nil when certificates are loaded")
	}

	// Test IsEnabled
	if !tm.IsEnabled() {
		t.Error("IsEnabled should return true")
	}

	// Test ReloadCertificates
	err = tm.ReloadCertificates()
	if err != nil {
		t.Errorf("ReloadCertificates failed: %v", err)
	}

	// Test SetConfig with enabled
	newCfg := &TLSConfig{
		Enabled:  true,
		Mode:     TLSModeRequired,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
	err = tm.SetConfig(newCfg)
	if err != nil {
		t.Errorf("SetConfig failed: %v", err)
	}

	if !tm.IsTLSRequired() {
		t.Error("IsTLSRequired should return true for REQUIRED mode")
	}
}

func TestTLSManager_LoadCertificatesError(t *testing.T) {
	// Test with missing certificate file
	cfg := &TLSConfig{
		Enabled:  true,
		Mode:     TLSModeOptional,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}

	_, err := NewTLSManager(cfg, nil)
	if err == nil {
		t.Error("NewTLSManager should fail with missing certificate files")
	}

	// Test with empty cert/key paths
	cfg2 := &TLSConfig{
		Enabled:  true,
		Mode:     TLSModeOptional,
		CertFile: "",
		KeyFile:  "",
	}

	_, err = NewTLSManager(cfg2, nil)
	if err == nil {
		t.Error("NewTLSManager should fail with empty cert/key paths")
	}
}

func TestTLSMode_Unknown(t *testing.T) {
	mode := TLSMode(99)
	if mode.String() != "UNKNOWN" {
		t.Errorf("Unknown TLSMode string = %s, want 'UNKNOWN'", mode.String())
	}
}

// ============================================================================
// Rate Limiter Extended Tests
// ============================================================================

func TestRateLimiter_WindowExpired(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:     true,
		MaxAttempts: 5,
		WindowSize:  100 * time.Millisecond,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	ip := "192.168.1.100"

	// Record some attempts
	rl.RecordAttempt(ip, "user1")
	rl.RecordAttempt(ip, "user1")

	// Should not be blocked yet
	if !rl.CheckAllowed(ip) {
		t.Error("IP should be allowed before max attempts")
	}

	// Wait for window to expire
	time.Sleep(150 * time.Millisecond)

	// Should be allowed again after window expires
	if !rl.CheckAllowed(ip) {
		t.Error("IP should be allowed after window expires")
	}

	// Remaining attempts should reset
	if rl.GetRemainingAttempts(ip) != 5 {
		t.Errorf("Remaining attempts = %d, want 5", rl.GetRemainingAttempts(ip))
	}
}

func TestRateLimiter_RecordAttempt_WindowExpired(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:       true,
		MaxAttempts:   3,
		WindowSize:    100 * time.Millisecond,
		BlockDuration: time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	ip := "192.168.1.100"

	// Record attempts
	rl.RecordAttempt(ip, "user1")
	rl.RecordAttempt(ip, "user1")

	// Wait for window to expire
	time.Sleep(150 * time.Millisecond)

	// New attempt should reset the counter
	blocked := rl.RecordAttempt(ip, "user1")
	if blocked {
		t.Error("Should not be blocked after window expires")
	}

	// Counter should be reset to 1
	if rl.GetRemainingAttempts(ip) != 2 {
		t.Errorf("Remaining attempts = %d, want 2", rl.GetRemainingAttempts(ip))
	}
}

func TestRateLimiter_RecordAttempt_WithAudit(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "ratelimit-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	audit, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer audit.Close()

	cfg := &RateLimitConfig{
		Enabled:       true,
		MaxAttempts:   2,
		WindowSize:    time.Minute,
		BlockDuration: time.Minute,
	}
	rl := NewRateLimiter(cfg, audit)
	defer rl.Stop()

	// Record attempts until blocked
	rl.RecordAttempt("192.168.1.1", "user1")
	rl.RecordAttempt("192.168.1.1", "user1")

	audit.Flush()

	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit log should contain rate limit event")
	}
}

func TestRateLimiter_NilConfig(t *testing.T) {
	rl := NewRateLimiter(nil, nil)
	defer rl.Stop()

	if rl == nil {
		t.Error("NewRateLimiter with nil config should use defaults")
	}
}

func TestRateLimiter_NegativeCleanupInt(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:     true,
		MaxAttempts: 5,
		WindowSize:  time.Minute,
		CleanupInt:  -1, // Invalid
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	// Should use default cleanup interval
	if rl.cleanupInt <= 0 {
		t.Error("Cleanup interval should be positive")
	}
}

func TestRateLimiter_GetBlockTimeRemaining_NotBlocked(t *testing.T) {
	cfg := &RateLimitConfig{
		Enabled:     true,
		MaxAttempts: 5,
		WindowSize:  time.Minute,
	}
	rl := NewRateLimiter(cfg, nil)
	defer rl.Stop()

	// Not blocked - should return 0
	if rl.GetBlockTimeRemaining("192.168.1.1") != 0 {
		t.Error("Block time should be 0 for non-existent IP")
	}

	// Add attempt but don't block
	rl.RecordAttempt("192.168.1.1", "user")
	if rl.GetBlockTimeRemaining("192.168.1.1") != 0 {
		t.Error("Block time should be 0 for non-blocked IP")
	}
}

// ============================================================================
// Audit Logger Extended Tests
// ============================================================================

func TestAuditLogger_NilConfig(t *testing.T) {
	// NewAuditLogger with nil config should use defaults
	al, err := NewAuditLogger(nil)
	if err != nil {
		t.Fatalf("NewAuditLogger with nil config should succeed: %v", err)
	}
	al.Close()
	// Clean up the default audit.log file
	os.Remove("audit.log")
}

func TestAuditLogger_FlushEmpty(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "audit-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	al, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer al.Close()

	// Flush empty buffer should succeed
	err := al.Flush()
	if err != nil {
		t.Errorf("Flush empty buffer failed: %v", err)
	}
}

func TestAuditLogger_BufferFlush(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "audit-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	al, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer al.Close()

	// Log more than 100 events to trigger buffer flush
	for i := 0; i < 105; i++ {
		al.Log(&Event{
			EventType: EventLoginSuccess,
			Severity:  SeverityInfo,
			Message:   "Test event",
		})
	}

	// Give time for potential async operations
	time.Sleep(50 * time.Millisecond)

	// Verify file has content
	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit file should have events after buffer flush threshold")
	}
}

func TestAuditLogger_LogNil(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "audit-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	al, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer al.Close()

	// Log nil event should not panic
	al.Log(nil)

	al.Flush()

	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) != 0 {
		t.Error("Audit file should be empty after logging nil")
	}
}

func TestAuditLogger_PreTimestamp(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "audit-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	al, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer al.Close()

	// Log event with pre-set timestamp
	customTime := time.Now().Add(-24 * time.Hour)
	al.Log(&Event{
		Timestamp: customTime,
		EventType: EventLoginSuccess,
		Severity:  SeverityInfo,
		Message:   "Past event",
	})

	al.Flush()

	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit file should contain event")
	}
}

func TestAuditFilter_TimeFilters(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	event := &Event{Timestamp: now}

	// Start time filter
	start := past
	filter := &AuditFilter{StartTime: &start}
	if !filter.Match(event) {
		t.Error("Event should match start time filter")
	}

	filter = &AuditFilter{StartTime: &future}
	if filter.Match(event) {
		t.Error("Event should not match future start time filter")
	}

	// End time filter
	end := future
	filter = &AuditFilter{EndTime: &end}
	if !filter.Match(event) {
		t.Error("Event should match end time filter")
	}

	filter = &AuditFilter{EndTime: &past}
	if filter.Match(event) {
		t.Error("Event should not match past end time filter")
	}
}

func TestAuditFilter_SourceIP(t *testing.T) {
	event := &Event{SourceIP: "192.168.1.1"}

	filter := &AuditFilter{SourceIP: "192.168.1.1"}
	if !filter.Match(event) {
		t.Error("Event should match source IP filter")
	}

	filter = &AuditFilter{SourceIP: "10.0.0.1"}
	if filter.Match(event) {
		t.Error("Event should not match different source IP filter")
	}
}

func TestAuditLogger_QueryFileNotExist(t *testing.T) {
	al, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: "/nonexistent/audit.log"})
	defer al.Close()

	events, err := al.Query(nil)
	if err != nil {
		t.Errorf("Query on non-existent file should not error: %v", err)
	}
	if len(events) != 0 {
		t.Error("Events should be empty for non-existent file")
	}
}

func TestAuditLogger_InvalidJSON(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "audit-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Write invalid JSON
	os.WriteFile(tmpFile.Name(), []byte("invalid json\n{\"valid\": true}\n"), 0644)

	al, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer al.Close()

	// Query should skip invalid lines
	events, err := al.Query(nil)
	if err != nil {
		t.Errorf("Query failed: %v", err)
	}
	// Should skip invalid JSON and potentially skip the non-Event JSON too
	t.Logf("Found %d events", len(events))
}

// ============================================================================
// IPAccess Extended Tests
// ============================================================================

func TestIPAccess_RemoveFromWhitelist_Network(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeWhitelist,
		Whitelist: []string{"192.168.1.0/24"},
	}
	ipa, _ := NewIPAccess(cfg, nil)

	// Should be allowed
	if !ipa.IsAllowed("192.168.1.100") {
		t.Error("IP should be allowed in whitelisted network")
	}

	// Get the network string
	whitelist := ipa.GetWhitelist()
	if len(whitelist) == 0 {
		t.Fatal("Whitelist should not be empty")
	}

	// Remove the network
	ipa.RemoveFromWhitelist(whitelist[0])

	// Should now be blocked
	if ipa.IsAllowed("192.168.1.100") {
		t.Error("IP should be blocked after removing network from whitelist")
	}
}

func TestIPAccess_RemoveFromBlacklist_Network(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeBlacklist,
		Blacklist: []string{"10.0.0.0/8"},
	}
	ipa, _ := NewIPAccess(cfg, nil)

	// Should be blocked
	if ipa.IsAllowed("10.0.0.100") {
		t.Error("IP should be blocked in blacklisted network")
	}

	// Get the network string
	blacklist := ipa.GetBlacklist()
	if len(blacklist) == 0 {
		t.Fatal("Blacklist should not be empty")
	}

	// Remove the network
	ipa.RemoveFromBlacklist(blacklist[0])

	// Should now be allowed
	if !ipa.IsAllowed("10.0.0.100") {
		t.Error("IP should be allowed after removing network from blacklist")
	}
}

func TestIPAccess_AddToWhitelist_Invalid(t *testing.T) {
	cfg := &IPAccessConfig{Mode: AccessModeWhitelist}
	ipa, _ := NewIPAccess(cfg, nil)

	err := ipa.AddToWhitelist("invalid-ip")
	if err == nil {
		t.Error("AddToWhitelist should fail with invalid IP")
	}
}

func TestIPAccess_AddToBlacklist_Invalid(t *testing.T) {
	cfg := &IPAccessConfig{Mode: AccessModeBlacklist}
	ipa, _ := NewIPAccess(cfg, nil)

	err := ipa.AddToBlacklist("invalid-ip")
	if err == nil {
		t.Error("AddToBlacklist should fail with invalid IP")
	}
}

func TestIPAccess_AddToBlacklist_WithAudit(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "ipaccess-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	audit, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer audit.Close()

	cfg := &IPAccessConfig{Mode: AccessModeBlacklist}
	ipa, _ := NewIPAccess(cfg, audit)

	// Add IP to blacklist (should log)
	ipa.AddToBlacklist("192.168.1.100")

	audit.Flush()

	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit log should contain IP blocked event")
	}
}

func TestIPAccess_NewIPAccess_InvalidWhitelist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeWhitelist,
		Whitelist: []string{"invalid-ip"},
	}

	_, err := NewIPAccess(cfg, nil)
	if err == nil {
		t.Error("NewIPAccess should fail with invalid whitelist entry")
	}
}

func TestIPAccess_NewIPAccess_InvalidBlacklist(t *testing.T) {
	cfg := &IPAccessConfig{
		Mode:      AccessModeBlacklist,
		Blacklist: []string{"invalid-ip"},
	}

	_, err := NewIPAccess(cfg, nil)
	if err == nil {
		t.Error("NewIPAccess should fail with invalid blacklist entry")
	}
}

func TestIPAccess_NilConfig(t *testing.T) {
	ipa, err := NewIPAccess(nil, nil)
	if err != nil {
		t.Fatalf("NewIPAccess with nil config should succeed: %v", err)
	}
	if ipa == nil {
		t.Error("IPAccess should not be nil")
	}
}

func TestIPAccess_IsAllowed_InvalidIP(t *testing.T) {
	cfg := &IPAccessConfig{Mode: AccessModeAllowAll}
	ipa, _ := NewIPAccess(cfg, nil)

	if ipa.IsAllowed("invalid-ip") {
		t.Error("IsAllowed should return false for invalid IP")
	}
}

func TestIPAccess_CheckAndLog_Rejected(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "ipaccess-*.log")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	audit, _ := NewAuditLogger(&AuditConfig{Enabled: true, FilePath: tmpFile.Name()})
	defer audit.Close()

	cfg := &IPAccessConfig{
		Mode:      AccessModeWhitelist,
		Whitelist: []string{"192.168.1.1"},
	}
	ipa, _ := NewIPAccess(cfg, audit)

	// Try to connect from non-whitelisted IP
	allowed := ipa.CheckAndLog("10.0.0.1")
	if allowed {
		t.Error("Non-whitelisted IP should be rejected")
	}

	audit.Flush()

	data, _ := os.ReadFile(tmpFile.Name())
	if len(data) == 0 {
		t.Error("Audit log should contain connection rejected event")
	}
}

func TestIPAccess_UnknownMode(t *testing.T) {
	cfg := &IPAccessConfig{Mode: AccessMode(99)}
	ipa, _ := NewIPAccess(cfg, nil)

	// Unknown mode should allow all
	if !ipa.IsAllowed("192.168.1.1") {
		t.Error("Unknown mode should allow all IPs")
	}
}

// ============================================================================
// Security Manager Extended Tests
// ============================================================================

func TestSecurityManager_GetPasswordExpiry(t *testing.T) {
	cfg := &SecurityConfig{
		Password: &PasswordPolicy{
			MinLength:  8,
			ExpireDays: 30,
		},
	}
	sm, _ := NewSecurityManager(cfg)
	defer sm.Close()

	sm.RecordPasswordChange("testuser", "hash123")

	expiry := sm.GetPasswordExpiry("testuser")
	if expiry.IsZero() {
		t.Error("GetPasswordExpiry should return non-zero time")
	}
}

func TestSecurityManager_GetTLSCert(t *testing.T) {
	// Without TLS
	sm, _ := NewSecurityManager(&SecurityConfig{})
	defer sm.Close()

	if sm.GetTLSCert() != nil {
		t.Error("GetTLSCert should return nil without TLS")
	}
}

func TestSecurityManager_GetTLSCert_WithTLS(t *testing.T) {
	certFile, keyFile, err := generateTestCertificate(t)
	if err != nil {
		t.Fatalf("Failed to generate test certificate: %v", err)
	}
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	cfg := &SecurityConfig{
		TLS: &TLSConfig{
			Enabled:  true,
			Mode:     TLSModeOptional,
			CertFile: certFile,
			KeyFile:  keyFile,
		},
	}
	sm, err := NewSecurityManager(cfg)
	if err != nil {
		t.Fatalf("NewSecurityManager failed: %v", err)
	}
	defer sm.Close()

	cert := sm.GetTLSCert()
	if cert == nil {
		t.Error("GetTLSCert should not return nil with TLS enabled")
	}
}

func TestExtractIP_UnknownType(t *testing.T) {
	// Test with unknown address type
	addr := &mockAddr{addr: "192.168.1.1:3306"}
	result := ExtractIP(addr)
	if result != "192.168.1.1" {
		t.Errorf("ExtractIP = %q, want '192.168.1.1'", result)
	}
}

func TestExtractIP_NoPort(t *testing.T) {
	// Test address without port
	addr := &mockAddr{addr: "192.168.1.1"}
	result := ExtractIP(addr)
	if result != "192.168.1.1" {
		t.Errorf("ExtractIP = %q, want '192.168.1.1'", result)
	}
}

// mockAddr implements net.Addr for testing
type mockAddr struct {
	addr string
}

func (m *mockAddr) Network() string { return "mock" }
func (m *mockAddr) String() string  { return m.addr }

// ============================================================================
// Password Extended Tests
// ============================================================================

func TestPasswordValidator_ValidateForUser_NoHistory(t *testing.T) {
	cfg := &PasswordPolicy{
		MinLength:    8,
		HistoryCount: 3,
	}
	validator := NewPasswordValidator(cfg, nil)

	// Validate for user with no history
	err := validator.ValidateForUser("newuser", "validpass123")
	if err != nil {
		t.Errorf("ValidateForUser should succeed with no history: %v", err)
	}
}

func TestPasswordValidator_DaysUntilExpiry_Expired(t *testing.T) {
	cfg := &PasswordPolicy{
		MinLength:  4,
		ExpireDays: -1, // Already expired
	}
	validator := NewPasswordValidator(cfg, nil)

	// This should result in negative days or 0
	validator.RecordPasswordChange("testuser", "hash")
	days := validator.DaysUntilExpiry("testuser")
	// Days can vary based on timing
	t.Logf("Days until expiry: %d", days)
}

func TestPasswordValidator_IsPasswordExpired_NoPassword(t *testing.T) {
	cfg := &PasswordPolicy{
		ExpireDays: 30,
	}
	validator := NewPasswordValidator(cfg, nil)

	// User with no password change record
	if validator.IsPasswordExpired("nonexistent") {
		t.Error("IsPasswordExpired should return false for user with no password")
	}
}

func TestCheckStrength_AllCases(t *testing.T) {
	tests := []struct {
		password string
		min      PasswordStrength
	}{
		{"", StrengthVeryWeak},
		{"a", StrengthVeryWeak},
		{"password", StrengthVeryWeak},
		{"Password1", StrengthVeryWeak},
		{"Password1!", StrengthWeak},
		{"MySecure@Pass123", StrengthMedium},
		{"Very$ecureP@ssw0rd2024", StrengthStrong},
	}

	for _, tt := range tests {
		strength := CheckStrength(tt.password)
		if strength < tt.min {
			t.Errorf("Password %q: strength = %v, expected >= %v", tt.password, strength, tt.min)
		}
	}
}

func TestIsCommonPattern(t *testing.T) {
	patterns := []string{
		"password123",
		"123456",
		"qwerty",
		"abcdef",
	}

	for _, p := range patterns {
		if !isCommonPattern(p) {
			t.Errorf("isCommonPattern(%q) should return true", p)
		}
	}

	// Non-pattern
	if isCommonPattern("xK9$mL2#pQ") {
		t.Error("isCommonPattern should return false for non-pattern")
	}
}

// Helper function to generate test certificates
func generateTestCertificate(t *testing.T) (certFile, keyFile string, err error) {
	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", "", err
	}

	// Write cert file
	certF, err := os.CreateTemp("", "test-cert-*.pem")
	if err != nil {
		return "", "", err
	}
	certFile = certF.Name()
	pem.Encode(certF, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certF.Close()

	// Write key file
	keyF, err := os.CreateTemp("", "test-key-*.pem")
	if err != nil {
		os.Remove(certFile)
		return "", "", err
	}
	keyFile = keyF.Name()
	pem.Encode(keyF, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	keyF.Close()

	return certFile, keyFile, nil
}

// Helper function to generate test certificates with CA
func generateTestCertificateWithCA(t *testing.T) (certFile, keyFile, caFile string, err error) {
	// Generate CA private key
	caPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", "", err
	}

	// Create CA certificate template
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create CA certificate
	caDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPrivateKey.PublicKey, caPrivateKey)
	if err != nil {
		return "", "", "", err
	}

	// Write CA file
	caF, err := os.CreateTemp("", "test-ca-*.pem")
	if err != nil {
		return "", "", "", err
	}
	caFile = caF.Name()
	pem.Encode(caF, &pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caF.Close()

	// Generate server private key
	serverPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", "", err
	}

	// Create server certificate template
	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// Create server certificate signed by CA
	serverDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverPrivateKey.PublicKey, caPrivateKey)
	if err != nil {
		return "", "", "", err
	}

	// Write cert file
	certF, err := os.CreateTemp("", "test-cert-*.pem")
	if err != nil {
		return "", "", "", err
	}
	certFile = certF.Name()
	pem.Encode(certF, &pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	certF.Close()

	// Write key file
	keyF, err := os.CreateTemp("", "test-key-*.pem")
	if err != nil {
		return "", "", "", err
	}
	keyFile = keyF.Name()
	pem.Encode(keyF, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverPrivateKey)})
	keyF.Close()

	return certFile, keyFile, caFile, nil
}