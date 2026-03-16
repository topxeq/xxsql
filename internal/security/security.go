package security

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// SecurityConfig contains all security configuration.
type SecurityConfig struct {
	Audit      *AuditConfig
	RateLimit  *RateLimitConfig
	IPAccess   *IPAccessConfig
	Password   *PasswordPolicy
	TLS        *TLSConfig
}

// DefaultSecurityConfig returns default security configuration.
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		Audit:     DefaultAuditConfig(),
		RateLimit: DefaultRateLimitConfig(),
		IPAccess:  DefaultIPAccessConfig(),
		Password:  DefaultPasswordPolicy(),
		TLS:       DefaultTLSConfig(),
	}
}

// SecurityManager provides unified security management.
type SecurityManager struct {
	audit      *AuditLogger
	rateLimit  *RateLimiter
	ipAccess   *IPAccess
	password   *PasswordValidator
	tls        *TLSManager
}

// NewSecurityManager creates a new security manager.
func NewSecurityManager(cfg *SecurityConfig) (*SecurityManager, error) {
	if cfg == nil {
		cfg = DefaultSecurityConfig()
	}

	sm := &SecurityManager{}

	var err error

	// Initialize audit logger first (others may depend on it)
	if cfg.Audit != nil && cfg.Audit.Enabled {
		sm.audit, err = NewAuditLogger(cfg.Audit)
		if err != nil {
			return nil, fmt.Errorf("failed to create audit logger: %w", err)
		}
	}

	// Initialize rate limiter
	if cfg.RateLimit != nil && cfg.RateLimit.Enabled {
		sm.rateLimit = NewRateLimiter(cfg.RateLimit, sm.audit)
	}

	// Initialize IP access control
	if cfg.IPAccess != nil {
		sm.ipAccess, err = NewIPAccess(cfg.IPAccess, sm.audit)
		if err != nil {
			return nil, fmt.Errorf("failed to create IP access: %w", err)
		}
	}

	// Initialize password validator
	if cfg.Password != nil {
		sm.password = NewPasswordValidator(cfg.Password, sm.audit)
	}

	// Initialize TLS manager
	if cfg.TLS != nil && cfg.TLS.Enabled {
		sm.tls, err = NewTLSManager(cfg.TLS, sm.audit)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS manager: %w", err)
		}
	}

	return sm, nil
}

// CheckConnection checks if a connection should be allowed.
// This checks IP access control and rate limiting.
func (sm *SecurityManager) CheckConnection(ip string) error {
	// Check IP access
	if sm.ipAccess != nil {
		if !sm.ipAccess.CheckAndLog(ip) {
			return fmt.Errorf("connection from %s is not allowed", ip)
		}
	}

	// Check rate limiting
	if sm.rateLimit != nil {
		if !sm.rateLimit.CheckAllowed(ip) {
			remaining := sm.rateLimit.GetBlockTimeRemaining(ip)
			return fmt.Errorf("rate limit exceeded, try again in %v", remaining)
		}
	}

	return nil
}

// RecordAuthFailure records a failed authentication attempt.
func (sm *SecurityManager) RecordAuthFailure(ip, username string) {
	if sm.rateLimit != nil {
		sm.rateLimit.RecordAttempt(ip, username)
	}

	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType: EventLoginFailed,
			Severity:  SeverityWarning,
			User:      username,
			SourceIP:  ip,
			Message:   "Authentication failed",
		})
	}
}

// RecordAuthSuccess records a successful authentication.
func (sm *SecurityManager) RecordAuthSuccess(ip, username string) {
	if sm.rateLimit != nil {
		sm.rateLimit.RecordSuccess(ip)
	}

	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType: EventLoginSuccess,
			Severity:  SeverityInfo,
			User:      username,
			SourceIP:  ip,
			Message:   "Authentication successful",
		})
	}
}

// RecordLogout records a logout event.
func (sm *SecurityManager) RecordLogout(ip, username string) {
	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType: EventLogout,
			Severity:  SeverityInfo,
			User:      username,
			SourceIP:  ip,
			Message:   "User logged out",
		})
	}
}

// ValidatePassword validates a password against the policy.
func (sm *SecurityManager) ValidatePassword(password string) error {
	if sm.password == nil {
		return nil
	}
	return sm.password.Validate(password)
}

// ValidatePasswordForUser validates a password for a specific user.
func (sm *SecurityManager) ValidatePasswordForUser(username, password string) error {
	if sm.password == nil {
		return nil
	}
	return sm.password.ValidateForUser(username, password)
}

// RecordPasswordChange records a password change.
func (sm *SecurityManager) RecordPasswordChange(username string, passwordHash string) {
	if sm.password != nil {
		sm.password.RecordPasswordChange(username, passwordHash)
	}

	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType: EventPasswordChange,
			Severity:  SeverityInfo,
			User:      username,
			Message:   "Password changed",
		})
	}
}

// IsPasswordExpired checks if a user's password is expired.
func (sm *SecurityManager) IsPasswordExpired(username string) bool {
	if sm.password == nil {
		return false
	}
	return sm.password.IsPasswordExpired(username)
}

// GetPasswordExpiry returns when a user's password expires.
func (sm *SecurityManager) GetPasswordExpiry(username string) time.Time {
	if sm.password == nil {
		return time.Time{}
	}
	return sm.password.GetPasswordExpiry(username)
}

// DaysUntilPasswordExpiry returns days until password expires.
func (sm *SecurityManager) DaysUntilPasswordExpiry(username string) int {
	if sm.password == nil {
		return -1
	}
	return sm.password.DaysUntilExpiry(username)
}

// GetTLSConfig returns the TLS configuration.
func (sm *SecurityManager) GetTLSConfig() *TLSConfig {
	if sm.tls == nil {
		return nil
	}
	return sm.tls.GetConfig()
}

// GetTLSCert returns the TLS certificate for listeners.
func (sm *SecurityManager) GetTLSCert() interface {
	GetTLSConfig() *tls.Config
	IsEnabled() bool
	IsTLSRequired() bool
} {
	if sm.tls == nil {
		return nil
	}
	return sm.tls
}

// IsTLSEnabled returns whether TLS is enabled.
func (sm *SecurityManager) IsTLSEnabled() bool {
	if sm.tls == nil {
		return false
	}
	return sm.tls.IsEnabled()
}

// IsTLSRequired returns whether TLS is required.
func (sm *SecurityManager) IsTLSRequired() bool {
	if sm.tls == nil {
		return false
	}
	return sm.tls.IsTLSRequired()
}

// LogUserCreated logs a user creation event.
func (sm *SecurityManager) LogUserCreated(actor, newUser string) {
	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType: EventUserCreated,
			Severity:  SeverityInfo,
			User:      actor,
			Message:   fmt.Sprintf("User %s created", newUser),
		})
	}
}

// LogUserDeleted logs a user deletion event.
func (sm *SecurityManager) LogUserDeleted(actor, deletedUser string) {
	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType: EventUserDeleted,
			Severity:  SeverityWarning,
			User:      actor,
			Message:   fmt.Sprintf("User %s deleted", deletedUser),
		})
	}
}

// LogPermissionGranted logs a permission grant event.
func (sm *SecurityManager) LogPermissionGranted(actor, targetUser, permission string) {
	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType:  EventPermissionGranted,
			Severity:   SeverityInfo,
			User:       actor,
			Permission: permission,
			Message:    fmt.Sprintf("Permission %s granted to %s", permission, targetUser),
		})
	}
}

// LogPermissionRevoked logs a permission revoke event.
func (sm *SecurityManager) LogPermissionRevoked(actor, targetUser, permission string) {
	if sm.audit != nil {
		sm.audit.Log(&Event{
			EventType:  EventPermissionRevoked,
			Severity:   SeverityInfo,
			User:       actor,
			Permission: permission,
			Message:    fmt.Sprintf("Permission %s revoked from %s", permission, targetUser),
		})
	}
}

// AddIPToBlacklist adds an IP to the blacklist.
func (sm *SecurityManager) AddIPToBlacklist(ip string) error {
	if sm.ipAccess == nil {
		return fmt.Errorf("IP access control not enabled")
	}
	return sm.ipAccess.AddToBlacklist(ip)
}

// RemoveIPFromBlacklist removes an IP from the blacklist.
func (sm *SecurityManager) RemoveIPFromBlacklist(ip string) {
	if sm.ipAccess != nil {
		sm.ipAccess.RemoveFromBlacklist(ip)
	}
}

// AddIPToWhitelist adds an IP to the whitelist.
func (sm *SecurityManager) AddIPToWhitelist(ip string) error {
	if sm.ipAccess == nil {
		return fmt.Errorf("IP access control not enabled")
	}
	return sm.ipAccess.AddToWhitelist(ip)
}

// RemoveIPFromWhitelist removes an IP from the whitelist.
func (sm *SecurityManager) RemoveIPFromWhitelist(ip string) {
	if sm.ipAccess != nil {
		sm.ipAccess.RemoveFromWhitelist(ip)
	}
}

// GetRateLimiter returns the rate limiter.
func (sm *SecurityManager) GetRateLimiter() *RateLimiter {
	return sm.rateLimit
}

// GetAuditLogger returns the audit logger.
func (sm *SecurityManager) GetAuditLogger() *AuditLogger {
	return sm.audit
}

// GetPasswordValidator returns the password validator.
func (sm *SecurityManager) GetPasswordValidator() *PasswordValidator {
	return sm.password
}

// GetIPAccess returns the IP access controller.
func (sm *SecurityManager) GetIPAccess() *IPAccess {
	return sm.ipAccess
}

// Close closes all security components.
func (sm *SecurityManager) Close() error {
	if sm.rateLimit != nil {
		sm.rateLimit.Stop()
	}
	if sm.audit != nil {
		return sm.audit.Close()
	}
	return nil
}

// ExtractIP extracts IP address from a network address.
func ExtractIP(addr net.Addr) string {
	switch a := addr.(type) {
	case *net.TCPAddr:
		return a.IP.String()
	case *net.UDPAddr:
		return a.IP.String()
	case *net.IPAddr:
		return a.IP.String()
	default:
		// Try to parse from string
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return addr.String()
		}
		return host
	}
}
