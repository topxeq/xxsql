package security

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"sync"
	"time"
	"unicode"
)

// PasswordPolicy defines password requirements.
type PasswordPolicy struct {
	MinLength        int
	MaxLength        int
	RequireUppercase bool
	RequireLowercase bool
	RequireDigit     bool
	RequireSpecial   bool
	MinSpecialChars  int
	ExpireDays       int       // 0 = no expiration
	HistoryCount     int       // Number of previous passwords to check
	LockoutAttempts  int       // Failed attempts before lockout
	LockoutDuration  time.Duration
}

// DefaultPasswordPolicy returns a default password policy.
func DefaultPasswordPolicy() *PasswordPolicy {
	return &PasswordPolicy{
		MinLength:        8,
		MaxLength:        128,
		RequireUppercase: true,
		RequireLowercase: true,
		RequireDigit:     true,
		RequireSpecial:   false,
		MinSpecialChars:  0,
		ExpireDays:       0,
		HistoryCount:     5,
		LockoutAttempts:  5,
		LockoutDuration:  30 * time.Minute,
	}
}

// PasswordValidator validates passwords against a policy.
type PasswordValidator struct {
	policy     *PasswordPolicy
	history    map[string][]string      // username -> password hashes
	expiration map[string]time.Time     // username -> expiration time
	mu         sync.RWMutex
	audit      *AuditLogger
}

// NewPasswordValidator creates a new password validator.
func NewPasswordValidator(policy *PasswordPolicy, audit *AuditLogger) *PasswordValidator {
	if policy == nil {
		policy = DefaultPasswordPolicy()
	}

	return &PasswordValidator{
		policy:     policy,
		history:    make(map[string][]string),
		expiration: make(map[string]time.Time),
		audit:      audit,
	}
}

// Validate checks if a password meets the policy requirements.
// Returns nil if valid, or an error describing the violation.
func (pv *PasswordValidator) Validate(password string) error {
	if len(password) < pv.policy.MinLength {
		return errors.New("password too short")
	}

	if pv.policy.MaxLength > 0 && len(password) > pv.policy.MaxLength {
		return errors.New("password too long")
	}

	var hasUpper, hasLower, hasDigit bool
	specialCount := 0

	for _, r := range password {
		switch {
		case unicode.IsUpper(r):
			hasUpper = true
		case unicode.IsLower(r):
			hasLower = true
		case unicode.IsDigit(r):
			hasDigit = true
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			specialCount++
		}
	}

	if pv.policy.RequireUppercase && !hasUpper {
		return errors.New("password must contain uppercase letter")
	}

	if pv.policy.RequireLowercase && !hasLower {
		return errors.New("password must contain lowercase letter")
	}

	if pv.policy.RequireDigit && !hasDigit {
		return errors.New("password must contain digit")
	}

	if pv.policy.RequireSpecial && specialCount < pv.policy.MinSpecialChars {
		return errors.New("password must contain special character")
	}

	return nil
}

// ValidateForUser validates a password for a specific user.
// This also checks against password history.
func (pv *PasswordValidator) ValidateForUser(username, password string) error {
	// First check basic policy
	if err := pv.Validate(password); err != nil {
		return err
	}

	// Check history
	pv.mu.RLock()
	history := pv.history[username]
	pv.mu.RUnlock()

	// Check if password was recently used
	// Note: In production, you'd hash the password and compare hashes
	// For simplicity, we're storing hashes would be handled by auth manager
	for _, prev := range history {
		if prev == password { // In production: compare hashes
			return errors.New("password was recently used")
		}
	}

	return nil
}

// RecordPasswordChange records a password change for history tracking.
func (pv *PasswordValidator) RecordPasswordChange(username, passwordHash string) {
	pv.mu.Lock()
	defer pv.mu.Unlock()

	history := pv.history[username]
	history = append(history, passwordHash)

	// Keep only the last N passwords
	if len(history) > pv.policy.HistoryCount {
		history = history[len(history)-pv.policy.HistoryCount:]
	}

	pv.history[username] = history

	// Set expiration if configured
	if pv.policy.ExpireDays > 0 {
		pv.expiration[username] = time.Now().AddDate(0, 0, pv.policy.ExpireDays)
	}
}

// IsPasswordExpired checks if a user's password has expired.
func (pv *PasswordValidator) IsPasswordExpired(username string) bool {
	if pv.policy.ExpireDays <= 0 {
		return false
	}

	pv.mu.RLock()
	expiration, exists := pv.expiration[username]
	pv.mu.RUnlock()

	if !exists {
		return false
	}

	return time.Now().After(expiration)
}

// GetPasswordExpiry returns when the password expires.
func (pv *PasswordValidator) GetPasswordExpiry(username string) time.Time {
	pv.mu.RLock()
	defer pv.mu.RUnlock()
	return pv.expiration[username]
}

// DaysUntilExpiry returns days until password expires.
func (pv *PasswordValidator) DaysUntilExpiry(username string) int {
	pv.mu.RLock()
	expiration, exists := pv.expiration[username]
	pv.mu.RUnlock()

	if !exists || pv.policy.ExpireDays <= 0 {
		return -1 // No expiration
	}

	remaining := time.Until(expiration)
	days := int(remaining.Hours() / 24)
	if days < 0 {
		return 0
	}
	return days
}

// ClearHistory clears password history for a user.
func (pv *PasswordValidator) ClearHistory(username string) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	delete(pv.history, username)
	delete(pv.expiration, username)
}

// GetPolicy returns the current password policy.
func (pv *PasswordValidator) GetPolicy() *PasswordPolicy {
	return pv.policy
}

// SetPolicy sets a new password policy.
func (pv *PasswordValidator) SetPolicy(policy *PasswordPolicy) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.policy = policy
}

// PasswordStrength represents password strength level.
type PasswordStrength int

const (
	StrengthVeryWeak PasswordStrength = iota
	StrengthWeak
	StrengthMedium
	StrengthStrong
	StrengthVeryStrong
)

// String returns the string representation.
func (s PasswordStrength) String() string {
	switch s {
	case StrengthVeryWeak:
		return "Very Weak"
	case StrengthWeak:
		return "Weak"
	case StrengthMedium:
		return "Medium"
	case StrengthStrong:
		return "Strong"
	case StrengthVeryStrong:
		return "Very Strong"
	default:
		return "Unknown"
	}
}

// CheckStrength checks the strength of a password.
func CheckStrength(password string) PasswordStrength {
	score := 0
	length := len(password)

	// Length scoring
	if length >= 8 {
		score++
	}
	if length >= 12 {
		score++
	}
	if length >= 16 {
		score++
	}

	// Character variety
	var hasUpper, hasLower, hasDigit, hasSpecial bool
	for _, r := range password {
		switch {
		case unicode.IsUpper(r):
			hasUpper = true
		case unicode.IsLower(r):
			hasLower = true
		case unicode.IsDigit(r):
			hasDigit = true
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			hasSpecial = true
		}
	}

	if hasUpper {
		score++
	}
	if hasLower {
		score++
	}
	if hasDigit {
		score++
	}
	if hasSpecial {
		score++
	}

	// Check for common patterns
	if isCommonPattern(password) {
		score -= 2
	}

	// Convert score to strength
	switch {
	case score <= 2:
		return StrengthVeryWeak
	case score <= 4:
		return StrengthWeak
	case score <= 6:
		return StrengthMedium
	case score <= 8:
		return StrengthStrong
	default:
		return StrengthVeryStrong
	}
}

// commonPatterns are regex patterns for common weak passwords.
var commonPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^password`),
	regexp.MustCompile(`(?i)^123456`),
	regexp.MustCompile(`(?i)^qwerty`),
	regexp.MustCompile(`(?i)^abc123`),
	regexp.MustCompile(`(?i)^[a-z]+$`), // Only letters
	regexp.MustCompile(`(?i)^[0-9]+$`), // Only digits
}

// isCommonPattern checks if password matches common patterns.
func isCommonPattern(password string) bool {
	for _, pattern := range commonPatterns {
		if pattern.MatchString(password) {
			return true
		}
	}

	// Check for repeated characters
	if len(password) > 0 {
		allSame := true
		first := password[0]
		for i := 1; i < len(password); i++ {
			if password[i] != first {
				allSame = false
				break
			}
		}
		if allSame {
			return true
		}
	}

	return false
}

// GenerateRandomPassword generates a random password meeting the policy.
func GenerateRandomPassword(policy *PasswordPolicy) (string, error) {
	if policy == nil {
		policy = DefaultPasswordPolicy()
	}

	length := policy.MinLength
	if length < 12 {
		length = 12
	}

	const (
		upperChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		lowerChars  = "abcdefghijklmnopqrstuvwxyz"
		digitChars  = "0123456789"
		specialChars = "!@#$%^&*()-_=+[]{}|;:,.<>?"
	)

	// Build character set
	charset := lowerChars
	if policy.RequireUppercase {
		charset += upperChars
	}
	if policy.RequireDigit {
		charset += digitChars
	}
	if policy.RequireSpecial {
		charset += specialChars
	}

	// Generate random bytes
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	// Build password
	password := make([]byte, length)
	for i := range password {
		password[i] = charset[int(bytes[i])%len(charset)]
	}

	return string(password), nil
}

// GenerateResetToken generates a secure password reset token.
func GenerateResetToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
