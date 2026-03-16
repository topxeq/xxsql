package security

import (
	"sync"
	"time"
)

// RateLimiter provides rate limiting functionality.
type RateLimiter struct {
	mu           sync.Mutex
	attempts     map[string]*attemptRecord
	maxAttempts  int
	windowSize   time.Duration
	blockDuration time.Duration
	cleanupInt   time.Duration
	stopCh       chan struct{}
	audit        *AuditLogger
}

type attemptRecord struct {
	count       int
	firstAttempt time.Time
	blockedUntil time.Time
}

// RateLimitConfig contains configuration for rate limiting.
type RateLimitConfig struct {
	Enabled       bool
	MaxAttempts   int           // Max failed attempts before blocking
	WindowSize    time.Duration // Time window for counting attempts
	BlockDuration time.Duration // How long to block after max attempts
	CleanupInt    time.Duration // Interval for cleaning old records
}

// DefaultRateLimitConfig returns default rate limit configuration.
func DefaultRateLimitConfig() *RateLimitConfig {
	return &RateLimitConfig{
		Enabled:       true,
		MaxAttempts:   5,
		WindowSize:    15 * time.Minute,
		BlockDuration: 30 * time.Minute,
		CleanupInt:    5 * time.Minute,
	}
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(cfg *RateLimitConfig, audit *AuditLogger) *RateLimiter {
	if cfg == nil {
		cfg = DefaultRateLimitConfig()
	}

	// Ensure cleanup interval is positive
	cleanupInt := cfg.CleanupInt
	if cleanupInt <= 0 {
		cleanupInt = 5 * time.Minute
	}

	rl := &RateLimiter{
		attempts:      make(map[string]*attemptRecord),
		maxAttempts:   cfg.MaxAttempts,
		windowSize:    cfg.WindowSize,
		blockDuration: cfg.BlockDuration,
		cleanupInt:    cleanupInt,
		stopCh:        make(chan struct{}),
		audit:         audit,
	}

	// Start cleanup goroutine
	go rl.cleanup()

	return rl
}

// CheckAllowed checks if a request from the given key is allowed.
// Returns true if allowed, false if rate limited.
func (rl *RateLimiter) CheckAllowed(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	record, exists := rl.attempts[key]

	if !exists {
		return true
	}

	// Check if currently blocked
	if !record.blockedUntil.IsZero() && now.Before(record.blockedUntil) {
		return false
	}

	// Check if window has expired
	if now.Sub(record.firstAttempt) > rl.windowSize {
		// Reset the record
		delete(rl.attempts, key)
		return true
	}

	// Check if under limit
	return record.count < rl.maxAttempts
}

// RecordAttempt records a failed attempt for the given key.
// Returns true if this attempt caused blocking.
func (rl *RateLimiter) RecordAttempt(key string, user string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	record, exists := rl.attempts[key]

	if !exists {
		rl.attempts[key] = &attemptRecord{
			count:        1,
			firstAttempt: now,
		}
		return false
	}

	// Reset if window expired
	if now.Sub(record.firstAttempt) > rl.windowSize {
		record.count = 1
		record.firstAttempt = now
		record.blockedUntil = time.Time{}
		return false
	}

	record.count++

	// Check if should block
	if record.count >= rl.maxAttempts {
		record.blockedUntil = now.Add(rl.blockDuration)

		// Log the blocking
		if rl.audit != nil {
			rl.audit.Log(&Event{
				EventType: EventRateLimitExceeded,
				Severity:  SeverityWarning,
				User:      user,
				SourceIP:  key,
				Message:   "Rate limit exceeded, IP blocked",
				Details: map[string]interface{}{
					"attempts":      record.count,
					"block_duration": rl.blockDuration.String(),
				},
			})
		}

		return true
	}

	return false
}

// RecordSuccess clears the attempt record for the given key.
func (rl *RateLimiter) RecordSuccess(key string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.attempts, key)
}

// GetRemainingAttempts returns the number of remaining attempts for a key.
func (rl *RateLimiter) GetRemainingAttempts(key string) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	record, exists := rl.attempts[key]
	if !exists {
		return rl.maxAttempts
	}

	// Check if window expired
	if time.Now().Sub(record.firstAttempt) > rl.windowSize {
		return rl.maxAttempts
	}

	remaining := rl.maxAttempts - record.count
	if remaining < 0 {
		return 0
	}
	return remaining
}

// GetBlockTimeRemaining returns how long until a blocked key is unblocked.
// Returns 0 if not blocked.
func (rl *RateLimiter) GetBlockTimeRemaining(key string) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	record, exists := rl.attempts[key]
	if !exists {
		return 0
	}

	if record.blockedUntil.IsZero() {
		return 0
	}

	remaining := time.Until(record.blockedUntil)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Unblock removes the block for a key.
func (rl *RateLimiter) Unblock(key string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if record, exists := rl.attempts[key]; exists {
		record.blockedUntil = time.Time{}
		record.count = 0
	}
}

// Clear removes all records for a key.
func (rl *RateLimiter) Clear(key string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.attempts, key)
}

// cleanup periodically removes expired records.
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(rl.cleanupInt)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.doCleanup()
		case <-rl.stopCh:
			return
		}
	}
}

// doCleanup removes expired records.
func (rl *RateLimiter) doCleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for key, record := range rl.attempts {
		// Remove if window expired and not blocked
		if now.Sub(record.firstAttempt) > rl.windowSize {
			if record.blockedUntil.IsZero() || now.After(record.blockedUntil) {
				delete(rl.attempts, key)
			}
		}
	}
}

// Stop stops the rate limiter.
func (rl *RateLimiter) Stop() {
	close(rl.stopCh)
}

// Stats returns current rate limiter statistics.
func (rl *RateLimiter) Stats() RateLimitStats {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	stats := RateLimitStats{}
	for _, record := range rl.attempts {
		stats.TotalTracked++
		if !record.blockedUntil.IsZero() && time.Now().Before(record.blockedUntil) {
			stats.CurrentlyBlocked++
		}
	}
	return stats
}

// RateLimitStats contains rate limiter statistics.
type RateLimitStats struct {
	TotalTracked   int
	CurrentlyBlocked int
}
