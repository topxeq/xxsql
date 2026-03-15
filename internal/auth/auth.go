// Package auth provides authentication and authorization for XxSql.
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// UserRole represents a user's role.
type UserRole int

const (
	RoleAdmin UserRole = iota
	RoleUser
)

// String returns the string representation of the role.
func (r UserRole) String() string {
	switch r {
	case RoleAdmin:
		return "admin"
	case RoleUser:
		return "user"
	default:
		return "unknown"
	}
}

// Permission represents a permission bit.
type Permission uint32

const (
	PermManageUsers Permission = 1 << iota
	PermManageConfig
	PermStartStop
	PermCreateTable
	PermDropTable
	PermCreateDatabase
	PermDropDatabase
	PermSelect
	PermInsert
	PermUpdate
	PermDelete
	PermCreateIndex
	PermDropIndex
	PermBackup
	PermRestore
)

// RolePermissions maps roles to their permissions.
var RolePermissions = map[UserRole]Permission{
	RoleAdmin: PermManageUsers | PermManageConfig | PermStartStop |
		PermCreateTable | PermDropTable | PermCreateDatabase | PermDropDatabase |
		PermSelect | PermInsert | PermUpdate | PermDelete |
		PermCreateIndex | PermDropIndex | PermBackup | PermRestore,
	RoleUser: PermSelect | PermInsert | PermUpdate | PermDelete,
}

// User represents a database user.
type User struct {
	ID           uint64
	Username     string
	PasswordHash string
	Role         UserRole
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Session represents an authenticated session.
type Session struct {
	ID        string
	UserID    uint64
	Username  string
	Role      UserRole
	CreatedAt time.Time
	ExpiresAt time.Time
	Database  string
}

// IsExpired checks if the session is expired.
func (s *Session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}

// HasPermission checks if the session has the given permission.
func (s *Session) HasPermission(perm Permission) bool {
	perms := RolePermissions[s.Role]
	return perms&perm != 0
}

// Manager manages users and sessions.
type Manager struct {
	mu          sync.RWMutex
	users       map[string]*User       // username -> User
	usersByID   map[uint64]*User       // id -> User
	sessions    map[string]*Session    // sessionID -> Session
	nextUserID  uint64
	sessionTTL  time.Duration
}

// ManagerOption is a functional option for Manager.
type ManagerOption func(*Manager)

// WithSessionTTL sets the session TTL.
func WithSessionTTL(ttl time.Duration) ManagerOption {
	return func(m *Manager) {
		m.sessionTTL = ttl
	}
}

// NewManager creates a new auth manager.
func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		users:      make(map[string]*User),
		usersByID:  make(map[uint64]*User),
		sessions:   make(map[string]*Session),
		nextUserID: 1,
		sessionTTL: 24 * time.Hour,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// CreateUser creates a new user.
func (m *Manager) CreateUser(username, password string, role UserRole) (*User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if user exists
	if _, exists := m.users[username]; exists {
		return nil, fmt.Errorf("user %q already exists", username)
	}

	// Hash password
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	user := &User{
		ID:           m.nextUserID,
		Username:     username,
		PasswordHash: string(hash),
		Role:         role,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	m.nextUserID++
	m.users[username] = user
	m.usersByID[user.ID] = user

	return user, nil
}

// GetUser retrieves a user by username.
func (m *Manager) GetUser(username string) (*User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return nil, fmt.Errorf("user %q not found", username)
	}

	return user, nil
}

// GetUserByID retrieves a user by ID.
func (m *Manager) GetUserByID(id uint64) (*User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.usersByID[id]
	if !exists {
		return nil, fmt.Errorf("user with id %d not found", id)
	}

	return user, nil
}

// DeleteUser deletes a user.
func (m *Manager) DeleteUser(username string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	user, exists := m.users[username]
	if !exists {
		return fmt.Errorf("user %q not found", username)
	}

	// Don't allow deleting the last admin
	if user.Role == RoleAdmin {
		adminCount := 0
		for _, u := range m.users {
			if u.Role == RoleAdmin {
				adminCount++
			}
		}
		if adminCount <= 1 {
			return fmt.Errorf("cannot delete the last admin user")
		}
	}

	delete(m.users, username)
	delete(m.usersByID, user.ID)

	return nil
}

// ListUsers lists all users.
func (m *Manager) ListUsers() []*User {
	m.mu.RLock()
	defer m.mu.RUnlock()

	users := make([]*User, 0, len(m.users))
	for _, user := range m.users {
		users = append(users, user)
	}
	return users
}

// Authenticate authenticates a user and creates a session.
func (m *Manager) Authenticate(username, password string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find user
	user, exists := m.users[username]
	if !exists {
		return nil, fmt.Errorf("authentication failed")
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, fmt.Errorf("authentication failed")
	}

	// Create session
	sessionID, err := generateSessionID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate session ID: %w", err)
	}

	session := &Session{
		ID:        sessionID,
		UserID:    user.ID,
		Username:  user.Username,
		Role:      user.Role,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(m.sessionTTL),
	}

	m.sessions[sessionID] = session

	return session, nil
}

// ValidateSession validates a session and returns it if valid.
func (m *Manager) ValidateSession(sessionID string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return nil, fmt.Errorf("session not found")
	}

	if session.IsExpired() {
		return nil, fmt.Errorf("session expired")
	}

	return session, nil
}

// RefreshSession refreshes a session's expiration time.
func (m *Manager) RefreshSession(sessionID string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return nil, fmt.Errorf("session not found")
	}

	if session.IsExpired() {
		delete(m.sessions, sessionID)
		return nil, fmt.Errorf("session expired")
	}

	session.ExpiresAt = time.Now().Add(m.sessionTTL)
	return session, nil
}

// InvalidateSession invalidates a session.
func (m *Manager) InvalidateSession(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
}

// CleanupExpiredSessions removes expired sessions.
func (m *Manager) CleanupExpiredSessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for id, session := range m.sessions {
		if session.IsExpired() {
			delete(m.sessions, id)
			count++
		}
	}
	return count
}

// SetUserDatabase sets the database for a session.
func (m *Manager) SetUserDatabase(sessionID, database string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return fmt.Errorf("session not found")
	}

	session.Database = database
	return nil
}

// ChangePassword changes a user's password.
func (m *Manager) ChangePassword(username, oldPassword, newPassword string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	user, exists := m.users[username]
	if !exists {
		return fmt.Errorf("user %q not found", username)
	}

	// Verify old password
	if oldPassword != "" {
		if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(oldPassword)); err != nil {
			return fmt.Errorf("incorrect password")
		}
	}

	// Hash new password
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.UpdatedAt = time.Now()

	return nil
}

// CheckPermission checks if a user has a specific permission.
func (m *Manager) CheckPermission(username string, perm Permission) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return false, fmt.Errorf("user %q not found", username)
	}

	perms := RolePermissions[user.Role]
	return perms&perm != 0, nil
}

// generateSessionID generates a random session ID.
func generateSessionID() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// PermissionChecker provides permission checking for a session.
type PermissionChecker struct {
	session *Session
}

// NewPermissionChecker creates a new permission checker.
func NewPermissionChecker(session *Session) *PermissionChecker {
	return &PermissionChecker{session: session}
}

// Check checks if the session has the given permission.
func (p *PermissionChecker) Check(perm Permission) bool {
	if p.session == nil {
		return false
	}
	return p.session.HasPermission(perm)
}

// Require checks the permission and returns an error if not granted.
func (p *PermissionChecker) Require(perm Permission) error {
	if !p.Check(perm) {
		return fmt.Errorf("permission denied")
	}
	return nil
}