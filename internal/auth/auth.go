// Package auth provides authentication and authorization for XxSql.
package auth

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	ID             uint64
	Username       string
	PasswordHash   string     // bcrypt hash for internal auth
	MySQLAuthHash  []byte     // SHA1(SHA1(password)) for MySQL native auth
	Role           UserRole
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// TablePrivilege represents privileges on a specific table.
type TablePrivilege struct {
	Database string
	Table    string
	Select   bool
	Insert   bool
	Update   bool
	Delete   bool
	Create   bool
	Drop     bool
	Index    bool
	Alter    bool
}

// HasPrivilege checks if a specific privilege is granted.
func (t *TablePrivilege) HasPrivilege(perm Permission) bool {
	switch perm {
	case PermSelect:
		return t.Select
	case PermInsert:
		return t.Insert
	case PermUpdate:
		return t.Update
	case PermDelete:
		return t.Delete
	case PermCreateTable:
		return t.Create
	case PermDropTable:
		return t.Drop
	case PermCreateIndex:
		return t.Index
	case PermDropIndex:
		return t.Index
	}
	return false
}

// DatabasePrivilege represents privileges on all tables in a database.
type DatabasePrivilege struct {
	Database string
	Select   bool
	Insert   bool
	Update   bool
	Delete   bool
	Create   bool
	Drop     bool
	Index    bool
	Alter    bool
}

// GlobalPrivilege represents global privileges.
type GlobalPrivilege struct {
	Select   bool
	Insert   bool
	Update   bool
	Delete   bool
	Create   bool
	Drop     bool
	Index    bool
	Alter    bool
	Grant    bool // WITH GRANT OPTION
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
	mu              sync.RWMutex
	users           map[string]*User       // username -> User
	usersByID       map[uint64]*User       // id -> User
	sessions        map[string]*Session    // sessionID -> Session
	nextUserID      uint64
	sessionTTL      time.Duration
	dataDir         string                 // directory for persistence
	persistFile     string                 // path to users file
	grantsFile      string                 // path to grants file
	// Grant storage
	globalGrants    map[string]*GlobalPrivilege       // username -> global privileges
	dbGrants        map[string]map[string]*DatabasePrivilege  // username -> database -> db privileges
	tableGrants     map[string]map[string]map[string]*TablePrivilege // username -> database -> table -> table privileges
}

// ManagerOption is a functional option for Manager.
type ManagerOption func(*Manager)

// WithSessionTTL sets the session TTL.
func WithSessionTTL(ttl time.Duration) ManagerOption {
	return func(m *Manager) {
		m.sessionTTL = ttl
	}
}

//WithDataDir sets the data directory for persistence.
func WithDataDir(dir string) ManagerOption {
	return func(m *Manager) {
		m.dataDir = dir
		m.persistFile = filepath.Join(dir, "users.json")
		m.grantsFile = filepath.Join(dir, "grants.json")
	}
}

// NewManager creates a new auth manager.
func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		users:        make(map[string]*User),
		usersByID:    make(map[uint64]*User),
		sessions:     make(map[string]*Session),
		nextUserID:   1,
		sessionTTL:   24 * time.Hour,
		globalGrants: make(map[string]*GlobalPrivilege),
		dbGrants:     make(map[string]map[string]*DatabasePrivilege),
		tableGrants:  make(map[string]map[string]map[string]*TablePrivilege),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// userPersistData is the JSON structure for persistence.
type userPersistData struct {
	NextUserID uint64  `json:"next_user_id"`
	Users      []*User `json:"users"`
}

// Load loads users from the persistence file.
func (m *Manager) Load() error {
	if m.persistFile == "" {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.persistFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet, that's OK
		}
		return fmt.Errorf("failed to read users file: %w", err)
	}

	var pdata userPersistData
	if err := json.Unmarshal(data, &pdata); err != nil {
		return fmt.Errorf("failed to parse users file: %w", err)
	}

	m.nextUserID = pdata.NextUserID
	for _, user := range pdata.Users {
		m.users[user.Username] = user
		m.usersByID[user.ID] = user
	}

	return nil
}

// Save saves users to the persistence file.
func (m *Manager) Save() error {
	if m.persistFile == "" {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Ensure directory exists
	if m.dataDir != "" {
		if err := os.MkdirAll(m.dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	pdata := userPersistData{
		NextUserID: m.nextUserID,
		Users:      make([]*User, 0, len(m.users)),
	}
	for _, user := range m.users {
		pdata.Users = append(pdata.Users, user)
	}

	data, err := json.MarshalIndent(pdata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal users: %w", err)
	}

	if err := os.WriteFile(m.persistFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write users file: %w", err)
	}

	return nil
}

// CreateUser creates a new user.
func (m *Manager) CreateUser(username, password string, role UserRole) (*User, error) {
	m.mu.Lock()

	// Check if user exists
	if _, exists := m.users[username]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("user %q already exists", username)
	}

	// Hash password with bcrypt
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// Compute MySQL auth hash: SHA1(SHA1(password))
	sha1Hash := sha1.Sum([]byte(password))
	mysqlAuthHash := sha1.Sum(sha1Hash[:])

	user := &User{
		ID:            m.nextUserID,
		Username:      username,
		PasswordHash:  string(hash),
		MySQLAuthHash: mysqlAuthHash[:],
		Role:          role,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	m.nextUserID++
	m.users[username] = user
	m.usersByID[user.ID] = user

	m.mu.Unlock()

	// Save to disk (outside lock)
	_ = m.Save()

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

	user, exists := m.users[username]
	if !exists {
		m.mu.Unlock()
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
			m.mu.Unlock()
			return fmt.Errorf("cannot delete the last admin user")
		}
	}

	delete(m.users, username)
	delete(m.usersByID, user.ID)

	m.mu.Unlock()

	// Save to disk (outside lock)
	_ = m.Save()

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

	user, exists := m.users[username]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	// Verify old password
	if oldPassword != "" {
		if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(oldPassword)); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("incorrect password")
		}
	}

	// Hash new password
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("failed to hash password: %w", err)
	}

	// Compute MySQL auth hash
	sha1Hash := sha1.Sum([]byte(newPassword))
	mysqlAuthHash := sha1.Sum(sha1Hash[:])

	user.PasswordHash = string(hash)
	user.MySQLAuthHash = mysqlAuthHash[:]
	user.UpdatedAt = time.Now()

	m.mu.Unlock()

	// Save to disk (outside lock)
	_ = m.Save()

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

// VerifyMySQLAuth verifies MySQL native password authentication.
// The client sends: SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
// We have stored: SHA1(SHA1(password))
func (m *Manager) VerifyMySQLAuth(username string, salt, authResponse []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return false, nil
	}

	if len(user.MySQLAuthHash) != 20 {
		return false, nil
	}

	// Compute SHA1(salt + SHA1(SHA1(password)))
	combined := append(salt, user.MySQLAuthHash...)
	hash3 := sha1.Sum(combined)

	// XOR with authResponse to get SHA1(password)
	// authResponse = SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	// So: SHA1(password) = authResponse XOR hash3
	hash1 := make([]byte, 20)
	for i := 0; i < 20; i++ {
		hash1[i] = authResponse[i] ^ hash3[i]
	}

	// Compute SHA1(SHA1(password)) and compare with stored hash
	computedHash := sha1.Sum(hash1)

	for i := 0; i < 20; i++ {
		if computedHash[i] != user.MySQLAuthHash[i] {
			return false, nil
		}
	}

	return true, nil
}

// GetMySQLAuthHash returns the MySQL auth hash for a user.
func (m *Manager) GetMySQLAuthHash(username string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return nil, fmt.Errorf("user %q not found", username)
	}

	return user.MySQLAuthHash, nil
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

// ============================================================================
// Grant Management
// ============================================================================

// GrantGlobal grants global privileges to a user.
func (m *Manager) GrantGlobal(username string, priv *GlobalPrivilege) error {
	m.mu.Lock()

	if _, exists := m.users[username]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	// Merge with existing grants
	if existing, ok := m.globalGrants[username]; ok {
		if priv.Select {
			existing.Select = true
		}
		if priv.Insert {
			existing.Insert = true
		}
		if priv.Update {
			existing.Update = true
		}
		if priv.Delete {
			existing.Delete = true
		}
		if priv.Create {
			existing.Create = true
		}
		if priv.Drop {
			existing.Drop = true
		}
		if priv.Index {
			existing.Index = true
		}
		if priv.Alter {
			existing.Alter = true
		}
		if priv.Grant {
			existing.Grant = true
		}
	} else {
		m.globalGrants[username] = priv
	}

	m.mu.Unlock()
	_ = m.SaveGrants()
	return nil
}

// GrantDatabase grants database-level privileges to a user.
func (m *Manager) GrantDatabase(username, database string, priv *DatabasePrivilege) error {
	m.mu.Lock()

	if _, exists := m.users[username]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	if m.dbGrants[username] == nil {
		m.dbGrants[username] = make(map[string]*DatabasePrivilege)
	}

	// Merge with existing grants
	if existing, ok := m.dbGrants[username][database]; ok {
		if priv.Select {
			existing.Select = true
		}
		if priv.Insert {
			existing.Insert = true
		}
		if priv.Update {
			existing.Update = true
		}
		if priv.Delete {
			existing.Delete = true
		}
		if priv.Create {
			existing.Create = true
		}
		if priv.Drop {
			existing.Drop = true
		}
		if priv.Index {
			existing.Index = true
		}
		if priv.Alter {
			existing.Alter = true
		}
	} else {
		priv.Database = database
		m.dbGrants[username][database] = priv
	}

	m.mu.Unlock()
	_ = m.SaveGrants()
	return nil
}

// GrantTable grants table-level privileges to a user.
func (m *Manager) GrantTable(username, database, table string, priv *TablePrivilege) error {
	m.mu.Lock()

	if _, exists := m.users[username]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	if m.tableGrants[username] == nil {
		m.tableGrants[username] = make(map[string]map[string]*TablePrivilege)
	}
	if m.tableGrants[username][database] == nil {
		m.tableGrants[username][database] = make(map[string]*TablePrivilege)
	}

	// Merge with existing grants
	if existing, ok := m.tableGrants[username][database][table]; ok {
		if priv.Select {
			existing.Select = true
		}
		if priv.Insert {
			existing.Insert = true
		}
		if priv.Update {
			existing.Update = true
		}
		if priv.Delete {
			existing.Delete = true
		}
		if priv.Create {
			existing.Create = true
		}
		if priv.Drop {
			existing.Drop = true
		}
		if priv.Index {
			existing.Index = true
		}
		if priv.Alter {
			existing.Alter = true
		}
	} else {
		priv.Database = database
		priv.Table = table
		m.tableGrants[username][database][table] = priv
	}

	m.mu.Unlock()
	_ = m.SaveGrants()
	return nil
}

// RevokeGlobal revokes global privileges from a user.
func (m *Manager) RevokeGlobal(username string, priv *GlobalPrivilege) error {
	m.mu.Lock()

	if _, exists := m.users[username]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	if existing, ok := m.globalGrants[username]; ok {
		if priv.Select {
			existing.Select = false
		}
		if priv.Insert {
			existing.Insert = false
		}
		if priv.Update {
			existing.Update = false
		}
		if priv.Delete {
			existing.Delete = false
		}
		if priv.Create {
			existing.Create = false
		}
		if priv.Drop {
			existing.Drop = false
		}
		if priv.Index {
			existing.Index = false
		}
		if priv.Alter {
			existing.Alter = false
		}
		if priv.Grant {
			existing.Grant = false
		}
	}

	m.mu.Unlock()
	_ = m.SaveGrants()
	return nil
}

// RevokeDatabase revokes database-level privileges from a user.
func (m *Manager) RevokeDatabase(username, database string, priv *DatabasePrivilege) error {
	m.mu.Lock()

	if _, exists := m.users[username]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	if m.dbGrants[username] != nil && m.dbGrants[username][database] != nil {
		existing := m.dbGrants[username][database]
		if priv.Select {
			existing.Select = false
		}
		if priv.Insert {
			existing.Insert = false
		}
		if priv.Update {
			existing.Update = false
		}
		if priv.Delete {
			existing.Delete = false
		}
		if priv.Create {
			existing.Create = false
		}
		if priv.Drop {
			existing.Drop = false
		}
		if priv.Index {
			existing.Index = false
		}
		if priv.Alter {
			existing.Alter = false
		}
	}

	m.mu.Unlock()
	_ = m.SaveGrants()
	return nil
}

// RevokeTable revokes table-level privileges from a user.
func (m *Manager) RevokeTable(username, database, table string, priv *TablePrivilege) error {
	m.mu.Lock()

	if _, exists := m.users[username]; !exists {
		m.mu.Unlock()
		return fmt.Errorf("user %q not found", username)
	}

	if m.tableGrants[username] != nil &&
		m.tableGrants[username][database] != nil &&
		m.tableGrants[username][database][table] != nil {
		existing := m.tableGrants[username][database][table]
		if priv.Select {
			existing.Select = false
		}
		if priv.Insert {
			existing.Insert = false
		}
		if priv.Update {
			existing.Update = false
		}
		if priv.Delete {
			existing.Delete = false
		}
		if priv.Create {
			existing.Create = false
		}
		if priv.Drop {
			existing.Drop = false
		}
		if priv.Index {
			existing.Index = false
		}
		if priv.Alter {
			existing.Alter = false
		}
	}

	m.mu.Unlock()
	_ = m.SaveGrants()
	return nil
}

// CheckTablePermission checks if a user has a specific permission on a table.
func (m *Manager) CheckTablePermission(username, database, table string, perm Permission) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check role-based permissions first
	if user, exists := m.users[username]; exists {
		if RolePermissions[user.Role]&perm != 0 {
			return true
		}
	}

	// Check global grants
	if global, ok := m.globalGrants[username]; ok {
		if global.HasPermission(perm) {
			return true
		}
	}

	// Check database grants
	if m.dbGrants[username] != nil {
		if db, ok := m.dbGrants[username][database]; ok {
			if db.HasPrivilege(perm) {
				return true
			}
		}
	}

	// Check table grants
	if m.tableGrants[username] != nil {
		if m.tableGrants[username][database] != nil {
			if tbl, ok := m.tableGrants[username][database][table]; ok {
				return tbl.HasPrivilege(perm)
			}
		}
	}

	return false
}

// HasPermission checks if a global privilege has a specific permission.
func (g *GlobalPrivilege) HasPermission(perm Permission) bool {
	switch perm {
	case PermSelect:
		return g.Select
	case PermInsert:
		return g.Insert
	case PermUpdate:
		return g.Update
	case PermDelete:
		return g.Delete
	case PermCreateTable, PermCreateDatabase:
		return g.Create
	case PermDropTable, PermDropDatabase:
		return g.Drop
	case PermCreateIndex, PermDropIndex:
		return g.Index
	}
	return false
}

// HasPrivilege checks if a database privilege has a specific permission.
func (d *DatabasePrivilege) HasPrivilege(perm Permission) bool {
	switch perm {
	case PermSelect:
		return d.Select
	case PermInsert:
		return d.Insert
	case PermUpdate:
		return d.Update
	case PermDelete:
		return d.Delete
	case PermCreateTable:
		return d.Create
	case PermDropTable:
		return d.Drop
	case PermCreateIndex, PermDropIndex:
		return d.Index
	}
	return false
}

// GetGrants returns all grants for a user.
func (m *Manager) GetGrants(username string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, exists := m.users[username]; !exists {
		return nil, fmt.Errorf("user %q not found", username)
	}

	var grants []string

	// Role-based grants
	if user, ok := m.users[username]; ok {
		grants = append(grants, fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%%'", formatPermissions(RolePermissions[user.Role]), username))
	}

	// Global grants
	if global, ok := m.globalGrants[username]; ok {
		perms := formatGlobalPrivilege(global)
		if perms != "" {
			grants = append(grants, fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%%'", perms, username))
		}
	}

	// Database grants
	if dbs, ok := m.dbGrants[username]; ok {
		for db, priv := range dbs {
			perms := formatDatabasePrivilege(priv)
			if perms != "" {
				grants = append(grants, fmt.Sprintf("GRANT %s ON %s.* TO '%s'@'%%'", perms, db, username))
			}
		}
	}

	// Table grants
	if dbs, ok := m.tableGrants[username]; ok {
		for db, tables := range dbs {
			for table, priv := range tables {
				perms := formatTablePrivilege(priv)
				if perms != "" {
					grants = append(grants, fmt.Sprintf("GRANT %s ON %s.%s TO '%s'@'%%'", perms, db, table, username))
				}
			}
		}
	}

	return grants, nil
}

// formatPermissions formats permission bits to a string.
func formatPermissions(perms Permission) string {
	var parts []string
	if perms&PermSelect != 0 {
		parts = append(parts, "SELECT")
	}
	if perms&PermInsert != 0 {
		parts = append(parts, "INSERT")
	}
	if perms&PermUpdate != 0 {
		parts = append(parts, "UPDATE")
	}
	if perms&PermDelete != 0 {
		parts = append(parts, "DELETE")
	}
	if perms&PermCreateTable != 0 || perms&PermCreateDatabase != 0 {
		parts = append(parts, "CREATE")
	}
	if perms&PermDropTable != 0 || perms&PermDropDatabase != 0 {
		parts = append(parts, "DROP")
	}
	if perms&PermCreateIndex != 0 || perms&PermDropIndex != 0 {
		parts = append(parts, "INDEX")
	}
	if len(parts) == 0 {
		return "USAGE"
	}
	return strings.Join(parts, ", ")
}

// formatGlobalPrivilege formats global privileges to a string.
func formatGlobalPrivilege(priv *GlobalPrivilege) string {
	var parts []string
	if priv.Select {
		parts = append(parts, "SELECT")
	}
	if priv.Insert {
		parts = append(parts, "INSERT")
	}
	if priv.Update {
		parts = append(parts, "UPDATE")
	}
	if priv.Delete {
		parts = append(parts, "DELETE")
	}
	if priv.Create {
		parts = append(parts, "CREATE")
	}
	if priv.Drop {
		parts = append(parts, "DROP")
	}
	if priv.Index {
		parts = append(parts, "INDEX")
	}
	if priv.Alter {
		parts = append(parts, "ALTER")
	}
	if len(parts) == 0 {
		return "USAGE"
	}
	result := strings.Join(parts, ", ")
	if priv.Grant {
		result += " WITH GRANT OPTION"
	}
	return result
}

// formatDatabasePrivilege formats database privileges to a string.
func formatDatabasePrivilege(priv *DatabasePrivilege) string {
	var parts []string
	if priv.Select {
		parts = append(parts, "SELECT")
	}
	if priv.Insert {
		parts = append(parts, "INSERT")
	}
	if priv.Update {
		parts = append(parts, "UPDATE")
	}
	if priv.Delete {
		parts = append(parts, "DELETE")
	}
	if priv.Create {
		parts = append(parts, "CREATE")
	}
	if priv.Drop {
		parts = append(parts, "DROP")
	}
	if priv.Index {
		parts = append(parts, "INDEX")
	}
	if priv.Alter {
		parts = append(parts, "ALTER")
	}
	if len(parts) == 0 {
		return "USAGE"
	}
	return strings.Join(parts, ", ")
}

// formatTablePrivilege formats table privileges to a string.
func formatTablePrivilege(priv *TablePrivilege) string {
	var parts []string
	if priv.Select {
		parts = append(parts, "SELECT")
	}
	if priv.Insert {
		parts = append(parts, "INSERT")
	}
	if priv.Update {
		parts = append(parts, "UPDATE")
	}
	if priv.Delete {
		parts = append(parts, "DELETE")
	}
	if priv.Create {
		parts = append(parts, "CREATE")
	}
	if priv.Drop {
		parts = append(parts, "DROP")
	}
	if priv.Index {
		parts = append(parts, "INDEX")
	}
	if priv.Alter {
		parts = append(parts, "ALTER")
	}
	if len(parts) == 0 {
		return "USAGE"
	}
	return strings.Join(parts, ", ")
}

// grantsPersistData is the JSON structure for grants persistence.
type grantsPersistData struct {
	GlobalGrants map[string]*GlobalPrivilege            `json:"global_grants"`
	DbGrants     map[string]map[string]*DatabasePrivilege `json:"db_grants"`
	TableGrants  map[string]map[string]map[string]*TablePrivilege `json:"table_grants"`
}

// SaveGrants saves grants to the persistence file.
func (m *Manager) SaveGrants() error {
	if m.grantsFile == "" {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Ensure directory exists
	if m.dataDir != "" {
		if err := os.MkdirAll(m.dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	pdata := grantsPersistData{
		GlobalGrants: m.globalGrants,
		DbGrants:     m.dbGrants,
		TableGrants:  m.tableGrants,
	}

	data, err := json.MarshalIndent(pdata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal grants: %w", err)
	}

	if err := os.WriteFile(m.grantsFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write grants file: %w", err)
	}

	return nil
}

// LoadGrants loads grants from the persistence file.
func (m *Manager) LoadGrants() error {
	if m.grantsFile == "" {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.grantsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet, that's OK
		}
		return fmt.Errorf("failed to read grants file: %w", err)
	}

	var pdata grantsPersistData
	if err := json.Unmarshal(data, &pdata); err != nil {
		return fmt.Errorf("failed to parse grants file: %w", err)
	}

	m.globalGrants = pdata.GlobalGrants
	if m.globalGrants == nil {
		m.globalGrants = make(map[string]*GlobalPrivilege)
	}
	m.dbGrants = pdata.DbGrants
	if m.dbGrants == nil {
		m.dbGrants = make(map[string]map[string]*DatabasePrivilege)
	}
	m.tableGrants = pdata.TableGrants
	if m.tableGrants == nil {
		m.tableGrants = make(map[string]map[string]map[string]*TablePrivilege)
	}

	return nil
}