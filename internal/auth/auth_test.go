package auth_test

import (
	"crypto/sha1"
	"os"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
)

func TestUserRoles(t *testing.T) {
	tests := []struct {
		role     auth.UserRole
		expected string
	}{
		{auth.RoleAdmin, "admin"},
		{auth.RoleUser, "user"},
	}

	for _, tt := range tests {
		if tt.role.String() != tt.expected {
			t.Errorf("Role %d: expected %s, got %s", tt.role, tt.expected, tt.role.String())
		}
	}
}

func TestCreateUser(t *testing.T) {
	m := auth.NewManager()

	user, err := m.CreateUser("testuser", "password123", auth.RoleUser)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	if user.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %s", user.Username)
	}

	if user.Role != auth.RoleUser {
		t.Errorf("Expected role RoleUser, got %d", user.Role)
	}

	if len(user.MySQLAuthHash) != 20 {
		t.Errorf("Expected MySQL auth hash length 20, got %d", len(user.MySQLAuthHash))
	}

	// Duplicate user should fail
	_, err = m.CreateUser("testuser", "password456", auth.RoleUser)
	if err == nil {
		t.Error("Expected error when creating duplicate user")
	}
}

func TestGetUser(t *testing.T) {
	m := auth.NewManager()

	_, err := m.CreateUser("alice", "password", auth.RoleAdmin)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	user, err := m.GetUser("alice")
	if err != nil {
		t.Fatalf("Failed to get user: %v", err)
	}

	if user.Username != "alice" {
		t.Errorf("Expected username 'alice', got %s", user.Username)
	}

	// Non-existent user should fail
	_, err = m.GetUser("nonexistent")
	if err == nil {
		t.Error("Expected error when getting non-existent user")
	}
}

func TestDeleteUser(t *testing.T) {
	m := auth.NewManager()

	_, _ = m.CreateUser("user1", "password", auth.RoleUser)
	_, _ = m.CreateUser("user2", "password", auth.RoleAdmin)

	err := m.DeleteUser("user1")
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// User should no longer exist
	_, err = m.GetUser("user1")
	if err == nil {
		t.Error("Expected error when getting deleted user")
	}

	// Non-existent user should fail
	err = m.DeleteUser("nonexistent")
	if err == nil {
		t.Error("Expected error when deleting non-existent user")
	}
}

func TestDeleteLastAdmin(t *testing.T) {
	m := auth.NewManager()

	_, _ = m.CreateUser("admin", "password", auth.RoleAdmin)

	// Should not be able to delete last admin
	err := m.DeleteUser("admin")
	if err == nil {
		t.Error("Expected error when deleting last admin")
	}

	// Create another admin
	_, _ = m.CreateUser("admin2", "password", auth.RoleAdmin)

	// Now should be able to delete first admin
	err = m.DeleteUser("admin")
	if err != nil {
		t.Errorf("Failed to delete admin when another exists: %v", err)
	}
}

func TestListUsers(t *testing.T) {
	m := auth.NewManager()

	_, _ = m.CreateUser("user1", "password", auth.RoleUser)
	_, _ = m.CreateUser("user2", "password", auth.RoleAdmin)

	users := m.ListUsers()
	if len(users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(users))
	}
}

func TestAuthenticate(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "correctpassword", auth.RoleUser)

	// Correct password
	session, err := m.Authenticate("testuser", "correctpassword")
	if err != nil {
		t.Fatalf("Failed to authenticate with correct password: %v", err)
	}

	if session.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %s", session.Username)
	}

	if session.Role != auth.RoleUser {
		t.Errorf("Expected role RoleUser, got %d", session.Role)
	}

	// Wrong password
	_, err = m.Authenticate("testuser", "wrongpassword")
	if err == nil {
		t.Error("Expected error with wrong password")
	}

	// Non-existent user
	_, err = m.Authenticate("nonexistent", "password")
	if err == nil {
		t.Error("Expected error with non-existent user")
	}
}

func TestSessionExpiry(t *testing.T) {
	m := auth.NewManager(auth.WithSessionTTL(100 * time.Millisecond))
	m.CreateUser("testuser", "password", auth.RoleUser)

	session, _ := m.Authenticate("testuser", "password")

	// Session should be valid
	_, err := m.ValidateSession(session.ID)
	if err != nil {
		t.Errorf("Session should be valid: %v", err)
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Session should be expired
	_, err = m.ValidateSession(session.ID)
	if err == nil {
		t.Error("Expected error with expired session")
	}
}

func TestSessionCleanup(t *testing.T) {
	m := auth.NewManager(auth.WithSessionTTL(100 * time.Millisecond))
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Create session
	m.Authenticate("testuser", "password")

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Cleanup
	count := m.CleanupExpiredSessions()
	if count != 1 {
		t.Errorf("Expected 1 expired session cleaned, got %d", count)
	}
}

func TestChangePassword(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "oldpassword", auth.RoleUser)

	// Change password with correct old password
	err := m.ChangePassword("testuser", "oldpassword", "newpassword")
	if err != nil {
		t.Fatalf("Failed to change password: %v", err)
	}

	// Authenticate with new password
	_, err = m.Authenticate("testuser", "newpassword")
	if err != nil {
		t.Errorf("Failed to authenticate with new password: %v", err)
	}

	// Old password should fail
	_, err = m.Authenticate("testuser", "oldpassword")
	if err == nil {
		t.Error("Expected error with old password")
	}

	// Wrong old password should fail
	err = m.ChangePassword("testuser", "wrongold", "anothernew")
	if err == nil {
		t.Error("Expected error with wrong old password")
	}
}

func TestPermissions(t *testing.T) {
	// Admin should have all permissions
	adminPerms := auth.RolePermissions[auth.RoleAdmin]

	perms := []auth.Permission{
		auth.PermManageUsers,
		auth.PermCreateTable,
		auth.PermDropTable,
		auth.PermSelect,
		auth.PermInsert,
		auth.PermUpdate,
		auth.PermDelete,
	}

	for _, perm := range perms {
		if adminPerms&perm == 0 {
			t.Errorf("Admin should have permission %d", perm)
		}
	}

	// User should have limited permissions
	userPerms := auth.RolePermissions[auth.RoleUser]

	userAllowed := []auth.Permission{
		auth.PermSelect,
		auth.PermInsert,
		auth.PermUpdate,
		auth.PermDelete,
	}

	for _, perm := range userAllowed {
		if userPerms&perm == 0 {
			t.Errorf("User should have permission %d", perm)
		}
	}

	userDenied := []auth.Permission{
		auth.PermManageUsers,
		auth.PermCreateTable,
		auth.PermDropTable,
	}

	for _, perm := range userDenied {
		if userPerms&perm != 0 {
			t.Errorf("User should not have permission %d", perm)
		}
	}
}

func TestSessionHasPermission(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("admin", "password", auth.RoleAdmin)
	m.CreateUser("user", "password", auth.RoleUser)

	adminSession, _ := m.Authenticate("admin", "password")
	userSession, _ := m.Authenticate("user", "password")

	if !adminSession.HasPermission(auth.PermCreateTable) {
		t.Error("Admin should have CreateTable permission")
	}

	if userSession.HasPermission(auth.PermCreateTable) {
		t.Error("User should not have CreateTable permission")
	}

	if !userSession.HasPermission(auth.PermSelect) {
		t.Error("User should have Select permission")
	}
}

func TestCheckPermission(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("admin", "password", auth.RoleAdmin)
	m.CreateUser("user", "password", auth.RoleUser)

	hasPerm, err := m.CheckPermission("admin", auth.PermCreateTable)
	if err != nil || !hasPerm {
		t.Error("Admin should have CreateTable permission")
	}

	hasPerm, err = m.CheckPermission("user", auth.PermCreateTable)
	if err != nil || hasPerm {
		t.Error("User should not have CreateTable permission")
	}
}

func TestUserPersistence(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "xxsql-auth-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager with persistence
	m1 := auth.NewManager(auth.WithDataDir(tmpDir))
	m1.CreateUser("user1", "password1", auth.RoleUser)
	m1.CreateUser("admin1", "adminpass", auth.RoleAdmin)

	// Verify save
	if err := m1.Save(); err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	// Create new manager and load
	m2 := auth.NewManager(auth.WithDataDir(tmpDir))
	if err := m2.Load(); err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	// Verify users loaded
	user, err := m2.GetUser("user1")
	if err != nil {
		t.Fatalf("Failed to get user: %v", err)
	}

	if user.Role != auth.RoleUser {
		t.Errorf("Expected role RoleUser, got %d", user.Role)
	}

	// Verify password works
	session, err := m2.Authenticate("user1", "password1")
	if err != nil {
		t.Errorf("Failed to authenticate loaded user: %v", err)
	}

	if session.Username != "user1" {
		t.Errorf("Expected username 'user1', got %s", session.Username)
	}
}

func TestMySQLAuthVerification(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("mysqluser", "mysqlpassword", auth.RoleUser)

	// Get the user to access the MySQL auth hash
	_, err := m.GetUser("mysqluser")
	if err != nil {
		t.Fatalf("Failed to get user: %v", err)
	}

	// Simulate MySQL auth flow
	// Salt is 20 bytes
	salt := make([]byte, 20)
	for i := range salt {
		salt[i] = byte(i)
	}

	// The client would compute: SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	// We simulate this

	// Compute what the client would send
	sha1Hash := sha1.Sum([]byte("mysqlpassword"))
	hash2 := sha1.Sum(sha1Hash[:])
	combined := append(salt, hash2[:]...)
	hash3 := sha1.Sum(combined)

	authResponse := make([]byte, 20)
	for i := 0; i < 20; i++ {
		authResponse[i] = sha1Hash[i] ^ hash3[i]
	}

	// Verify using the auth manager
	valid, err := m.VerifyMySQLAuth("mysqluser", salt, authResponse)
	if err != nil {
		t.Fatalf("Verification error: %v", err)
	}

	if !valid {
		t.Error("MySQL auth verification should succeed with correct password")
	}

	// Wrong password should fail
	wrongResponse := make([]byte, 20)
	valid, _ = m.VerifyMySQLAuth("mysqluser", salt, wrongResponse)
	if valid {
		t.Error("MySQL auth verification should fail with wrong response")
	}
}
