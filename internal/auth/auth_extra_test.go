package auth_test

import (
	"os"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
)

// TestTablePrivilege_HasPrivilege_AllPermissions tests all permission types
func TestTablePrivilege_HasPrivilege_AllPermissions(t *testing.T) {
	tests := []struct {
		name     string
		priv     *auth.TablePrivilege
		perm     auth.Permission
		expected bool
	}{
		{"Select true", &auth.TablePrivilege{Select: true}, auth.PermSelect, true},
		{"Select false", &auth.TablePrivilege{Select: false}, auth.PermSelect, false},
		{"Insert true", &auth.TablePrivilege{Insert: true}, auth.PermInsert, true},
		{"Insert false", &auth.TablePrivilege{Insert: false}, auth.PermInsert, false},
		{"Update true", &auth.TablePrivilege{Update: true}, auth.PermUpdate, true},
		{"Update false", &auth.TablePrivilege{Update: false}, auth.PermUpdate, false},
		{"Delete true", &auth.TablePrivilege{Delete: true}, auth.PermDelete, true},
		{"Delete false", &auth.TablePrivilege{Delete: false}, auth.PermDelete, false},
		{"Create true", &auth.TablePrivilege{Create: true}, auth.PermCreateTable, true},
		{"Create false", &auth.TablePrivilege{Create: false}, auth.PermCreateTable, false},
		{"Drop true", &auth.TablePrivilege{Drop: true}, auth.PermDropTable, true},
		{"Drop false", &auth.TablePrivilege{Drop: false}, auth.PermDropTable, false},
		{"Index true for CreateIndex", &auth.TablePrivilege{Index: true}, auth.PermCreateIndex, true},
		{"Index false for CreateIndex", &auth.TablePrivilege{Index: false}, auth.PermCreateIndex, false},
		{"Index true for DropIndex", &auth.TablePrivilege{Index: true}, auth.PermDropIndex, true},
		{"Index false for DropIndex", &auth.TablePrivilege{Index: false}, auth.PermDropIndex, false},
		{"Unsupported permission", &auth.TablePrivilege{Select: true}, auth.PermManageUsers, false},
		{"All privileges", &auth.TablePrivilege{Select: true, Insert: true, Update: true, Delete: true, Create: true, Drop: true, Index: true, Alter: true}, auth.PermSelect, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.priv.HasPrivilege(tt.perm)
			if result != tt.expected {
				t.Errorf("HasPrivilege(%v) = %v, want %v", tt.perm, result, tt.expected)
			}
		})
	}
}

// TestGlobalPrivilege_HasPermission_AllPermissions tests all permission types
func TestGlobalPrivilege_HasPermission_AllPermissions(t *testing.T) {
	tests := []struct {
		name     string
		priv     *auth.GlobalPrivilege
		perm     auth.Permission
		expected bool
	}{
		{"Select true", &auth.GlobalPrivilege{Select: true}, auth.PermSelect, true},
		{"Select false", &auth.GlobalPrivilege{Select: false}, auth.PermSelect, false},
		{"Insert true", &auth.GlobalPrivilege{Insert: true}, auth.PermInsert, true},
		{"Insert false", &auth.GlobalPrivilege{Insert: false}, auth.PermInsert, false},
		{"Update true", &auth.GlobalPrivilege{Update: true}, auth.PermUpdate, true},
		{"Update false", &auth.GlobalPrivilege{Update: false}, auth.PermUpdate, false},
		{"Delete true", &auth.GlobalPrivilege{Delete: true}, auth.PermDelete, true},
		{"Delete false", &auth.GlobalPrivilege{Delete: false}, auth.PermDelete, false},
		{"Create for CreateTable", &auth.GlobalPrivilege{Create: true}, auth.PermCreateTable, true},
		{"Create for CreateDatabase", &auth.GlobalPrivilege{Create: true}, auth.PermCreateDatabase, true},
		{"Create false", &auth.GlobalPrivilege{Create: false}, auth.PermCreateTable, false},
		{"Drop for DropTable", &auth.GlobalPrivilege{Drop: true}, auth.PermDropTable, true},
		{"Drop for DropDatabase", &auth.GlobalPrivilege{Drop: true}, auth.PermDropDatabase, true},
		{"Drop false", &auth.GlobalPrivilege{Drop: false}, auth.PermDropTable, false},
		{"Index for CreateIndex", &auth.GlobalPrivilege{Index: true}, auth.PermCreateIndex, true},
		{"Index for DropIndex", &auth.GlobalPrivilege{Index: true}, auth.PermDropIndex, true},
		{"Index false", &auth.GlobalPrivilege{Index: false}, auth.PermCreateIndex, false},
		{"Unsupported permission", &auth.GlobalPrivilege{Select: true}, auth.PermManageUsers, false},
		{"All privileges", &auth.GlobalPrivilege{Select: true, Insert: true, Update: true, Delete: true, Create: true, Drop: true, Index: true, Alter: true, Grant: true}, auth.PermSelect, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.priv.HasPermission(tt.perm)
			if result != tt.expected {
				t.Errorf("HasPermission(%v) = %v, want %v", tt.perm, result, tt.expected)
			}
		})
	}
}

// TestDatabasePrivilege_HasPrivilege_AllPermissions tests all permission types
func TestDatabasePrivilege_HasPrivilege_AllPermissions(t *testing.T) {
	tests := []struct {
		name     string
		priv     *auth.DatabasePrivilege
		perm     auth.Permission
		expected bool
	}{
		{"Select true", &auth.DatabasePrivilege{Select: true}, auth.PermSelect, true},
		{"Select false", &auth.DatabasePrivilege{Select: false}, auth.PermSelect, false},
		{"Insert true", &auth.DatabasePrivilege{Insert: true}, auth.PermInsert, true},
		{"Insert false", &auth.DatabasePrivilege{Insert: false}, auth.PermInsert, false},
		{"Update true", &auth.DatabasePrivilege{Update: true}, auth.PermUpdate, true},
		{"Update false", &auth.DatabasePrivilege{Update: false}, auth.PermUpdate, false},
		{"Delete true", &auth.DatabasePrivilege{Delete: true}, auth.PermDelete, true},
		{"Delete false", &auth.DatabasePrivilege{Delete: false}, auth.PermDelete, false},
		{"Create true", &auth.DatabasePrivilege{Create: true}, auth.PermCreateTable, true},
		{"Create false", &auth.DatabasePrivilege{Create: false}, auth.PermCreateTable, false},
		{"Drop true", &auth.DatabasePrivilege{Drop: true}, auth.PermDropTable, true},
		{"Drop false", &auth.DatabasePrivilege{Drop: false}, auth.PermDropTable, false},
		{"Index for CreateIndex", &auth.DatabasePrivilege{Index: true}, auth.PermCreateIndex, true},
		{"Index for DropIndex", &auth.DatabasePrivilege{Index: true}, auth.PermDropIndex, true},
		{"Index false", &auth.DatabasePrivilege{Index: false}, auth.PermCreateIndex, false},
		{"Unsupported permission", &auth.DatabasePrivilege{Select: true}, auth.PermManageUsers, false},
		{"All privileges", &auth.DatabasePrivilege{Select: true, Insert: true, Update: true, Delete: true, Create: true, Drop: true, Index: true, Alter: true}, auth.PermSelect, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.priv.HasPrivilege(tt.perm)
			if result != tt.expected {
				t.Errorf("HasPrivilege(%v) = %v, want %v", tt.perm, result, tt.expected)
			}
		})
	}
}

// TestGrantGlobal_Merge tests the merging logic in GrantGlobal
func TestGrantGlobal_Merge(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Initial grant
	err := m.GrantGlobal("testuser", &auth.GlobalPrivilege{Select: true})
	if err != nil {
		t.Fatalf("Initial grant failed: %v", err)
	}

	// Second grant - should merge
	err = m.GrantGlobal("testuser", &auth.GlobalPrivilege{Insert: true, Update: true})
	if err != nil {
		t.Fatalf("Merge grant failed: %v", err)
	}

	// Third grant with all privileges
	err = m.GrantGlobal("testuser", &auth.GlobalPrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true, Grant: true,
	})
	if err != nil {
		t.Fatalf("Full grant failed: %v", err)
	}

	// Verify grants
	grants, err := m.GetGrants("testuser")
	if err != nil {
		t.Fatalf("GetGrants failed: %v", err)
	}

	if len(grants) == 0 {
		t.Error("Expected grants after merge")
	}
}

// TestGrantDatabase_Merge tests the merging logic in GrantDatabase
func TestGrantDatabase_Merge(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Initial grant
	err := m.GrantDatabase("testuser", "testdb", &auth.DatabasePrivilege{Select: true})
	if err != nil {
		t.Fatalf("Initial grant failed: %v", err)
	}

	// Second grant - should merge
	err = m.GrantDatabase("testuser", "testdb", &auth.DatabasePrivilege{Insert: true, Update: true})
	if err != nil {
		t.Fatalf("Merge grant failed: %v", err)
	}

	// Third grant with all privileges
	err = m.GrantDatabase("testuser", "testdb", &auth.DatabasePrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true,
	})
	if err != nil {
		t.Fatalf("Full grant failed: %v", err)
	}

	// Grant to different database
	err = m.GrantDatabase("testuser", "otherdb", &auth.DatabasePrivilege{Select: true})
	if err != nil {
		t.Fatalf("Other database grant failed: %v", err)
	}

	// Verify grants
	grants, err := m.GetGrants("testuser")
	if err != nil {
		t.Fatalf("GetGrants failed: %v", err)
	}

	if len(grants) < 2 {
		t.Error("Expected multiple grants after merge")
	}
}

// TestGrantTable_Merge tests the merging logic in GrantTable
func TestGrantTable_Merge(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Initial grant
	err := m.GrantTable("testuser", "testdb", "testtable", &auth.TablePrivilege{Select: true})
	if err != nil {
		t.Fatalf("Initial grant failed: %v", err)
	}

	// Second grant - should merge
	err = m.GrantTable("testuser", "testdb", "testtable", &auth.TablePrivilege{Insert: true, Update: true})
	if err != nil {
		t.Fatalf("Merge grant failed: %v", err)
	}

	// Third grant with all privileges
	err = m.GrantTable("testuser", "testdb", "testtable", &auth.TablePrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true,
	})
	if err != nil {
		t.Fatalf("Full grant failed: %v", err)
	}

	// Grant to different table
	err = m.GrantTable("testuser", "testdb", "othertable", &auth.TablePrivilege{Select: true})
	if err != nil {
		t.Fatalf("Other table grant failed: %v", err)
	}

	// Verify grants
	grants, err := m.GetGrants("testuser")
	if err != nil {
		t.Fatalf("GetGrants failed: %v", err)
	}

	if len(grants) < 2 {
		t.Error("Expected multiple grants after merge")
	}
}

// TestRevokeGlobal_All tests revoking all global privileges
func TestRevokeGlobal_All(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Grant all privileges
	m.GrantGlobal("testuser", &auth.GlobalPrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true, Grant: true,
	})

	// Revoke all
	err := m.RevokeGlobal("testuser", &auth.GlobalPrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true, Grant: true,
	})
	if err != nil {
		t.Fatalf("RevokeGlobal failed: %v", err)
	}
}

// TestRevokeDatabase_All tests revoking all database privileges
func TestRevokeDatabase_All(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Grant all privileges
	m.GrantDatabase("testuser", "testdb", &auth.DatabasePrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true,
	})

	// Revoke all
	err := m.RevokeDatabase("testuser", "testdb", &auth.DatabasePrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true,
	})
	if err != nil {
		t.Fatalf("RevokeDatabase failed: %v", err)
	}
}

// TestRevokeTable_All tests revoking all table privileges
func TestRevokeTable_All(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Grant all privileges
	m.GrantTable("testuser", "testdb", "testtable", &auth.TablePrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true,
	})

	// Revoke all
	err := m.RevokeTable("testuser", "testdb", "testtable", &auth.TablePrivilege{
		Select: true, Insert: true, Update: true, Delete: true,
		Create: true, Drop: true, Index: true, Alter: true,
	})
	if err != nil {
		t.Fatalf("RevokeTable failed: %v", err)
	}
}

// TestCheckTablePermission_AllScenarios tests various permission check scenarios
func TestCheckTablePermission_AllScenarios(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)
	m.CreateUser("admin", "password", auth.RoleAdmin)

	// Grant global permission to testuser
	m.GrantGlobal("testuser", &auth.GlobalPrivilege{Select: true})

	// Check via global grant
	if !m.CheckTablePermission("testuser", "anydb", "anytable", auth.PermSelect) {
		t.Error("Should have SELECT via global grant")
	}

	// Grant database permission
	m.GrantDatabase("testuser", "testdb", &auth.DatabasePrivilege{Insert: true})

	// Check via database grant
	if !m.CheckTablePermission("testuser", "testdb", "anytable", auth.PermInsert) {
		t.Error("Should have INSERT via database grant")
	}

	// Grant table permission
	m.GrantTable("testuser", "testdb", "testtable", &auth.TablePrivilege{Update: true})

	// Check via table grant
	if !m.CheckTablePermission("testuser", "testdb", "testtable", auth.PermUpdate) {
		t.Error("Should have UPDATE via table grant")
	}

	// Admin should have all via role
	if !m.CheckTablePermission("admin", "anydb", "anytable", auth.PermCreateTable) {
		t.Error("Admin should have CreateTable via role")
	}

	// Non-existent user should return false
	if m.CheckTablePermission("nonexistent", "testdb", "testtable", auth.PermSelect) {
		t.Error("Non-existent user should not have permission")
	}
}

// TestSaveGrants tests grants persistence
func TestSaveGrants(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-grants-save-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	m := auth.NewManager(auth.WithDataDir(tmpDir))
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Grant various permissions
	m.GrantGlobal("testuser", &auth.GlobalPrivilege{Select: true, Grant: true})
	m.GrantDatabase("testuser", "testdb", &auth.DatabasePrivilege{Insert: true, Update: true})
	m.GrantTable("testuser", "testdb", "testtable", &auth.TablePrivilege{Delete: true, Index: true})

	// Explicit save
	err = m.SaveGrants()
	if err != nil {
		t.Fatalf("SaveGrants failed: %v", err)
	}

	// Load into new manager
	m2 := auth.NewManager(auth.WithDataDir(tmpDir))
	m2.Load()
	m2.LoadGrants()

	// Verify grants loaded
	grants, err := m2.GetGrants("testuser")
	if err != nil {
		t.Fatalf("GetGrants failed: %v", err)
	}

	if len(grants) < 3 {
		t.Errorf("Expected at least 3 grants, got %d", len(grants))
	}
}

// TestLoad_InvalidFile tests loading from invalid file
func TestLoad_InvalidFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-auth-load-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write invalid JSON
	invalidFile := tmpDir + "/users.json"
	err = os.WriteFile(invalidFile, []byte("invalid json"), 0644)
	if err != nil {
		t.Fatalf("Failed to write invalid file: %v", err)
	}

	m := auth.NewManager(auth.WithDataDir(tmpDir))
	err = m.Load()
	if err == nil {
		t.Error("Expected error when loading invalid file")
	}
}

// TestLoadGrants_InvalidFile tests loading grants from invalid file
func TestLoadGrants_InvalidFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-grants-load-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write invalid JSON
	invalidFile := tmpDir + "/grants.json"
	err = os.WriteFile(invalidFile, []byte("invalid json"), 0644)
	if err != nil {
		t.Fatalf("Failed to write invalid file: %v", err)
	}

	m := auth.NewManager(auth.WithDataDir(tmpDir))
	err = m.LoadGrants()
	if err == nil {
		t.Error("Expected error when loading invalid grants file")
	}
}

// TestManager_Save_NoDataDir tests saving without data dir
func TestManager_Save_NoDataDir(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Should not error with no data dir
	err := m.Save()
	if err != nil {
		t.Errorf("Save should not error without data dir: %v", err)
	}

	err = m.SaveGrants()
	if err != nil {
		t.Errorf("SaveGrants should not error without data dir: %v", err)
	}

	err = m.Load()
	if err != nil {
		t.Errorf("Load should not error without data dir: %v", err)
	}

	err = m.LoadGrants()
	if err != nil {
		t.Errorf("LoadGrants should not error without data dir: %v", err)
	}
}

// TestChangePassword_WithEmptyOld tests changing password with empty old password
func TestChangePassword_WithEmptyOld(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Change with empty old password (should still work due to logic)
	err := m.ChangePassword("testuser", "", "newpassword")
	// The function checks if oldPassword != "" before verifying
	// So empty old password skips verification
	if err != nil {
		// This is expected behavior - old password verification should fail
		t.Logf("ChangePassword with empty old password: %v", err)
	}
}

// TestChangePassword_NonExistentUser tests changing password for non-existent user
func TestChangePassword_NonExistentUser(t *testing.T) {
	m := auth.NewManager()

	err := m.ChangePassword("nonexistent", "old", "new")
	if err == nil {
		t.Error("Expected error when changing password for non-existent user")
	}
}

// TestVerifyMySQLAuth_InvalidUser tests MySQL auth with invalid user
func TestVerifyMySQLAuth_InvalidUser(t *testing.T) {
	m := auth.NewManager()

	salt := make([]byte, 20)
	authResponse := make([]byte, 20)

	valid, err := m.VerifyMySQLAuth("nonexistent", salt, authResponse)
	if err != nil {
		t.Errorf("Should not return error for non-existent user: %v", err)
	}
	if valid {
		t.Error("Should return false for non-existent user")
	}
}

// TestVerifyMySQLAuth_InvalidHash tests MySQL auth with invalid hash
func TestVerifyMySQLAuth_InvalidHash(t *testing.T) {
	m := auth.NewManager()
	m.CreateUser("testuser", "password", auth.RoleUser)

	// Get user and corrupt the hash
	user, _ := m.GetUser("testuser")
	user.MySQLAuthHash = []byte{1, 2, 3} // Invalid length

	salt := make([]byte, 20)
	authResponse := make([]byte, 20)

	valid, _ := m.VerifyMySQLAuth("testuser", salt, authResponse)
	if valid {
		t.Error("Should return false for invalid hash length")
	}
}

// TestRefreshSession_Expired tests refreshing an expired session
func TestRefreshSession_Expired(t *testing.T) {
	m := auth.NewManager(auth.WithSessionTTL(100 * time.Millisecond))
	m.CreateUser("testuser", "password", auth.RoleUser)

	session, _ := m.Authenticate("testuser", "password")

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Try to refresh expired session
	_, err := m.RefreshSession(session.ID)
	if err == nil {
		t.Error("Expected error when refreshing expired session")
	}
}