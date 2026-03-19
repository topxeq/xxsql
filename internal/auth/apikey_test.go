package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAPIKey_IsExpired(t *testing.T) {
	tests := []struct {
		name      string
		expiresAt time.Time
		want      bool
	}{
		{"no expiration", time.Time{}, false},
		{"future expiration", time.Now().Add(24 * time.Hour), false},
		{"past expiration", time.Now().Add(-1 * time.Hour), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := &APIKey{ExpiresAt: tt.expiresAt}
			if got := key.IsExpired(); got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAPIKey_HasPermission(t *testing.T) {
	key := &APIKey{
		Permissions: PermSelect | PermInsert | PermUpdate,
	}

	if !key.HasPermission(PermSelect) {
		t.Error("should have SELECT permission")
	}
	if !key.HasPermission(PermInsert) {
		t.Error("should have INSERT permission")
	}
	if key.HasPermission(PermDelete) {
		t.Error("should not have DELETE permission")
	}
	if key.HasPermission(PermManageUsers) {
		t.Error("should not have MANAGE_USERS permission")
	}
}

func TestAPIKeyManager_GenerateKey(t *testing.T) {
	mgr := NewAPIKeyManager("")

	fullKey, key, err := mgr.GenerateKey("test-key", "testuser", PermSelect|PermInsert, 0)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	// Verify key format
	if len(fullKey) < 20 {
		t.Errorf("key too short: %s", fullKey)
	}
	if fullKey[:6] != "xxsql_" {
		t.Errorf("key should start with 'xxsql_': %s", fullKey)
	}

	// Verify key struct
	if key.Name != "test-key" {
		t.Errorf("name = %s, want 'test-key'", key.Name)
	}
	if key.Username != "testuser" {
		t.Errorf("username = %s, want 'testuser'", key.Username)
	}
	if key.Permissions != PermSelect|PermInsert {
		t.Errorf("permissions = %v, want %v", key.Permissions, PermSelect|PermInsert)
	}
	if !key.Enabled {
		t.Error("key should be enabled by default")
	}
	if !key.ExpiresAt.IsZero() {
		t.Error("key should have no expiration")
	}
}

func TestAPIKeyManager_GenerateKeyWithExpiration(t *testing.T) {
	mgr := NewAPIKeyManager("")

	_, key, err := mgr.GenerateKey("exp-key", "user", PermSelect, 24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	if key.ExpiresAt.IsZero() {
		t.Error("key should have expiration")
	}
	if key.IsExpired() {
		t.Error("newly created key should not be expired")
	}
}

func TestAPIKeyManager_ValidateKey(t *testing.T) {
	mgr := NewAPIKeyManager("")

	fullKey, _, err := mgr.GenerateKey("test", "user", PermSelect, 0)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	// Validate correct key
	key, err := mgr.ValidateKey(fullKey)
	if err != nil {
		t.Fatalf("ValidateKey failed: %v", err)
	}
	if key.Name != "test" {
		t.Errorf("name = %s, want 'test'", key.Name)
	}

	// Validate wrong key
	_, err = mgr.ValidateKey("xxsql_invalid_key")
	if err == nil {
		t.Error("ValidateKey should fail for invalid key")
	}
}

func TestAPIKeyManager_ValidateKey_Disabled(t *testing.T) {
	mgr := NewAPIKeyManager("")

	fullKey, _, err := mgr.GenerateKey("test", "user", PermSelect, 0)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	// Disable the key
	mgr.EnableKey("ak_", false) // This won't work, need actual ID

	// Get the actual key ID
	keys := mgr.ListKeys("user")
	if len(keys) == 0 {
		t.Fatal("no keys found")
	}
	mgr.EnableKey(keys[0].ID, false)

	// Try to validate disabled key
	_, err = mgr.ValidateKey(fullKey)
	if err == nil {
		t.Error("ValidateKey should fail for disabled key")
	}
}

func TestAPIKeyManager_ListKeys(t *testing.T) {
	mgr := NewAPIKeyManager("")

	// Generate keys for different users
	_, _, _ = mgr.GenerateKey("key1", "user1", PermSelect, 0)
	_, _, _ = mgr.GenerateKey("key2", "user1", PermInsert, 0)
	_, _, _ = mgr.GenerateKey("key3", "user2", PermUpdate, 0)

	// List keys for user1
	keys1 := mgr.ListKeys("user1")
	if len(keys1) != 2 {
		t.Errorf("user1 should have 2 keys, got %d", len(keys1))
	}

	// List keys for user2
	keys2 := mgr.ListKeys("user2")
	if len(keys2) != 1 {
		t.Errorf("user2 should have 1 key, got %d", len(keys2))
	}

	// List all keys
	allKeys := mgr.ListAllKeys()
	if len(allKeys) != 3 {
		t.Errorf("total keys should be 3, got %d", len(allKeys))
	}
}

func TestAPIKeyManager_RevokeKey(t *testing.T) {
	mgr := NewAPIKeyManager("")

	_, key, err := mgr.GenerateKey("test", "user", PermSelect, 0)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	// Revoke the key
	err = mgr.RevokeKey(key.ID)
	if err != nil {
		t.Fatalf("RevokeKey failed: %v", err)
	}

	// Verify key is gone
	_, err = mgr.GetKey(key.ID)
	if err == nil {
		t.Error("key should be revoked")
	}

	// Verify user's key list is updated
	keys := mgr.ListKeys("user")
	if len(keys) != 0 {
		t.Errorf("user should have 0 keys after revocation, got %d", len(keys))
	}
}

func TestAPIKeyManager_EnableKey(t *testing.T) {
	mgr := NewAPIKeyManager("")

	_, key, err := mgr.GenerateKey("test", "user", PermSelect, 0)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	// Disable
	err = mgr.EnableKey(key.ID, false)
	if err != nil {
		t.Fatalf("EnableKey(false) failed: %v", err)
	}

	key, _ = mgr.GetKey(key.ID)
	if key.Enabled {
		t.Error("key should be disabled")
	}

	// Enable
	err = mgr.EnableKey(key.ID, true)
	if err != nil {
		t.Fatalf("EnableKey(true) failed: %v", err)
	}

	key, _ = mgr.GetKey(key.ID)
	if !key.Enabled {
		t.Error("key should be enabled")
	}
}

func TestAPIKeyManager_DeleteUserKeys(t *testing.T) {
	mgr := NewAPIKeyManager("")

	// Generate multiple keys for a user
	_, _, _ = mgr.GenerateKey("key1", "user1", PermSelect, 0)
	_, _, _ = mgr.GenerateKey("key2", "user1", PermInsert, 0)
	_, _, _ = mgr.GenerateKey("key3", "user2", PermUpdate, 0)

	// Delete all keys for user1
	mgr.DeleteUserKeys("user1")

	// Verify
	keys1 := mgr.ListKeys("user1")
	if len(keys1) != 0 {
		t.Errorf("user1 should have 0 keys, got %d", len(keys1))
	}

	keys2 := mgr.ListKeys("user2")
	if len(keys2) != 1 {
		t.Errorf("user2 should still have 1 key, got %d", len(keys2))
	}
}

func TestAPIKeyManager_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create manager and generate key
	mgr1 := NewAPIKeyManager(tmpDir)
	fullKey, _, err := mgr1.GenerateKey("test", "user", PermSelect, 0)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	// Save
	if err := mgr1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Create new manager and load
	mgr2 := NewAPIKeyManager(tmpDir)
	if err := mgr2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify key works
	key, err := mgr2.ValidateKey(fullKey)
	if err != nil {
		t.Fatalf("ValidateKey failed after load: %v", err)
	}
	if key.Name != "test" {
		t.Errorf("name = %s, want 'test'", key.Name)
	}
}

func TestAPIKeyManager_Stats(t *testing.T) {
	mgr := NewAPIKeyManager("")

	// Generate some keys
	_, _, _ = mgr.GenerateKey("active1", "user", PermSelect, 0)
	_, _, _ = mgr.GenerateKey("active2", "user", PermSelect, 0)

	// Generate and disable a key
	_, key, _ := mgr.GenerateKey("disabled", "user", PermSelect, 0)
	mgr.EnableKey(key.ID, false)

	stats := mgr.Stats()

	if stats["total"] != 3 {
		t.Errorf("total = %v, want 3", stats["total"])
	}
	if stats["active"] != 2 {
		t.Errorf("active = %v, want 2", stats["active"])
	}
	if stats["disabled"] != 1 {
		t.Errorf("disabled = %v, want 1", stats["disabled"])
	}
	if stats["expired"] != 0 {
		t.Errorf("expired = %v, want 0", stats["expired"])
	}
}

func TestAPIKeyManager_LoadNonExistent(t *testing.T) {
	mgr := NewAPIKeyManager("/nonexistent/path")

	// Should not error
	err := mgr.Load()
	if err != nil {
		t.Errorf("Load should not error for non-existent file: %v", err)
	}
}

func TestAPIKeyManager_SaveNoDataDir(t *testing.T) {
	mgr := NewAPIKeyManager("")

	// Should not error
	err := mgr.Save()
	if err != nil {
		t.Errorf("Save should not error when no data dir: %v", err)
	}
}

func TestAPIKeyManager_ValidateKey_InvalidFormat(t *testing.T) {
	mgr := NewAPIKeyManager("")

	tests := []string{
		"",
		"invalid",
		"xxsql_",          // too short
		"wrong_prefix_abc",
	}

	for _, key := range tests {
		_, err := mgr.ValidateKey(key)
		if err == nil {
			t.Errorf("ValidateKey(%q) should fail", key)
		}
	}
}

func TestAPIKeyManager_GetKeyNotFound(t *testing.T) {
	mgr := NewAPIKeyManager("")

	_, err := mgr.GetKey("nonexistent")
	if err == nil {
		t.Error("GetKey should fail for nonexistent key")
	}
}

func TestAPIKeyManager_RevokeKeyNotFound(t *testing.T) {
	mgr := NewAPIKeyManager("")

	err := mgr.RevokeKey("nonexistent")
	if err == nil {
		t.Error("RevokeKey should fail for nonexistent key")
	}
}

func TestAPIKeyManager_EnableKeyNotFound(t *testing.T) {
	mgr := NewAPIKeyManager("")

	err := mgr.EnableKey("nonexistent", true)
	if err == nil {
		t.Error("EnableKey should fail for nonexistent key")
	}
}

// TestAPIKeyManager_LoadInvalidJSON tests loading corrupted file
func TestAPIKeyManager_LoadInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Write invalid JSON
	invalidFile := filepath.Join(tmpDir, "api_keys.json")
	os.WriteFile(invalidFile, []byte("invalid json {"), 0644)

	mgr := NewAPIKeyManager(tmpDir)
	err := mgr.Load()
	if err == nil {
		t.Error("Load should fail for invalid JSON")
	}
}