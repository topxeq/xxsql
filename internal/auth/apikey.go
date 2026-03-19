package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// APIKey represents an API key for programmatic access.
type APIKey struct {
	ID          string    `json:"id"`           // Key identifier (e.g., "ak_abc123")
	Name        string    `json:"name"`         // Human-readable name
	KeyHash     string    `json:"key_hash"`     // SHA256 hash of the full key
	Username    string    `json:"username"`     // Owner username
	Permissions Permission `json:"permissions"`  // Granted permissions
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`   // Zero means no expiration
	LastUsedAt  time.Time `json:"last_used_at"`
	Enabled     bool      `json:"enabled"`
}

// IsExpired checks if the API key is expired.
func (k *APIKey) IsExpired() bool {
	if k.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(k.ExpiresAt)
}

// HasPermission checks if the key has the given permission.
func (k *APIKey) HasPermission(perm Permission) bool {
	return k.Permissions&perm != 0
}

// APIKeyManager manages API keys.
type APIKeyManager struct {
	mu          sync.RWMutex
	keys        map[string]*APIKey  // id -> APIKey
	keysByUser  map[string][]string // username -> []keyID
	dataDir     string
	persistFile string
}

// NewAPIKeyManager creates a new API key manager.
func NewAPIKeyManager(dataDir string) *APIKeyManager {
	return &APIKeyManager{
		keys:       make(map[string]*APIKey),
		keysByUser: make(map[string][]string),
		dataDir:    dataDir,
	}
}

// GenerateKey generates a new API key.
// Returns the full key (to be shown once) and the stored APIKey struct.
func (m *APIKeyManager) GenerateKey(name, username string, permissions Permission, expiresIn time.Duration) (string, *APIKey, error) {
	// Generate random key: xxsql_ak_<32 random bytes>
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}

	keyID := "ak_" + hex.EncodeToString(randomBytes[:8])
	fullKey := "xxsql_" + keyID + "_" + hex.EncodeToString(randomBytes[8:])

	// Hash the full key for storage
	hash := sha256.Sum256([]byte(fullKey))

	now := time.Now()
	apiKey := &APIKey{
		ID:          keyID,
		Name:        name,
		KeyHash:     hex.EncodeToString(hash[:]),
		Username:    username,
		Permissions: permissions,
		CreatedAt:   now,
		Enabled:     true,
	}

	if expiresIn > 0 {
		apiKey.ExpiresAt = now.Add(expiresIn)
	}

	m.mu.Lock()
	m.keys[keyID] = apiKey
	m.keysByUser[username] = append(m.keysByUser[username], keyID)
	m.mu.Unlock()

	// Persist
	_ = m.Save()

	return fullKey, apiKey, nil
}

// ValidateKey validates an API key and returns the associated APIKey struct.
func (m *APIKeyManager) ValidateKey(fullKey string) (*APIKey, error) {
	// Verify key format
	if len(fullKey) < 10 || fullKey[:6] != "xxsql_" {
		return nil, fmt.Errorf("invalid key format")
	}

	// Hash the provided key
	hash := sha256.Sum256([]byte(fullKey))
	hashStr := hex.EncodeToString(hash[:])

	m.mu.Lock()
	defer m.mu.Unlock()

	// Find key by hash
	for _, key := range m.keys {
		if key.KeyHash == hashStr {
			// Check if key is valid
			if !key.Enabled {
				return nil, fmt.Errorf("API key is disabled")
			}
			if key.IsExpired() {
				return nil, fmt.Errorf("API key has expired")
			}

			// Update last used
			key.LastUsedAt = time.Now()
			return key, nil
		}
	}

	return nil, fmt.Errorf("invalid API key")
}

// GetKey retrieves an API key by ID.
func (m *APIKeyManager) GetKey(keyID string) (*APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key, exists := m.keys[keyID]
	if !exists {
		return nil, fmt.Errorf("API key not found")
	}
	return key, nil
}

// ListKeys lists all API keys for a user.
func (m *APIKeyManager) ListKeys(username string) []*APIKey {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keyIDs, exists := m.keysByUser[username]
	if !exists {
		return nil
	}

	keys := make([]*APIKey, 0, len(keyIDs))
	for _, id := range keyIDs {
		if key, exists := m.keys[id]; exists {
			keys = append(keys, key)
		}
	}
	return keys
}

// ListAllKeys lists all API keys (admin only).
func (m *APIKeyManager) ListAllKeys() []*APIKey {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]*APIKey, 0, len(m.keys))
	for _, key := range m.keys {
		keys = append(keys, key)
	}
	return keys
}

// RevokeKey revokes (deletes) an API key.
func (m *APIKeyManager) RevokeKey(keyID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, exists := m.keys[keyID]
	if !exists {
		return fmt.Errorf("API key not found")
	}

	delete(m.keys, keyID)

	// Remove from user's key list
	userKeys := m.keysByUser[key.Username]
	for i, id := range userKeys {
		if id == keyID {
			m.keysByUser[key.Username] = append(userKeys[:i], userKeys[i+1:]...)
			break
		}
	}

	// Persist
	go m.Save()

	return nil
}

// EnableKey enables or disables an API key.
func (m *APIKeyManager) EnableKey(keyID string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, exists := m.keys[keyID]
	if !exists {
		return fmt.Errorf("API key not found")
	}

	key.Enabled = enabled
	go m.Save()

	return nil
}

// DeleteUserKeys deletes all API keys for a user.
func (m *APIKeyManager) DeleteUserKeys(username string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	keyIDs := m.keysByUser[username]
	for _, id := range keyIDs {
		delete(m.keys, id)
	}
	delete(m.keysByUser, username)

	go m.Save()
}

// apiKeyPersistData is the JSON structure for persistence.
type apiKeyPersistData struct {
	Keys []*APIKey `json:"keys"`
}

// Save saves API keys to the persistence file.
func (m *APIKeyManager) Save() error {
	if m.dataDir == "" {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Ensure directory exists
	if err := os.MkdirAll(m.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	pdata := apiKeyPersistData{
		Keys: make([]*APIKey, 0, len(m.keys)),
	}
	for _, key := range m.keys {
		pdata.Keys = append(pdata.Keys, key)
	}

	data, err := json.MarshalIndent(pdata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal API keys: %w", err)
	}

	file := filepath.Join(m.dataDir, "api_keys.json")
	if err := os.WriteFile(file, data, 0600); err != nil {
		return fmt.Errorf("failed to write API keys file: %w", err)
	}

	return nil
}

// Load loads API keys from the persistence file.
func (m *APIKeyManager) Load() error {
	if m.dataDir == "" {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	file := filepath.Join(m.dataDir, "api_keys.json")
	data, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet
		}
		return fmt.Errorf("failed to read API keys file: %w", err)
	}

	var pdata apiKeyPersistData
	if err := json.Unmarshal(data, &pdata); err != nil {
		return fmt.Errorf("failed to parse API keys file: %w", err)
	}

	for _, key := range pdata.Keys {
		m.keys[key.ID] = key
		m.keysByUser[key.Username] = append(m.keysByUser[key.Username], key.ID)
	}

	return nil
}

// Stats returns statistics about API keys.
func (m *APIKeyManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	active := 0
	disabled := 0
	expired := 0

	for _, key := range m.keys {
		if !key.Enabled {
			disabled++
		} else if key.IsExpired() {
			expired++
		} else {
			active++
		}
	}

	return map[string]interface{}{
		"total":    len(m.keys),
		"active":   active,
		"disabled": disabled,
		"expired":  expired,
	}
}