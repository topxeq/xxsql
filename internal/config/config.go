// Package config provides configuration management for XxSql.
package config

import (
	"time"
)

// Config is the root configuration structure for XxSql.
type Config struct {
	Server     ServerConfig     `json:"server"`
	Network    NetworkConfig    `json:"network"`
	Storage    StorageConfig    `json:"storage"`
	Worker     WorkerConfig     `json:"worker"`
	Log        LogConfig        `json:"log"`
	Auth       AuthConfig       `json:"auth"`
	Security   SecurityConfig   `json:"security"`
	Backup     BackupConfig     `json:"backup"`
	Recovery   RecoveryConfig   `json:"recovery"`
	Safety     SafetyConfig     `json:"safety"`
	Connection ConnectionConfig `json:"connection"`
	WorkerPool WorkerPoolConfig `json:"worker_pool"`
}

// ServerConfig contains server-level configuration.
type ServerConfig struct {
	Name    string `json:"name"`     // Server instance name
	DataDir string `json:"data_dir"` // Data directory path
	PIDFile string `json:"pid_file"` // PID file path
}

// NetworkConfig contains network-related configuration.
type NetworkConfig struct {
	PrivatePort int    `json:"private_port"` // Private protocol port (default: 9527)
	MySQLPort   int    `json:"mysql_port"`   // MySQL compatible port (default: 3306)
	HTTPPort    int    `json:"http_port"`    // HTTP API port (default: 8080)
	Bind        string `json:"bind"`         // Bind address (default: "0.0.0.0")
}

// StorageConfig contains storage engine configuration.
type StorageConfig struct {
	PageSize         int `json:"page_size"`          // Page size in bytes (default: 4096)
	BufferPoolSize   int `json:"buffer_pool_size"`   // Number of buffer pool pages (default: 1000)
	WALMaxSizeMB     int `json:"wal_max_size_mb"`    // Maximum WAL size in MB
	WALSyncInterval  int `json:"wal_sync_interval"`  // WAL sync interval in milliseconds
	CheckpointPages  int `json:"checkpoint_pages"`   // Pages threshold for checkpoint
	CheckpointIntSec int `json:"checkpoint_int_sec"` // Checkpoint interval in seconds
}

// WorkerConfig contains worker configuration (deprecated, use WorkerPoolConfig).
type WorkerConfig struct {
	PoolSize      int `json:"pool_size"`       // Worker pool size (default: 32)
	MaxConnection int `json:"max_connection"`  // Max connections per worker (default: 200)
}

// LogConfig contains logging configuration.
type LogConfig struct {
	Level      string `json:"level"`        // Log level: DEBUG, INFO, WARN, ERROR
	File       string `json:"file"`         // Log file path (empty for stdout)
	MaxSizeMB  int    `json:"max_size_mb"`  // Max log file size before rotation
	MaxBackups int    `json:"max_backups"`  // Max number of old log files to keep
	MaxAgeDays int    `json:"max_age_days"` // Max days to keep old log files
	Compress   bool   `json:"compress"`     // Whether to compress rotated logs
}

// AuthConfig contains authentication configuration.
type AuthConfig struct {
	Enabled       bool   `json:"enabled"`         // Enable authentication
	AdminPassword string `json:"admin_password"`  // Admin password hash
	AdminUser     string `json:"admin_user"`      // Admin username
	SessionTimeouSec int `json:"session_timeout_sec"` // Session timeout in seconds
}

// SecurityConfig contains security configuration.
type SecurityConfig struct {
	// Audit logging
	AuditEnabled    bool   `json:"audit_enabled"`
	AuditFile       string `json:"audit_file"`
	AuditMaxSizeMB  int    `json:"audit_max_size_mb"`
	AuditMaxBackups int    `json:"audit_max_backups"`

	// Rate limiting
	RateLimitEnabled     bool `json:"rate_limit_enabled"`
	RateLimitMaxAttempts int  `json:"rate_limit_max_attempts"`
	RateLimitWindowMin   int  `json:"rate_limit_window_min"`
	RateLimitBlockMin    int  `json:"rate_limit_block_min"`

	// Password policy
	PasswordMinLength    int  `json:"password_min_length"`
	PasswordRequireUpper bool `json:"password_require_upper"`
	PasswordRequireLower bool `json:"password_require_lower"`
	PasswordRequireDigit bool `json:"password_require_digit"`
	PasswordRequireSpecial bool `json:"password_require_special"`
	PasswordExpireDays   int  `json:"password_expire_days"`
	PasswordHistoryCount int  `json:"password_history_count"`

	// TLS
	TLSEnabled  bool   `json:"tls_enabled"`
	TLSMode     string `json:"tls_mode"`     // disabled, optional, required, verify_ca
	TLSCertFile string `json:"tls_cert_file"`
	TLSKeyFile  string `json:"tls_key_file"`
	TLSCAFile   string `json:"tls_ca_file"`

	// IP Access
	IPAccessMode string   `json:"ip_access_mode"` // allow_all, whitelist, blacklist
	IPWhitelist  []string `json:"ip_whitelist"`
	IPBlacklist  []string `json:"ip_blacklist"`
}

// BackupConfig contains backup configuration.
type BackupConfig struct {
	AutoIntervalHours int    `json:"auto_interval_hours"` // Auto backup interval in hours
	KeepCount         int    `json:"keep_count"`          // Number of backups to keep
	BackupDir         string `json:"backup_dir"`          // Backup directory path
}

// RecoveryConfig contains recovery configuration.
type RecoveryConfig struct {
	WALSyncIntervalMs  int `json:"wal_sync_interval_ms"`  // WAL sync interval in ms
	CheckpointIntervalSec int `json:"checkpoint_interval_sec"` // Checkpoint interval in seconds
	CheckpointPages    int `json:"checkpoint_pages"`      // Pages threshold for checkpoint
	WALRetentionSec    int `json:"wal_retention_sec"`     // WAL retention time in seconds
}

// SafetyConfig contains safety configuration.
type SafetyConfig struct {
	EnableChecksum      bool `json:"enable_checksum"`       // Enable data checksums
	MaxRecoveryAttempts int  `json:"max_recovery_attempts"` // Max recovery attempts
}

// ConnectionConfig contains connection management configuration.
type ConnectionConfig struct {
	MaxConnections   int    `json:"max_connections"`   // Maximum total connections
	WaitTimeout      int    `json:"wait_timeout"`      // Connection wait timeout in seconds
	Strategy         string `json:"strategy"`          // Connection strategy: fifo, lifo, random
	DegradeThreshold int    `json:"degrade_threshold"` // Threshold for degraded mode
	IdleTimeout      int    `json:"idle_timeout"`      // Idle connection timeout in seconds
}

// WorkerPoolConfig contains worker pool configuration.
type WorkerPoolConfig struct {
	WorkerCount    int           `json:"worker_count"`    // Number of workers
	TaskQueueSize  int           `json:"task_queue_size"` // Task queue size per worker
	TaskTimeout    time.Duration `json:"task_timeout"`    // Task execution timeout
	Strategy       string        `json:"strategy"`        // Load balancing strategy
}

// DefaultConfig returns a Config with all default values applied.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Name:    "xxsql",
			DataDir: "./data",
			PIDFile: "./xxsql.pid",
		},
		Network: NetworkConfig{
			PrivatePort: 9527,
			MySQLPort:   3306,
			HTTPPort:    8080,
			Bind:        "0.0.0.0",
		},
		Storage: StorageConfig{
			PageSize:         4096,
			BufferPoolSize:   1000,
			WALMaxSizeMB:     100,
			WALSyncInterval:  100,
			CheckpointPages:  1000,
			CheckpointIntSec: 300,
		},
		Worker: WorkerConfig{
			PoolSize:      32,
			MaxConnection: 200,
		},
		Log: LogConfig{
			Level:      "INFO",
			File:       "",
			MaxSizeMB:  100,
			MaxBackups: 5,
			MaxAgeDays: 30,
			Compress:   false,
		},
		Auth: AuthConfig{
			Enabled:       false,
			AdminUser:     "admin",
			SessionTimeouSec: 3600,
		},
		Security: SecurityConfig{
			AuditEnabled:         true,
			AuditFile:            "audit.log",
			AuditMaxSizeMB:       100,
			AuditMaxBackups:      10,
			RateLimitEnabled:     true,
			RateLimitMaxAttempts: 5,
			RateLimitWindowMin:   15,
			RateLimitBlockMin:    30,
			PasswordMinLength:     8,
			PasswordRequireUpper:  true,
			PasswordRequireLower:  true,
			PasswordRequireDigit:  true,
			PasswordRequireSpecial: false,
			PasswordExpireDays:    0,
			PasswordHistoryCount:  5,
			TLSEnabled:           false,
			TLSMode:             "optional",
			IPAccessMode:         "allow_all",
		},
		Backup: BackupConfig{
			AutoIntervalHours: 24,
			KeepCount:         7,
			BackupDir:         "./backup",
		},
		Recovery: RecoveryConfig{
			WALSyncIntervalMs:    100,
			CheckpointIntervalSec: 300,
			CheckpointPages:      1000,
			WALRetentionSec:      86400,
		},
		Safety: SafetyConfig{
			EnableChecksum:      true,
			MaxRecoveryAttempts: 3,
		},
		Connection: ConnectionConfig{
			MaxConnections:   200,
			WaitTimeout:      30,
			Strategy:         "fifo",
			DegradeThreshold: 150,
			IdleTimeout:      28800,
		},
		WorkerPool: WorkerPoolConfig{
			WorkerCount:    32,
			TaskQueueSize:  1000,
			TaskTimeout:    30 * time.Second,
			Strategy:       "round_robin",
		},
	}
}