// Package xxsql provides a Go SQL driver for XxSql database.
package xxsql

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Default values for connection configuration.
const (
	DefaultAddr     = "127.0.0.1:3306"
	DefaultNet      = "tcp"
	DefaultTimeout  = 10 * time.Second
	DefaultReadTimeout  = 30 * time.Second
	DefaultWriteTimeout = 30 * time.Second
	DefaultCharset      = "utf8mb4"
	DefaultCollation    = "utf8mb4_general_ci"
	MaxAllowedPacket    = 4 * 1024 * 1024 // 4MB
)

// Config represents the configuration for an XxSql connection.
type Config struct {
	User             string
	Passwd           string
	Net              string        // Network type: "tcp"
	Addr             string        // Network address: "host:port"
	DBName           string        // Database name
	Timeout          time.Duration // Connection timeout
	ReadTimeout      time.Duration // Read timeout
	WriteTimeout     time.Duration // Write timeout
	Charset          string        // Character set
	Collation        string        // Collation
	TLS              bool          // Enable TLS
	AllowOldPassword bool          // Allow old password authentication
	MaxAllowedPacket int           // Maximum packet size allowed
}

// NewConfig creates a new Config with default values.
func NewConfig() *Config {
	return &Config{
		Net:              DefaultNet,
		Addr:             DefaultAddr,
		Timeout:          DefaultTimeout,
		ReadTimeout:      DefaultReadTimeout,
		WriteTimeout:     DefaultWriteTimeout,
		Charset:          DefaultCharset,
		Collation:        DefaultCollation,
		MaxAllowedPacket: MaxAllowedPacket,
	}
}

// ParseDSN parses a MySQL-style DSN string into a Config.
// DSN format: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
//
// Examples:
//   - root@tcp(localhost:3306)/testdb
//   - admin:secret@tcp(127.0.0.1:3306)/mydb?charset=utf8mb4&timeout=10s
//   - user:pass@/dbname
func ParseDSN(dsn string) (*Config, error) {
	if dsn == "" {
		return nil, errors.New("empty DSN")
	}

	cfg := NewConfig()

	// Split into user part and rest
	// Format: [user[:password]@]...
	atIndex := strings.LastIndex(dsn, "@")
	var userPart, rest string
	if atIndex >= 0 {
		userPart = dsn[:atIndex]
		rest = dsn[atIndex+1:]
	} else {
		rest = dsn
	}

	// Parse user:password
	if userPart != "" {
		colonIndex := strings.Index(userPart, ":")
		if colonIndex >= 0 {
			cfg.User = userPart[:colonIndex]
			cfg.Passwd = userPart[colonIndex+1:]
		} else {
			cfg.User = userPart
		}
	}

	// Parse protocol(address)/dbname[?params]
	// Look for the slash that separates address from dbname
	slashIndex := strings.Index(rest, "/")
	if slashIndex < 0 {
		return nil, errors.New("missing database name in DSN")
	}

	protoPart := rest[:slashIndex]
	dbPart := rest[slashIndex+1:]

	// Parse protocol(address)
	if protoPart != "" {
		// Check for protocol(address) format
		protoRegex := regexp.MustCompile(`^(\w+)\(([^)]+)\)$`)
		matches := protoRegex.FindStringSubmatch(protoPart)
		if matches != nil {
			cfg.Net = matches[1]
			cfg.Addr = matches[2]
		} else {
			// Assume it's just an address
			cfg.Addr = protoPart
		}
	}

	// Parse dbname?params
	questionIndex := strings.Index(dbPart, "?")
	if questionIndex >= 0 {
		cfg.DBName = dbPart[:questionIndex]
		paramsStr := dbPart[questionIndex+1:]
		if err := parseParams(cfg, paramsStr); err != nil {
			return nil, err
		}
	} else {
		cfg.DBName = dbPart
	}

	return cfg, nil
}

// parseParams parses query parameters from the DSN.
func parseParams(cfg *Config, paramsStr string) error {
	if paramsStr == "" {
		return nil
	}

	values, err := url.ParseQuery(paramsStr)
	if err != nil {
		return fmt.Errorf("invalid query parameters: %w", err)
	}

	for key, vals := range values {
		if len(vals) == 0 {
			continue
		}
		val := vals[0]

		switch key {
		case "timeout":
			d, err := time.ParseDuration(val)
			if err != nil {
				return fmt.Errorf("invalid timeout value: %w", err)
			}
			cfg.Timeout = d
		case "readTimeout":
			d, err := time.ParseDuration(val)
			if err != nil {
				return fmt.Errorf("invalid readTimeout value: %w", err)
			}
			cfg.ReadTimeout = d
		case "writeTimeout":
			d, err := time.ParseDuration(val)
			if err != nil {
				return fmt.Errorf("invalid writeTimeout value: %w", err)
			}
			cfg.WriteTimeout = d
		case "charset":
			cfg.Charset = val
		case "collation":
			cfg.Collation = val
		case "tls":
			cfg.TLS = val == "true" || val == "1"
		case "allowOldPasswords":
			cfg.AllowOldPassword = val == "true" || val == "1"
		case "maxAllowedPacket":
			size, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("invalid maxAllowedPacket value: %w", err)
			}
			cfg.MaxAllowedPacket = size
		}
	}

	return nil
}

// FormatDSN returns a DSN string from the configuration.
func (c *Config) FormatDSN() string {
	var dsn strings.Builder

	// User:password@
	if c.User != "" {
		dsn.WriteString(c.User)
		if c.Passwd != "" {
			dsn.WriteString(":")
			dsn.WriteString(c.Passwd)
		}
		dsn.WriteString("@")
	}

	// protocol(address)
	if c.Net != "" && c.Addr != "" {
		dsn.WriteString(c.Net)
		dsn.WriteString("(")
		dsn.WriteString(c.Addr)
		dsn.WriteString(")")
	}

	// /dbname
	dsn.WriteString("/")
	dsn.WriteString(c.DBName)

	// Query parameters
	params := url.Values{}
	if c.Timeout != DefaultTimeout {
		params.Set("timeout", c.Timeout.String())
	}
	if c.ReadTimeout != DefaultReadTimeout {
		params.Set("readTimeout", c.ReadTimeout.String())
	}
	if c.WriteTimeout != DefaultWriteTimeout {
		params.Set("writeTimeout", c.WriteTimeout.String())
	}
	if c.Charset != DefaultCharset {
		params.Set("charset", c.Charset)
	}
	if c.Collation != DefaultCollation {
		params.Set("collation", c.Collation)
	}
	if c.TLS {
		params.Set("tls", "true")
	}
	if c.AllowOldPassword {
		params.Set("allowOldPasswords", "true")
	}
	if c.MaxAllowedPacket != MaxAllowedPacket {
		params.Set("maxAllowedPacket", strconv.Itoa(c.MaxAllowedPacket))
	}

	if len(params) > 0 {
		dsn.WriteString("?")
		dsn.WriteString(params.Encode())
	}

	return dsn.String()
}

// Clone returns a copy of the configuration.
func (c *Config) Clone() *Config {
	return &Config{
		User:             c.User,
		Passwd:           c.Passwd,
		Net:              c.Net,
		Addr:             c.Addr,
		DBName:           c.DBName,
		Timeout:          c.Timeout,
		ReadTimeout:      c.ReadTimeout,
		WriteTimeout:     c.WriteTimeout,
		Charset:          c.Charset,
		Collation:        c.Collation,
		TLS:              c.TLS,
		AllowOldPassword: c.AllowOldPassword,
		MaxAllowedPacket: c.MaxAllowedPacket,
	}
}
