package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
)

// TLSMode represents the TLS configuration mode.
type TLSMode int

const (
	// TLSModeDisabled means TLS is not enabled.
	TLSModeDisabled TLSMode = iota
	// TLSModeOptional means TLS is optional (both secure and insecure connections allowed).
	TLSModeOptional
	// TLSModeRequired means TLS is required for all connections.
	TLSModeRequired
	// TLSModeVerifyCA means TLS is required and client certificate must be verified.
	TLSModeVerifyCA
)

// String returns the string representation.
func (m TLSMode) String() string {
	switch m {
	case TLSModeDisabled:
		return "DISABLED"
	case TLSModeOptional:
		return "OPTIONAL"
	case TLSModeRequired:
		return "REQUIRED"
	case TLSModeVerifyCA:
		return "VERIFY_CA"
	default:
		return "UNKNOWN"
	}
}

// TLSConfig contains TLS configuration.
type TLSConfig struct {
	Enabled        bool
	Mode           TLSMode
	CertFile       string
	KeyFile        string
	CAFile         string
	MinVersion     uint16
	CipherSuites   []uint16
}

// DefaultTLSConfig returns default TLS configuration.
func DefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		Enabled:    false,
		Mode:       TLSModeOptional,
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}
}

// TLSManager manages TLS configuration and certificates.
type TLSManager struct {
	mu       sync.RWMutex
	config   *TLSConfig
	cert     *tls.Certificate
	caPool   *x509.CertPool
	tlsConfig *tls.Config
	audit    *AuditLogger
}

// NewTLSManager creates a new TLS manager.
func NewTLSManager(cfg *TLSConfig, audit *AuditLogger) (*TLSManager, error) {
	if cfg == nil {
		cfg = DefaultTLSConfig()
	}

	tm := &TLSManager{
		config: cfg,
		audit:  audit,
	}

	if cfg.Enabled {
		if err := tm.loadCertificates(); err != nil {
			return nil, err
		}
	}

	return tm, nil
}

// loadCertificates loads TLS certificates.
func (tm *TLSManager) loadCertificates() error {
	if tm.config.CertFile == "" || tm.config.KeyFile == "" {
		return fmt.Errorf("certificate and key files must be specified")
	}

	// Load certificate
	cert, err := tls.LoadX509KeyPair(tm.config.CertFile, tm.config.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load certificate: %w", err)
	}
	tm.cert = &cert

	// Load CA if specified
	if tm.config.CAFile != "" {
		caData, err := os.ReadFile(tm.config.CAFile)
		if err != nil {
			return fmt.Errorf("failed to read CA file: %w", err)
		}

		tm.caPool = x509.NewCertPool()
		if !tm.caPool.AppendCertsFromPEM(caData) {
			return fmt.Errorf("failed to parse CA certificate")
		}
	}

	// Build TLS config
	tm.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{*tm.cert},
		MinVersion:   tm.config.MinVersion,
		CipherSuites: tm.config.CipherSuites,
	}

	if tm.config.Mode == TLSModeVerifyCA && tm.caPool != nil {
		tm.tlsConfig.ClientCAs = tm.caPool
		tm.tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return nil
}

// ReloadCertificates reloads TLS certificates (for hot reload).
func (tm *TLSManager) ReloadCertificates() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.config.Enabled {
		return nil
	}

	return tm.loadCertificates()
}

// GetTLSConfig returns the TLS configuration.
func (tm *TLSManager) GetTLSConfig() *tls.Config {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.tlsConfig
}

// IsEnabled returns whether TLS is enabled.
func (tm *TLSManager) IsEnabled() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.config.Enabled
}

// GetMode returns the TLS mode.
func (tm *TLSManager) GetMode() TLSMode {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.config.Mode
}

// ShouldUpgrade checks if a connection should be upgraded to TLS.
func (tm *TLSManager) ShouldUpgrade() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	switch tm.config.Mode {
	case TLSModeDisabled:
		return false
	case TLSModeOptional, TLSModeRequired, TLSModeVerifyCA:
		return tm.config.Enabled
	default:
		return false
	}
}

// IsTLSRequired returns whether TLS is required.
func (tm *TLSManager) IsTLSRequired() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.config.Mode == TLSModeRequired || tm.config.Mode == TLSModeVerifyCA
}

// LogTLSHandshake logs a TLS handshake event.
func (tm *TLSManager) LogTLSHandshake(sourceIP string, success bool, err error) {
	if tm.audit == nil {
		return
	}

	eventType := EventTLSHandshake
	severity := SeverityInfo
	message := "TLS handshake successful"

	if !success {
		eventType = EventTLSHandshakeFailed
		severity = SeverityWarning
		if err != nil {
			message = fmt.Sprintf("TLS handshake failed: %v", err)
		} else {
			message = "TLS handshake failed"
		}
	}

	tm.audit.Log(&Event{
		EventType: eventType,
		Severity:  severity,
		SourceIP:  sourceIP,
		Message:   message,
	})
}

// SetConfig sets new TLS configuration.
func (tm *TLSManager) SetConfig(cfg *TLSConfig) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.config = cfg

	if cfg.Enabled {
		return tm.loadCertificates()
	}

	tm.cert = nil
	tm.caPool = nil
	tm.tlsConfig = nil
	return nil
}

// GetConfig returns current TLS configuration.
func (tm *TLSManager) GetConfig() *TLSConfig {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.config
}

// VerifyClient verifies a client certificate.
func (tm *TLSManager) VerifyClient(cert *x509.Certificate) error {
	if tm.config.Mode != TLSModeVerifyCA {
		return nil
	}

	if tm.caPool == nil {
		return fmt.Errorf("no CA configured for client verification")
	}

	// Create verification options
	opts := x509.VerifyOptions{
		Roots: tm.caPool,
	}

	_, err := cert.Verify(opts)
	return err
}

// GenerateSelfSignedCert generates a self-signed certificate for development.
// Note: For production, use proper certificates from a CA.
func GenerateSelfSignedCert(certFile, keyFile string, hosts []string) error {
	// This is a placeholder - in production, use proper certificate generation
	// via crypto/rsa and crypto/x509
	return fmt.Errorf("self-signed certificate generation not implemented - use proper certificates")
}
