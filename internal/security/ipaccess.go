package security

import (
	"net"
	"sync"
)

// AccessMode determines how IP access is controlled.
type AccessMode int

const (
	// AccessModeAllowAll allows all connections (default).
	AccessModeAllowAll AccessMode = iota
	// AccessModeWhitelist only allows IPs in the whitelist.
	AccessModeWhitelist
	// AccessModeBlacklist allows all IPs except those in the blacklist.
	AccessModeBlacklist
)

// IPAccess provides IP-based access control.
type IPAccess struct {
	mu        sync.RWMutex
	mode      AccessMode
	whitelist map[string]bool
	blacklist map[string]bool
	networks  []*net.IPNet // CIDR networks for whitelist/blacklist
	audit     *AuditLogger
}

// IPAccessConfig contains configuration for IP access control.
type IPAccessConfig struct {
	Mode      AccessMode
	Whitelist []string
	Blacklist []string
}

// DefaultIPAccessConfig returns default IP access configuration.
func DefaultIPAccessConfig() *IPAccessConfig {
	return &IPAccessConfig{
		Mode: AccessModeAllowAll,
	}
}

// NewIPAccess creates a new IP access controller.
func NewIPAccess(cfg *IPAccessConfig, audit *AuditLogger) (*IPAccess, error) {
	if cfg == nil {
		cfg = DefaultIPAccessConfig()
	}

	ipa := &IPAccess{
		mode:      cfg.Mode,
		whitelist: make(map[string]bool),
		blacklist: make(map[string]bool),
		networks:  make([]*net.IPNet, 0),
		audit:     audit,
	}

	// Add whitelist entries
	for _, ip := range cfg.Whitelist {
		if err := ipa.AddToWhitelist(ip); err != nil {
			return nil, err
		}
	}

	// Add blacklist entries
	for _, ip := range cfg.Blacklist {
		if err := ipa.AddToBlacklist(ip); err != nil {
			return nil, err
		}
	}

	return ipa, nil
}

// IsAllowed checks if an IP address is allowed to connect.
func (ipa *IPAccess) IsAllowed(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	ipa.mu.RLock()
	defer ipa.mu.RUnlock()

	switch ipa.mode {
	case AccessModeAllowAll:
		// Check blacklist
		if ipa.blacklist[ipStr] {
			return false
		}
		for _, network := range ipa.networks {
			if network.Contains(ip) && ipa.blacklist[network.String()] {
				return false
			}
		}
		return true

	case AccessModeWhitelist:
		// Only allow if in whitelist
		if ipa.whitelist[ipStr] {
			return true
		}
		for _, network := range ipa.networks {
			if network.Contains(ip) && ipa.whitelist[network.String()] {
				return true
			}
		}
		return false

	case AccessModeBlacklist:
		// Allow all except blacklist
		if ipa.blacklist[ipStr] {
			return false
		}
		for _, network := range ipa.networks {
			if network.Contains(ip) && ipa.blacklist[network.String()] {
				return false
			}
		}
		return true

	default:
		return true
	}
}

// CheckAndLog checks if IP is allowed and logs rejection.
func (ipa *IPAccess) CheckAndLog(ipStr string) bool {
	allowed := ipa.IsAllowed(ipStr)
	if !allowed && ipa.audit != nil {
		ipa.audit.Log(&Event{
			EventType: EventConnectionRejected,
			Severity:  SeverityWarning,
			SourceIP:  ipStr,
			Message:   "Connection rejected by IP access control",
		})
	}
	return allowed
}

// AddToWhitelist adds an IP or CIDR to the whitelist.
func (ipa *IPAccess) AddToWhitelist(ipOrCIDR string) error {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()

	// Try parsing as CIDR first
	_, network, err := net.ParseCIDR(ipOrCIDR)
	if err == nil {
		ipa.networks = append(ipa.networks, network)
		ipa.whitelist[network.String()] = true
		return nil
	}

	// Try parsing as IP
	ip := net.ParseIP(ipOrCIDR)
	if ip == nil {
		return &net.ParseError{Type: "IP address", Text: ipOrCIDR}
	}

	ipa.whitelist[ip.String()] = true
	return nil
}

// RemoveFromWhitelist removes an IP or CIDR from the whitelist.
func (ipa *IPAccess) RemoveFromWhitelist(ipStr string) {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()

	delete(ipa.whitelist, ipStr)
	// Remove from networks if present
	for i, network := range ipa.networks {
		if network.String() == ipStr {
			ipa.networks = append(ipa.networks[:i], ipa.networks[i+1:]...)
			break
		}
	}
}

// AddToBlacklist adds an IP or CIDR to the blacklist.
func (ipa *IPAccess) AddToBlacklist(ipOrCIDR string) error {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()

	// Try parsing as CIDR first
	_, network, err := net.ParseCIDR(ipOrCIDR)
	if err == nil {
		ipa.networks = append(ipa.networks, network)
		ipa.blacklist[network.String()] = true

		// Log the blocking
		if ipa.audit != nil {
			ipa.audit.Log(&Event{
				EventType: EventIPBlocked,
				Severity:  SeverityWarning,
				SourceIP:  ipOrCIDR,
				Message:   "IP added to blacklist",
			})
		}

		return nil
	}

	// Try parsing as IP
	ip := net.ParseIP(ipOrCIDR)
	if ip == nil {
		return &net.ParseError{Type: "IP address", Text: ipOrCIDR}
	}

	ipa.blacklist[ip.String()] = true

	// Log the blocking
	if ipa.audit != nil {
		ipa.audit.Log(&Event{
			EventType: EventIPBlocked,
			Severity:  SeverityWarning,
			SourceIP:  ip.String(),
			Message:   "IP added to blacklist",
		})
	}

	return nil
}

// RemoveFromBlacklist removes an IP or CIDR from the blacklist.
func (ipa *IPAccess) RemoveFromBlacklist(ipStr string) {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()

	delete(ipa.blacklist, ipStr)
	// Remove from networks if present
	for i, network := range ipa.networks {
		if network.String() == ipStr {
			ipa.networks = append(ipa.networks[:i], ipa.networks[i+1:]...)
			break
		}
	}
}

// SetMode sets the access mode.
func (ipa *IPAccess) SetMode(mode AccessMode) {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()
	ipa.mode = mode
}

// GetMode returns the current access mode.
func (ipa *IPAccess) GetMode() AccessMode {
	ipa.mu.RLock()
	defer ipa.mu.RUnlock()
	return ipa.mode
}

// GetWhitelist returns a copy of the whitelist.
func (ipa *IPAccess) GetWhitelist() []string {
	ipa.mu.RLock()
	defer ipa.mu.RUnlock()

	list := make([]string, 0, len(ipa.whitelist))
	for ip := range ipa.whitelist {
		list = append(list, ip)
	}
	return list
}

// GetBlacklist returns a copy of the blacklist.
func (ipa *IPAccess) GetBlacklist() []string {
	ipa.mu.RLock()
	defer ipa.mu.RUnlock()

	list := make([]string, 0, len(ipa.blacklist))
	for ip := range ipa.blacklist {
		list = append(list, ip)
	}
	return list
}

// ClearWhitelist clears all whitelist entries.
func (ipa *IPAccess) ClearWhitelist() {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()
	ipa.whitelist = make(map[string]bool)
}

// ClearBlacklist clears all blacklist entries.
func (ipa *IPAccess) ClearBlacklist() {
	ipa.mu.Lock()
	defer ipa.mu.Unlock()
	ipa.blacklist = make(map[string]bool)
}

// Stats returns IP access statistics.
func (ipa *IPAccess) Stats() IPAccessStats {
	ipa.mu.RLock()
	defer ipa.mu.RUnlock()

	return IPAccessStats{
		Mode:            ipa.mode,
		WhitelistCount:  len(ipa.whitelist),
		BlacklistCount:  len(ipa.blacklist),
		NetworkCount:    len(ipa.networks),
	}
}

// IPAccessStats contains IP access statistics.
type IPAccessStats struct {
	Mode           AccessMode
	WhitelistCount int
	BlacklistCount int
	NetworkCount   int
}
