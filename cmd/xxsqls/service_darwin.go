//go:build darwin

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const launchdPlistTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.xxsql.%s</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>-config</string>
        <string>%s</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/xxsql/%s.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/xxsql/%s.error.log</string>
    <key>WorkingDirectory</key>
    <string>%s</string>
</dict>
</plist>
`

// installService installs the service for the current platform
func installService(name, user, configPath string) error {
	// Check if running as root
	if os.Getuid() != 0 {
		return fmt.Errorf("service installation requires root privileges (use sudo)")
	}

	// Get executable path
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Resolve symlinks
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return fmt.Errorf("failed to resolve executable path: %w", err)
	}

	// Make config path absolute if not already
	if !filepath.IsAbs(configPath) {
		absPath, err := filepath.Abs(configPath)
		if err != nil {
			return fmt.Errorf("failed to get absolute config path: %w", err)
		}
		configPath = absPath
	}

	// Default user if not specified
	if user == "" {
		user = "xxsql"
	}

	// Determine working directory
	workDir := fmt.Sprintf("/var/lib/%s", name)

	// Create directories
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}
	if err := os.MkdirAll(fmt.Sprintf("/var/log/xxsql"), 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Create plist content
	plistContent := fmt.Sprintf(launchdPlistTemplate, name, execPath, configPath, name, name, workDir)

	// Write plist file
	plistPath := fmt.Sprintf("/Library/LaunchDaemons/com.xxsql.%s.plist", name)
	if err := os.WriteFile(plistPath, []byte(plistContent), 0644); err != nil {
		return fmt.Errorf("failed to write plist file: %w", err)
	}

	// Load the service
	cmd := exec.Command("launchctl", "load", plistPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to load service: %w\n%s", err, string(output))
	}

	fmt.Printf("Service '%s' installed successfully.\n", name)
	fmt.Printf("Plist file: %s\n", plistPath)
	fmt.Printf("\nThe service will start automatically on boot.\n")
	fmt.Printf("\nTo start manually:\n")
	fmt.Printf("  sudo launchctl load %s\n", plistPath)
	fmt.Printf("\nTo stop:\n")
	fmt.Printf("  sudo launchctl unload %s\n", plistPath)
	fmt.Printf("\nTo check status:\n")
	fmt.Printf("  sudo launchctl list | grep xxsql\n")

	return nil
}

// uninstallService removes the service for the current platform
func uninstallService(name string) error {
	// Check if running as root
	if os.Getuid() != 0 {
		return fmt.Errorf("service uninstallation requires root privileges (use sudo)")
	}

	plistPath := fmt.Sprintf("/Library/LaunchDaemons/com.xxsql.%s.plist", name)

	// Check if service exists
	if _, err := os.Stat(plistPath); os.IsNotExist(err) {
		return fmt.Errorf("service '%s' is not installed", name)
	}

	// Unload the service
	cmd := exec.Command("launchctl", "unload", plistPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("Warning: failed to unload service: %s\n", string(output))
	}

	// Remove plist file
	if err := os.Remove(plistPath); err != nil {
		return fmt.Errorf("failed to remove plist file: %w", err)
	}

	fmt.Printf("Service '%s' uninstalled successfully.\n", name)

	return nil
}

// checkServiceInstalled returns whether the service is installed
func checkServiceInstalled(name string) bool {
	plistPath := fmt.Sprintf("/Library/LaunchDaemons/com.xxsql.%s.plist", name)
	_, err := os.Stat(plistPath)
	return err == nil
}

// getServiceStatus returns the current status of the service
func getServiceStatus(name string) string {
	// Check if service is loaded
	cmd := exec.Command("launchctl", "list", fmt.Sprintf("com.xxsql.%s", name))
	if err := cmd.Run(); err != nil {
		return "stopped"
	}
	return "running"
}
