//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const systemdServiceTemplate = `[Unit]
Description=XxSql Database Server (%s)
After=network.target

[Service]
Type=simple
User=%s
Group=%s
WorkingDirectory=%s
ExecStart=%s -config %s
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
`

// installService installs the service for the current platform
func installService(name, user, configPath string) error {
	// Check if systemd is available
	if _, err := os.Stat("/run/systemd/system"); os.IsNotExist(err) {
		return fmt.Errorf("systemd is not available on this system")
	}

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

	// Create working directory if it doesn't exist
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}

	// Create service file content
	serviceContent := fmt.Sprintf(systemdServiceTemplate, name, user, user, workDir, execPath, configPath)

	// Write service file
	servicePath := fmt.Sprintf("/etc/systemd/system/%s.service", name)
	if err := os.WriteFile(servicePath, []byte(serviceContent), 0644); err != nil {
		return fmt.Errorf("failed to write service file: %w", err)
	}

	// Reload systemd
	cmd := exec.Command("systemctl", "daemon-reload")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to reload systemd: %w\n%s", err, string(output))
	}

	// Enable service
	cmd = exec.Command("systemctl", "enable", name)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to enable service: %w\n%s", err, string(output))
	}

	fmt.Printf("Service '%s' installed successfully.\n", name)
	fmt.Printf("Service file: %s\n", servicePath)
	fmt.Printf("\nTo start the service:\n")
	fmt.Printf("  sudo systemctl start %s\n", name)
	fmt.Printf("\nTo check status:\n")
	fmt.Printf("  sudo systemctl status %s\n", name)

	return nil
}

// uninstallService removes the service for the current platform
func uninstallService(name string) error {
	// Check if systemd is available
	if _, err := os.Stat("/run/systemd/system"); os.IsNotExist(err) {
		return fmt.Errorf("systemd is not available on this system")
	}

	// Check if running as root
	if os.Getuid() != 0 {
		return fmt.Errorf("service uninstallation requires root privileges (use sudo)")
	}

	servicePath := fmt.Sprintf("/etc/systemd/system/%s.service", name)

	// Check if service exists
	if _, err := os.Stat(servicePath); os.IsNotExist(err) {
		return fmt.Errorf("service '%s' is not installed", name)
	}

	// Stop service if running
	cmd := exec.Command("systemctl", "stop", name)
	cmd.Run() // Ignore error if not running

	// Disable service
	cmd = exec.Command("systemctl", "disable", name)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("Warning: failed to disable service: %s\n", string(output))
	}

	// Remove service file
	if err := os.Remove(servicePath); err != nil {
		return fmt.Errorf("failed to remove service file: %w", err)
	}

	// Reload systemd
	cmd = exec.Command("systemctl", "daemon-reload")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to reload systemd: %w\n%s", err, string(output))
	}

	// Reset failed state if any
	cmd = exec.Command("systemctl", "reset-failed", name)
	cmd.Run() // Ignore error

	fmt.Printf("Service '%s' uninstalled successfully.\n", name)

	return nil
}

// checkServiceInstalled returns whether the service is installed
func checkServiceInstalled(name string) bool {
	servicePath := fmt.Sprintf("/etc/systemd/system/%s.service", name)
	_, err := os.Stat(servicePath)
	return err == nil
}

// getServiceStatus returns the current status of the service
func getServiceStatus(name string) string {
	cmd := exec.Command("systemctl", "is-active", name)
	output, _ := cmd.Output()
	status := strings.TrimSpace(string(output))
	if status == "active" {
		return "running"
	} else if status == "inactive" {
		return "stopped"
	} else if status == "failed" {
		return "failed"
	}
	return "unknown"
}
