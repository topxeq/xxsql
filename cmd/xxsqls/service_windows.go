//go:build windows

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

// installService installs the service for the current platform
func installService(name, user, configPath string) error {
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

	// Open service manager
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	// Check if service already exists
	s, err := m.OpenService(name)
	if err == nil {
		s.Close()
		return fmt.Errorf("service '%s' already exists", name)
	}

	// Create service
	s, err = m.CreateService(name, execPath, mgr.Config{
		DisplayName:  fmt.Sprintf("XxSql Database Server (%s)", name),
		Description:  "Lightweight SQL database with microservices support",
		StartType:    mgr.StartAutomatic,
		ErrorControl: mgr.ErrorNormal,
	}, "-config", configPath)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer s.Close()

	fmt.Printf("Service '%s' installed successfully.\n", name)
	fmt.Printf("\nTo start the service:\n")
	fmt.Printf("  sc start %s\n", name)
	fmt.Printf("\nOr use Windows Services management console (services.msc)\n")

	return nil
}

// uninstallService removes the service for the current platform
func uninstallService(name string) error {
	// Open service manager
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	// Open service
	s, err := m.OpenService(name)
	if err != nil {
		return fmt.Errorf("service '%s' is not installed", name)
	}
	defer s.Close()

	// Stop service if running
	_, err = s.Control(svc.Stop)
	if err != nil {
		// Service might not be running, ignore error
	}

	// Delete service
	err = s.Delete()
	if err != nil {
		return fmt.Errorf("failed to delete service: %w", err)
	}

	fmt.Printf("Service '%s' uninstalled successfully.\n", name)

	return nil
}

// checkServiceInstalled returns whether the service is installed
func checkServiceInstalled(name string) bool {
	m, err := mgr.Connect()
	if err != nil {
		return false
	}
	defer m.Disconnect()

	s, err := m.OpenService(name)
	if err != nil {
		return false
	}
	s.Close()
	return true
}

// getServiceStatus returns the current status of the service
func getServiceStatus(name string) string {
	m, err := mgr.Connect()
	if err != nil {
		return "unknown"
	}
	defer m.Disconnect()

	s, err := m.OpenService(name)
	if err != nil {
		return "not installed"
	}
	defer s.Close()

	status, err := s.Query()
	if err != nil {
		return "unknown"
	}

	switch status.State {
	case svc.Stopped:
		return "stopped"
	case svc.StartPending:
		return "starting"
	case svc.StopPending:
		return "stopping"
	case svc.Running:
		return "running"
	case svc.ContinuePending:
		return "continuing"
	case svc.PausePending:
		return "pausing"
	case svc.Paused:
		return "paused"
	default:
		return "unknown"
	}
}
