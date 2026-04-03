//go:build !linux && !windows && !darwin

package main

import (
	"fmt"
	"runtime"
)

// installService installs the service for the current platform
func installService(name, user, configPath string) error {
	return fmt.Errorf("service installation is not supported on %s", runtime.GOOS)
}

// uninstallService removes the service for the current platform
func uninstallService(name string) error {
	return fmt.Errorf("service uninstallation is not supported on %s", runtime.GOOS)
}

// checkServiceInstalled returns whether the service is installed
func checkServiceInstalled(name string) bool {
	return false
}

// getServiceStatus returns the current status of the service
func getServiceStatus(name string) string {
	return "unsupported"
}
