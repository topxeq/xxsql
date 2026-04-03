// Package main provides the XxSql server entry point.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/server"
	"github.com/topxeq/xxsql/internal/storage"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.4"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// Command-line flags
var (
	flagConfig        = flag.String("config", "", "Path to configuration file")
	flagVersion       = flag.Bool("version", false, "Print version information")
	flagInitConfig    = flag.Bool("init-config", false, "Print example configuration to stdout")
	flagLogLevel      = flag.String("log-level", "", "Override log level (DEBUG, INFO, WARN, ERROR)")
	flagDataDir       = flag.String("data-dir", "", "Override data directory")
	flagBind          = flag.String("bind", "", "Override bind address")
	flagPrivatePort   = flag.Int("private-port", 0, "Override private protocol port")
	flagMySQLPort     = flag.Int("mysql-port", 0, "Override MySQL compatible port")
	flagHTTPPort      = flag.Int("http-port", 0, "Override HTTP API port")
	flagInstallService   = flag.Bool("install-service", false, "Install as system service")
	flagUninstallService = flag.Bool("uninstall-service", false, "Uninstall system service")
	flagServiceName   = flag.String("service-name", "xxsql", "Service name (for install/uninstall)")
	flagServiceUser   = flag.String("service-user", "", "Service user (for install, default: xxsql)")
	flagServiceStatus = flag.Bool("service-status", false, "Check service status")
)

// Global server instance
var srv *server.Server

func main() {
	os.Exit(run())
}

// run contains the main server logic. It returns an exit code.
func run() int {
	flag.Parse()

	// Handle version flag
	if *flagVersion {
		fmt.Printf("XxSql Server v%s\n", Version)
		fmt.Printf("Git Commit: %s\n", GitCommit)
		fmt.Printf("Build Time: %s\n", BuildTime)
		return 0
	}

	// Handle init-config flag
	if *flagInitConfig {
		cfg, err := config.GenerateExampleConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating config: %v\n", err)
			return 1
		}
		fmt.Println(string(cfg))
		return 0
	}

	// Handle service management flags
	if *flagInstallService {
		if err := handleServiceInstall(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		return 0
	}

	if *flagUninstallService {
		if err := handleServiceUninstall(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		return 0
	}

	if *flagServiceStatus {
		handleServiceStatus()
		return 0
	}

	// Load configuration
	loader := config.NewLoader(*flagConfig)
	cfg, err := loader.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		return 1
	}

	// Apply command-line overrides
	applyConfigOverrides(cfg)

	// Initialize logger
	logger := initLogger(cfg)
	log.SetGlobal(logger)

	logger.Info("XxSql Server v%s starting...", Version)
	logger.Info("Configuration loaded from: %s", getConfigPath())

	// Check ports availability before any write operations
	if err := checkPortsAvailable(cfg); err != nil {
		logger.Error("Port check failed: %v", err)
		return 1
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.Server.DataDir, 0755); err != nil {
		logger.Error("Failed to create data directory: %v", err)
		return 1
	}

	// Initialize storage engine
	engine := storage.NewEngine(cfg.Server.DataDir)
	if err := engine.Open(); err != nil {
		logger.Error("Failed to open storage engine: %v", err)
		return 1
	}
	defer engine.Close()

	// Create and start server
	srv = server.New(cfg, logger, engine)
	srv.SetConfigPath(getConfigPath())
	if err := srv.Start(); err != nil {
		logger.Error("Failed to start server: %v", err)
		return 1
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for shutdown signal
	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP:
			// Handle config reload (hot reload support)
			logger.Info("Received SIGHUP, reloading configuration...")
			newCfg, err := loader.Load()
			if err != nil {
				logger.Error("Failed to reload configuration: %v", err)
				continue
			}
			applyConfigOverrides(newCfg)
			cfg = newCfg
			// Update log level if changed
			logger.SetLevelString(cfg.Log.Level)
			logger.Info("Configuration reloaded successfully")

		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("Received %v, shutting down...", sig)
			srv.Stop()
			logger.Info("Server stopped")
			return 0
		}
	}
}

// applyConfigOverrides applies command-line flags to the configuration.
func applyConfigOverrides(cfg *config.Config) {
	if *flagLogLevel != "" {
		cfg.Log.Level = *flagLogLevel
	}
	if *flagDataDir != "" {
		cfg.Server.DataDir = *flagDataDir
	}
	if *flagBind != "" {
		cfg.Network.Bind = *flagBind
	}
	if *flagPrivatePort > 0 {
		cfg.Network.PrivatePort = *flagPrivatePort
	}
	if *flagMySQLPort > 0 {
		cfg.Network.MySQLPort = *flagMySQLPort
	}
	if *flagHTTPPort > 0 {
		cfg.Network.HTTPPort = *flagHTTPPort
	}
}

// initLogger initializes the logger based on configuration.
func initLogger(cfg *config.Config) *log.Logger {
	opts := []log.Option{
		log.WithLevel(log.ParseLevel(cfg.Log.Level)),
		log.WithCaller(true),
	}

	if cfg.Log.File != "" {
		opts = append(opts,
			log.WithFile(cfg.Log.File),
			log.WithRotation(cfg.Log.MaxSizeMB, cfg.Log.MaxBackups, cfg.Log.MaxAgeDays, cfg.Log.Compress),
		)
	}

	return log.NewLogger(opts...)
}

// getConfigPath returns the configuration file path.
func getConfigPath() string {
	if *flagConfig != "" {
		return *flagConfig
	}
	return "defaults"
}

// checkPortsAvailable checks if all enabled ports are available before starting.
func checkPortsAvailable(cfg *config.Config) error {
	ports := []struct {
		name    string
		port    int
		enabled bool
		bind    string
	}{
		{"private", cfg.Network.PrivatePort, cfg.Network.IsPrivateEnabled(), cfg.Network.GetPrivateBind()},
		{"mysql", cfg.Network.MySQLPort, cfg.Network.IsMySQLEnabled(), cfg.Network.GetMySQLBind()},
		{"http", cfg.Network.HTTPPort, cfg.Network.IsHTTPEnabled(), cfg.Network.GetHTTPBind()},
	}

	for _, p := range ports {
		if p.enabled && p.port > 0 {
			addr := fmt.Sprintf("%s:%d", p.bind, p.port)
			listener, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("port %d (%s) is already in use: %w", p.port, p.name, err)
			}
			listener.Close()
		}
	}
	return nil
}

// handleServiceInstall handles the -install-service flag
func handleServiceInstall() error {
	// Determine config path
	configPath := *flagConfig
	if configPath == "" {
		// Use default config path
		configPath = "/etc/xxsql/xxsql.json"
	}

	// Check if config file exists, if not generate a default one
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Config file not found at %s, generating default config...\n", configPath)

		// Create directory if needed
		configDir := filepath.Dir(configPath)
		if err := os.MkdirAll(configDir, 0755); err != nil {
			return fmt.Errorf("failed to create config directory: %w", err)
		}

		// Generate default config
		cfg, err := config.GenerateExampleConfig()
		if err != nil {
			return fmt.Errorf("failed to generate config: %w", err)
		}

		if err := os.WriteFile(configPath, cfg, 0644); err != nil {
			return fmt.Errorf("failed to write config: %w", err)
		}

		fmt.Printf("Generated default config at %s\n", configPath)
	}

	return installService(*flagServiceName, *flagServiceUser, configPath)
}

// handleServiceUninstall handles the -uninstall-service flag
func handleServiceUninstall() error {
	return uninstallService(*flagServiceName)
}

// handleServiceStatus handles the -service-status flag
func handleServiceStatus() {
	name := *flagServiceName
	installed := checkServiceInstalled(name)
	status := getServiceStatus(name)

	fmt.Printf("Service: %s\n", name)
	fmt.Printf("Installed: %v\n", installed)
	fmt.Printf("Status: %s\n", status)
}