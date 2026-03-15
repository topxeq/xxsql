// Package main provides the XxSql server entry point.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.1"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// Command-line flags
var (
	flagConfig     = flag.String("config", "", "Path to configuration file")
	flagVersion    = flag.Bool("version", false, "Print version information")
	flagInitConfig = flag.Bool("init-config", false, "Print example configuration to stdout")
	flagLogLevel   = flag.String("log-level", "", "Override log level (DEBUG, INFO, WARN, ERROR)")
	flagDataDir    = flag.String("data-dir", "", "Override data directory")
	flagBind       = flag.String("bind", "", "Override bind address")
	flagPrivatePort = flag.Int("private-port", 0, "Override private protocol port")
	flagMySQLPort  = flag.Int("mysql-port", 0, "Override MySQL compatible port")
	flagHTTPPort   = flag.Int("http-port", 0, "Override HTTP API port")
)

func main() {
	flag.Parse()

	// Handle version flag
	if *flagVersion {
		fmt.Printf("XxSql Server v%s\n", Version)
		fmt.Printf("Git Commit: %s\n", GitCommit)
		fmt.Printf("Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	// Handle init-config flag
	if *flagInitConfig {
		cfg, err := config.GenerateExampleConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating config: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(cfg))
		os.Exit(0)
	}

	// Load configuration
	loader := config.NewLoader(*flagConfig)
	cfg, err := loader.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Apply command-line overrides
	applyConfigOverrides(cfg)

	// Initialize logger
	logger := initLogger(cfg)
	log.SetGlobal(logger)

	logger.Info("XxSql Server v%s starting...", Version)
	logger.Info("Configuration loaded from: %s", getConfigPath())

	// Create PID file
	if err := createPIDFile(cfg.Server.PIDFile); err != nil {
		logger.Error("Failed to create PID file: %v", err)
		os.Exit(1)
	}
	defer removePIDFile(cfg.Server.PIDFile)

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.Server.DataDir, 0755); err != nil {
		logger.Error("Failed to create data directory: %v", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Start server components (will be implemented in later phases)
	logger.Info("Server initialized successfully")
	logger.Info("Private port: %d", cfg.Network.PrivatePort)
	if cfg.Network.MySQLPort > 0 {
		logger.Info("MySQL port: %d", cfg.Network.MySQLPort)
	}
	if cfg.Network.HTTPPort > 0 {
		logger.Info("HTTP port: %d", cfg.Network.HTTPPort)
	}

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
			shutdown(logger)
			logger.Info("Server stopped")
			return
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

// createPIDFile creates a PID file.
func createPIDFile(path string) error {
	if path == "" {
		return nil
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Write PID
	pid := os.Getpid()
	return os.WriteFile(path, []byte(fmt.Sprintf("%d\n", pid)), 0644)
}

// removePIDFile removes the PID file.
func removePIDFile(path string) {
	if path != "" {
		os.Remove(path)
	}
}

// shutdown performs graceful shutdown.
func shutdown(logger *log.Logger) {
	logger.Info("Performing graceful shutdown...")

	// Close database connections (Phase 2+)
	// Flush buffers (Phase 4+)
	// Close listeners (Phase 2+)
	// Wait for active connections to finish (Phase 2+)

	logger.Info("Shutdown complete")
}