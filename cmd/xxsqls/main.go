// Package main provides the XxSql server entry point.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/topxeq/xxsql/internal/config"
	"github.com/topxeq/xxsql/internal/log"
	"github.com/topxeq/xxsql/internal/server"
	"github.com/topxeq/xxsql/internal/storage"
)

// Build information (set via ldflags)
var (
	Version   = "0.0.3"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// Command-line flags
var (
	flagConfig      = flag.String("config", "", "Path to configuration file")
	flagVersion     = flag.Bool("version", false, "Print version information")
	flagInitConfig  = flag.Bool("init-config", false, "Print example configuration to stdout")
	flagLogLevel    = flag.String("log-level", "", "Override log level (DEBUG, INFO, WARN, ERROR)")
	flagDataDir     = flag.String("data-dir", "", "Override data directory")
	flagBind        = flag.String("bind", "", "Override bind address")
	flagPrivatePort = flag.Int("private-port", 0, "Override private protocol port")
	flagMySQLPort   = flag.Int("mysql-port", 0, "Override MySQL compatible port")
	flagHTTPPort    = flag.Int("http-port", 0, "Override HTTP API port")
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

	// Create PID file
	if err := server.CreatePIDFile(cfg.Server.PIDFile); err != nil {
		logger.Error("Failed to create PID file: %v", err)
		return 1
	}
	defer server.RemovePIDFile(cfg.Server.PIDFile)

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