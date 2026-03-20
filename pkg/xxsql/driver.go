package xxsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
)

// Driver name for registration.
const DriverName = "xxsql"

var (
	// driverInstance is the singleton driver instance.
	driverInstance = &xxsqlDriver{}

	// registerOnce ensures the driver is only registered once.
	registerOnce sync.Once
)

func init() {
	// Register driver on import
	RegisterDriver()
}

// RegisterDriver registers the xxsql driver with database/sql.
func RegisterDriver() {
	registerOnce.Do(func() {
		sql.Register(DriverName, driverInstance)
	})
}

// xxsqlDriver implements driver.Driver and driver.DriverContext.
type xxsqlDriver struct{}

// Open opens a new connection.
func (d *xxsqlDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return newConnection(cfg)
}

// OpenConnector returns a connector for the given DSN.
func (d *xxsqlDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &connector{
		dsn:  dsn,
		cfg:  cfg,
		drv:  d,
	}, nil
}

// connector implements driver.Connector.
type connector struct {
	dsn string
	cfg *Config
	drv *xxsqlDriver
}

// Connect creates a new connection.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return newConnection(c.cfg)
}

// Driver returns the underlying driver.
func (c *connector) Driver() driver.Driver {
	return c.drv
}

// newConnection creates a new connection based on the protocol in config.
func newConnection(cfg *Config) (driver.Conn, error) {
	switch cfg.Protocol {
	case ProtocolMySQL:
		// Use MySQL wire protocol
		return newConn(cfg)
	case ProtocolPrivate:
		// Use XxSql private protocol
		return newPrivateConn(cfg)
	default:
		// Default to private protocol
		return newPrivateConn(cfg)
	}
}

// Open is a convenience function that opens a database connection.
func Open(dsn string) (*sql.DB, error) {
	return sql.Open(DriverName, dsn)
}

// OpenDB opens a database using a connector.
func OpenDB(dsn string) (*sql.DB, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	connector := &connector{
		dsn: dsn,
		cfg: cfg,
		drv: driverInstance,
	}

	return sql.OpenDB(connector), nil
}
