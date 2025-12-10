package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client wraps ClickHouse connection
type Client struct {
	conn driver.Conn
}

// Config holds ClickHouse connection configuration
//
//nolint:govet // fieldalignment: logical field grouping preferred over minor memory optimization
type Config struct {
	Hosts          []string
	Username       string
	Password       string
	Timeout        time.Duration // Used for DialTimeout and ReadTimeout
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Logger         *slog.Logger
}

// NewClient creates a new ClickHouse client
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("at least one host must be provided")
	}

	options := &clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: DatabaseName,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: cfg.Timeout,
		// ReadTimeout applies to reading from the connection (query execution)
		// This prevents queries from hanging indefinitely
		ReadTimeout: cfg.Timeout,
	}

	var conn driver.Conn
	var err error
	backoff := cfg.InitialBackoff

	// MaxRetries = number of retries after initial attempt
	// Total attempts = 1 initial + MaxRetries retries
	maxAttempts := cfg.MaxRetries + 1

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			cfg.Logger.Info("retrying ClickHouse connection after backoff",
				"attempt", attempt+1,
				"backoffSeconds", backoff.Seconds())

			select {
			case <-time.After(backoff):
				// Backoff completed
			case <-ctx.Done():
				return nil, fmt.Errorf("context canceled during retry backoff: %w", ctx.Err())
			}

			backoff *= 2
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}
		}

		conn, err = clickhouse.Open(options)
		if err != nil {
			if attempt < maxAttempts-1 {
				cfg.Logger.Warn("failed to connect to ClickHouse, will retry",
					"attempt", attempt+1,
					"error", err)
				continue
			}
			break
		}

		err = conn.Ping(ctx)
		if err == nil {
			return &Client{conn: conn}, nil
		}

		_ = conn.Close()

		if attempt < maxAttempts-1 {
			cfg.Logger.Warn("failed to ping ClickHouse, will retry",
				"attempt", attempt+1,
				"error", err)
		}
	}

	return nil, fmt.Errorf("failed to connect to ClickHouse after %d attempts: %w", maxAttempts, err)
}

// Close closes the ClickHouse connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Exec executes a query without returning results
func (c *Client) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

// ExecAsync executes a query with async_insert enabled
func (c *Client) ExecAsync(ctx context.Context, query string, args ...any) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":          1,
		"wait_for_async_insert": 0,
	}))
	return c.conn.Exec(ctx, query, args...)
}

// Query executes a query and returns rows
func (c *Client) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// QueryRow executes a query expected to return at most one row
func (c *Client) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}
