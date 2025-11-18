package clickhouse

import (
	"context"
	"fmt"
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
	Hosts    []string
	Username string
	Password string
	Timeout  time.Duration
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
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &Client{conn: conn}, nil
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

// Query executes a query and returns rows
func (c *Client) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// QueryRow executes a query expected to return at most one row
func (c *Client) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}
