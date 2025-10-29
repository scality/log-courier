# Log-Courier Implementation Plan (Phase 1)

**Last Updated:** 2025-10-27

## Overview

This document contains the complete implementation plan for log-courier Phase 1. For additional context about project decisions and architecture, see `PROJECT_UNDERSTANDING.md`.

## Project Context

**What is log-courier?**
Log-courier is the "Bucket Log Processor" (BLP) consumer service that reads S3 access log records from ClickHouse, builds log objects, and writes them to destination S3 buckets.

**Architecture Position:**
- **Upstream:** ClickHouse (contains log records inserted by Fluent Bit from CloudServer)
- **Downstream:** S3 destination buckets (via external S3 API)
- **Out of scope:** CloudServer (producer), Fluent Bit (shipper)

**Phase 1 Scope:**
- Single instance consumer (no work partitioning)
- Processes ALL buckets (no raft session filtering)
- Full end-to-end functionality: ClickHouse → log processing → S3 upload → offset commit
- Simpler deployment (no coordination mechanism needed)

**Phase 2 (Future):**
- Multiple instances with work partitioning
- Raft session filtering
- Deployment mechanism (Ballot or alternative)

## Key Principles

### Testing Philosophy
- Tests built incrementally with each task
- Every PR includes code + tests with good coverage
- Can defer some testing when dependencies are blocking
- No separate testing phase at the end

### PR Organization
- General pattern: 1 PR per task
- Each PR focuses on a single aspect
- Balance: not trivially small, not too large to review
- Exception: combine tasks if changes are very small
- Task 1 (Infrastructure) will be discussed and split into PRs after implementation

### Team Conventions
Before starting each task, consult `/home/dimitrios/scality/metadata-migration` for team conventions:
- Code structure and organization
- Error handling patterns
- Logging patterns (slog)
- Testing patterns (Ginkgo + Gomega)
- Naming conventions
- Configuration management (Viper + pflag)
- CLI structure (Cobra)
- Build tooling (Makefile)

## Configuration Management

Following metadata-migration patterns:

**Framework:** Viper + spf13/pflag

**Configuration Sources** (precedence order):
1. Command-line flags (highest)
2. Environment variables
3. YAML configuration file
4. Default values (lowest)

**Key Patterns:**
- Centralized `ConfigSpec` map defining all configuration items
- Dotted naming: `"component.subsystem.setting"`
- Environment variables: `LOG_COURIER_<UPPERCASE_PATH>`
- Optional YAML file via `--config-file` flag or `LOG_COURIER_CONFIG_FILE` env var
- Type-safe access: `GetString()`, `GetInt()`, `GetBool()`, `GetStringSlice()`

**Reusable Code:**
- Copy `pkg/util/config.go` from metadata-migration as-is
- Create log-courier-specific `configspec.go`

---

## Task 1: Project Infrastructure & CI/CD Setup

### Objective
Set up complete project infrastructure, build tooling, CI/CD, and documentation following team conventions.

### Study Phase

Examine metadata-migration infrastructure in detail:
1. **Makefile** (`/home/dimitrios/scality/metadata-migration/Makefile`)
   - Build targets: all, clean, test, test-coverage, lint, fmt, coverage-report
   - Coverage handling with covdata
   - CGO_ENABLED=0 for static builds

2. **Linting** (`.golangci.yml`)
   - Linter configuration
   - Exclusions for generated code
   - Presets used

3. **GitHub Actions** (`.github/workflows/`)
   - `tests.yaml` - CI testing workflow
   - `build-image.yaml` - Docker image build
   - `release.yaml` - Release automation
   - Study workflow structure and job definitions

4. **Dependabot** (`.github/dependabot.yaml`)
   - Configuration for Go module updates

5. **Codecov** (`codecov.yml`)
   - Coverage reporting configuration

6. **Docker** (`images/svc-base/Dockerfile`, `.dockerignore`)
   - Dockerfile structure
   - Multi-stage builds if used
   - Ignore patterns

7. **Git Configuration** (`.gitignore`, `.gitattributes`)
   - Ignore patterns for build artifacts, coverage, etc.

8. **Documentation** (`README.md`, `CLAUDE.md`)
   - Structure and content
   - Section organization

### Implementation Steps

#### 1. Initialize Go Module
```bash
cd /home/dimitrios/scality/log-courier
go mod init github.com/scality/log-courier
```
- Use Go 1.24.2 (matching metadata-migration)

#### 2. Project Structure Reference

The following structure will be created incrementally as files are added (directories created only when first file is placed inside):

```
log-courier/
├── cmd/
│   └── log-courier/
│       └── main.go          # Main entry point
├── pkg/
│   ├── logcourier/          # Core logic
│   ├── clickhouse/          # ClickHouse client
│   ├── s3/                  # S3 client
│   ├── util/                # Utilities
│   └── testutil/            # Test utilities
├── scripts/                 # Utility scripts (as needed)
├── images/
│   └── log-courier/
│       └── Dockerfile
├── bin/                     # Build outputs (gitignored)
└── ...
```

**Note:** Create directories as needed when adding files, not upfront.

#### 3. Build Tooling (Makefile)

Create `Makefile` with targets:
- `all` - Build binary to bin/log-courier (CGO_ENABLED=0)
- `clean` - Remove bin/ and coverage files
- `test` - Run all tests (Go tests)
- `test-coverage` - Run tests with coverage (unit + integration coverage)
- `lint` - Run golangci-lint
- `fmt` - Run go fmt
- `coverage-report` - Generate HTML coverage reports

Follow metadata-migration patterns for coverage handling.

#### 4. Code Quality Configuration

**`.golangci.yml`:**
- Copy from metadata-migration
- Adapt for log-courier (no Python scripts initially)
- Keep timeout: 5m
- Keep exclusion patterns

**Go Formatting:**
- Use standard `go fmt`
- Integrate into Makefile

#### 5. Git Configuration

**`.gitignore`:**
```
bin/
__pycache__/
*~
cover.out
covdata.out
covdatafiles/
*.pem
.DS_Store
```

**`.gitattributes`:**
Copy from metadata-migration if present.

#### 6. Testing Framework Setup

Add dependencies to go.mod:
```bash
go get github.com/onsi/ginkgo/v2
go get github.com/onsi/gomega
```

Create initial test structure:
- `pkg/logcourier/suite_test.go` - Ginkgo test suite
- Follow metadata-migration test patterns

#### 7. GitHub Actions Workflows

**`.github/workflows/tests.yaml`:**
- Trigger on push/pull_request
- Set up Go 1.24.2
- Run `make test-coverage`
- Upload coverage to codecov
- Run `make lint`

Study metadata-migration workflows for exact structure.

**Note:** `build-image.yaml` and `release.yaml` will be created together with the Dockerfile in step 10 below, since they depend on it.

#### 8. Dependabot Configuration

**`.github/dependabot.yaml`:**
```yaml
version: 2
updates:
  - package-ecosystem: gomod
    directory: /
    schedule:
      interval: weekly
```

#### 9. Codecov Configuration

**`codecov.yml`:**
```yaml
version: 2
coverage:
  status:
    project:
      default:
        target: auto
        threshold: 1%
```

Adapt from metadata-migration if they have specific settings.

#### 10. Docker Support and Related Workflows

**`images/log-courier/Dockerfile`:**
- Multi-stage build
- Build stage: compile Go binary
- Runtime stage: minimal image (alpine or scratch)
- Copy binary from build stage
- Set entrypoint

**`.dockerignore`:**
```
.git
.github
bin/
covdatafiles/
*.md
```

**`.github/workflows/build-image.yaml`:**
- Trigger on push to main, tags
- Build Docker image
- Push to registry (configure as needed)

**`.github/workflows/release.yaml`:**
- Trigger on tag push (v*)
- Build release artifacts
- Create GitHub release

Study metadata-migration workflows for exact structure.

#### 11. Documentation

**Update `README.md`:**
```markdown
# Log-Courier

Bucket Log Processor (BLP) for AWS S3 Server Access Logging.

## Overview

Log-courier reads S3 access log records from ClickHouse, builds log objects,
and writes them to destination S3 buckets.

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture.

## Components

- `log-courier`: Consumer service daemon

## Prerequisites

- Go 1.24.2
- ClickHouse instance
- S3-compatible object storage

## Building

```bash
make
```

## Testing

```bash
make test
```

For coverage:
```bash
make test-coverage
make coverage-report
```

## Running

```bash
./bin/log-courier --config-file config.yaml
```

## Configuration

Configuration can be provided via:
- Command-line flags
- YAML config file (`--config-file` or `LOG_COURIER_CONFIG_FILE` env var)
- Environment variables (prefixed with `LOG_COURIER_`)

See configuration section in ARCHITECTURE.md for details.

## Development

### Code Quality

```bash
make lint    # Run linter
make fmt     # Format code
```

## License

See [LICENSE](LICENSE)
```

**Create `CLAUDE.md`:**
```markdown
# CLAUDE.md

This file provides guidance to Claude Code when working with log-courier.

## Project Overview

Log-courier is the Bucket Log Processor (BLP) for AWS S3 Server Access Logging.
It reads log records from ClickHouse, builds log objects, and writes them to S3.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture.
See [PROJECT_UNDERSTANDING.md](PROJECT_UNDERSTANDING.md) for implementation context.

## Build and Development Commands

### Building
```bash
make        # Build all binaries
make clean  # Clean build artifacts
```

### Testing
```bash
make test              # Run all tests
make test-coverage     # Run tests with coverage
make coverage-report   # Generate coverage reports
```

### Code Quality
```bash
make lint   # Run linter
make fmt    # Format code
```

## Architecture

### Core Components
- Consumer loop: batch finder + log processing phases
- ClickHouse client: read logs, commit offsets
- S3 uploader: write log objects to destination buckets

### Package Structure
- `pkg/logcourier/`: Core consumer logic
- `pkg/clickhouse/`: ClickHouse client and schema
- `pkg/s3/`: S3 client and uploader
- `pkg/util/`: Shared utilities
- `pkg/testutil/`: Testing utilities

### Configuration
- Uses **Viper** for configuration management
- Config via: command-line flags, YAML file, environment variables
- See `pkg/logcourier/configspec.go` for all config options

### Testing
- Uses **Ginkgo** and **Gomega** for Go tests
- Integration tests require ClickHouse and S3-compatible storage

## Development Notes

### Running Locally
- Requires ClickHouse instance (local Docker or remote)
- Requires S3-compatible storage (MinIO, localstack, or real S3)

### Code Style
- Follow existing Go conventions
- Run `make fmt` before committing
- Ensure `make lint` passes
```

### Testing

**Verification checklist:**
- [x] `make all` builds successfully
- [x] `make lint` runs without errors (requires golangci-lint installation)
- [x] `make test` runs successfully
- [x] GitHub Actions workflows are valid YAML
- [ ] Docker image builds successfully (deferred to step 10)
- [x] README.md is comprehensive

### Deliverables

- Complete project infrastructure
- Working build system
- CI/CD pipelines configured
- Documentation in place

### Post-Implementation Decision

**After completing Task 1, discuss with Dimitrios:**
How to split this work into reviewable PRs. Possible split:
1. Basic project structure + Makefile
2. GitHub Actions + Dependabot + Codecov
3. Docker support
4. Documentation updates

### Status: ✅ COMPLETED (2025-10-23)

**Commits created:**
1. `LOGC-1: Initialize Go module with testing dependencies`
2. `LOGC-1: Add Git configuration`
3. `LOGC-1: Add Makefile`
4. `LOGC-1: Add stub main`
5. `LOGC-1: Add test suite`
6. `LOGC-1: Add golangci-lint configuration`
7. `LOGC-1: Add test workflow`
8. `LOGC-1: Add Go module dependency management to Dependabot`
9. `LOGC-1: Add Codecov configuration`
10. `LOGC-1: Update README`

**What was completed:**
- ✅ Go module initialization with Go 1.24.2
- ✅ Git configuration (.gitignore, .gitattributes)
- ✅ Build system (Makefile with all targets)
- ✅ Stub main.go for functional builds
- ✅ Ginkgo/Gomega test framework setup
- ✅ golangci-lint configuration
- ✅ GitHub Actions CI/CD pipeline (tests.yaml)
- ✅ Dependabot configuration (gomod + github-actions)
- ✅ Codecov configuration
- ✅ README.md documentation
- ✅ All build targets verified (make all, test, fmt, clean)

**What was deferred:**
- ⏸️ Docker support (Dockerfile, .dockerignore) - will be completed in step 10 with actual code
- ⏸️ build-image.yaml and release.yaml workflows - depend on Dockerfile
- ⏸️ CLAUDE.md - created but not committed (untracked file)

**Notes:**
- Project structure created incrementally (directories created as files are added)
- golangci-lint requires separate installation (not in go.mod)
- Branch: `improvement/LOGC-1`

---

## Task 2: Configuration Management

### Objective
Implement configuration management following team conventions (Viper + pflag pattern).

### Status: ✅ COMPLETED (2025-10-27)

### Study Phase

1. Review `pkg/util/config.go` from metadata-migration
   - Understand ConfigSpec structure
   - Understand LoadConfiguration flow
   - Understand flag binding with Viper

2. Review `pkg/migration/configspec.go`
   - See examples of config item definitions
   - Understand required vs optional fields
   - See environment variable naming

### Implementation Steps

#### 1. Copy Configuration Utilities

Copy `pkg/util/config.go` from metadata-migration to `pkg/util/config.go`:
```bash
cp /home/dimitrios/scality/metadata-migration/pkg/util/config.go \
   /home/dimitrios/scality/log-courier/pkg/util/config.go
```

No modifications needed - use as-is.

#### 2. Define Configuration Specification

Create `pkg/logcourier/configspec.go`:

```go
package logcourier

import "github.com/scality/log-courier/pkg/util"

var ConfigSpec = util.ConfigSpec{
    // ClickHouse connection
    "clickhouse.url": util.ConfigVarSpec{
        Help:         "ClickHouse connection URL, e.g. 'clickhouse://localhost:9000'",
        Required:     true,
        DefaultValue: "",
        EnvVar:       "LOG_COURIER_CLICKHOUSE_URL",
    },
    "clickhouse.database": util.ConfigVarSpec{
        Help:         "ClickHouse database name",
        DefaultValue: "logs",
        EnvVar:       "LOG_COURIER_CLICKHOUSE_DATABASE",
    },
    "clickhouse.username": util.ConfigVarSpec{
        Help:         "ClickHouse username",
        DefaultValue: "default",
        EnvVar:       "LOG_COURIER_CLICKHOUSE_USERNAME",
    },
    "clickhouse.password": util.ConfigVarSpec{
        Help:         "ClickHouse password",
        DefaultValue: "",
        EnvVar:       "LOG_COURIER_CLICKHOUSE_PASSWORD",
        ExposeFunc: func(rawValue any) any {
            if rawValue.(string) == "" {
                return ""
            }
            return "***"
        },
    },
    "clickhouse.timeout-seconds": util.ConfigVarSpec{
        Help:         "ClickHouse query timeout in seconds",
        DefaultValue: 30,
        EnvVar:       "LOG_COURIER_CLICKHOUSE_TIMEOUT_SECONDS",
    },

    // Work discovery thresholds
    "consumer.count-threshold": util.ConfigVarSpec{
        Help:         "Minimum number of unprocessed logs to trigger batch processing",
        DefaultValue: 1000,
        EnvVar:       "LOG_COURIER_CONSUMER_COUNT_THRESHOLD",
    },
    "consumer.time-threshold-seconds": util.ConfigVarSpec{
        Help:         "Age in seconds after which logs should be processed regardless of count",
        DefaultValue: 3600, // 1 hour
        EnvVar:       "LOG_COURIER_CONSUMER_TIME_THRESHOLD_SECONDS",
    },
    "consumer.discovery-interval-seconds": util.ConfigVarSpec{
        Help:         "Interval in seconds between batch finder queries",
        DefaultValue: 60,
        EnvVar:       "LOG_COURIER_CONSUMER_DISCOVERY_INTERVAL_SECONDS",
    },

    // S3 configuration
    "s3.endpoint": util.ConfigVarSpec{
        Help:         "S3 endpoint URL (leave empty for AWS S3)",
        DefaultValue: "",
        EnvVar:       "LOG_COURIER_S3_ENDPOINT",
    },
    "s3.region": util.ConfigVarSpec{
        Help:         "S3 region",
        DefaultValue: "us-east-1",
        EnvVar:       "LOG_COURIER_S3_REGION",
    },
    "s3.access-key-id": util.ConfigVarSpec{
        Help:         "S3 access key ID (leave empty to use IAM role)",
        DefaultValue: "",
        EnvVar:       "LOG_COURIER_S3_ACCESS_KEY_ID",
    },
    "s3.secret-access-key": util.ConfigVarSpec{
        Help:         "S3 secret access key",
        DefaultValue: "",
        EnvVar:       "LOG_COURIER_S3_SECRET_ACCESS_KEY",
        ExposeFunc: func(rawValue any) any {
            if rawValue.(string) == "" {
                return ""
            }
            return "***"
        },
    },
    "s3.timeout-seconds": util.ConfigVarSpec{
        Help:         "S3 request timeout in seconds",
        DefaultValue: 60,
        EnvVar:       "LOG_COURIER_S3_TIMEOUT_SECONDS",
    },

    // General
    "log-level": util.ConfigVarSpec{
        Help:         "Log level (error|warn|info|debug)",
        DefaultValue: "info",
        EnvVar:       "LOG_COURIER_LOG_LEVEL",
    },
    "shutdown-timeout-seconds": util.ConfigVarSpec{
        Help:         "Graceful shutdown timeout in seconds",
        DefaultValue: 30,
        EnvVar:       "LOG_COURIER_SHUTDOWN_TIMEOUT_SECONDS",
    },
}
```

#### 3. Implement Configuration Loading in Main

Create initial `cmd/log-courier/main.go`:

```go
package main

import (
    "fmt"
    "os"

    "github.com/spf13/pflag"
    "github.com/scality/log-courier/pkg/logcourier"
)

func main() {
    // Add command-line flags
    logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "clickhouse-url", "clickhouse.url")
    logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "log-level", "log-level")
    // Add more flags as needed

    configFileFlag := pflag.String("config-file", "", "Path to YAML configuration file")
    pflag.Parse()

    // Load configuration
    configFile := *configFileFlag
    if configFile == "" {
        configFile = os.Getenv("LOG_COURIER_CONFIG_FILE")
    }

    err := logcourier.ConfigSpec.LoadConfiguration(configFile, "", nil)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
        pflag.Usage()
        os.Exit(2)
    }

    // TODO: Initialize and run consumer
    fmt.Println("log-courier started (stub)")
}
```

#### 4. Configuration Validation

Add validation helper in `pkg/logcourier/config.go`:

```go
package logcourier

import "fmt"

// ValidateConfig performs additional validation beyond required field checks
func ValidateConfig() error {
    // Validate count threshold is positive
    countThreshold := ConfigSpec.GetInt("consumer.count-threshold")
    if countThreshold <= 0 {
        return fmt.Errorf("consumer.count-threshold must be positive, got %d", countThreshold)
    }

    // Validate time threshold is positive
    timeThreshold := ConfigSpec.GetInt("consumer.time-threshold-seconds")
    if timeThreshold <= 0 {
        return fmt.Errorf("consumer.time-threshold-seconds must be positive, got %d", timeThreshold)
    }

    // Validate discovery interval is positive
    interval := ConfigSpec.GetInt("consumer.discovery-interval-seconds")
    if interval <= 0 {
        return fmt.Errorf("consumer.discovery-interval-seconds must be positive, got %d", interval)
    }

    // Validate log level
    logLevel := ConfigSpec.GetString("log-level")
    validLevels := map[string]bool{"error": true, "warn": true, "info": true, "debug": true}
    if !validLevels[logLevel] {
        return fmt.Errorf("invalid log-level: %s (must be error|warn|info|debug)", logLevel)
    }

    return nil
}
```

Call `ValidateConfig()` after `LoadConfiguration()` in main.

### Testing

Create `pkg/logcourier/config_test.go`:

```go
package logcourier_test

import (
    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"
    "github.com/scality/log-courier/pkg/logcourier"
)

var _ = Describe("Configuration", func() {
    BeforeEach(func() {
        logcourier.ConfigSpec.Reset()
    })

    Describe("ConfigSpec", func() {
        It("should have required clickhouse.url", func() {
            err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
            Expect(err).To(HaveOccurred())
            Expect(err.Error()).To(ContainSubstring("clickhouse.url"))
        })

        It("should load configuration from environment variables", func() {
            // Set environment variable
            // Load config
            // Verify value
        })

        // Add more tests
    })

    Describe("ValidateConfig", func() {
        BeforeEach(func() {
            // Set valid defaults
        })

        It("should reject negative count threshold", func() {
            // Set negative value
            // Expect validation error
        })

        It("should reject invalid log level", func() {
            // Set invalid log level
            // Expect validation error
        })

        // Add more validation tests
    })
})
```

Create test suite: `pkg/logcourier/suite_test.go`:

```go
package logcourier_test

import (
    "testing"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"
)

func TestLogCourier(t *testing.T) {
    RegisterFailHandler(Fail)
    RunSpecs(t, "LogCourier Suite")
}
```

### Deliverables

- Working configuration management system
- All configuration items defined in ConfigSpec
- Configuration loading from flags/env/file
- Configuration validation
- Comprehensive unit tests

---

## Task 3: ClickHouse Integration

### Objective
Implement ClickHouse client, schema management, and testing utilities.

### Status: ✅ COMPLETED (2025-10-27)

### Study Phase

1. **Research ClickHouse Go client:**
   - clickhouse-go (https://github.com/ClickHouse/clickhouse-go) - official client
   - Review documentation and examples
   - Check version compatibility

2. **Review schema from ARCHITECTURE.md:**
   - Understand table structures
   - Understand materialized views
   - Understand distributed tables
   - Note: For Phase 1 (single instance testing), we may simplify to local tables only

### Implementation Steps

#### 1. Add ClickHouse Dependency

```bash
go get github.com/ClickHouse/clickhouse-go/v2
```

Review version in metadata-migration if they use ClickHouse.

#### 2. Implement ClickHouse Client

Create `pkg/clickhouse/client.go`:

```go
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
type Config struct {
    URL      string
    Database string
    Username string
    Password string
    Timeout  time.Duration
}

// NewClient creates a new ClickHouse client
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
    // Parse connection options
    options := &clickhouse.Options{
        Addr: []string{cfg.URL},
        Auth: clickhouse.Auth{
            Database: cfg.Database,
            Username: cfg.Username,
            Password: cfg.Password,
        },
        DialTimeout: cfg.Timeout,
        // Add more options as needed
    }

    conn, err := clickhouse.Open(options)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
    }

    // Verify connection
    if err := conn.Ping(ctx); err != nil {
        conn.Close()
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
```

#### 3. Implement Schema Management

Create `pkg/clickhouse/schema.go`:

```go
package clickhouse

import (
    "context"
    "fmt"
)

// CreateSchema creates all necessary tables and views
// For Phase 1, we create local tables only (no cluster, no distributed tables)
func CreateSchema(ctx context.Context, client *Client) error {
    // Create ingest table (Null engine)
    if err := createIngestTable(ctx, client); err != nil {
        return fmt.Errorf("failed to create ingest table: %w", err)
    }

    // Create logs table (MergeTree)
    if err := createLogsTable(ctx, client); err != nil {
        return fmt.Errorf("failed to create logs table: %w", err)
    }

    // Create materialized view
    if err := createLogsFilterMV(ctx, client); err != nil {
        return fmt.Errorf("failed to create logs filter materialized view: %w", err)
    }

    // Create offsets table (ReplacingMergeTree)
    if err := createOffsetsTable(ctx, client); err != nil {
        return fmt.Errorf("failed to create offsets table: %w", err)
    }

    return nil
}

func createIngestTable(ctx context.Context, client *Client) error {
    query := `
        CREATE TABLE IF NOT EXISTS logs.access_logs_ingest
        (
            loggingEnabled      Bool        DEFAULT false,
            insertedAt          DateTime    DEFAULT now(),
            bucketName          String,
            timestamp           DateTime,
            req_id              String,
            action              LowCardinality(String),
            httpURL             String
            -- Add other columns from ARCHITECTURE.md
        )
        ENGINE = Null()
    `
    return client.Exec(ctx, query)
}

func createLogsTable(ctx context.Context, client *Client) error {
    query := `
        CREATE TABLE IF NOT EXISTS logs.access_logs
        (
            insertedAt      DateTime,
            bucketName      String,
            req_id          String,
            timestamp       DateTime,
            action          LowCardinality(String),
            httpURL         String
            -- Add other columns from ARCHITECTURE.md
        )
        ENGINE = MergeTree()
        PARTITION BY toStartOfDay(insertedAt)
        ORDER BY (raftSessionId, bucketName, insertedAt, req_id)
    `
    return client.Exec(ctx, query)
}

func createLogsFilterMV(ctx context.Context, client *Client) error {
    query := `
        CREATE MATERIALIZED VIEW IF NOT EXISTS logs.access_logs_ingest_mv
        TO logs.access_logs
        AS SELECT *
        FROM logs.access_logs_ingest
        WHERE loggingEnabled = true
    `
    return client.Exec(ctx, query)
}

func createOffsetsTable(ctx context.Context, client *Client) error {
    query := `
        CREATE TABLE IF NOT EXISTS logs.offsets
        (
            bucketName          String,
            raftSessionId       UInt16,
            last_processed_ts   DateTime
        )
        ENGINE = MergeTree()
        ORDER BY (bucketName, raftSessionId)
    `
    return client.Exec(ctx, query)
}

// DropSchema drops all tables (for testing cleanup)
func DropSchema(ctx context.Context, client *Client) error {
    queries := []string{
        "DROP VIEW IF EXISTS logs.access_logs_ingest_mv",
        "DROP TABLE IF EXISTS logs.access_logs",
        "DROP TABLE IF EXISTS logs.access_logs_ingest",
        "DROP TABLE IF EXISTS logs.offsets",
    }

    for _, query := range queries {
        if err := client.Exec(ctx, query); err != nil {
            return err
        }
    }

    return nil
}
```

**Note:** Complete column definitions need to be extracted from ARCHITECTURE.md.

#### 4. Implement Test Utilities

Create `pkg/testutil/clickhouse.go`:

```go
package testutil

import (
    "context"
    "fmt"
    "time"

    "github.com/scality/log-courier/pkg/clickhouse"
)

// ClickHouseTestHelper provides utilities for testing with ClickHouse
type ClickHouseTestHelper struct {
    Client *clickhouse.Client
}

// NewClickHouseTestHelper creates a new test helper
// Connects to ClickHouse specified by LOG_COURIER_CLICKHOUSE_URL env var
// Defaults to localhost:9000
func NewClickHouseTestHelper(ctx context.Context) (*ClickHouseTestHelper, error) {
    url := os.Getenv("LOG_COURIER_CLICKHOUSE_URL")
    if url == "" {
        url = "localhost:9000"
    }

    cfg := clickhouse.Config{
        URL:      url,
        Database: "logs",
        Username: "default",
        Password: "",
        Timeout:  10 * time.Second,
    }

    client, err := clickhouse.NewClient(ctx, cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to test ClickHouse: %w", err)
    }

    return &ClickHouseTestHelper{Client: client}, nil
}

// Setup creates schema for testing
func (h *ClickHouseTestHelper) Setup(ctx context.Context) error {
    // Create database if not exists
    if err := h.Client.Exec(ctx, "CREATE DATABASE IF NOT EXISTS logs"); err != nil {
        return err
    }

    // Create schema
    return clickhouse.CreateSchema(ctx, h.Client)
}

// Teardown drops all test tables
func (h *ClickHouseTestHelper) Teardown(ctx context.Context) error {
    return clickhouse.DropSchema(ctx, h.Client)
}

// InsertTestLog inserts a test log record
func (h *ClickHouseTestHelper) InsertTestLog(ctx context.Context, log TestLogRecord) error {
    query := `
        INSERT INTO logs.access_logs_ingest
        (loggingEnabled, bucketName, timestamp, req_id, action, httpURL)
        VALUES (?, ?, ?, ?, ?, ?)
    `
    return h.Client.Exec(ctx, query,
        log.LoggingEnabled,
        log.BucketName,
        log.Timestamp,
        log.ReqID,
        log.Action,
        log.HttpURL,
    )
}

// TestLogRecord represents a test log record
type TestLogRecord struct {
    LoggingEnabled bool
    BucketName     string
    Timestamp      time.Time
    ReqID          string
    Action         string
    HttpURL        string
    // Add other fields as needed
}

// Close closes the helper
func (h *ClickHouseTestHelper) Close() error {
    if h.Client != nil {
        return h.Client.Close()
    }
    return nil
}
```

#### 5. Create Schema Script (Optional)

Create `scripts/clickhouse/create-schema.sql`:
- Full DDL statements for all tables
- Can be used for manual setup or reference

### Testing

Create `pkg/clickhouse/client_test.go`:

```go
package clickhouse_test

import (
    "context"
    "os"
    "testing"
    "time"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/clickhouse"
)

func TestClickHouse(t *testing.T) {
    RegisterFailHandler(Fail)
    RunSpecs(t, "ClickHouse Suite")
}

var _ = Describe("ClickHouse Client", func() {
    var (
        ctx    context.Context
        client *clickhouse.Client
    )

    BeforeEach(func() {
        ctx = context.Background()

        // Skip if no ClickHouse available
        url := os.Getenv("LOG_COURIER_CLICKHOUSE_URL")
        if url == "" {
            Skip("LOG_COURIER_CLICKHOUSE_URL not set, skipping ClickHouse integration tests")
        }

        cfg := clickhouse.Config{
            URL:      url,
            Database: "logs",
            Username: "default",
            Password: "",
            Timeout:  10 * time.Second,
        }

        var err error
        client, err = clickhouse.NewClient(ctx, cfg)
        Expect(err).NotTo(HaveOccurred())
    })

    AfterEach(func() {
        if client != nil {
            client.Close()
        }
    })

    Describe("Connection", func() {
        It("should connect successfully", func() {
            Expect(client).NotTo(BeNil())
        })

        It("should execute queries", func() {
            err := client.Exec(ctx, "SELECT 1")
            Expect(err).NotTo(HaveOccurred())
        })
    })

    Describe("Schema", func() {
        AfterEach(func() {
            // Clean up schema
            clickhouse.DropSchema(ctx, client)
        })

        It("should create schema", func() {
            err := clickhouse.CreateSchema(ctx, client)
            Expect(err).NotTo(HaveOccurred())
        })

        It("should allow inserting into ingest table", func() {
            err := clickhouse.CreateSchema(ctx, client)
            Expect(err).NotTo(HaveOccurred())

            query := `INSERT INTO logs.logs_ingest
                      (logging_enabled, bucket, time, requestID, operation, key)
                      VALUES (1, 'test-bucket', now(), 'req-123', 'GetObject', 'test-key')`
            err = client.Exec(ctx, query)
            Expect(err).NotTo(HaveOccurred())
        })

        It("should filter logs through materialized view", func() {
            err := clickhouse.CreateSchema(ctx, client)
            Expect(err).NotTo(HaveOccurred())

            // Insert with loggingEnabled = true
            query1 := `INSERT INTO logs.access_logs_ingest
                       (loggingEnabled, bucketName, timestamp, req_id, action, httpURL)
                       VALUES (true, 'test-bucket', now(), 'req-1', 'GetObject', '/test-bucket/key1')`
            err = client.Exec(ctx, query1)
            Expect(err).NotTo(HaveOccurred())

            // Insert with loggingEnabled = false
            query2 := `INSERT INTO logs.access_logs_ingest
                       (loggingEnabled, bucketName, timestamp, req_id, action, httpURL)
                       VALUES (false, 'test-bucket', now(), 'req-2', 'GetObject', '/test-bucket/key2')`
            err = client.Exec(ctx, query2)
            Expect(err).NotTo(HaveOccurred())

            // Wait for materialized view to process
            time.Sleep(100 * time.Millisecond)

            // Verify only loggingEnabled=true record in logs table
            rows, err := client.Query(ctx, "SELECT req_id FROM logs.access_logs WHERE bucketName = 'test-bucket'")
            Expect(err).NotTo(HaveOccurred())
            defer rows.Close()

            var requestIDs []string
            for rows.Next() {
                var requestID string
                err := rows.Scan(&requestID)
                Expect(err).NotTo(HaveOccurred())
                requestIDs = append(requestIDs, requestID)
            }

            Expect(requestIDs).To(ConsistOf("req-1"))
        })
    })
})
```

### Environment Setup

**For local testing:**
```bash
docker run -d --name clickhouse-test \
    -p 9000:9000 \
    clickhouse/clickhouse-server:latest

export LOG_COURIER_CLICKHOUSE_URL=localhost:9000
```

**For CI (add to .github/workflows/tests.yaml):**
```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - 9000:9000
env:
  LOG_COURIER_CLICKHOUSE_URL: localhost:9000
```

### Deliverables

- Working ClickHouse client
- Schema creation and management
- Test utilities for integration testing
- Comprehensive tests (unit + integration)

---

## Task 4: Work Discovery

### Objective
Implement batch finder query to identify batches of logs ready for processing.

### Status: ✅ COMPLETED (2025-10-28)

**What was completed:**
- ✅ Consumer threshold configuration (count: 1000, time: 900s)
- ✅ LogBatch type with optimal field alignment
- ✅ BatchFinder implementation with configurable database name
- ✅ Comprehensive tests with condition-based waiting
- ✅ Fixed test interference with unique database per test suite
- ✅ All 33 tests passing (8 ClickHouse + 25 LogCourier specs)

**Key improvements:**
- Tests use `WaitForMaterializedView` instead of arbitrary sleeps
- Test isolation prevents race conditions in parallel execution
- Configuration tests read defaults from ConfigSpec (no hardcoded values)

### Study Phase

1. **Review batch finder query from ARCHITECTURE.md (lines 218-276)**
   - Understand CTEs (bucket_offsets, instance_logs, new_logs_by_bucket)
   - Understand thresholds (count and time)
   - Understand offset tracking

2. **Simplification for Phase 1:**
   - Remove `instance_logs` CTE (no raft session filtering)
   - Remove `raft_session_id % ${totalInstances} = ${instanceID}` clause

### Implementation Steps

#### 1. Define Work Order Model

Create `pkg/logcourier/workorder.go`:

```go
package logcourier

import (
    "fmt"
    "time"
)

// LogBatch represents a batch of logs ready for processing
type LogBatch struct {
    Bucket       string
    MinTimestamp time.Time
    MaxTimestamp time.Time
    LogCount     int64
}

// String returns a string representation for logging
func (w LogBatch) String() string {
    return fmt.Sprintf("LogBatch{Bucket: %s, Logs: %d, TimeRange: [%s, %s]}",
        w.Bucket, w.LogCount, w.MinTimestamp, w.MaxTimestamp)
}
```

#### 2. Implement Work Discovery

Create `pkg/logcourier/batchfinder.go`:

```go
package logcourier

import (
    "context"
    "fmt"
    "time"

    "github.com/scality/log-courier/pkg/clickhouse"
)

// BatchFinder discovers log batchs from ClickHouse
type BatchFinder struct {
    client           *clickhouse.Client
    countThreshold   int
    timeThresholdSec int
}

// NewBatchFinder creates a new batch finder instance
func NewBatchFinder(client *clickhouse.Client, countThreshold, timeThresholdSec int) *BatchFinder {
    return &BatchFinder{
        client:           client,
        countThreshold:   countThreshold,
        timeThresholdSec: timeThresholdSec,
    }
}

// FindBatches executes batch finder query and returns log batchs
func (wd *BatchFinder) FindBatches(ctx context.Context) ([]LogBatch, error) {
    query := `
        WITH
            -- Find the most recent timestamp processed for each bucket
            bucket_offsets AS (
                SELECT
                    bucketName,
                    max(last_processed_ts) as last_processed_ts
                FROM logs.offsets
                GROUP BY bucketName
            ),

            -- Find unprocessed logs for each bucket (Phase 1: no raft session filtering)
            new_logs_by_bucket AS (
                SELECT
                    l.bucketName,
                    count() AS new_log_count,
                    min(l.insertedAt) as min_ts,
                    max(l.insertedAt) as max_ts
                FROM logs.access_logs AS l
                LEFT JOIN bucket_offsets AS o ON l.bucketName = o.bucketName
                WHERE l.insertedAt > COALESCE(o.last_processed_ts, toDateTime64('1970-01-01 00:00:00', 3))
                GROUP BY l.bucketName
            )
        -- Select buckets ready for batch processing
        SELECT
            bucketName,
            new_log_count,
            min_ts,
            max_ts
        FROM new_logs_by_bucket
        WHERE new_log_count >= ?
            OR min_ts <= now() - INTERVAL ? SECOND
    `

    rows, err := wd.client.Query(ctx, query, wd.countThreshold, wd.timeThresholdSec)
    if err != nil {
        return nil, fmt.Errorf("batch finder query failed: %w", err)
    }
    defer rows.Close()

    var batchs []LogBatch
    for rows.Next() {
        var wo LogBatch
        var minTs, maxTs time.Time

        err := rows.Scan(&wo.Bucket, &wo.LogCount, &minTs, &maxTs)
        if err != nil {
            return nil, fmt.Errorf("failed to scan log batch: %w", err)
        }

        wo.MinTimestamp = minTs
        wo.MaxTimestamp = maxTs
        batchs = append(batchs, wo)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating log batchs: %w", err)
    }

    return batchs, nil
}
```

### Testing

Create `pkg/logcourier/batchfinder_test.go`:

```go
package logcourier_test

import (
    "context"
    "os"
    "time"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/logcourier"
    "github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("BatchFinder", func() {
    var (
        ctx    context.Context
        helper *testutil.ClickHouseTestHelper
        wd     *logcourier.BatchFinder
    )

    BeforeEach(func() {
        ctx = context.Background()

        if os.Getenv("LOG_COURIER_CLICKHOUSE_URL") == "" {
            Skip("LOG_COURIER_CLICKHOUSE_URL not set")
        }

        var err error
        helper, err = testutil.NewClickHouseTestHelper(ctx)
        Expect(err).NotTo(HaveOccurred())

        err = helper.SetupSchema(ctx)
        Expect(err).NotTo(HaveOccurred())

        // Create batch finder with test thresholds
        wd = logcourier.NewBatchFinder(helper.Client, 10, 60)
    })

    AfterEach(func() {
        if helper != nil {
            helper.TeardownSchema(ctx)
            helper.Close()
        }
    })

    Describe("FindBatches", func() {
        It("should return empty list when no logs", func() {
            batchs, err := wd.FindBatches(ctx)
            Expect(err).NotTo(HaveOccurred())
            Expect(batchs).To(BeEmpty())
        })

        It("should return log batch when count threshold met", func() {
            // Insert 15 logs (threshold is 10)
            for i := 0; i < 15; i++ {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: true,
                    BucketName:     "test-bucket",
                    Timestamp:      time.Now(),
                    ReqID:          fmt.Sprintf("req-%d", i),
                    Action:         "GetObject",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            // Wait for materialized view
            time.Sleep(100 * time.Millisecond)

            batchs, err := wd.FindBatches(ctx)
            Expect(err).NotTo(HaveOccurred())
            Expect(batchs).To(HaveLen(1))
            Expect(batchs[0].Bucket).To(Equal("test-bucket"))
            Expect(batchs[0].LogCount).To(BeNumerically(">=", 15))
        })

        It("should return log batch when time threshold met", func() {
            // Insert 1 log with old timestamp
            oldTime := time.Now().Add(-2 * time.Hour)
            err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                LoggingEnabled: true,
                BucketName:     "test-bucket",
                Timestamp:      oldTime,
                ReqID:          "req-old",
                Action:         "GetObject",
            })
            Expect(err).NotTo(HaveOccurred())

            time.Sleep(100 * time.Millisecond)

            batchs, err := wd.FindBatches(ctx)
            Expect(err).NotTo(HaveOccurred())
            Expect(batchs).To(HaveLen(1))
            Expect(batchs[0].Bucket).To(Equal("test-bucket"))
        })

        It("should not return work when neither threshold met", func() {
            // Insert 5 logs (below count threshold of 10)
            // With recent timestamp (within 60 seconds)
            for i := 0; i < 5; i++ {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: true,
                    BucketName:     "test-bucket",
                    Timestamp:      time.Now(),
                    ReqID:          fmt.Sprintf("req-%d", i),
                    Action:         "GetObject",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            time.Sleep(100 * time.Millisecond)

            batchs, err := wd.FindBatches(ctx)
            Expect(err).NotTo(HaveOccurred())
            Expect(batchs).To(BeEmpty())
        })

        It("should respect bucket offsets", func() {
            // Insert logs
            for i := 0; i < 15; i++ {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: true,
                    BucketName:     "test-bucket",
                    Timestamp:      time.Now(),
                    ReqID:          fmt.Sprintf("req-%d", i),
                    Action:         "GetObject",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            time.Sleep(100 * time.Millisecond)

            // Commit offset at current time
            offsetTime := time.Now()
            err := helper.Client.Exec(ctx,
                "INSERT INTO logs.offsets (bucketName, raftSessionId, last_processed_ts) VALUES (?, ?, ?)",
                "test-bucket", uint16(0), offsetTime)
            Expect(err).NotTo(HaveOccurred())

            // Should not return work (all logs processed)
            batchs, err := wd.FindBatches(ctx)
            Expect(err).NotTo(HaveOccurred())
            Expect(batchs).To(BeEmpty())
        })

        It("should handle multiple buckets", func() {
            // Insert logs for bucket-1 (above threshold)
            for i := 0; i < 15; i++ {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: true,
                    BucketName:     "bucket-1",
                    Timestamp:      time.Now(),
                    ReqID:          fmt.Sprintf("req-1-%d", i),
                    Action:         "GetObject",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            // Insert logs for bucket-2 (above threshold)
            for i := 0; i < 20; i++ {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: true,
                    BucketName:     "bucket-2",
                    Timestamp:      time.Now(),
                    ReqID:          fmt.Sprintf("req-2-%d", i),
                    Action:         "PutObject",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            time.Sleep(100 * time.Millisecond)

            batchs, err := wd.FindBatches(ctx)
            Expect(err).NotTo(HaveOccurred())
            Expect(batchs).To(HaveLen(2))

            buckets := []string{batchs[0].Bucket, batchs[1].Bucket}
            Expect(buckets).To(ConsistOf("bucket-1", "bucket-2"))
        })
    })
})
```

### Deliverables

- Working batch finder implementation
- LogBatch model
- Comprehensive integration tests with ClickHouse
- Threshold-based selection working correctly

---

## Task 5: Log Processing

### Status: ✅ COMPLETED (2025-10-29)

### Objective
Implement log fetching and log object building.

### DECISIONS

Clarifications made with Dimitrios on 2025-10-28:

1. **Log object format:** ✅ RESOLVED
   - **MUST implement AWS S3 Server Access Log format exactly as documented**
   - Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
   - Follow AWS field ordering, delimiters, and escaping rules
   - **Omit these fields** (not applicable to Scality S3):
     - Host Id
     - Access Point ARN
   - All other AWS format fields should be included

2. **Log object key naming:** ✅ RESOLVED
   - **Format**: `<LoggingTargetPrefix>YYYY-mm-DD-HH-MM-SS-UniqueString`
   - **Prefix**: Use `loggingTargetPrefix` field from ClickHouse log record
   - **Timestamp**: Use batch's `MinTimestamp` (for idempotence - same batch produces same key)
   - **UniqueString**: Generate random suffix using AWS approach (e.g., 16-character hex string)
   - **Example**: `logs/2025-10-22-12-30-45-A1B2C3D4E5F6G7H8`

3. **Custom fields:** ✅ RESOLVED
   - No custom fields beyond standard AWS format
   - x-* query parameters are already captured in `httpURL` field and will appear in Request-URI output
   - Skip testing for x-* parameters initially (defer to later)

4. **Complete column list:** ✅ RESOLVED

   **Field Mapping (ClickHouse → AWS S3 Server Access Log Format):**

   | # | AWS Log Field | ClickHouse Column | Notes |
   |---|---------------|-------------------|-------|
   | 1 | Bucket Owner | `bucketOwner` | |
   | 2 | Bucket | `bucketName` | |
   | 3 | Time | `startTime` | ⚠️ Verify later: should this be `timestamp` instead? |
   | 4 | Remote IP | `clientIP` | |
   | 5 | Requester | `requester` | |
   | 6 | Request ID | `req_id` | |
   | 7 | Operation | `action` | |
   | 8 | Key | `objectKey` | ✅ Added to schema |
   | 9 | Request-URI | `httpURL` | Includes x-* query parameters |
   | 10 | HTTP Status | `httpCode` | |
   | 11 | Error Code | `errorCode` | |
   | 12 | Bytes Sent | `bytesSent` | |
   | 13 | Object Size | `contentLength` | ⚠️ Verify later: should this be `bodyLength` instead? |
   | 14 | Total Time | `elapsed_ms` | |
   | 15 | Turn-Around Time | `turnAroundTime` | |
   | 16 | Referer | `referer` | |
   | 17 | User-Agent | `userAgent` | |
   | 18 | Version Id | `versionId` | |
   | 19 | Host Id | **OMIT** | Not applicable to Scality S3 |
   | 20 | Signature Version | `signatureVersion` | |
   | 21 | Cipher Suite | `cipherSuite` | |
   | 22 | Authentication Type | `authenticationType` | |
   | 23 | Host Header | `hostHeader` | |
   | 24 | TLS Version | `tlsVersion` | |
   | 25 | Access Point ARN | **OMIT** | Not applicable to Scality S3 |
   | 26 | ACL Required | `aclRequired` | |

   **Columns NOT included in log output:**
   - `timestamp` - internal tracking
   - `hostname` - internal metadata
   - `accountName` - internal metadata
   - `accountDisplayName` - internal metadata
   - `userName` - internal metadata
   - `httpMethod` - internal metadata
   - `bytesDeleted` - internal metadata
   - `bytesReceived` - internal metadata
   - `logFormatVersion` - internal version tracking
   - `loggingEnabled` - configuration flag
   - `loggingTargetBucket` - configuration metadata
   - `loggingTargetPrefix` - configuration metadata (used for key generation only)
   - `insertedAt` - internal tracking
   - `raftSessionId` - internal sharding

### Prerequisites

**✅ ClickHouse Schema Update - COMPLETED (2025-10-28):**
- Added `objectKey String` column to ClickHouse schema
- Updated in: ARCHITECTURE.md (line 82), pkg/testutil/clickhouse.go
- This column is needed to populate the "Key" field in AWS S3 Server Access Log format
- **Note**: This is an upstream dependency - CloudServer must populate this field and Fluent Bit must forward it

### Implementation Steps

#### 1. Define Log Record Model

Create `pkg/logcourier/logrecord.go`:

```go
package logcourier

import "time"

// LogRecord represents a single log record from ClickHouse
type LogRecord struct {
    InsertedAt time.Time
    Bucket     string
    RequestID  string
    Timestamp  time.Time
    Action     string
    HttpURL    string
    // TODO: Add complete field list based on ARCHITECTURE.md schema
    // Required fields from schema:
    // StartTime         time.Time
    // Hostname          string
    // AccountName       string
    // AccountDisplayName string
    // BucketOwner       string
    // UserName          string
    // Requester         string
    // HttpMethod        string
    // HttpCode          uint16
    // ErrorCode         string
    // VersionId         string
    // BytesDeleted      uint64
    // BytesReceived     uint64
    // BytesSent         uint64
    // BodyLength        uint64
    // ContentLength     uint64
    // ClientIP          string
    // Referer           string
    // UserAgent         string
    // HostHeader        string
    // ElapsedMs         float32
    // TurnAroundTime    float32
    // RaftSessionId     uint16
    // SignatureVersion  string
    // CipherSuite       string
    // AuthenticationType string
    // TlsVersion        string
    // AclRequired       string
    // LogFormatVersion  string
    // LoggingEnabled    bool
    // LoggingTargetBucket string
    // LoggingTargetPrefix string
}
```

#### 2. Implement Log Fetching

Create `pkg/logcourier/logfetch.go`:

```go
package logcourier

import (
    "context"
    "fmt"
    "time"

    "github.com/scality/log-courier/pkg/clickhouse"
)

// LogFetcher fetches logs from ClickHouse
type LogFetcher struct {
    client *clickhouse.Client
}

// NewLogFetcher creates a new log fetcher
func NewLogFetcher(client *clickhouse.Client) *LogFetcher {
    return &LogFetcher{client: client}
}

// FetchLogs fetches logs for a log batch
// Returns logs sorted by time (ascending)
func (lf *LogFetcher) FetchLogs(ctx context.Context, wo LogBatch) ([]LogRecord, error) {
    query := `
        SELECT
            insertedAt,
            bucketName,
            req_id,
            timestamp,
            action,
            httpURL
            -- TODO: Add all columns from schema
        FROM logs.access_logs
        WHERE bucketName = ?
          AND insertedAt >= ?
          AND insertedAt <= ?
        ORDER BY timestamp ASC
    `

    rows, err := lf.client.Query(ctx, query, wo.Bucket, wo.MinTimestamp, wo.MaxTimestamp)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch logs: %w", err)
    }
    defer rows.Close()

    var records []LogRecord
    for rows.Next() {
        var rec LogRecord
        err := rows.Scan(
            &rec.InsertedAt,
            &rec.Bucket,
            &rec.RequestID,
            &rec.Timestamp,
            &rec.Action,
            &rec.HttpURL,
            // TODO: Scan all columns from schema
        )
        if err != nil {
            return nil, fmt.Errorf("failed to scan log record: %w", err)
        }
        records = append(records, rec)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating log records: %w", err)
    }

    return records, nil
}
```

#### 3. Implement Log Object Building

Create `pkg/logcourier/logobject.go`:

```go
package logcourier

import (
    "bytes"
    "fmt"
    "time"
)

// LogObject represents a log object to be written to S3
type LogObject struct {
    Key     string
    Content []byte
}

// LogObjectBuilder builds log objects from log records
type LogObjectBuilder struct {
    // Configuration for log format if needed
}

// NewLogObjectBuilder creates a new log object builder
func NewLogObjectBuilder() *LogObjectBuilder {
    return &LogObjectBuilder{}
}

// Build builds a log object from log records
func (b *LogObjectBuilder) Build(bucket string, records []LogRecord) (LogObject, error) {
    if len(records) == 0 {
        return LogObject{}, fmt.Errorf("no records to build log object")
    }

    // Generate object key
    // TODO: Clarify key format with Dimitrios
    // Example format: <bucket-name>/2025-10-22-12-30-45-ABCD1234
    key := b.generateKey(bucket, records[0].Time)

    // Format log records according to AWS S3 Server Access Log format
    content := b.formatLogRecords(records)

    return LogObject{
        Key:     key,
        Content: content,
    }, nil
}

func (b *LogObjectBuilder) generateKey(bucket string, timestamp time.Time) string {
    // TODO: Implement proper key generation
    // AWS format: <TargetPrefix>YYYY-mm-DD-HH-MM-SS-UniqueString
    // For now, simple format:
    return fmt.Sprintf("%s/%s", bucket, timestamp.Format("2006-01-02-15-04-05"))
}

func (b *LogObjectBuilder) formatLogRecords(records []LogRecord) []byte {
    // TODO: Implement proper AWS S3 Server Access Log format
    // Format: space-separated fields, quoted strings, dash for empty fields
    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html

    var buf bytes.Buffer

    for _, rec := range records {
        // Example format (simplified, need proper implementation):
        // <bucket-owner> <bucket> <time> <remote-ip> <requester> <request-id> <operation> ...
        line := fmt.Sprintf("%s %s [%s] - - %s %s %s -\n",
            "-", // bucket owner (placeholder)
            rec.Bucket,
            rec.Time.Format("02/Jan/2006:15:04:05 -0700"),
            rec.RequestID,
            rec.Operation,
            rec.Key,
        )
        buf.WriteString(line)
    }

    return buf.Bytes()
}

// Helper to escape strings if needed
func (b *LogObjectBuilder) escapeField(s string) string {
    // TODO: Implement proper escaping according to AWS format
    // Quote strings that contain spaces
    // Escape quotes
    return s
}
```

**Note:** This is a placeholder implementation. Proper AWS S3 Server Access Log format must be implemented after discussing with Dimitrios.

### Testing

Create `pkg/logcourier/logfetch_test.go`:

```go
package logcourier_test

import (
    "context"
    "os"
    "time"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/logcourier"
    "github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("LogFetcher", func() {
    var (
        ctx    context.Context
        helper *testutil.ClickHouseTestHelper
        lf     *logcourier.LogFetcher
    )

    BeforeEach(func() {
        ctx = context.Background()

        if os.Getenv("LOG_COURIER_CLICKHOUSE_URL") == "" {
            Skip("LOG_COURIER_CLICKHOUSE_URL not set")
        }

        var err error
        helper, err = testutil.NewClickHouseTestHelper(ctx)
        Expect(err).NotTo(HaveOccurred())

        err = helper.Setup(ctx)
        Expect(err).NotTo(HaveOccurred())

        lf = logcourier.NewLogFetcher(helper.Client)
    })

    AfterEach(func() {
        if helper != nil {
            helper.Teardown(ctx)
            helper.Close()
        }
    })

    Describe("FetchLogs", func() {
        It("should fetch logs in time window", func() {
            now := time.Now()

            // Insert 3 logs
            for i := 0; i < 3; i++ {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: 1,
                    Bucket:         "test-bucket",
                    Time:           now.Add(time.Duration(i) * time.Second),
                    RequestID:      fmt.Sprintf("req-%d", i),
                    Operation:      "GetObject",
                    Key:            "test-key",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            time.Sleep(100 * time.Millisecond)

            // Fetch logs
            wo := logcourier.LogBatch{
                Bucket:       "test-bucket",
                MinTimestamp: now.Add(-1 * time.Second),
                MaxTimestamp: now.Add(10 * time.Second),
            }

            records, err := lf.FetchLogs(ctx, wo)
            Expect(err).NotTo(HaveOccurred())
            Expect(records).To(HaveLen(3))
        })

        It("should return logs sorted by time", func() {
            now := time.Now()

            // Insert logs in reverse order
            times := []time.Time{
                now.Add(3 * time.Second),
                now.Add(1 * time.Second),
                now.Add(2 * time.Second),
            }

            for i, t := range times {
                err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: 1,
                    Bucket:         "test-bucket",
                    Time:           t,
                    RequestID:      fmt.Sprintf("req-%d", i),
                    Operation:      "GetObject",
                    Key:            "test-key",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            time.Sleep(100 * time.Millisecond)

            wo := logcourier.LogBatch{
                Bucket:       "test-bucket",
                MinTimestamp: now,
                MaxTimestamp: now.Add(10 * time.Second),
            }

            records, err := lf.FetchLogs(ctx, wo)
            Expect(err).NotTo(HaveOccurred())
            Expect(records).To(HaveLen(3))

            // Verify sorted by time
            for i := 1; i < len(records); i++ {
                Expect(records[i].Time.After(records[i-1].Time)).To(BeTrue())
            }
        })

        It("should only fetch logs for specified bucket", func() {
            now := time.Now()

            // Insert logs for bucket-1
            err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
                LoggingEnabled: 1,
                Bucket:         "bucket-1",
                Time:           now,
                RequestID:      "req-1",
                Operation:      "GetObject",
                Key:            "key1",
            })
            Expect(err).NotTo(HaveOccurred())

            // Insert logs for bucket-2
            err = helper.InsertTestLog(ctx, testutil.TestLogRecord{
                LoggingEnabled: 1,
                Bucket:         "bucket-2",
                Time:           now,
                RequestID:      "req-2",
                Operation:      "GetObject",
                Key:            "key2",
            })
            Expect(err).NotTo(HaveOccurred())

            time.Sleep(100 * time.Millisecond)

            // Fetch only bucket-1
            wo := logcourier.LogBatch{
                Bucket:       "bucket-1",
                MinTimestamp: now.Add(-1 * time.Second),
                MaxTimestamp: now.Add(1 * time.Second),
            }

            records, err := lf.FetchLogs(ctx, wo)
            Expect(err).NotTo(HaveOccurred())
            Expect(records).To(HaveLen(1))
            Expect(records[0].Bucket).To(Equal("bucket-1"))
        })
    })
})
```

Create `pkg/logcourier/logobject_test.go`:

```go
package logcourier_test

import (
    "time"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/logcourier"
)

var _ = Describe("LogObjectBuilder", func() {
    var builder *logcourier.LogObjectBuilder

    BeforeEach(func() {
        builder = logcourier.NewLogObjectBuilder()
    })

    Describe("Build", func() {
        It("should build log object from records", func() {
            now := time.Now()
            records := []logcourier.LogRecord{
                {
                    Bucket:    "test-bucket",
                    Time:      now,
                    RequestID: "req-1",
                    Operation: "GetObject",
                    Key:       "test-key",
                },
            }

            obj, err := builder.Build("test-bucket", records)
            Expect(err).NotTo(HaveOccurred())
            Expect(obj.Key).NotTo(BeEmpty())
            Expect(obj.Content).NotTo(BeEmpty())
        })

        It("should fail with empty records", func() {
            _, err := builder.Build("test-bucket", []logcourier.LogRecord{})
            Expect(err).To(HaveOccurred())
        })

        // TODO: Add more tests after log format is finalized
    })
})
```

### Deliverables

- Log fetching implementation
- Log object building (AWS S3 Server Access Log format)
- Comprehensive tests
- Clear documentation of log format

### Future Verification Tasks

After initial implementation, verify the following:
1. **Time field verification**: Confirm `startTime` is the correct timestamp for AWS Time field (vs `timestamp`)
2. **Object Size verification**: Confirm `contentLength` correctly represents total object size (vs `bodyLength`)
3. **objectKey column**: Verify CloudServer populates this field correctly and it's available in ClickHouse

---

## Task 6: S3 Integration

### Objective
Implement S3 client and log object upload functionality.

### DECISION POINT - Discuss with Dimitrios

Before implementing this task, need to evaluate S3 SDK options:

#### Option 1: AWS SDK for Go v2
**Pros:**
- Official AWS SDK
- Comprehensive features
- Well-maintained
- Good documentation
- Supports IAM roles, assume role, etc.

**Cons:**
- Large dependency footprint
- More complex API
- AWS-specific (but S3-compatible services work)

**Usage example:**
```go
import "github.com/aws/aws-sdk-go-v2/service/s3"
```

#### Option 2: MinIO Go Client
**Pros:**
- Simpler API
- S3-compatible focused
- Smaller dependency
- Easy to use

**Cons:**
- Third-party SDK
- Less comprehensive than AWS SDK
- May lack some advanced AWS features

**Usage example:**
```go
import "github.com/minio/minio-go/v7"
```

#### Option 3: Check metadata-migration
Does metadata-migration use an S3 client? Follow their choice for consistency.

**Questions:**
1. Which SDK should we use?
2. S3 configuration requirements:
   - Credentials: access key/secret key vs IAM role?
   - Endpoint: custom endpoint for non-AWS S3?
   - Region configuration?
3. Destination bucket configuration:
   - Where is destination bucket info stored? (in log records? separate config?)
   - Target prefix for log objects?

### Implementation Steps (Using AWS SDK v2 as example)

#### 1. Add S3 SDK Dependency

```bash
go get github.com/aws/aws-sdk-go-v2
go get github.com/aws/aws-sdk-go-v2/service/s3
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/credentials
```

#### 2. Implement S3 Client

Create `pkg/s3/client.go`:

```go
package s3

import (
    "context"
    "fmt"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client wraps S3 client
type Client struct {
    s3Client *s3.Client
}

// Config holds S3 client configuration
type Config struct {
    Endpoint        string // Empty for AWS S3
    Region          string
    AccessKeyID     string // Empty to use IAM role
    SecretAccessKey string
}

// NewClient creates a new S3 client
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
    var optFns []func(*config.LoadOptions) error

    // Set region
    optFns = append(optFns, config.WithRegion(cfg.Region))

    // Set credentials if provided
    if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
        optFns = append(optFns, config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider(
                cfg.AccessKeyID,
                cfg.SecretAccessKey,
                "",
            ),
        ))
    }

    awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
    if err != nil {
        return nil, fmt.Errorf("failed to load AWS config: %w", err)
    }

    // Create S3 client
    s3ClientOpts := []func(*s3.Options){}

    // Set custom endpoint if provided (for S3-compatible services)
    if cfg.Endpoint != "" {
        s3ClientOpts = append(s3ClientOpts, func(o *s3.Options) {
            o.BaseEndpoint = aws.String(cfg.Endpoint)
            o.UsePathStyle = true // Required for MinIO and some S3-compatible services
        })
    }

    s3Client := s3.NewFromConfig(awsCfg, s3ClientOpts...)

    return &Client{s3Client: s3Client}, nil
}

// Close closes the client (no-op for AWS SDK v2)
func (c *Client) Close() error {
    return nil
}
```

#### 3. Implement Upload Functionality

Create `pkg/s3/uploader.go`:

```go
package s3

import (
    "bytes"
    "context"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Uploader uploads log objects to S3
type Uploader struct {
    client *Client
}

// NewUploader creates a new uploader
func NewUploader(client *Client) *Uploader {
    return &Uploader{client: client}
}

// Upload uploads a log object to the specified bucket
func (u *Uploader) Upload(ctx context.Context, bucket, key string, content []byte) error {
    _, err := u.client.s3Client.PutObject(ctx, &s3.PutObjectInput{
        Bucket:        aws.String(bucket),
        Key:           aws.String(key),
        Body:          bytes.NewReader(content),
        ContentType:   aws.String("text/plain"),
        ContentLength: aws.Int64(int64(len(content))),
    })

    if err != nil {
        return fmt.Errorf("failed to upload to S3: bucket=%s, key=%s: %w", bucket, key, err)
    }

    return nil
}

// UploadWithRetry uploads with exponential backoff retry
func (u *Uploader) UploadWithRetry(ctx context.Context, bucket, key string, content []byte, maxRetries int) error {
    var lastErr error

    for attempt := 0; attempt <= maxRetries; attempt++ {
        err := u.Upload(ctx, bucket, key, content)
        if err == nil {
            return nil
        }

        lastErr = err

        if attempt < maxRetries {
            // Exponential backoff: 1s, 2s, 4s, 8s, ...
            backoff := time.Duration(1<<attempt) * time.Second
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(backoff):
                // Continue to next attempt
            }
        }
    }

    return fmt.Errorf("upload failed after %d attempts: %w", maxRetries+1, lastErr)
}
```

#### 4. Update ConfigSpec

Add S3 configuration to `pkg/logcourier/configspec.go` (if not already there):

```go
"s3.endpoint": util.ConfigVarSpec{
    Help:         "S3 endpoint URL (leave empty for AWS S3)",
    DefaultValue: "",
    EnvVar:       "LOG_COURIER_S3_ENDPOINT",
},
"s3.region": util.ConfigVarSpec{
    Help:         "S3 region",
    DefaultValue: "us-east-1",
    EnvVar:       "LOG_COURIER_S3_REGION",
},
"s3.access-key-id": util.ConfigVarSpec{
    Help:         "S3 access key ID (leave empty to use IAM role)",
    DefaultValue: "",
    EnvVar:       "LOG_COURIER_S3_ACCESS_KEY_ID",
},
"s3.secret-access-key": util.ConfigVarSpec{
    Help:         "S3 secret access key",
    DefaultValue: "",
    EnvVar:       "LOG_COURIER_S3_SECRET_ACCESS_KEY",
    ExposeFunc: func(rawValue any) any {
        if rawValue.(string) == "" {
            return ""
        }
        return "***"
    },
},
```

### Testing

Create `pkg/s3/uploader_test.go`:

```go
package s3_test

import (
    "context"
    "fmt"
    "os"
    "testing"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/s3"
)

func TestS3(t *testing.T) {
    RegisterFailHandler(Fail)
    RunSpecs(t, "S3 Suite")
}

var _ = Describe("S3 Uploader", func() {
    var (
        ctx      context.Context
        client   *s3.Client
        uploader *s3.Uploader
        bucket   string
    )

    BeforeEach(func() {
        ctx = context.Background()

        // Skip if no S3 endpoint configured
        endpoint := os.Getenv("TEST_S3_ENDPOINT")
        if endpoint == "" {
            Skip("TEST_S3_ENDPOINT not set, skipping S3 integration tests")
        }

        accessKey := os.Getenv("TEST_S3_ACCESS_KEY")
        secretKey := os.Getenv("TEST_S3_SECRET_KEY")
        bucket = os.Getenv("TEST_S3_BUCKET")

        if bucket == "" {
            bucket = "test-log-courier"
        }

        cfg := s3.Config{
            Endpoint:        endpoint,
            Region:          "us-east-1",
            AccessKeyID:     accessKey,
            SecretAccessKey: secretKey,
        }

        var err error
        client, err = s3.NewClient(ctx, cfg)
        Expect(err).NotTo(HaveOccurred())

        uploader = s3.NewUploader(client)
    })

    AfterEach(func() {
        if client != nil {
            client.Close()
        }
    })

    Describe("Upload", func() {
        It("should upload object successfully", func() {
            key := fmt.Sprintf("test-logs/%d", time.Now().Unix())
            content := []byte("test log content\n")

            err := uploader.Upload(ctx, bucket, key, content)
            Expect(err).NotTo(HaveOccurred())
        })

        It("should handle large objects", func() {
            key := fmt.Sprintf("test-logs/large-%d", time.Now().Unix())
            // 1MB content
            content := make([]byte, 1024*1024)
            for i := range content {
                content[i] = byte(i % 256)
            }

            err := uploader.Upload(ctx, bucket, key, content)
            Expect(err).NotTo(HaveOccurred())
        })

        It("should fail with invalid bucket", func() {
            key := "test-key"
            content := []byte("test")

            err := uploader.Upload(ctx, "nonexistent-bucket-12345", key, content)
            Expect(err).To(HaveOccurred())
        })
    })

    Describe("UploadWithRetry", func() {
        It("should retry on failure", func() {
            // This test is hard to implement without mocking or flaky network
            // Consider mocking the S3 client for unit tests
            Skip("Retry testing requires mocking or flaky network simulation")
        })
    })
})
```

**Environment setup for local testing:**
```bash
# Start MinIO locally
docker run -d --name minio-test \
    -p 9000:9000 \
    -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"

# Create test bucket
aws --endpoint-url http://localhost:9000 \
    s3 mb s3://test-log-courier

export TEST_S3_ENDPOINT=http://localhost:9000
export TEST_S3_ACCESS_KEY=minioadmin
export TEST_S3_SECRET_KEY=minioadmin
export TEST_S3_BUCKET=test-log-courier
```

**For CI:** Add MinIO service to GitHub Actions workflow.

### Deliverables

- S3 client implementation
- Upload functionality with retry logic
- Comprehensive tests (unit + integration)
- Support for both AWS S3 and S3-compatible services

---

## Task 7: Offset Management

### Objective
Implement offset commit to track processing progress per bucket.

### Study Phase

1. Review offsets table schema from ARCHITECTURE.md
   - ReplicatedReplacingMergeTree semantics
   - Primary key: bucket
   - Version column: last_processed_ts (latest value wins)

2. Understand consistency guarantees:
   - At-least-once delivery (logs may be duplicated on failure)
   - Only commit offset after successful S3 upload

### Implementation Steps

#### 1. Implement Offset Manager

Create `pkg/logcourier/offset.go`:

```go
package logcourier

import (
    "context"
    "fmt"
    "time"

    "github.com/scality/log-courier/pkg/clickhouse"
)

// OffsetManager manages bucket offsets
type OffsetManager struct {
    client *clickhouse.Client
}

// NewOffsetManager creates a new offset manager
func NewOffsetManager(client *clickhouse.Client) *OffsetManager {
    return &OffsetManager{client: client}
}

// CommitOffset commits the processing offset for a bucket
// Uses max(insertedAt) from processed log records
// Phase 1: raftSessionId is always 0 (no raft session filtering)
func (om *OffsetManager) CommitOffset(ctx context.Context, bucket string, timestamp time.Time) error {
    query := `
        INSERT INTO logs.offsets (bucketName, raftSessionId, last_processed_ts)
        VALUES (?, ?, ?)
    `

    // Phase 1: Use raftSessionId = 0 since we process all logs
    err := om.client.Exec(ctx, query, bucket, uint16(0), timestamp)
    if err != nil {
        return fmt.Errorf("failed to commit offset for bucket %s: %w", bucket, err)
    }

    return nil
}

// GetOffset retrieves the current offset for a bucket
// Returns zero time if bucket has no offset
// Phase 1: Gets max timestamp across all raft sessions for the bucket
func (om *OffsetManager) GetOffset(ctx context.Context, bucket string) (time.Time, error) {
    query := `
        SELECT max(last_processed_ts)
        FROM logs.offsets
        WHERE bucketName = ?
    `

    row := om.client.QueryRow(ctx, query, bucket)

    var timestamp time.Time
    err := row.Scan(&timestamp)
    if err != nil {
        // Check if no rows (bucket never processed)
        if err.Error() == "sql: no rows in result set" {
            return time.Time{}, nil
        }
        return time.Time{}, fmt.Errorf("failed to get offset for bucket %s: %w", bucket, err)
    }

    return timestamp, nil
}
```

#### 2. Extract Offset from Log Records

Add helper method to extract max inserted_at from log records:

```go
// GetMaxInsertedAt returns the maximum inserted_at timestamp from records
// This is used as the offset to commit
func GetMaxInsertedAt(records []LogRecord) time.Time {
    if len(records) == 0 {
        return time.Time{}
    }

    maxTs := records[0].InsertedAt
    for _, rec := range records[1:] {
        if rec.InsertedAt.After(maxTs) {
            maxTs = rec.InsertedAt
        }
    }

    return maxTs
}
```

#### 3. Document Consistency Guarantees

Add documentation to `offset.go`:

```go
// Consistency Guarantees
//
// 1. At-least-once delivery:
//    - Offsets are committed AFTER successful S3 upload
//    - If upload succeeds but offset commit fails, logs may be reprocessed and duplicated
//    - If consumer crashes after S3 upload but before offset commit, logs will be reprocessed
//
// 2. Offset semantics:
//    - Offset is max(inserted_at) from processed log records
//    - On restart, consumer processes logs with inserted_at > last committed offset
//
// 3. ReplacingMergeTree behavior:
//    - Multiple offset commits for same bucket: latest timestamp wins
//    - Eventually consistent (ClickHouse merges may be delayed)
//    - Work discovery query uses max(last_processed_ts) to get latest offset
```

### Testing

Create `pkg/logcourier/offset_test.go`:

```go
package logcourier_test

import (
    "context"
    "os"
    "time"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/logcourier"
    "github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("OffsetManager", func() {
    var (
        ctx    context.Context
        helper *testutil.ClickHouseTestHelper
        om     *logcourier.OffsetManager
    )

    BeforeEach(func() {
        ctx = context.Background()

        if os.Getenv("LOG_COURIER_CLICKHOUSE_URL") == "" {
            Skip("LOG_COURIER_CLICKHOUSE_URL not set")
        }

        var err error
        helper, err = testutil.NewClickHouseTestHelper(ctx)
        Expect(err).NotTo(HaveOccurred())

        err = helper.Setup(ctx)
        Expect(err).NotTo(HaveOccurred())

        om = logcourier.NewOffsetManager(helper.Client)
    })

    AfterEach(func() {
        if helper != nil {
            helper.Teardown(ctx)
            helper.Close()
        }
    })

    Describe("CommitOffset", func() {
        It("should commit offset successfully", func() {
            now := time.Now()
            err := om.CommitOffset(ctx, "test-bucket", now)
            Expect(err).NotTo(HaveOccurred())
        })

        It("should allow multiple commits for same bucket", func() {
            t1 := time.Now()
            t2 := t1.Add(1 * time.Hour)

            err := om.CommitOffset(ctx, "test-bucket", t1)
            Expect(err).NotTo(HaveOccurred())

            err = om.CommitOffset(ctx, "test-bucket", t2)
            Expect(err).NotTo(HaveOccurred())
        })
    })

    Describe("GetOffset", func() {
        It("should return zero time for bucket with no offset", func() {
            offset, err := om.GetOffset(ctx, "nonexistent-bucket")
            Expect(err).NotTo(HaveOccurred())
            Expect(offset.IsZero()).To(BeTrue())
        })

        It("should return committed offset", func() {
            now := time.Now()
            err := om.CommitOffset(ctx, "test-bucket", now)
            Expect(err).NotTo(HaveOccurred())

            // Wait for ClickHouse to process
            time.Sleep(100 * time.Millisecond)

            offset, err := om.GetOffset(ctx, "test-bucket")
            Expect(err).NotTo(HaveOccurred())
            // Allow small time difference due to precision
            Expect(offset.Unix()).To(Equal(now.Unix()))
        })

        It("should return latest offset when multiple commits", func() {
            t1 := time.Now()
            t2 := t1.Add(1 * time.Hour)
            t3 := t2.Add(1 * time.Hour)

            err := om.CommitOffset(ctx, "test-bucket", t1)
            Expect(err).NotTo(HaveOccurred())

            err = om.CommitOffset(ctx, "test-bucket", t2)
            Expect(err).NotTo(HaveOccurred())

            err = om.CommitOffset(ctx, "test-bucket", t3)
            Expect(err).NotTo(HaveOccurred())

            time.Sleep(100 * time.Millisecond)

            offset, err := om.GetOffset(ctx, "test-bucket")
            Expect(err).NotTo(HaveOccurred())
            Expect(offset.Unix()).To(Equal(t3.Unix()))
        })
    })

    Describe("GetMaxInsertedAt", func() {
        It("should return zero time for empty records", func() {
            maxTs := logcourier.GetMaxInsertedAt([]logcourier.LogRecord{})
            Expect(maxTs.IsZero()).To(BeTrue())
        })

        It("should return max timestamp from records", func() {
            now := time.Now()
            records := []logcourier.LogRecord{
                {InsertedAt: now},
                {InsertedAt: now.Add(1 * time.Hour)},
                {InsertedAt: now.Add(-1 * time.Hour)},
            }

            maxTs := logcourier.GetMaxInsertedAt(records)
            Expect(maxTs.Unix()).To(Equal(now.Add(1 * time.Hour).Unix()))
        })
    })
})
```

### Deliverables

- Offset commit implementation
- Offset retrieval implementation
- Helper for extracting max timestamp
- Comprehensive tests
- Clear documentation of consistency guarantees

---

## Task 8: Core Consumer Loop

### Objective
Implement the main consumer service that ties all components together.

### Study Phase

1. Review consumer algorithm from ARCHITECTURE.md (lines 200-283)
   - Two-phase operation
   - Periodic batch finder
   - Sequential log batch processing

2. Review signal handling from metadata-migration
   - Graceful shutdown on SIGINT, SIGTERM
   - Context cancellation

### Implementation Steps

#### 1. Define Consumer Structure

Create `pkg/logcourier/consumer.go`:

```go
package logcourier

import (
    "context"
    "fmt"
    "log/slog"
    "time"

    "github.com/scality/log-courier/pkg/clickhouse"
    "github.com/scality/log-courier/pkg/s3"
)

// Consumer is the main log courier consumer
type Consumer struct {
    // Components
    clickhouseClient *clickhouse.Client
    s3Uploader       *s3.Uploader
    workDiscovery    *BatchFinder
    logFetcher       *LogFetcher
    logBuilder       *LogObjectBuilder
    offsetManager    *OffsetManager

    // Configuration
    discoveryInterval time.Duration

    // Logger
    logger *slog.Logger
}

// Config holds consumer configuration
type Config struct {
    // ClickHouse
    ClickHouseURL      string
    ClickHouseDatabase string
    ClickHouseUsername string
    ClickHousePassword string
    ClickHouseTimeout  time.Duration

    // Work discovery
    CountThreshold   int
    TimeThresholdSec int
    DiscoveryInterval time.Duration

    // S3
    S3Endpoint        string
    S3Region          string
    S3AccessKeyID     string
    S3SecretAccessKey string

    // General
    Logger *slog.Logger
}

// NewConsumer creates a new consumer
func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
    // Create ClickHouse client
    chClient, err := clickhouse.NewClient(ctx, clickhouse.Config{
        URL:      cfg.ClickHouseURL,
        Database: cfg.ClickHouseDatabase,
        Username: cfg.ClickHouseUsername,
        Password: cfg.ClickHousePassword,
        Timeout:  cfg.ClickHouseTimeout,
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
    }

    // Create S3 client
    s3Client, err := s3.NewClient(ctx, s3.Config{
        Endpoint:        cfg.S3Endpoint,
        Region:          cfg.S3Region,
        AccessKeyID:     cfg.S3AccessKeyID,
        SecretAccessKey: cfg.S3SecretAccessKey,
    })
    if err != nil {
        chClient.Close()
        return nil, fmt.Errorf("failed to create S3 client: %w", err)
    }

    return &Consumer{
        clickhouseClient:  chClient,
        s3Uploader:        s3.NewUploader(s3Client),
        workDiscovery:     NewBatchFinder(chClient, cfg.CountThreshold, cfg.TimeThresholdSec),
        logFetcher:        NewLogFetcher(chClient),
        logBuilder:        NewLogObjectBuilder(),
        offsetManager:     NewOffsetManager(chClient),
        discoveryInterval: cfg.DiscoveryInterval,
        logger:            cfg.Logger,
    }, nil
}

// Close closes the consumer and releases resources
func (c *Consumer) Close() error {
    if c.clickhouseClient != nil {
        return c.clickhouseClient.Close()
    }
    return nil
}
```

#### 2. Implement Main Loop

Add to `consumer.go`:

```go
// Run runs the consumer main loop
// Blocks until context is cancelled
func (c *Consumer) Run(ctx context.Context) error {
    c.logger.Info("consumer starting")

    ticker := time.NewTicker(c.discoveryInterval)
    defer ticker.Stop()

    // Run initial batch finder immediately
    if err := c.runBatchFinder(ctx); err != nil {
        c.logger.Error("initial batch finder failed", "error", err)
        // Continue anyway
    }

    for {
        select {
        case <-ctx.Done():
            c.logger.Info("consumer stopping")
            return ctx.Err()

        case <-ticker.C:
            if err := c.runBatchFinder(ctx); err != nil {
                c.logger.Error("batch finder failed", "error", err)
                // Continue to next interval
            }
        }
    }
}

// runBatchFinder executes batch finder and processes log batchs
func (c *Consumer) runBatchFinder(ctx context.Context) error {
    // Phase 1: Work Discovery
    c.logger.Debug("starting batch finder")

    batchs, err := c.workDiscovery.FindBatches(ctx)
    if err != nil {
        return fmt.Errorf("batch finder failed: %w", err)
    }

    c.logger.Info("batch finder completed", "work_orders", len(batchs))

    if len(batchs) == 0 {
        return nil
    }

    // Phase 2: Process log batchs sequentially
    for _, wo := range batchs {
        if err := c.processLogBatch(ctx, wo); err != nil {
            c.logger.Error("failed to process log batch",
                "bucket", wo.Bucket,
                "log_count", wo.LogCount,
                "error", err)
            // Continue to next log batch
        }
    }

    return nil
}

// processLogBatch processes a single log batch
func (c *Consumer) processLogBatch(ctx context.Context, wo LogBatch) error {
    c.logger.Info("processing log batch",
        "bucket", wo.Bucket,
        "log_count", wo.LogCount,
        "time_range", fmt.Sprintf("[%s, %s]", wo.MinTimestamp, wo.MaxTimestamp))

    // 1. Fetch logs
    records, err := c.logFetcher.FetchLogs(ctx, wo)
    if err != nil {
        return fmt.Errorf("failed to fetch logs: %w", err)
    }

    if len(records) == 0 {
        c.logger.Warn("no logs fetched for log batch", "bucket", wo.Bucket)
        return nil
    }

    c.logger.Debug("fetched logs", "bucket", wo.Bucket, "count", len(records))

    // 2. Build log object
    logObj, err := c.logBuilder.Build(wo.Bucket, records)
    if err != nil {
        return fmt.Errorf("failed to build log object: %w", err)
    }

    c.logger.Debug("built log object", "bucket", wo.Bucket, "key", logObj.Key, "size", len(logObj.Content))

    // 3. Upload to S3
    // TODO: Determine destination bucket (from records? config?)
    destinationBucket := wo.Bucket // Placeholder

    err = c.s3Uploader.UploadWithRetry(ctx, destinationBucket, logObj.Key, logObj.Content, 3)
    if err != nil {
        return fmt.Errorf("failed to upload log object: %w", err)
    }

    c.logger.Info("uploaded log object",
        "bucket", wo.Bucket,
        "destination_bucket", destinationBucket,
        "key", logObj.Key,
        "size", len(logObj.Content))

    // 4. Commit offset (only after successful upload)
    maxInsertedAt := GetMaxInsertedAt(records)
    err = c.offsetManager.CommitOffset(ctx, wo.Bucket, maxInsertedAt)
    if err != nil {
        // Log error but don't fail - offset will be retried next time
        c.logger.Error("failed to commit offset", "bucket", wo.Bucket, "error", err)
        // Note: This means logs may be duplicated on next run (at-least-once delivery)
    } else {
        c.logger.Debug("committed offset", "bucket", wo.Bucket, "offset", maxInsertedAt)
    }

    return nil
}
```

#### 3. Implement Main Entry Point

Update `cmd/log-courier/main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "os/signal"
    "time"

    "github.com/spf13/pflag"
    "golang.org/x/sys/unix"

    "github.com/scality/log-courier/pkg/logcourier"
)

func main() {
    // Add command-line flags
    logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "clickhouse-url", "clickhouse.url")
    logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "log-level", "log-level")
    // Add more flags as needed

    configFileFlag := pflag.String("config-file", "", "Path to YAML configuration file")
    pflag.Parse()

    // Load configuration
    configFile := *configFileFlag
    if configFile == "" {
        configFile = os.Getenv("LOG_COURIER_CONFIG_FILE")
    }

    err := logcourier.ConfigSpec.LoadConfiguration(configFile, "", nil)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
        pflag.Usage()
        os.Exit(2)
    }

    // Validate configuration
    err = logcourier.ValidateConfig()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Configuration validation error: %v\n", err)
        os.Exit(2)
    }

    // Set up logger
    logLevel := parseLogLevel(logcourier.ConfigSpec.GetString("log-level"))
    logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: logLevel,
    }))

    // Create consumer
    ctx := context.Background()

    consumerCfg := logcourier.Config{
        ClickHouseURL:      logcourier.ConfigSpec.GetString("clickhouse.url"),
        ClickHouseDatabase: logcourier.ConfigSpec.GetString("clickhouse.database"),
        ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
        ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
        ClickHouseTimeout:  time.Duration(logcourier.ConfigSpec.GetInt("clickhouse.timeout-seconds")) * time.Second,
        CountThreshold:     logcourier.ConfigSpec.GetInt("consumer.count-threshold"),
        TimeThresholdSec:   logcourier.ConfigSpec.GetInt("consumer.time-threshold-seconds"),
        DiscoveryInterval:  time.Duration(logcourier.ConfigSpec.GetInt("consumer.discovery-interval-seconds")) * time.Second,
        S3Endpoint:         logcourier.ConfigSpec.GetString("s3.endpoint"),
        S3Region:           logcourier.ConfigSpec.GetString("s3.region"),
        S3AccessKeyID:      logcourier.ConfigSpec.GetString("s3.access-key-id"),
        S3SecretAccessKey:  logcourier.ConfigSpec.GetString("s3.secret-access-key"),
        Logger:             logger,
    }

    consumer, err := logcourier.NewConsumer(ctx, consumerCfg)
    if err != nil {
        logger.Error("failed to create consumer", "error", err)
        os.Exit(1)
    }
    defer consumer.Close()

    // Set up signal handling for graceful shutdown
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    signalsChan := make(chan os.Signal, 1)
    signal.Notify(signalsChan, unix.SIGINT, unix.SIGTERM)

    // Start consumer in goroutine
    errChan := make(chan error, 1)
    go func() {
        errChan <- consumer.Run(ctx)
    }()

    // Wait for signal or error
    select {
    case sig := <-signalsChan:
        logger.Info("signal received", "signal", sig)
        cancel()

        // Wait for consumer to stop gracefully (with timeout)
        shutdownTimeout := time.Duration(logcourier.ConfigSpec.GetInt("shutdown-timeout-seconds")) * time.Second
        select {
        case <-time.After(shutdownTimeout):
            logger.Warn("shutdown timeout exceeded, forcing exit")
            os.Exit(1)
        case err := <-errChan:
            if err != nil && err != context.Canceled {
                logger.Error("consumer stopped with error", "error", err)
                os.Exit(1)
            }
        }

    case err := <-errChan:
        if err != nil && err != context.Canceled {
            logger.Error("consumer error", "error", err)
            os.Exit(1)
        }
    }

    logger.Info("log-courier stopped")
}

func parseLogLevel(level string) slog.Level {
    switch level {
    case "debug":
        return slog.LevelDebug
    case "info":
        return slog.LevelInfo
    case "warn":
        return slog.LevelWarn
    case "error":
        return slog.LevelError
    default:
        return slog.LevelInfo
    }
}
```

### Testing

Create `pkg/logcourier/consumer_test.go`:

```go
package logcourier_test

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "time"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/logcourier"
    "github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("Consumer", func() {
    var (
        ctx              context.Context
        cancel           context.CancelFunc
        clickhouseHelper *testutil.ClickHouseTestHelper
        consumer         *logcourier.Consumer
    )

    BeforeEach(func() {
        if os.Getenv("LOG_COURIER_CLICKHOUSE_URL") == "" || os.Getenv("TEST_S3_ENDPOINT") == "" {
            Skip("Integration test environment not configured")
        }

        ctx, cancel = context.WithCancel(context.Background())

        // Set up ClickHouse
        var err error
        clickhouseHelper, err = testutil.NewClickHouseTestHelper(ctx)
        Expect(err).NotTo(HaveOccurred())

        err = clickhouseHelper.Setup(ctx)
        Expect(err).NotTo(HaveOccurred())

        // Create consumer
        cfg := logcourier.Config{
            ClickHouseURL:      os.Getenv("LOG_COURIER_CLICKHOUSE_URL"),
            ClickHouseDatabase: "logs",
            ClickHouseUsername: "default",
            ClickHousePassword: "",
            ClickHouseTimeout:  10 * time.Second,
            CountThreshold:     10,
            TimeThresholdSec:   60,
            DiscoveryInterval:  1 * time.Second,
            S3Endpoint:         os.Getenv("TEST_S3_ENDPOINT"),
            S3Region:           "us-east-1",
            S3AccessKeyID:      os.Getenv("TEST_S3_ACCESS_KEY"),
            S3SecretAccessKey:  os.Getenv("TEST_S3_SECRET_KEY"),
            Logger:             slog.Default(),
        }

        consumer, err = logcourier.NewConsumer(ctx, cfg)
        Expect(err).NotTo(HaveOccurred())
    })

    AfterEach(func() {
        cancel()
        if consumer != nil {
            consumer.Close()
        }
        if clickhouseHelper != nil {
            clickhouseHelper.Teardown(ctx)
            clickhouseHelper.Close()
        }
    })

    Describe("End-to-end", func() {
        It("should process logs end-to-end", func() {
            // Insert test logs (above threshold)
            for i := 0; i < 15; i++ {
                err := clickhouseHelper.InsertTestLog(ctx, testutil.TestLogRecord{
                    LoggingEnabled: 1,
                    Bucket:         "test-bucket",
                    Time:           time.Now(),
                    RequestID:      fmt.Sprintf("req-%d", i),
                    Operation:      "GetObject",
                    Key:            "test-key",
                })
                Expect(err).NotTo(HaveOccurred())
            }

            // Wait for materialized view
            time.Sleep(100 * time.Millisecond)

            // Run consumer in background
            go consumer.Run(ctx)

            // Wait for consumer to process (discovery interval + processing time)
            time.Sleep(3 * time.Second)

            // Verify offset was committed
            om := logcourier.NewOffsetManager(clickhouseHelper.Client)
            offset, err := om.GetOffset(ctx, "test-bucket")
            Expect(err).NotTo(HaveOccurred())
            Expect(offset.IsZero()).To(BeFalse())

            // TODO: Verify log object was uploaded to S3
            // Would need S3 client to check
        })
    })
})
```

### Deliverables

- Complete consumer implementation
- Main entry point with signal handling
- End-to-end integration test
- Graceful shutdown working correctly

---

## Task 9: Observability

### Objective
Add comprehensive logging, metrics, and health checks.

### Study Phase

1. Review logging patterns from metadata-migration
   - slog usage
   - Structured logging
   - Log levels

2. Check if metadata-migration uses Prometheus metrics
   - Metrics server implementation
   - Metrics exposed

### Implementation Steps

#### 1. Enhance Logging

Review and enhance logging throughout codebase:

**In `consumer.go`:**
- Already has basic logging
- Add more detailed metrics:
  - Processing duration per log batch
  - Log object size
  - Upload duration
  - Error details

Example enhancements:

```go
func (c *Consumer) processLogBatch(ctx context.Context, wo LogBatch) error {
    startTime := time.Now()

    c.logger.Info("processing log batch started",
        "bucket", wo.Bucket,
        "log_count", wo.LogCount,
        "time_range_start", wo.MinTimestamp,
        "time_range_end", wo.MaxTimestamp)

    // ... existing code ...

    duration := time.Since(startTime)
    c.logger.Info("processing log batch completed",
        "bucket", wo.Bucket,
        "log_count", len(records),
        "log_object_size", len(logObj.Content),
        "duration_ms", duration.Milliseconds())

    return nil
}
```

Add error logging with context:

```go
if err != nil {
    c.logger.Error("failed to fetch logs",
        "bucket", wo.Bucket,
        "work_order", wo.String(),
        "error", err)
    return fmt.Errorf("failed to fetch logs: %w", err)
}
```

#### 2. Implement Metrics (Optional for Phase 1)

If metrics are required, follow metadata-migration pattern:

Create `pkg/logcourier/metrics.go`:

```go
package logcourier

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    batchsDiscovered = promauto.NewCounter(prometheus.CounterOpts{
        Name: "log_courier_work_orders_discovered_total",
        Help: "Total number of log batchs discovered",
    })

    batchsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "log_courier_work_orders_processed_total",
        Help: "Total number of log batchs processed",
    }, []string{"status"}) // status: success, error

    logsProcessed = promauto.NewCounter(prometheus.CounterOpts{
        Name: "log_courier_logs_processed_total",
        Help: "Total number of log records processed",
    })

    s3UploadDuration = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "log_courier_s3_upload_duration_seconds",
        Help:    "Duration of S3 uploads",
        Buckets: prometheus.DefBuckets,
    })

    s3UploadSize = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "log_courier_s3_upload_size_bytes",
        Help:    "Size of uploaded log objects",
        Buckets: prometheus.ExponentialBuckets(1024, 2, 15), // 1KB to 16MB
    })

    clickhouseQueryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "log_courier_clickhouse_query_duration_seconds",
        Help:    "Duration of ClickHouse queries",
        Buckets: prometheus.DefBuckets,
    })

    consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "log_courier_consumer_lag_seconds",
        Help: "Age of oldest unprocessed log per bucket",
    }, []string{"bucket"})
)
```

Instrument code to update metrics:

```go
// In runBatchFinder:
batchsDiscovered.Add(float64(len(batchs)))

// In processLogBatch:
defer func(start time.Time) {
    status := "success"
    if err != nil {
        status = "error"
    }
    batchsProcessed.WithLabelValues(status).Inc()
}(time.Now())

logsProcessed.Add(float64(len(records)))
s3UploadSize.Observe(float64(len(logObj.Content)))
```

Create metrics server (following metadata-migration):

```go
// In cmd/log-courier/main.go:
import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

// Start metrics server if enabled
metricsEnabled := logcourier.ConfigSpec.GetBool("metrics-server.enabled")
if metricsEnabled {
    metricsAddr := fmt.Sprintf("%s:%d",
        logcourier.ConfigSpec.GetString("metrics-server.listen-address"),
        logcourier.ConfigSpec.GetInt("metrics-server.listen-port"))

    go func() {
        http.Handle("/metrics", promhttp.Handler())
        logger.Info("starting metrics server", "address", metricsAddr)
        if err := http.ListenAndServe(metricsAddr, nil); err != nil {
            logger.Error("metrics server failed", "error", err)
        }
    }()
}
```

Add metrics configuration to ConfigSpec:

```go
"metrics-server.enabled": util.ConfigVarSpec{
    Help:         "Enable Prometheus metrics server",
    DefaultValue: false,
    EnvVar:       "LOG_COURIER_METRICS_SERVER_ENABLED",
},
"metrics-server.listen-address": util.ConfigVarSpec{
    Help:         "Metrics server listen address",
    DefaultValue: "127.0.0.1",
    EnvVar:       "LOG_COURIER_METRICS_SERVER_LISTEN_ADDRESS",
},
"metrics-server.listen-port": util.ConfigVarSpec{
    Help:         "Metrics server listen port",
    DefaultValue: 9090,
    EnvVar:       "LOG_COURIER_METRICS_SERVER_LISTEN_PORT",
},
```

#### 3. Implement Health Check

Create `pkg/logcourier/health.go`:

```go
package logcourier

import (
    "context"
    "encoding/json"
    "net/http"
    "time"
)

// HealthChecker checks health of dependencies
type HealthChecker struct {
    consumer *Consumer
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(consumer *Consumer) *HealthChecker {
    return &HealthChecker{consumer: consumer}
}

// HealthStatus represents health status
type HealthStatus struct {
    Status           string            `json:"status"` // "healthy" or "unhealthy"
    Checks           map[string]string `json:"checks"`
    Timestamp        time.Time         `json:"timestamp"`
}

// Check performs health check
func (hc *HealthChecker) Check(ctx context.Context) HealthStatus {
    status := HealthStatus{
        Status:    "healthy",
        Checks:    make(map[string]string),
        Timestamp: time.Now(),
    }

    // Check ClickHouse connectivity
    if err := hc.checkClickHouse(ctx); err != nil {
        status.Checks["clickhouse"] = fmt.Sprintf("unhealthy: %v", err)
        status.Status = "unhealthy"
    } else {
        status.Checks["clickhouse"] = "healthy"
    }

    // Check S3 connectivity (optional - may be expensive)
    // Could be a simple bucket list or head bucket operation
    // status.Checks["s3"] = "healthy"

    return status
}

func (hc *HealthChecker) checkClickHouse(ctx context.Context) error {
    // Simple ping query
    return hc.consumer.clickhouseClient.Exec(ctx, "SELECT 1")
}

// ServeHTTP implements http.Handler for health check endpoint
func (hc *HealthChecker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
    defer cancel()

    status := hc.Check(ctx)

    w.Header().Set("Content-Type", "application/json")
    if status.Status == "unhealthy" {
        w.WriteStatus(http.StatusServiceUnavailable)
    } else {
        w.WriteStatus(http.StatusOK)
    }

    json.NewEncoder(w).Encode(status)
}
```

Add health check endpoint to main:

```go
// In cmd/log-courier/main.go:

// Start health check server
healthAddr := ":8080" // Or from config
healthChecker := logcourier.NewHealthChecker(consumer)

go func() {
    http.Handle("/health", healthChecker)
    logger.Info("starting health check server", "address", healthAddr)
    if err := http.ListenAndServe(healthAddr, nil); err != nil {
        logger.Error("health check server failed", "error", err)
    }
}()
```

### Testing

Create `pkg/logcourier/health_test.go`:

```go
package logcourier_test

import (
    "context"
    "net/http"
    "net/http/httptest"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    "github.com/scality/log-courier/pkg/logcourier"
)

var _ = Describe("HealthChecker", func() {
    // Mock consumer for testing
    // Would need to create a test consumer or mock

    It("should return healthy status", func() {
        Skip("Requires consumer setup")
    })

    It("should return unhealthy when ClickHouse down", func() {
        Skip("Requires consumer setup")
    })
})
```

### Deliverables

- Enhanced logging throughout codebase
- Metrics instrumentation (if required)
- Metrics server
- Health check endpoint
- Tests for health checker

---

## Task 10: Multi-Node ClickHouse Test Infrastructure

### Objective
Upgrade test infrastructure from single-node ClickHouse to realistic multi-node cluster with sharding and replication.

### Background
Phase 1 testing uses simplified single-node ClickHouse setup for speed and simplicity. This task enhances testing to match production deployment architecture found in Federation (`roles/run-s3-analytics-clickhouse`).

### Study Phase

1. Review production ClickHouse setup in `/home/dimitrios/scality/Federation/roles/run-s3-analytics-clickhouse`:
   - `files/3.access_logging/*.sql` - Production schema (ReplicatedMergeTree, Distributed tables)
   - `templates/clickhouse.xml.j2` - Cluster configuration (shards, replicas, ZooKeeper)
   - Understanding of `main_cluster` configuration

2. Research docker-compose patterns for ClickHouse clusters:
   - Multi-node ClickHouse setup
   - ZooKeeper coordination
   - Network configuration between nodes

### Implementation Steps

#### 1. Create Multi-Node docker-compose Configuration

Create `docker-compose-clickhouse-cluster.yml`:

```yaml
version: '3.8'

services:
  zookeeper:
    image: zookeeper:3.8
    container_name: log-courier-test-zk
    ports:
      - "2181:2181"
    environment:
      ZOO_MY_ID: 1
      ZOO_SERVERS: server.1=0.0.0.0:2888:3888;2181

  clickhouse-01:
    image: docker.io/clickhouse/clickhouse-server:24.3.2.23-alpine
    container_name: log-courier-test-ch-01
    depends_on:
      - zookeeper
    ports:
      - "9001:9000"
      - "8124:8123"
    volumes:
      - ./test-configs/clickhouse-01:/etc/clickhouse-server/config.d
    environment:
      CLICKHOUSE_DB: logs

  clickhouse-02:
    image: docker.io/clickhouse/clickhouse-server:24.3.2.23-alpine
    container_name: log-courier-test-ch-02
    depends_on:
      - zookeeper
    ports:
      - "9002:9000"
      - "8125:8123"
    volumes:
      - ./test-configs/clickhouse-02:/etc/clickhouse-server/config.d
    environment:
      CLICKHOUSE_DB: logs

  clickhouse-03:
    image: docker.io/clickhouse/clickhouse-server:24.3.2.23-alpine
    container_name: log-courier-test-ch-03
    depends_on:
      - zookeeper
    ports:
      - "9003:9000"
      - "8126:8123"
    volumes:
      - ./test-configs/clickhouse-03:/etc/clickhouse-server/config.d
    environment:
      CLICKHOUSE_DB: logs

  clickhouse-04:
    image: docker.io/clickhouse/clickhouse-server:24.3.2.23-alpine
    container_name: log-courier-test-ch-04
    depends_on:
      - zookeeper
    ports:
      - "9004:9000"
      - "8127:8123"
    volumes:
      - ./test-configs/clickhouse-04:/etc/clickhouse-server/config.d
    environment:
      CLICKHOUSE_DB: logs

networks:
  default:
    name: log-courier-test-network
```

#### 2. Create ClickHouse Cluster Configuration Files

Create configuration files for each node defining the cluster topology:

`test-configs/clickhouse-01/cluster.xml`:
```xml
<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <replica>
                    <host>clickhouse-01</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse-02</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>clickhouse-03</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse-04</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
    </remote_servers>
    <zookeeper>
        <node>
            <host>zookeeper</host>
            <port>2181</port>
        </node>
    </zookeeper>
    <macros>
        <shard>01</shard>
        <replica>01</replica>
    </macros>
</clickhouse>
```

(Similar configs for other nodes with appropriate shard/replica macros)

#### 3. Update Schema Creation for Production Schema

Modify `pkg/testutil/clickhouse.go` to support both single-node and cluster schemas:

```go
// CreateSchema creates schema appropriate for cluster mode
func CreateSchema(ctx context.Context, client *Client, clusterMode bool) error {
    if clusterMode {
        return createClusterSchema(ctx, client)
    }
    return createSingleNodeSchema(ctx, client)
}

func createClusterSchema(ctx context.Context, client *Client) error {
    // Create access_logs_ingest (Null engine)
    // Create access_logs (ReplicatedMergeTree) on each node
    // Create access_logs_federated (Distributed)
    // Create materialized view
    // Matches Federation production schema
}

func createSingleNodeSchema(ctx context.Context, client *Client) error {
    // Existing simplified schema
}
```

#### 4. Add Integration Tests for Cluster Behavior

Create `pkg/clickhouse/cluster_test.go`:

```go
var _ = Describe("ClickHouse Cluster", func() {
    // Test that writes distribute across shards
    // Test that replicas synchronize
    // Test failover scenarios
    // Test distributed queries work correctly
})
```

#### 5. Update CI Configuration

Add cluster testing to CI with environment variable to toggle:
- `TEST_CLICKHOUSE_CLUSTER=true` - Run cluster tests
- Default: single-node (faster for most tests)

### Testing Strategy

**Default (Fast):** Single-node for unit/integration tests
**Optional (Thorough):** Cluster mode for comprehensive testing

```bash
# Fast tests (default)
make test

# Cluster tests
TEST_CLICKHOUSE_CLUSTER=true make test
```

### Deliverables

- Multi-node docker-compose configuration
- ClickHouse cluster config files
- Updated testutil with cluster schema support
- Cluster-specific integration tests
- CI configuration for optional cluster testing
- Documentation on running cluster tests locally

### Out of Scope

This task does NOT change log-courier application code - only test infrastructure. The application continues to work with both single-node and clustered ClickHouse.

---

## Task 11: Phase 2 - Work Partitioning (Future)

This task is **out of scope for Phase 1** and will be implemented in a future phase.

### Objectives (Future)

1. Study and decide on deployment mechanism:
   - Ballot with limited candidate pools
   - Alternative coordination approaches (e.g., provider/dispatcher pattern)
   - Evaluate ZooKeeper connection constraints

2. Implement work partitioning:
   - Modify batch finder query to filter by raft session
   - Add configuration: instance ID, total instances
   - Implement: `raft_session_id % ${totalInstances} = ${instanceID}`

3. Implement instance coordination:
   - Integration with chosen deployment mechanism
   - Leader election / work assignment
   - Failover handling

4. Testing:
   - Test work partitioning across multiple instances
   - Test failover scenarios
   - Verify no duplicate processing

### Prerequisites

- Phase 1 complete and deployed
- Performance evaluation of single-instance consumer
- Decision on deployment mechanism
- Understanding of production deployment constraints

---

## Summary

### Phase 1 Deliverables

**Core Functionality:**
- ✅ Single-instance consumer service
- ✅ Work discovery from ClickHouse
- ✅ Log fetching and object building
- ✅ S3 upload with retry
- ✅ Offset tracking
- ✅ Full end-to-end pipeline

**Infrastructure:**
- ✅ Complete project setup
- ✅ Build tooling (Makefile)
- ✅ CI/CD (GitHub Actions)
- ✅ Docker support
- ✅ Linting and formatting
- ✅ Comprehensive testing

**Observability:**
- ✅ Structured logging (slog)
- ✅ Metrics (optional)
- ✅ Health checks

### Key Decision Points

Throughout implementation, pause and discuss with Dimitrios:

1. **Task 1 completion:** How to split infrastructure into PRs
2. **Task 5:** Log object format and key naming convention
3. **Task 6:** S3 SDK selection and configuration

### Testing Strategy

- Unit tests for all components
- Integration tests with real ClickHouse
- Integration tests with MinIO/S3
- End-to-end tests
- Every PR includes tests

### Environment Requirements

**Development:**
- Go 1.24.2
- ClickHouse (Docker or remote)
- MinIO or S3-compatible storage (Docker or remote)

**CI:**
- GitHub Actions
- ClickHouse service
- MinIO service
- Codecov integration

### Next Steps

1. Implement Task 1 (Project Infrastructure)
2. Discuss PR split for Task 1
3. Continue with Tasks 2-9 sequentially
4. Each task = 1 PR (generally)
5. Phase 2 (work partitioning) deferred

---

## References

- **ARCHITECTURE.md**: Full architecture specification
- **PROJECT_UNDERSTANDING.md**: Project context and decisions
- **metadata-migration**: Reference project for team conventions
  - Location: `/home/dimitrios/scality/metadata-migration`
  - Consult throughout implementation

---

**Document End**
