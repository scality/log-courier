# CLAUDE.md

This file provides guidance to Claude Code when working with log-courier.

## Project Overview

Log-courier is the consumer at the end of the S3 Server Access Logging pipeline:

```
CloudServer → log file → Fluent Bit → ClickHouse → Log Courier → S3 destination bucket
```

It reads log records from ClickHouse, builds log objects in the AWS S3 Server Access Log format, and writes them to destination buckets via the S3 API.

**Delivery guarantees:** best-effort, at-least-once. Logs may be lost (upstream failures) or duplicated (consumer crash between upload and offset commit).

**Scope:** Log-courier only. CloudServer (log producer), Fluent Bit (log shipper), and ClickHouse schema management in production are out of scope.

## Build and Development Commands

### Building
```bash
make            # Build all binaries
make all-debug  # Build with race detector and debug symbols
make clean      # Clean build artifacts
```

### Testing
```bash
make test       # Run unit tests with coverage
make test-e2e   # Run end-to-end tests
```

### Code Quality
```bash
make fmt        # Format code
make lint       # Run linters
```

## Architecture

### Package Structure
- `cmd/log-courier/`: Main application entry point
- `cmd/ensureServiceUser/`: IAM service user management utility
- `pkg/logcourier/`: Core consumer logic
- `pkg/clickhouse/`: ClickHouse client and schema
- `pkg/s3/`: S3 client and uploader
- `pkg/ensureserviceuser/`: IAM user management logic
- `pkg/util/`: Shared utilities (config, logging, metrics)
- `pkg/testutil/`: Testing utilities
- `test/e2e/`: End-to-end integration tests
- `deployment/`: Deployment configuration for e2e tests

### Configuration
- Uses **Viper** for configuration management
- Config via: command-line flags, YAML file, environment variables
- See `pkg/logcourier/configspec.go` for all config options

### ClickHouse Tables

Log-courier interacts with these tables in the `logs` database:

| Table | Engine | Purpose |
|-------|--------|---------|
| `access_logs` | ReplicatedMergeTree | Local log storage, partitioned by day, TTL-based retention |
| `access_logs_federated` | Distributed (sharded by `raftSessionID`) | Distributed view across shards |
| `offsets` | ReplicatedReplacingMergeTree | Local offset storage, deduplicates by `(bucketName, raftSessionID)` |
| `offsets_federated` | Distributed (sharded by `raftSessionID`) | Distributed view for offset writes |

### Core Components
- Consumer loop: adaptive work discovery + log processing phases
  - Uses shorter interval when work is found, longer when idle
  - Accounts for processing time in scheduling
  - See `configspec.go` for interval defaults
- ClickHouse client: read logs, commit offsets
- S3 uploader: write log objects to destination buckets

### Testing
- Uses **Ginkgo** and **Gomega** for all Go tests
- Integration tests require ClickHouse and S3-compatible storage

## Design Decisions (Do Not Change Without Discussion)

### Offset Table Read/Write Pattern
The code intentionally uses different tables for reading and writing offsets:
- **Writes** go to `offsets_federated` (distributed table) via `offset.go`
- **Reads** come from `offsets` (local table) via `batchfinder.go`

This is **intentional design**, not a bug. Do not "fix" this mismatch - it has been investigated and the current pattern is correct for our distributed ClickHouse setup.

### Access Logs Table Read Pattern
Similarly, different components read access logs from different tables:
- **BatchFinder** reads from `access_logs` (local table) for work discovery
- **LogFetcher** reads from `access_logs_federated` (distributed table) for log retrieval

## System Knowledge

### Triple Composite Offset
Progress tracking uses a composite offset of three fields: `(insertedAt, startTime, reqID)` compared in lexicographic order. This composite appears in three places that must stay in sync:
- **BatchFinder** (`batchfinder.go`): WHERE clause to find unprocessed logs
- **LogFetcher** (`logfetch.go`): WHERE clause to fetch logs after last offset
- **Offset commit** (`offset.go`): fields written to `offsets_federated`

Any change to the offset comparison logic in one place requires the same change in all three.

### OffsetBuffer and Cycle Flush
The OffsetBuffer (`offset_buffer.go`) sits between batch processing and ClickHouse. Offsets are buffered in memory and flushed in batches to reduce ClickHouse write load.

Five flush triggers:
- **Time threshold**: periodic timer
- **Count threshold**: buffer reaches size limit
- **Cycle boundary**: mandatory flush after each discovery cycle
- **Shutdown**: drain on graceful termination
- **Explicit**: manual call

The **cycle boundary flush is critical for correctness**. Without it, the next cycle's BatchFinder would query stale offsets from ClickHouse and re-discover batches that were already processed.

### Dual Context Pattern
The processor uses two independent contexts:
- **ctx**: lifecycle context, cancelled on shutdown signal. Checked at cycle boundaries to stop dispatching.
- **workCtx**: created from `context.Background()`, used for in-flight operations (uploads, offset commits). Allows ongoing work to complete after shutdown signal.

Using the wrong context would either prevent graceful shutdown (if `workCtx` is used for dispatch control) or abort in-flight uploads (if `ctx` is used for work operations).

### Error Classification
Errors are classified as permanent or transient (`IsPermanentError()` in `processor.go`):
- **Permanent** (never retried): `NoSuchBucket`, `InvalidAccessKeyId`, `AccessDenied` — detected via string matching on the error message because the AWS SDK v2 doesn't properly implement error unwrapping for these types.
- **Transient** (retried with exponential backoff): everything else.

### At-Least-Once Delivery
Offsets are committed only after successful S3 upload. If the upload succeeds but the offset commit fails (or the process crashes between the two), the same logs will be re-fetched and re-uploaded on the next cycle. This means:
- Logs are never lost (at-least-once)
- Duplicates are possible across log objects on failure

## Code Review Protocol

Read [`docs/code-review-protocol.md`](docs/code-review-protocol.md) and follow it when reviewing:
- Changes to core processing loop (Discovery, Processor, BatchFinder)
- Changes to state management (offsets, metrics, configuration)
- Performance optimizations (batching, caching, async operations)
- Any change affecting multiple components
- Any change where "timing matters"

You may skip the protocol for trivial bug fixes, purely additive features with no shared state, test-only changes, and documentation changes.

## Development Notes

### Running Locally
- Requires ClickHouse instance
- Requires S3-compatible storage

### Code Style
- Follow existing Go conventions
- Run `make fmt` before committing
- Ensure `make lint` passes