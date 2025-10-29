# Log-Courier Project Understanding

**Last Updated:** 2025-10-22

## Project Overview
Building the "Bucket Log Processor" (BLP) consumer service, referred to as `log-courier`. This is a Go service that reads S3 access log records from ClickHouse, builds log objects, and writes them to destination S3 buckets.

## Architecture Context
- **Position in system**: Consumer at the end of the logging pipeline
- **Upstream**: ClickHouse (contains log records inserted by Fluent Bit from CloudServer log files)
- **Downstream**: S3 destination buckets (via external S3 API)
- **Related components NOT in scope**: CloudServer (log producer), Fluent Bit (log shipper)

## Scope

### In Scope
1. **Consumer service implementation (log-courier)**
   - Written in Go
   - Reads from ClickHouse
   - Builds log objects
   - Writes to S3 buckets via external S3 API
   - Handles work sharding across multiple instances
   - Two-phase operation:
     - Work discovery: Query ClickHouse for batches ready for processing
     - Log fetch and processing: Fetch logs, build log object, write to S3, commit offset

2. **Testing infrastructure**
   - ClickHouse deployment for CI and local testing
   - Scripts/tooling to create ClickHouse schema
   - Scripts/tooling to insert test data
   - Scripts/tooling for cleanup/test isolation

### Out of Scope
- CloudServer changes (log production)
- Fluent Bit configuration (log shipping)
- ClickHouse schema creation/management in production environments
- Features deferred to later phases:
  - ACLs
  - Custom logs
  - Log object key format options
  - Consumer idempotence (no duplicate logs in case of consumer crash)
  - Scale consumers up/down without downtime

## Key Requirements from Architecture

### Core Algorithm
1. **Work Discovery Phase** (at configured interval)
   - Query ClickHouse to identify batches ready for processing
   - Select buckets owned by this BLP instance
   - Use offsets table to compute unprocessed log counts and timestamps per bucket
   - Select buckets ready based on:
     - Log count threshold (configurable)
     - Time threshold (configurable)
   - Output: List of work orders `(Bucket, min_ts, max_ts)`

2. **Log Fetch and Processing Phase**
   - For each work order: fetch logs in time window
   - Parse logs, build log object
   - Write to destination bucket
   - Commit offset by inserting into offsets table

### Work Sharding
- Multiple consumer instances deployed
- Each instance processes distinct, non-overlapping subset of raft sessions
- Default: number of instances = number of raft sessions (1:1 mapping)
- Sharding based on: `raft_session_id % totalInstances = instanceID`

### ClickHouse Schema (for reference/testing)
Tables needed:
- `logs.logs_ingest_local` (Null engine, doesn't store data)
- `logs.logs_local` (ReplicatedMergeTree)
- `logs.logs` (Distributed table)
- `logs.offsets_local` (ReplicatedReplacingMergeTree)
- `logs.offsets` (Distributed table)

### Performance Targets (from architecture)
- Target: ~52k ops/s (from Core42 ring calculations)
- ClickHouse ingestion capacity: ~234k rows/sec (distributed, 2 shards)
- Log retention: 3-day TTL in ClickHouse

## Phased Delivery Approach

### Phase 1 (First Version)
- **Single instance** of log-courier
- Processes ALL buckets (no raft session filtering/partitioning)
- Simpler deployment - no coordination mechanism needed
- Focus: Get core algorithm working end-to-end
- Work discovery query simplified: no `raft_session_id % totalInstances = instanceID` filter
- **Full functionality**:
  - Read log records from ClickHouse
  - Build log objects
  - Write log objects to destination S3 buckets
  - Commit offsets to ClickHouse

### Phase 2 (Later Version)
- **Multiple instances** with work partitioning
- Implement sharding by raft_session_id
- **Prerequisite**: Study and decide on deployment mechanism (Ballot vs alternatives)
- Full work discovery query with raft session filtering

## Configuration Management

Following the team's established pattern from metadata-migration:

**Framework**: Viper + spf13/pflag

**Configuration Sources** (precedence order):
1. Command-line flags (highest)
2. Environment variables
3. YAML configuration file
4. Default values (lowest)

**Pattern Details**:
- Centralized `ConfigSpec` map defining all configuration items
- Dotted naming convention: `"component.subsystem.setting"`
- Environment variables: `LOG_COURIER_<UPPERCASE_PATH>` (e.g., `LOG_COURIER_CLICKHOUSE_URL`)
- Optional YAML file via `--config-file` flag or `LOG_COURIER_CONFIG_FILE` env var
- Component-specific prefixes with validation of required fields
- Type-safe access methods: `GetString()`, `GetInt()`, `GetBool()`, `GetStringSlice()`

**Reusable Code**:
- Can copy `pkg/util/config.go` from metadata-migration as-is
- Create log-courier-specific `configspec.go` defining our config items

## Codebase Status
Starting from scratch. Repository exists at `/home/dimitrios/scality/log-courier` with:
- Git repository initialized
- LICENSE file
- README.md (minimal)
- ARCHITECTURE.md (design document)
- PROJECT_UNDERSTANDING.md (this file)

No Go code exists yet. Implementation plan will include full project setup.

## Decisions to Make During Implementation

### S3 SDK/Library Selection
**Context**: Log-courier needs to write log objects to destination S3 buckets via external S3 API

**Decision needed**: Which Go S3 SDK/library to use

**Approach**: Implementation plan should include:
- Research and evaluate alternatives (likely AWS SDK for Go v2, MinIO Go client, others)
- Consider factors: team familiarity, features needed, maintenance, compatibility with existing infrastructure
- Present options with pros/cons
- Make decision before implementing S3 write functionality

**REMINDER**: Bring this up when we reach the implementation planning for:
- Creating log objects (format, structure)
- Writing to S3 (SDK selection)
- S3 configuration (credentials, endpoints, etc.)

## Reference Projects

### metadata-migration
Location: `/home/dimitrios/scality/metadata-migration`

**Purpose**: Reference project to learn team conventions for Go projects

**Key learnings extracted so far**:
- Configuration management pattern (Viper + pflag)
- Project structure (`cmd/`, `pkg/`, `scripts/`)
- Build tooling (Makefile conventions)
- Testing approach (Ginkgo + Gomega for Go tests)

**Action**: Continue consulting this codebase throughout implementation to learn:
- Code style and formatting conventions
- Error handling patterns
- Logging patterns (appears to use slog)
- Testing patterns and test structure
- CLI structure (uses Cobra)
- Go module organization
- Any other team-specific conventions

## Topics to Discuss

### Topic #1: Project Infrastructure & Tooling (Study from metadata-migration)

The following elements need to be added to log-courier, based on metadata-migration patterns:

**Build & Development Tooling:**
- `Makefile` - Build targets (all, clean, test, test-coverage, lint, fmt, coverage-report)
- `go.mod` / `go.sum` - Go module definition
- `.gitignore` - Ignore patterns for build artifacts, coverage files, etc.
- `.gitattributes` - Git attributes configuration

**Code Quality:**
- `.golangci.yml` - Linter configuration (golangci-lint)
- Formatting standards (via `go fmt`)

**GitHub Workflows (`.github/workflows/`):**
- `tests.yaml` - CI testing workflow
- `build-image.yaml` - Docker image build workflow
- `release.yaml` - Release workflow

**Dependency Management:**
- `.github/dependabot.yaml` - Dependabot configuration for dependency updates

**Testing & Coverage:**
- `codecov.yml` - Codecov configuration for coverage reporting
- Testing structure (Ginkgo + Gomega patterns)

**Container Support:**
- `Dockerfile` (likely in `images/` directory structure)
- `.dockerignore` - Docker build ignore patterns

**Documentation:**
- Enhanced `README.md` with:
  - Project overview
  - Components description
  - Build instructions
  - Testing instructions
  - Running instructions
  - Prerequisites
- `CLAUDE.md` - AI assistant context/guidance

**Additional considerations:**
- License verification (already have LICENSE file)
- Go version specification (metadata-migration uses 1.24.2)
- Project structure conventions (`cmd/`, `pkg/` directories)
- `scripts/` directory - Add if/when we have utility scripts (e.g., for test setup, ClickHouse schema management)

**Implementation Plan Actions:**
- Add study tasks at beginning of plan to examine each of these elements in detail
- Add discussion/decision points where team preferences may differ
- Include setup tasks for each infrastructure component

**Confirmed scope:**
All 9 infrastructure elements from the first list are in scope for log-courier.

### Topic #2: Implementation Task Breakdown

**Agreed task breakdown:**

1. **Project Infrastructure & CI/CD Setup** (combined)
   - Go module initialization
   - Project structure (cmd/, pkg/ directories)
   - Build tooling (Makefile)
   - Linting and formatting configuration
   - Git configuration files
   - Testing framework setup
   - GitHub Actions workflows (tests, build, release)
   - Dependabot configuration
   - Codecov configuration
   - Docker support
   - Enhanced README and CLAUDE.md

2. **Configuration Management**
   - Copy/adapt util/config.go from metadata-migration
   - Define ConfigSpec for log-courier
   - Add configuration loading and validation

3. **ClickHouse Integration**
   - ClickHouse client setup
   - Schema creation tooling (for testing)
   - Test data insertion utilities
   - Connection management and error handling

4. **Work Discovery**
   - Implement work discovery query (Phase 1: no raft session filtering)
   - Bucket offset tracking
   - Threshold-based batch selection (count + time thresholds)

5. **Log Processing**
   - Implement log fetch query
   - Log record parsing
   - Log object building (need to discuss format)

6. **S3 Integration**
   - S3 SDK selection and evaluation (decision point - remind about scope questions)
   - S3 client configuration
   - Log object upload implementation
   - Error handling and retries

7. **Offset Management**
   - Implement offset commit to ClickHouse
   - Offset table interactions
   - Consistency guarantees

8. **Core Consumer Loop**
   - Main service loop (work discovery + processing phases)
   - Interval-based scheduling
   - Graceful shutdown handling
   - Signal handling (SIGINT, SIGTERM)

9. **Observability**
   - Logging setup (slog, following team patterns)
   - Metrics instrumentation (Prometheus, if needed)
   - Health checks

10. **Phase 2: Work Partitioning** (separate/later)
    - Multi-instance coordination study
    - Raft session filtering in queries
    - Instance configuration
    - Deployment mechanism decision

**Testing approach:**
- Tests built incrementally with each task
- Every PR includes code + tests with good coverage
- Can defer some testing when dependencies are blocking
- No separate testing phase at the end

**PR organization:**
- General pattern: 1 PR per task
- Each PR focuses on a single aspect
- Balance: not trivially small, not too large to review
- Exception: combine tasks if changes are very small
- Task 1 (Infrastructure) will be discussed and split into PRs after implementation
  - Likely too large for a single PR
  - Will determine split points based on implementation
