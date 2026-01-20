# Log-Courier

## Overview

Log-courier reads S3 access log records from ClickHouse, builds log objects,
and writes them to destination S3 buckets.

## Building

```bash
make
```

## Testing

### Unit Tests

```bash
make test
```

For coverage:
```bash
make test-coverage
make coverage-report
```

### End-to-End Tests

E2E tests use [Ginkgo](https://onsi.github.io/ginkgo/) for test execution. The Ginkgo CLI is required and will be automatically installed if not present.

To install Ginkgo CLI manually:
```bash
go install github.com/onsi/ginkgo/v2/ginkgo@latest
```

#### Local Infrastructure Setup

E2E tests run against a local ClickHouse and S3 environment.
- Use [Scality Workbench](https://github.com/scality/workbench) to set up the required infrastructure.
- Use the configuration in `env/default/values.yaml`.

#### Running end-to-end tests

```bash
make test-e2e
```

To run a specific test by name:
```bash
make test-e2e-focus TEST="logs basic CRUD operations"
```

The `TEST` variable accepts a regex pattern that matches test names. For example:
- `TEST="CRUD"` - runs all tests with "CRUD" in the name
- `TEST="bucket operations"` - runs tests matching "bucket operations"

## License

See [LICENSE](LICENSE)
