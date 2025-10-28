# Log-Courier

## Overview

Log-courier reads S3 access log records from ClickHouse, builds log objects,
and writes them to destination S3 buckets.

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

## License

See [LICENSE](LICENSE)
