# httpcachefs — Smart HTTP Parquet Reader

A high-performance Python library for reading Parquet files over HTTP with intelligent row group pruning, SQL query execution, and multi-level caching. A cross-platform alternative to DuckDB's `cache_httpfs` extension, supporting Linux, macOS, and Windows.

## Benchmark

Real-world test: **425 MB** Parquet file, **33.5M total rows**, **2,502 rows** returned.

| Metric         | Without Cache | Cache Build | Cache Hit          |
| -------------- | ------------- | ----------- | ------------------ |
| Execution Time | 4.3262 s      | 0.0241 s    | 0.0041 s           |
| Rows Returned  | 2,502         | 2,502       | 2,502              |
| Speedup        | 1.0x          | 179.8x      | **1,067.2x** |

> With expired TTL but unchanged ETag: `1.5863 s` (2.73x). Measured by `test_performance.py`.

## Features

- **Row Group Pruning** — auto-detects partition columns from SQL WHERE clauses, skips non-matching row groups via min/max statistics, with seamless fallback for complex queries
- **Multi-Level Caching** — TTL-based SQL result cache, ETag content validation, per-query result caching, and HTTP session/header caching
- **SQL Query Execution** — full DuckDB integration with column pushdown, WHERE clause filter extraction, and configurable result caching
- **HTTP Range Requests (RFC 7233)** — partial file downloads, connection pooling, exponential backoff retry, automatic ETag and Content-Length detection
- **Parquet Optimization** — row group statistics extraction, min/max filtering, column selection pushdown, atomic cache operations

## Quick Start

### Basic Usage

```python
from httpcachefs.smart_parquet import SmartParquetReader

with SmartParquetReader("https://example.com/file.parquet") as reader:
    result = reader.execute_sql("""
        SELECT * FROM {url}
        WHERE key = 'VALUE' AND score > 100
    """)
    df = result.to_pandas()
    print(df)
```

### Explicit Configuration

```python
from httpcachefs.smart_parquet import SmartParquetReader
from pathlib import Path

reader = SmartParquetReader(
    url="https://example.com/file.parquet",
    partition_col="key",         # Optional: explicit partition column
    cache_dir=Path("./cache"),   # Where to store cached data
    cache_ttl=3600,              # Cache validity in seconds
    debug=True                   # Enable detailed logging
)

result = reader.query("VALUE")
df = result.to_pandas()
reader.close()
```

### Repeated Queries with Caching

```python
with SmartParquetReader(url, cache_ttl=600) as reader:
    result1 = reader.execute_sql("SELECT * FROM {url} WHERE id = 1", cache_result=True)
    result2 = reader.execute_sql("SELECT * FROM {url} WHERE id = 2", cache_result=True)
    # Subsequent identical queries return instantly from cache
```

## Configuration

### SmartParquetReader Parameters

```python
SmartParquetReader(
    url: str,                                        # HTTP(S) URL to Parquet file
    partition_col: Optional[str] = None,             # Partition column (auto-detected if None)
    cache_dir: Union[str, Path] = "./smart_cache",   # Cache directory
    cache_ttl: int = 600,                            # Cache TTL in seconds
    debug: bool = False                              # Enable debug logging
)
```

### HTTPClientConfig Parameters

```python
from httpcachefs.http_client import HTTPClientConfig, HTTPRangeClient

config = HTTPClientConfig(
    retries=3,             # Number of retry attempts
    backoff_factor=0.3,    # Exponential backoff multiplier
    timeout=(5, 30),       # (connect_timeout, read_timeout)
    pool_connections=10,   # Connection pool size
    pool_maxsize=10,       # Max connections per host
    headers={}             # Custom HTTP headers
)
client = HTTPRangeClient(url, config=config)
```

## API Reference

### `initialize() -> None`

Pre-loads Parquet metadata and initializes caches. Called automatically by query methods. Detects partition column and row group statistics.

### `execute_sql(sql: str, cache_result: bool = False) -> pa.Table`

Executes a SQL query with automatic optimizations: partition column detection, column pushdown, and optional result caching. Returns a PyArrow Table.

```python
result = reader.execute_sql("""
    SELECT key, date, score FROM {url}
    WHERE key IN ('VALUE_1', 'VALUE_2') AND score > 100
""", cache_result=True)
```

### `query(value: Any, columns: Optional[List[str]] = None) -> pa.Table`

Fetches all rows for a specific partition value with optional column selection. More efficient than `execute_sql()` for single-value lookups. Returns a PyArrow Table.

```python
data   = reader.query('VALUE')
data = reader.query('VALUE', columns=['date', 'score'])
```

### `close() -> None`

Closes the HTTP connection and releases resources. Called automatically when using a context manager.

### Properties

- **`row_groups: List[RowGroupMeta]`** — metadata for each row group, including min/max statistics for the partition column
- **`partition_col: str`** — partition column used for row group filtering; auto-detected or set at initialization

## Optimization Strategies

1. **Row Group Pruning** — fetches only row groups matching filter conditions; skips unnecessary downloads, saving bandwidth and time
2. **Column Pushdown** — reads only columns in the SQL SELECT clause; reduces memory usage automatically
3. **Query Caching** — caches SQL results with TTL; eliminates repeated query execution with instant return
4. **Metadata Caching** — caches Parquet footer and statistics; ETag invalidation avoids re-downloading metadata

## Caching

### Cache Layers (fastest → slowest)

1. **SQL Result Cache** — entire SQL query results, TTL-invalidated, independent of row group changes
2. **Query Cache** — single partition value results, per-value caching, filtered row group data
3. **Metadata Cache** — Parquet footer and row group statistics, ETag-invalidated
4. **Session Cache** — HTTP connection pooling, header and file size caching

### Cache Directory Structure

```
~/.cache/httpcachefs/
└── <url_hash>/
    ├── footer.bin              # Parquet metadata
    ├── metadata.json           # Row group statistics
    ├── etag.txt                # Last known ETag
    ├── timestamp.txt           # Cache creation time
    ├── queries/                # Query result cache
    │   ├── <query_hash>.arrow
    │   └── <query_hash>.timestamp
    └── sql_results/            # SQL result cache
        ├── <sql_hash>.arrow
        └── <sql_hash>.timestamp
```

## Architecture

### Core Components

| Component              | File                        | Responsibility                                                                |
| ---------------------- | --------------------------- | ----------------------------------------------------------------------------- |
| `HTTPRangeClient`    | `http_client/client.py`   | HTTP Range requests, session pooling, retry logic, metadata caching           |
| `RemoteFileObject`   | `http_client/io.py`       | File-like interface for remote URLs, buffered reading, HTTP Range integration |
| `SmartParquetReader` | `smart_parquet/reader.py` | High-level Parquet API, row group pruning, SQL execution, multi-level caching |
| `FusedFileObject`    | `smart_parquet/io.py`     | Hybrid memory/network reading, seamless fallback to full download             |

### Design Patterns

- **Reader Pattern** — file-like interfaces for transparent remote access
- **Caching Strategy** — multi-level with TTL and ETag validation
- **Helper Methods** — common operations extracted for maintainability
- **Type Safety** — comprehensive type hints throughout
- **Error Handling** — proper exception chaining and validation

## Error Handling

```python
from httpcachefs.http_client.exceptions import (
    HTTPRangeError,
    ResourceNotFoundError,
    RangeNotSupportedError,
    FileSizeError
)

try:
    with SmartParquetReader(url) as reader:
        result = reader.execute_sql(sql)
except ResourceNotFoundError:
    print("File not found on server")
except RangeNotSupportedError:
    print("Server doesn't support HTTP Range requests")
except FileSizeError:
    print("Could not determine file size")
except HTTPRangeError as e:
    print(f"HTTP error: {e}")
```

## Performance Tips

1. **Set `cache_ttl` appropriately** — longer for stable data sources, shorter for frequently updated files
2. **Use `cache_result=True`** for repeated identical queries — eliminates re-execution entirely
3. **Prefer `query()` for single lookups** — more efficient row group pruning than `execute_sql()`

## Troubleshooting

| Symptom           | Solution                                                                                                                         |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| High memory usage | Use column selection:`columns=['col1', 'col2']`; increase `cache_ttl` to reuse fetched data; process results in batches      |
| Slow queries      | Ensure partition column is in WHERE clause; enable `debug=True` to inspect network; verify server supports HTTP Range requests |
| Cache issues      | Clear with `rm -rf ~/.cache/httpcachefs/`; temporarily disable with `cache_ttl=0`; check available disk space                |
