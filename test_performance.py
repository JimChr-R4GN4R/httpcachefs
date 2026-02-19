import time
from httpcachefs.smart_parquet import SmartParquetReader

URL = "https://example.com/file.parquet" # Make sure it supports HTTP Range Requests

QUERY = """
SELECT key, date, score
FROM {url}
WHERE key IN ('VALUE_1', 'VALUE_2')
AND score > 100
ORDER BY date DESC
"""

def run_test(cache_ttl: int, label: str):
    start = time.time()

    with SmartParquetReader(
        URL,
        partition_col="key", # optional
        cache_ttl=cache_ttl,
        debug=False,
    ) as reader:
        result = reader.execute_sql(QUERY, cache_result=True)
        df = result.to_pandas()

    elapsed = time.time() - start

    print(f"\n--- {label} ---")
    print(f"Rows returned:   {len(df):,}")
    print(f"Execution time:  {elapsed:.4f} sec")
    return elapsed


print("\nHTTPcachefs Cache Benchmark")
print("=" * 50)

t_no_cache = run_test(
    cache_ttl=0,
    label="WITHOUT CACHE (fresh execution)"
)

t_cache_build = run_test(
    cache_ttl=3600,
    label="WITH CACHE (first run - builds cache)"
)

t_cache_hit = run_test(
    cache_ttl=3600,
    label="WITH CACHE (second run - cache hit)"
)

print("\nSummary")
print("=" * 50)
print(f"No cache:        {t_no_cache:.4f} sec")
print(f"Cache build:     {t_cache_build:.4f} sec")
print(f"Cache hit:       {t_cache_hit:.4f} sec")
print(f"Speedup (hit):   {t_no_cache / t_cache_hit:.2f}x faster")
