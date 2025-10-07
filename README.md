# DuckDB QuackStore Extension

Speed up your data queries by caching remote files locally. The QuackStore extension uses **block-based caching** to automatically store frequently accessed file portions in a local cache, dramatically reducing load times for repeated queries on the same data.

## What does it do?

When you query remote files (like CSV files from the web), DuckDB normally downloads them every time. With QuackStore, the first query downloads and caches the file locally. Subsequent queries use the cached version, making them much faster.

**Key Benefits:**
- ✅ **Block-based caching**: Only caches the parts of files you actually access (blocks)
- ✅ **Persistent cache**: Cache survives database restarts and is stored on disk
- ✅ **Data integrity**: Automatic corruption detection and recovery - corrupt blocks are automatically evicted and re-fetched from source  
- ✅ **Seamless integration**: Works with existing file systems without breaking compatibility
- ✅ **Smart caching**: LRU eviction automatically manages cache size; frequently accessed blocks stay cached longer
- ✅ **Perfect for**: Scenarios where data files are accessed repeatedly or where network I/O is a bottleneck

## Quick Start

1. **Enable the extension** (if not already loaded):
   ```sql
   INSTALL quackstore;
   LOAD quackstore;
   ```

2. **Configure cache location**:
   ```sql
   SET GLOBAL quackstore_cache_path = '/tmp/my_duckdb_cache.bin';
   SET GLOBAL quackstore_cache_enabled = true;
   ```

3. **Use cached file access** by adding `quackstore://` prefix:
   ```sql
   -- Slow: Downloads every time
   SELECT * FROM 'https://example.com/data.csv';
   
   -- Fast: Cached after first download
   SELECT * FROM 'quackstore://https://example.com/data.csv';
   ```

## Configuration

### Required Settings

```sql
-- Set where to store the cache file (GLOBAL only - cache is shared across all sessions)
SET GLOBAL quackstore_cache_path = '/path/to/cache.bin';

-- Enable caching (GLOBAL only)
SET GLOBAL quackstore_cache_enabled = true;
```

**Note:** Cache path, size, and enabled settings are global-only because the cache is shared across all database sessions. Currently, it's not possible to have multiple per-session caches.

### Optional Settings

```sql
-- Set maximum cache size (GLOBAL only - default: 2GB)
SET GLOBAL quackstore_cache_size = 1073741824; -- 1GB
```

**Note:** Cache path, size, and enabled settings are global-only because the cache is shared across all database sessions. Currently, it's not possible to have multiple per-session caches.

```sql
-- Control cache behavior for mutable vs immutable data (can be per-session or global)
SET quackstore_data_mutable = true;  -- Per-session setting for mutable data, default setting
SET quackstore_data_mutable = false; -- Per-session setting for immutable data (better caching)
SET GLOBAL quackstore_data_mutable = true;  -- Global setting
SET GLOBAL quackstore_data_mutable = false; -- Global setting
```

## Usage Examples

### Remote Files
```sql
-- Cache a CSV file from GitHub
SELECT * FROM 'quackstore://https://raw.githubusercontent.com/owner/repo/main/data.csv';

-- Cache a single Parquet file from S3
SELECT * FROM parquet_scan('quackstore://s3://example_bucket/data/file.parquet');

-- Cache whole Iceberg catalog from S3
SELECT * FROM iceberg_scan('quackstore://s3://example_bucket/iceberg/catalog');

-- Cache any web resource
SELECT content FROM read_text('quackstore://https://example.com/file.txt');
```

### When to Use QuackStore

✅ **Good for:**
- Large files you query repeatedly
- Remote files (HTTP/HTTPS URLs, S3, etc.)
- Slow network connections
- Both static and changing data (use appropriate `quackstore_data_mutable` setting)

❌ **Don't use for:**
- Local files (already fast)
- One-time queries on small files

### Data Mutability Settings

The `quackstore_data_mutable` parameter controls how aggressively the cache validates data freshness:

**For immutable data** (recommended for most analytics workloads):
```sql
SET quackstore_data_mutable = false;
```
- Files are assumed not to change once cached
- Maximum performance (no validation checks)
- Perfect for: Historical data, archived files, static datasets

**For mutable data** (default behavior):
```sql
SET quackstore_data_mutable = true;
```
- Cache validates file freshness on access
- Performs lightweight metadata checks (modification time and file size) - these are small requests that don't download the whole file
- Use for: Live datasets, frequently updated files

**Session vs Global scope:**
- `SET GLOBAL` - affects all database connections
- `SET` (session) - affects only current connection
- Useful for mixed workloads where different queries need different behaviors

## Cache Management

```sql
-- Clear all cached data
CALL quackstore_clear_cache();

-- Remove specific files from cache (must include quackstore:// prefix)
CALL quackstore_evict_files(['quackstore://https://example.com/data.csv']);

-- Remove multiple files from cache
CALL quackstore_evict_files([
    'quackstore://https://example.com/file1.csv',
    'quackstore://https://example.com/file2.parquet',
    'quackstore://s3://bucket/data/file3.json'
]);

-- Check current settings
SELECT current_setting('quackstore_cache_enabled');
SELECT current_setting('quackstore_cache_path');
SELECT current_setting('quackstore_cache_size');
SELECT current_setting('quackstore_data_mutable');
```

### Cache Management Functions

- **`quackstore_clear_cache()`**: Removes all cached data and resets the cache
  - Safe to call multiple times or when cache doesn't exist
  - Works even when caching is disabled

- **`quackstore_evict_files(['quackstore://file1', 'quackstore://file2', ...])`**: Removes specific files from the cache
  - **Important**: File paths must include the `quackstore://` prefix, exactly as used in queries
  - Takes a list of file paths with the prefix: `['quackstore://https://example.com/data.csv']`
  - Useful for removing outdated files without clearing the entire cache
  - Safe to call with non-existent files (no error)
  - Validates input parameters and provides clear error messages for invalid arguments

## Performance Tips

- **Cache Location**: Store the cache on fast storage (SSD) for best performance
- **Cache Size**: Set it large enough for your working dataset, but remember it's block-based - you don't need space for entire files
- **Access Patterns**: Sequential reads within 1MB boundaries are most efficient
- **Block Alignment**: Works best with files larger than 1MB (the internal block size)

## How It Works

The extension uses **block-based caching** with 1MB blocks. This means:

- **Partial file caching**: Only the portions of files you actually read are cached
- **Efficient memory usage**: Large files don't need to be fully downloaded if you only need part of them
- **Block-level eviction**: Individual blocks are evicted independently using LRU (least recently used)
- **Whole files can span multiple blocks**: A large file may be cached across many blocks, but some blocks might be evicted while others remain

When you access a cached file:
1. **First access**: Downloads only the needed blocks and stores them in cache
2. **Subsequent access**: Reads cached blocks from local storage (much faster)
3. **Cache pressure**: Individual blocks are evicted as needed, not entire files

## Troubleshooting

**Cache not working?**
- Check that `quackstore_cache_enabled = true`
- Ensure `quackstore_cache_path` is set to a writable location
- Make sure you're using the `quackstore://` prefix

**Cache file growing too large?**
- Adjust `quackstore_cache_size` setting
- Use `quackstore_clear_cache()` to reset

**Still slow?**
- Cache works best for repeated queries on the same files
- First query will always be slow (downloading + caching)
- Very small files may not benefit much

**Function errors?**
- `quackstore_evict_files` requires file paths with `quackstore://` prefix: use `['quackstore://https://example.com/file.csv']`
- `quackstore_evict_files` requires a list of strings: use proper list syntax with quotes
- Empty lists need explicit casting: use `[]::VARCHAR[]` instead of `[]`
- NULL arguments are not allowed: ensure all parameters are valid
- For subqueries, extract to variable first: `SET files = (SELECT list(...)); CALL quackstore_evict_files(getvariable('files'));`

## Building and Installation

For most users, the extension can be installed directly:
```sql
INSTALL quackstore;
LOAD quackstore;
```

For building from source, see the build instructions in the original documentation.

## License

See LICENSE.txt

