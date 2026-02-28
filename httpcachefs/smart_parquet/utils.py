import hashlib
import logging
import re
import time
from pathlib import Path
from typing import Union, List, Optional
import sqlglot
from sqlglot import exp

logger = logging.getLogger("SmartParquet.Utils")

def get_url_hash(url: str) -> str:
    """Generate SHA256 hash of a URL for use as a directory name.
    
    Creates a unique, stable identifier for caching data associated with a URL.
    
    Args:
        url: The HTTP(S) URL to hash
    Returns:
        Hex string of SHA256 hash
    """
    return hashlib.sha256(url.encode()).hexdigest()

def get_query_hash(key: str, columns: Optional[List[str]]) -> str:
    """Generate unique hash for a query configuration.
    
    Combines key and column list to create a unique cache key.
    
    Args:
        key: The partition column value being queried
        columns: List of columns to fetch (or None)
    Returns:
        Hex string of SHA256 hash
    """
    key = f"{key}|{str(columns)}"
    return hashlib.sha256(key.encode()).hexdigest()

def extract_partition_column(sql: str) -> Optional[str]:
    """Extract partition column from WHERE clause for SmartParquetReader optimization.
    
    This function parses SQL queries to identify the column used for filtering,
    allowing SmartParquetReader to skip unnecessary row groups during initialization.
    
    Examples:
        'WHERE uuid = value' -> 'uuid'
        'WHERE related_keys = value' -> 'related_keys'
        'WHERE id IN (...)' -> 'id'
        'WHERE table.id = 1 AND status = 2' -> 'id'

    Args:
        sql: SQL query string
        
    Returns:
        First detected partition column if found, None otherwise
    """
    try:
        parsed = sqlglot.parse_one(sql)
        where = parsed.find(exp.Where)

        if not where:
            return None

        # Look for equality or IN expressions
        for condition in where.find_all((exp.EQ, exp.In)):
            column = None

            # EQ: column = value
            if isinstance(condition, exp.EQ):
                if isinstance(condition.left, exp.Column):
                    column = condition.left

            # IN: column IN (...)
            elif isinstance(condition, exp.In):
                if isinstance(condition.this, exp.Column):
                    column = condition.this

            if column:
                return column.name  # returns unqualified column name

        return None

    except Exception:
        return None

def atomic_write(path: Path, content: Union[str, bytes], max_retries: int = 3):
    """Write content to a file atomically using temp file and rename.
    
    Handles Windows file locking issues with retry logic and exponential backoff.
    
    Args:
        path: Target file path
        content: Content to write (str or bytes)
        max_retries: Maximum number of retry attempts for file locking issues
    Raises:
        Exception: If write or rename fails after all retries (temp file cleanup attempted)
    """
    temp_path = path.with_suffix(".tmp")
    mode = "wb" if isinstance(content, bytes) else "w"
    
    last_error = None
    for attempt in range(max_retries):
        try:
            # Ensure parent directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Clean up stale temp file if it exists
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    # Temp file is locked, continue anyway
                    pass
            
            # Write to temp file
            with open(temp_path, mode) as f:
                f.write(content)
            
            # Atomic rename with Windows file locking handling
            temp_path.replace(path)
            return  # Success
            
        except (OSError, PermissionError, FileExistsError) as e:
            last_error = e
            # Check if this is a file locking error
            is_lock_error = (
                isinstance(e, OSError) and (
                    "being used by another process" in str(e) or
                    "Permission denied" in str(e)
                )
            )
            
            if is_lock_error and attempt < max_retries - 1:
                # Exponential backoff: 10ms, 20ms, 40ms
                wait_time = (2 ** attempt) * 0.01
                logger.debug(f"File lock conflict writing {path}, retrying in {wait_time*1000:.0f}ms (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                # Not a lock error or last attempt
                break
        except Exception as e:
            # Other unexpected errors
            last_error = e
            break
    
    # All retries failed
    logger.error(f"Failed to write atomic file {path}: {last_error}")
    if temp_path.exists(): 
        try:
            temp_path.unlink()
        except OSError:
            pass  # Couldn't clean up, but log the real error
    
    raise last_error if last_error else RuntimeError(f"Failed to write {path}")

def extract_sql_url(sql: str) -> Optional[str]:
    """Extract HTTP/HTTPS URL from SQL FROM clause.
    
    Handles various formatting:
    - FROM 'url'
    - FROM  'url' (multiple spaces)
    - FROM\\n'url' (newlines)
    - FROM\\n        'url' (newlines with indentation)
    
    Args:
        sql: SQL query string
        
    Returns:
        URL if found, None otherwise
    """
    # Match FROM followed by optional whitespace (including newlines) and a quote
    match = re.search(r"FROM\s+'([^']+)'", sql, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1)
    return None
