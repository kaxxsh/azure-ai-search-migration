"""
Azure AI Search Backup Service

A modular backup solution for Azure AI Search indexes with support for:
- Time-windowed pagination to bypass 100K document limits
- Incremental and full backups
- Progress tracking with Azure Table Storage
- Concurrent window processing for better performance
"""

from .config import BackupConfig, normalize_connection_string
from .audit_tracker import AuditTracker
from .backup_engine import BackupEngine
from .logger import setup_logger, logger
from .constants import (
    DEFAULT_TIMESTAMP_FIELD,
    DEFAULT_KEY_FIELD,
    MAX_BATCH_SIZE_MB,
    DEFAULT_BATCH_SIZE,
    DEFAULT_WINDOW_SIZE_DAYS,
    DEFAULT_MAX_DOCS_PER_WINDOW,
    DEFAULT_CONCURRENT_WINDOWS,
    DEFAULT_AUDIT_TABLE_NAME
)
from .utils import (
    format_datetime_for_odata,
    parse_datetime_from_iso,
    safe_close_async_client,
    build_incremental_filter,
    build_time_window_filter,
    build_null_timestamp_filter,
    is_null_timestamp_window,
    get_null_timestamp_window
)

# Sub-modules are available but not exported by default
# Users can import them explicitly if needed:
# from backup_service.index_operations import verify_timestamp_field
# from backup_service.window_manager import generate_time_windows
# from backup_service.document_fetcher import fetch_batch_split_by_timestamp_ranges
# from backup_service.upload_manager import upload_with_retry

__version__ = "1.0.0"

__all__ = [
    # Main classes
    'BackupConfig',
    'AuditTracker',
    'BackupEngine',
    # Logging
    'setup_logger',
    'logger',
    # Utilities
    'normalize_connection_string',
    'format_datetime_for_odata',
    'parse_datetime_from_iso',
    'safe_close_async_client',
    'build_incremental_filter',
    'build_time_window_filter',
    'build_null_timestamp_filter',
    'is_null_timestamp_window',
    'get_null_timestamp_window',
    # Constants
    'DEFAULT_TIMESTAMP_FIELD',
    'DEFAULT_KEY_FIELD',
    'MAX_BATCH_SIZE_MB',
    'DEFAULT_BATCH_SIZE',
    'DEFAULT_WINDOW_SIZE_DAYS',
    'DEFAULT_MAX_DOCS_PER_WINDOW',
    'DEFAULT_CONCURRENT_WINDOWS',
    'DEFAULT_AUDIT_TABLE_NAME',
]
