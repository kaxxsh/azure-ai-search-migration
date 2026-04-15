"""
Constants and default values for Azure AI Search backup.
"""

# Default field name for timestamp-based pagination
DEFAULT_TIMESTAMP_FIELD = "created_at"

# Default field name for document key
DEFAULT_KEY_FIELD = "id"

# Maximum batch size in MB (Azure AI Search limit)
MAX_BATCH_SIZE_MB = 16

# Default retry settings
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_BASE = 2
DEFAULT_MAX_CONSECUTIVE_ERRORS = 3

# Performance defaults (can be overridden via config)
DEFAULT_BATCH_SIZE = 100
DEFAULT_WINDOW_SIZE_DAYS = 30
DEFAULT_MAX_DOCS_PER_WINDOW = 1000
DEFAULT_CONCURRENT_WINDOWS = 5

# Timeout and delay settings
DEFAULT_INDEX_CREATION_WAIT_SECONDS = 5
DEFAULT_ERROR_RETRY_BASE_SECONDS = 5

# Size estimation defaults
DEFAULT_FALLBACK_DOC_SIZE_MB = 0.01  # 10KB per document

# Audit tracking defaults
DEFAULT_AUDIT_TABLE_NAME = "AzureSearchBackupAudit"
