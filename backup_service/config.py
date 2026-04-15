import os
from dataclasses import dataclass
from typing import Optional
from azure.core.credentials import AzureKeyCredential
from .utils import normalize_connection_string
from .constants import DEFAULT_TIMESTAMP_FIELD,DEFAULT_WINDOW_SIZE_DAYS,DEFAULT_MAX_DOCS_PER_WINDOW,DEFAULT_CONCURRENT_WINDOWS,DEFAULT_BATCH_SIZE,DEFAULT_AUDIT_TABLE_NAME


@dataclass
class BackupConfig:
    """Configuration for Azure AI Search backup operation"""

    # Required fields (no defaults) must come first
    source_endpoint: str
    source_key: str
    source_index_name: str
    destination_endpoint: str
    destination_key: str
    destination_index_name: str

    # Optional fields with defaults
    source_credential: Optional[AzureKeyCredential] = None
    destination_credential: Optional[AzureKeyCredential] = None
    timestamp_field: str = DEFAULT_TIMESTAMP_FIELD
    window_size_days: int = DEFAULT_WINDOW_SIZE_DAYS
    max_docs_per_window: int = DEFAULT_MAX_DOCS_PER_WINDOW
    concurrent_windows: int = DEFAULT_CONCURRENT_WINDOWS
    batch_size: int = DEFAULT_BATCH_SIZE
    storage_connection_string: Optional[str] = None
    storage_account_name: Optional[str] = None
    storage_account_key: Optional[str] = None
    storage_table_endpoint: Optional[str] = None
    audit_table_name: str = DEFAULT_AUDIT_TABLE_NAME

    def __post_init__(self):
        """Initialize credentials after dataclass initialization"""
        if not self.source_credential:
            self.source_credential = AzureKeyCredential(self.source_key)
        if not self.destination_credential:
            self.destination_credential = AzureKeyCredential(self.destination_key)

    def has_storage_config(self) -> bool:
        """Check if any storage configuration is provided"""
        return bool(
            self.storage_connection_string or
            (self.storage_account_name and self.storage_account_key)
        )

    @classmethod
    def from_env(cls, args=None) -> 'BackupConfig':
        """
        Create configuration from environment variables and optional command-line arguments.

        Args:
            args: Parsed argparse.Namespace object with command-line arguments

        Returns:
            BackupConfig instance
        """
        # Get environment variables with defaults
        timestamp_field = os.getenv("AZURE_AI_SEARCH_BACKUP_TIMESTAMP_FIELD")
        window_size_days = int(os.getenv("AZURE_AI_SEARCH_BACKUP_WINDOW_SIZE_DAYS"))
        max_docs_per_window = int(os.getenv("AZURE_AI_SEARCH_BACKUP_MAX_DOCS_PER_WINDOW"))
        concurrent_windows = int(os.getenv("AZURE_AI_SEARCH_BACKUP_CONCURRENT_WINDOWS"))
        batch_size = int(os.getenv("AZURE_AI_SEARCH_BACKUP_BATCH_SIZE"))

        # Source configuration
        source_endpoint = os.getenv("AZURE_AI_SEARCH_BACKUP_SOURCE_SERVICE_ENDPOINT")
        source_key = os.getenv("AZURE_AI_SEARCH_BACKUP_SOURCE_API_KEY")
        source_index_name = os.getenv("AZURE_AI_SEARCH_BACKUP_SOURCE_INDEX_NAME")

        # Destination configuration
        destination_endpoint = os.getenv("AZURE_AI_SEARCH_BACKUP_DESTINATION_SERVICE_ENDPOINT")
        destination_key = os.getenv("AZURE_AI_SEARCH_BACKUP_DESTINATION_API_KEY")
        destination_index_name = os.getenv("AZURE_AI_SEARCH_BACKUP_DESTINATION_INDEX_NAME")

        # Audit storage configuration
        storage_connection_string = os.getenv("AZURE_AI_SEARCH_BACKUP_STORAGE_CONNECTION_STRING")
        storage_account_name = os.getenv("AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_NAME")
        storage_account_key = os.getenv("AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_KEY")
        storage_table_endpoint = os.getenv("AZURE_AI_SEARCH_BACKUP_STORAGE_TABLE_ENDPOINT")
        audit_table_name = os.getenv("AZURE_AI_SEARCH_BACKUP_AUDIT_TABLE_NAME")

        # Override with command-line arguments if provided
        if args:
            source_endpoint = args.source_endpoint or source_endpoint
            source_key = args.source_key or source_key
            source_index_name = args.source_index or source_index_name

            destination_endpoint = args.destination_endpoint or destination_endpoint
            destination_key = args.destination_key or destination_key
            destination_index_name = args.destination_index or destination_index_name

            if args.window_days is not None:
                window_size_days = args.window_days
            if args.max_docs_per_window is not None:
                max_docs_per_window = args.max_docs_per_window
            if args.concurrent_windows is not None:
                concurrent_windows = args.concurrent_windows
            if args.timestamp_field is not None:
                timestamp_field = args.timestamp_field

            # Storage configuration overrides
            if hasattr(args, 'storage_connection_string') and args.storage_connection_string:
                storage_connection_string = args.storage_connection_string
            if hasattr(args, 'storage_account_name') and args.storage_account_name:
                storage_account_name = args.storage_account_name
            if hasattr(args, 'storage_account_key') and args.storage_account_key:
                storage_account_key = args.storage_account_key
            if hasattr(args, 'storage_table_endpoint') and args.storage_table_endpoint:
                storage_table_endpoint = args.storage_table_endpoint
            if hasattr(args, 'audit_table_name') and args.audit_table_name:
                audit_table_name = args.audit_table_name

        # Default destination index to source index if not specified
        if not destination_index_name:
            destination_index_name = source_index_name

        # Validate required parameters
        if not source_endpoint:
            raise ValueError("Source endpoint is required! Provide via --source-endpoint or AZURE_AI_SEARCH_BACKUP_SOURCE_SERVICE_ENDPOINT")
        if not source_key:
            raise ValueError("Source API key is required! Provide via --source-key or AZURE_AI_SEARCH_BACKUP_SOURCE_API_KEY")
        if not source_index_name:
            raise ValueError("Source index name is required! Provide via --source-index or AZURE_AI_SEARCH_BACKUP_SOURCE_INDEX_NAME")
        if not destination_endpoint:
            raise ValueError("Destination endpoint is required! Provide via --destination-endpoint or AZURE_AI_SEARCH_BACKUP_DESTINATION_SERVICE_ENDPOINT")
        if not destination_key:
            raise ValueError("Destination API key is required! Provide via --destination-key or AZURE_AI_SEARCH_BACKUP_DESTINATION_API_KEY")

        # Normalize connection string once if provided
        if storage_connection_string:
            storage_connection_string = normalize_connection_string(storage_connection_string)

        return cls(
            source_endpoint=source_endpoint,
            source_key=source_key,
            source_index_name=source_index_name,
            destination_endpoint=destination_endpoint,
            destination_key=destination_key,
            destination_index_name=destination_index_name,
            window_size_days=window_size_days,
            max_docs_per_window=max_docs_per_window,
            concurrent_windows=concurrent_windows,
            batch_size=batch_size,
            timestamp_field=timestamp_field,
            storage_connection_string=storage_connection_string,
            storage_account_name=storage_account_name,
            storage_account_key=storage_account_key,
            storage_table_endpoint=storage_table_endpoint,
            audit_table_name=audit_table_name
        )

    def log_configuration(self, logger):
        """Log the current configuration"""
        logger.info("")
        logger.info("📋 Migration Configuration")
        logger.info("─" * 70)
        logger.info("")
        logger.info("🔵 Source Index:")
        logger.info(f"   Endpoint: {self.source_endpoint}")
        logger.info(f"   Index:    {self.source_index_name}")
        logger.info("")
        logger.info("🟢 Destination Index:")
        logger.info(f"   Endpoint: {self.destination_endpoint}")
        logger.info(f"   Index:    {self.destination_index_name}")
        logger.info("")
        logger.info("⚙️  Performance Settings:")
        logger.info(f"   Batch size:         {self.batch_size:,} documents")
        logger.info(f"   Time window:        {self.window_size_days} days")
        logger.info(f"   Max docs/window:    {self.max_docs_per_window:,}")
        logger.info(f"   Parallel windows:   {self.concurrent_windows}")
        logger.info("")
        logger.info("💡 Strategy: Time-windowed keyset pagination")
        logger.info("   Bypasses 100K result limit via temporal partitioning")
        logger.info("")
        logger.info(f"📄 Log file: ai_search_migration.log")
        logger.info("─" * 70)
