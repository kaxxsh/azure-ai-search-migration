#!/usr/bin/env python3
"""
Azure AI Search Migration Tool - Main Entry Point

A professional migration solution for Azure AI Search indexes using time-windowed pagination.
Designed for seamless data transfer between Azure AI Search instances.
"""

import asyncio
import argparse
import time
from dotenv import load_dotenv
from backup_service import BackupConfig, BackupEngine, logger
from backup_service.utils import print_banner

# Load environment variables from .env file (only works when running locally)
# In Docker, environment variables are passed via --env-file or -e flags
load_dotenv(override=False)

async def main():
    """Main entry point for the migration tool"""
    print_banner()
    # Record the start time for calculating total migration duration
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser(
        description='Azure AI Search Migration Tool - Time-Windowed Keyset Pagination',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
                Environment Variables (can be overridden by command-line arguments):
                AZURE_AI_SEARCH_BACKUP_SOURCE_SERVICE_ENDPOINT
                AZURE_AI_SEARCH_BACKUP_SOURCE_API_KEY
                AZURE_AI_SEARCH_BACKUP_SOURCE_INDEX_NAME
                AZURE_AI_SEARCH_BACKUP_DESTINATION_SERVICE_ENDPOINT
                AZURE_AI_SEARCH_BACKUP_DESTINATION_API_KEY
                AZURE_AI_SEARCH_BACKUP_DESTINATION_INDEX_NAME
                AZURE_AI_SEARCH_BACKUP_TIMESTAMP_FIELD (default: created_at)
                AZURE_AI_SEARCH_BACKUP_WINDOW_SIZE_DAYS (default: 30)
                AZURE_AI_SEARCH_BACKUP_MAX_DOCS_PER_WINDOW (default: 1000)
                AZURE_AI_SEARCH_BACKUP_CONCURRENT_WINDOWS (default: 5)
                AZURE_AI_SEARCH_BACKUP_BATCH_SIZE (default: 100)

                Audit Storage Configuration (Optional - for incremental backups):
                Option 1 (Recommended):
                    AZURE_AI_SEARCH_BACKUP_STORAGE_CONNECTION_STRING
                Option 2 (Alternative):
                    AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_NAME
                    AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_KEY
                    AZURE_AI_SEARCH_BACKUP_STORAGE_TABLE_ENDPOINT (optional)

                AZURE_AI_SEARCH_BACKUP_AUDIT_TABLE_NAME (default: AzureSearchBackupAudit)

                Note: Without storage configuration, backup will run as ONE-TIME only
                    (no incremental backups or resume capability)
                """
    )

    # Source configuration
    parser.add_argument('--source-endpoint', type=str, default=None,
                        help='Source Azure AI Search endpoint')
    parser.add_argument('--source-key', type=str, default=None,
                        help='Source Azure AI Search API key')
    parser.add_argument('--source-index', type=str, default=None,
                        help='Source index name')

    # Destination configuration
    parser.add_argument('--destination-endpoint', type=str, default=None,
                        help='Destination Azure AI Search endpoint')
    parser.add_argument('--destination-key', type=str, default=None,
                        help='Destination Azure AI Search API key')
    parser.add_argument('--destination-index', type=str, default=None,
                        help='Destination index name (defaults to source index name)')

    # Performance tuning
    parser.add_argument('--window-days', type=int, default=None,
                        help='Time window size in days (default: 30)')
    parser.add_argument('--max-docs-per-window', type=int, default=None,
                        help='Maximum documents per window (default: 1000)')
    parser.add_argument('--concurrent-windows', type=int, default=None,
                        help='Number of windows to process concurrently (default: 5)')

    # Field configuration
    parser.add_argument('--timestamp-field', type=str, default=None,
                        help='Name of the timestamp field for pagination (default: created_at)')
    parser.add_argument('--skip-schema-copy', action='store_true',
                        help='Skip copying the index schema to destination')

    # Migration validation flags
    parser.add_argument('--pre-migration', action='store_true',
                        help='Run pre-migration validation: validate connections and schema (create if missing)')
    parser.add_argument('--post-migration', action='store_true',
                        help='Run post-migration validation: validate document counts and data consistency')

    # Storage configuration for audit tracking
    parser.add_argument('--storage-connection-string', type=str, default=None,
                        help='Azure Storage connection string for audit tracking')
    parser.add_argument('--storage-account-name', type=str, default=None,
                        help='Azure Storage account name (alternative to connection string)')
    parser.add_argument('--storage-account-key', type=str, default=None,
                        help='Azure Storage account key (used with account name)')
    parser.add_argument('--storage-table-endpoint', type=str, default=None,
                        help='Azure Storage Table endpoint (optional)')
    parser.add_argument('--audit-table-name', type=str, default=None,
                        help='Audit table name (default: AzureSearchBackupAudit)')

    args = parser.parse_args()

    try:
        logger.info("=" * 70)
        logger.info("Starting Azure AI Search Migration")
        logger.info("=" * 70)

        # Create configuration from environment and command-line arguments
        config = BackupConfig.from_env(args)

        # Log configuration
        config.log_configuration(logger)

        async with BackupEngine(
            config,
        ) as engine:
            # Handle pre-migration validation
            if args.pre_migration:
                if not await engine.run_pre_migration_validation():
                    logger.error("❌ Pre-migration validation failed. Please fix the issues before proceeding.")
                    return
                # Exit after pre-migration validation
                return

            # Handle post-migration validation
            if args.post_migration:
                if not await engine.run_post_migration_validation():
                    logger.warning("⚠️  Post-migration validation completed with warnings.")
                    return
                # Exit after post-migration validation
                return

            # Normal migration flow (when no validation flags are set)
            logger.info("")
            logger.info("Validating connections...")

            # Validate connections
            if not await engine.validate_connection():
                logger.error("❌ Connection validation failed. Please check your credentials and endpoints.")
                return

            logger.info("✓ Connections validated successfully")
            logger.info("")

            # Run the backup (skip schema copy if requested)
            await engine.run_backup(copy_schema=not args.skip_schema_copy)

        logger.info("")
        logger.info("=" * 70)
        logger.info("✓ Migration completed successfully!")
        logger.info("=" * 70)

    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        return
    except KeyboardInterrupt:
        logger.warning("\n⚠ Migration interrupted by user.")
        logger.info("Migration can be safely resumed - completed windows are tracked in audit logs.")
    except Exception as e:
        logger.error(f"❌ Fatal error during migration: {e}", exc_info=True)
        raise

    elapsed = time.perf_counter() - start_time
    hours, remainder = divmod(int(elapsed), 3600)
    minutes, seconds = divmod(remainder, 60)

    logger.info("")
    if hours > 0:
        logger.info(f"⏱  Total migration time: {hours}h {minutes}m {seconds}s")
    elif minutes > 0:
        logger.info(f"⏱  Total migration time: {minutes}m {seconds}s")
    else:
        logger.info(f"⏱  Total migration time: {elapsed:.2f}s")

    logger.info(f"📋 Detailed logs available at: ai_search_migration.log")


if __name__ == "__main__":
    asyncio.run(main())
