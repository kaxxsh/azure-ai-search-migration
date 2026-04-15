"""
Core backup engine for Azure AI Search.

Orchestrates the complete backup process with window-based pagination.
"""

import asyncio
from datetime import datetime
from typing import Optional, Dict, Tuple, List, Any
from azure.search.documents.aio import SearchClient
from azure.search.documents.indexes.aio import SearchIndexClient

from .config import BackupConfig
from .audit_tracker import AuditTracker
from .logger import logger
from .constants import (
    DEFAULT_KEY_FIELD,
    MAX_BATCH_SIZE_MB,
    DEFAULT_MAX_CONSECUTIVE_ERRORS,
    DEFAULT_ERROR_RETRY_BASE_SECONDS
)
from .index_operations import verify_timestamp_field, copy_index_definition, get_documents_count
from .window_manager import generate_time_windows
from .document_fetcher import fetch_batch_split_by_timestamp_ranges
from .upload_manager import upload_with_retry
from .utils import format_datetime_for_odata, is_null_timestamp_window


class BackupEngine:
    """Core backup engine for Azure AI Search"""

    def __init__(self, config: BackupConfig, key_field: str = DEFAULT_KEY_FIELD):
        self.config = config
        self.timestamp_field = config.timestamp_field
        self.key_field = key_field

        self.source_client: Optional[SearchClient] = None
        self.destination_client: Optional[SearchClient] = None
        self.source_index_client: Optional[SearchIndexClient] = None
        self.destination_index_client: Optional[SearchIndexClient] = None
        self.audit_tracker: Optional[AuditTracker] = None

    async def __aenter__(self):
        """Initialize search clients"""
        self.source_client = SearchClient(
            endpoint=self.config.source_endpoint,
            index_name=self.config.source_index_name,
            credential=self.config.source_credential
        )

        self.destination_client = SearchClient(
            endpoint=self.config.destination_endpoint,
            index_name=self.config.destination_index_name,
            credential=self.config.destination_credential
        )

        self.source_index_client = SearchIndexClient(
            endpoint=self.config.source_endpoint,
            credential=self.config.source_credential
        )

        self.destination_index_client = SearchIndexClient(
            endpoint=self.config.destination_endpoint,
            credential=self.config.destination_credential
        )

        # Initialize audit tracker if storage credentials are provided
        if self.config.has_storage_config():
            self.audit_tracker = AuditTracker(
                source_endpoint=self.config.source_endpoint,
                destination_endpoint=self.config.destination_endpoint,
                connection_string=self.config.storage_connection_string,
                table_name=self.config.audit_table_name,
                account_name=self.config.storage_account_name,
                account_key=self.config.storage_account_key,
                table_endpoint=self.config.storage_table_endpoint
            )
            await self.audit_tracker.__aenter__()
        else:
            logger.warning("")
            logger.warning("⚠️  WARNING: No Azure Storage configuration provided")
            logger.warning("   This will be a ONE-TIME backup without audit tracking")
            logger.warning("   ")
            logger.warning("   Without storage configuration:")
            logger.warning("   • Cannot track last backup timestamp")
            logger.warning("   • Cannot resume from where it left off")
            logger.warning("   • Cannot perform incremental backups")
            logger.warning("   ")
            logger.warning("   To enable audit tracking, provide either:")
            logger.warning("   1. AZURE_AI_SEARCH_BACKUP_STORAGE_CONNECTION_STRING")
            logger.warning("   OR")
            logger.warning("   2. AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_NAME + AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_KEY")
            logger.warning("")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources"""
        if self.source_client:
            await self.source_client.close()
        if self.destination_client:
            await self.destination_client.close()
        if self.source_index_client:
            await self.source_index_client.close()
        if self.destination_index_client:
            await self.destination_index_client.close()
        if self.audit_tracker:
            await self.audit_tracker.__aexit__(exc_type, exc_val, exc_tb)

    async def validate_connection(self) -> bool:
        """Validate connection to both source and destination"""
        try:
            await self.source_client.get_document_count()
            logger.info("✓ Source index connection established")

            try:
                await self.destination_client.get_document_count()
                logger.info("✓ Destination index connection established")
            except Exception as dest_error:
                # If destination index doesn't exist, it will be created during schema copy
                if "was not found" in str(dest_error):
                    logger.info(f"✓ Destination index '{self.config.destination_index_name}' will be created from source schema")
                else:
                    raise

            return True
        except Exception as e:
            logger.error(f"❌ Connection validation failed: {e}")
            return False

    async def run_pre_migration_validation(self) -> bool:
        """
        Run pre-migration validation checks:
        1. Validate connections to source and destination
        2. Validate schema exists (create if missing)

        Returns:
            True if all validations pass, False otherwise
        """
        try:
            logger.info("")
            logger.info("=" * 70)
            logger.info("🔍 PRE-MIGRATION VALIDATION")
            logger.info("=" * 70)
            logger.info("")

            # Step 1: Validate connections
            logger.info("Step 1: Validating connections...")
            if not await self.validate_connection():
                logger.error("❌ Connection validation failed")
                return False
            logger.info("✓ Connection validation passed")
            logger.info("")

            # Step 2: Validate schema (create if not present)
            logger.info("Step 2: Validating schema...")
            try:
                dest_index = await self.destination_index_client.get_index(
                    self.config.destination_index_name
                )
                logger.info(f"✓ Destination index '{self.config.destination_index_name}' exists")

                # Verify the schema structure
                source_index = await self.source_index_client.get_index(
                    self.config.source_index_name
                )
                logger.info(f"✓ Source index has {len(source_index.fields)} fields")
                logger.info(f"✓ Destination index has {len(dest_index.fields)} fields")

            except Exception as e:
                if "was not found" in str(e):
                    logger.info(f"⚠️  Destination index '{self.config.destination_index_name}' not found")
                    logger.info("🔧 Creating destination index from source schema...")

                    await copy_index_definition(
                        self.source_index_client,
                        self.destination_index_client,
                        self.config.source_index_name,
                        self.config.destination_index_name
                    )
                    logger.info("✓ Destination index created successfully")
                else:
                    logger.error(f"❌ Schema validation failed: {e}")
                    return False

            logger.info("")
            logger.info("=" * 70)
            logger.info("✅ PRE-MIGRATION VALIDATION PASSED")
            logger.info("=" * 70)
            logger.info("")
            return True

        except Exception as e:
            logger.error(f"❌ Pre-migration validation failed: {e}", exc_info=True)
            return False

    async def run_post_migration_validation(self) -> bool:
        """
        Run post-migration validation checks:
        1. Validate document counts match between source and destination
        2. Sample and compare data from both indexes

        Returns:
            True if all validations pass, False otherwise
        """
        try:
            logger.info("")
            logger.info("=" * 70)
            logger.info("🔍 POST-MIGRATION VALIDATION")
            logger.info("=" * 70)
            logger.info("")

            # Step 1: Validate document counts
            logger.info("Step 1: Validating document counts...")
            source_count = await get_documents_count(self.source_client)
            dest_count = await get_documents_count(self.destination_client)

            logger.info(f"   Source index: {source_count:,} documents")
            logger.info(f"   Destination index: {dest_count:,} documents")

            if source_count != dest_count:
                logger.warning(f"⚠️  Document count mismatch!")
                logger.warning(f"   Difference: {abs(source_count - dest_count):,} documents")
                logger.warning(f"   This may indicate an incomplete migration")
            else:
                logger.info("✓ Document counts match")
            logger.info("")

            # Step 2: Sample and validate data
            logger.info("Step 2: Validating data consistency (sampling 100 documents)...")

            # Initialize validation tracking variables
            matched_count = 0
            missing_docs = []
            mismatched_docs = []

            if source_count == 0:
                logger.info("✓ No documents to validate (source index is empty)")
            else:
                # Get key field
                self.key_field = await self.get_key_field()

                # Fetch sample documents from source
                sample_size = min(100, source_count)
                source_results = await self.source_client.search(
                    search_text="*",
                    top=sample_size
                )

                source_docs = []
                async for doc in source_results:
                    source_docs.append(doc)

                if not source_docs:
                    logger.warning("⚠️  No documents found in source index for sampling")
                else:
                    logger.info(f"   Sampled {len(source_docs)} documents from source")

                    # Check if same documents exist in destination
                    for source_doc in source_docs:
                        doc_id = source_doc.get(self.key_field)
                        try:
                            dest_doc = await self.destination_client.get_document(key=doc_id)
                            if dest_doc:
                                matched_count += 1

                                # Compare field values (sample check)
                                mismatched_fields = []
                                for field_name, source_value in source_doc.items():
                                    # Skip metadata fields added by Azure Search
                                    if field_name.startswith('@'):
                                        continue
                                    dest_value = dest_doc.get(field_name)
                                    if source_value != dest_value:
                                        mismatched_fields.append(field_name)

                                if mismatched_fields:
                                    logger.warning(f"   ⚠️  Document '{doc_id}' has mismatched fields: {', '.join(mismatched_fields[:3])}")
                                    mismatched_docs.append(doc_id)
                        except Exception:
                            missing_docs.append(doc_id)

                    logger.info(f"   Matched: {matched_count}/{len(source_docs)} documents")

                    if missing_docs:
                        logger.warning(f"⚠️  {len(missing_docs)} sampled documents not found in destination")
                        logger.warning(f"   Missing IDs (first 5): {missing_docs[:5]}")
                    else:
                        logger.info("✓ All sampled documents found in destination")

                    if mismatched_docs:
                        logger.warning(f"⚠️  {len(mismatched_docs)} documents have field mismatches")
                        logger.warning(f"   Mismatched IDs (first 5): {mismatched_docs[:5]}")

            logger.info("")

            # Final validation result
            validation_passed = (source_count == dest_count and len(missing_docs) == 0 and len(mismatched_docs) == 0)

            logger.info("=" * 70)
            if validation_passed:
                logger.info("✅ POST-MIGRATION VALIDATION PASSED")
            else:
                logger.info("⚠️  POST-MIGRATION VALIDATION COMPLETED WITH WARNINGS")
            logger.info("=" * 70)
            logger.info("")

            return validation_passed

        except Exception as e:
            logger.error(f"❌ Post-migration validation failed: {e}", exc_info=True)
            return False

    async def get_key_field(self) -> str:
        """
        Get the key field name from the index.

        Returns:
            Name of the key field

        Raises:
            ValueError: If no key field is found
        """
        index = await self.source_index_client.get_index(self.config.source_index_name)
        for field in index.fields:
            if field.key:
                logger.info(f"Using key field: '{field.name}' for document ID tracking")
                return field.name

        raise ValueError("No key field found in index")

    async def run_backup(self, copy_schema: bool = True) -> bool:
        """
        Execute the complete backup operation with incremental backup support.

        Args:
            copy_schema: Whether to copy index schema before backup

        Returns:
            True if successful
        """
        import time

        try:
            # Step 1: Verify timestamp field
            logger.info(f"Verifying timestamp field '{self.timestamp_field}'...")
            if not await verify_timestamp_field(
                self.source_index_client,
                self.timestamp_field,
                self.config.source_index_name
            ):
                logger.error("CRITICAL ERROR: Timestamp field must be sortable and filterable!")
                return False

            # Get key field
            self.key_field = await self.get_key_field()
            logger.info(f"Using timestamp field: '{self.timestamp_field}' for sorting and pagination")

            # Step 2: Copy index schema if requested
            if copy_schema:
                await copy_index_definition(
                    self.source_index_client,
                    self.destination_index_client,
                    self.config.source_index_name,
                    self.config.destination_index_name
                )

            # Step 3: Check for incremental backup
            last_backup_time = None
            is_full_backup = True

            if self.audit_tracker:
                last_backup_time = await self.audit_tracker.get_last_successful_backup_time(
                    self.config.source_index_name,
                    self.config.destination_index_name
                )
                is_full_backup = last_backup_time is None

            # Count documents and display backup type
            if is_full_backup:
                logger.info("")
                logger.info("📊 Migration Type: FULL INDEX MIGRATION")
                logger.info("   Using time-windowed keyset pagination for optimal performance")
                logger.info("")
                logger.info("⏳ Analyzing source index...")
                initial_count = await get_documents_count(self.source_client)
                logger.info(f"✓ Found {initial_count:,} documents to migrate")
            else:
                logger.info("")
                logger.info(f"📊 Migration Type: INCREMENTAL (changes since {last_backup_time})")
                logger.info("   Transferring only modified documents")
                logger.info("")
                timestamp_str = format_datetime_for_odata(last_backup_time)
                filter_query = f"{self.timestamp_field} gt {timestamp_str}"
                logger.info("⏳ Analyzing changes...")
                initial_count = await get_documents_count(self.source_client, filter_query)
                logger.info(f"✓ Found {initial_count:,} documents to migrate")

            logger.info(f"📋 Strategy: Time-windowed transfer with '{self.timestamp_field}' based pagination")

            if initial_count == 0:
                logger.info("")
                logger.info("✓ Index is up to date - no documents require migration")
                return True

            # Step 4: Generate time windows
            windows = await generate_time_windows(
                self.source_client,
                self.timestamp_field,
                last_backup_time,
                self.config.window_size_days,
                self.config.max_docs_per_window
            )

            if not windows:
                logger.info("✓ Index is up to date - no documents require migration")
                return True

            # Recalculate accurate total from windows (excludes empty windows)
            accurate_total_docs = sum(w[2] for w in windows)

            # Verify window coverage
            if accurate_total_docs != initial_count:
                logger.warning("")
                logger.warning(f"⚠️  NOTICE: Refined count ({accurate_total_docs:,}) differs from initial estimate ({initial_count:,})")
                logger.warning(f"   Difference: {initial_count - accurate_total_docs} document(s)")
                logger.warning(f"   This may indicate timestamp boundary variations")
                logger.warning(f"   Using refined count ({accurate_total_docs:,}) as authoritative")
                logger.warning("")

            # Step 5: Initialize audit tracking
            if self.audit_tracker:
                # Mark any stale 'In Progress' runs as cancelled before starting new run
                await self.audit_tracker.mark_stale_runs_as_cancelled(
                    self.config.source_index_name,
                    self.config.destination_index_name
                )

                run_id = await self.audit_tracker.create_backup_run(
                    total_documents=accurate_total_docs,
                    source_index=self.config.source_index_name,
                    destination_index=self.config.destination_index_name,
                    is_full_backup=is_full_backup,
                    last_backup_time=last_backup_time
                )
                logger.info(f"📝 Migration run ID: {run_id}")

            # Step 6: Process windows concurrently with progress tracking
            logger.info("")
            logger.info("=" * 70)
            logger.info(f"🚀 Starting migration: {accurate_total_docs:,} documents across {len(windows)} time windows")
            logger.info(f"⚡ Concurrent processing: {self.config.concurrent_windows} windows in parallel")
            logger.info("=" * 70)
            logger.info("")

            start_time = time.time()
            shared_progress = await self._process_windows_concurrently(windows, last_backup_time, accurate_total_docs)

            # Final progress update
            elapsed = time.time() - start_time
            docs_per_sec = shared_progress["documents_processed"] / elapsed if elapsed > 0 else 0
            logger.info("")
            logger.info("=" * 70)
            logger.info(f"📈 Migration Progress: 100% Complete")
            logger.info(f"   Documents transferred: {shared_progress['documents_processed']:,}/{accurate_total_docs:,}")
            logger.info(f"   Transfer rate: {docs_per_sec:.1f} docs/sec")
            logger.info(f"   Duration: {elapsed:.1f}s")
            logger.info("=" * 70)
            logger.info("")

            # Step 7: Mark backup as completed
            if self.audit_tracker:
                await self.audit_tracker.mark_completed(
                    documents_processed=shared_progress["documents_processed"],
                    batches_completed=shared_progress["batches_completed"],
                    last_doc_timestamp=shared_progress.get("last_timestamp"),
                    docs_added=shared_progress["documents_added"]
                )

            logger.info("✅ Migration completed successfully!")
            logger.info(f"   Total documents migrated: {shared_progress['documents_processed']:,}")
            logger.info(f"   Migration type: {'FULL' if is_full_backup else 'INCREMENTAL'}")
            return True

        except Exception as e:
            logger.error(f"❌ Migration failed: {e}", exc_info=True)
            if self.audit_tracker:
                try:
                    await self.audit_tracker.mark_failed(str(e))
                except Exception as audit_error:
                    logger.error(f"Failed to update audit tracker: {audit_error}")
            raise

    async def _process_windows_concurrently(
        self,
        windows: List[Tuple[datetime, datetime, int]],
        last_backup_time: Optional[datetime],
        total_docs: int
    ) -> Dict[str, Any]:
        """
        Process all windows concurrently using a queue and workers.

        Returns:
            Dictionary with progress statistics
        """
        import time

        # Create queue and shared progress tracking
        windows_queue = asyncio.Queue()
        progress_lock = asyncio.Lock()
        shared_progress = {
            "documents_processed": 0,
            "batches_completed": 0,
            "documents_added": 0,
            "documents_updated": 0,
            "windows_completed": 0,
            "windows_in_progress": 0,
            "last_timestamp": None,
            "total_docs": total_docs,
            "start_time": time.time(),
            "last_print_time": time.time()
        }

        # Populate queue with windows
        for i, window_info in enumerate(windows):
            await windows_queue.put((window_info, i))

        # Add sentinel values to stop workers
        num_concurrent_windows = min(self.config.concurrent_windows, len(windows))
        for _ in range(num_concurrent_windows):
            await windows_queue.put(None)

        # Create worker tasks
        workers = [
            asyncio.create_task(
                self._window_processor(
                    windows_queue,
                    progress_lock,
                    shared_progress,
                    len(windows),
                    last_backup_time
                )
            )
            for _ in range(num_concurrent_windows)
        ]

        # Wait for all workers to complete
        results = await asyncio.gather(*workers, return_exceptions=True)

        # Check if any worker raised an exception
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"❌ Worker thread {i+1} encountered an error:")
                logger.error(f"   {type(result).__name__}: {result}")
                raise result

        logger.info(f"✓ All workers completed: {shared_progress['documents_processed']:,} documents transferred in {shared_progress['batches_completed']} batches")

        return shared_progress

    async def _window_processor(
        self,
        windows_queue: asyncio.Queue,
        progress_lock: asyncio.Lock,
        shared_progress: Dict,
        total_windows: int,
        last_backup_time: Optional[datetime]
    ):
        """Worker that processes windows from the queue"""
        while True:
            try:
                item = await windows_queue.get()
                if item is None:
                    windows_queue.task_done()
                    break

                window_info, window_index = item

                # Mark window as in progress
                async with progress_lock:
                    shared_progress["windows_in_progress"] += 1

                # Process the entire window
                await self._process_window(
                    window_info,
                    window_index,
                    total_windows,
                    progress_lock,
                    shared_progress,
                    last_backup_time
                )

                windows_queue.task_done()

            except Exception as e:
                logger.error(f"Window processor encountered fatal error: {e}", exc_info=True)
                windows_queue.task_done()
                raise

    async def _process_window(
        self,
        window_info: Tuple[datetime, datetime, int],
        window_index: int,
        total_windows: int,
        progress_lock: asyncio.Lock,
        shared_progress: Dict,
        last_backup_time: Optional[datetime]
    ):
        """Process a single time window completely"""
        start_time, end_time, expected_docs = window_info
        window_id = f"W{window_index+1}/{total_windows}"

        # Check if this is the special null timestamp window
        is_null_window = is_null_timestamp_window(start_time, end_time)

        if is_null_window:
            logger.info(f"[{window_id}] 🔄 Processing NULL timestamps ({expected_docs:,} docs)")
        else:
            logger.info(f"[{window_id}] 🔄 Processing {start_time.date()} to {end_time.date()} ({expected_docs:,} docs)")

        window_docs_processed = 0
        cursor_value = None
        processed_ids = set()
        batch_count = 0
        consecutive_errors = 0
        max_consecutive_errors = DEFAULT_MAX_CONSECUTIVE_ERRORS

        try:
            while True:
                try:
                    # Fetch batch for this window
                    documents, new_cursor, new_processed_ids, has_more = await fetch_batch_split_by_timestamp_ranges(
                        self.source_client,
                        self.timestamp_field,
                        self.key_field,
                        self.config.batch_size,
                        cursor_value,
                        processed_ids,
                        (start_time, end_time),
                        last_backup_time
                    )

                    consecutive_errors = 0

                    if not documents:
                        break

                except Exception as fetch_error:
                    consecutive_errors += 1
                    logger.warning(f"[{window_id}] Fetch error ({consecutive_errors}/{max_consecutive_errors}): {fetch_error}")

                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"[{window_id}] Too many consecutive errors, failing window")
                        raise

                    await asyncio.sleep(DEFAULT_ERROR_RETRY_BASE_SECONDS * consecutive_errors)
                    continue

                # Upload documents
                await upload_with_retry(
                    self.destination_client,
                    documents,
                    max_batch_mb=MAX_BATCH_SIZE_MB
                )

                # Track last document timestamp
                last_timestamp = None
                if self.timestamp_field and documents:
                    last_doc = documents[-1]
                    if self.timestamp_field in last_doc:
                        last_timestamp = last_doc[self.timestamp_field]

                # Update progress
                batch_count += 1
                window_docs_processed += len(documents)

                async with progress_lock:
                    shared_progress["documents_processed"] += len(documents)
                    shared_progress["batches_completed"] += 1
                    shared_progress["documents_added"] += len(documents)

                    if last_timestamp:
                        shared_progress["last_timestamp"] = last_timestamp

                    # Update audit table after every batch
                    if self.audit_tracker:
                        try:
                            await self.audit_tracker.update_progress(
                                documents_processed=shared_progress["documents_processed"],
                                batches_completed=shared_progress["batches_completed"],
                                last_doc_timestamp=shared_progress.get("last_timestamp"),
                                docs_added=shared_progress["documents_added"]
                            )
                        except Exception as e:
                            logger.error(f"[{window_id}] Failed to update audit table: {e}")

                # Continue if there are more documents in this window
                if not has_more or not new_cursor:
                    break

                cursor_value = new_cursor
                processed_ids = new_processed_ids

            # Window completed
            async with progress_lock:
                shared_progress["windows_completed"] += 1
                shared_progress["windows_in_progress"] -= 1

            logger.info(f"[{window_id}] ✅ Complete - Migrated {window_docs_processed:,} docs in {batch_count} batches")

        except Exception as e:
            logger.error(f"[{window_id}] ❌ Error: {e}", exc_info=True)
            raise
