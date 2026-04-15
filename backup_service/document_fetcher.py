"""
Document fetching with pagination for Azure AI Search backup.

Handles batch fetching with cursor-based pagination and retry logic.
"""

import asyncio
from datetime import datetime
from typing import Any, List, Optional, Set, Tuple
from azure.search.documents.aio import SearchClient
from azure.core.exceptions import IncompleteReadError, ServiceRequestError, ServiceResponseError
from .logger import logger
from .constants import DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF_BASE
from .utils import (
    format_datetime_for_odata,
    build_null_timestamp_filter,
    build_time_window_filter,
    is_null_timestamp_window
)


async def fetch_batch_split_by_timestamp_ranges(
    client: SearchClient,
    timestamp_field: str,
    key_field: str,
    batch_size: int,
    last_cursor_value: Optional[Any],
    processed_ids_at_cursor: Set,
    timestamp_range: Tuple[datetime, datetime],
    last_backup_time: Optional[datetime] = None,
    max_retries: int = DEFAULT_MAX_RETRIES
) -> Tuple[List[dict], Optional[Any], Set, bool]:
    """
    Fetch documents using combined timestamp range + timestamp cursor pagination.
    Handles duplicate timestamps by tracking processed document IDs.
    This bypasses the 100K limit by breaking data into smaller time windows.

    Args:
        client: SearchClient instance
        timestamp_field: Name of the timestamp field
        key_field: Name of the key field (to track processed IDs)
        batch_size: Number of documents per batch
        last_cursor_value: Last timestamp value from previous batch
        processed_ids_at_cursor: Set of document IDs already processed at this timestamp
        timestamp_range: Tuple of (start_time, end_time) for this window
        last_backup_time: For incremental backups, only fetch docs after this time
        max_retries: Maximum number of retry attempts for network errors

    Returns:
        Tuple of (documents, next_cursor, processed_ids, has_more_in_window)
    """
    for attempt in range(max_retries):
        try:
            return await _fetch_batch_internal(
                client, timestamp_field, key_field, batch_size,
                last_cursor_value, processed_ids_at_cursor,
                timestamp_range, last_backup_time
            )
        except (IncompleteReadError, ServiceRequestError, ServiceResponseError, ConnectionResetError) as e:
            if attempt < max_retries - 1:
                wait_time = DEFAULT_RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"Network error during fetch (attempt {attempt + 1}/{max_retries}): {e}")
                logger.warning(f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch batch after {max_retries} attempts")
                raise


async def _fetch_batch_internal(
    client: SearchClient,
    timestamp_field: str,
    key_field: str,
    batch_size: int,
    last_cursor_value: Optional[Any],
    processed_ids_at_cursor: Set,
    timestamp_range: Tuple[datetime, datetime],
    last_backup_time: Optional[datetime] = None
) -> Tuple[List[dict], Optional[Any], Set, bool]:
    """Internal implementation of fetch_batch_split_by_timestamp_ranges"""
    start_time, end_time = timestamp_range

    # Special handling for null timestamp window (marked by epoch time)
    is_null_window = is_null_timestamp_window(start_time, end_time)

    # Build filter query
    if is_null_window:
        # For null timestamp window, only filter for null timestamps
        filter_query = build_null_timestamp_filter(timestamp_field)
    else:
        # Use utility function for base time window filter
        filter_query = build_time_window_filter(timestamp_field, start_time, end_time, last_backup_time)

        # Add timestamp cursor filter within this time window
        if last_cursor_value:
            # Format timestamp for filter
            if isinstance(last_cursor_value, datetime):
                cursor_str = format_datetime_for_odata(last_cursor_value)
            else:
                cursor_str = str(last_cursor_value)
            filter_query = f"{filter_query} and {timestamp_field} ge {cursor_str}"

    # Execute search with ordering by timestamp field
    if is_null_window:
        results = await client.search(
            search_text="*",
            filter=filter_query,
            select="*",
            top=batch_size
        )
    else:
        results = await client.search(
            search_text="*",
            filter=filter_query,
            select="*",
            top=batch_size,
            order_by=f"{timestamp_field} asc"
        )

    documents = []
    new_cursor = None
    new_processed_ids = set()
    total_fetched = 0

    async for doc in results:
        total_fetched += 1
        doc_id = doc.get(key_field)
        doc_timestamp = doc.get(timestamp_field)

        # Skip documents we've already processed at this exact timestamp
        if doc_timestamp == last_cursor_value and doc_id in processed_ids_at_cursor:
            continue

        documents.append(doc)

        # Track the last timestamp value for next pagination
        if timestamp_field in doc:
            new_cursor = doc[timestamp_field]

        # Track processed IDs at the current cursor timestamp
        if doc_timestamp == new_cursor:
            new_processed_ids.add(doc_id)

    # Check if there might be more documents in this time window
    # has_more is True if we fetched a full batch from search (even if some were skipped as duplicates)
    has_more = total_fetched == batch_size

    return documents, new_cursor, new_processed_ids, has_more
