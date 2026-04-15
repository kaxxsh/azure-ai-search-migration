"""
Time window management for Azure AI Search backup.

Handles generation and splitting of time windows to bypass 100K document limits.
"""

from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple
from azure.search.documents.aio import SearchClient
from .logger import logger
from .utils import (
    parse_datetime_from_iso,
    build_time_window_filter,
    build_null_timestamp_filter,
    is_null_timestamp_window
)

async def count_documents_in_window(
    client: SearchClient,
    timestamp_field: str,
    window: Tuple[datetime, datetime],
    last_backup_time: Optional[datetime] = None,
    include_null_timestamps: bool = False
) -> int:
    """
    Count documents in a specific time window.

    Args:
        client: SearchClient instance
        timestamp_field: Name of the timestamp field
        window: Tuple of (start_time, end_time)
        last_backup_time: For incremental backups
        include_null_timestamps: Count null timestamps instead

    Returns:
        Document count
    """
    start_time, end_time = window

    # Special handling for null timestamp window
    if include_null_timestamps:
        filter_query = build_null_timestamp_filter(timestamp_field)
        results = await client.search(
            search_text="*",
            filter=filter_query,
            include_total_count=True,
            top=0
        )
        return await results.get_count() or 0

    filter_query = build_time_window_filter(timestamp_field, start_time, end_time, last_backup_time)

    results = await client.search(
        search_text="*",
        filter=filter_query,
        include_total_count=True,
        top=0
    )
    return await results.get_count() or 0


async def split_large_window(
    client: SearchClient,
    timestamp_field: str,
    window: Tuple[datetime, datetime],
    last_backup_time: Optional[datetime] = None,
    max_docs_per_window: int = 30000
) -> List[Tuple[datetime, datetime]]:
    """
    Split a time window if it contains more than max_docs_per_window documents.
    Recursively splits until all windows are under the threshold.

    Args:
        client: SearchClient instance
        timestamp_field: Name of the timestamp field
        window: Tuple of (start_time, end_time)
        last_backup_time: For incremental backups
        max_docs_per_window: Maximum documents per window

    Returns:
        List of time window tuples
    """
    doc_count = await count_documents_in_window(client, timestamp_field, window, last_backup_time)

    if doc_count <= max_docs_per_window:
        return [window]

    # Split the window in half
    start_time, end_time = window
    duration = end_time - start_time
    mid_time = start_time + duration / 2

    # Recursively split both halves
    first_half = await split_large_window(
        client, timestamp_field, (start_time, mid_time), last_backup_time, max_docs_per_window
    )
    second_half = await split_large_window(
        client, timestamp_field, (mid_time, end_time), last_backup_time, max_docs_per_window
    )

    return first_half + second_half


async def generate_time_windows(
    client: SearchClient,
    timestamp_field: str,
    last_backup_time: Optional[datetime],
    window_size_days: int,
    max_docs_per_window: int
) -> List[Tuple[datetime, datetime, int]]:
    """
    Generate time windows to split the backup into manageable chunks.
    Each window will contain <= max_docs_per_window documents.

    Args:
        client: SearchClient instance
        timestamp_field: Name of the timestamp field
        last_backup_time: Start from this time for incremental backups
        window_size_days: Initial window size in days
        max_docs_per_window: Maximum documents per window

    Returns:
        List of (start_time, end_time, doc_count) tuples
    """
    logger.info(f"Generating time windows (max {max_docs_per_window} docs per window)...")

    # Get the earliest timestamp
    if last_backup_time:
        start_date = last_backup_time
        logger.info(f"  Starting from last backup: {start_date}")
    else:
        logger.info(f"  Fetching earliest document timestamp...")
        results = await client.search(
            search_text="*",
            filter=f"{timestamp_field} ne null",
            select=timestamp_field,
            top=1,
            order_by=f"{timestamp_field} asc"
        )
        first_doc = None
        async for doc in results:
            first_doc = doc
            break

        if not first_doc or timestamp_field not in first_doc or first_doc[timestamp_field] is None:
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
            logger.info(f"  No timestamp found, using default: {start_date}")
        else:
            start_date = parse_datetime_from_iso(first_doc[timestamp_field])
            if not start_date:
                start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
            logger.info(f"  Earliest document: {start_date}")

    # Get the latest timestamp
    logger.info(f"  Fetching latest document timestamp...")
    results = await client.search(
        search_text="*",
        filter=f"{timestamp_field} ne null",
        select=timestamp_field,
        top=1,
        order_by=f"{timestamp_field} desc"
    )
    last_doc = None
    async for doc in results:
        last_doc = doc
        break

    if not last_doc or timestamp_field not in last_doc or last_doc[timestamp_field] is None:
        end_date = datetime.now(timezone.utc)
        logger.info(f"  No timestamp found, using now: {end_date}")
    else:
        end_date = parse_datetime_from_iso(last_doc[timestamp_field])
        if not end_date:
            end_date = datetime.now(timezone.utc)
        logger.info(f"  Latest document: {end_date}")

    # Generate initial windows
    logger.info(f"  Creating initial {window_size_days}-day windows...")
    windows = []
    current = start_date
    delta = timedelta(days=window_size_days)

    while current < end_date:
        window_end = min(current + delta, end_date + timedelta(milliseconds=1))
        windows.append((current, window_end))
        current = window_end

    logger.info(f"  Generated {len(windows)} initial time windows")

    # Split large windows
    logger.info(f"  Checking and splitting windows with > {max_docs_per_window} docs...")
    final_windows = []
    for i, window in enumerate(windows):
        logger.debug(f"    Analyzing window {i+1}/{len(windows)}...")
        split_windows = await split_large_window(
            client, timestamp_field, window, last_backup_time, max_docs_per_window
        )
        final_windows.extend(split_windows)

    logger.info(f"  Counting documents in final windows...")
    windows_with_counts = []
    for i, window in enumerate(final_windows):
        logger.debug(f"    Counting window {i+1}/{len(final_windows)}...")
        count = await count_documents_in_window(client, timestamp_field, window, last_backup_time)
        if count > 0:
            windows_with_counts.append((window[0], window[1], count))

    # Check for documents with null/missing timestamps
    logger.info(f"  Checking for documents with null timestamps...")
    null_timestamp_count = await count_documents_in_window(
        client, timestamp_field,
        (datetime.now(timezone.utc), datetime.now(timezone.utc)),
        last_backup_time,
        include_null_timestamps=True
    )
    if null_timestamp_count > 0:
        logger.info(f"  Found {null_timestamp_count:,} documents with null timestamps")
        # Add a special window for null timestamps (using epoch time as marker)
        null_window_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
        null_window_end = datetime(1970, 1, 1, tzinfo=timezone.utc)
        windows_with_counts.append((null_window_start, null_window_end, null_timestamp_count))

    logger.info(f"✅ Generated {len(windows_with_counts)} time windows (skipped empty windows)")
    logger.info(f"  Date range: {start_date.date()} to {end_date.date()}")
    total_docs = sum(w[2] for w in windows_with_counts)
    logger.info(f"  Total documents: {total_docs:,}")
    if windows_with_counts:
        max_count = max(w[2] for w in windows_with_counts)
        logger.info(f"  Max docs per window: {max_count:,}")

    # Show last few windows
    if len(windows_with_counts) > 0:
        logger.debug(f"Last 3 windows:")
        for i, (start, end, count) in enumerate(windows_with_counts[-3:]):
            if is_null_timestamp_window(start, end):
                logger.debug(f"  Window {len(windows_with_counts)-3+i+1}: NULL timestamps ({count:,} docs)")
            else:
                logger.debug(f"  Window {len(windows_with_counts)-3+i+1}: {start} to {end} ({count:,} docs)")

    return windows_with_counts
