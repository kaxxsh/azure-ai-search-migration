"""
Document upload manager for Azure AI Search backup.

Handles document uploads with retry logic and automatic batch splitting.
"""

import asyncio
import json
import sys
from typing import List
from azure.search.documents.aio import SearchClient
from .logger import logger
from .constants import DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF_BASE, DEFAULT_FALLBACK_DOC_SIZE_MB, MAX_BATCH_SIZE_MB


async def upload_with_retry(
    client: SearchClient,
    documents: List[dict],
    max_retries: int = DEFAULT_MAX_RETRIES,
    max_batch_mb: int = MAX_BATCH_SIZE_MB
) -> bool:
    """
    Upload documents with retry logic and automatic batch splitting.

    Args:
        client: SearchClient instance
        documents: List of documents to upload
        max_retries: Number of retry attempts
        max_batch_mb: Maximum batch size in MB (Azure limit is ~16MB per batch)

    Returns:
        True if successful
    """
    def estimate_size_mb(docs):
        """Estimate payload size (rough approximation)"""
        try:
            json_str = json.dumps(docs, default=str)
            size_bytes = sys.getsizeof(json_str)
            return size_bytes / (1024 * 1024)
        except Exception:
            return len(docs) * DEFAULT_FALLBACK_DOC_SIZE_MB

    async def upload_batch(docs, attempt=0):
        """Recursive function to upload with splitting"""
        if not docs:
            return True

        try:
            await client.merge_or_upload_documents(documents=docs)
            return True

        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e).lower()

            # Check if this is a "Request Entity Too Large" error
            if "requestentitytoolarge" in error_type.lower() or "request entity too large" in error_msg or "too large" in error_msg:
                if len(docs) == 1:
                    logger.warning(f"Single document too large to upload (size: ~{estimate_size_mb([docs[0]]):.2f}MB), skipping")
                    return False

                # Split batch in half
                mid = len(docs) // 2
                logger.warning(f"Batch too large ({len(docs)} docs, ~{estimate_size_mb(docs):.2f}MB), splitting into {mid} + {len(docs)-mid}")

                result1 = await upload_batch(docs[:mid], attempt)
                result2 = await upload_batch(docs[mid:], attempt)
                return result1 and result2

            # Check for network/connection errors
            if ("incompleteread" in error_type.lower() or
                "connectionreset" in error_type.lower() or
                "connection reset" in error_msg or
                "incomplete read" in error_msg):
                if attempt >= max_retries - 1:
                    logger.error(f"Network error: Failed to upload batch after {max_retries} attempts: {e}")
                    raise

                wait_time = DEFAULT_RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"Network error during upload, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
                return await upload_batch(docs, attempt + 1)

            # For other errors, retry with exponential backoff
            if attempt >= max_retries - 1:
                logger.error(f"Failed to upload batch after {max_retries} attempts: {e}")
                raise

            await asyncio.sleep(DEFAULT_RETRY_BACKOFF_BASE ** attempt)
            return await upload_batch(docs, attempt + 1)

    # Pre-emptively split if batch seems too large
    estimated_size = estimate_size_mb(documents)
    if estimated_size > max_batch_mb:
        chunk_size = int(len(documents) * (max_batch_mb / estimated_size))
        chunk_size = max(1, chunk_size)

        logger.warning(f"Large batch detected (~{estimated_size:.1f}MB), splitting into chunks of ~{chunk_size} docs")

        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]
            await upload_batch(chunk)
        return True
    else:
        return await upload_batch(documents)
