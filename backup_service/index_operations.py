"""
Index operations for Azure AI Search backup.

Handles index schema copying and field verification.
"""

import asyncio
from typing import Optional
from azure.search.documents.indexes.aio import SearchIndexClient
from azure.search.documents.aio import SearchClient
from .logger import logger
from .constants import DEFAULT_INDEX_CREATION_WAIT_SECONDS


async def verify_timestamp_field(
    source_client: SearchIndexClient,
    field_name: str,
    index_name: str
) -> bool:
    """
    Verify that the timestamp field exists and is filterable/sortable.

    Args:
        source_client: SearchIndexClient for the source
        field_name: Name of the timestamp field
        index_name: Name of the index

    Returns:
        True if field is valid, False otherwise
    """
    try:
        index = await source_client.get_index(index_name)
        for field in index.fields:
            if field.name == field_name:
                if not field.filterable or not field.sortable:
                    logger.error(f"Field '{field_name}' MUST be both filterable AND sortable!")
                    logger.error(f"Current settings - Filterable: {field.filterable}, Sortable: {field.sortable}")
                    return False
                return True

        # Field not found - suggest alternatives
        logger.error(f"Timestamp field '{field_name}' not found in index")

        # Find potential timestamp fields (Edm.DateTimeOffset fields that are filterable and sortable)
        potential_fields = []
        for field in index.fields:
            if str(field.type) == 'Edm.DateTimeOffset' and field.filterable and field.sortable:
                potential_fields.append(field.name)

        if potential_fields:
            logger.error(f"Available timestamp fields (filterable & sortable): {', '.join(potential_fields)}")
            logger.error(f"Please specify one using --timestamp-field parameter")
        else:
            logger.error("No suitable timestamp fields found in the index")
            logger.error("A timestamp field must be of type Edm.DateTimeOffset and both filterable and sortable")

        return False
    except Exception as e:
        logger.error(f"Error verifying timestamp field: {e}")
        return False


async def get_documents_count(
    client: SearchClient,
    filter_query: Optional[str] = None
) -> int:
    """
    Get count of documents matching filter.

    Args:
        client: SearchClient instance
        filter_query: Optional OData filter query

    Returns:
        Document count
    """
    try:
        results = await client.search(
            search_text="*",
            filter=filter_query,
            include_total_count=True,
            top=0
        )
        return await results.get_count() or 0
    except Exception as e:
        logger.error(f"Error getting document count: {e}")
        return 0


async def copy_index_definition(
    source_client: SearchIndexClient,
    dest_client: SearchIndexClient,
    source_idx_name: str,
    dest_idx_name: str
) -> bool:
    """
    Copy index schema from source to destination.

    Args:
        source_client: SearchIndexClient for source
        dest_client: SearchIndexClient for destination
        source_idx_name: Source index name
        dest_idx_name: Destination index name

    Returns:
        True if successful, raises exception otherwise
    """
    logger.info(f"Copying index schema from '{source_idx_name}' to '{dest_idx_name}'...")

    try:
        source_index = await source_client.get_index(source_idx_name)

        # Check if destination index already exists
        try:
            dest_index = await dest_client.get_index(dest_idx_name)
            logger.info(f"Destination index '{dest_idx_name}' already exists. Skipping schema copy.")
            return True
        except Exception:
            pass

        # Handle synonym maps
        synonym_map_names = []
        for field in source_index.fields:
            if field.synonym_map_names:
                synonym_map_names.extend(field.synonym_map_names)

        for synonym_map_name in set(synonym_map_names):
            try:
                logger.info(f"Copying synonym map: {synonym_map_name}")
                synonym_map = await source_client.get_synonym_map(synonym_map_name)
                await dest_client.create_or_update_synonym_map(synonym_map)
            except Exception as e:
                logger.warning(f"Error copying synonym map {synonym_map_name}: {e}")

        # Update the index name if different
        if source_idx_name != dest_idx_name:
            source_index.name = dest_idx_name

        logger.info(f"Creating index '{dest_idx_name}' in destination...")
        await dest_client.create_or_update_index(source_index)
        logger.info("Index schema copied successfully!")

        # Wait for index to be ready
        await asyncio.sleep(DEFAULT_INDEX_CREATION_WAIT_SECONDS)
        return True

    except Exception as e:
        logger.error(f"Error copying index schema: {e}")
        raise
