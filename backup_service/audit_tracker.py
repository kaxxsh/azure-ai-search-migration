from typing import Optional
from datetime import datetime, timezone
from uuid import uuid4
from azure.data.tables.aio import TableServiceClient, TableClient
from azure.data.tables import UpdateMode
from azure.core.credentials import AzureNamedKeyCredential
from backup_service.constants import DEFAULT_AUDIT_TABLE_NAME
from .utils import parse_datetime_from_iso, safe_close_async_client
from .logger import logger


class AuditTracker:
    """Track incremental backup progress in Azure Table Storage"""

    def __init__(self,
                 source_endpoint: str,
                 destination_endpoint: str,
                 connection_string: Optional[str] = None,
                 table_name: str = DEFAULT_AUDIT_TABLE_NAME,
                 account_name: Optional[str] = None,
                 account_key: Optional[str] = None,
                 table_endpoint: Optional[str] = None):
        self.source_endpoint = source_endpoint
        self.destination_endpoint = destination_endpoint
        self.connection_string = connection_string
        self.table_name = table_name
        self.account_name = account_name
        self.account_key = account_key
        self.table_endpoint = table_endpoint
        self.run_id = str(uuid4())
        self.service_client: Optional[TableServiceClient] = None
        self.table_client: Optional[TableClient] = None

    async def __aenter__(self):
        """Initialize table client and create table if needed"""
        try:
            if self.connection_string:
                try:
                    logger.info("Initializing audit tracker with connection string...")
                    self.service_client = TableServiceClient.from_connection_string(self.connection_string)
                except ValueError as ve:
                    logger.warning(f"Failed to parse connection string: {ve}")
                    if self.account_name and self.account_key:
                        endpoint = self.table_endpoint or f"https://{self.account_name}.table.core.windows.net"
                        credential = AzureNamedKeyCredential(self.account_name, self.account_key)
                        logger.info(f"Using explicit credentials for endpoint: {endpoint}")
                        self.service_client = TableServiceClient(endpoint=endpoint, credential=credential)
                    else:
                        raise ValueError(
                            "Unable to parse AZURE_AI_SEARCH_BACKUP_STORAGE_CONNECTION_STRING. "
                            "Ensure it includes AccountName and AccountKey or switch to explicit account configuration."
                        )
            elif self.account_name and self.account_key:
                endpoint = self.table_endpoint or f"https://{self.account_name}.table.core.windows.net"
                credential = AzureNamedKeyCredential(self.account_name, self.account_key)
                logger.info(f"Initializing audit tracker with explicit credentials for endpoint: {endpoint}")
                self.service_client = TableServiceClient(endpoint=endpoint, credential=credential)
            else:
                raise ValueError(
                    "No Azure Table Storage credentials supplied."
                )

            # Create table if it doesn't exist
            try:
                await self.service_client.create_table(self.table_name)
                logger.info(f"Created audit table '{self.table_name}'")
            except Exception as e:
                error_msg = str(e)
                if "TableAlreadyExists" in error_msg or "already exists" in error_msg.lower():
                    logger.debug(f"Audit table '{self.table_name}' already exists")
                else:
                    logger.warning(f"Could not create audit table '{self.table_name}': {e}")

            self.table_client = self.service_client.get_table_client(self.table_name)
            logger.info(f"Audit tracker initialized successfully with table '{self.table_name}'")
            return self
        except Exception as e:
            logger.error(f"Failed to initialize audit tracker: {e}", exc_info=True)
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup"""
        if self.table_client is not None:
            await safe_close_async_client(self.table_client)
        if self.service_client is not None:
            await safe_close_async_client(self.service_client)

    async def get_last_successful_backup_time(self, source_index: str, dest_index: str) -> Optional[datetime]:
        """Get the timestamp of the last successful backup or in-progress run to enable resumption"""
        # Check both 'Completed' and 'In Progress' runs to support resuming interrupted backups
        query_filter = (f"PartitionKey eq 'BackupRun' and (Status eq 'Completed' or Status eq 'In Progress') "
                        f"and SourceIndex eq '{source_index}' and DestinationIndex eq '{dest_index}'")
        entities = self.table_client.query_entities(query_filter=query_filter)

        last_time = None
        last_entity = None
        async for entity in entities:
            if "LastDocumentTimestamp" in entity and entity["LastDocumentTimestamp"]:
                timestamp = parse_datetime_from_iso(entity["LastDocumentTimestamp"])

                if timestamp and (last_time is None or timestamp > last_time):
                    last_time = timestamp
                    last_entity = entity

        # Log if resuming from an interrupted run
        if last_entity and last_entity.get("Status") == "In Progress":
            logger.info(f"🔄 Found interrupted backup run: {last_entity.get('RunId')}")
            logger.info(f"   Last processed: {last_entity.get('DocumentsProcessed', 0):,} documents")
            logger.info(f"   Resuming from: {last_time}")

        return last_time

    async def mark_stale_runs_as_cancelled(self, source_index: str, dest_index: str):
        """Mark any old 'In Progress' runs as cancelled before starting a new run"""
        query_filter = (f"PartitionKey eq 'BackupRun' and Status eq 'In Progress' "
                        f"and SourceIndex eq '{source_index}' and DestinationIndex eq '{dest_index}'")
        entities = self.table_client.query_entities(query_filter=query_filter)

        cancelled_count = 0
        async for entity in entities:
            try:
                entity.update({
                    "Status": "Cancelled",
                    "ErrorMessage": "Cancelled - New backup run started",
                    "EndTime": datetime.now(timezone.utc),
                    "LastUpdateTime": datetime.now(timezone.utc)
                })
                await self.table_client.update_entity(entity, mode=UpdateMode.MERGE)
                cancelled_count += 1
                logger.debug(f"Marked run {entity['RunId']} as cancelled")
            except Exception as e:
                logger.warning(f"Failed to mark run {entity.get('RunId')} as cancelled: {e}")

        if cancelled_count > 0:
            logger.info(f"Marked {cancelled_count} stale 'In Progress' run(s) as cancelled")

    async def create_backup_run(self, total_documents: int, source_index: str,
                                destination_index: str, is_full_backup: bool,
                                last_backup_time: Optional[datetime] = None) -> str:
        """Create a new backup run entry"""
        try:
            entity = {
                "PartitionKey": "BackupRun",
                "RowKey": self.run_id,
                "RunId": self.run_id,
                "Status": "In Progress",
                "BackupType": "Full" if is_full_backup else "Incremental",
                "SourceEndpoint": self.source_endpoint,
                "DestinationEndpoint": self.destination_endpoint,
                "SourceIndex": source_index,
                "DestinationIndex": destination_index,
                "TotalDocuments": total_documents,
                "DocumentsProcessed": 0,
                "DocumentsUpdated": 0,
                "DocumentsAdded": 0,
                "BatchesCompleted": 0,
                "StartTime": datetime.now(timezone.utc),
                "LastUpdateTime": datetime.now(timezone.utc),
                "LastBackupTime": last_backup_time,
                "LastDocumentTimestamp": None,
                "ErrorMessage": "",
                "PercentComplete": 0.0
            }
            await self.table_client.create_entity(entity)
            logger.info(f"Created backup run entry with ID: {self.run_id}")
            return self.run_id
        except Exception as e:
            logger.error(f"Failed to create backup run entry: {e}", exc_info=True)
            raise

    async def update_progress(self, documents_processed: int, batches_completed: int,
                              last_doc_timestamp: Optional[datetime] = None,
                              docs_added: int = 0, docs_updated: int = 0):
        """Update the backup run progress"""
        try:
            entity = await self.table_client.get_entity("BackupRun", self.run_id)

            total_docs = entity["TotalDocuments"]
            percent_complete = (documents_processed / total_docs * 100) if total_docs > 0 else 0

            entity.update({
                "DocumentsProcessed": documents_processed,
                "DocumentsAdded": docs_added,
                "DocumentsUpdated": docs_updated,
                "BatchesCompleted": batches_completed,
                "LastUpdateTime": datetime.now(timezone.utc),
                "PercentComplete": round(percent_complete, 2)
            })

            if last_doc_timestamp:
                entity["LastDocumentTimestamp"] = last_doc_timestamp

            await self.table_client.update_entity(entity, mode=UpdateMode.MERGE)
            logger.debug(f"Updated progress: {documents_processed:,} docs, {batches_completed} batches, {percent_complete:.1f}% complete")
        except Exception as e:
            logger.error(f"Failed to update progress in audit table: {e}", exc_info=True)
            raise

    async def mark_completed(self, documents_processed: int, batches_completed: int,
                             last_doc_timestamp: Optional[datetime] = None,
                             docs_added: int = 0, docs_updated: int = 0):
        """Mark the backup run as completed"""
        try:
            entity = await self.table_client.get_entity("BackupRun", self.run_id)

            entity.update({
                "Status": "Completed",
                "DocumentsProcessed": documents_processed,
                "DocumentsAdded": docs_added,
                "DocumentsUpdated": docs_updated,
                "BatchesCompleted": batches_completed,
                "EndTime": datetime.now(timezone.utc),
                "LastUpdateTime": datetime.now(timezone.utc),
                "PercentComplete": 100.0
            })

            if last_doc_timestamp:
                entity["LastDocumentTimestamp"] = last_doc_timestamp

            await self.table_client.update_entity(entity, mode=UpdateMode.MERGE)
            logger.info(f"Marked backup run {self.run_id} as completed")
        except Exception as e:
            logger.error(f"Failed to mark backup as completed in audit table: {e}", exc_info=True)
            raise

    async def mark_failed(self, error_message: str, documents_processed: int = 0):
        """Mark the backup run as failed"""
        try:
            entity = await self.table_client.get_entity("BackupRun", self.run_id)

            entity.update({
                "Status": "Failed",
                "ErrorMessage": error_message,
                "DocumentsProcessed": documents_processed,
                "EndTime": datetime.now(timezone.utc),
                "LastUpdateTime": datetime.now(timezone.utc)
            })

            await self.table_client.update_entity(entity, mode=UpdateMode.MERGE)
            logger.warning(f"Marked backup run {self.run_id} as failed: {error_message}")
        except Exception as e:
            logger.error(f"Failed to update failed status in audit table: {e}", exc_info=True)
