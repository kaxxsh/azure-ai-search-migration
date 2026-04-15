# Azure AI Search Backup and Migration Tool

Time-windowed, incremental backup and migration for Azure AI Search indexes. This tool copies
documents from a source index to a destination index using timestamp-based keyset pagination
to bypass the 100K result limit.

## Features
- Full and incremental migrations based on the last successful run
- Time-windowed pagination with concurrent processing
- Automatic destination index creation (schema copied from source if missing)
- Optional audit tracking in Azure Table Storage for resumability

## Requirements
- Python 3.9+
- uv (https://docs.astral.sh/uv/) for environment and dependency management
- Azure AI Search source and destination services
- Azure Table Storage (optional, for incremental or resumable runs)

## Quick Start (Local)
```bash
uv venv .venv
source .venv/bin/activate
uv sync

cp .env.example .env
uv run main.py
```

Command-line arguments override environment variables:
```bash
uv run main.py \
  --source-endpoint https://source.search.windows.net \
  --source-key YOUR_SOURCE_KEY \
  --source-index source-index \
  --destination-endpoint https://dest.search.windows.net \
  --destination-key YOUR_DEST_KEY \
  --destination-index dest-index \
  --window-days 7 \
  --max-docs-per-window 500 \
  --concurrent-windows 10
```

## Configuration

### Required environment variables
- `AZURE_AI_SEARCH_BACKUP_SOURCE_SERVICE_ENDPOINT`
- `AZURE_AI_SEARCH_BACKUP_SOURCE_API_KEY`
- `AZURE_AI_SEARCH_BACKUP_SOURCE_INDEX_NAME`
- `AZURE_AI_SEARCH_BACKUP_DESTINATION_SERVICE_ENDPOINT`
- `AZURE_AI_SEARCH_BACKUP_DESTINATION_API_KEY`

Optional (defaults to source index name when omitted):
- `AZURE_AI_SEARCH_BACKUP_DESTINATION_INDEX_NAME`

### Optional environment variables
- `AZURE_AI_SEARCH_BACKUP_TIMESTAMP_FIELD` (default: `created_at`)
- `AZURE_AI_SEARCH_BACKUP_WINDOW_SIZE_DAYS` (default: `30`)
- `AZURE_AI_SEARCH_BACKUP_MAX_DOCS_PER_WINDOW` (default: `1000`)
- `AZURE_AI_SEARCH_BACKUP_CONCURRENT_WINDOWS` (default: `5`)
- `AZURE_AI_SEARCH_BACKUP_BATCH_SIZE` (default: `100`)

Audit tracking (enable incremental or resumable runs):
- `AZURE_AI_SEARCH_BACKUP_STORAGE_CONNECTION_STRING` (recommended)
  or
- `AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_NAME`
- `AZURE_AI_SEARCH_BACKUP_STORAGE_ACCOUNT_KEY`
- `AZURE_AI_SEARCH_BACKUP_STORAGE_TABLE_ENDPOINT` (optional)
- `AZURE_AI_SEARCH_BACKUP_AUDIT_TABLE_NAME` (default: `AzureSearchBackupAudit`)

If no storage configuration is provided, the tool runs a one-time migration without
audit tracking or incremental resume.

### Command-line options
Run `python main.py --help` for the full list. Common options:
- `--source-endpoint`, `--source-key`, `--source-index`
- `--destination-endpoint`, `--destination-key`, `--destination-index`
- `--window-days`, `--max-docs-per-window`, `--concurrent-windows`
- `--timestamp-field`
- `--storage-connection-string` or `--storage-account-name`/`--storage-account-key`
- `--storage-table-endpoint`, `--audit-table-name`
- `--skip-schema-copy`

## Docker

Build the image:
```bash
docker build -t azure-search-migration:latest .
```

Build the image for multiple platforms:
```bash
docker buildx build --platform linux/amd64,linux/arm64 -t azure-search-migration:latest .
```

Run with an `.env` file:
```bash
docker run --rm \
  --env-file .env \
  azure-search-migration:latest
```

Run with command-line arguments:
```bash
docker run --rm \
  azure-search-migration:latest \
  --source-endpoint https://source.search.windows.net \
  --source-key YOUR_SOURCE_KEY \
  --source-index source-index \
  --destination-endpoint https://dest.search.windows.net \
  --destination-key YOUR_DEST_KEY \
  --destination-index dest-index
```

## Logs
- Default log file: `ai_search_migration.log` in the project root.
- Console logs are printed during execution with colored output for better readability.