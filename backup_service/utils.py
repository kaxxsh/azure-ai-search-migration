"""
Common utility functions for Azure AI Search backup.

Consolidates repeated patterns across the codebase.
"""

from datetime import datetime, timezone
from typing import Optional, Any
from urllib.parse import unquote


def print_banner():
    """Print a professional banner for the migration tool"""
    banner = """
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║           Azure AI Search Migration Tool v1.0                    ║
║                                                                  ║
║     Professional Index Migration with Time-Windowed Transfer     ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def normalize_connection_string(value: Optional[str]) -> Optional[str]:
    """Decode common escaping so Azure SDK can parse the connection string."""
    if not value:
        return None
    cleaned = value.strip()
    if "\\;" in cleaned or "\\=" in cleaned:
        cleaned = cleaned.replace("\\;", ";").replace("\\=", "=")
    if "%" in cleaned:
        try:
            decoded = unquote(cleaned)
            if decoded:
                cleaned = decoded
        except Exception:
            pass
    return cleaned


def format_datetime_for_odata(dt: datetime) -> str:
    """
    Format datetime to OData filter string format.

    Args:
        dt: Datetime object to format

    Returns:
        OData-formatted string (e.g., "2024-01-15T10:30:45.123Z")
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"


def parse_datetime_from_iso(value: Any) -> Optional[datetime]:
    """
    Parse datetime from ISO string or return as-is if already datetime.

    Args:
        value: String or datetime object

    Returns:
        Datetime object or None if parsing fails
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            # parsing with fromisoformat
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            try:
                # Fallback to strptime
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
            except Exception:
                return None

    return None


async def safe_close_async_client(client: Any) -> None:
    """
    Safely close an async client, handling both sync and async close methods.

    Args:
        client: Client object with a close() method
    """
    if client is None:
        return

    try:
        close_result = client.close()
        # Check if the result is awaitable (coroutine)
        if hasattr(close_result, '__await__'):
            await close_result
    except (AttributeError, RuntimeError, ConnectionError):
        # Expected errors during cleanup - safe to ignore
        pass


def build_incremental_filter(
    timestamp_field: str,
    last_backup_time: Optional[datetime]
) -> Optional[str]:
    """
    Build OData filter for incremental backup.

    Args:
        timestamp_field: Name of the timestamp field
        last_backup_time: Last backup timestamp

    Returns:
        OData filter string or None
    """
    if not last_backup_time:
        return None

    timestamp_str = format_datetime_for_odata(last_backup_time)
    return f"{timestamp_field} gt {timestamp_str}"


def build_time_window_filter(
    timestamp_field: str,
    start_time: datetime,
    end_time: datetime,
    last_backup_time: Optional[datetime] = None
) -> str:
    """
    Build OData filter for a time window with optional incremental filter.

    Args:
        timestamp_field: Name of the timestamp field
        start_time: Window start time
        end_time: Window end time
        last_backup_time: Optional last backup time for incremental

    Returns:
        OData filter string
    """
    filter_parts = []

    # Add incremental backup filter if needed
    incremental_filter = build_incremental_filter(timestamp_field, last_backup_time)
    if incremental_filter:
        filter_parts.append(incremental_filter)

    # Add time window filter
    start_str = format_datetime_for_odata(start_time)
    end_str = format_datetime_for_odata(end_time)
    filter_parts.append(f"({timestamp_field} ge {start_str} and {timestamp_field} lt {end_str})")

    return " and ".join(filter_parts)


def build_null_timestamp_filter(timestamp_field: str) -> str:
    """
    Build OData filter for null timestamps.

    Args:
        timestamp_field: Name of the timestamp field

    Returns:
        OData filter string
    """
    return f"{timestamp_field} eq null"


def is_null_timestamp_window(start_time: datetime, end_time: datetime) -> bool:
    """
    Check if a time window represents the special null timestamp window.

    Args:
        start_time: Window start time
        end_time: Window end time

    Returns:
        True if this is the null timestamp window marker
    """
    null_marker = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return start_time == null_marker and end_time == null_marker


def get_null_timestamp_window() -> tuple[datetime, datetime]:
    """
    Get the marker for null timestamp window.

    Returns:
        Tuple of (start_time, end_time) representing null window
    """
    null_marker = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (null_marker, null_marker)