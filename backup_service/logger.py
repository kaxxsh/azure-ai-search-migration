import logging
import sys


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

    def format(self, record):
        # Add color to level name for console
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.BOLD}{self.COLORS[levelname]}{levelname}{self.RESET}"

        # Format timestamp in a cleaner way
        formatted = super().format(record)
        return formatted


def setup_logger(log_file: str = 'ai_search_migration.log', log_level: int = logging.INFO) -> logging.Logger:
    """
    Configure and return a logger instance with file and console handlers.
    Optimized for Azure AI Search Migration tracking.

    Args:
        log_file: Path to the log file
        log_level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    # Suppress verbose Azure SDK logging
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
    logging.getLogger('azure.identity').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Create logger
    logger = logging.getLogger('ai_search_migration')
    logger.setLevel(log_level)
    logger.handlers.clear()  # Clear any existing handlers

    # File handler - plain text format for logs
    file_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)

    # Console handler - colored output for better readability
    console_formatter = ColoredFormatter(
        fmt='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Create default logger instance
logger = setup_logger()