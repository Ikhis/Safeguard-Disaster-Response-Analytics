"""
Centralized Logging Configuration
──────────────────────────────────
Provides a consistent logging setup used by every module
in the pipeline.

Each logger writes to two destinations simultaneously:
    1. Console (stdout) — so you can see what's happening
       in real-time when running the pipeline
    2. File (logs/pipeline.log) — so you have a permanent
       record for debugging and auditing

Log format:
    2026-05-25 02:33:54 | INFO     | etl.ingestion.fema_ingest | Fetching FEMA page 1

    Fields: timestamp | level | module name | message

The log level and directory are controlled by Config.LOG_LEVEL
and Config.LOG_DIR, which are read from the .env file.

Usage:
    from utils.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Starting ingestion")
    logger.warning("Retrying API call")
    logger.error("Pipeline failed")
"""

import logging
import os
from config.settings import Config


def get_logger(name):
    """Return a configured logger for the given module.

    Creates a logger with dual output (console + file) on first
    call. Subsequent calls with the same name return the existing
    logger without adding duplicate handlers.

    The logger name typically uses __name__, which gives the full
    module path (e.g., 'etl.ingestion.fema_ingest'). This makes
    it easy to identify which module produced each log line.

    Args:
        name (str): Logger name, usually __name__ from the
            calling module. Examples:
                - 'etl.ingestion.fema_ingest'
                - 'utils.database'
                - 'pipeline' (for the orchestrator)

    Returns:
        logging.Logger: A configured logger instance with
            console and file handlers attached.

    Example:
        logger = get_logger(__name__)
        logger.info("Processing started")
        logger.debug("Row count: 5000")     # only shown if LOG_LEVEL=DEBUG
        logger.error("Connection failed")
    """
    logger = logging.getLogger(name)

    # Prevent adding duplicate handlers if get_logger is
    # called multiple times with the same name
    if logger.handlers:
        return logger

    # Set the minimum log level from config (INFO, DEBUG, WARNING, etc.)
    logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))

    # Define the format for all log messages
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Handler 1: Console output (visible in terminal)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Handler 2: File output (persistent record)
    # Creates the logs/ directory if it doesn't exist
    os.makedirs(Config.LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(Config.LOG_DIR, 'pipeline.log'))
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger