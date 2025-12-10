"""
logger.py
=========

Centralized logging utility for the ETL automation system.

Features:
    - Rotating file logging (max file size: 5 MB, backup count: 5)
    - Console + File handlers
    - Timestamped, level-based log formatting
    - Thread-safe logger instance
    - Lightweight wrapper function `log()` for easy use across modules

"""

import logging
from logging.handlers import RotatingFileHandler
import os

# Ensure log directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "etl_app.log")


# ------------------------------------------------------------
# LOGGER INITIALIZATION
# ------------------------------------------------------------
def _create_logger():
    """
    Creates a configured logger instance used across the ETL system.

    Returns:
        logging.Logger: Configured logger object.
    """
    logger = logging.getLogger("ETLLogger")
    logger.setLevel(logging.INFO)

    # Prevent adding duplicate handlers when re-imported
    if logger.hasHandlers():
        return logger

    # FORMATTER
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # FILE HANDLER (Rotating)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # CONSOLE HANDLER
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Register handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Create shared logger instance
LOGGER = _create_logger()


# ------------------------------------------------------------
# PUBLIC LOGGING FUNCTION
# ------------------------------------------------------------
def log(message: str, level: str = "info"):
    """
    Public logging wrapper.

    Args:
        message (str): Log message
        level (str): Log level -> "info", "warning", "error"

    This wrapper ensures:
        - Single-line call style across ETL modules
        - Consistent log formatting and behavior
    """
    level = level.lower()

    if level == "error":
        LOGGER.error(message)
    elif level == "warning":
        LOGGER.warning(message)
    else:
        LOGGER.info(message)