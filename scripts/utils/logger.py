"""
logger.py
---------
Centralized logger factory for the entire pipeline.
Import this in any script to get a consistent log format.
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a logger with a consistent format.

    Args:
        name (str): Usually __name__ of the calling module.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger