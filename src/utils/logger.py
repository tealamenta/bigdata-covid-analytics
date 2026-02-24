"""
Centralized logging module for COVID Analytics project.

Provides structured logging with proper formatting and file rotation.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class CovidLogger:
    """Custom logger for COVID Analytics project."""

    def __init__(
        self,
        name: str,
        log_dir: str = "logs",
        level: int = logging.INFO,
        console: bool = True
    ):
        """
        Initialize logger.

        Args:
            name: Logger name (usually module name)
            log_dir: Directory for log files
            level: Logging level
            console: Whether to also log to console
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers = []  # Clear existing handlers

        # Create log directory
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        # File handler
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = log_path / f"{name}_{timestamp}.log"

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)

        # Console handler
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        if console:
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def get_logger(self) -> logging.Logger:
        """Return the logger instance."""
        return self.logger


def get_logger(
    name: str,
    log_dir: str = "logs",
    level: int = logging.INFO
) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name
        log_dir: Directory for log files
        level: Logging level

    Returns:
        Configured logger instance

    Example:
        >>> logger = get_logger("ingestion")
        >>> logger.info("Starting data ingestion")
    """
    covid_logger = CovidLogger(name, log_dir, level)
    return covid_logger.get_logger()


def log_execution_time(logger: logging.Logger):
    """
    Decorator to log function execution time.

    Args:
        logger: Logger instance

    Example:
        >>> logger = get_logger("mymodule")
        >>> @log_execution_time(logger)
        >>> def my_function():
        >>>     pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.info(f"Starting {func.__name__}")

            try:
                result = func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(
                    f"Completed {func.__name__} in {duration:.2f} seconds"
                )
                return result

            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                logger.error(
                    f"Failed {func.__name__} after {duration:.2f} seconds: {e}"
                )
                raise

        return wrapper
    return decorator


if __name__ == "__main__":
    # Test du logger
    logger = get_logger("test")
    logger.info("Test info message")
    logger.warning("Test warning message")
    logger.error("Test error message")
