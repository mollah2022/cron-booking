import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a logger with a consistent format across
    the whole project.

    Usage in any file:
        from utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Something happened")
        logger.error("Something went wrong")
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if get_logger() is called
    # multiple times for the same module (common issue in Spark jobs)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger