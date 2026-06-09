"""Configure structured logging for the project."""

import logging
import sys


def setup_logging(level: int = logging.INFO):
    """Set up structured logging with timestamp, level, and module."""
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates on re-init
    root.handlers.clear()
    root.addHandler(handler)
