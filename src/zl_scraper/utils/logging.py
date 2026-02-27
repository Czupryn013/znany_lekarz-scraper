"""Structured logging setup with colored console output."""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a readable console format."""
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger("zl_scraper")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the zl_scraper namespace."""
    return logging.getLogger(f"zl_scraper.{name}")
