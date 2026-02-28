"""Structured logging setup with Rich-powered colored console output."""

import logging

from rich.logging import RichHandler

# ── Tier → color mapping (reused by any module that logs tier names) ─────
TIER_COLORS: dict[str, str] = {
    "datacenter": "bright_magenta",
    "residential": "bright_blue",
    "unlocker": "bright_red",
    "?": "dim",
}


def tier_tag(name: str) -> str:
    """Return a Rich-markup colored tag for a proxy tier name, e.g. [bright_magenta]datacenter[/]."""
    color = TIER_COLORS.get(name, "white")
    return f"[{color}]{name}[/]"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with Rich colored output."""
    handler = RichHandler(
        level=level,
        markup=True,
        rich_tracebacks=True,
        show_path=False,
        show_time=True,
        omit_repeated_times=False,
        tracebacks_show_locals=False,
    )
    handler.setFormatter(logging.Formatter("%(name)s │ %(message)s"))

    root = logging.getLogger("zl_scraper")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the zl_scraper namespace."""
    return logging.getLogger(f"zl_scraper.{name}")
