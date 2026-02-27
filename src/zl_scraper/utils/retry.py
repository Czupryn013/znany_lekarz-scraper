"""Tenacity-based retry decorator with structured logging."""

import httpx
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from zl_scraper.config import MAX_RETRIES, RETRY_WAIT_MULTIPLIER
from zl_scraper.utils.logging import get_logger

logger = get_logger("retry")


def _log_before_retry(retry_state: RetryCallState) -> None:
    """Log every retry attempt with context."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    url = retry_state.args[1] if len(retry_state.args) > 1 else "unknown"
    wait = retry_state.next_action.sleep if retry_state.next_action else 0
    logger.warning(
        "Retry attempt %d for url=%s | waiting %.1fs | reason: %s",
        retry_state.attempt_number,
        url,
        wait,
        str(exc)[:200] if exc else "unknown",
    )


def _log_give_up(retry_state: RetryCallState) -> None:
    """Log final failure after all retries exhausted."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    url = retry_state.args[1] if len(retry_state.args) > 1 else "unknown"
    logger.error(
        "All %d retries exhausted for url=%s | final error: %s",
        retry_state.attempt_number,
        url,
        str(exc) if exc else "unknown",
    )


retry_on_http_error = retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_WAIT_MULTIPLIER, min=2, max=30),
    before_sleep=_log_before_retry,
    retry_error_callback=_log_give_up,
    reraise=True,
)
