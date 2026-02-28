"""Async sliding-window rate limiter for proxy request throttling."""

import asyncio
import time
from collections import deque

from zl_scraper.utils.logging import get_logger

logger = get_logger("rate_limiter")


class RateLimiter:
    """Limit throughput to max_requests per window_seconds using a sliding window."""

    def __init__(self, max_requests: int, window_seconds: float = 60.0) -> None:
        self._max_requests = max_requests
        self._window = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request slot is available, then record the timestamp."""
        while True:
            async with self._lock:
                now = time.monotonic()

                # Purge timestamps outside the sliding window
                while self._timestamps and self._timestamps[0] <= now - self._window:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._max_requests:
                    self._timestamps.append(now)
                    return

                # Calculate how long to wait for the oldest slot to expire
                sleep_for = self._timestamps[0] - (now - self._window)

            # Sleep OUTSIDE the lock so other coroutines can proceed
            if sleep_for > 0:
                logger.debug("Rate limit reached (%d/%d) — waiting %.2fs", self._max_requests, self._max_requests, sleep_for)
                await asyncio.sleep(sleep_for)
