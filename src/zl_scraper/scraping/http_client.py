"""Async httpx client factory with proxy rotation and semaphore-controlled fetching."""

import asyncio

import httpx

from zl_scraper.config import PROXY_URL, REQUEST_TIMEOUT
from zl_scraper.utils.logging import get_logger
from zl_scraper.utils.retry import retry_on_http_error

logger = get_logger("http_client")


def create_client() -> httpx.AsyncClient:
    """Return an httpx.AsyncClient configured with proxy, timeout, and headers."""
    return httpx.AsyncClient(
        proxy=PROXY_URL,
        timeout=httpx.Timeout(REQUEST_TIMEOUT),
        follow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        },
        verify=False,
    )


@retry_on_http_error
async def fetch(client: httpx.AsyncClient, url: str, semaphore: asyncio.Semaphore) -> httpx.Response:
    """Acquire semaphore, make GET request, log result, return response."""
    async with semaphore:
        logger.debug("Fetching %s", url)
        response = await client.get(url)
        if response.status_code != 200:
            logger.warning("Non-200 response: status=%d url=%s", response.status_code, url)
            response.raise_for_status()
        return response
