"""Async httpx waterfall client — try datacenter → residential → web unlocker, escalating on failure."""

import asyncio

import httpx

from zl_scraper.config import (
    DATACENTER_PROXY_URL,
    DATACENTER_RATE_LIMIT,
    REQUEST_TIMEOUT,
    RESIDENTIAL_PROXY_URL,
    RESIDENTIAL_RATE_LIMIT,
    WEB_UNLOCKER_RATE_LIMIT,
    WEB_UNLOCKER_URL,
)
from zl_scraper.utils.logging import get_logger, tier_tag
from zl_scraper.utils.rate_limiter import RateLimiter

logger = get_logger("http_client")

# ── Proxy tiers (ordered cheapest → most expensive) ─────────────────────
PROXY_TIERS: list[dict] = [
    {"name": "datacenter",   "url": DATACENTER_PROXY_URL,   "limiter": RateLimiter(max_requests=DATACENTER_RATE_LIMIT, window_seconds=60.0)},
    {"name": "residential",  "url": RESIDENTIAL_PROXY_URL,  "limiter": RateLimiter(max_requests=RESIDENTIAL_RATE_LIMIT, window_seconds=60.0)},
    {"name": "unlocker",     "url": WEB_UNLOCKER_URL,       "limiter": RateLimiter(max_requests=WEB_UNLOCKER_RATE_LIMIT, window_seconds=60.0)},
]

# Map tier names to indices for --proxy-level flag
TIER_INDEX = {t["name"]: i for i, t in enumerate(PROXY_TIERS)}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}


def _make_client(proxy_url: str | None) -> httpx.AsyncClient:
    """Build an httpx.AsyncClient with the given proxy (or direct if None)."""
    return httpx.AsyncClient(
        proxy=proxy_url,
        timeout=httpx.Timeout(REQUEST_TIMEOUT),
        follow_redirects=True,
        headers=_HEADERS,
        verify=False,
    )


class WaterfallClient:
    """Manages a pool of proxy-tier clients and escalates on failure.

    Usage::

        async with WaterfallClient(start_tier="datacenter") as wf:
            response = await wf.fetch(url, semaphore)
    """

    def __init__(self, start_tier: str = "datacenter") -> None:
        idx = TIER_INDEX.get(start_tier, 0)
        self._tiers = PROXY_TIERS[idx:]
        self._clients: list[httpx.AsyncClient] = []
        logger.info(
            "Waterfall client — tiers: %s",
            " → ".join(tier_tag(t["name"]) for t in self._tiers),
        )

    async def __aenter__(self) -> "WaterfallClient":
        """Open an httpx client per tier."""
        for tier in self._tiers:
            self._clients.append(_make_client(tier["url"]))
        return self

    async def __aexit__(self, *exc) -> None:
        """Close all tier clients."""
        for c in self._clients:
            await c.aclose()
        self._clients.clear()

    async def _cooldown_before_next(self, failed_tier: dict) -> None:
        """Sleep 5 s before escalating to unlocker tier."""
        idx = self._tiers.index(failed_tier)
        if idx + 1 < len(self._tiers) and self._tiers[idx + 1]["name"] == "unlocker":
            logger.info("[dim]Cooling down 5 s before escalating to[/] %s[dim]…[/]", tier_tag("unlocker"))
            await asyncio.sleep(5)

    async def _cooldown_after_unlocker(self, tier_name: str) -> None:
        """Sleep 20 s after a successful unlocker request to avoid follow-up bans."""
        if tier_name == "unlocker":
            logger.info("[dim]Cooling down 20 s after[/] %s [dim]request…[/]", tier_tag("unlocker"))
            await asyncio.sleep(20)

    async def fetch(self, url: str, semaphore: asyncio.Semaphore) -> httpx.Response:
        """Try each tier in order; escalate to the next on non-200 / network error."""
        async with semaphore:
            last_exc: Exception | None = None
            for tier, client in zip(self._tiers, self._clients):
                limiter: RateLimiter = tier["limiter"]
                await limiter.acquire()

                try:
                    logger.debug("Fetching %s (tier=%s)", url, tier["name"])
                    response = await client.get(url)
                    if response.status_code == 200:
                        response._proxy_tier = tier["name"]  # type: ignore[attr-defined]
                        await self._cooldown_after_unlocker(tier["name"])
                        return response

                    logger.warning(
                        "%s returned status=[bold]%d[/] for %s — [yellow]escalating[/]",
                        tier_tag(tier["name"]), response.status_code, url,
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"{response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    await self._cooldown_before_next(tier)
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    logger.warning("%s failed for %s: %s — [yellow]escalating[/]", tier_tag(tier["name"]), url, e)
                    last_exc = e
                    await self._cooldown_before_next(tier)

            # All tiers exhausted
            logger.error("[bold red]All proxy tiers exhausted[/] for %s", url)
            raise last_exc  # type: ignore[misc]


# ── Backwards-compatible thin wrappers (used by doctors API etc.) ────────

def create_client(
    proxy_mode: str | None = None,
    **_kwargs,
) -> httpx.AsyncClient:
    """Create a single-tier httpx client. Use WaterfallClient for discovery/enrichment."""
    tier_name = proxy_mode or "residential"
    idx = TIER_INDEX.get(tier_name, 1)
    proxy_url = PROXY_TIERS[idx]["url"] if tier_name != "none" else None

    label = tier_name
    if proxy_url:
        logger.info("Creating client — tier=%s proxy=%s", label, proxy_url)
    else:
        logger.info("Creating client — proxy disabled (direct)")

    client = _make_client(proxy_url)
    client._proxy_tier = tier_name  # type: ignore[attr-defined]
    return client


async def fetch_single(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
) -> httpx.Response:
    """Simple fetch for a single-tier client — acquires semaphore + tier rate-limiter."""
    tier_name = getattr(client, "_proxy_tier", "residential")
    idx = TIER_INDEX.get(tier_name, 1)
    limiter: RateLimiter = PROXY_TIERS[idx]["limiter"]

    async with semaphore:
        await limiter.acquire()
        logger.debug("Fetching %s (single-tier=%s)", url, tier_name)
        return await client.get(url)
