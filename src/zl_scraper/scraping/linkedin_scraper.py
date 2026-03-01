"""Apify LinkedIn Company Details scraper wrapper with parallel execution."""

import asyncio

from apify_client import ApifyClientAsync

from zl_scraper.config import APIFY_API_TOKEN, APIFY_CONCURRENCY
from zl_scraper.utils.logging import get_logger

logger = get_logger("linkedin_scraper")

LINKEDIN_ACTOR_ID = "UwSdACBp7ymaGUJjS"


async def _scrape_single_batch(
    client: ApifyClientAsync,
    urls: list[str],
) -> list[dict]:
    """Run one Apify LinkedIn Company Details actor for a batch of URLs."""
    run_input = {"companies": urls}

    logger.info("Starting LinkedIn detail actor for %d URLs", len(urls))
    run = await client.actor(LINKEDIN_ACTOR_ID).call(run_input=run_input)

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        logger.error("LinkedIn actor run returned no dataset ID")
        return []

    items = []
    async for item in client.dataset(dataset_id).iterate_items():
        items.append(item)

    logger.info("LinkedIn actor returned %d profiles", len(items))
    return items


async def scrape_linkedin_companies(
    urls: list[str],
    semaphore: asyncio.Semaphore | None = None,
) -> list[dict]:
    """Scrape LinkedIn company details for all URLs, parallelised up to APIFY_CONCURRENCY."""
    if not urls:
        return []

    if semaphore is None:
        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)

    logger.info("Scraping %d LinkedIn company profiles (concurrency=%d)", len(urls), APIFY_CONCURRENCY)

    client = ApifyClientAsync(token=APIFY_API_TOKEN)

    async def _run_one(url: str, idx: int) -> tuple[int, list[dict]]:
        async with semaphore:
            logger.info("LinkedIn detail %d/%d — %s", idx + 1, len(urls), url)
            profiles = await _scrape_single_batch(client, [url])
            return idx, profiles

    tasks = [_run_one(url, i) for i, url in enumerate(urls)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten preserving order
    ordered: list[dict] = []
    for result in sorted(
        (r for r in results if not isinstance(r, Exception)),
        key=lambda r: r[0],
    ):
        _, profiles = result
        ordered.extend(profiles)

    for result in results:
        if isinstance(result, Exception):
            logger.error("LinkedIn detail scrape failed: %s", result)

    return ordered
