"""Thin wrapper around the shared SERP module for company enrichment searches."""

from zl_scraper.scraping.serp import (
    SerpResponse,
    SerpResult,
    dedup_results_by_domain,
    run_serp_search as _run_serp_search,
)

import asyncio

from zl_scraper.config import SERP_CONCURRENCY

# Max organic results returned per query (company enrichment default)
MAX_RESULTS_PER_QUERY = 5

# Queries sent per single actor call
QUERIES_PER_BATCH = 10


async def run_serp_search(
    keywords: list[str],
    semaphore: asyncio.Semaphore | None = None,
    max_results: int | None = None,
) -> list[SerpResponse | None]:
    """Run SERP searches for company enrichment keywords."""
    if semaphore is None:
        semaphore = asyncio.Semaphore(SERP_CONCURRENCY)
    return await _run_serp_search(
        keywords,
        semaphore=semaphore,
        keywords_per_call=QUERIES_PER_BATCH,
        results_per_keyword=max_results or MAX_RESULTS_PER_QUERY,
    )


__all__ = [
    "SerpResult",
    "SerpResponse",
    "dedup_results_by_domain",
    "run_serp_search",
]
