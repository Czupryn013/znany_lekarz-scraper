"""Apify SERP search via the free Google search results actor."""

import asyncio
from collections import defaultdict

from apify_client import ApifyClientAsync

from zl_scraper.config import APIFY_API_TOKEN, APIFY_ACTOR_TIMEOUT_SECS, APIFY_CONCURRENCY
from zl_scraper.scraping.serp import SerpResponse, SerpResult, dedup_results_by_domain
from zl_scraper.utils.logging import get_logger

logger = get_logger("company_serp")

# Free Google Search Results SERP actor (cheap, reliable)
SERP_ACTOR_ID = "s-r/free-google-search-results-serp---only-0-25-per-1-000-results"

# Queries sent per single actor call
QUERIES_PER_BATCH = 10

# Max organic results returned per query
MAX_RESULTS_PER_QUERY = 5


async def _run_actor_batch(
    client: ApifyClientAsync,
    queries: list[str],
    semaphore: asyncio.Semaphore,
    max_results: int | None = None,
) -> list[dict] | None:
    """Run the SERP actor for a batch of queries and return raw flat result items."""
    async with semaphore:
        run_input = {
            "country": "pl",
            "currentUsage": 0,
            "isPaidSubscription": False,
            "maxResults": max_results or MAX_RESULTS_PER_QUERY,
            "queries": queries,
        }

        try:
            logger.info("Starting SERP actor with %d queries", len(queries))
            run = await client.actor(SERP_ACTOR_ID).call(
                run_input=run_input,
                timeout_secs=APIFY_ACTOR_TIMEOUT_SECS,
            )

            if not run:
                logger.error("SERP actor .call() returned None")
                return None

            run_status = run.get("status")
            if run_status not in ("SUCCEEDED", None):
                logger.warning("SERP actor finished with status=%s", run_status)
                return None

            dataset_id = run.get("defaultDatasetId")
            if not dataset_id:
                logger.error("SERP actor returned no dataset ID")
                return None

            items: list[dict] = []
            async for item in client.dataset(dataset_id).iterate_items():
                items.append(item)

            logger.info(
                "SERP actor returned %d result items for %d queries",
                len(items),
                len(queries),
            )
            return items

        except Exception:
            logger.exception("SERP actor call failed for %d queries", len(queries))
            return None


def _group_items_by_keyword(
    items: list[dict],
    queries: list[str],
) -> list[SerpResponse | None]:
    """Group flat result items by their keyword field into one SerpResponse per original query."""
    by_keyword: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        keyword = item.get("keyword", "")
        by_keyword[keyword].append(item)

    # Also index by whitespace-collapsed keyword for fuzzy matching
    by_keyword_clean: dict[str, list[dict]] = defaultdict(list)
    for kw, results in by_keyword.items():
        by_keyword_clean[" ".join(kw.split())].extend(results)

    responses: list[SerpResponse | None] = []
    for query in queries:
        clean_query = " ".join(query.split())

        raw_results = by_keyword.get(query) or by_keyword_clean.get(clean_query) or []

        results = [
            SerpResult(
                url=r.get("url", ""),
                title=r.get("title", ""),
                description=r.get("snippet", "") or r.get("description", ""),
            )
            for r in raw_results
            if r.get("url")
        ]
        responses.append(SerpResponse(search_term=clean_query, results=results))

    return responses


async def run_serp_search(
    keywords: list[str],
    semaphore: asyncio.Semaphore | None = None,
    max_results: int | None = None,
) -> list[SerpResponse | None]:
    """Run SERP searches for all keywords, chunked and parallelised via Apify actors."""
    if not keywords:
        return []

    if semaphore is None:
        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)

    # Chunk keywords into batches
    chunks: list[list[str]] = []
    for i in range(0, len(keywords), QUERIES_PER_BATCH):
        chunks.append(keywords[i : i + QUERIES_PER_BATCH])

    logger.info(
        "SERP search: %d keywords → %d batches of ≤%d, concurrency=%d",
        len(keywords),
        len(chunks),
        QUERIES_PER_BATCH,
        APIFY_CONCURRENCY,
    )

    client = ApifyClientAsync(token=APIFY_API_TOKEN)

    async def _run_chunk(chunk: list[str], idx: int) -> tuple[int, list[dict] | None]:
        logger.info("Batch %d/%d — launching SERP for %d queries", idx + 1, len(chunks), len(chunk))
        items = await _run_actor_batch(client, chunk, semaphore, max_results=max_results)
        return idx, items

    tasks = [_run_chunk(chunk, i) for i, chunk in enumerate(chunks)]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten results preserving original keyword order
    ordered: list[SerpResponse | None] = []
    succeeded = [r for r in chunk_results if not isinstance(r, Exception)]
    for chunk_idx, items in sorted(succeeded, key=lambda r: r[0]):
        chunk = chunks[chunk_idx]
        if items is None:
            # Chunk failed — pad with None for each keyword
            ordered.extend([None] * len(chunk))
        else:
            grouped = _group_items_by_keyword(items, chunk)
            ordered.extend(grouped)

    # Log gather-level failures
    for r in chunk_results:
        if isinstance(r, Exception):
            logger.error("SERP batch failed unexpectedly: %s", r)

    return ordered


__all__ = [
    "SerpResult",
    "SerpResponse",
    "dedup_results_by_domain",
    "run_serp_search",
]
