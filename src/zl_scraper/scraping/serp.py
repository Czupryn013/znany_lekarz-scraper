"""Apify SERP (Google Search Results) wrapper with batch + parallel execution."""

import asyncio
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from apify_client import ApifyClientAsync

from zl_scraper.config import (
    APIFY_ACTOR_TIMEOUT_SECS,
    APIFY_API_TOKEN,
    APIFY_CONCURRENCY,
    SERP_KEYWORDS_PER_CALL,
)
from zl_scraper.utils.logging import get_logger

logger = get_logger("serp")

SERP_ACTOR_ID = "563JCPLOqM1kMmbbP"


@dataclass
class SerpResult:
    """A single organic search result from Google."""

    url: str
    title: str
    description: str


@dataclass
class SerpResponse:
    """All results for one search term."""

    search_term: str
    results: list[SerpResult] = field(default_factory=list)


def _parse_dataset_items(items: list[dict]) -> list[SerpResponse | None]:
    """Convert raw Apify dataset items into SerpResponse objects. None for errored items."""
    responses: list[SerpResponse | None] = []
    for item in items:
        # Apify returns items with 'error' field for keywords that failed/timed-out
        if item.get("error"):
            logger.warning("SERP item error: %s (keyword=%s)", item["error"], item.get("search_term", "?"))
            responses.append(None)
            continue

        search_term = item.get("search_term") or item.get("keyword") or ""
        organic = item.get("results") or item.get("organic_results") or []
        results = [
            SerpResult(
                url=r.get("url", ""),
                title=r.get("title", ""),
                description=r.get("description", "") or r.get("snippet", ""),
            )
            for r in organic
            if r.get("url")
        ]
        responses.append(SerpResponse(search_term=search_term, results=results))
    return responses


async def _run_single_serp(
    client: ApifyClientAsync,
    keywords: list[str],
    results_per_keyword: int = 10,
) -> list[SerpResponse | None] | None:
    """Run one Apify SERP actor. Returns None on total failure, or list with per-item None for errored keywords."""
    # Sanitise keywords: collapse whitespace so embedded newlines don't split batches
    clean_keywords = [" ".join(kw.split()) for kw in keywords]
    keyword_str = "\n".join(clean_keywords)

    run_input = {
        "country": "PL",
        "cr": "countryPL",
        "gl": "PL",
        "hl": "pl",
        "include_merged": False,
        "keyword": keyword_str,
        "limit": str(results_per_keyword),
        "lr": "lang_pl",
    }

    try:
        logger.info("Starting SERP actor with %d keywords", len(keywords))
        run = await client.actor(SERP_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=APIFY_ACTOR_TIMEOUT_SECS,
        )

        if run is None:
            logger.error("SERP actor .call() returned None")
            return None

        run_status = run.get("status")
        if run_status not in ("SUCCEEDED", None):
            logger.warning("SERP actor run finished with status=%s", run_status)
            return None

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logger.error("SERP actor run returned no dataset ID")
            return None

        items = []
        async for item in client.dataset(dataset_id).iterate_items():
            items.append(item)

        logger.info(
            "SERP actor returned %d result sets for %d keywords",
            len(items),
            len(keywords),
        )
        return _parse_dataset_items(items)

    except Exception:
        logger.exception("SERP actor call failed for %d keywords", len(keywords))
        return None


async def run_serp_search(
    keywords: list[str],
    semaphore: asyncio.Semaphore | None = None,
) -> list[SerpResponse | None]:
    """Run SERP searches for all keywords, chunked and parallelised via Apify actors."""
    if not keywords:
        return []

    if semaphore is None:
        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)

    # Chunk keywords into groups of SERP_KEYWORDS_PER_CALL
    chunks: list[list[str]] = []
    for i in range(0, len(keywords), SERP_KEYWORDS_PER_CALL):
        chunks.append(keywords[i : i + SERP_KEYWORDS_PER_CALL])

    logger.info(
        "SERP search: %d total keywords → %d chunks of ≤%d, concurrency=%d",
        len(keywords),
        len(chunks),
        SERP_KEYWORDS_PER_CALL,
        APIFY_CONCURRENCY,
    )

    client = ApifyClientAsync(token=APIFY_API_TOKEN)

    async def _run_chunk(chunk: list[str], chunk_idx: int) -> tuple[int, list[SerpResponse | None] | None]:
        async with semaphore:
            logger.info("Chunk %d/%d — launching SERP for %d keywords", chunk_idx + 1, len(chunks), len(chunk))
            responses = await _run_single_serp(client, chunk)
            return chunk_idx, responses

    tasks = [_run_chunk(chunk, i) for i, chunk in enumerate(chunks)]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten results preserving original keyword order.
    # None entries signal a failed chunk — keep them so callers can skip those rows.
    ordered: list[SerpResponse | None] = []
    succeeded = [r for r in chunk_results if not isinstance(r, Exception)]
    for chunk_idx, responses in sorted(succeeded, key=lambda r: r[0]):
        if responses is None:
            # Chunk failed — pad with None for each keyword in that chunk
            ordered.extend([None] * len(chunks[chunk_idx]))
        else:
            ordered.extend(responses)

    # Log any gather-level failures (shouldn't happen since _run_single_serp catches)
    for r in chunk_results:
        if isinstance(r, Exception):
            logger.error("SERP chunk failed unexpectedly: %s", r)

    return ordered


def dedup_results_by_domain(results: list[SerpResult]) -> list[SerpResult]:
    """Keep the highest-scored URL per domain (homepage > contact > other)."""
    by_domain: dict[str, tuple[SerpResult, int]] = {}

    for r in results:
        parsed = urlparse(r.url)
        if not parsed.hostname:
            continue

        domain = re.sub(r"^www\.", "", parsed.hostname)
        path = parsed.path.rstrip("/")

        # Score: homepage=3, contact page=2, other=1
        if not path or path == "/":
            score = 3
        elif re.search(r"kontakt|contact", path, re.IGNORECASE):
            score = 2
        else:
            score = 1

        existing = by_domain.get(domain)
        if existing is None or existing[1] < score:
            by_domain[domain] = (r, score)

    return [item[0] for item in by_domain.values()]
