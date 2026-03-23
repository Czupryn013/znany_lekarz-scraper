"""Apify LinkedIn Keyword Search actor wrapper — find company profiles by name."""

import asyncio

from apify_client import ApifyClientAsync

from zl_scraper.config import APIFY_API_TOKEN, APIFY_ACTOR_TIMEOUT_SECS
from zl_scraper.utils.logging import get_logger

logger = get_logger("linkedin_keyword_search")

KEYWORD_SEARCH_ACTOR_ID = "QwLfX9hYQXhA84LY3"


async def search_company_by_keyword(
    keyword: str,
    client: ApifyClientAsync | None = None,
    max_results: int = 5,
) -> list[dict]:
    """Search LinkedIn for companies matching a keyword, filtered to Poland."""
    if client is None:
        client = ApifyClientAsync(token=APIFY_API_TOKEN)

    run_input = {
        "keyword": keyword,
        "geo": "105072130",
        "maxResults": max_results,
    }

    try:
        logger.info("Starting keyword search actor for '%s'", keyword)
        run = await client.actor(KEYWORD_SEARCH_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=APIFY_ACTOR_TIMEOUT_SECS,
        )

        if run is None:
            logger.error("Keyword search actor returned None for '%s'", keyword)
            return []

        run_status = run.get("status")
        if run_status not in ("SUCCEEDED", None):
            logger.warning("Keyword search actor finished with status=%s", run_status)
            return []

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logger.error("Keyword search actor returned no dataset ID")
            return []

        items = []
        async for item in client.dataset(dataset_id).iterate_items():
            # Skip "not found" responses
            if item.get("message"):
                logger.info("Keyword search '%s': %s", keyword, item["message"])
                continue
            items.append(item)

        logger.info("Keyword search '%s' returned %d companies", keyword, len(items))
        return items

    except Exception:
        logger.exception("Keyword search actor failed for '%s'", keyword)
        return []
