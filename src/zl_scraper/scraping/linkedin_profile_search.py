"""Apify LinkedIn Profile Search By Name wrapper (actor CP1SVZfEwWflrmWCX)."""

import asyncio

from apify_client import ApifyClientAsync

from zl_scraper.config import APIFY_API_TOKEN, APIFY_ACTOR_TIMEOUT_SECS
from zl_scraper.utils.logging import get_logger

logger = get_logger("linkedin_profile_search")

PROFILE_SEARCH_ACTOR_ID = "CP1SVZfEwWflrmWCX"

# Healthcare-related LinkedIn industry IDs (from n8n workflow)
HEALTHCARE_INDUSTRY_IDS = [
    "14", "2115", "2112", "2081", "88", "2125", "13", "2077",
    "2048", "2045", "2060", "2074", "2069", "139", "2050", "2063",
    "2054", "2040",
]


async def search_profiles_by_name(
    first_name: str,
    last_name: str,
    locations: list[str] | None = None,
    industry_ids: list[str] | None = None,
    max_items: int = 10,
    scraper_mode: str = "Full",
    client: ApifyClientAsync | None = None,
) -> list[dict]:
    """Search LinkedIn profiles by name via Apify actor.

    Returns a list of profile dicts from the dataset.
    """
    if locations is None:
        locations = ["Poland"]

    run_input: dict = {
        "firstName": first_name,
        "lastName": last_name,
        "locations": locations,
        "maxItems": max_items,
        "maxPages": 10,
        "profileScraperMode": scraper_mode,
    }
    if industry_ids:
        run_input["industryIds"] = industry_ids

    _client = client or ApifyClientAsync(token=APIFY_API_TOKEN)
    try:
        logger.info(
            "Searching LinkedIn profiles: %s %s (industries=%s, max=%d)",
            first_name, last_name,
            "yes" if industry_ids else "no",
            max_items,
        )

        run = await _client.actor(PROFILE_SEARCH_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=APIFY_ACTOR_TIMEOUT_SECS,
        )

        if run is None:
            logger.error("Profile search actor returned None")
            return []

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logger.error("Profile search actor returned no dataset ID")
            return []

        items: list[dict] = []
        async for item in _client.dataset(dataset_id).iterate_items():
            items.append(item)

        logger.info(
            "Profile search for %s %s returned %d results",
            first_name, last_name, len(items),
        )
        return items

    except Exception:
        logger.exception("Profile search failed for %s %s", first_name, last_name)
        return []
