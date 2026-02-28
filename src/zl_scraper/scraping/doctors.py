"""Fetch the doctors endpoint for a clinic and parse the results."""

import asyncio

import httpx

from zl_scraper.scraping.http_client import WaterfallClient
from zl_scraper.scraping.parsers import DoctorData, parse_doctors_response
from zl_scraper.utils.logging import get_logger

logger = get_logger("doctors")

DOCTORS_URL_TEMPLATE = (
    "https://www.znanylekarz.pl/facility/{profile_id}/profile/doctors"
    "?filters%5BisSearchIndexable%5D=true"
)


async def fetch_doctors(
    profile_id: str,
    client: WaterfallClient,
    semaphore: asyncio.Semaphore,
) -> list[DoctorData]:
    """Fetch the doctors JSON endpoint via waterfall and return the list of doctors."""
    url = DOCTORS_URL_TEMPLATE.format(profile_id=profile_id)
    try:
        response = await client.fetch(url, semaphore)
        return parse_doctors_response(response.text)
    except httpx.HTTPError as e:
        logger.error("Failed to fetch doctors for profile_id=%s: %s", profile_id, e)
        return []
