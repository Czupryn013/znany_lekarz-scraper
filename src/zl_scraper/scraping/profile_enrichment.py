"""Fetch a clinic's profile page + doctors count and return enriched data."""

import asyncio

import httpx

from zl_scraper.scraping.doctors import fetch_doctors_count
from zl_scraper.scraping.http_client import fetch
from zl_scraper.scraping.parsers import ProfileData, parse_profile_page
from zl_scraper.utils.logging import get_logger

logger = get_logger("profile_enrichment")


async def enrich_clinic(
    clinic_url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> tuple[ProfileData | None, int]:
    """Fetch profile HTML + doctors endpoint concurrently, return (profile_data, doctors_count)."""
    # Fetch profile page
    try:
        profile_response = await fetch(client, clinic_url, semaphore)
    except httpx.HTTPError as e:
        logger.error("Failed to fetch profile for %s: %s", clinic_url, e)
        return None, 0

    profile_data = parse_profile_page(profile_response.text)

    # Fetch doctors count if we have a profile ID
    doctors_count = 0
    if profile_data.zl_profile_id:
        doctors_count = await fetch_doctors_count(profile_data.zl_profile_id, client, semaphore)

    return profile_data, doctors_count
