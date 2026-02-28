"""Fetch a clinic's profile page + doctors list and return enriched data."""

import asyncio

import httpx

from zl_scraper.scraping.doctors import fetch_doctors
from zl_scraper.scraping.http_client import WaterfallClient
from zl_scraper.scraping.parsers import DoctorData, ProfileData, parse_profile_page
from zl_scraper.utils.logging import get_logger

logger = get_logger("profile_enrichment")


async def enrich_clinic(
    clinic_url: str,
    profile_client: WaterfallClient,
    doctors_client: WaterfallClient,
    semaphore: asyncio.Semaphore,
) -> tuple[ProfileData | None, list[DoctorData]]:
    """Fetch profile HTML + doctors JSON (both via waterfall), return (profile_data, doctors_list)."""
    # Fetch profile page — routed through the waterfall client
    try:
        profile_response = await profile_client.fetch(clinic_url, semaphore)
    except httpx.HTTPError as e:
        logger.error("Failed to fetch profile for %s: %s", clinic_url, e)
        return None, []

    profile_data = parse_profile_page(profile_response.text)

    # Fetch doctors list — also routed through a waterfall client
    doctors: list[DoctorData] = []
    if profile_data.zl_profile_id:
        doctors = await fetch_doctors(profile_data.zl_profile_id, doctors_client, semaphore)

    return profile_data, doctors
