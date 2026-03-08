"""Lusha person enrichment API client."""

import httpx

from zl_scraper.config import LUSHA_API_KEY
from zl_scraper.utils.logging import get_logger

logger = get_logger("lusha")

LUSHA_PERSON_URL = "https://api.lusha.com/v2/person"


def enrich_bulk(contacts: list[dict]) -> dict:
    """Call Lusha person API and return the raw JSON response.

    Args:
        contacts: list of dicts, each with keys:
            contactId (str), fullName, companies (list of {domain, name, isCurrent}),
            linkedinUrl (optional).

    Returns:
        Raw API response dict with 'contacts' mapping contactId → result.
    """
    if not contacts:
        return {"contacts": {}}

    logger.info("Lusha: enriching %d contacts", len(contacts))

    resp = httpx.post(
        LUSHA_PERSON_URL,
        headers={
            "api_key": LUSHA_API_KEY,
            "Content-Type": "application/json",
        },
        json={
            "contacts": contacts,
            "metadata": {"revealEmails": False},
        },
        timeout=120,
    )
    if resp.is_error:
        logger.error(
            "Lusha API error %d: %s", resp.status_code, resp.text,
        )
    resp.raise_for_status()
    data = resp.json()

    contacts_map = data.get("contacts", {})
    found = sum(1 for v in contacts_map.values() if not v.get("error"))
    logger.info("Lusha: %d/%d contacts returned data", found, len(contacts))
    return data


def parse_lusha_result(contact_id: str, result: dict) -> dict:
    """Extract phone, linkedin_url from a single Lusha contact result."""
    data = result.get("data", {})

    # Phones
    phone = None
    phones = data.get("phones", [])
    if phones:
        phone = phones[0] if isinstance(phones[0], str) else phones[0].get("number")

    # LinkedIn
    linkedin_url = None
    social_links = data.get("socialLinks", {})
    if social_links.get("linkedin"):
        linkedin_url = social_links["linkedin"]

    return {
        "contact_id": contact_id,
        "phone": phone,
        "linkedin_url": linkedin_url,
    }
