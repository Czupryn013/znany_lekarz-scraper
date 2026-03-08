"""Prospeo bulk person enrichment API client."""

import httpx

from zl_scraper.config import PROSPEO_API_KEY
from zl_scraper.utils.logging import get_logger

logger = get_logger("prospeo")

PROSPEO_BULK_URL = "https://api.prospeo.io/bulk-enrich-person"

# Dummy lead injected into every batch to prevent API errors on zero matches
_DUMMY_LEAD = {
    "identifier": "9999999",
    "full_name": "Lena Garnik",
    "company_name": "Centrum Medyczne Gamma",
    "company_website": "cmgamma.pl",
    "linkedin_url": "",
}


def enrich_bulk(leads: list[dict]) -> dict:
    """Call Prospeo bulk-enrich-person and return the raw JSON response.

    A dummy lead is always injected to avoid API errors when no real leads
    match; it is stripped from the response before returning.

    Args:
        leads: list of dicts, each with keys:
            identifier (str), full_name, company_website, company_name,
            linkedin_url (optional).

    Returns:
        Raw API response dict with 'matched' list (dummy lead excluded).
    """
    if not leads:
        return {"matched": []}

    real_count = len(leads)
    logger.info("Prospeo: enriching %d leads", real_count)

    payload = [_DUMMY_LEAD] + leads

    resp = httpx.post(
        PROSPEO_BULK_URL,
        headers={
            "Content-Type": "application/json",
            "X-KEY": PROSPEO_API_KEY,
        },
        json={
            "only_verified_mobile": True,
            "data": payload,
        },
        timeout=120,
    )
    if resp.is_error:
        logger.error(
            "Prospeo API error %d: %s", resp.status_code, resp.text,
        )
    resp.raise_for_status()
    data = resp.json()

    # Strip the dummy lead from matched results
    data["matched"] = [
        item for item in data.get("matched", [])
        if item.get("identifier") != _DUMMY_LEAD["identifier"]
    ]

    matched = data["matched"]
    logger.info("Prospeo: %d/%d matched", len(matched), real_count)
    return data


def parse_prospeo_result(item: dict) -> dict:
    """Extract phone, email, linkedin_url from a single Prospeo matched item."""
    person = item.get("person", {})
    mobile_info = person.get("mobile", {})
    email_info = person.get("email", {})

    return {
        "identifier": item.get("identifier"),
        "phone": mobile_info.get("mobile") or None,
        "email": email_info.get("email") or None,
        "linkedin_url": person.get("linkedin_url") or None,
    }
