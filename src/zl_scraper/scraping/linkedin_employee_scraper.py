"""Apify LinkedIn Employee List scraper wrapper — extract employees from company pages."""

import asyncio

from apify_client import ApifyClientAsync

from zl_scraper.config import (
    APIFY_API_TOKEN,
    APIFY_ACTOR_TIMEOUT_SECS,
    MAX_EMPLOYEES_PER_COMPANY,
)
from zl_scraper.utils.logging import get_logger

logger = get_logger("linkedin_employee_scraper")

EMPLOYEE_LIST_ACTOR_ID = "Vb6LZkh4EqRlR0Ka9"


def parse_employee(item: dict) -> dict:
    """Extract structured fields from a raw Apify employee item."""
    positions = item.get("currentPositions") or []
    first_pos = positions[0] if positions else {}

    return {
        "linkedin_url": item.get("linkedinUrl", ""),
        "full_name": f"{item.get('firstName', '')} {item.get('lastName', '')}".strip(),
        "position_title": first_pos.get("title"),
        "company_name": first_pos.get("companyName"),
        "raw_profile": item,
    }


async def scrape_company_employees(
    company_linkedin_url: str,
    client: ApifyClientAsync | None = None,
    max_employees: int | None = None,
) -> list[dict]:
    """Scrape employees from a LinkedIn company page (full mode)."""
    if client is None:
        client = ApifyClientAsync(token=APIFY_API_TOKEN)

    limit = max_employees or MAX_EMPLOYEES_PER_COMPANY

    run_input = {
        "startUrls": [company_linkedin_url],
        "maxEmployees": limit,
    }

    try:
        logger.info(
            "Starting employee scraper for %s (max=%d)",
            company_linkedin_url,
            limit,
        )
        run = await client.actor(EMPLOYEE_LIST_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=APIFY_ACTOR_TIMEOUT_SECS,
        )

        if run is None:
            logger.error("Employee scraper returned None for %s", company_linkedin_url)
            return []

        run_status = run.get("status")
        if run_status not in ("SUCCEEDED", None):
            logger.warning(
                "Employee scraper finished with status=%s for %s",
                run_status,
                company_linkedin_url,
            )
            return []

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logger.error("Employee scraper returned no dataset ID")
            return []

        items = []
        async for item in client.dataset(dataset_id).iterate_items():
            items.append(item)

        logger.info(
            "Employee scraper returned %d employees for %s",
            len(items),
            company_linkedin_url,
        )

        # Extract total count from _meta if available
        if items:
            meta = items[0].get("_meta", {}).get("pagination", {})
            total = meta.get("totalElements")
            if total:
                logger.info(
                    "Company %s has %d total employees on LinkedIn",
                    company_linkedin_url,
                    total,
                )

        return [parse_employee(item) for item in items if item.get("linkedinUrl")]

    except Exception:
        logger.exception("Employee scraper failed for %s", company_linkedin_url)
        return []
