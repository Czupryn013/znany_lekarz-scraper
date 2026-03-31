"""FullEnrich Company Search API step for company LinkedIn discovery."""

import asyncio
from datetime import datetime

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic
from zl_scraper.scraping.fullenrich import search_company
from zl_scraper.utils.logging import get_logger

logger = get_logger("company_enrich.fe_search")

FE_RATE_LIMIT_INTERVAL = 1.0  # minimum seconds between FE API calls


def _get_clinics_needing_linkedin(
    session, limit: int | None, icp_only: bool, retry_serp: bool,
) -> list[Clinic]:
    """Query clinics with a domain but no linkedin_url yet."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.website_domain.isnot(None),
            Clinic.linkedin_url.is_(None),
        )
        .order_by(Clinic.id)
    )
    if not retry_serp:
        query = query.filter(Clinic.linkedin_searched_at.is_(None))
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


async def run_fe_company_search(
    limit: int | None = None,
    icp_only: bool = True,
    retry_serp: bool = False,
    skip_location: bool = False,
) -> dict:
    """Search FullEnrich for company LinkedIn URLs for clinics without one."""
    logger.info("Starting FE company LinkedIn search (icp_only=%s, retry_serp=%s, skip_location=%s)", icp_only, retry_serp, skip_location)

    session = SessionLocal()
    try:
        clinics = _get_clinics_needing_linkedin(session, limit, icp_only, retry_serp)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics need FE company LinkedIn search — nothing to do")
            return {"found": 0, "not_found": 0, "errors": 0, "total": 0}

        logger.info("Found %d clinics to search via FullEnrich Company Search", total)

        found = 0
        not_found = 0
        errors = 0

        for i, clinic in enumerate(clinics, 1):
            domain = clinic.website_domain
            name = clinic.name or ""

            try:
                result = await asyncio.to_thread(search_company, name, domain, not skip_location)
            except Exception:
                logger.exception(
                    "FE company search failed for clinic #%d (%s)",
                    clinic.id, clinic.name,
                )
                errors += 1
                continue

            if result and result.get("linkedin_url"):
                clinic.linkedin_url = result["linkedin_url"]
                found += 1
                logger.info(
                    "FE found: #%d %s → %s", clinic.id, clinic.name, result["linkedin_url"],
                )
            else:
                not_found += 1

            clinic.linkedin_searched_at = datetime.utcnow()

            # Rate limit: ~60/min
            await asyncio.sleep(FE_RATE_LIMIT_INTERVAL)

            if i % 50 == 0:
                session.commit()
                logger.info("Progress: %d / %d (found=%d)", i, total, found)

        session.commit()
        logger.info(
            "FE company search done: %d found, %d not found, %d errors out of %d",
            found, not_found, errors, total,
        )
        return {"found": found, "not_found": not_found, "errors": errors, "total": total}

    except Exception:
        session.rollback()
        logger.exception("Error during FE company search")
        raise
    finally:
        session.close()
