"""FullEnrich People Search API step for personal LinkedIn discovery."""

import asyncio
import time
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Lead, lead_clinic_roles
from zl_scraper.scraping.fullenrich import search_person
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.fe_search")

MAX_AGE = 75
FE_RATE_LIMIT_INTERVAL = 1.0  # minimum seconds between FE API calls (60/min)


def _get_lead_company_domain(session: Session, lead_id: int) -> str | None:
    """Get the first associated clinic's website_domain for a lead."""
    row = (
        session.query(Clinic.website_domain)
        .join(lead_clinic_roles, Clinic.id == lead_clinic_roles.c.clinic_id)
        .filter(
            lead_clinic_roles.c.lead_id == lead_id,
            Clinic.website_domain.isnot(None),
        )
        .first()
    )
    return row.website_domain if row else None


async def run_fe_batch(lead_ids: list[int]) -> dict:
    """Run FE search for a batch of leads (by ID). Sequential with 60/min rate limit."""
    session = SessionLocal()
    try:
        leads = session.query(Lead).filter(Lead.id.in_(lead_ids)).order_by(Lead.id).all()
        # Only leads still without linkedin_url
        leads = [l for l in leads if l.linkedin_url is None]

        if not leads:
            return {"yes": 0, "maybe": 0, "no": 0, "failed_ids": set()}

        found = 0
        not_found = 0
        failed_ids: set[int] = set()

        for lead in leads:
            from zl_scraper.pipeline.personal_linkedin.serp import _age_from_pesel

            age = _age_from_pesel(lead.pesel)
            if age is not None and age > MAX_AGE:
                continue

            domain = _get_lead_company_domain(session, lead.id)
            if not domain:
                continue

            try:
                result = await asyncio.to_thread(search_person, lead.full_name, domain)
            except Exception:
                logger.exception("FE search failed for lead #%d (%s)", lead.id, lead.full_name)
                failed_ids.add(lead.id)
                continue

            if result and result.get("linkedin_url"):
                lead.linkedin_url = result["linkedin_url"]
                found += 1
                logger.info("FE found: #%d %s → %s", lead.id, lead.full_name, result["linkedin_url"])
            else:
                not_found += 1

            # Rate limit: ~60/min
            # await asyncio.sleep(FE_RATE_LIMIT_INTERVAL)

        session.commit()
        logger.info("FE batch done: %d found, %d not found, %d errors", found, not_found, len(failed_ids))
        return {"yes": found, "maybe": 0, "no": not_found, "failed_ids": failed_ids}
    except Exception:
        session.rollback()
        logger.exception("Error during FE batch")
        raise
    finally:
        session.close()
