"""FullEnrich People Search API step for personal LinkedIn discovery."""

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Lead, lead_clinic_roles
from zl_scraper.scraping.fullenrich import search_person
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.fe_search")

MAX_AGE = 75


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


def _get_leads_for_fe_search(
    session: Session,
    limit: Optional[int] = None,
) -> list[Lead]:
    """Get leads still without linkedin_url and not yet fully searched, with a company domain."""
    leads = (
        session.query(Lead)
        .filter(
            Lead.linkedin_url.is_(None),
            Lead.linkedin_searched_at.is_(None),
            Lead.pesel.isnot(None),
        )
        .order_by(Lead.id)
        .all()
    )

    # Filter: must have company domain and age ≤ 75
    from zl_scraper.pipeline.personal_linkedin.serp import _age_from_pesel

    filtered = []
    for lead in leads:
        age = _age_from_pesel(lead.pesel)
        if age is not None and age > MAX_AGE:
            continue
        domain = _get_lead_company_domain(session, lead.id)
        if domain:
            filtered.append((lead, domain))

    if limit is not None:
        filtered = filtered[:limit]

    return filtered


def run_fe_search_step(limit: Optional[int] = None) -> dict:
    """Find LinkedIn profiles for leads via FullEnrich People Search API.

    Returns dict with keys: yes, maybe, no.
    """
    logger.info("Starting personal LinkedIn FE search (limit=%s)", limit)

    session = SessionLocal()
    try:
        lead_domains = _get_leads_for_fe_search(session, limit)

        if not lead_domains:
            logger.info("No leads need FE LinkedIn search")
            return {"yes": 0, "maybe": 0, "no": 0}

        logger.info("Found %d leads for FE LinkedIn search", len(lead_domains))

        found = 0
        not_found = 0

        for lead, domain in lead_domains:
            try:
                result = search_person(lead.full_name, domain)
            except Exception:
                logger.exception("FE search failed for lead #%d (%s)", lead.id, lead.full_name)
                continue

            if result and result.get("linkedin_url"):
                lead.linkedin_url = result["linkedin_url"]
                found += 1
                logger.info(
                    "FE search found: #%d %s → %s",
                    lead.id, lead.full_name, result["linkedin_url"],
                )
            else:
                not_found += 1

            session.commit()

        logger.info(
            "FE search complete: %d found, %d not found",
            found, not_found,
        )
        return {"yes": found, "maybe": 0, "no": not_found}
    except Exception:
        session.rollback()
        logger.exception("Error during FE LinkedIn search")
        raise
    finally:
        session.close()
