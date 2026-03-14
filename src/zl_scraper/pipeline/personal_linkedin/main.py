"""Orchestrator for the personal LinkedIn discovery pipeline."""

import asyncio
from datetime import datetime
from typing import Optional

from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.main")


def _mark_searched(lead_ids: list[int]) -> None:
    """Bulk-set linkedin_searched_at on the given leads."""
    if not lead_ids:
        return
    from zl_scraper.db.engine import SessionLocal
    from zl_scraper.db.models import Lead

    session = SessionLocal()
    try:
        session.query(Lead).filter(Lead.id.in_(lead_ids)).update(
            {Lead.linkedin_searched_at: datetime.utcnow()},
            synchronize_session=False,
        )
        session.commit()
        logger.info("Marked %d leads as linkedin_searched", len(lead_ids))
    finally:
        session.close()


def _get_unsearched_lead_ids(limit: int | None = None) -> list[int]:
    """Return IDs of leads that haven't been through the LinkedIn pipeline yet."""
    from zl_scraper.db.engine import SessionLocal
    from zl_scraper.db.models import Lead

    session = SessionLocal()
    try:
        q = (
            session.query(Lead.id)
            .filter(Lead.linkedin_searched_at.is_(None), Lead.pesel.isnot(None))
            .order_by(Lead.id)
        )
        if limit:
            q = q.limit(limit)
        return [r[0] for r in q.all()]
    finally:
        session.close()


async def run_lead_linkedin(
    limit: Optional[int] = None,
    step: Optional[str] = None,
) -> dict[str, dict]:
    """Run the personal LinkedIn discovery waterfall: SERP → FE Search → Apify.

    Args:
        limit: Cap on leads to process per step.
        step: Run only one step: 'serp', 'fe', or 'apify'. None = full waterfall.

    Returns:
        Dict mapping step name to {yes, maybe, no} counts.
    """
    if step and step not in ("serp", "fe", "apify"):
        raise ValueError(f"Unknown step: {step}. Must be serp, fe, or apify.")

    logger.info("Starting personal LinkedIn pipeline (limit=%s, step=%s)", limit, step)

    # Snapshot lead IDs before running steps so we can mark them all at the end
    lead_ids = _get_unsearched_lead_ids(limit)
    stats: dict[str, dict] = {}

    if not step or step == "serp":
        from zl_scraper.pipeline.personal_linkedin.serp import run_serp_search_step
        stats["serp"] = await run_serp_search_step(limit=limit)

    if not step or step == "fe":
        from zl_scraper.pipeline.personal_linkedin.fe_search import run_fe_search_step
        stats["fe"] = run_fe_search_step(limit=limit)

    if not step or step == "apify":
        from zl_scraper.pipeline.personal_linkedin.apify_search import run_apify_search_step
        stats["apify"] = await run_apify_search_step(limit=limit)

    # Mark all processed leads as searched (single timestamp for the whole waterfall)
    _mark_searched(lead_ids)

    logger.info("Personal LinkedIn pipeline complete")
    return stats
