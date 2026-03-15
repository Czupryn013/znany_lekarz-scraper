"""Orchestrator for the personal LinkedIn discovery pipeline.

Processes leads in small batches (default 5), running each step
(SERP → FE → Apify) per batch before moving on.
"""

import asyncio
from datetime import datetime
from typing import Optional

from apify_client import ApifyClientAsync

from zl_scraper.config import APIFY_API_TOKEN
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Lead
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.main")

BATCH_SIZE = 5


def _mark_searched(lead_ids: list[int]) -> None:
    """Bulk-set linkedin_searched_at on the given leads."""
    if not lead_ids:
        return

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
    session = SessionLocal()
    try:
        q = (
            session.query(Lead.id)
            .filter(Lead.linkedin_searched_at.is_(None))
            .order_by(Lead.id)
        )
        if limit:
            q = q.limit(limit)
        return [r[0] for r in q.all()]
    finally:
        session.close()


def _add_stats(totals: dict, step_result: dict) -> None:
    """Accumulate yes/maybe/no counts from a step result into totals."""
    for key in ("yes", "maybe", "no"):
        totals[key] = totals.get(key, 0) + step_result.get(key, 0)


async def run_lead_linkedin(
    limit: Optional[int] = None,
    step: Optional[str] = None,
) -> dict[str, dict]:
    """Run the personal LinkedIn discovery waterfall in batches of 5.

    Args:
        limit: Cap on total leads to process.
        step: Run only one step: 'serp', 'fe', or 'apify'. None = full waterfall.

    Returns:
        Dict mapping step name to {yes, maybe, no} counts.
    """
    if step and step not in ("serp", "fe", "apify"):
        raise ValueError(f"Unknown step: {step}. Must be serp, fe, or apify.")

    from zl_scraper.pipeline.personal_linkedin.serp import run_serp_batch
    from zl_scraper.pipeline.personal_linkedin.fe_search import run_fe_batch
    from zl_scraper.pipeline.personal_linkedin.apify_search import run_apify_batch

    all_ids = _get_unsearched_lead_ids(limit)
    total = len(all_ids)

    if total == 0:
        logger.info("No unsearched leads found")
        return {}

    logger.info(
        "Starting personal LinkedIn pipeline (limit=%s, step=%s, total=%d, batch=%d)",
        limit, step, total, BATCH_SIZE,
    )

    stats: dict[str, dict] = {}
    client = ApifyClientAsync(token=APIFY_API_TOKEN)
    processed = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch_ids = all_ids[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info("── Batch %d/%d (%d leads) ──", batch_num, total_batches, len(batch_ids))

        failed_ids: set[int] = set()

        if not step or step == "serp":
            result = await run_serp_batch(batch_ids)
            failed_ids |= result.pop("failed_ids", set())
            stats.setdefault("serp", {"yes": 0, "maybe": 0, "no": 0})
            _add_stats(stats["serp"], result)

        if not step or step == "fe":
            result = await run_fe_batch(batch_ids)
            failed_ids |= result.pop("failed_ids", set())
            stats.setdefault("fe", {"yes": 0, "maybe": 0, "no": 0})
            _add_stats(stats["fe"], result)

        if not step or step == "apify":
            result = await run_apify_batch(batch_ids, client=client)
            failed_ids |= result.pop("failed_ids", set())
            stats.setdefault("apify", {"yes": 0, "maybe": 0, "no": 0})
            _add_stats(stats["apify"], result)

        ids_to_mark = [lid for lid in batch_ids if lid not in failed_ids]
        if failed_ids:
            logger.info("Skipping %d leads with errors (will retry next run)", len(failed_ids))
        _mark_searched(ids_to_mark)
        processed += len(batch_ids)

    logger.info("Personal LinkedIn pipeline complete — processed %d leads", processed)
    return stats
