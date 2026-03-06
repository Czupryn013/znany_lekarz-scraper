"""Find Polish NIP (tax ID) for clinics via SERP search on their domain + LLM extraction."""

import asyncio
from datetime import datetime

from sqlalchemy.orm import Session

from zl_scraper.config import APIFY_CONCURRENCY, DOMAIN_CHECKPOINT_SIZE
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic
from zl_scraper.scraping.llm import extract_nip
from zl_scraper.pipeline.company_enrich.serp import SerpResponse, run_serp_search
from zl_scraper.utils.logging import get_logger

logger = get_logger("find_nip")

# Only fetch top 3 results — NIP is usually in the first few hits
MAX_NIP_RESULTS = 3


def _get_clinics_needing_nip(session: Session, limit: int | None = None, icp_only: bool = True) -> list[Clinic]:
    """Query clinics that have a domain but no NIP and haven't been NIP-searched yet."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.enriched_at.isnot(None),
            Clinic.website_domain.isnot(None),
            Clinic.nip.is_(None),
            Clinic.nip_searched_at.is_(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def _build_nip_keyword(clinic: Clinic) -> str:
    """Build a Google search query to find a clinic's NIP on its domain."""
    return f"nip site:{clinic.website_domain}"


async def _process_batch(
    clinics: list[Clinic],
    serp_responses: list[SerpResponse | None],
    session: Session,
) -> tuple[int, int, int]:
    """Extract NIP from SERP results via LLM and save to DB. Returns (found, not_found, skipped)."""
    found = 0
    not_found = 0
    skipped = 0

    for clinic, serp_resp in zip(clinics, serp_responses):
        if serp_resp is None:
            skipped += 1
            logger.debug("Skipping clinic %d (%s) — SERP call failed", clinic.id, clinic.name)
            continue

        if not serp_resp.search_term:
            skipped += 1
            logger.debug("Skipping clinic %d (%s) — SERP returned undefined search_term", clinic.id, clinic.name)
            continue

        if len(serp_resp.results) == 0:
            not_found += 1
            clinic.nip_searched_at = datetime.utcnow()
            logger.debug("Clinic %d (%s) — SERP returned 0 results for NIP query", clinic.id, clinic.name)
            continue

        nip = await extract_nip(clinic.name, clinic.website_domain, serp_resp.results)

        if nip:
            clinic.nip = nip
            found += 1
            logger.info("Clinic %d (%s) — NIP found: %s", clinic.id, clinic.name, nip)
        else:
            not_found += 1
            logger.info("Clinic %d (%s) — no NIP extracted from %d results", clinic.id, clinic.name, len(serp_resp.results))

        clinic.nip_searched_at = datetime.utcnow()

    session.commit()
    return found, not_found, skipped


async def run_find_nip(limit: int | None = None, icp_only: bool = True) -> None:
    """Discover NIP for clinics by searching 'nip site:domain' via SERP + LLM extraction."""
    logger.info("Starting NIP discovery pipeline (icp_only=%s)", icp_only)

    session = SessionLocal()
    try:
        clinics = _get_clinics_needing_nip(session, limit, icp_only=icp_only)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics need NIP discovery — nothing to do")
            return

        logger.info(
            "Found %d clinics needing NIP search (checkpoint=%d, concurrency=%d)",
            total,
            DOMAIN_CHECKPOINT_SIZE,
            APIFY_CONCURRENCY,
        )

        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)
        total_found = 0
        total_not_found = 0
        total_skipped = 0

        for ckpt_start in range(0, total, DOMAIN_CHECKPOINT_SIZE):
            ckpt_end = min(ckpt_start + DOMAIN_CHECKPOINT_SIZE, total)
            ckpt_clinics = clinics[ckpt_start:ckpt_end]
            ckpt_keywords = [_build_nip_keyword(c) for c in ckpt_clinics]

            logger.info(
                "Checkpoint %d–%d / %d — launching SERP for %d clinics",
                ckpt_start + 1,
                ckpt_end,
                total,
                len(ckpt_clinics),
            )

            serp_responses = await run_serp_search(ckpt_keywords, semaphore, max_results=MAX_NIP_RESULTS)

            while len(serp_responses) < len(ckpt_clinics):
                serp_responses.append(None)

            found, not_found, skipped = await _process_batch(ckpt_clinics, serp_responses, session)
            total_found += found
            total_not_found += not_found
            total_skipped += skipped

            logger.info(
                "Checkpoint %d–%d committed (%d found, %d not found, %d skipped so far)",
                ckpt_start + 1,
                ckpt_end,
                total_found,
                total_not_found,
                total_skipped,
            )

        logger.info(
            "NIP discovery complete: %d found, %d not found, %d skipped (SERP fail) out of %d total",
            total_found,
            total_not_found,
            total_skipped,
            total,
        )
    finally:
        session.close()
