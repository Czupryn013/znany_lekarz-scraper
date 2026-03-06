"""Find clinic website domains via Apify SERP + LLM validation."""

import asyncio
import re
from datetime import datetime

from sqlalchemy.orm import Session

from zl_scraper.config import APIFY_CONCURRENCY, DOMAIN_CHECKPOINT_SIZE
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic
from zl_scraper.scraping.llm import validate_domain
from zl_scraper.pipeline.company_enrich.serp import SerpResponse, dedup_results_by_domain, run_serp_search
from zl_scraper.utils.logging import get_logger

logger = get_logger("find_domains")

# Sites to exclude from Google search (mirrors the n8n workflow)
EXCLUDE_SITES = (
    "-site:znanylekarz.pl -site:facebook.com -site:booksy.com "
    "-site:kliniki.pl -site:mp.pl -site:instagram.com -site:fresha.com "
    "-site:rejestr.io -site:waze.com -site:pracuj.pl -site:aleo.com "
    "-site:linkedin.com -site:lekarzebezkolejki.pl"
)


def _get_clinics_needing_domain(session: Session, limit: int | None = None, icp_only: bool = True) -> list[Clinic]:
    """Query enriched clinics that haven't been SERP-searched for a domain yet."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.enriched_at.isnot(None),
            Clinic.website_domain.is_(None),
            Clinic.domain_searched_at.is_(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def _get_clinics_not_found(session: Session, limit: int | None = None, icp_only: bool = True) -> list[Clinic]:
    """Query clinics where SERP was done but no domain was found (retry candidates)."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.enriched_at.isnot(None),
            Clinic.website_domain.is_(None),
            Clinic.domain_searched_at.isnot(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def _extract_cities(clinic: Clinic) -> list[str]:
    """Extract unique city names from a clinic's location addresses."""
    cities: list[str] = []
    for loc in clinic.locations:
        if not loc.address:
            continue
        if re.search(r"online", loc.address, re.IGNORECASE):
            continue
        match = re.search(r"\b\d{2}-\d{3}\s+(.+)$", loc.address)
        if match:
            cities.append(match.group(1).strip())
    return list(dict.fromkeys(cities))  # deduplicate preserving order


def _build_serp_keyword(clinic: Clinic) -> str:
    """Build a Google search query for a clinic's website domain."""
    # No quotes around name — broad match like n8n workflow
    clean_name = " ".join(clinic.name.split())
    return f'{clean_name} {EXCLUDE_SITES}'


async def _process_batch(
    clinics: list[Clinic],
    serp_responses: list[SerpResponse | None],
    session: Session,
) -> tuple[int, int, int]:
    """Validate SERP results via LLM and save domains. Returns (found, not_found, skipped)."""
    found = 0
    not_found = 0
    skipped = 0

    for clinic, serp_resp in zip(clinics, serp_responses):
        if serp_resp is None:
            # SERP call failed — leave domain_searched_at unset so it's auto-retried next run
            skipped += 1
            logger.debug("Skipping clinic %d (%s) — SERP call failed", clinic.id, clinic.name)
            continue

        if not serp_resp.search_term:
            # Apify returned undefined search_term — the query didn't actually execute
            skipped += 1
            logger.debug("Skipping clinic %d (%s) — SERP returned undefined search_term", clinic.id, clinic.name)
            continue

        if len(serp_resp.results) == 0:
            # Legitimate 0 organic results — mark as searched so it's not auto-retried
            not_found += 1
            clinic.domain_searched_at = datetime.utcnow()
            logger.debug("Clinic %d (%s) — SERP returned 0 organic results", clinic.id, clinic.name)
            continue

        deduped = dedup_results_by_domain(serp_resp.results)
        cities = _extract_cities(clinic)

        domain = await validate_domain(clinic.name, cities, deduped)

        if domain:
            clinic.website_domain = domain
            found += 1
        else:
            not_found += 1

        clinic.domain_searched_at = datetime.utcnow()

    session.commit()
    return found, not_found, skipped


async def run_find_domains(limit: int | None = None, retry_not_found: bool = False, icp_only: bool = True) -> None:
    """Discover website domains for clinics via SERP search + LLM validation."""
    mode = "retry-not-found" if retry_not_found else "new"
    logger.info("Starting domain discovery pipeline (mode=%s, icp_only=%s)", mode, icp_only)

    session = SessionLocal()
    try:
        if retry_not_found:
            clinics = _get_clinics_not_found(session, limit, icp_only=icp_only)
            # Reset domain_searched_at so the pipeline treats them as fresh
            for c in clinics:
                c.domain_searched_at = None
            session.commit()
        else:
            clinics = _get_clinics_needing_domain(session, limit, icp_only=icp_only)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics need domain discovery (mode=%s) — nothing to do", mode)
            return

        logger.info(
            "Found %d clinics needing domain search (checkpoint=%d, concurrency=%d)",
            total,
            DOMAIN_CHECKPOINT_SIZE,
            APIFY_CONCURRENCY,
        )

        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)
        total_found = 0
        total_not_found = 0
        total_skipped = 0

        # Process in checkpoint blocks: SERP fetch → LLM validate → commit per block
        for ckpt_start in range(0, total, DOMAIN_CHECKPOINT_SIZE):
            ckpt_end = min(ckpt_start + DOMAIN_CHECKPOINT_SIZE, total)
            ckpt_clinics = clinics[ckpt_start:ckpt_end]
            ckpt_keywords = [_build_serp_keyword(c) for c in ckpt_clinics]

            logger.info(
                "Checkpoint %d–%d / %d — launching SERP for %d clinics",
                ckpt_start + 1,
                ckpt_end,
                total,
                len(ckpt_clinics),
            )

            serp_responses = await run_serp_search(ckpt_keywords, semaphore)

            # Pad with None if we got fewer responses than clinics (failed chunks)
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
            "Domain discovery complete: %d found, %d not found, %d skipped (SERP fail) out of %d total",
            total_found,
            total_not_found,
            total_skipped,
            total,
        )

    finally:
        session.close()
