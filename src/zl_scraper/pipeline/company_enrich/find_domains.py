"""Find clinic website domains via Apify SERP + LLM validation."""

import asyncio
import re
from datetime import datetime

from sqlalchemy.orm import Session

from zl_scraper.config import APIFY_CONCURRENCY, SERP_KEYWORDS_PER_CALL
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation
from zl_scraper.scraping.llm import validate_domain
from zl_scraper.scraping.serp import SerpResponse, dedup_results_by_domain, run_serp_search
from zl_scraper.utils.logging import get_logger

logger = get_logger("find_domains")

# Sites to exclude from Google search (mirrors the n8n workflow)
EXCLUDE_SITES = (
    "-site:znanylekarz.pl -site:facebook.com -site:booksy.com "
    "-site:kliniki.pl -site:mp.pl -site:instagram.com -site:fresha.com "
    "-site:rejestr.io -site:waze.com -site:pracuj.pl -site:aleo.com "
    "-site:linkedin.com -site:lekarzebezkolejki.pl"
)


def _get_clinics_needing_domain(session: Session, limit: int | None = None) -> list[Clinic]:
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
            # SERP failed for this clinic — leave domain_searched_at unset so it's retried
            skipped += 1
            logger.debug("Skipping clinic %d (%s) — SERP failed", clinic.id, clinic.name)
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


async def run_find_domains(limit: int | None = None) -> None:
    """Discover website domains for clinics via SERP search + LLM validation."""
    logger.info("Starting domain discovery pipeline")

    session = SessionLocal()
    try:
        clinics = _get_clinics_needing_domain(session, limit)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics need domain discovery — nothing to do")
            return

        logger.info(
            "Found %d clinics needing domain search (batch=%d, concurrency=%d)",
            total,
            SERP_KEYWORDS_PER_CALL,
            APIFY_CONCURRENCY,
        )

        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)
        total_found = 0
        total_not_found = 0
        total_skipped = 0

        # Build all keywords, then run_serp_search handles chunking + parallel Apify calls
        keywords = [_build_serp_keyword(c) for c in clinics]
        serp_responses = await run_serp_search(keywords, semaphore)

        # Pad with None if we got fewer responses than clinics (failed chunks)
        while len(serp_responses) < total:
            serp_responses.append(None)

        # Process in sub-batches for incremental commits
        for batch_start in range(0, total, SERP_KEYWORDS_PER_CALL):
            batch_end = min(batch_start + SERP_KEYWORDS_PER_CALL, total)
            batch_clinics = clinics[batch_start:batch_end]
            batch_responses = serp_responses[batch_start:batch_end]

            found, not_found, skipped = await _process_batch(batch_clinics, batch_responses, session)
            total_found += found
            total_not_found += not_found
            total_skipped += skipped

            logger.info(
                "Progress: %d / %d processed (%d found, %d not found, %d skipped)",
                batch_end,
                total,
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
