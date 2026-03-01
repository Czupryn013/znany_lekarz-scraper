"""Find LinkedIn company profiles via Apify SERP + LLM categorisation."""

import asyncio
from datetime import datetime

from sqlalchemy.orm import Session

from zl_scraper.config import APIFY_CONCURRENCY, SERP_KEYWORDS_PER_CALL
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, LinkedInCandidate
from zl_scraper.scraping.linkedin_scraper import scrape_linkedin_companies
from zl_scraper.scraping.llm import categorize_linkedin_results, validate_linkedin_profile
from zl_scraper.scraping.serp import SerpResponse, SerpResult, run_serp_search
from zl_scraper.utils.logging import get_logger

logger = get_logger("find_linkedin")


def _get_clinics_needing_linkedin(session: Session, limit: int | None = None) -> list[Clinic]:
    """Query clinics that have a domain but haven't been LinkedIn-searched yet."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.website_domain.isnot(None),
            Clinic.linkedin_url.is_(None),
            Clinic.linkedin_searched_at.is_(None),
        )
        .order_by(Clinic.id)
    )
    if limit:
        query = query.limit(limit)
    return query.all()


def _build_linkedin_keyword(clinic: Clinic) -> str:
    """Build a Google search query targeting LinkedIn company pages."""
    return f'"{clinic.website_domain}" site:pl.linkedin.com/company'


def _save_candidates(
    session: Session,
    clinic: Clinic,
    categorized: list[tuple[int, str]],
    serp_results: list[SerpResult],
) -> None:
    """Insert LinkedInCandidate rows and set clinic.linkedin_url for YES matches."""
    for idx, status in categorized:
        if idx >= len(serp_results):
            continue

        url = serp_results[idx].url
        session.add(
            LinkedInCandidate(
                clinic_id=clinic.id,
                url=url,
                status=status,
            )
        )

        # Direct match — set on clinic immediately
        if status == "yes" and not clinic.linkedin_url:
            clinic.linkedin_url = url
            logger.info("[green]LinkedIn YES[/] for '%s': %s", clinic.name, url)

    clinic.linkedin_searched_at = datetime.utcnow()


async def _process_serp_batch(
    clinics: list[Clinic],
    serp_responses: list[SerpResponse | None],
    session: Session,
) -> tuple[int, int, int, int]:
    """Categorise SERP results via LLM and save candidates. Returns (yes, maybe, no, skipped) counts."""
    yes_count = 0
    maybe_count = 0
    no_count = 0
    skipped = 0

    for clinic, serp_resp in zip(clinics, serp_responses):
        if serp_resp is None:
            # SERP failed — leave linkedin_searched_at unset so it's retried next run
            skipped += 1
            logger.debug("Skipping clinic %d (%s) — SERP failed", clinic.id, clinic.name)
            continue

        if not serp_resp.results:
            clinic.linkedin_searched_at = datetime.utcnow()
            continue

        categorized = await categorize_linkedin_results(
            clinic.name,
            clinic.website_domain or "",
            serp_resp.results,
        )

        _save_candidates(session, clinic, categorized, serp_resp.results)

        for _, status in categorized:
            if status == "yes":
                yes_count += 1
            elif status == "maybe":
                maybe_count += 1
            else:
                no_count += 1

    session.commit()
    return yes_count, maybe_count, no_count, skipped


async def _validate_maybe_candidates(session: Session) -> tuple[int, int]:
    """Second pass: scrape LinkedIn details for MAYBE candidates and re-validate via LLM."""
    candidates = (
        session.query(LinkedInCandidate)
        .join(Clinic, Clinic.id == LinkedInCandidate.clinic_id)
        .filter(
            LinkedInCandidate.status == "maybe",
            Clinic.linkedin_url.is_(None),
        )
        .all()
    )

    if not candidates:
        logger.info("No MAYBE candidates to validate")
        return 0, 0

    logger.info("Validating %d MAYBE LinkedIn candidates via profile scraping", len(candidates))

    # Group by clinic to avoid duplicate scrapes
    clinic_candidates: dict[int, list[LinkedInCandidate]] = {}
    for cand in candidates:
        clinic_candidates.setdefault(cand.clinic_id, []).append(cand)

    # Collect all unique URLs to scrape
    urls_to_scrape = list({c.url for c in candidates})
    semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)

    profiles = await scrape_linkedin_companies(urls_to_scrape, semaphore)

    # Index profiles by their LinkedIn URL for lookup
    profile_by_url: dict[str, dict] = {}
    for profile in profiles:
        linkedin_url = profile.get("linkedinUrl") or profile.get("url") or ""
        if linkedin_url:
            profile_by_url[linkedin_url] = profile

    confirmed = 0
    rejected = 0

    for clinic_id, cands in clinic_candidates.items():
        clinic = session.get(Clinic, clinic_id)
        if not clinic or clinic.linkedin_url:
            continue

        for cand in cands:
            profile = profile_by_url.get(cand.url)
            if not profile:
                # Could not scrape — leave as maybe
                continue

            is_match = await validate_linkedin_profile(clinic.name, profile)

            if is_match:
                cand.status = "yes"
                clinic.linkedin_url = cand.url
                confirmed += 1
                logger.info("[green]MAYBE→YES[/] for '%s': %s", clinic.name, cand.url)
                break  # Stop checking other candidates for this clinic
            else:
                cand.status = "no"
                rejected += 1

    session.commit()
    logger.info("MAYBE validation: %d confirmed, %d rejected", confirmed, rejected)
    return confirmed, rejected


async def run_find_linkedin(
    limit: int | None = None,
    skip_maybe: bool = False,
) -> None:
    """Discover LinkedIn company profiles for clinics via SERP + LLM categorisation."""
    logger.info("Starting LinkedIn discovery pipeline")

    session = SessionLocal()
    try:
        clinics = _get_clinics_needing_linkedin(session, limit)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics need LinkedIn discovery — nothing to do")
            if not skip_maybe:
                await _validate_maybe_candidates(session)
            return

        logger.info(
            "Found %d clinics needing LinkedIn search (batch=%d, concurrency=%d)",
            total,
            SERP_KEYWORDS_PER_CALL,
            APIFY_CONCURRENCY,
        )

        semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)
        total_yes = 0
        total_maybe = 0
        total_no = 0
        total_skipped = 0

        # Build all keywords, run_serp_search handles chunking + parallel Apify calls
        keywords = [_build_linkedin_keyword(c) for c in clinics]
        serp_responses = await run_serp_search(keywords, semaphore)

        # Pad with None if some chunks failed
        while len(serp_responses) < total:
            serp_responses.append(None)

        # Process in sub-batches for incremental commits
        for batch_start in range(0, total, SERP_KEYWORDS_PER_CALL):
            batch_end = min(batch_start + SERP_KEYWORDS_PER_CALL, total)
            batch_clinics = clinics[batch_start:batch_end]
            batch_responses = serp_responses[batch_start:batch_end]

            yes, maybe, no, skipped = await _process_serp_batch(batch_clinics, batch_responses, session)
            total_yes += yes
            total_maybe += maybe
            total_no += no
            total_skipped += skipped

            logger.info(
                "Progress: %d / %d processed (YES=%d, MAYBE=%d, NO=%d, skipped=%d)",
                batch_end,
                total,
                total_yes,
                total_maybe,
                total_no,
                total_skipped,
            )

        logger.info(
            "SERP pass complete: YES=%d, MAYBE=%d, NO=%d out of %d clinics",
            total_yes,
            total_maybe,
            total_no,
            total,
        )

        # Second pass: validate MAYBE candidates
        if not skip_maybe:
            await _validate_maybe_candidates(session)

    finally:
        session.close()
