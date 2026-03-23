"""Find LinkedIn company profiles via SERP (by domain) + keyword search fallback (by name)."""

import asyncio
from datetime import datetime

from apify_client import ApifyClientAsync

from zl_scraper.config import APIFY_API_TOKEN, APIFY_CONCURRENCY
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, LinkedInCandidate
from zl_scraper.pipeline.employee_scraper.queries import (
    get_clinics_for_apify_retry,
    get_clinics_without_linkedin,
)
from zl_scraper.scraping.linkedin_keyword_search import search_company_by_keyword
from zl_scraper.scraping.linkedin_scraper import scrape_linkedin_companies
from zl_scraper.scraping.llm import (
    categorize_linkedin_results,
    match_keyword_company,
    shorten_company_name,
    validate_linkedin_profile,
)
from zl_scraper.scraping.serp import run_serp_search
from zl_scraper.utils.logging import get_logger

logger = get_logger("employee_scraper.find_company")


def _build_company_serp_keyword(clinic: Clinic) -> str:
    """Build a Google search query targeting LinkedIn company pages by domain."""
    return f'"{clinic.website_domain}" site:linkedin.com/company'


async def _serp_step(
    clinics: list[Clinic],
    session,
) -> list[Clinic]:
    """Run SERP search for company LinkedIn pages by domain. Returns clinics still without a match."""
    # Only clinics with a domain can use SERP
    has_domain = [c for c in clinics if c.website_domain]
    no_domain = [c for c in clinics if not c.website_domain]

    if no_domain:
        logger.info(
            "SERP step: skipping %d clinics without domain (will go to keyword search)",
            len(no_domain),
        )

    if not has_domain:
        return clinics

    logger.info("SERP step: searching %d clinics by domain for LinkedIn company pages", len(has_domain))

    # 5 separate actors running concurrently, each with 1 keyword (actor processes sequentially inside)
    keywords = [_build_company_serp_keyword(c) for c in has_domain]
    semaphore = asyncio.Semaphore(5)
    serp_responses = await run_serp_search(keywords, semaphore, keywords_per_call=1)

    # Pad with None if some chunks failed
    while len(serp_responses) < len(has_domain):
        serp_responses.append(None)

    remaining = list(no_domain)

    for clinic, serp_resp in zip(has_domain, serp_responses):
        if serp_resp is None or not serp_resp.results:
            logger.debug("SERP found nothing for '%s' (domain=%s)", clinic.name, clinic.website_domain)
            remaining.append(clinic)
            clinic.linkedin_searched_at = datetime.utcnow()
            continue

        # LLM categorisation
        categorized = await categorize_linkedin_results(
            clinic.name,
            clinic.website_domain or "",
            serp_resp.results,
        )

        found = False
        for idx, status in categorized:
            if idx >= len(serp_resp.results):
                continue
            url = serp_resp.results[idx].url

            session.add(
                LinkedInCandidate(clinic_id=clinic.id, url=url, status=status)
            )

            if status == "yes":
                if not clinic.linkedin_url:
                    clinic.linkedin_url = url
                    found = True
                logger.info("[green]YES[/]   '%s' → %s", clinic.name, url)
            elif status == "maybe":
                logger.info("[yellow]MAYBE[/] '%s' → %s", clinic.name, url)
            else:
                logger.info("[red]NO[/]    '%s' → %s", clinic.name, url)

        if not found:
            remaining.append(clinic)

        clinic.linkedin_searched_at = datetime.utcnow()

    session.commit()

    logger.info(
        "SERP step done: %d found, %d remaining",
        len(has_domain) - len([c for c in remaining if c.website_domain]),
        len(remaining),
    )
    return remaining


async def _validate_maybe_candidates(session) -> tuple[int, int]:
    """Second pass: scrape MAYBE LinkedIn candidates and re-validate via LLM."""
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

    # Group by clinic
    clinic_candidates: dict[int, list[LinkedInCandidate]] = {}
    for cand in candidates:
        clinic_candidates.setdefault(cand.clinic_id, []).append(cand)

    # Scrape all unique MAYBE URLs
    urls_to_scrape = list({c.url for c in candidates})
    semaphore = asyncio.Semaphore(APIFY_CONCURRENCY)
    profiles = await scrape_linkedin_companies(urls_to_scrape, semaphore)

    # Index by URL
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
                continue

            is_match = await validate_linkedin_profile(clinic.name, profile)

            if is_match:
                cand.status = "yes"
                clinic.linkedin_url = cand.url
                confirmed += 1
                logger.info("[green]MAYBE→YES[/] for '%s': %s", clinic.name, cand.url)
                break
            else:
                cand.status = "no"
                rejected += 1

    session.commit()
    logger.info("MAYBE validation: %d confirmed, %d rejected", confirmed, rejected)
    return confirmed, rejected


KEYWORD_CONCURRENCY = 10


async def _search_single_clinic(
    clinic: Clinic,
    client: ApifyClientAsync,
    semaphore: asyncio.Semaphore,
) -> tuple[Clinic, str | None]:
    """Search Apify + LLM-match for one clinic, bounded by semaphore."""
    async with semaphore:
        search_name = await shorten_company_name(clinic.name)
        results = await search_company_by_keyword(search_name, client, max_results=5)

        if not results:
            logger.debug("Keyword search found nothing for '%s'", clinic.name)
            return clinic, None

        matched_url = await match_keyword_company(
            clinic.name,
            clinic.legal_name,
            clinic.website_domain,
            results,
        )
        return clinic, matched_url


async def _keyword_step(
    clinics: list[Clinic],
    session,
) -> int:
    """Fallback: search by name via Apify keyword search actor (10 concurrent)."""
    logger.info("Keyword step: searching %d clinics via Apify keyword search (%d concurrent)", len(clinics), KEYWORD_CONCURRENCY)

    client = ApifyClientAsync(token=APIFY_API_TOKEN)
    semaphore = asyncio.Semaphore(KEYWORD_CONCURRENCY)

    found_count = 0

    for i in range(0, len(clinics), KEYWORD_CONCURRENCY):
        batch = clinics[i : i + KEYWORD_CONCURRENCY]
        logger.info("Keyword batch %d–%d / %d", i + 1, i + len(batch), len(clinics))

        tasks = [_search_single_clinic(c, client, semaphore) for c in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error("Keyword search task failed: %s", result)
                continue

            clinic, matched_url = result

            if matched_url:
                clinic.linkedin_url = matched_url
                found_count += 1
                logger.info("[green]Keyword YES[/] for '%s': %s", clinic.name, matched_url)

            clinic.linkedin_searched_at = datetime.utcnow()

        session.commit()

    logger.info("Keyword step done: %d found out of %d", found_count, len(clinics))
    return found_count


async def _retry_apify_step(
    limit: int | None,
    icp_only: bool,
    session,
) -> None:
    """Re-run only the Apify keyword search for clinics that were searched but have no linkedin_url."""
    clinics = get_clinics_for_apify_retry(session, icp_only=icp_only, limit=limit)

    if not clinics:
        logger.info("No clinics to retry Apify keyword search for — nothing to do")
        return

    logger.info("Retrying Apify keyword search for %d clinics", len(clinics))
    found = await _keyword_step(clinics, session)
    logger.info("Apify retry done: %d/%d found", found, len(clinics))


async def run_find_company_linkedin(
    limit: int | None = None,
    icp_only: bool = True,
    retry_apify: bool = False,
) -> None:
    """Find LinkedIn company pages: SERP by domain → validate MAYBEs → keyword search fallback."""
    logger.info("Starting find-linkedin (icp_only=%s, retry_apify=%s)", icp_only, retry_apify)

    session = SessionLocal()
    try:
        if retry_apify:
            await _retry_apify_step(limit, icp_only, session)
            return

        clinics = get_clinics_without_linkedin(session, icp_only=icp_only, limit=limit)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics need LinkedIn company discovery — nothing to do")
            # Still validate any pending MAYBEs from previous runs
            await _validate_maybe_candidates(session)
            return

        logger.info("Found %d clinics needing LinkedIn company search", total)

        # Track source per clinic
        found_source: dict[int, str] = {}  # clinic_id → source

        # Step 1: SERP by domain (cheapest)
        ids_before_serp = {c.id for c in clinics if c.linkedin_url}
        remaining = await _serp_step(clinics, session)
        for c in clinics:
            if c.linkedin_url and c.id not in ids_before_serp:
                found_source[c.id] = "SERP"

        # Step 2: Validate MAYBE candidates from SERP
        ids_before_maybe = set(found_source.keys())
        await _validate_maybe_candidates(session)
        # Refresh to detect MAYBE→YES upgrades
        session.expire_all()
        for c in remaining:
            if c.linkedin_url and c.id not in ids_before_maybe:
                found_source[c.id] = "SERP→MAYBE"

        # Step 3: Keyword search fallback for clinics still without linkedin_url
        still_missing = [c for c in remaining if not c.linkedin_url]
        if still_missing:
            await _keyword_step(still_missing, session)
            for c in still_missing:
                if c.linkedin_url:
                    found_source[c.id] = "KEYWORD"

        # Final summary
        clinic_ids = [c.id for c in clinics]

        updated_clinics = (
            session.query(Clinic)
            .filter(Clinic.id.in_(clinic_ids))
            .all()
        )

        found_clinics = [c for c in updated_clinics if c.linkedin_url]
        missing_clinics = [c for c in updated_clinics if not c.linkedin_url]

        maybe_count = (
            session.query(LinkedInCandidate)
            .filter(
                LinkedInCandidate.clinic_id.in_(clinic_ids),
                LinkedInCandidate.status == "maybe",
            )
            .count()
        )

        logger.info("=" * 60)
        logger.info("SUMMARY: %d/%d found, %d maybes pending", len(found_clinics), total, maybe_count)
        logger.info("-" * 60)

        if found_clinics:
            logger.info("[green]FOUND (%d):[/]", len(found_clinics))
            for c in found_clinics:
                source = found_source.get(c.id, "?")
                logger.info("  ✓ [%s] %s → %s", source, c.name, c.linkedin_url)

        if missing_clinics:
            logger.info("[red]NOT FOUND (%d):[/]", len(missing_clinics))
            for c in missing_clinics:
                logger.info("  ✗ %s (domain=%s)", c.name, c.website_domain or "none")

        if maybe_count:
            logger.info("[yellow]MAYBE candidates: %d (use review-linkedin to resolve)[/]", maybe_count)

        logger.info("=" * 60)
    except Exception:
        session.rollback()
        logger.exception("Error during find-linkedin")
        raise
    finally:
        session.close()
