"""Apify-based personal LinkedIn profile search (two-pass: industry-filtered → broad)."""

import asyncio
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from zl_scraper.config import APIFY_CONCURRENCY
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Lead, lead_clinic_roles
from zl_scraper.scraping.linkedin_profile_search import (
    HEALTHCARE_INDUSTRY_IDS,
    search_profiles_by_name,
)
from zl_scraper.scraping.llm import (
    categorize_personal_linkedin_results,
    validate_personal_linkedin_profile,
)
from zl_scraper.pipeline.personal_linkedin.serp import (
    MAX_AGE,
    _age_from_pesel,
    _dedup_urls,
    _get_lead_company_names,
    _merge_csv_urls,
    _normalize_linkedin_url,
)
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.apify")

APIFY_LEAD_CONCURRENCY = min(APIFY_CONCURRENCY, 10)


# ── Query ────────────────────────────────────────────────────────────────


def _get_leads_for_apify(
    session: Session,
    limit: Optional[int] = None,
) -> list[Lead]:
    """Get leads still without linkedin_url, not yet fully searched, with PESEL and age ≤ 75."""
    leads = (
        session.query(Lead)
        .filter(
            Lead.linkedin_url.is_(None),
            Lead.pesel.isnot(None),
            Lead.linkedin_searched_at.is_(None),
        )
        .order_by(Lead.id)
        .all()
    )

    filtered = []
    for lead in leads:
        age = _age_from_pesel(lead.pesel)
        if age is not None and age <= MAX_AGE:
            filtered.append(lead)

    if limit is not None:
        filtered = filtered[:limit]

    return filtered


def _split_name(full_name: str) -> tuple[str, str]:
    """Split full_name into (first_name, last_name)."""
    parts = full_name.strip().split(" ", 1)
    first = parts[0]
    last = parts[1].replace(" ", "-") if len(parts) > 1 else ""
    return first, last


# ── Validate profiles via LLM ───────────────────────────────────────────


async def _validate_profiles(
    full_name: str,
    company_names: list[str],
    profiles: list[dict],
) -> dict:
    """Validate Apify profile results via LLM. Returns {linkedin_yes, linkedin_maybe, linkedin_no, ...}."""
    if not profiles:
        return {"linkedin_yes": None, "linkedin_maybe": [], "linkedin_no": []}

    # Build SerpResult-like objects for LLM categorisation
    from zl_scraper.scraping.serp import SerpResult

    serp_results = []
    for p in profiles:
        url = p.get("url") or p.get("linkedinUrl") or ""
        title = p.get("name") or p.get("fullName") or ""
        headline = p.get("headline") or p.get("title") or ""
        company = p.get("currentCompany") or p.get("company") or ""
        description = f"{headline} | {company}"
        serp_results.append(SerpResult(url=url, title=title, description=description))

    categorized = await categorize_personal_linkedin_results(
        full_name, company_names, serp_results,
    )

    yes_urls: list[str] = []
    maybe_urls: list[str] = []
    no_urls: list[str] = []

    for idx, status in categorized:
        if idx >= len(serp_results):
            continue
        url = serp_results[idx].url
        if not url:
            continue
        label = status.upper()
        logger.info("  LLM %s: %s", label, url)
        if status == "yes":
            yes_urls.append(url)
        elif status == "maybe":
            maybe_urls.append(url)
        else:
            no_urls.append(url)

    # Second pass: validate YES profiles with full profile data
    confirmed_yes: list[str] = []
    for url in yes_urls:
        profile_data = next((p for p in profiles if (p.get("url") or p.get("linkedinUrl")) == url), None)
        if profile_data:
            is_match = await validate_personal_linkedin_profile(full_name, company_names, profile_data)
            if is_match:
                logger.info("  LLM validate YES (confirmed): %s", url)
                confirmed_yes.append(url)
            else:
                logger.info("  LLM validate NO (demoted to MAYBE): %s", url)
                maybe_urls.append(url)
        else:
            confirmed_yes.append(url)

    return {
        "linkedin_yes": confirmed_yes[0] if confirmed_yes else None,
        "linkedin_maybe": maybe_urls + confirmed_yes[1:],
        "linkedin_no": no_urls,
    }


# ── Two-pass search ─────────────────────────────────────────────────────


async def _search_and_validate_one(
    session: Session,
    lead: Lead,
    progress: str = "",
) -> None:
    """Run two-pass Apify search for a single lead and save results."""
    first_name, last_name = _split_name(lead.full_name)
    company_names = _get_lead_company_names(session, lead.id)

    # Pass 1: with healthcare industry filters, max 10 results
    logger.info(
        "%sApify pass 1 (industry-filtered) for #%d %s",
        progress, lead.id, lead.full_name,
    )
    profiles_1 = await search_profiles_by_name(
        first_name, last_name,
        industry_ids=HEALTHCARE_INDUSTRY_IDS,
        max_items=10,
    )

    result_1 = await _validate_profiles(lead.full_name, company_names, profiles_1)

    # Build set of already-rejected URLs to avoid re-adding them
    existing_no = set(_normalize_linkedin_url(u) for u in (lead.linkedin_no or "").split(",") if u.strip())

    if result_1["linkedin_yes"]:
        yes_url = _normalize_linkedin_url(result_1["linkedin_yes"])
        if yes_url not in existing_no:
            # Found in pass 1 — save and return
            lead.linkedin_url = yes_url
            maybe_1 = [u for u in (result_1["linkedin_maybe"] or []) if _normalize_linkedin_url(u) not in existing_no]
            if maybe_1:
                lead.linkedin_maybe = _merge_csv_urls(lead.linkedin_maybe, maybe_1)
            if result_1["linkedin_no"]:
                lead.linkedin_no = _merge_csv_urls(lead.linkedin_no, result_1["linkedin_no"])
            logger.info("%sApify pass 1 found: #%d %s → %s", progress, lead.id, lead.full_name, lead.linkedin_url)
            return

    # Pass 2: no industry filter, max 5 results
    logger.info(
        "%sApify pass 2 (no filter) for #%d %s",
        progress, lead.id, lead.full_name,
    )
    profiles_2 = await search_profiles_by_name(
        first_name, last_name,
        industry_ids=None,
        max_items=5,
    )

    result_2 = await _validate_profiles(lead.full_name, company_names, profiles_2)

    # Merge results from both passes
    all_maybe = (result_1.get("linkedin_maybe", []) or []) + (result_2.get("linkedin_maybe", []) or [])
    all_no = (result_1.get("linkedin_no", []) or []) + (result_2.get("linkedin_no", []) or [])

    # Drop URLs already rejected in a previous review
    existing_no = set(_normalize_linkedin_url(u) for u in (lead.linkedin_no or "").split(",") if u.strip())
    all_maybe = [u for u in all_maybe if _normalize_linkedin_url(u) not in existing_no]

    yes_url = result_2["linkedin_yes"]
    if yes_url and _normalize_linkedin_url(yes_url) in existing_no:
        yes_url = None  # previously rejected — don't re-accept

    if yes_url:
        lead.linkedin_url = _normalize_linkedin_url(yes_url)
        logger.info("%sApify pass 2 found: #%d %s → %s", progress, lead.id, lead.full_name, lead.linkedin_url)

    if all_maybe:
        lead.linkedin_maybe = _merge_csv_urls(lead.linkedin_maybe, all_maybe)
    if all_no:
        lead.linkedin_no = _merge_csv_urls(lead.linkedin_no, all_no)


_counter_lock = asyncio.Lock()
_counter = 0


async def _process_one_lead(
    lead_id: int,
    semaphore: asyncio.Semaphore,
    total: int,
) -> str:
    """Search and validate one lead in its own DB session. Returns 'yes', 'maybe', or 'no'."""
    global _counter
    async with semaphore:
        async with _counter_lock:
            _counter += 1
            current = _counter
        progress = f"[{current}/{total}] "

        session = SessionLocal()
        try:
            lead = session.query(Lead).get(lead_id)
            if lead is None:
                return "no"

            old_maybe = lead.linkedin_maybe
            await _search_and_validate_one(session, lead, progress)
            session.commit()

            if lead.linkedin_url:
                return "yes"
            elif lead.linkedin_maybe and lead.linkedin_maybe != old_maybe:
                return "maybe"
            return "no"
        except Exception:
            session.rollback()
            logger.exception(
                "Apify search failed for lead #%d (%s)",
                lead_id, getattr(lead, "full_name", "?"),
            )
            return "no"
        finally:
            session.close()


async def run_apify_search_step(limit: Optional[int] = None) -> dict:
    """Find LinkedIn profiles for leads via Apify two-pass profile search.

    Returns dict with keys: yes, maybe, no.
    """
    logger.info("Starting personal LinkedIn Apify search (limit=%s)", limit)

    session = SessionLocal()
    try:
        leads = _get_leads_for_apify(session, limit)

        if not leads:
            logger.info("No leads need Apify LinkedIn search")
            return {"yes": 0, "maybe": 0, "no": 0}

        lead_ids = [lead.id for lead in leads]
        logger.info(
            "Found %d leads for Apify LinkedIn search (concurrency=%d)",
            len(lead_ids), APIFY_LEAD_CONCURRENCY,
        )
    finally:
        session.close()

    global _counter
    _counter = 0

    semaphore = asyncio.Semaphore(APIFY_LEAD_CONCURRENCY)
    tasks = [_process_one_lead(lid, semaphore, len(lead_ids)) for lid in lead_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    found = 0
    maybe_count = 0
    not_found = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error("Unexpected Apify task error: %s", r)
            not_found += 1
        elif r == "yes":
            found += 1
        elif r == "maybe":
            maybe_count += 1
        else:
            not_found += 1

    logger.info(
        "Apify search complete: %d found, %d maybe, %d not found out of %d",
        found, maybe_count, not_found, len(lead_ids),
    )
    return {"yes": found, "maybe": maybe_count, "no": not_found}
