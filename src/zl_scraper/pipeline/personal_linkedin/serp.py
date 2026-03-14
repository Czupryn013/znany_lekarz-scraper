"""SERP-based personal LinkedIn discovery for leads (board members / prokura)."""

import asyncio
from datetime import datetime
from typing import Optional
from urllib.parse import quote, urlparse

from sqlalchemy import func
from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Lead, lead_clinic_roles
from zl_scraper.scraping.llm import categorize_personal_linkedin_results
from zl_scraper.scraping.serp import run_serp_search
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.serp")

SERP_BATCH_SIZE = 5
MAX_AGE = 75


# ── Helpers ──────────────────────────────────────────────────────────────


def _age_from_pesel(pesel: str) -> Optional[int]:
    """Return age in years from PESEL, or None if invalid."""
    from datetime import date

    digits = "".join(ch for ch in pesel if ch.isdigit())
    if len(digits) != 11:
        return None

    yy = int(digits[0:2])
    mm_raw = int(digits[2:4])
    dd = int(digits[4:6])

    if 1 <= mm_raw <= 12:
        century, mm = 1900, mm_raw
    elif 21 <= mm_raw <= 32:
        century, mm = 2000, mm_raw - 20
    elif 41 <= mm_raw <= 52:
        century, mm = 2100, mm_raw - 40
    elif 61 <= mm_raw <= 72:
        century, mm = 2200, mm_raw - 60
    elif 81 <= mm_raw <= 92:
        century, mm = 1800, mm_raw - 80
    else:
        return None

    year = century + yy
    try:
        birth = date(year, mm, dd)
    except ValueError:
        return None

    today = date.today()
    age = today.year - birth.year
    if (today.month, today.day) < (birth.month, birth.day):
        age -= 1
    return age


def _normalize_linkedin_url(url: str) -> str:
    """Lowercase and strip trailing slashes for dedup."""
    return url.strip().lower().rstrip("/")


def _dedup_urls(urls: list[str]) -> list[str]:
    """Deduplicate LinkedIn URLs by normalized form, preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        norm = _normalize_linkedin_url(url)
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def _merge_csv_urls(existing: str | None, new_urls: list[str]) -> str:
    """Merge new URLs into an existing comma-separated string, deduplicating."""
    existing_list = [u.strip() for u in (existing or "").split(",") if u.strip()]
    combined = existing_list + new_urls
    return ", ".join(_dedup_urls(combined))


def _get_lead_company_names(session: Session, lead_id: int) -> list[str]:
    """Get all company names associated with a lead via lead_clinic_roles."""
    rows = (
        session.query(Clinic.name, Clinic.legal_name)
        .join(lead_clinic_roles, Clinic.id == lead_clinic_roles.c.clinic_id)
        .filter(lead_clinic_roles.c.lead_id == lead_id)
        .all()
    )
    names = []
    for name, legal_name in rows:
        display = legal_name or name
        if display and display not in names:
            names.append(display)
    return names


# ── Query ────────────────────────────────────────────────────────────────


def _get_leads_for_serp(
    session: Session,
    limit: Optional[int] = None,
) -> list[Lead]:
    """Get leads needing SERP LinkedIn search: have PESEL, age ≤ 75, not yet searched."""
    leads = (
        session.query(Lead)
        .filter(
            Lead.pesel.isnot(None),
            Lead.linkedin_url.is_(None),
            Lead.linkedin_searched_at.is_(None),
        )
        .order_by(Lead.id)
        .all()
    )

    # Filter by age in Python (PESEL-derived)
    filtered = []
    for lead in leads:
        age = _age_from_pesel(lead.pesel)
        if age is not None and age <= MAX_AGE:
            filtered.append(lead)

    if limit is not None:
        filtered = filtered[:limit]

    return filtered


# ── Core ─────────────────────────────────────────────────────────────────


async def _process_serp_batch(
    session: Session,
    leads: list[Lead],
    serp_responses: list,
) -> tuple[int, int, int]:
    """Categorise SERP results via LLM and save to leads. Returns per-person (yes, maybe, no) counts."""
    yes_count = 0
    maybe_count = 0
    no_count = 0

    for lead, serp_resp in zip(leads, serp_responses):
        if serp_resp is None or not serp_resp.results:
            no_count += 1
            continue

        company_names = _get_lead_company_names(session, lead.id)

        categorized = await categorize_personal_linkedin_results(
            lead.full_name,
            company_names,
            serp_resp.results,
        )

        yes_urls: list[str] = []
        maybe_urls: list[str] = []
        no_urls: list[str] = []

        for idx, status in categorized:
            if idx >= len(serp_resp.results):
                continue
            url = serp_resp.results[idx].url
            label = status.upper()
            logger.info("  LLM %s: %s", label, url)
            if status == "yes":
                yes_urls.append(url)
            elif status == "maybe":
                maybe_urls.append(url)
            else:
                no_urls.append(url)

        # Drop URLs that were already rejected in a previous review
        existing_no = set(_normalize_linkedin_url(u) for u in (lead.linkedin_no or "").split(",") if u.strip())
        yes_urls = [u for u in yes_urls if _normalize_linkedin_url(u) not in existing_no]
        maybe_urls = [u for u in maybe_urls if _normalize_linkedin_url(u) not in existing_no]

        # Set the first YES as linkedin_url
        if yes_urls and not lead.linkedin_url:
            lead.linkedin_url = _normalize_linkedin_url(yes_urls[0])
            # Additional YES URLs go to maybe for human review
            if len(yes_urls) > 1:
                maybe_urls.extend(yes_urls[1:])

        if maybe_urls:
            lead.linkedin_maybe = _merge_csv_urls(lead.linkedin_maybe, maybe_urls)
        if no_urls:
            lead.linkedin_no = _merge_csv_urls(lead.linkedin_no, no_urls)

        # Count per person: YES > MAYBE > NO
        if yes_urls:
            yes_count += 1
        elif maybe_urls:
            maybe_count += 1
        else:
            no_count += 1

    session.commit()
    return yes_count, maybe_count, no_count


async def run_serp_search_step(limit: Optional[int] = None) -> dict:
    """Find LinkedIn profiles for leads via SERP search + LLM categorisation.

    Returns dict with keys: yes, maybe, no.
    """
    logger.info("Starting personal LinkedIn SERP search (limit=%s)", limit)

    session = SessionLocal()
    try:
        leads = _get_leads_for_serp(session, limit)
        total = len(leads)

        if total == 0:
            logger.info("No leads need SERP LinkedIn search")
            return {"yes": 0, "maybe": 0, "no": 0}

        logger.info("Found %d leads for SERP LinkedIn search", total)

        # Build all keywords upfront and fire them in one parallel run_serp_search call.
        # SERP_BATCH_SIZE controls how many keywords go into each Apify actor (5),
        # while run_serp_search handles concurrency across actors.
        keywords = [f"{lead.full_name} site:pl.linkedin.com/in" for lead in leads]
        semaphore = asyncio.Semaphore(20)

        serp_responses = await run_serp_search(
            keywords, semaphore, keywords_per_call=SERP_BATCH_SIZE,
        )

        total_yes, total_maybe, total_no = await _process_serp_batch(
            session, leads, serp_responses,
        )

        logger.info(
            "SERP search complete: %d YES, %d MAYBE, %d NO out of %d leads",
            total_yes, total_maybe, total_no, total,
        )
        return {"yes": total_yes, "maybe": total_maybe, "no": total_no}
    except Exception:
        session.rollback()
        logger.exception("Error during SERP LinkedIn search")
        raise
    finally:
        session.close()
