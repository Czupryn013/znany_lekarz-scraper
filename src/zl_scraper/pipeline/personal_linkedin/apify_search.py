"""Apify-based personal LinkedIn profile search (two-pass: industry-filtered → broad)."""

import asyncio

from sqlalchemy.orm import Session

from apify_client import ApifyClientAsync

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
    _get_lead_company_names,
    _merge_csv_urls,
    _normalize_linkedin_url,
)
from zl_scraper.pipeline.personal_linkedin.save_profiles import save_profiles
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.apify")


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
    verdicts_map: dict[str, str] = {}

    for idx, status in categorized:
        if idx >= len(serp_results):
            continue
        url = serp_results[idx].url
        if not url:
            continue
        label = status.upper()
        logger.info("  LLM %s: %s", label, url)
        verdicts_map[_normalize_linkedin_url(url)] = label
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
                verdicts_map[_normalize_linkedin_url(url)] = "MAYBE"
        else:
            confirmed_yes.append(url)

    return {
        "linkedin_yes": confirmed_yes[0] if confirmed_yes else None,
        "linkedin_maybe": maybe_urls + confirmed_yes[1:],
        "linkedin_no": no_urls,
        "verdicts": verdicts_map,
    }


# ── Two-pass search ─────────────────────────────────────────────────────


async def _search_and_validate_one(
    session: Session,
    lead: Lead,
    progress: str = "",
    client: ApifyClientAsync | None = None,
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
        client=client,
    )

    result_1 = await _validate_profiles(lead.full_name, company_names, profiles_1)

    # Persist pass-1 profiles
    save_profiles(session, lead.id, profiles_1, result_1.get("verdicts", {}), "APIFY_PASS1")

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
        client=client,
    )

    result_2 = await _validate_profiles(lead.full_name, company_names, profiles_2)

    # Persist pass-2 profiles
    save_profiles(session, lead.id, profiles_2, result_2.get("verdicts", {}), "APIFY_PASS2")

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


async def run_apify_batch(lead_ids: list[int], client: ApifyClientAsync | None = None) -> dict:
    """Run Apify two-pass search for a batch of leads (by ID). Sequential, shared client."""
    session = SessionLocal()
    try:
        leads = session.query(Lead).filter(Lead.id.in_(lead_ids)).order_by(Lead.id).all()
        # Only leads still without linkedin_url
        leads = [l for l in leads if l.linkedin_url is None]

        if not leads:
            return {"yes": 0, "maybe": 0, "no": 0, "failed_ids": set()}

        logger.info("Apify batch: %d leads", len(leads))

        found = 0
        maybe_count = 0
        not_found = 0
        failed_ids: set[int] = set()

        for i, lead in enumerate(leads, 1):
            progress = f"[{i}/{len(leads)}] "
            try:
                old_maybe = lead.linkedin_maybe
                await _search_and_validate_one(session, lead, progress, client=client)
                session.commit()

                if lead.linkedin_url:
                    found += 1
                elif lead.linkedin_maybe and lead.linkedin_maybe != old_maybe:
                    maybe_count += 1
                else:
                    not_found += 1
            except Exception:
                session.rollback()
                logger.exception("Apify search failed for lead #%d (%s)", lead.id, lead.full_name)
                failed_ids.add(lead.id)

        logger.info(
            "Apify batch done: %d found, %d maybe, %d not found, %d errors",
            found, maybe_count, not_found, len(failed_ids),
        )
        return {"yes": found, "maybe": maybe_count, "no": not_found, "failed_ids": failed_ids}
    except Exception:
        session.rollback()
        logger.exception("Error during Apify batch")
        raise
    finally:
        session.close()
