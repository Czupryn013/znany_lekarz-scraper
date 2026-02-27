"""Scrape search result pages for a specialization — pagination + clinic stub extraction."""

import asyncio
from datetime import datetime

import httpx
from sqlalchemy.orm import Session

from zl_scraper.config import ZL_SEARCH_URL
from zl_scraper.db.models import ScrapeProgress
from zl_scraper.scraping.http_client import fetch
from zl_scraper.scraping.parsers import ClinicStub, parse_search_page, parse_total_pages
from zl_scraper.utils.logging import get_logger

logger = get_logger("search_pages")


def _build_search_url(spec_name: str, spec_id: int, page: int) -> str:
    """Build the ZnanyLekarz search URL for a specialization + page number."""
    return (
        f"{ZL_SEARCH_URL}?q={spec_name}&loc="
        f"&filters[entity_type][]=facility"
        f"&filters[specializations][]={spec_id}"
        f"&page={page}"
    )


async def scrape_specialization_pages(
    spec_id: int,
    spec_name: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    session: Session,
    max_pages: int | None = None,
) -> list[ClinicStub]:
    """Paginate through all search pages for one specialization, yielding clinic stubs."""
    logger.info("Starting discovery for specialization '%s' (id=%d)", spec_name, spec_id)

    # Check progress — resume from last checkpoint
    progress = session.query(ScrapeProgress).filter_by(specialization_id=spec_id).first()
    if progress and progress.status == "done":
        # If previously done but max_pages was capped, allow re-running with full range
        real_total = progress.total_pages or 1
        previously_capped = progress.last_page_scraped < real_total
        if not previously_capped and not max_pages:
            logger.info("Specialization '%s' already fully scraped — skipping", spec_name)
            return []
        if previously_capped:
            logger.info(
                "Specialization '%s' was previously capped at page %d/%d — resuming",
                spec_name, progress.last_page_scraped, real_total,
            )
            progress.status = "in_progress"
            session.commit()
        else:
            logger.info("Specialization '%s' already fully scraped — skipping", spec_name)
            return []

    start_page = (progress.last_page_scraped + 1) if progress else 1

    # Fetch page 1 to determine total pages
    if start_page == 1:
        url = _build_search_url(spec_name, spec_id, 1)
        try:
            response = await fetch(client, url, semaphore)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch page 1 for '%s': %s", spec_name, e)
            return []

        html = response.text
        total_pages = parse_total_pages(html)

        first_page_stubs = parse_search_page(html)
        all_stubs = list(first_page_stubs)

        # Create or update progress — always store the REAL total_pages
        if not progress:
            progress = ScrapeProgress(
                specialization_id=spec_id,
                last_page_scraped=1,
                total_pages=total_pages,
                status="in_progress",
                updated_at=datetime.utcnow(),
            )
            session.add(progress)
        else:
            progress.last_page_scraped = 1
            progress.total_pages = total_pages
            progress.status = "in_progress"
            progress.updated_at = datetime.utcnow()
        session.commit()

        start_page = 2
        logger.info("Specialization '%s': total_pages=%d, page 1 yielded %d stubs", spec_name, total_pages, len(first_page_stubs))
    else:
        total_pages = progress.total_pages or 1
        all_stubs = []
        logger.info("Resuming specialization '%s' from page %d/%d", spec_name, start_page, total_pages)

    # Apply max_pages cap ONLY to which pages we fetch, not to the stored total
    effective_last_page = min(total_pages, max_pages) if max_pages else total_pages

    if start_page > effective_last_page:
        # If we've reached the effective cap, mark done only if we covered ALL real pages
        if progress.last_page_scraped >= total_pages:
            progress.status = "done"
        progress.updated_at = datetime.utcnow()
        session.commit()
        return all_stubs

    # Fetch remaining pages concurrently
    remaining_pages = list(range(start_page, effective_last_page + 1))
    urls = [_build_search_url(spec_name, spec_id, p) for p in remaining_pages]

    tasks = [fetch(client, url, semaphore) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for page_num, result in zip(remaining_pages, results):
        if isinstance(result, Exception):
            logger.error("Failed to fetch page %d for '%s': %s", page_num, spec_name, result)
            continue

        stubs = parse_search_page(result.text)
        all_stubs.extend(stubs)

        # Update checkpoint
        progress.last_page_scraped = page_num
        progress.updated_at = datetime.utcnow()

    # Mark done only if we scraped through ALL real pages (not just the capped range)
    if progress.last_page_scraped >= total_pages:
        progress.status = "done"
    progress.updated_at = datetime.utcnow()
    session.commit()

    logger.info(
        "Specialization '%s' discovery: %d stubs from pages up to %d/%d",
        spec_name,
        len(all_stubs),
        progress.last_page_scraped,
        total_pages,
    )
    return all_stubs
