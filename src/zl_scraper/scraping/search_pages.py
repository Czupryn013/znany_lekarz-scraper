"""Scrape search result pages for a specialization — batched parallel pagination + immediate persistence."""

import asyncio
from datetime import datetime
from typing import Callable

from urllib.parse import quote

import httpx
from sqlalchemy.orm import Session

from zl_scraper.config import SEARCH_CONCURRENCY, ZL_SEARCH_URL
from zl_scraper.db.models import ScrapeProgress
from zl_scraper.scraping.http_client import WaterfallClient
from zl_scraper.scraping.parsers import ClinicStub, parse_search_page, parse_total_pages
from zl_scraper.utils.logging import get_logger, tier_tag

logger = get_logger("search_pages")

# Type alias for the callback that persists a batch of stubs
StubSaver = Callable[[list[ClinicStub], int], tuple[int, int, list[str]]]


def _build_search_url(spec_name: str, spec_id: int, page: int) -> str:
    """Build the ZnanyLekarz search URL for a specialization + page number."""
    encoded_name = quote(spec_name, safe="")
    return (
        f"{ZL_SEARCH_URL}?q={encoded_name}&loc="
        f"&filters[entity_type][0]=facility"
        f"&filters[specializations][0]={spec_id}"
        f"&page={page}"
    )


async def scrape_specialization_pages(
    spec_id: int,
    spec_name: str,
    wf_client: WaterfallClient,
    semaphore: asyncio.Semaphore,
    session: Session,
    save_stubs: StubSaver,
    max_pages: int | None = None,
) -> dict:
    """Paginate through search pages sequentially, persisting stubs after each page.

    Returns a summary dict with new_count, deduped_count, and last_page_scraped.
    """
    logger.info("Starting discovery for [bold cyan]%s[/] [dim](id=%d)[/]", spec_name, spec_id)

    total_new = 0
    total_deduped = 0

    # ── Resume from last checkpoint ──────────────────────────────────────
    progress = session.query(ScrapeProgress).filter_by(specialization_id=spec_id).first()
    if progress and progress.status == "done":
        real_total = progress.total_pages or 1
        previously_capped = progress.last_page_scraped < real_total
        if not previously_capped and not max_pages:
            logger.info("[bold cyan]%s[/] already fully scraped — [dim]skipping[/]", spec_name)
            return {"new": 0, "deduped": 0, "last_page": progress.last_page_scraped}
        if previously_capped:
            logger.info(
                "[bold cyan]%s[/] was previously capped at page %d/%d — [yellow]resuming[/]",
                spec_name, progress.last_page_scraped, real_total,
            )
            progress.status = "in_progress"
            session.commit()
        else:
            logger.info("[bold cyan]%s[/] already fully scraped — [dim]skipping[/]", spec_name)
            return {"new": 0, "deduped": 0, "last_page": progress.last_page_scraped}

    start_page = (progress.last_page_scraped + 1) if progress else 1

    # ── Fetch page 1 to determine total pages ────────────────────────────
    if start_page == 1:
        url = _build_search_url(spec_name, spec_id, 1)
        try:
            response = await wf_client.fetch(url, semaphore)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch page 1 for '%s': %s", spec_name, e)
            return {"new": 0, "deduped": 0, "last_page": 0}

        html = response.text
        total_pages = parse_total_pages(html)
        stubs = parse_search_page(html)

        # Persist page-1 stubs immediately
        new, deduped, deduped_names = save_stubs(stubs, spec_id)
        total_new += new
        total_deduped += deduped

        # Create or update progress
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
        tier_used = getattr(response, "_proxy_tier", "?")
        logger.info(
            "[bold cyan]%s[/] total_pages=[bold]%d[/], page 1 → [green]+%d new[/], [yellow]%d dedup[/] %s",
            spec_name, total_pages, new, deduped, tier_tag(tier_used),
        )
    else:
        total_pages = progress.total_pages or 1
        logger.info("Resuming [bold cyan]%s[/] from page %d/%d", spec_name, start_page, total_pages)

    # ── Determine effective range ────────────────────────────────────────
    effective_last_page = min(total_pages, max_pages) if max_pages else total_pages

    if start_page > effective_last_page:
        if progress.last_page_scraped >= total_pages:
            progress.status = "done"
        progress.updated_at = datetime.utcnow()
        session.commit()
        return {"new": total_new, "deduped": total_deduped, "last_page": progress.last_page_scraped}

    # ── Fetch remaining pages in ordered batches ───────────────────────────
    remaining_pages = list(range(start_page, effective_last_page + 1))
    batch_size = SEARCH_CONCURRENCY
    stop = False

    for batch_start in range(0, len(remaining_pages), batch_size):
        if stop:
            break

        batch_pages = remaining_pages[batch_start : batch_start + batch_size]
        urls = [_build_search_url(spec_name, spec_id, p) for p in batch_pages]

        # Fire batch in parallel — results come back in the same order as input
        tasks = [wf_client.fetch(url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results in page order so checkpoint advances correctly
        for page_num, result in zip(batch_pages, results):
            if isinstance(result, Exception):
                logger.error("Failed to fetch page %d for '%s': %s — stopping pagination", page_num, spec_name, result)
                stop = True
                break

            tier_used = getattr(result, "_proxy_tier", "?")
            stubs = parse_search_page(result.text)
            new, deduped, deduped_names = save_stubs(stubs, spec_id)
            total_new += new
            total_deduped += deduped

            # Advance checkpoint after every successful page
            progress.last_page_scraped = page_num
            progress.updated_at = datetime.utcnow()
            session.commit()

            logger.info(
                "[bold cyan]%s[/] page [bold]%d[/]/%d → [green]+%d new[/], [yellow]%d dedup[/] (%d stubs) %s",
                spec_name, page_num, total_pages, new, deduped, len(stubs), tier_tag(tier_used),
            )

    # ── Finalize ─────────────────────────────────────────────────────────
    if progress.last_page_scraped >= total_pages:
        progress.status = "done"
        progress.updated_at = datetime.utcnow()
        session.commit()

    logger.info(
        "[bold cyan]%s[/] done: [green]+%d new[/], [yellow]%d dedup[/], pages %d/%d",
        spec_name, total_new, total_deduped, progress.last_page_scraped, total_pages,
    )
    return {"new": total_new, "deduped": total_deduped, "last_page": progress.last_page_scraped}
