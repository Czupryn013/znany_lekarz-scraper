"""Discovery orchestrator — load specializations, paginate search pages, save clinic stubs."""

import asyncio
import json
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from zl_scraper.config import SEARCH_CONCURRENCY, SPECIALIZATIONS_PATH
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, SearchQuery, Specialization
from zl_scraper.scraping.http_client import WaterfallClient
from zl_scraper.scraping.search_pages import scrape_specialization_pages
from zl_scraper.utils.logging import get_logger

logger = get_logger("discover")


def load_specializations(session: Session) -> list[dict]:
    """Load specializations from JSON file and upsert into DB."""
    with open(SPECIALIZATIONS_PATH, encoding="utf-8") as f:
        specs = json.load(f)

    for spec in specs:
        existing = session.query(Specialization).filter_by(id=spec["id"]).first()
        if not existing:
            session.add(Specialization(id=spec["id"], name=spec["name"]))
    session.commit()

    return specs


def _save_clinic_stubs(
    stubs: list,
    spec_id: int,
    session: Session,
) -> tuple[int, int, list[str]]:
    """Insert clinic stubs and search query links. Return (new_count, deduped_count, deduped_names)."""
    new_count = 0
    deduped_count = 0
    deduped_names: list[str] = []

    for stub in stubs:
        # Upsert clinic — INSERT ON CONFLICT DO NOTHING
        stmt = (
            pg_insert(Clinic)
            .values(
                zl_url=stub.zl_url,
                name=stub.name,
                discovered_at=datetime.utcnow(),
            )
            .on_conflict_do_nothing(index_elements=["zl_url"])
            .returning(Clinic.id)
        )
        result = session.execute(stmt)
        row = result.fetchone()

        if row:
            clinic_id = row[0]
            new_count += 1
        else:
            # Already exists — look up the ID
            clinic = session.query(Clinic).filter_by(zl_url=stub.zl_url).first()
            clinic_id = clinic.id
            deduped_count += 1
            deduped_names.append(stub.name)

        # Link clinic ↔ specialization
        sq_stmt = (
            pg_insert(SearchQuery)
            .values(
                clinic_id=clinic_id,
                specialization_id=spec_id,
                discovered_at=datetime.now(),
            )
            .on_conflict_do_nothing()
        )
        session.execute(sq_stmt)

    session.commit()
    return new_count, deduped_count, deduped_names


async def run_discovery(
    spec_name: str | None = None,
    spec_id: int | None = None,
    max_pages: int | None = None,
    offset: int = 0,
    limit: int | None = None,
    start_tier: str = "datacenter",
) -> None:
    """Orchestrate search discovery across all (or filtered) specializations."""
    logger.info("[bold]Starting discovery pipeline[/]")

    session = SessionLocal()
    try:
        specs = load_specializations(session)

        # Filter specializations if requested
        if spec_name:
            specs = [s for s in specs if s["name"] == spec_name]
        if spec_id:
            specs = [s for s in specs if s["id"] == spec_id]
        if offset:
            specs = specs[offset:]
        if limit:
            specs = specs[:limit]

        if not specs:
            logger.warning("No specializations matched the given filters")
            return

        logger.info("Will process [bold]%d[/] specialization(s)", len(specs))

        total_new = 0
        total_deduped = 0

        semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)

        async with WaterfallClient(start_tier=start_tier) as wf_client:
            for spec in specs:
                # Build a saver closure that pins the current DB session
                def _saver(stubs, sid, _session=session):
                    return _save_clinic_stubs(stubs, sid, _session)

                result = await scrape_specialization_pages(
                    spec_id=spec["id"],
                    spec_name=spec["name"],
                    wf_client=wf_client,
                    semaphore=semaphore,
                    session=session,
                    save_stubs=_saver,
                    max_pages=max_pages,
                )

                total_new += result["new"]
                total_deduped += result["deduped"]
                logger.info(
                    "[bold cyan]%s[/] — [green]+%d new[/], [yellow]%d dedup[/]",
                    spec["name"],
                    result["new"],
                    result["deduped"],
                )

                # Pause between specializations so ZL doesn't flag burst traffic
                if result["new"] > 0 or result["deduped"] > 0:
                    logger.info("[dim]Waiting 15 s before next specialization…[/]")
                    await asyncio.sleep(15)

        logger.info(
            "[bold green]Discovery complete:[/] [green]+%d new[/], [yellow]%d dedup[/] across [bold]%d[/] specializations",
            total_new,
            total_deduped,
            len(specs),
        )

    finally:
        session.close()
