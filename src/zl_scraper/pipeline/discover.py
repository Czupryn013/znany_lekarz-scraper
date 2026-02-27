"""Discovery orchestrator — load specializations, paginate search pages, save clinic stubs."""

import asyncio
import json
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from zl_scraper.config import SEARCH_CONCURRENCY, SPECIALIZATIONS_PATH
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, SearchQuery, Specialization
from zl_scraper.scraping.http_client import create_client
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
) -> tuple[int, int]:
    """Insert clinic stubs and search query links. Return (new_count, deduped_count)."""
    new_count = 0
    deduped_count = 0

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
    return new_count, deduped_count


async def run_discovery(
    spec_name: str | None = None,
    spec_id: int | None = None,
    max_pages: int | None = None,
    limit: int | None = None,
) -> None:
    """Orchestrate search discovery across all (or filtered) specializations."""
    logger.info("Starting discovery pipeline")

    session = SessionLocal()
    try:
        specs = load_specializations(session)

        # Filter specializations if requested
        if spec_name:
            specs = [s for s in specs if s["name"] == spec_name]
        if spec_id:
            specs = [s for s in specs if s["id"] == spec_id]
        if limit:
            specs = specs[:limit]

        if not specs:
            logger.warning("No specializations matched the given filters")
            return

        logger.info("Will process %d specialization(s)", len(specs))

        total_new = 0
        total_deduped = 0
        total_pages_scraped = 0

        semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)

        async with create_client() as client:
            for spec in specs:
                stubs = await scrape_specialization_pages(
                    spec_id=spec["id"],
                    spec_name=spec["name"],
                    client=client,
                    semaphore=semaphore,
                    session=session,
                    max_pages=max_pages,
                )

                if stubs:
                    new_count, deduped_count = _save_clinic_stubs(stubs, spec["id"], session)
                    total_new += new_count
                    total_deduped += deduped_count
                    logger.info(
                        "Specialization '%s': %d new clinics, %d already known (deduped)",
                        spec["name"],
                        new_count,
                        deduped_count,
                    )

        logger.info(
            "Discovery complete: %d new clinics, %d deduped across %d specializations",
            total_new,
            total_deduped,
            len(specs),
        )

    finally:
        session.close()
