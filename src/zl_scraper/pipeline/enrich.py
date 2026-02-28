"""Enrichment orchestrator — pull un-enriched clinics, fetch profiles, save data."""

import asyncio
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from zl_scraper.config import PROFILE_CONCURRENCY
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation, Doctor, clinic_doctors
from zl_scraper.scraping.http_client import WaterfallClient
from zl_scraper.scraping.profile_enrichment import enrich_clinic
from zl_scraper.utils.logging import get_logger

logger = get_logger("enrich")

BATCH_SIZE = 30


def _get_unenriched_clinics(session: Session, limit: int | None = None) -> list[Clinic]:
    """Query clinics where enriched_at IS NULL, ordered by id."""
    query = session.query(Clinic).filter(Clinic.enriched_at.is_(None)).order_by(Clinic.id)
    if limit:
        query = query.limit(limit)
    return query.all()


def _save_enrichment(
    clinic: Clinic,
    profile_data,
    doctors_list: list,
    session: Session,
) -> None:
    """Update a clinic row with enriched profile data, insert locations and doctors."""
    clinic.zl_profile_id = profile_data.zl_profile_id
    clinic.nip = profile_data.nip
    clinic.legal_name = profile_data.legal_name
    clinic.description = profile_data.description[:500] if profile_data.description else None
    clinic.zl_reviews_cnt = profile_data.zl_reviews_cnt
    clinic.doctors_count = len(doctors_list)
    clinic.enriched_at = datetime.utcnow()

    # Insert locations
    for loc in profile_data.locations:
        session.add(
            ClinicLocation(
                clinic_id=clinic.id,
                address=loc.address,
                latitude=loc.latitude,
                longitude=loc.longitude,
                facebook_url=loc.facebook_url[:512] if loc.facebook_url else None,
                instagram_url=loc.instagram_url[:512] if loc.instagram_url else None,
                youtube_url=loc.youtube_url[:512] if loc.youtube_url else None,
                linkedin_url=loc.linkedin_url[:512] if loc.linkedin_url else None,
                website_url=loc.website_url[:512] if loc.website_url else None,
            )
        )

    # Upsert doctors (INSERT … ON CONFLICT DO NOTHING) and link via M2M
    for doc in doctors_list:
        if doc.id is None:
            continue
        stmt = pg_insert(Doctor.__table__).values(
            id=doc.id,
            name=doc.name,
            surname=doc.surname,
            zl_url=doc.zl_url,
        ).on_conflict_do_nothing(index_elements=["id"])
        session.execute(stmt)

        # Insert association (ignore if already linked)
        assoc_stmt = pg_insert(clinic_doctors).values(
            clinic_id=clinic.id,
            doctor_id=doc.id,
        ).on_conflict_do_nothing()
        session.execute(assoc_stmt)

    session.commit()


async def run_enrichment(limit: int | None = None, start_tier: str = "datacenter") -> None:
    """Orchestrate enrichment of all un-enriched clinics."""
    logger.info("Starting enrichment pipeline")

    session = SessionLocal()
    try:
        clinics = _get_unenriched_clinics(session, limit)
        total = len(clinics)

        if total == 0:
            logger.info("No un-enriched clinics found — nothing to do")
            return

        logger.info("Found %d clinics to enrich", total)

        semaphore = asyncio.Semaphore(PROFILE_CONCURRENCY)
        enriched_count = 0
        failed_count = 0

        async with WaterfallClient(start_tier=start_tier) as profile_client, \
                   WaterfallClient(start_tier=start_tier) as doctors_client:

            async def _enrich_one(clinic: Clinic) -> tuple[Clinic, tuple | Exception]:
                """Fetch + parse a single clinic, returning (clinic, result)."""
                try:
                    result = await enrich_clinic(clinic.zl_url, profile_client, doctors_client, semaphore)
                    return clinic, result
                except Exception as exc:
                    return clinic, exc

            for batch_start in range(0, total, BATCH_SIZE):
                batch = clinics[batch_start : batch_start + BATCH_SIZE]
                batch_num = (batch_start // BATCH_SIZE) + 1
                logger.info("Batch %d — launching %d tasks (concurrency=%d)", batch_num, len(batch), PROFILE_CONCURRENCY)

                tasks = [asyncio.ensure_future(_enrich_one(c)) for c in batch]

                for coro in asyncio.as_completed(tasks):
                    clinic, result = await coro

                    if isinstance(result, Exception):
                        logger.error("Failed to enrich clinic id=%d url=%s: %s", clinic.id, clinic.zl_url, result)
                        failed_count += 1
                        continue

                    profile_data, doctors_list = result
                    if profile_data is None:
                        failed_count += 1
                        continue

                    _save_enrichment(clinic, profile_data, doctors_list, session)
                    enriched_count += 1
                    logger.info(
                        "[green]%d/%d[/] enriched [bold]%s[/] — %d locations, %d doctors",
                        enriched_count,
                        total,
                        clinic.name,
                        len(profile_data.locations),
                        len(doctors_list),
                    )

        logger.info(
            "Enrichment complete: %d enriched, %d failed out of %d total",
            enriched_count,
            failed_count,
            total,
        )

    finally:
        session.close()
