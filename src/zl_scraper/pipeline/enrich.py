"""Enrichment orchestrator — pull un-enriched clinics, fetch profiles, save data."""

import asyncio
from datetime import datetime

from sqlalchemy.orm import Session

from zl_scraper.config import PROFILE_CONCURRENCY
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation
from zl_scraper.scraping.http_client import create_client
from zl_scraper.scraping.profile_enrichment import enrich_clinic
from zl_scraper.utils.logging import get_logger

logger = get_logger("enrich")

BATCH_SIZE = 30


def _get_unenriched_clinics(session: Session, limit: int | None = None) -> list[Clinic]:
    """Query clinics where enriched_at IS NULL."""
    query = session.query(Clinic).filter(Clinic.enriched_at.is_(None))
    if limit:
        query = query.limit(limit)
    return query.all()


def _save_enrichment(
    clinic: Clinic,
    profile_data,
    doctors_count: int,
    session: Session,
) -> None:
    """Update a clinic row with enriched profile data and insert locations."""
    clinic.zl_profile_id = profile_data.zl_profile_id
    clinic.nip = profile_data.nip
    clinic.legal_name = profile_data.legal_name
    clinic.description = profile_data.description
    clinic.zl_reviews_cnt = profile_data.zl_reviews_cnt
    clinic.doctors_count = doctors_count
    clinic.enriched_at = datetime.utcnow()

    # Insert locations
    for loc in profile_data.locations:
        session.add(
            ClinicLocation(
                clinic_id=clinic.id,
                address=loc.address,
                latitude=loc.latitude,
                longitude=loc.longitude,
            )
        )

    session.commit()


async def run_enrichment(limit: int | None = None) -> None:
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

        async with create_client() as client:
            # Process in batches
            for batch_start in range(0, total, BATCH_SIZE):
                batch = clinics[batch_start : batch_start + BATCH_SIZE]
                batch_num = (batch_start // BATCH_SIZE) + 1

                tasks = [
                    enrich_clinic(clinic.zl_url, client, semaphore)
                    for clinic in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for clinic, result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error("Failed to enrich clinic id=%d url=%s: %s", clinic.id, clinic.zl_url, result)
                        failed_count += 1
                        continue

                    profile_data, doctors_count = result
                    if profile_data is None:
                        failed_count += 1
                        continue

                    _save_enrichment(clinic, profile_data, doctors_count, session)
                    enriched_count += 1

                logger.info(
                    "Batch %d complete: enriched %d/%d total, %d failures so far",
                    batch_num,
                    enriched_count,
                    total,
                    failed_count,
                )

        logger.info(
            "Enrichment complete: %d enriched, %d failed out of %d total",
            enriched_count,
            failed_count,
            total,
        )

    finally:
        session.close()
