"""Refetch doctors from clinic profile pages to backfill specializations and extra fields."""

import asyncio
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from zl_scraper.config import PROFILE_CONCURRENCY
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Doctor, Specialization, clinic_doctors, doctor_specializations
from zl_scraper.scraping.doctors import fetch_doctors
from zl_scraper.scraping.http_client import WaterfallClient
from zl_scraper.scraping.parsers import DoctorData
from zl_scraper.utils.logging import get_logger

logger = get_logger("refetch_doctors")

BATCH_SIZE = 12


def _count_total_unprocessed(session: Session) -> tuple[int, int]:
    """Return (unprocessed_count, total_with_profile_id) for progress display."""
    row = session.execute(text("""
        SELECT
            count(*) FILTER (WHERE doctors_refetched_at IS NULL) AS unprocessed,
            count(*) AS total
        FROM clinics
        WHERE zl_profile_id IS NOT NULL
    """)).fetchone()
    return row[0], row[1]


def _get_clinics_to_refetch(session: Session, limit: int | None = None) -> list[tuple[int, str]]:
    """Return (clinic_id, zl_profile_id) for clinics that haven't had doctors refetched."""
    query = text("""
        SELECT id, zl_profile_id FROM clinics
        WHERE zl_profile_id IS NOT NULL
          AND doctors_refetched_at IS NULL
        ORDER BY id
    """)
    rows = session.execute(query).fetchall()
    result = [(r[0], r[1]) for r in rows]
    if limit:
        result = result[:limit]
    return result


def _save_doctors(clinic_id: int, doctors_list: list[DoctorData], session: Session) -> int:
    """Upsert doctors, their specializations, and clinic-doctor links. Returns count saved."""
    saved = 0
    for doc in doctors_list:
        if doc.id is None:
            continue

        # Upsert doctor row
        stmt = pg_insert(Doctor.__table__).values(
            id=doc.id,
            name=doc.name,
            surname=doc.surname,
            zl_url=doc.zl_url,
            gender=doc.gender,
            img_url=doc.img_url,
            opinions_positive=doc.opinions_positive,
            opinions_neutral=doc.opinions_neutral,
            opinions_negative=doc.opinions_negative,
        ).on_conflict_do_update(
            index_elements=["id"],
            set_={
                "name": doc.name,
                "surname": doc.surname,
                "zl_url": doc.zl_url,
                "gender": doc.gender,
                "img_url": doc.img_url,
                "opinions_positive": doc.opinions_positive,
                "opinions_neutral": doc.opinions_neutral,
                "opinions_negative": doc.opinions_negative,
            },
        )
        session.execute(stmt)

        # Upsert clinic-doctor association with booking fields
        assoc_stmt = pg_insert(clinic_doctors).values(
            clinic_id=clinic_id,
            doctor_id=doc.id,
            booking_ratio=doc.booking_ratio,
            is_bookable=doc.is_bookable,
        ).on_conflict_do_update(
            constraint="clinic_doctors_pkey",
            set_={
                "booking_ratio": doc.booking_ratio,
                "is_bookable": doc.is_bookable,
            },
        )
        session.execute(assoc_stmt)

        # Upsert specializations
        for spec in doc.specializations:
            spec_stmt = pg_insert(Specialization.__table__).values(
                id=spec.zl_id,
                name=spec.name,
            ).on_conflict_do_nothing(index_elements=["id"])
            session.execute(spec_stmt)

            ds_stmt = pg_insert(doctor_specializations).values(
                doctor_id=doc.id,
                specialization_id=spec.zl_id,
                is_in_progress=spec.is_in_progress,
            ).on_conflict_do_update(
                constraint="doctor_specializations_pkey",
                set_={"is_in_progress": spec.is_in_progress},
            )
            session.execute(ds_stmt)

        saved += 1
    return saved


async def run_refetch_doctors(
    limit: int | None = None,
    start_tier: str = "datacenter",
    concurrency: int | None = None,
) -> None:
    """Re-fetch doctor data from clinic pages, saving specializations and extra fields."""
    logger.info("Starting doctor refetch pipeline")

    sem_value = concurrency or PROFILE_CONCURRENCY
    session = SessionLocal()
    try:
        unprocessed, total_eligible = _count_total_unprocessed(session)
        already_done = total_eligible - unprocessed
        logger.info(
            "Clinics with profile_id: %d total, %d already refetched, %d remaining",
            total_eligible, already_done, unprocessed,
        )

        clinics = _get_clinics_to_refetch(session, limit)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics to refetch — all already processed")
            return

        logger.info(
            "Will refetch %d clinics this run (concurrency=%d, tier=%s)",
            total, sem_value, start_tier,
        )

        semaphore = asyncio.Semaphore(sem_value)
        success_count = 0
        failed_count = 0
        total_doctors = 0

        async with WaterfallClient(start_tier=start_tier) as client:

            async def _fetch_one(clinic_id: int, profile_id: str) -> tuple[int, str, list[DoctorData] | Exception]:
                """Fetch doctors for a single clinic."""
                logger.debug("Fetching clinic_id=%d profile_id=%s", clinic_id, profile_id)
                try:
                    doctors = await fetch_doctors(profile_id, client, semaphore)
                    return clinic_id, profile_id, doctors
                except Exception as exc:
                    logger.error(
                        "clinic_id=%d profile_id=%s FAILED: %s: %s",
                        clinic_id, profile_id, type(exc).__name__, exc,
                    )
                    return clinic_id, profile_id, exc

            for batch_start in range(0, total, BATCH_SIZE):
                batch = clinics[batch_start : batch_start + BATCH_SIZE]
                batch_num = (batch_start // BATCH_SIZE) + 1
                logger.info(
                    "Batch %d — launching %d tasks (%d/%d)",
                    batch_num, len(batch), batch_start + 1, total,
                )

                batch_success = 0
                batch_failed = 0
                tasks = [asyncio.ensure_future(_fetch_one(cid, pid)) for cid, pid in batch]

                for coro in asyncio.as_completed(tasks):
                    clinic_id, profile_id, result = await coro

                    if isinstance(result, Exception):
                        failed_count += 1
                        batch_failed += 1
                        continue

                    doctors_list = result
                    saved = _save_doctors(clinic_id, doctors_list, session)

                    # Mark clinic as refetched
                    session.execute(
                        text("UPDATE clinics SET doctors_refetched_at = :ts WHERE id = :cid"),
                        {"ts": datetime.utcnow(), "cid": clinic_id},
                    )

                    success_count += 1
                    batch_success += 1
                    total_doctors += saved
                    logger.info(
                        "[green]%d/%d[/] clinic_id=%d — %d doctors saved",
                        success_count, total, clinic_id, len(doctors_list),
                    )

                # Commit once per batch
                session.commit()
                logger.info(
                    "Batch %d done — %d ok, %d failed",
                    batch_num, batch_success, batch_failed,
                )

        logger.info(
            "Refetch complete: %d clinics processed, %d failed, %d total doctors upserted",
            success_count, failed_count, total_doctors,
        )

    finally:
        session.close()
