"""Backfill linkedin_url on clinics from existing ClinicLocation.linkedin_url data."""

import re
from urllib.parse import urlparse

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation
from zl_scraper.utils.logging import get_logger

logger = get_logger("backfill_linkedin")

BATCH_SIZE = 100


def normalize_linkedin_url(url: str) -> str:
    """Normalize a LinkedIn company URL to a canonical form."""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    path = parsed.path.rstrip("/")
    return f"https://www.linkedin.com{path}" if path else ""


def run_backfill_linkedin() -> None:
    """Set linkedin_url on clinics that already have a linkedin_url on any location."""
    logger.info("Starting LinkedIn backfill from existing location linkedin_url data")

    session = SessionLocal()
    try:
        clinic_ids_with_url = (
            session.query(ClinicLocation.clinic_id)
            .filter(ClinicLocation.linkedin_url.isnot(None))
            .distinct()
            .scalar_subquery()
        )
        clinics = (
            session.query(Clinic)
            .filter(
                Clinic.linkedin_url.is_(None),
                Clinic.id.in_(clinic_ids_with_url),
            )
            .order_by(Clinic.id)
            .all()
        )

        if not clinics:
            logger.info("No clinics to backfill — all already have linkedin_url or no location URLs")
            return

        logger.info("Found %d clinics to backfill", len(clinics))
        count = 0

        for i, clinic in enumerate(clinics):
            loc = (
                session.query(ClinicLocation)
                .filter(
                    ClinicLocation.clinic_id == clinic.id,
                    ClinicLocation.linkedin_url.isnot(None),
                )
                .first()
            )
            if loc and loc.linkedin_url:
                normalized = normalize_linkedin_url(loc.linkedin_url)
                if normalized:
                    clinic.linkedin_url = normalized
                    count += 1

            if (i + 1) % BATCH_SIZE == 0:
                session.commit()
                logger.info("Backfilled %d / %d clinics so far", count, i + 1)

        session.commit()
        logger.info("Backfill complete: %d clinics updated with linkedin_url", count)

    finally:
        session.close()
