"""Backfill website_domain on clinics from existing ClinicLocation.website_url data."""

import re
from urllib.parse import urlparse

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation
from zl_scraper.utils.logging import get_logger

logger = get_logger("backfill_domains")

BATCH_SIZE = 100


def extract_domain(url: str) -> str:
    """Extract the bare domain from a URL, stripping www. prefix."""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = parsed.hostname or ""
    return re.sub(r"^www\.", "", host).lower()


def run_backfill_domains() -> None:
    """Set website_domain on clinics that already have a website_url on any location."""
    logger.info("Starting domain backfill from existing location website_url data")

    session = SessionLocal()
    try:
        # Clinics with no website_domain but at least one location with a website_url
        clinic_ids_with_url = (
            session.query(ClinicLocation.clinic_id)
            .filter(ClinicLocation.website_url.isnot(None))
            .distinct()
            .scalar_subquery()
        )
        clinics = (
            session.query(Clinic)
            .filter(
                Clinic.website_domain.is_(None),
                Clinic.id.in_(clinic_ids_with_url),
            )
            .order_by(Clinic.id)
            .all()
        )

        if not clinics:
            logger.info("No clinics to backfill — all already have website_domain or no location URLs")
            return

        logger.info("Found %d clinics to backfill", len(clinics))
        count = 0

        for i, clinic in enumerate(clinics):
            # Pick the first non-null website_url from any location
            loc = (
                session.query(ClinicLocation)
                .filter(
                    ClinicLocation.clinic_id == clinic.id,
                    ClinicLocation.website_url.isnot(None),
                )
                .first()
            )
            if loc and loc.website_url:
                domain = extract_domain(loc.website_url)
                if domain:
                    clinic.website_domain = domain
                    count += 1

            # Commit in batches
            if (i + 1) % BATCH_SIZE == 0:
                session.commit()
                logger.info("Backfilled %d / %d clinics so far", count, i + 1)

        session.commit()
        logger.info("Backfill complete: %d clinics updated with website_domain", count)

    finally:
        session.close()
