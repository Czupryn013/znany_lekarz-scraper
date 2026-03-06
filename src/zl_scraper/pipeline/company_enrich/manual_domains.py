"""Interactive CLI tool for manually assigning website domains to clinics."""

import re
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic
from zl_scraper.utils.logging import get_logger

logger = get_logger("manual_domains")

_DOMAIN_RE = re.compile(
    r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$",
    re.IGNORECASE,
)


def _clean_domain(raw: str) -> str:
    """Strip protocol, www prefix, trailing slashes and paths from user input."""
    domain = raw.replace("https://", "").replace("http://", "").strip("/").split("/")[0]
    domain = re.sub(r"^www\.", "", domain).strip()
    return domain


def _get_clinics_without_domain(session: Session, only_searched: bool = True, icp_only: bool = True) -> list[Clinic]:
    """Get clinics that need manual domain assignment."""
    query = session.query(Clinic).filter(
        Clinic.enriched_at.isnot(None),
        Clinic.website_domain.is_(None),
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if only_searched:
        query = query.filter(Clinic.domain_searched_at.isnot(None))
    return query.order_by(Clinic.id).all()


def _print_clinic_info(clinic: Clinic, index: int, total: int) -> None:
    """Display all relevant clinic information for manual review."""
    print(f"\n{'─' * 60}")
    print(f"  [{index}/{total}]  ID: {clinic.id}")
    print(f"  Name:        {clinic.name}")
    print(f"  ZL URL:      {clinic.zl_url}")

    if clinic.nip:
        print(f"  NIP:         {clinic.nip}")
    if clinic.legal_name:
        print(f"  Legal name:  {clinic.legal_name}")
    if clinic.doctors_count:
        print(f"  Doctors:     {clinic.doctors_count}")
    if clinic.zl_reviews_cnt:
        print(f"  Reviews:     {clinic.zl_reviews_cnt}")

    for loc in clinic.locations:
        parts: list[str] = []
        if loc.address:
            parts.append(loc.address)
        if loc.website_url:
            parts.append(f"web: {loc.website_url}")
        if loc.linkedin_url:
            parts.append(f"li: {loc.linkedin_url}")
        if loc.facebook_url:
            parts.append(f"fb: {loc.facebook_url}")
        if parts:
            print(f"  Location:    {' | '.join(parts)}")


def run_manual_domains(only_searched: bool = True, icp_only: bool = True) -> None:
    """Walk through clinics without a domain and ask the user to supply one."""
    session = SessionLocal()
    try:
        clinics = _get_clinics_without_domain(session, only_searched, icp_only=icp_only)

        if not clinics:
            print("No clinics need manual domain assignment.")
            return

        total = len(clinics)
        print(f"\n{total} clinics need manual domain assignment.")
        print("Enter a bare domain (e.g. example.com), press Enter to skip, or 'q' to quit.\n")

        assigned = 0
        skipped = 0

        for i, clinic in enumerate(clinics, 1):
            _print_clinic_info(clinic, i, total)

            while True:
                raw = input("\n  Domain: ").strip()

                if raw.lower() == "q":
                    session.commit()
                    print(f"\nQuitting. Assigned {assigned}, skipped {skipped}.")
                    logger.info("Manual domains: assigned %d, skipped %d (user quit)", assigned, skipped)
                    return

                if not raw:
                    if not clinic.domain_searched_at:
                        clinic.domain_searched_at = datetime.now(timezone.utc)
                    skipped += 1
                    break

                domain = _clean_domain(raw)

                if _DOMAIN_RE.match(domain):
                    clinic.website_domain = domain
                    assigned += 1
                    print(f"  ✓ Saved: {domain}")
                    logger.info("Manual domain for clinic %d (%s): %s", clinic.id, clinic.name, domain)
                    break
                else:
                    print(f"  ✗ Invalid domain: '{domain}'. Try again.")

            # Commit after every clinic so progress isn't lost
            session.commit()

        print(f"\nDone. Assigned {assigned}, skipped {skipped}.")
        logger.info("Manual domains complete: assigned %d, skipped %d out of %d", assigned, skipped, total)

    finally:
        session.close()
