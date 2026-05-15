"""Database queries for the ICP clinic viewer."""

import logging
from typing import Any

from sqlalchemy.orm import Session

from zl_scraper.db.models import Clinic, ClinicLocation

logger = logging.getLogger(__name__)

SEARCH_FIELDS = {
    "name": Clinic.name,
    "legal_name": Clinic.legal_name,
    "website_domain": Clinic.website_domain,
    "linkedin_url": Clinic.linkedin_url,
    "nip": Clinic.nip,
    "krs_number": Clinic.krs_number,
}


def get_clinics(
    db: Session,
    search_field: str | None = None,
    search_value: str | None = None,
    missing_domain: bool = False,
    missing_linkedin: bool = False,
    missing_nip: bool = False,
    limit: int = 25,
) -> dict:
    """Return ICP clinics matching the given filters plus total count before limit."""
    q = db.query(Clinic).filter(Clinic.icp_match.is_(True))

    if search_field and search_value and search_field in SEARCH_FIELDS:
        col = SEARCH_FIELDS[search_field]
        q = q.filter(col.ilike(f"%{search_value}%"))

    if missing_domain:
        q = q.filter(Clinic.website_domain.is_(None))

    if missing_linkedin:
        q = q.filter(Clinic.linkedin_url.is_(None))

    if missing_nip:
        q = q.filter(Clinic.nip.is_(None))

    total = q.count()
    rows = q.order_by(Clinic.name).limit(limit).all()

    logger.info(
        "get_clinics: field=%s value=%s missing_domain=%s missing_linkedin=%s missing_nip=%s limit=%s → %d/%d results",
        search_field, search_value, missing_domain, missing_linkedin, missing_nip, limit, len(rows), total,
    )

    return {
        "total": total,
        "results": [
            {
                "id": c.id,
                "name": c.name,
                "legal_name": c.legal_name,
                "website_domain": c.website_domain,
                "linkedin_url": c.linkedin_url,
                "doctors_count": c.doctors_count,
                "nip": c.nip,
            }
            for c in rows
        ],
    }


def get_clinic(db: Session, clinic_id: int) -> dict | None:
    """Return full clinic detail including locations."""
    clinic = (
        db.query(Clinic)
        .filter(Clinic.id == clinic_id)
        .first()
    )
    if not clinic:
        logger.warning("get_clinic: id=%d not found", clinic_id)
        return None

    locations = (
        db.query(ClinicLocation)
        .filter(ClinicLocation.clinic_id == clinic_id)
        .all()
    )

    logger.info("get_clinic: id=%d name=%s locations=%d", clinic_id, clinic.name, len(locations))

    def _fmt(dt):
        return dt.isoformat() if dt else None

    return {
        "id": clinic.id,
        "name": clinic.name,
        "legal_name": clinic.legal_name,
        "zl_url": clinic.zl_url,
        "nip": clinic.nip,
        "krs_number": clinic.krs_number,
        "regon": clinic.regon,
        "legal_type": clinic.legal_type,
        "registration_date": clinic.registration_date,
        "doctors_count": clinic.doctors_count,
        "website_domain": clinic.website_domain,
        "linkedin_url": clinic.linkedin_url,
        # enrichment timestamps
        "discovered_at": _fmt(clinic.discovered_at),
        "enriched_at": _fmt(clinic.enriched_at),
        "domain_searched_at": _fmt(clinic.domain_searched_at),
        "linkedin_searched_at": _fmt(clinic.linkedin_searched_at),
        "nip_searched_at": _fmt(clinic.nip_searched_at),
        "krs_searched_at": _fmt(clinic.krs_searched_at),
        "employees_scraped_at": _fmt(clinic.employees_scraped_at),
        "doctors_refetched_at": _fmt(clinic.doctors_refetched_at),
        "locations": [
            {
                "id": loc.id,
                "address": loc.address,
                "latitude": loc.latitude,
                "longitude": loc.longitude,
                "facebook_url": loc.facebook_url,
                "instagram_url": loc.instagram_url,
                "youtube_url": loc.youtube_url,
                "linkedin_url": loc.linkedin_url,
                "website_url": loc.website_url,
            }
            for loc in locations
        ],
    }


def patch_clinic(db: Session, clinic_id: int, fields: dict[str, Any]) -> bool:
    """Update editable fields on a clinic. Returns True if found and updated."""
    clinic = db.query(Clinic).filter(Clinic.id == clinic_id).first()
    if not clinic:
        logger.warning("patch_clinic: id=%d not found", clinic_id)
        return False

    allowed = {"website_domain", "linkedin_url", "nip", "krs_number"}
    updated = {k: v for k, v in fields.items() if k in allowed}
    for key, val in updated.items():
        setattr(clinic, key, val or None)

    db.commit()
    logger.info("patch_clinic: id=%d updated fields=%s", clinic_id, list(updated.keys()))
    return True
