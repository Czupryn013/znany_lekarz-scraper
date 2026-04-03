"""Database queries for clinic market analytics and distribution charts."""

import logging

from sqlalchemy import func, case, text
from sqlalchemy.orm import Session

from zl_scraper.db.models import (
    Clinic,
    ClinicLocation,
    Doctor,
    clinic_doctors,
    doctor_specializations,
    SearchQuery,
    Specialization,
)

logger = logging.getLogger(__name__)


def get_clinic_totals(session: Session) -> dict:
    """Return high-level clinic counts: total, ICP, enriched, with NIP, etc."""
    total = session.query(func.count(Clinic.id)).scalar()
    icp = session.query(func.count(Clinic.id)).filter(Clinic.icp_match.is_(True)).scalar()
    enriched = session.query(func.count(Clinic.id)).filter(Clinic.enriched_at.isnot(None)).scalar()
    with_nip = session.query(func.count(Clinic.id)).filter(Clinic.nip.isnot(None)).scalar()
    with_website = session.query(func.count(Clinic.id)).filter(Clinic.website_domain.isnot(None)).scalar()
    with_linkedin = session.query(func.count(Clinic.id)).filter(Clinic.linkedin_url.isnot(None)).scalar()

    return {
        "total": total,
        "icp": icp,
        "enriched": enriched,
        "with_nip": with_nip,
        "with_website": with_website,
        "with_linkedin": with_linkedin,
    }


def get_doctors_count_distribution(session: Session, icp_only: bool = False) -> list[dict]:
    """Return histogram buckets of clinics grouped by their doctors_count."""
    buckets = [
        (0, 0, "0"),
        (1, 1, "1"),
        (2, 3, "2-3"),
        (4, 5, "4-5"),
        (6, 10, "6-10"),
        (11, 20, "11-20"),
        (21, 50, "21-50"),
        (51, 100, "51-100"),
        (101, None, "101+"),
    ]

    q = session.query(Clinic.doctors_count)
    if icp_only:
        q = q.filter(Clinic.icp_match.is_(True))

    results = []
    for lo, hi, label in buckets:
        bq = q
        if hi is not None:
            bq = bq.filter(Clinic.doctors_count >= lo, Clinic.doctors_count <= hi)
        else:
            bq = bq.filter(Clinic.doctors_count >= lo)
        results.append({"label": label, "count": bq.count()})
    return results


def get_locations_count_distribution(session: Session, icp_only: bool = False) -> list[dict]:
    """Return histogram buckets of clinics grouped by number of physical locations."""
    sub = (
        session.query(
            ClinicLocation.clinic_id,
            func.count(ClinicLocation.id).label("loc_cnt"),
        )
        .group_by(ClinicLocation.clinic_id)
        .subquery()
    )

    q = session.query(Clinic.id, func.coalesce(sub.c.loc_cnt, 0).label("loc_cnt")).outerjoin(
        sub, Clinic.id == sub.c.clinic_id
    )
    if icp_only:
        q = q.filter(Clinic.icp_match.is_(True))
    q = q.subquery()

    buckets = [
        (0, 0, "0"),
        (1, 1, "1"),
        (2, 3, "2-3"),
        (4, 5, "4-5"),
        (6, 10, "6-10"),
        (11, None, "11+"),
    ]
    results = []
    for lo, hi, label in buckets:
        if hi is not None:
            cnt = session.query(func.count()).filter(q.c.loc_cnt >= lo, q.c.loc_cnt <= hi).scalar()
        else:
            cnt = session.query(func.count()).filter(q.c.loc_cnt >= lo).scalar()
        results.append({"label": label, "count": cnt})
    return results


def get_legal_type_distribution(session: Session, icp_only: bool = False) -> list[dict]:
    """Return clinic counts grouped by legal_type (KRS, CEIDG_JDG, CEIDG_SC, etc.)."""
    q = session.query(
        func.coalesce(Clinic.legal_type, "Unknown").label("legal_type"),
        func.count(Clinic.id).label("cnt"),
    )
    if icp_only:
        q = q.filter(Clinic.icp_match.is_(True))
    rows = q.group_by(text("1")).order_by(func.count(Clinic.id).desc()).all()
    return [{"label": r.legal_type, "count": r.cnt} for r in rows]


def get_top_specializations(session: Session, limit: int = 20, icp_only: bool = False) -> list[dict]:
    """Return the most common specializations by clinic count."""
    q = (
        session.query(
            Specialization.name,
            func.count(func.distinct(SearchQuery.clinic_id)).label("cnt"),
        )
        .join(SearchQuery, SearchQuery.specialization_id == Specialization.id)
    )
    if icp_only:
        q = q.join(Clinic, Clinic.id == SearchQuery.clinic_id).filter(Clinic.icp_match.is_(True))
    rows = (
        q.group_by(Specialization.name)
        .order_by(func.count(func.distinct(SearchQuery.clinic_id)).desc())
        .limit(limit)
        .all()
    )
    return [{"label": r.name, "count": r.cnt} for r in rows]


def get_icp_breakdown(session: Session) -> list[dict]:
    """Return ICP match vs non-match counts."""
    icp = session.query(func.count(Clinic.id)).filter(Clinic.icp_match.is_(True)).scalar()
    non_icp = session.query(func.count(Clinic.id)).filter(Clinic.icp_match.is_(False)).scalar()
    return [
        {"label": "ICP Match", "count": icp},
        {"label": "Non-ICP", "count": non_icp},
    ]


def get_enrichment_funnel(session: Session, icp_only: bool = False) -> list[dict]:
    """Return counts for each enrichment stage to show a funnel chart."""
    base = session.query(func.count(Clinic.id))
    if icp_only:
        base = base.filter(Clinic.icp_match.is_(True))

    total = base.scalar()
    enriched = base.filter(Clinic.enriched_at.isnot(None)).scalar()
    has_nip = base.filter(Clinic.nip.isnot(None)).scalar()
    has_website = base.filter(Clinic.website_domain.isnot(None)).scalar()
    has_linkedin = base.filter(Clinic.linkedin_url.isnot(None)).scalar()
    has_krs = base.filter(Clinic.krs_searched_at.isnot(None)).scalar()

    return [
        {"label": "Discovered", "count": total},
        {"label": "Enriched", "count": enriched},
        {"label": "NIP Found", "count": has_nip},
        {"label": "Website Found", "count": has_website},
        {"label": "LinkedIn Found", "count": has_linkedin},
        {"label": "KRS Searched", "count": has_krs},
    ]


def get_discovery_timeline(session: Session, icp_only: bool = False) -> list[dict]:
    """Return clinic discovery counts grouped by month."""
    q = session.query(
        func.date_trunc("month", Clinic.discovered_at).label("month"),
        func.count(Clinic.id).label("cnt"),
    )
    if icp_only:
        q = q.filter(Clinic.icp_match.is_(True))
    rows = q.group_by(text("1")).order_by(text("1")).all()
    return [{"label": r.month.strftime("%Y-%m") if r.month else "N/A", "count": r.cnt} for r in rows]


def get_doctor_totals(session: Session) -> dict:
    """Return high-level doctor counts."""
    total = session.query(func.count(Doctor.id)).scalar()
    male = session.query(func.count(Doctor.id)).filter(Doctor.gender == 1).scalar()
    female = session.query(func.count(Doctor.id)).filter(Doctor.gender == 0).scalar()
    with_opinions = session.query(func.count(Doctor.id)).filter(
        Doctor.opinions_positive.isnot(None)
    ).scalar()

    return {
        "total": total,
        "male": male,
        "female": female,
        "unknown_gender": total - male - female,
        "with_opinions": with_opinions,
    }


def get_doctor_gender_distribution(session: Session) -> list[dict]:
    """Return doctor counts by gender."""
    male = session.query(func.count(Doctor.id)).filter(Doctor.gender == 1).scalar()
    female = session.query(func.count(Doctor.id)).filter(Doctor.gender == 0).scalar()
    unknown = session.query(func.count(Doctor.id)).filter(Doctor.gender.is_(None)).scalar()
    return [
        {"label": "Male", "count": male},
        {"label": "Female", "count": female},
        {"label": "Unknown", "count": unknown},
    ]


def get_doctor_clinics_distribution(session: Session) -> list[dict]:
    """Return histogram of doctors grouped by how many clinics they work at."""
    sub = (
        session.query(
            clinic_doctors.c.doctor_id,
            func.count(clinic_doctors.c.clinic_id).label("clinic_cnt"),
        )
        .group_by(clinic_doctors.c.doctor_id)
        .subquery()
    )

    buckets = [
        (1, 1, "1"),
        (2, 2, "2"),
        (3, 3, "3"),
        (4, 5, "4-5"),
        (6, 10, "6-10"),
        (11, None, "11+"),
    ]
    results = []
    for lo, hi, label in buckets:
        q = session.query(func.count()).select_from(sub)
        q = q.filter(sub.c.clinic_cnt >= lo)
        if hi is not None:
            q = q.filter(sub.c.clinic_cnt <= hi)
        results.append({"label": label, "count": q.scalar()})
    return results


def get_doctor_top_specializations(session: Session, limit: int = 20) -> list[dict]:
    """Return top specializations by doctor count."""
    rows = (
        session.query(
            Specialization.name,
            func.count(doctor_specializations.c.doctor_id).label("cnt"),
        )
        .join(doctor_specializations, doctor_specializations.c.specialization_id == Specialization.id)
        .group_by(Specialization.name)
        .order_by(func.count(doctor_specializations.c.doctor_id).desc())
        .limit(limit)
        .all()
    )
    return [{"label": r.name, "count": r.cnt} for r in rows]


def get_doctor_opinions_distribution(session: Session) -> list[dict]:
    """Return histogram of doctors by total positive opinions count."""
    buckets = [
        (0, 0, "0"),
        (1, 10, "1-10"),
        (11, 50, "11-50"),
        (51, 100, "51-100"),
        (101, 500, "101-500"),
        (501, None, "500+"),
    ]
    results = []
    for lo, hi, label in buckets:
        q = session.query(func.count(Doctor.id))
        if lo == 0:
            q = q.filter((Doctor.opinions_positive == 0) | (Doctor.opinions_positive.is_(None)))
        elif hi is not None:
            q = q.filter(Doctor.opinions_positive >= lo, Doctor.opinions_positive <= hi)
        else:
            q = q.filter(Doctor.opinions_positive >= lo)
        results.append({"label": label, "count": q.scalar()})
    return results
