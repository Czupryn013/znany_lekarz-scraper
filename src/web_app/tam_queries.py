"""Database queries for TAM (Total Addressable Market) funnel calculator."""

import logging

from sqlalchemy import Float, cast, func, or_
from sqlalchemy.orm import Session

from zl_scraper.db.models import (
    Clinic,
    ClinicLocation,
    Doctor,
    SearchQuery,
    Specialization,
    clinic_doctors,
    doctor_specializations,
)

logger = logging.getLogger(__name__)


def _keyword_above_threshold_subquery(session: Session, keywords: list[str]):
    """Return subquery of clinic IDs where >50% of doctors match any keyword (case-insensitive partial)."""
    patterns = [f"%{kw.lower()}%" for kw in keywords if kw.strip()]
    if not patterns:
        return None

    spec_filter = or_(*[func.lower(Specialization.name).like(p) for p in patterns])

    # Distinct matching doctors per clinic
    match_sub = (
        session.query(
            clinic_doctors.c.clinic_id,
            func.count(func.distinct(clinic_doctors.c.doctor_id)).label("matches"),
        )
        .join(doctor_specializations, doctor_specializations.c.doctor_id == clinic_doctors.c.doctor_id)
        .join(Specialization, Specialization.id == doctor_specializations.c.specialization_id)
        .filter(spec_filter)
        .group_by(clinic_doctors.c.clinic_id)
        .subquery()
    )

    # Total doctors per clinic
    total_sub = (
        session.query(
            clinic_doctors.c.clinic_id,
            func.count(clinic_doctors.c.doctor_id).label("total"),
        )
        .group_by(clinic_doctors.c.clinic_id)
        .subquery()
    )

    # Clinics where matches/total > 0.5
    return (
        session.query(total_sub.c.clinic_id)
        .outerjoin(match_sub, match_sub.c.clinic_id == total_sub.c.clinic_id)
        .filter(
            cast(func.coalesce(match_sub.c.matches, 0), Float)
            / func.nullif(total_sub.c.total, 0)
            > 0.5
        )
        .subquery()
    )


def _base_clinic_query(
    session: Session,
    spec_ids: list[int] | None,
    keywords: list[str] | None,
    keyword_mode: str,
    min_doctors: int | None,
    min_locations: int | None,
):
    """Build a subquery of clinic IDs matching all active filters."""
    q = session.query(Clinic.id)

    if spec_ids:
        q = (
            q.join(SearchQuery, SearchQuery.clinic_id == Clinic.id)
            .filter(SearchQuery.specialization_id.in_(spec_ids))
            .distinct()
        )

    if keywords:
        above_sub = _keyword_above_threshold_subquery(session, keywords)
        if above_sub is not None:
            above_ids = session.query(above_sub.c.clinic_id)
            if keyword_mode == "include":
                q = q.filter(Clinic.id.in_(above_ids))
            else:
                q = q.filter(~Clinic.id.in_(above_ids))

    if min_doctors is not None:
        q = q.filter(Clinic.doctors_count >= min_doctors)

    if min_locations is not None:
        loc_sub = (
            session.query(ClinicLocation.clinic_id)
            .group_by(ClinicLocation.clinic_id)
            .having(func.count(ClinicLocation.id) >= min_locations)
            .subquery()
        )
        q = q.filter(Clinic.id.in_(loc_sub))

    return q.subquery()


def _count_doctors_for_clinics(session: Session, clinic_subq) -> int:
    """Count distinct doctors linked to the given clinic subquery."""
    return (
        session.query(func.count(func.distinct(clinic_doctors.c.doctor_id)))
        .filter(clinic_doctors.c.clinic_id.in_(session.query(clinic_subq.c.id)))
        .scalar()
        or 0
    )


def get_keyword_breakdown(
    session: Session,
    keywords: list[str],
    base_clinic_ids,
) -> list[dict]:
    """Return per-keyword stats: how many clinics and doctors match each keyword in the base set."""
    results = []
    for kw in keywords:
        if not kw.strip():
            continue
        pattern = f"%{kw.lower()}%"
        spec_filter = func.lower(Specialization.name).like(pattern)

        # Clinics in base set that have >=1 doctor matching this keyword
        matching_clinic_count = (
            session.query(func.count(func.distinct(clinic_doctors.c.clinic_id)))
            .join(doctor_specializations, doctor_specializations.c.doctor_id == clinic_doctors.c.doctor_id)
            .join(Specialization, Specialization.id == doctor_specializations.c.specialization_id)
            .filter(clinic_doctors.c.clinic_id.in_(base_clinic_ids))
            .filter(spec_filter)
            .scalar()
            or 0
        )

        # Distinct doctors in base set matching this keyword
        matching_doctor_count = (
            session.query(func.count(func.distinct(clinic_doctors.c.doctor_id)))
            .join(doctor_specializations, doctor_specializations.c.doctor_id == clinic_doctors.c.doctor_id)
            .join(Specialization, Specialization.id == doctor_specializations.c.specialization_id)
            .filter(clinic_doctors.c.clinic_id.in_(base_clinic_ids))
            .filter(spec_filter)
            .scalar()
            or 0
        )

        results.append({
            "keyword": kw.strip(),
            "clinics_with_match": matching_clinic_count,
            "doctors_with_match": matching_doctor_count,
        })

    return results


def get_tam_funnel(
    session: Session,
    spec_ids: list[int] | None = None,
    keywords: list[str] | None = None,
    keyword_mode: str = "exclude",
    min_doctors: int | None = None,
    min_locations: int | None = None,
) -> list[dict]:
    """Return funnel steps showing clinic + doctor counts as each filter is applied."""
    steps = []

    # Step 1: total
    total_clinics = session.query(func.count(Clinic.id)).scalar() or 0
    total_doctors = session.query(func.count(Doctor.id)).scalar() or 0
    steps.append({"label": "All clinics", "clinics": total_clinics, "doctors": total_doctors})

    # Step 2: after spec filter
    if spec_ids:
        spec_clinic_sub = (
            session.query(Clinic.id)
            .join(SearchQuery, SearchQuery.clinic_id == Clinic.id)
            .filter(SearchQuery.specialization_id.in_(spec_ids))
            .distinct()
            .subquery()
        )
        after_spec = session.query(func.count()).select_from(spec_clinic_sub).scalar() or 0
        after_spec_doctors = _count_doctors_for_clinics(session, spec_clinic_sub)
        spec_names = (
            session.query(Specialization.name)
            .filter(Specialization.id.in_(spec_ids))
            .all()
        )
        label = "Spec include: " + ", ".join(r.name for r in spec_names)
        steps.append({"label": label, "clinics": after_spec, "doctors": after_spec_doctors})

    # Step 3: after keyword filter (cumulative with spec)
    if keywords:
        kw_sub = _base_clinic_query(session, spec_ids, keywords, keyword_mode, None, None)
        after_kw = session.query(func.count()).select_from(kw_sub).scalar() or 0
        after_kw_doctors = _count_doctors_for_clinics(session, kw_sub)
        mode_label = "include" if keyword_mode == "include" else "exclude"
        kw_preview = ", ".join(kw.strip() for kw in keywords[:4])
        if len(keywords) > 4:
            kw_preview += f" +{len(keywords) - 4} more"
        steps.append({
            "label": f"Keyword {mode_label}: {kw_preview}",
            "clinics": after_kw,
            "doctors": after_kw_doctors,
        })

    # Step 4: after min_doctors filter (cumulative)
    if min_doctors is not None:
        cumulative_sub = _base_clinic_query(session, spec_ids, keywords, keyword_mode, min_doctors, None)
        after_doc = session.query(func.count()).select_from(cumulative_sub).scalar() or 0
        after_doc_doctors = _count_doctors_for_clinics(session, cumulative_sub)
        steps.append({
            "label": f"Min {min_doctors} doctors",
            "clinics": after_doc,
            "doctors": after_doc_doctors,
        })

    # Step 5: after min_locations filter (cumulative)
    if min_locations is not None:
        cumulative_sub = _base_clinic_query(session, spec_ids, keywords, keyword_mode, min_doctors, min_locations)
        after_loc = session.query(func.count()).select_from(cumulative_sub).scalar() or 0
        after_loc_doctors = _count_doctors_for_clinics(session, cumulative_sub)
        steps.append({
            "label": f"Min {min_locations} locations",
            "clinics": after_loc,
            "doctors": after_loc_doctors,
        })

    logger.info("TAM funnel computed: %d steps", len(steps))
    return steps


def get_tam_distributions(
    session: Session,
    spec_ids: list[int] | None = None,
    keywords: list[str] | None = None,
    keyword_mode: str = "exclude",
    min_doctors: int | None = None,
    min_locations: int | None = None,
) -> dict:
    """Return distribution breakdowns for the filtered clinic set."""
    filtered_sub = _base_clinic_query(session, spec_ids, keywords, keyword_mode, min_doctors, min_locations)
    filtered_ids = session.query(filtered_sub.c.id)

    # Top specializations by clinic count within filtered set
    top_specs = (
        session.query(
            Specialization.name,
            func.count(func.distinct(SearchQuery.clinic_id)).label("cnt"),
        )
        .join(SearchQuery, SearchQuery.specialization_id == Specialization.id)
        .filter(SearchQuery.clinic_id.in_(filtered_ids))
        .group_by(Specialization.name)
        .order_by(func.count(func.distinct(SearchQuery.clinic_id)).desc())
        .limit(25)
        .all()
    )

    # Doctor count histogram
    doc_buckets = [
        (0, 0, "0"),
        (1, 1, "1"),
        (2, 3, "2-3"),
        (4, 5, "4-5"),
        (6, 10, "6-10"),
        (11, 20, "11-20"),
        (21, 50, "21-50"),
        (51, None, "51+"),
    ]
    doc_dist = []
    for lo, hi, label in doc_buckets:
        q = session.query(func.count(Clinic.id)).filter(Clinic.id.in_(filtered_ids))
        if hi is not None:
            q = q.filter(Clinic.doctors_count >= lo, Clinic.doctors_count <= hi)
        else:
            q = q.filter(Clinic.doctors_count >= lo)
        doc_dist.append({"label": label, "count": q.scalar() or 0})

    # Location count histogram
    loc_sub = (
        session.query(
            ClinicLocation.clinic_id,
            func.count(ClinicLocation.id).label("loc_cnt"),
        )
        .filter(ClinicLocation.clinic_id.in_(filtered_ids))
        .group_by(ClinicLocation.clinic_id)
        .subquery()
    )
    clinic_with_loc = (
        session.query(filtered_sub.c.id, func.coalesce(loc_sub.c.loc_cnt, 0).label("loc_cnt"))
        .outerjoin(loc_sub, filtered_sub.c.id == loc_sub.c.clinic_id)
        .subquery()
    )
    loc_buckets = [
        (0, 0, "0"),
        (1, 1, "1"),
        (2, 3, "2-3"),
        (4, 5, "4-5"),
        (6, 10, "6-10"),
        (11, None, "11+"),
    ]
    loc_dist = []
    for lo, hi, label in loc_buckets:
        q = session.query(func.count()).select_from(clinic_with_loc)
        if hi is not None:
            q = q.filter(clinic_with_loc.c.loc_cnt >= lo, clinic_with_loc.c.loc_cnt <= hi)
        else:
            q = q.filter(clinic_with_loc.c.loc_cnt >= lo)
        loc_dist.append({"label": label, "count": q.scalar() or 0})

    # Top doctor specializations within filtered clinics
    top_doc_specs = (
        session.query(
            Specialization.name,
            func.count(func.distinct(doctor_specializations.c.doctor_id)).label("cnt"),
        )
        .join(doctor_specializations, doctor_specializations.c.specialization_id == Specialization.id)
        .join(clinic_doctors, clinic_doctors.c.doctor_id == doctor_specializations.c.doctor_id)
        .filter(clinic_doctors.c.clinic_id.in_(filtered_ids))
        .group_by(Specialization.name)
        .order_by(func.count(func.distinct(doctor_specializations.c.doctor_id)).desc())
        .limit(25)
        .all()
    )

    # Per-keyword breakdown (only when keywords are provided)
    keyword_breakdown = []
    if keywords:
        keyword_breakdown = get_keyword_breakdown(session, keywords, filtered_ids)

    logger.info("TAM distributions computed for filtered set")
    return {
        "top_clinic_specializations": [{"label": r.name, "count": r.cnt} for r in top_specs],
        "doctors_count_distribution": doc_dist,
        "locations_count_distribution": loc_dist,
        "top_doctor_specializations": [{"label": r.name, "count": r.cnt} for r in top_doc_specs],
        "keyword_breakdown": keyword_breakdown,
    }
