"""API routes for clinic market analytics — only active when DEBUG_VIEWS=true."""

import logging

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from web_app.analytics_queries import (
    get_clinic_totals,
    get_doctors_count_distribution,
    get_locations_count_distribution,
    get_legal_type_distribution,
    get_top_specializations,
    get_icp_breakdown,
    get_enrichment_funnel,
    get_discovery_timeline,
    get_doctor_totals,
    get_doctor_gender_distribution,
    get_doctor_clinics_distribution,
    get_doctor_top_specializations,
    get_doctor_opinions_distribution,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics")


def _get_db(request: Request):
    """Yield a DB session per request."""
    session = request.app.state.SessionLocal()
    try:
        yield session
    finally:
        session.close()


@router.get("/totals")
def totals(db: Session = Depends(_get_db)):
    """Return high-level clinic counts."""
    return get_clinic_totals(db)


@router.get("/doctors-distribution")
def doctors_distribution(
    icp_only: bool = Query(False),
    db: Session = Depends(_get_db),
):
    """Return histogram of clinics by doctors_count."""
    return get_doctors_count_distribution(db, icp_only=icp_only)


@router.get("/locations-distribution")
def locations_distribution(
    icp_only: bool = Query(False),
    db: Session = Depends(_get_db),
):
    """Return histogram of clinics by number of locations."""
    return get_locations_count_distribution(db, icp_only=icp_only)


@router.get("/legal-type")
def legal_type(
    icp_only: bool = Query(False),
    db: Session = Depends(_get_db),
):
    """Return clinic counts by legal entity type."""
    return get_legal_type_distribution(db, icp_only=icp_only)


@router.get("/specializations")
def specializations(
    limit: int = Query(20, ge=1, le=50),
    icp_only: bool = Query(False),
    db: Session = Depends(_get_db),
):
    """Return top specializations by clinic count."""
    return get_top_specializations(db, limit=limit, icp_only=icp_only)


@router.get("/icp-breakdown")
def icp_breakdown(db: Session = Depends(_get_db)):
    """Return ICP vs non-ICP split."""
    return get_icp_breakdown(db)


@router.get("/enrichment-funnel")
def enrichment_funnel(
    icp_only: bool = Query(False),
    db: Session = Depends(_get_db),
):
    """Return enrichment pipeline funnel counts."""
    return get_enrichment_funnel(db, icp_only=icp_only)


@router.get("/discovery-timeline")
def discovery_timeline(
    icp_only: bool = Query(False),
    db: Session = Depends(_get_db),
):
    """Return monthly clinic discovery counts."""
    return get_discovery_timeline(db, icp_only=icp_only)


@router.get("/doctor-totals")
def doctor_totals(db: Session = Depends(_get_db)):
    """Return high-level doctor counts."""
    return get_doctor_totals(db)


@router.get("/doctor-gender")
def doctor_gender(db: Session = Depends(_get_db)):
    """Return doctor gender breakdown."""
    return get_doctor_gender_distribution(db)


@router.get("/doctor-clinics-distribution")
def doctor_clinics_distribution(db: Session = Depends(_get_db)):
    """Return histogram of doctors by how many clinics they work at."""
    return get_doctor_clinics_distribution(db)


@router.get("/doctor-specializations")
def doctor_specializations_route(
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(_get_db),
):
    """Return top specializations by doctor count."""
    return get_doctor_top_specializations(db, limit=limit)


@router.get("/doctor-opinions")
def doctor_opinions(db: Session = Depends(_get_db)):
    """Return histogram of doctors by positive opinion count."""
    return get_doctor_opinions_distribution(db)
