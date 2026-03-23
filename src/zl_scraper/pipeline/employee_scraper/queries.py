"""Database queries for the employee scraping pipeline."""

from sqlalchemy.orm import Session

from zl_scraper.db.models import Clinic, Employee
from zl_scraper.utils.logging import get_logger

logger = get_logger("employee_scraper.queries")


def get_clinics_without_linkedin(
    session: Session,
    icp_only: bool = True,
    limit: int | None = None,
) -> list[Clinic]:
    """Return ICP clinics that have no linkedin_url and haven't been searched yet."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.linkedin_url.is_(None),
            Clinic.linkedin_searched_at.is_(None),
            Clinic.enriched_at.isnot(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def get_clinics_for_apify_retry(
    session: Session,
    icp_only: bool = True,
    limit: int | None = None,
) -> list[Clinic]:
    """Return clinics that were searched but still have no linkedin_url (for Apify retry)."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.linkedin_url.is_(None),
            Clinic.linkedin_searched_at.isnot(None),
            Clinic.enriched_at.isnot(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def get_clinics_ready_for_employees(
    session: Session,
    icp_only: bool = True,
    limit: int | None = None,
) -> list[Clinic]:
    """Return clinics with linkedin_url set but employees not yet scraped."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.linkedin_url.isnot(None),
            Clinic.employees_scraped_at.is_(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def get_pending_employees(
    session: Session,
    icp_only: bool = True,
) -> list[Employee]:
    """Return employees with PENDING review status."""
    query = (
        session.query(Employee)
        .join(Clinic, Employee.clinic_id == Clinic.id)
        .filter(Employee.review_status == "PENDING")
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    return query.all()
