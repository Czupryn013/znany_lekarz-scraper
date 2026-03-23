"""Sync APPROVED employees into the leads table with lead_source='EMPLOYEE'."""

from datetime import datetime

from sqlalchemy import insert, select

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Employee, Lead, lead_clinic_roles
from zl_scraper.utils.logging import get_logger

logger = get_logger("employee_scraper.sync")


def _upsert_lead_clinic_role(session, lead_id: int, clinic_id: int, role: str) -> None:
    """Insert a lead_clinic_roles row if it doesn't already exist."""
    exists = session.execute(
        select(lead_clinic_roles).where(
            lead_clinic_roles.c.lead_id == lead_id,
            lead_clinic_roles.c.clinic_id == clinic_id,
            lead_clinic_roles.c.role == role,
        )
    ).first()
    if not exists:
        session.execute(
            insert(lead_clinic_roles).values(
                lead_id=lead_id,
                clinic_id=clinic_id,
                role=role,
            )
        )


def run_sync_employees(icp_only: bool = True) -> None:
    """Sync APPROVED employees into leads table — dedup by linkedin_url or full_name+clinic."""
    session = SessionLocal()
    try:
        logger.info("Starting sync-employees (icp_only=%s)", icp_only)

        query = (
            session.query(Employee)
            .join(Clinic, Employee.clinic_id == Clinic.id)
            .filter(Employee.review_status == "APPROVED")
        )
        if icp_only:
            query = query.filter(Clinic.icp_match.is_(True))

        employees = query.all()
        logger.info("Found %d approved employees to sync", len(employees))

        if not employees:
            logger.info("Nothing to sync")
            return

        created = 0
        roles_added = 0
        skipped = 0

        for emp in employees:
            # Primary dedup: by linkedin_url
            existing = (
                session.query(Lead)
                .filter(Lead.linkedin_url == emp.linkedin_url)
                .first()
            )

            # Fallback dedup: by full_name + clinic
            if not existing:
                existing = (
                    session.query(Lead)
                    .join(lead_clinic_roles, Lead.id == lead_clinic_roles.c.lead_id)
                    .filter(
                        Lead.full_name == emp.full_name,
                        lead_clinic_roles.c.clinic_id == emp.clinic_id,
                    )
                    .first()
                )

            if existing:
                # Update linkedin_url if not set
                if not existing.linkedin_url and emp.linkedin_url:
                    existing.linkedin_url = emp.linkedin_url

                _upsert_lead_clinic_role(session, existing.id, emp.clinic_id, "EMPLOYEE")
                roles_added += 1
                skipped += 1
                continue

            # Create new lead
            lead = Lead(
                pesel=None,
                full_name=emp.full_name,
                linkedin_url=emp.linkedin_url,
                lead_source="EMPLOYEE",
                enrichment_status="PENDING",
                created_at=datetime.utcnow(),
            )
            session.add(lead)
            session.flush()
            created += 1

            _upsert_lead_clinic_role(session, lead.id, emp.clinic_id, "EMPLOYEE")
            roles_added += 1

        session.commit()
        logger.info(
            "Sync complete: %d leads created, %d role associations, %d skipped (existing)",
            created, roles_added, skipped,
        )
    except Exception:
        session.rollback()
        logger.exception("Error during sync-employees")
        raise
    finally:
        session.close()
