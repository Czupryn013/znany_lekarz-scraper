"""Import Prospeo CSV into employees and leads, matching by company domain."""

import csv
from datetime import datetime

from sqlalchemy import insert, select

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Employee, Lead, lead_clinic_roles
from zl_scraper.pipeline.company_enrich.backfill_domains import extract_domain
from zl_scraper.utils.logging import get_logger

logger = get_logger("import_prospeo")


def _parse_rows(csv_path: str) -> list[dict]:
    """Read CSV and return list of row dicts with cleaned fields."""
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _clean_domain(raw: str | None) -> str | None:
    """Normalise a domain value from CSV."""
    if not raw or not raw.strip():
        return None
    return extract_domain(raw.strip())


def _build_domain_to_clinic(session) -> dict[str, int]:
    """Build a mapping from website_domain -> clinic id."""
    clinics = (
        session.query(Clinic.id, Clinic.website_domain)
        .filter(Clinic.website_domain.isnot(None))
        .all()
    )
    return {c.website_domain.lower(): c.id for c in clinics}


def _upsert_employee(session, clinic_id: int, row: dict) -> tuple[bool, int | None]:
    """Insert employee if not already present for this clinic+linkedin combo. Returns (created, employee_id)."""
    linkedin_url = (row.get("Person LinkedIn URL") or "").strip()
    if not linkedin_url:
        return False, None

    existing = (
        session.query(Employee)
        .filter(
            Employee.clinic_id == clinic_id,
            Employee.linkedin_url == linkedin_url,
        )
        .first()
    )
    if existing:
        return False, existing.id

    emp = Employee(
        clinic_id=clinic_id,
        full_name=(row.get("Full name") or "").strip(),
        linkedin_url=linkedin_url,
        position_title=(row.get("Job title") or "").strip() or None,
        company_name=(row.get("Company name") or "").strip() or None,
        review_status="APPROVED",
        scraped_at=datetime.utcnow(),
    )
    session.add(emp)
    session.flush()
    return True, emp.id


def _upsert_lead(session, clinic_id: int, row: dict) -> tuple[str, int]:
    """Create or skip lead. Returns (action, lead_id) where action is 'created'|'skipped'."""
    linkedin_url = (row.get("Person LinkedIn URL") or "").strip()
    full_name = (row.get("Full name") or "").strip()

    # Dedup by linkedin_url
    existing = None
    if linkedin_url:
        existing = (
            session.query(Lead)
            .filter(Lead.linkedin_url == linkedin_url)
            .first()
        )

    # Fallback dedup: name + clinic
    if not existing:
        existing = (
            session.query(Lead)
            .join(lead_clinic_roles, Lead.id == lead_clinic_roles.c.lead_id)
            .filter(
                Lead.full_name == full_name,
                lead_clinic_roles.c.clinic_id == clinic_id,
            )
            .first()
        )

    if existing:
        _ensure_role(session, existing.id, clinic_id)
        return "skipped", existing.id

    email = (row.get("Email") or "").strip() or None

    lead = Lead(
        full_name=full_name,
        linkedin_url=linkedin_url or None,
        email=email,
        lead_source="PROSPEO_EMPLOYEE",
        enrichment_status="PENDING",
        created_at=datetime.utcnow(),
    )
    session.add(lead)
    session.flush()

    _ensure_role(session, lead.id, clinic_id)
    return "created", lead.id


def _ensure_role(session, lead_id: int, clinic_id: int) -> None:
    """Insert lead_clinic_roles row if missing."""
    exists = session.execute(
        select(lead_clinic_roles).where(
            lead_clinic_roles.c.lead_id == lead_id,
            lead_clinic_roles.c.clinic_id == clinic_id,
            lead_clinic_roles.c.role == "EMPLOYEE",
        )
    ).first()
    if not exists:
        session.execute(
            insert(lead_clinic_roles).values(
                lead_id=lead_id,
                clinic_id=clinic_id,
                role="EMPLOYEE",
            )
        )


def run_import_prospeo(csv_path: str, dry_run: bool = False) -> dict:
    """Import a Prospeo CSV into employees + leads, matching on company domain."""
    session = SessionLocal()
    try:
        logger.info("Starting Prospeo import from %s", csv_path)

        rows = _parse_rows(csv_path)
        logger.info("Parsed %d rows from CSV", len(rows))

        domain_map = _build_domain_to_clinic(session)
        logger.info("Loaded %d clinic domains for matching", len(domain_map))

        stats = {
            "total_rows": len(rows),
            "emp_created": 0,
            "emp_skipped": 0,
            "lead_created": 0,
            "lead_skipped": 0,
            "no_domain": 0,
            "no_match": 0,
            "unmatched": [],
        }

        for row in rows:
            domain = _clean_domain(row.get("Company domain"))
            if not domain:
                stats["no_domain"] += 1
                stats["unmatched"].append(row)
                continue

            clinic_id = domain_map.get(domain)
            if not clinic_id:
                stats["no_match"] += 1
                stats["unmatched"].append(row)
                logger.debug("No clinic match for domain: %s", domain)
                continue

            if dry_run:
                stats["emp_created"] += 1
                stats["lead_created"] += 1
                continue

            created, _ = _upsert_employee(session, clinic_id, row)
            if created:
                stats["emp_created"] += 1
            else:
                stats["emp_skipped"] += 1

            action, _ = _upsert_lead(session, clinic_id, row)
            if action == "created":
                stats["lead_created"] += 1
            else:
                stats["lead_skipped"] += 1

        if not dry_run:
            session.commit()
        stats["matched"] = stats["total_rows"] - stats["no_domain"] - stats["no_match"]
        logger.info("Import complete: %s", {k: v for k, v in stats.items() if k != "unmatched"})
        return stats
    except Exception:
        session.rollback()
        logger.exception("Error during Prospeo import")
        raise
    finally:
        session.close()
