"""Sync board_members → leads table, deduplicating by PESEL (KRS) or full_name+clinic (CEIDG)."""

from datetime import datetime
from typing import Optional

from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import BoardMember, Clinic, Lead, lead_clinic_roles
from zl_scraper.utils.logging import get_logger

logger = get_logger("sync_leads")

# Map board_member.source → lead_source
_SOURCE_MAP = {
    "KRS_BOARD": "KRS",
    "KRS_PROKURA": "KRS",
    "CEIDG_JDG": "JDG",
    "CEIDG_SC": "SC",
}


def _get_board_members(
    session: Session,
    icp_only: bool = True,
) -> list[BoardMember]:
    """Fetch all board members, optionally filtered to ICP clinics."""
    query = (
        session.query(BoardMember)
        .join(Clinic, BoardMember.clinic_id == Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    return query.all()


def _upsert_lead_clinic_role(
    session: Session,
    lead_id: int,
    clinic_id: int,
    role: str,
) -> None:
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


def _sync_krs_members(
    session: Session,
    members: list[BoardMember],
) -> tuple[int, int]:
    """Sync KRS board members (have PESEL) — dedup by PESEL.

    Returns (created_count, roles_added_count).
    """
    created = 0
    roles_added = 0

    # Group by PESEL
    by_pesel: dict[str, list[BoardMember]] = {}
    for m in members:
        if not m.pesel:
            continue
        by_pesel.setdefault(m.pesel, []).append(m)

    for pesel, group in by_pesel.items():
        # Check if lead already exists for this PESEL
        lead = session.query(Lead).filter(Lead.pesel == pesel).first()
        if not lead:
            first = group[0]
            lead = Lead(
                pesel=pesel,
                full_name=first.full_name,
                lead_source=_SOURCE_MAP.get(first.source, "KRS"),
                enrichment_status="PENDING",
                created_at=datetime.utcnow(),
            )
            session.add(lead)
            session.flush()  # get lead.id
            created += 1

        # Upsert clinic roles for all members in this PESEL group
        for m in group:
            role = m.role or "UNKNOWN"
            _upsert_lead_clinic_role(session, lead.id, m.clinic_id, role)
            roles_added += 1

    return created, roles_added


def _sync_ceidg_members(
    session: Session,
    members: list[BoardMember],
) -> tuple[int, int]:
    """Sync CEIDG board members (no PESEL) — dedup by full_name + clinic_id.

    Returns (created_count, roles_added_count).
    """
    created = 0
    roles_added = 0

    for m in members:
        if not m.full_name:
            continue

        role = m.role or "UNKNOWN"
        source = _SOURCE_MAP.get(m.source, "JDG")

        # Check if a lead already exists for this person at this clinic
        existing = (
            session.query(Lead)
            .join(lead_clinic_roles, Lead.id == lead_clinic_roles.c.lead_id)
            .filter(
                Lead.full_name == m.full_name,
                Lead.pesel.is_(None),
                lead_clinic_roles.c.clinic_id == m.clinic_id,
            )
            .first()
        )

        if existing:
            _upsert_lead_clinic_role(session, existing.id, m.clinic_id, role)
            roles_added += 1
            continue

        # Create new lead
        lead = Lead(
            pesel=None,
            full_name=m.full_name,
            lead_source=source,
            enrichment_status="PENDING",
            created_at=datetime.utcnow(),
        )
        session.add(lead)
        session.flush()
        created += 1

        _upsert_lead_clinic_role(session, lead.id, m.clinic_id, role)
        roles_added += 1

    return created, roles_added


def run_sync_leads(icp_only: bool = True) -> None:
    """Sync board_members into leads table — dedup KRS by PESEL, CEIDG by name+clinic."""
    session = SessionLocal()
    try:
        logger.info("Starting sync-leads (icp_only=%s)", icp_only)
        all_members = _get_board_members(session, icp_only=icp_only)
        logger.info("Found %d board members to sync", len(all_members))

        if not all_members:
            logger.info("Nothing to sync")
            return

        # Split by source type
        krs_members = [m for m in all_members if m.pesel]
        ceidg_members = [m for m in all_members if not m.pesel]

        krs_created, krs_roles = _sync_krs_members(session, krs_members)
        session.commit()
        logger.info(
            "KRS sync: %d new leads, %d role associations (from %d members)",
            krs_created, krs_roles, len(krs_members),
        )

        ceidg_created, ceidg_roles = _sync_ceidg_members(session, ceidg_members)
        session.commit()
        logger.info(
            "CEIDG sync: %d new leads, %d role associations (from %d members)",
            ceidg_created, ceidg_roles, len(ceidg_members),
        )

        total_created = krs_created + ceidg_created
        total_roles = krs_roles + ceidg_roles
        logger.info(
            "Sync complete: %d leads created, %d role associations",
            total_created, total_roles,
        )
    except Exception:
        session.rollback()
        logger.exception("Error during sync-leads")
        raise
    finally:
        session.close()
