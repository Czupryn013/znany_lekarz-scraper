"""SQLAlchemy read queries for lead and ICP-clinic details, search, and batch metadata."""

import logging

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


def get_lead_details(session: Session, lead_id: int) -> dict | None:
    """Fetch lead info with contact data and clinic roles."""
    row = session.execute(
        text("""
            SELECT l.id, l.full_name, l.phone, l.email, l.linkedin_url,
                   l.lead_source, l.phone_source, l.enrichment_status
            FROM leads l WHERE l.id = :lid
        """),
        {"lid": lead_id},
    ).fetchone()

    if not row:
        return None

    lead = {
        "id": row[0], "full_name": row[1], "phone": row[2],
        "email": row[3], "linkedin_url": row[4],
        "lead_source": row[5], "phone_source": row[6],
        "enrichment_status": row[7],
    }

    # fetch roles at clinics
    roles = session.execute(
        text("""
            SELECT c.id, c.name, lcr.role
            FROM lead_clinic_roles lcr
            JOIN clinics c ON c.id = lcr.clinic_id
            WHERE lcr.lead_id = :lid
            ORDER BY c.name
        """),
        {"lid": lead_id},
    ).fetchall()
    lead["clinic_roles"] = [
        {"clinic_id": r[0], "clinic_name": r[1], "role": r[2]} for r in roles
    ]

    # fetch linkedin profile if exists
    profile = session.execute(
        text("""
            SELECT linkedin_url, headline, current_company, current_position,
                   location_text, connections_count
            FROM linkedin_profiles
            WHERE lead_id = :lid AND review_status = 'APPROVED'
            LIMIT 1
        """),
        {"lid": lead_id},
    ).fetchone()

    if profile:
        lead["linkedin_profile"] = {
            "url": profile[0], "headline": profile[1],
            "company": profile[2], "position": profile[3],
            "location": profile[4], "connections": profile[5],
        }

    return lead


def get_icp_clinic_details(session: Session, clinic_id: int) -> dict | None:
    """Fetch ICP clinic info with locations, specializations, and lead count."""
    row = session.execute(
        text("""
            SELECT c.id, c.name, c.zl_url, c.doctors_count, c.nip,
                   c.website_domain, c.linkedin_url, c.legal_name
            FROM clinics c WHERE c.id = :cid AND c.icp_match = true
        """),
        {"cid": clinic_id},
    ).fetchone()

    if not row:
        return None

    clinic = {
        "id": row[0], "name": row[1], "zl_url": row[2],
        "doctors_count": row[3], "nip": row[4],
        "website_domain": row[5], "linkedin_url": row[6],
        "legal_name": row[7],
    }

    locations = session.execute(
        text("SELECT address FROM clinic_locations WHERE clinic_id = :cid"),
        {"cid": clinic_id},
    ).fetchall()
    clinic["locations"] = [{"address": l[0]} for l in locations]

    specs = session.execute(
        text("""
            SELECT DISTINCT s.name FROM specializations s
            JOIN search_queries sq ON sq.specialization_id = s.id
            WHERE sq.clinic_id = :cid
        """),
        {"cid": clinic_id},
    ).fetchall()
    clinic["specializations"] = [s[0] for s in specs]

    lead_count = session.execute(
        text("SELECT COUNT(DISTINCT lead_id) FROM lead_clinic_roles WHERE clinic_id = :cid"),
        {"cid": clinic_id},
    ).scalar()
    clinic["leads_count"] = lead_count

    # board members
    board = session.execute(
        text("""
            SELECT full_name, role, source, pesel
            FROM board_members WHERE clinic_id = :cid
            ORDER BY role, full_name
        """),
        {"cid": clinic_id},
    ).fetchall()
    clinic["board_members"] = [
        {"full_name": r[0], "role": r[1], "source": r[2], "pesel": r[3]} for r in board
    ]

    # linkedin people — leads with linkedin profiles tied to this clinic
    linkedin_people = session.execute(
        text("""
            SELECT l.id, l.full_name, l.linkedin_url, l.phone, l.email,
                   lp.headline, lp.current_company, lp.current_position,
                   lp.review_status, lcr.role
            FROM lead_clinic_roles lcr
            JOIN leads l ON l.id = lcr.lead_id
            LEFT JOIN linkedin_profiles lp ON lp.lead_id = l.id
            WHERE lcr.clinic_id = :cid
            ORDER BY l.full_name
        """),
        {"cid": clinic_id},
    ).fetchall()
    clinic["linkedin_people"] = [
        {
            "lead_id": r[0], "full_name": r[1], "linkedin_url": r[2],
            "phone": r[3], "email": r[4], "headline": r[5],
            "company": r[6], "position": r[7], "review_status": r[8],
            "role": r[9],
        }
        for r in linkedin_people
    ]

    return clinic


def search_lead_nodes(session: Session, query_string: str, limit: int = 20) -> dict:
    """Search ICP clinics and leads by name using ILIKE."""
    pattern = f"%{query_string}%"

    clinics = session.execute(
        text("""
            SELECT c.id, c.name FROM clinics c
            WHERE c.icp_match = true AND c.name ILIKE :q
            ORDER BY c.doctors_count DESC NULLS LAST
            LIMIT :lim
        """),
        {"q": pattern, "lim": limit},
    ).fetchall()

    leads = session.execute(
        text("""
            SELECT id, full_name FROM leads
            WHERE full_name ILIKE :q
            LIMIT :lim
        """),
        {"q": pattern, "lim": limit},
    ).fetchall()

    return {
        "clinics": [{"id": r[0], "name": r[1]} for r in clinics],
        "leads": [{"id": r[0], "full_name": r[1]} for r in leads],
    }


def get_lead_metadata_batch(
    session: Session, clinic_ids: list[int], lead_ids: list[int]
) -> dict:
    """Batch-fetch names and contact info for sets of node IDs."""
    clinics = {}
    leads = {}

    if clinic_ids:
        rows = session.execute(
            text("""
                SELECT id, name, doctors_count, website_domain, linkedin_url, nip
                FROM clinics WHERE id = ANY(:ids)
            """),
            {"ids": clinic_ids},
        ).fetchall()
        clinics = {
            r[0]: {"name": r[1], "doctors_count": r[2], "website": r[3], "linkedin": r[4], "nip": r[5]}
            for r in rows
        }

    if lead_ids:
        rows = session.execute(
            text("""
                SELECT id, full_name, phone, email, linkedin_url, lead_source
                FROM leads WHERE id = ANY(:ids)
            """),
            {"ids": lead_ids},
        ).fetchall()
        leads = {
            r[0]: {
                "full_name": r[1], "phone": r[2], "email": r[3],
                "linkedin_url": r[4], "lead_source": r[5],
            }
            for r in rows
        }

    return {"clinics": clinics, "leads": leads}


def get_random_connected_lead(session: Session, min_connections: int = 2) -> dict | None:
    """Pick a random lead that has at least min_connections distinct clinics."""
    row = session.execute(
        text("""
            SELECT lead_id, COUNT(DISTINCT clinic_id) as cnt
            FROM lead_clinic_roles
            GROUP BY lead_id
            HAVING COUNT(DISTINCT clinic_id) >= :min_cnt
            ORDER BY RANDOM()
            LIMIT 1
        """),
        {"min_cnt": min_connections},
    ).fetchone()

    if not row:
        return None

    return {"lead_id": row[0], "clinic_count": row[1]}


def get_lead_clinic_roles_batch(
    session: Session, lead_ids: list[int], clinic_ids: list[int]
) -> dict[str, list[str]]:
    """Batch-fetch roles for lead-clinic pairs. Returns {'lead_id-clinic_id': [roles]}."""
    if not lead_ids or not clinic_ids:
        return {}

    rows = session.execute(
        text("""
            SELECT lead_id, clinic_id, role
            FROM lead_clinic_roles
            WHERE lead_id = ANY(:lids) AND clinic_id = ANY(:cids)
        """),
        {"lids": lead_ids, "cids": clinic_ids},
    ).fetchall()

    result: dict[str, list[str]] = {}
    for lead_id, clinic_id, role in rows:
        key = f"{lead_id}-{clinic_id}"
        result.setdefault(key, []).append(role)
    return result
