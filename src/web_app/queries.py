"""SQLAlchemy read queries for node details, search, and batch metadata."""

import logging

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


def get_clinic_details(session: Session, clinic_id: int) -> dict | None:
    """Fetch clinic info with locations and specializations."""
    row = session.execute(
        text("""
            SELECT c.id, c.name, c.zl_url, c.doctors_count, c.nip,
                   c.website_domain, c.linkedin_url, c.legal_name
            FROM clinics c WHERE c.id = :cid
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
        text("""
            SELECT address, latitude, longitude
            FROM clinic_locations WHERE clinic_id = :cid
        """),
        {"cid": clinic_id},
    ).fetchall()
    clinic["locations"] = [
        {"address": l[0], "latitude": l[1], "longitude": l[2]} for l in locations
    ]

    specs = session.execute(
        text("""
            SELECT DISTINCT s.name FROM specializations s
            JOIN search_queries sq ON sq.specialization_id = s.id
            WHERE sq.clinic_id = :cid
        """),
        {"cid": clinic_id},
    ).fetchall()
    clinic["specializations"] = [s[0] for s in specs]

    return clinic


def get_doctor_details(session: Session, doctor_id: int) -> dict | None:
    """Fetch doctor info with direct specializations, opinions, and per-clinic booking data."""
    row = session.execute(
        text("""
            SELECT id, name, surname, zl_url, gender, img_url,
                   opinions_positive, opinions_neutral, opinions_negative
            FROM doctors WHERE id = :did
        """),
        {"did": doctor_id},
    ).fetchone()

    if not row:
        return None

    doctor = {
        "id": row[0], "name": row[1], "surname": row[2], "zl_url": row[3],
        "gender": row[4], "img_url": row[5],
        "opinions_positive": row[6], "opinions_neutral": row[7], "opinions_negative": row[8],
    }

    # Direct specializations from doctor_specializations M2M
    specs = session.execute(
        text("""
            SELECT s.name, ds.is_in_progress FROM doctor_specializations ds
            JOIN specializations s ON s.id = ds.specialization_id
            WHERE ds.doctor_id = :did
        """),
        {"did": doctor_id},
    ).fetchall()
    doctor["specializations"] = [{"name": s[0], "is_in_progress": s[1]} for s in specs]

    # Per-clinic booking data
    clinic_bookings = session.execute(
        text("""
            SELECT c.id, c.name, cd.booking_ratio, cd.is_bookable
            FROM clinic_doctors cd
            JOIN clinics c ON c.id = cd.clinic_id
            WHERE cd.doctor_id = :did
        """),
        {"did": doctor_id},
    ).fetchall()
    doctor["clinic_bookings"] = [
        {"clinic_id": r[0], "clinic_name": r[1], "booking_ratio": r[2], "is_bookable": r[3]}
        for r in clinic_bookings
    ]

    return doctor


def search_nodes(session: Session, query_string: str, limit: int = 20) -> dict:
    """Search clinics and doctors by name using ILIKE."""
    pattern = f"%{query_string}%"

    clinics = session.execute(
        text("""
            SELECT c.id, c.name,
                   (SELECT cl.address FROM clinic_locations cl WHERE cl.clinic_id = c.id LIMIT 1) as address
            FROM clinics c
            WHERE c.name ILIKE :q
            ORDER BY c.doctors_count DESC NULLS LAST
            LIMIT :lim
        """),
        {"q": pattern, "lim": limit},
    ).fetchall()

    doctors = session.execute(
        text("""
            SELECT id, name, surname FROM doctors
            WHERE (name || ' ' || surname) ILIKE :q
            LIMIT :lim
        """),
        {"q": pattern, "lim": limit},
    ).fetchall()

    return {
        "clinics": [{"id": r[0], "name": r[1], "address": r[2]} for r in clinics],
        "doctors": [{"id": r[0], "name": r[1], "surname": r[2]} for r in doctors],
    }


def get_node_metadata_batch(
    session: Session, clinic_ids: list[int], doctor_ids: list[int]
) -> dict:
    """Batch-fetch names for sets of node IDs."""
    clinics = {}
    doctors = {}

    if clinic_ids:
        rows = session.execute(
            text("SELECT id, name, doctors_count FROM clinics WHERE id = ANY(:ids)"),
            {"ids": clinic_ids},
        ).fetchall()
        clinics = {r[0]: {"name": r[1], "doctors_count": r[2]} for r in rows}

    if doctor_ids:
        rows = session.execute(
            text("SELECT id, name, surname, gender, img_url FROM doctors WHERE id = ANY(:ids)"),
            {"ids": doctor_ids},
        ).fetchall()
        doctors = {r[0]: {"name": r[1], "surname": r[2], "gender": r[3], "img_url": r[4]} for r in rows}

    return {"clinics": clinics, "doctors": doctors}


def get_clinic_specializations_batch(
    session: Session, clinic_ids: list[int]
) -> dict[int, list[str]]:
    """Batch-fetch specializations for multiple clinics."""
    if not clinic_ids:
        return {}

    rows = session.execute(
        text("""
            SELECT sq.clinic_id, s.name FROM specializations s
            JOIN search_queries sq ON sq.specialization_id = s.id
            WHERE sq.clinic_id = ANY(:ids)
        """),
        {"ids": clinic_ids},
    ).fetchall()

    result: dict[int, list[str]] = {}
    for clinic_id, spec_name in rows:
        result.setdefault(clinic_id, []).append(spec_name)
    return result


def get_doctor_specializations_batch(
    session: Session, doctor_ids: list[int]
) -> dict[int, list[str]]:
    """Batch-fetch direct specializations for multiple doctors."""
    if not doctor_ids:
        return {}

    rows = session.execute(
        text("""
            SELECT ds.doctor_id, s.name FROM doctor_specializations ds
            JOIN specializations s ON s.id = ds.specialization_id
            WHERE ds.doctor_id = ANY(:ids)
        """),
        {"ids": doctor_ids},
    ).fetchall()

    result: dict[int, list[str]] = {}
    for doctor_id, spec_name in rows:
        result.setdefault(doctor_id, []).append(spec_name)
    return result


def get_booking_ratio_batch(
    session: Session, pairs: list[tuple[int, int]]
) -> dict[str, dict]:
    """Batch-fetch booking_ratio and is_bookable for clinic-doctor pairs."""
    if not pairs:
        return {}

    clinic_ids = list({p[0] for p in pairs})
    doctor_ids = list({p[1] for p in pairs})

    rows = session.execute(
        text("""
            SELECT clinic_id, doctor_id, booking_ratio, is_bookable
            FROM clinic_doctors
            WHERE clinic_id = ANY(:cids) AND doctor_id = ANY(:dids)
        """),
        {"cids": clinic_ids, "dids": doctor_ids},
    ).fetchall()

    result: dict[str, dict] = {}
    for cid, did, ratio, bookable in rows:
        result[f"c_{cid}-d_{did}"] = {"booking_ratio": ratio, "is_bookable": bookable}
        result[f"d_{did}-c_{cid}"] = {"booking_ratio": ratio, "is_bookable": bookable}
    return result


def search_specializations(session: Session, query_string: str, limit: int = 15) -> list[dict]:
    """Autocomplete search for specializations by name."""
    pattern = f"%{query_string}%"
    rows = session.execute(
        text("""
            SELECT s.id, s.name, count(ds.doctor_id) as doc_count
            FROM specializations s
            LEFT JOIN doctor_specializations ds ON ds.specialization_id = s.id
            WHERE s.name ILIKE :q
            GROUP BY s.id, s.name
            ORDER BY doc_count DESC
            LIMIT :lim
        """),
        {"q": pattern, "lim": limit},
    ).fetchall()
    return [{"id": r[0], "name": r[1], "doctor_count": r[2]} for r in rows]
