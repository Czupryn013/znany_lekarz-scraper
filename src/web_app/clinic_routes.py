"""Routes for the ICP clinic viewer (list + detail pages + API)."""

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from web_app.clinic_queries import get_clinic, get_clinics, patch_clinic

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

router = APIRouter()


def _get_db(request: Request):
    """Yield a DB session per request."""
    session = request.app.state.SessionLocal()
    try:
        yield session
    finally:
        session.close()


# ── Page routes ──────────────────────────────────────────────────────────────

@router.get("/clinics")
def clinics_page():
    """Serve the ICP clinic list page."""
    return FileResponse(STATIC_DIR / "clinics.html")


@router.get("/clinics/{clinic_id}")
def clinic_detail_page(clinic_id: int):
    """Serve the ICP clinic detail page."""
    return FileResponse(STATIC_DIR / "clinic_detail.html")


# ── API routes ────────────────────────────────────────────────────────────────

@router.get("/api/clinics")
def api_list_clinics(
    search_field: str | None = Query(default=None),
    search_value: str | None = Query(default=None),
    missing_domain: bool = Query(default=False),
    missing_linkedin: bool = Query(default=False),
    missing_nip: bool = Query(default=False),
    limit: int = Query(default=25, ge=1, le=2000),
    db: Session = Depends(_get_db),
):
    """Return filtered list of ICP clinics."""
    logger.info(
        "API clinics list: field=%s value=%s missing_domain=%s missing_linkedin=%s missing_nip=%s limit=%s",
        search_field, search_value, missing_domain, missing_linkedin, missing_nip, limit,
    )
    return get_clinics(
        db,
        search_field=search_field,
        search_value=search_value,
        missing_domain=missing_domain,
        missing_linkedin=missing_linkedin,
        missing_nip=missing_nip,
        limit=limit,
    )


@router.get("/api/clinics/{clinic_id}")
def api_get_clinic(clinic_id: int, db: Session = Depends(_get_db)):
    """Return full detail for a single clinic."""
    clinic = get_clinic(db, clinic_id)
    if not clinic:
        raise HTTPException(status_code=404, detail="Clinic not found")
    return clinic


@router.patch("/api/clinics/{clinic_id}")
async def api_patch_clinic(clinic_id: int, request: Request, db: Session = Depends(_get_db)):
    """Update editable fields (website_domain, linkedin_url, nip, krs_number)."""
    body = await request.json()
    ok = patch_clinic(db, clinic_id, body)
    if not ok:
        raise HTTPException(status_code=404, detail="Clinic not found")
    return {"ok": True}
