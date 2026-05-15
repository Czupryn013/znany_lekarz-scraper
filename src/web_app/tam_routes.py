"""API routes for the TAM (Total Addressable Market) funnel calculator."""

import logging

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from web_app.tam_queries import get_tam_funnel, get_tam_distributions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tam")


def _get_db(request: Request):
    """Yield a DB session per request."""
    session = request.app.state.SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _parse_spec_ids(spec_ids: str | None) -> list[int] | None:
    """Parse comma-separated specialization IDs into a list of ints."""
    if not spec_ids:
        return None
    parsed = [int(s.strip()) for s in spec_ids.split(",") if s.strip().isdigit()]
    return parsed or None


def _parse_keywords(keywords: str | None) -> list[str] | None:
    """Parse newline-separated keyword string into a clean list."""
    if not keywords:
        return None
    parsed = [kw.strip() for kw in keywords.splitlines() if kw.strip()]
    return parsed or None


@router.get("/calculate")
def calculate(
    spec_ids: str | None = Query(default=None, description="Comma-separated specialization IDs"),
    keywords: str | None = Query(default=None, description="Newline-separated keyword list"),
    keyword_mode: str = Query(default="exclude", pattern="^(include|exclude)$"),
    min_doctors: int | None = Query(default=None, ge=0),
    min_locations: int | None = Query(default=None, ge=0),
    db: Session = Depends(_get_db),
):
    """Return funnel steps with clinic + doctor counts as each filter is applied."""
    parsed_specs = _parse_spec_ids(spec_ids)
    parsed_keywords = _parse_keywords(keywords)
    logger.info(
        "TAM calculate: spec_ids=%s keywords=%s mode=%s min_doctors=%s min_locations=%s",
        parsed_specs, parsed_keywords, keyword_mode, min_doctors, min_locations,
    )
    steps = get_tam_funnel(
        db,
        spec_ids=parsed_specs,
        keywords=parsed_keywords,
        keyword_mode=keyword_mode,
        min_doctors=min_doctors,
        min_locations=min_locations,
    )
    return {"steps": steps}


@router.get("/distributions")
def distributions(
    spec_ids: str | None = Query(default=None, description="Comma-separated specialization IDs"),
    keywords: str | None = Query(default=None, description="Newline-separated keyword list"),
    keyword_mode: str = Query(default="exclude", pattern="^(include|exclude)$"),
    min_doctors: int | None = Query(default=None, ge=0),
    min_locations: int | None = Query(default=None, ge=0),
    db: Session = Depends(_get_db),
):
    """Return distribution breakdowns for the filtered clinic set."""
    parsed_specs = _parse_spec_ids(spec_ids)
    parsed_keywords = _parse_keywords(keywords)
    logger.info(
        "TAM distributions: spec_ids=%s keywords=%s mode=%s min_doctors=%s min_locations=%s",
        parsed_specs, parsed_keywords, keyword_mode, min_doctors, min_locations,
    )
    return get_tam_distributions(
        db,
        spec_ids=parsed_specs,
        keywords=parsed_keywords,
        keyword_mode=keyword_mode,
        min_doctors=min_doctors,
        min_locations=min_locations,
    )
