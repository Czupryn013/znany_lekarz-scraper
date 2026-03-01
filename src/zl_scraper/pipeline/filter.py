"""Filter enriched clinics by doctor count and specialization fit (ICP matching)."""

import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from zl_scraper.config import SPECIALIZATIONS_PATH
from zl_scraper.db.models import Clinic, ClinicLocation, Doctor, SearchQuery, Specialization, clinic_doctors

logger = logging.getLogger(__name__)

# ── Specialization keywords to EXCLUDE ───────────────────────────────────
# A specialization is excluded if its name contains ANY of these substrings
# (case-insensitive). This covers the root specialization and all its
# child / related variants automatically.
DEFAULT_EXCLUDED_KEYWORDS: list[str] = [
    "psychiatra",
    "psycholog",
    "geriatra",
    "fizjo",
    "weterynarz",
    "stomatolog",
    "lekarz rodzinny",
    "położna",
    "logopeda",
    "onkolog",
    "medycyny estetycznej",
    "rehabilitac",
    "biegły sądowy",
    "chirurg",
    "lekarz pierwszego kontaktu",
]

DEFAULT_MIN_DOCTORS = 20


def load_all_specializations(path: Path = SPECIALIZATIONS_PATH) -> list[dict[str, Any]]:
    """Load the full specialization catalogue from the JSON file."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _matches_any_keyword(name: str, keywords: list[str]) -> bool:
    """Return True if *name* contains any of the exclusion keywords (case-insensitive)."""
    lower = name.lower()
    return any(kw.lower() in lower for kw in keywords)


def build_allowed_specialization_names(
    excluded_keywords: list[str] = DEFAULT_EXCLUDED_KEYWORDS,
    spec_path: Path = SPECIALIZATIONS_PATH,
) -> list[str]:
    """Return specialization names that pass the ICP filter (all minus excluded)."""
    all_specs = load_all_specializations(spec_path)
    allowed = [
        s["name"]
        for s in all_specs
        if not _matches_any_keyword(s["name"], excluded_keywords)
    ]
    logger.info(
        "ICP specialization filter: %d allowed out of %d total (%d excluded)",
        len(allowed),
        len(all_specs),
        len(all_specs) - len(allowed),
    )
    return allowed


def build_excluded_specialization_names(
    excluded_keywords: list[str] = DEFAULT_EXCLUDED_KEYWORDS,
    spec_path: Path = SPECIALIZATIONS_PATH,
) -> list[str]:
    """Return specialization names that are excluded by the ICP filter."""
    all_specs = load_all_specializations(spec_path)
    return [
        s["name"]
        for s in all_specs
        if _matches_any_keyword(s["name"], excluded_keywords)
    ]


class FilterResult:
    """Container for staged filter output with per-step rejection counts."""

    def __init__(
        self,
        total_enriched: int,
        rejected_doctors: int,
        rejected_specialization: int,
        matched: list[Clinic],
    ) -> None:
        self.total_enriched = total_enriched
        self.rejected_doctors = rejected_doctors
        self.rejected_specialization = rejected_specialization
        self.matched = matched

    @property
    def total_matched(self) -> int:
        return len(self.matched)

    @property
    def total_filtered_out(self) -> int:
        return self.rejected_doctors + self.rejected_specialization

    @property
    def total_doctors_in_matched(self) -> int:
        return sum(c.doctors_count or 0 for c in self.matched)

    @property
    def avg_doctors(self) -> float:
        return self.total_doctors_in_matched / self.total_matched if self.matched else 0


def query_filtered_clinics(
    session: Session,
    min_doctors: int = DEFAULT_MIN_DOCTORS,
    allowed_spec_names: list[str] | None = None,
) -> FilterResult:
    """Return enriched clinics filtered in two stages: doctor count first, then specialization."""
    if allowed_spec_names is None:
        allowed_spec_names = build_allowed_specialization_names()

    # Stage 0: all enriched clinics
    total_enriched = (
        session.query(Clinic)
        .filter(Clinic.enriched_at.isnot(None))
        .count()
    )

    # Stage 1: filter by minimum doctor count
    passed_doctors_q = (
        session.query(Clinic)
        .filter(
            Clinic.enriched_at.isnot(None),
            Clinic.doctors_count >= min_doctors,
        )
    )
    passed_doctors_count = passed_doctors_q.count()
    rejected_doctors = total_enriched - passed_doctors_count

    # Stage 2: from those, keep only clinics with ≥1 allowed specialization
    has_allowed_spec = (
        session.query(SearchQuery.clinic_id)
        .join(Specialization, SearchQuery.specialization_id == Specialization.id)
        .filter(Specialization.name.in_(allowed_spec_names))
        .distinct()
        .subquery()
    )

    clinics = (
        passed_doctors_q
        .filter(Clinic.id.in_(session.query(has_allowed_spec.c.clinic_id)))
        .order_by(Clinic.doctors_count.desc())
        .all()
    )
    rejected_specialization = passed_doctors_count - len(clinics)

    logger.info(
        "Filter: %d enriched → %d rejected (doctors < %d) → %d rejected (spec) → %d matched",
        total_enriched,
        rejected_doctors,
        min_doctors,
        rejected_specialization,
        len(clinics),
    )
    return FilterResult(
        total_enriched=total_enriched,
        rejected_doctors=rejected_doctors,
        rejected_specialization=rejected_specialization,
        matched=clinics,
    )


def get_clinic_specialization_names(session: Session, clinic_id: int) -> list[str]:
    """Return all specialization names linked to a clinic via search_queries."""
    rows = (
        session.query(Specialization.name)
        .join(SearchQuery, SearchQuery.specialization_id == Specialization.id)
        .filter(SearchQuery.clinic_id == clinic_id)
        .distinct()
        .all()
    )
    return [r[0] for r in rows]


def build_export_rows(
    session: Session,
    clinics: list[Clinic],
) -> list[dict[str, Any]]:
    """Build flat dicts ready for CSV/JSON export from filtered clinics."""
    rows: list[dict[str, Any]] = []
    for clinic in clinics:
        locations = session.query(ClinicLocation).filter_by(clinic_id=clinic.id).all()
        addresses = [loc.address for loc in locations if loc.address]
        spec_names = get_clinic_specialization_names(session, clinic.id)

        # Collect social / web URLs from locations
        websites = list({loc.website_url for loc in locations if loc.website_url})
        linkedins = list({loc.linkedin_url for loc in locations if loc.linkedin_url})

        rows.append(
            {
                "id": clinic.id,
                "name": clinic.name,
                "zl_url": clinic.zl_url,
                "nip": clinic.nip,
                "legal_name": clinic.legal_name,
                "doctors_count": clinic.doctors_count,
                "zl_reviews_cnt": clinic.zl_reviews_cnt,
                "specializations": "; ".join(sorted(spec_names)),
                "addresses": "; ".join(addresses),
                "website_url": "; ".join(websites),
                "linkedin_url": "; ".join(linkedins),
                "enriched_at": str(clinic.enriched_at) if clinic.enriched_at else None,
            }
        )
    return rows
