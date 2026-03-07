"""Exclude worked domains (demos, lost deals, big chains, pipeline) from ICP."""

import logging
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from zl_scraper.db.models import Clinic
from zl_scraper.pipeline.worked_domains import WORKED_DOMAINS, get_worked_domain_set

logger = logging.getLogger(__name__)


@dataclass
class WorkedFilterResult:
    """Summary of the worked-domain exclusion pass."""

    total_icp: int = 0
    excluded_count: int = 0
    excluded_clinics: list[tuple[int, str, str, str]] = field(default_factory=list)
    """Each tuple: (clinic_id, clinic_name, domain, reason)."""


def _reason_for_domain(domain: str) -> str:
    """Look up the human-readable reason for a worked domain."""
    domain_lower = domain.lower()
    for entry in WORKED_DOMAINS:
        if entry["domain"].lower() == domain_lower:
            return entry["reason"]
    return "unknown"


def exclude_worked_clinics(
    session: Session,
    *,
    dry_run: bool = False,
) -> WorkedFilterResult:
    """Flip icp_match to False for ICP clinics whose domain is in the worked list."""
    logger.info("filter-worked: starting exclusion pass (dry_run=%s)", dry_run)

    worked_set = get_worked_domain_set()
    result = WorkedFilterResult()

    icp_clinics = (
        session.query(Clinic)
        .filter(Clinic.icp_match.is_(True))
        .all()
    )
    result.total_icp = len(icp_clinics)

    for clinic in icp_clinics:
        domain = (clinic.website_domain or "").lower()
        if domain in worked_set:
            reason = _reason_for_domain(domain)
            result.excluded_clinics.append((clinic.id, clinic.name, domain, reason))
            if not dry_run:
                clinic.icp_match = False

    result.excluded_count = len(result.excluded_clinics)

    if not dry_run and result.excluded_count > 0:
        session.commit()
        logger.info("filter-worked: excluded %d clinics", result.excluded_count)
    else:
        logger.info(
            "filter-worked: %d clinics would be excluded (dry-run)" if dry_run
            else "filter-worked: no clinics matched worked domains",
            result.excluded_count,
        )

    return result
