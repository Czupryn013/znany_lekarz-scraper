"""Helpers for persisting full LinkedIn profiles fetched from Apify."""

import json
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from zl_scraper.db.models import LinkedInProfile
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.save_profiles")


def _normalize_profile_url(url: str) -> str:
    """Normalize profile URL for robust dedup and verdict lookup."""
    raw = (url or "").strip().lower().rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    return raw


def _as_optional_text(value) -> str | None:
    """Convert incoming values to text safely for VARCHAR/TEXT columns."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        # Common media structures: {"url": "..."}
        maybe_url = value.get("url") if "url" in value else None
        if isinstance(maybe_url, str) and maybe_url.strip():
            return maybe_url
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _as_optional_int(value) -> int | None:
    """Convert values to int when possible, otherwise None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit())
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
    return None


def _extract_profile_fields(profile: dict) -> dict:
    """Pull key SQL-column fields from a raw Apify profile dict."""
    url = profile.get("linkedinUrl") or profile.get("url") or ""

    # Location
    loc = profile.get("location") or {}
    parsed = loc.get("parsed") or {}
    location_text = parsed.get("text") or loc.get("linkedinText")
    country_code = parsed.get("countryCode") or loc.get("countryCode")

    # Profile picture — try several fields
    picture_url = profile.get("profilePicture") or profile.get("photo") or profile.get("coverPicture")

    # Current position from experience or currentPosition
    current_company = None
    current_position = None
    current_pos_list = profile.get("currentPosition") or []
    if current_pos_list:
        current_company = current_pos_list[0].get("companyName")
    exp = profile.get("experience") or []
    if exp:
        current_position = exp[0].get("position") or exp[0].get("title")
        if not current_company:
            current_company = exp[0].get("companyName")

    return {
        "linkedin_url": _as_optional_text(url),
        "public_identifier": _as_optional_text(profile.get("publicIdentifier")),
        "first_name": _as_optional_text(profile.get("firstName")),
        "last_name": _as_optional_text(profile.get("lastName")),
        "headline": _as_optional_text(profile.get("headline")),
        "location_text": _as_optional_text(location_text),
        "country_code": _as_optional_text(country_code),
        "profile_picture_url": _as_optional_text(picture_url),
        "current_company": _as_optional_text(current_company),
        "current_position": _as_optional_text(current_position),
        "connections_count": _as_optional_int(profile.get("connectionsCount")),
    }


def save_profiles(
    session: Session,
    lead_id: int,
    profiles: list[dict],
    verdicts: dict[str, str],
    search_context: str,
) -> int:
    """Persist Apify profile results to linkedin_profiles table.

    verdicts maps normalized linkedin_url → 'YES'/'MAYBE'/'NO'.
    Returns count of newly inserted rows.
    """
    if not profiles:
        return 0

    # Fetch existing URLs for this lead to avoid duplicates
    with session.no_autoflush:
        existing = set(
            _normalize_profile_url(row[0] or "")
            for row in session.query(LinkedInProfile.linkedin_url)
            .filter(LinkedInProfile.lead_id == lead_id)
            .all()
        )

    inserted = 0
    for profile in profiles:
        fields = _extract_profile_fields(profile)
        url = fields["linkedin_url"]
        if not url:
            continue

        norm_url = _normalize_profile_url(url)
        if norm_url in existing:
            continue
        existing.add(norm_url)

        verdict = (
            verdicts.get(norm_url)
            or verdicts.get(url.strip().lower().rstrip("/"))
            or "UNKNOWN"
        ).upper()

        row = LinkedInProfile(
            lead_id=lead_id,
            llm_verdict=verdict,
            search_context=search_context,
            raw_profile=profile,
            **fields,
        )
        session.add(row)
        inserted += 1

    if inserted:
        logger.info("Saved %d profiles for lead #%d (%s)", inserted, lead_id, search_context)

    return inserted
