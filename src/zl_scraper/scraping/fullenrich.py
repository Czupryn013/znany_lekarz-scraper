"""FullEnrich bulk contact enrichment API client (async submit + poll)."""

import time
import unicodedata

import httpx

from zl_scraper.config import (
    FULLENRICH_API_KEY,
    FULLENRICH_POLL_INTERVAL_SECS,
    FULLENRICH_POLL_TIMEOUT_SECS,
)
from zl_scraper.utils.logging import get_logger

logger = get_logger("fullenrich")

FULLENRICH_SUBMIT_URL = "https://app.fullenrich.com/api/v1/contact/enrich/bulk"
FULLENRICH_POLL_URL = "https://app.fullenrich.com/api/v2/contact/enrich/bulk/{enrichment_id}"


def _strip_invisible_chars(value: str) -> str:
    """Remove invisible/control Unicode chars that can break domain validation."""
    return "".join(
        ch for ch in value
        if unicodedata.category(ch) not in {"Cf", "Cc", "Cs"}
    )


def _sanitize_domain(domain: str) -> str:
    """Normalize a domain string for FullEnrich payloads."""
    cleaned = _strip_invisible_chars(domain or "").strip().lower()

    if cleaned.startswith("http://"):
        cleaned = cleaned[len("http://"):]
    elif cleaned.startswith("https://"):
        cleaned = cleaned[len("https://"):]

    cleaned = cleaned.split("/", 1)[0]
    cleaned = cleaned.split("?", 1)[0]
    cleaned = cleaned.split("#", 1)[0]

    return cleaned.strip(".")


def _sanitize_bulk_datas(datas: list[dict]) -> list[dict]:
    """Return a sanitized copy of FE payload items, cleaning domains."""
    sanitized: list[dict] = []
    changed = 0

    for item in datas:
        clean_item = dict(item)
        raw_domain = str(clean_item.get("domain") or "")
        clean_domain = _sanitize_domain(raw_domain)
        if clean_domain != raw_domain:
            changed += 1
        clean_item["domain"] = clean_domain
        sanitized.append(clean_item)

    if changed:
        logger.info("FullEnrich: sanitized %d domain values before submit", changed)

    return sanitized


def _auth_headers() -> dict:
    return {
        "Authorization": f"Bearer {FULLENRICH_API_KEY}",
        "Content-Type": "application/json",
    }


def submit_bulk(datas: list[dict], name: str | None = None) -> str:
    """Submit a bulk enrichment job and return the enrichment_id.

    Args:
        datas: list of dicts, each with keys: firstname, lastname, domain,
            company_name, linkedin_url (optional), enrich_fields, custom.
        name: optional job name for identification in FE dashboard.

    Returns:
        enrichment_id string.
    """
    if not datas:
        raise ValueError("Cannot submit empty batch to FullEnrich")

    sanitized_datas = _sanitize_bulk_datas(datas)

    job_name = name or f"KRS-{time.strftime('%Y-%m-%d_%H-%M')}"
    logger.info("FullEnrich: submitting %d contacts as '%s'", len(datas), job_name)

    resp = httpx.post(
        FULLENRICH_SUBMIT_URL,
        headers=_auth_headers(),
        json={
            "name": job_name,
            "datas": sanitized_datas,
        },
        timeout=60,
    )
    if resp.is_error:
        logger.info(datas)
        logger.error(
            "FullEnrich submit error %d: %s", resp.status_code, resp.text,
        )
    resp.raise_for_status()
    data = resp.json()
    enrichment_id = data.get("id") or data.get("enrichment_id")

    if not enrichment_id:
        raise RuntimeError(f"FullEnrich submit did not return an ID: {data}")

    logger.info("FullEnrich: job submitted, enrichment_id=%s", enrichment_id)
    return enrichment_id


def poll_results(enrichment_id: str) -> dict:
    """Poll FullEnrich until the job finishes. Returns the full response dict.

    Polls every FULLENRICH_POLL_INTERVAL_SECS seconds, gives up after
    FULLENRICH_POLL_TIMEOUT_SECS total.
    """
    url = FULLENRICH_POLL_URL.format(enrichment_id=enrichment_id)
    deadline = time.time() + FULLENRICH_POLL_TIMEOUT_SECS
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        resp = httpx.get(url, headers=_auth_headers(), timeout=30)
        if resp.is_error:
            logger.error(
                "FullEnrich poll error %d: %s", resp.status_code, resp.text,
            )
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "UNKNOWN")

        logger.info(
            "FullEnrich poll #%d: status=%s (enrichment_id=%s)",
            attempt, status, enrichment_id,
        )

        if status == "FINISHED":
            return data

        if status in ("FAILED", "ERROR"):
            raise RuntimeError(f"FullEnrich job {enrichment_id} failed: {data}")

        time.sleep(FULLENRICH_POLL_INTERVAL_SECS)

    raise TimeoutError(
        f"FullEnrich job {enrichment_id} did not finish within "
        f"{FULLENRICH_POLL_TIMEOUT_SECS}s ({attempt} polls)"
    )


def enrich_and_wait(datas: list[dict], name: str | None = None) -> dict:
    """Submit a bulk job and block until results are ready."""
    enrichment_id = submit_bulk(datas, name=name)
    return poll_results(enrichment_id)


def parse_fullenrich_result(item: dict) -> dict:
    """Extract phone, email, linkedin_url from a single FullEnrich data item."""
    contact_info = item.get("contact_info", {})
    profile = item.get("profile", {})
    custom = item.get("custom", {})

    # Best phone
    phone = None
    most_probable = contact_info.get("most_probable_phone", {})
    if most_probable and most_probable.get("number"):
        phone = most_probable["number"]
    elif contact_info.get("phones"):
        phone = contact_info["phones"][0].get("number")

    # Best email (prefer work, then personal)
    email = None
    work_email = contact_info.get("most_probable_work_email", {})
    personal_email = contact_info.get("most_probable_personal_email", {})
    if work_email and work_email.get("email"):
        email = work_email["email"]
    elif personal_email and personal_email.get("email"):
        email = personal_email["email"]

    # LinkedIn from profile
    linkedin_url = None
    social = profile.get("social_profiles", {})
    li = social.get("linkedin", {})
    if li and li.get("url"):
        linkedin_url = li["url"]

    return {
        "lead_id": custom.get("lead_id"),
        "phone": phone,
        "email": email,
        "linkedin_url": linkedin_url,
    }


# ── People Search API (v2) ──────────────────────────────────────────────

FULLENRICH_SEARCH_URL = "https://app.fullenrich.com/api/v2/people/search"


def search_person(full_name: str, company_domain: str) -> dict | None:
    """Search for a person by name + company domain via FullEnrich People Search API.

    Returns dict with linkedin_url and connection_count if found, else None.
    """
    if not full_name or not company_domain:
        return None

    domain = _sanitize_domain(company_domain)
    logger.info("FullEnrich search: %s @ %s", full_name, domain)

    resp = httpx.post(
        FULLENRICH_SEARCH_URL,
        headers=_auth_headers(),
        json={
            "limit": 1,
            "person_names": [{"value": full_name, "exact_match": True, "exclude": False}],
            "current_company_domains": [{"value": domain, "exact_match": True, "exclude": False}],
        },
        timeout=30,
    )
    if resp.is_error:
        logger.error("FullEnrich search error %d: %s", resp.status_code, resp.text)
        resp.raise_for_status()

    data = resp.json()
    people = data.get("people", [])

    if not people:
        logger.info("FullEnrich search: no results for %s @ %s", full_name, domain)
        return None

    person = people[0]
    social = person.get("social_profiles", {})
    li = social.get("linkedin", {})

    result = {
        "linkedin_url": li.get("url"),
        "connection_count": li.get("connection_count"),
    }
    logger.info("FullEnrich search found: %s → %s", full_name, result["linkedin_url"])
    return result
