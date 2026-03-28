"""KRS/CEIDG enrichment orchestrator — look up clinics by NIP, extract board members."""

import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright, Page, BrowserContext
from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import BoardMember, Clinic
from zl_scraper.utils.logging import get_logger

from .krs_scraper import scrape_krs, navigate_to_krs, KRSResult
from .ceidg_scraper import scrape_ceidg, CEIDGResult
from .krs_pdf import fetch_krs_pdf, extract_text_pages, parse_board_members
from .utils import KRS_SEARCH_URL

from sqlalchemy import func

logger = get_logger("krs_enrich")


# ── Query ────────────────────────────────────────────────────────────────


def _get_clinics_to_process(
    session: Session,
    icp_only: bool = True,
    limit: Optional[int] = None,
) -> list[Clinic]:
    """Return clinics with a NIP that have not yet been KRS-searched."""
    query = (
        session.query(Clinic)
        .filter(
            Clinic.nip.isnot(None),
            Clinic.nip != "",
            Clinic.krs_searched_at.is_(None),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


def _get_clinics_to_retry(
    session: Session,
    icp_only: bool = True,
    limit: Optional[int] = None,
) -> list[Clinic]:
    """Return clinics that were KRS-searched but had NOT_FOUND/ERROR or 0 board members."""
    member_count = (
        session.query(BoardMember.clinic_id, func.count(BoardMember.id).label("cnt"))
        .group_by(BoardMember.clinic_id)
        .subquery()
    )
    query = (
        session.query(Clinic)
        .outerjoin(member_count, Clinic.id == member_count.c.clinic_id)
        .filter(
            Clinic.nip.isnot(None),
            Clinic.nip != "",
            Clinic.krs_searched_at.isnot(None),
            (
                Clinic.legal_type.in_(["NOT_FOUND", "ERROR"])
                | (func.coalesce(member_count.c.cnt, 0) == 0)
            ),
        )
        .order_by(Clinic.id)
    )
    if icp_only:
        query = query.filter(Clinic.icp_match.is_(True))
    if limit:
        query = query.limit(limit)
    return query.all()


# ── Single NIP processing ───────────────────────────────────────────────


def _process_single_nip(
    page: Page,
    context: BrowserContext,
    nip: str,
) -> dict:
    """Scrape KRS then CEIDG for a single NIP. Return structured result dict."""
    krs_result: Optional[KRSResult] = None
    ceidg_result: Optional[CEIDGResult] = None

    # Try KRS first
    krs_result = scrape_krs(page, nip)

    if krs_result and krs_result.found:
        return {
            "legal_type": "KRS",
            "company_name": krs_result.company_name,
            "krs_number": krs_result.krs_number,
            "regon": krs_result.regon,
            "registration_date": krs_result.registration_date,
            "krs_code": krs_result.krs_code,
            "apikey": krs_result.apikey,
            "register_type": krs_result.register_type,
        }

    # Fallback to CEIDG
    logger.info("NIP %s not in KRS, trying CEIDG…", nip)
    ceidg_result = scrape_ceidg(context, page, nip)

    # Navigate back to KRS for next iteration
    navigate_to_krs(page)

    if ceidg_result and ceidg_result.found:
        return {
            "legal_type": f"CEIDG_{ceidg_result.source}",  # CEIDG_JDG or CEIDG_SC
            "company_name": ceidg_result.legal_name,
            "krs_number": None,
            "regon": ceidg_result.regon,
            "registration_date": ceidg_result.registered_at,
            "krs_code": None,
            "apikey": None,
            "owners": ceidg_result.owners,
        }

    return {"legal_type": "NOT_FOUND"}


# ── KRS PDF board extraction ────────────────────────────────────────────


def _extract_krs_board(apikey: str, krs_code: str, register_type: str = "P") -> list[dict]:
    """Fetch KRS PDF and parse board members + prokurenci. Raises on failure."""
    pdf_bytes = fetch_krs_pdf(apikey, krs_code, register_type=register_type)
    pages = extract_text_pages(pdf_bytes)
    members = parse_board_members(pages)
    return members


# ── CEIDG owners → board member dicts ────────────────────────────────────


def _ceidg_owners_to_members(owners: list, legal_type: str) -> list[dict]:
    """Convert CEIDG Owner dataclass instances to board member dicts."""
    source = legal_type  # CEIDG_JDG or CEIDG_SC
    members: list[dict] = []
    for owner in owners:
        if not owner.full_name:
            continue
        members.append({
            "full_name": owner.full_name,
            "pesel": None,
            "role": "WŁAŚCICIEL" if source == "CEIDG_JDG" else "WSPÓLNIK",
            "source": source,
        })
    return members


# ── Save helpers ─────────────────────────────────────────────────────────


def _save_clinic_krs_data(session: Session, clinic: Clinic, result: dict) -> None:
    """Update clinic row with KRS/CEIDG registry data."""
    clinic.legal_type = result.get("legal_type")
    clinic.krs_number = result.get("krs_number")
    clinic.regon = result.get("regon")
    clinic.registration_date = result.get("registration_date")
    clinic.krs_searched_at = datetime.utcnow()

    # Overwrite legal_name from KRS (more authoritative than ZL)
    if result.get("legal_type") == "KRS" and result.get("company_name"):
        clinic.legal_name = result["company_name"]


def _save_board_members(session: Session, clinic: Clinic, members: list[dict]) -> None:
    """Insert BoardMember rows for a clinic."""
    for m in members:
        session.add(BoardMember(
            clinic_id=clinic.id,
            full_name=m["full_name"].title() if m["full_name"] else None,
            pesel=m.get("pesel"),
            role=m.get("role"),
            source=m["source"],
        ))


# ── Summary helpers ──────────────────────────────────────────────────────


def _print_summary(stats: dict, processed: int, total: int, label: str = "Progress") -> None:
    """Print a formatted summary table of entity types and board member counts."""
    all_types = sorted(stats["counts"].keys())
    header = f"{'Entity type':<20} {'Clinics':>8} {'Board members':>14}"
    separator = "-" * len(header)
    lines = [
        f"\n=== {label} ({processed}/{total} processed) ===",
        header,
        separator,
    ]
    for lt in all_types:
        lines.append(
            f"{lt:<20} {stats['counts'][lt]:>8} {stats['members'][lt]:>14}"
        )
    lines.append(separator)
    lines.append(
        f"{'TOTAL':<20} {sum(stats['counts'].values()):>8} {sum(stats['members'].values()):>14}"
    )
    logger.info("\n".join(lines))


# ── Main orchestrator ────────────────────────────────────────────────────


def run_krs_enrich(
    limit: Optional[int] = None,
    icp_only: bool = True,
    headless: bool = False,
    retry_404: bool = False,
) -> None:
    """Look up clinics by NIP in KRS/CEIDG, extract board members, save to DB.

    Args:
        limit: Maximum number of clinics to process (None = all pending).
        icp_only: Only process ICP-matched clinics.
        headless: Run Playwright browser without visible window (default: False = visible).
        retry_404: Re-scrape clinics with NOT_FOUND/ERROR or 0 board members.
    """
    session = SessionLocal()
    try:
        if retry_404:
            clinics = _get_clinics_to_retry(session, icp_only=icp_only, limit=limit)
        else:
            clinics = _get_clinics_to_process(session, icp_only=icp_only, limit=limit)
        total = len(clinics)
        if total == 0:
            logger.info("No clinics pending KRS enrichment")
            return

        logger.info("Starting KRS enrichment for %d clinics", total)

        stats: dict = {"counts": defaultdict(int), "members": defaultdict(int)}

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless, args=["--start-maximized"])
            context = browser.new_context(no_viewport=True)
            page = context.new_page()

            # Start on KRS search page
            page.goto(KRS_SEARCH_URL)
            page.wait_for_load_state("networkidle")

            for idx, clinic in enumerate(clinics, 1):
                nip = clinic.nip
                logger.info(
                    "[%d/%d] Processing clinic #%d (%s) NIP=%s",
                    idx, total, clinic.id, clinic.name, nip,
                )

                try:
                    result = _process_single_nip(page, context, nip)
                    legal_type = result.get("legal_type", "NOT_FOUND")

                    # On retry, wipe old board members before re-saving
                    if retry_404:
                        session.query(BoardMember).filter(
                            BoardMember.clinic_id == clinic.id
                        ).delete()

                    # Save clinic-level data
                    _save_clinic_krs_data(session, clinic, result)

                    # Extract and save board members
                    members: list[dict] = []

                    if legal_type == "KRS":
                        apikey = result.get("apikey")
                        krs_code = result.get("krs_code")
                        register_type = result.get("register_type", "P")
                        if apikey and krs_code:
                            members = _extract_krs_board(apikey, krs_code, register_type=register_type)
                        else:
                            logger.warning(
                                "KRS found but missing apikey/krs_code for clinic #%d", clinic.id
                            )

                    elif legal_type.startswith("CEIDG_"):
                        owners = result.get("owners", [])
                        members = _ceidg_owners_to_members(owners, legal_type)

                    if members:
                        _save_board_members(session, clinic, members)
                        logger.info(
                            "  Saved %d board members (source=%s)", len(members), legal_type
                        )
                    else:
                        logger.info("  No board members extracted (legal_type=%s)", legal_type)

                    stats["counts"][legal_type] += 1
                    stats["members"][legal_type] += len(members)

                except Exception:
                    logger.exception("Error processing clinic #%d NIP=%s", clinic.id, nip)
                    # Still stamp as searched so we don't retry endlessly
                    clinic.krs_searched_at = datetime.utcnow()
                    clinic.legal_type = "ERROR"
                    stats["counts"]["ERROR"] += 1

                # Commit after each clinic
                session.commit()
                logger.info("  Committed clinic #%d (%d/%d)", clinic.id, idx, total)

                # Print summary every 100 clinics
                if idx % 100 == 0:
                    _print_summary(stats, idx, total, label="Interim summary")

                # Cooldown between clinics
                time.sleep(2)

            _print_summary(stats, total, total, label="Final summary")
            logger.info("KRS enrichment complete: %d/%d clinics processed", total, total)

            browser.close()
    finally:
        session.close()
