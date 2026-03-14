"""Phone enrichment waterfall: Prospeo → FullEnrich → Lusha."""

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, Lead, lead_clinic_roles
from zl_scraper.scraping.prospeo import enrich_bulk as prospeo_enrich, parse_prospeo_result
from zl_scraper.scraping.fullenrich import enrich_and_wait as fe_enrich, parse_fullenrich_result
from zl_scraper.scraping.lusha import enrich_bulk as lusha_enrich, parse_lusha_result
from zl_scraper.utils.logging import get_logger

logger = get_logger("enrich_phones")

# Batch sizes per provider
PROSPEO_BATCH_SIZE = 49
FULLENRICH_BATCH_SIZE = 50
LUSHA_BATCH_SIZE = 50


# ── Query helpers ────────────────────────────────────────────────────────


def _get_leads_by_status(
    session: Session,
    status: str,
    limit: Optional[int] = None,
    allowed_lead_ids: Optional[set[int]] = None,
) -> list[Lead]:
    """Return leads with the given enrichment_status, ordered by id."""
    query = (
        session.query(Lead)
        .filter(Lead.enrichment_status == status)
        .order_by(Lead.id)
    )
    if allowed_lead_ids is not None:
        if not allowed_lead_ids:
            return []
        query = query.filter(Lead.id.in_(allowed_lead_ids))
    if limit is not None:
        query = query.limit(limit)
    return query.all()


def _get_lead_clinic_info(session: Session, lead_id: int) -> dict:
    """Get the first associated clinic's domain and name for enrichment input."""
    row = (
        session.query(Clinic.website_domain, Clinic.name, Clinic.legal_name)
        .join(lead_clinic_roles, Clinic.id == lead_clinic_roles.c.clinic_id)
        .filter(lead_clinic_roles.c.lead_id == lead_id)
        .first()
    )
    if not row:
        return {"domain": "", "company_name": ""}
    return {
        "domain": row.website_domain or "",
        "company_name": row.legal_name or row.name or "",
    }


def _update_lead_contact(lead: Lead, phone: str | None, email: str | None, linkedin_url: str | None) -> None:
    """Set phone/email/linkedin_url on a lead if they are newly discovered (don't overwrite existing)."""
    if phone and not lead.phone:
        lead.phone = phone
    if email and not lead.email:
        lead.email = email
    if linkedin_url and not lead.linkedin_url:
        lead.linkedin_url = linkedin_url


# ── Prospeo tier ─────────────────────────────────────────────────────────


def _run_prospeo(
    session: Session,
    limit: Optional[int] = None,
    allowed_lead_ids: Optional[set[int]] = None,
) -> tuple[int, int]:
    """Enrich PENDING leads via Prospeo. Returns (processed, phones_found)."""
    leads = _get_leads_by_status(session, "PENDING", limit=limit, allowed_lead_ids=allowed_lead_ids)
    if not leads:
        logger.info("Prospeo: no PENDING leads to process")
        return 0, 0

    logger.info("Prospeo: %d PENDING leads to process", len(leads))
    total_processed = 0
    total_phones = 0

    for i in range(0, len(leads), PROSPEO_BATCH_SIZE):
        batch = leads[i : i + PROSPEO_BATCH_SIZE]
        lead_map = {str(lead.id): lead for lead in batch}

        # Build Prospeo input
        prospeo_input = []
        for lead in batch:
            clinic_info = _get_lead_clinic_info(session, lead.id)
            prospeo_input.append({
                "identifier": str(lead.id),
                "full_name": lead.full_name,
                "company_website": clinic_info["domain"],
                "company_name": clinic_info["company_name"],
                "linkedin_url": lead.linkedin_url or "",
            })

        try:
            response = prospeo_enrich(prospeo_input)
        except Exception:
            logger.exception("Prospeo API call failed for batch starting at lead #%d – halting pipeline", batch[0].id)
            raise

        # Parse matched results
        matched_ids: set[str] = set()
        for item in response.get("matched", []):
            parsed = parse_prospeo_result(item)
            identifier = parsed["identifier"]
            if identifier not in lead_map:
                continue

            lead = lead_map[identifier]
            matched_ids.add(identifier)

            _update_lead_contact(lead, parsed["phone"], parsed["email"], parsed["linkedin_url"])

            if lead.phone:
                lead.phone_source = "PROSPEO"
            lead.enrichment_status = "PROSPEO_DONE"
            lead.updated_at = datetime.utcnow()

        # Mark unmatched as PROSPEO_DONE too
        for identifier, lead in lead_map.items():
            if identifier not in matched_ids:
                lead.enrichment_status = "PROSPEO_DONE"
                lead.updated_at = datetime.utcnow()

        session.commit()
        total_processed += len(batch)
        found_phones = sum(1 for lid in matched_ids if lead_map[lid].phone)
        total_phones += found_phones
        logger.info(
            "Prospeo batch: %d/%d matched, %d phones found",
            len(matched_ids), len(batch), found_phones,
        )

    return total_processed, total_phones


# ── FullEnrich tier ──────────────────────────────────────────────────────


def _run_fullenrich(
    session: Session,
    limit: Optional[int] = None,
    allowed_lead_ids: Optional[set[int]] = None,
) -> tuple[int, int]:
    """Enrich PROSPEO_DONE leads (without phone) via FullEnrich. Returns (processed, phones_found)."""
    leads = (
        session.query(Lead)
        .filter(
            Lead.enrichment_status == "PROSPEO_DONE",
            Lead.phone.is_(None),
        )
        .order_by(Lead.id)
    )
    if allowed_lead_ids is not None:
        if not allowed_lead_ids:
            logger.info("FullEnrich: no cohort leads allowed")
            return 0, 0
        leads = leads.filter(Lead.id.in_(allowed_lead_ids))
    if limit is not None:
        leads = leads.limit(limit)
    leads = leads.all()

    if not leads:
        logger.info("FullEnrich: no PROSPEO_DONE leads without phone")
        return 0, 0

    logger.info("FullEnrich: %d leads to process", len(leads))
    total_processed = 0
    total_phones = 0

    for i in range(0, len(leads), FULLENRICH_BATCH_SIZE):
        batch = leads[i : i + FULLENRICH_BATCH_SIZE]
        lead_map = {str(lead.id): lead for lead in batch}

        # Build FullEnrich input
        fe_input = []
        for lead in batch:
            clinic_info = _get_lead_clinic_info(session, lead.id)
            name_parts = lead.full_name.split(" ", 1)
            firstname = name_parts[0]
            lastname = name_parts[1] if len(name_parts) > 1 else ""

            fe_input.append({
                "firstname": firstname,
                "lastname": lastname,
                "linkedin_url": lead.linkedin_url or "",
                "domain": clinic_info["domain"] or "repto.pl",
                "company_name": clinic_info["company_name"],
                "enrich_fields": ["contact.phones"],
                "custom": {
                    "lead_id": str(lead.id),
                },
            })

        try:
            response = fe_enrich(fe_input)
        except Exception:
            logger.exception(
                "FullEnrich call failed for batch starting at lead #%d – halting pipeline", batch[0].id
            )
            raise

        # Parse results
        found_phones = 0
        for item in response.get("data", []):
            parsed = parse_fullenrich_result(item)
            lead_id_str = parsed.get("lead_id")
            if not lead_id_str or lead_id_str not in lead_map:
                continue

            lead = lead_map[lead_id_str]
            _update_lead_contact(lead, parsed["phone"], parsed["email"], parsed["linkedin_url"])

            if lead.phone:
                lead.phone_source = "FULLENRICH"
                found_phones += 1

        # Advance all to FE_DONE
        for lead in batch:
            lead.enrichment_status = "FE_DONE"
            lead.updated_at = datetime.utcnow()

        session.commit()
        total_processed += len(batch)
        total_phones += found_phones
        logger.info(
            "FullEnrich batch: %d/%d phones found",
            found_phones, len(batch),
        )

    return total_processed, total_phones


# ── Lusha tier ───────────────────────────────────────────────────────────


def _run_lusha(
    session: Session,
    limit: Optional[int] = None,
    allowed_lead_ids: Optional[set[int]] = None,
) -> tuple[int, int]:
    """Enrich FE_DONE leads (without phone) via Lusha. Returns (processed, phones_found)."""
    leads = (
        session.query(Lead)
        .filter(
            Lead.enrichment_status == "FE_DONE",
            Lead.phone.is_(None),
        )
        .order_by(Lead.id)
    )
    if allowed_lead_ids is not None:
        if not allowed_lead_ids:
            logger.info("Lusha: no cohort leads allowed")
            return 0, 0
        leads = leads.filter(Lead.id.in_(allowed_lead_ids))
    if limit is not None:
        leads = leads.limit(limit)
    leads = leads.all()

    if not leads:
        logger.info("Lusha: no FE_DONE leads without phone")
        return 0, 0

    logger.info("Lusha: %d leads to process", len(leads))
    total_processed = 0
    total_phones = 0

    for i in range(0, len(leads), LUSHA_BATCH_SIZE):
        batch = leads[i : i + LUSHA_BATCH_SIZE]
        lead_map = {str(lead.id): lead for lead in batch}

        # Build Lusha input
        lusha_input = []
        for lead in batch:
            clinic_info = _get_lead_clinic_info(session, lead.id)
            lusha_input.append({
                "contactId": str(lead.id),
                "fullName": lead.full_name,
                "companies": [{
                    "domain": clinic_info["domain"],
                    "name": clinic_info["company_name"],
                    "isCurrent": True,
                }],
                "linkedinUrl": lead.linkedin_url or "",
            })

        try:
            response = lusha_enrich(lusha_input)
        except Exception:
            logger.exception("Lusha API call failed for batch starting at lead #%d – halting pipeline", batch[0].id)
            raise

        # Parse results
        contacts_map = response.get("contacts", {})
        found_phones = 0

        for contact_id_str, result in contacts_map.items():
            if result.get("error"):
                continue
            parsed = parse_lusha_result(contact_id_str, result)

            # Lusha keys contacts by the contactId we provided
            lead = lead_map.get(parsed["contact_id"])
            if not lead:
                continue

            _update_lead_contact(lead, parsed["phone"], None, parsed["linkedin_url"])
            if lead.phone:
                lead.phone_source = "LUSHA"
                found_phones += 1

        # Advance all to LUSHA_DONE
        for lead in batch:
            lead.enrichment_status = "LUSHA_DONE"
            lead.updated_at = datetime.utcnow()

        session.commit()
        total_processed += len(batch)
        total_phones += found_phones
        logger.info(
            "Lusha batch: %d/%d phones found",
            found_phones, len(batch),
        )

    return total_processed, total_phones


# ── Leads that already got a phone at an earlier step ────────────────────


def _advance_leads_with_phone(session: Session, from_status: str, to_status: str) -> int:
    """Move leads that already have a phone past the current waterfall tier."""
    leads = (
        session.query(Lead)
        .filter(
            Lead.enrichment_status == from_status,
            Lead.phone.isnot(None),
        )
        .all()
    )
    for lead in leads:
        lead.enrichment_status = to_status
        lead.updated_at = datetime.utcnow()
    if leads:
        session.commit()
        logger.info(
            "Advanced %d already-phone leads from %s → %s",
            len(leads), from_status, to_status,
        )
    return len(leads)


def _build_run_cohort(session: Session, limit: Optional[int], step: Optional[str]) -> Optional[set[int]]:
    """Build a lead-id cohort so --limit counts each lead once per CLI run."""
    if limit is None:
        return None

    if limit <= 0:
        return set()

    remaining = limit
    cohort: set[int] = set()

    def add_ids(query) -> None:
        nonlocal remaining
        if remaining <= 0:
            return
        rows = query.limit(remaining).all()
        for lead_id, in rows:
            if lead_id not in cohort:
                cohort.add(lead_id)
                remaining -= 1
                if remaining <= 0:
                    break

    if step == "prospeo":
        add_ids(
            session.query(Lead.id)
            .filter(Lead.enrichment_status == "PENDING")
            .order_by(Lead.id)
        )
    elif step == "fullenrich":
        add_ids(
            session.query(Lead.id)
            .filter(Lead.enrichment_status == "PROSPEO_DONE", Lead.phone.is_(None))
            .order_by(Lead.id)
        )
    elif step == "lusha":
        add_ids(
            session.query(Lead.id)
            .filter(Lead.enrichment_status == "FE_DONE", Lead.phone.is_(None))
            .order_by(Lead.id)
        )
    else:
        add_ids(
            session.query(Lead.id)
            .filter(Lead.enrichment_status == "PROSPEO_DONE", Lead.phone.is_(None))
            .order_by(Lead.id)
        )
        add_ids(
            session.query(Lead.id)
            .filter(Lead.enrichment_status == "FE_DONE", Lead.phone.is_(None))
            .order_by(Lead.id)
        )
        add_ids(
            session.query(Lead.id)
            .filter(Lead.enrichment_status == "PENDING")
            .order_by(Lead.id)
        )

    return cohort


# ── Main orchestrator ────────────────────────────────────────────────────


def run_enrich_phones(
    limit: Optional[int] = None,
    step: Optional[str] = None,
    retry_no_phone: bool = False,
    retry_linkedin: bool = False,
) -> None:
    """Run the phone enrichment waterfall: Prospeo → FullEnrich → Lusha.

    Mid-work leads (PROSPEO_DONE, FE_DONE) are processed first, then fresh
    PENDING leads enter Prospeo with the optional --limit cap.

    Args:
        limit: Global cap on unique leads in this CLI run. A lead can pass
               multiple providers, but counts once toward --limit.
        step: Run only a specific tier: 'prospeo', 'fullenrich', or 'lusha'.
        retry_no_phone: Reset LUSHA_DONE leads that still have no phone back
                        to PENDING so they re-enter the waterfall.
        retry_linkedin: Reset leads that have no phone but DO have
                        a linkedin_url (any status), then re-run only those
                        through the full waterfall.
    """
    session = SessionLocal()
    try:
        logger.info("Starting enrich-phones (limit=%s, step=%s, retry_no_phone=%s, retry_linkedin=%s)", limit, step, retry_no_phone, retry_linkedin)

        if step and step not in ("prospeo", "fullenrich", "lusha"):
            raise ValueError(f"Unknown step: {step}. Must be prospeo, fullenrich, or lusha.")

        # ── Retry: reset LUSHA_DONE leads without phone back to PENDING ──
        if retry_no_phone:
            reset_count = (
                session.query(Lead)
                .filter(
                    Lead.enrichment_status == "LUSHA_DONE",
                    Lead.phone.is_(None),
                )
                .update({Lead.enrichment_status: "PENDING", Lead.updated_at: datetime.utcnow()})
            )
            session.commit()
            logger.info("retry_no_phone: reset %d LUSHA_DONE leads (no phone) → PENDING", reset_count)

        # ── Retry-linkedin: reset phone-less leads that have a linkedin_url ──
        retry_linkedin_cohort: Optional[set[int]] = None
        if retry_linkedin:
            retry_leads = (
                session.query(Lead.id)
                .filter(
                    Lead.phone.is_(None),
                    Lead.linkedin_url.isnot(None),
                )
                .all()
            )
            retry_linkedin_cohort = {row[0] for row in retry_leads}
            if not retry_linkedin_cohort:
                logger.info("retry_linkedin: no leads with linkedin_url and no phone")
                return

            session.query(Lead).filter(Lead.id.in_(retry_linkedin_cohort)).update(
                {Lead.enrichment_status: "PENDING", Lead.updated_at: datetime.utcnow()},
            )
            session.commit()
            logger.info(
                "retry_linkedin: reset %d leads (has linkedin, no phone) → PENDING",
                len(retry_linkedin_cohort),
            )

        run_cohort = _build_run_cohort(session, limit=limit, step=step)
        # If retry_linkedin is active, constrain cohort to only those leads
        if retry_linkedin_cohort is not None:
            if run_cohort is not None:
                run_cohort = run_cohort & retry_linkedin_cohort
            else:
                run_cohort = retry_linkedin_cohort
        if run_cohort is not None:
            logger.info("Run cohort selected: %d unique leads", len(run_cohort))

            if not run_cohort:
                logger.info("Run cohort is empty for current step/scope; nothing to process")
                return

        # ── Resume mid-work leads first ──────────────────────────────

        # Track phones found per source during this run
        run_phones = {"PROSPEO": 0, "FULLENRICH": 0, "LUSHA": 0}
        run_processed = 0

        if not step or step == "fullenrich":
            # Advance PROSPEO_DONE leads that already have phone → LUSHA_DONE
            _advance_leads_with_phone(session, "PROSPEO_DONE", "LUSHA_DONE")
            fe_count, fe_phones = _run_fullenrich(session, allowed_lead_ids=run_cohort)
            run_processed += fe_count
            run_phones["FULLENRICH"] += fe_phones
            if fe_count:
                logger.info("FullEnrich (resume): processed %d leads", fe_count)

        if not step or step == "lusha":
            # Advance FE_DONE leads that already have phone → LUSHA_DONE
            _advance_leads_with_phone(session, "FE_DONE", "LUSHA_DONE")
            lusha_count, lusha_phones = _run_lusha(session, allowed_lead_ids=run_cohort)
            run_processed += lusha_count
            run_phones["LUSHA"] += lusha_phones
            if lusha_count:
                logger.info("Lusha (resume): processed %d leads", lusha_count)

        # ── Fresh intake via Prospeo ─────────────────────────────────

        if not step or step == "prospeo":
            prospeo_count, prospeo_phones = _run_prospeo(session, allowed_lead_ids=run_cohort)
            run_processed += prospeo_count
            run_phones["PROSPEO"] += prospeo_phones
            if prospeo_count:
                logger.info("Prospeo: processed %d fresh leads", prospeo_count)

        # ── Continue waterfall for newly PROSPEO_DONE leads ──────────

        if not step:
            _advance_leads_with_phone(session, "PROSPEO_DONE", "LUSHA_DONE")
            fe_count_2, fe_phones_2 = _run_fullenrich(session, allowed_lead_ids=run_cohort)
            run_processed += fe_count_2
            run_phones["FULLENRICH"] += fe_phones_2
            if fe_count_2:
                logger.info("FullEnrich (waterfall): processed %d leads", fe_count_2)

            _advance_leads_with_phone(session, "FE_DONE", "LUSHA_DONE")
            lusha_count_2, lusha_phones_2 = _run_lusha(session, allowed_lead_ids=run_cohort)
            run_processed += lusha_count_2
            run_phones["LUSHA"] += lusha_phones_2
            if lusha_count_2:
                logger.info("Lusha (waterfall): processed %d leads", lusha_count_2)

        # ── Summary ──────────────────────────────────────────────────

        total_run_phones = sum(run_phones.values())
        logger.info(
            "Enrichment complete. Processed %d leads, found %d phones this run:\n"
            "  %-14s %s\n"
            "  %-14s %s\n"
            "  %-14s %s\n"
            "  %-14s %s\n"
            "  %-14s %s",
            run_processed, total_run_phones,
            "Source", "Phones",
            "-" * 14, "-" * 6,
            "PROSPEO", run_phones["PROSPEO"],
            "FULLENRICH", run_phones["FULLENRICH"],
            "LUSHA", run_phones["LUSHA"],
        )

    except Exception:
        session.rollback()
        logger.exception("Error during enrich-phones")
        raise
    finally:
        session.close()
