"""Typer CLI entrypoint for the ZnanyLekarz scraping pipeline."""

import asyncio
import csv
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional

# Allow running directly: python src/zl_scraper/cli.py <command>
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import BoardMember, Clinic, ClinicLocation, Doctor, Lead, LinkedInCandidate, ScrapeProgress, SearchQuery, Specialization, clinic_doctors, lead_clinic_roles
from zl_scraper.utils.logging import setup_logging

app = typer.Typer(
    name="zl-scraper",
    help="ZnanyLekarz clinic scraping pipeline",
    rich_markup_mode="rich",
)
console = Console()


@app.callback()
def main_callback() -> None:
    """Initialize logging on every command invocation."""
    setup_logging()


# ── discover ─────────────────────────────────────────────────────────────


@app.command()
def discover(
    spec_name: Optional[str] = typer.Option(None, "--spec-name", help="Run for a single specialization name"),
    spec_id: Optional[int] = typer.Option(None, "--spec-id", help="Run for a single specialization ID"),
    max_pages: Optional[int] = typer.Option(None, "--max-pages", help="Cap pages per specialization"),
    offset: int = typer.Option(0, "--offset", help="Skip the first N specializations (0-based)"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap total specializations to process (applied after offset)"),
    proxy_level: str = typer.Option("datacenter", "--proxy-level", help="Starting proxy tier: datacenter, residential, unlocker, or none"),
) -> None:
    """Run search page discovery for all (or specific) specializations."""
    from zl_scraper.pipeline.discover import run_discovery

    asyncio.run(
        run_discovery(
            spec_name=spec_name,
            spec_id=spec_id,
            max_pages=max_pages,
            offset=offset,
            limit=limit,
            start_tier=proxy_level,
        )
    )
    console.print("[green]Discovery complete.[/green]")


# ── enrich ───────────────────────────────────────────────────────────────


@app.command()
def enrich(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to enrich"),
    proxy_level: str = typer.Option("datacenter", "--proxy-level", help="Starting proxy tier: datacenter, residential, unlocker, or none"),
) -> None:
    """Enrich all un-enriched clinics with profile + doctors data."""
    from zl_scraper.pipeline.enrich import run_enrichment

    asyncio.run(run_enrichment(limit=limit, start_tier=proxy_level))
    console.print("[green]Enrichment complete.[/green]")


# ── status ───────────────────────────────────────────────────────────────


@app.command()
def status(
    icp_only: bool = typer.Option(False, "--icp", help="Show stats for ICP-matched clinics only"),
) -> None:
    """Print progress: specializations scraped, clinics discovered/enriched."""
    session = SessionLocal()
    try:
        total_specs = session.query(Specialization).count()
        done_specs = session.query(ScrapeProgress).filter_by(status="done").count()
        in_progress_specs = session.query(ScrapeProgress).filter_by(status="in_progress").count()

        total_clinics = session.query(Clinic).count()
        enriched_clinics = session.query(Clinic).filter(Clinic.enriched_at.isnot(None)).count()
        unenriched_clinics = total_clinics - enriched_clinics

        # Base query for clinic-level stats
        if icp_only:
            base_filter = [Clinic.icp_match.is_(True)]
            scope_clinics = session.query(Clinic).filter(*base_filter).count()
            scope_label = "ICP-matched clinics"
        else:
            base_filter = [Clinic.enriched_at.isnot(None)]
            scope_clinics = enriched_clinics
            scope_label = "Enriched clinics"

        clinics_with_nip = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.nip.isnot(None))
            .count()
        )

        total_locations = session.query(ClinicLocation).count()
        if icp_only:
            loc_join_filter = [Clinic.icp_match.is_(True)]
        else:
            loc_join_filter = [Clinic.enriched_at.isnot(None)]
        clinics_with_linkedin = (
            session.query(ClinicLocation.clinic_id)
            .join(Clinic, Clinic.id == ClinicLocation.clinic_id)
            .filter(*loc_join_filter, ClinicLocation.linkedin_url.isnot(None))
            .distinct()
            .count()
        )
        clinics_with_website = (
            session.query(ClinicLocation.clinic_id)
            .join(Clinic, Clinic.id == ClinicLocation.clinic_id)
            .filter(*loc_join_filter, ClinicLocation.website_url.isnot(None))
            .distinct()
            .count()
        )

        total_doctors = session.query(Doctor).count()

        title = "ZL Scraper Status (ICP only)" if icp_only else "ZL Scraper Status"
        table = Table(title=title)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")

        if not icp_only:
            table.add_row("Total specializations", str(total_specs))
            table.add_row("Specializations done", str(done_specs))
            table.add_row("Specializations in progress", str(in_progress_specs))
            table.add_row("Specializations pending", str(total_specs - done_specs - in_progress_specs))
            table.add_row("───", "───")
            table.add_row("Total clinics discovered", str(total_clinics))
            table.add_row("Clinics enriched", str(enriched_clinics))
            table.add_row("Clinics awaiting enrichment", str(unenriched_clinics))
        else:
            table.add_row(scope_label, str(scope_clinics))

        table.add_row("───", "───")
        table.add_row("Clinics with NIP", f"{clinics_with_nip} / {scope_clinics}")
        table.add_row("Clinics with LinkedIn URL (location)", f"{clinics_with_linkedin} / {scope_clinics}")
        table.add_row("Clinics with website URL (location)", f"{clinics_with_website} / {scope_clinics}")
        table.add_row("───", "───")

        # Company enrichment metrics (clinic-level)
        clinics_with_domain = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.website_domain.isnot(None))
            .count()
        )
        domain_searched = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.domain_searched_at.isnot(None))
            .count()
        )
        clinics_with_li = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.linkedin_url.isnot(None))
            .count()
        )
        linkedin_searched = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.linkedin_searched_at.isnot(None))
            .count()
        )
        maybe_pending = (
            session.query(LinkedInCandidate)
            .filter(LinkedInCandidate.status == "maybe")
            .count()
        )
        nip_searched = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.nip_searched_at.isnot(None))
            .count()
        )
        nip_from_serp = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.nip.isnot(None), Clinic.nip_searched_at.isnot(None))
            .count()
        )

        table.add_row("[bold]Company enrichment[/]", "")
        table.add_row("Domain found", f"{clinics_with_domain} / {scope_clinics}")
        table.add_row("Domain SERP searched", f"{domain_searched} / {scope_clinics}")
        table.add_row("LinkedIn found", f"{clinics_with_li} / {scope_clinics}")
        table.add_row("LinkedIn SERP searched", f"{linkedin_searched} / {scope_clinics}")
        table.add_row("LinkedIn MAYBE pending", str(maybe_pending))
        table.add_row("NIP found (SERP)", f"{nip_from_serp} / {scope_clinics}")
        table.add_row("NIP SERP searched", f"{nip_searched} / {scope_clinics}")
        table.add_row("───", "───")

        # KRS / CEIDG enrichment metrics
        krs_searched = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.krs_searched_at.isnot(None))
            .count()
        )
        krs_found = (
            session.query(Clinic)
            .filter(*base_filter, Clinic.legal_type.isnot(None), Clinic.legal_type != "NOT_FOUND")
            .count()
        )
        total_board = (
            session.query(BoardMember)
            .join(Clinic, BoardMember.clinic_id == Clinic.id)
            .filter(*base_filter)
            .count()
        )

        table.add_row("[bold]KRS / CEIDG enrichment[/]", "")
        table.add_row("KRS/CEIDG searched", f"{krs_searched} / {scope_clinics}")
        table.add_row("KRS/CEIDG found", f"{krs_found} / {scope_clinics}")
        table.add_row("Board members total", str(total_board))
        table.add_row("───", "───")

        if not icp_only:
            table.add_row("Total doctors", str(total_doctors))
            table.add_row("───", "───")
            table.add_row("Total clinic locations", str(total_locations))

        console.print(table)
    finally:
        session.close()


@app.command(name="status-discover")
def status_discover() -> None:
    """Detailed discovery progress — per-specialization page counts and clinic totals."""
    from sqlalchemy import func

    session = SessionLocal()
    try:
        # Subquery: clinic count per specialization via search_queries
        clinic_counts = (
            session.query(
                SearchQuery.specialization_id,
                func.count(SearchQuery.clinic_id).label("clinics"),
            )
            .group_by(SearchQuery.specialization_id)
            .subquery()
        )

        rows = (
            session.query(
                Specialization.id,
                Specialization.name,
                ScrapeProgress.last_page_scraped,
                ScrapeProgress.total_pages,
                ScrapeProgress.status,
                clinic_counts.c.clinics,
            )
            .outerjoin(ScrapeProgress, Specialization.id == ScrapeProgress.specialization_id)
            .outerjoin(clinic_counts, Specialization.id == clinic_counts.c.specialization_id)
            .order_by(Specialization.id)
            .all()
        )

        table = Table(title="Discovery Progress by Specialization")
        table.add_column("#", style="dim", justify="right")
        table.add_column("Specialization", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Pages", justify="right")
        table.add_column("Clinics", style="green", justify="right")

        total_clinics = 0
        for idx, (spec_id, name, last_page, total_pages, prog_status, clinics) in enumerate(rows, 1):
            clinics = clinics or 0
            total_clinics += clinics

            if prog_status == "done":
                status_str = "[green]done[/]"
                pages_str = f"{last_page}/{total_pages}"
            elif prog_status == "in_progress":
                status_str = "[yellow]in progress[/]"
                pages_str = f"{last_page}/{total_pages}"
            else:
                status_str = "[dim]pending[/]"
                pages_str = "—"

            table.add_row(str(idx), name, status_str, pages_str, str(clinics))

        console.print(table)

        # Unique clinic count (clinics appear in multiple specializations)
        unique_clinics = session.query(Clinic).count()
        console.print(
            f"\n[bold]Total:[/] [green]{unique_clinics}[/] unique clinics across "
            f"[bold]{len(rows)}[/] specializations "
            f"[dim]({total_clinics} incl. overlaps)[/]"
        )
    finally:
        session.close()


# ── export ───────────────────────────────────────────────────────────────


@app.command()
def export(
    format: str = typer.Option("csv", "--format", help="Output format: csv or json"),
    output: str = typer.Option("export", "--output", help="Output file path (without extension)"),
) -> None:
    """Export enriched clinic data to CSV or JSON."""
    session = SessionLocal()
    try:
        clinics = (
            session.query(Clinic)
            .filter(Clinic.enriched_at.isnot(None))
            .all()
        )

        if not clinics:
            console.print("[yellow]No enriched clinics to export.[/yellow]")
            return

        rows = []
        for clinic in clinics:
            locations = session.query(ClinicLocation).filter_by(clinic_id=clinic.id).all()
            addresses = [loc.address for loc in locations if loc.address]
            coords = [
                f"{loc.latitude},{loc.longitude}"
                for loc in locations
                if loc.latitude is not None
            ]

            rows.append(
                {
                    "id": clinic.id,
                    "name": clinic.name,
                    "zl_url": clinic.zl_url,
                    "zl_profile_id": clinic.zl_profile_id,
                    "nip": clinic.nip,
                    "legal_name": clinic.legal_name,
                    "description": clinic.description,
                    "zl_reviews_cnt": clinic.zl_reviews_cnt,
                    "doctors_count": clinic.doctors_count,
                    "addresses": "; ".join(addresses),
                    "coordinates": "; ".join(coords),
                    "address_count": len(locations),
                    "discovered_at": str(clinic.discovered_at),
                    "enriched_at": str(clinic.enriched_at),
                }
            )

        if format == "csv":
            filepath = f"{output}.csv"
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
        elif format == "json":
            filepath = f"{output}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
        else:
            console.print(f"[red]Unknown format: {format}[/red]")
            raise typer.Exit(1)

        console.print(f"[green]Exported {len(rows)} clinics to {filepath}[/green]")
    finally:
        session.close()


def _parse_pesel_birth_date(pesel: Optional[str]) -> Optional[date]:
    """Return birth date parsed from PESEL, or None when invalid/unknown."""
    if not pesel:
        return None

    digits = "".join(ch for ch in pesel if ch.isdigit())
    if len(digits) != 11:
        return None

    yy = int(digits[0:2])
    mm_raw = int(digits[2:4])
    dd = int(digits[4:6])

    if 1 <= mm_raw <= 12:
        century = 1900
        mm = mm_raw
    elif 21 <= mm_raw <= 32:
        century = 2000
        mm = mm_raw - 20
    elif 41 <= mm_raw <= 52:
        century = 2100
        mm = mm_raw - 40
    elif 61 <= mm_raw <= 72:
        century = 2200
        mm = mm_raw - 60
    elif 81 <= mm_raw <= 92:
        century = 1800
        mm = mm_raw - 80
    else:
        return None

    year = century + yy
    try:
        return date(year, mm, dd)
    except ValueError:
        return None


def _age_from_pesel(pesel: Optional[str]) -> Optional[int]:
    """Return age in full years parsed from PESEL, or None when unavailable."""
    birth_date = _parse_pesel_birth_date(pesel)
    if not birth_date:
        return None

    today = date.today()
    age = today.year - birth_date.year
    if (today.month, today.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age


@app.command(name="export-leads")
def export_leads(
    output: str = typer.Option("leads_export.csv", "--output", help="Output CSV file path"),
    phone_only: bool = typer.Option(False, "--phone-only", help="Include only leads with a phone"),
    email_only: bool = typer.Option(False, "--email-only", help="Include only leads with an email"),
) -> None:
    """Export leads to CSV with associated companies/roles and age derived from PESEL.

    Filters are stackable. If no filters are provided, defaults to phone-only.
    """
    session = SessionLocal()
    try:
        query = session.query(Lead).order_by(Lead.id)

        # Default behavior: only leads with phone unless user provided explicit filters.
        effective_phone_only = phone_only or not (phone_only or email_only)

        if effective_phone_only:
            query = query.filter(Lead.phone.isnot(None), Lead.phone != "")
        if email_only:
            query = query.filter(Lead.email.isnot(None), Lead.email != "")

        leads = query.all()
        if not leads:
            console.print("[yellow]No leads matched export filters.[/yellow]")
            return

        role_rows = (
            session.query(
                lead_clinic_roles.c.lead_id,
                Clinic.name,
                Clinic.legal_name,
                Clinic.website_domain,
                lead_clinic_roles.c.role,
            )
            .join(Clinic, Clinic.id == lead_clinic_roles.c.clinic_id)
            .order_by(lead_clinic_roles.c.lead_id, Clinic.id, lead_clinic_roles.c.role)
            .all()
        )

        companies_by_lead: dict[int, list[str]] = {}
        companies_index: dict[int, dict[str, dict]] = {}

        for lead_id, clinic_name, legal_name, domain, role in role_rows:
            company_base = legal_name or clinic_name or "Unknown company"
            domain_text = (domain or "").strip()
            domain_key = domain_text.lower()

            # Dedup by domain when domain exists; otherwise keep rows separate.
            if domain_key:
                dedup_key = f"domain:{domain_key}"
                company_label = f"{company_base} ({domain_text})"
            else:
                dedup_key = f"nodomain:{company_base}:{role}"
                company_label = company_base

            lead_companies = companies_index.setdefault(lead_id, {})
            if dedup_key not in lead_companies:
                lead_companies[dedup_key] = {
                    "label": company_label,
                    "roles": [],
                }

            roles = lead_companies[dedup_key]["roles"]
            if role not in roles:
                roles.append(role)

        for lead_id, grouped in companies_index.items():
            bullets = []
            for entry in grouped.values():
                roles_text = ", ".join(entry["roles"])
                bullets.append(f"- {entry['label']} — {roles_text}")
            companies_by_lead[lead_id] = bullets

        rows = []
        for lead in leads:
            rows.append(
                {
                    "lead_id": lead.id,
                    "full_name": lead.full_name,
                    "pesel": lead.pesel,
                    "age": _age_from_pesel(lead.pesel),
                    "phone": lead.phone,
                    "email": lead.email,
                    "linkedin_url": lead.linkedin_url,
                    "lead_source": lead.lead_source,
                    "phone_source": lead.phone_source,
                    "enrichment_status": lead.enrichment_status,
                    "created_at": str(lead.created_at),
                    "updated_at": str(lead.updated_at) if lead.updated_at else None,
                    "companies": "\n".join(companies_by_lead.get(lead.id, [])),
                }
            )

        filepath = output if output.lower().endswith(".csv") else f"{output}.csv"
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        active_filters = []
        if effective_phone_only:
            active_filters.append("phone-only")
        if email_only:
            active_filters.append("email-only")
        filters_text = " + ".join(active_filters) if active_filters else "none"

        console.print(
            f"[green]Exported {len(rows)} leads to {filepath} (filters: {filters_text}).[/green]"
        )
    finally:
        session.close()


# ── filter ────────────────────────────────────────────────────────────────


@app.command(name="filter")
def filter_clinics(
    min_doctors: int = typer.Option(20, "--min-doctors", help="Minimum doctor count threshold"),
    show_excluded: bool = typer.Option(False, "--show-excluded", help="Print excluded specializations and exit"),
    show_allowed: bool = typer.Option(False, "--show-allowed", help="Print allowed (ICP) specializations and exit"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show filter summary without writing to DB"),
) -> None:
    """Mark enriched clinics as ICP-fit based on doctor count and specialization."""
    from zl_scraper.pipeline.filter import (
        build_allowed_specialization_names,
        build_excluded_specialization_names,
        query_filtered_clinics,
    )

    allowed_specs = build_allowed_specialization_names()
    excluded_specs = build_excluded_specialization_names()

    if show_excluded:
        console.print(f"[bold]Excluded specializations ({len(excluded_specs)}):[/bold]")
        for name in sorted(excluded_specs):
            console.print(f"  [red]✗[/red] {name}")
        raise typer.Exit()

    if show_allowed:
        console.print(f"[bold]Allowed (ICP) specializations ({len(allowed_specs)}):[/bold]")
        for name in sorted(allowed_specs):
            console.print(f"  [green]✓[/green] {name}")
        raise typer.Exit()

    session = SessionLocal()
    try:
        result = query_filtered_clinics(session, min_doctors=min_doctors, allowed_spec_names=allowed_specs)
        clinics = result.matched
        matched_ids = [c.id for c in clinics]

        with_nip = (
            session.query(Clinic)
            .filter(Clinic.id.in_(matched_ids), Clinic.nip.isnot(None), Clinic.nip != "")
            .count()
        ) if matched_ids else 0
        with_website = (
            session.query(ClinicLocation.clinic_id)
            .filter(ClinicLocation.clinic_id.in_(matched_ids), ClinicLocation.website_url.isnot(None))
            .distinct()
            .count()
        ) if matched_ids else 0
        with_linkedin = (
            session.query(ClinicLocation.clinic_id)
            .filter(ClinicLocation.clinic_id.in_(matched_ids), ClinicLocation.linkedin_url.isnot(None))
            .distinct()
            .count()
        ) if matched_ids else 0

        # Company enrichment metrics (clinic-level fields set by find-domains / find-linkedin)
        with_domain = (
            session.query(Clinic)
            .filter(Clinic.id.in_(matched_ids), Clinic.website_domain.isnot(None))
            .count()
        ) if matched_ids else 0
        domain_searched = (
            session.query(Clinic)
            .filter(Clinic.id.in_(matched_ids), Clinic.domain_searched_at.isnot(None))
            .count()
        ) if matched_ids else 0
        with_li_company = (
            session.query(Clinic)
            .filter(Clinic.id.in_(matched_ids), Clinic.linkedin_url.isnot(None))
            .count()
        ) if matched_ids else 0
        li_searched = (
            session.query(Clinic)
            .filter(Clinic.id.in_(matched_ids), Clinic.linkedin_searched_at.isnot(None))
            .count()
        ) if matched_ids else 0

        title = "Filter Dry Run" if dry_run else "Filter Results"
        table = Table(title=title)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("Min doctors threshold", str(min_doctors))
        table.add_row("Allowed specializations", str(len(allowed_specs)))
        table.add_row("Excluded specializations", str(len(excluded_specs)))
        table.add_row("───", "───")
        table.add_row("Total enriched clinics", str(result.total_enriched))
        table.add_row("[red]Rejected (too few doctors)[/]", str(result.rejected_doctors))
        table.add_row("[red]Rejected (wrong specialization)[/]", str(result.rejected_specialization))
        table.add_row("[red]Rejected (public / big chain)[/]", str(result.rejected_name))
        table.add_row("[bold]Clinics matched (ICP fit)[/]", f"[bold]{result.total_matched}[/]")
        table.add_row("───", "───")
        table.add_row("Total doctors in matched", str(result.total_doctors_in_matched))
        table.add_row("Avg doctors per clinic", f"{result.avg_doctors:.1f}")
        table.add_row("───", "───")
        table.add_row("With NIP", f"{with_nip} / {result.total_matched}")
        table.add_row("With website URL (location)", f"{with_website} / {result.total_matched}")
        table.add_row("With LinkedIn URL (location)", f"{with_linkedin} / {result.total_matched}")
        table.add_row("───", "───")
        table.add_row("[bold]Company enrichment[/]", "")
        table.add_row("Domain SERP searched", f"{domain_searched} / {result.total_matched}")
        table.add_row("Domain found", f"{with_domain} / {result.total_matched}")
        table.add_row("LinkedIn SERP searched", f"{li_searched} / {result.total_matched}")
        table.add_row("LinkedIn found", f"{with_li_company} / {result.total_matched}")
        console.print(table)

        if dry_run:
            return

        # Reset all enriched clinics to icp_match=False, then stamp matched ones as True
        session.query(Clinic).filter(Clinic.enriched_at.isnot(None)).update({Clinic.icp_match: False})
        if matched_ids:
            session.query(Clinic).filter(Clinic.id.in_(matched_ids)).update({Clinic.icp_match: True})
        session.commit()
        console.print(f"[green]Marked {result.total_matched} clinics as ICP fit (reset {result.total_filtered_out} to not-fit).[/green]")
    finally:
        session.close()


# ── company enrichment ────────────────────────────────────────────────────


@app.command(name="backfill-domains")
def backfill_domains() -> None:
    """Extract website_domain from existing location website_url data."""
    from zl_scraper.pipeline.company_enrich.backfill_domains import run_backfill_domains

    run_backfill_domains()
    console.print("[green]Domain backfill complete.[/green]")


@app.command(name="find-domains")
def find_domains(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    retry_not_found: bool = typer.Option(False, "--retry-not-found", help="Re-process clinics where SERP ran but no domain was found"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
) -> None:
    """Discover website domains for clinics via SERP search + LLM validation."""
    from zl_scraper.pipeline.company_enrich.find_domains import run_find_domains

    asyncio.run(run_find_domains(limit=limit, retry_not_found=retry_not_found, icp_only=not all_clinics))
    console.print("[green]Domain discovery complete.[/green]")


@app.command(name="manual-domains")
def manual_domains(
    all_clinics: bool = typer.Option(False, "--all", help="Include clinics not yet SERP-searched (default: only SERP-searched with no result)"),
) -> None:
    """Interactively assign website domains to clinics that SERP couldn't resolve."""
    from zl_scraper.pipeline.company_enrich.manual_domains import run_manual_domains

    run_manual_domains(only_searched=not all_clinics, icp_only=True)


@app.command(name="find-linkedin")
def find_linkedin(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    skip_maybe: bool = typer.Option(False, "--skip-maybe", help="Skip second-pass MAYBE validation"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
) -> None:
    """Discover LinkedIn company pages for clinics via SERP + LLM categorisation."""
    from zl_scraper.pipeline.company_enrich.find_linkedin import run_find_linkedin

    asyncio.run(run_find_linkedin(limit=limit, skip_maybe=skip_maybe, icp_only=not all_clinics))
    console.print("[green]LinkedIn discovery complete.[/green]")


@app.command(name="krs-enrich")
def krs_enrich(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
    headless: bool = typer.Option(False, "--headless", help="Run browser in headless mode (default: visible)"),
    retry_404: bool = typer.Option(False, "--retry-404", help="Re-scrape clinics with NOT_FOUND/ERROR or 0 board members"),
) -> None:
    """Look up clinics by NIP in KRS/CEIDG, extract board members, save to DB."""
    from zl_scraper.pipeline.krs_scraper.pipeline import run_krs_enrich

    run_krs_enrich(
        limit=limit,
        icp_only=not all_clinics,
        headless=headless,
        retry_404=retry_404,
    )
    console.print("[green]KRS enrichment complete.[/green]")


@app.command(name="sync-leads")
def sync_leads(
    all_clinics: bool = typer.Option(False, "--all", help="Process all clinics, not just ICP-fit"),
) -> None:
    """Sync board_members into leads table — dedup KRS by PESEL, CEIDG by name+clinic."""
    from zl_scraper.pipeline.phone_enrich.sync_leads import run_sync_leads

    run_sync_leads(icp_only=not all_clinics)
    console.print("[green]Sync-leads complete.[/green]")


@app.command(name="enrich-phones")
def enrich_phones(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many fresh PENDING leads enter Prospeo"),
    step: Optional[str] = typer.Option(None, "--step", help="Run only one tier: prospeo, fullenrich, or lusha"),
) -> None:
    """Run phone enrichment waterfall: Prospeo → FullEnrich → Lusha."""
    from zl_scraper.pipeline.phone_enrich.enrich_phones import run_enrich_phones

    run_enrich_phones(limit=limit, step=step)
    console.print("[green]Phone enrichment complete.[/green]")


@app.command(name="status-leads")
def status_leads() -> None:
    """Show lead counts and phone enrichment progress."""
    from sqlalchemy import func

    session = SessionLocal()
    try:
        total = session.query(Lead).count()
        if total == 0:
            console.print("[yellow]No leads yet. Run sync-leads first.[/yellow]")
            return

        # By enrichment status
        status_counts = (
            session.query(Lead.enrichment_status, func.count(Lead.id))
            .group_by(Lead.enrichment_status)
            .all()
        )
        status_map = dict(status_counts)

        # By lead source
        source_counts = (
            session.query(Lead.lead_source, func.count(Lead.id))
            .group_by(Lead.lead_source)
            .all()
        )

        # Contact stats
        with_phone = session.query(Lead).filter(Lead.phone.isnot(None)).count()
        with_email = session.query(Lead).filter(Lead.email.isnot(None)).count()
        with_linkedin = session.query(Lead).filter(Lead.linkedin_url.isnot(None)).count()

        # By phone source
        phone_source_counts = (
            session.query(Lead.phone_source, func.count(Lead.id))
            .filter(Lead.phone_source.isnot(None))
            .group_by(Lead.phone_source)
            .all()
        )

        # Roles
        total_roles = session.query(func.count()).select_from(lead_clinic_roles).scalar()
        unique_clinics = (
            session.query(func.count(func.distinct(lead_clinic_roles.c.clinic_id)))
            .scalar()
        )

        table = Table(title="Leads Status")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")

        table.add_row("[bold]Overview[/]", "")
        table.add_row("Total leads", str(total))
        table.add_row("Linked to clinics", f"{unique_clinics} clinics via {total_roles} roles")
        table.add_row("───", "───")

        table.add_row("[bold]By source[/]", "")
        for source, cnt in sorted(source_counts, key=lambda x: -x[1]):
            table.add_row(f"  {source}", str(cnt))
        table.add_row("───", "───")

        table.add_row("[bold]Enrichment status[/]", "")
        status_order = ["PENDING", "PROSPEO_DONE", "FE_DONE", "LUSHA_DONE"]
        for s in status_order:
            cnt = status_map.get(s, 0)
            if cnt > 0:
                table.add_row(f"  {s}", str(cnt))
        # Any other statuses not in the expected list
        for s, cnt in sorted(status_map.items()):
            if s not in status_order and cnt > 0:
                table.add_row(f"  {s}", str(cnt))
        table.add_row("───", "───")

        table.add_row("[bold]Contact data[/]", "")
        table.add_row("With phone", f"{with_phone} / {total}")
        table.add_row("With email", f"{with_email} / {total}")
        table.add_row("With LinkedIn URL", f"{with_linkedin} / {total}")
        table.add_row("───", "───")

        if phone_source_counts:
            table.add_row("[bold]Phones by source[/]", "")
            for source, cnt in sorted(phone_source_counts, key=lambda x: -x[1]):
                table.add_row(f"  {source}", str(cnt))

        console.print(table)
    finally:
        session.close()


@app.command(name="find-nip")
def find_nip(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
) -> None:
    """Find Polish NIP (tax ID) for clinics via SERP search on their domain + LLM extraction."""
    from zl_scraper.pipeline.company_enrich.find_nip import run_find_nip

    asyncio.run(run_find_nip(limit=limit, icp_only=not all_clinics))
    console.print("[green]NIP discovery complete.[/green]")


@app.command(name="review-linkedin")
def review_linkedin(
    list_all: bool = typer.Option(False, "--list", help="List all MAYBE candidates"),
    approve: Optional[int] = typer.Option(None, "--approve", help="Approve candidate by ID"),
    reject: Optional[int] = typer.Option(None, "--reject", help="Reject candidate by ID"),
) -> None:
    """Review LinkedIn MAYBE candidates — list, approve, or reject."""
    session = SessionLocal()
    try:
        if approve is not None:
            candidate = session.get(LinkedInCandidate, approve)
            if not candidate:
                console.print(f"[red]Candidate ID {approve} not found.[/red]")
                raise typer.Exit(1)
            candidate.status = "yes"
            clinic = session.get(Clinic, candidate.clinic_id)
            if clinic:
                clinic.linkedin_url = candidate.url
            session.commit()
            console.print(f"[green]Approved candidate {approve} — {candidate.url}[/green]")
            return

        if reject is not None:
            candidate = session.get(LinkedInCandidate, reject)
            if not candidate:
                console.print(f"[red]Candidate ID {reject} not found.[/red]")
                raise typer.Exit(1)
            candidate.status = "no"
            session.commit()
            console.print(f"[yellow]Rejected candidate {reject} — {candidate.url}[/yellow]")
            return

        # Default: list MAYBE candidates
        candidates = (
            session.query(LinkedInCandidate)
            .join(Clinic, Clinic.id == LinkedInCandidate.clinic_id)
            .filter(LinkedInCandidate.status == "maybe")
            .order_by(LinkedInCandidate.id)
            .all()
        )

        if not candidates:
            console.print("[green]No MAYBE candidates pending review.[/green]")
            return

        table = Table(title=f"LinkedIn MAYBE Candidates ({len(candidates)})")
        table.add_column("ID", style="dim", justify="right")
        table.add_column("Clinic", style="cyan")
        table.add_column("Domain", style="blue")
        table.add_column("LinkedIn URL", style="yellow")

        for cand in candidates:
            clinic = session.get(Clinic, cand.clinic_id)
            table.add_row(
                str(cand.id),
                clinic.name if clinic else "?",
                clinic.website_domain if clinic else "?",
                cand.url,
            )

        console.print(table)
        console.print(
            "\nUse [bold]--approve <ID>[/bold] or [bold]--reject <ID>[/bold] to resolve."
        )
    finally:
        session.close()


# ── filter-worked ─────────────────────────────────────────────────────────


@app.command(name="filter-worked")
def filter_worked(
    list_domains: bool = typer.Option(False, "--list", help="Print the worked domains list and exit"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be excluded without writing to DB"),
) -> None:
    """Exclude worked domains (demos, lost deals, big chains, pipeline) from ICP."""
    from zl_scraper.pipeline.worked_domains import WORKED_DOMAINS

    if list_domains:
        table = Table(title=f"Worked Domains ({len(WORKED_DOMAINS)})")
        table.add_column("Domain", style="cyan")
        table.add_column("Company", style="green")
        table.add_column("Reason", style="yellow")
        for entry in WORKED_DOMAINS:
            table.add_row(entry["domain"], entry["name"], entry["reason"])
        console.print(table)
        raise typer.Exit()

    from zl_scraper.pipeline.filter_worked import exclude_worked_clinics

    session = SessionLocal()
    try:
        result = exclude_worked_clinics(session, dry_run=dry_run)

        title = "Filter-Worked Dry Run" if dry_run else "Filter-Worked Results"
        table = Table(title=title)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("ICP clinics before", str(result.total_icp))
        table.add_row("[red]Excluded (worked domain)[/]", str(result.excluded_count))
        table.add_row("[bold]ICP clinics after[/]", f"[bold]{result.total_icp - result.excluded_count}[/]")
        console.print(table)

        if result.excluded_clinics:
            detail = Table(title="Excluded Clinics")
            detail.add_column("ID", style="dim", justify="right")
            detail.add_column("Clinic", style="cyan")
            detail.add_column("Domain", style="blue")
            detail.add_column("Reason", style="yellow")
            for clinic_id, name, domain, reason in result.excluded_clinics:
                detail.add_row(str(clinic_id), name, domain, reason)
            console.print(detail)

        if not dry_run:
            console.print(f"[green]Excluded {result.excluded_count} clinics from ICP.[/green]")
    finally:
        session.close()


# ── reset ────────────────────────────────────────────────────────────────


@app.command()
def reset(
    step: str = typer.Option(..., "--step", help="Which step to reset: discover, enrich, or domains"),
) -> None:
    """Reset progress for re-runs (discover, enrich, or domains)."""
    session = SessionLocal()
    try:
        if step == "discover":
            deleted = session.query(ScrapeProgress).delete()
            session.commit()
            console.print(f"[green]Reset discovery progress ({deleted} records cleared).[/green]")

        elif step == "enrich":
            updated = (
                session.query(Clinic)
                .filter(Clinic.enriched_at.isnot(None))
                .update({
                    Clinic.enriched_at: None,
                    Clinic.zl_profile_id: None,
                    Clinic.nip: None,
                    Clinic.legal_name: None,
                    Clinic.description: None,
                    Clinic.zl_reviews_cnt: None,
                    Clinic.doctors_count: None,
                })
            )
            # Remove clinic locations, doctor associations, and orphan doctors
            session.query(ClinicLocation).delete()
            session.execute(clinic_doctors.delete())
            session.query(Doctor).delete()
            session.commit()
            console.print(f"[green]Reset enrichment for {updated} clinics (locations, doctors cleared).[/green]")

        elif step == "domains":
            updated = (
                session.query(Clinic)
                .filter(
                    Clinic.enriched_at.isnot(None),
                    (Clinic.website_domain.isnot(None)) | (Clinic.domain_searched_at.isnot(None)),
                )
                .update({
                    Clinic.website_domain: None,
                    Clinic.domain_searched_at: None,
                })
            )
            session.commit()
            console.print(f"[green]Reset domain search for {updated} clinics (website_domain + domain_searched_at cleared).[/green]")

        elif step == "icp":
            updated = (
                session.query(Clinic)
                .filter(Clinic.icp_match.is_(True))
                .update({Clinic.icp_match: False})
            )
            session.commit()
            console.print(f"[green]Reset ICP filter for {updated} clinics (icp_match set to False).[/green]")

        elif step == "krs":
            deleted_members = session.query(BoardMember).delete()
            updated = (
                session.query(Clinic)
                .filter(Clinic.krs_searched_at.isnot(None))
                .update({
                    Clinic.krs_searched_at: None,
                    Clinic.legal_type: None,
                    Clinic.krs_number: None,
                    Clinic.regon: None,
                    Clinic.registration_date: None,
                })
            )
            session.commit()
            console.print(
                f"[green]Reset KRS enrichment for {updated} clinics "
                f"({deleted_members} board members removed).[/green]"
            )

        else:
            console.print(f"[red]Unknown step: {step}. Use 'discover', 'enrich', 'domains', 'icp', or 'krs'.[/red]")
            raise typer.Exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    app()
