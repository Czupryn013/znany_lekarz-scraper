"""Typer CLI entrypoint for the ZnanyLekarz scraping pipeline."""

import asyncio
import csv
import json
import sys
from datetime import date, datetime
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


@app.command(rich_help_panel="Scraping")
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


@app.command(rich_help_panel="Scraping")
def enrich(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to enrich"),
    proxy_level: str = typer.Option("datacenter", "--proxy-level", help="Starting proxy tier: datacenter, residential, unlocker, or none"),
) -> None:
    """Enrich all un-enriched clinics with profile + doctors data."""
    from zl_scraper.pipeline.enrich import run_enrichment

    asyncio.run(run_enrichment(limit=limit, start_tier=proxy_level))
    console.print("[green]Enrichment complete.[/green]")


# ── status ───────────────────────────────────────────────────────────────


def _status_diff(session) -> None:
    """Show stats for clinics that are NOT in ICP but have linked leads."""
    from sqlalchemy import func

    # Clinics not in ICP that have at least one lead
    non_icp_with_leads = (
        session.query(Clinic)
        .join(lead_clinic_roles, lead_clinic_roles.c.clinic_id == Clinic.id)
        .filter(Clinic.icp_match.is_(False))
        .distinct()
        .all()
    )

    total_non_icp_leads = (
        session.query(func.count(func.distinct(Lead.id)))
        .join(lead_clinic_roles, lead_clinic_roles.c.lead_id == Lead.id)
        .join(Clinic, Clinic.id == lead_clinic_roles.c.clinic_id)
        .filter(Clinic.icp_match.is_(False))
        .scalar()
    )

    table = Table(title="Non-ICP Clinics with Leads (excluded from ICP)")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")

    table.add_row("Clinics excluded from ICP but with leads", str(len(non_icp_with_leads)))
    table.add_row("Total leads in those clinics", str(total_non_icp_leads))
    table.add_row("───", "───")

    # Breakdown by doctors count
    ranges = [(20, 49), (50, 99), (100, 199), (200, None)]
    for lo, hi in ranges:
        if hi:
            count = sum(1 for c in non_icp_with_leads if lo <= (c.doctors_count or 0) <= hi)
            label = f"  {lo}–{hi} doctors"
        else:
            count = sum(1 for c in non_icp_with_leads if (c.doctors_count or 0) >= lo)
            label = f"  {lo}+ doctors"
        if count > 0:
            table.add_row(label, str(count))

    table.add_row("───", "───")

    # Show the clinics sorted by doctor count
    table.add_row("[bold]Top excluded clinics[/]", "")
    for c in sorted(non_icp_with_leads, key=lambda x: x.doctors_count or 0, reverse=True)[:20]:
        lead_count = (
            session.query(func.count(func.distinct(Lead.id)))
            .join(lead_clinic_roles, lead_clinic_roles.c.lead_id == Lead.id)
            .filter(lead_clinic_roles.c.clinic_id == c.id)
            .scalar()
        )
        table.add_row(
            f"  {c.name[:50]}",
            f"{c.doctors_count or 0} docs, {lead_count} leads",
        )

    console.print(table)


@app.command(rich_help_panel="Status")
def status(
    show_all: bool = typer.Option(False, "--all", help="Show stats for all enriched clinics (default: ICP only)"),
    show_diff: bool = typer.Option(False, "--show-diff", help="Show stats for non-ICP clinics that have leads"),
) -> None:
    """Print progress: specializations scraped, clinics discovered/enriched."""
    session = SessionLocal()
    try:
        if show_diff:
            _status_diff(session)
            return

        icp_only = not show_all

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


@app.command(name="status-discover", rich_help_panel="Status")
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


# ── chains ───────────────────────────────────────────────────────────────


@app.command(rich_help_panel="Export")
def chains(
    min_locations: int = typer.Option(2, "--min-locations", help="Minimum total locations to count as a chain"),
    icp_only: bool = typer.Option(False, "--icp", help="Restrict to ICP-matched clinics only"),
    top: int = typer.Option(40, "--top", help="Number of chains to show in the table"),
    export_path: Optional[str] = typer.Option(None, "--export", help="Export results to a CSV file at this path"),
) -> None:
    """Show clinic chains (companies with multiple locations) for account targeting."""
    import csv as csv_mod

    from sqlalchemy import func, literal_column

    session = SessionLocal()
    try:
        # ── subquery: location count per clinic ──────────────────────────
        loc_sub = (
            session.query(
                ClinicLocation.clinic_id,
                func.count(ClinicLocation.id).label("loc_count"),
            )
            .group_by(ClinicLocation.clinic_id)
            .subquery()
        )

        # ── subquery: lead count and phone count per clinic ──────────────
        lead_sub = (
            session.query(
                lead_clinic_roles.c.clinic_id,
                func.count(lead_clinic_roles.c.lead_id).label("lead_count"),
            )
            .group_by(lead_clinic_roles.c.clinic_id)
            .subquery()
        )
        phone_sub = (
            session.query(
                lead_clinic_roles.c.clinic_id,
                func.count(lead_clinic_roles.c.lead_id).label("phone_count"),
            )
            .join(Lead, Lead.id == lead_clinic_roles.c.lead_id)
            .filter(Lead.phone.isnot(None))
            .group_by(lead_clinic_roles.c.clinic_id)
            .subquery()
        )

        base_filter = [Clinic.enriched_at.isnot(None)]
        if icp_only:
            base_filter.append(Clinic.icp_match.is_(True))

        # ── NIP-grouped chains ───────────────────────────────────────────
        # Same NIP appearing on multiple clinic rows = same legal entity
        nip_rows = (
            session.query(
                Clinic.nip.label("nip"),
                func.max(Clinic.legal_name).label("legal_name"),
                func.string_agg(func.distinct(Clinic.name), " / ").label("names"),
                func.count(Clinic.id).label("clinic_count"),
                func.sum(func.coalesce(loc_sub.c.loc_count, 0)).label("total_locs"),
                func.coalesce(func.sum(Clinic.doctors_count), 0).label("total_docs"),
                func.bool_or(Clinic.icp_match).label("any_icp"),
                func.bool_or(Clinic.linkedin_url.isnot(None)).label("has_linkedin"),
                func.max(Clinic.legal_type).label("legal_type"),
                func.bool_or(Clinic.krs_searched_at.isnot(None)).label("krs_searched"),
                func.coalesce(func.sum(func.coalesce(lead_sub.c.lead_count, 0)), 0).label("leads"),
                func.coalesce(func.sum(func.coalesce(phone_sub.c.phone_count, 0)), 0).label("phones"),
            )
            .outerjoin(loc_sub, Clinic.id == loc_sub.c.clinic_id)
            .outerjoin(lead_sub, Clinic.id == lead_sub.c.clinic_id)
            .outerjoin(phone_sub, Clinic.id == phone_sub.c.clinic_id)
            .filter(*base_filter, Clinic.nip.isnot(None))
            .group_by(Clinic.nip)
            .having(func.sum(func.coalesce(loc_sub.c.loc_count, 0)) >= min_locations)
            .all()
        )

        # ── no-NIP multi-location clinics ────────────────────────────────
        # Single clinic entry with 2+ physical locations, no NIP to group by
        nip_grouped_clinic_ids_sub = (
            session.query(Clinic.id)
            .filter(*base_filter, Clinic.nip.isnot(None))
            .subquery()
        )
        no_nip_rows = (
            session.query(
                literal_column("NULL").label("nip"),
                Clinic.legal_name.label("legal_name"),
                Clinic.name.label("names"),
                literal_column("1").label("clinic_count"),
                loc_sub.c.loc_count.label("total_locs"),
                func.coalesce(Clinic.doctors_count, 0).label("total_docs"),
                Clinic.icp_match.label("any_icp"),
                (Clinic.linkedin_url.isnot(None)).label("has_linkedin"),
                Clinic.legal_type.label("legal_type"),
                (Clinic.krs_searched_at.isnot(None)).label("krs_searched"),
                func.coalesce(lead_sub.c.lead_count, 0).label("leads"),
                func.coalesce(phone_sub.c.phone_count, 0).label("phones"),
            )
            .join(loc_sub, Clinic.id == loc_sub.c.clinic_id)
            .outerjoin(lead_sub, Clinic.id == lead_sub.c.clinic_id)
            .outerjoin(phone_sub, Clinic.id == phone_sub.c.clinic_id)
            .filter(
                *base_filter,
                Clinic.nip.is_(None),
                loc_sub.c.loc_count >= min_locations,
            )
            .all()
        )

        # ── merge & sort ─────────────────────────────────────────────────
        all_chains = []
        for r in nip_rows:
            all_chains.append({
                "nip": r.nip or "—",
                "name": (r.names or "")[:50],
                "legal_name": (r.legal_name or "")[:40],
                "clinic_count": r.clinic_count,
                "total_locs": int(r.total_locs or 0),
                "total_docs": int(r.total_docs or 0),
                "any_icp": bool(r.any_icp),
                "has_linkedin": bool(r.has_linkedin),
                "legal_type": r.legal_type or "—",
                "krs_searched": bool(r.krs_searched),
                "leads": int(r.leads or 0),
                "phones": int(r.phones or 0),
            })
        for r in no_nip_rows:
            all_chains.append({
                "nip": "—",
                "name": (r.names or "")[:50],
                "legal_name": (r.legal_name or "")[:40],
                "clinic_count": int(r.clinic_count),
                "total_locs": int(r.total_locs or 0),
                "total_docs": int(r.total_docs or 0),
                "any_icp": bool(r.any_icp),
                "has_linkedin": bool(r.has_linkedin),
                "legal_type": r.legal_type or "—",
                "krs_searched": bool(r.krs_searched),
                "leads": int(r.leads or 0),
                "phones": int(r.phones or 0),
            })

        all_chains.sort(key=lambda x: x["total_locs"], reverse=True)
        total_chains = len(all_chains)
        total_chain_locs = sum(c["total_locs"] for c in all_chains)
        avg_locs = total_chain_locs / total_chains if total_chains else 0

        # Single-location clinic count (for context)
        single_loc_count = (
            session.query(Clinic)
            .outerjoin(loc_sub, Clinic.id == loc_sub.c.clinic_id)
            .filter(*base_filter, func.coalesce(loc_sub.c.loc_count, 0) < min_locations)
            .count()
        )

        # ── summary ──────────────────────────────────────────────────────
        scope_label = "ICP-matched" if icp_only else "enriched"
        console.print(f"\n[bold cyan]Chains Dashboard[/] [dim](min {min_locations} locations, {scope_label} clinics)[/]\n")

        summary = Table(show_header=False, box=None, padding=(0, 2))
        summary.add_column("Metric", style="cyan")
        summary.add_column("Value", style="bold white", justify="right")
        summary.add_row("Total chains found", str(total_chains))
        summary.add_row("Total chain locations", str(total_chain_locs))
        summary.add_row(f"Single-location clinics (< {min_locations} locs)", str(single_loc_count))
        summary.add_row("Avg locations per chain", f"{avg_locs:.1f}")
        console.print(summary)
        console.print()

        # ── chains table ─────────────────────────────────────────────────
        display = all_chains[:top]
        t = Table(
            title=f"Top {len(display)} chains by location count",
            show_lines=False,
        )
        t.add_column("#", style="dim", justify="right", width=3)
        t.add_column("Name", style="cyan", max_width=36)
        t.add_column("NIP", style="dim", width=12)
        t.add_column("Legal name", style="white", max_width=30)
        t.add_column("Locs", style="bold green", justify="right", width=5)
        t.add_column("Clncs", justify="right", width=5)
        t.add_column("Docs", justify="right", width=5)
        t.add_column("ICP", justify="center", width=4)
        t.add_column("LI", justify="center", width=3)
        t.add_column("KRS", justify="center", width=5)
        t.add_column("Leads", justify="right", width=5)
        t.add_column("Phones", style="bold yellow", justify="right", width=6)

        for idx, c in enumerate(display, 1):
            icp_str = "[green]✓[/]" if c["any_icp"] else "[dim]—[/]"
            li_str = "[blue]✓[/]" if c["has_linkedin"] else "[dim]—[/]"
            krs_str = c["legal_type"] if c["legal_type"] != "—" else ("[dim]?[/]" if not c["krs_searched"] else "[dim]✗[/]")
            leads_str = str(c["leads"]) if c["leads"] else "[dim]—[/]"
            phones_str = f"[bold yellow]{c['phones']}[/]" if c["phones"] else "[dim]—[/]"
            t.add_row(
                str(idx),
                c["name"],
                c["nip"],
                c["legal_name"],
                str(c["total_locs"]),
                str(c["clinic_count"]),
                str(c["total_docs"]) if c["total_docs"] else "—",
                icp_str,
                li_str,
                krs_str,
                leads_str,
                phones_str,
            )

        console.print(t)

        # ── enrichment gaps ───────────────────────────────────────────────
        no_nip = sum(1 for c in all_chains if c["nip"] == "—")
        no_krs = sum(1 for c in all_chains if not c["krs_searched"])
        no_li = sum(1 for c in all_chains if not c["has_linkedin"])
        no_leads = sum(1 for c in all_chains if c["leads"] == 0)
        no_phones = sum(1 for c in all_chains if c["phones"] == 0)

        console.print()
        gap = Table(title="Enrichment gaps (chains)", show_header=False, box=None, padding=(0, 2))
        gap.add_column("Gap", style="cyan")
        gap.add_column("Count", style="bold red", justify="right")
        gap.add_row("Chains with no NIP", str(no_nip))
        gap.add_row("Chains with no KRS searched", str(no_krs))
        gap.add_row("Chains with no LinkedIn", str(no_li))
        gap.add_row("Chains with no leads", str(no_leads))
        gap.add_row("Chains with no phones", str(no_phones))
        console.print(gap)
        console.print()

        # ── CSV export ────────────────────────────────────────────────────
        if export_path:
            out = Path(export_path)
            with out.open("w", newline="", encoding="utf-8") as f:
                writer = csv_mod.DictWriter(f, fieldnames=list(all_chains[0].keys()) if all_chains else [])
                writer.writeheader()
                writer.writerows(all_chains)
            console.print(f"[green]Exported {len(all_chains)} chains to {out}[/green]")

    finally:
        session.close()


# ── export ───────────────────────────────────────────────────────────────


@app.command(rich_help_panel="Export")
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


@app.command(name="export-leads", rich_help_panel="Export")
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


@app.command(name="filter", rich_help_panel="Filtering")
def filter_clinics(
    min_doctors: int = typer.Option(20, "--min-doctors", help="Minimum doctor count threshold"),
    strict: bool = typer.Option(False, "--strict", help="Require >=50% of specializations to be allowed (catches psych clinics with incidental allowed specs)"),
    reset: bool = typer.Option(False, "--reset", help="Reset all icp_match to False before applying (default: only set non-matching to False)"),
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
        result = query_filtered_clinics(
            session, min_doctors=min_doctors, allowed_spec_names=allowed_specs, strict=strict,
        )
        clinics = result.matched
        matched_ids = [c.id for c in clinics]

        with_nip = (
            session.query(Clinic)
            .filter(Clinic.id.in_(matched_ids), Clinic.nip.isnot(None), Clinic.nip != "")
            .count()
        ) if matched_ids else 0
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

        mode_label = "strict" if strict else "normal"
        title = f"Filter Dry Run ({mode_label})" if dry_run else f"Filter Results ({mode_label})"
        table = Table(title=title)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("Min doctors threshold", str(min_doctors))
        table.add_row("Strict mode (>=50% allowed)", "[green]ON[/]" if strict else "[dim]OFF[/]")
        table.add_row("Reset mode", "[yellow]FULL RESET[/]" if reset else "[dim]exclude-only[/]")
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
        table.add_row("With domain", f"{with_domain} / {result.total_matched}")
        table.add_row("Domain SERP searched", f"{domain_searched} / {result.total_matched}")
        table.add_row("With LinkedIn", f"{with_li_company} / {result.total_matched}")
        table.add_row("LinkedIn SERP searched", f"{li_searched} / {result.total_matched}")
        console.print(table)

        if dry_run:
            return

        if reset:
            # Full reset: clear all icp_match, then set matched ones
            session.query(Clinic).filter(Clinic.enriched_at.isnot(None)).update({Clinic.icp_match: False})
        else:
            # Exclude-only: only turn off clinics that didn't match (preserve existing icp_match=True for matched)
            not_matched_q = session.query(Clinic).filter(
                Clinic.enriched_at.isnot(None),
                Clinic.icp_match.is_(True),
            )
            if matched_ids:
                not_matched_q = not_matched_q.filter(~Clinic.id.in_(matched_ids))
            not_matched_q.update({Clinic.icp_match: False})

        if matched_ids:
            session.query(Clinic).filter(Clinic.id.in_(matched_ids)).update({Clinic.icp_match: True})
        session.commit()

        if reset:
            console.print(f"[green]Full reset: marked {result.total_matched} clinics as ICP fit (reset all others to not-fit).[/green]")
        else:
            console.print(f"[green]Marked {result.total_matched} clinics as ICP fit (excluded non-matching from existing ICP set).[/green]")
    finally:
        session.close()


# ── company enrichment ────────────────────────────────────────────────────


@app.command(name="backfill-domains", rich_help_panel="Company Enrichment")
def backfill_domains() -> None:
    """Extract website_domain from existing location website_url data."""
    from zl_scraper.pipeline.company_enrich.backfill_domains import run_backfill_domains

    run_backfill_domains()
    console.print("[green]Domain backfill complete.[/green]")


@app.command(name="backfill-linkedin", rich_help_panel="Company Enrichment")
def backfill_linkedin() -> None:
    """Extract linkedin_url from existing location linkedin_url data."""
    from zl_scraper.pipeline.company_enrich.backfill_linkedin import run_backfill_linkedin

    run_backfill_linkedin()
    console.print("[green]LinkedIn backfill complete.[/green]")


@app.command(name="find-domains", rich_help_panel="Company Enrichment")
def find_domains(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    retry_not_found: bool = typer.Option(False, "--retry-not-found", help="Re-process clinics where SERP ran but no domain was found"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
) -> None:
    """Discover website domains for clinics via SERP search + LLM validation."""
    from zl_scraper.pipeline.company_enrich.find_domains import run_find_domains

    asyncio.run(run_find_domains(limit=limit, retry_not_found=retry_not_found, icp_only=not all_clinics))
    console.print("[green]Domain discovery complete.[/green]")


@app.command(name="manual-domains", rich_help_panel="Company Enrichment")
def manual_domains(
    all_clinics: bool = typer.Option(False, "--all", help="Include clinics not yet SERP-searched (default: only SERP-searched with no result)"),
) -> None:
    """Interactively assign website domains to clinics that SERP couldn't resolve."""
    from zl_scraper.pipeline.company_enrich.manual_domains import run_manual_domains

    run_manual_domains(only_searched=not all_clinics, icp_only=True)


@app.command(name="find-linkedin", rich_help_panel="Company Enrichment")
def find_linkedin(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    skip_maybe: bool = typer.Option(False, "--skip-maybe", help="Skip second-pass MAYBE validation"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
) -> None:
    """Discover LinkedIn company pages for clinics via SERP + LLM categorisation."""
    from zl_scraper.pipeline.company_enrich.find_linkedin import run_find_linkedin

    asyncio.run(run_find_linkedin(limit=limit, skip_maybe=skip_maybe, icp_only=not all_clinics))
    console.print("[green]LinkedIn discovery complete.[/green]")


@app.command(name="krs-enrich", rich_help_panel="Company Enrichment")
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


@app.command(name="sync-leads", rich_help_panel="Lead Enrichment")
def sync_leads(
    all_clinics: bool = typer.Option(False, "--all", help="Process all clinics, not just ICP-fit"),
) -> None:
    """Sync board_members into leads table — dedup KRS by PESEL, CEIDG by name+clinic."""
    from zl_scraper.pipeline.phone_enrich.sync_leads import run_sync_leads

    run_sync_leads(icp_only=not all_clinics)
    console.print("[green]Sync-leads complete.[/green]")


@app.command(name="enrich-phones", rich_help_panel="Lead Enrichment")
def enrich_phones(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many fresh PENDING leads enter Prospeo"),
    step: Optional[str] = typer.Option(None, "--step", help="Run only one tier: prospeo, fullenrich, or lusha"),
    retry_no_phone: bool = typer.Option(False, "--retry-no-phone", help="Re-run waterfall for LUSHA_DONE leads that still have no phone"),
    retry_linkedin: bool = typer.Option(False, "--retry-linkedin", help="Re-run waterfall for linkedin_url + no-phone leads that were not already retried"),
) -> None:
    """Run phone enrichment waterfall: Prospeo → FullEnrich → Lusha."""
    from zl_scraper.pipeline.phone_enrich.enrich_phones import run_enrich_phones

    run_enrich_phones(limit=limit, step=step, retry_no_phone=retry_no_phone, retry_linkedin=retry_linkedin)
    console.print("[green]Phone enrichment complete.[/green]")


@app.command(name="status-leads", rich_help_panel="Status")
def status_leads(
    show_all: bool = typer.Option(False, "--all", help="Show stats for all leads (default: ICP clinics only)"),
) -> None:
    """Show lead counts and phone enrichment progress."""
    from sqlalchemy import func

    icp_only = not show_all

    session = SessionLocal()
    try:
        # Base lead query — optionally scoped to ICP clinics
        def lead_q():
            """Return a base Lead query, filtered to ICP clinics when needed."""
            q = session.query(Lead)
            if icp_only:
                icp_lead_ids = (
                    session.query(lead_clinic_roles.c.lead_id)
                    .join(Clinic, Clinic.id == lead_clinic_roles.c.clinic_id)
                    .filter(Clinic.icp_match.is_(True))
                    .distinct()
                    .subquery()
                )
                q = q.filter(Lead.id.in_(session.query(icp_lead_ids)))
            return q

        total = lead_q().count()
        if total == 0:
            console.print("[yellow]No leads yet. Run sync-leads first.[/yellow]")
            return

        # By enrichment status
        status_counts = (
            lead_q()
            .with_entities(Lead.enrichment_status, func.count(Lead.id))
            .group_by(Lead.enrichment_status)
            .all()
        )
        status_map = dict(status_counts)

        # By lead source
        source_counts = (
            lead_q()
            .with_entities(Lead.lead_source, func.count(Lead.id))
            .group_by(Lead.lead_source)
            .all()
        )

        # Contact stats
        with_phone = lead_q().filter(Lead.phone.isnot(None)).count()
        with_email = lead_q().filter(Lead.email.isnot(None)).count()
        with_linkedin = lead_q().filter(Lead.linkedin_url.isnot(None)).count()

        # By phone source
        phone_source_counts = (
            lead_q()
            .with_entities(Lead.phone_source, func.count(Lead.id))
            .filter(Lead.phone_source.isnot(None))
            .group_by(Lead.phone_source)
            .all()
        )

        # Roles (scoped to ICP clinics if needed)
        roles_q = session.query(lead_clinic_roles)
        if icp_only:
            roles_q = roles_q.join(Clinic, Clinic.id == lead_clinic_roles.c.clinic_id).filter(Clinic.icp_match.is_(True))
        total_roles = roles_q.count()
        unique_clinics = (
            session.query(func.count(func.distinct(lead_clinic_roles.c.clinic_id)))
        )
        if icp_only:
            unique_clinics = (
                unique_clinics
                .join(Clinic, Clinic.id == lead_clinic_roles.c.clinic_id)
                .filter(Clinic.icp_match.is_(True))
            )
        unique_clinics = unique_clinics.scalar()

        title = "Leads Status (ICP only)" if icp_only else "Leads Status"
        table = Table(title=title)
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
        status_order = ["PENDING", "PROSPEO_DONE", "FE_DONE", "LUSHA_DONE", "LUSHA_DONE_LI"]
        for s in status_order:
            cnt = status_map.get(s, 0)
            if cnt > 0:
                label = "  LUSHA_DONE (retry-linkedin exhausted)" if s == "LUSHA_DONE_LI" else f"  {s}"
                table.add_row(label, str(cnt))
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

        # LinkedIn search breakdown
        li_searched = lead_q().filter(Lead.linkedin_searched_at.isnot(None)).count()
        li_not_searched = total - li_searched
        li_found = with_linkedin
        li_searched_no_result = li_searched - li_found
        li_with_maybe = lead_q().filter(
            Lead.linkedin_maybe.isnot(None), Lead.linkedin_maybe != ""
        ).count()
        li_with_no = lead_q().filter(
            Lead.linkedin_no.isnot(None), Lead.linkedin_no != ""
        ).count()

        table.add_row("[bold]LinkedIn search[/]", "")
        table.add_row("Searched", f"{li_searched} / {total}")
        table.add_row("Not searched yet", str(li_not_searched))
        table.add_row("Found (confirmed)", f"{li_found} / {li_searched}")
        table.add_row("Searched, no result", str(li_searched_no_result))
        table.add_row("Has MAYBE candidates", str(li_with_maybe))
        table.add_row("Has rejected candidates", str(li_with_no))
        table.add_row("───", "───")

        if phone_source_counts:
            table.add_row("[bold]Phones by source[/]", "")
            for source, cnt in sorted(phone_source_counts, key=lambda x: -x[1]):
                table.add_row(f"  {source}", str(cnt))

        console.print(table)
    finally:
        session.close()


@app.command(name="find-nip", rich_help_panel="Company Enrichment")
def find_nip(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    all_clinics: bool = typer.Option(False, "--all", help="Process all enriched clinics, not just ICP-fit"),
) -> None:
    """Find Polish NIP (tax ID) for clinics via SERP search on their domain + LLM extraction."""
    from zl_scraper.pipeline.company_enrich.find_nip import run_find_nip

    asyncio.run(run_find_nip(limit=limit, icp_only=not all_clinics))
    console.print("[green]NIP discovery complete.[/green]")


@app.command(name="review-linkedin", rich_help_panel="Company Enrichment")
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


# ── personal LinkedIn discovery ──────────────────────────────────────────


@app.command(name="find-lead-linkedin", rich_help_panel="Lead Enrichment")
def find_lead_linkedin(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many leads to process per step"),
    step: Optional[str] = typer.Option(None, "--step", help="Run only one step: serp, fe, or apify"),
) -> None:
    """Discover personal LinkedIn URLs for leads: SERP → FullEnrich → Apify waterfall."""
    from zl_scraper.pipeline.personal_linkedin import run_lead_linkedin

    stats = asyncio.run(run_lead_linkedin(limit=limit, step=step))

    if stats:
        step_labels = {"serp": "SERP", "fe": "FullEnrich", "apify": "Apify"}
        table = Table(title="Personal LinkedIn Discovery Summary")
        table.add_column("Step", style="cyan")
        table.add_column("YES", style="green", justify="right")
        table.add_column("MAYBE", style="yellow", justify="right")
        table.add_column("NO", style="red", justify="right")

        sum_yes = sum_maybe = sum_no = 0
        for key in ("serp", "fe", "apify"):
            if key in stats:
                s = stats[key]
                table.add_row(step_labels[key], str(s["yes"]), str(s["maybe"]), str(s["no"]))
                sum_yes += s["yes"]
                sum_maybe += s["maybe"]
                sum_no += s["no"]

        table.add_row("───", "───", "───", "───")
        table.add_row("[bold]Total[/]", f"[bold]{sum_yes}[/]", f"[bold]{sum_maybe}[/]", f"[bold]{sum_no}[/]")
        console.print(table)

    console.print("[green]Personal LinkedIn discovery complete.[/green]")


@app.command(name="review-lead-linkedin", rich_help_panel="Lead Enrichment")
def review_lead_linkedin() -> None:
    """Interactively review linkedin_maybe URLs — one lead at a time, pick a number to approve."""
    import subprocess
    from urllib.parse import quote, unquote

    BRAVE_PATH = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"

    def _open_urls_in_brave(urls: list[str]) -> None:
        """Open all URLs as new tabs in a single Brave window."""
        try:
            subprocess.Popen([BRAVE_PATH, *urls])
        except FileNotFoundError:
            console.print("  [red]Brave not found at expected path — skipping auto-open[/red]")

    def _normalize_url(url: str) -> str:
        return quote(unquote(url.strip().rstrip("/").lower()), safe="/:@!$&'()*+,;=-._~?#[]")

    def _dedup_csv(csv_val: str | None) -> list[str]:
        if not csv_val:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for u in csv_val.split(","):
            norm = _normalize_url(u)
            if norm and norm not in seen:
                seen.add(norm)
                result.append(norm)
        return result

    session = SessionLocal()
    try:
        leads = (
            session.query(Lead)
            .filter(
                Lead.linkedin_maybe.isnot(None),
                Lead.linkedin_maybe != "",
                Lead.linkedin_url.is_(None),
            )
            .order_by(Lead.id)
            .all()
        )

        if not leads:
            console.print("[green]No leads with linkedin_maybe pending review.[/green]")
            return

        total = len(leads)
        console.print(f"\n{total} leads with MAYBE URLs to review.")
        console.print("[dim]Enter number to approve that URL  |  0 = reject all  |  Enter = skip  |  q = quit[/dim]\n")

        approved = 0
        rejected = 0
        skipped = 0

        company_cache: dict[int, list[str]] = {}

        def _get_companies(lead_id: int) -> list[str]:
            if lead_id not in company_cache:
                rows = (
                    session.query(Clinic.name, Clinic.legal_name, Clinic.website_domain)
                    .join(lead_clinic_roles, Clinic.id == lead_clinic_roles.c.clinic_id)
                    .filter(lead_clinic_roles.c.lead_id == lead_id)
                    .all()
                )
                company_cache[lead_id] = [
                    f"{r.legal_name or r.name}{f' ({r.website_domain})' if r.website_domain else ''}"
                    for r in rows
                ]
            return company_cache[lead_id]

        for i, lead in enumerate(leads, 1):
            urls = _dedup_csv(lead.linkedin_maybe)
            if not urls:
                continue

            companies = _get_companies(lead.id)
            age = _age_from_pesel(lead.pesel)
            console.print(f"{'─' * 60}")
            age_str = f"  Age: [magenta]{age}[/magenta]" if age is not None else ""
            console.print(f"  [dim][{i}/{total}][/dim]  Lead #{lead.id}  [cyan]{lead.full_name}[/cyan]{age_str}")
            if lead.linkedin_url:
                console.print(f"  Current LinkedIn: [green]{lead.linkedin_url}[/green]")
            for comp in companies:
                console.print(f"  Company: [blue]{comp}[/blue]")
            console.print()
            for idx, url in enumerate(urls, 1):
                console.print(f"  [yellow]{idx})[/yellow] {url}")

            open_list = ([lead.linkedin_url] if lead.linkedin_url else []) + urls
            _open_urls_in_brave(open_list)

            while True:
                raw = input("\n  [1-N / 0 / Enter / q]: ").strip().lower()

                if raw == "q":
                    session.commit()
                    console.print(f"\nQuit. Approved {approved}, rejected {rejected}, skipped {skipped}.")
                    return

                if raw == "":
                    skipped += 1
                    break

                if raw == "0":
                    # Reject all maybe URLs for this lead
                    no_list = _dedup_csv(lead.linkedin_no)
                    for u in urls:
                        norm = _normalize_url(u)
                        if norm not in no_list:
                            no_list.append(norm)
                    lead.linkedin_no = ",".join(no_list) if no_list else None
                    lead.linkedin_maybe = None
                    lead.updated_at = datetime.utcnow()
                    session.commit()
                    rejected += len(urls)
                    console.print(f"  [red]✗ Rejected all {len(urls)} URLs[/red]")
                    break

                if raw.isdigit():
                    choice = int(raw)
                    if 1 <= choice <= len(urls):
                        chosen_url = urls[choice - 1]
                        norm = _normalize_url(chosen_url)
                        lead.linkedin_url = norm
                        # Reject the rest, clear maybe
                        no_list = _dedup_csv(lead.linkedin_no)
                        for u in urls:
                            u_norm = _normalize_url(u)
                            if u_norm != norm and u_norm not in no_list:
                                no_list.append(u_norm)
                        lead.linkedin_no = ",".join(no_list) if no_list else None
                        lead.linkedin_maybe = None
                        lead.updated_at = datetime.utcnow()
                        session.commit()
                        approved += 1
                        rejected += len(urls) - 1
                        console.print(f"  [green]✓ Approved #{choice}[/green]: {norm}")
                        break

                console.print(f"  [dim]Invalid input. Enter 1-{len(urls)}, 0, Enter, or q[/dim]")

        console.print(f"\nDone. Approved {approved}, rejected {rejected}, skipped {skipped}.")
    finally:
        session.close()


# ── LinkedIn profile viewer ──────────────────────────────────────────────


def _print_review_import_summary(result: dict) -> None:
    """Render a compact summary table after importing viewer decisions."""
    table = Table(title="LinkedIn Review Import Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    table.add_row("Processed decision rows", str(result.get("processed", 0)))
    table.add_row("Approved profiles", str(result.get("approved", 0)))
    table.add_row("Rejected profiles", str(result.get("rejected", 0)))
    table.add_row("Skipped / invalid rows", str(result.get("skipped", 0)))
    table.add_row("Missing profile IDs", str(result.get("missing_profiles", 0)))
    table.add_row("Unique leads with linkedin_url set", str(result.get("leads_linkedin_set", 0)))
    table.add_row("URLs appended to linkedin_no", str(result.get("linkedin_no_appended", 0)))
    table.add_row("Auto-rejected sibling profiles", str(result.get("auto_rejected", 0)))
    console.print(table)


@app.command(name="export-viewer", rich_help_panel="Lead Enrichment")
def export_viewer(
    output: str = typer.Option("linkedin_viewer.html", "--output", help="Output HTML file path"),
    brave_path: str = typer.Option(
        r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
        "--brave-path",
        help="Brave executable path used to auto-open the viewer",
    ),
) -> None:
    """Interactive viewer loop: export, auto-open, then import/delete in one flow."""
    import subprocess

    from zl_scraper.pipeline.personal_linkedin.viewer import export_viewer_html, import_review_decisions

    output_path = Path(output).expanduser()

    console.print(
        "[cyan]Interactive viewer mode:[/cyan] exports HTML, opens it in Brave, "
        "then waits for action (import/delete/quit)."
    )

    while True:
        path = Path(export_viewer_html(output_path=str(output_path))).resolve()
        console.print(f"[green]Viewer exported to {path}[/green]")

        try:
            subprocess.Popen([brave_path, str(path)])
            console.print("[dim]Opened viewer in Brave.[/dim]")
        except FileNotFoundError:
            console.print(
                f"[yellow]Brave not found at {brave_path}. Open manually: {path}[/yellow]"
            )

        console.print(
            "\nType one of:\n"
            "  [bold]delete[/bold]  → delete exported HTML and exit\n"
            "  [bold]<file.json>[/bold]  → import review decisions from JSON\n"
            "  [bold]quit[/bold]  → exit\n"
        )

        while True:
            raw = input("[delete | decisions.json | quit]: ").strip()
            if not raw:
                continue

            lower = raw.lower()
            if lower in {"quit", "q", "exit"}:
                console.print("[green]Viewer loop finished.[/green]")
                return

            if lower == "delete":
                if path.exists():
                    path.unlink()
                    console.print(f"[green]Deleted {path.name}[/green]")
                else:
                    console.print("[yellow]File already missing; nothing to delete.[/yellow]")
                console.print("[green]Viewer loop finished.[/green]")
                return

            decisions_path = Path(raw).expanduser()
            if not decisions_path.is_absolute():
                decisions_path = Path.cwd() / decisions_path

            if not decisions_path.exists():
                console.print(f"[red]File not found:[/red] {decisions_path}")
                continue

            result = import_review_decisions(json_path=str(decisions_path))
            _print_review_import_summary(result)

            delete_after = input("Delete exported viewer HTML now? [y/N]: ").strip().lower()
            if delete_after in {"y", "yes", "delete"}:
                if path.exists():
                    path.unlink()
                    console.print(f"[green]Deleted {path.name}[/green]")
                console.print("[green]Viewer loop finished.[/green]")
                return

            break


@app.command(name="import-reviews", rich_help_panel="Lead Enrichment")
def import_reviews(
    file: str = typer.Option(..., "--file", help="Path to decisions JSON file"),
) -> None:
    """Import LinkedIn profile review decisions from the exported viewer."""
    from zl_scraper.pipeline.personal_linkedin.viewer import import_review_decisions

    result = import_review_decisions(json_path=file)
    _print_review_import_summary(result)


# ── filter-worked ─────────────────────────────────────────────────────────


@app.command(name="filter-worked", rich_help_panel="Filtering")
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


# ── search-clinic ─────────────────────────────────────────────────────────


@app.command(name="search-clinic", rich_help_panel="Filtering")
def search_clinic(
    query: str = typer.Argument(..., help="Fuzzy search term (matched against name, legal_name, website_domain)"),
    limit: int = typer.Option(20, "--limit", help="Max results to show"),
    icp_only: bool = typer.Option(False, "--icp", help="Only show ICP-matched clinics"),
) -> None:
    """Fuzzy-search clinics by name, legal_name, or website_domain and show detailed stats."""
    from sqlalchemy import case, func, or_

    from zl_scraper.db.models import lead_clinic_roles

    session = SessionLocal()
    try:
        pattern = f"%{query}%"
        clinics_q = (
            session.query(Clinic)
            .filter(
                or_(
                    Clinic.name.ilike(pattern),
                    Clinic.legal_name.ilike(pattern),
                    Clinic.website_domain.ilike(pattern),
                )
            )
        )
        if icp_only:
            clinics_q = clinics_q.filter(Clinic.icp_match.is_(True))

        clinics = clinics_q.order_by(Clinic.doctors_count.desc().nullslast()).limit(limit).all()

        if not clinics:
            console.print(f"[yellow]No clinics matching '{query}'.[/yellow]")
            return

        console.print(f"\n[bold]Found {len(clinics)} clinic(s) matching '{query}':[/bold]\n")

        for clinic in clinics:
            # Specializations via search_queries
            spec_names = (
                session.query(Specialization.name)
                .join(SearchQuery, SearchQuery.specialization_id == Specialization.id)
                .filter(SearchQuery.clinic_id == clinic.id)
                .distinct()
                .all()
            )
            specs = sorted([r[0] for r in spec_names])

            # Locations
            locations = session.query(ClinicLocation).filter_by(clinic_id=clinic.id).all()
            addresses = [loc.address for loc in locations if loc.address]

            # Leads count
            leads_count = (
                session.query(func.count(func.distinct(lead_clinic_roles.c.lead_id)))
                .filter(lead_clinic_roles.c.clinic_id == clinic.id)
                .scalar()
            )

            # Board members count
            board_count = (
                session.query(func.count(BoardMember.id))
                .filter(BoardMember.clinic_id == clinic.id)
                .scalar()
            )

            icp_badge = "[green]ICP[/green]" if clinic.icp_match else "[red]NOT ICP[/red]"
            table = Table(
                title=f"{clinic.name}  {icp_badge}",
                title_style="bold cyan",
                show_header=False,
                padding=(0, 2),
            )
            table.add_column("Key", style="dim", width=22)
            table.add_column("Value")

            table.add_row("ID", str(clinic.id))
            table.add_row("ZL URL", clinic.zl_url or "—")
            table.add_row("Legal name", clinic.legal_name or "—")
            table.add_row("NIP", clinic.nip or "—")
            table.add_row("Domain", clinic.website_domain or "—")
            table.add_row("LinkedIn", clinic.linkedin_url or "—")
            table.add_row("Doctors", str(clinic.doctors_count or 0))
            table.add_row("Reviews", str(clinic.zl_reviews_cnt or 0))
            table.add_row("Leads", str(leads_count))
            table.add_row("Board members", str(board_count))
            table.add_row("Legal type", clinic.legal_type or "—")
            table.add_row("Locations", "; ".join(addresses) if addresses else "—")
            table.add_row(
                "Specializations",
                ", ".join(f"[magenta]{s}[/magenta]" for s in specs) if specs else "—",
            )
            console.print(table)
            console.print()

    finally:
        session.close()


# ── reset ────────────────────────────────────────────────────────────────


@app.command(rich_help_panel="Admin")
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
