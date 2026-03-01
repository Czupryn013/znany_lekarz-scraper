"""Typer CLI entrypoint for the ZnanyLekarz scraping pipeline."""

import asyncio
import csv
import json
import sys
from pathlib import Path
from typing import Optional

# Allow running directly: python src/zl_scraper/cli.py <command>
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation, Doctor, LinkedInCandidate, ScrapeProgress, SearchQuery, Specialization, clinic_doctors
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
def status() -> None:
    """Print progress: specializations scraped, clinics discovered/enriched."""
    session = SessionLocal()
    try:
        total_specs = session.query(Specialization).count()
        done_specs = session.query(ScrapeProgress).filter_by(status="done").count()
        in_progress_specs = session.query(ScrapeProgress).filter_by(status="in_progress").count()

        total_clinics = session.query(Clinic).count()
        enriched_clinics = session.query(Clinic).filter(Clinic.enriched_at.isnot(None)).count()
        unenriched_clinics = total_clinics - enriched_clinics
        clinics_with_nip = (
            session.query(Clinic)
            .filter(Clinic.enriched_at.isnot(None), Clinic.nip.isnot(None))
            .count()
        )

        total_locations = session.query(ClinicLocation).count()
        clinics_with_linkedin = (
            session.query(ClinicLocation.clinic_id)
            .join(Clinic, Clinic.id == ClinicLocation.clinic_id)
            .filter(Clinic.enriched_at.isnot(None), ClinicLocation.linkedin_url.isnot(None))
            .distinct()
            .count()
        )
        clinics_with_website = (
            session.query(ClinicLocation.clinic_id)
            .join(Clinic, Clinic.id == ClinicLocation.clinic_id)
            .filter(Clinic.enriched_at.isnot(None), ClinicLocation.website_url.isnot(None))
            .distinct()
            .count()
        )

        total_doctors = session.query(Doctor).count()

        table = Table(title="ZL Scraper Status")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")

        table.add_row("Total specializations", str(total_specs))
        table.add_row("Specializations done", str(done_specs))
        table.add_row("Specializations in progress", str(in_progress_specs))
        table.add_row("Specializations pending", str(total_specs - done_specs - in_progress_specs))
        table.add_row("───", "───")
        table.add_row("Total clinics discovered", str(total_clinics))
        table.add_row("Clinics enriched", str(enriched_clinics))
        table.add_row("Clinics awaiting enrichment", str(unenriched_clinics))
        table.add_row("Clinics with NIP", f"{clinics_with_nip} / {enriched_clinics}")
        table.add_row("Clinics with LinkedIn URL", f"{clinics_with_linkedin} / {enriched_clinics}")
        table.add_row("Clinics with website URL", f"{clinics_with_website} / {enriched_clinics}")
        table.add_row("───", "───")

        # Company enrichment metrics (clinic-level)
        clinics_with_domain = (
            session.query(Clinic)
            .filter(Clinic.enriched_at.isnot(None), Clinic.website_domain.isnot(None))
            .count()
        )
        domain_searched = (
            session.query(Clinic)
            .filter(Clinic.enriched_at.isnot(None), Clinic.domain_searched_at.isnot(None))
            .count()
        )
        clinics_with_li = (
            session.query(Clinic)
            .filter(Clinic.enriched_at.isnot(None), Clinic.linkedin_url.isnot(None))
            .count()
        )
        linkedin_searched = (
            session.query(Clinic)
            .filter(Clinic.enriched_at.isnot(None), Clinic.linkedin_searched_at.isnot(None))
            .count()
        )
        maybe_pending = (
            session.query(LinkedInCandidate)
            .filter(LinkedInCandidate.status == "maybe")
            .count()
        )

        table.add_row("[bold]Company enrichment[/]", "")
        table.add_row("Domain found (clinic-level)", f"{clinics_with_domain} / {enriched_clinics}")
        table.add_row("Domain SERP searched", f"{domain_searched} / {enriched_clinics}")
        table.add_row("LinkedIn found (clinic-level)", f"{clinics_with_li} / {enriched_clinics}")
        table.add_row("LinkedIn SERP searched", f"{linkedin_searched} / {enriched_clinics}")
        table.add_row("LinkedIn MAYBE pending", str(maybe_pending))
        table.add_row("───", "───")

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


# ── filter ────────────────────────────────────────────────────────────────


@app.command(name="filter")
def filter_clinics(
    min_doctors: int = typer.Option(20, "--min-doctors", help="Minimum doctor count threshold"),
    format: str = typer.Option("csv", "--format", help="Output format: csv or json"),
    output: str = typer.Option("filtered_clinics", "--output", help="Output file path (without extension)"),
    show_excluded: bool = typer.Option(False, "--show-excluded", help="Print excluded specializations and exit"),
    show_allowed: bool = typer.Option(False, "--show-allowed", help="Print allowed (ICP) specializations and exit"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show filter summary without exporting"),
) -> None:
    """Filter enriched clinics by doctor count and ICP specialization match."""
    from zl_scraper.pipeline.filter import (
        build_allowed_specialization_names,
        build_excluded_specialization_names,
        build_export_rows,
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

        if dry_run:
            table = Table(title="Filter Dry Run")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green", justify="right")
            table.add_row("Min doctors threshold", str(min_doctors))
            table.add_row("Allowed specializations", str(len(allowed_specs)))
            table.add_row("Excluded specializations", str(len(excluded_specs)))
            table.add_row("───", "───")
            table.add_row("Total enriched clinics", str(result.total_enriched))
            table.add_row("[red]Rejected (too few doctors)[/]", str(result.rejected_doctors))
            table.add_row("[red]Rejected (wrong specialization)[/]", str(result.rejected_specialization))
            table.add_row("[bold]Clinics matched (kept)[/]", f"[bold]{result.total_matched}[/]")
            table.add_row("───", "───")
            table.add_row("Total doctors in matched", str(result.total_doctors_in_matched))
            table.add_row("Avg doctors per clinic", f"{result.avg_doctors:.1f}")
            table.add_row("───", "───")
            table.add_row("With NIP", f"{with_nip} / {result.total_matched}")
            table.add_row("With website URL", f"{with_website} / {result.total_matched}")
            table.add_row("With LinkedIn URL", f"{with_linkedin} / {result.total_matched}")
            console.print(table)
            return

        if not clinics:
            console.print("[yellow]No clinics matched the filter criteria.[/yellow]")
            return

        rows = build_export_rows(session, clinics)

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

        # Summary table
        table = Table(title="Filter Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("Min doctors threshold", str(min_doctors))
        table.add_row("Allowed specializations", str(len(allowed_specs)))
        table.add_row("Excluded specializations", str(len(excluded_specs)))
        table.add_row("───", "───")
        table.add_row("Total enriched clinics", str(result.total_enriched))
        table.add_row("Rejected (too few doctors)", str(result.rejected_doctors))
        table.add_row("Rejected (wrong specialization)", str(result.rejected_specialization))
        table.add_row("Clinics matched", str(result.total_matched))
        table.add_row("Total doctors in matched", str(result.total_doctors_in_matched))
        table.add_row("Avg doctors per clinic", f"{result.avg_doctors:.1f}")
        table.add_row("───", "───")
        table.add_row("With NIP", f"{with_nip} / {result.total_matched}")
        table.add_row("With website URL", f"{with_website} / {result.total_matched}")
        table.add_row("With LinkedIn URL", f"{with_linkedin} / {result.total_matched}")
        table.add_row("───", "───")
        table.add_row("Exported to", filepath)
        console.print(table)
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
) -> None:
    """Discover website domains for clinics via SERP search + LLM validation."""
    from zl_scraper.pipeline.company_enrich.find_domains import run_find_domains

    asyncio.run(run_find_domains(limit=limit))
    console.print("[green]Domain discovery complete.[/green]")


@app.command(name="find-linkedin")
def find_linkedin(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to process"),
    skip_maybe: bool = typer.Option(False, "--skip-maybe", help="Skip second-pass MAYBE validation"),
) -> None:
    """Discover LinkedIn company pages for clinics via SERP + LLM categorisation."""
    from zl_scraper.pipeline.company_enrich.find_linkedin import run_find_linkedin

    asyncio.run(run_find_linkedin(limit=limit, skip_maybe=skip_maybe))
    console.print("[green]LinkedIn discovery complete.[/green]")


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

        else:
            console.print(f"[red]Unknown step: {step}. Use 'discover', 'enrich', or 'domains'.[/red]")
            raise typer.Exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    app()
