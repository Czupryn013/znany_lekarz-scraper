"""Typer CLI entrypoint for the ZnanyLekarz scraping pipeline."""

import asyncio
import csv
import json
import sys
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation, ScrapeProgress, Specialization
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
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap total specializations to process"),
) -> None:
    """Run search page discovery for all (or specific) specializations."""
    from zl_scraper.pipeline.discover import run_discovery

    asyncio.run(
        run_discovery(
            spec_name=spec_name,
            spec_id=spec_id,
            max_pages=max_pages,
            limit=limit,
        )
    )
    console.print("[green]Discovery complete.[/green]")


# ── enrich ───────────────────────────────────────────────────────────────


@app.command()
def enrich(
    limit: Optional[int] = typer.Option(None, "--limit", help="Cap how many clinics to enrich"),
) -> None:
    """Enrich all un-enriched clinics with profile + doctors data."""
    from zl_scraper.pipeline.enrich import run_enrichment

    asyncio.run(run_enrichment(limit=limit))
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

        total_locations = session.query(ClinicLocation).count()

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
        table.add_row("───", "───")
        table.add_row("Total clinic locations", str(total_locations))

        console.print(table)
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


# ── reset ────────────────────────────────────────────────────────────────


@app.command()
def reset(
    step: str = typer.Option(..., "--step", help="Which step to reset: discover or enrich"),
) -> None:
    """Reset progress for re-runs (discover or enrich)."""
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
                .update({Clinic.enriched_at: None})
            )
            # Also remove clinic locations since they'll be re-created
            session.query(ClinicLocation).delete()
            session.commit()
            console.print(f"[green]Reset enrichment for {updated} clinics (locations cleared).[/green]")

        else:
            console.print(f"[red]Unknown step: {step}. Use 'discover' or 'enrich'.[/red]")
            raise typer.Exit(1)
    finally:
        session.close()
