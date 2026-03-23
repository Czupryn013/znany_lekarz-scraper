"""Scrape LinkedIn employees for clinics with confirmed company linkedin_url."""

import asyncio
from datetime import datetime

from apify_client import ApifyClientAsync
from sqlalchemy.dialects.postgresql import insert as pg_insert

from zl_scraper.config import APIFY_API_TOKEN, EMPLOYEE_SCRAPE_CONCURRENCY
from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Employee
from zl_scraper.pipeline.employee_scraper.queries import get_clinics_ready_for_employees
from zl_scraper.scraping.linkedin_employee_scraper import scrape_company_employees
from zl_scraper.utils.logging import get_logger

logger = get_logger("employee_scraper.scrape")


def _save_employees(session, clinic_id: int, parsed_employees: list[dict]) -> int:
    """Upsert employees into the DB. Returns count of new employees."""
    created = 0
    for emp in parsed_employees:
        linkedin_url = emp.get("linkedin_url", "")
        if not linkedin_url:
            continue

        existing = (
            session.query(Employee)
            .filter(
                Employee.clinic_id == clinic_id,
                Employee.linkedin_url == linkedin_url,
            )
            .first()
        )

        if existing:
            continue

        session.add(
            Employee(
                clinic_id=clinic_id,
                full_name=emp.get("full_name", "Unknown"),
                linkedin_url=linkedin_url,
                position_title=emp.get("position_title"),
                company_name=emp.get("company_name"),
                raw_profile=emp.get("raw_profile"),
                scraped_at=datetime.utcnow(),
            )
        )
        created += 1

    return created


async def run_scrape_employees(
    limit: int | None = None,
    icp_only: bool = True,
) -> None:
    """Scrape LinkedIn employees for clinics that have a linkedin_url."""
    logger.info("Starting scrape-employees (icp_only=%s)", icp_only)

    session = SessionLocal()
    try:
        clinics = get_clinics_ready_for_employees(session, icp_only=icp_only, limit=limit)
        total = len(clinics)

        if total == 0:
            logger.info("No clinics ready for employee scraping — nothing to do")
            return

        logger.info(
            "Found %d clinics ready for employee scraping (concurrency=%d)",
            total,
            EMPLOYEE_SCRAPE_CONCURRENCY,
        )

        client = ApifyClientAsync(token=APIFY_API_TOKEN)
        semaphore = asyncio.Semaphore(EMPLOYEE_SCRAPE_CONCURRENCY)
        total_employees = 0

        for i, clinic in enumerate(clinics):
            async with semaphore:
                logger.info(
                    "Scraping employees %d/%d — '%s' (%s)",
                    i + 1,
                    total,
                    clinic.name,
                    clinic.linkedin_url,
                )

                parsed = await scrape_company_employees(
                    clinic.linkedin_url,
                    client=client,
                )

                if parsed:
                    created = _save_employees(session, clinic.id, parsed)
                    total_employees += created
                    logger.info(
                        "Saved %d new employees for '%s' (%d returned)",
                        created,
                        clinic.name,
                        len(parsed),
                    )
                else:
                    logger.info("No employees found for '%s'", clinic.name)

                clinic.employees_scraped_at = datetime.utcnow()
                session.commit()

        logger.info(
            "scrape-employees complete: %d employees scraped from %d clinics",
            total_employees,
            total,
        )
    except Exception:
        session.rollback()
        logger.exception("Error during scrape-employees")
        raise
    finally:
        session.close()
