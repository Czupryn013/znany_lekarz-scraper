"""FastAPI application with lifespan graph loader and static file serving."""

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Add project src to path so zl_scraper imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from zl_scraper.db.engine import SessionLocal  # noqa: E402
from web_app.graph import load_graph, load_nip_mapping, build_merged_graph, load_doctor_specs  # noqa: E402
from web_app.lead_graph import (  # noqa: E402
    load_lead_graph,
    load_lead_nip_mapping,
    build_lead_merged_graph,
)
from web_app.routes import router  # noqa: E402
from web_app.lead_routes import router as lead_router  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load both doctor-clinic and lead-clinic graphs into memory at startup."""
    logger.info("Starting web app — loading graphs...")
    session = SessionLocal()
    try:
        # ── Doctor-Clinic graph ──
        c2d, d2c = load_graph(session)
        app.state.c2d = c2d
        app.state.d2c = d2c
        app.state.SessionLocal = SessionLocal

        clinic_to_rep, rep_to_members = load_nip_mapping(session)
        merged_c2d, merged_d2c = build_merged_graph(c2d, d2c, clinic_to_rep)
        app.state.merged_c2d = merged_c2d
        app.state.merged_d2c = merged_d2c
        app.state.clinic_to_rep = clinic_to_rep
        app.state.rep_to_members = rep_to_members

        # Doctor-specialization map for spec search
        doctor_specs = load_doctor_specs(session)
        app.state.doctor_specs = doctor_specs

        logger.info("Doctor graph ready: %d clinics, %d doctors", len(c2d), len(d2c))

        # ── Lead-Clinic graph ──
        lead_c2l, lead_l2c = load_lead_graph(session)
        app.state.lead_c2l = lead_c2l
        app.state.lead_l2c = lead_l2c

        lead_clinic_to_rep, lead_rep_to_members = load_lead_nip_mapping(session)
        lead_merged_c2l, lead_merged_l2c = build_lead_merged_graph(
            lead_c2l, lead_l2c, lead_clinic_to_rep
        )
        app.state.lead_merged_c2l = lead_merged_c2l
        app.state.lead_merged_l2c = lead_merged_l2c
        app.state.lead_clinic_to_rep = lead_clinic_to_rep
        app.state.lead_rep_to_members = lead_rep_to_members

        logger.info("Lead graph ready: %d clinics, %d leads", len(lead_c2l), len(lead_l2c))
    finally:
        session.close()
    yield
    logger.info("Shutting down web app.")


app = FastAPI(title="Doctor-Clinic & Lead Network", lifespan=lifespan)
app.include_router(router)
app.include_router(lead_router)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    """Serve the doctor-clinic frontend."""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/leads")
def leads_page():
    """Serve the lead-clinic frontend."""
    return FileResponse(STATIC_DIR / "leads.html")
