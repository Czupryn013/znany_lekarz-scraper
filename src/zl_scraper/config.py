"""Application settings loaded from environment variables / .env file."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Paths ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SPECIALIZATIONS_PATH = PROJECT_ROOT / "specializations.json"

# ── Database ─────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/zl_scraper",
)

# ── Proxy ────────────────────────────────────────────────────────────────
PROXY_URL: str = os.getenv(
    "PROXY_URL",
    "http://groups-BUYPROXIES94952:some_pwd@proxy.apify.com:8000",
)

# ── Concurrency ──────────────────────────────────────────────────────────
SEARCH_CONCURRENCY: int = int(os.getenv("SEARCH_CONCURRENCY", "5"))
PROFILE_CONCURRENCY: int = int(os.getenv("PROFILE_CONCURRENCY", "15"))
DOCTORS_CONCURRENCY: int = int(os.getenv("DOCTORS_CONCURRENCY", "15"))

# ── HTTP ─────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "10"))

# ── Retry ────────────────────────────────────────────────────────────────
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RETRY_WAIT_MULTIPLIER: float = float(os.getenv("RETRY_WAIT_MULTIPLIER", "2"))

# ── ZnanyLekarz URLs ────────────────────────────────────────────────────
ZL_BASE_URL = "https://www.znanylekarz.pl"
ZL_SEARCH_URL = f"{ZL_BASE_URL}/szukaj"
