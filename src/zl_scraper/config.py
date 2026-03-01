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
# Waterfall tiers: datacenter (cheapest) → residential → web unlocker (most expensive)
DATACENTER_PROXY_URL: str = os.getenv(
    "DATACENTER_PROXY_URL",
    "abc",
)
RESIDENTIAL_PROXY_URL: str = os.getenv(
    "RESIDENTIAL_PROXY_URL",
    "abc",
)
WEB_UNLOCKER_URL: str = os.getenv(
    "WEB_UNLOCKER_URL",
    "abc",
)
DATACENTER_RATE_LIMIT: int = int(os.getenv("DATACENTER_RATE_LIMIT", "100"))
RESIDENTIAL_RATE_LIMIT: int = int(os.getenv("RESIDENTIAL_RATE_LIMIT", "100"))
WEB_UNLOCKER_RATE_LIMIT: int = int(os.getenv("WEB_UNLOCKER_RATE_LIMIT", "100"))
USE_PROXY: bool = os.getenv("USE_PROXY", "true").lower() in ("1", "true", "yes")
# ── Concurrency ──────────────────────────────────────────────────────────
SEARCH_CONCURRENCY: int = int(os.getenv("SEARCH_CONCURRENCY", "5"))
PROFILE_CONCURRENCY: int = int(os.getenv("PROFILE_CONCURRENCY", "15"))

# ── HTTP ─────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "10"))

# ── Retry ────────────────────────────────────────────────────────────────
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RETRY_WAIT_MULTIPLIER: float = float(os.getenv("RETRY_WAIT_MULTIPLIER", "2"))

# ── Apify ────────────────────────────────────────────────────────────────
APIFY_API_TOKEN: str = os.getenv("APIFY_API_TOKEN", "")
SERP_KEYWORDS_PER_CALL: int = int(os.getenv("SERP_KEYWORDS_PER_CALL", "20"))
APIFY_CONCURRENCY: int = int(os.getenv("APIFY_CONCURRENCY", "20"))
APIFY_ACTOR_TIMEOUT_SECS: int = int(os.getenv("APIFY_ACTOR_TIMEOUT_SECS", "300"))

# ── OpenAI ───────────────────────────────────────────────────────────────
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# ── ZnanyLekarz URLs ────────────────────────────────────────────────────
ZL_BASE_URL = "https://www.znanylekarz.pl"
ZL_SEARCH_URL = f"{ZL_BASE_URL}/szukaj"
