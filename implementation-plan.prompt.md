## Plan: ZnanyLekarz Clinic Scraping Pipeline (Python)

**TL;DR**: Rewrite the n8n-based ZnanyLekarz clinic scraper as a modular Python CLI pipeline backed by PostgreSQL (via SQLAlchemy + Alembic). The pipeline has 2 strict stages run sequentially: (1) **discover** — scrape search result pages across ALL ~100 specializations to collect every clinic URL (with deduplication across specializations), then (2) **enrich** — fetch profile page + doctor list for each unique clinic not yet enriched. Data is stored in a normalized schema (clinic ↔ locations as separate tables, search queries tracked per specialization). Each step is idempotent and pull-based — it queries the DB for "unprocessed" work rather than passing state. Concurrent fetching via `httpx` async with semaphore-controlled concurrency + Apify rotating proxies.

**Steps**

### 0. Project scaffolding

Create this directory structure under the repo root:

```
specializations.json          (fix formatting: JS → proper JSON)
pyproject.toml                (dependencies, project metadata)
alembic.ini                   (Alembic config)
alembic/                      (auto-generated migration directory)
src/
  zl_scraper/
    __init__.py
    cli.py                    (Typer CLI entrypoint)
    config.py                 (settings: DB URL, proxy URL, concurrency limits, etc.)
    db/
      __init__.py
      engine.py               (create_engine, sessionmaker)
      models.py               (SQLAlchemy declarative models)
    scraping/
      __init__.py
      http_client.py           (async httpx client factory with proxy rotation)
      search_pages.py          (scrape search result pages → extract clinic stubs)
      profile_enrichment.py    (fetch profile HTML → extract all fields)
      doctors.py               (fetch /facility/{id}/profile/doctors endpoint)
      parsers.py               (pure HTML parsing functions — BeautifulSoup)
    pipeline/
      __init__.py
      discover.py              (orchestrate: load specializations → paginate → save stubs)
      enrich.py                (orchestrate: pull un-enriched clinics → fetch → save)
    utils/
      __init__.py
      logging.py               (structured logging setup)
      retry.py                 (tenacity-based retry decorator with logging)
```

Dependencies: `sqlalchemy`, `alembic`, `psycopg2-binary`, `httpx`, `beautifulsoup4`, `lxml`, `typer`, `tenacity`, `python-dotenv`.

### 1. Fix specializations.json

Convert from JS object notation (no-quote keys) to valid JSON array: `[{"id": 57, "name": "ginekolog"}, ...]`.

### 2. Database models — `src/zl_scraper/db/models.py`

Normalized schema replacing the flat Google Sheet row:

- **`Specialization`** — `id` (PK, from ZL), `name` (e.g. "ginekolog"). Loaded from specializations.json.
- **`Clinic`** — `id` (PK auto), `zl_url` (unique index, the dedup key), `name`, `zl_profile_id` (from ZL's `data-eec-entity-id`), `nip`, `legal_name`, `description`, `zl_reviews_cnt`, `doctors_count`, `discovered_at`, `enriched_at` (NULL = not yet enriched).
- **`ClinicLocation`** — `id`, `clinic_id` (FK), `address`, `latitude`, `longitude`.
- **`SearchQuery`** — `clinic_id` (FK), `specialization_id` (FK), `discovered_at`. Composite PK on (`clinic_id`, `specialization_id`). Tracks which search queries found this clinic. When a clinic appears in a new specialization search, we just INSERT into this table — no re-fetch needed.
- **`ScrapeProgress`** — `specialization_id` (FK, unique), `last_page_scraped`, `total_pages`, `status` (pending/in_progress/done), `updated_at`. Enables checkpoint/resume per specialization.

### 3. Config — `src/zl_scraper/config.py`

Load from `.env` file via `python-dotenv`:
- `DATABASE_URL` (PostgreSQL connection string)
- `PROXY_URL` (Apify proxy — current value from n8n flow)
- `SEARCH_CONCURRENCY` (default 5, matching n8n batch size)
- `PROFILE_CONCURRENCY` (default 15, matching n8n batch size)
- `DOCTORS_CONCURRENCY` (default 15)
- `REQUEST_TIMEOUT` (default 10s)
- `MAX_RETRIES` (default 3)
- `RETRY_WAIT_MULTIPLIER` (default: exponential backoff — 2s, 10s, 30s)

### 4. HTTP client — `src/zl_scraper/scraping/http_client.py`

- Factory function returning an `httpx.AsyncClient` configured with proxy, timeout, retry headers.
- Semaphore wrapper: `async def fetch(client, url, semaphore)` — acquires semaphore, makes request, logs result, returns response.
- Retry logic via `tenacity` decorator: retry on connection errors / non 200 responses with **exponential backoff** (2s → 10s → 30s), then give up. Log every retry attempt with: attempt number, wait duration, URL, error reason. Log final failure with full context if all retries exhausted.

### 5. Parsers — `src/zl_scraper/scraping/parsers.py`

Pure functions (no I/O), each with a one-line docstring:

- `parse_search_page(html: str) -> list[ClinicStub]` — Extract name, zl_profile href, specializations text from search result HTML. CSS selectors: `h3.h4.mb-0 a.text-body span` (name), `h3.h4.mb-0 a.text-body[href]` (url), `span[data-test-id="doctor-specializations"]` (specializations).
- `parse_total_pages(html: str) -> int` — Extract the last page number from pagination controls on the first search results page.
- `parse_profile_page(html: str) -> ProfileData` — Extract addresses, profile_id, coordinates, reviews count, description, NIP, legal_name. CSS selectors from the n8n HTML node.
- `parse_coordinates(maps_url: str) -> tuple[float, float]` — Regex-extract lat/lng from Google Maps URL.
- `parse_doctors_response(json_text: str) -> int` — Parse the doctors JSON array, return count.

### 6. Search page scraping — `src/zl_scraper/scraping/search_pages.py`

- `async def scrape_specialization_pages(spec_id, spec_name, client, semaphore, session)` — For one specialization, paginate through all pages. URL pattern: `https://www.znanylekarz.pl/szukaj?q={name}&loc=&filters[entity_type][]=facility&filters[specializations][]={id}&page={page}`.
- First fetch page 1, determine total pages (parse pagination or detect empty results).
- Fetch remaining pages concurrently (controlled by semaphore).
- For each page, call `parse_search_page()`, yield clinic stubs.
- **Idempotent**: check `ScrapeProgress` table — resume from `last_page_scraped + 1`. Update progress after each page batch.

### 7. Profile enrichment — `src/zl_scraper/scraping/profile_enrichment.py`

- `async def enrich_clinic(clinic_url, client, semaphore)` — Fetch profile HTML, parse it, fetch doctors endpoint, return enriched data.
- Two HTTP calls per clinic (profile page + doctors API), can be done concurrently for the same clinic.
- **Idempotent**: orchestrator only pulls clinics where `enriched_at IS NULL`.

### 8. Pipeline orchestrators — `src/zl_scraper/pipeline/`

The two stages are strictly sequential: **discover ALL, then enrich**. This is a deliberate change from the n8n flow (which interleaved scrape→enrich per specialization) — running discovery across all ~100 specializations first ensures proper deduplication. A clinic appearing in 5 specialization searches gets 5 `SearchQuery` rows but only 1 profile fetch.

**`discover.py`** — Search discovery orchestrator:
1. Load specializations from JSON (or DB).
2. For each specialization, check `ScrapeProgress` — skip if `status = 'done'`.
3. Call `scrape_specialization_pages()`.
4. For each discovered clinic stub:
   - `INSERT ... ON CONFLICT (zl_url) DO NOTHING` into `Clinic` table (discovered_at = now, enriched_at = NULL).
   - `INSERT ... ON CONFLICT DO NOTHING` into `SearchQuery` (link clinic ↔ specialization).
5. Update `ScrapeProgress` to done.
6. Log summary: X new clinics, Y already known (deduped), Z pages scraped.
7. After all specializations complete, log grand total: total unique clinics discovered, total search query links.

**`enrich.py`** — Enrichment orchestrator (runs after discover is fully complete):
1. Query `Clinic WHERE enriched_at IS NULL` — these are the "not worked" items.
2. Batch them (e.g. 30 at a time, matching n8n pattern).
3. For each batch, concurrently call `enrich_clinic()`.
4. For each successful result:
   - Update `Clinic` row with profile data (nip, legal_name, description, reviews_cnt, doctors_count, enriched_at = now).
   - Insert `ClinicLocation` rows (one per address).
5. Log per-batch progress: enriched X/Y total, Z failures.

### 9. CLI — `src/zl_scraper/cli.py`

Typer-based CLI with commands:

- `python -m zl_scraper discover` — Run search page discovery for all (or specific) specializations. Flags: `--spec-name`, `--spec-id` to run a single one, `--max-pages N` to cap pages per specialization (useful for testing), `--limit N` to cap total specializations to process.
- `python -m zl_scraper enrich` — Enrich all un-enriched clinics. Flags: `--limit N` to cap how many to process.
- `python -m zl_scraper status` — Print progress: how many specializations scraped, how many clinics discovered/enriched.
- `python -m zl_scraper export` — Export enriched data to CSV or JSON (future: upload to Google Sheets).
- `python -m zl_scraper reset --step discover|enrich` — Reset progress for re-runs.

### 10. Logging — `src/zl_scraper/utils/logging.py`

- Structured logging with `logging` module.
- Log at start/end of each pipeline step.
- Log every HTTP error with full context (URL, status, response snippet).
- Log retries with attempt number and reason.
- Console output with colors via Typer's `rich` integration.

## Verification

1. Run `alembic upgrade head` to create tables in a local PostgreSQL instance.
2. Run `python -m zl_scraper discover --spec-name ginekolog --max-pages 1` to test single-specialization discovery (capped to 1 page for quick verification).
3. Verify `Clinic` and `SearchQuery` rows were created. Re-run the same command — verify idempotency (no duplicates, progress shows "already done").
4. Run `python -m zl_scraper enrich --limit 5` to test enrichment of 5 clinics.
5. Verify `ClinicLocation` rows were created, `enriched_at` is set. Re-run — verify it skips already-enriched clinics.
6. Run `python -m zl_scraper status` to confirm progress reporting.
7. Run `python -m zl_scraper export --format csv` to verify export.

## Decisions

- **SQLAlchemy + Alembic** over Django (no web overhead) or raw SQL (migrations + ORM convenience).
- **httpx async** over aiohttp (cleaner API, native proxy support) or requests+threads (less efficient).
- **Normalized schema** (Clinic → ClinicLocation, SearchQuery) instead of flat row — avoids the current Google Sheets problem of squishing addresses into one cell. Specializations from the profile page are not stored (ZL dumps everything there, not useful); only `SearchQuery` links matter.
- **Discover-all-then-enrich** — unlike the n8n flow which interleaved scrape→enrich per specialization, we run all discovery first to properly deduplicate clinics across specializations before fetching profiles.
- **Exponential backoff retries** (2s → 10s → 30s) instead of flat 5s — handles nighttime 500s gracefully. Every retry logged with attempt/reason/URL.
- **Typer** for CLI (built on Click, auto-generates help, rich output).
- **Doctors filter removed** as requested — all clinics saved regardless of doctor count.
- **Pull-based idempotency**: each step queries DB for unprocessed work (`WHERE enriched_at IS NULL`, `WHERE status != 'done'`) rather than passing state between steps.
- **ScrapeProgress per specialization** enables resuming mid-pipeline if a specialization fails at page 87/200.
