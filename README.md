# ZnanyLekarz Scraper

A modular pipeline for scraping and enriching clinic data from ZnanyLekarz.pl.

## Setup

1. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install .
   ```

3. **Configure environment:**
   - Copy `.env` and adjust settings if needed (DB, proxy, etc).

4. **Create the database:**
   Ensure PostgreSQL is running and create the database if needed:
   ```bash
   psql -U postgres -c "CREATE DATABASE zl_scraper;"
   ```

5. **Run migrations:**
   ```bash
   alembic upgrade head
   ```

## Usage

### Discover clinics
```bash
python src/zl_scraper/cli.py discover
```
Options:
- `--spec-name` — Run for a single specialization by name
- `--spec-id` — Run for a single specialization by ID
- `--offset N` — Skip the first N specializations (0-based, applied after name/id filter)
- `--limit N` — Cap how many **specializations** to process (applied after offset)
- `--max-pages N` — Cap how many **pages** to scrape per specialization (~20 clinics/page)
- `--proxy-level` — Starting proxy tier: `datacenter` (default), `residential`, `unlocker`, or `none`

Examples:
```bash
# Process specializations 10–19, up to 50 pages each
python src/zl_scraper/cli.py discover --offset 10 --limit 10 --max-pages 50

# Process one specialization by name, no page cap
python src/zl_scraper/cli.py discover --spec-name ortopeda

# Skip proxies entirely (direct connection)
python src/zl_scraper/cli.py discover --proxy-level none --limit 1
```

Already-scraped specializations are auto-skipped via the checkpoint system, so re-running the same range is safe.

### Enrich clinics
```bash
python src/zl_scraper/cli.py enrich
```
Options:
- `--limit N` — Cap how many clinics to enrich
- `--proxy-level` — Starting proxy tier: `datacenter` (default), `residential`, `unlocker`, or `none`

### Check progress
```bash
python src/zl_scraper/cli.py status
```

### Export data
```bash
python src/zl_scraper/cli.py export --format csv --output clinics
```

### Reset progress
```bash
python src/zl_scraper/cli.py reset --step discover   # clear discovery checkpoints
python src/zl_scraper/cli.py reset --step enrich      # clear enrichment flags
```

### Filter Worked Domains

The `filter worked domains` command allows you to filter out domains that are already marked as "worked" in the pipeline. This is useful for ensuring that no unnecessary processing is done on domains that are already excluded.

#### Usage
```bash
python src/zl_scraper/cli.py filter-worked
python src/zl_scraper/cli.py filter-worked --dry-run
python src/zl_scraper/cli.py filter-worked --list
```

### Find personal LinkedIn profiles for leads
```bash
python src/zl_scraper/cli.py find-lead-linkedin
```
Runs a three-step waterfall: **SERP → FullEnrich → Apify** to discover personal LinkedIn URLs for leads (board members / prokura).

Options:
- `--limit N` — Cap how many leads to process per step
- `--step serp|fe|apify` — Run only a single step instead of the full waterfall

The SERP step uses LLM-based categorisation to classify results as YES / MAYBE / NO. The first YES is saved as `linkedin_url`, MAYBE URLs go to `linkedin_maybe` for manual review. The FE step uses FullEnrich People Search. The Apify step does a two-pass LinkedIn profile search (industry-filtered, then broad) with LLM validation.

Examples:
```bash
# Full waterfall, 50 leads per step
python src/zl_scraper/cli.py find-lead-linkedin --limit 50

# Only run the SERP step
python src/zl_scraper/cli.py find-lead-linkedin --step serp

# Only run Apify for leads still without a match
python src/zl_scraper/cli.py find-lead-linkedin --step apify --limit 20
```

### Review personal LinkedIn MAYBE candidates
```bash
python src/zl_scraper/cli.py review-lead-linkedin
```
Interactive Tinder-style review — walks through each MAYBE URL one by one, showing lead name, companies, and the candidate URL. Press `y` to approve (sets as `linkedin_url`), `n` to reject (moves to `linkedin_no`), `s` to skip, or `q` to quit. Progress is committed after each action.

### Phone enrichment
```bash
python src/zl_scraper/cli.py enrich-phones
```
Runs the phone enrichment waterfall: **Prospeo → FullEnrich → Lusha**.

Options:
- `--limit N` — Cap how many fresh PENDING leads enter Prospeo
- `--step prospeo|fullenrich|lusha` — Run only one tier
- `--retry-no-phone` — Reset LUSHA_DONE leads that still have no phone back to PENDING and re-run the waterfall

Examples:
```bash
# Full waterfall
python src/zl_scraper/cli.py enrich-phones --limit 100

# Re-run for leads that completed the waterfall but still have no phone
python src/zl_scraper/cli.py enrich-phones --retry-no-phone
```

## Proxy Waterfall

The scraper uses a waterfall proxy strategy, trying tiers from cheapest to most expensive:

1. **datacenter** — Bright Data datacenter proxy (cheapest)
2. **residential** — Apify residential proxy
3. **unlocker** — Bright Data Web Unlocker (most expensive, best anti-bot bypass)

If a tier returns a non-200 status or a network error, the request automatically escalates to the next tier. Use `--proxy-level` to set the starting tier (e.g. `--proxy-level residential` skips datacenter).

## Notes
- All configuration is via `.env` or environment variables.
- Proxy URLs and per-tier rate limits are set in `.env`.
- See `pyproject.toml` for dependencies.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://…` | PostgreSQL connection string |
| `DATACENTER_PROXY_URL` | — | Bright Data datacenter proxy URL |
| `RESIDENTIAL_PROXY_URL` | — | Apify residential proxy URL |
| `WEB_UNLOCKER_URL` | — | Bright Data Web Unlocker URL |
| `DATACENTER_RATE_LIMIT` | `100` | Max requests/min for datacenter tier |
| `RESIDENTIAL_RATE_LIMIT` | `100` | Max requests/min for residential tier |
| `WEB_UNLOCKER_RATE_LIMIT` | `100` | Max requests/min for unlocker tier |
| `USE_PROXY` | `true` | Enable/disable proxy usage |
| `SEARCH_CONCURRENCY` | `5` | Max concurrent discovery page fetches |
| `PROFILE_CONCURRENCY` | `15` | Max concurrent enrichment fetches (profile + doctors) |
| `REQUEST_TIMEOUT` | `10` | HTTP request timeout in seconds |
| `MAX_RETRIES` | `3` | Retry count on transient failures |
| `RETRY_WAIT_MULTIPLIER` | `2` | Exponential backoff multiplier |
