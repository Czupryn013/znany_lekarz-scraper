"""OpenAI LLM validation helpers for domain and LinkedIn matching."""

import asyncio

from openai import AsyncOpenAI

from zl_scraper.config import OPENAI_API_KEY, OPENAI_MODEL
from zl_scraper.scraping.serp import SerpResult
from zl_scraper.utils.logging import get_logger

logger = get_logger("llm")

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    """Lazily initialise a shared AsyncOpenAI client."""
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    return _client


# ── Domain validation ────────────────────────────────────────────────────

DOMAIN_SYSTEM_PROMPT = (
    "You validate which result (if any) is the target medical clinic's OFFICIAL website.\n\n"
    "Reject directories, booking portals, 3rd-party sites.\n"
    "If exactly one official site matches, output its domain.\n"
    "If none match, output NULL.\n"
    "Output only the domain or NULL."
)


def _build_domain_user_prompt(
    clinic_name: str,
    cities: list[str],
    serp_results: list[SerpResult],
) -> str:
    """Build the user message for domain validation."""
    results_str = "\n".join(
        f"- {r.url.split('/')[2] if '/' in r.url else r.url}: {r.title} - {r.description}"
        for r in serp_results
    )
    return (
        f"TARGET\n- name: {clinic_name}\n- cities: {', '.join(cities)}\n\n"
        f"RESULTS:\n{results_str}"
    )


async def validate_domain(
    clinic_name: str,
    cities: list[str],
    serp_results: list[SerpResult],
) -> str | None:
    """Ask LLM to pick the official website domain from SERP results, or return None."""
    if not serp_results:
        logger.info("[domain] '%s' — no SERP results to validate", clinic_name)
        return None

    client = _get_client()
    user_msg = _build_domain_user_prompt(clinic_name, cities, serp_results)

    # Log candidate domains being sent to LLM
    candidate_domains = [
        r.url.split('/')[2] if '/' in r.url else r.url
        for r in serp_results
    ]
    logger.info(
        "[domain] '%s' cities=%s — evaluating %d candidates: %s",
        clinic_name,
        cities,
        len(serp_results),
        candidate_domains,
    )

    try:
        resp = await client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.1,
            messages=[
                {"role": "system", "content": DOMAIN_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        answer = resp.choices[0].message.content.strip()
        if answer.upper() == "NULL" or not answer:
            logger.info(
                "[domain] '%s' — REJECTED all %d candidates (LLM said NULL)",
                clinic_name,
                len(serp_results),
            )
            return None
        # Strip any accidental protocol/path from the answer
        domain = answer.replace("https://", "").replace("http://", "").strip("/").strip()
        logger.info(
            "[domain] '%s' — ACCEPTED '%s' from candidates %s",
            clinic_name,
            domain,
            candidate_domains,
        )
        return domain
    except Exception as exc:
        logger.error("[domain] '%s' — LLM call failed: %s", clinic_name, exc)
        return None


# ── LinkedIn categorisation ──────────────────────────────────────────────

LINKEDIN_CATEGORIZE_SYSTEM_PROMPT = (
    "You are an expert data researcher. Your task is to identify the official LinkedIn Company Page "
    "for a specific MEDICAL CLINIC from the provided Google search results.\n\n"
    "CATEGORIZATION RULES:\n"
    "- YES:  Only if clinic_name matches result title super close. Cues from description alone are "
    "not enough, the title must match!\n"
    "- MAYBE: The result is a LinkedIn page for a parent group/owner mentioned in the description, "
    "or a partial name match that requires human verification. If not sure lean towards MAYBE over YES/NO\n"
    "- NO: The result is an unrelated company\n\n"
    "INSTRUCTIONS:\n"
    "1. Compare the company name/domain and result title/description\n"
    "2. If no exact match for the clinic is found, look for the group/parent company name in the "
    "snippets and find the best match for that group.\n"
    "3. Prioritize the main profile of the group/parent company if the specific clinic page doesn't exist.\n\n"
    "Title is main signal, use description only to match by domain or parent company\n\n"
    "OUTPUT FORMAT - list of ALL input id's with their status. example:\n"
    "first_id: [STATUS]\n"
    "....\n"
    "last_id: [STATUS]\n"
    "YOU CAN'T ADD ANY OTHER TEXT OR EXPLANATIONS, if no id's empty output"
)


def _build_linkedin_categorize_prompt(
    clinic_name: str,
    website_domain: str,
    serp_results: list[SerpResult],
) -> str:
    """Build the user message for LinkedIn categorisation."""
    results_str = "\n".join(
        f"{i}: {r.title}: {r.description}"
        for i, r in enumerate(serp_results)
    )
    return (
        f"TARGET: {clinic_name} - domain {website_domain}\n\n"
        f"RESULTS:\n{results_str}"
    )


def _parse_categorization(text: str, result_count: int) -> list[tuple[int, str]]:
    """Parse LLM output like '0: [YES]\n1: [NO]' into (index, status) pairs."""
    pairs: list[tuple[int, str]] = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # Accept formats like "0: [YES]", "0: YES", "0 - YES"
        for status in ("YES", "MAYBE", "NO"):
            if status in line.upper():
                # Extract the leading number
                parts = line.split(":", 1) if ":" in line else line.split("-", 1)
                try:
                    idx = int(parts[0].strip())
                    if 0 <= idx < result_count:
                        pairs.append((idx, status.lower()))
                except (ValueError, IndexError):
                    pass
                break
    return pairs


async def categorize_linkedin_results(
    clinic_name: str,
    website_domain: str,
    serp_results: list[SerpResult],
) -> list[tuple[int, str]]:
    """Ask LLM to categorise each SERP result as yes/maybe/no for LinkedIn match."""
    if not serp_results:
        return []

    client = _get_client()
    user_msg = _build_linkedin_categorize_prompt(clinic_name, website_domain, serp_results)

    try:
        resp = await client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.1,
            messages=[
                {"role": "system", "content": LINKEDIN_CATEGORIZE_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        answer = resp.choices[0].message.content.strip()
        pairs = _parse_categorization(answer, len(serp_results))
        logger.info("LLM LinkedIn categorise for '%s': %s", clinic_name, pairs)
        return pairs
    except Exception as exc:
        logger.error("LLM LinkedIn categorisation failed for '%s': %s", clinic_name, exc)
        return []


# ── LinkedIn profile validation (second pass for MAYBEs) ────────────────

LINKEDIN_VALIDATE_SYSTEM_PROMPT = (
    "Determine if linkedin company profile belongs to the target medical clinic.\n\n"
    "OUTPUT ONLY YES OR NO. No extra text allowed!"
)


async def validate_linkedin_profile(
    clinic_name: str,
    profile_data: dict,
) -> bool:
    """Ask LLM whether a scraped LinkedIn profile matches the target clinic."""
    client = _get_client()
    user_msg = (
        f"TARGET: {clinic_name}\n\n"
        f"PROFILE:\n"
        f"- tagline: {profile_data.get('tagline', '')}\n"
        f"- name: {profile_data.get('name', '')}\n"
        f"- website: {profile_data.get('website', '')}\n"
        f"- description: {profile_data.get('description', '')}"
    )

    try:
        resp = await client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.1,
            messages=[
                {"role": "system", "content": LINKEDIN_VALIDATE_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        answer = resp.choices[0].message.content.strip().upper()
        is_match = answer == "YES"
        logger.info("LLM LinkedIn validate '%s' → %s", clinic_name, answer)
        return is_match
    except Exception as exc:
        logger.error("LLM LinkedIn validation failed for '%s': %s", clinic_name, exc)
        return False
