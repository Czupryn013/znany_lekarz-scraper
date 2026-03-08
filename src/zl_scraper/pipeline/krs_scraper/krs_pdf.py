"""Fetch and parse KRS full-extract PDF to extract board members and prokurenci."""

import io
import logging
import re
from typing import Optional

import httpx
import pdfplumber

logger = logging.getLogger(__name__)

KRS_PDF_URL = "https://wyszukiwarka-krs-api.ms.gov.pl/api/wyszukiwarka/OdpisPelny/pdf"

_BROWSER_HEADERS = {
    "x-api-key": "TopSecretApiKey",
    "Sec-Ch-Ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-ch-ua-platform": '"Windows"',
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    ),
}


# ── PDF fetch ─────────────────────────────────────────────────────────────


def fetch_krs_pdf(apikey: str, search_token: str, *, register_type: str = "P") -> bytes:
    """Download the full KRS extract as PDF bytes.

    Args:
        apikey: API key captured from the KRS detail-page network request.
        search_token: The encoded KRS identifier (krs_code from scraper URL).
        register_type: Register type from the detail-page URL ("P" or "S").

    Returns:
        Raw PDF bytes.
    """
    headers = {**_BROWSER_HEADERS, "apikey": apikey}
    body = {"krs": search_token, "register": register_type, "format": "PDF"}

    logger.info("Fetching KRS PDF for search_token=%s apikey=%s", search_token, apikey)
    resp = httpx.post(KRS_PDF_URL, json=body, headers=headers, timeout=30, verify=False)
    resp.raise_for_status()
    logger.info("KRS PDF fetched, %d bytes", len(resp.content))
    return resp.content


# ── PDF text extraction ──────────────────────────────────────────────────


def extract_text_pages(pdf_bytes: bytes) -> list[str]:
    """Extract text from each page of a PDF.

    Returns:
        List of page texts (one string per page).
    """
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(text)
    return pages


# ── Parsing helpers ──────────────────────────────────────────────────────

_FOOTER_RE = re.compile(r"\n?Strona\s+\d+\s+z\d+\s*$", re.IGNORECASE)
_CLEANUP_PATTERNS = [
    (re.compile(r"\r"), ""),
    (re.compile(r"Strona\s+\d+\s+z\s*\d+", re.IGNORECASE), ""),
    (re.compile(r"[ \t]+"), " "),
    (re.compile(r"\n+"), "\n"),
]

# Polish uppercase letters for name matching
_PL_UPPER = r"A-ZĄĆĘŁŃÓŚŻŹ"


def _clean_text(text: str) -> str:
    """Strip page footers and normalise whitespace."""
    for pat, repl in _CLEANUP_PATTERNS:
        text = pat.sub(repl, text)
    return text.strip()


def _build_full_name(names: Optional[str], surname: Optional[str]) -> Optional[str]:
    """Build 'Firstname Surname' from parsed fields, title-cased."""
    if not names and not surname:
        return None
    first = names.split()[0] if names else ""
    sur = surname or ""
    full = f"{first} {sur}".strip()
    # title-case each part
    return " ".join(part.capitalize() for part in full.split())


def _extract_dzial2(pages: list[str]) -> str:
    """Join pages, strip footers, extract Dział 2 section."""
    joined = "\n".join(
        _FOOTER_RE.sub("", page) for page in pages
    )
    m = re.search(r"Dział 2([\s\S]*?)Dział 3", joined, re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_section(text: str, start: str, end: Optional[str]) -> Optional[str]:
    """Extract a sub-section between start and end regex markers."""
    if end:
        pat = re.compile(f"{start}([\\s\\S]*?){end}", re.IGNORECASE)
    else:
        pat = re.compile(f"{start}([\\s\\S]*)", re.IGNORECASE)
    m = pat.search(text)
    return m.group(1).strip() if m else None


def _extract_dzial1(pages: list[str]) -> str:
    """Join pages, strip footers, extract Dział 1 section."""
    joined = "\n".join(
        _FOOTER_RE.sub("", page) for page in pages
    )
    m = re.search(r"Dział 1([\s\S]*?)Dział 2", joined, re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_forma_prawna(dzial1: str) -> Optional[str]:
    """Detect the legal form (forma prawna) from Dział 1."""
    # Pattern 1: "1 - FORMA"
    m = re.search(r"1\.Oznaczenie formy prawnej\s+1\s+-\s+(.+?)(?=\n|$)", dzial1)
    if not m:
        # Pattern 2: "FORMA" without "1 -"
        m = re.search(r"1\.Oznaczenie formy prawnej\s+(.+?)(?=\n|$)", dzial1)
    return m.group(1).strip() if m else None


# ── Board member parsing (rubryka 1 — zarząd / wspólnicy) ───────────────


def _parse_rubryka1(text: str) -> list[dict]:
    """Parse board members / representatives from rubryka 1."""
    text = _clean_text(text)

    blocks = re.split(r"(?=\n?\d+\.Nazwisko\s*/\s*Nazwa\s*lub\s*Firma)", text)
    blocks = [b for b in blocks if b.strip()]

    is_wspolnicy = "Dane wspólników reprezentujących spółkę" in (blocks[0] if blocks else "")

    people: list[dict] = []
    for block in blocks:
        m_lp = re.match(r"^(\d+)", block.strip())
        if not m_lp:
            continue

        # Surname / company name
        m_sur = re.search(
            r"1\.Nazwisko / Nazwa lub Firma(?: [\d-]+){0,2} ([^\n]+)", block, re.IGNORECASE
        )
        surname_raw = m_sur.group(1).strip() if m_sur else None
        surname = "-".join(surname_raw.split()) if surname_raw else None

        # Given names
        m_names = re.search(
            rf"2\.Imiona(?: [\d-]+){{0,2}} ([{_PL_UPPER}\s-]+)", block, re.IGNORECASE
        )
        names = m_names.group(1).strip() if m_names else None

        # PESEL
        m_pesel = re.search(r"\b(\d{11})\b", block)
        pesel = m_pesel.group(1) if m_pesel else None

        # Role — take the latest (most recent) role entry
        role: Optional[str] = None
        start_date: Optional[str] = None
        end_date: Optional[str] = None

        section_m = re.search(
            r"Funkcja w organie(?:\s+reprezentującym)?([\s\S]*?)(?=\n\d+\.)", block, re.IGNORECASE
        )
        if section_m:
            role_re = re.compile(
                rf"(?P<start>\d+)\s*(?:-\s*)?(?P<end>\d+|-)?\s+(?P<role>[{_PL_UPPER}][{_PL_UPPER}\s-]+)",
                re.IGNORECASE,
            )
            roles = list(role_re.finditer(section_m.group(1)))
            if roles:
                last = roles[-1]
                start_date = last.group("start")
                end_date = last.group("end")
                role = last.group("role").strip()

        if is_wspolnicy:
            role = "WSPÓLNIK"

        # Filter out inactive (has end date that is not '-')
        if end_date and end_date != "-":
            continue

        # Skip entries without PESEL (e.g. corporate entities on the board)
        if not pesel:
            continue

        full_name = _build_full_name(names, surname)
        if not full_name:
            continue

        people.append({
            "full_name": full_name,
            "pesel": pesel,
            "role": role.upper() if role else None,
            "source": "KRS_BOARD",
        })

    return people


# ── Spółka partnerska — rubryka 1 (partners) ────────────────────────────


def _parse_rubryka1_partnerska(text: str) -> list[dict]:
    """Parse partners from spółka partnerska rubryka 1."""
    text = _clean_text(text)

    blocks = re.split(r"\n(?=\d+\s+1\.Nazwisko)", text)

    people: list[dict] = []
    for block in blocks:
        m_lp = re.match(r"^(\d+)", block.strip())
        if not m_lp:
            continue

        # Surname
        m_sur = re.search(
            r"1\.Nazwisko\s+(?:\d+\s*-\s*)?([^\n]+)", block, re.IGNORECASE
        )
        surname_raw = m_sur.group(1).strip() if m_sur else None
        surname = "-".join(surname_raw.split()) if surname_raw else None

        # Given names
        m_names = re.search(
            r"2\.Imiona\s+(?:\d+\s*-\s*)?([^\n]+)", block, re.IGNORECASE
        )
        names = m_names.group(1).strip() if m_names else None

        # PESEL
        m_pesel = re.search(r"\b(\d{11})\b", block)
        pesel = m_pesel.group(1) if m_pesel else None

        if not pesel:
            continue

        full_name = _build_full_name(names, surname)
        if not full_name:
            continue

        people.append({
            "full_name": full_name,
            "pesel": pesel,
            "role": "PARTNER",
            "source": "KRS_BOARD",
        })

    return people


# ── Spółka komandytowa — Dział 1 rubryka 7 (partners) ───────────────────


def _parse_rubryka7_komandytowa(text: str) -> list[dict]:
    """Parse partners from spółka komandytowa Dział 1 rubryka 7."""
    text = _clean_text(text)

    blocks = re.split(r"\n(?=\d+\s+1\.Nazwisko)", text)

    people: list[dict] = []
    for block in blocks:
        m_lp = re.match(r"^(\d+)", block.strip())
        if not m_lp:
            continue

        # Surname / company name
        m_sur = re.search(
            r"1\.Nazwisko / Nazwa lub firma\s+(?:\d+\s*-\s*)?([^\n]+)", block, re.IGNORECASE
        )
        surname_raw = m_sur.group(1).strip() if m_sur else None
        surname = "-".join(surname_raw.split()) if surname_raw else None

        # Given names
        m_names = re.search(r"2\.Imiona\s+(?:\d+\s*-\s*)?([^\n]+)", block, re.IGNORECASE)
        names = m_names.group(1).strip() if m_names else None

        # PESEL
        m_pesel = re.search(r"3\.Numer PESEL[^\n]*?(\d{11})", block, re.IGNORECASE)
        pesel = m_pesel.group(1) if m_pesel else None

        # Komandytariusz check
        m_kom = re.search(
            r"9\.Czy wspólnik jest komandytariuszem\?\s+(?:\d+\s*-\s*)?(TAK|NIE)", block, re.IGNORECASE
        )
        is_komandytariusz = m_kom.group(1).upper() if m_kom else None
        role = "KOMANDYTARIUSZ" if is_komandytariusz == "TAK" else "KOMPLEMENTARIUSZ"

        # Skip komplementariusz — typically a corporate entity, not a person
        if role == "KOMPLEMENTARIUSZ":
            continue

        if not pesel:
            continue

        full_name = _build_full_name(names, surname)
        if not full_name:
            continue

        people.append({
            "full_name": full_name,
            "pesel": pesel,
            "role": role,
            "source": "KRS_BOARD",
        })

    return people


# ── SPZOZ — single director ─────────────────────────────────────────────


def _parse_spzoz(text: str) -> list[dict]:
    """Parse single director from SAMODZIELNY PUBLICZNY ZAKŁAD OPIEKI ZDROWOTNEJ."""
    text = _clean_text(text)

    # Surname
    m_sur = re.search(r"2\.Nazwisko\s+([^\n]+)", text, re.IGNORECASE)
    surname_raw = m_sur.group(1).strip() if m_sur else None
    surname = "-".join(surname_raw.split()) if surname_raw else None

    # Given names
    m_names = re.search(r"3\.Imiona\s+([^\n]+)", text, re.IGNORECASE)
    names = m_names.group(1).strip() if m_names else None

    # PESEL
    m_pesel = re.search(r"4\.Numer PESEL[^\n]*?(\d{11})", text, re.IGNORECASE)
    pesel = m_pesel.group(1) if m_pesel else None

    # Role
    m_role = re.search(r"1\.Nazwa organu[^\n]*\n([^\n]+)", text, re.IGNORECASE)
    role = m_role.group(1).strip() if m_role else "KIEROWNIK"

    if not pesel:
        return []

    full_name = _build_full_name(names, surname)
    if not full_name:
        return []

    return [{
        "full_name": full_name,
        "pesel": pesel,
        "role": role.upper(),
        "source": "KRS_BOARD",
    }]


# ── Prokurenci parsing (rubryka 3) ───────────────────────────────────────


def _parse_rubryka3(text: str) -> list[dict]:
    """Parse prokurenci from rubryka 3."""
    text = _clean_text(text)

    blocks = re.split(r"\n(?=\d+\s*\n)", text)

    people: list[dict] = []
    for block in blocks:
        m_lp = re.match(r"^(\d+)", block.strip())
        if not m_lp:
            continue

        # Surname
        m_sur = re.search(
            r"1\.Nazwisko(?: [\d-]+){0,2} ([^\n]+)", block, re.IGNORECASE
        )
        surname_raw = m_sur.group(1).strip() if m_sur else None
        surname = "-".join(surname_raw.split()) if surname_raw else None

        # Given names
        m_names = re.search(
            rf"2\.Imiona(?: [\d-]+){{0,2}} ([{_PL_UPPER}\s-]+)", block, re.IGNORECASE
        )
        names = m_names.group(1).strip() if m_names else None

        # PESEL
        m_pesel = re.search(r"\b(\d{11})\b", block)
        pesel = m_pesel.group(1) if m_pesel else None

        # Prokura type
        m_prok = re.search(
            rf"4\.Rodzaj prokury\s*(?P<start>\d+)\s*(?:-\s*)?(?P<end>\d+|-)?\s+(?P<role>[{_PL_UPPER}][{_PL_UPPER}\s-]+)",
            block,
            re.IGNORECASE,
        )
        end_date = m_prok.group("end") if m_prok else None
        prokura = m_prok.group("role").strip() if m_prok else None

        # Filter out inactive
        if end_date and end_date != "-":
            continue

        # Skip entries without PESEL
        if not pesel:
            continue

        full_name = _build_full_name(names, surname)
        if not full_name:
            continue

        people.append({
            "full_name": full_name,
            "pesel": pesel,
            "role": prokura.upper() if prokura else "PROKURENT",
            "source": "KRS_PROKURA",
        })

    return people


# ── Public entry point ───────────────────────────────────────────────────


def parse_board_members(pages: list[str]) -> list[dict]:
    """Parse all board members and prokurenci from KRS PDF pages.

    Routes to the correct parser based on the detected legal form:
    - sp. z o.o. (default): rubryka 1 board + rubryka 3 prokurenci
    - SPÓŁKA PARTNERSKA:    rubryka 1 partners + rubryka 3 prokurenci
    - SPÓŁKA KOMANDYTOWA:   Dział 1 rubryka 7 partners + rubryka 3 prokurenci
    - SPZOZ:                single director from rubryka 1 + rubryka 3 prokurenci

    Returns:
        List of dicts with keys: full_name, pesel, role, source.
    """
    dzial1 = _extract_dzial1(pages)
    dzial2 = _extract_dzial2(pages)

    forma_prawna = _extract_forma_prawna(dzial1) if dzial1 else None
    logger.info("Detected forma prawna: %s", forma_prawna)

    if not dzial2:
        logger.warning("Dział 2 section not found in PDF")
        return []

    members: list[dict] = []

    # ── Board / representatives — route by legal form ──
    if forma_prawna == "SPÓŁKA KOMANDYTOWA":
        rubryka7 = _extract_section(dzial1, r"Dane wspólników", None)
        if rubryka7:
            partners = _parse_rubryka7_komandytowa(rubryka7)
            logger.info("Parsed %d partners from rubryka 7 (komandytowa)", len(partners))
            members.extend(partners)
        else:
            logger.info("Rubryka 7 (komandytowa) section not found or empty")

    elif forma_prawna == "SAMODZIELNY PUBLICZNY ZAKŁAD OPIEKI ZDROWOTNEJ":
        rubryka1_spzoz = _extract_section(dzial2, "Rubryka 1", "Rubryka 2")
        if rubryka1_spzoz:
            directors = _parse_spzoz(rubryka1_spzoz)
            logger.info("Parsed %d directors from SPZOZ", len(directors))
            members.extend(directors)
        else:
            logger.info("Rubryka 1 (SPZOZ) section not found or empty")

    elif forma_prawna == "SPÓŁKA PARTNERSKA":
        rubryka1 = _extract_section(dzial2, "Podrubryka 1", "Rubryka 2")
        if rubryka1:
            partners = _parse_rubryka1_partnerska(rubryka1)
            logger.info("Parsed %d partners from rubryka 1 (partnerska)", len(partners))
            members.extend(partners)
        else:
            logger.info("Rubryka 1 (partnerska) section not found or empty")

    else:
        # Default: sp. z o.o. and similar
        rubryka1 = _extract_section(dzial2, "Podrubryka 1", "Rubryka 2")
        if rubryka1:
            board = _parse_rubryka1(rubryka1)
            logger.info("Parsed %d board members from rubryka 1", len(board))
            members.extend(board)
        else:
            logger.info("Rubryka 1 (board) section not found or empty")

    # ── Prokurenci — always parsed regardless of legal form ──
    rubryka3 = _extract_section(dzial2, r"Rubryka 3[\s\S]*?Prokurenci", None)
    if rubryka3:
        proks = _parse_rubryka3(rubryka3)
        logger.info("Parsed %d prokurenci from rubryka 3", len(proks))
        members.extend(proks)
    else:
        logger.info("Rubryka 3 (prokurenci) section not found or empty")

    return members
