"""Pure HTML/JSON parsing functions — no I/O, only data extraction."""

import json
import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup


@dataclass
class ClinicStub:
    """Minimal clinic data extracted from a search result page."""

    name: str
    zl_url: str
    specializations_text: str
    zl_profile_id: str | None = None


@dataclass
class LocationData:
    """A single clinic address with optional coordinates and social links."""

    address: str
    latitude: float | None = None
    longitude: float | None = None
    facebook_url: str | None = None
    instagram_url: str | None = None
    youtube_url: str | None = None
    linkedin_url: str | None = None
    website_url: str | None = None


@dataclass
class ProfileData:
    """Enriched clinic data extracted from its profile page."""

    zl_profile_id: str | None = None
    nip: str | None = None
    legal_name: str | None = None
    description: str | None = None
    zl_reviews_cnt: int | None = None
    locations: list[LocationData] = field(default_factory=list)


@dataclass
class DoctorData:
    """A doctor record from the facility doctors API."""

    id: int | None = None
    name: str | None = None
    surname: str | None = None
    zl_url: str | None = None


def _extract_profile_id_from_card(element) -> str | None:
    """Walk up the DOM from a search-result link looking for a profile-ID data attribute."""
    # Check the element itself first
    for attr in ("data-id", "data-eec-entity-id", "data-entity-id", "data-doctor-id"):
        val = element.get(attr)
        if val:
            return str(val)

    # Walk up ancestors looking for data-id / data-eec-entity-id
    for attr in ("data-id", "data-eec-entity-id"):
        parent = element.find_parent(attrs={attr: True})
        if parent:
            return str(parent[attr])

    return None


def parse_search_page(html: str) -> list[ClinicStub]:
    """Extract clinic stubs (name, URL, specializations, profile ID) from a search results page."""
    soup = BeautifulSoup(html, "lxml")
    stubs: list[ClinicStub] = []

    name_elements = soup.select("h3.h4.mb-0 a.text-body span")
    url_elements = soup.select("h3.h4.mb-0 a.text-body[href]")
    spec_elements = soup.select('span[data-test-id="doctor-specializations"]')

    count = min(len(name_elements), len(url_elements))
    for i in range(count):
        name = name_elements[i].get_text(strip=True)
        href = url_elements[i].get("href", "")
        if isinstance(href, list):
            href = href[0]
        if href.startswith("/"):
            href = f"https://www.znanylekarz.pl{href}"
        specs = spec_elements[i].get_text(strip=True) if i < len(spec_elements) else ""
        profile_id = _extract_profile_id_from_card(url_elements[i])

        if name and href:
            stubs.append(ClinicStub(name=name, zl_url=href, specializations_text=specs, zl_profile_id=profile_id))

    return stubs


def parse_total_pages(html: str) -> int:
    """Extract the last page number from pagination controls on the search page."""
    soup = BeautifulSoup(html, "lxml")

    # Try to find pagination links
    pagination_links = soup.select("ul.pagination li a")
    if pagination_links:
        max_page = 1
        for link in pagination_links:
            href = link.get("href", "")
            if isinstance(href, list):
                href = href[0]
            match = re.search(r"page=(\d+)", str(href))
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
            # Also check text content for page numbers
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
        return max_page

    return 1


def parse_profile_page(html: str) -> ProfileData:
    """Extract addresses, profile_id, coordinates, reviews, description, NIP, legal_name."""
    soup = BeautifulSoup(html, "lxml")
    data = ProfileData()

    # Profile ID from data-eec-entity-id
    profile_el = soup.select_one("#facility-basic-profile, #facility-premium-profile")
    if profile_el:
        data.zl_profile_id = profile_el.get("data-eec-entity-id")

    # Iterate over each address tab-pane within the contact section
    tab_panes = soup.select("#contact-section .tab-content .tab-pane")
    for pane in tab_panes:
        # Address (separate element from facility name)
        addr_el = pane.select_one('[data-test-id="contact-facility-address"]')
        address = addr_el.get_text(strip=True) if addr_el else None

        # Coordinates from map link inside this pane
        map_el = pane.select_one('a.map-placeholder[href]')
        lat, lng = None, None
        if map_el:
            href = map_el.get("href", "")
            if isinstance(href, list):
                href = href[0]
            lat, lng = parse_coordinates(str(href))

        # Social / website links from the modal that belongs to this pane
        pane_id = pane.get("id", "")
        modal_idx = pane_id.replace("tab-address-", "") if pane_id.startswith("tab-address-") else ""
        modal = soup.select_one(f'[data-id="facility-contact-modal-{modal_idx}"]') if modal_idx else None

        social: dict[str, str | None] = {
            "facebook_url": None,
            "instagram_url": None,
            "youtube_url": None,
            "linkedin_url": None,
            "website_url": None,
        }
        links_section = modal.select_one('[data-test-id="contact-modal-links-section"]') if modal else None
        if links_section:
            for a_tag in links_section.select("a[href]"):
                link_href = a_tag.get("href", "")
                if isinstance(link_href, list):
                    link_href = link_href[0]
                field_name, url = _classify_link(str(link_href))
                if field_name and social.get(field_name) is None:
                    social[field_name] = url

        if address:
            data.locations.append(
                LocationData(
                    address=address,
                    latitude=lat,
                    longitude=lng,
                    **social,
                )
            )

    # Reviews count
    reviews_el = soup.select_one("#facility-opinion-stats h2.h3")
    if reviews_el:
        reviews_text = re.sub(r"\D", "", reviews_el.get_text())
        data.zl_reviews_cnt = int(reviews_text) if reviews_text else None

    # Description
    desc_el = soup.select_one("div.about-description.about-content")
    if desc_el:
        data.description = desc_el.get_text(strip=True)

    # NIP
    nip_el = soup.select_one('div[data-id="facility-about-us-details"] span[data-test-id="fiscal-number"]')
    if nip_el:
        data.nip = nip_el.get_text(strip=True)

    # Legal name
    legal_el = soup.select_one('div[data-id="facility-about-us-details"] span[data-test-id="fiscal-name"]')
    if legal_el:
        data.legal_name = legal_el.get_text(strip=True)

    return data


# Domains to skip when classifying links (not social / not website)
_SKIP_DOMAINS = {"znanylekarz.pl", "docplanner.com", "hiredoc.com", "google.com", "apple.com"}

# Base-domain -> field name mapping for social networks
_SOCIAL_DOMAIN_MAP: dict[str, str] = {
    "facebook.com": "facebook_url",
    "instagram.com": "instagram_url",
    "youtube.com": "youtube_url",
    "linkedin.com": "linkedin_url",
}


def _base_domain(hostname: str) -> str:
    """Return the last two parts of a hostname (handles country sub-domains like pl.linkedin.com)."""
    parts = hostname.rstrip(".").split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else hostname


def _classify_link(url: str) -> tuple[str | None, str]:
    """Return (field_name, url) for a recognised social/website link, or (None, url) to skip."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    base = _base_domain(host)

    # Skip internal / irrelevant domains
    if base in _SKIP_DOMAINS:
        return None, url

    # Check known social domains
    field = _SOCIAL_DOMAIN_MAP.get(base)
    if field:
        return field, url

    # Everything else is a generic website
    return "website_url", url


def parse_coordinates(maps_url: str) -> tuple[float | None, float | None]:
    """Regex-extract lat/lng from a Google Maps URL."""
    match = re.search(r"query=(-?\d+\.\d+),(-?\d+\.\d+)", maps_url)
    if match:
        return float(match.group(1)), float(match.group(2))
    return None, None


def parse_doctors_response(json_text: str) -> list[DoctorData]:
    """Parse the doctors JSON array, return a list of DoctorData."""
    try:
        data = json.loads(json_text)
        if not isinstance(data, list):
            return []
        doctors: list[DoctorData] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            doc_id = item.get("id")
            if doc_id is None:
                continue
            name = item.get("name") or None
            surname = item.get("surname") or None
            zl_url = item.get("url") or None
            if zl_url and zl_url.startswith("/"):
                zl_url = f"https://www.znanylekarz.pl{zl_url}"
            doctors.append(DoctorData(
                id=int(doc_id),
                name=name,
                surname=surname,
                zl_url=zl_url,
            ))
        return doctors
    except (json.JSONDecodeError, TypeError, ValueError):
        return []
