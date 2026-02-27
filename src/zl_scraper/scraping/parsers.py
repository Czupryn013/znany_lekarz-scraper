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


@dataclass
class LocationData:
    """A single clinic address with optional coordinates."""

    address: str
    latitude: float | None = None
    longitude: float | None = None


@dataclass
class ProfileData:
    """Enriched clinic data extracted from its profile page."""

    zl_profile_id: str | None = None
    nip: str | None = None
    legal_name: str | None = None
    description: str | None = None
    zl_reviews_cnt: int | None = None
    locations: list[LocationData] = field(default_factory=list)


def parse_search_page(html: str) -> list[ClinicStub]:
    """Extract clinic stubs (name, URL, specializations) from a search results page."""
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

        if name and href:
            stubs.append(ClinicStub(name=name, zl_url=href, specializations_text=specs))

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

    # Addresses
    address_elements = soup.select(".tab-content .tab-pane div.d-flex div.d-flex div div div.mr-1")
    addresses = [el.get_text(strip=True) for el in address_elements if el.get_text(strip=True)]

    # Coordinates from Google Maps links
    coord_elements = soup.select("div.mb-2.mb-md-0 a.map-placeholder")
    coords: list[tuple[float | None, float | None]] = []
    for el in coord_elements:
        href = el.get("href", "")
        if isinstance(href, list):
            href = href[0]
        lat, lng = parse_coordinates(str(href))
        coords.append((lat, lng))

    # Build locations
    for i, addr in enumerate(addresses):
        lat, lng = coords[i] if i < len(coords) else (None, None)
        data.locations.append(LocationData(address=addr, latitude=lat, longitude=lng))

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


def parse_coordinates(maps_url: str) -> tuple[float | None, float | None]:
    """Regex-extract lat/lng from a Google Maps URL."""
    match = re.search(r"query=(-?\d+\.\d+),(-?\d+\.\d+)", maps_url)
    if match:
        return float(match.group(1)), float(match.group(2))
    return None, None


def parse_doctors_response(json_text: str) -> int:
    """Parse the doctors JSON array, return the count."""
    try:
        data = json.loads(json_text)
        if isinstance(data, list):
            return len(data)
        return 0
    except (json.JSONDecodeError, TypeError):
        return 0
