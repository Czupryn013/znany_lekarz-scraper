"""
KRS (Krajowy Rejestr Sądowy) scraper module
"""
from playwright.sync_api import Page
from urllib.parse import unquote
import time
from typing import Optional
from dataclasses import dataclass


@dataclass
class KRSResult:
    """Result from KRS scraping"""
    found: bool
    company_name: Optional[str] = None
    krs_code: Optional[str] = None
    krs_number: Optional[str] = None
    regon: Optional[str] = None
    registration_date: Optional[str] = None
    apikey: Optional[str] = None
    register_type: Optional[str] = None  # "P" or "S" from ;typ= in URL
    url: Optional[str] = None


def scrape_krs(page: Page, nip: str) -> KRSResult:
    """
    Scrape KRS data for a single NIP number.
    
    Args:
        page: Playwright page object (should already be on KRS search page)
        nip: NIP number to search
    
    Returns:
        KRSResult with scraped data or found=False if not found
    """
    print(f"  🔍 Searching KRS for NIP: {nip}")
    
    try:
        # Find the NIP input field
        nip_input = page.locator('ds-input[label="NIP"] input')
        
        # Clear and fill the NIP input
        nip_input.scroll_into_view_if_needed()
        nip_input.clear()
        nip_input.fill(nip)
        print(f"    ✓ Entered NIP: {nip}")
        
        # Click search button
        search_button = page.locator('.ds-panel-footer > div > div:nth-of-type(2) button')
        search_button.scroll_into_view_if_needed()
        search_button.click()
        print("    ✓ Clicked search button")
        
        # Wait for network to be idle (search results to load)
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        
        # Check if "Brak danych" (no data) message appears
        try:
            no_data = page.locator('text="Brak danych"').first
            if no_data.is_visible(timeout=2000):
                print(f"    ℹ No data found in KRS for NIP: {nip}")
                return KRSResult(found=False)
        except:
            pass
        
        # Find all result rows
        rows = page.locator('tbody tr.table-row.ng-star-inserted').all()
        
        if len(rows) == 0:
            print(f"    ℹ No results found in KRS for NIP: {nip}")
            return KRSResult(found=False)
        
        print(f"    ✓ Found {len(rows)} result row(s) in KRS")
        
        # Process first valid (non-deleted) row
        for idx, row in enumerate(rows, 1):
            result = _process_krs_row(page, row, idx, len(rows))
            if result and result.found:
                return result
        
        # All rows were deleted entities
        print(f"    ℹ All KRS results are deleted entities")
        return KRSResult(found=False)
        
    except Exception as e:
        print(f"    ✗ Error searching KRS: {str(e)}")
        return KRSResult(found=False)


def _process_krs_row(page: Page, row, idx: int, total: int) -> Optional[KRSResult]:
    """
    Process a single KRS search result row.
    
    Args:
        page: Playwright page object
        row: Row locator
        idx: Row index (1-based)
        total: Total number of rows
    
    Returns:
        KRSResult or None if row should be skipped
    """
    try:
        # Set up request listener to capture API key
        captured_apikey = None
        
        def capture_apikey(request):
            nonlocal captured_apikey
            if 'wyszukiwarka-krs-api.ms.gov.pl/api/wyszukiwarka/danepodmiotu' in request.url:
                captured_apikey = request.headers.get('apikey')
        
        page.on("request", capture_apikey)
        
        # Find the button in the row
        button = row.locator('text="Wyświetl szczegóły"').first
        
        print(f"      → Processing KRS row {idx}/{total}...")
        
        # Click the button to go to details page
        button.scroll_into_view_if_needed()
        button.click()
        
        # Wait for navigation
        page.wait_for_load_state("networkidle")
        time.sleep(0.5)
        
        # Check if entity is deleted from KRS
        try:
            deleted_header = page.locator('h4:has-text("PODMIOT WYKREŚLONY Z KRS")').first
            if deleted_header.is_visible(timeout=2000):
                print(f"        ⚠ Entity deleted from KRS - skipping row {idx}")
                page.remove_listener("request", capture_apikey)
                page.go_back()
                page.wait_for_load_state("networkidle")
                time.sleep(0.5)
                return None
        except:
            pass
        
        # Get the details page URL
        details_url = page.url
        print(f"        ✓ Details page: {details_url}")
        
        # Extract numerKRS from URL
        numer_krs = _extract_numer_krs_from_url(details_url)
        if numer_krs:
            print(f"        ✓ Numer KRS (encoded): {numer_krs}")
        
        # Extract details from page
        nazwa = _extract_field(page, "Nazwa")
        krs_number = _extract_field(page, "Numer KRS")
        regon = _extract_field(page, "REGON")
        registration_date = _extract_field(page, "Data wpisu do Rejestru Przedsiębiorców")
        
        if nazwa:
            print(f"        ✓ Nazwa: {nazwa}")
        if krs_number:
            print(f"        ✓ KRS Number: {krs_number}")
        if regon:
            print(f"        ✓ REGON: {regon}")
        if registration_date:
            print(f"        ✓ Registration Date: {registration_date}")
        if captured_apikey:
            print(f"        ✓ API Key captured: {captured_apikey}")
        
        # Remove the listener
        page.remove_listener("request", capture_apikey)
        
        # Go back to search page
        page.go_back()
        page.wait_for_load_state("networkidle")
        time.sleep(0.5)
        
        # Extract register type (P or S) from URL
        register_type = _extract_typ_from_url(details_url)

        return KRSResult(
            found=True,
            company_name=nazwa,
            krs_code=numer_krs,
            krs_number=krs_number,
            regon=regon,
            registration_date=registration_date,
            apikey=captured_apikey,
            register_type=register_type,
            url=details_url
        )
        
    except Exception as e:
        print(f"        ✗ Error processing KRS row {idx}: {str(e)}")
        try:
            page.remove_listener("request", capture_apikey)
        except:
            pass
        return None


def _extract_typ_from_url(url: str) -> Optional[str]:
    """Extract register type (P or S) from ;typ=P or ;typ=S in URL."""
    if 'typ=' in url:
        parts = url.split(';')
        for part in parts:
            if part.startswith('typ='):
                return part.split('=', 1)[1]
    return None


def _extract_numer_krs_from_url(url: str) -> Optional[str]:
    """Extract and decode numerKRS from URL"""
    if 'numerKRS=' in url:
        parts = url.split(';')
        for part in parts:
            if part.startswith('numerKRS='):
                encoded_krs = part.split('=', 1)[1]
                return unquote(encoded_krs)
    return None


def _extract_field(page: Page, label: str) -> Optional[str]:
    """
    Extract a field value from KRS details page.
    
    Args:
        page: Playwright page object
        label: The label text to look for
    
    Returns:
        Field value or None
    """
    try:
        label_elem = page.locator(f'div.title--bg-color:has-text("{label}")').first
        if label_elem.is_visible(timeout=2000):
            value_div = page.locator(f'div.title--bg-color:has-text("{label}") + div').first
            value = value_div.inner_text().strip()
            if value == "-":
                return None
            return value
    except:
        pass
    return None


def navigate_to_krs(page: Page) -> None:
    """
    Navigate to KRS search page.
    
    Args:
        page: Playwright page object
    """
    from .utils import KRS_SEARCH_URL
    print("  📍 Navigating to KRS search page...")
    page.goto(KRS_SEARCH_URL)
    page.wait_for_load_state("networkidle")
