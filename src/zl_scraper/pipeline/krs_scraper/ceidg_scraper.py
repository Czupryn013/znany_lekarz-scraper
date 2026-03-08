"""
CEIDG (Centralna Ewidencja i Informacja o Działalności Gospodarczej) scraper module
Handles both JDG (jednoosobowa działalność gospodarcza) and Spółka Cywilna searches
"""
from playwright.sync_api import Page, BrowserContext
import time
from typing import Optional, List
from dataclasses import dataclass, field
from .utils import extract_email, clean_phone, clean_text, CEIDG_SEARCH_URL


@dataclass
class Owner:
    """Owner information"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    regon: Optional[str] = None


@dataclass 
class CEIDGResult:
    """Result from CEIDG scraping"""
    found: bool
    source: Optional[str] = None  # 'JDG' or 'SC' (Spółka Cywilna)
    legal_name: Optional[str] = None
    regon: Optional[str] = None
    registered_at: Optional[str] = None
    owners: List[Owner] = field(default_factory=list)


def scrape_ceidg(context: BrowserContext, page: Page, nip: str) -> CEIDGResult:
    """
    Scrape CEIDG data for a single NIP number.
    First tries JDG search, then falls back to Spółka Cywilna search.
    
    Args:
        context: Browser context for opening new tabs
        page: Playwright page object
        nip: NIP number to search
    
    Returns:
        CEIDGResult with scraped data or found=False if not found
    """
    print(f"  🔍 Searching CEIDG for NIP: {nip}")
    
    # Navigate to CEIDG search page
    navigate_to_ceidg(page)
    
    # First try JDG search
    result = _search_jdg(context, page, nip)
    if result.found:
        return result
    
    # If not found, try Spółka Cywilna search
    print(f"    ℹ JDG not found, trying Spółka Cywilna...")
    result = _search_spolka_cywilna(context, page, nip)
    
    return result


def navigate_to_ceidg(page: Page) -> None:
    """
    Navigate to CEIDG search page.
    
    Args:
        page: Playwright page object
    """
    print("    📍 Navigating to CEIDG search page...")
    page.goto(CEIDG_SEARCH_URL)
    page.wait_for_load_state("networkidle")
    time.sleep(1)


def _search_jdg(context: BrowserContext, page: Page, nip: str) -> CEIDGResult:
    """
    Search for JDG (jednoosobowa działalność gospodarcza) by NIP.
    
    Args:
        context: Browser context for opening new tabs
        page: Playwright page object
        nip: NIP number to search
    
    Returns:
        CEIDGResult with JDG data or found=False
    """
    print(f"    🔍 Searching JDG by NIP: {nip}")
    
    try:
        # Clear any previous search
        _clear_ceidg_search(page)
        
        # Fill JDG NIP input - use type() for input mask compatibility
        nip_input = page.locator('input#MainContentForm_txtNip')
        nip_input.scroll_into_view_if_needed()
        nip_input.click()  # Focus the input first
        nip_input.press('Control+a')  # Select all existing content
        nip_input.type(nip, delay=50)  # Type slowly to work with input mask
        print(f"      ✓ Entered NIP in JDG field")
        
        # Click search button (it's an input[type=submit], not a button)
        search_button = page.locator('input#MainContentForm_btnInputSearch')
        search_button.scroll_into_view_if_needed()
        search_button.click()
        print(f"      ✓ Clicked search button")
        
        # Wait for results
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        
        # Check if no results
        if _check_no_results(page):
            print(f"      ℹ No JDG found for NIP: {nip}")
            return CEIDGResult(found=False)
        
        # Extract JDG data
        return _extract_jdg_data(context, page)
        
    except Exception as e:
        print(f"      ✗ Error searching JDG: {str(e)}")
        return CEIDGResult(found=False)


def _search_spolka_cywilna(context: BrowserContext, page: Page, nip: str) -> CEIDGResult:
    """
    Search for Spółka Cywilna by NIP.
    
    Args:
        context: Browser context for opening new tabs
        page: Playwright page object
        nip: NIP number to search
    
    Returns:
        CEIDGResult with Spółka Cywilna data or found=False
    """
    print(f"    🔍 Searching Spółka Cywilna by NIP: {nip}")
    
    try:
        # Clear any previous search
        _clear_ceidg_search(page)
        
        # Fill Spółka Cywilna NIP input
        nip_input = page.locator('input#MainContentForm_txtPartnershipNIP')
        nip_input.scroll_into_view_if_needed()
        nip_input.click()  # Focus the input first
        nip_input.press('Control+a')  # Select all existing content
        nip_input.type(nip, delay=50)  # Type slowly for consistency
        print(f"      ✓ Entered NIP in Spółka Cywilna field")
        
        # Click search button (it's an input[type=submit], not a button)
        search_button = page.locator('input#MainContentForm_btnInputSearch')
        search_button.scroll_into_view_if_needed()
        search_button.click()
        print(f"      ✓ Clicked search button")
        
        # Wait for results
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        
        # Check if no results
        if _check_no_results(page):
            print(f"      ℹ No Spółka Cywilna found for NIP: {nip}")
            return CEIDGResult(found=False)
        
        # Extract Spółka Cywilna data
        return _extract_spolka_cywilna_data(context, page)
        
    except Exception as e:
        print(f"      ✗ Error searching Spółka Cywilna: {str(e)}")
        return CEIDGResult(found=False)


def _clear_ceidg_search(page: Page) -> None:
    """Clear all CEIDG search inputs"""
    try:
        # Clear JDG NIP
        jdg_input = page.locator('input#MainContentForm_txtNip')
        if jdg_input.is_visible(timeout=1000):
            jdg_input.clear()
    except:
        pass
    
    try:
        # Clear Spółka Cywilna NIP
        sc_input = page.locator('input#MainContentForm_txtPartnershipNIP')
        if sc_input.is_visible(timeout=1000):
            sc_input.clear()
    except:
        pass


def _check_no_results(page: Page) -> bool:
    """
    Check if CEIDG search returned no results.
    
    Returns:
        True if no results found, False otherwise
    """
    try:
        no_result_span = page.locator('span#MainContentForm_lblNoResult')
        if no_result_span.is_visible(timeout=2000):
            text = no_result_span.inner_text()
            if "Brak wpisów spełniających podane kryteria" in text:
                return True
    except:
        pass
    return False


def _extract_jdg_data(context: BrowserContext, page: Page) -> CEIDGResult:
    """
    Extract JDG data from search results.
    
    Args:
        context: Browser context for opening new tabs
        page: Playwright page object
    
    Returns:
        CEIDGResult with JDG data
    """
    print(f"      ✓ JDG result found, extracting data...")
    
    try:
        # Extract basic info from search results
        legal_name = _get_element_text(page, 'span#MainContentForm_DataListEntities_spanName_0')
        first_name = _get_element_text(page, 'span#MainContentForm_DataListEntities_spanFirstName_0')
        last_name = _get_element_text(page, 'span#MainContentForm_DataListEntities_spanLastName_0')
        regon = _get_element_text(page, 'dd#MainContentForm_DataListEntities_spanRegonValue_0')
        
        if legal_name:
            print(f"        ✓ Legal name: {legal_name}")
        if first_name or last_name:
            print(f"        ✓ Owner: {first_name} {last_name}")
        if regon:
            print(f"        ✓ REGON: {regon}")
        
        # Get details from detail page
        details = _extract_details_from_page(context, page, 0)
        
        owner = Owner(
            first_name=first_name,
            last_name=last_name,
            full_name=f"{first_name} {last_name}".strip() if first_name or last_name else None,
            email=details.get('email'),
            phone=details.get('phone'),
            regon=regon
        )
        
        return CEIDGResult(
            found=True,
            source='JDG',
            legal_name=legal_name,
            regon=regon,
            registered_at=details.get('registered_at'),
            owners=[owner]
        )
        
    except Exception as e:
        print(f"        ✗ Error extracting JDG data: {str(e)}")
        return CEIDGResult(found=False)


def _extract_spolka_cywilna_data(context: BrowserContext, page: Page) -> CEIDGResult:
    """
    Extract Spółka Cywilna data from search results.
    For SC, we get owner names, email, phone, and REGON.
    
    Args:
        context: Browser context for opening new tabs
        page: Playwright page object
    
    Returns:
        CEIDGResult with Spółka Cywilna data
    """
    print(f"      ✓ Spółka Cywilna result found, extracting data...")
    
    try:
        # Count number of owners by counting rows in the results table
        rows = page.locator('table#MainContentForm_DataListEntities tbody tr').all()
        owner_count = len(rows)
        print(f"        ✓ Found {owner_count} owner(s)")
        
        owners = []
        
        for i in range(owner_count):
            first_name = _get_element_text(page, f'span#MainContentForm_DataListEntities_spanFirstName_{i}')
            last_name = _get_element_text(page, f'span#MainContentForm_DataListEntities_spanLastName_{i}')
            regon = _get_element_text(page, f'dd#MainContentForm_DataListEntities_spanRegonValue_{i}')
            
            if first_name or last_name:
                print(f"        ✓ Owner {i+1}: {first_name} {last_name}")
                if regon:
                    print(f"          REGON: {regon}")
                
                # Get details from detail page for this owner
                details = _extract_details_from_page(context, page, i)
                
                owner = Owner(
                    first_name=first_name,
                    last_name=last_name,
                    full_name=f"{first_name} {last_name}".strip() if first_name or last_name else None,
                    email=details.get('email'),
                    phone=details.get('phone'),
                    regon=regon
                )
                owners.append(owner)
        
        return CEIDGResult(
            found=True,
            source='SC',
            legal_name=None,  # Not applicable for SC
            regon=None,  # Individual REGONs are stored per owner
            registered_at=None,  # Not applicable for SC
            owners=owners
        )
        
    except Exception as e:
        print(f"        ✗ Error extracting Spółka Cywilna data: {str(e)}")
        return CEIDGResult(found=False)


def _extract_details_from_page(context: BrowserContext, page: Page, index: int) -> dict:
    """
    Open details page in new tab and extract additional info.
    
    Args:
        context: Browser context for opening new tabs
        page: Playwright page object
        index: Index of the result row (0-based)
    
    Returns:
        Dict with 'registered_at', 'email', 'phone'
    """
    details = {
        'registered_at': None,
        'email': None,
        'phone': None
    }
    
    try:
        # Get the details link href
        link = page.locator(f'a#MainContentForm_DataListEntities_hrefDetails_{index}')
        if not link.is_visible(timeout=2000):
            print(f"          ℹ Details link not found for index {index}")
            return details
        
        link.scroll_into_view_if_needed()
        href = link.get_attribute('href')
        print(f"          → Details link href: {href}")
        
        if not href or href == '#' or href.startswith('javascript:'):
            print(f"          ℹ Invalid href, skipping details extraction")
            return details
        
        href = f"https://aplikacja.ceidg.gov.pl/ceidg/ceidg.public.ui/{href}"
        
        print(f"          → Opening details page: {href}")
        
        # Open in new tab
        detail_page = context.new_page()
        detail_page.goto(href)
        detail_page.wait_for_load_state("networkidle")
        time.sleep(0.5)
        
        try:
            # Extract registration date
            date_span = detail_page.locator('span#MainContentForm_lblDateOfCommencementOfBusiness')
            if date_span.is_visible(timeout=2000):
                date_text = date_span.inner_text().strip()
                if date_text and date_text != "-":
                    details['registered_at'] = date_text
                    print(f"          ✓ Registered at: {date_text}")
            
            # Extract email
            email_elem = detail_page.locator('span#MainContentForm_lblEmail a')
            if email_elem.is_visible(timeout=2000):
                email_text = email_elem.inner_text().strip()
                extracted_email = extract_email(email_text)
                if extracted_email:
                    details['email'] = extracted_email
                    print(f"          ✓ Email: {extracted_email}")
            
            # Extract phone
            phone_elem = detail_page.locator('dd#MainContentForm_divPhone1')
            if phone_elem.is_visible(timeout=2000):
                phone_text = phone_elem.inner_text().strip()
                cleaned_phone = clean_phone(phone_text)
                if cleaned_phone:
                    details['phone'] = cleaned_phone
                    print(f"          ✓ Phone: {cleaned_phone}")
                    
        finally:
            # Close the detail page
            detail_page.close()
        
    except Exception as e:
        print(f"          ⚠ Error extracting details: {str(e)}")
    
    return details


def _get_element_text(page: Page, selector: str) -> Optional[str]:
    """
    Get text from an element by selector.
    
    Args:
        page: Playwright page object
        selector: CSS selector
    
    Returns:
        Element text or None if not found
    """
    try:
        element = page.locator(selector)
        if element.is_visible(timeout=1000):
            text = element.inner_text().strip()
            return clean_text(text)
    except:
        pass
    return None
