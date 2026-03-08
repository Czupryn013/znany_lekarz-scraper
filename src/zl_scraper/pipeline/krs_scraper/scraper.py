"""
KRS NIP Scraper - Main scraper orchestrator
Combines KRS and CEIDG scraping with fallback logic
"""
from playwright.sync_api import sync_playwright
import time
import json
import requests
from typing import List, Dict, Any, Optional

from .utils import KRS_SEARCH_URL, WEBHOOK_URL
from .krs_scraper import scrape_krs, navigate_to_krs, KRSResult
from .ceidg_scraper import scrape_ceidg, CEIDGResult, Owner


def read_nip_file(nip_file_path: str) -> List[Dict[str, Any]]:
    """
    Read NIP numbers from a text file.
    
    Args:
        nip_file_path: Path to text file containing NIP numbers (one per line)
    
    Returns:
        List of dicts with 'nip' and 'company_name' keys
    """
    nip_data = []
    with open(nip_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split('\t')
                nip = parts[0].strip()
                company_name = parts[1].strip() if len(parts) > 1 else None
                nip_data.append({'nip': nip, 'company_name': company_name})
    return nip_data


def build_output_record(
    nip: str,
    original_company_name: Optional[str],
    krs_result: Optional[KRSResult] = None,
    ceidg_result: Optional[CEIDGResult] = None
) -> Dict[str, Any]:
    """
    Build a standardized output record from scraping results.
    
    Args:
        nip: NIP number
        original_company_name: Company name from input file
        krs_result: Result from KRS scraping (if found)
        ceidg_result: Result from CEIDG scraping (if found)
    
    Returns:
        Standardized output dictionary
    """
    record = {
        'nip': nip,
        'original_company_name': original_company_name,
        'source': None,
        'company_name': None,
        'krs_code': None,
        'apikey': None,
        'krs': None,
        'regon': None,
        'registration_date': None,
        'owners': None
    }
    
    if krs_result and krs_result.found:
        record['source'] = 'KRS'
        record['company_name'] = krs_result.company_name
        record['krs_code'] = krs_result.krs_code
        record['apikey'] = krs_result.apikey
        record['krs'] = krs_result.krs_number
        record['regon'] = krs_result.regon
        record['registration_date'] = krs_result.registration_date
        
    elif ceidg_result and ceidg_result.found:
        record['source'] = f'CEIDG_{ceidg_result.source}'  # CEIDG_JDG or CEIDG_SC
        record['company_name'] = ceidg_result.legal_name
        record['regon'] = ceidg_result.regon
        record['registration_date'] = ceidg_result.registered_at
        
        # Convert owners to serializable format
        if ceidg_result.owners:
            record['owners'] = [
                {
                    'full_name': owner.full_name,
                    'first_name': owner.first_name,
                    'last_name': owner.last_name,
                    'email': owner.email,
                    'phone': owner.phone,
                    'regon': owner.regon
                }
                for owner in ceidg_result.owners
            ]
    
    return record


def call_webhook(data: List[Dict[str, Any]]) -> None:
    """
    Call webhook with the scraped data.
    
    Args:
        data: List of scraped company records
    """
    try:
        print(f"\n📡 Calling webhook...")
        response = requests.post(
            WEBHOOK_URL, 
            json=data, 
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get('message') == 'Workflow was started':
                print(f"✓ Webhook called successfully: {response_data.get('message')}")
            else:
                print(f"⚠ Webhook responded with 200 but unexpected message: {response.text}")
        else:
            print(f"⚠ Webhook responded with status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"✗ Error calling webhook: {str(e)}")


def save_results(data: List[Dict[str, Any]], output_file: str = 'results.json') -> None:
    """
    Save results to JSON file.
    
    Args:
        data: List of scraped company records
        output_file: Output file path
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Results saved to {output_file} ({len(data)} entries)")


def print_summary(results: List[Dict[str, Any]], timings: Dict[str, float], total_time: float) -> None:
    """
    Print summary of scraping results.
    
    Args:
        results: List of scraped company records
        timings: Dict mapping NIP to processing time
        total_time: Total processing time
    """
    print(f"\n\n{'='*60}")
    print("FINAL RESULTS")
    print(f"{'='*60}\n")
    
    for record in results:
        nip = record['nip']
        print(f"NIP: {nip}")
        print(f"  Time: {timings.get(nip, 0):.2f}s")
        print(f"  Source: {record.get('source') or 'NOT_FOUND'}")
        
        if record.get('company_name'):
            print(f"  Company: {record['company_name']}")
        if record.get('krs'):
            print(f"  KRS: {record['krs']}")
        if record.get('regon'):
            print(f"  REGON: {record['regon']}")
        if record.get('registration_date'):
            print(f"  Registration Date: {record['registration_date']}")
        if record.get('owners'):
            print(f"  Owners:")
            for owner in record['owners']:
                print(f"    - {owner['full_name']}")
                if owner.get('email'):
                    print(f"      Email: {owner['email']}")
                if owner.get('phone'):
                    print(f"      Phone: {owner['phone']}")
        print()
    
    # Print timing summary
    print(f"{'='*60}")
    print("TIMING SUMMARY")
    print(f"{'='*60}")
    print(f"Total time: {total_time:.2f}s")
    avg_time = total_time / len(results) if results else 0
    print(f"Average time per NIP: {avg_time:.2f}s")
    print(f"NIPs processed: {len(results)}")
    
    # Count by source
    sources = {}
    for r in results:
        src = r.get('source') or 'NOT_FOUND'
        sources[src] = sources.get(src, 0) + 1
    
    print(f"\nBy source:")
    for src, count in sorted(sources.items()):
        print(f"  {src}: {count}")
    print()


def scrape_all(nip_file_path: str) -> None:
    """
    Main scraping function - processes all NIPs with KRS -> CEIDG fallback.
    
    Args:
        nip_file_path: Path to text file containing NIP numbers
    """
    # Read NIP numbers from file
    nip_data = read_nip_file(nip_file_path)
    print(f"Found {len(nip_data)} NIP numbers to process\n")
    
    # Results storage
    json_output = []
    timings = {}
    start_time = time.time()
    
    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(headless=False, args=["--start-maximized"])
        context = browser.new_context(no_viewport=True)
        page = context.new_page()
        
        # Navigate to KRS search page initially
        print("Navigating to KRS search page...")
        page.goto(KRS_SEARCH_URL)
        page.wait_for_load_state("networkidle")
        
        for item in nip_data:
            nip = item['nip']
            company_name = item['company_name']
            nip_start_time = time.time()
            
            print(f"\n{'='*60}")
            print(f"Processing NIP: {nip}")
            if company_name:
                print(f"Company: {company_name}")
            print(f"{'='*60}")
            
            krs_result = None
            ceidg_result = None
            
            try:
                # Step 1: Try KRS first
                krs_result = scrape_krs(page, nip)
                
                # Step 2: If not found in KRS, try CEIDG
                if not krs_result.found:
                    print(f"\n  ℹ Not found in KRS, trying CEIDG...")
                    ceidg_result = scrape_ceidg(context, page, nip)
                    
                    # After CEIDG, navigate back to KRS for next iteration
                    if not ceidg_result.found:
                        print(f"\n  ℹ Not found in CEIDG either")
                    
                    # Return to KRS page for next NIP
                    navigate_to_krs(page)
                
                # Build output record
                record = build_output_record(nip, company_name, krs_result, ceidg_result)
                json_output.append(record)
                
            except Exception as e:
                print(f"  ✗ Error processing NIP {nip}: {str(e)}")
                # Add error record
                json_output.append({
                    'nip': nip,
                    'original_company_name': company_name,
                    'source': None,
                    'company_name': None,
                    'krs_code': None,
                    'apikey': None,
                    'krs': None,
                    'regon': None,
                    'registration_date': None,
                    'owners': None,
                    'error': str(e)
                })
            finally:
                # Calculate time for this NIP
                nip_elapsed = time.time() - nip_start_time
                timings[nip] = nip_elapsed
                print(f"\n  ⏱ Time for {nip}: {nip_elapsed:.2f}s")
                
                # Add 1 second delay between items
                time.sleep(1)
        
        # Close browser
        browser.close()
    
    # Calculate total time
    total_time = time.time() - start_time
    
    # Save results
    save_results(json_output)
    
    # Call webhook
    # call_webhook(json_output)
    
    # Print summary
    print_summary(json_output, timings, total_time)


def load_results_json(output_file: str = 'results.json') -> List[Dict[str, Any]]:
    """
    Load results from JSON file.
    
    Args:
        output_file: Output file path
    
    Returns:
        List of scraped company records
    """
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠ File {output_file} not found")
        return []
    except json.JSONDecodeError:
        print(f"⚠ Error parsing {output_file}")
        return []


def get_not_found_nips(nip_file_path: str, results_file: str = 'results.json') -> List[Dict[str, Any]]:
    """
    Get NIPs from input file that were not found in results.json.
    
    Args:
        nip_file_path: Path to text file containing NIP numbers
        results_file: Path to results JSON file
    
    Returns:
        List of dicts with 'nip' and 'company_name' keys for not found NIPs
    """
    all_nips = read_nip_file(nip_file_path)
    results = load_results_json(results_file)
    
    # Get set of NIPs that were found (have a source)
    found_nips = {r['nip'] for r in results if r.get('source')}
    
    # Filter out found NIPs
    not_found = [item for item in all_nips if item['nip'] not in found_nips]
    
    print(f"\nTotal NIPs in file: {len(all_nips)}")
    print(f"Already found: {len(found_nips)}")
    print(f"Not found (to process): {len(not_found)}")
    
    return not_found


def display_results_stats(results_file: str = 'results.json') -> None:
    """
    Display statistics from results.json.
    
    Args:
        results_file: Path to results JSON file
    """
    results = load_results_json(results_file)
    
    if not results:
        print("\nNo results to display")
        return
    
    print(f"\n{'='*60}")
    print("RESULTS STATISTICS")
    print(f"{'='*60}\n")
    
    total = len(results)
    found = sum(1 for r in results if r.get('source'))
    not_found = total - found
    
    print(f"Total entries: {total}")
    print(f"Found: {found} ({found/total*100:.1f}%)")
    print(f"Not found: {not_found} ({not_found/total*100:.1f}%)")
    
    # Breakdown by source
    sources = {}
    for r in results:
        src = r.get('source') or 'NOT_FOUND'
        sources[src] = sources.get(src, 0) + 1
    
    print(f"\nBreakdown by source:")
    for src, count in sorted(sources.items()):
        print(f"  {src}: {count} ({count/total*100:.1f}%)")
    
    print()


def send_results_to_webhook(results_file: str = 'results.json') -> None:
    """
    Send results.json to webhook.
    
    Args:
        results_file: Path to results JSON file
    """
    results = load_results_json(results_file)
    
    if not results:
        print("\nNo results to send")
        return
    
    call_webhook(results)


def scrape_not_found_only(nip_file_path: str) -> None:
    """
    Scrape only NIPs that were not found in previous results.
    
    Args:
        nip_file_path: Path to text file containing NIP numbers
    """
    not_found_nips = get_not_found_nips(nip_file_path)
    
    if not not_found_nips:
        print("\n✓ All NIPs already processed!")
        return
    
    # Load existing results
    existing_results = load_results_json()
    
    # Results storage
    json_output = list(existing_results)  # Copy existing results
    timings = {}
    start_time = time.time()
    
    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(headless=False, args=["--start-maximized"])
        context = browser.new_context(no_viewport=True)
        page = context.new_page()
        
        # Navigate to KRS search page initially
        print("\nNavigating to KRS search page...")
        page.goto(KRS_SEARCH_URL)
        page.wait_for_load_state("networkidle")
        
        for item in not_found_nips:
            nip = item['nip']
            company_name = item['company_name']
            nip_start_time = time.time()
            
            print(f"\n{'='*60}")
            print(f"Processing NIP: {nip}")
            if company_name:
                print(f"Company: {company_name}")
            print(f"{'='*60}")
            
            krs_result = None
            ceidg_result = None
            
            try:
                # Step 1: Try KRS first
                krs_result = scrape_krs(page, nip)
                
                # Step 2: If not found in KRS, try CEIDG
                if not krs_result.found:
                    print(f"\n  ℹ Not found in KRS, trying CEIDG...")
                    ceidg_result = scrape_ceidg(context, page, nip)
                    
                    # After CEIDG, navigate back to KRS for next iteration
                    if not ceidg_result.found:
                        print(f"\n  ℹ Not found in CEIDG either")
                    
                    # Return to KRS page for next NIP
                    navigate_to_krs(page)
                
                # Build output record
                record = build_output_record(nip, company_name, krs_result, ceidg_result)
                
                # Update or append to results
                existing_idx = next((i for i, r in enumerate(json_output) if r['nip'] == nip), None)
                if existing_idx is not None:
                    json_output[existing_idx] = record
                else:
                    json_output.append(record)
                
            except Exception as e:
                print(f"  ✗ Error processing NIP {nip}: {str(e)}")
                # Add error record
                error_record = {
                    'nip': nip,
                    'original_company_name': company_name,
                    'source': None,
                    'company_name': None,
                    'krs_code': None,
                    'apikey': None,
                    'krs': None,
                    'regon': None,
                    'registration_date': None,
                    'owners': None,
                    'error': str(e)
                }
                existing_idx = next((i for i, r in enumerate(json_output) if r['nip'] == nip), None)
                if existing_idx is not None:
                    json_output[existing_idx] = error_record
                else:
                    json_output.append(error_record)
            finally:
                # Calculate time for this NIP
                nip_elapsed = time.time() - nip_start_time
                timings[nip] = nip_elapsed
                print(f"\n  ⏱ Time for {nip}: {nip_elapsed:.2f}s")
                
                # Add 1 second delay between items
                time.sleep(1)
        
        # Close browser
        browser.close()
    
    # Calculate total time
    total_time = time.time() - start_time
    
    # Save results
    save_results(json_output)
    
    # Print summary for newly processed NIPs
    new_results = [r for r in json_output if r['nip'] in [item['nip'] for item in not_found_nips]]
    print_summary(new_results, timings, total_time)


def show_menu() -> None:
    """
    Display console menu and handle user selection.
    """
    while True:
        print(f"\n{'='*60}")
        print("KRS/CEIDG NIP SCRAPER MENU")
        print(f"{'='*60}")
        print("1. Start enrich from nip_numbers.txt")
        print("2. Start enrich on only not found NIP's from the text file")
        print("3. Send the results.json as is to webhook endpoint")
        print("4. Display results.json stats - found/not found, breakdown by type")
        print("5. Exit")
        print(f"{'='*60}")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            print("\n🚀 Starting full enrichment from nip_numbers.txt...")
            scrape_all("nip_numbers.txt")
            
        elif choice == '2':
            print("\n🚀 Starting enrichment for not found NIPs only...")
            scrape_not_found_only("nip_numbers.txt")
            
        elif choice == '3':
            print("\n📤 Sending results.json to webhook...")
            send_results_to_webhook()
            
        elif choice == '4':
            display_results_stats()
            
        elif choice == '5':
            print("\n👋 Goodbye!")
            break
            
        else:
            print("\n⚠ Invalid choice. Please enter 1-5.")


# For backwards compatibility
def scrape_krs_by_nip(nip_file_path: str):
    """Legacy function name - calls scrape_all"""
    scrape_all(nip_file_path)


if __name__ == "__main__":
    show_menu()
