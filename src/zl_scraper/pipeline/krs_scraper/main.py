"""
KRS NIP Scraper - Entry point
Scrapes company details from KRS and CEIDG databases by NIP number
"""
from scraper import scrape_all


def main():
    """Main entry point for the scraper"""
    scrape_all("nip_numbers.txt")


if __name__ == "__main__":
    main()
