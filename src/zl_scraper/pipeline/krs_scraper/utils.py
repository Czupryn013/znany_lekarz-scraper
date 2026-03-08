"""
Utility functions for the KRS/CEIDG scraper
"""
import re
from typing import Optional


def extract_email(text: str) -> Optional[str]:
    """
    Extract email address from text that might contain other content.
    
    Args:
        text: Text that might contain email and other content (e.g., "email@example.com; www.example.com")
    
    Returns:
        Extracted email or None if not found or text is "-"
    """
    if not text or text.strip() == "-":
        return None
    
    # Email regex pattern
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, text)
    
    return match.group(0) if match else None


def clean_phone(text: str) -> Optional[str]:
    """
    Clean phone number text.
    
    Args:
        text: Phone text that might be "-" or empty
    
    Returns:
        Cleaned phone or None if empty/dash
    """
    if not text or text.strip() == "-":
        return None
    return text.strip()


def clean_text(text: str) -> Optional[str]:
    """
    Clean text, returning None for empty or dash values.
    
    Args:
        text: Text to clean
    
    Returns:
        Cleaned text or None
    """
    if not text or text.strip() == "-":
        return None
    return text.strip()


# Constants
KRS_SEARCH_URL = "https://wyszukiwarka-krs.ms.gov.pl/"
CEIDG_SEARCH_URL = "https://aplikacja.ceidg.gov.pl/ceidg/ceidg.public.ui/search.aspx"
WEBHOOK_URL = "https://piotr-n8n.up.railway.app/webhook/6607ccba-fccf-4b05-ac06-66c66869e1eb"
