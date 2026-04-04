#!/usr/bin/env python3
"""
GMB Lead Enricher v2 — Streamlit UI
High hit-rate UK business owner finder with cross-referencing.

Run with:  streamlit run gmb_enricher_ui.py
"""

import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

try:
    import whois
    WHOIS_AVAILABLE = True
except ImportError:
    WHOIS_AVAILABLE = False

# ──────────────────────────────────────────────────────────────────────────────
# Page Config & Styling
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="GMB Lead Enricher", page_icon="🔍",
                   layout="wide")

st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%); }
    .main-header {
        background: linear-gradient(90deg, #00d2ff 0%, #3a7bd5 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        font-size: 2.8rem; font-weight: 800; letter-spacing: -1px; margin-bottom: 0;
    }
    .sub-header { color: #8892b0; font-size: 1.1rem; margin-top: -10px; margin-bottom: 30px; }
    .stat-card {
        background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);
        border-radius: 12px; padding: 20px; text-align: center;
    }
    .stat-number { font-size: 2rem; font-weight: 700; color: #00d2ff; }
    .stat-label { color: #8892b0; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 1px; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    [data-testid="stHeader"] { background: transparent !important; }
    [data-testid="stToolbar"] { visibility: hidden; }
    .processing-text { font-family: 'JetBrains Mono', monospace; color: #ccd6f6; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 12

SCRAPE_PATHS = ["/about", "/about-us", "/team", "/our-team",
                "/contact", "/contact-us", "/",
                "/impressum", "/mentions-legales", "/about-me"]

OWNER_TITLE_PATTERNS = [
    r"(?i)\b(founder|co-founder|owner|proprietor|director|managing\s+director"
    r"|ceo|chief\s+executive|principal|president)\b"
]

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(
    r"(?:\+?\d{1,4}[\s\-.]?)?"
    r"(?:\(?\d{1,5}\)?[\s\-.]?)?"
    r"\d{3,4}[\s\-.]?\d{3,4}"
)
FB_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.|m\.)?facebook\.com/([a-zA-Z0-9._\-]+)/?", re.I
)

# UK mobile prefixes
UK_MOBILE_PREFIXES = ("07", "+447", "00447", "+44 7", "+44(0)7")

# Common UK trade words to strip from company names when searching
TRADE_WORDS = {
    "plumbing", "plumber", "plumbers", "electrical", "electrician", "electricians",
    "roofing", "roofer", "roofers", "carpentry", "carpenter", "carpenters",
    "painting", "painter", "painters", "decorating", "decorator", "decorators",
    "building", "builder", "builders", "construction", "landscaping", "landscaper",
    "gardening", "gardener", "cleaning", "cleaner", "cleaners", "catering",
    "kitchen", "kitchens", "bathroom", "bathrooms", "flooring", "fencing",
    "glazing", "glazier", "windows", "doors", "heating", "gas", "boiler",
    "locksmith", "mechanic", "garage", "auto", "motor", "motors", "cars",
    "cafe", "coffee", "restaurant", "bakery", "bar", "pub", "salon", "barber",
    "hairdresser", "beauty", "spa", "dental", "dentist", "clinic", "surgery",
    "pharmacy", "veterinary", "vet", "vets", "services", "service", "solutions",
    "group", "associates", "consultants", "consulting", "agency", "studio",
    "design", "designs", "developments", "properties", "property", "estates",
    "maintenance", "repairs", "installations", "supplies", "supply", "hire",
    "transport", "logistics", "removals", "storage", "security", "training",
    "education", "academy", "school", "care", "home", "homes", "fitness",
    "gym", "health", "wellness", "photography", "photo", "media", "digital",
    "web", "tech", "technology", "it", "computing", "software", "engineering",
    "engineers", "fabrication", "welding", "joinery", "carpets", "tiling",
    "plastering", "plasterer", "scaffolding", "drainage", "paving",
    "driveways", "conservatories", "extensions", "lofts", "conversions",
}

# Location words to strip
LOCATION_WORDS = {
    "london", "manchester", "birmingham", "leeds", "liverpool", "sheffield",
    "bristol", "newcastle", "nottingham", "leicester", "coventry", "bradford",
    "cardiff", "edinburgh", "glasgow", "belfast", "southampton", "portsmouth",
    "brighton", "plymouth", "stoke", "wolverhampton", "derby", "swansea",
    "hull", "middlesbrough", "sunderland", "reading", "luton", "bolton",
    "bournemouth", "norwich", "swindon", "oxford", "cambridge", "exeter",
    "york", "bath", "chester", "canterbury", "worcester", "lancaster",
    "north", "south", "east", "west", "central", "greater", "uk", "england",
    "scotland", "wales", "surrey", "kent", "essex", "sussex", "hampshire",
    "hertfordshire", "berkshire", "buckinghamshire", "oxfordshire",
    "cambridgeshire", "norfolk", "suffolk", "devon", "cornwall", "dorset",
    "somerset", "wiltshire", "gloucestershire", "warwickshire", "staffordshire",
    "lancashire", "yorkshire", "cheshire", "derbyshire", "lincolnshire",
    "northamptonshire", "leicestershire", "nottinghamshire",
}


# ──────────────────────────────────────────────────────────────────────────────
# Data Model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NameFinding:
    """A single name finding from one source."""
    name: str
    title: str
    source: str           # e.g. "companies_house", "website", "facebook"
    method: str           # human-readable explanation
    confidence: float     # base confidence for this finding alone
    company_number: str = ""  # for Companies House


@dataclass
class EnrichmentResult:
    owner_name: Optional[str] = None
    owner_title: Optional[str] = None
    owner_email: Optional[str] = None
    personal_phone: Optional[str] = None
    emails_found: list = field(default_factory=list)
    phones_found: list = field(default_factory=list)
    confidence: float = 0.0
    how_found: str = ""
    name_findings: List[NameFinding] = field(default_factory=list)

    def finalize(self, business_phones: list = None):
        """Deduplicate, cross-reference names, pick best results."""
        self.emails_found = list(dict.fromkeys(self.emails_found))
        self.phones_found = list(dict.fromkeys(self.phones_found))

        if not self.owner_email and self.emails_found:
            self.owner_email = self._best_email()

        # Personal phone filtering
        if not self.personal_phone and self.phones_found:
            self.personal_phone = self._best_personal_phone(business_phones or [])

        # Cross-reference name findings
        if self.name_findings:
            self._resolve_owner_name()

    def _best_email(self) -> Optional[str]:
        generic = {"info@", "contact@", "hello@", "support@", "admin@",
                   "sales@", "office@", "enquiries@", "mail@"}
        personal = [e for e in self.emails_found
                    if not any(e.lower().startswith(g) for g in generic)]
        return personal[0] if personal else (
            self.emails_found[0] if self.emails_found else None)

    def _best_personal_phone(self, business_phones: list) -> Optional[str]:
        """Find a personal phone number, filtering out business numbers."""
        # Normalize business phones for comparison
        biz_digits = set()
        for bp in business_phones:
            digits = re.sub(r"\D", "", str(bp))
            if len(digits) >= 7:
                biz_digits.add(digits[-10:])  # last 10 digits for comparison

        candidates = []
        for phone in self.phones_found:
            digits = re.sub(r"\D", "", phone)
            # Skip if it matches a known business number
            if digits[-10:] in biz_digits:
                continue
            # Prioritize UK mobile numbers
            is_mobile = any(phone.strip().startswith(p) for p in UK_MOBILE_PREFIXES)
            candidates.append((phone, is_mobile))

        # Return first mobile, or nothing
        mobiles = [c[0] for c in candidates if c[1]]
        if mobiles:
            return mobiles[0]
        return None  # leave blank if no personal number found

    def _resolve_owner_name(self):
        """Cross-reference all name findings and pick the best with explanation."""
        if not self.name_findings:
            return

        # Sort by confidence (highest first)
        findings = sorted(self.name_findings, key=lambda f: f.confidence, reverse=True)
        best = findings[0]

        # Check for cross-references
        cross_refs = []
        for other in findings[1:]:
            if _names_match(best.name, other.name):
                cross_refs.append(other)

        # Check for conflicts
        conflicts = []
        for other in findings[1:]:
            if other not in cross_refs and other.name:
                conflicts.append(other)

        # Build how_found explanation and calculate final confidence
        self.owner_name = best.name
        self.owner_title = best.title

        explanation = f"Found from {best.method}"

        if cross_refs:
            ref_sources = [cr.method.split(":")[0].strip() for cr in cross_refs]
            explanation += f". Confirmed by {', '.join(ref_sources)}"
            # Boost confidence for cross-references
            boost = min(len(cross_refs) * 0.10, 0.20)
            self.confidence = min(best.confidence + boost, 0.99)
        else:
            self.confidence = best.confidence

        if conflicts:
            conflict_info = [f"'{c.name}' ({c.method.split(':')[0].strip()})"
                            for c in conflicts[:2]]
            explanation += f". Note: different name(s) found elsewhere: {', '.join(conflict_info)}"
            # Reduce confidence slightly if there are conflicts
            if not cross_refs:
                self.confidence = max(self.confidence - 0.10, 0.20)

        explanation += f". Confidence: {int(self.confidence * 100)}%"
        self.how_found = explanation


def _names_match(name1: str, name2: str) -> bool:
    """Check if two names likely refer to the same person."""
    if not name1 or not name2:
        return False

    n1 = name1.lower().strip()
    n2 = name2.lower().strip()

    # Exact match
    if n1 == n2:
        return True

    # One contains the other
    if n1 in n2 or n2 in n1:
        return True

    # Split into parts and check surname match
    parts1 = n1.split()
    parts2 = n2.split()

    # Same surname (last word)
    if parts1 and parts2 and parts1[-1] == parts2[-1]:
        return True

    # First name matches (handle Dave/David, etc.)
    if parts1 and parts2:
        f1, f2 = parts1[0], parts2[0]
        # Common nickname mappings
        nicknames = {
            "dave": "david", "mike": "michael", "rob": "robert", "bob": "robert",
            "bill": "william", "will": "william", "jim": "james", "jimmy": "james",
            "tom": "thomas", "tommy": "thomas", "tony": "anthony", "nick": "nicholas",
            "chris": "christopher", "matt": "matthew", "dan": "daniel", "danny": "daniel",
            "steve": "steven", "sam": "samuel", "ben": "benjamin", "joe": "joseph",
            "alex": "alexander", "andy": "andrew", "pat": "patrick", "rick": "richard",
            "dick": "richard", "ted": "edward", "ed": "edward", "charlie": "charles",
            "harry": "harold", "jack": "john", "liz": "elizabeth", "beth": "elizabeth",
            "kate": "katherine", "katie": "katherine", "jenny": "jennifer",
            "sue": "susan", "meg": "margaret", "maggie": "margaret",
        }
        f1_full = nicknames.get(f1, f1)
        f2_full = nicknames.get(f2, f2)
        if f1_full == f2_full and parts1[-1] == parts2[-1]:
            return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# Business Name Owner Extraction
# ──────────────────────────────────────────────────────────────────────────────

def extract_name_from_business(company_name: str) -> Optional[Tuple[str, str]]:
    """
    Try to extract an owner name from the business name.
    Returns (extracted_name_part, explanation) or None.

    Examples:
      "Dave's Plumbing"        → ("Dave", "first_name")
      "Smith & Sons Roofing"   → ("Smith", "surname")
      "John Smith Plumbing"    → ("John Smith", "full_name")
      "Mitchell's Kitchens"    → ("Mitchell", "surname")
      "J Smith Electrical"     → ("Smith", "surname")
    """
    if not company_name:
        return None

    name = company_name.strip()

    # Pattern 1: "Dave's Plumbing" or "Johnson's Carpets" (possessive)
    poss_match = re.match(
        r"^([A-Z][a-z]+)(?:'s|'s)\s+", name
    )
    if poss_match:
        found = poss_match.group(1)
        # Check it's not a place name
        if found.lower() not in LOCATION_WORDS and found.lower() not in TRADE_WORDS:
            return (found, "possessive_name")

    # Pattern 2: "[FirstName] [LastName] [Trade]" — e.g. "John Smith Plumbing"
    full_name_match = re.match(
        r"^([A-Z][a-z]+)\s+([A-Z][a-z]+)\s+", name
    )
    if full_name_match:
        first, second = full_name_match.group(1), full_name_match.group(2)
        if (first.lower() not in TRADE_WORDS and first.lower() not in LOCATION_WORDS
                and second.lower() not in TRADE_WORDS and second.lower() not in LOCATION_WORDS):
            return (f"{first} {second}", "full_name")

    # Pattern 3: "[Surname] & Sons/Brothers/Partners/Family"
    and_sons_match = re.match(
        r"^([A-Z][a-z]+)\s*&\s*(?:Sons?|Brothers?|Partners?|Family|Daughters?|Co)\b",
        name
    )
    if and_sons_match:
        found = and_sons_match.group(1)
        if found.lower() not in LOCATION_WORDS and found.lower() not in TRADE_WORDS:
            return (found, "surname_and_family")

    # Pattern 4: "[Surname] [Trade]" — e.g. "Smith Plumbing", "Mitchell Electrics"
    surname_trade_match = re.match(r"^([A-Z][a-z]+)\s+(\w+)", name)
    if surname_trade_match:
        potential_surname = surname_trade_match.group(1)
        next_word = surname_trade_match.group(2)
        if (next_word.lower() in TRADE_WORDS
                and potential_surname.lower() not in LOCATION_WORDS
                and potential_surname.lower() not in TRADE_WORDS
                and len(potential_surname) > 2):
            return (potential_surname, "surname_before_trade")

    # Pattern 5: "[Initial] [Surname] [Trade]" — e.g. "J Smith Electrical"
    initial_match = re.match(r"^([A-Z])\s+([A-Z][a-z]+)\s+(\w+)", name)
    if initial_match:
        surname = initial_match.group(2)
        next_word = initial_match.group(3)
        if (next_word.lower() in TRADE_WORDS
                and surname.lower() not in LOCATION_WORDS
                and surname.lower() not in TRADE_WORDS):
            return (surname, "initial_surname")

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────

def get_domain(url: str) -> str:
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    return urlparse(url).netloc or ""


def safe_request(url: str, **kwargs) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True, **kwargs)
        if resp.status_code == 200:
            return resp
    except Exception:
        pass
    return None


def extract_emails(text: str) -> list:
    emails = EMAIL_RE.findall(text)
    return list(dict.fromkeys(
        e for e in emails
        if not re.search(r"\.(png|jpg|jpeg|gif|svg|webp|css|js)$", e, re.I)
    ))


def extract_phones(text: str) -> list:
    phones = PHONE_RE.findall(text)
    cleaned = []
    for p in phones:
        digits = re.sub(r"\D", "", p)
        if 7 <= len(digits) <= 15:
            cleaned.append(p.strip())
    return list(dict.fromkeys(cleaned))


def get_field(row: dict, candidates: list) -> str:
    for key in candidates:
        if key in row:
            val = row[key]
            if pd.notna(val):
                return str(val).strip()
        for col in row:
            if str(col).lower().replace(" ", "_") == key.lower():
                val = row[col]
                if pd.notna(val):
                    return str(val).strip()
    return ""


def extract_postcode(address: str) -> str:
    """Extract a UK postcode from an address string."""
    if not address:
        return ""
    match = re.search(
        r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", address.upper()
    )
    return match.group(0).strip() if match else ""


# ──────────────────────────────────────────────────────────────────────────────
# Companies House (Aggressive UK Matching)
# ──────────────────────────────────────────────────────────────────────────────

def _generate_ch_search_names(company_name: str) -> list:
    """Generate multiple name variations for Companies House search."""
    names = []
    name = company_name.strip()

    # Original name
    names.append(name)

    # Strip common suffixes that might not be in GMB data
    stripped = re.sub(r"\s*(Ltd\.?|Limited|LLP|PLC|Inc\.?)\s*$", "", name, flags=re.I).strip()
    if stripped != name:
        names.append(stripped)

    # Add Ltd if not present
    if not re.search(r"(Ltd\.?|Limited|LLP|PLC)\s*$", name, re.I):
        names.append(f"{name} Ltd")
        names.append(f"{stripped} Limited")

    # Swap & and 'and'
    if "&" in name:
        names.append(name.replace("&", "and"))
    if " and " in name.lower():
        names.append(re.sub(r"\band\b", "&", name, flags=re.I))

    # Strip location words
    words = name.split()
    core_words = [w for w in words
                  if w.lower() not in LOCATION_WORDS and w.lower() not in {"the"}]
    if len(core_words) < len(words) and len(core_words) >= 2:
        names.append(" ".join(core_words))
        names.append(" ".join(core_words) + " Ltd")

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for n in names:
        nl = n.lower().strip()
        if nl and nl not in seen:
            seen.add(nl)
            unique.append(n)

    return unique[:6]  # max 6 variations


def enrich_companies_house(company_name: str, address: str,
                           api_key: str) -> Tuple[EnrichmentResult, Optional[NameFinding]]:
    """Aggressively search Companies House with multiple name variations."""
    result = EnrichmentResult()
    finding = None

    if not api_key or not company_name:
        return result, finding

    gmb_postcode = extract_postcode(address)
    search_names = _generate_ch_search_names(company_name)

    best_officer = None
    best_company_number = ""
    best_match_score = 0

    for search_name in search_names:
        resp = safe_request(
            "https://api.company-information.service.gov.uk/search/companies",
            params={"q": search_name, "items_per_page": 5},
            auth=(api_key, ""),
        )
        if not resp:
            continue

        items = resp.json().get("items", [])

        for item in items:
            company_number = item.get("company_number", "")
            status = (item.get("company_status") or "").lower()
            ch_address = item.get("address_snippet", "")
            ch_title = item.get("title", "")

            # Skip dissolved companies
            if status in ("dissolved", "liquidation", "administration"):
                continue

            # Score this match
            score = 0

            # Name similarity
            if company_name.lower() in ch_title.lower() or ch_title.lower() in company_name.lower():
                score += 3
            elif any(w in ch_title.lower() for w in company_name.lower().split() if len(w) > 3):
                score += 1

            # Postcode match (strong signal)
            ch_postcode = extract_postcode(ch_address)
            if gmb_postcode and ch_postcode and gmb_postcode.replace(" ", "") == ch_postcode.replace(" ", ""):
                score += 5

            # Active company bonus
            if status == "active":
                score += 2

            if score > best_match_score and company_number:
                best_match_score = score
                best_company_number = company_number

        if best_match_score >= 5:  # good match found, stop searching variations
            break

    if not best_company_number:
        return result, finding

    # Get officers for the best matching company
    resp2 = safe_request(
        f"https://api.company-information.service.gov.uk/company/{best_company_number}/officers",
        auth=(api_key, ""),
    )
    if not resp2:
        return result, finding

    for officer in resp2.json().get("items", []):
        role = (officer.get("officer_role") or "").lower()
        name = officer.get("name", "")
        resigned = officer.get("resigned_on")

        if not resigned and role in ("director", "llp-member",
                                      "llp-designated-member", "managing-officer"):
            if "," in name:
                parts = name.split(",", 1)
                name = f"{parts[1].strip().title()} {parts[0].strip().title()}"
            else:
                name = name.title()

            # Determine confidence based on match quality
            if best_match_score >= 5:
                conf = 0.92
            elif best_match_score >= 3:
                conf = 0.82
            else:
                conf = 0.70

            finding = NameFinding(
                name=name,
                title=role.replace("-", " ").title(),
                source="companies_house",
                method=(f"Companies House: active {role.replace('-', ' ').title()} "
                        f"(company #{best_company_number}"
                        f"{', postcode match' if best_match_score >= 5 else ''})"),
                confidence=conf,
                company_number=best_company_number,
            )

            result.name_findings.append(finding)
            break

    return result, finding


# ──────────────────────────────────────────────────────────────────────────────
# Website Scraper
# ──────────────────────────────────────────────────────────────────────────────

def enrich_website(website: str, company_name: str) -> Tuple[EnrichmentResult, Optional[NameFinding]]:
    result = EnrichmentResult()
    finding = None

    if not website:
        return result, finding

    base_url = website if website.startswith("http") else f"https://{website}"
    base_url = base_url.rstrip("/")

    for path in SCRAPE_PATHS:
        if result.name_findings and result.emails_found:
            break

        url = base_url + path
        resp = safe_request(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        result.emails_found.extend(extract_emails(text))
        result.phones_found.extend(extract_phones(text))

        # Schema.org JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    for key in ("founder", "author", "employee", "member"):
                        person = item.get(key)
                        persons = []
                        if isinstance(person, dict):
                            persons = [person]
                        elif isinstance(person, list):
                            persons = person

                        for p in persons:
                            if isinstance(p, dict) and p.get("name") and not finding:
                                f = NameFinding(
                                    name=p["name"],
                                    title=key.title(),
                                    source="website",
                                    method=f"Website {path}: schema.org '{key}' field",
                                    confidence=0.70,
                                )
                                result.name_findings.append(f)
                                finding = f
                                break

                    if item.get("email"):
                        result.emails_found.append(item["email"])
                    if item.get("telephone"):
                        result.phones_found.append(item["telephone"])
            except (json.JSONDecodeError, TypeError):
                continue

        # Owner text patterns in HTML tags
        if not finding:
            for tag in soup.find_all(["h1", "h2", "h3", "h4", "strong", "b", "p"]):
                tag_text = tag.get_text(strip=True)
                for pattern in OWNER_TITLE_PATTERNS:
                    if re.search(pattern, tag_text):
                        parts = re.split(r"[—\-–|,]", tag_text)
                        for part in parts:
                            part = part.strip()
                            if re.match(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$", part):
                                if not re.search(OWNER_TITLE_PATTERNS[0], part):
                                    title_match = re.search(pattern, tag_text)
                                    matched_title = title_match.group(0) if title_match else "Owner"
                                    f = NameFinding(
                                        name=part,
                                        title=matched_title,
                                        source="website",
                                        method=f"Website {path}: name near '{matched_title}' text",
                                        confidence=0.50,
                                    )
                                    result.name_findings.append(f)
                                    finding = f
                                    break
                    if finding:
                        break
                if finding:
                    break

        # Copyright footer pattern: "© 2024 John Smith" or "Copyright John Smith"
        if not finding:
            copyright_patterns = [
                r"(?:©|copyright)\s*(?:\d{4}\s*)?([A-Z][a-z]+\s+[A-Z][a-z]+)",
                r"(?:©|copyright)\s*(?:\d{4}\s*[-–]\s*\d{4}\s*)?([A-Z][a-z]+\s+[A-Z][a-z]+)",
            ]
            for cp in copyright_patterns:
                match = re.search(cp, text, re.I)
                if match:
                    cname = match.group(1).strip()
                    # Make sure it's not the company name itself
                    if cname.lower() not in company_name.lower():
                        f = NameFinding(
                            name=cname,
                            title="Copyright holder",
                            source="website",
                            method=f"Website {path}: copyright notice '© {cname}'",
                            confidence=0.40,
                        )
                        result.name_findings.append(f)
                        finding = f
                        break

        # Meta author
        if not finding:
            author_meta = soup.find("meta", attrs={"name": "author"})
            if author_meta and author_meta.get("content"):
                aname = author_meta["content"].strip()
                if len(aname.split()) >= 2:
                    f = NameFinding(
                        name=aname,
                        title="Author",
                        source="website",
                        method=f"Website {path}: HTML meta author tag",
                        confidence=0.30,
                    )
                    result.name_findings.append(f)
                    finding = f

    return result, finding


# ──────────────────────────────────────────────────────────────────────────────
# Facebook
# ──────────────────────────────────────────────────────────────────────────────

def _find_fb_on_website(website: str) -> Optional[str]:
    base_url = website if website.startswith("http") else f"https://{website}"
    resp = safe_request(base_url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for link in soup.find_all("a", href=True):
        match = FB_URL_RE.search(link["href"])
        if match:
            slug = match.group(1)
            if slug.lower() not in ("sharer", "sharer.php", "share", "dialog",
                                     "plugins", "tr", "flx", "login", "help"):
                return f"https://www.facebook.com/{slug}"
    return None


def enrich_facebook(website: str, company_name: str,
                    facebook_url: str = "") -> Tuple[EnrichmentResult, Optional[NameFinding]]:
    result = EnrichmentResult()
    finding = None

    fb_url = None
    if facebook_url:
        match = FB_URL_RE.search(facebook_url)
        if match:
            fb_url = f"https://www.facebook.com/{match.group(1)}"
    if not fb_url and website:
        fb_url = _find_fb_on_website(website)
    if not fb_url:
        return result, finding

    urls_to_try = [
        fb_url.replace("www.facebook.com", "m.facebook.com"),
        (fb_url.rstrip("/") + "/about").replace("www.facebook.com", "m.facebook.com"),
    ]

    for url in urls_to_try:
        if finding and result.emails_found:
            break

        resp = safe_request(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        result.emails_found.extend(extract_emails(text))
        result.phones_found.extend(extract_phones(text))

        # OG tags
        for prop in ["og:description", "og:email", "og:phone_number"]:
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"):
                content = tag["content"]
                if prop == "og:description":
                    result.emails_found.extend(extract_emails(content))
                    result.phones_found.extend(extract_phones(content))
                elif prop == "og:email":
                    result.emails_found.append(content)
                elif prop == "og:phone_number":
                    result.phones_found.append(content)

        # Owner text patterns
        if not finding:
            owner_patterns = [
                r"(?:owned|founded|managed|run|started|created)\s+by\s+"
                r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
                r"(?:owner|founder|manager|proprietor|director)[\s:]+\s*"
                r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
            ]
            for pattern in owner_patterns:
                match = re.search(pattern, text)
                if match:
                    fname = match.group(1).strip()
                    words = fname.split()
                    bad = {"Facebook Page", "Our Team", "Our Company", "This Page",
                           "The Company", "More Info", "Read More", "Learn More"}
                    if 2 <= len(words) <= 4 and fname not in bad:
                        f = NameFinding(
                            name=fname,
                            title="Owner",
                            source="facebook",
                            method=f"Facebook page: '{match.group(0)[:50].strip()}'",
                            confidence=0.50,
                        )
                        result.name_findings.append(f)
                        finding = f
                        break

        # Script-embedded page owner
        if not finding:
            for script in soup.find_all("script"):
                script_text = script.string or ""
                owner_match = re.search(
                    r'"page_owner(?:_name)?":\s*"([^"]+)"', script_text
                )
                if owner_match:
                    fname = owner_match.group(1)
                    if len(fname.split()) >= 2:
                        f = NameFinding(
                            name=fname,
                            title="Page Owner",
                            source="facebook",
                            method="Facebook page: transparency data (admin name)",
                            confidence=0.65,
                        )
                        result.name_findings.append(f)
                        finding = f
                        break

    return result, finding


# ──────────────────────────────────────────────────────────────────────────────
# Hunter.io
# ──────────────────────────────────────────────────────────────────────────────

def enrich_hunter(website: str, company_name: str,
                  api_key: str) -> Tuple[EnrichmentResult, Optional[NameFinding]]:
    result = EnrichmentResult()
    finding = None

    if not api_key or not website:
        return result, finding

    domain = get_domain(website)
    if not domain:
        return result, finding

    resp = safe_request(
        "https://api.hunter.io/v2/domain-search",
        params={"domain": domain, "api_key": api_key, "limit": 10}
    )
    if not resp:
        return result, finding

    data = resp.json().get("data", {})
    for entry in data.get("emails", []):
        email = entry.get("value", "")
        if email:
            result.emails_found.append(email)

        position = (entry.get("position") or "").lower()
        is_owner = any(kw in position for kw in
                       ["owner", "founder", "ceo", "director", "president", "principal"])
        first = entry.get("first_name", "")
        last = entry.get("last_name", "")

        if is_owner and first and last and not finding:
            f = NameFinding(
                name=f"{first} {last}",
                title=entry.get("position", "Owner"),
                source="hunter",
                method=f"Hunter.io: listed as '{entry.get('position', 'Owner')}' on domain",
                confidence=0.75,
            )
            result.name_findings.append(f)
            result.owner_email = email
            finding = f

    return result, finding


# ──────────────────────────────────────────────────────────────────────────────
# WHOIS
# ──────────────────────────────────────────────────────────────────────────────

def enrich_whois(website: str) -> Tuple[EnrichmentResult, Optional[NameFinding]]:
    result = EnrichmentResult()
    finding = None

    if not WHOIS_AVAILABLE or not website:
        return result, finding

    domain = get_domain(website)
    if not domain:
        return result, finding

    try:
        w = whois.whois(domain)
    except Exception:
        return result, finding

    registrant = getattr(w, "name", None) or getattr(w, "registrant_name", None)
    if registrant and isinstance(registrant, str):
        privacy_kw = ["privacy", "proxy", "whoisguard", "domains by",
                      "redacted", "data protected", "withheld"]
        if not any(kw in registrant.lower() for kw in privacy_kw):
            if len(registrant.split()) >= 2:
                f = NameFinding(
                    name=registrant.title(),
                    title="Domain Registrant",
                    source="whois",
                    method="WHOIS: domain registrant name (may be web developer)",
                    confidence=0.25,
                )
                result.name_findings.append(f)
                finding = f

    emails = getattr(w, "emails", None)
    if emails:
        if isinstance(emails, str):
            emails = [emails]
        for e in emails:
            if "abuse" not in e.lower() and "privacy" not in e.lower():
                result.emails_found.append(e)

    return result, finding


# ──────────────────────────────────────────────────────────────────────────────
# Main Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def enrich_single_row(row: dict, config: dict) -> EnrichmentResult:
    """Run all sources IN PARALLEL, then cross-reference."""
    company = get_field(row, ["company", "business_name", "name",
                               "company_name", "business", "title"])
    website = get_field(row, ["website", "url", "site", "web",
                               "website_url", "domain"])
    address = get_field(row, ["address", "full_address", "location", "street"])
    facebook = get_field(row, ["facebook", "facebook_url", "fb",
                                "fb_url", "facebook_page", "social_facebook"])
    biz_phone = get_field(row, ["phone", "telephone", "tel", "business_phone"])
    biz_intl_phone = get_field(row, ["intl_phone", "international_phone",
                                      "intl phone", "intl_tel"])

    business_phones = [p for p in [biz_phone, biz_intl_phone] if p]

    combined = EnrichmentResult()

    # 0. Extract potential name from business name (for cross-referencing later)
    biz_name_extract = extract_name_from_business(company)

    # Run all enabled sources in parallel
    ch_key = config.get("ch_key", "")
    hunter_key = config.get("hunter_key", "")
    tasks = {}

    with ThreadPoolExecutor(max_workers=5) as source_executor:
        if config.get("enable_ch", False) and ch_key:
            tasks["ch"] = source_executor.submit(
                enrich_companies_house, company, address, ch_key)

        if config.get("enable_website", True):
            tasks["web"] = source_executor.submit(
                enrich_website, website, company)

        if config.get("enable_hunter", False) and hunter_key:
            tasks["hunter"] = source_executor.submit(
                enrich_hunter, website, company, hunter_key)

        if config.get("enable_facebook", True):
            tasks["fb"] = source_executor.submit(
                enrich_facebook, website, company, facebook)

        if config.get("enable_whois", True):
            tasks["whois"] = source_executor.submit(
                enrich_whois, website)

        # Collect results as they complete
        for key, future in tasks.items():
            try:
                r, _ = future.result(timeout=30)
                combined.emails_found.extend(r.emails_found)
                combined.phones_found.extend(r.phones_found)
                combined.name_findings.extend(r.name_findings)
                if r.owner_email and not combined.owner_email:
                    combined.owner_email = r.owner_email
            except Exception:
                pass

    # Business name cross-reference
    if biz_name_extract:
        extracted, extract_type = biz_name_extract
        # Check if any finding matches the extracted name
        for f in combined.name_findings:
            if _names_match(f.name, extracted) or extracted.lower() in f.name.lower():
                # Boost confidence — business name confirms the finding
                f.confidence = min(f.confidence + 0.10, 0.99)
                f.method += f" (confirmed: business name contains '{extracted}')"
                break
        else:
            # No other source found, use business name as weak finding
            if not combined.name_findings:
                explanation = {
                    "possessive_name": f"first name from business name ('{company}')",
                    "full_name": f"full name from business name ('{company}')",
                    "surname_and_family": f"surname from business name ('{company}')",
                    "surname_before_trade": f"likely surname from business name ('{company}')",
                    "initial_surname": f"likely surname from business name ('{company}')",
                }
                combined.name_findings.append(NameFinding(
                    name=extracted,
                    title="Possible owner",
                    source="business_name",
                    method=f"Business name analysis: {explanation.get(extract_type, extract_type)}",
                    confidence=0.25,
                ))

    # Finalize with cross-referencing
    combined.finalize(business_phones=business_phones)

    # If nothing found at all
    if not combined.owner_name and not combined.how_found:
        combined.how_found = (
            "Not found — company may be a sole trader (not at Companies House), "
            "or website/social media did not contain owner information"
        )
        combined.confidence = 0.0

    return combined


# ──────────────────────────────────────────────────────────────────────────────
# Parallel Worker
# ──────────────────────────────────────────────────────────────────────────────

def _process_one_row(args):
    idx, row, config = args
    try:
        result = enrich_single_row(row, config)
    except Exception as e:
        result = EnrichmentResult()
        result.how_found = f"Error during enrichment: {str(e)[:100]}"

    company = get_field(row, ["company", "business_name", "name",
                               "company_name", "business", "title"])

    result_row = dict(row)
    result_row["owner_name"] = result.owner_name or ""
    result_row["owner_title"] = result.owner_title or ""
    result_row["owner_email"] = result.owner_email or ""
    result_row["personal_phone"] = result.personal_phone or ""
    result_row["all_emails_found"] = "; ".join(result.emails_found[:5])
    result_row["confidence_score"] = f"{result.confidence:.2f}"
    result_row["how_found"] = result.how_found

    return idx, result_row, result, company


# ──────────────────────────────────────────────────────────────────────────────
# Main Content
# ──────────────────────────────────────────────────────────────────────────────

st.markdown('<p class="main-header">🔍 GMB Lead Enricher</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Upload your GMB data → get owner names, emails & personal numbers</p>',
            unsafe_allow_html=True)

# ── Settings (on main page, always visible) ──
with st.expander("⚙️ Settings — click to configure sources, API keys & performance", expanded=False):
    set_col1, set_col2, set_col3 = st.columns(3)

    with set_col1:
        st.markdown("**Enrichment Sources**")
        enable_website = st.toggle("🌐 Website Scraper", value=True)
        enable_facebook = st.toggle("📘 Facebook Pages", value=True)
        enable_whois = st.toggle("🔎 WHOIS Lookup", value=WHOIS_AVAILABLE,
                                  disabled=not WHOIS_AVAILABLE)

    with set_col2:
        st.markdown("**API Keys**")
        ch_key = st.text_input("🏛️ Companies House Key",
                               value=os.environ.get("COMPANIES_HOUSE_KEY", ""),
                               type="password")
        enable_ch = st.toggle("Enable Companies House", value=bool(ch_key)) if ch_key else False

        hunter_key = st.text_input("📧 Hunter.io Key",
                                   value=os.environ.get("HUNTER_API_KEY", ""),
                                   type="password")
        enable_hunter = st.toggle("Enable Hunter.io", value=bool(hunter_key)) if hunter_key else False

    with set_col3:
        st.markdown("**Performance**")
        workers = st.slider("Parallel workers", 1, 15, 10,
                             help="10 = safe & fast, 15 = aggressive")

        st.markdown("**Active Sources**")
        sources = {
            "Companies House": bool(ch_key and enable_ch),
            "Website Scraper": enable_website,
            "Facebook Pages": enable_facebook,
            "Hunter.io": bool(hunter_key and enable_hunter),
            "WHOIS": enable_whois and WHOIS_AVAILABLE,
            "Business Name Analysis": True,
        }
        for name, active in sources.items():
            st.markdown(f"{'🟢' if active else '🔴'} {name}")

uploaded_file = st.file_uploader("Upload your GMB CSV or Excel file",
                                 type=["csv", "xlsx", "xls"])

if uploaded_file:
    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        st.stop()

    st.markdown(f"### 📋 Preview — {len(df)} rows loaded")
    st.dataframe(df.head(10), use_container_width=True, height=300)

    # Column detection
    detected = {}
    for label, candidates in [
        ("Company", ["company", "business_name", "name", "company_name", "business", "title"]),
        ("Website", ["website", "url", "site", "web", "website_url", "domain"]),
        ("Address", ["address", "full_address", "location", "street"]),
        ("Country", ["country", "country_code", "region"]),
        ("Facebook", ["facebook", "facebook_url", "fb", "fb_url", "facebook_page"]),
        ("Phone", ["phone", "telephone", "tel", "business_phone"]),
    ]:
        for c in candidates:
            for col in df.columns:
                if col.lower().replace(" ", "_") == c:
                    detected[label] = col
                    break
            if label in detected:
                break

    if detected:
        cols = ", ".join([f"**{k}** → `{v}`" for k, v in detected.items()])
        st.success(f"Auto-detected columns: {cols}")

    # Row range
    col1, col2 = st.columns(2)
    with col1:
        start_row = st.number_input("Start from row", 1, len(df), 1) - 1
    with col2:
        end_row = st.number_input("End at row", 1, len(df), len(df))

    total_rows = end_row - start_row
    active_count = sum(1 for v in sources.values() if v)
    est_minutes = (total_rows / workers) * 3.0 / 60
    st.info(f"Will process **{total_rows} rows** with **{workers} workers** "
            f"and **{active_count} sources**. Estimated: **~{est_minutes:.1f} minutes**")

    # Session state for stop button and results persistence
    if "enrichment_running" not in st.session_state:
        st.session_state.enrichment_running = False
    if "stop_requested" not in st.session_state:
        st.session_state.stop_requested = False
    if "results_df" not in st.session_state:
        st.session_state.results_df = None
    if "results_stats" not in st.session_state:
        st.session_state.results_stats = None
    if "run_history" not in st.session_state:
        st.session_state.run_history = []

    # Buttons
    col_start, col_stop = st.columns(2)
    with col_start:
        start_clicked = st.button("🚀 Start Enrichment", type="primary",
                                   use_container_width=True,
                                   disabled=st.session_state.enrichment_running)
    with col_stop:
        stop_clicked = st.button("🛑 Stop Enrichment", type="secondary",
                                  use_container_width=True,
                                  disabled=not st.session_state.enrichment_running)

    if stop_clicked:
        st.session_state.stop_requested = True

    if start_clicked:
        st.session_state.enrichment_running = True
        st.session_state.stop_requested = False
        st.session_state.results_df = None
        st.session_state.results_stats = None

        config = {
            "enable_website": enable_website,
            "enable_facebook": enable_facebook,
            "enable_hunter": bool(hunter_key and enable_hunter),
            "enable_ch": bool(ch_key and enable_ch),
            "enable_whois": enable_whois and WHOIS_AVAILABLE,
            "hunter_key": hunter_key,
            "ch_key": ch_key,
        }

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.container()

        found_names = 0
        found_emails = 0
        found_phones = 0
        results_data = [None] * total_rows
        completed = 0
        stopped_early = False

        start_time = time.time()

        work_items = []
        for i, idx in enumerate(range(start_row, end_row)):
            row = df.iloc[idx].to_dict()
            work_items.append((i, row, config))

        # Process in batches for stop button support
        batch_size = workers
        for batch_start in range(0, len(work_items), batch_size):
            if st.session_state.stop_requested:
                stopped_early = True
                break

            batch = work_items[batch_start:batch_start + batch_size]

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_process_one_row, item): item[0]
                           for item in batch}

                for future in as_completed(futures):
                    try:
                        i, result_row, result, company = future.result()
                    except Exception:
                        i = futures[future]
                        result_row = dict(work_items[i][1])
                        result_row.update({
                            "owner_name": "", "owner_title": "",
                            "owner_email": "", "personal_phone": "",
                            "all_emails_found": "",
                            "confidence_score": "0.00",
                            "how_found": "Error during processing"
                        })
                        result = EnrichmentResult()
                        company = "Unknown"

                    results_data[i] = result_row
                    completed += 1

                    if result.owner_name:
                        found_names += 1
                    if result.owner_email:
                        found_emails += 1
                    if result.personal_phone:
                        found_phones += 1

                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (total_rows - completed) / rate if rate > 0 else 0

                    progress_bar.progress(completed / total_rows)
                    status_text.markdown(
                        f'<p class="processing-text">'
                        f'Completed {completed}/{total_rows} '
                        f'({rate:.1f} rows/sec) — ~{remaining:.0f}s remaining</p>',
                        unsafe_allow_html=True
                    )

                    with log_container:
                        found_items = []
                        if result.owner_name:
                            found_items.append(f"👤 {result.owner_name}")
                        if result.owner_email:
                            found_items.append(f"📧 {result.owner_email}")
                        if result.personal_phone:
                            found_items.append(f"📞 {result.personal_phone}")

                        conf = result.confidence
                        icon = "🟢" if conf >= 0.70 else "🟡" if conf >= 0.40 else "🔴" if conf > 0 else "⚪"

                        if found_items:
                            st.markdown(f"{icon} **{company}** — {' | '.join(found_items)}")
                        else:
                            st.markdown(f"⚪ **{company}** — no owner data found")

            # Save partial results after each batch (survives app restarts)
            partial_df = pd.DataFrame([r for r in results_data if r is not None])
            st.session_state.results_df = partial_df
            st.session_state.results_stats = {
                "completed": completed,
                "found_names": found_names,
                "found_emails": found_emails,
                "found_phones": found_phones,
                "total_time": time.time() - start_time,
            }

        # Done
        st.session_state.enrichment_running = False
        st.session_state.stop_requested = False

        total_time = time.time() - start_time
        status_text.empty()
        progress_bar.empty()

        if stopped_early:
            st.warning(f"Enrichment stopped early after {completed} rows.")

        # Save results to session state so they persist across reruns
        final_df = pd.DataFrame([r for r in results_data if r is not None])
        st.session_state.results_df = final_df
        st.session_state.results_stats = {
            "completed": completed,
            "found_names": found_names,
            "found_emails": found_emails,
            "found_phones": found_phones,
            "total_time": total_time,
        }

        # Save to run history (max 10)
        history_entry = {
            "timestamp": datetime.now().strftime("%H:%M — %d %b %Y"),
            "filename": uploaded_file.name,
            "rows": completed,
            "found_names": found_names,
            "found_emails": found_emails,
            "found_phones": found_phones,
            "hit_rate": int(found_names / completed * 100) if completed else 0,
            "total_time": total_time,
            "csv_data": final_df.to_csv(index=False),
            "xlsx_data": None,  # generated on demand to save memory
            "df": final_df,
        }
        st.session_state.run_history.insert(0, history_entry)  # newest first
        if len(st.session_state.run_history) > 10:
            st.session_state.run_history.pop()  # drop oldest

    # ── Display results (persists across page reruns) ──
    if st.session_state.results_df is not None and not st.session_state.results_df.empty:
        stats = st.session_state.results_stats
        results_df = st.session_state.results_df

        st.markdown("---")
        st.markdown("## 📊 Results")

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(f'<div class="stat-card"><div class="stat-number">{stats["completed"]}</div>'
                        f'<div class="stat-label">Processed</div></div>', unsafe_allow_html=True)
        with c2:
            pct = int(stats["found_names"] / stats["completed"] * 100) if stats["completed"] else 0
            st.markdown(f'<div class="stat-card"><div class="stat-number">{stats["found_names"]} ({pct}%)</div>'
                        f'<div class="stat-label">Names Found</div></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="stat-card"><div class="stat-number">{stats["found_emails"]}</div>'
                        f'<div class="stat-label">Emails Found</div></div>', unsafe_allow_html=True)
        with c4:
            st.markdown(f'<div class="stat-card"><div class="stat-number">{stats["found_phones"]}</div>'
                        f'<div class="stat-label">Personal Phones</div></div>', unsafe_allow_html=True)
        with c5:
            st.markdown(f'<div class="stat-card"><div class="stat-number">{stats["total_time"]:.0f}s</div>'
                        f'<div class="stat-label">Total Time</div></div>', unsafe_allow_html=True)

        st.dataframe(results_df, use_container_width=True, height=400)

        st.markdown("### 📥 Download")
        c1, c2 = st.columns(2)
        with c1:
            csv_buf = io.StringIO()
            results_df.to_csv(csv_buf, index=False)
            st.download_button("⬇️ Download CSV", csv_buf.getvalue(),
                               "enriched_leads.csv", "text/csv",
                               use_container_width=True)
        with c2:
            xlsx_buf = io.BytesIO()
            results_df.to_excel(xlsx_buf, index=False, engine="openpyxl")
            st.download_button("⬇️ Download Excel", xlsx_buf.getvalue(),
                               "enriched_leads.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)

    # ── Run History (persists across reruns within session) ──
    if st.session_state.run_history:
        st.markdown("---")
        st.markdown(f"## 📜 Run History ({len(st.session_state.run_history)} of 10)")

        for idx, entry in enumerate(st.session_state.run_history):
            hit_pct = entry["hit_rate"]
            hit_icon = "🟢" if hit_pct >= 70 else "🟡" if hit_pct >= 40 else "🔴"

            with st.expander(
                f"{hit_icon} **{entry['filename']}** — "
                f"{entry['rows']} rows — {hit_pct}% hit rate — "
                f"{entry['timestamp']}"
            ):
                # Stats row
                hc1, hc2, hc3, hc4 = st.columns(4)
                with hc1:
                    st.metric("Names Found", f"{entry['found_names']} ({hit_pct}%)")
                with hc2:
                    st.metric("Emails Found", entry["found_emails"])
                with hc3:
                    st.metric("Personal Phones", entry["found_phones"])
                with hc4:
                    st.metric("Time", f"{entry['total_time']:.0f}s")

                # Download buttons
                dc1, dc2 = st.columns(2)
                with dc1:
                    st.download_button(
                        "⬇️ Download CSV",
                        entry["csv_data"],
                        f"enriched_{entry['filename']}",
                        "text/csv",
                        use_container_width=True,
                        key=f"hist_csv_{idx}",
                    )
                with dc2:
                    hist_xlsx = io.BytesIO()
                    entry["df"].to_excel(hist_xlsx, index=False, engine="openpyxl")
                    st.download_button(
                        "⬇️ Download Excel",
                        hist_xlsx.getvalue(),
                        f"enriched_{entry['filename'].rsplit('.', 1)[0]}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"hist_xlsx_{idx}",
                    )

        if st.button("🗑️ Clear History", use_container_width=True):
            st.session_state.run_history = []
            st.rerun()

else:
    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("""#### 🏛️ Companies House
        Aggressive UK company matching with address verification,
        multiple name variations, and postcode cross-checking.""")
    with c2:
        st.markdown("""#### 🌐 Website + Facebook
        Scrapes business websites and Facebook pages for owner names,
        emails, and personal phone numbers.""")
    with c3:
        st.markdown("""#### 🔄 Cross-Referencing
        Verifies owner names across multiple sources including
        business name analysis. Confidence-scored results.""")
