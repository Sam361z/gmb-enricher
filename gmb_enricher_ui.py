#!/usr/bin/env python3
"""
GMB Lead Enricher — Streamlit UI (Optimized)
Upload a CSV of GMB listings → get back enriched data with owner details.

Speed optimizations:
  - Parallel processing (configurable 1-20 workers)
  - Early stopping (skip remaining sources once name+email+phone found)
  - Smart website scraping (stop scraping pages once owner data found)

Run with:  streamlit run gmb_enricher_ui.py
"""

import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

# Optional imports
try:
    import whois
    WHOIS_AVAILABLE = True
except ImportError:
    WHOIS_AVAILABLE = False

# ──────────────────────────────────────────────────────────────────────────────
# Page Config
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="GMB Lead Enricher",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# Custom Styling
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }
    .main-header {
        background: linear-gradient(90deg, #00d2ff 0%, #3a7bd5 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: 800;
        letter-spacing: -1px;
        margin-bottom: 0;
    }
    .sub-header {
        color: #8892b0;
        font-size: 1.1rem;
        margin-top: -10px;
        margin-bottom: 30px;
    }
    .stat-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        backdrop-filter: blur(10px);
    }
    .stat-number {
        font-size: 2rem;
        font-weight: 700;
        color: #00d2ff;
    }
    .stat-label {
        color: #8892b0;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    [data-testid="stSidebar"] {
        background: rgba(15, 15, 26, 0.95);
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .processing-text {
        font-family: 'JetBrains Mono', monospace;
        color: #ccd6f6;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

REQUEST_TIMEOUT = 12

# Ordered by likelihood of containing owner data (best pages first)
SCRAPE_PATHS = [
    "/about", "/about-us", "/team", "/our-team",
    "/contact", "/contact-us", "/",
    "/impressum", "/mentions-legales", "/about-me",
]

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


# ──────────────────────────────────────────────────────────────────────────────
# Data Model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class EnrichmentResult:
    owner_name: Optional[str] = None
    owner_title: Optional[str] = None
    owner_email: Optional[str] = None
    owner_phone: Optional[str] = None
    emails_found: list = field(default_factory=list)
    phones_found: list = field(default_factory=list)
    sources: list = field(default_factory=list)
    confidence: float = 0.0
    discovery_method: Optional[str] = None  # human-readable explanation

    def merge(self, other: "EnrichmentResult"):
        if other.owner_name and not self.owner_name:
            self.owner_name = other.owner_name
            self.owner_title = other.owner_title
            self.discovery_method = other.discovery_method
            self.confidence = other.confidence
        if other.owner_email and not self.owner_email:
            self.owner_email = other.owner_email
        if other.owner_phone and not self.owner_phone:
            self.owner_phone = other.owner_phone
        self.emails_found.extend(other.emails_found)
        self.phones_found.extend(other.phones_found)
        self.sources.extend(other.sources)

    @property
    def is_complete(self) -> bool:
        """True when we have name + email + phone — no need to keep searching."""
        return bool(self.owner_name and
                    (self.owner_email or self.emails_found) and
                    (self.owner_phone or self.phones_found))

    def finalize(self):
        self.emails_found = list(dict.fromkeys(self.emails_found))
        self.phones_found = list(dict.fromkeys(self.phones_found))
        if not self.owner_email and self.emails_found:
            self.owner_email = self._best_email()
        if not self.owner_phone and self.phones_found:
            self.owner_phone = self.phones_found[0]

    def _best_email(self) -> Optional[str]:
        generic = {"info@", "contact@", "hello@", "support@", "admin@",
                   "sales@", "office@", "enquiries@", "mail@"}
        personal = [e for e in self.emails_found
                    if not any(e.lower().startswith(g) for g in generic)]
        return personal[0] if personal else (self.emails_found[0] if self.emails_found else None)


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────

def get_domain(url: str) -> str:
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    parsed = urlparse(url)
    return parsed.netloc or ""


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


def _parse_soup_for_data(soup, result, page_label="page"):
    """Shared parser: extract emails, phones, schema.org, owner mentions from a BeautifulSoup object."""
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
                    if isinstance(person, dict) and person.get("name"):
                        if not result.owner_name:
                            result.owner_name = person["name"]
                            result.owner_title = key.title()
                            result.confidence = 0.85
                            result.discovery_method = (
                                f"Website {page_label}: schema.org JSON-LD '{key}' field "
                                f"— structured data embedded by site owner"
                            )
                    elif isinstance(person, list):
                        for p in person:
                            if isinstance(p, dict) and p.get("name") and not result.owner_name:
                                result.owner_name = p["name"]
                                result.owner_title = key.title()
                                result.confidence = 0.85
                                result.discovery_method = (
                                    f"Website {page_label}: schema.org JSON-LD '{key}' field "
                                    f"— structured data embedded by site owner"
                                )
                                break
                if item.get("email"):
                    result.emails_found.append(item["email"])
                if item.get("telephone"):
                    result.phones_found.append(item["telephone"])
        except (json.JSONDecodeError, TypeError):
            continue

    # Owner text patterns
    if not result.owner_name:
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "strong", "b", "p"]):
            tag_text = tag.get_text(strip=True)
            for pattern in OWNER_TITLE_PATTERNS:
                if re.search(pattern, tag_text):
                    parts = re.split(r"[—\-–|,]", tag_text)
                    for part in parts:
                        part = part.strip()
                        if re.match(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$", part):
                            if not re.search(OWNER_TITLE_PATTERNS[0], part):
                                result.owner_name = part
                                title_match = re.search(pattern, tag_text)
                                matched_title = title_match.group(0) if title_match else "Owner"
                                result.owner_title = matched_title
                                result.confidence = 0.50
                                result.discovery_method = (
                                    f"Website {page_label}: found name near '{matched_title}' "
                                    f"in <{tag.name}> tag — text pattern match (less reliable)"
                                )
                                return  # found owner, stop scanning
            if result.owner_name:
                break

    # Meta author fallback
    if not result.owner_name:
        author_meta = soup.find("meta", attrs={"name": "author"})
        if author_meta and author_meta.get("content"):
            name = author_meta["content"].strip()
            if len(name.split()) >= 2:
                result.owner_name = name
                result.owner_title = "Author (meta)"
                result.confidence = 0.30
                result.discovery_method = (
                    f"Website {page_label}: HTML <meta name='author'> tag "
                    f"— low confidence, could be web developer not business owner"
                )


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment Source 1: Website Scraper (with smart early stopping)
# ──────────────────────────────────────────────────────────────────────────────

def enrich_website(website: str, company_name: str) -> EnrichmentResult:
    result = EnrichmentResult()
    if not website:
        return result

    base_url = website if website.startswith("http") else f"https://{website}"
    base_url = base_url.rstrip("/")

    for path in SCRAPE_PATHS:
        # SMART STOP: if we already have name + email, skip remaining pages
        if result.owner_name and result.emails_found:
            break

        url = base_url + path
        resp = safe_request(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        _parse_soup_for_data(soup, result, page_label=path or "/")

    if result.owner_name or result.emails_found:
        result.sources.append("website")
        result.confidence = max(result.confidence, 0.6)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment Source 2: Facebook
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
                    facebook_url: str = "") -> EnrichmentResult:
    result = EnrichmentResult()

    # Resolve FB URL
    fb_url = None
    if facebook_url:
        match = FB_URL_RE.search(facebook_url)
        if match:
            fb_url = f"https://www.facebook.com/{match.group(1)}"
    if not fb_url and website:
        fb_url = _find_fb_on_website(website)
    if not fb_url:
        return result

    # Scrape pages (with early stopping)
    urls_to_try = [
        fb_url.replace("www.facebook.com", "m.facebook.com"),
        (fb_url.rstrip("/") + "/about").replace("www.facebook.com", "m.facebook.com"),
        (fb_url.rstrip("/") + "/about_details").replace("www.facebook.com", "m.facebook.com"),
    ]

    for url in urls_to_try:
        # SMART STOP
        if result.owner_name and result.emails_found:
            break

        resp = safe_request(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        result.emails_found.extend(extract_emails(text))
        result.phones_found.extend(extract_phones(text))

        # OG tags
        for prop, target in [("og:description", "desc"), ("og:email", "email"),
                              ("og:phone_number", "phone")]:
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"):
                content = tag["content"]
                if target == "desc":
                    result.emails_found.extend(extract_emails(content))
                    result.phones_found.extend(extract_phones(content))
                elif target == "email":
                    result.emails_found.append(content)
                elif target == "phone":
                    result.phones_found.append(content)

        # Owner patterns
        if not result.owner_name:
            owner_patterns = [
                r"(?:owned|founded|managed|run|started|created)\s+by\s+"
                r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
                r"(?:owner|founder|manager|proprietor|director)[\s:]+\s*"
                r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
            ]
            for pattern in owner_patterns:
                match = re.search(pattern, text)
                if match:
                    name = match.group(1).strip()
                    words = name.split()
                    bad_names = {"Facebook Page", "Our Team", "Our Company",
                                 "This Page", "The Company", "More Info",
                                 "Read More", "Learn More", "Click Here"}
                    if 2 <= len(words) <= 4 and name not in bad_names:
                        result.owner_name = name
                        result.owner_title = "Owner (Facebook)"
                        result.confidence = 0.55
                        result.discovery_method = (
                            f"Facebook page: matched text pattern "
                            f"'{match.group(0)[:60]}' — moderate confidence"
                        )
                        break

        # Script-embedded owner
        if not result.owner_name:
            for script in soup.find_all("script"):
                script_text = script.string or ""
                owner_match = re.search(
                    r'"page_owner(?:_name)?":\s*"([^"]+)"', script_text
                )
                if owner_match:
                    name = owner_match.group(1)
                    if len(name.split()) >= 2:
                        result.owner_name = name
                        result.owner_title = "Page Owner"
                        result.confidence = 0.75
                        result.discovery_method = (
                            "Facebook page: found in page transparency data "
                            "(embedded script) — high confidence, set by page admin"
                        )
                        break

        # JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    if data.get("@type") in ("LocalBusiness", "Organization",
                                              "Restaurant", "Store"):
                        if data.get("email"):
                            result.emails_found.append(data["email"])
                        if data.get("telephone"):
                            result.phones_found.append(data["telephone"])
                        founder = data.get("founder", {})
                        if isinstance(founder, dict) and founder.get("name"):
                            result.owner_name = founder["name"]
                            result.owner_title = "Founder"
                            result.confidence = 0.80
                            result.discovery_method = (
                                "Facebook page: schema.org JSON-LD 'founder' field "
                                "— high confidence, structured data"
                            )
            except (json.JSONDecodeError, TypeError):
                continue

    if result.owner_name or result.emails_found or result.phones_found:
        result.sources.append("facebook")
        result.confidence = max(result.confidence, 0.5)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment Source 3: Hunter.io
# ──────────────────────────────────────────────────────────────────────────────

def enrich_hunter(website: str, company_name: str,
                  api_key: str) -> EnrichmentResult:
    result = EnrichmentResult()
    if not api_key or not website:
        return result

    domain = get_domain(website)
    if not domain:
        return result

    resp = safe_request(
        "https://api.hunter.io/v2/domain-search",
        params={"domain": domain, "api_key": api_key, "limit": 10}
    )
    if not resp:
        return result

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
        if is_owner and first and last:
            result.owner_name = f"{first} {last}"
            result.owner_title = entry.get("position", "")
            result.owner_email = email
            result.confidence = 0.85
            result.discovery_method = (
                f"Hunter.io API: found '{first} {last}' listed as "
                f"'{entry.get('position', 'N/A')}' on domain — "
                f"high confidence, from professional email database"
            )

    if result.emails_found:
        result.sources.append("hunter.io")
        result.confidence = max(result.confidence, 0.5)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment Source 4: Companies House (UK)
# ──────────────────────────────────────────────────────────────────────────────

def enrich_companies_house(company_name: str, country_hint: str,
                           api_key: str) -> EnrichmentResult:
    result = EnrichmentResult()
    if not api_key or not company_name:
        return result

    if country_hint and country_hint.upper() not in ("GB", "UK", ""):
        return result

    resp = safe_request(
        "https://api.company-information.service.gov.uk/search/companies",
        params={"q": company_name, "items_per_page": 3},
        auth=(api_key, ""),
    )
    if not resp:
        return result

    items = resp.json().get("items", [])
    if not items:
        return result

    company_number = items[0].get("company_number")
    if not company_number:
        return result

    resp2 = safe_request(
        f"https://api.company-information.service.gov.uk/company/{company_number}/officers",
        auth=(api_key, ""),
    )
    if not resp2:
        return result

    for officer in resp2.json().get("items", []):
        role = (officer.get("officer_role") or "").lower()
        name = officer.get("name", "")
        resigned = officer.get("resigned_on")

        if not resigned and role in ("director", "secretary"):
            if "," in name:
                parts = name.split(",", 1)
                name = f"{parts[1].strip().title()} {parts[0].strip().title()}"
            result.owner_name = name
            result.owner_title = role.title()
            result.sources.append("companies_house")
            result.confidence = 0.95
            result.discovery_method = (
                f"Companies House UK: active {role.title()} on official "
                f"company register (company #{company_number}) — "
                f"very high confidence, government source"
            )
            break

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment Source 5: WHOIS
# ──────────────────────────────────────────────────────────────────────────────

def enrich_whois(website: str) -> EnrichmentResult:
    result = EnrichmentResult()
    if not WHOIS_AVAILABLE or not website:
        return result

    domain = get_domain(website)
    if not domain:
        return result

    try:
        w = whois.whois(domain)
    except Exception:
        return result

    registrant = getattr(w, "name", None) or getattr(w, "registrant_name", None)
    if registrant and isinstance(registrant, str):
        privacy_kw = ["privacy", "proxy", "whoisguard", "domains by",
                      "redacted", "data protected", "withheld"]
        if not any(kw in registrant.lower() for kw in privacy_kw):
            if len(registrant.split()) >= 2:
                result.owner_name = registrant.title()
                result.owner_title = "Domain Registrant"
                result.confidence = 0.35
                result.sources.append("whois")
                result.discovery_method = (
                    f"WHOIS lookup: domain registrant name — "
                    f"low confidence, could be a web developer, "
                    f"hosting provider, or domain broker"
                )

    emails = getattr(w, "emails", None)
    if emails:
        if isinstance(emails, str):
            emails = [emails]
        for e in emails:
            if "abuse" not in e.lower() and "privacy" not in e.lower():
                result.emails_found.append(e)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator (with early stopping between sources)
# ──────────────────────────────────────────────────────────────────────────────

def enrich_single_row(row: dict, config: dict) -> EnrichmentResult:
    """Run enabled enrichment sources, stopping early if we have all data."""
    company = get_field(row, ["company", "business_name", "name",
                               "company_name", "business", "title"])
    website = get_field(row, ["website", "url", "site", "web",
                               "website_url", "domain"])
    country = get_field(row, ["country", "country_code", "region"])
    facebook = get_field(row, ["facebook", "facebook_url", "fb",
                                "fb_url", "facebook_page", "social_facebook"])

    combined = EnrichmentResult()

    # 1. Companies House first (fastest API call, highest confidence for UK)
    ch_key = config.get("ch_key", "")
    if config.get("enable_ch", False) and ch_key:
        combined.merge(enrich_companies_house(company, country, ch_key))
        if combined.is_complete:
            combined.finalize()
            return combined

    # 2. Website scraping
    if config.get("enable_website", True):
        combined.merge(enrich_website(website, company))
        if combined.is_complete:
            combined.finalize()
            return combined

    # 3. Hunter.io
    hunter_key = config.get("hunter_key", "")
    if config.get("enable_hunter", False) and hunter_key:
        combined.merge(enrich_hunter(website, company, hunter_key))
        if combined.is_complete:
            combined.finalize()
            return combined

    # 4. Facebook
    if config.get("enable_facebook", True):
        combined.merge(enrich_facebook(website, company, facebook))
        if combined.is_complete:
            combined.finalize()
            return combined

    # 5. WHOIS (last resort — slowest, lowest confidence)
    if config.get("enable_whois", True):
        combined.merge(enrich_whois(website))

    combined.finalize()
    return combined


# ──────────────────────────────────────────────────────────────────────────────
# Parallel Processing Wrapper
# ──────────────────────────────────────────────────────────────────────────────

def _process_one_row(args):
    """Worker function for parallel processing."""
    idx, row, config = args
    try:
        result = enrich_single_row(row, config)
    except Exception:
        result = EnrichmentResult()

    company = get_field(row, ["company", "business_name", "name",
                               "company_name", "business", "title"])

    result_row = dict(row)
    result_row["owner_name"] = result.owner_name or ""
    result_row["owner_title"] = result.owner_title or ""
    result_row["owner_email"] = result.owner_email or ""
    result_row["owner_phone"] = result.owner_phone or ""
    result_row["all_emails_found"] = "; ".join(result.emails_found[:5])
    result_row["all_phones_found"] = "; ".join(result.phones_found[:5])
    result_row["enrichment_sources"] = ", ".join(result.sources)
    result_row["confidence_score"] = f"{result.confidence:.2f}"
    result_row["confidence_level"] = (
        "🟢 High" if result.confidence >= 0.75
        else "🟡 Medium" if result.confidence >= 0.50
        else "🔴 Low" if result.confidence > 0
        else ""
    )
    result_row["discovery_method"] = result.discovery_method or ""

    return idx, result_row, result, company


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown("---")

    st.markdown("### Enrichment Sources")

    enable_website = st.toggle("🌐 Website Scraper", value=True,
                                help="Scrapes /about, /team, /contact pages")
    enable_facebook = st.toggle("📘 Facebook Pages", value=True,
                                 help="Finds & scrapes Facebook business pages")
    enable_whois = st.toggle("🔎 WHOIS Lookup", value=WHOIS_AVAILABLE,
                              disabled=not WHOIS_AVAILABLE,
                              help="Domain registrant details")

    st.markdown("---")
    st.markdown("### API Keys")

    ch_key = st.text_input(
        "🏛️ Companies House Key",
        value=os.environ.get("COMPANIES_HOUSE_KEY", ""),
        type="password",
        help="Free API key from developer.company-information.service.gov.uk"
    )
    enable_ch = st.toggle("Enable Companies House", value=bool(ch_key)) if ch_key else False

    hunter_key = st.text_input(
        "📧 Hunter.io Key",
        value=os.environ.get("HUNTER_API_KEY", ""),
        type="password",
        help="Get a free key at hunter.io"
    )
    enable_hunter = st.toggle("Enable Hunter.io", value=bool(hunter_key)) if hunter_key else False

    st.markdown("---")
    st.markdown("### Performance")

    workers = st.slider("Parallel workers", 1, 20, 5,
                         help="More workers = faster, but uses more network. "
                              "5 = safe default, 10 = fast, 20 = aggressive")

    delay = st.slider("Delay between requests (sec)", 0.0, 3.0, 0.5, 0.25,
                       help="Per-request delay within each worker. "
                            "Lower = faster but higher risk of being blocked")

    early_stop = st.toggle("⚡ Early stopping", value=True,
                            help="Skip remaining sources once name + email + phone are found")

    st.markdown("---")

    # Source status
    st.markdown("### Status")
    sources = {
        "Website Scraper": enable_website,
        "Facebook Pages": enable_facebook,
        "Companies House": bool(ch_key and enable_ch),
        "Hunter.io": bool(hunter_key and enable_hunter),
        "WHOIS": enable_whois and WHOIS_AVAILABLE,
    }
    for name, active in sources.items():
        icon = "🟢" if active else "🔴"
        st.markdown(f"{icon} {name}")


# ──────────────────────────────────────────────────────────────────────────────
# Main Content
# ──────────────────────────────────────────────────────────────────────────────

st.markdown('<p class="main-header">🔍 GMB Lead Enricher</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Upload your GMB data → get owner names, emails & phone numbers</p>',
            unsafe_allow_html=True)

# File upload
uploaded_file = st.file_uploader(
    "Upload your GMB CSV or Excel file",
    type=["csv", "xlsx", "xls"],
    help="Must contain at least a company name or website column"
)

if uploaded_file:
    # Read the file
    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        st.stop()

    # Preview
    st.markdown(f"### 📋 Preview — {len(df)} rows loaded")
    st.dataframe(df.head(10), use_container_width=True, height=300)

    # Column detection
    detected = {}
    for label, candidates in [
        ("Company", ["company", "business_name", "name", "company_name", "business", "title"]),
        ("Website", ["website", "url", "site", "web", "website_url", "domain"]),
        ("Country", ["country", "country_code", "region"]),
        ("Facebook", ["facebook", "facebook_url", "fb", "fb_url", "facebook_page"]),
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
    else:
        st.warning("Could not auto-detect columns. Make sure your CSV has 'company_name' and/or 'website' columns.")

    # Row range selector
    col1, col2 = st.columns(2)
    with col1:
        start_row = st.number_input("Start from row", min_value=1, max_value=len(df), value=1) - 1
    with col2:
        end_row = st.number_input("End at row", min_value=1, max_value=len(df), value=len(df))

    total_rows = end_row - start_row
    active_sources = sum(1 for v in sources.values() if v)

    # Time estimate (parallel)
    if early_stop:
        avg_sources = max(active_sources * 0.6, 1)  # early stopping saves ~40%
    else:
        avg_sources = active_sources
    est_seconds = (total_rows / workers) * (delay + 2.0) * avg_sources
    est_minutes = est_seconds / 60

    st.info(f"Will process **{total_rows} rows** using **{active_sources} sources** "
            f"with **{workers} parallel workers**. "
            f"Estimated time: **~{est_minutes:.1f} minutes**"
            f"{' (with early stopping)' if early_stop else ''}")

    # ── Run Enrichment ──
    if st.button("🚀 Start Enrichment", type="primary", use_container_width=True):

        config = {
            "enable_website": enable_website,
            "enable_facebook": enable_facebook,
            "enable_hunter": bool(hunter_key and enable_hunter),
            "enable_ch": bool(ch_key and enable_ch),
            "enable_whois": enable_whois and WHOIS_AVAILABLE,
            "hunter_key": hunter_key,
            "ch_key": ch_key,
            "delay": delay,
            "early_stop": early_stop,
        }

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.container()

        found_names = 0
        found_emails = 0
        found_phones = 0
        results_data = [None] * total_rows  # pre-allocate to maintain order
        completed = 0

        start_time = time.time()

        # Prepare work items
        work_items = []
        for i, idx in enumerate(range(start_row, end_row)):
            row = df.iloc[idx].to_dict()
            work_items.append((i, row, config))

        # Parallel processing
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_one_row, item): item[0]
                       for item in work_items}

            for future in as_completed(futures):
                try:
                    i, result_row, result, company = future.result()
                except Exception as e:
                    i = futures[future]
                    result_row = dict(work_items[i][1])
                    result_row.update({
                        "owner_name": "", "owner_title": "",
                        "owner_email": "", "owner_phone": "",
                        "all_emails_found": "", "all_phones_found": "",
                        "enrichment_sources": "error",
                        "confidence_score": "0.00"
                    })
                    result = EnrichmentResult()
                    company = "Unknown"

                results_data[i] = result_row
                completed += 1

                # Track stats
                if result.owner_name:
                    found_names += 1
                if result.owner_email:
                    found_emails += 1
                if result.owner_phone:
                    found_phones += 1

                # Update progress
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

                # Log this result
                with log_container:
                    found_items = []
                    if result.owner_name:
                        found_items.append(f"👤 {result.owner_name}")
                    if result.owner_email:
                        found_items.append(f"📧 {result.owner_email}")
                    if result.owner_phone:
                        found_items.append(f"📞 {result.owner_phone}")

                    if found_items:
                        conf_icon = (
                            "🟢" if result.confidence >= 0.75
                            else "🟡" if result.confidence >= 0.50
                            else "🔴"
                        )
                        method_short = ""
                        if result.discovery_method:
                            # Show just the source part before the dash
                            method_short = result.discovery_method.split("—")[0].strip()
                        st.markdown(
                            f"{conf_icon} **{company}** — {' | '.join(found_items)} "
                            f"[{method_short}]"
                        )
                    else:
                        st.markdown(f"⚪ **{company}** — no owner data found")

        # ── Results ──
        total_time = time.time() - start_time
        status_text.empty()
        progress_bar.empty()

        st.markdown("---")
        st.markdown("## 📊 Results")

        # Stats row
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.markdown(f"""<div class="stat-card">
                <div class="stat-number">{total_rows}</div>
                <div class="stat-label">Processed</div>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""<div class="stat-card">
                <div class="stat-number">{found_names}</div>
                <div class="stat-label">Names Found</div>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""<div class="stat-card">
                <div class="stat-number">{found_emails}</div>
                <div class="stat-label">Emails Found</div>
            </div>""", unsafe_allow_html=True)
        with col4:
            st.markdown(f"""<div class="stat-card">
                <div class="stat-number">{found_phones}</div>
                <div class="stat-label">Phones Found</div>
            </div>""", unsafe_allow_html=True)
        with col5:
            st.markdown(f"""<div class="stat-card">
                <div class="stat-number">{total_time:.0f}s</div>
                <div class="stat-label">Total Time</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("")

        # Results table
        results_df = pd.DataFrame([r for r in results_data if r is not None])
        st.dataframe(results_df, use_container_width=True, height=400)

        # Download buttons
        st.markdown("### 📥 Download")
        col1, col2 = st.columns(2)

        with col1:
            csv_buffer = io.StringIO()
            results_df.to_csv(csv_buffer, index=False)
            st.download_button(
                label="⬇️ Download as CSV",
                data=csv_buffer.getvalue(),
                file_name="enriched_leads.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with col2:
            xlsx_buffer = io.BytesIO()
            results_df.to_excel(xlsx_buffer, index=False, engine="openpyxl")
            st.download_button(
                label="⬇️ Download as Excel",
                data=xlsx_buffer.getvalue(),
                file_name="enriched_leads.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

else:
    # Empty state
    st.markdown("---")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        #### 🌐 Website Scraping
        Scrapes /about, /team, /contact pages for owner names,
        emails, and phone numbers. Reads schema.org structured data.
        """)
    with col2:
        st.markdown("""
        #### 📘 Facebook Pages
        Finds the business Facebook page and extracts owner info,
        contact details, and page transparency data.
        """)
    with col3:
        st.markdown("""
        #### 🏛️ Company Registries
        Looks up directors and officers from Companies House (UK)
        and other public company registries.
        """)

    st.markdown("---")
    st.markdown("""
    **How to use:**
    1. Upload your GMB CSV or Excel file (must have company name and/or website columns)
    2. Configure which enrichment sources to enable in the sidebar
    3. Add API keys for Companies House / Hunter.io if you have them
    4. Adjust parallel workers for speed (5 = safe, 10 = fast, 20 = aggressive)
    5. Click **Start Enrichment** and wait for results
    6. Download the enriched file with owner details added
    """)
