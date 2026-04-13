"""
pipeline.py — ProntoHQ enrichment pipeline (Steps 1–3)
"""

import os
import threading
import time
import logging
import requests
from typing import Any, Optional

logger = logging.getLogger(__name__)

PRONTO_BASE = "https://app.prontohq.com/api/v2"

SENIORITY_LABEL_MAP = {
    "Senior": "100",
    "Manager": "110",
    "Director": "200",
    "VP": "220",
    "C-Suite": "300",
}

VALID_SENIORITY_IDS = set(SENIORITY_LABEL_MAP.values())

COUNTRY_ABBREVIATIONS = {
    "UAE": "United Arab Emirates",
    "UK": "United Kingdom",
    "USA": "United States",
    "US": "United States",
    "KSA": "Saudi Arabia",
    "RSA": "South Africa",
    "ROI": "Ireland",
    "NZ": "New Zealand",
    "AU": "Australia",
}

# City → Country fallback for common single-segment addresses
CITY_TO_COUNTRY = {
    "DUBAI": "United Arab Emirates",
    "ABU DHABI": "United Arab Emirates",
    "SHARJAH": "United Arab Emirates",
    "RIYADH": "Saudi Arabia",
    "JEDDAH": "Saudi Arabia",
    "DOHA": "Qatar",
    "KUWAIT CITY": "Kuwait",
    "MANAMA": "Bahrain",
    "MUSCAT": "Oman",
    "CAIRO": "Egypt",
    "LONDON": "United Kingdom",
    "MANCHESTER": "United Kingdom",
    "NEW YORK": "United States",
    "LOS ANGELES": "United States",
    "SINGAPORE": "Singapore",
    "SYDNEY": "Australia",
    "MELBOURNE": "Australia",
    "TORONTO": "Canada",
    "DUBAI CITY": "United Arab Emirates",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_key() -> str:
    key = os.getenv("PRONTOHQ_API_KEY", "")
    if not key:
        raise RuntimeError("PRONTOHQ_API_KEY is not set")
    return key


def _pronto_post(path: str, body: dict) -> requests.Response:
    """POST to ProntoHQ with exponential backoff on 429."""
    url = f"{PRONTO_BASE}{path}"
    headers = {"X-API-KEY": _api_key(), "Content-Type": "application/json"}
    delay = 1
    for attempt in range(4):  # 0, 1, 2, 3
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=30)
            if resp.status_code == 429 and attempt < 3:
                logger.warning(f"429 rate-limit on {path}, retrying in {delay}s…")
                time.sleep(delay)
                delay *= 2
                continue
            return resp
        except requests.RequestException as exc:
            if attempt < 3:
                logger.warning(f"Request error on {path}: {exc}, retrying in {delay}s…")
                time.sleep(delay)
                delay *= 2
                continue
            raise
    raise RuntimeError(f"All retries exhausted for {path}")


def resolve_seniority_ids(seniority_levels: list) -> list:
    resolved = []
    for level in seniority_levels:
        if level in VALID_SENIORITY_IDS:
            resolved.append(level)
        elif level in SENIORITY_LABEL_MAP:
            resolved.append(SENIORITY_LABEL_MAP[level])
        else:
            logger.warning(f"Unknown seniority level: {level!r} — skipping")
    return resolved


def parse_country(address: str) -> str:
    """Extract country from address, expanding abbreviations and known cities."""
    if not address:
        return ""
    raw = address.split(",")[-1].strip()
    upper = raw.upper()
    if upper in COUNTRY_ABBREVIATIONS:
        return COUNTRY_ABBREVIATIONS[upper]
    if upper in CITY_TO_COUNTRY:
        return CITY_TO_COUNTRY[upper]
    return raw


# ---------------------------------------------------------------------------
# Step 1 — Resolve location ID
# ---------------------------------------------------------------------------

def step1_resolve_location(company_name: str, address: str) -> Optional[str]:
    """
    POST /locations with the country parsed from address.
    Returns the best-matching location ID, or None if not found.
    """
    country = parse_country(address)
    if not country:
        logger.warning(f"[{company_name}] Could not parse country from address: {address!r}")
        return None

    logger.info(f"[{company_name}] Resolving location for country: {country!r}")

    try:
        resp = _pronto_post("/locations", {"name": country, "hiring": False})
    except Exception as exc:
        logger.error(f"[{company_name}] Location API error: {exc}")
        return None

    logger.info(f"[{company_name}] Location API status: {resp.status_code}")

    if resp.status_code != 200:
        logger.error(f"[{company_name}] Location API non-200: {resp.status_code} — {resp.text}")
        return None

    try:
        locations = resp.json()
    except Exception:
        logger.error(f"[{company_name}] Failed to parse location response JSON")
        return None

    if not isinstance(locations, list) or len(locations) == 0:
        logger.warning(f"[{company_name}] No locations returned for {country!r}")
        return None

    best = locations[0]
    loc_id = str(best.get("id") or best.get("_id") or "")
    logger.info(f"[{company_name}] Resolved location: {best.get('name')!r} → id={loc_id!r}")
    return loc_id if loc_id else None


# ---------------------------------------------------------------------------
# Step 2 — Find people at company (async search + webhook wait)
# ---------------------------------------------------------------------------

def step2_find_people(
    job_id: str,
    company: dict,
    location_id: str,
    filters: dict,
    company_index: int,
    job_store: dict,
    lock: threading.Lock,
) -> Optional[list]:
    """
    POST /leads/search with webhook_url pointing back to this server.
    Blocks on threading.Event until webhook fires or 120s timeout.
    Returns list of people, or None on timeout/error.
    """
    base_url = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")
    webhook_url = f"{base_url}/webhook/{job_id}"
    company_name = company.get("company_name", "Unknown")

    seniority = filters.get("seniority_levels") or []

    body = {
        "name": company.get("company_name", ""),
        "company_linkedin_url": company.get("company_linkedin_url", ""),
        "included_locations": [location_id],
        "job_titles": filters.get("job_titles", []),
        "excluded_job_titles": filters.get("excluded_job_titles", []),
        "custom": {"seniority": seniority},
        "webhook_url": webhook_url,
        "streaming": True,
        "limit": filters.get("limit", 25),
    }

    logger.info(f"[{job_id}] {company_name} — posting to /leads/search, webhook={webhook_url}")

    try:
        resp = _pronto_post("/leads/search", body)
    except Exception as exc:
        logger.error(f"[{job_id}] {company_name} — leads/search error: {exc}")
        return None

    logger.info(f"[{job_id}] {company_name} — leads/search status: {resp.status_code}")

    if resp.status_code not in (200, 201, 202):
        logger.error(f"[{job_id}] {company_name} — leads/search error: {resp.status_code} — {resp.text}")
        return None

    # Block until webhook fires
    with lock:
        event: threading.Event = job_store[job_id]["events"][company_index]

    fired = event.wait(timeout=300)

    if not fired:
        logger.warning(f"[{job_id}] {company_name} — webhook timeout after 300s (search_timeout)")
        return None

    with lock:
        people = job_store[job_id]["webhook_data"].get(company_index, [])

    return people


# ---------------------------------------------------------------------------
# Step 3 — Enrich a single person
# ---------------------------------------------------------------------------

def step3_enrich_person(job_id: str, person: dict, source_company: dict) -> Optional[dict]:
    """
    POST /leads/single_enrich for one person.
    Returns a fully-shaped contact dict, or None on error.
    """
    first_name = person.get("first_name", "")
    last_name  = person.get("last_name", "")
    company_name = person.get("company_name") or source_company.get("company_name", "")
    linkedin_url = person.get("linkedin_url") or person.get("linkedin_profile_url", "")

    logger.info(f"[{job_id}] Enriching {first_name} {last_name} @ {company_name}")

    try:
        resp = _pronto_post("/leads/single_enrich", {
            "first_name":    first_name,
            "last_name":     last_name,
            "company_name":  company_name,
            "linkedin_url":  linkedin_url,
        })
    except Exception as exc:
        logger.error(f"[{job_id}] single_enrich request error: {exc}")
        return None

    logger.info(f"[{job_id}] single_enrich status: {resp.status_code}")

    if resp.status_code not in (200, 201):
        logger.error(f"[{job_id}] single_enrich error {resp.status_code}: {resp.text}")
        return None

    import json as _json
    print(f"\n{'='*60}")
    print(f"[STEP 3 RAW ENRICH RESPONSE] {first_name} {last_name}")
    print(_json.dumps(resp.json(), indent=2))
    print(f"{'='*60}\n")

    try:
        data = resp.json()
    except Exception:
        logger.error(f"[{job_id}] Failed to parse single_enrich response")
        return None

    status               = data.get("status", "")
    most_probable_email  = data.get("most_probable_email") or ""
    phones               = data.get("phones") or []
    phone                = phones[0] if phones else ""
    current_pos          = data.get("current_position") or {}

    # Classification
    if most_probable_email and status == "QUALIFIED":
        contact_classification = "In-market"
    else:
        contact_classification = "Out-market"

    # Enrichment status
    if status == "QUALIFIED":
        enrichment_status = "Qualified"
    elif status == "DISQUALIFIED":
        enrichment_status = "Disqualified"
    elif not most_probable_email:
        enrichment_status = "No Email"
    else:
        enrichment_status = status.title() if status else "Unknown"

    return {
        "full_name":                  data.get("full_name", f"{first_name} {last_name}".strip()),
        "first_name":                 data.get("first_name", first_name),
        "last_name":                  data.get("last_name", last_name),
        "title":                      data.get("title", person.get("title", "")),
        "company_name":               data.get("company_name") or current_pos.get("companyName") or company_name,
        "linkedin_profile_url":       data.get("linkedin_profile_url", linkedin_url),
        "most_probable_email":        most_probable_email,
        "most_probable_email_status": data.get("most_probable_email_status", ""),
        "phone":                      phone,
        "years_in_position":          data.get("years_in_position"),
        "years_in_company":           data.get("years_in_company"),
        "contact_classification":     contact_classification,
        "enrichment_status":          enrichment_status,
        "source_company":             source_company.get("company_name", ""),
    }


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(job_id: str, job_store: dict, lock: threading.Lock, request_data: dict):
    """Background thread — processes each company through Steps 1–3."""
    companies = request_data["companies"]
    filters = request_data["filters"]

    for idx, company in enumerate(companies):
        try:
            _process_company(job_id, idx, company, filters, job_store, lock)
        except Exception as exc:
            logger.error(
                f"[{job_id}] Unhandled error processing "
                f"{company.get('company_name')}: {exc}"
            )

    with lock:
        job_store[job_id]["status"] = "complete"

    logger.info(f"[{job_id}] Pipeline complete.")


def _process_company(
    job_id: str, company_index: int, company: dict, filters: dict, job_store: dict, lock: threading.Lock
):
    """Process a single company through all pipeline steps."""
    company_name = company.get("company_name", "Unknown")
    logger.info(f"[{job_id}] Processing company: {company_name}")

    # Register event for this company index upfront
    with lock:
        job_store[job_id]["events"][company_index] = threading.Event()

    # STEP 1 — resolve location
    address = company.get("address", "")
    location_id = step1_resolve_location(company_name, address)

    if location_id is None:
        logger.warning(f"[{job_id}] Skipping {company_name}: location_not_found")
        with lock:
            job_store[job_id]["progress"]["companies_processed"] += 1
        return

    logger.info(f"[{job_id}] {company_name} → location_id={location_id}")

    # STEP 2 — find people + wait for webhook
    people = step2_find_people(
        job_id=job_id,
        company=company,
        location_id=location_id,
        filters=filters,
        company_index=company_index,
        job_store=job_store,
        lock=lock,
    )

    if people is None:
        # Timeout or API error — already logged inside step2
        with lock:
            job_store[job_id]["progress"]["companies_processed"] += 1
        return

    logger.info(f"[{job_id}] {company_name} → {len(people)} people received from webhook")

    with lock:
        job_store[job_id]["progress"]["people_found"] += len(people)

    # STEP 3 — enrich each person
    for person in people:
        try:
            contact = step3_enrich_person(job_id, person, company)
            if contact:
                with lock:
                    job_store[job_id]["results"].append(contact)
                    job_store[job_id]["progress"]["emails_enriched"] += (
                        1 if contact.get("most_probable_email") else 0
                    )
        except Exception as exc:
            logger.error(f"[{job_id}] Enrich error for {person.get('first_name')} {person.get('last_name')}: {exc}")

    with lock:
        job_store[job_id]["progress"]["companies_processed"] += 1
