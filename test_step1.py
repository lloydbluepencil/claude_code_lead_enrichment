"""
test_step1.py — Tests Step 1 (location resolution) with ONE company.
Prints the raw API response so you can verify before proceeding.

Run: python test_step1.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

PRONTO_BASE = "https://app.prontohq.com/api/v2"
API_KEY = os.getenv("PRONTOHQ_API_KEY", "")

# ── Test company ─────────────────────────────────────────────────────────────
TEST_COMPANY = {
    "company_name": "Acme Corp",
    "domain": "acme.com",
    "company_linkedin_url": "https://www.linkedin.com/company/acme",
    "address": "Dubai, UAE",
}
# ─────────────────────────────────────────────────────────────────────────────


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

def parse_country(address: str) -> str:
    if not address:
        return ""
    raw = address.split(",")[-1].strip()
    return COUNTRY_ABBREVIATIONS.get(raw.upper(), raw)


def test_location():
    company_name = TEST_COMPANY["company_name"]
    address = TEST_COMPANY["address"]
    country = parse_country(address)

    print(f"\n{'='*60}")
    print(f"Company : {company_name}")
    print(f"Address : {address}")
    print(f"Parsed country : {country!r}")
    print(f"{'='*60}")

    url = f"{PRONTO_BASE}/locations"
    headers = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
    body = {"name": country, "hiring": False}

    print(f"\nPOST {url}")
    print(f"Body: {json.dumps(body, indent=2)}")
    print()

    resp = requests.post(url, json=body, headers=headers, timeout=30)

    print(f"Status code : {resp.status_code}")
    print(f"\n--- RAW RESPONSE ---")
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)

    print(f"\n--- ANALYSIS ---")
    if resp.status_code == 200:
        locations = resp.json()
        if isinstance(locations, list) and locations:
            best = locations[0]
            loc_id = best.get("id") or best.get("_id")
            print(f"Returned {len(locations)} location(s)")
            print(f"PICKING FIRST: id={loc_id}  name={best.get('name')!r}  type={best.get('type')!r}")
        else:
            print("Empty or unexpected response shape")
    else:
        print("Non-200 response — check API key and endpoint.")


if __name__ == "__main__":
    if not API_KEY:
        print("ERROR: PRONTOHQ_API_KEY not set in .env")
    else:
        test_location()
