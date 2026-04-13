"""
test_step3.py — Calls /leads/single_enrich directly with a real person.
Edit PERSON below with someone you know is in ProntoHQ (real name + LinkedIn URL).

Run: python test_step3.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY     = os.getenv("PRONTOHQ_API_KEY", "")
PRONTO_BASE = "https://app.prontohq.com/api/v2"

# ── Edit this with a real person ─────────────────────────────────────────────
PERSON = {
    "first_name":   "Lloyd",
    "last_name":    "G",
    "company_name": "BluePencil",
    "linkedin_url": "www.linkedin.com/in/lloyd-g-609970347",
}
# ─────────────────────────────────────────────────────────────────────────────

DIVIDER = "=" * 60


def main():
    print(f"\n{DIVIDER}")
    print("Calling /leads/single_enrich")
    print(DIVIDER)
    print(f"\nPerson : {PERSON}")

    resp = requests.post(
        f"{PRONTO_BASE}/leads/single_enrich",
        json=PERSON,
        headers={"X-API-KEY": API_KEY, "Content-Type": "application/json"},
        timeout=30,
    )

    print(f"\nStatus : {resp.status_code}")
    print(f"\n--- RAW RESPONSE ---")
    try:
        data = resp.json()
        print(json.dumps(data, indent=2))
    except Exception:
        print(resp.text)
        return

    if resp.status_code not in (200, 201):
        print("\nNon-success status — check person details or API key.")
        return

    print(f"\n--- MAPPED CONTACT ---")
    status              = data.get("status", "")
    email               = data.get("most_probable_email") or ""
    phones              = data.get("phones") or []
    current_pos         = data.get("current_position") or {}

    contact = {
        "full_name":                  data.get("full_name", ""),
        "first_name":                 data.get("first_name", PERSON["first_name"]),
        "last_name":                  data.get("last_name", PERSON["last_name"]),
        "title":                      data.get("title", ""),
        "company_name":               data.get("company_name") or current_pos.get("companyName") or PERSON["company_name"],
        "linkedin_profile_url":       data.get("linkedin_profile_url", PERSON["linkedin_url"]),
        "most_probable_email":        email,
        "most_probable_email_status": data.get("most_probable_email_status", ""),
        "phone":                      phones[0] if phones else "",
        "years_in_position":          data.get("years_in_position"),
        "years_in_company":           data.get("years_in_company"),
        "contact_classification":     "In-market" if (email and status == "QUALIFIED") else "Out-market",
        "enrichment_status":          "Qualified" if status == "QUALIFIED" else ("Disqualified" if status == "DISQUALIFIED" else "No Email"),
        "source_company":             PERSON["company_name"],
    }

    print(json.dumps(contact, indent=2))


if __name__ == "__main__":
    if not API_KEY:
        print("ERROR: PRONTOHQ_API_KEY not set in .env")
    else:
        main()
