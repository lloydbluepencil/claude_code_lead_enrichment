"""
test_step2.py — Two-part local test for Step 2 (no ngrok needed).

PART A: Calls /leads/search directly so you can see the raw API response
        and confirm the request shape is correct.

PART B: Starts a real job via POST /enrich (server must be running),
        waits 3 seconds, then manually POSTs fake people to the webhook
        to verify the event signaling and pipeline flow work end-to-end.

Run:
  Terminal 1:  uvicorn main:app --reload --port 8000
  Terminal 2:  python test_step2.py
"""

import os
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY  = os.getenv("PRONTOHQ_API_KEY", "")
BASE_URL = "http://localhost:8000"
PRONTO_BASE = "https://app.prontohq.com/api/v2"

TEST_COMPANY = {
    "company_name": "Acme Corp",
    "domain": "acme.com",
    "company_linkedin_url": "https://www.linkedin.com/company/acme",
    "address": "Dubai, UAE",
}

FILTERS = {
    "job_titles": ["CFO", "Head of Finance"],
    "excluded_job_titles": ["Intern", "Contractor"],
    "seniority_levels": ["220", "300"],
    "limit": 5,
}

# Fake people payload to simulate the ProntoHQ webhook callback
FAKE_PEOPLE = [
    {
        "first_name": "Jane",
        "last_name": "Doe",
        "full_name": "Jane Doe",
        "title": "CFO",
        "company_name": "Acme Corp",
        "linkedin_url": "https://www.linkedin.com/in/jane-doe-cfo",
        "linkedin_profile_url": "https://www.linkedin.com/in/jane-doe-cfo",
    }
]

DIVIDER = "=" * 60


def part_a_direct_search():
    """Call /leads/search directly — prints raw ProntoHQ response."""
    print(f"\n{DIVIDER}")
    print("PART A — Direct /leads/search call (raw ProntoHQ response)")
    print(DIVIDER)

    body = {
        "name": TEST_COMPANY["company_name"],
        "company_linkedin_url": TEST_COMPANY["company_linkedin_url"],
        "included_locations": ["104305776"],  # UAE id confirmed from Step 1
        "job_titles": FILTERS["job_titles"],
        "excluded_job_titles": FILTERS["excluded_job_titles"],
        "seniority_levels": FILTERS["seniority_levels"],
        "webhook_url": f"{BASE_URL}/webhook/test-job-id",
        "limit": FILTERS["limit"],
    }

    print(f"\nPOST {PRONTO_BASE}/leads/search")
    print(f"Body:\n{json.dumps(body, indent=2)}\n")

    resp = requests.post(
        f"{PRONTO_BASE}/leads/search",
        json=body,
        headers={"X-API-KEY": API_KEY, "Content-Type": "application/json"},
        timeout=30,
    )

    print(f"Status : {resp.status_code}")
    print(f"\n--- RAW RESPONSE ---")
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)


def part_b_local_pipeline():
    """Start a real job, then simulate the webhook to test local event flow."""
    print(f"\n{DIVIDER}")
    print("PART B — Full local pipeline with simulated webhook")
    print(DIVIDER)

    enrich_payload = {
        "companies": [TEST_COMPANY],
        "filters": FILTERS,
    }

    print(f"\nPOST {BASE_URL}/enrich")
    resp = requests.post(f"{BASE_URL}/enrich", json=enrich_payload)
    print(f"Status : {resp.status_code}")
    print(f"Body   : {json.dumps(resp.json(), indent=2)}")

    if resp.status_code not in (200, 202):
        print("ERROR: Could not start job. Is the server running?")
        return

    job_id = resp.json()["job_id"]
    print(f"\nJob ID : {job_id}")

    # Give the pipeline thread time to reach step2 and register the event
    print("\nWaiting 5s for pipeline to reach webhook-wait state…")
    time.sleep(5)

    print(f"\nSimulating ProntoHQ webhook → POST {BASE_URL}/webhook/{job_id}")
    wh_resp = requests.post(
        f"{BASE_URL}/webhook/{job_id}",
        json=FAKE_PEOPLE,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    print(f"Webhook response : {wh_resp.status_code} — {wh_resp.json()}")

    # Poll status — allow extra time for Step 3 enrich calls after webhook
    print("\nPolling status (up to 60s for Step 3 to finish)…")
    for _ in range(30):
        time.sleep(2)
        r = requests.get(f"{BASE_URL}/enrich/{job_id}/status")
        data = r.json()
        print(f"  status={data['status']}  progress={data['progress']}")
        if data["status"] in ("complete", "failed"):
            break

    # Results
    r = requests.get(f"{BASE_URL}/enrich/{job_id}/results")
    print(f"\n--- RESULTS ---")
    print(json.dumps(r.json(), indent=2))


if __name__ == "__main__":
    if not API_KEY:
        print("ERROR: PRONTOHQ_API_KEY not set in .env")
    else:
        part_a_direct_search()
        part_b_local_pipeline()
