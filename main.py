import uuid
import threading
import logging
import csv
import io
from typing import Any

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv

from models import (
    EnrichRequest,
    EnrichStartResponse,
    JobStatusResponse,
    Progress,
    Contact,
)
from pipeline import run_pipeline

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Contact Enrichment API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# In-memory job store  { job_id: { status, progress, results, events, lock } }
# ---------------------------------------------------------------------------
job_store: dict[str, Any] = {}
store_lock = threading.Lock()


def _make_job(total_companies: int) -> dict:
    return {
        "status": "processing",
        "progress": {
            "companies_processed": 0,
            "companies_total": total_companies,
            "people_found": 0,
            "emails_enriched": 0,
        },
        "results": [],
        "events": {},      # company_index -> threading.Event
        "webhook_data": {},  # company_index -> list[people]
    }


# ---------------------------------------------------------------------------
# POST /enrich
# ---------------------------------------------------------------------------
@app.post("/enrich", response_model=EnrichStartResponse, status_code=202)
def start_enrich(body: EnrichRequest):
    if not body.companies:
        raise HTTPException(status_code=400, detail="companies must not be empty")
    if not body.filters.job_titles:
        raise HTTPException(status_code=400, detail="filters.job_titles must not be empty")

    job_id = str(uuid.uuid4())
    total = len(body.companies)

    with store_lock:
        job_store[job_id] = _make_job(total)

    # Serialize to plain dicts so the thread doesn't hold Pydantic model refs
    request_data = {
        "companies": [c.model_dump() for c in body.companies],
        "filters": body.filters.model_dump(),
    }

    thread = threading.Thread(
        target=run_pipeline,
        args=(job_id, job_store, store_lock, request_data),
        daemon=True,
    )
    thread.start()

    logger.info(f"Job {job_id} started for {total} companies.")
    return EnrichStartResponse(job_id=job_id, status="processing", total_companies=total)


# ---------------------------------------------------------------------------
# GET /enrich/{job_id}/status
# ---------------------------------------------------------------------------
@app.get("/enrich/{job_id}/status", response_model=JobStatusResponse)
def get_status(job_id: str):
    with store_lock:
        job = job_store.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(
        job_id=job_id,
        status=job["status"],
        progress=Progress(**job["progress"]),
    )


CSV_FIELDS = [
    "full_name", "first_name", "last_name", "title", "company_name",
    "linkedin_profile_url", "most_probable_email", "most_probable_email_status",
    "phone", "years_in_position", "years_in_company",
    "contact_classification", "enrichment_status", "source_company",
]


# ---------------------------------------------------------------------------
# GET /enrich/{job_id}/results        → JSON (default)
# GET /enrich/{job_id}/results?format=csv  → CSV download
# ---------------------------------------------------------------------------
@app.get("/enrich/{job_id}/results")
def get_results(job_id: str, format: str = "json"):
    with store_lock:
        job = job_store.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] != "complete":
        return {"status": job["status"]}

    results = job["results"]

    if format.lower() == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=enrichment_{job_id}.csv"},
        )

    return results


# ---------------------------------------------------------------------------
# POST /webhook/{job_id}
# ---------------------------------------------------------------------------
@app.post("/webhook/{job_id}")
async def webhook(job_id: str, request: Request):
    with store_lock:
        job = job_store.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        payload = await request.json()
    except Exception:
        payload = []

    import json as _json
    logger.info(f"[{job_id}] Webhook raw payload: {_json.dumps(payload)[:500]}")

    with store_lock:
        events: dict = job["events"]
        active_idx = next((i for i, e in events.items() if not e.is_set()), None)

        if active_idx is None:
            return {"received": True}

        # Streaming mode: ProntoHQ sends one person per call, then a final
        # completion call with a "leads" key (leads: []) to signal done.
        if isinstance(payload, dict) and "leads" in payload:
            # Completion signal — fire the event with whatever we've accumulated
            accumulated = job["webhook_data"].get(active_idx, [])
            logger.info(f"[{job_id}] Stream complete — {len(accumulated)} people accumulated.")
            events[active_idx].set()

        elif isinstance(payload, dict) and ("first_name" in payload or "linkedin_profile_url" in payload):
            # Individual streamed person — accumulate, don't fire event yet
            if active_idx not in job["webhook_data"]:
                job["webhook_data"][active_idx] = []
            job["webhook_data"][active_idx].append(payload)
            logger.info(f"[{job_id}] Streamed person #{len(job['webhook_data'][active_idx])}: {payload.get('full_name', '')}")

        elif isinstance(payload, list):
            # Non-streaming batch response (fallback)
            job["webhook_data"][active_idx] = payload
            logger.info(f"[{job_id}] Batch webhook: {len(payload)} people.")
            events[active_idx].set()

        else:
            logger.warning(f"[{job_id}] Unrecognised webhook shape, ignoring.")

    return {"received": True}
