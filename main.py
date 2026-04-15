import uuid
import threading
import logging
import csv
import io
import json as _json
from typing import Any

from fastapi import FastAPI, HTTPException, Request
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
from pipeline import run_pipeline, step3_enrich_person
from research_pipeline import run_research_pipeline
from models import (
    ResearchRequest, ResearchStartResponse,
    ResearchStatusResponse, ResearchProgress,
    ALL_SIGNAL_KEYS,
)

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
# In-memory job store
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
        "events": {},           # company_index -> threading.Event
        "webhook_data": {},     # company_index -> list (unused in streaming, kept for batch)
        "source_companies": {}, # company_index -> company dict
        "timers": {},           # company_index -> threading.Timer (idle timeout)
    }


# ---------------------------------------------------------------------------
# Webhook helpers
# ---------------------------------------------------------------------------

def _fire_event(job_id: str, active_idx: int, reason: str):
    """Set the event for a company and cancel any pending idle timer."""
    with store_lock:
        job = job_store.get(job_id)
        if not job:
            return
        timer = job["timers"].pop(active_idx, None)
        if timer:
            timer.cancel()
        event = job["events"].get(active_idx)
        if event and not event.is_set():
            logger.info(f"[{job_id}] Company {active_idx} done ({reason})")
            event.set()


def _reset_idle_timer(job_id: str, active_idx: int, idle_seconds: int = 60):
    """Restart the idle timer — fires if no new person arrives within idle_seconds."""
    def on_idle():
        logger.info(f"[{job_id}] Idle {idle_seconds}s exceeded for company {active_idx} — closing stream")
        _fire_event(job_id, active_idx, "idle_timeout")

    with store_lock:
        job = job_store.get(job_id)
        if not job:
            return
        old = job["timers"].pop(active_idx, None)
        if old:
            old.cancel()
        t = threading.Timer(idle_seconds, on_idle)
        t.daemon = True
        t.start()
        job["timers"][active_idx] = t


def _enrich_in_background(job_id: str, person: dict, source_company: dict):
    """Enrich a single person and append the result to job results immediately."""
    try:
        contact = step3_enrich_person(job_id, person, source_company)
        if contact:
            with store_lock:
                job_store[job_id]["results"].append(contact)
                if contact.get("most_probable_email"):
                    job_store[job_id]["progress"]["emails_enriched"] += 1
    except Exception as exc:
        logger.error(f"[{job_id}] Background enrich error: {exc}")


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
# GET /enrich/{job_id}/results  (json default, ?format=csv for download)
# ---------------------------------------------------------------------------
@app.get("/enrich/{job_id}/results")
def get_results(job_id: str, format: str = "json"):
    with store_lock:
        job = job_store.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Return partial results while still processing
    results = job["results"]
    status = job["status"]

    if format.lower() == "csv":
        if not results:
            raise HTTPException(status_code=404, detail="No results yet")
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

    if status != "complete" and not results:
        return {"status": status}

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
        payload = {}

    logger.info(f"[{job_id}] Webhook: {_json.dumps(payload)[:500]}")

    # Find the active (unset) event
    with store_lock:
        active_idx = next((i for i, e in job["events"].items() if not e.is_set()), None)

    if active_idx is None:
        return {"received": True}

    if isinstance(payload, dict) and ("first_name" in payload or "linkedin_profile_url" in payload):
        # --- Streaming: individual person ---
        with store_lock:
            job["progress"]["people_found"] += 1
            source_company = job["source_companies"].get(active_idx, {})

        logger.info(f"[{job_id}] Streamed person: {payload.get('full_name', '')} — enriching in background")

        _reset_idle_timer(job_id, active_idx)

        threading.Thread(
            target=_enrich_in_background,
            args=(job_id, payload, source_company),
            daemon=True,
        ).start()

    elif isinstance(payload, dict) and "leads" in payload:
        # --- Completion signal from ProntoHQ ---
        _fire_event(job_id, active_idx, "completion_signal")

    elif isinstance(payload, list):
        # --- Batch (non-streaming fallback) ---
        with store_lock:
            source_company = job["source_companies"].get(active_idx, {})
            job["progress"]["people_found"] += len(payload)

        for person in payload:
            threading.Thread(
                target=_enrich_in_background,
                args=(job_id, person, source_company),
                daemon=True,
            ).start()

        _fire_event(job_id, active_idx, "batch_complete")

    else:
        logger.warning(f"[{job_id}] Unrecognised webhook shape — ignoring")

    return {"received": True}


# ===========================================================================
# RESEARCH PIPELINE endpoints  /research/*
# ===========================================================================

def _make_research_job(total_companies: int, signals: list) -> dict:
    return {
        "type": "research",
        "status": "processing",
        "progress": {
            "companies_processed": 0,
            "companies_total": total_companies,
            "signals_completed": 0,
            "signals_total": total_companies * len(signals),
        },
        "results": [],
    }


# ---------------------------------------------------------------------------
# POST /research
# ---------------------------------------------------------------------------
@app.post("/research", response_model=ResearchStartResponse, status_code=202)
def start_research(body: ResearchRequest):
    if not body.companies:
        raise HTTPException(status_code=400, detail="companies must not be empty")

    signals = body.signals or ALL_SIGNAL_KEYS
    invalid = [s for s in signals if s not in ALL_SIGNAL_KEYS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown signal(s): {invalid}. Valid: {ALL_SIGNAL_KEYS}",
        )

    job_id = str(uuid.uuid4())
    total  = len(body.companies)

    with store_lock:
        job_store[job_id] = _make_research_job(total, signals)

    provider = body.provider if body.provider in ("claude", "openai") else "claude"

    request_data = {
        "companies": [c.model_dump() for c in body.companies],
        "signals": signals,
        "provider": provider,
    }

    threading.Thread(
        target=run_research_pipeline,
        args=(job_id, job_store, store_lock, request_data),
        daemon=True,
    ).start()

    logger.info(f"Research job {job_id} started — {total} companies, {len(signals)} signals, provider={provider}.")
    return ResearchStartResponse(
        job_id=job_id, status="processing",
        total_companies=total, signals=signals,
    )


# ---------------------------------------------------------------------------
# GET /research/{job_id}/status
# ---------------------------------------------------------------------------
@app.get("/research/{job_id}/status", response_model=ResearchStatusResponse)
def get_research_status(job_id: str):
    with store_lock:
        job = job_store.get(job_id)

    if job is None or job.get("type") != "research":
        raise HTTPException(status_code=404, detail="Research job not found")

    return ResearchStatusResponse(
        job_id=job_id,
        status=job["status"],
        progress=ResearchProgress(**job["progress"]),
    )


# ---------------------------------------------------------------------------
# GET /research/{job_id}/results  (?format=csv for download)
# ---------------------------------------------------------------------------
_SIGNAL_PAIRS = [f for s in ALL_SIGNAL_KEYS for f in (s, f"{s}_reasoning")]
RESEARCH_CSV_FIELDS = (
    ["date_today", "date_90_days_ago", "company_name", "domain", "company_linkedin_url"]
    + _SIGNAL_PAIRS
)


@app.get("/research/{job_id}/results")
def get_research_results(job_id: str, format: str = "json"):
    with store_lock:
        job = job_store.get(job_id)

    if job is None or job.get("type") != "research":
        raise HTTPException(status_code=404, detail="Research job not found")

    results = job["results"]
    status  = job["status"]

    if format.lower() == "csv":
        if not results:
            raise HTTPException(status_code=404, detail="No results yet")
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=RESEARCH_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=research_{job_id}.csv"},
        )

    if status != "complete" and not results:
        return {"status": status}

    return results
