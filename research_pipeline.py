"""
research_pipeline.py — Company signal research pipeline.
Supports Claude (Anthropic) and GPT-4o (OpenAI) as LLM providers.
Runs 7 configurable signals per company and returns Yes/No for each.
"""

import csv
import io
import os
import re
import time
import random
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import anthropic
import requests as _requests
from openai import OpenAI

logger = logging.getLogger(__name__)

SUPPORTED_PROVIDERS = ("claude", "openai")

# Max concurrent LLM calls across all jobs
_LLM_SEMAPHORE = threading.Semaphore(5)

# Delay between signals per provider (seconds)
_SIGNAL_DELAYS = {
    "claude": 30.0,   # Claude has stricter TPM limits
    "openai": 5.0,    # GPT-4o handles much higher throughput
}

# Max companies processed in parallel
_COMPANY_WORKERS = 5

# Make.com webhook for job completion reports
MAKE_WEBHOOK_URL = "https://hook.eu2.make.com/ynopubwvvpk3h6c5ftvjqqwdmh2igy99"

# Token pricing per 1M tokens (USD)
_PRICING = {
    "claude": {"input": 3.00,  "output": 15.00},
    "openai": {"input": 2.50,  "output": 10.00},
}

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _today() -> str:
    return datetime.now().strftime("%B %d, %Y")


def _cutoff() -> str:
    return (datetime.now() - timedelta(days=90)).strftime("%B %d, %Y")


# ---------------------------------------------------------------------------
# Signal prompt templates
# ---------------------------------------------------------------------------

def _build_custom_prompt(template: str, company_name: str, domain: str, website: str, linkedin_url: str) -> str:
    """Substitute user-defined variables into a custom signal prompt template."""
    filled = template.format(
        company_name=company_name,
        domain=domain,
        website=website,
        company_linkedin_url=linkedin_url,
    )
    return (
        filled
        + "\n\nOutput format:\n"
        "1. Write a short findings summary in 2-3 sentences: what you found (or didn't find), "
        "which source(s), and the date(s). Start directly with the finding — do not open with "
        "phrases like \"After reviewing\", \"Based on\", or \"I found\".\n"
        "2. End with a single line containing only \"Yes\" or \"No\"."
    )


def _build_prompt(signal: str, company_name: str, website: str, linkedin_url: str) -> str:
    today  = _today()
    cutoff = _cutoff()

    DATE_GATE = f"""
Hard date gate (apply before deciding):
- Extract an explicit date from each source (LinkedIn post date, press release date, article date).
- If the source has no explicit date, discard it.
- If the date is earlier than {cutoff}, discard it.
- Do NOT treat "page updated/last modified", "recently", "this year", or similar as a valid date.

Output format:
1. Write a short findings summary in 2-3 sentences: what you found (or didn't find), which source(s), and the date(s). Start directly with the finding — do not open with phrases like "After reviewing", "Based on", or "I found".
2. End with a single line containing only "Yes" or "No"."""

    SEARCH_SOURCES = f"""Search:
- {website} (news / press releases / blog / IR)
- {linkedin_url} posts
- Reputable third-party news sources"""

    prompts = {

        "digital_transformation": f"""Determine whether {company_name} has announced any digital transformation initiative with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}

A qualifying "digital transformation initiative" is an enterprise-scale program such as: ERP/CRM modernization or consolidation, core system/cloud migration, data platform (warehouse/lakehouse) + governance, DevSecOps/CI/CD + observability, Zero Trust/security transformation, finance/procurement automation, omnichannel customer platform, supply chain digitization/control tower/IoT tracking, enterprise integration via APIs/event-driven architecture, or AI deployed at scale for customer ops or core operations.

Non-qualifying: small pilots, minor website/app refreshes, isolated tool purchase, generic "innovation" statements.
{DATE_GATE}""",

        "loyalty_program": f"""Determine whether {company_name} has announced or launched a new, significantly revamped, or expanded loyalty/rewards program with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}
- App store listings (Apple App Store, Google Play) — check for a new dedicated loyalty app or a major update explicitly tied to a loyalty program launch.

What qualifies: launched a brand-new loyalty/rewards/membership program; rebranded or replaced an existing program with a new one (new name, new structure, new tier system); completed a major overhaul that fundamentally changes how members earn, redeem, or are tiered; launched a new co-branded or coalition program; expanded an existing program into a new market/country for the first time with a dated launch; launched a dedicated loyalty app or digital wallet; merged two or more previously separate programs; opened enrollment for a previously announced program.

What does NOT qualify: minor point-value adjustments, bonus-point promotions, or limited-time multiplier campaigns; seasonal promotions branded as "rewards"; generic discount codes or referral bonuses; rumors or "exploring" with no confirmed launch; employee loyalty/HR programs; adding a payment method without structural change; old launches outside the date window.
{DATE_GATE}""",

        "app_digital_channel": f"""Determine whether {company_name} has announced or launched a new digital channel with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}

What qualifies: launched a new mobile app (iOS, Android, or both); launched a new ecommerce site, online store, or digital marketplace; launched a new customer-facing web portal, self-service platform, or client dashboard; launched a new super-app, mini-program, or embedded digital experience; completed a major redesign/rebuild/replatforming explicitly described as "new version", "completely redesigned", "rebuilt from the ground up", "V2", or "all-new experience"; migrated to a new digital platform with a public-facing launch announcement; launched a new digital banking channel, telehealth portal, or industry-specific digital service layer.

What does NOT qualify: minor feature updates, bug fixes, or incremental version releases; A/B tests, beta programs, or invite-only pilots; backend migrations with no customer-facing change; landing pages or microsites; social media account launches; integrations where the company is not the channel owner; announcements of intent with no confirmed launch date.
{DATE_GATE}""",

        "ma": f"""Determine whether {company_name} has announced any merger, acquisition, divestiture, or majority investment with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}

What qualifies: acquired another company/asset/business unit; been acquired or entered into an agreement to be acquired; merged with another company; sold/divested a business unit or major asset; received a majority investment resulting in a change of control; signed/announced a definitive agreement, term sheet, LOI, or submitted/received regulatory approvals related to the above.

What does NOT qualify: rumors or speculation with no confirmation from the company or official filings; minority investments with no control change; partnerships/alliances/joint marketing agreements; internal restructurings without an external buyer/seller; old deals outside the date window or undated claims.
{DATE_GATE}""",

        "financing": f"""Determine whether {company_name} has announced any funding round, capital raise, or significant financial investment with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}

What qualifies: raised a named funding round (Pre-Seed, Seed, Series A/B/C/D+, Bridge, Extension); closed a debt financing round, venture debt facility, or convertible note; received a growth equity investment or growth capital infusion; completed a public offering (IPO, secondary offering, SPAC merger resulting in capital raise); secured a grant or non-dilutive funding exceeding $500K from a government body, foundation, or institutional program.

What does NOT qualify: rumors or speculation with no confirmation; revenue/income/earnings announcements; credit lines or revolving facilities used in ordinary course of business; awards or accelerator acceptances under $500K; crowdfunding campaigns not yet closed; customer contracts framed as "investment in the platform"; old rounds outside the date window or undated claims.
{DATE_GATE}""",

        "ipo": f"""Determine whether {company_name} has announced, filed for, or completed an initial public offering (or equivalent public listing event) with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}
- Regulatory/exchange filings (SEC EDGAR, ADX, DFM, Tadawul/CMA, QSE, Boursa Kuwait, Bahrain Bourse, MSX, or other relevant stock exchange announcements).

What qualifies: filed a registration statement, prospectus, or offering document for an IPO (e.g., S-1, F-1, or regional equivalent); announced intention to pursue an IPO or public listing; received regulatory or exchange approval to list; priced its IPO or set a price range; begun trading on a public stock exchange for the first time; completed a direct listing; completed a SPAC/de-SPAC transaction resulting in the company becoming publicly traded; completed a dual/cross-listing on a new exchange (if previously private); announced appointment of underwriters or bookrunners specifically for an IPO; published a pre-IPO investor presentation, roadshow schedule, or "intention to float" notice.

What does NOT qualify: rumors or speculation with no confirmation from the company, advisors, or regulatory filings; secondary offerings by existing shareholders of an already-public company; private placements or pre-IPO funding rounds; internal board discussions or leaked memos with no public filing; analyst predictions or "IPO watch" articles without a confirmed filing; postponed/shelved IPOs without a dated reactivation announcement within the window.
{DATE_GATE}""",

        "geo_expansion": f"""Determine whether {company_name} has announced any expansion plans (new office/location, new stores, market/country entry, facility opening, HQ move, or major footprint expansion) with an explicit announcement/publish date on or after {cutoff} and on or before {today}.

{SEARCH_SOURCES}

What qualifies: opening a new office or branch; opening a new store; entering a new market/country/region; launching a new site/facility (plant, warehouse, R&D center, innovation hub, service center); relocating or expanding a HQ or major operational footprint; hiring plans explicitly tied to a new location (e.g., "opening our Dubai office", "setting up in Riyadh"); signing a lease, securing permits, or announcing a launch date for a new location.

What does NOT qualify: remote hiring with no new location; "we serve customers in X" without an opening/entry announcement; partner/distributor/reseller office unless the company states it's their own office; event attendance, roadshows, or temporary pop-ups; vague growth statements ("expanding globally") without a specific location/action; rumors not confirmed by the company.
{DATE_GATE}""",
    }

    return prompts[signal]


# ---------------------------------------------------------------------------
# Claude web search call
# ---------------------------------------------------------------------------

def _parse_yes_no(text: str, provider: str) -> tuple[str, str]:
    """
    Extract the last standalone Yes/No from a model response.
    Returns (answer, reasoning) where reasoning is the full response text.
    """
    text = text.strip()
    logger.info(f"[{provider}] raw response: {text[:300]}")
    reasoning = text  # preserve full text for hover display
    if not text:
        return "Unknown", ""
    matches = re.findall(r'\b(yes|no)\b', text, re.IGNORECASE)
    if matches:
        answer = "Yes" if matches[-1].lower() == "yes" else "No"
        return answer, reasoning
    return "Unknown", reasoning


def _call_claude(prompt: str) -> tuple[str, str, int, int]:
    """Call Claude Sonnet with built-in web search.
    Returns (answer, reasoning, input_tokens, output_tokens)."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        tools=[{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 3,
        }],
        messages=[{"role": "user", "content": prompt}],
    )

    full_text = " ".join(
        block.text.strip()
        for block in response.content
        if hasattr(block, "text") and block.text.strip()
    )
    answer, reasoning = _parse_yes_no(full_text, "claude")
    input_tokens  = getattr(response.usage, "input_tokens",  0)
    output_tokens = getattr(response.usage, "output_tokens", 0)
    return answer, reasoning, input_tokens, output_tokens


def _call_openai(prompt: str) -> tuple[str, str, int, int]:
    """Call GPT-4o with built-in web search via the Responses API.
    Returns (answer, reasoning, input_tokens, output_tokens)."""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model="gpt-4o",
        tools=[{"type": "web_search_preview"}],
        input=prompt,
    )

    text = response.output_text or ""
    answer, reasoning = _parse_yes_no(text, "openai")
    usage = getattr(response, "usage", None)
    input_tokens  = getattr(usage, "input_tokens",  0) if usage else 0
    output_tokens = getattr(usage, "output_tokens", 0) if usage else 0
    return answer, reasoning, input_tokens, output_tokens


def _call_model(prompt: str, provider: str) -> tuple[str, str, int, int]:
    """
    Route to the selected LLM provider.
    Wraps the call with exponential backoff + jitter on 429 errors.
    Returns (answer, reasoning, input_tokens, output_tokens).
    """
    max_retries = 4
    delay = _SIGNAL_DELAYS.get(provider, 5.0)

    with _LLM_SEMAPHORE:
        for attempt in range(max_retries):
            try:
                if provider == "openai":
                    return _call_openai(prompt)
                return _call_claude(prompt)
            except Exception as exc:
                is_rate_limit = "429" in str(exc) or "rate_limit" in str(exc).lower()
                if is_rate_limit and attempt < max_retries - 1:
                    jitter = random.uniform(0, delay * 0.25)
                    wait   = delay + jitter
                    logger.warning(f"Rate limited (attempt {attempt + 1}/{max_retries}), retrying in {wait:.1f}s")
                    time.sleep(wait)
                    delay *= 2  # exponential backoff
                else:
                    raise

    return "Unknown", "", 0, 0


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _research_company(
    job_id: str,
    company: dict,
    signals: list,
    provider: str,
    job_store: dict,
    lock: threading.Lock,
    custom_signals: list = None,  # list of CustomSignal dicts
) -> dict:
    company_name = company.get("company_name", "")
    domain       = company.get("domain", "")
    linkedin_url = company.get("company_linkedin_url", "")
    website = (
        f"https://{domain}"
        if domain and not domain.startswith("http")
        else domain or f"https://www.google.com/search?q={company_name}"
    )

    result: dict = {
        "company_name":         company_name,
        "domain":               domain,
        "company_linkedin_url": linkedin_url,
        "date_today":           _today(),
        "date_90_days_ago":     _cutoff(),
    }

    def _run_signal(signal_key: str, prompt: str):
        nonlocal result
        logger.info(f"[{job_id}] {company_name} — [{provider}] researching: {signal_key}")
        try:
            answer, reasoning, in_tok, out_tok = _call_model(prompt, provider)
            result[signal_key]                = answer
            result[f"{signal_key}_reasoning"] = reasoning
            logger.info(f"[{job_id}] {company_name} — {signal_key}: {answer}")
        except Exception as exc:
            logger.error(f"[{job_id}] {company_name} — {signal_key} error: {exc}")
            result[signal_key]                = "Error"
            result[f"{signal_key}_reasoning"] = str(exc)
            in_tok, out_tok = 0, 0

        with lock:
            job_store[job_id]["progress"]["signals_completed"] += 1
            job_store[job_id]["token_usage"]["input_tokens"]  += in_tok
            job_store[job_id]["token_usage"]["output_tokens"] += out_tok

        base_delay = _SIGNAL_DELAYS.get(provider, 5.0)
        time.sleep(base_delay + random.uniform(0, base_delay * 0.15))

    # Built-in signals
    for signal in signals:
        _run_signal(signal, _build_prompt(signal, company_name, website, linkedin_url))

    # Custom signals
    for cs in (custom_signals or []):
        key      = cs.get("key", "")
        template = cs.get("prompt_template", "")
        if not key or not template:
            continue
        try:
            prompt = _build_custom_prompt(template, company_name, domain, website, linkedin_url)
        except KeyError as exc:
            prompt = template  # fall back to raw template if variable missing
            logger.warning(f"[{job_id}] Custom signal {key!r} template variable missing: {exc}")
        _run_signal(key, prompt)

    return result


# ---------------------------------------------------------------------------
# Make.com webhook helpers
# ---------------------------------------------------------------------------

def _build_csv(results: list, signals: list) -> str:
    """Build CSV string from research results."""
    signal_pairs = [f for s in signals for f in (s, f"{s}_reasoning")]
    fields = ["date_today", "date_90_days_ago", "company_name", "domain", "company_linkedin_url"] + signal_pairs
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
    return out.getvalue()


def _send_make_webhook(job_id: str, job: dict, all_signals: list):
    """POST completion report + full CSV to Make.com webhook."""
    started_at   = job.get("started_at")
    completed_at = datetime.now()
    elapsed_sec  = (completed_at - started_at).total_seconds() if started_at else 0
    processing_time = f"{int(elapsed_sec // 60)}m {int(elapsed_sec % 60)}s"

    provider = job.get("provider", "openai")
    model    = "claude-sonnet-4-6" if provider == "claude" else "gpt-4o"

    usage   = job.get("token_usage", {})
    in_tok  = usage.get("input_tokens",  0)
    out_tok = usage.get("output_tokens", 0)
    pricing = _PRICING.get(provider, _PRICING["claude"])
    cost    = (in_tok * pricing["input"] + out_tok * pricing["output"]) / 1_000_000

    csv_content = _build_csv(job.get("results", []), all_signals)

    metadata = {
        "date_run":        completed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "processing_time": processing_time,
        "provider":        provider,
        "model":           model,
        "tokens_cost":     f"${cost:.4f}",
        "input_tokens":    str(in_tok),
        "output_tokens":   str(out_tok),
        "total_companies": str(job["progress"]["companies_total"]),
        "run_via":         job.get("run_via", "webapp"),
        "job_id":          job_id,
        "status":          "complete",
    }

    try:
        resp = _requests.post(
            MAKE_WEBHOOK_URL,
            files={"data": (f"research_{job_id}.csv", csv_content, "text/csv")},
            data=metadata,
            timeout=30,
        )
        logger.info(f"[{job_id}] Make.com webhook → {resp.status_code}")
    except Exception as exc:
        logger.error(f"[{job_id}] Make.com webhook failed: {exc}")


def run_research_pipeline(
    job_id: str,
    job_store: dict,
    lock: threading.Lock,
    request_data: dict,
):
    """Background thread — researches each company for all selected signals."""
    companies      = request_data["companies"]
    signals        = request_data["signals"]
    provider       = request_data.get("provider", "openai")
    custom_signals = request_data.get("custom_signals", [])

    def _process(company):
        try:
            result = _research_company(job_id, company, signals, provider, job_store, lock, custom_signals)
            with lock:
                job_store[job_id]["results"].append(result)
                job_store[job_id]["progress"]["companies_processed"] += 1
        except Exception as exc:
            logger.error(f"[{job_id}] Unhandled error researching {company.get('company_name')}: {exc}")
            with lock:
                job_store[job_id]["progress"]["companies_processed"] += 1

    with ThreadPoolExecutor(max_workers=_COMPANY_WORKERS) as pool:
        futures = [pool.submit(_process, company) for company in companies]
        for future in as_completed(futures):
            future.result()

    with lock:
        job_store[job_id]["status"] = "complete"
        job_snapshot = dict(job_store[job_id])

    all_signals = signals + [cs.get("key", "") for cs in custom_signals if cs.get("key")]
    logger.info(f"[{job_id}] Research pipeline complete.")
    _send_make_webhook(job_id, job_snapshot, all_signals)
