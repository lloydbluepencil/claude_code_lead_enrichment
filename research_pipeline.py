"""
research_pipeline.py — Company signal research pipeline using Claude + web search.
Runs 7 configurable signals per company and returns Yes/No for each.
"""

import os
import time
import threading
import logging
from datetime import datetime, timedelta

import anthropic

logger = logging.getLogger(__name__)

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

def _build_prompt(signal: str, company_name: str, website: str, linkedin_url: str) -> str:
    today  = _today()
    cutoff = _cutoff()

    DATE_GATE = f"""
Hard date gate (apply before deciding):
- Extract an explicit date from each source (LinkedIn post date, press release date, article date).
- If the source has no explicit date, discard it.
- If the date is earlier than {cutoff}, discard it.
- Do NOT treat "page updated/last modified", "recently", "this year", or similar as a valid date.

Output only: Yes or No."""

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

def _call_claude(prompt: str) -> str:
    """Call Claude with web search. Returns 'Yes', 'No', or 'Error'."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")

    client = anthropic.Anthropic(api_key=api_key)

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        tools=[{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 5,
        }],
        messages=[{"role": "user", "content": prompt}],
    )

    # Collect all text blocks and log for debugging
    full_text = ""
    for block in response.content:
        if hasattr(block, "text") and block.text.strip():
            full_text += block.text.strip() + " "

    full_text = full_text.strip()
    logger.info(f"Claude raw response: {full_text[:300]}")

    if not full_text:
        return "Unknown"

    # Use the LAST standalone Yes/No in the response — the model often
    # explains reasoning first and gives the final answer at the end.
    import re
    matches = re.findall(r'\b(yes|no)\b', full_text, re.IGNORECASE)
    if matches:
        return "Yes" if matches[-1].lower() == "yes" else "No"

    return "Unknown"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _research_company(
    job_id: str,
    company: dict,
    signals: list,
    job_store: dict,
    lock: threading.Lock,
) -> dict:
    company_name = company.get("company_name", "")
    domain       = company.get("domain", "")
    linkedin_url = company.get("company_linkedin_url", "")
    website = (
        f"https://{domain}"
        if domain and not domain.startswith("http")
        else domain or f"https://www.google.com/search?q={company_name}"
    )

    result: dict = {"company_name": company_name, "domain": domain}

    for signal in signals:
        logger.info(f"[{job_id}] {company_name} — researching signal: {signal}")
        try:
            prompt = _build_prompt(signal, company_name, website, linkedin_url)
            answer = _call_claude(prompt)
            result[signal] = answer
            logger.info(f"[{job_id}] {company_name} — {signal}: {answer}")
        except Exception as exc:
            logger.error(f"[{job_id}] {company_name} — {signal} error: {exc}")
            result[signal] = "Error"

        with lock:
            job_store[job_id]["progress"]["signals_completed"] += 1

        time.sleep(0.5)  # avoid hitting rate limits between signals

    return result


def run_research_pipeline(
    job_id: str,
    job_store: dict,
    lock: threading.Lock,
    request_data: dict,
):
    """Background thread — researches each company for all selected signals."""
    companies = request_data["companies"]
    signals   = request_data["signals"]

    for company in companies:
        try:
            result = _research_company(job_id, company, signals, job_store, lock)
            with lock:
                job_store[job_id]["results"].append(result)
                job_store[job_id]["progress"]["companies_processed"] += 1
        except Exception as exc:
            logger.error(
                f"[{job_id}] Unhandled error researching "
                f"{company.get('company_name')}: {exc}"
            )
            with lock:
                job_store[job_id]["progress"]["companies_processed"] += 1

    with lock:
        job_store[job_id]["status"] = "complete"

    logger.info(f"[{job_id}] Research pipeline complete.")
