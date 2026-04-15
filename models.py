from pydantic import BaseModel
from typing import List, Optional, Dict


class Company(BaseModel):
    company_name: str
    domain: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    address: Optional[str] = None


class Filters(BaseModel):
    job_titles: List[str]
    excluded_job_titles: Optional[List[str]] = []
    seniority_levels: Optional[List[str]] = []
    limit: Optional[int] = 25


class EnrichRequest(BaseModel):
    companies: List[Company]
    filters: Filters


class EnrichStartResponse(BaseModel):
    job_id: str
    status: str
    total_companies: int


class Progress(BaseModel):
    companies_processed: int
    companies_total: int
    people_found: int
    emails_enriched: int


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    progress: Progress


# ---------------------------------------------------------------------------
# Research models
# ---------------------------------------------------------------------------

ALL_SIGNAL_KEYS = [
    "digital_transformation",
    "loyalty_program",
    "app_digital_channel",
    "ma",
    "financing",
    "ipo",
    "geo_expansion",
]

SIGNAL_DISPLAY_NAMES: Dict[str, str] = {
    "digital_transformation": "Recent Digital Transformation Initiative",
    "loyalty_program":        "Recent Loyalty Program Launch",
    "app_digital_channel":    "Recent App / Digital Channel Launch",
    "ma":                     "Recent M&A",
    "financing":              "Recent Financing",
    "ipo":                    "Recent IPO Announcement",
    "geo_expansion":          "Recent Market / Geo Expansion",
}


class ResearchCompany(BaseModel):
    company_name: str
    domain: Optional[str] = None
    company_linkedin_url: Optional[str] = None


class ResearchRequest(BaseModel):
    companies: List[ResearchCompany]
    signals: Optional[List[str]] = None   # None = run all signals


class ResearchStartResponse(BaseModel):
    job_id: str
    status: str
    total_companies: int
    signals: List[str]


class ResearchProgress(BaseModel):
    companies_processed: int
    companies_total: int
    signals_completed: int
    signals_total: int


class ResearchStatusResponse(BaseModel):
    job_id: str
    status: str
    progress: ResearchProgress


class ResearchResult(BaseModel):
    company_name: str
    domain: Optional[str] = ""
    digital_transformation: Optional[str] = None
    loyalty_program: Optional[str] = None
    app_digital_channel: Optional[str] = None
    ma: Optional[str] = None
    financing: Optional[str] = None
    ipo: Optional[str] = None
    geo_expansion: Optional[str] = None


# ---------------------------------------------------------------------------
# Contact enrichment models
# ---------------------------------------------------------------------------


class Contact(BaseModel):
    full_name: Optional[str] = ""
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    title: Optional[str] = ""
    company_name: Optional[str] = ""
    linkedin_profile_url: Optional[str] = ""
    most_probable_email: Optional[str] = ""
    most_probable_email_status: Optional[str] = ""
    phone: Optional[str] = ""
    years_in_position: Optional[float] = None
    years_in_company: Optional[float] = None
    contact_classification: Optional[str] = ""
    enrichment_status: Optional[str] = ""
    source_company: Optional[str] = ""
