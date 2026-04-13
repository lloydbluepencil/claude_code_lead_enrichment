from pydantic import BaseModel
from typing import List, Optional


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
