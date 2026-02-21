# app/models/evidence.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from enum import Enum

# Document Summary (replaces per-document listing)

class DocumentSummary(BaseModel):
    """Aggregated document stats for a company — no individual doc rows."""
    total_documents: int = 0
    by_status: Dict[str, int] = Field(default_factory=dict)
    by_filing_type: Dict[str, int] = Field(default_factory=dict)
    total_chunks: int = 0
    total_words: int = 0
    earliest_filing: Optional[str] = None
    latest_filing: Optional[str] = None
    last_collected: Optional[str] = None
    last_processed: Optional[str] = None



# Signal Models (unchanged)


class SignalEvidence(BaseModel):
    """A single external signal observation."""
    id: str
    category: str
    source: str
    signal_date: Optional[datetime] = None
    raw_value: Optional[str] = None
    normalized_score: Optional[float] = None
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


class SignalSummary(BaseModel):
    """Aggregated signal scores for a company."""
    technology_hiring_score: Optional[float] = None
    innovation_activity_score: Optional[float] = None
    digital_presence_score: Optional[float] = None
    leadership_signals_score: Optional[float] = None
    composite_score: Optional[float] = None
    signal_count: int = 0
    last_updated: Optional[datetime] = None



# Company Evidence Response


class CompanyEvidenceResponse(BaseModel):
    """Combined evidence response for a company."""
    company_id: str
    company_name: str
    ticker: str
    document_summary: DocumentSummary = Field(default_factory=DocumentSummary)
    signals: List[SignalEvidence] = []
    signal_count: int = 0
    signal_summary: Optional[SignalSummary] = None



# Backfill Models (unchanged)


class BackfillStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    CANCELLED = "cancelled"
    FAILED = "failed"


class BackfillResponse(BaseModel):
    """Returned immediately when a backfill is triggered."""
    task_id: str
    status: BackfillStatus
    message: str


class CompanyBackfillResult(BaseModel):
    """Result of backfill for a single company."""
    ticker: str
    status: str
    sec_result: Optional[Dict[str, Any]] = None
    signal_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class BackfillProgress(BaseModel):
    """Progress info for a backfill task."""
    companies_completed: int = 0
    total_companies: int = 0
    current_company: Optional[str] = None
    skipped_companies: List[str] = []


class BackfillTaskStatus(BaseModel):
    """Full status response for a backfill task."""
    task_id: str
    status: BackfillStatus
    progress: BackfillProgress
    company_results: List[CompanyBackfillResult] = []
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

# ─── NEW: Glassdoor Culture Models ───

class GlassdoorReview(BaseModel):
    """A single Glassdoor review."""
    review_id: str
    rating: float  # 1-5 stars
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str] = None
    is_current_employee: bool = False
    job_title: str = ""
    review_date: Optional[str] = None  # date string from scrape


class CultureSignal(BaseModel):
    """Aggregated culture signal from Glassdoor."""
    company_id: str
    ticker: str

    # Component scores (0-100)
    innovation_score: float = 0.0
    data_driven_score: float = 0.0
    change_readiness_score: float = 0.0
    ai_awareness_score: float = 0.0

    # Aggregate
    overall_score: float = 0.0

    # Metadata
    review_count: int = 0
    avg_rating: float = 0.0
    current_employee_ratio: float = 0.0
    confidence: float = 0.0

    # Evidence
    positive_keywords_found: List[str] = []
    negative_keywords_found: List[str] = []
    sample_reviews: List[Dict[str, Any]] = []   # first few reviews for audit


# ─── NEW: Board Governance Models ───

class BoardMember(BaseModel):
    """A board member or executive."""
    name: str
    title: str
    committees: List[str] = []
    bio: str = ""
    is_independent: bool = False
    tenure_years: int = 0


class GovernanceSignal(BaseModel):
    """Board-derived governance signal."""
    company_id: str
    ticker: str

    # Boolean indicators
    has_tech_committee: bool = False
    has_ai_expertise: bool = False
    has_data_officer: bool = False
    has_risk_tech_oversight: bool = False
    has_ai_in_strategy: bool = False

    # Metrics
    tech_expertise_count: int = 0
    independent_ratio: float = 0.0

    # Final score
    governance_score: float = 20.0   # base score
    confidence: float = 0.0

    # Evidence
    ai_experts: List[str] = []
    relevant_committees: List[str] = []
    board_members: List[Dict[str, Any]] = []

# Stats Models (unchanged)


class CompanyDocumentStat(BaseModel):
    """Document counts by filing type for one company."""
    ticker: str
    form_10k: int = 0
    form_10q: int = 0
    form_8k: int = 0
    def_14a: int = 0
    total: int = 0
    chunks: int = 0
    word_count: int = 0
    last_collected: Optional[str] = None
    last_processed: Optional[str] = None


class CompanySignalStat(BaseModel):
    """Signal summary for one company (from company_signal_summaries table)."""
    ticker: str
    technology_hiring_score: Optional[float] = None
    innovation_activity_score: Optional[float] = None
    digital_presence_score: Optional[float] = None
    leadership_signals_score: Optional[float] = None
    composite_score: Optional[float] = None
    signal_count: int = 0
    last_updated: Optional[str] = None


class SignalCategoryBreakdown(BaseModel):
    """Signal count and average confidence per category."""
    category: str
    count: int = 0
    avg_score: Optional[float] = None
    avg_confidence: Optional[float] = None


class EvidenceStatsResponse(BaseModel):
    """Overall evidence collection statistics."""
    companies_tracked: int
    total_documents: int
    total_chunks: int
    total_words: int
    total_signals: int
    documents_by_status: Dict[str, int] = {}
    signals_by_category: List[SignalCategoryBreakdown] = []
    documents_by_company: List[CompanyDocumentStat] = []
    signals_by_company: List[CompanySignalStat] = []


